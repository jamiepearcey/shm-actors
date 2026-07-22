//! The [`Artifact`] handle and its RCU/MVCC read/write/reclaim machinery.
//!
//! # Pin hazard handshake — the interleavings the `SeqCst` ordering forbids
//!
//! (ADR-0003a mandate 1.) S0 repacked [`PackedRef`](shm_core::PackedRef) to
//! `[seg:32|off:32]`, dropping the generation field, and made a
//! [`VersionManifest`](crate::VersionManifest) self-validate its
//! `{artifact_id, version}`. That alone does **not** close the *ghost-read
//! window*: if a reader's pin published *after* a reclaimer's pin scan, the
//! reclaimer could free + recycle the manifest chunk while it still held intact
//! old bytes, and a bare `manifest.version == pinned` check would validate
//! against ghost data whose `ChunkDesc`s reference freed chunks.
//!
//! The fix is a hazard-pointer handshake on each live-version [`PinSlot`]:
//!
//! - **Reader** ([`pin`](Artifact::pin)): (1) `Acquire`-load `current` → `v`;
//!   (2) `SeqCst` `fetch_add` on the slot's pin count — this **publishes** the
//!   pin; (3) re-validate the slot is still `{version == v, state == SLOT_LIVE}`
//!   with `SeqCst` loads — if it flipped to `SLOT_FREEING` or the version moved,
//!   undo the bump and retry; (4) only *then* `Acquire`-load `manifest_desc`,
//!   confirm it names the slot we pinned, and `read_manifest_checked`.
//! - **Reclaimer** ([`try_retire_version`]): (1) CAS-elect itself
//!   `SLOT_LIVE → SLOT_FREEING` and store `FREEING` with `SeqCst` **before**
//!   scanning; (2) `SeqCst`-load the pin count; `== 0` ⇒ free the version's
//!   exclusively-owned chunks + manifest and store `SLOT_FREE`; `> 0` ⇒ revert
//!   to `SLOT_LIVE` and leave the drop of the live pin (or a re-loop) to retire.
//!
//! **Why the ghost read is now impossible.** Let an *accepting* reader perform
//! `A1 = pins.fetch_add (SeqCst)` then `A2 = state.load (SeqCst) == LIVE`, and
//! let the *freeing* reclaimer perform `B1 = state.store FREEING (SeqCst)` then
//! `B2 = pins.load (SeqCst) == 0`. In the single `SeqCst` total order **S**:
//! the freeing reclaimer never reverts (it reverts only on `pins > 0`, i.e. it
//! did *not* free), so if `B1 <_S A2` then `A2` observes `FREEING` and the
//! reader rejects — contradiction. Hence `A2 <_S B1`. With `A1 <_S A2`
//! (program order) and `B1 <_S B2` (program order), transitivity gives
//! `A1 <_S B2`, so `B2` observes the incremented pin count (`≥ 1 ≠ 0`) and the
//! reclaimer does **not** free. ∎ No accepting reader ever dereferences a chunk
//! a reclaimer freed.
//!
//! Because `version` is monotonic and never reissued, a slot re-validated as
//! `{version == v, LIVE}` is unambiguously `v`'s slot; and confirming
//! `slot.manifest == head.manifest_desc` rejects a not-yet-endorsed slot (the
//! transient duplicate two committers create when both claim a slot for the same
//! `n+1` before the install CAS elects one).

use std::panic::RefUnwindSafe;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use shm_arrow::{
    read_batch_chunks, write_batch_chunks, ChunkAllocator, PoolAllocator, SchemaRegistry,
    SegmentBase,
};
use shm_core::{BorrowJournal, ChunkCtrl, ChunkDesc, PackedRef, Pool, PoolConfig, Segment, PUBLISHED};

use crate::error::{Error, Result};
use crate::event::{CommitKind, VersionEvent};
use crate::head::{ArtifactHead, NO_VERSION, OWNER_NONE, SLOT_FREE, SLOT_FREEING, SLOT_LIVE};
use crate::manifest::{read_manifest, read_manifest_checked, write_manifest, Manifest};

/// How a commit relates the new version to its predecessor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Commit {
    /// The new version supersedes its predecessor wholesale: its manifest lists
    /// only the newly staged chunk(s). Prior chunks are released once the old
    /// version is reclaimed.
    Replace,
    /// The new version's manifest is the prior version's retained chunks **plus**
    /// the newly staged chunk, so consecutive versions share the prior chunks
    /// (chunk-level refcount shared). Turnover costs O(new data), not O(table).
    Append,
    /// A ranged in-place-style update. **Deferred to v0.2**: currently returns
    /// [`Error::Unsupported`]. The range names the row span a full
    /// implementation would rewrite.
    Patch(core::ops::Range<u64>),
}

impl Commit {
    /// The wire [`CommitKind`] this commit publishes in its [`VersionEvent`].
    fn kind(&self) -> CommitKind {
        match self {
            Commit::Replace => CommitKind::Replace,
            Commit::Append => CommitKind::Append,
            Commit::Patch(_) => CommitKind::Patch,
        }
    }
}

/// A named, versioned, shared-memory object with RCU/MVCC concurrency.
///
/// An `Artifact` spans two segments:
///
/// - a **management** segment whose payload begins with the [`ArtifactHead`]
///   atomic control block, and
/// - a **data** segment carved into a [`shm_core::Pool`] that backs every data
///   chunk *and* every [`VersionManifest`](crate::VersionManifest) chunk.
///
/// Readers [`pin`](Artifact::pin) a version (never blocking); writers commit a
/// new version off to the side and install it with a single atomic swap.
pub struct Artifact {
    name_id: u32,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
    watch: Option<Box<dyn Fn(VersionEvent) + Send + Sync>>,
}

impl Artifact {
    /// Create a fresh artifact: lay a [`Pool`] into `data_seg` per `pool_config`
    /// and initialise the [`ArtifactHead`] at the base of `head_seg`'s payload.
    ///
    /// `name_id` is the interned artifact name stamped into every
    /// [`VersionEvent`]. The two segments may be distinct or the caller's choice;
    /// they must stay mapped for the artifact's (and every pin's) lifetime.
    pub fn create(
        name_id: u32,
        head_seg: Arc<Segment>,
        data_seg: Arc<Segment>,
        pool_config: &PoolConfig,
    ) -> Result<Artifact> {
        // Lay out the chunk pool in the data segment.
        Pool::create(&data_seg, pool_config)?;

        if head_seg.payload_len() < ArtifactHead::region_bytes() {
            return Err(Error::Core(shm_core::Error::LayoutOverflow(
                "head segment too small for ArtifactHead",
            )));
        }
        let ptr = head_seg.payload_ptr().cast::<ArtifactHead>();
        // SAFETY: `payload_ptr` is 64-byte aligned (>= `ArtifactHead`'s 8-byte
        // alignment) and the check above guarantees the region is large enough.
        // Creation is single-threaded by contract.
        unsafe { ArtifactHead::init_at(ptr) };

        Ok(Artifact {
            name_id,
            head_seg,
            data_seg,
            watch: None,
        })
    }

    /// Attach to an artifact whose segments were already initialised by
    /// [`Artifact::create`], validating the pool and head magics.
    pub fn attach(
        name_id: u32,
        head_seg: Arc<Segment>,
        data_seg: Arc<Segment>,
    ) -> Result<Artifact> {
        // Validates `POOL_MAGIC`.
        Pool::attach(&data_seg)?;
        if head_seg.payload_len() < ArtifactHead::region_bytes() {
            return Err(Error::BadMagic);
        }
        if !head_ref(&head_seg).check_magic() {
            return Err(Error::BadMagic);
        }
        Ok(Artifact {
            name_id,
            head_seg,
            data_seg,
            watch: None,
        })
    }

    /// Install a watch sink invoked with a [`VersionEvent`] after every
    /// successful commit. Keeps the pub/sub coupling injected: the runtime can
    /// forward the event onto an [`shm-ring`](shm_ring) `__artifacts` topic
    /// (e.g. `publisher.publish(event.to_desc())`) without `shm-artifact`
    /// depending on a live ring.
    pub fn with_watch<F>(mut self, sink: F) -> Artifact
    where
        F: Fn(VersionEvent) + Send + Sync + 'static,
    {
        self.watch = Some(Box::new(sink));
        self
    }

    /// The artifact's interned name id.
    #[inline]
    pub fn name_id(&self) -> u32 {
        self.name_id
    }

    /// The current (latest installed) version number, or [`NO_VERSION`] (`0`) if
    /// nothing has been committed yet.
    #[inline]
    pub fn current_version(&self) -> u64 {
        self.head().current.load(SeqCst)
    }

    /// Borrow the on-shm [`ArtifactHead`].
    #[inline]
    fn head(&self) -> &ArtifactHead {
        head_ref(&self.head_seg)
    }

    // ---- Write path: exclusive + optimistic commit ----

    /// Acquire the **fenced** exclusive write lease, returning a [`Committer`]
    /// bound to the fence token it acquired under. A second caller fails fast with
    /// [`Error::WriteLocked`] while a live owner holds it (unchanged healthy
    /// behaviour); `owner` must be a non-zero actor id.
    ///
    /// This is the **single-process / unjournaled** open: a lease leaked by a
    /// `kill -9`ed writer is not reclaimed. Cross-process actors should open
    /// through [`open_exclusive_journaled`](Artifact::open_exclusive_journaled) so
    /// a dead writer's lease is force-released (with a fence bump) by the
    /// coordinator (ADR-0003 item K), mirroring [`pin`](Artifact::pin) vs
    /// [`pin_journaled`](Artifact::pin_journaled).
    pub fn open_exclusive(&self, owner: u32) -> Result<Committer<'_>> {
        debug_assert_ne!(owner, OWNER_NONE, "writer owner id must be non-zero");
        match self.head().acquire_write_lease(owner) {
            Some(token) => Ok(Committer {
                artifact: self,
                owner,
                token,
                lease: None,
            }),
            None => Err(Error::WriteLocked),
        }
    }

    /// Acquire the fenced exclusive write lease **and journal it** so a crash
    /// while holding it is reclaimed (item K): the open records a
    /// [`WriteLease`](shm_core::JournalRecord::WriteLease) entry into `journal`
    /// (carrying this artifact's id and the acquired fence), and a clean release
    /// (commit / abort / drop) removes it. If this actor dies while the lease is
    /// held, the coordinator's lease-monitor replay force-releases the lease with
    /// a fence bump via [`release_leaked_write_lease`](Artifact::release_leaked_write_lease),
    /// unwedging the artifact and fencing the dead writer out.
    ///
    /// The lease is acquired first, then journalled; on journal exhaustion the
    /// lease is released again so the artifact is left exactly as it was found.
    pub fn open_exclusive_journaled<'j>(
        &'j self,
        owner: u32,
        journal: &'j BorrowJournal<'j>,
    ) -> Result<Committer<'j>> {
        debug_assert_ne!(owner, OWNER_NONE, "writer owner id must be non-zero");
        let head = self.head();
        let token = head.acquire_write_lease(owner).ok_or(Error::WriteLocked)?;
        match journal.record_write_lease(self.name_id, token) {
            Ok(slot) => Ok(Committer {
                artifact: self,
                owner,
                token,
                lease: Some(LeaseJournal { journal, slot }),
            }),
            Err(e) => {
                // Undo the acquire (bumps the fence — harmless) so a failed open
                // never leaves the lease stuck.
                head.release_write_lease(owner, token);
                Err(e.into())
            }
        }
    }

    /// Optimistically commit a new version, expecting the current version to be
    /// `expect`. Fails with [`Error::Conflict`] if another committer moved
    /// `current` first. Takes **no** lease, so multiple optimistic committers may
    /// race; the install CAS elects the winner. `owner` must be non-zero.
    pub fn commit_optimistic(
        &self,
        owner: u32,
        expect: u64,
        commit: Commit,
        batch: &RecordBatch,
        registry: &SchemaRegistry,
    ) -> Result<u64> {
        // Optimistic commits hold no lease, so there is no fence to validate.
        self.commit_inner(expect, owner, commit, batch, registry, None)
    }

    /// **ADDITIVE (v0.2 stage C — for `shm-stream`).** Optimistically install a
    /// batch of **pre-staged** chunks (each already written + loaned via
    /// [`shm_arrow::write_batch`] + [`ChunkCtrl::try_loan`](shm_core::ChunkCtrl::try_loan))
    /// as the next version, expecting `current == expect`.
    ///
    /// This is the pre-staged, multi-chunk analogue of
    /// [`commit_optimistic`](Artifact::commit_optimistic): where that writes one
    /// batch inline, this installs `N` chunks a stream accumulated invisibly.
    /// It shares the identical RCU install path
    /// ([`commit_staged_inner`](Artifact::commit_staged_inner)); see that method
    /// for the success/failure ownership contract `shm-stream` relies on. Fails
    /// with [`Error::Conflict`] if another committer moved `current` first, after
    /// returning every staged chunk to the pool.
    ///
    /// `schema_id` is the interned id shared by all staged chunks; `owner` must
    /// be non-zero. `batch_spans` partitions `staged` into Arrow batches (item F):
    /// `batch_spans[b]` consecutive `staged` chunks form batch `b`, and their sum
    /// must equal `staged.len()`.
    pub fn commit_staged_optimistic(
        &self,
        owner: u32,
        expect: u64,
        commit: Commit,
        staged: &[ChunkDesc],
        batch_spans: &[u32],
        schema_id: u32,
    ) -> Result<u64> {
        // Optimistic commits hold no lease, so there is no fence to validate.
        self.commit_staged_inner(expect, owner, commit, staged, batch_spans, schema_id, None)
    }

    /// The shared install path for both exclusive and optimistic single-batch
    /// commits.
    ///
    /// Writes and loans the single data chunk, then delegates to
    /// [`commit_staged_inner`](Artifact::commit_staged_inner) — the one true RCU
    /// install path — so single-batch commits and pre-staged (`shm-stream`)
    /// commits share identical staging, reference-counting, and CAS logic.
    fn commit_inner(
        &self,
        expect: u64,
        owner: u32,
        commit: Commit,
        batch: &RecordBatch,
        registry: &SchemaRegistry,
        lease_fence: Option<u32>,
    ) -> Result<u64> {
        debug_assert_ne!(owner, OWNER_NONE, "commit owner id must be non-zero");
        if let Commit::Patch(_) = commit {
            return Err(Error::Unsupported("Patch commit deferred to v0.2"));
        }

        let pool = Pool::attach(&self.data_seg)?;
        let alloc = PoolAllocator::new(&pool, &self.data_seg);
        let schema_id = registry.intern(&batch.schema());

        // Write the batch's data chunk(s) and loan each (FREE -> LOANED): exactly
        // the pre-staged shape `commit_staged_inner` installs. A large/nested
        // batch may span multiple chunks (item F); they form ONE batch, so its
        // span is the whole chunk list. Any staging failure below (including a
        // fenced lease) returns every chunk to the pool via that path's rollback.
        let staged = write_batch_chunks(&alloc, registry, batch)?;
        loan_all(&pool, &alloc, &staged, owner)?;
        let spans = [staged.len() as u32];
        self.commit_staged_inner(expect, owner, commit, &staged, &spans, schema_id, lease_fence)
    }

    /// **The one true RCU install path**, shared by single-batch commits and by
    /// `shm-stream`'s pre-staged multi-chunk commits.
    ///
    /// `staged` names chunks that have already been **written and loaned**
    /// (`LOANED`, owned by `owner`) — for a single-batch commit by
    /// [`commit_inner`](Artifact::commit_inner) just above, for a stream by its
    /// `append_batch`. This method publishes them, reference-counts any Append
    /// predecessor chunks, stages the manifest, and installs the new version with
    /// one linearising CAS.
    ///
    /// # Contract (relied on by `shm-stream`)
    ///
    /// - **Success:** every `staged` chunk is `PUBLISHED` and owned by the new
    ///   version (its reclamation is now governed by the artifact's pin/refcount
    ///   rules, not the writer's borrow journal). Returns the new version number.
    /// - **Failure:** every `staged` chunk is returned to the pool (`FREE`) —
    ///   whether it had been published (released via refcount) or was still
    ///   `LOANED` (dropped) — so the caller must **not** free them again. The
    ///   caller need only release its own borrow-journal slots. A fenced lease
    ///   ([`Error::Fenced`], item K) is one such failure: the `LOANED` staged
    ///   chunks are freed before returning, so a zombie writer's late commit
    ///   installs nothing and leaks nothing. (The `Patch` rejection is the sole
    ///   early return that does *not* consume `staged`; callers pre-validate the
    ///   commit kind, so a real staged commit never hits it — `shm-stream` rejects
    ///   `Patch` at `open`.)
    ///
    /// `lease_fence` is `Some(token)` for a leased ([`Committer`]) commit and
    /// `None` for an optimistic one; when `Some`, the head lease must still read
    /// `{owner, token}` or the commit is fenced.
    // The parameters are the irreducible install-path inputs (expectation, owner,
    // commit kind, the staged chunks + their batch spans, schema id, fence); each
    // is a distinct scalar/slice, so bundling them into a struct would only add
    // indirection without clarifying the one true RCU path.
    #[allow(clippy::too_many_arguments)]
    fn commit_staged_inner(
        &self,
        expect: u64,
        owner: u32,
        commit: Commit,
        staged: &[ChunkDesc],
        staged_spans: &[u32],
        schema_id: u32,
        lease_fence: Option<u32>,
    ) -> Result<u64> {
        debug_assert_ne!(owner, OWNER_NONE, "commit owner id must be non-zero");
        debug_assert_eq!(
            staged_spans.iter().map(|&s| s as usize).sum::<usize>(),
            staged.len(),
            "staged batch spans must partition the staged chunk list"
        );
        if let Commit::Patch(_) = commit {
            return Err(Error::Unsupported("Patch commit deferred to v0.2"));
        }

        let head = self.head();
        let pool = Pool::attach(&self.data_seg)?;

        // 0. Fenced-lease guard (item K). If this committer's lease was fenced
        //    (its fence advanced — the coordinator declared it dead and released
        //    the lease, letting a second writer take over), reject **before**
        //    publishing anything and return every `LOANED` staged chunk to the
        //    pool. Checking here linearises against the release CAS: once the
        //    fence has advanced, no `{owner, token}` load ever matches again, so a
        //    zombie can never install. A second writer that took over but has not
        //    yet committed leaves the fence check passing, and the install CAS on
        //    `current` (step 5) is the backstop — a superseding commit moves
        //    `current`, so the zombie's CAS fails with `Conflict`.
        if let Some(token) = lease_fence {
            if !head.lease_held_by(owner, token) {
                for d in staged {
                    free_loaned(&pool, d);
                }
                return Err(Error::Fenced);
            }
        }

        // 1. Publish each pre-staged data chunk (LOANED -> PUBLISHED, +1 version
        //    ref, owner released). On a mid-loop failure, undo so *every* staged
        //    chunk is returned to the pool: release those already published and
        //    drop the remaining still-LOANED loans.
        let mut published: Vec<ChunkDesc> = Vec::with_capacity(staged.len());
        for (i, desc) in staged.iter().enumerate() {
            match publish_staged(&pool, desc) {
                Ok(()) => published.push(*desc),
                Err(e) => {
                    for c in &published {
                        release_chunk(&pool, c);
                    }
                    for d in &staged[i..] {
                        free_loaned(&pool, d);
                    }
                    return Err(e);
                }
            }
        }

        // 2. Assemble the new version's manifest chunk list + batch boundaries,
        //    referencing any retained (Append) chunks into the new version up
        //    front. Retained chunks keep the prior version's batch spans; the
        //    newly staged chunks contribute `staged_spans`.
        let mut chunks: Vec<ChunkDesc> = Vec::new();
        let mut batch_spans: Vec<u32> = Vec::new();
        let mut retained: Vec<ChunkDesc> = Vec::new();
        let is_append = matches!(commit, Commit::Append) && expect != NO_VERSION;
        if is_append {
            let mref = PackedRef(head.manifest_desc.load(Acquire));
            // Validate the prior manifest self-identifies as this artifact's
            // `expect` version (ADR-0003a) before adopting its chunks.
            match read_manifest_checked(&self.data_seg, mref, self.name_id, expect) {
                Ok(prior) => {
                    for c in &prior.chunks {
                        // +1 ref for the new version on each shared chunk.
                        match pool.ctrl(c).and_then(|ctrl| ctrl.borrow_shared()) {
                            Ok(()) => {
                                retained.push(*c);
                                chunks.push(*c);
                            }
                            Err(e) => {
                                self.rollback_staged(&pool, &published, &retained, None, None);
                                return Err(Error::from(e));
                            }
                        }
                    }
                    batch_spans.extend_from_slice(&prior.batch_spans);
                }
                _ => {
                    // Prior version moved or is unreadable: treat as a conflict.
                    self.rollback_staged(&pool, &published, &retained, None, None);
                    return Err(Error::Conflict {
                        expected: expect,
                        actual: head.current.load(SeqCst),
                    });
                }
            }
        }
        chunks.extend_from_slice(&published);
        batch_spans.extend_from_slice(staged_spans);

        let target = expect + 1;

        // 3. Stage the manifest chunk for the new version.
        let alloc = PoolAllocator::new(&pool, &self.data_seg);
        let manifest_desc = match stage_chunk(
            &pool,
            |a| write_manifest(a, self.name_id, target, schema_id, &chunks, &batch_spans),
            &alloc,
            owner,
        ) {
            Ok(d) => d,
            Err(e) => {
                self.rollback_staged(&pool, &published, &retained, None, None);
                return Err(e);
            }
        };
        let manifest_ref = PackedRef::from_desc(&manifest_desc);

        // 4. Claim a live-version slot (readers can find it once installed).
        let slot_idx = match head.claim_slot(target, manifest_ref.to_bits()) {
            Some(i) => i,
            None => {
                self.rollback_staged(&pool, &published, &retained, Some(&manifest_desc), None);
                return Err(Error::Unsupported("live-version table full"));
            }
        };

        // 5. Install: the single linearising CAS of `current`.
        match head.current.compare_exchange(expect, target, SeqCst, SeqCst) {
            Ok(_) => {
                // Publish the manifest pointer (readers validate manifest.version
                // so the brief two-word window is never observed torn).
                head.manifest_desc.store(manifest_ref.to_bits(), Release);
                let _ = head
                    .schema_id
                    .compare_exchange(0, schema_id, SeqCst, Acquire);

                // The predecessor is now non-current; reclaim it if unpinned.
                if expect != NO_VERSION {
                    let _ = try_retire_version(head, &self.data_seg, expect);
                }

                if let Some(sink) = &self.watch {
                    sink(VersionEvent::new(self.name_id, target, commit.kind()));
                }
                Ok(target)
            }
            Err(actual) => {
                // Lost the race: undo everything staged.
                self.rollback_staged(
                    &pool,
                    &published,
                    &retained,
                    Some(&manifest_desc),
                    Some(slot_idx),
                );
                Err(Error::Conflict { expected: expect, actual })
            }
        }
    }

    /// Undo a failed commit: free the claimed slot, release the manifest chunk,
    /// undo retained-chunk references, and release every published staged chunk
    /// (refcount to 0 → `FREE`).
    fn rollback_staged(
        &self,
        pool: &Pool<'_>,
        published: &[ChunkDesc],
        retained: &[ChunkDesc],
        manifest_desc: Option<&ChunkDesc>,
        slot_idx: Option<usize>,
    ) {
        if let Some(idx) = slot_idx {
            let slot = &self.head().pins[idx];
            slot.version.store(0, Release);
            slot.state.store(crate::head::SLOT_FREE, Release);
        }
        if let Some(m) = manifest_desc {
            release_chunk(pool, m);
        }
        for c in retained {
            // Undo the new version's reference (never frees: prior version keeps
            // its own reference).
            release_chunk(pool, c);
        }
        for c in published {
            release_chunk(pool, c);
        }
    }

    // ---- Read path ----

    /// Pin the current version, freezing it so its chunks cannot be reclaimed
    /// while the returned [`VersionPin`] (or any Arrow batch built from it) is
    /// alive.
    ///
    /// The fast path is a single `Acquire` load of `current` plus one `SeqCst`
    /// `fetch_add` on the version's pin counter — no lock, no data-chunk CAS —
    /// followed by the ADR-0003a hazard-handshake re-validation (see this
    /// module's doc). A commit racing the pin is detected and retried. Returns
    /// [`Error::VersionGone`] if nothing has been committed yet.
    ///
    /// This is the **single-process / unjournaled** pin: a leaked pin (its
    /// holder never dropping it) pins the version forever. Cross-process actors
    /// should instead take a [`pin_journaled`](Artifact::pin_journaled) pin so a
    /// `kill -9` mid-pin is crash-reclaimed (ADR-0003 item J).
    pub fn pin(&self) -> Result<VersionPin> {
        self.pin_inner(None)
    }

    /// Pin the current version **and journal the pin** so a crash mid-pin is
    /// reclaimed: the pin records an [`ArtifactPin`](shm_core::JournalRecord)
    /// entry into `journal_seg`'s [`BorrowJournal`] and releases it on drop
    /// (item J), mirroring how `shm-stream` journals staged chunks.
    ///
    /// `journal_seg` is the actor's borrow-journal segment (an owned
    /// [`Arc<Segment>`] so the pin's [`Drop`] can re-attach the journal to
    /// release its slot without borrowing). If this actor dies while the pin is
    /// live, the coordinator's lease-monitor replay decrements this artifact's
    /// per-version pin count via the *same* retire path as a clean drop.
    pub fn pin_journaled(&self, journal_seg: &Arc<Segment>) -> Result<VersionPin> {
        self.pin_inner(Some(journal_seg.clone()))
    }

    /// The shared pin path: run the hazard handshake, and (if `journal_seg` is
    /// `Some`) journal the resulting version pin for crash reclamation.
    fn pin_inner(&self, journal_seg: Option<Arc<Segment>>) -> Result<VersionPin> {
        let head = self.head();
        let mut spins: u32 = 0;
        loop {
            // (1) Acquire-load the current version.
            let v = head.current.load(Acquire);
            if v == NO_VERSION {
                return Err(Error::VersionGone);
            }
            let idx = match head.find_slot(v) {
                Some(i) => i,
                None => {
                    // `v` is being reclaimed after a newer commit; re-read.
                    backoff(&mut spins);
                    continue;
                }
            };
            let slot = &head.pins[idx];

            // (2) Publish the pin: SeqCst fetch_add. This only ever *adds* then
            // (on failure) *subtracts* the same slot, so pin counts are never
            // under-counted — a chunk is never freed under a live reader.
            slot.pins.fetch_add(1, SeqCst);

            // (3) Re-validate the slot is still {version == v, state == LIVE}
            // with SeqCst loads — the reader half of the hazard handshake. If it
            // flipped to SLOT_FREEING (a reclaimer won the election) or the
            // version moved, back off and retry. The `state` load is SeqCst and
            // ordered after the SeqCst bump: this is the Dekker pairing against
            // the reclaimer's `FREEING`-store-then-pins-scan.
            if slot.state.load(SeqCst) != SLOT_LIVE || slot.version.load(SeqCst) != v {
                undo_pin(head, &self.data_seg, idx);
                backoff(&mut spins);
                continue;
            }

            // (4) Only now Acquire-load `manifest_desc`. Confirm it names the
            // slot we pinned (rejects the transient duplicate slot two optimistic
            // committers create for the same version, and the install window
            // where `current == v` but `manifest_desc` is not yet stored), then
            // `read_manifest_checked` confirms it self-identifies as this
            // artifact's version `v` (ADR-0003a manifest self-validation).
            let head_md = head.manifest_desc.load(Acquire);
            if slot.manifest.load(Acquire) != head_md {
                undo_pin(head, &self.data_seg, idx);
                backoff(&mut spins);
                continue;
            }
            let mref = PackedRef(head_md);
            let manifest = match read_manifest_checked(&self.data_seg, mref, self.name_id, v) {
                Ok(m) => m,
                _ => {
                    undo_pin(head, &self.data_seg, idx);
                    backoff(&mut spins);
                    continue;
                }
            };

            // The pin is accepted. If journalled, record the ArtifactPin now (so
            // a crash after this point is reclaimable); on journal exhaustion,
            // undo the pin and surface the backpressure.
            let journal = match &journal_seg {
                Some(seg) => {
                    let j = BorrowJournal::attach(seg)?;
                    match j.record_artifact_pin(self.name_id, v) {
                        Ok(slot_idx) => Some(JournalPin {
                            seg: seg.clone(),
                            slot: slot_idx,
                        }),
                        Err(e) => {
                            undo_pin(head, &self.data_seg, idx);
                            return Err(e.into());
                        }
                    }
                }
                None => None,
            };

            return Ok(VersionPin {
                inner: Arc::new(PinState {
                    head_seg: self.head_seg.clone(),
                    data_seg: self.data_seg.clone(),
                    version: v,
                    slot_idx: idx,
                    manifest,
                    journal,
                }),
            });
        }
    }

    /// The live pin count on `version`'s slot, or `None` if no live slot tracks
    /// it (never committed, or already reclaimed). Observability / test helper —
    /// e.g. a coordinator proving a leaked pin was decremented after a crash.
    pub fn version_pin_count(&self, version: u64) -> Option<u32> {
        let head = self.head();
        let idx = head.find_slot(version)?;
        Some(head.pins[idx].pins.load(SeqCst))
    }

    /// **ADR-0003 item J — crash reclamation.** Decrement `version`'s pin count
    /// for a pin a dead actor leaked (its `VersionPin` never dropped), running
    /// the *exact* retire path a clean drop would: if this releases the last pin
    /// and the version is no longer current, its chunks are reclaimed.
    ///
    /// The coordinator calls this once per replayed
    /// [`ArtifactPin`](shm_core::JournalRecord::ArtifactPin) journal entry, so a
    /// leaked cross-process pin retires exactly as if the reader had dropped it.
    /// Returns `true` iff a live slot for `version` was found and decremented.
    pub fn release_leaked_pin(&self, version: u64) -> Result<bool> {
        let head = self.head();
        let idx = match head.find_slot(version) {
            Some(i) => i,
            None => return Ok(false),
        };
        let slot = &head.pins[idx];
        let prev = slot.pins.fetch_sub(1, SeqCst);
        if prev == 1 && head.current.load(SeqCst) != version {
            try_retire_version(head, &self.data_seg, version)?;
        }
        Ok(true)
    }

    /// **ADR-0003 item K — crash reclamation.** Force-release the exclusive write
    /// lease a dead writer leaked (its [`Committer`] never dropped), bumping the
    /// fence so the dead writer is fenced out: a second writer can immediately
    /// acquire, and the dead writer's late commit fails with [`Error::Fenced`].
    ///
    /// The coordinator calls this once per replayed
    /// [`WriteLease`](shm_core::JournalRecord::WriteLease) journal entry. Returns
    /// `true` iff a lease was actually held (and is now released + fenced);
    /// `false` if it had already been released cleanly.
    pub fn release_leaked_write_lease(&self) -> bool {
        self.head().force_release_write_lease()
    }

    /// The current exclusive-lease owner actor id, or [`OWNER_NONE`](crate::head::OWNER_NONE)
    /// (`0`) if the lease is free. Observability / test helper — e.g. proving a
    /// dead writer's lease was force-released.
    pub fn write_lease_owner(&self) -> u32 {
        self.head().write_lease_owner()
    }
}

impl RefUnwindSafe for Artifact {}

/// Borrow the [`ArtifactHead`] at the base of a management segment's payload.
#[inline]
fn head_ref(head_seg: &Segment) -> &ArtifactHead {
    // SAFETY: `Artifact::create` initialised an `ArtifactHead` at the payload
    // base (64-byte aligned, sufficiently large). The region stays mapped for
    // the segment's lifetime, and every field is an atomic (`Sync`), so a shared
    // reference for concurrent atomic access is sound.
    unsafe { &*head_seg.payload_ptr().cast::<ArtifactHead>() }
}

/// Stage one chunk: run `write` (which loans + writes it), then publish it, take
/// the new version's `+1` reference, and release exclusive ownership so the
/// chunk's refcount alone gates reclamation.
///
/// The chunk is FREE after `write` (allocation does not change its state), so the
/// `try_loan → publish` sequence matches `shm-arrow`'s established pattern.
fn stage_chunk<W>(
    pool: &Pool<'_>,
    write: W,
    alloc: &PoolAllocator<'_>,
    owner: u32,
) -> Result<ChunkDesc>
where
    W: FnOnce(&PoolAllocator<'_>) -> Result<ChunkDesc>,
{
    let desc = write(alloc)?;
    let ctrl = pool.ctrl(&desc)?;
    ctrl.try_loan(owner)?; // FREE -> LOANED
    ctrl.publish()?; // LOANED -> PUBLISHED (refcount 0)
    ctrl.borrow_shared()?; // refcount 0 -> 1 : this version's reference
    ctrl.owner_release(); // owner -> NONE; refcount 1 so no reclaim
    Ok(desc)
}

/// Transition one **pre-staged** chunk (already `LOANED` + written, e.g. by an
/// `shm-stream` `append_batch`) into a published, version-owned chunk: publish
/// it, take this version's `+1` reference, and release the writer's exclusive
/// ownership so the refcount alone gates reclamation.
///
/// This is the tail of [`stage_chunk`] for a chunk that was written and loaned
/// earlier rather than in the same call.
fn publish_staged(pool: &Pool<'_>, desc: &ChunkDesc) -> Result<()> {
    let ctrl = pool.ctrl(desc)?;
    ctrl.publish()?; // LOANED -> PUBLISHED
    ctrl.borrow_shared()?; // refcount 0 -> 1 : this version's reference
    ctrl.owner_release(); // owner -> NONE; refcount 1 so no reclaim
    Ok(())
}

/// Loan every chunk of a freshly written batch (`FREE -> LOANED`, owned by
/// `owner`) — the pre-staged shape `commit_staged_inner` expects. On a mid-loop
/// failure, undo: drop the loans already taken and return **every** chunk
/// (loaned or not) to the pool, so a failed inline commit leaks nothing.
fn loan_all(
    pool: &Pool<'_>,
    alloc: &PoolAllocator<'_>,
    chunks: &[ChunkDesc],
    owner: u32,
) -> Result<()> {
    for (i, desc) in chunks.iter().enumerate() {
        match pool.ctrl(desc).and_then(|c| c.try_loan(owner)) {
            Ok(()) => {}
            Err(e) => {
                for d in &chunks[..i] {
                    if let Ok(c) = pool.ctrl(d) {
                        let _ = c.drop_loan();
                    }
                }
                for d in chunks {
                    alloc.free(d);
                }
                return Err(e.into());
            }
        }
    }
    Ok(())
}

/// Free a staged chunk still in the `LOANED` state (a commit failed before it
/// was published): recycle its control word to `FREE` and return it to the pool.
fn free_loaned(pool: &Pool<'_>, desc: &ChunkDesc) {
    if let Ok(ctrl) = pool.ctrl(desc) {
        if ctrl.drop_loan().is_ok() {
            let _ = pool.free(desc);
        }
    }
}

/// Release one version's reference on a chunk; if that was the last reference the
/// chunk is recycled to `FREE` and returned to the pool's free list.
fn release_chunk(pool: &Pool<'_>, desc: &ChunkDesc) {
    if let Ok(ctrl) = pool.ctrl(desc) {
        if ctrl.state() == PUBLISHED {
            // `release_shared` decrements the refcount and reclaims iff it hit 0
            // (owner already released at stage time).
            if ctrl.release_shared() {
                let _ = pool.free(desc);
            }
        }
    }
}

/// Undo a speculative pin bump on slot `idx`, helping reclaim whatever version
/// the slot tracks if this released its last pin while it is non-current.
fn undo_pin(head: &ArtifactHead, data_seg: &Segment, idx: usize) {
    let slot = &head.pins[idx];
    let prev = slot.pins.fetch_sub(1, SeqCst);
    if prev == 1 {
        let sv = slot.version.load(Acquire);
        if sv != 0 && head.current.load(SeqCst) != sv {
            let _ = try_retire_version(head, data_seg, sv);
        }
    }
}

/// Reclaim `version` iff it is non-current and unpinned, using the ADR-0003a
/// hazard handshake: mark the slot `SLOT_FREEING` **before** scanning pins.
///
/// # The reclaimer half of the handshake
///
/// The reclaimer CAS-elects itself `SLOT_LIVE → SLOT_FREEING` and *then*
/// `SeqCst`-loads the pin count. This `FREEING`-store-before-scan, paired with a
/// reader's publish-then-revalidate ([`Artifact::pin`]), guarantees that either
/// the reclaimer observes `pins > 0` (and does not free) or the reader observes
/// `SLOT_FREEING` (and retries) — never a ghost read of a freed manifest chunk
/// (see the module doc for the `SeqCst` total-order proof).
///
/// - `pins == 0` under `SLOT_FREEING`: no reader can hold or complete a pin on
///   `version`, so we free it and store `SLOT_FREE`.
/// - `pins > 0`: a reader is (or a racing reader may be mid-protocol). We
///   **revert** `SLOT_FREEING → SLOT_LIVE` and either hand the retire to that
///   reader's drop (a genuine live pin remains) or re-loop (a racing reader has
///   since backed off, dropping pins to `0`, and its retire attempt found us
///   mid-`FREEING`, so we retry the free ourselves). Only the elected reclaimer
///   ever leaves `SLOT_FREEING`, so these stores need no CAS.
///
/// # The exact reclamation rule
///
/// A version's chunks are released only when its pin count is `0` **and** it is
/// not `current`. Each chunk carries a `refcount` equal to the number of live
/// versions referencing it (one `borrow_shared` per referencing version); a
/// shared (Append) chunk therefore survives until the **last** referencing
/// version is reclaimed. A version's manifest chunk is unique to it and is freed
/// with it. Because `current` is monotonic and never revisits `version`, once
/// `current != version` it stays so — the reclaimability precondition is stable.
fn try_retire_version(head: &ArtifactHead, data_seg: &Segment, version: u64) -> Result<()> {
    let mut spins: u32 = 0;
    loop {
        let idx = match head.find_slot(version) {
            Some(i) => i,
            None => return Ok(()), // already reclaimed, or another reclaimer owns FREEING
        };
        let slot = &head.pins[idx];

        if head.current.load(SeqCst) == version {
            return Ok(()); // still current: not reclaimable
        }
        // Elect the single reclaimer AND publish the `FREEING` hazard flag with a
        // SeqCst store, all in one CAS — this must precede the pin scan below.
        if slot
            .state
            .compare_exchange(SLOT_LIVE, SLOT_FREEING, SeqCst, Acquire)
            .is_err()
        {
            return Ok(()); // someone else is reclaiming (or slot changed)
        }

        // Scan pins AFTER publishing FREEING (the handshake ordering).
        if slot.pins.load(SeqCst) != 0 {
            // A reader is live (or a racing reader is mid-protocol). Do NOT free.
            // Revert so a later retire can proceed.
            slot.state.store(SLOT_LIVE, SeqCst);
            if slot.pins.load(SeqCst) != 0 {
                // A genuine live pin remains; its drop re-runs retire.
                return Ok(());
            }
            // Raced with a reader that backed off (decrementing to 0) while we
            // held FREEING, so its own retire attempt no-op'd on our FREEING
            // slot. Retry the free ourselves. No new pins can target `version`
            // (readers only pin `current`, which has moved on), so this loop is
            // bounded by the finite set of racing readers.
            backoff(&mut spins);
            continue;
        }

        let pool = Pool::attach(data_seg)?;

        // Release this version's reference on each data chunk (shared chunks
        // survive if another version still references them).
        let mref = PackedRef(slot.manifest.load(Acquire));
        if let Ok(manifest) = read_manifest(data_seg, mref) {
            for c in &manifest.chunks {
                release_chunk(&pool, c);
            }
        }

        // Free the version's own (unshared) manifest chunk. `len`/`generation`
        // are irrelevant to `ctrl`/`free`, which locate the chunk by `offset`.
        let manifest_chunk = ChunkDesc {
            segment_id: mref.segment_id(),
            generation: 0,
            offset: mref.offset(),
            len: 0,
            schema_id: 0,
            _pad: 0,
        };
        release_chunk(&pool, &manifest_chunk);

        // Return the slot to the free pool (FREEING -> FREE).
        slot.version.store(0, Release);
        slot.manifest.store(0, Release);
        slot.state.store(SLOT_FREE, SeqCst);
        return Ok(());
    }
}

/// Small bounded backoff for the pin/commit retry loops.
#[inline]
fn backoff(spins: &mut u32) {
    *spins = spins.wrapping_add(1);
    if spins.is_multiple_of(64) {
        std::thread::yield_now();
    } else {
        core::hint::spin_loop();
    }
}

/// A journalled write lease: the borrow journal recording this lease's
/// [`WriteLease`](shm_core::JournalRecord::WriteLease) entry, and its slot. Held
/// by a [`Committer`] opened via
/// [`open_exclusive_journaled`](Artifact::open_exclusive_journaled); its
/// [`Drop`] releases the slot so a *clean* release leaves nothing for the
/// coordinator to replay, while a crash (no `Drop`) leaves it for the
/// lease-monitor force-release (item K).
struct LeaseJournal<'a> {
    journal: &'a shm_core::BorrowJournal<'a>,
    slot: usize,
}

/// The **fenced** exclusive-write-lease handle. Holding it excludes other
/// exclusive writers (a second [`Artifact::open_exclusive`] returns
/// [`Error::WriteLocked`]); the lease — and its journal entry, if journalled — is
/// released on drop, bumping the fence.
///
/// The committer carries the **fence token** it acquired the lease under. Every
/// commit re-validates the head lease still reads `{owner, token}`; if a
/// coordinator declared this writer dead and force-released the lease (advancing
/// the fence), the commit is rejected with [`Error::Fenced`] and installs no
/// version (ADR-0003 item K).
pub struct Committer<'a> {
    artifact: &'a Artifact,
    owner: u32,
    /// The fence generation this lease was acquired under (the fencing token).
    token: u32,
    /// The crash-reclaim journal entry backing this lease, if opened journalled.
    lease: Option<LeaseJournal<'a>>,
}

impl Committer<'_> {
    /// Commit a new version under the held lease. The predecessor is whatever
    /// `current` reads now; the install CAS then advances it by one. Fails with
    /// [`Error::Fenced`] if this writer's lease was fenced (declared dead and
    /// superseded) — installing no version.
    pub fn commit(
        &mut self,
        commit: Commit,
        batch: &RecordBatch,
        registry: &SchemaRegistry,
    ) -> Result<u64> {
        let expect = self.artifact.head().current.load(SeqCst);
        self.artifact
            .commit_inner(expect, self.owner, commit, batch, registry, Some(self.token))
    }

    /// **ADDITIVE (v0.2 stage C — for `shm-stream`).** Install a batch of
    /// **pre-staged** chunks (each already written + loaned via
    /// [`shm_arrow::write_batch`] + [`ChunkCtrl::try_loan`](shm_core::ChunkCtrl::try_loan))
    /// as the next version under the held exclusive lease.
    ///
    /// This is the pre-staged, multi-chunk analogue of
    /// [`commit`](Committer::commit): where that writes one batch inline, this
    /// installs the `N` chunks a stream accumulated invisibly under the lease.
    /// The predecessor is whatever `current` reads now; the install CAS then
    /// advances it by one. Shares the identical RCU install path
    /// ([`commit_staged_inner`](Artifact::commit_staged_inner)); see that method
    /// for the success/failure ownership contract `shm-stream` relies on.
    ///
    /// `schema_id` is the interned id shared by all staged chunks. `batch_spans`
    /// partitions `staged` into Arrow batches (item F); their sum must equal
    /// `staged.len()`.
    pub fn commit_staged(
        &mut self,
        commit: Commit,
        staged: &[ChunkDesc],
        batch_spans: &[u32],
        schema_id: u32,
    ) -> Result<u64> {
        let expect = self.artifact.head().current.load(SeqCst);
        self.artifact.commit_staged_inner(
            expect,
            self.owner,
            commit,
            staged,
            batch_spans,
            schema_id,
            Some(self.token),
        )
    }

    /// The actor id holding the lease.
    #[inline]
    pub fn owner(&self) -> u32 {
        self.owner
    }

    /// The fence generation (token) this lease was acquired under.
    #[inline]
    pub fn fence_token(&self) -> u32 {
        self.token
    }
}

impl Drop for Committer<'_> {
    fn drop(&mut self) {
        // Release the fenced lease FIRST (this clears the wedge and bumps the
        // fence), THEN the crash-ledger journal entry. This order is deliberate:
        // if we crashed between the two, the coordinator would replay the still-
        // present WriteLease entry and force-release a lease that is already free
        // — a harmless no-op — rather than the reverse order, which could clear
        // the ledger while the lease stayed stuck (the exact wedge item K fixes).
        // A `false` result (the lease was already fenced by the coordinator) is
        // fine: the CAS is a no-op and the journal entry, if any, is still cleared.
        let _ = self
            .artifact
            .head()
            .release_write_lease(self.owner, self.token);
        if let Some(l) = &self.lease {
            let _ = l.journal.release(l.slot);
        }
    }
}

/// A reader's pin on one frozen version (an RCU read-side critical section).
///
/// While a `VersionPin` — or any Arrow [`RecordBatch`] reconstructed from it via
/// [`as_arrow`](VersionPin::as_arrow) — is alive, the pinned version's chunks are
/// held against reclamation. Dropping the last such reference releases the pin.
#[derive(Clone)]
pub struct VersionPin {
    inner: Arc<PinState>,
}

/// The shared inner state of a [`VersionPin`]; also the Arrow buffer keep-alive.
///
/// A clone of `Arc<PinState>` is moved into every zero-copy Arrow buffer built by
/// [`read_batch`], so the mapping (and the pinned version) outlives every buffer.
/// When the last clone drops, [`Drop`] releases the version pin.
struct PinState {
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
    version: u64,
    slot_idx: usize,
    manifest: Manifest,
    /// The crash-reclaim journal entry backing this pin, if it was taken via
    /// [`Artifact::pin_journaled`]. `Drop` releases the slot so a *clean* drop
    /// leaves nothing for the coordinator to replay; a crash (no `Drop`) leaves
    /// it for the lease-monitor replay (item J).
    journal: Option<JournalPin>,
}

/// A journalled version pin: the actor's borrow-journal segment plus the slot
/// index recording this pin's [`ArtifactPin`](shm_core::JournalRecord) entry.
struct JournalPin {
    seg: Arc<Segment>,
    slot: usize,
}

impl SegmentBase for PinState {
    fn base_ptr(&self) -> *const u8 {
        self.data_seg.base_ptr()
    }
}

impl Drop for PinState {
    fn drop(&mut self) {
        // Release the crash-reclaim journal entry first (best-effort): a clean
        // drop must leave nothing for the coordinator to replay.
        if let Some(jp) = &self.journal {
            if let Ok(journal) = BorrowJournal::attach(&jp.seg) {
                let _ = journal.release(jp.slot);
            }
        }
        let head = head_ref(&self.head_seg);
        let slot = &head.pins[self.slot_idx];
        // We hold a live pin, so the slot still tracks our version and cannot be
        // reclaimed/reused underneath us.
        let prev = slot.pins.fetch_sub(1, SeqCst);
        if prev == 1 {
            // Last pin gone: reclaim if this version is no longer current.
            if head.current.load(SeqCst) != self.version {
                let _ = try_retire_version(head, &self.data_seg, self.version);
            }
        }
    }
}

impl VersionPin {
    /// The pinned version number.
    #[inline]
    pub fn version(&self) -> u64 {
        self.inner.version
    }

    /// The parsed [`Manifest`] of the pinned version.
    #[inline]
    pub fn manifest(&self) -> &Manifest {
        &self.inner.manifest
    }

    /// Reconstruct the pinned version's data as an Arrow [`RecordBatch`],
    /// zero-copy: each column's buffers point directly into the mapped segment,
    /// and a clone of this pin is the buffers' keep-alive (its lifetime holds the
    /// mapping *and* the version alive).
    ///
    /// Each Arrow batch in the version is reconstructed from its group of chunks
    /// (a multi-chunk batch spans several consecutive `manifest.chunks`, per the
    /// [`Manifest::batch_spans`] boundary table); the batches are then
    /// concatenated in row order into one logical [`RecordBatch`]. A single
    /// batch (the common case) is returned directly. Turnover stays O(new data)
    /// while reads still yield one logical batch.
    pub fn as_arrow(&self, registry: &SchemaRegistry) -> Result<RecordBatch> {
        let pool = Pool::attach(&self.inner.data_seg)?;
        let manifest = &self.inner.manifest;
        let mut batches: Vec<RecordBatch> = Vec::with_capacity(manifest.batch_spans.len());
        let mut idx = 0usize;
        for &span in &manifest.batch_spans {
            let span = span as usize;
            let group = manifest
                .chunks
                .get(idx..idx + span)
                .ok_or(Error::Core(shm_core::Error::OutOfBounds))?;
            idx += span;
            let ctrls: Vec<&ChunkCtrl> = group
                .iter()
                .map(|d| pool.ctrl(d))
                .collect::<core::result::Result<Vec<_>, shm_core::Error>>()?;
            let batch = read_batch_chunks(self.inner.clone(), group, &ctrls, registry)?;
            batches.push(batch);
        }
        match batches.len() {
            0 => Err(Error::VersionGone),
            1 => Ok(batches.pop().expect("len checked")),
            _ => {
                let schema = batches[0].schema();
                Ok(concat_batches(&schema, &batches).map_err(shm_arrow::Error::from)?)
            }
        }
    }
}
