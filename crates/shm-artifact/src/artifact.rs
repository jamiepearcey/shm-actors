//! The [`Artifact`] handle and its RCU/MVCC read/write/reclaim machinery.

use std::panic::RefUnwindSafe;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use shm_arrow::{read_batch, write_batch, PoolAllocator, SchemaRegistry, SegmentBase};
use shm_core::{ChunkDesc, PackedRef, Pool, PoolConfig, Segment, PUBLISHED};

use crate::error::{Error, Result};
use crate::event::{CommitKind, VersionEvent};
use crate::head::{ArtifactHead, NO_VERSION, NO_WRITER, SLOT_LIVE, SLOT_RETIRED};
use crate::manifest::{read_manifest, write_manifest, Manifest};

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

    /// Acquire the exclusive write lease, returning a [`Committer`]. A second
    /// caller fails fast with [`Error::WriteLocked`] until the returned committer
    /// is dropped. `owner` must be a non-zero actor id.
    ///
    /// v0.1 models the lease as an atomic owner field in the [`ArtifactHead`]; a
    /// real coordinator-backed lease (with crash reclamation) arrives in
    /// `shm-runtime`.
    pub fn open_exclusive(&self, owner: u32) -> Result<Committer<'_>> {
        debug_assert_ne!(owner, NO_WRITER, "writer owner id must be non-zero");
        let head = self.head();
        match head
            .writer_owner
            .compare_exchange(NO_WRITER, owner, SeqCst, Acquire)
        {
            Ok(_) => Ok(Committer {
                artifact: self,
                owner,
            }),
            Err(_) => Err(Error::WriteLocked),
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
        self.commit_inner(expect, owner, commit, batch, registry)
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
    /// be non-zero.
    pub fn commit_staged_optimistic(
        &self,
        owner: u32,
        expect: u64,
        commit: Commit,
        staged: &[ChunkDesc],
        schema_id: u32,
    ) -> Result<u64> {
        self.commit_staged_inner(expect, owner, commit, staged, schema_id)
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
    ) -> Result<u64> {
        debug_assert_ne!(owner, NO_WRITER, "commit owner id must be non-zero");
        if let Commit::Patch(_) = commit {
            return Err(Error::Unsupported("Patch commit deferred to v0.2"));
        }

        let pool = Pool::attach(&self.data_seg)?;
        let alloc = PoolAllocator::new(&pool, &self.data_seg);
        let schema_id = registry.intern(&batch.schema());

        // Write the one data chunk and loan it (FREE -> LOANED): exactly the
        // pre-staged shape `commit_staged_inner` installs. Any staging failure
        // below returns the chunk to the pool via that path's rollback.
        let desc = write_batch(&alloc, registry, batch)?;
        pool.ctrl(&desc)?.try_loan(owner)?;
        self.commit_staged_inner(expect, owner, commit, &[desc], schema_id)
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
    ///   caller need only release its own borrow-journal slots. (The `Patch`
    ///   rejection is the sole early return that does *not* consume `staged`;
    ///   callers pre-validate the commit kind, so a real staged commit never hits
    ///   it — `shm-stream` rejects `Patch` at `open`.)
    fn commit_staged_inner(
        &self,
        expect: u64,
        owner: u32,
        commit: Commit,
        staged: &[ChunkDesc],
        schema_id: u32,
    ) -> Result<u64> {
        debug_assert_ne!(owner, NO_WRITER, "commit owner id must be non-zero");
        if let Commit::Patch(_) = commit {
            return Err(Error::Unsupported("Patch commit deferred to v0.2"));
        }

        let head = self.head();
        let pool = Pool::attach(&self.data_seg)?;

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

        // 2. Assemble the new version's manifest chunk list, referencing any
        //    retained (Append) chunks into the new version up front.
        let mut chunks: Vec<ChunkDesc> = Vec::new();
        let mut retained: Vec<ChunkDesc> = Vec::new();
        let is_append = matches!(commit, Commit::Append) && expect != NO_VERSION;
        if is_append {
            let mref = PackedRef(head.manifest_desc.load(Acquire));
            match read_manifest(&self.data_seg, mref) {
                Ok(prior) if prior.version == expect => {
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

        let target = expect + 1;

        // 3. Stage the manifest chunk for the new version.
        let alloc = PoolAllocator::new(&pool, &self.data_seg);
        let manifest_desc = match stage_chunk(
            &pool,
            |a| write_manifest(a, target, schema_id, &chunks),
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
    /// The fast path is a single `SeqCst` load of `current` plus one `SeqCst`
    /// `fetch_add` on the version's pin counter — no lock, no data-chunk CAS.
    /// A commit racing the pin is detected by re-reading `current`; the pin then
    /// retries against the new current version. Returns [`Error::VersionGone`] if
    /// nothing has been committed yet.
    pub fn pin(&self) -> Result<VersionPin> {
        let head = self.head();
        let mut spins: u32 = 0;
        loop {
            let v = head.current.load(SeqCst);
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

            // Bump first, then validate — the SeqCst bump ordered against the
            // committer's SeqCst `current` store is the Dekker guarantee that a
            // live pin is never missed by reclamation. This only ever *adds*
            // then (on failure) *subtracts* the same slot, so pin counts are
            // never under-counted — a chunk is never freed under a live reader.
            slot.pins.fetch_add(1, SeqCst);
            if head.current.load(SeqCst) != v || slot.version.load(Acquire) != v {
                undo_pin(head, &self.data_seg, idx);
                backoff(&mut spins);
                continue;
            }

            // Read the manifest and confirm it is `v`'s (guards the install race
            // where `current == v` but `manifest_desc` is not yet stored).
            let mref = PackedRef(head.manifest_desc.load(Acquire));
            let manifest = match read_manifest(&self.data_seg, mref) {
                Ok(m) if m.version == v => m,
                _ => {
                    undo_pin(head, &self.data_seg, idx);
                    backoff(&mut spins);
                    continue;
                }
            };

            return Ok(VersionPin {
                inner: Arc::new(PinState {
                    head_seg: self.head_seg.clone(),
                    data_seg: self.data_seg.clone(),
                    version: v,
                    slot_idx: idx,
                    manifest,
                }),
            });
        }
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

/// Reclaim `version` iff it is non-current and unpinned: elect a single reclaimer
/// via a `SLOT_LIVE → SLOT_RETIRED` CAS, then release every reference the version
/// held (its data chunks and its manifest chunk) and free the slot.
///
/// # The exact reclamation rule
///
/// A version's chunks are released only when its pin count is `0` **and** it is
/// not `current`. Each chunk carries a `refcount` equal to the number of live
/// versions referencing it (one `borrow_shared` per referencing version); a
/// shared (Append) chunk therefore survives until the **last** referencing
/// version is reclaimed. A version's manifest chunk is unique to it and is freed
/// with it. Because a non-current version's pin count can only *decrease* (new
/// pins always target `current`), the `pins == 0` check is stable once observed.
fn try_retire_version(head: &ArtifactHead, data_seg: &Segment, version: u64) -> Result<()> {
    let idx = match head.find_slot(version) {
        Some(i) => i,
        None => return Ok(()), // already reclaimed
    };
    let slot = &head.pins[idx];

    if head.current.load(SeqCst) == version {
        return Ok(()); // still current: not reclaimable
    }
    if slot.pins.load(SeqCst) != 0 {
        return Ok(()); // still pinned
    }
    // Elect the single reclaimer.
    if slot
        .state
        .compare_exchange(SLOT_LIVE, SLOT_RETIRED, SeqCst, Acquire)
        .is_err()
    {
        return Ok(()); // someone else is reclaiming
    }

    let pool = Pool::attach(data_seg)?;

    // Release this version's reference on each data chunk (shared chunks survive
    // if another version still references them).
    let mref = PackedRef(slot.manifest.load(Acquire));
    if let Ok(manifest) = read_manifest(data_seg, mref) {
        for c in &manifest.chunks {
            release_chunk(&pool, c);
        }
    }

    // Free the version's own (unshared) manifest chunk. `len`/`generation` are
    // irrelevant to `ctrl`/`free`, which locate the chunk by `offset` alone.
    let manifest_chunk = ChunkDesc {
        segment_id: mref.segment_id(),
        generation: 0,
        offset: mref.offset(),
        len: 0,
        schema_id: 0,
        _pad: 0,
    };
    release_chunk(&pool, &manifest_chunk);

    // Return the slot to the free pool.
    slot.version.store(0, Release);
    slot.manifest.store(0, Release);
    slot.state.store(crate::head::SLOT_FREE, Release);
    Ok(())
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

/// The exclusive-write-lease handle. Holding it excludes other exclusive writers
/// (a second [`Artifact::open_exclusive`] returns [`Error::WriteLocked`]); the
/// lease is released on drop.
pub struct Committer<'a> {
    artifact: &'a Artifact,
    owner: u32,
}

impl Committer<'_> {
    /// Commit a new version under the held lease. The predecessor is whatever
    /// `current` reads now; the install CAS then advances it by one.
    pub fn commit(
        &mut self,
        commit: Commit,
        batch: &RecordBatch,
        registry: &SchemaRegistry,
    ) -> Result<u64> {
        let expect = self.artifact.head().current.load(SeqCst);
        self.artifact
            .commit_inner(expect, self.owner, commit, batch, registry)
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
    /// `schema_id` is the interned id shared by all staged chunks.
    pub fn commit_staged(
        &mut self,
        commit: Commit,
        staged: &[ChunkDesc],
        schema_id: u32,
    ) -> Result<u64> {
        let expect = self.artifact.head().current.load(SeqCst);
        self.artifact
            .commit_staged_inner(expect, self.owner, commit, staged, schema_id)
    }

    /// The actor id holding the lease.
    #[inline]
    pub fn owner(&self) -> u32 {
        self.owner
    }
}

impl Drop for Committer<'_> {
    fn drop(&mut self) {
        let _ = self
            .artifact
            .head()
            .writer_owner
            .compare_exchange(self.owner, NO_WRITER, SeqCst, Acquire);
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
}

impl SegmentBase for PinState {
    fn base_ptr(&self) -> *const u8 {
        self.data_seg.base_ptr()
    }
}

impl Drop for PinState {
    fn drop(&mut self) {
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
    /// v0.1 single-data-chunk versions return that chunk's batch directly. An
    /// Append version with multiple data chunks is reconstructed by reading each
    /// chunk's batch and concatenating them in row order — turnover stays
    /// O(new data) while reads still yield one logical batch.
    pub fn as_arrow(&self, registry: &SchemaRegistry) -> Result<RecordBatch> {
        let pool = Pool::attach(&self.inner.data_seg)?;
        let mut batches: Vec<RecordBatch> = Vec::with_capacity(self.inner.manifest.chunks.len());
        for desc in &self.inner.manifest.chunks {
            let ctrl = pool.ctrl(desc)?;
            let batch = read_batch(self.inner.clone(), ctrl, desc, registry)?;
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
