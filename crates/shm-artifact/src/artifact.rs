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
use shm_core::{
    BorrowJournal, ChunkCtrl, ChunkDesc, PackedRef, Pool, PoolConfig, Segment, PUBLISHED,
};

use crate::error::{Error, Result};
use crate::event::{CommitKind, VersionEvent};
use crate::head::{
    lease_fence, pack_lease, ArtifactHead, FIRST_INCARNATION, NO_INCARNATION, NO_VERSION,
    OWNER_NONE, SLOT_LIVE,
};

/// The owner id an `evict_all` teardown registers under. Reserved: never a
/// real actor id (actor ids are coordinator-issued small integers).
pub const EVICTOR_OWNER: u32 = u32::MAX;
use crate::manifest::{
    read_manifest, read_manifest_checked, walk_chain_with, write_manifest, Manifest,
    ManifestLink,
};

/// How a commit relates the new version to its predecessor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Commit {
    /// The new version supersedes its predecessor wholesale: its manifest is a
    /// chain **root** listing only the newly staged chunk(s). The prior
    /// version's whole chain is released (cascade) once it is reclaimed.
    Replace,
    /// The new version extends its predecessor: its manifest lists only the
    /// newly staged chunk(s) and carries a reference-counted **link** to the
    /// prior version's manifest (ADR-0013), so the table is the chain of
    /// manifests and nothing prior is copied or re-listed. Commit and pin cost
    /// O(new data) regardless of the lineage's length; a read is O(batches).
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
    /// Which occupant of the head region this handle believes it is talking to
    /// (ADR-0008 P0.1). Adopted at attach, stamped at create; re-validated by
    /// [`check_live`](Artifact::check_live) on every operation so a handle held
    /// across a keyed-store slot reclaim fails [`Error::Stale`] instead of
    /// silently operating on the region's next occupant.
    incarnation: u32,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
    /// Byte offset of this artifact's [`ArtifactHead`] within `head_seg`'s
    /// payload. `0` for the standard one-head-per-segment layout
    /// ([`create`](Self::create)/[`attach`](Self::attach)); non-zero only for the
    /// keyed-store layout ([`create_at`](Self::create_at)/[`attach_at`](Self::attach_at),
    /// ADR-0007 G3) that packs many heads into one shared management segment.
    head_off: usize,
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
        // Commission last: the release store publishes the initialised head.
        head_ref(&head_seg).commission(FIRST_INCARNATION);

        Ok(Artifact {
            name_id,
            incarnation: FIRST_INCARNATION,
            head_seg,
            data_seg,
            head_off: 0,
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
        let head = head_ref(&head_seg);
        if !head.check_magic() {
            return Err(Error::BadMagic);
        }
        // Adopt whatever occupant is in service now; every later operation
        // re-validates against it.
        let incarnation = head.incarnation();
        if incarnation == NO_INCARNATION {
            return Err(Error::Stale);
        }
        Ok(Artifact {
            name_id,
            incarnation,
            head_seg,
            data_seg,
            head_off: 0,
            watch: None,
        })
    }

    /// **ADDITIVE (v0.5 / ADR-0007 G3 — `shm-store`).** Create an artifact whose
    /// [`ArtifactHead`] is placed at byte `head_off` within `head_seg`'s payload,
    /// over a [`Pool`] that has **already** been laid into `data_seg` by the
    /// caller (this does **not** create the pool).
    ///
    /// This is the offset-and-shared-pool counterpart of [`create`](Self::create)
    /// that a keyed store (`shm-store`) uses to pack **many** artifact heads into
    /// one shared management segment while all of them share one data pool: the
    /// store creates the pool once, then calls this per entry. The RCU/MVCC
    /// read/write/reclaim machinery is otherwise identical — only where the head
    /// lives differs. `head_off` must be 8-byte aligned.
    pub fn create_at(
        name_id: u32,
        incarnation: u32,
        head_seg: Arc<Segment>,
        head_off: usize,
        data_seg: Arc<Segment>,
    ) -> Result<Artifact> {
        debug_assert_ne!(
            incarnation, NO_INCARNATION,
            "an occupant's incarnation must be non-zero"
        );
        debug_assert!(
            head_off.is_multiple_of(8),
            "head_off must be 8-byte aligned"
        );
        let need = head_off
            .checked_add(ArtifactHead::region_bytes())
            .ok_or(Error::Core(shm_core::Error::LayoutOverflow(
                "head offset overflow",
            )))?;
        if head_seg.payload_len() < need {
            return Err(Error::Core(shm_core::Error::LayoutOverflow(
                "head segment too small for ArtifactHead at offset",
            )));
        }
        // SAFETY: `payload_ptr()` is 64-byte aligned, `head_off` is 8-aligned (so
        // the sum meets `ArtifactHead`'s 8-byte alignment), and the check above
        // guarantees the region is large enough. Creation is single-threaded by
        // contract (the store's per-entry slot is freshly claimed).
        let ptr = unsafe { head_seg.payload_ptr().add(head_off).cast::<ArtifactHead>() };
        // Carry the write-lease fence across incarnations. A fresh head starts
        // at fence 0, so the first lease on a recycled slot would be
        // `{owner, 0}` — and a `Committer` from the slot's *previous* occupant
        // that was force-released at `{owner, 0}` would, on drop, CAS the new
        // occupant's identical lease word and revoke an innocent writer. With
        // the fence monotonic per region, a stale token can never match.
        let prior = head_ref_at(&head_seg, head_off);
        let carry = if prior.check_magic() {
            lease_fence(prior.write_lease.load(Acquire))
        } else {
            0
        };
        // SAFETY: as above; the region is exclusively owned for this init.
        unsafe { ArtifactHead::init_at(ptr) };
        let fresh = head_ref_at(&head_seg, head_off);
        fresh.write_lease.store(pack_lease(OWNER_NONE, carry), Release);
        // Commission last: the release store publishes the initialised head to
        // any handle that resolves this slot from now on.
        fresh.commission(incarnation);
        Ok(Artifact {
            name_id,
            incarnation,
            head_seg,
            data_seg,
            head_off,
            watch: None,
        })
    }

    /// **ADDITIVE (v0.5 / ADR-0007 G3 — `shm-store`).** Attach to an artifact
    /// whose [`ArtifactHead`] lives at byte `head_off` within `head_seg`, over a
    /// shared pool already present in `data_seg`. The offset counterpart of
    /// [`attach`](Self::attach); validates the pool and head magics.
    pub fn attach_at(
        name_id: u32,
        head_seg: Arc<Segment>,
        head_off: usize,
        data_seg: Arc<Segment>,
    ) -> Result<Artifact> {
        // Validates `POOL_MAGIC` on the shared store pool.
        Pool::attach(&data_seg)?;
        let need = head_off
            .checked_add(ArtifactHead::region_bytes())
            .ok_or(Error::BadMagic)?;
        if head_seg.payload_len() < need {
            return Err(Error::BadMagic);
        }
        let head = head_ref_at(&head_seg, head_off);
        if !head.check_magic() {
            return Err(Error::BadMagic);
        }
        let incarnation = head.incarnation();
        if incarnation == NO_INCARNATION {
            // The slot is retired (or mid-reclaim): there is nothing live here
            // to attach to. A caller that resolved by key should re-resolve.
            return Err(Error::Stale);
        }
        Ok(Artifact {
            name_id,
            incarnation,
            head_seg,
            data_seg,
            head_off,
            watch: None,
        })
    }

    /// Attach **only if** `expected` is still the occupant in service.
    ///
    /// The validating counterpart of [`attach_at`](Self::attach_at), for a
    /// caller that already knows which occupant it means — chiefly the
    /// coordinator routing a dead actor's journaled pin or write lease back to
    /// the head it was taken against (ADR-0008 P0.1). Without this, a journal
    /// record outliving a slot reclaim would release a pin belonging to the
    /// slot's *next* occupant.
    pub fn attach_at_incarnation(
        name_id: u32,
        expected: u32,
        head_seg: Arc<Segment>,
        head_off: usize,
        data_seg: Arc<Segment>,
    ) -> Result<Artifact> {
        let artifact = Artifact::attach_at(name_id, head_seg, head_off, data_seg)?;
        if artifact.incarnation != expected {
            return Err(Error::Stale);
        }
        Ok(artifact)
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

    /// Borrow the on-shm [`ArtifactHead`] (at this artifact's `head_off`).
    #[inline]
    fn head(&self) -> &ArtifactHead {
        head_ref_at(&self.head_seg, self.head_off)
    }

    /// The `PackedRef` bits of the current version's manifest chunk (`0` if
    /// nothing is committed) — the value an installed version's slot
    /// endorses; see [`reclaim_staged_manifest`](Self::reclaim_staged_manifest).
    #[inline]
    pub fn current_manifest_bits(&self) -> u64 {
        self.head().manifest_desc.load(Acquire)
    }

    /// The occupant of the head region this handle is bound to (ADR-0008 P0.1).
    #[inline]
    pub fn incarnation(&self) -> u32 {
        self.incarnation
    }

    /// Fail [`Error::Stale`] if this handle's occupant is no longer the one in
    /// service — i.e. the keyed store reclaimed the slot under it.
    ///
    /// This is the **unfenced early-out** form, for the head of an operation
    /// (or `evict_all`, whose caller holds the tombstone). It is *not* the
    /// recycle-handshake revalidation: every operation that registers
    /// (publishes a pin, claims a version slot, takes the write lease)
    /// re-validates **after** registering, through a path that carries a
    /// `SeqCst` fence — [`PinSlot::accept_pin`]'s on the pin path,
    /// [`ArtifactHead::revalidate_incarnation`] elsewhere. A check before
    /// registering alone would be a plain race.
    #[inline]
    fn check_live(&self) -> Result<()> {
        if self.head().is_incarnation(self.incarnation) {
            Ok(())
        } else {
            Err(Error::Stale)
        }
    }

    /// **Sweep support (ADR-0008 P0.1).** Does this entry still hold anything?
    ///
    /// `true` iff there is no current version, no live version slot, no pin, and
    /// no write lease — i.e. the head region holds nothing a reclaimer would
    /// destroy. Delegates to [`ArtifactHead::is_quiescent`], the fenced,
    /// loom-modeled sweep half of the recycle handshake; it must be called
    /// after [`retire_head`](Self::retire_head), and its docs carry the
    /// ordering argument.
    pub fn is_quiescent(&self) -> bool {
        self.head().is_quiescent()
    }

    /// **Sweep support (ADR-0008 P0.1).** Take the head region out of service,
    /// returning the incarnation that was in it so an aborted sweep can put it
    /// back with [`commission_head`](Self::commission_head).
    ///
    /// Must be called **before** [`is_quiescent`](Self::is_quiescent): retiring
    /// first is what stops a new registration from slipping in behind the scan.
    pub fn retire_head(&self) -> u32 {
        self.head().retire()
    }

    /// **Sweep support (ADR-0008 P0.1).** Put an incarnation back into service
    /// after a sweep found the entry still busy and aborted.
    pub fn commission_head(&self, incarnation: u32) {
        self.head().commission(incarnation);
    }

    /// **ADDITIVE (v0.5 / ADR-0007 G3 — `shm-store` eviction).** Tear down every
    /// version so a keyed store can reclaim all of an evicted entry's chunks by
    /// refcount, reusing the *unchanged* RCU retire path.
    ///
    /// It stores [`NO_VERSION`] into `current` (and clears `manifest_desc`), which
    /// makes **every** live version non-current, then drives
    /// [`try_retire_version`] on each live slot:
    ///
    /// - an **unpinned** version is reclaimed immediately here (its one
    ///   reference — its manifest chunk — released; if that was the manifest's
    ///   last reference the ADR-0013 cascade frees its data chunks and follows
    ///   its `Append` link, stopping at the first manifest a pinned older
    ///   version still holds);
    /// - a **still-pinned** version (a live reader, or a crash-leaked pin the
    ///   coordinator has not yet released) is reclaimed by the *standard*
    ///   non-current retire when its last pin drops — because `current` is now
    ///   [`NO_VERSION`], [`VersionPin`]'s `Drop` (and
    ///   [`release_leaked_pin`](Self::release_leaked_pin)) see it as non-current
    ///   and retire it.
    ///
    /// No RCU rule is reinvented; this only flips `current` and nudges the retirer.
    /// Idempotent: a second call finds no live versions and is a no-op. The caller
    /// must have first quiesced writers to the entry (the store tombstones the
    /// catalog slot before calling this, so no new commit or pin can target it).
    ///
    /// **P0.3 (ADR-0010, G12a) — the write lease dies with the entry.** The
    /// teardown also force-releases the fenced exclusive write lease (with a
    /// fence bump, the exact item-K crash-release CAS). Without this, a
    /// tombstoned entry whose lease is held by a live-but-idle committer never
    /// goes quiescent — [`ArtifactHead::is_quiescent`] requires the lease
    /// unowned, and nothing else releases a *live* actor's lease — so the slot
    /// would leak until that actor died. The fence bump makes the idle holder's
    /// token stale: any later commit that **observes** the bump fails the
    /// `lease_held_by` step-0 check ([`Error::Fenced`]) before staging
    /// anything, which also turns the straggler-resurrect race (eviction is a
    /// level, not an edge) into a clean rejection for leased commits. The
    /// step-0 gate is an `Acquire` load and may in principle read a stale
    /// lease; the load-bearing guarantee against a maximally-stale zombie is
    /// the commit's **registered** step-4b revalidation (`Error::Stale` on a
    /// retired head) and the install CAS — no interleaving installs onto a
    /// reclaimed head. This force-release runs in the teardown phase, before
    /// any sweep `retire`, and is a de-registration — it adds no registration
    /// point to the ADR-0008 recycle handshake (modeled in
    /// `tests/loom_reclaim.rs::loom_reclaim_vs_fenced_lease`).
    pub fn evict_all(&self) -> Result<()> {
        self.check_live()?;
        let head = self.head();
        // **Register before tearing down.** The stores below are not visible
        // to a sweep's quiescence scan, so an unregistered teardown could be
        // preempted, have its slot reclaimed and re-created underneath it, and
        // then land `current = NO_VERSION` + a lease revocation + a version
        // retire on the *next occupant* — silent destruction of a different
        // entry. Holding the write lease is exactly what the scan refuses to
        // reclaim under, and `revalidate_incarnation` after taking it is the
        // same register-then-check shape every other operation uses
        // (ADR-0008). A live-but-idle holder is force-released first: that is
        // the P0.3 thesis (the lease dies with the entry), and the loop covers
        // a rival acquirer slipping in between.
        let token = loop {
            head.force_release_write_lease();
            if let Some(t) = head.acquire_write_lease(EVICTOR_OWNER) {
                break t;
            }
        };
        if !head.revalidate_incarnation(self.incarnation) {
            head.release_write_lease(EVICTOR_OWNER, token);
            return Err(Error::Stale);
        }
        head.current.store(NO_VERSION, SeqCst);
        head.manifest_desc.store(0, Release);
        let mut result = Ok(());
        for slot in head.pins.iter() {
            if slot.state.load(Acquire) == SLOT_LIVE {
                let v = slot.version.load(Acquire);
                if v != NO_VERSION {
                    if let Err(e) = try_retire_version(head, &self.data_seg, v) {
                        result = Err(e);
                        break;
                    }
                }
            }
        }
        // Release last (bumps the fence): the sweep's scan may now find the
        // lease free and the entry quiescent.
        head.release_write_lease(EVICTOR_OWNER, token);
        result
    }

    /// **P0.3 (ADR-0010, G4) — evict the *current* version specifically,
    /// without tearing down the lineage.** Optimistically commit an **empty**
    /// `Replace` version (zero chunks, zero batches) expecting `current ==
    /// expect`; the install then retires `expect` through the standard
    /// ADR-0003a handshake (a still-pinned reader drains via pin drop, exactly
    /// as with any superseded version). Returns the new (empty) version number.
    ///
    /// This is deliberately **not** a `current → NO_VERSION` CAS: version
    /// numbers are monotonic and never reissued (the invariant `find_slot` /
    /// `accept_pin` disambiguation rests on), and clearing `current` would make
    /// the next commit reissue `expect + 1` while the old same-numbered slot
    /// may still be `SLOT_LIVE` under a reader's pin. Committing an empty
    /// version through the unchanged [`commit_staged_inner`] preserves
    /// monotonicity, reuses the already-loom-modeled commit registration
    /// (ADR-0008), and adds **zero** new branches to the pin hot path — a
    /// reader of the evicted-current entry sees [`Error::VersionGone`] from
    /// `as_arrow`'s existing zero-batch arm, identical to a never-committed
    /// entry. The entry stays live: the next commit continues the version
    /// sequence at `target + 1`. Cost: one 32-byte manifest chunk, freed when
    /// the empty version is itself superseded or evicted-all.
    ///
    /// Fails [`Error::VersionGone`] when nothing is committed (`expect ==
    /// NO_VERSION` — there is no current version to evict) and
    /// [`Error::Conflict`] if another committer moved `current` first.
    pub fn evict_current_optimistic(&self, owner: u32, expect: u64) -> Result<u64> {
        if expect == NO_VERSION {
            return Err(Error::VersionGone);
        }
        let schema_id = self.head().schema_id.load(Acquire);
        self.commit_staged_inner(expect, owner, Commit::Replace, &[], &[], schema_id, None)
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
        let head = self.head();
        let token = head.acquire_write_lease(owner).ok_or(Error::WriteLocked)?;
        // Register-then-validate: holding the lease is what a reclaim sweep's
        // quiescence check sees, so taking it *before* the incarnation check —
        // through the fenced revalidate — is what makes the two mutually
        // exclusive (ADR-0008 P0.1; loom-modeled in `tests/loom_reclaim.rs`).
        if !head.revalidate_incarnation(self.incarnation) {
            head.release_write_lease(owner, token);
            return Err(Error::Stale);
        }
        Ok(Committer {
            artifact: self,
            owner,
            token,
            lease: None,
        })
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
        // Register-then-validate (fenced), as in `open_exclusive`.
        if !head.revalidate_incarnation(self.incarnation) {
            head.release_write_lease(owner, token);
            return Err(Error::Stale);
        }
        match journal.record_write_lease(self.name_id, self.incarnation, token) {
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
        self.commit_inner_j(expect, owner, commit, batch, registry, lease_fence, None)
    }

    /// [`commit_inner`](Self::commit_inner) with the caller's borrow journal,
    /// so the staged manifest is crash-reclaimable (ADR-0014 §3).
    #[allow(clippy::too_many_arguments)]
    fn commit_inner_j(
        &self,
        expect: u64,
        owner: u32,
        commit: Commit,
        batch: &RecordBatch,
        registry: &SchemaRegistry,
        lease_fence: Option<u32>,
        journal: Option<&BorrowJournal<'_>>,
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
        self.commit_staged_inner_j(
            expect,
            owner,
            commit,
            &staged,
            &spans,
            schema_id,
            lease_fence,
            journal,
        )
    }

    /// **The one true RCU install path**, shared by single-batch commits and by
    /// `shm-stream`'s pre-staged multi-chunk commits.
    ///
    /// `staged` names chunks that have already been **written and loaned**
    /// (`LOANED`, owned by `owner`) — for a single-batch commit by
    /// [`commit_inner`](Artifact::commit_inner) just above, for a stream by its
    /// `append_batch`. This method publishes them, takes one reference on the
    /// predecessor's manifest for an `Append` link (ADR-0013), stages the
    /// manifest, and installs the new version with one linearising CAS.
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
        self.commit_staged_inner_j(
            expect,
            owner,
            commit,
            staged,
            staged_spans,
            schema_id,
            lease_fence,
            None,
        )
    }

    /// **Optimistic staged commit with a borrow journal** (ADR-0014 §3): the
    /// staged manifest is journaled between staging and the install CAS, so a
    /// committer that dies in that window is torn down by replay instead of
    /// stranding its manifest chain. `shm-stream` uses this.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_staged_optimistic_journaled(
        &self,
        owner: u32,
        expect: u64,
        commit: Commit,
        staged: &[ChunkDesc],
        batch_spans: &[u32],
        schema_id: u32,
        journal: &BorrowJournal<'_>,
    ) -> Result<u64> {
        self.commit_staged_inner_j(
            expect,
            owner,
            commit,
            staged,
            batch_spans,
            schema_id,
            None,
            Some(journal),
        )
    }

    /// The one true install path, journaled (see
    /// [`commit_staged_inner`](Self::commit_staged_inner) for the contract).
    #[allow(clippy::too_many_arguments)]
    fn commit_staged_inner_j(
        &self,
        expect: u64,
        owner: u32,
        commit: Commit,
        staged: &[ChunkDesc],
        staged_spans: &[u32],
        schema_id: u32,
        lease_fence: Option<u32>,
        journal: Option<&BorrowJournal<'_>>,
    ) -> Result<u64> {
        debug_assert_ne!(owner, OWNER_NONE, "commit owner id must be non-zero");
        // Cheap early-out; the authoritative check is step 4b, after the slot
        // claim registers this commit.
        self.check_live()?;
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

        // 2. For an Append with a prior version, resolve the link (ADR-0013):
        //    validate the prior manifest self-identifies as this artifact's
        //    `expect` version (ADR-0003a) and carries the same schema. The
        //    prior's chunks are NOT re-listed — the new manifest links to the
        //    prior manifest, and the table is the chain.
        let is_append = matches!(commit, Commit::Append) && expect != NO_VERSION;
        let mut link: Option<ManifestLink> = None;
        if is_append {
            let mref = PackedRef(head.manifest_desc.load(Acquire));
            let prior = match read_manifest_checked(&self.data_seg, mref, self.name_id, expect) {
                Ok(prior) => prior,
                _ => {
                    // Prior version moved or is unreadable: treat as a conflict.
                    self.rollback_staged(&pool, &published, None, None, None);
                    return Err(Error::Conflict {
                        expected: expect,
                        actual: head.current.load(SeqCst),
                    });
                }
            };
            if prior.schema_id != schema_id {
                self.rollback_staged(&pool, &published, None, None, None);
                return Err(Error::Unsupported("Append schema differs from the prior version"));
            }

            // 3. ONE `borrow_shared` on the prior *manifest* chunk: the new
            //    manifest's link reference. Then re-validate the prior by
            //    identity under our held reference (it freezes the bytes). A
            //    failed borrow means the prior was already retired (a concurrent
            //    commit moved `current`); a failed re-validation means the chunk
            //    was recycled across the `ChunkCtrl` split-word window — its
            //    next occupant's `try_loan` reset the refcount, so there is no
            //    reference of ours to release (see `rollback_staged`).
            let mdesc = manifest_chunk_desc(mref, 0);
            let ctrl = match pool.ctrl(&mdesc) {
                Ok(c) => c,
                Err(e) => {
                    self.rollback_staged(&pool, &published, None, None, None);
                    return Err(Error::from(e));
                }
            };
            // The generation is sampled BEFORE the borrow and compared after
            // the identity check: equality proves the chunk was not recycled
            // across our bump, i.e. the reference we hold is genuine.
            let generation = ctrl.generation();
            if ctrl.borrow_shared().is_err() {
                self.rollback_staged(&pool, &published, None, None, None);
                return Err(Error::Conflict {
                    expected: expect,
                    actual: head.current.load(SeqCst),
                });
            }
            let identity_ok =
                read_manifest_checked(&self.data_seg, mref, self.name_id, expect).is_ok();
            let generation_ok = ctrl.generation() == generation;
            if !identity_ok || !generation_ok {
                // If the generation still matches, our +1 landed on a
                // still-PUBLISHED occupant at that generation (a reset can only
                // follow a FREE, which bumps it) — even if identity failed
                // because the chunk was recycled and republished as something
                // else *before* our sample. That reference is ours and must be
                // released, through the cascade in case it was the last one
                // (ADR-0013 review F1). Only a moved generation is the
                // ambiguous split-word case, where `try_loan` already wiped
                // the bump and releasing would steal the next occupant's ref.
                if generation_ok {
                    release_manifest_ref(&pool, &self.data_seg, mref, Some(generation));
                }
                self.rollback_staged(&pool, &published, None, None, None);
                return Err(Error::Conflict {
                    expected: expect,
                    actual: head.current.load(SeqCst),
                });
            }
            link = Some(prior.link_from(mref, generation));
        }

        let target = expect + 1;

        // 4. Stage the manifest chunk for the new version: its OWN chunks +
        //    batch spans, and the link (depth + 1, running batch total).
        let alloc = PoolAllocator::new(&pool, &self.data_seg);
        let manifest_desc = match stage_chunk(
            &pool,
            |a| {
                write_manifest(
                    a,
                    self.name_id,
                    target,
                    schema_id,
                    &published,
                    staged_spans,
                    link.as_ref(),
                )
            },
            &alloc,
            owner,
        ) {
            Ok(d) => d,
            Err(e) => {
                self.rollback_staged(&pool, &published, link.as_ref(), None, None);
                return Err(e);
            }
        };
        let manifest_ref = PackedRef::from_desc(&manifest_desc);
        // Crash ledger for the window between here and the install CAS
        // (ADR-0014 §3). A full journal just means an unjournaled window, as
        // before — never a failed commit.
        let staged_rec = journal.and_then(|j| {
            j.record_staged_manifest(
                self.name_id,
                self.incarnation,
                manifest_ref.to_bits(),
                manifest_desc.generation,
            )
            .ok()
        });
        let drop_rec = || {
            if let (Some(j), Some(s)) = (journal, staged_rec) {
                let _ = j.release(s);
            }
        };

        // 5. Claim a live-version slot (readers can find it once installed).
        let slot_idx = match head.claim_slot(target, manifest_ref.to_bits()) {
            Some(i) => i,
            None => {
                self.rollback_staged(
                    &pool,
                    &published,
                    link.as_ref(),
                    Some(&manifest_desc),
                    None,
                );
                drop_rec();
                return Err(Error::Unsupported("live-version table full"));
            }
        };

        // 4b. Re-validate the occupant now that the slot claim has registered
        // this commit (ADR-0008 P0.1). A claimed slot is `SLOT_LIVE`, which is
        // exactly what a reclaim sweep's quiescence check refuses to reclaim
        // under — so as with the pin path, claiming *before* checking is what
        // makes the two mutually exclusive rather than racy. The fenced variant:
        // unlike the pin path there is no `accept_pin` fence between the claim
        // and this load, and the StoreLoad barrier is what the pairing (and
        // loom, `tests/loom_reclaim.rs`) needs.
        if !head.revalidate_incarnation(self.incarnation) {
            head.pins[slot_idx].store_free();
            self.rollback_staged(
                &pool,
                &published,
                link.as_ref(),
                Some(&manifest_desc),
                None,
            );
            drop_rec();
            return Err(Error::Stale);
        }

        // The predecessor's endorsed manifest, so the post-install retire
        // targets exactly the slot that owns `expect`'s references and never a
        // stale duplicate a late committer claimed for the same version.
        let prior_bits = head.manifest_desc.load(Acquire);

        // 6. Install: the single linearising CAS of `current`.
        match head
            .current
            .compare_exchange(expect, target, SeqCst, SeqCst)
        {
            Ok(_) => {
                // Publish the manifest pointer (readers validate manifest.version
                // so the brief two-word window is never observed torn).
                head.manifest_desc.store(manifest_ref.to_bits(), Release);
                let _ = head
                    .schema_id
                    .compare_exchange(0, schema_id, SeqCst, Acquire);

                // The predecessor is now non-current; reclaim it if unpinned —
                // the endorsed slot only (ADR-0013 review F3).
                if expect != NO_VERSION {
                    let _ =
                        try_retire_version_at(head, &self.data_seg, expect, Some(prior_bits));
                }

                if let Some(sink) = &self.watch {
                    sink(VersionEvent::new(self.name_id, target, commit.kind()));
                }
                // Installed: the version's slot now endorses the manifest, so
                // the ledger entry is redundant (and replay would treat it as
                // installed anyway — see `reclaim_staged_manifest`).
                drop_rec();
                Ok(target)
            }
            Err(actual) => {
                // Lost the race: undo everything staged.
                self.rollback_staged(
                    &pool,
                    &published,
                    link.as_ref(),
                    Some(&manifest_desc),
                    Some((slot_idx, target)),
                );
                drop_rec();
                Err(Error::Conflict {
                    expected: expect,
                    actual,
                })
            }
        }
    }

    /// Undo a failed commit: free the claimed slot, release the new manifest
    /// chunk, release the Append link's reference on the prior manifest
    /// (through the cascade — a concurrent `Replace` may have retired the prior
    /// version meanwhile, making our link its last reference), and release
    /// every published staged chunk (refcount to 0 → `FREE`).
    ///
    /// The link is released only if the prior manifest chunk still carries the
    /// generation the link was taken at: a changed generation means the chunk
    /// was recycled across the `ChunkCtrl` split-word window (state and
    /// refcount are separate words), its next occupant's `try_loan` reset the
    /// refcount, and a release here would take a reference that is not ours.
    fn rollback_staged(
        &self,
        pool: &Pool<'_>,
        published: &[ChunkDesc],
        link: Option<&ManifestLink>,
        manifest_desc: Option<&ChunkDesc>,
        slot_idx: Option<(usize, u64)>,
    ) {
        if let Some((idx, version)) = slot_idx {
            // Only if the slot still holds OUR claimed version: an `evict_all`
            // that raced the claim window may already have retired and freed
            // it — and it may since have been claimed by someone else
            // (ADR-0013 review F2).
            let slot = &self.head().pins[idx];
            if slot.state.load(Acquire) == SLOT_LIVE && slot.version.load(Acquire) == version {
                slot.version.store(0, Release);
                slot.state.store(crate::head::SLOT_FREE, Release);
            }
        }
        if let Some(m) = manifest_desc {
            // Our own manifest lists `published` and the link, which are
            // released explicitly below — so this is a plain release, never the
            // cascade.
            release_chunk(pool, m);
        }
        if let Some(l) = link {
            let mdesc = manifest_chunk_desc(l.mref, 0);
            let live = pool
                .ctrl(&mdesc)
                .map(|c| c.generation() == l.generation)
                .unwrap_or(false);
            if live {
                release_manifest_ref(pool, &self.data_seg, l.mref, Some(l.generation));
            }
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
            slot.publish_pin();

            // (3) Re-validate the slot is still {version == v, state == LIVE}
            // with SeqCst loads — the reader half of the hazard handshake. If it
            // flipped to SLOT_FREEING (a reclaimer won the election) or the
            // version moved, back off and retry. The `state` load is SeqCst and
            // ordered after the SeqCst bump: this is the Dekker pairing against
            // the reclaimer's `FREEING`-store-then-pins-scan. (Both loads live in
            // `PinSlot::accept_pin`, the loom-checked reader half — ADR-0004 L.)
            if !slot.accept_pin(v) {
                undo_pin(head, &self.data_seg, idx);
                backoff(&mut spins);
                continue;
            }

            // (3b) Re-validate the *occupant* on the same register-then-check
            // discipline (ADR-0008 P0.1). The pin is already published, so a
            // reclaim sweep racing us either sees it and aborts, or retired the
            // head before we looked and we see that here. The StoreLoad fence
            // this load needs is the one `accept_pin` just executed (which is
            // why this is the bare `is_incarnation`, not the fenced
            // `revalidate_incarnation` — no second fence on the hot pin path).
            // A stale pin is not retryable — the entry this handle names is
            // gone — so undo and report rather than spinning.
            if !head.is_incarnation(self.incarnation) {
                undo_pin(head, &self.data_seg, idx);
                return Err(Error::Stale);
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
                    match j.record_artifact_pin(self.name_id, self.incarnation, v) {
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
                    head_off: self.head_off,
                    version: v,
                    slot_idx: idx,
                    manifest,
                    journal,
                }),
            });
        }
    }

    /// **P0.3 (ADR-0010, G12b) — take a guard-less *retained* pin on the
    /// current version**, returning the pinned version number. The pin count is
    /// left incremented with **no** RAII guard and **no** journal entry: the
    /// caller records the `{artifact_id, incarnation, version}` triple
    /// somewhere durable (the task queue's lease side table) and later releases
    /// it through [`release_leaked_pin`](Self::release_leaked_pin) via
    /// [`attach_at_incarnation`](Self::attach_at_incarnation) — exactly the
    /// coordinator's item-J crash route, which drops a binding whose
    /// incarnation no longer matches instead of touching the slot's next
    /// occupant.
    ///
    /// Deliberately **not** journaled in the caller's borrow journal: a task
    /// input binding must *survive* the submitter's death (the task still needs
    /// its input) and die with the **task** instead — released at requester ack
    /// or by the coordinator's reap backstop. Registration-wise this is the
    /// already-loom-modeled pin handshake ([`PinSlot::publish_pin`] →
    /// [`PinSlot::accept_pin`] → [`ArtifactHead::is_incarnation`]) run twice —
    /// once by the inner [`pin`](Self::pin) and once for the retained
    /// increment taken while that pin is held — so it adds no new registration
    /// point to the ADR-0008 recycle handshake. While a retained pin is armed,
    /// the entry cannot go quiescent, so its catalog slot cannot be recycled
    /// out from under the binding.
    pub fn retain_pin(&self) -> Result<u64> {
        // The base pin does the full validated handshake (manifest endorsement
        // included), and holding it freezes the slot's `{version, LIVE}` pair.
        let pin = self.pin_inner(None)?;
        let head = self.head();
        let slot = &head.pins[pin.inner.slot_idx];
        let mut spins: u32 = 0;
        loop {
            // Publish the retained increment, then revalidate — the reader half
            // of both the ADR-0003a pin hazard handshake and the ADR-0008
            // recycle handshake (accept_pin's fence is the StoreLoad barrier
            // `is_incarnation` rides, as on the hot pin path).
            slot.publish_pin();
            if slot.accept_pin(pin.inner.version) && head.is_incarnation(self.incarnation) {
                break;
            }
            // A transient FREEING election (reverted, since our base pin holds
            // `pins > 0`) or a mid-abort sweep retire (re-commissioned for the
            // same reason): undo and retry — both provably converge while the
            // base pin is held.
            slot.unpin();
            backoff(&mut spins);
        }
        let version = pin.inner.version;
        drop(pin); // the base pin releases; the retained increment stays
        Ok(version)
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
    /// **Crash replay of a [`StagedManifest`](shm_core::JournalRecord::StagedManifest)
    /// record (ADR-0014 §3).** The committer died between staging its manifest
    /// and the install CAS — or between the install and releasing the record.
    /// Distinguish the two by what an install leaves behind: a live pin slot
    /// (or the head) naming the manifest. Endorsed ⇒ installed ⇒ nothing to
    /// do (`Ok(false)`). Otherwise release the manifest's own reference
    /// through the cascade — own data chunks, then the link — validated
    /// against the generation it was staged at, so a rolled-back-and-
    /// reallocated chunk is never touched. Returns `true` iff something was
    /// released.
    pub fn reclaim_staged_manifest(&self, manifest: u64, generation: u32) -> Result<bool> {
        let head = self.head();
        if head.manifest_desc.load(Acquire) == manifest {
            return Ok(false);
        }
        for slot in head.pins.iter() {
            if slot.state.load(Acquire) == SLOT_LIVE && slot.manifest.load(Acquire) == manifest {
                return Ok(false);
            }
        }
        let pool = Pool::attach(&self.data_seg)?;
        let mref = PackedRef(manifest);
        let desc = manifest_chunk_desc(mref, generation);
        match pool.ctrl(&desc) {
            Ok(ctrl) if ctrl.validate(&desc).is_ok() && ctrl.state() == PUBLISHED => {}
            _ => return Ok(false), // already rolled back (and possibly reused)
        }
        release_manifest_ref(&pool, &self.data_seg, mref, Some(generation));
        Ok(true)
    }

    /// **Crash replay of an `ArtifactPin` record (item J).** Decrement the pin
    /// count on `version` through the guarded [`PinSlot::try_unpin`] and retire
    /// it if that was the last pin. `Ok(false)` if nothing tracked it.
    pub fn release_leaked_pin(&self, version: u64) -> Result<bool> {
        let head = self.head();
        let idx = match head.find_slot(version) {
            Some(i) => i,
            None => return Ok(false),
        };
        let slot = &head.pins[idx];
        // Guarded: a replay (or a binding release) cannot prove it still holds
        // a pin, and an unconditional decrement on a count another releaser
        // already took to zero would free the version under a live reader.
        let Some(prev) = slot.try_unpin() else {
            return Ok(false);
        };
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
    head_ref_at(head_seg, 0)
}

/// Borrow the [`ArtifactHead`] at byte `off` within a management segment's
/// payload (`off == 0` is the standard single-head layout; a non-zero offset is
/// the keyed-store shared-management-segment layout, ADR-0007 G3).
#[inline]
fn head_ref_at(head_seg: &Segment, off: usize) -> &ArtifactHead {
    // SAFETY: `create`/`create_at` initialised an `ArtifactHead` at `off`
    // (payload_ptr is 64-byte aligned and `off` is 8-aligned, so the address
    // meets the head's alignment; a bounds check at construction guaranteed the
    // region is large enough). The region stays mapped for the segment's
    // lifetime, and every field is an atomic (`Sync`), so a shared reference for
    // concurrent atomic access is sound.
    unsafe { &*head_seg.payload_ptr().add(off).cast::<ArtifactHead>() }
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

/// Release one reference on a chunk; if that was the last reference the chunk
/// is recycled to `FREE` and returned to the pool's free list. Returns `true`
/// iff **this** release freed it — `try_reclaim`'s `PUBLISHED → FREE` CAS
/// elects exactly one releaser (modeled in `tests/loom_ctrl.rs`).
fn release_chunk(pool: &Pool<'_>, desc: &ChunkDesc) -> bool {
    if let Ok(ctrl) = pool.ctrl(desc) {
        // Generation-checked: a descriptor whose chunk was freed and
        // re-published to someone else since it was written must NOT
        // decrement the new occupant's count (ADR-0013 review F2/F5). Every
        // caller's descriptor carries the generation it was staged/linked at.
        if ctrl.validate(desc).is_ok() && ctrl.state() == PUBLISHED {
            // `release_shared` decrements the refcount and reclaims iff it hit 0
            // (owner already released at stage time).
            if ctrl.release_shared() {
                let _ = pool.free(desc);
                return true;
            }
        }
    }
    false
}

/// The [`ChunkDesc`] that locates a manifest chunk from its [`PackedRef`].
/// `len`/`generation` are irrelevant to `ctrl`/`free`, which locate the chunk by
/// `offset`.
#[inline]
fn manifest_chunk_desc(mref: PackedRef, generation: u32) -> ChunkDesc {
    ChunkDesc {
        segment_id: mref.segment_id(),
        generation,
        offset: mref.offset(),
        len: 0,
        schema_id: 0,
        _pad: 0,
    }
}

/// Release one reference on the manifest chunk at `mref`, running the
/// **retire cascade** (ADR-0013) if that was its last reference:
///
/// ```text
/// loop:
///   parsed = read_manifest(mref)          // under our held ref: immutable, mapped
///   if !release_chunk(mref): return       // still referenced (a successor links it)
///   for c in parsed.chunks: release_chunk(c)
///   mref = parsed.prev or return
/// ```
///
/// The caller must hold a reference on `mref` (a version's ref on its own
/// manifest, or a successor manifest's link ref). The manifest is read
/// **before** the release, while the reference still freezes its bytes; once
/// freed it is never read again (`Pool::free` overwrites its first word, so
/// the magic would fail anyway). The cascade runs exactly once per manifest —
/// whichever releaser's `release_shared` observes `true` — so every data chunk
/// and every link reference is released exactly once, by its sole owner. It
/// stops at the first manifest another reference still holds: a successor's
/// link, or a reader's pinned version further back in the chain.
/// `generation`: the generation the caller's reference was taken at, when it
/// knows it (a link's `generation`; a rollback's sampled value). `None` for the
/// first hop of a retire, where the caller provably owns a reference (the
/// version ref) and the chunk cannot have moved. Every subsequent hop uses the
/// link generation the manifest recorded, so a stale link can never decrement
/// a recycled chunk's next occupant.
fn release_manifest_ref(
    pool: &Pool<'_>,
    data_seg: &Segment,
    mref: PackedRef,
    generation: Option<u32>,
) {
    let mut mref = mref;
    let mut generation = generation;
    loop {
        let parsed = read_manifest(data_seg, mref).ok();
        let released = match generation {
            Some(g) => release_chunk(pool, &manifest_chunk_desc(mref, g)),
            None => release_chunk_owned(pool, &manifest_chunk_desc(mref, 0)),
        };
        if !released {
            return;
        }
        let Some(m) = parsed else {
            return;
        };
        for c in &m.chunks {
            release_chunk(pool, c);
        }
        match m.prev {
            Some(link) => {
                mref = link.mref;
                generation = Some(link.generation);
            }
            None => return,
        }
    }
}

/// Release a reference the caller provably owns on a chunk it cannot name the
/// generation of (the retire's first hop: the version reference, held since
/// commit). Unvalidated by construction; never use it for a reference that
/// might have been taken across a recycle.
fn release_chunk_owned(pool: &Pool<'_>, desc: &ChunkDesc) -> bool {
    if let Ok(ctrl) = pool.ctrl(desc) {
        if ctrl.state() == PUBLISHED && ctrl.release_shared() {
            let _ = pool.free(desc);
            return true;
        }
    }
    false
}

/// Undo a speculative pin bump on slot `idx`, helping reclaim whatever version
/// the slot tracks if this released its last pin while it is non-current.
fn undo_pin(head: &ArtifactHead, data_seg: &Segment, idx: usize) {
    let slot = &head.pins[idx];
    let prev = slot.unpin();
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
/// # The exact reclamation rule (ADR-0013)
///
/// A version's chunks are released only when its pin count is `0` **and** it is
/// not `current`. A version owns exactly **one** reference: its manifest chunk.
/// A manifest owns one reference on each data chunk it lists and one on its
/// predecessor manifest (its `Append` link, if any). Retiring a version is one
/// `release_shared` on its manifest; if that was the manifest's last reference
/// (no successor links it), the cascade ([`release_manifest_ref`]) releases the
/// manifest's own chunks and follows the link — so a chain is freed exactly
/// once, back to the first manifest something else still holds (a successor's
/// link or a pinned older version). A data chunk's `refcount` is the number of
/// manifests listing it (`1` in practice). Because `current` is monotonic and
/// never revisits `version`, once `current != version` it stays so — the
/// reclaimability precondition is stable.
fn try_retire_version(head: &ArtifactHead, data_seg: &Segment, version: u64) -> Result<()> {
    try_retire_version_at(head, data_seg, version, None)
}

/// [`try_retire_version`] targeting the **endorsed** slot when the caller
/// knows which manifest `version` was installed with (`Some(bits)`); with
/// `None`, the first live slot tracking `version` (ADR-0013 review F3).
fn try_retire_version_at(
    head: &ArtifactHead,
    data_seg: &Segment,
    version: u64,
    manifest_bits: Option<u64>,
) -> Result<()> {
    let mut spins: u32 = 0;
    loop {
        let found = match manifest_bits {
            Some(bits) => head.find_slot_with_manifest(version, bits),
            None => head.find_slot(version),
        };
        let idx = match found {
            Some(i) => i,
            None => return Ok(()), // already reclaimed, or another reclaimer owns FREEING
        };
        let slot = &head.pins[idx];

        if head.current.load(SeqCst) == version {
            return Ok(()); // still current: not reclaimable
        }
        // Elect the single reclaimer AND publish the `FREEING` hazard flag with a
        // SeqCst store, all in one CAS — this must precede the pin scan below.
        // (`elect_freeing`/`pin_scan`/`revert_live` are the loom-checked reclaimer
        // half of the hazard handshake — ADR-0004 stage L.)
        if !slot.elect_freeing() {
            return Ok(()); // someone else is reclaiming (or slot changed)
        }

        // Scan pins AFTER publishing FREEING (the handshake ordering).
        if slot.pin_scan() != 0 {
            // A reader is live (or a racing reader is mid-protocol). Do NOT free.
            // Revert so a later retire can proceed.
            slot.revert_live();
            if slot.pin_scan() != 0 {
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

        // Release this version's one reference — its manifest chunk — and, if
        // that was the manifest's last reference, cascade through its own data
        // chunks and down the chain (ADR-0013).
        let mref = PackedRef(slot.manifest.load(Acquire));
        release_manifest_ref(&pool, data_seg, mref, None);

        // Return the slot to the free pool (FREEING -> FREE).
        slot.store_free();
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
        self.artifact.commit_inner_j(
            expect,
            self.owner,
            commit,
            batch,
            registry,
            Some(self.token),
            self.lease.as_ref().map(|l| l.journal),
        )
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
        self.artifact.commit_staged_inner_j(
            expect,
            self.owner,
            commit,
            staged,
            batch_spans,
            schema_id,
            Some(self.token),
            self.lease.as_ref().map(|l| l.journal),
        )
    }

    /// **P0.3 (ADR-0010, G4).** Evict the current version under the held
    /// lease: commit an **empty** `Replace` version superseding it, retiring
    /// the evicted version through the standard handshake. The leased
    /// counterpart of [`Artifact::evict_current_optimistic`] (see there for
    /// why this is an empty commit, not a `current → NO_VERSION` CAS). Fails
    /// [`Error::VersionGone`] when nothing is committed and [`Error::Fenced`]
    /// if this writer's lease was force-released (entry evicted, or declared
    /// dead) — installing nothing.
    pub fn evict_current(&mut self) -> Result<u64> {
        let head = self.artifact.head();
        let expect = head.current.load(SeqCst);
        if expect == NO_VERSION {
            return Err(Error::VersionGone);
        }
        let schema_id = head.schema_id.load(Acquire);
        self.artifact.commit_staged_inner(
            expect,
            self.owner,
            Commit::Replace,
            &[],
            &[],
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
        // Election first (ADR-0014 §4): a journaled lease the coordinator
        // already force-released (this committer is a zombie) is not ours to
        // release — and the fence bump makes the token stale in any case.
        if let Some(l) = &self.lease {
            if let Ok(false) = l.journal.release(l.slot) {
                return;
            }
        }
        let _ = self
            .artifact
            .head()
            .release_write_lease(self.owner, self.token);
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
    /// Byte offset of the [`ArtifactHead`] within `head_seg` (see
    /// [`Artifact::head_off`]); `0` for the standard layout.
    head_off: usize,
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
                // The journal slot IS the pin's ownership (ADR-0014 §4). If
                // the coordinator's replay already cleared it — this actor was
                // declared dead and is a zombie — the replay performed the
                // decrement, and doing it again would steal a live reader's.
                if let Ok(false) = journal.release(jp.slot) {
                    return;
                }
            }
        }
        let head = head_ref_at(&self.head_seg, self.head_off);
        let slot = &head.pins[self.slot_idx];
        // We hold a live pin, so the slot still tracks our version and cannot be
        // reclaimed/reused underneath us.
        let prev = slot.unpin();
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

    /// The parsed **head** [`Manifest`] of the pinned version: the chunks this
    /// version *added* plus its link to the predecessor's manifest (ADR-0013).
    /// The pin parses only this manifest, so pinning is O(own data). For the
    /// whole table see [`chain`](Self::chain) / [`data_chunks`](Self::data_chunks).
    #[inline]
    pub fn manifest(&self) -> &Manifest {
        &self.inner.manifest
    }

    /// The pinned version's manifest chain, **oldest-first** (the root at index
    /// `0`, [`manifest`](Self::manifest) last) — the row order of the table.
    ///
    /// A pure [`walk_chain_with`] bounded by the head's `depth`, with every hop
    /// identity-checked (`read_manifest_checked`, ADR-0003a) and its chunk
    /// generation compared to the link's. While this pin is held every member
    /// of the chain is `PUBLISHED` with `refcount >= 1` (the head is held by
    /// the pinned version; each predecessor by its successor's link), so the
    /// walk never dereferences a freed chunk. Cost: O(chain length).
    pub fn chain(&self) -> Result<Vec<Manifest>> {
        let seg = &self.inner.data_seg;
        let pool = Pool::attach(seg)?;
        let artifact_id = self.inner.manifest.artifact_id;
        walk_chain_with(&self.inner.manifest, |link| {
            let ctrl = pool.ctrl(&manifest_chunk_desc(link.mref, 0))?;
            if ctrl.generation() != link.generation {
                return Err(Error::VersionGone);
            }
            read_manifest_checked(seg, link.mref, artifact_id, link.version)
        })
    }

    /// Every data chunk of the pinned version's table, flat and in row order
    /// (each chain member's own chunks, oldest manifest first). O(chain +
    /// chunks).
    pub fn data_chunks(&self) -> Result<Vec<ChunkDesc>> {
        let chain = self.chain()?;
        let mut out = Vec::with_capacity(chain.iter().map(|m| m.chunks.len()).sum());
        for m in &chain {
            out.extend_from_slice(&m.chunks);
        }
        Ok(out)
    }

    /// Reconstruct the pinned version's data as one Arrow [`RecordBatch`] **per
    /// batch**, in row order, zero-copy: each column's buffers point directly
    /// into the mapped segment, and a clone of this pin is the buffers'
    /// keep-alive (its lifetime holds the mapping *and* the version alive).
    ///
    /// Walks the chain (oldest-first) and reconstructs each batch from its
    /// group of chunks (a multi-chunk batch spans several consecutive chunks,
    /// per each manifest's [`Manifest::batch_spans`] boundary table). Cost is
    /// O(batches) — independent of row count — and no byte is copied. An empty
    /// version (evicted-current) yields an empty `Vec`.
    pub fn as_arrow_batches(&self, registry: &SchemaRegistry) -> Result<Vec<RecordBatch>> {
        let pool = Pool::attach(&self.inner.data_seg)?;
        let chain = self.chain()?;
        let mut batches: Vec<RecordBatch> =
            Vec::with_capacity(self.inner.manifest.total_batches as usize);
        for manifest in &chain {
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
        }
        Ok(batches)
    }

    /// Reconstruct the pinned version's data as **one** logical Arrow
    /// [`RecordBatch`].
    ///
    /// A single-batch version (the common `Replace` case) is returned directly,
    /// zero-copy, exactly as [`as_arrow_batches`](Self::as_arrow_batches)
    /// builds it. A multi-batch version (an `Append` lineage, or a multi-batch
    /// stream commit) is **concatenated** via `concat_batches`, which copies
    /// every byte into a fresh batch — use `as_arrow_batches` to stay
    /// zero-copy. Returns [`Error::VersionGone`] for an empty version.
    pub fn as_arrow(&self, registry: &SchemaRegistry) -> Result<RecordBatch> {
        let mut batches = self.as_arrow_batches(registry)?;
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
