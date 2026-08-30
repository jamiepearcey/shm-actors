//! [`KeyedStore`] and [`Entry`]: a keyed collection **of** `shm-artifact`
//! artifacts over the store's granted catalog + head + data segments.
//!
//! The store adds **keying** (opaque byte string → coordinator-interned
//! `key_id` → catalog slot → `ArtifactHead`) on top of the *unchanged*
//! `shm-artifact` RCU: an [`Entry`] wraps a real
//! [`Artifact`](shm_artifact::Artifact) and delegates commit / pin / read /
//! append straight to it. No MVCC/RCU machinery is reinvented here.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use shm_arrow::SchemaRegistry;
use shm_artifact::{Artifact, Commit, Delta, VersionPin, WindowPolicy};
use shm_core::{BorrowJournal, Segment};

use crate::catalog::{Catalog, RefKind};
use crate::error::{Error, Result};
use crate::typed_ref::{resolve_path, ResolvePath, TypedRef};

/// The key-interning seam: opaque key bytes → coordinator-interned `key_id`
/// (`0` reserved for none). Injected by the runtime (the [`Node`] implements it
/// over UDS) so `shm-store` stays transport-agnostic and never speaks UDS itself
/// — exactly the seam `shm-artifact` uses for its watch sink and `shm-arrow` for
/// its schema cache. A [`KeyedStore`] borrows one for its lifetime.
pub trait KeyResolver {
    /// Intern `key`, returning its stable coordinator-issued `key_id`.
    fn intern_key(&self, key: &[u8]) -> Result<u32>;
}

/// The maximum length, in bytes, of a store key.
pub const MAX_KEY_LEN: usize = 1024;

/// A handle onto one keyed store: a keyed directory of `shm-artifact` artifacts
/// backed by three coordinator-granted segments — the **catalog** (the shm
/// fast-path index), the **head** management segment (packing every entry's
/// [`ArtifactHead`](shm_artifact::ArtifactHead)), and the **data** segment (one
/// shared [`Pool`](shm_core::Pool) backing every entry's data + manifest chunks).
///
/// Resolving a key to an entry is a pure shared-memory catalog scan — no UDS
/// round-trip — once the segments are mapped and the key is interned; only a
/// cold key-interning miss touches the coordinator (via [`KeyInterner`]).
pub struct KeyedStore<'a> {
    catalog_seg: Arc<Segment>,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
    journal_seg: Arc<Segment>,
    registry: Arc<SchemaRegistry>,
    owner: u32,
    resolver: &'a dyn KeyResolver,
}

impl<'a> KeyedStore<'a> {
    /// Build a store handle over its granted segments.
    ///
    /// - `catalog_seg` / `head_seg` / `data_seg` — the store's three segments
    ///   (the coordinator created + initialised the catalog and the data pool).
    /// - `journal_seg` — the calling actor's borrow journal (so entry pins and
    ///   exclusive write leases are journaled for crash reclaim).
    /// - `registry` — the actor's schema cache.
    /// - `owner` — the actor id stamped on commits/leases.
    /// - `resolver` — the coordinator key-interning seam (borrowed for the
    ///   handle's lifetime).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_seg: Arc<Segment>,
        head_seg: Arc<Segment>,
        data_seg: Arc<Segment>,
        journal_seg: Arc<Segment>,
        registry: Arc<SchemaRegistry>,
        owner: u32,
        resolver: &'a dyn KeyResolver,
    ) -> KeyedStore<'a> {
        KeyedStore {
            catalog_seg,
            head_seg,
            data_seg,
            journal_seg,
            registry,
            owner,
            resolver,
        }
    }

    /// The store's shared **data** segment (one pool backing every entry's
    /// chunks). Exposed so a layer above can place its own POD envelopes in the
    /// same pool the G1 `TypedRef` rides in ([`write_typed_ref`](crate::write_typed_ref)):
    /// an envelope destined for another process must live in a segment every
    /// actor has mapped, and this is the one they all share (ADR-0015).
    #[inline]
    pub fn data_segment(&self) -> &Arc<Segment> {
        &self.data_seg
    }

    /// Attach a fresh [`Catalog`] handle over the mapped catalog segment.
    fn catalog(&self) -> Result<Catalog<'_>> {
        Catalog::attach(&self.catalog_seg)
    }

    /// Intern `key` (validating its length) to a stable `key_id`.
    fn key_id(&self, key: &[u8]) -> Result<u32> {
        if key.len() > MAX_KEY_LEN {
            return Err(Error::KeyTooLong(key.len()));
        }
        self.resolver.intern_key(key)
    }

    /// Build an [`Entry`] over the catalog slot at `idx` (attaching a real
    /// [`Artifact`] at the slot's `head_off`).
    fn entry_at(&self, cat: &Catalog<'_>, idx: u32) -> Result<Entry> {
        let slot = cat.slot(idx);
        let artifact = Artifact::attach_at(
            slot.artifact_id(),
            self.head_seg.clone(),
            slot.head_off() as usize,
            self.data_seg.clone(),
        )?;
        Ok(Entry {
            artifact,
            key_id: slot.key_id(),
            kind: slot.kind()?,
            journal_seg: self.journal_seg.clone(),
            registry: self.registry.clone(),
            owner: self.owner,
        })
    }

    /// Get-or-create the entry for `key`: intern the key, allocate an
    /// [`ArtifactHead`](shm_artifact::ArtifactHead) in the head segment, run
    /// [`Artifact::create_at`], and publish a `LIVE` catalog slot. If a live
    /// entry for `key` already exists it is returned as-is (idempotent).
    ///
    /// `schema` is interned into the local registry so a subsequent commit stamps
    /// a consistent id; for cross-process agreement the caller should have
    /// coordinator-interned it first (the runtime does this before `create`).
    pub fn create(&self, key: &[u8], kind: RefKind, schema: &SchemaRef) -> Result<Entry> {
        let key_id = self.key_id(key)?;
        self.registry.intern(schema);
        let cat = self.catalog()?;
        if let Some(idx) = cat.find_live_by_key(key_id) {
            return self.entry_at(&cat, idx);
        }
        let idx = cat.alloc_slot()?;
        let artifact_id = cat.artifact_id_for(idx);
        let head_off = cat.head_off_for(idx);
        // The slot carries the incarnation its next occupant is stamped with, so
        // a handle or journal record left over from the slot's *previous*
        // occupant cannot be mistaken for one of ours (ADR-0008 P0.1).
        let incarnation = cat.slot(idx).gen();
        // Initialise the entry's ArtifactHead in the shared head segment over the
        // shared data pool (the coordinator laid the pool once).
        let artifact = Artifact::create_at(
            artifact_id,
            incarnation,
            self.head_seg.clone(),
            head_off as usize,
            self.data_seg.clone(),
        )?;
        // Publish the slot LIVE (recomputes the same artifact_id/head_off).
        cat.publish_slot(idx, key_id, kind);
        Ok(Entry {
            artifact,
            key_id,
            kind,
            journal_seg: self.journal_seg.clone(),
            registry: self.registry.clone(),
            owner: self.owner,
        })
    }

    /// Open the live entry for `key`, or [`Error::NotFound`] if none is live
    /// (never created, or evicted).
    pub fn open(&self, key: &[u8]) -> Result<Entry> {
        let key_id = self.key_id(key)?;
        self.open_id(key_id)
    }

    /// Open the live entry for an already-interned `key_id` (the pure fast path:
    /// a catalog scan with no UDS round-trip), or [`Error::NotFound`].
    pub fn open_id(&self, key_id: u32) -> Result<Entry> {
        let cat = self.catalog()?;
        let idx = cat.find_live_by_key(key_id).ok_or(Error::NotFound)?;
        self.entry_at(&cat, idx)
    }

    /// Evict `key`: CAS its catalog slot `LIVE → TOMBSTONE`, then tear down every
    /// version (draining pins via `shm-artifact`'s hazard/retire path and
    /// reclaiming chunks by refcount — see [`Artifact::evict_all`]). Idempotent:
    /// evicting an absent or already-tombstoned key is a clean no-op.
    ///
    /// Then it attempts to **reclaim the slot** (ADR-0008 P0.1). That succeeds
    /// only if the entry came out of `evict_all` quiescent — the common case,
    /// where nothing was pinned. An entry still held by a live reader stays
    /// tombstoned and is picked up by
    /// [`reclaim_tombstones`](Self::reclaim_tombstones) once that reader lets go.
    ///
    /// # Binding to the occupant (ADR-0008 P0.1)
    ///
    /// Because slots recycle, "the slot at `idx`" is not a stable identity: in
    /// the window between the key scan and the tombstone CAS the slot can in
    /// principle be evicted by someone else, swept, and re-created. So the
    /// teardown binds to the **occupant**, not the slot:
    ///
    /// - `gen` is read *before* the CAS and re-checked *after* it. `gen`
    ///   advances only when a sweep frees the slot, so an unchanged `gen`
    ///   proves the CAS hit the occupant that was current at the read.
    /// - `key_id` is re-checked too: an unchanged `gen` can still name an
    ///   occupant that replaced the scanned one before the `gen` read. Same
    ///   key ⇒ evicting it is simply this eviction linearised after the
    ///   re-create; different key ⇒ wrong entry, so the tombstone is undone.
    /// - The teardown attaches with [`Artifact::attach_at_incarnation`] against
    ///   that proven `gen` (never "whatever is live now"), so even a sweep
    ///   completing underneath cannot retarget `evict_all` at a next occupant.
    pub fn evict(&self, key: &[u8]) -> Result<()> {
        let key_id = self.key_id(key)?;
        let cat = self.catalog()?;
        let idx = match cat.find_live_by_key(key_id) {
            Some(i) => i,
            None => return Ok(()), // absent or already tombstoned
        };
        let slot = cat.slot(idx);
        let expected = slot.gen();
        // One CAS on `{gen, LIVE}` (ADR-0014): if the slot was recycled since
        // we found it by key, its gen differs and the CAS fails — the entry this
        // call meant to evict is already gone, and no other occupant was
        // touched. There is no window and nothing to undo.
        if !slot.tombstone_gen(expected) {
            return Ok(());
        }
        // Tear down the occupant we tombstoned. `Stale` here means a concurrent
        // sweep is holding the head retired mid-quiescence-check (or already
        // freed the slot); either way the sweep's own teardown pass converges
        // the entry, so it is not an error for the evictor.
        match Artifact::attach_at_incarnation(
            slot.artifact_id(),
            expected,
            self.head_seg.clone(),
            slot.head_off() as usize,
            self.data_seg.clone(),
        ) {
            Ok(artifact) => match artifact.evict_all() {
                Ok(()) | Err(shm_artifact::Error::Stale) => {}
                Err(e) => return Err(e.into()),
            },
            Err(shm_artifact::Error::Stale) => {}
            Err(e) => return Err(e.into()),
        }
        self.try_reclaim_slot(&cat, idx);
        Ok(())
    }

    /// Sweep the catalog, returning every tombstoned slot whose entry has gone
    /// quiescent to the free list. Returns how many were freed.
    ///
    /// Driven from the coordinator's lease-monitor tick. Eviction sweeps its own
    /// slot inline, so this only ever collects entries that were still busy at
    /// eviction time — held by a live reader's pin, or written to again by a
    /// straggler handle (the sweep re-runs the teardown before it judges
    /// quiescence, so those converge too).
    pub fn reclaim_tombstones(&self) -> Result<usize> {
        sweep_tombstones(&self.catalog_seg, &self.head_seg, &self.data_seg)
    }

    /// Try to reclaim the one slot at `idx` (already tombstoned).
    fn try_reclaim_slot(&self, cat: &Catalog<'_>, idx: u32) {
        cat.try_reclaim(idx, |artifact_id, head_off| {
            entry_is_finished(&self.head_seg, &self.data_seg, artifact_id, head_off)
        });
    }

    // ---- G1: resolve a typed-ref envelope (ADR-0007) ----

    /// Resolve a [`TypedRef`] to its live [`Entry`] by its authoritative
    /// `key_id` (ADR-0007 G1×G3). A [`RefKind::RawChunk`] envelope (no key) is
    /// [`Error::NotResolvable`]; any other kind opens the entry via
    /// [`open_id`](Self::open_id) (a pure catalog scan — no UDS), or
    /// [`Error::NotFound`] if no live entry tracks the key.
    pub fn resolve(&self, tref: &TypedRef) -> Result<Entry> {
        if tref.kind()? == RefKind::RawChunk {
            return Err(Error::NotResolvable);
        }
        self.open_id(tref.key_id)
    }

    /// Which path [`resolve_and_pin`](Self::resolve_and_pin) will take for `tref`
    /// (ADR-0007 G1): the pre-resolved [`ResolvePath::FastPath`] when the envelope
    /// carries a valid `locator`, else [`ResolvePath::ByKey`]. Pure (no store
    /// access), so a caller can log/branch on the decision.
    #[inline]
    pub fn resolve_path(&self, tref: &TypedRef) -> ResolvePath {
        resolve_path(tref)
    }

    /// Resolve a [`TypedRef`], pin the entry through the actor's borrow journal,
    /// check its `version`, and reconstruct the referent **zero-copy** (ADR-0007
    /// G1×G3). Returns the owned `(Entry, VersionPin, RecordBatch)` — the pin
    /// keeps the version (and the batch's shared-memory buffers) alive until
    /// dropped, and a `kill -9` mid-pin is crash-reclaimed by the coordinator.
    ///
    /// # Fast-path vs by-key
    ///
    /// [`resolve_path`](Self::resolve_path) documents which path a peer *would*
    /// take. A crash-safe read must hold a **journaled** pin the coordinator can
    /// reclaim, and a journaled pin is only obtainable through the entry's
    /// `ArtifactHead` (reached by `key_id` → catalog slot). So the *pin* is always
    /// taken by key (authoritative); when a valid `locator`/`manifest` fast path
    /// is present it is used only to skip re-deriving the referent, and the read
    /// still flows through the pinned manifest. The `key_id` therefore governs
    /// correctness; the fast path is a latency hint, never a trust boundary.
    ///
    /// # Version
    ///
    /// `version == 0` pins the entry's **current** version; otherwise the pinned
    /// current version must equal `version`, else [`Error::VersionMismatch`]
    /// (the G3 entry exposes only its current version to pin).
    pub fn resolve_and_pin(&self, tref: &TypedRef) -> Result<(Entry, VersionPin, RecordBatch)> {
        let entry = self.resolve(tref)?;
        let pin = entry.pin()?;
        if tref.version != 0 && pin.version() != tref.version {
            return Err(Error::VersionMismatch {
                expected: tref.version,
                actual: pin.version(),
            });
        }
        let batch = pin.as_arrow(&self.registry)?;
        Ok((entry, pin, batch))
    }
}

/// One keyed entry: a real [`Artifact`] plus its key/kind, delegating every RCU
/// operation to `shm-artifact` unchanged.
pub struct Entry {
    artifact: Artifact,
    key_id: u32,
    kind: RefKind,
    journal_seg: Arc<Segment>,
    registry: Arc<SchemaRegistry>,
    owner: u32,
}

impl Entry {
    /// The interned key id of this entry.
    #[inline]
    pub fn key_id(&self) -> u32 {
        self.key_id
    }

    /// The [`RefKind`] of this entry.
    #[inline]
    pub fn kind(&self) -> RefKind {
        self.kind
    }

    /// The lineage `artifact_id` (also the `name_id` stamped into version events
    /// and journaled pins).
    #[inline]
    pub fn artifact_id(&self) -> u32 {
        self.artifact.name_id()
    }

    /// The current (latest installed) version, or `0` if nothing is committed yet.
    #[inline]
    pub fn current_version(&self) -> u64 {
        self.artifact.current_version()
    }

    /// The underlying [`Artifact`] (for advanced RCU operations not surfaced here).
    #[inline]
    pub fn artifact(&self) -> &Artifact {
        &self.artifact
    }

    /// Commit a new version under the entry's **journaled exclusive write lease**
    /// (so a crash mid-write is force-released by the coordinator), delegating to
    /// [`Committer::commit`](shm_artifact::Committer::commit). Returns the new
    /// version number.
    pub fn commit(&self, commit: Commit, batch: &RecordBatch) -> Result<u64> {
        // The journal must outlive the committer; both live on this stack frame
        // (committer drops first), so the exclusive lease + its crash-ledger entry
        // are released cleanly on return.
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        let mut committer = self
            .artifact
            .open_exclusive_journaled(self.owner, &journal)?;
        let version = committer.commit(commit, batch, &self.registry)?;
        Ok(version)
    }

    /// Convenience: commit `batch` as a [`Commit::Replace`] version.
    pub fn commit_replace(&self, batch: &RecordBatch) -> Result<u64> {
        self.commit(Commit::Replace, batch)
    }

    /// **ADR-0016 — the high-churn stream commit.** Append `batch` under
    /// `policy`: a chained [`Commit::Append`] while the cell's history is
    /// shallower than `policy.max_depth`, then one [`Commit::Window`] that
    /// re-roots on the newest `policy.keep_batches` batches by reference. The
    /// cell's live chunks, manifest size and read cost stay bounded by the
    /// policy no matter how many versions are ever committed; amortised cost
    /// is O(1) reference RMWs per commit and no byte is ever copied. Same
    /// journaled exclusive lease as [`commit`](Self::commit).
    pub fn append_windowed(&self, policy: &WindowPolicy, batch: &RecordBatch) -> Result<u64> {
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        let mut committer = self
            .artifact
            .open_exclusive_journaled(self.owner, &journal)?;
        Ok(committer.commit_windowed(policy, batch, &self.registry)?)
    }

    /// **P0.3 (ADR-0010, G4) — evict the entry's *current* version without
    /// evicting the entry.** Commits an **empty** `Replace` version under the
    /// entry's journaled exclusive write lease; the evicted version retires
    /// through the standard handshake (a pinned reader drains via pin drop).
    /// The entry stays `LIVE` in the catalog — its key still resolves, and the
    /// next commit continues the version sequence — which is exactly what
    /// ArrowRef's `clear_on_ack` needs for a retained task *output*: drop the
    /// data, keep the address. Readers of the evicted-current entry see
    /// `VersionGone`, identical to a never-committed entry. Returns the new
    /// (empty) version number; fails `VersionGone` when nothing is committed.
    pub fn evict_current(&self) -> Result<u64> {
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        let mut committer = self
            .artifact
            .open_exclusive_journaled(self.owner, &journal)?;
        Ok(committer.evict_current()?)
    }

    /// Pin the entry's current version through the actor's borrow journal (ADR-0003
    /// item J), delegating to [`Artifact::pin_journaled`] so a `kill -9` mid-pin is
    /// crash-reclaimed by the coordinator. The pin is released on drop.
    pub fn pin(&self) -> Result<VersionPin> {
        Ok(self.artifact.pin_journaled(&self.journal_seg)?)
    }

    /// Pin + reconstruct the current version as a zero-copy [`RecordBatch`]
    /// (buffers point into the shared data segment; the returned [`VersionPin`]
    /// keeps them — and the version — alive). Delegates to
    /// [`VersionPin::as_arrow`](shm_artifact::VersionPin::as_arrow).
    pub fn read(&self) -> Result<(VersionPin, RecordBatch)> {
        let pin = self.pin()?;
        let batch = pin.as_arrow(&self.registry)?;
        Ok((pin, batch))
    }

    /// **ADR-0016 — the stream consumer's read.** Pin the current version and
    /// return only the batches added after version `since`
    /// ([`VersionPin::batches_since`]): O(new batches), zero-copy, with
    /// [`Delta::truncated`] telling the consumer a `Window`/`Replace`
    /// intervened and the delta is a table prefix to resynchronise from. Pass
    /// the pin's [`version`](VersionPin::version) back as the next `since`.
    pub fn read_since(&self, since: u64) -> Result<(VersionPin, Delta)> {
        let pin = self.pin()?;
        let delta = pin.batches_since(since, &self.registry)?;
        Ok((pin, delta))
    }

    /// **P0.3 (ADR-0010, G12) — retain the current version for a task's
    /// lifetime.** Takes a guard-less, **unjournaled** retained pin on the
    /// entry's current version ([`Artifact::retain_pin`]) and returns the
    /// `{artifact_id, incarnation, version}` triple the caller arms into the
    /// task queue's lease side table (`shm_task::TaskQueue::submit_with_binding`
    /// / `shm_task::ClaimedTask::bind_output`).
    ///
    /// Deliberately *not* journaled in this actor's borrow journal: the pin
    /// must survive this actor's death — the task still needs its input — and
    /// die with the **task** instead, released exactly-once (at requester ack,
    /// or by the coordinator's reap backstop) through
    /// [`release_task_binding`]. While the binding is armed the entry cannot
    /// go quiescent, so its catalog slot cannot recycle out from under it.
    pub fn retain_current(&self) -> Result<RetainedRef> {
        let version = self.artifact.retain_pin()?;
        let artifact_id = self.artifact.name_id();
        let incarnation = self.artifact.incarnation();
        // Journal the retain until it is armed (ADR-0010 addendum). Between
        // `retain_current` and a successful `submit_with_binding` /
        // `bind_output` nothing else tracks this pin: a crash, or an arm that
        // fails `LeaseTableFull`/`QueueFull`, would leak the version and its
        // slot forever. As an `ArtifactPin` record it is reclaimed by the
        // item-J replay exactly like a reader's pin. The record is released
        // at the handoff ([`KeyedStore::binding_armed`]) once the lease table
        // owns the pin.
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        let journal_slot = journal.record_artifact_pin(artifact_id, incarnation, version)?;
        Ok(RetainedRef {
            artifact_id,
            incarnation,
            version,
            journal_slot,
        })
    }

    /// The **handoff**, called **before** arming the binding in the task
    /// queue's lease table (ADR-0014 §4): release the journal record that
    /// covered the retain, and return the binding to arm. `Err(Stale)` means
    /// the coordinator's replay already released this pin — this actor was
    /// declared dead — and the caller must **not** arm: the lease table would
    /// otherwise own a pin that no longer exists and its reap would steal a
    /// live reader's. Ordering is deliberate: releasing after the arm let a
    /// zombie's replay and the lease reap both decrement; releasing before it
    /// leaves only a few-instruction crash window in which one pin leaks —
    /// a bounded leak, never a double release.
    pub fn handoff(&self, r: &RetainedRef) -> Result<RetainedRef> {
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        if !journal.release(r.journal_slot)? {
            return Err(Error::Artifact(shm_artifact::Error::Stale));
        }
        Ok(*r)
    }

    /// Undo a [`retain_current`](Self::retain_current) whose arm **failed**:
    /// release the retained pin (through the guarded leaked-pin path) and its
    /// journal record.
    pub fn release_retained(&self, r: RetainedRef) -> Result<bool> {
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        let _ = journal.release(r.journal_slot);
        Ok(self.artifact.release_leaked_pin(r.version)?)
    }
}

/// A retained-ref binding produced by [`Entry::retain_current`] (ADR-0010):
/// the opaque triple the task fabric carries in its lease side table and
/// [`release_task_binding`] later routes back to the entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetainedRef {
    /// The keyed-store lineage id (routes to a catalog slot).
    pub artifact_id: u32,
    /// The entry occupant the pin was taken against (ADR-0008 P0.1).
    pub incarnation: u32,
    /// The pinned version.
    pub version: u64,
    /// The borrow-journal slot holding this retain until it is armed; see
    /// [`KeyedStore::binding_armed`].
    pub journal_slot: usize,
}

/// **P0.3 (ADR-0010, G12) — release one task-tied retained-ref binding.**
/// Routes `artifact_id` to its catalog slot, attaches **at the recorded
/// incarnation** ([`Artifact::attach_at_incarnation`]), and decrements the
/// retained pin through the same retire path a clean pin drop takes
/// ([`Artifact::release_leaked_pin`]) — exactly the coordinator's item-J crash
/// route. A binding whose incarnation no longer matches (the entry was evicted
/// and its slot recycled — only reachable if the binding was already released
/// once, since an armed binding blocks quiescence) is **dropped**, so a task
/// lease can never decrement a slot's next occupant. Returns `true` iff a pin
/// was actually released.
///
/// Segment-level (like [`sweep_tombstones`]) because the **coordinator** runs
/// the reap backstop from its monitor tick with the store's segments and no
/// actor journal.
pub fn release_task_binding(
    catalog_seg: &Arc<Segment>,
    head_seg: &Arc<Segment>,
    data_seg: &Arc<Segment>,
    artifact_id: u32,
    incarnation: u32,
    version: u64,
) -> bool {
    let Ok(cat) = Catalog::attach(catalog_seg) else {
        return false;
    };
    let Some(idx) = cat.slot_for_artifact_id(artifact_id) else {
        return false;
    };
    let head_off = cat.slot(idx).head_off() as usize;
    match Artifact::attach_at_incarnation(
        artifact_id,
        incarnation,
        head_seg.clone(),
        head_off,
        data_seg.clone(),
    ) {
        Ok(artifact) => artifact.release_leaked_pin(version).unwrap_or(false),
        Err(_) => false, // stale incarnation (or nothing live): drop the binding
    }
}

/// Sweep a store's catalog over its raw segments, returning every tombstoned
/// slot whose entry has gone quiescent to the free list. Returns how many were
/// freed (ADR-0008 P0.1).
///
/// Segment-level rather than a [`KeyedStore`] method because the **coordinator**
/// runs this from its lease-monitor tick, and it has the store's segments but no
/// business holding a key resolver or an actor journal.
pub fn sweep_tombstones(
    catalog_seg: &Arc<Segment>,
    head_seg: &Arc<Segment>,
    data_seg: &Arc<Segment>,
) -> Result<usize> {
    let cat = Catalog::attach(catalog_seg)?;
    Ok(cat.reclaim_tombstones(|artifact_id, head_off| {
        entry_is_finished(head_seg, data_seg, artifact_id, head_off)
    }))
}

/// The sweep's quiescence predicate, run while the catalog holds the slot in
/// `RECLAIMING` (so no new occupant can appear underneath it — attaching and
/// adopting the incarnation found *is* therefore safe here, unlike anywhere
/// else).
///
/// First it **re-runs the teardown** ([`Artifact::evict_all`], idempotent and
/// cheap on an already-empty entry). Eviction is a level, not an edge: a
/// straggler handle held across the evict can still install a version *after*
/// the evictor's own teardown (its commit registers, re-validates the
/// incarnation — still in service — and succeeds), and without this pass such
/// an entry would never go quiescent and its slot would leak forever. Tearing
/// down again on every sweep makes the tombstone converge no matter how late
/// the straggler was; once the slot is finally freed, the straggler's *next*
/// operation fails `Stale`.
///
/// Then it takes the head **out of service**, and only then scans: retiring
/// before scanning is the sweep half of the recycle handshake, so an operation
/// racing this either registers in time to be seen by the scan — and the sweep
/// backs off — or observes the retirement and backs out itself (ADR-0008 P0.1).
/// A sweep that backs off restores the incarnation it took.
fn entry_is_finished(
    head_seg: &Arc<Segment>,
    data_seg: &Arc<Segment>,
    artifact_id: u32,
    head_off: u32,
) -> bool {
    let artifact = match Artifact::attach_at(
        artifact_id,
        head_seg.clone(),
        head_off as usize,
        data_seg.clone(),
    ) {
        Ok(a) => a,
        // Nothing attachable here (e.g. the previous occupant was reclaimed and
        // no new one commissioned). Leave it for the next pass rather than
        // guessing.
        Err(_) => return false,
    };
    if artifact.evict_all().is_err() {
        return false;
    }
    let held = artifact.retire_head();
    if artifact.is_quiescent() {
        true
    } else {
        artifact.commission_head(held);
        false
    }
}
