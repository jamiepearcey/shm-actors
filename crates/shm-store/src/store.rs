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
use shm_artifact::{Artifact, Commit, VersionPin};
use shm_core::{BorrowJournal, Segment};

use crate::catalog::{Catalog, RefKind};
use crate::error::{Error, Result};

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
        // Initialise the entry's ArtifactHead in the shared head segment over the
        // shared data pool (the coordinator laid the pool once).
        let artifact = Artifact::create_at(
            artifact_id,
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
    /// Retiring the lineage `artifact_id` is implicit: the tombstoned slot is
    /// never reused, so a later [`create`](Self::create) of the same key appends a
    /// fresh slot with a fresh lineage id.
    pub fn evict(&self, key: &[u8]) -> Result<()> {
        let key_id = self.key_id(key)?;
        let cat = self.catalog()?;
        let idx = match cat.find_live_by_key(key_id) {
            Some(i) => i,
            None => return Ok(()), // absent or already tombstoned
        };
        let slot = cat.slot(idx);
        if !slot.tombstone() {
            return Ok(()); // lost the race to another evictor; still evicted
        }
        let artifact = Artifact::attach_at(
            slot.artifact_id(),
            self.head_seg.clone(),
            slot.head_off() as usize,
            self.data_seg.clone(),
        )?;
        artifact.evict_all()?;
        Ok(())
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
}
