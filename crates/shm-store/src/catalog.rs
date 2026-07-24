//! The on-shm **catalog**: an append-only array of [`CatalogSlot`]s plus a header,
//! placed by hand into a store-owned management segment.
//!
//! The catalog is the keyed store's shared-memory **fast path**: a worker that has
//! mapped the catalog segment resolves `key_id → CatalogSlot → ArtifactHead` with
//! no UDS round-trip (§ADR-0007 G3). It mirrors how `shm-core` places
//! [`ChunkCtrl`](shm_core::ChunkCtrl) and `shm-ring` places its `RingHeader`:
//! every concurrently-touched word is a [`ShmU32`]/[`ShmU64`] atomic overlaid on
//! shared bytes.
//!
//! # Slot lifecycle (the CAS state machine)
//!
//! ```text
//!   FREE ──publish (CAS FREE→LIVE)──▶ LIVE ──evict (CAS LIVE→TOMBSTONE)──▶ TOMBSTONE
//! ```
//!
//! Slots are **append-only**: a monotonic `next_slot` bump hands each creator a
//! brand-new index it exclusively owns, so publishing a slot is race-free across
//! processes (no two creators contend for the same index). A slot's `head_off`
//! (where its [`ArtifactHead`](shm_artifact::ArtifactHead) lives in the head
//! segment) and `artifact_id` (its lineage id) are pure functions of the index —
//! `idx * head_stride` and `artifact_id_base + idx` — so they are globally unique
//! and monotonic, and a coordinator can route a journaled entry pin back to its
//! head by index alone. Eviction tombstones the slot and never reuses it; a
//! re-`create` of the same key appends a *new* slot with a *new* lineage id.

use core::sync::atomic::Ordering::{AcqRel, Acquire, Release};

use shm_core::segment::HEADER_SIZE;
use shm_core::{Segment, ShmU32, ShmU64};

use crate::error::{Error, Result};

/// Catalog header magic: little-endian bytes of `b"SHMSTOR1"`.
pub const CATALOG_MAGIC: u64 = u64::from_le_bytes(*b"SHMSTOR1");

/// Slot state: unused and claimable (also the zero-initialised value).
pub const SLOT_FREE: u32 = 0;
/// Slot state: tracks a live keyed entry.
pub const SLOT_LIVE: u32 = 1;
/// Slot state: the entry was evicted; the slot is retired and never reused.
pub const SLOT_TOMBSTONE: u32 = 2;

/// The kind of referent a [`CatalogSlot`] (and, in the G1 stage, a `TypedRef`)
/// names. The discriminants are the frozen ABI values from ADR-0007.
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefKind {
    /// A raw, untyped chunk (no key; `key_id == 0`).
    RawChunk = 0,
    /// An opaque object (single blob).
    Object = 1,
    /// A versioned artifact.
    Artifact = 2,
    /// A dataset (a table-shaped artifact).
    Dataset = 3,
    /// A task/computation result.
    Result = 4,
}

impl RefKind {
    /// The `u16` ABI discriminant.
    #[inline]
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// Decode a `u16` ABI discriminant, or [`Error::BadKind`] if out of range.
    #[inline]
    pub fn from_u16(v: u16) -> Result<RefKind> {
        Ok(match v {
            0 => RefKind::RawChunk,
            1 => RefKind::Object,
            2 => RefKind::Artifact,
            3 => RefKind::Dataset,
            4 => RefKind::Result,
            other => return Err(Error::BadKind(other)),
        })
    }
}

/// One catalog entry: a keyed pointer to an [`ArtifactHead`](shm_artifact::ArtifactHead).
///
/// # Layout (20 bytes, all fields atomic)
///
/// | field         | type       | meaning                                        |
/// |---------------|------------|------------------------------------------------|
/// | `key_id`      | `AtomicU32`| coordinator-interned key id (0 = none)         |
/// | `artifact_id` | `AtomicU32`| the entry's lineage id (`base + slot index`)   |
/// | `head_off`    | `AtomicU32`| byte offset of the `ArtifactHead` in the head seg |
/// | `kind`        | `AtomicU32`| [`RefKind`] discriminant (a `u16` value widened)|
/// | `state`       | `AtomicU32`| `FREE` / `LIVE` / `TOMBSTONE` — the CAS gate    |
///
/// ADR-0007 diagrams the field as `kind: u16, _pad: u16`; here the `u16` kind and
/// its pad are folded into one 4-byte atomic word so the whole slot is built from
/// uniform [`ShmU32`] atoms (the `shm-core` placed-by-hand convention) — the
/// on-shm size (20 B) and the logical field set are unchanged.
#[repr(C)]
pub struct CatalogSlot {
    /// Coordinator-interned key id (`0` = none / raw).
    pub key_id: ShmU32,
    /// The entry's lineage id, used to route journaled entry pins on crash.
    pub artifact_id: ShmU32,
    /// Byte offset of this entry's `ArtifactHead` within the head segment.
    pub head_off: ShmU32,
    /// The [`RefKind`] discriminant (a `u16` value stored in a `u32` word).
    pub kind: ShmU32,
    /// Lifecycle state; the single CAS gate publishing/retiring the slot.
    pub state: ShmU32,
}

const _: () = assert!(core::mem::size_of::<CatalogSlot>() == 20);
const _: () = assert!(core::mem::align_of::<CatalogSlot>() == 4);

impl CatalogSlot {
    /// The interned key id this slot tracks (`0` if none).
    #[inline]
    pub fn key_id(&self) -> u32 {
        self.key_id.load(Acquire)
    }

    /// The entry's lineage id.
    #[inline]
    pub fn artifact_id(&self) -> u32 {
        self.artifact_id.load(Acquire)
    }

    /// The byte offset of the entry's `ArtifactHead` within the head segment.
    #[inline]
    pub fn head_off(&self) -> u32 {
        self.head_off.load(Acquire)
    }

    /// The entry's [`RefKind`].
    #[inline]
    pub fn kind(&self) -> Result<RefKind> {
        RefKind::from_u16(self.kind.load(Acquire) as u16)
    }

    /// The current lifecycle state ([`SLOT_FREE`]/[`SLOT_LIVE`]/[`SLOT_TOMBSTONE`]).
    #[inline]
    pub fn state(&self) -> u32 {
        self.state.load(Acquire)
    }

    /// Write the four data fields (before publishing the state). The writer owns
    /// this slot exclusively (a freshly bumped index), so these plain `Release`
    /// stores need no CAS; the subsequent [`publish`](Self::publish) CAS is the
    /// release edge a reader's [`state`](Self::state) `Acquire` load synchronises
    /// with, so a `LIVE` reader always observes the fields written here.
    #[inline]
    fn write_fields(&self, key_id: u32, artifact_id: u32, head_off: u32, kind: RefKind) {
        self.key_id.store(key_id, Release);
        self.artifact_id.store(artifact_id, Release);
        self.head_off.store(head_off, Release);
        self.kind.store(u32::from(kind.as_u16()), Release);
    }

    /// Publish the slot: CAS `state` `FREE → LIVE`. `true` iff this caller won.
    #[inline]
    pub fn publish(&self) -> bool {
        self.state
            .compare_exchange(SLOT_FREE, SLOT_LIVE, AcqRel, Acquire)
            .is_ok()
    }

    /// Tombstone the slot: CAS `state` `LIVE → TOMBSTONE`. `true` iff this caller
    /// performed the transition; `false` if it was not `LIVE` (already tombstoned
    /// or never published) — which makes eviction idempotent.
    #[inline]
    pub fn tombstone(&self) -> bool {
        self.state
            .compare_exchange(SLOT_LIVE, SLOT_TOMBSTONE, AcqRel, Acquire)
            .is_ok()
    }
}

/// On-segment catalog header (placed at the payload base).
#[repr(C)]
struct CatalogHeader {
    magic: ShmU64,
    capacity: ShmU32,
    next_slot: ShmU32,
    head_stride: ShmU32,
    artifact_id_base: ShmU32,
}

/// Byte offset of the slot array within the catalog segment payload.
#[inline]
const fn slots_offset() -> usize {
    // Header is 8+4+4+4+4 = 24 bytes; `CatalogSlot` needs 4-byte alignment.
    let hdr = core::mem::size_of::<CatalogHeader>();
    (hdr + 3) & !3
}

/// A handle to a catalog laid out inside a [`Segment`].
///
/// Like [`Pool`](shm_core::Pool), the handle only caches derived pointers; all
/// state lives in the segment, so many processes may each hold their own
/// `Catalog` over the same shared bytes.
pub struct Catalog<'s> {
    #[allow(dead_code)]
    segment: &'s Segment,
    header: *const CatalogHeader,
    slots: *mut CatalogSlot,
    capacity: usize,
}

impl<'s> Catalog<'s> {
    /// Payload bytes a catalog of `capacity` slots needs.
    #[inline]
    pub const fn required_bytes(capacity: u32) -> usize {
        slots_offset() + (capacity as usize) * core::mem::size_of::<CatalogSlot>()
    }

    /// The total segment size (header + payload) a catalog of `capacity` needs.
    #[inline]
    pub const fn segment_bytes(capacity: u32) -> usize {
        HEADER_SIZE + Self::required_bytes(capacity)
    }

    /// Initialise a fresh catalog into `segment`: write the header and zero every
    /// slot to [`SLOT_FREE`].
    ///
    /// `head_stride` is the byte stride between consecutive entries'
    /// `ArtifactHead`s in the head segment; `artifact_id_base` is the first
    /// lineage id (kept well above the coordinator's per-name artifact id space so
    /// the two never collide in a journal record).
    pub fn init(
        segment: &'s Segment,
        capacity: u32,
        head_stride: u32,
        artifact_id_base: u32,
    ) -> Result<Catalog<'s>> {
        let need = Self::required_bytes(capacity);
        if need > segment.payload_len() {
            return Err(Error::Core(shm_core::Error::LayoutOverflow(
                "catalog does not fit in segment",
            )));
        }
        let base = segment.payload_ptr();
        // SAFETY: every offset below is `< need <= payload_len`, so all writes
        // stay within the mapped payload region; the segment is exclusively owned
        // by the coordinator for this init.
        unsafe {
            base.cast::<CatalogHeader>().write(CatalogHeader {
                magic: ShmU64::new(CATALOG_MAGIC),
                capacity: ShmU32::new(capacity),
                next_slot: ShmU32::new(0),
                head_stride: ShmU32::new(head_stride),
                artifact_id_base: ShmU32::new(artifact_id_base),
            });
            let slots = base.add(slots_offset()).cast::<CatalogSlot>();
            for i in 0..capacity as usize {
                slots.add(i).write(CatalogSlot {
                    key_id: ShmU32::new(0),
                    artifact_id: ShmU32::new(0),
                    head_off: ShmU32::new(0),
                    kind: ShmU32::new(0),
                    state: ShmU32::new(SLOT_FREE),
                });
            }
        }
        Self::attach(segment)
    }

    /// Attach to a catalog previously [`init`](Self::init)ialised in `segment`.
    pub fn attach(segment: &'s Segment) -> Result<Catalog<'s>> {
        let base = segment.payload_ptr();
        // SAFETY: the header lives at the payload base; the store guarantees at
        // least `size_of::<CatalogHeader>()` payload bytes.
        let hdr = unsafe { &*base.cast::<CatalogHeader>() };
        if hdr.magic.load(Acquire) != CATALOG_MAGIC {
            return Err(Error::BadCatalog);
        }
        let capacity = hdr.capacity.load(Acquire) as usize;
        // SAFETY: `slots_offset()` is within the payload for any real catalog.
        let slots = unsafe { base.add(slots_offset()).cast::<CatalogSlot>() };
        Ok(Catalog {
            segment,
            header: base.cast::<CatalogHeader>(),
            slots,
            capacity,
        })
    }

    #[inline]
    fn header(&self) -> &CatalogHeader {
        // SAFETY: header lives at the payload base for the catalog's lifetime.
        unsafe { &*self.header }
    }

    /// The fixed slot capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The byte stride between consecutive entries' `ArtifactHead`s.
    #[inline]
    pub fn head_stride(&self) -> u32 {
        self.header().head_stride.load(Acquire)
    }

    /// The first lineage id (`artifact_id` of slot 0).
    #[inline]
    pub fn artifact_id_base(&self) -> u32 {
        self.header().artifact_id_base.load(Acquire)
    }

    /// The number of slots appended so far (`>= ` the live entry count, since a
    /// tombstoned slot is never reused).
    #[inline]
    pub fn next_slot(&self) -> u32 {
        self.header().next_slot.load(Acquire)
    }

    /// Borrow slot `idx`. `idx` must be `< capacity`.
    #[inline]
    pub fn slot(&self, idx: u32) -> &CatalogSlot {
        debug_assert!((idx as usize) < self.capacity, "slot index out of range");
        // SAFETY: `idx < capacity`; the slot array has `capacity` entries.
        unsafe { &*self.slots.add(idx as usize) }
    }

    /// Claim the next append-only slot index, or [`Error::CatalogFull`] once the
    /// fixed table is exhausted.
    pub fn alloc_slot(&self) -> Result<u32> {
        let idx = self.header().next_slot.fetch_add(1, AcqRel);
        if idx as usize >= self.capacity {
            return Err(Error::CatalogFull(self.capacity as u32));
        }
        Ok(idx)
    }

    /// The `head_off` for slot `idx` (`idx * head_stride`).
    #[inline]
    pub fn head_off_for(&self, idx: u32) -> u32 {
        idx.wrapping_mul(self.head_stride())
    }

    /// The lineage `artifact_id` for slot `idx` (`artifact_id_base + idx`).
    #[inline]
    pub fn artifact_id_for(&self, idx: u32) -> u32 {
        self.artifact_id_base().wrapping_add(idx)
    }

    /// Populate and publish slot `idx` for `key_id`/`kind`, returning its derived
    /// `(artifact_id, head_off)`. Writes the data fields, then CAS-publishes
    /// `FREE → LIVE`.
    pub fn publish_slot(&self, idx: u32, key_id: u32, kind: RefKind) -> (u32, u32) {
        let artifact_id = self.artifact_id_for(idx);
        let head_off = self.head_off_for(idx);
        let slot = self.slot(idx);
        slot.write_fields(key_id, artifact_id, head_off, kind);
        slot.publish();
        (artifact_id, head_off)
    }

    /// Find the index of the single [`SLOT_LIVE`] slot tracking `key_id`, if any.
    /// Scans the appended slots (`0..next_slot`).
    pub fn find_live_by_key(&self, key_id: u32) -> Option<u32> {
        let n = self.next_slot().min(self.capacity as u32);
        for idx in 0..n {
            let slot = self.slot(idx);
            if slot.state() == SLOT_LIVE && slot.key_id() == key_id {
                return Some(idx);
            }
        }
        None
    }

    /// Resolve a lineage `artifact_id` back to its slot index (for a coordinator
    /// routing a journaled entry pin / write lease on crash). The index is derived
    /// (`artifact_id - base`) and then validated against the stored slot, so a
    /// stale or foreign id yields `None`.
    pub fn slot_for_artifact_id(&self, artifact_id: u32) -> Option<u32> {
        let base = self.artifact_id_base();
        if artifact_id < base {
            return None;
        }
        let idx = artifact_id - base;
        if idx >= self.next_slot().min(self.capacity as u32) {
            return None;
        }
        if self.slot(idx).artifact_id() == artifact_id {
            Some(idx)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_seg(id: u32, capacity: u32) -> Segment {
        let bytes = Catalog::segment_bytes(capacity);
        let _ = Segment::unlink_by_id(id);
        Segment::create(id, bytes).expect("create catalog seg")
    }

    #[test]
    fn ref_kind_round_trips() {
        for k in [
            RefKind::RawChunk,
            RefKind::Object,
            RefKind::Artifact,
            RefKind::Dataset,
            RefKind::Result,
        ] {
            assert_eq!(RefKind::from_u16(k.as_u16()).unwrap(), k);
        }
        assert!(matches!(RefKind::from_u16(5), Err(Error::BadKind(5))));
    }

    #[test]
    fn slot_cas_state_machine() {
        let base = 70_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 8);
        let cat = Catalog::init(&seg, 8, 2048, 1 << 28).expect("init");

        let idx = cat.alloc_slot().expect("alloc");
        let slot = cat.slot(idx);
        assert_eq!(slot.state(), SLOT_FREE, "fresh slot is FREE");

        // FREE -> LIVE (publish).
        let (aid, hoff) = cat.publish_slot(idx, 42, RefKind::Dataset);
        assert_eq!(slot.state(), SLOT_LIVE);
        assert_eq!(slot.key_id(), 42);
        assert_eq!(slot.artifact_id(), aid);
        assert_eq!(slot.head_off(), hoff);
        assert_eq!(slot.kind().unwrap(), RefKind::Dataset);
        // A second publish CAS is a no-op (already LIVE).
        assert!(!slot.publish());

        // LIVE -> TOMBSTONE (evict), and idempotent re-evict.
        assert!(slot.tombstone(), "first evict transitions LIVE->TOMBSTONE");
        assert_eq!(slot.state(), SLOT_TOMBSTONE);
        assert!(!slot.tombstone(), "second evict is a no-op");

        seg.unlink().ok();
    }

    #[test]
    fn find_by_key_and_by_artifact_id() {
        let base = 71_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 16);
        let cat = Catalog::init(&seg, 16, 2048, 1 << 28).expect("init");

        let i0 = cat.alloc_slot().unwrap();
        let (aid0, _) = cat.publish_slot(i0, 100, RefKind::Dataset);
        let i1 = cat.alloc_slot().unwrap();
        let (aid1, _) = cat.publish_slot(i1, 200, RefKind::Result);

        assert_eq!(cat.find_live_by_key(100), Some(i0));
        assert_eq!(cat.find_live_by_key(200), Some(i1));
        assert_eq!(cat.find_live_by_key(999), None, "absent key");

        assert_eq!(cat.slot_for_artifact_id(aid0), Some(i0));
        assert_eq!(cat.slot_for_artifact_id(aid1), Some(i1));
        assert_eq!(cat.slot_for_artifact_id(1), None, "below base");
        assert_eq!(cat.slot_for_artifact_id(aid1 + 1000), None, "unappended");

        // Tombstoning i0 makes its key un-findable but re-create appends a new slot.
        assert!(cat.slot(i0).tombstone());
        assert_eq!(cat.find_live_by_key(100), None, "tombstoned key is gone");
        let i2 = cat.alloc_slot().unwrap();
        let (aid2, _) = cat.publish_slot(i2, 100, RefKind::Dataset);
        assert_eq!(cat.find_live_by_key(100), Some(i2), "re-create finds new slot");
        assert_ne!(aid2, aid0, "reincarnation has a new lineage id");

        seg.unlink().ok();
    }

    #[test]
    fn alloc_slot_exhausts_cleanly() {
        let base = 72_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 2);
        let cat = Catalog::init(&seg, 2, 2048, 1 << 28).expect("init");
        assert_eq!(cat.alloc_slot().unwrap(), 0);
        assert_eq!(cat.alloc_slot().unwrap(), 1);
        assert!(matches!(cat.alloc_slot(), Err(Error::CatalogFull(2))));
        seg.unlink().ok();
    }
}
