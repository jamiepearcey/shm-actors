//! The [`ArtifactHead`] management region: the atomic RCU/MVCC control block.
//!
//! One `ArtifactHead` lives at the base of an artifact's *management* segment
//! payload. Every field is mutated concurrently by readers, committers, and the
//! reclaimer, so — like [`shm_core::ChunkCtrl`] and [`shm_ring::RingHeader`] —
//! it is built entirely from atomics and placed into shared memory **by hand**
//! (it is deliberately not `SharedPod`).

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Head magic: little-endian bytes of `b"SHMAHEAD"`.
pub const HEAD_MAGIC: u64 = u64::from_le_bytes(*b"SHMAHEAD");

/// Sentinel `current` value meaning "no version has been committed yet".
pub const NO_VERSION: u64 = 0;

/// Sentinel writer-owner value meaning "no exclusive lease is held".
pub const NO_WRITER: u32 = 0;

/// Number of concurrently-live version slots the pin table tracks.
///
/// A slot is occupied from the moment a committer claims it (just before the
/// install CAS) until the version is reclaimed (non-current, pin count 0). With
/// prompt reclamation the number of *simultaneously* live versions is bounded
/// by the number of versions any reader still pins, so this is generous for
/// v0.1; exhaustion surfaces as [`Error::Unsupported`](crate::Error::Unsupported).
pub const MAX_LIVE_VERSIONS: usize = 64;

/// Pin-slot lifecycle: the slot is unused and claimable.
pub const SLOT_FREE: u32 = 0;
/// Pin-slot lifecycle: the slot tracks a live version.
pub const SLOT_LIVE: u32 = 1;
/// Pin-slot lifecycle: the slot's version has been claimed for reclamation.
pub const SLOT_RETIRED: u32 = 2;

/// One entry in the [`ArtifactHead`] pin table: a version, its manifest, and its
/// live pin count.
///
/// The slot carries the version's manifest [`PackedRef`](shm_core::PackedRef) so
/// the reclaimer can find and release the version's chunks *without* consulting
/// the head's `manifest_desc` (which only ever names the **current** version).
///
/// # Layout (frozen ABI — 24 bytes)
///
/// | field      | type        | meaning                                     |
/// |------------|-------------|---------------------------------------------|
/// | `version`  | `AtomicU64` | version tracked by this slot (0 while free) |
/// | `manifest` | `AtomicU64` | packed ref to this version's manifest chunk |
/// | `pins`     | `AtomicU32` | number of live [`VersionPin`](crate::VersionPin)s |
/// | `state`    | `AtomicU32` | [`SLOT_FREE`] / [`SLOT_LIVE`] / [`SLOT_RETIRED`] |
#[repr(C)]
pub struct PinSlot {
    /// The version this slot currently tracks; `0` when the slot is free.
    pub version: AtomicU64,
    /// Packed [`PackedRef`](shm_core::PackedRef) to this version's manifest chunk.
    pub manifest: AtomicU64,
    /// Count of live pins on this version (readers freezing it).
    pub pins: AtomicU32,
    /// Slot lifecycle state; gates single-winner reclamation.
    pub state: AtomicU32,
}

const _: () = assert!(core::mem::size_of::<PinSlot>() == 24);
const _: () = assert!(core::mem::align_of::<PinSlot>() == 8);

/// The atomic RCU/MVCC control block for one artifact.
///
/// # Layout (frozen ABI)
///
/// | field           | type                        | meaning                       |
/// |-----------------|-----------------------------|-------------------------------|
/// | `magic`         | `AtomicU64`                 | [`HEAD_MAGIC`]                |
/// | `current`       | `AtomicU64`                 | current version (0 = none)    |
/// | `manifest_desc` | `AtomicU64`                 | packed [`PackedRef`](shm_core::PackedRef) to the current manifest |
/// | `writer_owner`  | `AtomicU32`                 | exclusive-lease owner (0 = none) |
/// | `schema_id`     | `AtomicU32`                 | interned Arrow schema id (informational) |
/// | `pins`          | `[PinSlot; MAX_LIVE_VERSIONS]` | live-version pin table     |
///
/// # RCU protocol summary
///
/// - **Install** (committer): claim a free [`PinSlot`] for `n+1`, then a single
///   `SeqCst` CAS of `current` from `n` to `n+1`, then a `Release` store of the
///   new `manifest_desc`. A reader validates `manifest.version == pinned`, so
///   the brief two-word window is never observed torn.
/// - **Pin** (reader): `SeqCst` `fetch_add` on the current version's slot, then
///   re-read `current`; if it moved, undo and retry. The `SeqCst` bump ordered
///   against the committer's `SeqCst` `current` store is the Dekker guarantee
///   that a live pin is never missed by reclamation.
/// - **Reclaim**: a version's chunks are freed only once its slot's `pins == 0`
///   **and** it is not `current`; a `SLOT_LIVE → SLOT_RETIRED` CAS elects the
///   single reclaimer.
#[repr(C)]
pub struct ArtifactHead {
    /// Must equal [`HEAD_MAGIC`] once initialised.
    pub magic: AtomicU64,
    /// The current (latest installed) version number; [`NO_VERSION`] until the
    /// first commit.
    pub current: AtomicU64,
    /// Packed [`PackedRef`](shm_core::PackedRef) to the current version's
    /// manifest chunk.
    pub manifest_desc: AtomicU64,
    /// The exclusive write-lease owner actor id, or [`NO_WRITER`].
    pub writer_owner: AtomicU32,
    /// The interned Arrow schema id of the artifact's data (set on first commit;
    /// informational — the authoritative id is in each manifest).
    pub schema_id: AtomicU32,
    /// Fixed table of live-version pin counters for reclamation.
    pub pins: [PinSlot; MAX_LIVE_VERSIONS],
}

impl ArtifactHead {
    /// The number of payload bytes an [`ArtifactHead`] occupies. A management
    /// segment's payload must be at least this large.
    pub const fn region_bytes() -> usize {
        core::mem::size_of::<ArtifactHead>()
    }

    /// Initialise a fresh head in place: magic set, no version, no writer, every
    /// pin slot free.
    ///
    /// # Safety
    ///
    /// `ptr` must point at [`ArtifactHead::region_bytes`] writable, 8-byte
    /// aligned bytes that stay mapped for the artifact's lifetime, and no other
    /// party may concurrently initialise the same region.
    pub unsafe fn init_at(ptr: *mut ArtifactHead) {
        // SAFETY: the caller guarantees `ptr` is valid, aligned, and exclusively
        // owned for this initialising write. Atomics are `#[repr(C)]` over their
        // integer, so an in-place `write` of the struct is well-defined.
        unsafe {
            ptr.write(ArtifactHead {
                magic: AtomicU64::new(HEAD_MAGIC),
                current: AtomicU64::new(NO_VERSION),
                manifest_desc: AtomicU64::new(0),
                writer_owner: AtomicU32::new(NO_WRITER),
                schema_id: AtomicU32::new(0),
                pins: core::array::from_fn(|_| PinSlot {
                    version: AtomicU64::new(0),
                    manifest: AtomicU64::new(0),
                    pins: AtomicU32::new(0),
                    state: AtomicU32::new(SLOT_FREE),
                }),
            });
        }
    }

    /// Validate that a region already holds an initialised head.
    #[inline]
    pub fn check_magic(&self) -> bool {
        self.magic.load(Ordering::Acquire) == HEAD_MAGIC
    }

    /// Find the index of the [`SLOT_LIVE`] slot tracking `version`, if any.
    pub fn find_slot(&self, version: u64) -> Option<usize> {
        for (i, slot) in self.pins.iter().enumerate() {
            if slot.state.load(Ordering::Acquire) == SLOT_LIVE
                && slot.version.load(Ordering::Acquire) == version
            {
                return Some(i);
            }
        }
        None
    }

    /// Claim a free slot for `version` (whose manifest is at packed ref
    /// `manifest`), returning its index, or `None` if the table is full. On
    /// success the slot is `SLOT_LIVE` with `pins == 0`.
    ///
    /// The slot's `version` is published *after* the `SLOT_FREE → SLOT_LIVE` CAS
    /// but the slot only becomes reader-findable once `version` is stored, so a
    /// reader scanning for `version` never matches a half-claimed slot (a free
    /// slot's `version` is `0`, which no real version equals).
    pub fn claim_slot(&self, version: u64, manifest: u64) -> Option<usize> {
        for (i, slot) in self.pins.iter().enumerate() {
            if slot
                .state
                .compare_exchange(SLOT_FREE, SLOT_LIVE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                slot.pins.store(0, Ordering::Release);
                slot.manifest.store(manifest, Ordering::Release);
                slot.version.store(version, Ordering::Release);
                return Some(i);
            }
        }
        None
    }
}
