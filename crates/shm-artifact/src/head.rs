//! The [`ArtifactHead`] management region: the atomic RCU/MVCC control block.
//!
//! One `ArtifactHead` lives at the base of an artifact's *management* segment
//! payload. Every field is mutated concurrently by readers, committers, and the
//! reclaimer, so — like [`shm_core::ChunkCtrl`] and [`shm_ring::RingHeader`] —
//! it is built entirely from atomics and placed into shared memory **by hand**
//! (it is deliberately not `SharedPod`).

use core::sync::atomic::Ordering;
use core::sync::atomic::Ordering::{Acquire, SeqCst};

use shm_core::substrate::fence;
use shm_core::{ShmU32, ShmU64};

/// Head magic: little-endian bytes of `b"SHMAHEA3"`.
///
/// Bumped from `SHMAHEA2` for the Phase 0 / ADR-0008 layout change: the head
/// gained an [`incarnation`](ArtifactHead::incarnation) word, so a head region
/// can be **recycled** by the keyed store's catalog and a handle held across
/// that recycle is detected rather than silently retargeted.
///
/// (`SHMAHEA2` itself replaced `SHMAHEAD` at v0.3 / ADR-0003 item K, when the
/// plain `writer_owner: AtomicU32` became a fenced write lease packed into one
/// [`AtomicU64`](core::sync::atomic::AtomicU64).)
pub const HEAD_MAGIC: u64 = u64::from_le_bytes(*b"SHMAHEA3");

/// Sentinel `incarnation` value meaning "this head is not in service": either
/// never initialised, or **retired** by a catalog sweep that is returning its
/// slot to the free list (ADR-0008 P0.1). Every operation validates the head's
/// incarnation against the one its handle adopted, so an operation arriving
/// against a retired — or already re-occupied — head fails
/// [`Error::Stale`](crate::Error::Stale) instead of corrupting the new occupant.
pub const NO_INCARNATION: u32 = 0;

/// The incarnation stamped on a head that is created once and never recycled
/// (every `Artifact::create`/`attach` artifact, as opposed to a keyed-store
/// entry whose slot the catalog may reclaim).
pub const FIRST_INCARNATION: u32 = 1;

/// Sentinel `current` value meaning "no version has been committed yet".
pub const NO_VERSION: u64 = 0;

/// Sentinel write-lease **owner** value meaning "no exclusive lease is held"
/// (the lease is free and acquirable).
pub const OWNER_NONE: u32 = 0;

/// Deprecated alias for [`OWNER_NONE`] (the field became a fenced lease in
/// ADR-0003 item K). Kept so external references do not break.
pub const NO_WRITER: u32 = OWNER_NONE;

/// Pack a fenced write lease `{owner, fence}` into one `u64`: `owner` in the high
/// 32 bits, `fence` in the low 32 bits. A **free** lease is any value with
/// `owner == OWNER_NONE` (its low bits carry the current fence generation).
#[inline]
pub const fn pack_lease(owner: u32, fence: u32) -> u64 {
    ((owner as u64) << 32) | (fence as u64)
}

/// The owner actor id of a packed write lease (`OWNER_NONE` ⇒ free).
#[inline]
pub const fn lease_owner(packed: u64) -> u32 {
    (packed >> 32) as u32
}

/// The fence generation of a packed write lease. Monotonically advanced on every
/// release (clean, or coordinator-forced on writer death), so a token captured at
/// acquire is invalidated the instant the lease is released out from under it.
#[inline]
pub const fn lease_fence(packed: u64) -> u32 {
    packed as u32
}

/// Number of concurrently-live version slots the pin table tracks.
///
/// A slot is occupied from the moment a committer claims it (just before the
/// install CAS) until the version is reclaimed (non-current, pin count 0). With
/// prompt reclamation the number of *simultaneously* live versions is bounded
/// by the number of versions any reader still pins, so this is generous for
/// v0.1; exhaustion surfaces as [`Error::Unsupported`](crate::Error::Unsupported).
#[cfg(not(loom))]
pub const MAX_LIVE_VERSIONS: usize = 64;

/// Under `--cfg loom` the pin table is shrunk to two slots so a whole
/// [`ArtifactHead`] (which a core-1/core-4 harness reconstructs in ordinary
/// memory and shares across `loom::thread`s) holds only a handful of loom atoms
/// — keeping the model's state space tractable. The claim/find/retire logic is
/// bounded by this constant, so the *same* code runs; only the table width, which
/// is irrelevant to the modeled handshake, differs. It has no on-shm ABI meaning
/// under loom (the size asserts are `not(loom)`-gated).
#[cfg(loom)]
pub const MAX_LIVE_VERSIONS: usize = 2;

/// Pin-slot lifecycle: the slot is unused and claimable.
pub const SLOT_FREE: u32 = 0;
/// Pin-slot lifecycle: the slot tracks a live version.
pub const SLOT_LIVE: u32 = 1;
/// Pin-slot lifecycle: a single reclaimer has been elected and is scanning
/// pins / freeing this version's chunks (ADR-0003a, formerly `SLOT_RETIRED`).
///
/// This is the **hazard flag** of the pin handshake: the reclaimer publishes
/// `SLOT_FREEING` with a `SeqCst` store *before* it scans the pin count, so a
/// racing reader either observes `SLOT_FREEING` (and backs off) or has already
/// published its pin (which the reclaimer then observes). See the state machine
/// on [`ArtifactHead`] and `shm-artifact`'s `artifact` module doc.
pub const SLOT_FREEING: u32 = 2;

/// Deprecated alias for [`SLOT_FREEING`] (the state was renamed in ADR-0003a to
/// name its role in the pin hazard handshake). Kept so external references do
/// not break; new code should use [`SLOT_FREEING`].
pub const SLOT_RETIRED: u32 = SLOT_FREEING;

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
/// | `state`    | `AtomicU32` | [`SLOT_FREE`] / [`SLOT_LIVE`] / [`SLOT_FREEING`] |
#[repr(C)]
pub struct PinSlot {
    /// The version this slot currently tracks; `0` when the slot is free.
    pub version: ShmU64,
    /// Packed [`PackedRef`](shm_core::PackedRef) to this version's manifest chunk.
    pub manifest: ShmU64,
    /// Count of live pins on this version (readers freezing it).
    pub pins: ShmU32,
    /// Slot lifecycle state; gates single-winner reclamation.
    pub state: ShmU32,
}

// 24-byte frozen ABI. Gated on `not(loom)`: each field is a
// `#[repr(transparent)]` substrate newtype (byte-identical to the bare atomic in
// production, loom's fat twin under `--cfg loom`); the loom build reconstructs the
// handshake in ordinary memory and never overlays these bytes on shm.
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<PinSlot>() == 24);
#[cfg(not(loom))]
const _: () = assert!(core::mem::align_of::<PinSlot>() == 8);

impl PinSlot {
    /// **Reader, hazard-handshake step 2 — publish the pin.** A `SeqCst`
    /// `fetch_add` on the pin count, ordered before [`accept_pin`](Self::accept_pin)'s
    /// revalidation. Publishing before revalidating is the reader half of the
    /// Dekker handshake against the reclaimer's `FREEING`-store-before-scan.
    #[inline]
    pub fn publish_pin(&self) {
        self.pins.fetch_add(1, SeqCst);
    }

    /// **Reader, hazard-handshake step 3 — revalidate.** After
    /// [`publish_pin`](Self::publish_pin), accept the pin iff the slot still tracks
    /// `{version == v, state == SLOT_LIVE}` (both `SeqCst` loads). A `false` means
    /// a reclaimer won the `SLOT_FREEING` election or the version moved, so the
    /// caller must undo its bump ([`unpin`](Self::unpin)) and retry.
    ///
    /// Opens with a `fence(SeqCst)`: the **StoreLoad barrier** ordering the
    /// preceding pin publish before these loads. This is the reader half of the
    /// Dekker handshake (paired with the reclaimer's fence in
    /// [`pin_scan`](Self::pin_scan)); see [`fence`] for why it is explicit.
    #[inline]
    pub fn accept_pin(&self, v: u64) -> bool {
        fence(SeqCst);
        self.state.load(SeqCst) == SLOT_LIVE && self.version.load(SeqCst) == v
    }

    /// Undo a speculative [`publish_pin`](Self::publish_pin) — a `SeqCst`
    /// `fetch_sub`, returning the previous pin count (so a caller can detect it
    /// released the last pin and drive reclamation).
    #[inline]
    pub fn unpin(&self) -> u32 {
        self.pins.fetch_sub(1, SeqCst)
    }

    /// Decrement the pin count **only if it is non-zero**, returning the
    /// previous value; `None` if it was already zero. The guard for release
    /// paths that cannot prove they hold a pin — a leaked-pin replay, a task
    /// binding release — where an unconditional `fetch_sub` on a count that
    /// another releaser already took to zero would free a version under a live
    /// reader or wrap to `u32::MAX`.
    pub fn try_unpin(&self) -> Option<u32> {
        let mut cur = self.pins.load(SeqCst);
        loop {
            if cur == 0 {
                return None;
            }
            match self.pins.compare_exchange(cur, cur - 1, SeqCst, SeqCst) {
                Ok(prev) => return Some(prev),
                Err(now) => cur = now,
            }
        }
    }

    /// **Reclaimer, step 1 — elect + publish the hazard flag.** One `SeqCst` CAS
    /// `SLOT_LIVE → SLOT_FREEING`; `true` means this caller is the sole elected
    /// reclaimer and has published `FREEING` **before** the pin scan. Ordered
    /// before [`pin_scan`](Self::pin_scan): the store-before-scan is the reclaimer
    /// half of the Dekker handshake.
    #[inline]
    pub fn elect_freeing(&self) -> bool {
        self.state
            .compare_exchange(SLOT_LIVE, SLOT_FREEING, SeqCst, Acquire)
            .is_ok()
    }

    /// **Reclaimer, step 2 — scan pins.** A `SeqCst` load of the pin count,
    /// ordered *after* [`elect_freeing`](Self::elect_freeing). `0` ⇒ safe to free;
    /// `> 0` ⇒ a reader is (or may be) live, so revert.
    ///
    /// Opens with a `fence(SeqCst)`: the **StoreLoad barrier** ordering the
    /// `SLOT_FREEING` store (published by `elect_freeing`) before this scan. This
    /// is the reclaimer half of the Dekker handshake (paired with the reader's
    /// fence in [`accept_pin`](Self::accept_pin)); see [`fence`] for why it is
    /// explicit.
    #[inline]
    pub fn pin_scan(&self) -> u32 {
        fence(SeqCst);
        self.pins.load(SeqCst)
    }

    /// **Reclaimer — revert.** `SeqCst` store `SLOT_FREEING → SLOT_LIVE` when the
    /// scan saw a live pin; only the elected reclaimer leaves `FREEING`, so this
    /// needs no CAS.
    #[inline]
    pub fn revert_live(&self) {
        self.state.store(SLOT_LIVE, SeqCst);
    }

    /// **Reclaimer — free.** Clear `version`/`manifest` and store `SLOT_FREE`
    /// (`SeqCst`), returning the slot to the claimable pool once the version's
    /// chunks have been released.
    #[inline]
    pub fn store_free(&self) {
        self.version.store(0, Ordering::Release);
        self.manifest.store(0, Ordering::Release);
        self.state.store(SLOT_FREE, SeqCst);
    }
}

/// The atomic RCU/MVCC control block for one artifact.
///
/// # Layout (frozen ABI)
///
/// | field           | type                        | meaning                       |
/// |-----------------|-----------------------------|-------------------------------|
/// | `magic`         | `AtomicU64`                 | [`HEAD_MAGIC`]                |
/// | `current`       | `AtomicU64`                 | current version (0 = none)    |
/// | `manifest_desc` | `AtomicU64`                 | packed [`PackedRef`](shm_core::PackedRef) to the current manifest |
/// | `write_lease`   | `AtomicU64`                 | fenced exclusive lease `pack_lease(owner, fence)` (owner 0 = free) |
/// | `schema_id`     | `AtomicU32`                 | interned Arrow schema id (informational) |
/// | `incarnation`   | `AtomicU32`                 | occupant in service (0 = none; ADR-0008 P0.1) |
/// | `pins`          | `[PinSlot; MAX_LIVE_VERSIONS]` | live-version pin table     |
///
/// `incarnation` (added at `SHMAHEA3`) fills the alignment padding that
/// previously sat between `schema_id` and `pins`, so the head's size — and the
/// keyed store's `head_stride` — did not change. It also lands on the same
/// cache line as `current`/`manifest_desc`/`write_lease`, which is deliberate:
/// every operation that re-validates it has just touched (or is about to touch)
/// those words, so the check rides a line that is already resident, and the
/// word itself is written only at commission/retire — never on the hot path.
///
/// # Fenced write lease (ADR-0003 item K)
///
/// Exclusive writes take `write_lease`, a single `AtomicU64` packing
/// `{owner: u32, fence: u32}` (see [`pack_lease`]). `owner == OWNER_NONE` means
/// free. A committer **acquires** by CAS-ing a free lease `{NONE, f}` to
/// `{me, f}` — it does **not** touch the fence, so its returned token is exactly
/// the fence `f` it acquired under. A **release** (clean drop, or a
/// coordinator-forced release after declaring the writer dead) CAS-es `{owner, f}`
/// to `{NONE, f+1}`: bumping the fence invalidates any token still outstanding, so
/// a slow/paused writer that was declared dead can no longer pass the commit-time
/// `{owner, fence}` check and its late commit is rejected ([`Error::Fenced`]).
///
/// # Live-version slot state machine (ADR-0003a)
///
/// ```text
///   FREE ──claim_slot (CAS FREE→LIVE)──▶ LIVE
///   LIVE ──reclaimer (CAS LIVE→FREEING)──▶ FREEING
///   FREEING ──pins==0: free chunks, store FREE──▶ FREE
///   FREEING ──pins>0: revert (store FREEING→LIVE)──▶ LIVE   (retry later)
/// ```
///
/// Only a committer performs `FREE→LIVE` (claiming); only the elected reclaimer
/// (the winner of the `LIVE→FREEING` CAS) leaves `FREEING` — to `FREE` when it
/// frees, or back to `LIVE` when it observed a live pin. Readers never mutate
/// `state`. Version numbers are monotonic and never reissued, so `version`
/// uniquely identifies a slot's occupant.
///
/// # RCU protocol summary
///
/// - **Install** (committer): claim a free [`PinSlot`] for `n+1`, then a single
///   `SeqCst` CAS of `current` from `n` to `n+1`, then a `Release` store of the
///   new `manifest_desc`. A reader validates `manifest.version == pinned`, so
///   the brief two-word window is never observed torn.
/// - **Pin** (reader): `SeqCst` `fetch_add` on the current version's slot
///   (this *publishes* the pin), then re-validate the slot is still
///   `{version == n, state == LIVE}` with `SeqCst` loads; if it flipped to
///   `SLOT_FREEING` or the version moved, undo and retry. Only then load
///   `manifest_desc` and deref.
/// - **Reclaim**: a version's chunks are freed only once its slot's `pins == 0`
///   **and** it is not `current`. The reclaimer CAS-elects itself
///   `SLOT_LIVE → SLOT_FREEING` and stores `FREEING` **before** the `SeqCst`
///   pin scan. The `FREEING`-store-before-scan paired with the reader's
///   publish-before-revalidate is the Dekker/hazard handshake: either the
///   reclaimer observes `pins > 0`, or the reader observes `FREEING` and
///   retries — so a live reader never dereferences a freed (recycled) chunk.
#[repr(C)]
pub struct ArtifactHead {
    /// Must equal [`HEAD_MAGIC`] once initialised.
    pub magic: ShmU64,
    /// The current (latest installed) version number; [`NO_VERSION`] until the
    /// first commit.
    pub current: ShmU64,
    /// Packed [`PackedRef`](shm_core::PackedRef) to the current version's
    /// manifest chunk.
    pub manifest_desc: ShmU64,
    /// The **fenced** exclusive write lease: `pack_lease(owner, fence)`. `owner ==
    /// OWNER_NONE` (0) means free; `fence` is a monotonic generation bumped on
    /// every release. See the type-level docs for the acquire/release protocol.
    pub write_lease: ShmU64,
    /// The interned Arrow schema id of the artifact's data (set on first commit;
    /// informational — the authoritative id is in each manifest).
    pub schema_id: ShmU32,
    /// Which **occupant** of this head region is in service (ADR-0008 P0.1).
    ///
    /// [`NO_INCARNATION`] means the region is not in service. A handle adopts
    /// this value when it attaches and re-validates it on every operation, which
    /// is what makes a head region safe to recycle: see
    /// [`is_incarnation`](Self::is_incarnation).
    pub incarnation: ShmU32,
    /// Fixed table of live-version pin counters for reclamation.
    pub pins: [PinSlot; MAX_LIVE_VERSIONS],
}

// Frozen ABI: the `SHMAHEA3` incarnation word occupies what was padding at
// `SHMAHEA2`, so the head's size (and the keyed store's 64-byte-rounded
// `head_stride`) is unchanged. Gated on `not(loom)` like the `PinSlot` asserts.
#[cfg(not(loom))]
const _: () = assert!(
    core::mem::size_of::<ArtifactHead>()
        == 40 + MAX_LIVE_VERSIONS * core::mem::size_of::<PinSlot>()
);

impl ArtifactHead {
    /// The number of payload bytes an [`ArtifactHead`] occupies. A management
    /// segment's payload must be at least this large.
    pub const fn region_bytes() -> usize {
        core::mem::size_of::<ArtifactHead>()
    }

    /// Build a fresh head **by value**: magic set, no version, no writer, every
    /// pin slot free.
    ///
    /// Factored out of [`init_at`](Self::init_at) so a loom harness (ADR-0004
    /// stage L) can construct a real `ArtifactHead` in ordinary heap memory and
    /// share it across `loom::thread`s, exercising the *same* RCU install / pin /
    /// retire code the production overlay runs.
    pub fn fresh() -> ArtifactHead {
        ArtifactHead {
            magic: ShmU64::new(HEAD_MAGIC),
            current: ShmU64::new(NO_VERSION),
            manifest_desc: ShmU64::new(0),
            write_lease: ShmU64::new(pack_lease(OWNER_NONE, 0)),
            schema_id: ShmU32::new(0),
            incarnation: ShmU32::new(NO_INCARNATION),
            pins: core::array::from_fn(|_| PinSlot {
                version: ShmU64::new(0),
                manifest: ShmU64::new(0),
                pins: ShmU32::new(0),
                state: ShmU32::new(SLOT_FREE),
            }),
        }
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
            ptr.write(ArtifactHead::fresh());
        }
    }

    /// Validate that a region already holds an initialised head.
    #[inline]
    pub fn check_magic(&self) -> bool {
        self.magic.load(Ordering::Acquire) == HEAD_MAGIC
    }

    /// The occupant currently in service ([`NO_INCARNATION`] if none).
    #[inline]
    pub fn incarnation(&self) -> u32 {
        self.incarnation.load(Ordering::Acquire)
    }

    /// **Reader/writer half of the recycle handshake.** Is `expected` still the
    /// occupant in service?
    ///
    /// The load is `SeqCst` because it is the second half of a Dekker pairing
    /// with [`retire`](Self::retire): an operation *registers itself first*
    /// (publishes a pin, claims a version slot, takes the write lease) and only
    /// then calls this. A sweep reclaiming the region swaps in
    /// [`NO_INCARNATION`] `SeqCst` and only then scans for registrations. So
    /// for any operation and any sweep, either the sweep observes the
    /// registration and aborts, or the operation observes the retirement and
    /// backs out — never both missing.
    ///
    /// The pairing needs all four accesses in the `SeqCst` total order, which
    /// is why the registering *writes* are `SeqCst` RMWs
    /// ([`PinSlot::publish_pin`], [`claim_slot`](Self::claim_slot),
    /// [`acquire_write_lease`](Self::acquire_write_lease)) and the sweep's
    /// quiescence loads ([`is_quiescent`](Self::is_quiescent)) are `SeqCst`
    /// too: an `AcqRel` registration followed by this `SeqCst` load is *not*
    /// enough — the model lets the sweep's scan miss the registration while
    /// the registration's thread misses the retirement.
    ///
    /// As with the pin hazard handshake, each side additionally carries an
    /// explicit `fence(SeqCst)` between its store and its load — the
    /// StoreLoad barrier loom needs to model the total order (see [`fence`]).
    /// On the pin path that fence is the one already inside
    /// [`PinSlot::accept_pin`], so `pin_inner` calls this method bare; every
    /// other operation revalidates through
    /// [`revalidate_incarnation`](Self::revalidate_incarnation), which fences
    /// first, and the sweep scans through [`is_quiescent`](Self::is_quiescent),
    /// which does the same.
    #[inline]
    pub fn is_incarnation(&self, expected: u32) -> bool {
        self.incarnation.load(SeqCst) == expected
    }

    /// **Operation half of the recycle handshake, fenced.** Like
    /// [`is_incarnation`](Self::is_incarnation), but opens with a
    /// `fence(SeqCst)` — the StoreLoad barrier between the caller's
    /// *registration* RMW (a slot claim, a lease acquire) and this load,
    /// mirroring [`PinSlot::accept_pin`]'s fence on the pin path (see
    /// [`fence`] for why it is explicit). Use this at every
    /// register-then-revalidate site whose registration does not already sit
    /// behind such a fence.
    #[inline]
    pub fn revalidate_incarnation(&self, expected: u32) -> bool {
        fence(SeqCst);
        self.is_incarnation(expected)
    }

    /// **Sweep half of the recycle handshake.** Take the region out of service,
    /// returning the incarnation that was in it.
    ///
    /// `SeqCst`, and ordered *before* the sweep's quiescence scan — see
    /// [`is_incarnation`](Self::is_incarnation).
    #[inline]
    pub fn retire(&self) -> u32 {
        self.incarnation.swap(NO_INCARNATION, SeqCst)
    }

    /// **Sweep half of the recycle handshake, step 2 — the quiescence scan.**
    /// `true` iff the head holds nothing a reclaimer would destroy: no current
    /// version, every pin slot `SLOT_FREE` with a zero pin count, and the
    /// write lease unowned.
    ///
    /// Must be called *after* [`retire`](Self::retire). Opens with a
    /// `fence(SeqCst)`: the StoreLoad barrier between the retire swap and
    /// these loads (the sweep's mirror of [`revalidate_incarnation`](Self::revalidate_incarnation)
    /// — see [`fence`]). Every load is `SeqCst`, each pairing against one
    /// registration RMW: `publish_pin` for the pin counts, `claim_slot` for
    /// the slot states, `acquire_write_lease` for the lease — so none of them
    /// may be weakened (see [`is_incarnation`](Self::is_incarnation) for the
    /// Dekker argument).
    pub fn is_quiescent(&self) -> bool {
        fence(SeqCst);
        if self.current.load(SeqCst) != NO_VERSION {
            return false;
        }
        if lease_owner(self.write_lease.load(SeqCst)) != OWNER_NONE {
            return false;
        }
        self.pins
            .iter()
            .all(|s| s.state.load(SeqCst) == SLOT_FREE && s.pin_scan() == 0)
    }

    /// Put the region into service as `incarnation`. Called last by a create, so
    /// it is the release edge publishing the freshly initialised head.
    #[inline]
    pub fn commission(&self, incarnation: u32) {
        self.incarnation.store(incarnation, Ordering::Release);
    }

    /// Find the [`SLOT_LIVE`] slot tracking `version` **whose manifest is
    /// `manifest_bits`** — the endorsed slot. Two optimistic committers can
    /// transiently claim duplicate slots for one version; only the one the
    /// install CAS endorsed (its manifest was `head.manifest_desc`) owns that
    /// version's references. Retiring "the first slot with this version" could
    /// retire a stale duplicate and strand the real one (ADR-0013 review F3).
    pub fn find_slot_with_manifest(&self, version: u64, manifest_bits: u64) -> Option<usize> {
        for (i, slot) in self.pins.iter().enumerate() {
            if slot.state.load(Ordering::Acquire) == SLOT_LIVE
                && slot.version.load(Ordering::Acquire) == version
                && slot.manifest.load(Ordering::Acquire) == manifest_bits
            {
                return Some(i);
            }
        }
        None
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
    /// `manifest`), returning its index, or `None` if the table is full.
    ///
    /// The slot's `version` is published *after* the `SLOT_FREE → SLOT_LIVE` CAS
    /// but the slot only becomes reader-findable once `version` is stored, so a
    /// reader scanning for `version` never matches a half-claimed slot (a free
    /// slot's `version` is `0`, which no real version equals).
    ///
    /// **The pin counter is deliberately *not* reset here.** A reader's pin is a
    /// balanced pair — every speculative `pins.fetch_add` is matched by a
    /// `fetch_sub` (on accept-then-drop, or on the handshake back-off). A reader
    /// that bumped this slot just before it was freed still owes its `fetch_sub`;
    /// blindly storing `0` on reuse would drop that bump and let the pending
    /// decrement underflow the counter. Leaving the counter alone keeps it a
    /// globally balanced, never-negative sum across reuse — a transient stale
    /// bump merely defers a retire (self-healed when that reader's `fetch_sub`
    /// re-runs it), and a freshly initialised slot already reads `0`.
    ///
    /// The winning CAS is `SeqCst` because it doubles as the **committer's
    /// registration** in the ADR-0008 recycle handshake: the commit re-validates
    /// the incarnation *after* this claim, and the Dekker pairing against the
    /// sweep's `retire`-then-scan only holds if the registering write itself
    /// participates in the `SeqCst` total order (an `AcqRel` claim and a `SeqCst`
    /// incarnation load can otherwise both miss the sweep). Same cost on
    /// x86-64/aarch64 as `AcqRel`.
    pub fn claim_slot(&self, version: u64, manifest: u64) -> Option<usize> {
        for (i, slot) in self.pins.iter().enumerate() {
            if slot
                .state
                .compare_exchange(SLOT_FREE, SLOT_LIVE, SeqCst, Ordering::Acquire)
                .is_ok()
            {
                slot.manifest.store(manifest, Ordering::Release);
                slot.version.store(version, Ordering::Release);
                return Some(i);
            }
        }
        None
    }

    // ---- Fenced write lease (ADR-0003 item K) ----

    /// Try to acquire the exclusive write lease for `owner`, returning the
    /// **fence token** it was acquired under, or `None` if a live owner holds it
    /// (the caller surfaces that as [`Error::WriteLocked`](crate::Error::WriteLocked)).
    ///
    /// A single CAS transitions a free lease `{OWNER_NONE, fence}` to
    /// `{owner, fence}` **without changing the fence** — so the returned token is
    /// exactly the fence a matching release will later advance past. The CAS is
    /// `SeqCst` on success: besides publishing the acquisition (and
    /// synchronising with the prior releaser's `Release`), it is the **writer's
    /// registration** in the ADR-0008 recycle handshake — `open_exclusive*`
    /// re-validates the incarnation *after* it, and the Dekker pairing against
    /// the sweep's `retire`-then-scan needs the registering write in the
    /// `SeqCst` total order (same cost as `AcqRel` on x86-64/aarch64). Failure
    /// is `Acquire` (to re-read a fresh value). A spurious CAS failure with the
    /// lease still free (a concurrent release bumped the fence) simply retries
    /// under the new fence; a failure with a live owner returns `None`.
    pub fn acquire_write_lease(&self, owner: u32) -> Option<u32> {
        loop {
            let cur = self.write_lease.load(Ordering::Acquire);
            if lease_owner(cur) != OWNER_NONE {
                return None; // held by a live owner: WriteLocked
            }
            let fence = lease_fence(cur);
            match self.write_lease.compare_exchange(
                cur,
                pack_lease(owner, fence),
                SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(fence),
                Err(_) => continue, // raced (fence bumped / owner taken): re-read
            }
        }
    }

    /// Whether the lease is currently held exactly as `{owner, fence}` (an
    /// `Acquire` load). A committer uses this to validate its token has not been
    /// fenced out before installing a version.
    #[inline]
    pub fn lease_held_by(&self, owner: u32, fence: u32) -> bool {
        self.write_lease.load(Ordering::Acquire) == pack_lease(owner, fence)
    }

    /// Clean release of a lease held as `{owner, token}`: CAS to
    /// `{OWNER_NONE, token + 1}`, bumping the fence so any still-outstanding token
    /// is invalidated. Returns `true` iff this holder still owned the lease under
    /// `token` (a `false` means it was already released/fenced — a harmless
    /// no-op). `AcqRel` publishes the release to the next acquirer.
    #[inline]
    pub fn release_write_lease(&self, owner: u32, token: u32) -> bool {
        self.write_lease
            .compare_exchange(
                pack_lease(owner, token),
                pack_lease(OWNER_NONE, token.wrapping_add(1)),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// **Crash release** (coordinator, on writer death): force the lease free and
    /// bump the fence, whatever owner currently holds it. Returns `true` iff a
    /// lease was actually held (and is now released + fenced); `false` if it was
    /// already free.
    ///
    /// Read the current value (`Acquire`); if an owner holds it, CAS to
    /// `{OWNER_NONE, fence + 1}` (`AcqRel`). The fence bump is what makes the dead
    /// writer's outstanding token stale, so its late commit fails the
    /// [`lease_held_by`](Self::lease_held_by) check. Retries only on a genuine
    /// concurrent change to a still-held lease.
    pub fn force_release_write_lease(&self) -> bool {
        loop {
            let cur = self.write_lease.load(Ordering::Acquire);
            if lease_owner(cur) == OWNER_NONE {
                return false; // already free (clean release beat us here)
            }
            let fence = lease_fence(cur);
            match self.write_lease.compare_exchange(
                cur,
                pack_lease(OWNER_NONE, fence.wrapping_add(1)),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // owner changed under us: re-read and retry
            }
        }
    }

    /// The current lease owner actor id (`OWNER_NONE` ⇒ free). Observability /
    /// test helper — e.g. a coordinator proving a dead writer's lease was
    /// force-released.
    #[inline]
    pub fn write_lease_owner(&self) -> u32 {
        lease_owner(self.write_lease.load(Ordering::Acquire))
    }
}
