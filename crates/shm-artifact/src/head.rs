//! The [`ArtifactHead`] management region: the atomic RCU/MVCC control block.
//!
//! One `ArtifactHead` lives at the base of an artifact's *management* segment
//! payload. Every field is mutated concurrently by readers, committers, and the
//! reclaimer, so — like [`shm_core::ChunkCtrl`] and [`shm_ring::RingHeader`] —
//! it is built entirely from atomics and placed into shared memory **by hand**
//! (it is deliberately not `SharedPod`).

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Head magic: little-endian bytes of `b"SHMAHEA2"`.
///
/// Bumped from `SHMAHEAD` for the v0.3 / ADR-0003 item K layout change: the plain
/// `writer_owner: AtomicU32` became a **fenced write lease** packed into one
/// [`AtomicU64`](core::sync::atomic::AtomicU64) (`owner << 32 | fence`), which
/// grows the region, so a v0.2 head is rejected by the magic check.
pub const HEAD_MAGIC: u64 = u64::from_le_bytes(*b"SHMAHEA2");

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
pub const MAX_LIVE_VERSIONS: usize = 64;

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
/// | `write_lease`   | `AtomicU64`                 | fenced exclusive lease `pack_lease(owner, fence)` (owner 0 = free) |
/// | `schema_id`     | `AtomicU32`                 | interned Arrow schema id (informational) |
/// | `pins`          | `[PinSlot; MAX_LIVE_VERSIONS]` | live-version pin table     |
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
    pub magic: AtomicU64,
    /// The current (latest installed) version number; [`NO_VERSION`] until the
    /// first commit.
    pub current: AtomicU64,
    /// Packed [`PackedRef`](shm_core::PackedRef) to the current version's
    /// manifest chunk.
    pub manifest_desc: AtomicU64,
    /// The **fenced** exclusive write lease: `pack_lease(owner, fence)`. `owner ==
    /// OWNER_NONE` (0) means free; `fence` is a monotonic generation bumped on
    /// every release. See the type-level docs for the acquire/release protocol.
    pub write_lease: AtomicU64,
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
                write_lease: AtomicU64::new(pack_lease(OWNER_NONE, 0)),
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
    pub fn claim_slot(&self, version: u64, manifest: u64) -> Option<usize> {
        for (i, slot) in self.pins.iter().enumerate() {
            if slot
                .state
                .compare_exchange(SLOT_FREE, SLOT_LIVE, Ordering::AcqRel, Ordering::Acquire)
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
    /// exactly the fence a matching release will later advance past. The CAS uses
    /// `AcqRel` on success (publishing the acquisition and synchronising with the
    /// prior releaser's `Release`) and `Acquire` on failure (to re-read a fresh
    /// value). A spurious CAS failure with the lease still free (a concurrent
    /// release bumped the fence) simply retries under the new fence; a failure
    /// with a live owner returns `None`.
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
                Ordering::AcqRel,
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
