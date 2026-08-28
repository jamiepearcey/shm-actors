//! The shared-memory ring ABI and the low-level [`Ring`] handle.
//!
//! The ring is a single-producer, multiple-consumer (SPMC) **broadcast** buffer:
//! every subscriber sees every published message (until it is lapped). It is a
//! bounded array of [`Slot`]s guarded by monotonically increasing sequence
//! numbers (an LMAX-disruptor-style broadcast), so a publish is wait-free and
//! never blocks on a slow consumer.
//!
//! # Layout (frozen ABI)
//!
//! The ring occupies a caller-provided region (e.g. a `shm-core` segment
//! payload). The [`RingHeader`] sits at the region base; the [`Slot`] array
//! begins at [`slots_offset`] (64-byte aligned) and runs for `capacity` slots.
//! All cross-process mutation goes through the atomics in the header and slots,
//! so — like [`shm_core::ChunkCtrl`] — these types are placed by hand and are
//! deliberately not `SharedPod`. The embedded [`ChunkDesc`] payload is written
//! before, and read after, the guarding `seq` release/acquire, so it is never
//! observed torn.

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use shm_core::ChunkDesc;

use crate::error::{Error, Result};

/// Ring magic: little-endian bytes of `b"SHMRING1"`.
pub const RING_MAGIC: u64 = u64::from_le_bytes(*b"SHMRING1");

/// Number of reliable-subscriber cursor slots in the header.
pub const MAX_RELIABLE: usize = 8;

/// Sentinel meaning "reliable slot unused".
pub const RELIABLE_UNUSED: u64 = u64::MAX;

/// Fixed header at the base of a ring region.
///
/// # Layout (frozen ABI — 96 bytes)
///
/// | field       | type                    | meaning                          |
/// |-------------|-------------------------|----------------------------------|
/// | `magic`     | `u64`                   | must equal [`RING_MAGIC`]         |
/// | `capacity`  | `u32`                   | slot count (power of two)         |
/// | `mask`      | `u32`                   | `capacity - 1`                    |
/// | `head`      | `AtomicU64`             | next sequence to publish (= count)|
/// | `waiters`   | `AtomicU32`             | parked subscriber count           |
/// | `doorbell_seq` | `AtomicU32`          | reserved for v0.4 futex doorbell  |
/// | `reliable`  | `[AtomicU64; MAX_RELIABLE]` | reliable subscriber cursors  |
#[repr(C)]
pub struct RingHeader {
    /// Must equal [`RING_MAGIC`].
    pub magic: u64,
    /// Slot count; always a power of two.
    pub capacity: u32,
    /// `capacity - 1`, used to map a sequence to a slot index.
    pub mask: u32,
    /// Next sequence number to publish. Also the total count published, so
    /// `head - cursor` is exactly how far a subscriber is behind.
    pub head: AtomicU64,
    /// Number of subscribers currently parked (a publish wakes them iff `> 0`).
    pub waiters: AtomicU32,
    /// **Reserved for v0.4's futex-based doorbell (ADR-0003).** Unused in v0.3:
    /// initialized to `0` and never read or written on the hot path. It consumes
    /// the former `_pad` word, so the header size and slot offsets are unchanged.
    pub doorbell_seq: AtomicU32,
    /// Fixed table of reliable-subscriber cursors; [`RELIABLE_UNUSED`] == free.
    pub reliable: [AtomicU64; MAX_RELIABLE],
}

const _: () = assert!(core::mem::size_of::<RingHeader>() == 96);
const _: () = assert!(core::mem::align_of::<RingHeader>() == 8);

/// One broadcast slot: a sequence guard plus the 24-byte descriptor payload.
///
/// # Layout (frozen ABI — 32 bytes)
///
/// | field  | type        | meaning                                          |
/// |--------|-------------|--------------------------------------------------|
/// | `seq`  | `AtomicU64` | sequence currently occupying this slot           |
/// | `desc` | `ChunkDesc` | the 24-byte published descriptor                 |
#[repr(C)]
pub struct Slot {
    /// The sequence number this slot currently holds; a subscriber at cursor
    /// `c` reads slot `c & mask` and accepts it only when `seq == c`.
    pub seq: AtomicU64,
    /// The published descriptor payload (guarded by `seq`).
    pub desc: ChunkDesc,
}

const _: () = assert!(core::mem::size_of::<Slot>() == 32);
const _: () = assert!(core::mem::align_of::<Slot>() == 8);

/// Round `x` up to a multiple of `a` (a power of two).
#[inline]
const fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

/// Byte offset of the slot array within a ring region (64-byte aligned).
#[inline]
pub const fn slots_offset() -> usize {
    align_up(core::mem::size_of::<RingHeader>(), 64)
}

/// Total bytes a ring of `capacity` slots needs.
#[inline]
pub const fn required_bytes(capacity: u32) -> usize {
    slots_offset() + capacity as usize * core::mem::size_of::<Slot>()
}

/// A process-local handle to a broadcast ring placed in shared memory.
///
/// Cheap to clone (it is just cached pointers + geometry); every clone refers to
/// the same shared ring. Cross-process safety comes from the atomics in the
/// mapped [`RingHeader`]/[`Slot`]s.
#[derive(Clone)]
pub struct Ring {
    header: *mut RingHeader,
    slots: *mut Slot,
    capacity: usize,
    mask: u64,
}

// SAFETY: the handle only holds pointers into a `MAP_SHARED` region; all shared
// mutation is via atomics with explicit ordering, and the `desc` payload is
// guarded by the per-slot `seq` seqlock. The handle is safe to send/share.
unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

impl Ring {
    /// Initialize a fresh ring of `capacity` slots into the region at `base`.
    ///
    /// `base` must be 8-byte aligned and `region_len` must be at least
    /// [`required_bytes`]. `capacity` must be a non-zero power of two.
    ///
    /// # Safety
    ///
    /// `base` must point at `region_len` writable bytes that stay mapped for the
    /// lifetime of every [`Ring`] handle derived from it, and no other party may
    /// concurrently initialize the same region.
    pub unsafe fn init(base: *mut u8, region_len: usize, capacity: u32) -> Result<Ring> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(Error::BadCapacity(capacity));
        }
        if !(base as usize).is_multiple_of(8) {
            return Err(Error::Misaligned);
        }
        let need = required_bytes(capacity);
        if region_len < need {
            return Err(Error::RegionTooSmall {
                need,
                have: region_len,
            });
        }

        // SAFETY: `base` is 8-aligned and `region_len >= need`, so the header and
        // every slot offset below stays within the writable region.
        unsafe {
            let header = base.cast::<RingHeader>();
            header.write(RingHeader {
                magic: RING_MAGIC,
                capacity,
                mask: capacity - 1,
                head: AtomicU64::new(0),
                waiters: AtomicU32::new(0),
                doorbell_seq: AtomicU32::new(0),
                reliable: core::array::from_fn(|_| AtomicU64::new(RELIABLE_UNUSED)),
            });
            let slots = base.add(slots_offset()).cast::<Slot>();
            for i in 0..capacity as usize {
                slots.add(i).write(Slot {
                    // Distinct from any real sequence a subscriber will request
                    // while `cursor < head`; empty is detected via the head gate,
                    // so this initial value is never accepted as a message.
                    seq: AtomicU64::new(RELIABLE_UNUSED),
                    desc: ChunkDesc::ZERO,
                });
            }
            Ok(Ring {
                header,
                slots,
                capacity: capacity as usize,
                mask: (capacity - 1) as u64,
            })
        }
    }

    /// Attach a handle onto an already-initialized ring region.
    ///
    /// # Safety
    ///
    /// `base` must point at a region previously initialized by [`Ring::init`]
    /// that stays mapped for the lifetime of the returned handle.
    pub unsafe fn attach(base: *mut u8, region_len: usize) -> Result<Ring> {
        if !(base as usize).is_multiple_of(8) {
            return Err(Error::Misaligned);
        }
        if region_len < core::mem::size_of::<RingHeader>() {
            return Err(Error::RegionTooSmall {
                need: core::mem::size_of::<RingHeader>(),
                have: region_len,
            });
        }
        // SAFETY: header lives at the region base; the region is at least header-
        // sized. Reading the POD scalar fields is in-bounds.
        let header = base.cast::<RingHeader>();
        let (magic, capacity, mask) = unsafe {
            let h = &*header;
            (h.magic, h.capacity, h.mask)
        };
        if magic != RING_MAGIC {
            return Err(Error::BadMagic);
        }
        let need = required_bytes(capacity);
        if region_len < need {
            return Err(Error::RegionTooSmall {
                need,
                have: region_len,
            });
        }
        // SAFETY: `capacity` was validated at init; the slot array is in-bounds.
        let slots = unsafe { base.add(slots_offset()).cast::<Slot>() };
        Ok(Ring {
            header,
            slots,
            capacity: capacity as usize,
            mask: mask as u64,
        })
    }

    /// The number of slots (a power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The current head — the number of messages published so far.
    #[inline]
    pub fn head(&self) -> u64 {
        self.header().head.load(Ordering::Acquire)
    }

    #[inline]
    fn header(&self) -> &RingHeader {
        // SAFETY: `header` points at the mapped region for this handle's life.
        unsafe { &*self.header }
    }

    #[inline]
    fn slot_ptr(&self, seq: u64) -> *mut Slot {
        let idx = (seq & self.mask) as usize;
        // SAFETY: `idx < capacity`; the slot array has `capacity` entries.
        unsafe { self.slots.add(idx) }
    }

    /// Publish `desc` at the next sequence (single-producer only).
    ///
    /// Wait-free: writes the descriptor, releases the slot's sequence, then
    /// advances `head`. Returns `true` if any subscriber is parked (the caller's
    /// notifier should then wake them).
    ///
    /// # Correctness
    ///
    /// The single producer owns `head`, so the load is uncontended. The slot's
    /// `desc` is written before its `seq` is `Release`-stored, and `head` is
    /// advanced last, so a consumer that observes `head > seq` (Acquire) also
    /// observes the matching descriptor.
    #[must_use = "the returned flag says whether to wake parked subscribers"]
    pub fn publish(&self, desc: ChunkDesc) -> bool {
        let header = self.header();
        let seq = header.head.load(Ordering::Relaxed); // sole writer
        let slot_ptr = self.slot_ptr(seq);
        // SAFETY: single producer owns this slot until the `seq` release below;
        // no consumer reads `desc` for this `seq` until it sees `seq` published.
        // The write pointer is derived from the raw slot pointer (not a shared
        // reference), so it carries write provenance.
        unsafe {
            core::ptr::addr_of_mut!((*slot_ptr).desc).write(desc);
        }
        // SAFETY: `slot_ptr` is in-bounds; `seq` is an atomic accessed by ref.
        let slot = unsafe { &*slot_ptr };
        slot.seq.store(seq, Ordering::Release);
        header.head.store(seq.wrapping_add(1), Ordering::Release);
        header.waiters.load(Ordering::Acquire) > 0
    }

    /// Try to read the message at `cursor`, classifying the outcome.
    ///
    /// Returns the raw [`ReadOutcome`]; the [`crate::Subscriber`] wraps this with
    /// cursor bookkeeping. `cursor` is the next sequence the caller wants.
    pub(crate) fn read_at(&self, cursor: u64) -> ReadOutcome {
        let head = self.header().head.load(Ordering::Acquire);
        // Head gate: nothing at or beyond `head` has been published, so a caller
        // that has caught up (including the initial empty ring) blocks here.
        if cursor >= head {
            return ReadOutcome::Empty;
        }
        // Lap check: if more than `capacity` messages have been published since
        // `cursor`, the slot for `cursor` has been overwritten. The oldest still-
        // live message is `head - capacity`.
        if head - cursor > self.capacity as u64 {
            let new_cursor = head - self.capacity as u64;
            return ReadOutcome::Lagged {
                skipped: new_cursor - cursor,
                new_cursor,
            };
        }
        let slot_ptr = self.slot_ptr(cursor);
        // SAFETY: `slot_ptr` is in-bounds; `seq` is an atomic, sound to borrow.
        let seq = unsafe { &(*slot_ptr).seq };
        if seq.load(Ordering::Acquire) != cursor {
            // A lap raced us between the head load and the slot read; re-derive
            // as a lag against a fresh head.
            let head = self.header().head.load(Ordering::Acquire);
            let new_cursor = head - self.capacity as u64;
            return ReadOutcome::Lagged {
                skipped: new_cursor.saturating_sub(cursor),
                new_cursor,
            };
        }
        // SAFETY: `seq == cursor` and `head - cursor <= capacity`, so this slot
        // is not being overwritten; the guarded `desc` is a fully-published POD.
        // Read it through the raw pointer (no reference spanning `desc` is held
        // while a producer could write it).
        let desc = unsafe { core::ptr::addr_of!((*slot_ptr).desc).read() };
        // Guard against an overwrite that landed during the read.
        if seq.load(Ordering::Acquire) != cursor {
            let head = self.header().head.load(Ordering::Acquire);
            let new_cursor = head - self.capacity as u64;
            return ReadOutcome::Lagged {
                skipped: new_cursor.saturating_sub(cursor),
                new_cursor,
            };
        }
        ReadOutcome::Sample(desc)
    }

    /// The ring's ABI-reserved futex doorbell word ([`RingHeader::doorbell_seq`],
    /// reserved by ADR-0003, activated by ADR-0011's Linux fast paths).
    ///
    /// A [`FutexNotifier`](crate::hooks) bumps it and futex-wakes; a
    /// [`FutexParker`](crate::hooks) futex-waits on it. The accessor is
    /// available on every platform (the word is plain shared memory); only the
    /// futex hooks that operate on it are Linux-gated. The returned reference
    /// borrows the mapped header, so it lives as long as this handle.
    pub fn doorbell_word(&self) -> &AtomicU32 {
        &self.header().doorbell_seq
    }

    /// Register interest as a parked subscriber (increment `waiters`).
    pub(crate) fn add_waiter(&self) {
        self.header().waiters.fetch_add(1, Ordering::AcqRel);
    }

    /// Withdraw from the parked set (decrement `waiters`).
    pub(crate) fn remove_waiter(&self) {
        self.header().waiters.fetch_sub(1, Ordering::AcqRel);
    }

    // ---- Reliable-subscriber cursor table ----

    /// Claim a reliable cursor slot, seeded to `cursor`. Returns its index.
    pub(crate) fn register_reliable(&self, cursor: u64) -> Result<usize> {
        for i in 0..MAX_RELIABLE {
            let cell = &self.header().reliable[i];
            if cell
                .compare_exchange(RELIABLE_UNUSED, cursor, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(i);
            }
        }
        Err(Error::ReliableTableFull(MAX_RELIABLE))
    }

    /// Publish a reliable subscriber's advanced cursor.
    pub(crate) fn update_reliable(&self, idx: usize, cursor: u64) {
        debug_assert!(idx < MAX_RELIABLE);
        self.header().reliable[idx].store(cursor, Ordering::Release);
    }

    /// Release a reliable cursor slot back to the free pool.
    pub(crate) fn release_reliable(&self, idx: usize) {
        debug_assert!(idx < MAX_RELIABLE);
        self.header().reliable[idx].store(RELIABLE_UNUSED, Ordering::Release);
    }

    /// The smallest cursor across all registered reliable subscribers, or
    /// `None` if there are none.
    pub fn min_reliable_cursor(&self) -> Option<u64> {
        let mut min: Option<u64> = None;
        for i in 0..MAX_RELIABLE {
            let v = self.header().reliable[i].load(Ordering::Acquire);
            if v != RELIABLE_UNUSED {
                min = Some(min.map_or(v, |m| m.min(v)));
            }
        }
        min
    }

    /// Whether any reliable subscriber has fallen more than `capacity` behind.
    ///
    /// The ring itself never blocks; a runtime consults this to decide whether
    /// to withhold a fresh loan from the producer (backpressure).
    pub fn would_backpressure(&self) -> bool {
        match self.min_reliable_cursor() {
            Some(min) => self.head().saturating_sub(min) > self.capacity as u64,
            None => false,
        }
    }
}

/// The classified result of a raw [`Ring::read_at`].
pub(crate) enum ReadOutcome {
    /// A message is available.
    Sample(ChunkDesc),
    /// The caller was lapped; advance the cursor and report the skip count.
    Lagged { skipped: u64, new_cursor: u64 },
    /// Nothing new to read (would block).
    Empty,
}
