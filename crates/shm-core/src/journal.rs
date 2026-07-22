//! Per-actor borrow journal: a fixed table of pinned [`ChunkDesc`]s.
//!
//! Each actor owns a journal (typically in its own management segment). When the
//! actor pins a chunk it [`record`](BorrowJournal::record)s the descriptor; when
//! it drops the pin it [`release`](BorrowJournal::release)s the slot. If the
//! actor dies, the coordinator [`replay`](BorrowJournal::replay)s the journal to
//! find every still-pinned chunk and release it.
//!
//! # Design (ADR-0001 §5)
//!
//! A fixed table of `N` slots plus an occupancy bitmap, all POD, in shared
//! memory. `record`/`release` are O(1) (a bitmap bit + one slot write); `replay`
//! is O(N). The bounded pin count is a **feature**: `record` returns
//! [`Error::JournalFull`] as natural backpressure and bounds crash-reclamation
//! work. An append log was rejected (unbounded shm growth, O(n) replay).

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::desc::ChunkDesc;
use crate::error::{Error, Result};
use crate::segment::Segment;

/// Magic for a journal header: little-endian `b"SHMJRNL1"`.
pub const JOURNAL_MAGIC: u64 = u64::from_le_bytes(*b"SHMJRNL1");

/// Default journal capacity (pins) when unspecified.
pub const DEFAULT_CAPACITY: usize = 1024;

/// On-segment journal header. `#[repr(C)]`, lives at the payload base.
#[repr(C)]
struct JournalHeader {
    magic: u64,
    capacity: u32,
    /// Hint for the next free-slot search (advisory; correctness never depends
    /// on it).
    hint: AtomicU32,
}

/// Bytes needed to lay out a journal of `capacity` pins.
fn layout(capacity: usize) -> (usize, usize, usize, usize) {
    let words = capacity.div_ceil(64);
    let header = core::mem::size_of::<JournalHeader>();
    let bitmap_off = align_up(header, 8);
    let slots_off = align_up(bitmap_off + words * 8, core::mem::align_of::<ChunkDesc>());
    let total = slots_off + capacity * core::mem::size_of::<ChunkDesc>();
    (bitmap_off, slots_off, words, total)
}

#[inline]
const fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

/// A handle to a borrow journal laid out inside a [`Segment`].
pub struct BorrowJournal<'s> {
    #[allow(dead_code)]
    segment: &'s Segment,
    header: *mut JournalHeader,
    bitmap: *mut AtomicU64,
    slots: *mut ChunkDesc,
    words: usize,
    capacity: usize,
}

impl<'s> BorrowJournal<'s> {
    /// Create and zero-initialize a journal of `capacity` pins in `segment`.
    pub fn create(segment: &'s Segment, capacity: usize) -> Result<BorrowJournal<'s>> {
        if capacity == 0 {
            return Err(Error::LayoutOverflow("journal capacity must be non-zero"));
        }
        let (bitmap_off, slots_off, words, total) = layout(capacity);
        if total > segment.payload_len() {
            return Err(Error::LayoutOverflow("journal does not fit in segment"));
        }
        let base = segment.payload_ptr();
        // SAFETY: every offset is `< total <= payload_len`, so all writes stay
        // within the mapped payload region.
        unsafe {
            base.cast::<JournalHeader>().write(JournalHeader {
                magic: JOURNAL_MAGIC,
                capacity: capacity as u32,
                hint: AtomicU32::new(0),
            });
            let bitmap = base.add(bitmap_off).cast::<AtomicU64>();
            for w in 0..words {
                bitmap.add(w).write(AtomicU64::new(0));
            }
            let slots = base.add(slots_off).cast::<ChunkDesc>();
            for i in 0..capacity {
                slots.add(i).write(ChunkDesc::ZERO);
            }
            Ok(BorrowJournal {
                segment,
                header: base.cast::<JournalHeader>(),
                bitmap,
                slots,
                words,
                capacity,
            })
        }
    }

    /// Attach to a journal previously created in `segment`.
    pub fn attach(segment: &'s Segment) -> Result<BorrowJournal<'s>> {
        let base = segment.payload_ptr();
        // SAFETY: header lives at the payload base.
        let hdr = unsafe { base.cast::<JournalHeader>().read() };
        if hdr.magic != JOURNAL_MAGIC {
            return Err(Error::LayoutMismatch);
        }
        let capacity = hdr.capacity as usize;
        let (bitmap_off, slots_off, words, _total) = layout(capacity);
        // SAFETY: offsets recomputed from the stored capacity are in-bounds.
        let bitmap = unsafe { base.add(bitmap_off).cast::<AtomicU64>() };
        let slots = unsafe { base.add(slots_off).cast::<ChunkDesc>() };
        Ok(BorrowJournal {
            segment,
            header: base.cast::<JournalHeader>(),
            bitmap,
            slots,
            words,
            capacity,
        })
    }

    /// Number of pin slots.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn header(&self) -> &JournalHeader {
        // SAFETY: header lives at the payload base for this journal's lifetime.
        unsafe { &*self.header }
    }

    #[inline]
    fn word(&self, w: usize) -> &AtomicU64 {
        debug_assert!(w < self.words);
        // SAFETY: `w < words`; the bitmap array has `words` entries.
        unsafe { &*self.bitmap.add(w) }
    }

    /// Bits that are meaningful in word `w` (the last word may be partial).
    #[inline]
    fn valid_mask(&self, w: usize) -> u64 {
        if w + 1 < self.words {
            u64::MAX
        } else {
            let bits = self.capacity - (self.words - 1) * 64;
            if bits == 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            }
        }
    }

    /// Record a pin: write `desc` into a free slot and mark it occupied.
    ///
    /// Returns the slot index, or [`Error::JournalFull`] if every slot is in use.
    /// O(1) amortized (starts scanning from the free-slot hint).
    pub fn record(&self, desc: ChunkDesc) -> Result<usize> {
        let start_word = (self.header().hint.load(Ordering::Relaxed) as usize / 64) % self.words.max(1);
        for step in 0..self.words {
            let w = (start_word + step) % self.words;
            let word = self.word(w);
            let valid = self.valid_mask(w);
            loop {
                let cur = word.load(Ordering::Acquire);
                if cur & valid == valid {
                    break; // this word is full; move on
                }
                let bit = (!cur & valid).trailing_zeros();
                let idx = w * 64 + bit as usize;
                let mask = 1u64 << bit;
                // Write the slot BEFORE publishing the bit so a replayer that
                // observes the bit (Acquire) also sees the descriptor.
                // SAFETY: `idx < capacity`; slot array has `capacity` entries.
                unsafe { self.slots.add(idx).write(desc) };
                match word.compare_exchange_weak(cur, cur | mask, Ordering::AcqRel, Ordering::Relaxed)
                {
                    Ok(_) => {
                        self.header().hint.store((idx + 1) as u32, Ordering::Relaxed);
                        return Ok(idx);
                    }
                    Err(_) => continue, // contended; re-read this word
                }
            }
        }
        Err(Error::JournalFull)
    }

    /// Release a pin previously returned by [`record`](Self::record).
    ///
    /// Idempotent-safe against out-of-range indices (returns
    /// [`Error::OutOfBounds`]). Clearing a slot that is already free is a no-op.
    pub fn release(&self, slot: usize) -> Result<()> {
        if slot >= self.capacity {
            return Err(Error::OutOfBounds);
        }
        let w = slot / 64;
        let mask = 1u64 << (slot % 64);
        self.word(w).fetch_and(!mask, Ordering::AcqRel);
        self.header().hint.store(slot as u32, Ordering::Relaxed);
        Ok(())
    }

    /// Whether `slot` currently holds a pin.
    pub fn is_occupied(&self, slot: usize) -> bool {
        if slot >= self.capacity {
            return false;
        }
        let w = slot / 64;
        let mask = 1u64 << (slot % 64);
        self.word(w).load(Ordering::Acquire) & mask != 0
    }

    /// Number of currently pinned slots (O(N/64)).
    pub fn len(&self) -> usize {
        let mut n = 0;
        for w in 0..self.words {
            n += (self.word(w).load(Ordering::Acquire) & self.valid_mask(w)).count_ones() as usize;
        }
        n
    }

    /// Whether no pins are currently held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Replay every currently-pinned chunk so the coordinator can release a dead
    /// actor's pins. O(N).
    pub fn replay(&self) -> impl Iterator<Item = ChunkDesc> + '_ {
        let mut out = Vec::new();
        for w in 0..self.words {
            let mut bits = self.word(w).load(Ordering::Acquire) & self.valid_mask(w);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let idx = w * 64 + bit;
                // SAFETY: bit set => slot was written before the bit was
                // published; `idx < capacity`.
                out.push(unsafe { self.slots.add(idx).read() });
            }
        }
        out.into_iter()
    }
}
