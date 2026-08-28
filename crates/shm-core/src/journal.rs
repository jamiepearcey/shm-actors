//! Per-actor borrow journal: a fixed table of tagged pin entries.
//!
//! Each actor owns a journal (typically in its own management segment). When the
//! actor takes a borrow it [`record`](BorrowJournal::record)s a **tagged entry**;
//! when it drops the borrow it [`release`](BorrowJournal::release)s the slot. If
//! the actor dies, the coordinator [`replay`](BorrowJournal::replay)s the journal
//! to find every still-held borrow and release it.
//!
//! # Design (ADR-0001 §5, extended in ADR-0003 S1 / item J)
//!
//! A fixed table of `N` slots plus an occupancy bitmap, all POD, in shared
//! memory. `record`/`release` are O(1) (a bitmap bit + one slot write); `replay`
//! is O(N). The bounded pin count is a **feature**: `record` returns
//! [`Error::JournalFull`] as natural backpressure and bounds crash-reclamation
//! work. An append log was rejected (unbounded shm growth, O(n) replay).
//!
//! ## Tagged entries (item J)
//!
//! v0.2 slots held a bare [`ChunkDesc`] (a chunk pin). v0.3 generalises a slot to
//! a fixed-size [`JournalEntry`] carrying a `kind` tag plus a 24-byte payload —
//! large enough for either a [`ChunkDesc`] (24 B) **or** an
//! [`ArtifactPin`](JournalRecord::ArtifactPin)'s `{artifact_id: u32, version:
//! u64}` (12 B). This lets a leaked artifact **version pin** (an
//! [`shm_artifact`](../../shm_artifact) `VersionPin`) be crash-reclaimed through
//! the *one* reclamation mechanism (the journal is the crash ledger; the pin
//! table stays truth). The slot format is an ABI break, so [`JOURNAL_MAGIC`] is
//! bumped `SHMJRNL1` → `SHMJRNL2`; the fixed-slot bitmap design and
//! [`DEFAULT_CAPACITY`] are preserved.
//!
//! ## Incarnations (ADR-0008 P0.1)
//!
//! Both artifact tags additionally carry the **incarnation** of the head region
//! the borrow was taken against, claimed out of the payload's reserved words
//! (`SHMJRNL2` → `SHMJRNL3`). Once the keyed store can reclaim and re-create a
//! catalog slot, `artifact_id` alone names a *slot*, not an occupant — so
//! replaying a record from a previous occupant would release a borrow belonging
//! to the current one. The incarnation is what lets replay tell them apart.

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::desc::ChunkDesc;
use crate::error::{Error, Result};
use crate::segment::Segment;

/// Magic for a journal header: little-endian `b"SHMJRNL3"`.
///
/// Bumped from `SHMJRNL2` for the ADR-0008 P0.1 payload change: both artifact
/// tags now carry an `incarnation`. (`SHMJRNL2` in turn replaced `SHMJRNL1` at
/// v0.3 / ADR-0003 item J, when a slot became a [`JournalEntry`] — `kind` tag
/// plus 24-byte payload — rather than a bare [`ChunkDesc`].)
pub const JOURNAL_MAGIC: u64 = u64::from_le_bytes(*b"SHMJRNL3");

/// Default journal capacity (pins) when unspecified.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Entry-kind tag: an unused/zeroed slot (never observed occupied — the bitmap
/// governs occupancy, so a set bit always names a real tagged entry).
pub const ENTRY_EMPTY: u32 = 0;
/// Entry-kind tag: a chunk pin, payload = a [`ChunkDesc`] (v0.2 semantics).
pub const ENTRY_CHUNK_PIN: u32 = 1;
/// Entry-kind tag: an artifact version pin, payload =
/// `{artifact_id, incarnation, version}`.
pub const ENTRY_ARTIFACT_PIN: u32 = 2;
/// Entry-kind tag: an exclusive artifact **write lease** (ADR-0003 item K),
/// payload = `{artifact_id, incarnation, fence}`. A leaked one (a `kill -9`ed exclusive
/// writer) is crash-reclaimed by the coordinator force-releasing the artifact's
/// fenced write lease.
pub const ENTRY_WRITE_LEASE: u32 = 3;

/// One fixed-size, POD journal slot: a `kind` tag plus a 24-byte payload.
///
/// # Layout (frozen ABI — 32 bytes, v0.3 / ADR-0003 item J)
///
/// | field  | type       | meaning                                            |
/// |--------|------------|----------------------------------------------------|
/// | `kind` | `u32`      | [`ENTRY_CHUNK_PIN`] / [`ENTRY_ARTIFACT_PIN`]       |
/// | `_pad` | `u32`      | reserved, zero (keeps the 24-byte payload 8-framed)|
/// | `w`    | `[u32; 6]` | payload, interpreted per `kind` (see below)        |
///
/// - **`ENTRY_CHUNK_PIN`**: `w` is a [`ChunkDesc`]'s six `u32` fields in ABI
///   order (`segment_id, generation, offset, len, schema_id, _pad`).
/// - **`ENTRY_ARTIFACT_PIN`**: `w[0]` = `artifact_id`; `w[1..3]` = the `u64`
///   `version` as little-endian `(lo, hi)`; `w[3]` = `incarnation`; `w[4..6]` = zero.
/// - **`ENTRY_WRITE_LEASE`**: `w[0]` = `artifact_id`; `w[1]` = the lease `fence`
///   the writer acquired under; `w[2]` = `incarnation`; `w[3..6]` = zero.
///
/// ADR-0008 P0.1 claimed the reserved words for `incarnation` on both artifact
/// tags, which **is** a payload-layout change and so does bump
/// [`JOURNAL_MAGIC`].
///
/// Adding a new **tag value** within this layout (as [`ENTRY_WRITE_LEASE`] was)
/// does not bump the magic — a reader predating the tag simply skips it via
/// [`to_record`](JournalEntry::to_record). Reinterpreting an existing tag's
/// payload words, as P0.1 did, does.
///
/// The payload is read/written as `u32` words, so no field ever needs `u64`
/// alignment inside the slot.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalEntry {
    /// The entry-kind tag ([`ENTRY_CHUNK_PIN`] / [`ENTRY_ARTIFACT_PIN`]).
    pub kind: u32,
    /// Reserved padding, kept zero.
    pub _pad: u32,
    /// The 24-byte payload, interpreted per [`kind`](Self::kind).
    pub w: [u32; 6],
}

const _: () = assert!(core::mem::size_of::<JournalEntry>() == 32);
const _: () = assert!(core::mem::align_of::<JournalEntry>() == 4);

impl JournalEntry {
    /// A zeroed entry ([`ENTRY_EMPTY`]).
    pub const ZERO: JournalEntry = JournalEntry {
        kind: ENTRY_EMPTY,
        _pad: 0,
        w: [0; 6],
    };

    /// Build a chunk-pin entry carrying `desc`.
    #[inline]
    pub fn chunk_pin(desc: ChunkDesc) -> JournalEntry {
        JournalEntry {
            kind: ENTRY_CHUNK_PIN,
            _pad: 0,
            w: [
                desc.segment_id,
                desc.generation,
                desc.offset,
                desc.len,
                desc.schema_id,
                desc._pad,
            ],
        }
    }

    /// Build an artifact-version-pin entry for `{artifact_id, incarnation, version}`.
    #[inline]
    pub fn artifact_pin(artifact_id: u32, incarnation: u32, version: u64) -> JournalEntry {
        JournalEntry {
            kind: ENTRY_ARTIFACT_PIN,
            _pad: 0,
            w: [
                artifact_id,
                version as u32,
                (version >> 32) as u32,
                incarnation,
                0,
                0,
            ],
        }
    }

    /// Build an exclusive write-lease entry for `{artifact_id, incarnation, fence}`
    /// (item K).
    #[inline]
    pub fn write_lease(artifact_id: u32, incarnation: u32, fence: u32) -> JournalEntry {
        JournalEntry {
            kind: ENTRY_WRITE_LEASE,
            _pad: 0,
            w: [artifact_id, fence, incarnation, 0, 0, 0],
        }
    }

    /// Decode this entry into a typed [`JournalRecord`], or `None` for an
    /// unrecognised / empty tag.
    #[inline]
    pub fn to_record(self) -> Option<JournalRecord> {
        match self.kind {
            ENTRY_CHUNK_PIN => Some(JournalRecord::ChunkPin(ChunkDesc {
                segment_id: self.w[0],
                generation: self.w[1],
                offset: self.w[2],
                len: self.w[3],
                schema_id: self.w[4],
                _pad: self.w[5],
            })),
            ENTRY_ARTIFACT_PIN => Some(JournalRecord::ArtifactPin {
                artifact_id: self.w[0],
                incarnation: self.w[3],
                version: u64::from(self.w[1]) | (u64::from(self.w[2]) << 32),
            }),
            ENTRY_WRITE_LEASE => Some(JournalRecord::WriteLease {
                artifact_id: self.w[0],
                incarnation: self.w[2],
                fence: self.w[1],
            }),
            _ => None,
        }
    }
}

/// A decoded journal entry: what a dead actor still held when it died.
///
/// [`replay`](BorrowJournal::replay) yields one of these per still-occupied slot;
/// the coordinator routes each to its reclamation path (a [`ChunkPin`] releases a
/// chunk's borrow; an [`ArtifactPin`] decrements an artifact version's pin
/// count).
///
/// [`ChunkPin`]: JournalRecord::ChunkPin
/// [`ArtifactPin`]: JournalRecord::ArtifactPin
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalRecord {
    /// A pinned chunk (v0.2): its [`ChunkDesc`].
    ChunkPin(ChunkDesc),
    /// A pinned artifact version (v0.3 / item J): the interned artifact id and
    /// the version number a `VersionPin` froze.
    ArtifactPin {
        /// The interned artifact name id the pin was taken against.
        artifact_id: u32,
        /// Which **occupant** of that id's head region the pin was taken
        /// against (ADR-0008 P0.1). A keyed-store slot can be reclaimed and
        /// re-created between a crash and its replay; routing a record whose
        /// incarnation no longer matches would decrement the *next* occupant's
        /// pin count, so replay drops it instead.
        incarnation: u32,
        /// The artifact version the pin froze.
        version: u64,
    },
    /// A held exclusive **write lease** (v0.3 / item K): the interned artifact id
    /// and the fence generation the writer acquired under. A leaked one is
    /// crash-reclaimed by the coordinator force-releasing the artifact's fenced
    /// write lease.
    WriteLease {
        /// The interned artifact name id whose write lease is held.
        artifact_id: u32,
        /// Which occupant of that id's head region the lease was taken against
        /// — see [`ArtifactPin`](JournalRecord::ArtifactPin).
        incarnation: u32,
        /// The fence generation the writer acquired the lease under.
        fence: u32,
    },
}

/// On-segment journal header. `#[repr(C)]`, lives at the payload base.
#[repr(C)]
struct JournalHeader {
    magic: u64,
    capacity: u32,
    /// Hint for the next free-slot search (advisory; correctness never depends
    /// on it).
    hint: AtomicU32,
}

/// Bytes needed to lay out a journal of `capacity` entries.
fn layout(capacity: usize) -> (usize, usize, usize, usize) {
    let words = capacity.div_ceil(64);
    let header = core::mem::size_of::<JournalHeader>();
    let bitmap_off = align_up(header, 8);
    let slots_off = align_up(
        bitmap_off + words * 8,
        core::mem::align_of::<JournalEntry>(),
    );
    let total = slots_off + capacity * core::mem::size_of::<JournalEntry>();
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
    slots: *mut JournalEntry,
    words: usize,
    capacity: usize,
}

impl<'s> BorrowJournal<'s> {
    /// Create and zero-initialize a journal of `capacity` entries in `segment`.
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
            let slots = base.add(slots_off).cast::<JournalEntry>();
            for i in 0..capacity {
                slots.add(i).write(JournalEntry::ZERO);
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
        let slots = unsafe { base.add(slots_off).cast::<JournalEntry>() };
        Ok(BorrowJournal {
            segment,
            header: base.cast::<JournalHeader>(),
            bitmap,
            slots,
            words,
            capacity,
        })
    }

    /// Number of entry slots.
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

    /// Record a chunk pin: the v0.2 entry, kept for chunk-pin callers
    /// (`shm-stream` staging, a node's `pin_and_read`). Convenience over
    /// [`record_entry`](Self::record_entry) with [`JournalEntry::chunk_pin`].
    pub fn record(&self, desc: ChunkDesc) -> Result<usize> {
        self.record_entry(JournalEntry::chunk_pin(desc))
    }

    /// Record an artifact **version pin** (item J): a leaked one is crash-
    /// reclaimed by the coordinator decrementing the artifact's per-version pin
    /// count. Convenience over [`record_entry`](Self::record_entry) with
    /// [`JournalEntry::artifact_pin`].
    pub fn record_artifact_pin(
        &self,
        artifact_id: u32,
        incarnation: u32,
        version: u64,
    ) -> Result<usize> {
        self.record_entry(JournalEntry::artifact_pin(artifact_id, incarnation, version))
    }

    /// Record an exclusive **write lease** (item K): a leaked one is crash-
    /// reclaimed by the coordinator force-releasing the artifact's fenced write
    /// lease. Convenience over [`record_entry`](Self::record_entry) with
    /// [`JournalEntry::write_lease`].
    pub fn record_write_lease(
        &self,
        artifact_id: u32,
        incarnation: u32,
        fence: u32,
    ) -> Result<usize> {
        self.record_entry(JournalEntry::write_lease(artifact_id, incarnation, fence))
    }

    /// Record a tagged `entry` into a free slot and mark it occupied.
    ///
    /// Returns the slot index, or [`Error::JournalFull`] if every slot is in use.
    /// O(1) amortized (starts scanning from the free-slot hint).
    pub fn record_entry(&self, entry: JournalEntry) -> Result<usize> {
        let start_word =
            (self.header().hint.load(Ordering::Relaxed) as usize / 64) % self.words.max(1);
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
                // observes the bit (Acquire) also sees the entry.
                // SAFETY: `idx < capacity`; slot array has `capacity` entries.
                unsafe { self.slots.add(idx).write(entry) };
                match word.compare_exchange_weak(
                    cur,
                    cur | mask,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.header()
                            .hint
                            .store((idx + 1) as u32, Ordering::Relaxed);
                        return Ok(idx);
                    }
                    Err(_) => continue, // contended; re-read this word
                }
            }
        }
        Err(Error::JournalFull)
    }

    /// Release an entry previously returned by [`record`](Self::record) /
    /// [`record_entry`](Self::record_entry).
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

    /// Whether `slot` currently holds an entry.
    pub fn is_occupied(&self, slot: usize) -> bool {
        if slot >= self.capacity {
            return false;
        }
        let w = slot / 64;
        let mask = 1u64 << (slot % 64);
        self.word(w).load(Ordering::Acquire) & mask != 0
    }

    /// Number of currently occupied slots (O(N/64)).
    pub fn len(&self) -> usize {
        let mut n = 0;
        for w in 0..self.words {
            n += (self.word(w).load(Ordering::Acquire) & self.valid_mask(w)).count_ones() as usize;
        }
        n
    }

    /// Whether no entries are currently held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Replay every currently-held entry so the coordinator can reclaim a dead
    /// actor's borrows. O(N). Yields each occupied slot decoded into a typed
    /// [`JournalRecord`] (an unrecognised tag is skipped).
    pub fn replay(&self) -> impl Iterator<Item = JournalRecord> + '_ {
        let mut out = Vec::new();
        for w in 0..self.words {
            let mut bits = self.word(w).load(Ordering::Acquire) & self.valid_mask(w);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let idx = w * 64 + bit;
                // SAFETY: bit set => slot was written before the bit was
                // published; `idx < capacity`.
                let entry = unsafe { self.slots.add(idx).read() };
                if let Some(rec) = entry.to_record() {
                    out.push(rec);
                }
            }
        }
        out.into_iter()
    }
}
