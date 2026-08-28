//! Fixed size-class chunk pools with ABA-safe lock-free free-lists.
//!
//! A pool carves a segment into a fixed set of size classes (powers of two).
//! Each class is a [Treiber stack](https://en.wikipedia.org/wiki/Treiber_stack)
//! of fixed chunks whose head lives in shared memory as a single `AtomicU64`.
//! Allocation is one CAS to pop the head; free is one CAS to push. Because the
//! classes are fixed, external fragmentation is structurally impossible.
//!
//! # ABA safety
//!
//! The free-list head packs a 32-bit **generation tag** in its high half and a
//! 32-bit chunk slot in its low half. Every push/pop increments the tag, so the
//! classic ABA sequence (pop A, pop B, push A, then a stale CAS that expects the
//! head to still read "A→…") fails: the tag has moved even though the slot came
//! back. This is the "tagged pointer" ABA defence and needs no hazard pointers.
//!
//! # Control words
//!
//! Each chunk has a matching [`ChunkCtrl`] in a control array held in the same
//! segment but in a region distinct from (not interleaved with) payload bytes.
//! [`Pool::alloc`] mints the descriptor's generation from that control word, so
//! a descriptor always carries the generation the chunk had when handed out.

use core::sync::atomic::Ordering;

use crate::ctrl::ChunkCtrl;
use crate::desc::ChunkDesc;
use crate::error::{Error, Result};
use crate::segment::{Segment, HEADER_SIZE};
use crate::substrate::ShmU64;

/// Magic for a pool header: little-endian `b"SHMPOOL1"`.
pub const POOL_MAGIC: u64 = u64::from_le_bytes(*b"SHMPOOL2");

/// Free-list slot sentinel meaning "empty".
const EMPTY: u32 = u32::MAX;

/// Round `x` up to a multiple of `a` (a power of two).
#[inline]
const fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

/// One size class in a [`PoolConfig`].
#[derive(Clone, Copy, Debug)]
pub struct SizeClass {
    /// Chunk size in bytes (should be a multiple of 64 for Arrow alignment).
    pub chunk_size: u32,
    /// Number of chunks in this class.
    pub chunk_count: u32,
}

/// Description of the size classes a pool should contain.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Size classes, smallest first. `alloc` rounds a request up to the first
    /// class whose `chunk_size` is large enough.
    pub classes: Vec<SizeClass>,
}

impl PoolConfig {
    /// Build power-of-two classes from `min_size` up to and including `max_size`,
    /// each holding `count_per_class` chunks.
    ///
    /// `min_size`/`max_size` are rounded up to a power of two; `min_size` is
    /// clamped to at least 64 (a chunk must hold the 4-byte free-list link and
    /// stay 64-byte aligned).
    pub fn power_of_two(min_size: u32, max_size: u32, count_per_class: u32) -> PoolConfig {
        let min = min_size.max(64).next_power_of_two();
        let max = max_size.max(min).next_power_of_two();
        let mut classes = Vec::new();
        let mut size = min;
        loop {
            classes.push(SizeClass {
                chunk_size: size,
                chunk_count: count_per_class,
            });
            if size >= max {
                break;
            }
            size <<= 1;
        }
        PoolConfig { classes }
    }

    /// The largest chunk size across all classes (0 if empty).
    pub fn max_chunk_size(&self) -> u32 {
        self.classes.iter().map(|c| c.chunk_size).max().unwrap_or(0)
    }
}

/// On-segment pool header. `#[repr(C)]`, lives at the payload base.
#[repr(C)]
struct PoolHeader {
    magic: u64,
    num_classes: u32,
    total_chunks: u32,
    classes_offset: u32, // payload-relative
    ctrl_offset: u32,    // payload-relative
}

/// On-segment per-class header. `#[repr(C)]`. Free-list head is atomic.
#[repr(C)]
struct ClassHeader {
    /// `(tag << 32) | slot`; `slot == EMPTY` means the free list is empty. A
    /// [`ShmU64`] (`#[repr(transparent)]` over `AtomicU64`), so this header's
    /// on-shm layout is unchanged while the Treiber CAS loop it drives can be
    /// model-checked under loom (ADR-0004 stage L).
    head: ShmU64,
    chunk_size: u32,
    chunk_count: u32,
    base_index: u32, // global chunk index of local chunk 0
    _pad: u32,
    data_offset: u32, // payload-relative start of this class's chunk region
    _pad2: u32,
}

#[inline]
fn head_pack(tag: u32, slot: u32) -> u64 {
    (u64::from(tag) << 32) | u64::from(slot)
}

#[inline]
fn head_unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, (v & 0xffff_ffff) as u32)
}

/// Treiber-stack **pop** on a tagged free-list `head`, returning the popped slot
/// (or `None` when the list is empty).
///
/// This is the pure ABA-safe pop algorithm, factored out of [`Pool::alloc`] so it
/// can be model-checked under loom (ADR-0004 stage L) against the *same* bytes the
/// production allocator runs. `read_next(slot)` yields the intrusive next-link
/// stored in `slot`'s node; in production that link lives in the chunk's payload
/// (resolved by pointer arithmetic in the addressing layer), while a loom harness
/// supplies an ordinary in-memory link table — the CAS loop itself is identical.
///
/// Each successful pop bumps the 32-bit generation **tag** in the head's high
/// half, so a stale CAS whose slot came back via an intervening pop/push (the
/// classic ABA sequence) fails on the moved tag and retries.
#[inline]
pub fn treiber_pop<R: Fn(u32) -> u32>(head: &ShmU64, read_next: R) -> Option<u32> {
    loop {
        let cur = head.load(Ordering::Acquire);
        let (tag, slot) = head_unpack(cur);
        if slot == EMPTY {
            return None;
        }
        let next = read_next(slot);
        let new = head_pack(tag.wrapping_add(1), next);
        if head
            .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return Some(slot);
        }
    }
}

/// Treiber-stack **push** of `slot` onto a tagged free-list `head`.
///
/// The pure ABA-safe push algorithm, factored out of [`Pool::free`] for loom
/// model-checking (see [`treiber_pop`]). `write_next(slot, next)` records the
/// intrusive next-link into `slot`'s node before the head CAS publishes it; the
/// head's tag is bumped on every push, matching the pop side.
#[inline]
pub fn treiber_push<W: Fn(u32, u32)>(head: &ShmU64, slot: u32, write_next: W) {
    loop {
        let cur = head.load(Ordering::Acquire);
        let (tag, old_head) = head_unpack(cur);
        write_next(slot, old_head);
        let new = head_pack(tag.wrapping_add(1), slot);
        if head
            .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

/// A handle to a size-class pool laid out inside a [`Segment`].
///
/// The pool's state (headers, free-list heads, control array) lives entirely in
/// the segment; this handle only caches derived pointers. Multiple processes may
/// each hold their own `Pool` over the same segment.
pub struct Pool<'s> {
    segment: &'s Segment,
    classes: *mut ClassHeader,
    ctrls: *mut ChunkCtrl,
    num_classes: usize,
    total_chunks: usize,
}

/// Computed byte layout for a [`PoolConfig`] within a payload region.
struct Layout {
    classes_offset: usize,
    ctrl_offset: usize,
    data_offsets: Vec<usize>,
    base_indices: Vec<u32>,
    total_chunks: u32,
    total_bytes: usize,
}

fn compute_layout(config: &PoolConfig) -> Layout {
    let num_classes = config.classes.len();
    let mut total_chunks: u32 = 0;
    let mut base_indices = Vec::with_capacity(num_classes);
    for c in &config.classes {
        base_indices.push(total_chunks);
        total_chunks += c.chunk_count;
    }

    let classes_offset = align_up(core::mem::size_of::<PoolHeader>(), 8);
    let after_classes = classes_offset + num_classes * core::mem::size_of::<ClassHeader>();
    let ctrl_offset = align_up(after_classes, core::mem::align_of::<ChunkCtrl>());
    let mut off = ctrl_offset + total_chunks as usize * core::mem::size_of::<ChunkCtrl>();

    let mut data_offsets = Vec::with_capacity(num_classes);
    for c in &config.classes {
        let data_off = align_up(off, 64);
        data_offsets.push(data_off);
        off = data_off + c.chunk_count as usize * c.chunk_size as usize;
    }

    Layout {
        classes_offset,
        ctrl_offset,
        data_offsets,
        base_indices,
        total_chunks,
        total_bytes: off,
    }
}

impl<'s> Pool<'s> {
    /// Create and initialize a pool inside `segment` per `config`.
    ///
    /// Fails with [`Error::LayoutOverflow`] if the classes do not fit in the
    /// segment payload or the config is empty.
    pub fn create(segment: &'s Segment, config: &PoolConfig) -> Result<Pool<'s>> {
        if config.classes.is_empty() {
            return Err(Error::LayoutOverflow("pool config has no size classes"));
        }
        let layout = compute_layout(config);
        if layout.total_bytes > segment.payload_len() {
            return Err(Error::LayoutOverflow("pool does not fit in segment"));
        }

        let base = segment.payload_ptr();
        // SAFETY: every offset below is `< total_bytes <= payload_len`, so each
        // pointer stays within the mapped payload region.
        unsafe {
            base.cast::<PoolHeader>().write(PoolHeader {
                magic: POOL_MAGIC,
                num_classes: config.classes.len() as u32,
                total_chunks: layout.total_chunks,
                classes_offset: layout.classes_offset as u32,
                ctrl_offset: layout.ctrl_offset as u32,
            });

            let classes = base.add(layout.classes_offset).cast::<ClassHeader>();
            for (i, c) in config.classes.iter().enumerate() {
                let ch = classes.add(i);
                // Build the intrusive free-list link chain for this class.
                let data = base.add(layout.data_offsets[i]);
                for local in 0..c.chunk_count {
                    let link = data
                        .add(local as usize * c.chunk_size as usize)
                        .cast::<u32>();
                    let next = if local + 1 < c.chunk_count {
                        local + 1
                    } else {
                        EMPTY
                    };
                    link.write(next);
                }
                let head_slot = if c.chunk_count > 0 { 0 } else { EMPTY };
                ch.write(ClassHeader {
                    head: ShmU64::new(head_pack(0, head_slot)),
                    chunk_size: c.chunk_size,
                    chunk_count: c.chunk_count,
                    base_index: layout.base_indices[i],
                    _pad: 0,
                    data_offset: layout.data_offsets[i] as u32,
                    _pad2: 0,
                });
            }

            let ctrls = base.add(layout.ctrl_offset).cast::<ChunkCtrl>();
            for g in 0..layout.total_chunks as usize {
                ChunkCtrl::init_at(ctrls.add(g), 0);
            }

            Ok(Pool {
                segment,
                classes,
                ctrls,
                num_classes: config.classes.len(),
                total_chunks: layout.total_chunks as usize,
            })
        }
    }

    /// Attach to a pool previously created in `segment`.
    pub fn attach(segment: &'s Segment) -> Result<Pool<'s>> {
        let base = segment.payload_ptr();
        // SAFETY: header lives at the payload base; the segment guarantees at
        // least `size_of::<PoolHeader>()` payload bytes for any real pool.
        let hdr = unsafe { base.cast::<PoolHeader>().read() };
        if hdr.magic != POOL_MAGIC {
            return Err(Error::LayoutMismatch);
        }
        // SAFETY: offsets recorded in the header are within the payload region.
        let classes = unsafe { base.add(hdr.classes_offset as usize).cast::<ClassHeader>() };
        let ctrls = unsafe { base.add(hdr.ctrl_offset as usize).cast::<ChunkCtrl>() };
        Ok(Pool {
            segment,
            classes,
            ctrls,
            num_classes: hdr.num_classes as usize,
            total_chunks: hdr.total_chunks as usize,
        })
    }

    /// Number of size classes.
    #[inline]
    pub fn num_classes(&self) -> usize {
        self.num_classes
    }

    /// Total number of chunks across all classes.
    #[inline]
    pub fn total_chunks(&self) -> usize {
        self.total_chunks
    }

    /// The largest chunk size (bytes) any class in this pool can satisfy — the
    /// ceiling a single [`alloc`](Self::alloc) request can be rounded up to.
    ///
    /// `shm-arrow`'s multi-chunk writer uses this as the per-chunk packing
    /// capacity: an individual Arrow buffer must fit within one chunk, so it must
    /// not exceed this bound.
    #[inline]
    pub fn max_chunk_size(&self) -> u32 {
        (0..self.num_classes)
            .map(|i| self.class(i).chunk_size)
            .max()
            .unwrap_or(0)
    }

    fn class(&self, i: usize) -> &ClassHeader {
        debug_assert!(i < self.num_classes);
        // SAFETY: `i < num_classes`; the class array has `num_classes` entries.
        unsafe { &*self.classes.add(i) }
    }

    /// The control word for the chunk a descriptor points at.
    pub fn ctrl(&self, desc: &ChunkDesc) -> Result<&ChunkCtrl> {
        let (_, _, global) = self.locate(desc.offset)?;
        // SAFETY: `global < total_chunks`; the control array has that many.
        Ok(unsafe { &*self.ctrls.add(global) })
    }

    /// The control word for a global chunk index (used by the coordinator).
    pub fn ctrl_by_index(&self, global: usize) -> Result<&ChunkCtrl> {
        if global >= self.total_chunks {
            return Err(Error::OutOfBounds);
        }
        // SAFETY: bounds checked just above.
        Ok(unsafe { &*self.ctrls.add(global) })
    }

    /// Pointer to a chunk's payload data for `(class, local)`.
    fn chunk_ptr(&self, class: &ClassHeader, local: u32) -> *mut u8 {
        // SAFETY: `local < chunk_count`, so the offset is within the class's
        // data region inside the mapped payload.
        unsafe {
            self.segment
                .payload_ptr()
                .add(class.data_offset as usize + local as usize * class.chunk_size as usize)
        }
    }

    /// Allocate a chunk large enough for `size` bytes, returning its descriptor.
    ///
    /// Rounds up to the smallest sufficient size class. Fails with
    /// [`Error::PoolExhausted`] if that class has no free chunk, or
    /// [`Error::LayoutOverflow`] if no class is large enough.
    pub fn alloc(&self, size: u32) -> Result<ChunkDesc> {
        let class_idx = (0..self.num_classes)
            .find(|&i| self.class(i).chunk_size >= size)
            .ok_or(Error::LayoutOverflow(
                "allocation exceeds largest size class",
            ))?;
        let class = self.class(class_idx);

        // Treiber pop (the ABA-safe CAS loop lives in `treiber_pop`; the closure
        // is the addressing layer resolving a slot's intrusive next-link out of
        // the chunk payload — the only per-node pointer arithmetic).
        let local = match treiber_pop(&class.head, |slot| {
            // SAFETY: `slot < chunk_count`; the first 4 bytes hold the next link.
            unsafe { *self.chunk_ptr(class, slot).cast::<u32>() }
        }) {
            Some(slot) => slot,
            None => return Err(Error::PoolExhausted),
        };

        let global = class.base_index as usize + local as usize;
        // SAFETY: `global < total_chunks`.
        let generation = unsafe { (*self.ctrls.add(global)).generation() };
        let seg_offset =
            HEADER_SIZE + class.data_offset as usize + local as usize * class.chunk_size as usize;

        Ok(ChunkDesc {
            segment_id: self.segment.id(),
            generation,
            offset: seg_offset as u32,
            len: class.chunk_size,
            schema_id: 0,
            _pad: 0,
        })
    }

    /// Return a chunk to its size class's free list.
    ///
    /// The caller is responsible for ensuring the chunk's [`ChunkCtrl`] has
    /// already been recycled to `FREE` (which bumps the generation); `free` only
    /// restores free-list membership.
    pub fn free(&self, desc: &ChunkDesc) -> Result<()> {
        let (class_idx, local, _global) = self.locate(desc.offset)?;
        let class = self.class(class_idx);

        // Treiber push (the ABA-safe CAS loop lives in `treiber_push`; the closure
        // is the addressing layer writing the freed node's next-link into the
        // chunk payload).
        treiber_push(&class.head, local, |slot, next| {
            // SAFETY: `slot < chunk_count`; write the next link into the chunk.
            unsafe {
                *self.chunk_ptr(class, slot).cast::<u32>() = next;
            }
        });
        Ok(())
    }

    /// Resolve a segment-base `offset` to `(class_idx, local_idx, global_idx)`.
    fn locate(&self, seg_offset: u32) -> Result<(usize, u32, usize)> {
        let seg_offset = seg_offset as usize;
        if seg_offset < HEADER_SIZE {
            return Err(Error::OutOfBounds);
        }
        let payload_off = seg_offset - HEADER_SIZE;
        for i in 0..self.num_classes {
            let class = self.class(i);
            let start = class.data_offset as usize;
            let span = class.chunk_count as usize * class.chunk_size as usize;
            if payload_off >= start && payload_off < start + span {
                let rel = payload_off - start;
                if !rel.is_multiple_of(class.chunk_size as usize) {
                    return Err(Error::Misaligned);
                }
                let local = (rel / class.chunk_size as usize) as u32;
                let global = class.base_index as usize + local as usize;
                return Ok((i, local, global));
            }
        }
        Err(Error::OutOfBounds)
    }

    /// Count free chunks in a class (test/introspection helper; walks the list).
    pub fn free_count(&self, class_idx: usize) -> usize {
        let class = self.class(class_idx);
        let (_, mut slot) = head_unpack(class.head.load(Ordering::Acquire));
        let mut n = 0;
        while slot != EMPTY {
            n += 1;
            // SAFETY: `slot < chunk_count` while walking a valid free chain.
            slot = unsafe { *self.chunk_ptr(class, slot).cast::<u32>() };
        }
        n
    }
}
