//! The chunk-allocator seam and a [`shm_core::Pool`]-backed implementation.

use shm_core::{ChunkDesc, Pool, Segment};

use crate::error::Result;

/// The minimal allocation surface `shm-arrow`'s write path needs.
///
/// Decoupling from a concrete pool lets tests and future arenas (e.g. a
/// bump allocator or a remote coordinator) supply chunks, while production uses
/// [`PoolAllocator`] over a [`shm_core::Pool`]. The contract:
///
/// - [`alloc`](ChunkAllocator::alloc) returns a chunk of **at least** `size`
///   bytes whose payload is 64-byte aligned (as `shm-core` guarantees).
/// - [`resolve`](ChunkAllocator::resolve) maps a descriptor produced by this
///   allocator to a writable pointer to the chunk's first byte.
/// - [`max_chunk_size`](ChunkAllocator::max_chunk_size) is the largest single
///   chunk `alloc` can return — the per-chunk packing capacity the multi-chunk
///   write path splits a large batch against.
/// - [`free`](ChunkAllocator::free) returns a freshly allocated but still-`FREE`
///   chunk to the pool (used to roll back a partial multi-chunk allocation).
pub trait ChunkAllocator {
    /// Allocate a chunk of at least `size` bytes.
    fn alloc(&self, size: usize) -> Result<ChunkDesc>;

    /// Resolve a descriptor from this allocator to the chunk's base pointer.
    ///
    /// The pointer is valid for `desc.len` bytes for as long as the underlying
    /// segment stays mapped and the chunk is not recycled.
    fn resolve(&self, desc: &ChunkDesc) -> *mut u8;

    /// The largest chunk size (bytes) [`alloc`](Self::alloc) can return.
    ///
    /// The multi-chunk writer packs Arrow buffers against this capacity: an
    /// individual buffer that exceeds it cannot be stored contiguously in one
    /// chunk and is rejected with [`Error::Unsupported`](crate::Error::Unsupported).
    fn max_chunk_size(&self) -> usize;

    /// Return a chunk obtained from [`alloc`](Self::alloc) — that was **never
    /// loaned** (its control word is still `FREE`) — to the pool's free list.
    ///
    /// Used only to unwind a partially allocated multi-chunk batch so a failed
    /// [`write_batch_chunks`](crate::write_batch_chunks) leaks nothing.
    fn free(&self, desc: &ChunkDesc);
}

/// A [`ChunkAllocator`] over a `shm-core` [`Pool`] living in a [`Segment`].
///
/// Holds shared borrows of both: the pool mints descriptors and the segment
/// resolves their `offset` (measured from the segment base) to a pointer.
pub struct PoolAllocator<'s> {
    pool: &'s Pool<'s>,
    segment: &'s Segment,
}

impl<'s> PoolAllocator<'s> {
    /// Wrap a pool and the segment it was created in.
    ///
    /// The caller must pass the same `segment` the `pool` was created/attached
    /// over, so `desc.offset` resolves correctly.
    pub fn new(pool: &'s Pool<'s>, segment: &'s Segment) -> PoolAllocator<'s> {
        PoolAllocator { pool, segment }
    }

    /// The wrapped pool.
    pub fn pool(&self) -> &Pool<'s> {
        self.pool
    }

    /// The wrapped segment.
    pub fn segment(&self) -> &Segment {
        self.segment
    }
}

impl ChunkAllocator for PoolAllocator<'_> {
    fn alloc(&self, size: usize) -> Result<ChunkDesc> {
        let size = u32::try_from(size)
            .map_err(|_| shm_core::Error::LayoutOverflow("batch larger than 4 GiB"))?;
        Ok(self.pool.alloc(size)?)
    }

    fn resolve(&self, desc: &ChunkDesc) -> *mut u8 {
        // The descriptor came from `self.pool` over `self.segment`, so its
        // `offset` (from the segment base) is in-bounds.
        // SAFETY: `offset <= segment.size()`; the resulting pointer stays inside
        // the mapping. It is only dereferenced by the caller under the loan's
        // exclusive-write discipline.
        unsafe { self.segment.base_ptr().add(desc.offset as usize) }
    }

    fn max_chunk_size(&self) -> usize {
        self.pool.max_chunk_size() as usize
    }

    fn free(&self, desc: &ChunkDesc) {
        // The chunk is still `FREE` (never loaned); `Pool::free` only re-links it
        // into the free list, so no control-word transition is needed.
        let _ = self.pool.free(desc);
    }
}
