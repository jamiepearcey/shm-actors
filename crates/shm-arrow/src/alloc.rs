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
pub trait ChunkAllocator {
    /// Allocate a chunk of at least `size` bytes.
    fn alloc(&self, size: usize) -> Result<ChunkDesc>;

    /// Resolve a descriptor from this allocator to the chunk's base pointer.
    ///
    /// The pointer is valid for `desc.len` bytes for as long as the underlying
    /// segment stays mapped and the chunk is not recycled.
    fn resolve(&self, desc: &ChunkDesc) -> *mut u8;
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
}
