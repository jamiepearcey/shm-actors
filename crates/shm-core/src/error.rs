//! Error and `Result` types shared across `shm-core`.

/// Errors produced by the shared-memory substrate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A [`ChunkDesc`](crate::desc::ChunkDesc) referenced a chunk whose
    /// generation no longer matches the live [`ChunkCtrl`](crate::ctrl::ChunkCtrl)
    /// (the chunk was recycled). Reads must be rejected.
    #[error("stale descriptor: chunk generation mismatch (chunk was recycled)")]
    StaleDescriptor,

    /// A segment's on-disk header did not match the expected magic or
    /// [`LAYOUT_VERSION`](crate::segment::LAYOUT_VERSION).
    #[error("segment layout mismatch: incompatible magic or layout version")]
    LayoutMismatch,

    /// A pointer view or offset fell outside the mapped region.
    #[error("access out of bounds of the mapped region")]
    OutOfBounds,

    /// A typed view was requested at an address that is not aligned for the type.
    #[error("misaligned access for the requested type")]
    Misaligned,

    /// A size-class pool had no free chunks of the requested class.
    #[error("pool exhausted: no free chunk for the requested size class")]
    PoolExhausted,

    /// A per-actor borrow journal had no free slot (intentional backpressure).
    #[error("borrow journal full: pin table exhausted")]
    JournalFull,

    /// A [`ChunkCtrl`](crate::ctrl::ChunkCtrl) state transition was not legal
    /// from the current state.
    #[error("invalid state transition")]
    InvalidState,

    /// The requested size class / geometry did not fit in the segment.
    #[error("layout does not fit: {0}")]
    LayoutOverflow(&'static str),

    /// A standard-library I/O error (segment name construction, etc.).
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// An error from an underlying `nix` syscall (`shm_open`, `mmap`, ...).
    #[error(transparent)]
    Nix(#[from] nix::Error),
}

/// Convenience alias for `Result<T, shm_core::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
