//! Error and `Result` types for `shm-ring`.

/// Errors produced while laying out or attaching to a broadcast ring.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error bubbled up from the `shm-core` substrate.
    #[error(transparent)]
    Core(#[from] shm_core::Error),

    /// The requested capacity was zero or not a power of two.
    #[error("ring capacity must be a non-zero power of two, got {0}")]
    BadCapacity(u32),

    /// The provided region was too small to hold the ring header + slots.
    #[error("ring region too small: need {need} bytes, have {have}")]
    RegionTooSmall {
        /// Bytes required for the header and slot array.
        need: usize,
        /// Bytes available in the provided region.
        have: usize,
    },

    /// The base pointer was not 8-byte aligned (required for the atomics).
    #[error("ring base pointer is not 8-byte aligned")]
    Misaligned,

    /// A region being attached did not carry the ring magic.
    #[error("bad ring magic (not a shm-ring region)")]
    BadMagic,

    /// The fixed reliable-subscriber table had no free slot.
    #[error("reliable subscriber table full (max {0})")]
    ReliableTableFull(usize),
}

/// Convenience alias for `Result<T, shm_ring::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
