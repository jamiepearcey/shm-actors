//! Error and `Result` types for `shm-arrow`.

/// Errors produced while writing or reading Arrow batches over chunks.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error bubbled up from the `shm-core` substrate (allocation, bounds,
    /// **stale descriptor**, etc.). A recycled chunk surfaces here as
    /// [`shm_core::Error::StaleDescriptor`].
    #[error(transparent)]
    Core(#[from] shm_core::Error),

    /// An error from the Arrow libraries (schema/array construction).
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),

    /// The batch chunk did not begin with [`crate::BATCH_MAGIC`].
    #[error("bad batch header magic (not an shm-arrow batch chunk)")]
    BadMagic,

    /// The schema id was not present in the reader's [`crate::SchemaRegistry`].
    #[error("unknown schema id {0} (registry not seeded identically?)")]
    UnknownSchema(u32),

    /// The [`shm_core::ChunkDesc::schema_id`] and the on-chunk
    /// [`crate::BatchHeader::schema_id`] disagreed.
    #[error("schema id mismatch: descriptor={desc}, header={header}")]
    SchemaMismatch {
        /// `schema_id` carried by the descriptor.
        desc: u32,
        /// `schema_id` written into the batch header.
        header: u32,
    },

    /// The serialized batch did not fit in the allocated chunk.
    #[error("batch does not fit in chunk: need {need} bytes, have {have}")]
    ChunkTooSmall {
        /// Bytes required by the serialized batch.
        need: usize,
        /// Bytes available in the loaned chunk.
        have: usize,
    },

    /// A v0.1 limitation was hit (sliced input, multi-chunk batch, ...).
    #[error("unsupported in v0.1: {0}")]
    Unsupported(&'static str),
}

/// Convenience alias for `Result<T, shm_arrow::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
