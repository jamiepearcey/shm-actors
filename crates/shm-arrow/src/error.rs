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

    /// The schema id was not present in the reader's [`crate::SchemaRegistry`]
    /// cache (resolve it from the coordinator first — ADR-0003 item E).
    #[error("unknown schema id {0} (not resolved into the local registry cache?)")]
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

    /// An unsupported input was hit (e.g. a Dictionary/Union/Map type, or a
    /// single Arrow buffer larger than the pool's max chunk size).
    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    /// The on-chunk batch layout was internally inconsistent (a table cursor ran
    /// off the end, a buffer entry pointed outside its chunk, the flattened
    /// node/buffer counts disagreed with the schema, etc.). Signals corruption or
    /// a version/format mismatch rather than a plain stale descriptor.
    #[error("inconsistent batch layout: {0}")]
    Layout(&'static str),
}

/// Convenience alias for `Result<T, shm_arrow::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
