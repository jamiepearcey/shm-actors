//! Error and `Result` types for `shm-stream`.

/// Errors produced while staging batches into, or committing/aborting, a
/// [`StreamWriter`](crate::StreamWriter).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error bubbled up from the `shm-core` substrate (chunk allocation, the
    /// borrow journal, an illegal control-word transition, ...).
    #[error(transparent)]
    Core(#[from] shm_core::Error),

    /// An error bubbled up from the `shm-arrow` batch layer while serializing an
    /// appended [`RecordBatch`](arrow_array::RecordBatch) into a chunk.
    #[error(transparent)]
    Arrow(#[from] shm_arrow::Error),

    /// An error bubbled up from the `shm-artifact` version machinery at
    /// [`commit`](crate::StreamWriter::commit) time — most importantly
    /// [`shm_artifact::Error::Conflict`] (optimistic race lost),
    /// [`shm_artifact::Error::WriteLocked`] (exclusive lease already held, at
    /// [`open`](crate::StreamWriter::open)), and [`shm_artifact::Error::Fenced`]
    /// (this exclusive writer was declared dead and its lease fenced — item K).
    #[error(transparent)]
    Artifact(#[from] shm_artifact::Error),

    /// A batch appended to the stream did not share the interned schema of the
    /// batches already staged. Every batch installed as one version must carry
    /// the same Arrow schema (they concatenate on read); mixing schemas within a
    /// single transaction is rejected.
    #[error("appended batch schema ({appended}) differs from the stream's schema ({expected})")]
    SchemaMismatch {
        /// The interned schema id the stream was pinned to by its first batch.
        expected: u32,
        /// The interned schema id of the offending appended batch.
        appended: u32,
    },

    /// A ranged [`Commit::Patch`](shm_artifact::Commit::Patch) stream was
    /// requested. Patch commits are deferred to v0.3 (ADR-0002); rejected at
    /// [`open`](crate::StreamWriter::open) so no chunks are ever staged for one.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
}

/// Convenience alias for `Result<T, shm_stream::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
