//! Error and `Result` types for `shm-artifact`.

/// Errors produced while committing, pinning, or reclaiming artifact versions.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error bubbled up from the `shm-core` substrate (allocation, bounds,
    /// stale descriptor, illegal control-word transition, ...).
    #[error(transparent)]
    Core(#[from] shm_core::Error),

    /// An error bubbled up from the `shm-arrow` batch layer (write/read of the
    /// data chunks that back a version).
    #[error(transparent)]
    Arrow(#[from] shm_arrow::Error),

    /// The exclusive write lease on the artifact is already held by another
    /// actor. A second [`Artifact::open_exclusive`](crate::Artifact::open_exclusive)
    /// fails fast with this rather than silently last-write-wins.
    #[error("artifact write lease is already held by another writer")]
    WriteLocked,

    /// An optimistic commit lost the race: the artifact's `current` version was
    /// not the `expected` value at the moment of the install CAS.
    #[error("optimistic commit conflict: expected version {expected}, found {actual}")]
    Conflict {
        /// The version the committer expected to supersede.
        expected: u64,
        /// The version actually installed when the CAS was attempted.
        actual: u64,
    },

    /// A feature is deliberately not implemented in this version (e.g. a
    /// [`Commit::Patch`](crate::Commit::Patch) commit, deferred to v0.2).
    #[error("unsupported in v0.1: {0}")]
    Unsupported(&'static str),

    /// An on-shm structure (the [`ArtifactHead`](crate::ArtifactHead) region or a
    /// [`VersionManifest`](crate::VersionManifest) chunk) did not carry the
    /// expected magic — the region was uninitialised or is not an artifact.
    #[error("bad magic: region is not an initialised artifact structure")]
    BadMagic,

    /// The requested version's chunks have already been reclaimed (its manifest
    /// chunk was recycled, or the version is no longer tracked). This surfaces a
    /// use-after-reclaim attempt as a clean error rather than a stale read.
    #[error("version is gone: its chunks have been reclaimed")]
    VersionGone,
}

/// Convenience alias for `Result<T, shm_artifact::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
