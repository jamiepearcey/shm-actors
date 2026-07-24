//! Error and result types for `shm-store`.

use thiserror::Error;

/// Errors surfaced by the keyed store.
#[derive(Debug, Error)]
pub enum Error {
    /// A key exceeded the [`MAX_KEY_LEN`](crate::MAX_KEY_LEN) byte cap.
    #[error("key length {0} exceeds the {max}-byte cap", max = crate::MAX_KEY_LEN)]
    KeyTooLong(usize),

    /// No live catalog entry exists for the requested key / key id.
    #[error("no live store entry for the key")]
    NotFound,

    /// The catalog's fixed slot table is full (append-only capacity reached).
    #[error("store catalog is full ({0} slots)")]
    CatalogFull(u32),

    /// The catalog segment's magic did not validate.
    #[error("store catalog segment has a bad or missing magic")]
    BadCatalog,

    /// A supplied [`RefKind`](crate::RefKind) discriminant was out of range.
    #[error("unknown ref kind {0}")]
    BadKind(u16),

    /// An error bubbled up from `shm-core`.
    #[error(transparent)]
    Core(#[from] shm_core::Error),

    /// An error bubbled up from `shm-artifact`.
    #[error(transparent)]
    Artifact(#[from] shm_artifact::Error),

    /// The injected [`KeyResolver`](crate::KeyResolver) failed to intern a key
    /// (e.g. a UDS round-trip to the coordinator failed).
    #[error("key interning failed: {0}")]
    Intern(String),
}

/// The crate result alias.
pub type Result<T> = core::result::Result<T, Error>;
