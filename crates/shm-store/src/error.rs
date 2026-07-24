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

    /// A [`TypedRef`](crate::TypedRef) envelope failed validation: its `magic` or
    /// `abi_version` did not match, or the chunk was too small to hold one
    /// (ADR-0007 G1).
    #[error("typed-ref envelope failed validation (bad magic/abi_version or too short)")]
    BadEnvelope,

    /// A [`ChunkDesc`](shm_core::ChunkDesc) handed to
    /// [`read_typed_ref`](crate::read_typed_ref) was not tagged with
    /// [`SCHEMA_TYPED_REF`](crate::SCHEMA_TYPED_REF): it is raw payload, not an
    /// envelope.
    #[error("descriptor is not a typed-ref envelope (schema_id {0} != SCHEMA_TYPED_REF)")]
    NotEnvelope(u32),

    /// A [`TypedRef`](crate::TypedRef) whose `kind` is
    /// [`RefKind::RawChunk`](crate::RefKind::RawChunk) (or otherwise carries no
    /// key) cannot be resolved against the keyed store (ADR-0007 G1).
    #[error("typed ref is not resolvable against the keyed store (raw / no key)")]
    NotResolvable,

    /// A [`resolve_and_pin`](crate::KeyedStore::resolve_and_pin) asked for a
    /// specific version that is not the entry's pinned current version.
    #[error("version mismatch: expected {expected}, entry is at {actual}")]
    VersionMismatch {
        /// The version the [`TypedRef`](crate::TypedRef) demanded.
        expected: u64,
        /// The version actually pinned (the entry's current version).
        actual: u64,
    },

    /// An error bubbled up from `shm-core`.
    #[error(transparent)]
    Core(#[from] shm_core::Error),

    /// An error bubbled up from `shm-arrow` (the envelope allocate/read seam).
    #[error(transparent)]
    Arrow(#[from] shm_arrow::Error),

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
