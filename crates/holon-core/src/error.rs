//! Error and result types for `holon-core`.

use thiserror::Error;

/// Errors surfaced by the envelope/payload/dispatch layer — and the error type
/// an actor handler returns (a failed handler fails the task; the asker sees
/// [`Outcome::Failed`](shm_core::ChunkDesc) as an error).
#[derive(Debug, Error)]
pub enum Error {
    /// An [`Envelope`](crate::Envelope) failed validation: bad magic / ABI
    /// version, or the buffer was too short to hold one.
    #[error("envelope failed validation (bad magic/abi_version or too short)")]
    BadEnvelope,

    /// A chunk descriptor was not tagged [`SCHEMA_ENVELOPE`](crate::SCHEMA_ENVELOPE).
    #[error("descriptor is not an envelope chunk (schema_id {0} != SCHEMA_ENVELOPE)")]
    NotEnvelope(u32),

    /// The envelope's `kind` discriminant is not a [`MessageKind`](crate::MessageKind).
    #[error("unknown message kind {0}")]
    BadKind(u16),

    /// A payload's byte length did not match its POD size.
    #[error("payload size mismatch: expected {expected} bytes, got {got}")]
    BadPayload {
        /// `size_of::<P>()`.
        expected: usize,
        /// The body length actually present.
        got: usize,
    },

    /// A payload's schema id did not match the envelope's.
    #[error("schema mismatch: expected {expected}, envelope carries {got}")]
    SchemaMismatch {
        /// The schema the caller asked for.
        expected: u32,
        /// The schema the envelope names.
        got: u32,
    },

    /// An inline body exceeds [`MAX_INLINE_BODY`](crate::MAX_INLINE_BODY) (or the
    /// chunk it must fit in).
    #[error("inline body of {len} bytes exceeds the {max}-byte cap")]
    BodyTooLarge {
        /// The body length requested.
        len: usize,
        /// The cap that rejected it.
        max: usize,
    },

    /// No handler is registered for the envelope's schema id.
    #[error("no handler registered for schema id {0}")]
    UnknownSchema(u32),

    /// The envelope was addressed to an actor this system does not host.
    #[error("no actor {0:#x} in this system")]
    NoSuchActor(u64),

    /// A cell operation (open / pin / read) failed inside a handler.
    #[error("cell: {0}")]
    Cell(String),

    /// An application-level handler failure.
    #[error("handler: {0}")]
    Handler(String),
}

/// The crate result alias.
pub type Result<T> = core::result::Result<T, Error>;
