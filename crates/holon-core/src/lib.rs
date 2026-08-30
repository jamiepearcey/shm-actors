//! `holon-core` — the ABI of the Holon actor layer (ADR-0015, design §4):
//! the 64-byte [`Envelope`], the POD [`Payload`] contract, the host-scoped
//! [`LocalRef`], and the zero-`dyn` [`Dispatch`] table.
//!
//! Nothing here touches shared memory or the OS. The crate defines *what* a
//! message is; `holon-actor` decides *where* it lives (today: a chunk in the
//! keyed store's shared pool, carried through `shm-task` as a 24-byte
//! `ChunkDesc` — the ADR-0007 G1 envelope-in-a-chunk pattern).
//!
//! # Message chunk layout
//!
//! ```text
//! offset 0   : Envelope (64 bytes, magic-validated)
//! offset 64  : inline body, `Envelope::body_len` bytes (FLAG_INLINE_PAYLOAD)
//! ```
//!
//! A message whose payload does not fit inline names it through
//! [`Envelope::payload`] as a [`LocalRef`] instead (`FLAG_LOCAL_REF`); the demo
//! never needs that path but the type exists so the compiler, not a reviewer,
//! keeps a host-scoped descriptor off the wire.

#![deny(missing_docs)]

pub mod dispatch;
pub mod envelope;
pub mod error;
pub mod payload;

pub use dispatch::{Dispatch, Handler};
pub use envelope::{
    decode_message, encode_message, ActorId, Envelope, MessageKind, ENVELOPE_ABI_VERSION,
    ENVELOPE_MAGIC, ENVELOPE_SIZE, FLAG_INLINE_PAYLOAD, FLAG_LOCAL_REF, FLAG_NO_REPLY,
    SCHEMA_ENVELOPE,
};
pub use error::{Error, Result};
pub use payload::{InlineBody, LocalRef, Payload, Reply, MAX_INLINE_BODY};
