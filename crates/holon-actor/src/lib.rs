//! `holon-actor` — the smallest actor layer that proves *actors own no state,
//! memory nodes own no code* end-to-end across processes (ADR-0015).
//!
//! Composition, not substrate: an [`ActorSystem`] owns a `shm-runtime`
//! [`Node`](shm_runtime::Node) and hosts any number of actors, of any types,
//! routed by the envelope's `to`; its **mailbox** is the coordinator's built-in
//! `shm-task` queue (exactly-once claim, lease reap, parked wake); a message is
//! a 64-byte [`Envelope`](holon_core::Envelope) plus an inline POD body written
//! into one chunk of the keyed store's shared pool and carried as the task's
//! 24-byte `ChunkDesc` (the ADR-0007 G1 envelope-in-a-chunk pattern); state is
//! a keyed-store **cell** the handler pins zero-copy through
//! [`Cx::pin`]; an `ask` is `submit` + `wait`, its reply the task's result
//! descriptor (a second chunk, freed by the asker).
//!
//! # Per-message chunk accounting
//!
//! | chunk            | allocated by | freed by                                  |
//! |------------------|--------------|-------------------------------------------|
//! | request envelope | the asker    | the handler process, **after** `complete` |
//! | reply chunk      | the asker    | the asker, after the task went terminal   |
//!
//! Both chunks are the asker's: the request envelope names the reply chunk as
//! a [`LocalRef`](holon_core::LocalRef), the handler writes its reply *into*
//! it and completes the task with a zero result descriptor. The reply must not
//! ride the task slot's own result word: a completed slot is reusable capacity
//! the instant it completes (LIFO FREE stack — the very next submit from any
//! other client takes it), so a concurrent requester's `poll` sees
//! `StaleHandle` instead of its result. A stale handle after a wait is provably
//! terminal (`seq` only advances on a submit, which only pops a slot that went
//! terminal), so the asker then reads its own chunk: kind `Reply` = answered,
//! kind `Err` = the handler failed, still zeroed = never handled (reaped).
//!
//! Freeing the request only after `complete` is what keeps a `kill -9`
//! mid-handle safe: the queue's requeued task still names the chunk, so the
//! successor reads the *same* bytes and frees it after its own `complete`. A
//! handler whose `complete` returns `Lost` (its lease lapsed and the task was
//! redelivered) leaves the request to the new owner. Before writing the reply a
//! handler checks it has used less than half its lease; past that it abandons
//! the message (the reap redelivers it) rather than write into a chunk whose
//! asker may already have been answered by a successor and freed it — a
//! time fence standing in for the design's `epoch` until `holon-mem` owns it.
//! The one leak this ordering admits — death between `complete` and the free —
//! is a few instructions wide and bounded to one 256-byte chunk.

#![deny(missing_docs)]

pub mod chunk;
pub mod error;
pub mod system;

pub use chunk::{MessagePool, REPLY_CHUNK_BYTES};
pub use error::{Error, Result};
pub use system::{Actor, ActorRef, ActorSystem, CellRef, Cx, Pinned, DEFAULT_LEASE};
