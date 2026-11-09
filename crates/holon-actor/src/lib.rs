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
//! | chunk            | allocated by | owned how                     | freed by                                          |
//! |------------------|--------------|-------------------------------|---------------------------------------------------|
//! | request envelope | the asker    | bare descriptor (`FREE`)      | the handler process, **after** `complete`         |
//! | reply chunk      | the asker    | `LOANED` + journaled          | the asker's [`ReplyRef`] guard, or the coordinator |
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
//! ## Why the reply chunk is loaned and journaled
//!
//! The asker `try_loan`s the chunk to its node's actor id and records it as an
//! `ENTRY_CHUNK_PIN` before submitting. Both halves are load-bearing:
//!
//! - The **journal** is what lets the coordinator reclaim the chunk when the
//!   asker dies mid-ask. `BorrowJournal::release` is an election, so the
//!   asker's own release and the coordinator's replay cannot both free it.
//! - The **loan** is what makes the journal record mean anything:
//!   `reclaim_one` dispatches on the chunk's state and reclaims nothing for a
//!   `FREE` chunk, which is what every bare message chunk is. Loan first, then
//!   record — the ordering `shm-stream`'s staging uses.
//!
//! The loan also makes the generation live: a reclaim's `drop_loan` bumps it,
//! so before writing, a handler re-reads the chunk's generation from the pool
//! and compares it with `Envelope::reply_gen`. A mismatch means the asker is
//! definitively gone and the task is failed rather than answered into a chunk
//! that now belongs to someone else. That is a real epoch check, and it
//! replaces the `lease / 2` clock heuristic this loop used to run.
//!
//! Freeing the request only after `complete` is what keeps a `kill -9`
//! mid-handle safe: the queue's requeued task still names the chunk, so the
//! successor reads the *same* bytes and frees it after its own `complete`. A
//! handler whose `complete` returns `Lost` (its lease lapsed and the task was
//! redelivered) leaves the request to the new owner.
//!
//! ## Residual leak, stated not hidden
//!
//! The **request** chunk of an ask whose task is never successfully claimed
//! (retries exhausted, or no worker ever runs) is freed by nobody: its
//! ownership deliberately transfers to the handler process, so the asker
//! cannot journal it without letting a coordinator reclaim free it under a
//! live handler mid-read. One chunk per such ask. Closing it needs a
//! coordinator-side sweep of the request descriptors of terminally-failed task
//! slots, which is not this slice.

#![deny(missing_docs)]

pub mod chunk;
pub mod error;
pub mod system;

pub use chunk::{MessagePool, DEFAULT_REPLY_BODY};
pub use error::{Error, Result};
pub use holon_core::MAX_REPLY_BODY;
pub use shm_task::SpinBudget;
pub use system::{
    Actor, ActorRef, ActorSystem, CellRef, Cx, PendingAsk, Pinned, ReplyRef, DEFAULT_LEASE,
    DEFAULT_SPIN_BUDGET,
};
