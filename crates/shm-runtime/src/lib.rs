//! `shm-runtime` — the coordinator + actor host that ties the substrate into a
//! running system: UDS control plane, `SCM_RIGHTS` fd passing, coordinator
//! leases, and crash reclamation via per-actor borrow-journal replay.
//!
//! This crate is the capstone of the v0.1 **walking skeleton** (ADR-0001, "First
//! Vertical Slice"): a [`Coordinator`] process grants a shared-memory segment to
//! actor processes over a Unix domain socket; a producer [`Node`] loans a chunk,
//! writes a `RecordBatch` via `shm-arrow`, and publishes its `ChunkDesc` on an
//! `shm-ring` topic; a consumer `Node` reconstructs the batch **zero-copy** and
//! records a pin in its borrow journal. When the consumer is `kill -9`ed while
//! holding the pin, the coordinator's lease expiry replays the dead actor's
//! journal and reclaims the chunk (bumping its generation), after which a stale
//! descriptor fails [`ChunkCtrl::validate`](shm_core::ChunkCtrl::validate).
//!
//! # Architecture
//!
//! - **Control plane ([`Coordinator`]).** Owns the payload segment + pool, each
//!   topic's ring segment, and one borrow-journal segment per actor. It passes
//!   segment fds to actors ([`uds`]) and never touches the payload data path.
//! - **Data plane ([`Node`]).** An actor maps the granted segments and drives
//!   loan / publish / subscribe / pin directly in shared memory.
//! - **Leases.** Actors renew a lease with periodic heartbeats over the control
//!   socket; a missed deadline — not a closed socket — declares death, so the
//!   same path handles clean exits, hangs, and `kill -9`.
//!
//! # Management-plane layout (v0.1)
//!
//! The shared control state is split across segments the coordinator creates and
//! grants:
//!
//! | segment             | id                            | holds |
//! |---------------------|-------------------------------|-------|
//! | payload             | `seg_base`                    | the [`Pool`](shm_core::Pool) (co-located [`ChunkCtrl`](shm_core::ChunkCtrl) array) + chunk payloads |
//! | ring (per topic)    | `seg_base + 1 + topic_index`  | one [`Ring`](shm_ring::Ring) region |
//! | journal (per actor) | `seg_base + 1024 + actor_id`  | that actor's [`BorrowJournal`](shm_core::BorrowJournal) |
//!
//! The lease table and topic/actor registry live in the coordinator's process
//! (not shared memory) since only the coordinator reads them; a consolidated
//! single management segment with sub-journals is a v0.2 layout optimization.

#![deny(missing_docs)]

pub mod config;
pub mod coordinator;
pub mod demo;
pub mod error;
pub mod node;
pub mod protocol;
pub mod uds;

pub use config::RuntimeConfig;
pub use coordinator::{ChunkSnapshot, Coordinator};
pub use error::{Error, Result};
pub use node::{Node, Pin};
pub use protocol::{Request, Response};
