//! `shm-task` — an MPMC **task queue** over `shm-core` chunks: the 3rd primitive
//! of the `shm-actors` substrate (the work-queue counterpart to `shm-ring`'s
//! pub/sub broadcast).
//!
//! Many submitters enqueue task descriptors; many workers each try to *claim* a
//! task; a per-slot CAS state machine (`QUEUED→CLAIMED→DONE`) makes claiming
//! **exactly-once**. Only the 24-byte [`ChunkDesc`](shm_core::ChunkDesc) request
//! and result descriptors ride in the slot — payload stays in pool chunks. The
//! correlation id **is** the slot's `{slot_idx, seq}` ([`TaskHandle`]); there is
//! no separate table.
//!
//! # Shape
//!
//! - [`TaskQueue`] — the shared handle over a caller-provided region
//!   ([`TaskQueue::init`] / [`TaskQueue::attach`]); see [`queue`] for the frozen
//!   [`TaskQueueHeader`]/[`TaskSlot`] ABI. All roles operate through it.
//! - **Submit** — [`TaskQueue::submit`] enqueues a request and returns a
//!   [`TaskHandle`]; rings the injected work doorbell iff idle workers are parked.
//! - **Work** — [`TaskQueue::claim`] / [`TaskQueue::claim_blocking`] hand back a
//!   [`ClaimedTask`] exposing the request, a cooperative
//!   [`is_cancelled`](ClaimedTask::is_cancelled) poll, and
//!   [`complete`](ClaimedTask::complete)/[`fail`](ClaimedTask::fail).
//! - **Request** — [`TaskQueue::poll`] / [`TaskQueue::wait`] observe a task's
//!   [`TaskStatus`]/[`Outcome`], re-validating `seq` (the ABA/stale-handle guard).
//! - **Reap** — [`TaskQueue::reap`] resets a lapsed `CLAIMED` claim to `QUEUED`
//!   (at-least-once) up to a retry cap, then fails it — the lease machinery.
//!
//! Blocking reuses `shm-ring`'s [`Notifier`](shm_ring::Notifier)/
//! [`Parker`](shm_ring::Parker) hooks: workers park on a work doorbell, requesters
//! park on a done doorbell.

#![deny(missing_docs)]

pub mod error;
pub mod queue;

pub use error::{Error, Result};
pub use queue::{
    required_bytes, slots_offset, ClaimedTask, Outcome, ReapReport, TaskHandle, TaskQueue,
    TaskQueueHeader, TaskSlot, TaskStatus, CANCELLED, CLAIMED, DEFAULT_MAX_RETRIES, DONE, EMPTY,
    FAILED, QUEUED, TASK_MAGIC,
};

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time in nanoseconds since the Unix epoch.
///
/// A convenience for building submit `deadline_nanos` and the `now` passed to
/// [`TaskQueue::reap`]; both must share one clock domain, and this provides a
/// single process-agnostic one. (A v0.3 Linux fast path may switch to
/// `CLOCK_MONOTONIC`; the deadline contract is unchanged.)
pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
