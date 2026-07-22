//! Error and `Result` types for `shm-task`.

/// Errors produced while laying out, attaching to, or operating an MPMC task
/// queue.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error bubbled up from the `shm-core` substrate.
    #[error(transparent)]
    Core(#[from] shm_core::Error),

    /// The requested capacity was zero or not a power of two.
    #[error("task queue capacity must be a non-zero power of two, got {0}")]
    BadCapacity(u32),

    /// The provided region was too small to hold the header + slot array.
    #[error("task queue region too small: need {need} bytes, have {have}")]
    RegionTooSmall {
        /// Bytes required for the header and slot array.
        need: usize,
        /// Bytes available in the provided region.
        have: usize,
    },

    /// The base pointer was not 8-byte aligned (required for the atomics).
    #[error("task queue base pointer is not 8-byte aligned")]
    Misaligned,

    /// A region being attached did not carry the task-queue magic.
    #[error("bad task queue magic (not a shm-task region)")]
    BadMagic,

    /// Every slot was occupied by a live (`QUEUED`/`CLAIMED`) task, so no fresh
    /// task could be enqueued. Intentional backpressure.
    #[error("task queue full: no reusable slot")]
    QueueFull,

    /// A handle referenced a slot whose `seq` no longer matches: the slot was
    /// reused for a newer task, so the handle's task is gone (the ABA guard).
    #[error("stale task handle: slot was reused for a newer task")]
    StaleHandle,

    /// A worker tried to `complete`/`fail` a task it no longer holds: its lease
    /// lapsed and the coordinator's [`reap`](crate::TaskQueue::reap) requeued or
    /// failed the task (another attempt now owns it). At-least-once in action.
    #[error("claim lost: the task was reaped and re-dispatched")]
    Lost,
}

/// Convenience alias for `Result<T, shm_task::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
