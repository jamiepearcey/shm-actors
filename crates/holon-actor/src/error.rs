//! Error and result types for `holon-actor`.

use thiserror::Error;

/// Errors surfaced by the actor system, refs and cells.
#[derive(Debug, Error)]
pub enum Error {
    /// An envelope/payload/dispatch error (also what a failed handler returns).
    #[error(transparent)]
    Core(#[from] holon_core::Error),

    /// An error from the runtime host (`Node`, control socket, task queue handle).
    #[error(transparent)]
    Runtime(#[from] shm_runtime::Error),

    /// An error from the keyed store (cell open / pin / read).
    #[error(transparent)]
    Store(#[from] shm_store::Error),

    /// An error from `shm-core` (pool alloc / free).
    #[error(transparent)]
    Shm(#[from] shm_core::Error),

    /// An error from the task queue (submit / complete / poll).
    #[error(transparent)]
    Task(#[from] shm_task::Error),

    /// The ask's task reached `Failed`: the handler returned an error, or the
    /// lease reap exhausted the retry cap (every attempt died).
    #[error("ask failed (handler error or retries exhausted)")]
    Failed,

    /// The ask's task was cancelled.
    #[error("ask cancelled")]
    Cancelled,

    /// The ask completed with no reply body (the handler returned `Reply::None`).
    #[error("ask completed without a reply")]
    NoReply,

    /// The reply chunk did not match the ask (kind / correlation id).
    #[error("reply does not match the ask (kind {kind}, corr {got} != {expected})")]
    BadReply {
        /// The reply envelope's kind discriminant.
        kind: u16,
        /// The correlation id the reply carried.
        got: u64,
        /// The correlation id the ask used.
        expected: u64,
    },

    /// [`ActorSystem::run`](crate::ActorSystem::run) was called before
    /// [`spawn`](crate::ActorSystem::spawn).
    #[error("no actor spawned on this system")]
    NoActor,

    /// [`spawn`](crate::ActorSystem::spawn) was called twice with the same name.
    #[error("an actor with id {0:#x} is already spawned on this system")]
    DuplicateActor(u64),
}

/// The crate result alias.
pub type Result<T> = core::result::Result<T, Error>;
