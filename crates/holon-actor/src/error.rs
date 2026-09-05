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
    /// The topic side (notification ring / schema intern) could not be opened.
    #[error("topic side: {0}")]
    Topic(String),

    /// The object store rejected an operation, or its side could not be opened.
    #[error(transparent)]
    Object(#[from] holon_object::Error),

    /// The handler answered with an error envelope, or the task went terminal
    /// without an answer (retries exhausted).
    #[error("ask failed (handler error or retries exhausted)")]
    Failed,

    /// The ask's task was cancelled.
    #[error("ask cancelled")]
    Cancelled,

    /// The ask completed with no reply body (the handler returned `Reply::None`).
    #[error("ask completed without a reply")]
    NoReply,

    /// A reply body cap larger than one envelope can describe
    /// ([`MAX_REPLY_BODY`](holon_core::MAX_REPLY_BODY)), raised by
    /// [`ask_start_sized`](crate::ActorRef::ask_start_sized).
    ///
    /// The symmetric case — a *handler* whose
    /// [`Reply::Bytes`](holon_core::Reply::Bytes) over-runs the chunk the asker
    /// sized — is answered on the wire as an error envelope, so the asker sees
    /// [`Error::Failed`]: a truncated answer must never read as a whole one.
    #[error("reply cap {got} B exceeds the {cap} B one envelope can describe")]
    ReplyTooLarge {
        /// The cap that was asked for.
        got: usize,
        /// The largest a single-envelope reply body can be.
        cap: usize,
    },

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
