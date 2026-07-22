//! Runtime error and `Result` types.
//!
//! The runtime composes four lower crates plus real OS syscalls (UDS,
//! `sendmsg`/`recvmsg`, `SCM_RIGHTS`), so its [`Error`] flattens all of their
//! failure modes into one enum a coordinator/actor caller can match on.

use shm_core::Error as CoreError;

/// Errors produced by the `shm-runtime` coordinator and actor host.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error bubbled up from `shm-core` (segments, pools, ctrl, journal).
    #[error(transparent)]
    Core(#[from] CoreError),

    /// An error from the Arrow write/read path.
    #[error("arrow: {0}")]
    Arrow(String),

    /// An error from the pub/sub ring.
    #[error("ring: {0}")]
    Ring(String),

    /// A standard-library I/O error (UDS accept/connect/read/write).
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// An error from an underlying `nix` syscall (`sendmsg`, `recvmsg`, ...).
    #[error(transparent)]
    Nix(#[from] nix::Error),

    /// A control-protocol frame was malformed or truncated.
    #[error("protocol decode error: {0}")]
    Protocol(&'static str),

    /// The peer closed the control connection (clean EOF).
    #[error("control connection closed by peer")]
    PeerClosed,

    /// A control message expected accompanying file descriptors that never
    /// arrived (e.g. a `Registered` reply without its segment fds).
    #[error("expected {expected} passed fd(s), received {received}")]
    MissingFds {
        /// How many descriptors the message required.
        expected: usize,
        /// How many actually arrived in the control message.
        received: usize,
    },

    /// The coordinator rejected a request (its message is carried inline).
    #[error("coordinator rejected request: {0}")]
    Rejected(String),

    /// A referenced actor, topic, or chunk was not known to the coordinator.
    #[error("not found: {0}")]
    NotFound(&'static str),
}

/// Convenience alias for `Result<T, shm_runtime::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

impl From<shm_arrow::Error> for Error {
    fn from(e: shm_arrow::Error) -> Self {
        Error::Arrow(e.to_string())
    }
}

impl From<shm_ring::Error> for Error {
    fn from(e: shm_ring::Error) -> Self {
        Error::Ring(e.to_string())
    }
}
