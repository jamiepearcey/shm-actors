//! Error type of the network fronts.

use thiserror::Error;

/// Errors of `holon-net`.
#[derive(Debug, Error)]
pub enum Error {
    /// A socket or fd operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The envelope ABI was violated.
    #[error(transparent)]
    Core(#[from] holon_core::Error),

    /// The actor layer rejected the operation.
    #[error(transparent)]
    Actor(#[from] holon_actor::Error),

    /// The topic log rejected the operation.
    #[error(transparent)]
    Topic(#[from] holon_topic::Error),

    /// The object store rejected the operation.
    #[error(transparent)]
    Object(#[from] holon_object::Error),

    /// The runtime rejected the operation.
    #[error(transparent)]
    Runtime(#[from] shm_runtime::Error),

    /// The shm substrate rejected the operation.
    #[error(transparent)]
    Shm(#[from] shm_core::Error),

    /// A `LocalRef` reached the wire (invariant I2: descriptors never cross
    /// a socket).
    #[error("envelope carries a LocalRef; descriptors never cross a socket")]
    LocalRefOnWire,

    /// The peer violated the wire protocol; the connection is closed.
    #[error("protocol: {0}")]
    Protocol(&'static str),

    /// An HTTP request could not be served; `status` is the reply code.
    #[error("http {status}: {reason}")]
    Http {
        /// HTTP status code.
        status: u16,
        /// Short reason.
        reason: &'static str,
    },
}

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;
