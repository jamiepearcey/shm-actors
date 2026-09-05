//! `holon-net` — the network fronts of Holon (ADR-0017).
//!
//! One connection model, one reactor, two wire faces:
//!
//! - [`wire`] — the TCP frame is the 64-byte [`holon_core::Envelope`]
//!   verbatim, followed by `body_len` bytes. Control messages (subscribe,
//!   publish, ping) are envelopes addressed to `ActorId::NONE` with schemas in
//!   the reserved system range.
//! - [`http`] — a hand-rolled HTTP/1.1 face: asks and tells to actors,
//!   publish, SSE topic fan-out with `Last-Event-ID` resume, NDJSON long-poll.
//! - [`object`] — `GET /objects/{key}`: a pinned, streamed read of a keyed-store
//!   object, written in slices that respect the write high-water mark rather
//!   than copied whole into the connection buffer.
//! - [`gateway`] — the shard: one `poll(2)` reactor thread per shard over the
//!   listeners, every client socket, the task queue's done doorbell (reply
//!   completions) and the topic-event ring's doorbell. Asks are pipelined by
//!   construction; topic delivery is backpressured by cursor.
//!
//! There is no executor and no thread per connection: the substrate's asks
//! are already asynchronous (asker-owned reply chunk, ADR-0015), and the
//! doorbells are plain fds.

#![deny(missing_docs)]

pub mod client;
pub mod conn;
pub mod error;
pub mod gateway;
pub mod http;
pub mod object;
pub mod reactor;
pub mod tcp;
pub mod wire;

pub use error::{Error, Result};
pub use gateway::{Gateway, GatewayConfig, GatewayStats, DEFAULT_MAX_REPLY};
