//! `shm-ring` — a single-producer, multi-consumer **broadcast** ring over
//! `shm-core` chunks.
//!
//! Publishers move only the 24-byte [`ChunkDesc`](shm_core::ChunkDesc); the ring
//! carries descriptors, never payload. It is the pub/sub fabric of the
//! substrate: every subscriber independently sees every published descriptor
//! until it is lapped, at which point it observes [`Msg::Lagged`] and resyncs.
//!
//! # Shape
//!
//! - [`Ring`] — the shared handle over a caller-provided region ([`Ring::init`]
//!   / [`Ring::attach`]); see [`ring`] for the frozen [`RingHeader`]/[`Slot`]
//!   ABI. Publish is **wait-free** and never blocks on a slow consumer.
//! - [`Publisher`] — the single-producer role; wakes parked subscribers through
//!   an injected [`Notifier`].
//! - [`Subscriber`] — a private-cursor consumer with non-blocking [`try_recv`]
//!   and blocking [`recv`] (bounded spin, then an injected [`Parker`]).
//! - **Reliable mode** — a subscriber can register its cursor in a fixed table
//!   in the ring header so a runtime can observe lag and apply backpressure via
//!   [`Ring::would_backpressure`] / [`Ring::min_reliable_cursor`]. The ring
//!   itself still never blocks.
//!
//! [`try_recv`]: Subscriber::try_recv
//! [`recv`]: Subscriber::recv

#![deny(missing_docs)]

pub mod error;
pub mod hooks;
pub mod publish;
pub mod ring;
pub mod subscribe;

pub use error::{Error, Result};
pub use hooks::{
    DoorbellNotifier, DoorbellParker, NoopNotifier, Notifier, Parker, YieldParker,
    DEFAULT_DOORBELL_TIMEOUT,
};
pub use publish::Publisher;
pub use ring::{
    required_bytes, slots_offset, Ring, RingHeader, Slot, MAX_RELIABLE, RELIABLE_UNUSED, RING_MAGIC,
};
pub use subscribe::{Msg, Subscriber};
