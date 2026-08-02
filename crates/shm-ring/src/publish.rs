//! The single-producer publish role.

use shm_core::ChunkDesc;

use crate::hooks::{NoopNotifier, Notifier};
use crate::ring::Ring;

/// The single-producer handle onto a [`Ring`].
///
/// There must be at most one `Publisher` per ring at a time (the ring's publish
/// path is single-producer). Publishing is wait-free and never blocks on slow
/// consumers; if any subscriber is parked, the injected [`Notifier`] is called
/// to wake them.
pub struct Publisher<N: Notifier = NoopNotifier> {
    ring: Ring,
    notifier: N,
}

impl Publisher<NoopNotifier> {
    /// Create a publisher with the no-op notifier (spin/yield subscribers).
    pub fn new(ring: Ring) -> Publisher<NoopNotifier> {
        Publisher {
            ring,
            notifier: NoopNotifier,
        }
    }
}

impl<N: Notifier> Publisher<N> {
    /// Create a publisher with a custom wake [`Notifier`].
    pub fn with_notifier(ring: Ring, notifier: N) -> Publisher<N> {
        Publisher { ring, notifier }
    }

    /// Publish a descriptor to every subscriber. Wait-free.
    ///
    /// Slow or lapped subscribers do not stall this call; they observe
    /// [`crate::Msg::Lagged`] later. Wakes parked subscribers via the notifier
    /// when needed.
    pub fn publish(&self, desc: ChunkDesc) {
        if self.ring.publish(desc) {
            self.notifier.notify();
        }
    }

    /// Borrow the underlying ring (e.g. to consult [`Ring::would_backpressure`]).
    pub fn ring(&self) -> &Ring {
        &self.ring
    }
}
