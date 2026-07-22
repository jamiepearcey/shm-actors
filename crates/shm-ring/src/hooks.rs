//! Injected wake/park hooks, keeping the ring decoupled from any `Platform`.
//!
//! The ring's fast paths are pure user-space atomics. Blocking is a *policy*
//! layered on top: a [`Notifier`] the producer calls to wake parked
//! subscribers, and a [`Parker`] a subscriber calls to sleep. v0.1 ships
//! spin/yield defaults so `shm-ring` needs no OS primitives; `shm-runtime` will
//! inject a doorbell-backed pair over [`shm_core::Platform`].

/// A hook the producer calls (when subscribers are parked) to wake them.
pub trait Notifier {
    /// Wake any parked subscribers. Must be safe to call spuriously.
    fn notify(&self);
}

/// A hook a subscriber calls to block until a publish may have occurred.
pub trait Parker {
    /// Park the caller. May return spuriously; the caller re-checks the ring.
    /// Only invoked after the subscriber has registered itself as a waiter.
    fn park(&self);
}

/// A no-op notifier: pairs with a spinning/yielding [`Parker`].
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopNotifier;

impl Notifier for NoopNotifier {
    fn notify(&self) {}
}

/// A [`Parker`] that yields the CPU. Combined with the subscriber's bounded
/// spin, this is the zero-dependency v0.1 blocking strategy: correct (the ring
/// is polled to completion) without any OS wait primitive.
#[derive(Clone, Copy, Debug, Default)]
pub struct YieldParker;

impl Parker for YieldParker {
    fn park(&self) {
        std::thread::yield_now();
    }
}
