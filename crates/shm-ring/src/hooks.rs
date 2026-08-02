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

// ---- v0.2 doorbell-backed hooks (ADR-0002 stage D) ----

use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::time::Duration;

/// The default bounded [`DoorbellParker`] poll timeout.
///
/// A parked subscriber re-checks the ring at least this often even absent a
/// wakeup, so a subscriber whose publisher died (and thus never rang the
/// doorbell) still makes progress — it observes lag/shutdown rather than
/// blocking forever. Tens of milliseconds keeps idle CPU negligible while
/// bounding the "stolen doorbell byte" worst-case wake latency (see
/// [`shm_core::doorbell_park`]).
pub const DEFAULT_DOORBELL_TIMEOUT: Duration = Duration::from_millis(50);

/// A [`Notifier`] that rings a pipe-backed doorbell: it writes one byte to a
/// ring's doorbell **write-end**, waking every subscriber parked in
/// [`DoorbellParker`] on the matching read-end.
///
/// Holds a borrowed [`RawFd`]; the owner (the runtime's per-topic handle) keeps
/// the write-end alive for as long as any publish may occur. This is a cheap
/// `Copy` value so a wait-free [`Publisher`](crate::Publisher) can hold it
/// inline.
#[derive(Clone, Copy, Debug)]
pub struct DoorbellNotifier {
    write_fd: RawFd,
}

impl DoorbellNotifier {
    /// Wrap a doorbell write-end fd. The caller must keep the underlying fd
    /// open for the notifier's lifetime.
    pub fn new(write_fd: RawFd) -> DoorbellNotifier {
        DoorbellNotifier { write_fd }
    }
}

impl Notifier for DoorbellNotifier {
    fn notify(&self) {
        // Best-effort: a failed doorbell must never fail a publish. A dropped
        // wakeup is bounded-recovered by the parker's timeout re-check.
        let _ = shm_core::doorbell_ring(self.write_fd);
    }
}

/// A [`Parker`] that blocks in level-triggered `poll(2)` on a ring's doorbell
/// **read-end** with a bounded timeout, then drains it.
///
/// Owns the read-end [`OwnedFd`] (closed on drop). Because the ring's `recv`
/// registers the waiter and re-checks emptiness *before* calling `park`, and
/// the doorbell is level-triggered, a publish that raced the registration is
/// still observed: its byte persists in the pipe until drained, so the next
/// `poll` returns immediately. The bounded timeout guarantees liveness even if
/// a wakeup is missed or the publisher died.
#[derive(Debug)]
pub struct DoorbellParker {
    read: OwnedFd,
    timeout: Duration,
}

impl DoorbellParker {
    /// Park on a doorbell read-end with the [`DEFAULT_DOORBELL_TIMEOUT`].
    pub fn new(read: OwnedFd) -> DoorbellParker {
        DoorbellParker {
            read,
            timeout: DEFAULT_DOORBELL_TIMEOUT,
        }
    }

    /// Park on a doorbell read-end with a custom bounded timeout.
    pub fn with_timeout(read: OwnedFd, timeout: Duration) -> DoorbellParker {
        DoorbellParker { read, timeout }
    }

    /// The raw read-end fd (e.g. for diagnostics); ownership stays with `self`.
    pub fn read_fd(&self) -> RawFd {
        self.read.as_raw_fd()
    }
}

impl Parker for DoorbellParker {
    fn park(&self) {
        // Ignore the wake/timeout distinction: the caller (`recv`) re-checks the
        // ring either way, and an error here degrades to the timeout re-check.
        let _ = shm_core::doorbell_park(self.read.as_raw_fd(), self.timeout);
    }
}
