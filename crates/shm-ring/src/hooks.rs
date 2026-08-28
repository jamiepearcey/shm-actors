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

// ---- ADR-0011 (Holon P0.4) futex-backed hooks — Linux only ----
//
// The futex doorbell of record for the ABI-reserved `RingHeader.doorbell_seq`
// word (and `shm-task`'s identically reserved twin). Unlike the pipe doorbell
// there is no fd to grant: the wake word rides inside the already-mapped ring
// region, so a notify is one `fetch_add` + one `FUTEX_WAKE` and a park is one
// `FUTEX_WAIT` — no descriptor plumbing at all.
//
// Liveness argument (same shape as the pipe parker's): `recv` registers as a
// waiter and re-checks the ring BEFORE parking. A publish that lands entirely
// between that re-check and the parker's own seq load makes the parker wait on
// the *new* seq value — a missed wake — which the bounded timeout recovers: the
// parker returns at worst one timeout later and `recv` observes the advanced
// head. A publish that lands after the seq load fails the futex value check
// (`EAGAIN`) or wakes the waiter; either way no wake is lost. A two-phase
// prepare/park API that closes the window entirely is deliberately deferred to
// Holon Phase 1's mailbox work (ACTOR-FRAMEWORK-DESIGN §5) — it would need a
// new `Parker` contract and the pipe parker already documents this exact
// bounded-recovery trade.

#[cfg(target_os = "linux")]
mod futex_hooks {
    use core::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::{Notifier, Parker, DEFAULT_DOORBELL_TIMEOUT};

    /// A [`Notifier`] that bumps a shared futex word and `FUTEX_WAKE`s every
    /// waiter parked on it (Linux only; ADR-0011).
    ///
    /// Holds a raw pointer into the mapped region (like
    /// [`DoorbellNotifier`](super::DoorbellNotifier) holds a borrowed fd) so a
    /// wait-free publisher can carry it inline as a `Copy` value.
    #[derive(Clone, Copy, Debug)]
    pub struct FutexNotifier {
        word: *const AtomicU32,
    }

    // SAFETY: the pointer targets an atomic in a `MAP_SHARED` mapping the
    // constructor's contract keeps alive; all access is atomic.
    unsafe impl Send for FutexNotifier {}
    // SAFETY: as above — shared access is atomic-only.
    unsafe impl Sync for FutexNotifier {}

    impl FutexNotifier {
        /// Wrap a ring's doorbell word (e.g.
        /// [`Ring::doorbell_word`](crate::Ring::doorbell_word)).
        ///
        /// # Safety
        ///
        /// The mapping containing `word` must stay mapped for the notifier's
        /// entire lifetime (the notifier holds a raw pointer to it).
        pub unsafe fn new(word: &AtomicU32) -> FutexNotifier {
            FutexNotifier { word }
        }
    }

    impl Notifier for FutexNotifier {
        fn notify(&self) {
            // SAFETY: constructor contract — the mapping outlives `self`.
            let word = unsafe { &*self.word };
            word.fetch_add(1, Ordering::Release);
            // Best-effort broadcast: a failed wake must never fail a publish (a
            // dropped wakeup is bounded-recovered by the parker's timeout).
            let _ = shm_core::futex_wake(word, i32::MAX);
        }
    }

    /// A [`Parker`] that `FUTEX_WAIT`s on a shared doorbell word with a bounded
    /// timeout (Linux only; ADR-0011).
    ///
    /// Reads the word, then waits only while it still holds that value: a
    /// notify between the read and the wait fails the kernel's value check and
    /// returns immediately. The bounded timeout covers the one remaining miss
    /// window (a notify completing entirely before the read) — see the module
    /// note above.
    #[derive(Clone, Copy, Debug)]
    pub struct FutexParker {
        word: *const AtomicU32,
        timeout: Duration,
    }

    // SAFETY: as for `FutexNotifier` — atomic-only access to a mapping the
    // constructor's contract keeps alive.
    unsafe impl Send for FutexParker {}
    // SAFETY: as above.
    unsafe impl Sync for FutexParker {}

    impl FutexParker {
        /// Park on a doorbell word with the [`DEFAULT_DOORBELL_TIMEOUT`].
        ///
        /// # Safety
        ///
        /// The mapping containing `word` must stay mapped for the parker's
        /// entire lifetime (the parker holds a raw pointer to it).
        pub unsafe fn new(word: &AtomicU32) -> FutexParker {
            FutexParker {
                word,
                timeout: DEFAULT_DOORBELL_TIMEOUT,
            }
        }

        /// Park on a doorbell word with a custom bounded timeout.
        ///
        /// # Safety
        ///
        /// As for [`FutexParker::new`].
        pub unsafe fn with_timeout(word: &AtomicU32, timeout: Duration) -> FutexParker {
            FutexParker { word, timeout }
        }
    }

    impl Parker for FutexParker {
        fn park(&self) {
            // SAFETY: constructor contract — the mapping outlives `self`.
            let word = unsafe { &*self.word };
            let observed = word.load(Ordering::Acquire);
            // Wake/timeout/mismatch are all treated alike: the caller (`recv`)
            // re-checks the ring either way, and an error degrades to the
            // bounded-timeout re-check — same contract as `DoorbellParker`.
            let _ = shm_core::futex_wait(word, observed, Some(self.timeout));
        }
    }
}

#[cfg(target_os = "linux")]
pub use futex_hooks::{FutexNotifier, FutexParker};
