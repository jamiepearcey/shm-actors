//! The multi-consumer subscribe role: private cursor, `try_recv`/`recv`.

use shm_core::ChunkDesc;

use crate::hooks::{Parker, YieldParker};
use crate::ring::{Ring, ReadOutcome};

/// The number of spin iterations `recv` makes before parking.
const SPIN_LIMIT: u32 = 256;

/// One delivery from the ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Msg {
    /// A published descriptor.
    Sample(ChunkDesc),
    /// The subscriber was lapped and missed `n` messages; its cursor has been
    /// resynced to the oldest still-live message.
    Lagged(u64),
}

/// A broadcast subscriber with a private cursor.
///
/// Each subscriber advances independently, so N subscribers each receive every
/// message (a true broadcast). A subscriber that falls more than the ring's
/// capacity behind is lapped and observes [`Msg::Lagged`], then resynchronises.
///
/// A subscriber may optionally be **reliable**: it registers its cursor in the
/// ring header so a runtime can apply backpressure (see
/// [`Ring::would_backpressure`](crate::Ring::would_backpressure)). Reliability
/// changes nothing about the ring's non-blocking behaviour; it only exposes the
/// cursor. The registration is released on drop.
pub struct Subscriber<P: Parker = YieldParker> {
    ring: Ring,
    cursor: u64,
    parker: P,
    reliable: Option<usize>,
}

impl Subscriber<YieldParker> {
    /// Subscribe from the current head with the default yield parker.
    ///
    /// The subscriber only sees messages published after this call.
    pub fn new(ring: Ring) -> Subscriber<YieldParker> {
        Self::with_parker(ring, YieldParker)
    }

    /// Subscribe from sequence `0` (replays whatever is still live in the ring).
    pub fn from_start(ring: Ring) -> Subscriber<YieldParker> {
        Self::from_seq(ring, 0)
    }

    /// Subscribe from an explicit sequence — resume a consumer that already knows
    /// how far it read.
    ///
    /// `seq` is the next sequence to deliver, matching [`cursor`](Subscriber::cursor),
    /// so a consumer resumes with `from_seq(ring, last_seen + 1)`. Both
    /// [`new`](Subscriber::new) and [`from_start`](Subscriber::from_start) are
    /// special cases (`ring.head()` and `0`).
    ///
    /// The cursor is not clamped, because the ring already resolves both ends:
    /// a `seq` that has been lapped out yields [`Msg::Lagged`] on the first read
    /// and resyncs to the oldest still-live message, and a `seq` at or beyond the
    /// head simply blocks until the producer reaches it. A caller that must
    /// distinguish "replayed from where I asked" from "resynced after a lap"
    /// should check for `Msg::Lagged` rather than pre-validating `seq`.
    pub fn from_seq(ring: Ring, seq: u64) -> Subscriber<YieldParker> {
        Self::from_seq_with_parker(ring, YieldParker, seq)
    }
}

impl<P: Parker> Subscriber<P> {
    /// Subscribe from the current head with a custom [`Parker`].
    pub fn with_parker(ring: Ring, parker: P) -> Subscriber<P> {
        let cursor = ring.head();
        Subscriber { ring, cursor, parker, reliable: None }
    }

    /// Subscribe from sequence `0` (replaying whatever is still live) with a
    /// custom [`Parker`] — the [`from_start`](Subscriber::from_start) counterpart
    /// for a doorbell-backed subscriber.
    pub fn from_start_with_parker(ring: Ring, parker: P) -> Subscriber<P> {
        Self::from_seq_with_parker(ring, parker, 0)
    }

    /// Subscribe from an explicit sequence with a custom [`Parker`] — the
    /// [`from_seq`](Subscriber::from_seq) counterpart for a doorbell-backed
    /// subscriber. See `from_seq` for the out-of-range semantics.
    pub fn from_seq_with_parker(ring: Ring, parker: P, seq: u64) -> Subscriber<P> {
        let mut s = Self::with_parker(ring, parker);
        s.cursor = seq;
        s
    }

    /// Promote this subscriber to **reliable**, publishing its cursor into the
    /// ring so the producer side can observe how far behind it is.
    ///
    /// Returns [`crate::Error::ReliableTableFull`] if the fixed table is full.
    pub fn make_reliable(&mut self) -> crate::Result<()> {
        if self.reliable.is_none() {
            let idx = self.ring.register_reliable(self.cursor)?;
            self.reliable = Some(idx);
        }
        Ok(())
    }

    /// Whether this subscriber is registered as reliable.
    pub fn is_reliable(&self) -> bool {
        self.reliable.is_some()
    }

    /// The subscriber's current cursor (next sequence it will read).
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The ring this subscriber reads from.
    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    /// Publish the current cursor to the reliable table (if reliable). Called
    /// automatically after every successful advance.
    fn sync_reliable(&self) {
        if let Some(idx) = self.reliable {
            self.ring.update_reliable(idx, self.cursor);
        }
    }

    /// Non-blocking read. Returns `Some(Msg)` if a sample or lag is available,
    /// or `None` if the subscriber has caught up (would block).
    pub fn try_recv(&mut self) -> Option<Msg> {
        match self.ring.read_at(self.cursor) {
            ReadOutcome::Sample(desc) => {
                self.cursor += 1;
                self.sync_reliable();
                Some(Msg::Sample(desc))
            }
            ReadOutcome::Lagged { skipped, new_cursor } => {
                self.cursor = new_cursor;
                self.sync_reliable();
                Some(Msg::Lagged(skipped))
            }
            ReadOutcome::Empty => None,
        }
    }

    /// Blocking read: spin a bounded number of times, then register as a waiter
    /// and [`Parker::park`] until a message (or a lag notice) is available.
    ///
    /// Never misses a wakeup: after registering as a waiter it re-polls before
    /// parking, so a publish that raced the registration is still observed.
    pub fn recv(&mut self) -> Msg {
        loop {
            for _ in 0..SPIN_LIMIT {
                if let Some(msg) = self.try_recv() {
                    return msg;
                }
                core::hint::spin_loop();
            }
            self.ring.add_waiter();
            // Re-check after publishing our interest to avoid a lost wakeup.
            if let Some(msg) = self.try_recv() {
                self.ring.remove_waiter();
                return msg;
            }
            self.parker.park();
            self.ring.remove_waiter();
        }
    }
}

impl<P: Parker> Drop for Subscriber<P> {
    fn drop(&mut self) {
        if let Some(idx) = self.reliable.take() {
            self.ring.release_reliable(idx);
        }
    }
}
