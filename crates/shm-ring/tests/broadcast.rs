//! Integration tests for `shm-ring`: real `Segment`, threads sharing one map.
//!
//! The trivial payload convention: `ChunkDesc.offset` carries the sequence
//! counter, so a delivered `Msg::Sample(desc)` is identified by `desc.offset`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;

use shm_core::{ChunkDesc, Segment};
use shm_ring::{required_bytes, Msg, Publisher, Ring, Subscriber};

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    48_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

/// An auto-unlinked segment sized to hold a ring of `capacity` slots.
struct Fixture {
    id: u32,
    segment: Arc<Segment>,
}

impl Fixture {
    fn new(capacity: u32) -> Fixture {
        let id = next_segment_id();
        let _ = Segment::unlink_by_id(id);
        // Header(64) + ring bytes, generously rounded up.
        let size = (required_bytes(capacity) + 4096).next_power_of_two();
        let segment = Segment::create(id, size).expect("create segment");
        Fixture {
            id,
            segment: Arc::new(segment),
        }
    }

    /// Initialize a ring in the segment payload and return the handle.
    fn init_ring(&self, capacity: u32) -> Ring {
        // SAFETY: the payload region stays mapped for the fixture's lifetime and
        // no other party initializes it concurrently.
        unsafe {
            Ring::init(
                self.segment.payload_ptr(),
                self.segment.payload_len(),
                capacity,
            )
            .expect("init ring")
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = Segment::unlink_by_id(self.id);
    }
}

/// A trivial descriptor whose `offset` field carries the sequence counter.
fn counter_desc(i: u64) -> ChunkDesc {
    ChunkDesc {
        offset: i as u32,
        ..ChunkDesc::ZERO
    }
}

#[test]
fn single_subscriber_gets_all_in_order() {
    let fx = Fixture::new(64);
    let ring = fx.init_ring(64);

    let publisher = Publisher::new(ring.clone());
    let mut sub = Subscriber::from_start(ring.clone());

    let n = 40u64; // < capacity, so nothing is lapped
    for i in 0..n {
        publisher.publish(counter_desc(i));
    }

    for i in 0..n {
        match sub.try_recv() {
            Some(Msg::Sample(desc)) => assert_eq!(desc.offset as u64, i),
            other => panic!("expected Sample({i}), got {other:?}"),
        }
    }
    assert!(sub.try_recv().is_none(), "ring should be drained");
}

#[test]
fn two_subscribers_each_get_everything() {
    let fx = Fixture::new(64);
    let ring = fx.init_ring(64);

    let publisher = Publisher::new(ring.clone());
    let mut a = Subscriber::from_start(ring.clone());
    let mut b = Subscriber::from_start(ring.clone());

    let n = 30u64;
    for i in 0..n {
        publisher.publish(counter_desc(i));
    }

    // Each subscriber independently observes the full broadcast.
    for i in 0..n {
        assert_eq!(a.try_recv(), Some(Msg::Sample(counter_desc(i))));
    }
    for i in 0..n {
        assert_eq!(b.try_recv(), Some(Msg::Sample(counter_desc(i))));
    }
    assert!(a.try_recv().is_none());
    assert!(b.try_recv().is_none());
}

#[test]
fn lapped_subscriber_observes_lag_then_resyncs() {
    let capacity = 4u32;
    let fx = Fixture::new(capacity);
    let ring = fx.init_ring(capacity);

    let publisher = Publisher::new(ring.clone());
    let mut sub = Subscriber::from_start(ring.clone());

    // Publish capacity + 2 without consuming: the subscriber is lapped by 2.
    let skipped = 2u64;
    let n = capacity as u64 + skipped; // 6 messages, seqs 0..=5
    for i in 0..n {
        publisher.publish(counter_desc(i));
    }

    // First read reports the lag and resyncs to the oldest live seq (== skipped).
    match sub.try_recv() {
        Some(Msg::Lagged(missed)) => assert_eq!(missed, skipped),
        other => panic!("expected Lagged({skipped}), got {other:?}"),
    }
    assert_eq!(sub.cursor(), skipped);

    // The `capacity` still-live messages follow, in order and gap-free.
    for i in skipped..n {
        assert_eq!(sub.try_recv(), Some(Msg::Sample(counter_desc(i))));
    }
    assert!(sub.try_recv().is_none());
}

#[test]
fn reliable_backpressure_flips_and_clears() {
    let capacity = 4u32;
    let fx = Fixture::new(capacity);
    let ring = fx.init_ring(capacity);

    let publisher = Publisher::new(ring.clone());
    let mut sub = Subscriber::from_start(ring.clone());
    sub.make_reliable().unwrap();
    assert!(sub.is_reliable());

    // No backpressure while empty / within capacity.
    assert!(!ring.would_backpressure());
    for i in 0..capacity as u64 {
        publisher.publish(counter_desc(i));
    }
    assert!(
        !ring.would_backpressure(),
        "exactly capacity behind is not backpressure"
    );

    // One more publish pushes the reliable subscriber > capacity behind.
    publisher.publish(counter_desc(capacity as u64));
    assert!(
        ring.would_backpressure(),
        "reliable subscriber is now > capacity behind"
    );
    assert_eq!(ring.min_reliable_cursor(), Some(0));

    // Draining catches the reliable cursor up, clearing backpressure.
    while sub.try_recv().is_some() {}
    assert_eq!(sub.cursor(), ring.head());
    assert!(
        !ring.would_backpressure(),
        "backpressure clears once caught up"
    );
    assert_eq!(ring.min_reliable_cursor(), Some(ring.head()));
}

#[test]
fn reliable_slot_released_on_drop() {
    let fx = Fixture::new(8);
    let ring = fx.init_ring(8);
    {
        let mut sub = Subscriber::from_start(ring.clone());
        sub.make_reliable().unwrap();
        assert!(ring.min_reliable_cursor().is_some());
    }
    // Dropping the subscriber frees its reliable slot.
    assert!(ring.min_reliable_cursor().is_none());
}

#[test]
fn one_producer_two_consumers_no_gaps() {
    // capacity > total messages => no slot is ever overwritten, so every
    // keeping-up consumer must receive every sequence with no gaps.
    let n = 4096u64;
    let capacity = 8192u32;
    let fx = Fixture::new(capacity);
    let ring = fx.init_ring(capacity);

    // Create both subscribers (at head 0) before publishing starts.
    let mut c1 = Subscriber::new(ring.clone());
    let mut c2 = Subscriber::new(ring.clone());

    let ring_for_producer = ring.clone();
    let t1 = thread::spawn(move || {
        let mut got = Vec::with_capacity(n as usize);
        while (got.len() as u64) < n {
            match c1.recv() {
                Msg::Sample(desc) => got.push(desc.offset as u64),
                Msg::Lagged(_) => panic!("capacity > n: must never lag"),
            }
        }
        got
    });
    let t2 = thread::spawn(move || {
        let mut got = Vec::with_capacity(n as usize);
        while (got.len() as u64) < n {
            match c2.recv() {
                Msg::Sample(desc) => got.push(desc.offset as u64),
                Msg::Lagged(_) => panic!("capacity > n: must never lag"),
            }
        }
        got
    });

    let producer = thread::spawn(move || {
        let publisher = Publisher::new(ring_for_producer);
        for i in 0..n {
            publisher.publish(counter_desc(i));
        }
    });

    producer.join().unwrap();
    let got1 = t1.join().unwrap();
    let got2 = t2.join().unwrap();

    let expected: Vec<u64> = (0..n).collect();
    assert_eq!(got1, expected, "consumer 1 saw gaps or reordering");
    assert_eq!(got2, expected, "consumer 2 saw gaps or reordering");
}

#[test]
fn from_seq_resumes_at_the_requested_sequence() {
    let capacity = 64u32;
    let fx = Fixture::new(capacity);
    let ring = fx.init_ring(capacity);

    let publisher = Publisher::new(ring.clone());
    for i in 0..10 {
        publisher.publish(counter_desc(i));
    }

    // Resume a consumer that has already read through seq 3.
    let mut sub = Subscriber::from_seq(ring.clone(), 4);
    assert_eq!(sub.cursor(), 4);
    for i in 4..10 {
        assert_eq!(sub.try_recv(), Some(Msg::Sample(counter_desc(i))));
    }
    assert!(sub.try_recv().is_none());
}

#[test]
fn from_seq_reduces_to_new_and_from_start_at_the_boundaries() {
    let capacity = 64u32;
    let fx = Fixture::new(capacity);
    let ring = fx.init_ring(capacity);

    let publisher = Publisher::new(ring.clone());
    for i in 0..5 {
        publisher.publish(counter_desc(i));
    }

    // from_seq(0) == from_start: replays everything still live.
    let mut from_zero = Subscriber::from_seq(ring.clone(), 0);
    let mut from_start = Subscriber::from_start(ring.clone());
    assert_eq!(from_zero.cursor(), from_start.cursor());
    for i in 0..5 {
        assert_eq!(from_zero.try_recv(), Some(Msg::Sample(counter_desc(i))));
        assert_eq!(from_start.try_recv(), Some(Msg::Sample(counter_desc(i))));
    }

    // from_seq(head) == new: live-only, sees nothing already published.
    let mut from_head = Subscriber::from_seq(ring.clone(), ring.head());
    let mut live_only = Subscriber::new(ring.clone());
    assert_eq!(from_head.cursor(), live_only.cursor());
    assert!(from_head.try_recv().is_none());
    assert!(live_only.try_recv().is_none());

    publisher.publish(counter_desc(5));
    assert_eq!(from_head.try_recv(), Some(Msg::Sample(counter_desc(5))));
    assert_eq!(live_only.try_recv(), Some(Msg::Sample(counter_desc(5))));
}

#[test]
fn from_seq_before_the_live_window_lags_then_resyncs() {
    let capacity = 4u32;
    let fx = Fixture::new(capacity);
    let ring = fx.init_ring(capacity);

    let publisher = Publisher::new(ring.clone());
    let n = 10u64; // seqs 0..=9; only the last `capacity` (6..=9) stay live
    for i in 0..n {
        publisher.publish(counter_desc(i));
    }

    // Asking for a sequence that has already been overwritten is not an error:
    // the first read reports the gap and resyncs to the oldest live message.
    let oldest_live = n - capacity as u64;
    let mut sub = Subscriber::from_seq(ring.clone(), 2);
    match sub.try_recv() {
        Some(Msg::Lagged(missed)) => assert_eq!(missed, oldest_live - 2),
        other => panic!("expected Lagged, got {other:?}"),
    }
    assert_eq!(sub.cursor(), oldest_live);
    for i in oldest_live..n {
        assert_eq!(sub.try_recv(), Some(Msg::Sample(counter_desc(i))));
    }
    assert!(sub.try_recv().is_none());
}

#[test]
fn from_seq_beyond_head_waits_for_the_producer() {
    let capacity = 64u32;
    let fx = Fixture::new(capacity);
    let ring = fx.init_ring(capacity);

    let publisher = Publisher::new(ring.clone());
    for i in 0..3 {
        publisher.publish(counter_desc(i));
    }

    // A cursor ahead of the head is not an error either — it simply blocks until
    // the producer reaches it, then delivers from exactly that sequence.
    let mut sub = Subscriber::from_seq(ring.clone(), 5);
    assert!(
        sub.try_recv().is_none(),
        "must not deliver before seq 5 exists"
    );

    publisher.publish(counter_desc(3));
    publisher.publish(counter_desc(4));
    assert!(sub.try_recv().is_none(), "seq 5 still unpublished");

    publisher.publish(counter_desc(5));
    assert_eq!(sub.try_recv(), Some(Msg::Sample(counter_desc(5))));
}
