//! v0.2 stage D — doorbell wakeup unit tests.
//!
//! These exercise the real pipe-backed doorbell primitive (`shm_core`) and the
//! `shm-ring` [`DoorbellNotifier`]/[`DoorbellParker`] hooks:
//!
//! - a parked reader wakes when the doorbell is rung;
//! - a ring that happens *before* the park is not lost (level-triggered);
//! - one ring wakes every reader blocked on the shared read-end (broadcast);
//! - a `Subscriber` parked in `recv` wakes promptly when a `Publisher` on the
//!   other end of the pipe publishes, across two threads.

use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use shm_core::{doorbell_pair, doorbell_park, doorbell_ring, ChunkDesc, Segment};
use shm_ring::{required_bytes, DoorbellNotifier, DoorbellParker, Msg, Publisher, Ring, Subscriber};

/// A generous timeout: a return sooner than this proves a real wakeup (vs the
/// bounded park timeout that a busy peer would otherwise mask).
const LONG: Duration = Duration::from_secs(5);

#[test]
fn doorbell_park_wakes_on_ring() {
    let db = doorbell_pair().expect("pair");
    let read = db.read.try_clone().expect("dup read");
    let worker = thread::spawn(move || {
        let start = Instant::now();
        let woke = doorbell_park(read.as_raw_fd(), LONG).expect("park");
        (woke, start.elapsed())
    });

    // Let the worker reach poll(), then ring.
    thread::sleep(Duration::from_millis(80));
    doorbell_ring(db.write.as_raw_fd()).expect("ring");

    let (woke, elapsed) = worker.join().unwrap();
    assert!(woke, "park must report a real wakeup");
    assert!(elapsed < Duration::from_secs(2), "woke slowly: {elapsed:?}");
}

#[test]
fn doorbell_ring_before_park_no_lost_wakeup() {
    let db = doorbell_pair().expect("pair");
    // Ring BEFORE anyone parks: the byte must persist so the later park returns
    // immediately (the classic self-pipe no-lost-wakeup guarantee).
    doorbell_ring(db.write.as_raw_fd()).expect("ring");

    let read = db.read;
    let start = Instant::now();
    let woke = doorbell_park(read.as_raw_fd(), LONG).expect("park");
    let elapsed = start.elapsed();
    assert!(woke, "a ring before park must still wake");
    assert!(elapsed < Duration::from_secs(1), "park blocked despite pending byte: {elapsed:?}");
}

#[test]
fn doorbell_broadcast_wakes_all_subscribers() {
    // The broadcast property that matters end-to-end: ONE publish wakes EVERY
    // subscriber parked on the shared doorbell, and each receives the message.
    // A subscriber whose doorbell byte was "stolen" by a peer's drain still
    // wakes within its bounded poll timeout and re-checks the ring, so this is
    // deterministic (no reliance on all pollers observing the single byte).
    let id = next_segment_id();
    let _ = Segment::unlink_by_id(id);
    let capacity = 64u32;
    let size = (required_bytes(capacity) + 4096).next_power_of_two();
    let segment = Segment::create(id, size).expect("segment");
    // SAFETY: the payload region stays mapped for `segment`'s lifetime and no
    // other party initializes it concurrently.
    let ring = unsafe { Ring::init(segment.payload_ptr(), segment.payload_len(), capacity) }
        .expect("init ring");

    let db = doorbell_pair().expect("pair");
    let write_fd = db.write.as_raw_fd();
    let n = 3usize;
    let barrier = Arc::new(Barrier::new(n + 1));
    let mut workers = Vec::new();
    for _ in 0..n {
        // Each subscriber owns its own duplicate of the shared doorbell read-end
        // (default bounded timeout).
        let parker = DoorbellParker::new(db.read.try_clone().expect("dup read"));
        let mut sub = Subscriber::from_start_with_parker(ring.clone(), parker);
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            let start = Instant::now();
            let msg = sub.recv();
            (msg, start.elapsed())
        }));
    }

    // Let every subscriber park, then publish ONCE, ringing the shared doorbell.
    barrier.wait();
    thread::sleep(Duration::from_millis(60));
    let published = ChunkDesc { offset: 42, ..ChunkDesc::ZERO };
    Publisher::with_notifier(ring, DoorbellNotifier::new(write_fd)).publish(published);

    for (i, w) in workers.into_iter().enumerate() {
        let (msg, elapsed) = w.join().unwrap();
        match msg {
            Msg::Sample(desc) => assert_eq!(desc.offset, 42, "subscriber {i} wrong sample"),
            other => panic!("subscriber {i} expected the sample, got {other:?}"),
        }
        assert!(elapsed < LONG, "subscriber {i} woke slowly: {elapsed:?}");
    }

    drop(db);
    let _ = Segment::unlink_by_id(id);
}

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    47_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

#[test]
fn subscriber_parks_and_wakes_via_doorbell() {
    // A real ring in shared memory, plus a doorbell wired into a Publisher's
    // notifier and a Subscriber's parker: prove a cross-thread publish wakes a
    // subscriber blocked in `recv`.
    let id = next_segment_id();
    let _ = Segment::unlink_by_id(id);
    let capacity = 64u32;
    let size = (required_bytes(capacity) + 4096).next_power_of_two();
    let segment = Segment::create(id, size).expect("segment");
    // SAFETY: the payload region stays mapped for `segment`'s lifetime and no
    // other party initializes it concurrently.
    let ring = unsafe { Ring::init(segment.payload_ptr(), segment.payload_len(), capacity) }
        .expect("init ring");

    let db = doorbell_pair().expect("pair");
    let write_fd = db.write.as_raw_fd();
    let parker = DoorbellParker::new(db.read.try_clone().expect("dup read"));
    let mut sub = Subscriber::from_start_with_parker(ring.clone(), parker);

    let consumer = thread::spawn(move || {
        let start = Instant::now();
        let msg = sub.recv();
        (msg, start.elapsed())
    });

    // Idle for a bit, then publish + ring the doorbell.
    thread::sleep(Duration::from_millis(120));
    let published = ChunkDesc { offset: 7, ..ChunkDesc::ZERO };
    Publisher::with_notifier(ring, DoorbellNotifier::new(write_fd)).publish(published);

    let (msg, elapsed) = consumer.join().unwrap();
    match msg {
        Msg::Sample(desc) => assert_eq!(desc.offset, 7),
        other => panic!("expected the published sample, got {other:?}"),
    }
    // Woke within a couple of park cycles of the publish, not after LONG.
    assert!(elapsed < Duration::from_secs(1), "subscriber woke slowly: {elapsed:?}");

    // Keep the write-end alive until after the publish, then clean up.
    drop(db);
    let _ = Segment::unlink_by_id(id);
}
