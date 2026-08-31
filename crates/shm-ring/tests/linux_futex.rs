//! ADR-0011 (Holon P0.4): futex doorbell hooks over the ABI-reserved
//! `RingHeader.doorbell_seq` word. Linux only — on the dev macOS box these run
//! in a container (`scripts/linux-test.sh`).
#![cfg(target_os = "linux")]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use shm_core::{ChunkDesc, Segment};
use shm_ring::{required_bytes, FutexNotifier, FutexParker, Msg, Publisher, Ring, Subscriber};

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    91_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A trivial descriptor whose `offset` field carries the sequence counter.
fn counter_desc(i: u64) -> ChunkDesc {
    ChunkDesc {
        segment_id: 1,
        generation: 1,
        offset: i as u32,
        len: 8,
        schema_id: 0,
        _pad: 0,
    }
}

/// A futex-parked subscriber wakes promptly on a publish that arrives after a
/// real idle gap (i.e. the wake is the futex, not the spin), and receives the
/// sample intact.
#[test]
fn futex_doorbell_wakes_idle_subscriber() {
    let id = next_segment_id();
    let _ = Segment::unlink_by_id(id);
    let size = (required_bytes(64) + 4096).next_power_of_two();
    let segment = Arc::new(Segment::create(id, size).expect("create segment"));
    // SAFETY: the payload stays mapped for the whole test; sole initializer.
    let ring =
        unsafe { Ring::init(segment.payload_ptr(), segment.payload_len(), 64).expect("init ring") };

    // SAFETY (both hooks): the word borrows the ring header inside `segment`,
    // whose Arc clones keep the mapping alive for both threads' lifetimes.
    let notifier = unsafe { FutexNotifier::new(ring.doorbell_word()) };
    let parker = unsafe { FutexParker::new(ring.doorbell_word()) };

    let publisher = Publisher::with_notifier(ring.clone(), notifier);
    let mut subscriber = Subscriber::with_parker(ring.clone(), parker);

    let seg_sub = segment.clone();
    let recv = thread::spawn(move || {
        let _keep_mapped = seg_sub;
        let start = Instant::now();
        let msg = subscriber.recv();
        (msg, start.elapsed())
    });

    // An idle gap far longer than the subscriber's bounded spin, so the recv
    // is genuinely futex-parked when the publish lands.
    thread::sleep(Duration::from_millis(300));
    let t_pub = Instant::now();
    publisher.publish(counter_desc(7));

    let (msg, blocked) = recv.join().expect("join subscriber");
    match msg {
        Msg::Sample(desc) => assert_eq!(desc.offset, 7),
        other => panic!("expected the published sample, got {other:?}"),
    }
    assert!(
        blocked >= Duration::from_millis(250),
        "the subscriber must have actually parked through the idle gap (blocked {blocked:?})"
    );
    // Prompt wake: well under one bounded park timeout after the publish. A
    // generous bound (2 timeouts) keeps CI honest without being flaky.
    assert!(
        t_pub.elapsed() < Duration::from_millis(100),
        "the futex wake must be prompt, not a timeout recovery ({:?})",
        t_pub.elapsed()
    );

    let _ = Segment::unlink_by_id(id);
}

/// The bounded-timeout liveness fallback: a subscriber whose wake was missed
/// (nothing ever notifies) still returns from `park` and observes a publish
/// made with **no** notifier — at worst one bounded timeout later.
#[test]
fn futex_park_bounded_timeout_recovers_a_silent_publish() {
    let id = next_segment_id();
    let _ = Segment::unlink_by_id(id);
    let size = (required_bytes(64) + 4096).next_power_of_two();
    let segment = Arc::new(Segment::create(id, size).expect("create segment"));
    // SAFETY: the payload stays mapped for the whole test; sole initializer.
    let ring =
        unsafe { Ring::init(segment.payload_ptr(), segment.payload_len(), 64).expect("init ring") };

    // Short bounded park so the test is quick; the contract is the same.
    // SAFETY: the word borrows the header in `segment`, kept alive via Arc.
    let parker =
        unsafe { FutexParker::with_timeout(ring.doorbell_word(), Duration::from_millis(20)) };
    let mut subscriber = Subscriber::with_parker(ring.clone(), parker);

    // A publisher with NO notifier: the doorbell word is never bumped, so only
    // the parker's bounded timeout can save the recv.
    let publisher = Publisher::new(ring.clone());

    let seg_sub = segment.clone();
    let recv = thread::spawn(move || {
        let _keep_mapped = seg_sub;
        subscriber.recv()
    });
    thread::sleep(Duration::from_millis(150)); // let it park for real
    publisher.publish(counter_desc(42));

    let msg = recv.join().expect("join subscriber");
    match msg {
        Msg::Sample(desc) => assert_eq!(desc.offset, 42),
        other => panic!("expected the silent publish's sample, got {other:?}"),
    }

    let _ = Segment::unlink_by_id(id);
}
