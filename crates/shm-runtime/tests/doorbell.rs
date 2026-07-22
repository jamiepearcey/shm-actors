//! v0.2 stage D — doorbell wakeup integration tests (coordinator-granted fds).
//!
//! - [`doorbell_wakes_subscriber_across_nodes`]: a subscriber `Node` blocks in
//!   `recv` on a coordinator-granted doorbell; a publisher `Node` on the same
//!   topic publishes after an idle gap and the subscriber wakes **promptly**
//!   (< ~150 ms). It also asserts the subscriber stays blocked (does not return
//!   spuriously) during the idle gap.
//! - [`idle_subscriber_blocks_not_spins`]: over a fixed idle window a
//!   doorbell-parked subscriber parks only ~`window / poll_timeout` times, not
//!   the millions a busy-spin would incur — the "idle costs ~0 CPU" proof.

use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_core::{doorbell_pair, ChunkDesc, Segment};
use shm_ring::{
    required_bytes, DoorbellNotifier, DoorbellParker, Msg, Parker, Publisher, Ring, Subscriber,
    DEFAULT_DOORBELL_TIMEOUT,
};
use shm_runtime::demo::{demo_batch, demo_schema, DEMO_TOPIC};
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// A per-run segment-id base, unique enough that concurrent test binaries and
/// reruns never collide on POSIX shm names.
fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    let pid = std::process::id() as u64;
    300_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
}

fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
}

#[test]
fn doorbell_wakes_subscriber_across_nodes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Consumer subscribes first (creates the topic ring + doorbell, receiving
    // the read-end); it will block in recv with nothing published yet.
    let mut consumer = Node::connect(&uds, "consumer", registry()).expect("consumer connect");
    consumer.start_heartbeat(Duration::from_millis(150));
    let mut sub = consumer.subscribe(DEMO_TOPIC).expect("subscribe");

    let handle = thread::spawn(move || {
        let msg = sub.recv();
        (msg, Instant::now())
    });

    // Idle gap: the subscriber must stay blocked (no spurious return) with no
    // publish in flight.
    thread::sleep(Duration::from_millis(120));
    assert!(
        !handle.is_finished(),
        "subscriber returned before any publish — it did not actually block"
    );

    // Publisher joins the same topic (gets the write-end) and publishes.
    let mut producer = Node::connect(&uds, "producer", registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let batch = demo_batch();
    let t_pub = Instant::now();
    producer.publish_batch(DEMO_TOPIC, &batch).expect("publish");

    let (msg, t_ret) = handle.join().unwrap();
    assert!(matches!(msg, Msg::Sample(_)), "expected a sample, got {msg:?}");
    let latency = t_ret.saturating_duration_since(t_pub);
    assert!(
        latency < Duration::from_millis(150),
        "doorbell wake was slow: {latency:?} (should be well under one poll cycle chain)"
    );
}

/// A [`Parker`] that counts how many times it parks, delegating to an inner
/// parker. Used to prove an idle subscriber blocks rather than busy-spins.
struct CountingParker<P: Parker> {
    inner: P,
    count: Arc<AtomicU64>,
}

impl<P: Parker> Parker for CountingParker<P> {
    fn park(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.park();
    }
}

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    46_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

#[test]
fn idle_subscriber_blocks_not_spins() {
    // Build a ring + doorbell directly so we can wrap the parker in a counter.
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
    let count = Arc::new(AtomicU64::new(0));
    let parker = CountingParker {
        inner: DoorbellParker::new(db.read.try_clone().expect("dup read")),
        count: count.clone(),
    };
    let mut sub = Subscriber::from_start_with_parker(ring.clone(), parker);

    let consumer = thread::spawn(move || sub.recv());

    // Idle window with NO publish: a level-triggered doorbell parker wakes only
    // on its bounded poll timeout, so parks ≈ window / timeout.
    let window = Duration::from_millis(300);
    thread::sleep(window);
    let parks = count.load(Ordering::Relaxed);

    // Upper bound: window/timeout plus generous slack. A busy-spin (YieldParker)
    // would park millions of times here; the doorbell parks a handful.
    let expected = (window.as_millis() / DEFAULT_DOORBELL_TIMEOUT.as_millis().max(1)) as u64;
    let ceiling = (expected + 5) * 4;
    assert!(
        parks >= 1,
        "subscriber never parked — it never reached the blocking path"
    );
    assert!(
        parks <= ceiling,
        "idle subscriber parked {parks} times over {window:?} (ceiling {ceiling}); \
         this indicates busy-spinning, not blocking"
    );

    // Release the blocked recv so the thread can join, then clean up.
    let published = ChunkDesc { offset: 1, ..ChunkDesc::ZERO };
    Publisher::with_notifier(ring, DoorbellNotifier::new(write_fd)).publish(published);
    let msg = consumer.join().unwrap();
    assert!(matches!(msg, Msg::Sample(_)), "expected the release sample, got {msg:?}");

    drop(db);
    let _ = Segment::unlink_by_id(id);
}
