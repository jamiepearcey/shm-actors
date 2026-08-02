//! The v0.1 walking-skeleton proof (ADR-0001, First Vertical Slice).
//!
//! Two tests exercise the *identical* coordinator reclaim path:
//!
//! - [`crash_reclaim_multiprocess`] — the target: a real coordinator (in-process,
//!   so the test can inspect the payload `ChunkCtrl`) grants segments to a
//!   producer and a consumer spawned as **separate OS processes**. The consumer
//!   pins a zero-copy batch, is `kill -9`ed while holding the pin, and the
//!   coordinator's lease expiry + borrow-journal replay reclaims the chunk.
//! - [`crash_reclaim_same_process_deterministic`] — the same reclaim code driven
//!   without the lease clock (via `force_reclaim`), for a fast, timing-immune
//!   proof of the invariant.

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_core::PUBLISHED;
use shm_ring::Msg;
use shm_runtime::demo::{demo_schema, verify_demo_batch, DEMO_TOPIC};
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// A per-run segment-id base, unique enough that concurrent test binaries and
/// reruns never collide on POSIX shm names.
fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
}

/// Poll `cond` until it returns `true` or `timeout` elapses.
fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    cond()
}

/// Ensure a child process is reaped even if an assertion unwinds.
struct Reaper(Child);
impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn crash_reclaim_multiprocess() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let result = dir.path().join("consumer.result");
    let uds_s = uds.to_str().unwrap().to_string();
    let result_s = result.to_str().unwrap().to_string();

    // --- Coordinator in-process (so we can inspect the payload ChunkCtrl). ---
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let exe = env!("CARGO_BIN_EXE_shm-skeleton");

    // --- Spawn producer + consumer as separate OS processes. ---
    let producer = Command::new(exe)
        .args(["producer", "--uds", &uds_s, "--topic", DEMO_TOPIC])
        .spawn()
        .expect("spawn producer");
    let mut producer = Reaper(producer);

    let consumer = Command::new(exe)
        .args([
            "consumer", "--uds", &uds_s, "--topic", DEMO_TOPIC, "--result", &result_s,
        ])
        .spawn()
        .expect("spawn consumer");
    let mut consumer = Reaper(consumer);

    // --- Wait for the consumer to pin + verify the zero-copy batch. ---
    let pinned = wait_until(Duration::from_secs(20), || {
        std::fs::read_to_string(&result)
            .map(|s| s.trim() == "OK")
            .unwrap_or(false)
    });
    assert!(
        pinned,
        "consumer never confirmed pin+verify (result: {:?})",
        std::fs::read_to_string(&result).ok()
    );

    // --- Wait until the chunk is "armed": published, owner released, pin held. ---
    let desc = wait_until(Duration::from_secs(5), || {
        coord.is_armed()
            && coord.last_published().is_some_and(|d| {
                coord
                    .chunk_snapshot(&d)
                    .is_some_and(|s| s.state == PUBLISHED && s.owner == 0 && s.refcount >= 1)
            })
    })
    .then(|| coord.last_published().unwrap());
    let desc = desc.expect("chunk was never armed (owner released with a live pin)");

    let before = coord.chunk_snapshot(&desc).expect("snapshot before kill");
    assert_eq!(before.state, PUBLISHED);
    assert_eq!(before.owner, 0, "producer ownership must be released");
    assert!(before.refcount >= 1, "consumer pin must be live");
    // The descriptor is valid *now* (pre-reclaim).
    assert!(coord.validate_desc(&desc).is_ok());

    // --- kill -9 the consumer WHILE it holds the pin. ---
    consumer.0.kill().expect("kill -9 consumer");
    let _ = consumer.0.wait();

    // --- The lease must expire and journal replay must reclaim the chunk. ---
    let reclaimed = wait_until(Duration::from_secs(5), || {
        coord
            .chunk_snapshot(&desc)
            .is_some_and(|s| s.state == shm_core::FREE && s.generation > before.generation)
    });
    let after = coord.chunk_snapshot(&desc).expect("snapshot after reclaim");
    assert!(
        reclaimed,
        "chunk was not reclaimed after consumer kill-9: {after:?} (before {before:?})"
    );
    assert_eq!(after.state, shm_core::FREE, "chunk must return to FREE");
    assert!(
        after.generation > before.generation,
        "generation must bump on recycle: {} -> {}",
        before.generation,
        after.generation
    );
    assert_eq!(after.refcount, 0, "refcount must be cleared");

    // The coordinator recorded the reclamation.
    assert!(
        coord.reclaimed().iter().any(|d| d.offset == desc.offset),
        "coordinator did not record the reclaim"
    );

    // --- The stale descriptor (captured pre-reclaim) now fails validation. ---
    assert!(
        matches!(
            coord.validate_desc(&desc),
            Err(shm_runtime::Error::Core(shm_core::Error::StaleDescriptor))
        ),
        "stale descriptor must fail ChunkCtrl::validate with StaleDescriptor"
    );

    let _ = producer.0.kill();
}

#[test]
fn crash_reclaim_same_process_deterministic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let registry = || Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]));

    // Producer publishes a batch. Heartbeats keep the lease monitor from
    // declaring either node dead before we drive the reclaim explicitly.
    let mut producer = Node::connect(&uds, "producer", registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let batch = shm_runtime::demo::demo_batch();
    let desc = producer
        .publish_batch(DEMO_TOPIC, &batch)
        .expect("publish batch");

    // Consumer subscribes, receives, pins, and verifies zero-copy contents.
    let mut consumer = Node::connect(&uds, "consumer", registry()).expect("consumer connect");
    consumer.start_heartbeat(Duration::from_millis(150));
    let mut sub = consumer.subscribe(DEMO_TOPIC).expect("subscribe");
    let recv_desc = loop {
        match sub.recv() {
            Msg::Sample(d) => break d,
            Msg::Lagged(_) => continue,
        }
    };
    assert_eq!(
        recv_desc, desc,
        "consumer received the producer's descriptor"
    );
    let pin = consumer.pin_and_read(&desc).expect("pin + read");
    verify_demo_batch(&pin.batch).expect("zero-copy batch verifies");

    // Let the coordinator process the Pinned notification (owner_release).
    assert!(
        wait_until(Duration::from_secs(2), || coord.is_armed()),
        "coordinator did not arm the chunk"
    );
    let before = coord.chunk_snapshot(&desc).expect("snapshot");
    assert_eq!(before.state, PUBLISHED);
    assert_eq!(before.owner, 0);
    assert_eq!(before.refcount, 1);

    // Drive the exact crash-reclaim path deterministically (simulated expiry).
    let reclaimed = coord
        .force_reclaim(consumer.actor_id())
        .expect("force reclaim");
    assert_eq!(reclaimed, vec![desc], "the pinned chunk is reclaimed");

    let after = coord.chunk_snapshot(&desc).expect("snapshot after");
    assert_eq!(after.state, shm_core::FREE);
    assert!(after.generation > before.generation);
    assert!(matches!(
        coord.validate_desc(&desc),
        Err(shm_runtime::Error::Core(shm_core::Error::StaleDescriptor))
    ));

    // The consumer's in-memory batch still exists but any re-read is now stale.
    drop(pin);
}

/// `subscribe_from` resumes at an explicit sequence, so a consumer that
/// persisted its progress does not re-receive what it already handled. This is
/// what a resumable transport front (SSE `Last-Event-ID`, a `?from=` cursor)
/// needs; plain `subscribe` can only start at the beginning of live history.
#[test]
fn subscribe_from_resumes_at_an_explicit_sequence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let registry = || Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]));

    let mut producer = Node::connect(&uds, "producer", registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let batch = shm_runtime::demo::demo_batch();

    // Three publishes occupy seqs 0, 1, 2.
    let descs: Vec<_> = (0..3)
        .map(|_| {
            producer
                .publish_batch(DEMO_TOPIC, &batch)
                .expect("publish batch")
        })
        .collect();

    // A consumer that already handled seq 0 resumes at 1.
    let mut consumer = Node::connect(&uds, "consumer", registry()).expect("consumer connect");
    consumer.start_heartbeat(Duration::from_millis(150));
    let mut sub = consumer
        .subscribe_from(DEMO_TOPIC, 1)
        .expect("subscribe_from");
    assert_eq!(sub.cursor(), 1, "cursor seeds to the requested sequence");

    for (offset, expected) in descs[1..].iter().enumerate() {
        let got = loop {
            match sub.recv() {
                Msg::Sample(d) => break d,
                Msg::Lagged(_) => continue,
            }
        };
        assert_eq!(
            &got,
            expected,
            "subscribe_from must deliver seq {} next",
            offset + 1
        );
    }

    // Seq 0 was skipped, not buffered: nothing further is pending.
    assert!(
        sub.try_recv().is_none(),
        "resumed subscriber must not replay the sequences it skipped"
    );
}
