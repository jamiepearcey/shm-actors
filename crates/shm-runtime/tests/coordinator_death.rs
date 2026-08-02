//! v0.4 stage O §2 — **coordinator death while the data plane is active**.
//!
//! The coordinator owns the control plane (fd granting, leases, reclamation) but
//! is deliberately **off the payload data path** (ADR-0001/0002): once an actor
//! has been granted and mapped a topic's ring segment + doorbell, pub/sub flows
//! through shared memory with zero coordinator involvement. This test kills the
//! coordinator **process** (`kill -9`) while a pre-connected producer/consumer
//! pair is mid-stream and asserts:
//!
//! 1. **The data plane keeps flowing.** A producer and consumer already connected
//!    and mapped continue to publish/receive after the coordinator dies — proven
//!    at the *ring* level (pure shm), the layer the coordinator never touches.
//!    Strikingly, `publish_batch`'s trailing *control-plane* notification
//!    (`fire(Published)`) now fails (its socket is dead) while the *ring* delivery
//!    that precedes it still lands — a direct demonstration that the two planes
//!    are independent.
//! 2. **New registrations fail cleanly.** A fresh `Node::connect` to the dead
//!    coordinator returns an error **promptly** (ECONNREFUSED / ENOENT) — it does
//!    not hang forever and does not corrupt anything.
//!
//! Recovery (a restarted coordinator re-adopting live segments, re-issuing fds,
//! resuming leases) is **out of scope** for v0.4 — the assertion here is only that
//! the data plane *survives* control-plane death; nothing wedges, nothing aborts.

use std::process::{Child, Command};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_ring::Msg;
use shm_runtime::demo::{demo_batch, demo_schema, DEMO_TOPIC};
use shm_runtime::Node;
use std::sync::Arc;

fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    900_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
}

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

struct Reaper(Child);
impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn exe() -> &'static str {
    env!("CARGO_BIN_EXE_shm-cacheloop")
}

fn demo_registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
}

#[test]
fn data_plane_survives_coordinator_death() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let seg_base = unique_seg_base();

    // --- Coordinator as a separate, killable OS process. ---
    let mut coord = Reaper(
        Command::new(exe())
            .args([
                "coordinator",
                "--uds",
                &uds_s,
                "--seg-base",
                &seg_base.to_string(),
            ])
            .spawn()
            .expect("spawn coordinator"),
    );

    // Wait until the coordinator is accepting (a connect handshake succeeds).
    assert!(
        wait_until(Duration::from_secs(20), || {
            Node::connect(&uds, "probe", demo_registry()).is_ok()
        }),
        "coordinator never became ready"
    );

    // --- Pre-connect a producer + consumer and establish the ring. ---
    let mut producer = Node::connect(&uds, "producer", demo_registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let mut consumer = Node::connect(&uds, "consumer", demo_registry()).expect("consumer connect");
    consumer.start_heartbeat(Duration::from_millis(150));
    let mut sub = consumer.subscribe(DEMO_TOPIC).expect("subscribe");

    // Publish one message while the coordinator is alive; the consumer receives it
    // — proof the pair is connected + mapped and messaging works.
    let d1 = producer
        .publish_batch(DEMO_TOPIC, &demo_batch())
        .expect("publish #1");
    let got1 = recv_sample(&mut sub, Duration::from_secs(5)).expect("consumer receives pre-death");
    assert_eq!(got1, d1.offset, "consumer received the pre-death message");

    // --- kill -9 the coordinator while the stream is live. ---
    coord.0.kill().expect("kill -9 coordinator");
    let _ = coord.0.wait();
    // Give the OS a beat to tear down the listener so a new connect is refused.
    std::thread::sleep(Duration::from_millis(100));

    // (2) A NEW registration must now fail PROMPTLY (not hang). Run it on a thread
    // guarded by a timeout so a hang is detectable as a failure rather than
    // blocking the test forever.
    let uds_for_connect = uds.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let r = Node::connect(&uds_for_connect, "late", demo_registry());
        let _ = tx.send(r.is_err());
    });
    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(is_err) => assert!(is_err, "a new registration to a dead coordinator must fail, not succeed"),
        Err(_) => panic!("Node::connect to a dead coordinator HUNG (no prompt error) — control-plane death must fail fast"),
    }

    // (1) The DATA PLANE keeps flowing: publish several more messages and confirm
    // the consumer receives every one over the ring — with no coordinator alive.
    // `publish_batch`'s trailing control-plane `fire(Published)` now errors, but
    // the ring delivery that precedes it still lands: we assert delivery, not the
    // (expected-failed) control-plane ack.
    let post_death = 4usize;
    let mut received = 0usize;
    for _ in 0..post_death {
        // The ring publish happens before the (now-failing) control-plane notify.
        let _ = producer.publish_batch(DEMO_TOPIC, &demo_batch());
        if recv_sample(&mut sub, Duration::from_secs(5)).is_some() {
            received += 1;
        }
    }
    assert_eq!(
        received, post_death,
        "the data plane must keep delivering after the coordinator died \
         (received {received}/{post_death} post-death messages over the ring)"
    );
}

/// Receive the next real `Sample` descriptor's offset within `timeout`, skipping
/// lag notices; `None` if none arrived (the ring is pure shm — no coordinator).
fn recv_sample(
    sub: &mut shm_ring::Subscriber<shm_ring::DoorbellParker>,
    timeout: Duration,
) -> Option<u32> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        match sub.try_recv() {
            Some(Msg::Sample(d)) => return Some(d.offset),
            Some(Msg::Lagged(_)) => continue,
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    None
}
