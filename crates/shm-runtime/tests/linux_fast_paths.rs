//! ADR-0011 (Holon P0.4): pidfd-accelerated crash reclaim. Linux only — on the
//! dev macOS box this runs in a container (`scripts/linux-test.sh`).
//!
//! The proof shape: the coordinator is configured with a lease deadline far
//! longer than the test's own patience (60 s vs a ~15 s assertion window), so
//! the ONLY way the dead worker's journal can be replayed in time is the pidfd
//! exit notification ending the lease monitor's tick early. Reverting the
//! pidfd wiring makes this test fail by timeout — the lease clock alone would
//! need the full 60 s.
#![cfg(target_os = "linux")]

use std::process::{Child, Command};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_runtime::demo::{demo_schema, CACHE_ARTIFACT};
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// A per-run segment-id base (same shape as the other runtime test crates).
fn unique_seg_base() -> u32 {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    900_000 + n * 100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 90_000) as u32
}

fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
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

fn spawn(role: &str, uds: &str, result: Option<&str>) -> Reaper {
    let mut cmd = Command::new(exe());
    cmd.args([role, "--uds", uds]);
    if let Some(r) = result {
        cmd.args(["--result", r]);
    }
    Reaper(cmd.spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}")))
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

fn line_tokens(path: &std::path::Path, prefix: &str) -> Option<Vec<String>> {
    let text = std::fs::read_to_string(path).ok()?;
    for line in text.lines() {
        if line.starts_with(prefix) {
            return Some(line.split_whitespace().map(str::to_string).collect());
        }
    }
    None
}

/// A `kill -9`ed pin-holding worker is reclaimed near-instantly via its pidfd,
/// with the lease deadline set far beyond the assertion window so the lease
/// clock provably cannot be what saved the test.
#[test]
fn pidfd_reclaims_killed_worker_long_before_the_lease_deadline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let prod_r = dir.path().join("producer.result");
    let pin_r = dir.path().join("pin.result");

    let mut config = RuntimeConfig::with_seg_base(unique_seg_base());
    // The load-bearing setting: leases alone would need a full minute.
    config.lease_deadline = Duration::from_secs(60);
    config.monitor_tick = Duration::from_millis(40);
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Producer commits v1 and stays alive.
    let _producer = spawn("producer", &uds_s, prod_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            coord.artifact_current_version(CACHE_ARTIFACT) == Some(1)
        }),
        "producer never committed version 1"
    );

    // A separate process journal-pins v1 and hangs holding the pin.
    let mut pin_hang = spawn("worker-pin-hang", &uds_s, pin_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&pin_r, "PINNED").is_some()
        }),
        "worker-pin-hang never pinned a version"
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_slot_pins(CACHE_ARTIFACT, 1) == Some(1)
        }),
        "the worker's journalled pin should register on v1's slot"
    );

    // Supersede v1 so the pinned version is reclaimable once the pin dies.
    let mut committer = Node::connect(&uds, "committer", registry()).expect("connect committer");
    committer.start_heartbeat(Duration::from_millis(150));
    committer
        .open_artifact(CACHE_ARTIFACT)
        .expect("open_artifact");
    // Reuse the demo commit shape: replace v1 with a fresh batch.
    {
        use shm_runtime::demo::demo_batch;
        use shm_stream::{Commit, Coordination};
        let stream = committer.stream(CACHE_ARTIFACT).expect("stream");
        let mut w = stream
            .writer(Commit::Replace, Coordination::Optimistic { expect_version: 1 })
            .expect("writer");
        w.append_batch(&demo_batch()).expect("append");
        assert_eq!(w.commit().expect("commit v2"), 2);
    }
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(2));
    assert_eq!(coord.artifact_slot_pins(CACHE_ARTIFACT, 1), Some(1));

    // kill -9 the pin holder and start the clock.
    pin_hang.0.kill().expect("kill -9 worker-pin-hang");
    let _ = pin_hang.0.wait();
    let t_kill = Instant::now();

    // The pidfd notification must drive the reclaim in seconds, not minutes.
    // Wait on the reclaim COUNTER (bumped after the journal replay finishes),
    // not just the shm pin word (released mid-replay) — polling only the pin
    // word races the bookkeeping by a few microseconds.
    let reclaimed = wait_until(Duration::from_secs(15), || {
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1).is_none()
            && coord.artifact_pins_reclaimed() >= 1
    });
    let waited = t_kill.elapsed();
    assert!(
        reclaimed,
        "the killed worker's pin was not reclaimed within 15s — with a 60s \
         lease deadline this means the pidfd fast path did not fire \
         (waited {waited:?}, slot_pins={:?}, pins_reclaimed={})",
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1),
        coord.artifact_pins_reclaimed()
    );
    assert!(
        waited < Duration::from_secs(15),
        "reclaim must beat the lease deadline by an order of magnitude (took {waited:?})"
    );
}
