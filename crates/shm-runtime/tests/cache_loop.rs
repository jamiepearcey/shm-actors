//! v0.2 walking skeleton — "the cache loop" (ADR-0002 §3) end-to-end proof.
//!
//! The coordinator runs **in-process** (so the test can inspect artifact
//! segments + the task queue); the producer / watcher / worker roles are spawned
//! as **separate OS processes** via the `shm-cacheloop` binary.
//!
//! - [`cache_loop_end_to_end`]: producer stages+commits → `VersionEvent` on
//!   `__artifacts` → watcher (parked on the doorbell) enqueues a task → worker
//!   CAS-claims, pins the version zero-copy, derives the result, marks it `DONE`.
//! - [`watcher_parks_until_event`]: the watcher genuinely blocks on the doorbell
//!   while idle and wakes only when a commit event arrives.
//! - [`worker_crash_requeues_at_least_once`]: `kill -9` a worker mid-`CLAIMED`;
//!   the lease-driven reap requeues and a second worker completes — same stable
//!   correlation id (at-least-once).
//! - [`worker_requeue_deterministic`]: the identical reap/requeue path, driven
//!   in-process without the kill clock (timing-immune fallback).
//! - [`producer_crash_mid_stage_no_version`]: `kill -9` a producer mid-stage;
//!   journal replay frees the staged chunks and no version is ever published.
//! - [`producer_crash_mid_stage_deterministic`]: the same, driven in-process via
//!   `force_reclaim`.

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_core::ChunkDesc;
use shm_runtime::demo::{demo_batch, demo_schema, CACHE_ARTIFACT, DEMO_ID_SUM};
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_stream::{Commit, Coordination};
use shm_task::now_nanos;

/// A per-run segment-id base, unique enough that concurrent test binaries and
/// reruns never collide on POSIX shm names.
fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    500_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
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

fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
}

/// The `shm-cacheloop` binary path.
fn exe() -> &'static str {
    env!("CARGO_BIN_EXE_shm-cacheloop")
}

/// Read the whitespace tokens of the first line of `path` beginning with `prefix`.
fn line_tokens(path: &std::path::Path, prefix: &str) -> Option<Vec<String>> {
    let text = std::fs::read_to_string(path).ok()?;
    for line in text.lines() {
        if line.starts_with(prefix) {
            return Some(line.split_whitespace().map(str::to_string).collect());
        }
    }
    None
}

/// Spawn a `shm-cacheloop` role wired to `uds`/`result`.
fn spawn(role: &str, uds: &str, result: Option<&str>) -> Reaper {
    let mut cmd = Command::new(exe());
    cmd.args([role, "--uds", uds]);
    if let Some(r) = result {
        cmd.args(["--result", r]);
    }
    Reaper(cmd.spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}")))
}

// ---------------------------------------------------------------------------

#[test]
fn cache_loop_end_to_end() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let prod_r = dir.path().join("producer.result");
    let watch_r = dir.path().join("watcher.result");
    let work_r = dir.path().join("worker.result");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Producer commits version 1 (publishing a VersionEvent on __artifacts).
    let _producer = spawn("producer", &uds_s, prod_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            coord.artifact_current_version(CACHE_ARTIFACT) == Some(1)
        }),
        "producer never committed version 1 (result: {:?})",
        std::fs::read_to_string(&prod_r).ok()
    );

    // Watcher parks on __artifacts, wakes, and enqueues a task.
    let _watcher = spawn("watcher", &uds_s, watch_r.to_str());
    // Worker claims, pins the version zero-copy, derives the result, marks DONE.
    let _worker = spawn("worker", &uds_s, work_r.to_str());

    // The worker records its completion (correlation id + derived value).
    let done = wait_until(Duration::from_secs(20), || {
        line_tokens(&work_r, "DONE").is_some()
    });
    assert!(
        done,
        "worker never completed the task (result: {:?})",
        std::fs::read_to_string(&work_r).ok()
    );
    let done = line_tokens(&work_r, "DONE").unwrap();
    // "DONE <slot> <seq> <sum> <rows>"
    let (w_slot, w_seq, sum, rows) = (&done[1], &done[2], &done[3], &done[4]);
    assert_eq!(sum, &DEMO_ID_SUM.to_string(), "worker derived the wrong sum");
    assert_eq!(rows, "4", "worker derived the wrong row count");

    // The watcher observed the SAME terminal outcome via its stable handle.
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&watch_r, "OUTCOME DONE").is_some()
        }),
        "watcher never observed a DONE outcome (result: {:?})",
        std::fs::read_to_string(&watch_r).ok()
    );
    let submit = line_tokens(&watch_r, "SUBMIT").expect("watcher submit line");
    // The correlation id the watcher submitted equals the one the worker
    // completed: proof the loop closed on the same task.
    assert_eq!(&submit[1], w_slot, "correlation slot mismatch watcher↔worker");
    assert_eq!(&submit[2], w_seq, "correlation seq mismatch watcher↔worker");
}

#[test]
fn watcher_parks_until_event() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Watcher subscribes first and blocks in recv with nothing published yet.
    let mut watcher_node = Node::connect(&uds, "watcher", registry()).expect("watcher connect");
    watcher_node.start_heartbeat(Duration::from_millis(150));
    let mut watcher = watcher_node.watch_artifacts().expect("watch_artifacts");

    let handle = std::thread::spawn(move || {
        let ev = watcher.recv();
        (ev, Instant::now())
    });

    // Idle gap: the watcher must stay parked (no spurious return) — nothing has
    // been committed, so its doorbell never rang.
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        !handle.is_finished(),
        "watcher returned before any commit — it did not actually block on the doorbell"
    );

    // A producer opens the artifact and commits version 1 → VersionEvent.
    let mut producer = Node::connect(&uds, "producer", registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    producer.open_artifact(CACHE_ARTIFACT).expect("open_artifact");
    let t_commit = Instant::now();
    {
        let stream = producer.stream(CACHE_ARTIFACT).expect("stream");
        let mut w = stream
            .writer(Commit::Replace, Coordination::Optimistic { expect_version: 0 })
            .expect("writer");
        w.append_batch(&demo_batch()).expect("append");
        assert_eq!(w.commit().expect("commit"), 1);
    }

    let (ev, t_ret) = handle.join().unwrap();
    assert_eq!(ev.version, 1, "watcher decoded the wrong version");
    assert_eq!(ev.name_id, 1, "watcher decoded the wrong artifact name id");
    let latency = t_ret.saturating_duration_since(t_commit);
    assert!(
        latency < Duration::from_millis(300),
        "doorbell wake was slow: {latency:?}"
    );
}

#[test]
fn worker_crash_requeues_at_least_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let prod_r = dir.path().join("producer.result");
    let watch_r = dir.path().join("watcher.result");
    let hang_r = dir.path().join("hang.result");
    let work_r = dir.path().join("worker.result");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Producer commits v1; watcher submits a task and waits.
    let _producer = spawn("producer", &uds_s, prod_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            coord.artifact_current_version(CACHE_ARTIFACT) == Some(1)
        }),
        "producer never committed version 1"
    );
    let _watcher = spawn("watcher", &uds_s, watch_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&watch_r, "SUBMIT").is_some()
        }),
        "watcher never submitted a task"
    );

    // A worker that claims then hangs; capture its claimed correlation id.
    let mut hang = spawn("worker-hang", &uds_s, hang_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&hang_r, "CLAIMED").is_some()
        }),
        "worker-hang never claimed the task"
    );
    let claimed = line_tokens(&hang_r, "CLAIMED").unwrap();
    let (hang_slot, hang_seq) = (claimed[1].clone(), claimed[2].clone());

    // kill -9 the hung worker while it holds the CLAIMED task.
    hang.0.kill().expect("kill -9 worker-hang");
    let _ = hang.0.wait();

    // A second worker must claim the requeued task and complete it.
    let _worker = spawn("worker", &uds_s, work_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&work_r, "DONE").is_some()
        }),
        "second worker never completed the requeued task (result: {:?})",
        std::fs::read_to_string(&work_r).ok()
    );
    let done = line_tokens(&work_r, "DONE").unwrap();

    // At-least-once: the correlation id is STABLE across the requeue — the
    // second worker completed the very same task the first worker had claimed.
    assert_eq!(done[1], hang_slot, "requeue changed the slot (correlation id)");
    assert_eq!(done[2], hang_seq, "requeue changed the seq (correlation id)");
    assert_eq!(done[3], DEMO_ID_SUM.to_string(), "wrong derived sum");
    assert_eq!(done[4], "4", "wrong derived row count");

    // The requester observed the same terminal outcome.
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&watch_r, "OUTCOME DONE").is_some()
        }),
        "watcher never observed the DONE outcome after requeue"
    );
}

#[test]
fn worker_requeue_deterministic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let mut node = Node::connect(&uds, "wq", registry()).expect("connect");
    node.start_heartbeat(Duration::from_millis(150));
    let queue = node.task_queue().expect("task_queue");

    let lease = Duration::from_millis(500).as_nanos() as u64;
    let request = ChunkDesc {
        schema_id: 42,
        ..ChunkDesc::ZERO
    };
    let handle = queue.submit(request, now_nanos() + lease).expect("submit");

    // Worker A claims, then its lease is forced to lapse (simulated death) and
    // the coordinator's reap requeues it.
    let a = queue.claim(lease).expect("A claims");
    let a_id = a.task_id();
    let report = coord.reap_tasks(now_nanos() + Duration::from_secs(3600).as_nanos() as u64);
    assert_eq!(report.requeued, 1, "lapsed claim must be requeued");
    assert!(a.complete(ChunkDesc::ZERO).is_err(), "reaped claim must be Lost");

    // Worker B claims the requeued task: SAME correlation id (at-least-once).
    let b = queue.claim(lease).expect("B claims requeued task");
    assert_eq!(b.task_id(), a_id, "correlation id stable across requeue");
    let result = ChunkDesc {
        offset: DEMO_ID_SUM as u32,
        len: 4,
        ..ChunkDesc::ZERO
    };
    b.complete(result).expect("B completes");

    match queue.poll(handle).expect("poll") {
        shm_task::TaskStatus::Done(r) => {
            assert_eq!(r.offset, DEMO_ID_SUM as u32);
            assert_eq!(r.len, 4);
        }
        other => panic!("expected Done, got {other:?}"),
    }
}

#[test]
fn producer_crash_mid_stage_no_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let hang_r = dir.path().join("producer-hang.result");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Producer stages the batch but never commits (holds it mid-transaction).
    let mut hang = spawn("producer-hang", &uds_s, hang_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&hang_r, "STAGED").is_some()
                && coord.artifact_free_total(CACHE_ARTIFACT).is_some()
        }),
        "producer-hang never staged"
    );
    // Mid-stage: a chunk is loaned out and NO version is published.
    let staged_free = coord
        .artifact_free_total(CACHE_ARTIFACT)
        .expect("artifact exists");
    assert_eq!(
        coord.artifact_current_version(CACHE_ARTIFACT),
        Some(0),
        "no version may be published mid-stage"
    );

    // kill -9 the producer while it holds the staged (uncommitted) chunk.
    hang.0.kill().expect("kill -9 producer-hang");
    let _ = hang.0.wait();

    // The lease monitor replays the dead producer's journal and frees the staged
    // chunk(s) in the artifact's DATA pool — the free count climbs back.
    let reclaimed = wait_until(Duration::from_secs(10), || {
        coord.artifact_free_total(CACHE_ARTIFACT).unwrap_or(0) > staged_free
    });
    assert!(
        reclaimed,
        "staged chunks were not reclaimed after producer kill-9 \
         (staged_free={staged_free}, now={:?})",
        coord.artifact_free_total(CACHE_ARTIFACT)
    );
    // And crucially: no version was EVER published.
    assert_eq!(
        coord.artifact_current_version(CACHE_ARTIFACT),
        Some(0),
        "current_version must never have advanced"
    );
    assert!(
        !coord.reclaimed().is_empty(),
        "the coordinator must have recorded the staged-chunk reclaim"
    );
}

#[test]
fn producer_crash_mid_stage_deterministic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let mut node = Node::connect(&uds, "producer", registry()).expect("connect");
    node.start_heartbeat(Duration::from_millis(150));
    node.open_artifact(CACHE_ARTIFACT).expect("open_artifact");

    let full_free = coord
        .artifact_free_total(CACHE_ARTIFACT)
        .expect("artifact exists");

    // Stage a batch, then simulate a crash: `mem::forget` the writer so its Drop
    // (which would free the staged chunk) never runs.
    {
        let stream = node.stream(CACHE_ARTIFACT).expect("stream");
        let mut writer = stream
            .writer(Commit::Replace, Coordination::Optimistic { expect_version: 0 })
            .expect("writer");
        writer.append_batch(&demo_batch()).expect("append");
        assert!(
            coord.artifact_free_total(CACHE_ARTIFACT).unwrap() < full_free,
            "staging must consume a chunk"
        );
        std::mem::forget(writer);
    }
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(0));

    // Drive the exact crash-reclaim path deterministically (simulated expiry):
    // the segment-routed journal replay frees the staged chunk in the artifact
    // data pool.
    let reclaimed = coord.force_reclaim(node.actor_id()).expect("force reclaim");
    assert!(!reclaimed.is_empty(), "staged chunk must be reclaimed");

    assert_eq!(
        coord.artifact_current_version(CACHE_ARTIFACT),
        Some(0),
        "no version was ever published"
    );
    assert_eq!(
        coord.artifact_free_total(CACHE_ARTIFACT),
        Some(full_free),
        "the staged chunk returned to the pool"
    );
}
