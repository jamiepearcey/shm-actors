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
//! - [`worker_crash_mid_pin_reclaims_version`] (ADR-0003 item J): `kill -9` a
//!   reader holding a **journalled artifact pin**; the lease-monitor replay
//!   decrements the pin table so the (superseded) version's chunks are reclaimed.
//! - [`artifact_pin_crash_reclaims_version_deterministic`]: the same, driven
//!   in-process via `force_reclaim` (timing-immune fallback).
//! - [`hostile_cache_loop`] (ADR-0003 S5): the whole thing chained — E + F + J +
//!   K on ONE nested multi-chunk artifact across real processes, ending in the
//!   definitive ZERO-LEAK pool census (free returns to the one-version baseline).
//! - [`hostile_cache_loop_deterministic`]: the identical chain + census driven
//!   fully in-process (timing-immune fallback).

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_core::ChunkDesc;
use shm_runtime::demo::{
    demo_batch, demo_derive, demo_schema, nested_batch, nested_schema, verify_nested_batch,
    CACHE_ARTIFACT, DEMO_ID_SUM, DEMO_ROWS, NESTED_ARTIFACT,
};
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
    spawn_with(role, uds, result, &[])
}

/// Spawn a `shm-cacheloop` role with the S5 `--nested` flag, so it operates on
/// the nested Struct/List multi-chunk artifact under its coordinator-negotiated
/// schema (item F) rather than the flat demo one.
fn spawn_nested(role: &str, uds: &str, result: Option<&str>) -> Reaper {
    spawn_with(role, uds, result, &["--nested"])
}

/// Spawn a `shm-cacheloop` role with extra trailing args.
fn spawn_with(role: &str, uds: &str, result: Option<&str>, extra: &[&str]) -> Reaper {
    let mut cmd = Command::new(exe());
    cmd.args([role, "--uds", uds]);
    if let Some(r) = result {
        cmd.args(["--result", r]);
    }
    cmd.args(extra);
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
    assert_eq!(
        sum,
        &DEMO_ID_SUM.to_string(),
        "worker derived the wrong sum"
    );
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
    assert_eq!(
        &submit[1], w_slot,
        "correlation slot mismatch watcher↔worker"
    );
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
    producer
        .open_artifact(CACHE_ARTIFACT)
        .expect("open_artifact");
    let t_commit = Instant::now();
    {
        let stream = producer.stream(CACHE_ARTIFACT).expect("stream");
        let mut w = stream
            .writer(
                Commit::Replace,
                Coordination::Optimistic { expect_version: 0 },
            )
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
    assert_eq!(
        done[1], hang_slot,
        "requeue changed the slot (correlation id)"
    );
    assert_eq!(
        done[2], hang_seq,
        "requeue changed the seq (correlation id)"
    );
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
    assert!(
        a.complete(ChunkDesc::ZERO).is_err(),
        "reaped claim must be Lost"
    );

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
            .writer(
                Commit::Replace,
                Coordination::Optimistic { expect_version: 0 },
            )
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

/// Commit one Replace version of the demo batch through `node`, expecting the
/// artifact's current version to be `expect` (→ `expect + 1`).
fn commit_replace(node: &Node, expect: u64) -> u64 {
    let stream = node.stream(CACHE_ARTIFACT).expect("stream");
    let mut w = stream
        .writer(
            Commit::Replace,
            Coordination::Optimistic {
                expect_version: expect,
            },
        )
        .expect("writer");
    w.append_batch(&demo_batch()).expect("append");
    w.commit().expect("commit")
}

#[test]
fn artifact_pin_crash_reclaims_version_deterministic() {
    // Item J, deterministic: a reader journal-pins v1, v1 is superseded by v2
    // (so v1 is non-current but pinned → not freed), then the reader "crashes"
    // (its pin is `mem::forget`ed, leaking the ArtifactPin entry + the +1 pin
    // count). `force_reclaim` replays the journal and decrements the pin table,
    // retiring v1 and reclaiming its chunks.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let mut node = Node::connect(&uds, "pinner", registry()).expect("connect");
    node.start_heartbeat(Duration::from_millis(150));
    node.open_artifact(CACHE_ARTIFACT).expect("open_artifact");

    // v1, then a journalled pin on it.
    assert_eq!(commit_replace(&node, 0), 1);
    let pin = node.pin_artifact(CACHE_ARTIFACT).expect("journalled pin");
    assert_eq!(pin.version(), 1);
    assert_eq!(
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1),
        Some(1),
        "the journalled pin is on v1's slot"
    );

    // v2 supersedes v1; v1 stays alive because our pin holds it.
    assert_eq!(commit_replace(&node, 1), 2);
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(2));
    let free_before = coord.artifact_free_total(CACHE_ARTIFACT).expect("artifact");

    // Simulate a `kill -9` mid-pin: forget the pin so its Drop never runs. The
    // ArtifactPin journal entry and the +1 pin count leak, exactly as a crash
    // would leave them.
    std::mem::forget(pin);
    assert_eq!(
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1),
        Some(1),
        "the leaked pin still holds v1"
    );

    // Drive the exact crash-reclaim path deterministically (simulated expiry).
    let _ = coord.force_reclaim(node.actor_id()).expect("force reclaim");
    assert!(
        coord.artifact_pins_reclaimed() >= 1,
        "the coordinator must record the leaked artifact-pin reclaim"
    );

    // v1 is retired: its slot is gone and its chunks are back in the pool.
    assert_eq!(
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1),
        None,
        "v1's slot must be freed once its leaked pin is reclaimed"
    );
    assert!(
        coord.artifact_free_total(CACHE_ARTIFACT).unwrap() > free_before,
        "v1's chunks must return to the pool after crash reclaim (before={free_before}, now={:?})",
        coord.artifact_free_total(CACHE_ARTIFACT)
    );
    assert_eq!(
        coord.artifact_current_version(CACHE_ARTIFACT),
        Some(2),
        "current must be unchanged by the reclaim"
    );
}

#[test]
fn worker_crash_mid_pin_reclaims_version() {
    // Item J, multi-process: a `worker-pin-hang` process journal-pins v1 and
    // hangs; the test supersedes v1 with v2; then `kill -9` the worker. The
    // lease-monitor replay decrements the pin table so v1 (non-current, and no
    // longer pinned) is retired and its chunks reclaimed.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let prod_r = dir.path().join("producer.result");
    let pin_r = dir.path().join("pin.result");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
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
    let pinned = line_tokens(&pin_r, "PINNED").unwrap();
    assert_eq!(pinned[1], "1", "worker-pin-hang should pin version 1");
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_slot_pins(CACHE_ARTIFACT, 1) == Some(1)
        }),
        "the worker's journalled pin should register on v1's slot"
    );

    // Supersede v1 with v2 (via an in-process node): v1 goes non-current but the
    // hung worker's pin holds its chunks against reclamation.
    let mut committer = Node::connect(&uds, "committer", registry()).expect("connect committer");
    committer.start_heartbeat(Duration::from_millis(150));
    committer
        .open_artifact(CACHE_ARTIFACT)
        .expect("open_artifact");
    assert_eq!(commit_replace(&committer, 1), 2);
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(2));
    // Still pinned → still Some(1) (retire skipped while the pin is live).
    assert_eq!(coord.artifact_slot_pins(CACHE_ARTIFACT, 1), Some(1));
    let free_before = coord.artifact_free_total(CACHE_ARTIFACT).expect("artifact");

    // kill -9 the worker while it holds the pin.
    pin_hang.0.kill().expect("kill -9 worker-pin-hang");
    let _ = pin_hang.0.wait();

    // The lease monitor replays the dead worker's journal, decrements v1's pin
    // count, and retires v1: its slot frees and its chunks return to the pool.
    let reclaimed = wait_until(Duration::from_secs(10), || {
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1).is_none()
            && coord.artifact_free_total(CACHE_ARTIFACT).unwrap_or(0) > free_before
    });
    assert!(
        reclaimed,
        "v1 was not reclaimed after the worker kill-9 (slot_pins={:?}, free_before={free_before}, now={:?})",
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1),
        coord.artifact_free_total(CACHE_ARTIFACT)
    );
    assert!(
        coord.artifact_pins_reclaimed() >= 1,
        "the coordinator must record the leaked artifact-pin reclaim"
    );
    assert_eq!(
        coord.artifact_current_version(CACHE_ARTIFACT),
        Some(2),
        "current must be unchanged by the reclaim"
    );
}

// ---------------------------------------------------------------------------
// ADR-0003 item K — coordinator-backed, FENCED write lease (crash release)
// ---------------------------------------------------------------------------

/// Commit one Replace version through `node` under the **exclusive** (fenced)
/// write lease, expecting the artifact's current version to be `expect`.
fn commit_replace_exclusive(node: &Node, expect: u64) -> u64 {
    let stream = node.stream(CACHE_ARTIFACT).expect("stream");
    let mut w = stream
        .writer(Commit::Replace, Coordination::Exclusive)
        .expect("exclusive writer");
    assert_eq!(
        node.artifact(CACHE_ARTIFACT).unwrap().current_version(),
        expect
    );
    w.append_batch(&demo_batch()).expect("append");
    w.commit().expect("commit")
}

#[test]
fn writer_crash_releases_and_fences_lease_deterministic() {
    // Item K, deterministic: an exclusive writer takes the (journalled) write
    // lease, stages a batch, then "crashes" (its writer is `mem::forget`ed,
    // leaking the lease + its WriteLease journal entry). `force_reclaim` replays
    // the journal, force-releases + fences the lease, and a second exclusive
    // writer then acquires and commits — proving the artifact is not wedged.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let mut node = Node::connect(&uds, "writer", registry()).expect("connect");
    node.start_heartbeat(Duration::from_millis(150));
    node.open_artifact(CACHE_ARTIFACT).expect("open_artifact");
    let full_free = coord.artifact_free_total(CACHE_ARTIFACT).expect("artifact");

    // Take the exclusive lease + stage a batch, then simulate a crash.
    {
        let stream = node.stream(CACHE_ARTIFACT).expect("stream");
        let mut writer = stream
            .writer(Commit::Replace, Coordination::Exclusive)
            .expect("exclusive writer");
        writer.append_batch(&demo_batch()).expect("append");
        assert_eq!(
            coord.artifact_write_lease_owner(CACHE_ARTIFACT),
            Some(node.actor_id()),
            "the lease is held by the exclusive writer"
        );
        assert!(
            coord.artifact_free_total(CACHE_ARTIFACT).unwrap() < full_free,
            "staging must consume a chunk"
        );
        std::mem::forget(writer);
    }
    // Mid-crash: the lease is stuck and no version was published.
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(0));
    assert_eq!(
        coord.artifact_write_lease_owner(CACHE_ARTIFACT),
        Some(node.actor_id()),
        "the leaked lease is still held"
    );

    // Drive crash reclaim deterministically: force-release + fence the lease and
    // free the staged chunk.
    let reclaimed = coord.force_reclaim(node.actor_id()).expect("force reclaim");
    assert!(!reclaimed.is_empty(), "the staged chunk must be reclaimed");
    assert!(
        coord.write_leases_reclaimed() >= 1,
        "the coordinator must record the leaked write-lease reclaim"
    );
    assert_eq!(
        coord.artifact_write_lease_owner(CACHE_ARTIFACT),
        Some(0),
        "the leaked lease must be force-released (unwedged)"
    );
    assert_eq!(
        coord.artifact_free_total(CACHE_ARTIFACT),
        Some(full_free),
        "the staged chunk returned to the pool"
    );

    // A second EXCLUSIVE writer acquires the (fenced) lease and commits v1.
    let mut w2 = Node::connect(&uds, "writer2", registry()).expect("connect w2");
    w2.start_heartbeat(Duration::from_millis(150));
    w2.open_artifact(CACHE_ARTIFACT).expect("open_artifact");
    assert_eq!(
        commit_replace_exclusive(&w2, 0),
        1,
        "second writer commits v1"
    );
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(1));
    assert_eq!(
        coord.artifact_write_lease_owner(CACHE_ARTIFACT),
        Some(0),
        "the second writer released its lease cleanly after committing"
    );
}

#[test]
fn writer_crash_releases_and_fences_lease() {
    // Item K, multi-process: a `writer-hang` process takes the exclusive write
    // lease and hangs; `kill -9` leaves it stuck. The lease-monitor replay
    // force-releases + fences it, and a second (in-process) exclusive writer
    // then acquires and commits — the artifact is never permanently wedged.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let wr_r = dir.path().join("writer.result");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // A separate process takes the exclusive lease and hangs holding it.
    let mut writer_hang = spawn("writer-hang", &uds_s, wr_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&wr_r, "LEASED").is_some()
        }),
        "writer-hang never took the exclusive lease"
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord
                .artifact_write_lease_owner(CACHE_ARTIFACT)
                .is_some_and(|o| o != 0)
        }),
        "the exclusive lease should register as held on the artifact head"
    );
    // No version was published while the writer holds the lease.
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(0));

    // kill -9 the writer while it holds the lease.
    writer_hang.0.kill().expect("kill -9 writer-hang");
    let _ = writer_hang.0.wait();

    // The lease monitor replays the dead writer's journal and force-releases +
    // fences the lease: the owner returns to 0 (free).
    let released = wait_until(Duration::from_secs(10), || {
        coord.artifact_write_lease_owner(CACHE_ARTIFACT) == Some(0)
    });
    assert!(
        released,
        "lease was not force-released after writer kill-9 (owner={:?})",
        coord.artifact_write_lease_owner(CACHE_ARTIFACT)
    );
    assert!(
        coord.write_leases_reclaimed() >= 1,
        "the coordinator must record the leaked write-lease reclaim"
    );

    // A second exclusive writer acquires the fenced lease and commits v1.
    let mut w2 = Node::connect(&uds, "writer2", registry()).expect("connect w2");
    w2.start_heartbeat(Duration::from_millis(150));
    w2.open_artifact(CACHE_ARTIFACT).expect("open_artifact");
    assert_eq!(
        commit_replace_exclusive(&w2, 0),
        1,
        "second writer commits v1"
    );
    assert_eq!(coord.artifact_current_version(CACHE_ARTIFACT), Some(1));
}

// ---------------------------------------------------------------------------
// ADR-0003 item E — cross-process SCHEMA COORDINATOR
// ---------------------------------------------------------------------------

#[test]
fn coordinator_interns_idempotently_by_content() {
    // Item E, unit-level: interning the SAME schema twice yields the SAME id
    // (idempotent by content hash), and two DIFFERENT schemas get different ids.
    use arrow_schema::{DataType, Field, Schema};

    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let coord = Coordinator::bind(&uds, config).expect("bind coordinator");

    let schema_a = demo_schema();
    let schema_b: arrow_schema::SchemaRef = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));

    let bytes_a = shm_arrow::serialize_schema(&schema_a);
    let bytes_b = shm_arrow::serialize_schema(&schema_b);

    let id_a1 = coord.intern_schema_bytes(&bytes_a);
    let id_a2 = coord.intern_schema_bytes(&bytes_a);
    let id_b = coord.intern_schema_bytes(&bytes_b);

    assert_eq!(id_a1, id_a2, "same schema must intern to the same id");
    assert_ne!(id_a1, id_b, "different schemas must get different ids");
    assert_ne!(id_a1, 0, "an interned id is never RAW_SCHEMA_ID (0)");

    // Resolving an issued id returns the exact bytes; an unknown id is None.
    assert_eq!(
        coord.resolve_schema_bytes(id_a1).as_deref(),
        Some(&bytes_a[..])
    );
    assert_eq!(coord.resolve_schema_bytes(9999), None);
}

#[test]
fn two_unseeded_nodes_agree_on_schema_via_coordinator() {
    // Item E, the key integration test: a producer and a consumer that were NOT
    // seeded with identical registries (both start EMPTY) still agree on a schema
    // — the producer interns it and writes a version; the consumer reads the id
    // from the manifest, resolves the schema through the coordinator, and
    // reconstructs a batch equal to the original, read zero-copy.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Two independent, EMPTY local caches — no identical seeding.
    let mut producer =
        Node::connect(&uds, "producer", Arc::new(SchemaRegistry::new())).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let mut consumer =
        Node::connect(&uds, "consumer", Arc::new(SchemaRegistry::new())).expect("consumer connect");
    consumer.start_heartbeat(Duration::from_millis(150));
    assert!(
        producer.registry().is_empty() && consumer.registry().is_empty(),
        "both nodes must start with an empty (unseeded) registry"
    );

    // Producer negotiates the schema id with the coordinator, then commits v1.
    producer
        .open_artifact(CACHE_ARTIFACT)
        .expect("open_artifact");
    let issued = producer
        .intern_schema(&demo_schema())
        .expect("intern_schema");
    assert_ne!(issued, 0, "coordinator issued a non-raw id");
    assert_eq!(commit_replace(&producer, 0), 1, "producer commits v1");

    // Consumer has never seen the schema. It pins v1, reads the manifest's
    // schema_id, and resolves the schema THROUGH THE COORDINATOR.
    consumer
        .open_artifact(CACHE_ARTIFACT)
        .expect("open_artifact");
    let pin = consumer.pin_artifact(CACHE_ARTIFACT).expect("pin");
    let manifest_schema_id = pin.manifest().schema_id;
    assert_eq!(
        manifest_schema_id, issued,
        "the id in the manifest is the coordinator-issued id the producer interned"
    );

    let resolved = consumer
        .resolve_schema(manifest_schema_id)
        .expect("resolve_schema");
    assert_eq!(
        resolved,
        demo_schema(),
        "the resolved schema equals the original — agreement without identical seeding"
    );

    // Reconstruct the batch zero-copy over the resolved schema and verify it
    // equals the producer's batch exactly.
    let batch = pin.as_arrow(consumer.registry()).expect("as_arrow");
    shm_runtime::demo::verify_demo_batch(&batch).expect("reconstructed batch matches original");
    assert_eq!(
        demo_derive(&batch).expect("derive"),
        (DEMO_ID_SUM, DEMO_ROWS as i64),
        "the consumer derived the right result from the zero-copy batch"
    );
}

// ---------------------------------------------------------------------------
// ADR-0003 item F — nested (Struct/List) MULTI-CHUNK version, coordinator schema
// ---------------------------------------------------------------------------

/// Commit one Replace version of the nested multi-chunk batch through `node`.
fn commit_nested_replace(node: &Node, expect: u64) -> u64 {
    let stream = node.stream(NESTED_ARTIFACT).expect("stream");
    let mut w = stream
        .writer(
            Commit::Replace,
            Coordination::Optimistic {
                expect_version: expect,
            },
        )
        .expect("writer");
    w.append_batch(&nested_batch())
        .expect("append nested batch");
    w.commit().expect("commit")
}

#[test]
fn nested_multichunk_version_via_coordinator_reads_zero_copy() {
    // The "real Arrow tables" half of the S5 hostile cache loop (item F + E): a
    // producer interns a NESTED schema (Struct + List) through the coordinator
    // and commits a MULTI-CHUNK version; an UNSEEDED consumer pins it, resolves
    // the schema through the coordinator, and reconstructs the batch — spanning
    // several chunks — zero-copy, equal to the original.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Two independent, EMPTY local caches — no identical seeding.
    let mut producer =
        Node::connect(&uds, "producer", Arc::new(SchemaRegistry::new())).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let mut consumer =
        Node::connect(&uds, "consumer", Arc::new(SchemaRegistry::new())).expect("consumer connect");
    consumer.start_heartbeat(Duration::from_millis(150));
    assert!(producer.registry().is_empty() && consumer.registry().is_empty());

    // Producer negotiates the NESTED schema id, then commits a multi-chunk v1.
    producer
        .open_artifact(NESTED_ARTIFACT)
        .expect("open_artifact");
    let issued = producer
        .intern_schema(&nested_schema())
        .expect("intern nested schema");
    assert_ne!(
        issued, 0,
        "coordinator issued a non-raw id for the nested schema"
    );
    assert_eq!(
        commit_nested_replace(&producer, 0),
        1,
        "producer commits nested v1"
    );

    // Consumer has never seen the schema. It pins v1 and inspects the manifest.
    consumer
        .open_artifact(NESTED_ARTIFACT)
        .expect("open_artifact");
    let pin = consumer.pin_artifact(NESTED_ARTIFACT).expect("pin");
    let m = pin.manifest();
    assert!(
        m.chunks.len() >= 2,
        "the nested version must be MULTI-CHUNK (got {} chunks)",
        m.chunks.len()
    );
    assert_eq!(
        m.batch_spans,
        vec![m.chunks.len() as u32],
        "one nested batch spanning all its chunks"
    );
    assert_eq!(
        m.schema_id, issued,
        "the manifest carries the coordinator-issued nested schema id"
    );

    // Resolve the nested schema THROUGH THE COORDINATOR (unseeded consumer).
    let resolved = consumer
        .resolve_schema(m.schema_id)
        .expect("resolve nested schema");
    assert_eq!(
        resolved,
        nested_schema(),
        "resolved nested schema equals the original"
    );

    // Reconstruct the multi-chunk nested batch zero-copy and verify it exactly.
    let batch = pin
        .as_arrow(consumer.registry())
        .expect("as_arrow nested multi-chunk");
    verify_nested_batch(&batch).expect("reconstructed nested batch matches the original");
}

// ---------------------------------------------------------------------------
// ADR-0003 S5 — the HOSTILE CACHE LOOP: E + F + J + K chained, with the
// definitive ZERO-LEAK pool census.
// ---------------------------------------------------------------------------

/// Commit one Replace version of the nested multi-chunk batch through `node`
/// under the **exclusive** (fenced) write lease. The predecessor is whatever
/// `current` reads now; returns the new version.
fn commit_nested_exclusive(node: &Node) -> u64 {
    let stream = node.stream(NESTED_ARTIFACT).expect("stream");
    let mut w = stream
        .writer(Commit::Replace, Coordination::Exclusive)
        .expect("exclusive nested writer");
    w.append_batch(&nested_batch())
        .expect("append nested batch");
    w.commit().expect("commit")
}

/// An UNSEEDED node opened on the nested artifact, heartbeating and ready to
/// negotiate the nested schema through the coordinator (item E).
fn nested_node(uds: &std::path::Path, name: &str) -> Node {
    let mut node = Node::connect(uds, name, Arc::new(SchemaRegistry::new()))
        .unwrap_or_else(|e| panic!("connect {name}: {e}"));
    node.start_heartbeat(Duration::from_millis(150));
    node.open_artifact(NESTED_ARTIFACT).expect("open_artifact");
    node
}

#[test]
fn hostile_cache_loop() {
    // ADR-0003 S5 "hostile cache loop", chained end-to-end across REAL
    // processes (the coordinator runs in-process so it can census the artifact
    // data pool; the crashing actors are separate OS processes spawned via
    // `shm-cacheloop`). One nested Struct/List MULTI-CHUNK artifact carries the
    // whole scenario, so a single pool census proves ZERO chunk leakage:
    //
    //   Leg 1 (E+F): a `producer` process — seeded with an EMPTY registry —
    //     interns a NESTED schema through the coordinator and commits a
    //     multi-chunk v1; an UNSEEDED in-process consumer resolves the schema
    //     through the coordinator and reads the batch back zero-copy == original.
    //   Leg 2 (J): a `worker-pin-hang` process journal-pins v1 and is `kill -9`ed
    //     MID-PIN; after v2 supersedes v1, the lease-monitor replay decrements
    //     v1's pin count and retires it (its chunks reclaimed).
    //   Leg 3 (K): a `writer-hang` process takes the exclusive write lease, stages
    //     a nested batch, and is `kill -9`ed MID-WRITE; the lease monitor
    //     force-releases + FENCES the lease and reclaims the staged chunks; a
    //     second exclusive writer then acquires and commits v3.
    //   Census: the artifact data pool's free count returns EXACTLY to the
    //     one-live-version baseline — every chunk of every crashed/superseded
    //     nested version is back in the pool.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let prod_r = dir.path().join("producer.result");
    let pin_r = dir.path().join("pin.result");
    let wr_r = dir.path().join("writer.result");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // --- Leg 1: a real producer process commits a nested multi-chunk v1. ---
    let _producer = spawn_nested("producer", &uds_s, prod_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            coord.artifact_current_version(NESTED_ARTIFACT) == Some(1)
        }),
        "producer never committed nested v1 (result: {:?})",
        std::fs::read_to_string(&prod_r).ok()
    );
    // BASELINE census: exactly one live (nested, multi-chunk) version's chunks
    // are allocated. Every later assertion returns the pool to this number.
    let one_version_free = coord
        .artifact_free_total(NESTED_ARTIFACT)
        .expect("nested artifact is known to the coordinator");

    // Read-back leg: an UNSEEDED consumer resolves the schema through the
    // coordinator and reconstructs the multi-chunk nested batch zero-copy.
    {
        let consumer = nested_node(&uds, "consumer");
        let pin = consumer.pin_artifact(NESTED_ARTIFACT).expect("pin v1");
        assert_eq!(pin.version(), 1, "consumer pins nested v1");
        assert!(
            pin.manifest().chunks.len() >= 2,
            "nested v1 must be MULTI-CHUNK (got {} chunks)",
            pin.manifest().chunks.len()
        );
        consumer
            .resolve_schema(pin.manifest().schema_id)
            .expect("resolve nested schema via coordinator");
        let batch = pin.as_arrow(consumer.registry()).expect("as_arrow nested");
        verify_nested_batch(&batch).expect("zero-copy nested batch equals the original");
        // Clean drop of the pin.
    }
    // The consumer's pin+read was clean: the census sits back at the baseline.
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_free_total(NESTED_ARTIFACT) == Some(one_version_free)
        }),
        "consumer read-back leaked (baseline={one_version_free}, now={:?})",
        coord.artifact_free_total(NESTED_ARTIFACT)
    );

    // --- Leg 2 (item J): a reader journal-pins v1 and is kill -9ed MID-PIN. ---
    let mut pin_hang = spawn_nested("worker-pin-hang", &uds_s, pin_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&pin_r, "PINNED").is_some()
        }),
        "worker-pin-hang never pinned a version (result: {:?})",
        std::fs::read_to_string(&pin_r).ok()
    );
    assert_eq!(
        line_tokens(&pin_r, "PINNED").unwrap()[1],
        "1",
        "worker-pin-hang should pin nested v1"
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_slot_pins(NESTED_ARTIFACT, 1) == Some(1)
        }),
        "the worker's journalled pin should register on v1's slot"
    );

    // Supersede v1 with v2 (in-process): v1 goes non-current but the hung pin
    // holds its chunks against reclamation.
    let committer = nested_node(&uds, "committer");
    committer
        .intern_schema(&nested_schema())
        .expect("intern nested schema");
    assert_eq!(
        commit_nested_replace(&committer, 1),
        2,
        "committer installs v2"
    );
    assert_eq!(coord.artifact_current_version(NESTED_ARTIFACT), Some(2));
    assert_eq!(
        coord.artifact_slot_pins(NESTED_ARTIFACT, 1),
        Some(1),
        "v1 is held by the hung worker's pin"
    );
    // Two live versions are now allocated (pinned v1 + current v2): the pool
    // census has dropped below the one-version baseline.
    assert!(
        coord.artifact_free_total(NESTED_ARTIFACT).unwrap() < one_version_free,
        "a second live version must consume more chunks (baseline={one_version_free}, now={:?})",
        coord.artifact_free_total(NESTED_ARTIFACT)
    );

    // kill -9 the pinner mid-pin: the lease-monitor replay decrements v1's pin
    // count and retires v1, returning the pool to the one-version baseline.
    pin_hang.0.kill().expect("kill -9 worker-pin-hang");
    let _ = pin_hang.0.wait();
    assert!(
        wait_until(Duration::from_secs(10), || {
            coord.artifact_slot_pins(NESTED_ARTIFACT, 1).is_none()
                && coord.artifact_free_total(NESTED_ARTIFACT) == Some(one_version_free)
        }),
        "v1 not reclaimed after pin-hang kill-9 (slot_pins={:?}, free={:?} vs baseline={one_version_free})",
        coord.artifact_slot_pins(NESTED_ARTIFACT, 1),
        coord.artifact_free_total(NESTED_ARTIFACT)
    );
    assert!(
        coord.artifact_pins_reclaimed() >= 1,
        "the coordinator must record the leaked artifact-pin reclaim"
    );

    // --- Leg 3 (item K): an exclusive writer is kill -9ed MID-WRITE. ---
    let mut writer_hang = spawn_nested("writer-hang", &uds_s, wr_r.to_str());
    assert!(
        wait_until(Duration::from_secs(20), || {
            line_tokens(&wr_r, "LEASED").is_some()
        }),
        "writer-hang never took the exclusive lease (result: {:?})",
        std::fs::read_to_string(&wr_r).ok()
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord
                .artifact_write_lease_owner(NESTED_ARTIFACT)
                .is_some_and(|o| o != 0)
        }),
        "the exclusive lease should register as held on the artifact head"
    );
    // The staged (uncommitted) nested batch consumes chunks; still no version.
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_free_total(NESTED_ARTIFACT).unwrap() < one_version_free
        }),
        "the writer's staged nested batch must consume chunks"
    );
    assert_eq!(
        coord.artifact_current_version(NESTED_ARTIFACT),
        Some(2),
        "no version may be published while the lease is held"
    );

    // kill -9 the writer mid-write: the lease monitor force-releases + fences the
    // lease and reclaims the staged chunks, back to the one-version baseline.
    writer_hang.0.kill().expect("kill -9 writer-hang");
    let _ = writer_hang.0.wait();
    assert!(
        wait_until(Duration::from_secs(10), || {
            coord.artifact_write_lease_owner(NESTED_ARTIFACT) == Some(0)
                && coord.artifact_free_total(NESTED_ARTIFACT) == Some(one_version_free)
        }),
        "lease not force-released / staged not reclaimed after writer kill-9 \
         (owner={:?}, free={:?} vs baseline={one_version_free})",
        coord.artifact_write_lease_owner(NESTED_ARTIFACT),
        coord.artifact_free_total(NESTED_ARTIFACT)
    );
    assert!(
        coord.write_leases_reclaimed() >= 1,
        "the coordinator must record the leaked write-lease reclaim"
    );

    // A second EXCLUSIVE writer acquires the fenced lease and commits nested v3.
    let w2 = nested_node(&uds, "writer2");
    w2.intern_schema(&nested_schema())
        .expect("intern nested schema");
    assert_eq!(
        commit_nested_exclusive(&w2),
        3,
        "second writer commits nested v3"
    );
    assert_eq!(coord.artifact_current_version(NESTED_ARTIFACT), Some(3));
    assert_eq!(
        coord.artifact_write_lease_owner(NESTED_ARTIFACT),
        Some(0),
        "the second writer released its lease cleanly after committing"
    );

    // --- FINAL ZERO-LEAK CENSUS. ---
    // After all crashes are reclaimed, all versions superseded/retired, and one
    // clean nested version (v3) installed, the pool must be EXACTLY back at the
    // one-live-version baseline. A single un-reclaimed continuation chunk, a
    // leaked pin, or a stuck lease's staged chunks would leave `free` below it.
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_free_total(NESTED_ARTIFACT) == Some(one_version_free)
        }),
        "FINAL CENSUS FAILED — chunk leak: free={:?} != one-version baseline={one_version_free} \
         (pins_reclaimed={}, leases_reclaimed={})",
        coord.artifact_free_total(NESTED_ARTIFACT),
        coord.artifact_pins_reclaimed(),
        coord.write_leases_reclaimed()
    );
    assert_eq!(
        coord.artifact_free_total(NESTED_ARTIFACT),
        Some(one_version_free),
        "every chunk of every crashed/superseded nested version returned to the pool"
    );
}

#[test]
fn hostile_cache_loop_deterministic() {
    // ADR-0003 S5, timing-immune variant: the identical coordinator
    // reclaim/fence/retire code paths driven fully in-process (staged crashes via
    // `mem::forget` + `force_reclaim`), so the zero-leak proof holds even if the
    // multi-process kill-9 timing is flaky in a busy sandbox. Same chain, same
    // one-live-version census.
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // --- Leg 1 (E+F): unseeded producer interns the nested schema + commits a
    //     nested multi-chunk v1. ---
    let producer = nested_node(&uds, "producer");
    let empty_free = coord
        .artifact_free_total(NESTED_ARTIFACT)
        .expect("nested artifact known (empty pool)");
    let issued = producer
        .intern_schema(&nested_schema())
        .expect("intern nested");
    assert_ne!(
        issued, 0,
        "coordinator issued a non-raw id for the nested schema"
    );
    assert_eq!(
        commit_nested_replace(&producer, 0),
        1,
        "producer commits nested v1"
    );
    let one_version_free = coord
        .artifact_free_total(NESTED_ARTIFACT)
        .expect("nested artifact known");
    let footprint = empty_free - one_version_free;
    assert!(
        footprint >= 3,
        "nested v1 must be genuinely MULTI-CHUNK (footprint={footprint} chunks incl. manifest)"
    );

    // Read-back leg: an unseeded consumer resolves the schema via the coordinator
    // and reconstructs the multi-chunk nested batch zero-copy == original.
    {
        let consumer = nested_node(&uds, "consumer");
        let pin = consumer.pin_artifact(NESTED_ARTIFACT).expect("pin v1");
        assert!(pin.manifest().chunks.len() >= 2, "v1 is multi-chunk");
        consumer
            .resolve_schema(pin.manifest().schema_id)
            .expect("resolve nested schema via coordinator");
        verify_nested_batch(&pin.as_arrow(consumer.registry()).expect("as_arrow"))
            .expect("zero-copy nested batch equals the original");
    }
    assert_eq!(
        coord.artifact_free_total(NESTED_ARTIFACT),
        Some(one_version_free),
        "the consumer read-back was clean (no leak)"
    );

    // --- Leg 2 (item J): journal-pin v1, supersede with v2, leak the pin, reclaim. ---
    let pinner = nested_node(&uds, "pinner");
    let pin = pinner
        .pin_artifact(NESTED_ARTIFACT)
        .expect("journalled pin");
    assert_eq!(pin.version(), 1);
    assert_eq!(coord.artifact_slot_pins(NESTED_ARTIFACT, 1), Some(1));

    let committer = nested_node(&uds, "committer");
    committer
        .intern_schema(&nested_schema())
        .expect("intern nested");
    assert_eq!(
        commit_nested_replace(&committer, 1),
        2,
        "committer installs v2"
    );
    assert_eq!(
        coord.artifact_slot_pins(NESTED_ARTIFACT, 1),
        Some(1),
        "v1 is held by the (soon-leaked) pin"
    );
    assert!(
        coord.artifact_free_total(NESTED_ARTIFACT).unwrap() < one_version_free,
        "two live versions must be allocated"
    );

    // `kill -9` mid-pin: forget the pin so its Drop never runs (the ArtifactPin
    // journal entry + the +1 pin count leak, exactly as a crash leaves them).
    std::mem::forget(pin);
    let _ = coord
        .force_reclaim(pinner.actor_id())
        .expect("force reclaim pinner");
    assert!(
        coord.artifact_pins_reclaimed() >= 1,
        "the coordinator must record the leaked artifact-pin reclaim"
    );
    assert_eq!(
        coord.artifact_slot_pins(NESTED_ARTIFACT, 1),
        None,
        "v1's slot is freed once its leaked pin is reclaimed"
    );
    assert_eq!(
        coord.artifact_free_total(NESTED_ARTIFACT),
        Some(one_version_free),
        "v1's chunks returned to the pool after crash reclaim"
    );

    // --- Leg 3 (item K): exclusive writer stages a nested batch, leaks, reclaim. ---
    let writer = nested_node(&uds, "writer");
    writer
        .intern_schema(&nested_schema())
        .expect("intern nested");
    {
        let stream = writer.stream(NESTED_ARTIFACT).expect("stream");
        let mut w = stream
            .writer(Commit::Replace, Coordination::Exclusive)
            .expect("exclusive writer");
        w.append_batch(&nested_batch()).expect("append nested");
        assert_eq!(
            coord.artifact_write_lease_owner(NESTED_ARTIFACT),
            Some(writer.actor_id()),
            "the lease is held by the exclusive writer"
        );
        assert!(
            coord.artifact_free_total(NESTED_ARTIFACT).unwrap() < one_version_free,
            "staging a nested batch must consume chunks"
        );
        std::mem::forget(w); // kill -9 mid-write: leak the lease + staged chunks.
    }
    assert_eq!(
        coord.artifact_current_version(NESTED_ARTIFACT),
        Some(2),
        "no version was published while the lease was held"
    );
    let reclaimed = coord
        .force_reclaim(writer.actor_id())
        .expect("force reclaim writer");
    assert!(
        !reclaimed.is_empty(),
        "the staged nested chunks must be reclaimed"
    );
    assert!(
        coord.write_leases_reclaimed() >= 1,
        "the coordinator must record the leaked write-lease reclaim"
    );
    assert_eq!(
        coord.artifact_write_lease_owner(NESTED_ARTIFACT),
        Some(0),
        "the leaked lease was force-released (unwedged)"
    );
    assert_eq!(
        coord.artifact_free_total(NESTED_ARTIFACT),
        Some(one_version_free),
        "the staged chunks returned to the pool"
    );

    // A second exclusive writer acquires the fenced lease and commits nested v3.
    let w2 = nested_node(&uds, "writer2");
    w2.intern_schema(&nested_schema()).expect("intern nested");
    assert_eq!(
        commit_nested_exclusive(&w2),
        3,
        "second writer commits nested v3"
    );
    assert_eq!(coord.artifact_current_version(NESTED_ARTIFACT), Some(3));
    assert_eq!(coord.artifact_write_lease_owner(NESTED_ARTIFACT), Some(0));

    // --- FINAL ZERO-LEAK CENSUS. ---
    assert_eq!(
        coord.artifact_free_total(NESTED_ARTIFACT),
        Some(one_version_free),
        "FINAL CENSUS: every chunk of every crashed/superseded nested version returned \
         (baseline={one_version_free}, footprint={footprint}, pins_reclaimed={}, leases_reclaimed={})",
        coord.artifact_pins_reclaimed(),
        coord.write_leases_reclaimed()
    );
}
