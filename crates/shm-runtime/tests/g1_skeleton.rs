//! The ADR-0007 **G1 × G3** typed-ref dispatch/resolve walking skeleton (mirrors
//! `store_skeleton.rs` and the hostile cache-loop census).
//!
//! - [`g1_dispatch_resolve_same_process`] — the full functional loop driven in one
//!   process (timing-immune): a producer commits `dataset/X` v1; a **front**
//!   dispatches a `TypedRef::by_key(Dataset, …)` through the task queue; a
//!   **worker** claims it, reads the envelope, `resolve_and_pin`s the dataset
//!   zero-copy, derives `sum(id)`, commits a `Result` entry `result/X`, and
//!   `complete`s the task with a by-key `TypedRef` to it; the front reads
//!   `result/X` and asserts the derived value. Then evict + a zero-leak census.
//! - [`g1_crash_reclaim_same_process_deterministic`] — the crash path without the
//!   lease clock: a first worker resolves+pins then "crashes" (`mem::forget` +
//!   `force_reclaim`), the task is requeued (`reap_tasks`), a second worker
//!   completes, and the store data-pool census returns to baseline.
//! - [`g1_crash_reclaim_multiprocess`] — the target: a real coordinator grants the
//!   store + task queue to a producer and a `--crash-first` worker spawned as
//!   separate OS processes. The worker is `kill -9`ed **while holding the dataset
//!   pin**; the coordinator's lease-sweep releases the leaked entry pin and
//!   requeues the task (at-least-once); a second worker completes; the front reads
//!   the result; evict + census returns to baseline (zero leak).

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_runtime::demo::{
    demo_batch, demo_derive, demo_schema, result_batch, result_schema, result_value, DEMO_ID_SUM,
};
use shm_runtime::{Coordinator, Node, Result, RuntimeConfig};
use shm_store::{RefKind, TypedRef};
use shm_task::{now_nanos, Outcome};

const DATASET: &str = "dataset/X";
const RESULT: &str = "result/X";

fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
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

/// A far-future absolute submit deadline (the worker re-stamps a fresh lease on
/// claim, so this only bounds the pre-claim window).
fn far_deadline() -> u64 {
    now_nanos().wrapping_add(60_000_000_000)
}

/// The producer half: create `DATASET` and commit the demo batch once (v1).
fn produce(node: &mut Node) -> Result<()> {
    node.intern_schema(&demo_schema())?;
    let entry = node
        .store()
        .and_then(|s| Ok(s.create(DATASET.as_bytes(), RefKind::Dataset, &demo_schema())?))?;
    entry.commit_replace(&demo_batch())?;
    Ok(())
}

/// The worker half (in-process): claim the one queued task, read its envelope,
/// resolve+pin the dataset, derive `sum(id)`, commit `RESULT`, and complete the
/// task with a by-key `TypedRef` to it (freeing the request envelope). Returns
/// the derived sum.
fn work_once(node: &mut Node) -> Result<i64> {
    node.open_store()?;
    node.open_task_queue()?;
    let tq = node.task_queue()?;
    let claimed = tq
        .claim(CLAIM_LEASE)
        .expect("a task must be queued for the worker");

    let tref = node.task_ref(&claimed)?;
    node.resolve_schema(tref.schema_id)?;
    let (_e, pin, batch) = node.store().and_then(|s| Ok(s.resolve_and_pin(&tref)?))?;
    let (sum, _rows) = demo_derive(&batch).expect("derive");
    drop((pin, batch));

    let rsid = node.intern_schema(&result_schema())?;
    let rkid = node.intern_key(RESULT.as_bytes())?;
    {
        let s = node.store()?;
        let e = s.create(RESULT.as_bytes(), RefKind::Result, &result_schema())?;
        e.commit_replace(&result_batch(sum))?;
    }

    let rref = TypedRef::by_key(RefKind::Result, rkid, rsid, 0);
    let rdesc = node.write_ref_chunk(&rref)?;
    let request_desc = claimed.request();
    claimed.complete(rdesc)?;
    node.free_ref_chunk(&request_desc)?;
    Ok(sum)
}

/// The front half: read the completed task's result envelope, resolve `RESULT`,
/// and return the derived value; free the result envelope.
fn read_result(node: &mut Node, result_desc: shm_core::ChunkDesc) -> Result<i64> {
    let rref = node.read_ref_chunk(&result_desc)?;
    assert_eq!(rref.kind().unwrap(), RefKind::Result);
    node.resolve_schema(rref.schema_id)?;
    let (_e, pin, batch) = node.store().and_then(|s| Ok(s.resolve_and_pin(&rref)?))?;
    let got = result_value(&batch).expect("result value");
    drop((pin, batch));
    node.free_ref_chunk(&result_desc)?;
    Ok(got)
}

/// A 1-second claim lease (nanoseconds), matching the binary's `CLAIM_LEASE`.
const CLAIM_LEASE: u64 = 1_000_000_000;

#[test]
fn g1_dispatch_resolve_same_process() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind");
    coord.start().expect("start");
    let baseline = coord.store_data_free_total().expect("baseline");

    // Producer.
    let mut producer =
        Node::connect(&uds, "producer", Arc::new(SchemaRegistry::new())).expect("producer");
    producer.start_heartbeat(Duration::from_millis(150));
    produce(&mut producer).expect("produce v1");
    assert_eq!(coord.store_entry_version(DATASET.as_bytes()), Some(1));

    // Front dispatches a by-key TypedRef through the task queue.
    let mut front = Node::connect(&uds, "front", Arc::new(SchemaRegistry::new())).expect("front");
    front.start_heartbeat(Duration::from_millis(150));
    let schema_id = front.intern_schema(&demo_schema()).expect("intern schema");
    let ds_key_id = front.intern_key(DATASET.as_bytes()).expect("intern key");
    let tref = TypedRef::by_key(RefKind::Dataset, ds_key_id, schema_id, 0);
    let handle = front
        .submit_typed_task(&tref, far_deadline())
        .expect("submit typed task");

    // Worker claims + processes the one queued task.
    let mut worker =
        Node::connect(&uds, "worker", Arc::new(SchemaRegistry::new())).expect("worker");
    worker.start_heartbeat(Duration::from_millis(150));
    let sum = work_once(&mut worker).expect("work once");
    assert_eq!(sum, DEMO_ID_SUM, "worker derived sum(id)");

    // Front reads the result the worker committed + referenced back.
    let result_desc = match front.task_queue().unwrap().poll(handle).expect("poll") {
        shm_task::TaskStatus::Done(d) => d,
        other => panic!("task not done: {other:?}"),
    };
    let got = read_result(&mut front, result_desc).expect("read result");
    assert_eq!(got, DEMO_ID_SUM, "requester read the derived result back");

    // Evict both keys; the two envelopes were freed by worker/front. Census → baseline.
    {
        let s = front.store().unwrap();
        s.evict(DATASET.as_bytes()).expect("evict dataset");
        s.evict(RESULT.as_bytes()).expect("evict result");
    }
    assert_eq!(coord.store_entry_version(DATASET.as_bytes()), None);
    assert_eq!(coord.store_entry_version(RESULT.as_bytes()), None);
    assert_eq!(
        coord.store_data_free_total().unwrap(),
        baseline,
        "evict + free envelopes returns the store data pool to baseline (zero leak)"
    );

    let _ = producer.say_bye();
}

#[test]
fn g1_crash_reclaim_same_process_deterministic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind");
    coord.start().expect("start");
    let baseline = coord.store_data_free_total().expect("baseline");

    // Producer commits dataset/X v1.
    let mut producer =
        Node::connect(&uds, "producer", Arc::new(SchemaRegistry::new())).expect("producer");
    producer.start_heartbeat(Duration::from_millis(150));
    produce(&mut producer).expect("produce v1");

    // Front dispatches the typed task.
    let mut front = Node::connect(&uds, "front", Arc::new(SchemaRegistry::new())).expect("front");
    front.start_heartbeat(Duration::from_millis(150));
    let schema_id = front.intern_schema(&demo_schema()).expect("intern schema");
    let ds_key_id = front.intern_key(DATASET.as_bytes()).expect("intern key");
    let tref = TypedRef::by_key(RefKind::Dataset, ds_key_id, schema_id, 0);
    let handle = front
        .submit_typed_task(&tref, far_deadline())
        .expect("submit typed task");

    // Worker A claims + resolve_and_pins, then "crashes" holding the pin.
    let mut worker_a =
        Node::connect(&uds, "worker-a", Arc::new(SchemaRegistry::new())).expect("worker-a");
    worker_a.start_heartbeat(Duration::from_millis(150));
    worker_a.open_store().unwrap();
    worker_a.open_task_queue().unwrap();
    let claimed = {
        let tq = worker_a.task_queue().unwrap();
        tq.claim(CLAIM_LEASE).expect("task queued")
    };
    let a_tref = worker_a.task_ref(&claimed).unwrap();
    worker_a.resolve_schema(a_tref.schema_id).unwrap();
    let (_e, pin, batch) = worker_a
        .store()
        .map(|s| s.resolve_and_pin(&a_tref).unwrap())
        .unwrap();
    assert_eq!(
        coord.store_entry_pins(DATASET.as_bytes(), 1),
        Some(1),
        "worker A's entry pin is live"
    );

    // Simulate a `kill -9` mid-pin: leak the pin (Drop never runs) and drop the
    // claim without completing (its lease will lapse → requeue).
    std::mem::forget(pin);
    drop(batch);
    drop(claimed);

    // Drive the exact crash-reclaim: journal replay releases the leaked entry pin.
    coord
        .force_reclaim(worker_a.actor_id())
        .expect("force reclaim worker A");
    assert_eq!(
        coord.store_entry_pins(DATASET.as_bytes(), 1),
        Some(0),
        "leaked entry pin released by journal replay"
    );
    assert_eq!(coord.artifact_pins_reclaimed(), 1);

    // Requeue the lapsed task (at-least-once): its claim deadline is in the past.
    let report = coord.reap_tasks(far_deadline());
    assert_eq!(report.requeued, 1, "the lapsed claim was requeued");

    // Worker B re-claims the requeued task and completes it.
    let mut worker_b =
        Node::connect(&uds, "worker-b", Arc::new(SchemaRegistry::new())).expect("worker-b");
    worker_b.start_heartbeat(Duration::from_millis(150));
    let sum = work_once(&mut worker_b).expect("worker B completes");
    assert_eq!(sum, DEMO_ID_SUM);

    // Front reads the result.
    let result_desc = match front.task_queue().unwrap().poll(handle).expect("poll") {
        shm_task::TaskStatus::Done(d) => d,
        other => panic!("task not done: {other:?}"),
    };
    assert_eq!(read_result(&mut front, result_desc).unwrap(), DEMO_ID_SUM);

    // Evict + census → baseline (zero leak).
    {
        let s = front.store().unwrap();
        s.evict(DATASET.as_bytes()).unwrap();
        s.evict(RESULT.as_bytes()).unwrap();
    }
    assert_eq!(
        coord.store_data_free_total().unwrap(),
        baseline,
        "evict + free envelopes returns to baseline"
    );

    let _ = producer.say_bye();
}

#[test]
fn g1_crash_reclaim_multiprocess() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let producer_result = dir.path().join("producer.result");
    let crash_result = dir.path().join("crash.result");
    let uds_s = uds.to_str().unwrap().to_string();

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind");
    coord.start().expect("start");
    let baseline = coord.store_data_free_total().expect("baseline");

    let exe = env!("CARGO_BIN_EXE_shm-g1-skeleton");

    // --- Producer process: dataset/X v1. ---
    let producer = Command::new(exe)
        .args([
            "producer",
            "--uds",
            &uds_s,
            "--dataset",
            DATASET,
            "--result",
            producer_result.to_str().unwrap(),
        ])
        .spawn()
        .expect("spawn producer");
    let mut producer = Reaper(producer);
    assert!(
        wait_until(Duration::from_secs(20), || {
            coord.store_entry_version(DATASET.as_bytes()) == Some(1)
        }),
        "producer never drove dataset/X to v1 (result: {:?})",
        std::fs::read_to_string(&producer_result).ok()
    );

    // --- Front (this process): dispatch the typed task. ---
    let mut front = Node::connect(&uds, "front", Arc::new(SchemaRegistry::new())).expect("front");
    front.start_heartbeat(Duration::from_millis(150));
    let schema_id = front.intern_schema(&demo_schema()).expect("intern schema");
    let ds_key_id = front.intern_key(DATASET.as_bytes()).expect("intern key");
    let tref = TypedRef::by_key(RefKind::Dataset, ds_key_id, schema_id, 0);
    let handle = front
        .submit_typed_task(&tref, far_deadline())
        .expect("submit typed task");

    // --- Crash-first worker: claims, resolve_and_pins, idles holding the pin. ---
    let crasher = Command::new(exe)
        .args([
            "worker",
            "--uds",
            &uds_s,
            "--dataset",
            DATASET,
            "--result-key",
            RESULT,
            "--crash-first",
            "--result",
            crash_result.to_str().unwrap(),
        ])
        .spawn()
        .expect("spawn crash worker");
    let mut crasher = Reaper(crasher);
    assert!(
        wait_until(Duration::from_secs(20), || {
            std::fs::read_to_string(&crash_result)
                .map(|s| s.trim() == "PINNED")
                .unwrap_or(false)
        }),
        "crash worker never pinned dataset/X (result: {:?})",
        std::fs::read_to_string(&crash_result).ok()
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.store_entry_pins(DATASET.as_bytes(), 1).unwrap_or(0) >= 1
        }),
        "crash worker's entry pin was never observed live"
    );

    // --- kill -9 the worker WHILE it holds the dataset pin. ---
    crasher.0.kill().expect("kill -9 crash worker");
    let _ = crasher.0.wait();

    // The lease-sweep + journal replay releases the leaked entry pin.
    assert!(
        wait_until(Duration::from_secs(6), || {
            coord.store_entry_pins(DATASET.as_bytes(), 1) == Some(0)
        }),
        "leaked entry pin was not released by lease-sweep (pins: {:?})",
        coord.store_entry_pins(DATASET.as_bytes(), 1)
    );

    // --- Second worker re-claims the requeued task (at-least-once) + completes. ---
    let worker2 = Command::new(exe)
        .args([
            "worker",
            "--uds",
            &uds_s,
            "--dataset",
            DATASET,
            "--result-key",
            RESULT,
        ])
        .spawn()
        .expect("spawn worker2");
    let mut worker2 = Reaper(worker2);

    // The front's wait returns Done once worker2 completes the requeued task.
    let outcome = front.task_queue().unwrap().wait(handle).expect("wait outcome");
    let result_desc = match outcome {
        Outcome::Done(d) => d,
        other => panic!("task did not complete: {other:?}"),
    };
    assert_eq!(
        read_result(&mut front, result_desc).expect("read result"),
        DEMO_ID_SUM,
        "requester read the derived result after crash + re-dispatch"
    );

    // --- Evict + zero-leak census. ---
    {
        let s = front.store().unwrap();
        s.evict(DATASET.as_bytes()).expect("evict dataset");
        s.evict(RESULT.as_bytes()).expect("evict result");
    }
    assert_eq!(coord.store_entry_version(DATASET.as_bytes()), None);
    assert_eq!(coord.store_entry_version(RESULT.as_bytes()), None);
    assert_eq!(
        coord.store_data_free_total().unwrap(),
        baseline,
        "crash + re-dispatch + evict returns the store data pool to baseline (zero leak)"
    );

    let _ = worker2.0.kill();
    let _ = producer.0.kill();
}
