//! v0.4 stage O §1 — the **kill-at-every-transition crash matrix**.
//!
//! v0.3 proved survival of two *scripted* `kill -9`s. This proves survival of
//! adversarial *timing*: for each reclaimable state transition across the four
//! primitives, a real OS process is driven to that exact transition and then
//! `std::process::abort()`s (see the `kill-at` role in `bin/shm-cacheloop.rs`) —
//! dying with the resource still held and no destructor run. The coordinator runs
//! **in-process** so it can census the artifact data pool + task queue; the
//! crashing actor is a separate process.
//!
//! For every point the assertion is the census: after the dead actor's lease
//! lapses and its journal is replayed, (a) everything it held is reclaimed (the
//! relevant reclaim *counter* increments — proof the resource was genuinely held —
//! and any staged chunk returns to `FREE`), (b) the artifact data pool's free
//! count returns EXACTLY to the pre-actor baseline (zero leak), and (c) a
//! surviving in-process reader still pins + reads a valid version and
//! `current_version` is coherent.
//!
//! Transitions covered (each a distinct reclaimable shared state reachable via the
//! public API — see the module note in `bin/shm-cacheloop.rs` for the ones that
//! are structurally un-injectable and why):
//!
//! | point         | primitive        | held-then-leaked state                    |
//! |---------------|------------------|-------------------------------------------|
//! | `stage-1`     | stream           | 1 staged chunk (LOANED+journalled)        |
//! | `stage-2`     | stream           | 2 staged batches                          |
//! | `lease-only`  | write lease      | exclusive lease held, nothing staged      |
//! | `lease-stage` | write lease+stream | lease held + a staged batch             |
//! | `art-pin`     | artifact pin (J) | version pin on a superseded version       |
//! | `payload-pin` | shared pin       | journalled payload pin (v0.1 path)        |
//! | `task-claim`  | task             | task CLAIMED, never completed             |
//! | `task-submit` | task             | task submitted, submitter dies pre-claim  |

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_core::{ChunkDesc, FREE};
use shm_runtime::demo::{
    demo_batch, demo_schema, verify_demo_batch, CACHE_ARTIFACT, DEMO_ID_SUM, DEMO_TOPIC,
};
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_stream::{Commit, Coordination};
use shm_task::now_nanos;

// --- shared harness (kept local; test crates don't share a module) ------------

/// A per-run segment-id base. A process-local counter gives each test in this
/// binary its own ≥100k-wide id band (so parallel test threads never share a base
/// and clash on POSIX shm names); pid/time jitter separates concurrent processes.
fn unique_seg_base() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    700_000 + n * 100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 50_000) as u32
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

/// An EMPTY local cache: the item-E contract (agreement rests on the coordinator,
/// not identical seeding). Used by the artifact/task actors + workers.
fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::new())
}

/// A demo-seeded cache: the v0.1 payload broadcast path's identical-seeding
/// contract (no coordinator schema resolve on that path).
fn demo_registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
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

/// Spawn a `kill-at` actor pinned to `point`, operating on `art`.
fn spawn_kill_at(point: &str, uds: &str, result: &str, art: &str) -> Reaper {
    let mut cmd = Command::new(exe());
    cmd.args([
        "kill-at",
        "--uds",
        uds,
        "--result",
        result,
        "--kill-at",
        point,
        "--art",
        art,
    ]);
    Reaper(
        cmd.spawn()
            .unwrap_or_else(|e| panic!("spawn kill-at {point}: {e}")),
    )
}

fn spawn_role(role: &str, uds: &str, result: &str) -> Reaper {
    let mut cmd = Command::new(exe());
    cmd.args([role, "--uds", uds, "--result", result]);
    Reaper(cmd.spawn().unwrap_or_else(|e| panic!("spawn {role}: {e}")))
}

/// Boot a coordinator + an in-process survivor producer that commits v1 and stays
/// alive (heartbeating), returning `(coord, survivor, one_version_free)`.
fn boot_with_v1(uds: &std::path::Path) -> (Coordinator, Node, usize) {
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let mut survivor = Node::connect(uds, "survivor", registry()).expect("survivor connect");
    survivor.start_heartbeat(Duration::from_millis(150));
    survivor
        .open_artifact(CACHE_ARTIFACT)
        .expect("open_artifact");
    // Negotiate the schema id through the coordinator (item E) so a manifest a
    // multi-process worker later reads carries a coordinator-resolvable id.
    survivor
        .intern_schema(&demo_schema())
        .expect("intern schema via coordinator");
    assert_eq!(commit_replace(&survivor, 0), 1, "survivor commits v1");
    let one_version_free = coord
        .artifact_free_total(CACHE_ARTIFACT)
        .expect("artifact known");
    (coord, survivor, one_version_free)
}

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

/// Assert the survivor still reads a valid `current_version` version zero-copy
/// (matrix assertion (c): the data plane / artifact stays coherent post-reclaim).
fn assert_survivor_consistent(coord: &Coordinator, survivor: &Node, expect_current: u64) {
    assert_eq!(
        coord.artifact_current_version(CACHE_ARTIFACT),
        Some(expect_current),
        "current_version must stay coherent after the crash reclaim"
    );
    let pin = survivor
        .pin_artifact(CACHE_ARTIFACT)
        .expect("survivor re-pins current");
    assert_eq!(
        pin.version(),
        expect_current,
        "survivor pins the current version"
    );
    survivor
        .resolve_schema(pin.manifest().schema_id)
        .expect("survivor resolves schema");
    let batch = pin
        .as_arrow(survivor.registry())
        .expect("survivor reads zero-copy");
    verify_demo_batch(&batch).expect("survivor's zero-copy read is valid, not torn");
}

fn tmp() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let res = dir.path().join("kill.result");
    (dir, uds, res)
}

// --- the stream / write-lease family (artifact data-pool census) --------------

/// Drive one artifact-data-pool injection point and assert the zero-leak census.
///
/// `expect_chunk_reclaim` = the point stages chunks (so `reclaimed()` must gain
/// entries); `expect_lease_reclaim` = the point holds the write lease (so the
/// lease counter must increment and the owner return to free).
fn run_data_pool_point(point: &str, expect_chunk_reclaim: bool, expect_lease_reclaim: bool) {
    let (_dir, uds, res) = tmp();
    let uds_s = uds.to_str().unwrap().to_string();
    let (coord, survivor, one_version_free) = boot_with_v1(&uds);

    let leases_before = coord.write_leases_reclaimed();

    let actor = spawn_kill_at(point, &uds_s, res.to_str().unwrap(), CACHE_ARTIFACT);
    assert!(
        wait_until(Duration::from_secs(20), || line_tokens(&res, "READY")
            .is_some()),
        "{point}: actor never reached the transition (result: {:?})",
        std::fs::read_to_string(&res).ok()
    );
    // The actor self-aborts immediately after READY; make sure it is gone.
    drop(actor);

    // The lease lapses (~500 ms) and journal replay reclaims everything the dead
    // actor held: the pool returns EXACTLY to the one-version baseline.
    let reclaimed = wait_until(Duration::from_secs(10), || {
        coord.artifact_free_total(CACHE_ARTIFACT) == Some(one_version_free)
            && (!expect_lease_reclaim
                || coord.artifact_write_lease_owner(CACHE_ARTIFACT) == Some(0))
    });
    assert!(
        reclaimed,
        "{point}: pool did not return to baseline (baseline={one_version_free}, now={:?}, \
         lease_owner={:?})",
        coord.artifact_free_total(CACHE_ARTIFACT),
        coord.artifact_write_lease_owner(CACHE_ARTIFACT)
    );

    if expect_chunk_reclaim {
        assert!(
            !coord.reclaimed().is_empty(),
            "{point}: the coordinator must have recorded a staged-chunk reclaim"
        );
    }
    if expect_lease_reclaim {
        assert!(
            coord.write_leases_reclaimed() > leases_before,
            "{point}: the coordinator must have recorded a write-lease reclaim \
             (before={leases_before}, now={})",
            coord.write_leases_reclaimed()
        );
        assert_eq!(
            coord.artifact_write_lease_owner(CACHE_ARTIFACT),
            Some(0),
            "{point}: the leaked lease must be force-released (unwedged)"
        );
    }

    // No partial version was ever published, and the survivor still reads v1.
    assert_survivor_consistent(&coord, &survivor, 1);

    // And a fresh writer can still install v2 — the artifact is not wedged.
    assert_eq!(
        commit_replace(&survivor, 1),
        2,
        "{point}: artifact still writable after reclaim"
    );
}

#[test]
fn kill_at_stream_stage_one() {
    run_data_pool_point("stage-1", true, false);
}

#[test]
fn kill_at_stream_stage_two() {
    run_data_pool_point("stage-2", true, false);
}

#[test]
fn kill_at_write_lease_only() {
    // Lease held, nothing staged: no chunk reclaim, but the lease MUST be
    // force-released + fenced (else the artifact is permanently un-writable).
    run_data_pool_point("lease-only", false, true);
}

#[test]
fn kill_at_write_lease_and_stage() {
    run_data_pool_point("lease-stage", true, true);
}

// --- artifact version pin (item J) --------------------------------------------

#[test]
fn kill_at_artifact_pin_mid_hold() {
    // A reader journal-pins v1 and hangs; the driver supersedes it with v2; the
    // reader (seeing current > pinned) aborts holding the pin. Journal replay
    // decrements v1's pin count and retires it — its chunks return to baseline.
    let (_dir, uds, res) = tmp();
    let uds_s = uds.to_str().unwrap().to_string();
    let (coord, survivor, one_version_free) = boot_with_v1(&uds);

    let actor = spawn_kill_at("art-pin", &uds_s, res.to_str().unwrap(), CACHE_ARTIFACT);
    assert!(
        wait_until(Duration::from_secs(20), || line_tokens(&res, "READY")
            .is_some()),
        "art-pin: actor never pinned a version (result: {:?})",
        std::fs::read_to_string(&res).ok()
    );
    assert_eq!(
        line_tokens(&res, "READY").unwrap()[2],
        "v=1",
        "art-pin should pin v1"
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_slot_pins(CACHE_ARTIFACT, 1) == Some(1)
        }),
        "art-pin's journalled pin should register on v1's slot"
    );

    // Supersede v1 with v2; the hung pin holds v1's chunks (free drops below base).
    assert_eq!(commit_replace(&survivor, 1), 2, "survivor installs v2");
    assert!(
        coord.artifact_free_total(CACHE_ARTIFACT).unwrap() < one_version_free,
        "two live versions (pinned v1 + v2) must consume more chunks"
    );
    // The actor observes current > pinned and aborts.
    drop(actor);

    let reclaimed = wait_until(Duration::from_secs(10), || {
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1).is_none()
            && coord.artifact_free_total(CACHE_ARTIFACT) == Some(one_version_free)
    });
    assert!(
        reclaimed,
        "art-pin: v1 not reclaimed after abort (slot_pins={:?}, free={:?} vs baseline={one_version_free})",
        coord.artifact_slot_pins(CACHE_ARTIFACT, 1),
        coord.artifact_free_total(CACHE_ARTIFACT)
    );
    assert!(
        coord.artifact_pins_reclaimed() >= 1,
        "art-pin: the coordinator must record the leaked artifact-pin reclaim"
    );
    assert_survivor_consistent(&coord, &survivor, 2);
}

// --- payload shared pin (v0.1 broadcast path) ---------------------------------

#[test]
fn kill_at_payload_pin_mid_hold() {
    // A consumer receives a broadcast batch, journal-pins it zero-copy, then aborts
    // holding the pin. Lease replay frees the chunk (FREE + bumped generation).
    let (_dir, uds, res) = tmp();
    let uds_s = uds.to_str().unwrap().to_string();

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // The test is the producer: publish one batch on the demo topic and keep the
    // node alive so the published chunk stays put until the consumer pins it.
    let mut producer = Node::connect(&uds, "producer", demo_registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let desc = producer
        .publish_batch(DEMO_TOPIC, &demo_batch())
        .expect("publish");

    let actor = spawn_kill_at("payload-pin", &uds_s, res.to_str().unwrap(), CACHE_ARTIFACT);
    assert!(
        wait_until(Duration::from_secs(20), || line_tokens(&res, "READY")
            .is_some()),
        "payload-pin: consumer never pinned (result: {:?})",
        std::fs::read_to_string(&res).ok()
    );
    // The chunk is armed: published, owner released, a live consumer pin.
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.is_armed()
                && coord.chunk_snapshot(&desc).is_some_and(|s| {
                    s.state == shm_core::PUBLISHED && s.owner == 0 && s.refcount >= 1
                })
        }),
        "payload-pin: the chunk was never armed with a live pin"
    );
    let before = coord.chunk_snapshot(&desc).expect("snapshot before");
    drop(actor);

    let reclaimed = wait_until(Duration::from_secs(10), || {
        coord
            .chunk_snapshot(&desc)
            .is_some_and(|s| s.state == FREE && s.generation > before.generation)
    });
    let after = coord.chunk_snapshot(&desc).expect("snapshot after");
    assert!(
        reclaimed,
        "payload-pin: chunk not reclaimed after abort: {after:?} (before {before:?})"
    );
    assert_eq!(after.refcount, 0, "refcount cleared");
    assert!(
        coord.reclaimed().iter().any(|d| d.offset == desc.offset),
        "the coordinator must record the payload-pin reclaim"
    );
    // The stale (pre-reclaim) descriptor now fails validation.
    assert!(
        matches!(
            coord.validate_desc(&desc),
            Err(shm_runtime::Error::Core(shm_core::Error::StaleDescriptor))
        ),
        "payload-pin: the recycled chunk's old descriptor must be stale"
    );
}

// --- task fabric --------------------------------------------------------------

#[test]
fn kill_at_task_claim_requeues() {
    // A worker claims a task then aborts (never completes). The lease-driven reap
    // requeues it (at-least-once) and a second worker completes the SAME task.
    let (_dir, uds, res) = tmp();
    let uds_s = uds.to_str().unwrap().to_string();
    let work_r = _dir.path().join("worker.result");
    let (_coord, survivor, _base) = boot_with_v1(&uds);
    let _ = &survivor; // keep the artifact + a heartbeating actor alive for the worker

    // Submit a task (short lease so the reap fires promptly after the abort).
    let queue = survivor_queue(&uds);
    let lease = Duration::from_millis(300).as_nanos() as u64;
    let handle = queue
        .submit(
            ChunkDesc {
                schema_id: 1,
                ..ChunkDesc::ZERO
            },
            now_nanos() + lease,
        )
        .expect("submit");

    let actor = spawn_kill_at("task-claim", &uds_s, res.to_str().unwrap(), CACHE_ARTIFACT);
    assert!(
        wait_until(Duration::from_secs(20), || line_tokens(&res, "READY")
            .is_some()),
        "task-claim: worker never claimed (result: {:?})",
        std::fs::read_to_string(&res).ok()
    );
    let ready = line_tokens(&res, "READY").unwrap();
    let (claimed_slot, claimed_seq) = (ready[2].clone(), ready[3].clone());
    drop(actor);

    // A second worker claims the requeued task and completes it.
    let _worker = spawn_role("worker", &uds_s, work_r.to_str().unwrap());
    assert!(
        wait_until(Duration::from_secs(20), || line_tokens(&work_r, "DONE")
            .is_some()),
        "task-claim: second worker never completed the requeued task (result: {:?})",
        std::fs::read_to_string(&work_r).ok()
    );
    let done = line_tokens(&work_r, "DONE").unwrap();
    // At-least-once: the correlation id is STABLE across the requeue.
    assert_eq!(
        done[1], claimed_slot,
        "requeue changed the slot (correlation id)"
    );
    assert_eq!(
        done[2], claimed_seq,
        "requeue changed the seq (correlation id)"
    );
    assert_eq!(done[3], DEMO_ID_SUM.to_string(), "wrong derived sum");

    // The original requester observes the same terminal outcome.
    assert!(
        wait_until(Duration::from_secs(10), || matches!(
            queue.poll(handle),
            Ok(shm_task::TaskStatus::Done(_))
        )),
        "task-claim: the submitted handle never reached Done"
    );
}

#[test]
fn kill_at_task_submit_dead_submitter() {
    // A submitter enqueues a task then aborts before any worker claims. The task
    // must remain claimable (a dead submitter wedges nothing): the test claims and
    // completes it in-process and the handle reaches Done.
    let (_dir, uds, res) = tmp();
    let uds_s = uds.to_str().unwrap().to_string();
    let (_coord, survivor, _base) = boot_with_v1(&uds);
    let _ = &survivor;

    let actor = spawn_kill_at("task-submit", &uds_s, res.to_str().unwrap(), CACHE_ARTIFACT);
    assert!(
        wait_until(Duration::from_secs(20), || line_tokens(&res, "READY")
            .is_some()),
        "task-submit: submitter never submitted (result: {:?})",
        std::fs::read_to_string(&res).ok()
    );
    let ready = line_tokens(&res, "READY").unwrap();
    let (slot, seq): (u32, u64) = (ready[2].parse().unwrap(), ready[3].parse().unwrap());
    let handle = shm_task::TaskHandle {
        slot_idx: slot,
        seq,
    };
    drop(actor);

    // The dead submitter's task is still QUEUED (not wedged): the handle it minted
    // resolves and reports the task as claimable.
    let queue = survivor_queue(&uds);
    assert!(
        wait_until(Duration::from_secs(10), || matches!(
            queue.poll(handle),
            Ok(shm_task::TaskStatus::Queued)
        )),
        "task-submit: the dead submitter's task is not Queued (status={:?})",
        queue.poll(handle)
    );

    // A worker claims and completes it; the handle then reaches Done — proof the
    // submitter's death left a fully live, completable task.
    let task = queue
        .claim(Duration::from_secs(5).as_nanos() as u64)
        .expect("the queued task is claimable after the submitter died");
    assert_eq!(
        task.task_id(),
        handle,
        "claimed the very task the dead submitter left"
    );
    task.complete(ChunkDesc {
        offset: 42,
        ..ChunkDesc::ZERO
    })
    .expect("complete the orphaned task");
    match queue.poll(handle).expect("poll") {
        shm_task::TaskStatus::Done(r) => assert_eq!(r.offset, 42, "result carried through"),
        other => panic!("task-submit: expected Done, got {other:?}"),
    }
}

/// Open the built-in task queue through a fresh in-process node (a submitter /
/// requester that outlives the crashing actor).
fn survivor_queue(uds: &std::path::Path) -> shm_runtime::TaskQueueHandle {
    // Leak a heartbeating node so the returned queue handle's segment stays mapped
    // for the rest of the test (the node owns the mapping).
    let node = Box::leak(Box::new(
        Node::connect(uds, "requester", registry()).expect("requester connect"),
    ));
    node.start_heartbeat(Duration::from_millis(150));
    node.task_queue().expect("task_queue")
}
