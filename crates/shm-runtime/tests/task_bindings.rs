//! P0.3 (ADR-0010) end-to-end: **task-lifecycle-tied retained-ref bindings**
//! over a real coordinator.
//!
//! The submitter retains a keyed-store input and ties it to the task
//! (`submit_with_binding`); the worker reads it zero-copy, retains its output,
//! ties it to the task (`bind_output`), and completes. The submitter then
//! "dies" (its journaled reader pin is leaked and `force_reclaim`ed): journal
//! replay releases the **actor's** pin, while the **task's** bindings survive —
//! the input a running task needs must outlive its submitter — and are finally
//! released by the coordinator's reap backstop (the requester never acked),
//! after which eviction returns every slot and chunk to baseline.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_runtime::demo::{demo_batch, demo_schema, verify_demo_batch};
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_store::RefKind;
use shm_task::now_nanos;

fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    2_100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
}

const IN_KEY: &[u8] = b"task/in";
const OUT_KEY: &[u8] = b"task/out";

#[test]
fn task_bindings_survive_submitter_death_and_release_by_reap_not_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");
    let baseline = coord.store_data_free_total().expect("baseline");

    // --- Submitter: retain the input, tie it to a task, submit. ---
    let mut submitter = Node::connect(&uds, "submitter", Arc::new(SchemaRegistry::new()))
        .expect("submitter connect");
    submitter.start_heartbeat(Duration::from_millis(150));
    submitter.intern_schema(&demo_schema()).expect("intern");

    let in_entry = {
        let store = submitter.store().expect("store");
        store
            .create(IN_KEY, RefKind::Dataset, &demo_schema())
            .expect("create input")
    };
    in_entry.commit_replace(&demo_batch()).expect("commit v1");

    // The submitter's own read pin: journaled, dies with the ACTOR.
    let own_pin = in_entry.pin().expect("journaled reader pin");
    // The task's input binding: unjournaled retained pin, dies with the TASK.
    let input_binding = in_entry.retain_current().expect("retain input");
    // Handoff BEFORE arming (ADR-0014 §4): the journal record that covered the
    // retain is released, and the lease table becomes the pin's sole owner.
    let input_binding = in_entry.handoff(&input_binding).expect("input handoff");
    assert_eq!(input_binding.version, 1);
    assert_eq!(
        coord.store_entry_pins(IN_KEY, 1),
        Some(2),
        "one journaled actor pin + one retained task pin"
    );

    let submitter_q = submitter.task_queue().expect("submitter queue");
    let handle = submitter_q
        .queue()
        .submit_with_binding(
            shm_core::ChunkDesc::ZERO,
            now_nanos(), // submit deadline already elapsed: the reap-backstop
            // window is then controlled purely by the grace we pass below
            shm_task::LeaseBinding {
                artifact_id: input_binding.artifact_id,
                incarnation: input_binding.incarnation,
                version: input_binding.version,
            },
        )
        .expect("submit with input binding");

    // --- Worker: claim, read the input zero-copy, retain + bind the output. ---
    let mut worker =
        Node::connect(&uds, "worker", Arc::new(SchemaRegistry::new())).expect("worker connect");
    worker.start_heartbeat(Duration::from_millis(150));
    let worker_q = worker.task_queue().expect("worker queue");
    let task = worker_q
        .claim(Duration::from_secs(3600).as_nanos() as u64)
        .expect("claim");

    // Cross-process schema agreement first (needs `&mut worker`), then the
    // borrowed store handle for the read + output retain.
    let schema_id = {
        let store = worker.store().expect("worker store");
        let win = store.open(IN_KEY).expect("open input");
        let pin = win.pin().expect("pin input");
        pin.manifest().schema_id
    };
    worker.resolve_schema(schema_id).expect("resolve schema");
    let out_binding = {
        let store = worker.store().expect("worker store");
        let win = store.open(IN_KEY).expect("open input");
        let (_pin, batch) = win.read().expect("zero-copy read");
        verify_demo_batch(&batch).expect("input verifies");

        let out = store
            .create(OUT_KEY, RefKind::Dataset, &demo_schema())
            .expect("create output");
        out.commit_replace(&batch).expect("commit output");
        let r = out.retain_current().expect("retain output");
        out.handoff(&r).expect("output handoff")
    };
    task.bind_output(shm_task::LeaseBinding {
        artifact_id: out_binding.artifact_id,
        incarnation: out_binding.incarnation,
        version: out_binding.version,
    })
    .expect("bind output");
    task.complete(shm_core::ChunkDesc::ZERO).expect("complete");
    assert_eq!(
        coord.store_entry_pins(OUT_KEY, 1),
        Some(1),
        "output retained"
    );

    // --- The submitter "dies" holding its journaled pin (kill -9 analogue). ---
    std::mem::forget(own_pin);
    coord
        .force_reclaim(submitter.actor_id())
        .expect("force reclaim submitter");
    assert_eq!(
        coord.artifact_pins_reclaimed(),
        1,
        "journal replay released the ACTOR's pin"
    );
    assert_eq!(
        coord.store_entry_pins(IN_KEY, 1),
        Some(1),
        "the TASK's input binding survived the submitter's death"
    );

    // --- Nobody acks (the requester is dead): the reap backstop releases. ---
    // Inside the grace window nothing moves; past it, both bindings release.
    assert_eq!(
        coord.reap_task_bindings_with_grace(now_nanos(), u64::MAX),
        0,
        "inside the ack window the backstop must not fire"
    );
    // The output binding's window anchors to the worker's claim-lease
    // deadline (1 h here), the input's to the submit deadline — drive `now`
    // past both.
    let past_both = now_nanos() + Duration::from_secs(2 * 3600).as_nanos() as u64;
    assert_eq!(
        coord.reap_task_bindings_with_grace(past_both, 0),
        2,
        "past the window the backstop releases input + output bindings"
    );
    assert_eq!(coord.task_bindings_reclaimed(), 2);
    assert_eq!(coord.store_entry_pins(IN_KEY, 1), Some(0));
    assert_eq!(coord.store_entry_pins(OUT_KEY, 1), Some(0));
    // A late ack (e.g. a restarted requester) finds nothing left.
    assert_eq!(worker_q.queue().ack(handle).expect("late ack"), 0);

    // --- Full teardown: everything returns to baseline. ---
    {
        let store = worker.store().expect("store for evict");
        store.evict(IN_KEY).expect("evict input");
        store.evict(OUT_KEY).expect("evict output");
    }
    assert_eq!(
        coord.store_data_free_total().unwrap(),
        baseline,
        "zero leak once the bindings and entries are gone"
    );
}
