//! v0.2 stage B — `shm-task` MPMC task-queue tests.
//!
//! Single-process: threads over one heap-mapped region exercise the CAS claim
//! state machine, the lease reap (at-least-once), cooperative cancel, the ABA
//! stale-handle guard, queue-full backpressure, and doorbell-blocked claiming.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use shm_core::{doorbell_pair, ChunkDesc};
use shm_ring::{DoorbellNotifier, DoorbellParker};
use shm_task::{now_nanos, required_bytes, Outcome, TaskQueue, TaskStatus};

/// An 8-byte-aligned heap region standing in for a shm segment payload.
struct Region {
    buf: Vec<u64>,
}

impl Region {
    fn for_capacity(capacity: u32) -> Region {
        let bytes = required_bytes(capacity) + 64;
        Region {
            buf: vec![0u64; bytes.div_ceil(8)],
        }
    }
    fn base(&self) -> *mut u8 {
        self.buf.as_ptr() as *mut u8
    }
    fn len(&self) -> usize {
        self.buf.len() * 8
    }
}

/// Far enough in the future that `reap` never fires during a test.
fn future() -> u64 {
    now_nanos() + Duration::from_secs(3600).as_nanos() as u64
}

/// A request descriptor tagging task index `i` in `schema_id`.
fn request(i: u32) -> ChunkDesc {
    ChunkDesc {
        schema_id: i,
        offset: i.wrapping_mul(3),
        ..ChunkDesc::ZERO
    }
}

/// The result a worker `w` produces for a request (encodes the owner + task).
fn result_for(req: ChunkDesc, w: u32) -> ChunkDesc {
    ChunkDesc {
        schema_id: req.schema_id,
        segment_id: w,
        offset: req.schema_id.wrapping_mul(2),
        ..ChunkDesc::ZERO
    }
}

#[test]
fn exactly_once_claim_all_reach_done() {
    let capacity = 256u32;
    let n = 200u32;
    let m = 8u32; // workers
    let region = Region::for_capacity(capacity);
    // SAFETY: `region` outlives every handle/thread below (joined before drop).
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");

    // Submit N tasks up front.
    let mut handles = Vec::new();
    for i in 0..n {
        handles.push(queue.submit(request(i), future()).expect("submit"));
    }

    // Each task must be claimed by exactly one worker.
    let claims: Arc<Vec<AtomicU32>> = Arc::new((0..n).map(|_| AtomicU32::new(0)).collect());
    let completed = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(m as usize));

    let mut workers = Vec::new();
    for w in 1..=m {
        let q = queue.clone();
        let claims = claims.clone();
        let completed = completed.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            while completed.load(Ordering::Acquire) < n as usize {
                match q.claim(w) {
                    Some(task) => {
                        let req = task.request();
                        claims[req.schema_id as usize].fetch_add(1, Ordering::AcqRel);
                        task.complete(result_for(req, w)).expect("complete");
                        completed.fetch_add(1, Ordering::AcqRel);
                    }
                    None => thread::yield_now(),
                }
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }

    // Every task claimed exactly once.
    for (i, c) in claims.iter().enumerate() {
        assert_eq!(
            c.load(Ordering::Acquire),
            1,
            "task {i} not claimed exactly once"
        );
    }
    assert_eq!(completed.load(Ordering::Acquire), n as usize);

    // Every requester reads its own result, produced by a valid worker.
    for (i, h) in handles.iter().enumerate() {
        match queue.poll(*h).expect("poll") {
            TaskStatus::Done(result) => {
                assert_eq!(result.schema_id, i as u32, "task {i} wrong result tag");
                assert_eq!(result.offset, (i as u32).wrapping_mul(2));
                assert!((1..=m).contains(&result.segment_id), "task {i} bad owner");
            }
            other => panic!("task {i} not DONE: {other:?}"),
        }
    }
}

#[test]
fn cancel_before_claim_is_cancelled() {
    let region = Region::for_capacity(4);
    // SAFETY: region outlives the queue.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 4) }.expect("init");

    let h = queue.submit(request(0), future()).expect("submit");
    queue.cancel(h).expect("cancel");
    assert_eq!(queue.poll(h).expect("poll"), TaskStatus::Cancelled);
    // No worker can claim a cancelled task.
    assert!(
        queue.claim(1).is_none(),
        "cancelled task must not be claimable"
    );
}

#[test]
fn cancel_after_claim_flags_worker() {
    let region = Region::for_capacity(4);
    // SAFETY: region outlives the queue.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 4) }.expect("init");

    let h = queue.submit(request(0), future()).expect("submit");
    let task = queue.claim(7).expect("claim");
    assert!(!task.is_cancelled(), "not cancelled yet");

    queue.cancel(h).expect("cancel");
    assert!(
        task.is_cancelled(),
        "worker must observe the cooperative cancel flag"
    );

    // A cooperative worker responds by failing.
    task.fail().expect("fail");
    assert_eq!(queue.poll(h).expect("poll"), TaskStatus::Failed);
}

#[test]
fn reap_requeues_and_second_worker_completes() {
    let region = Region::for_capacity(4);
    // SAFETY: region outlives the queue.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 4) }.expect("init");

    // Deadline already in the past: the first claim's lease is immediately lapse-able.
    let past = now_nanos().saturating_sub(1);
    let h = queue.submit(request(0), past).expect("submit");

    let a = queue.claim(1).expect("worker A claims");
    let report = queue.reap(now_nanos());
    assert_eq!(report.requeued, 1, "lapsed claim must be requeued");
    assert_eq!(report.failed, 0);

    // Worker A's stale claim is now lost (at-least-once): its complete is rejected.
    assert!(
        a.complete(result_for(request(0), 1)).is_err(),
        "reaped claim must be Lost"
    );

    // A second worker picks up the *same* task (stable correlation id) and finishes.
    let b = queue.claim(2).expect("worker B claims requeued task");
    assert_eq!(b.task_id(), h, "correlation id stable across retries");
    b.complete(result_for(request(0), 2)).expect("B completes");

    match queue.poll(h).expect("poll") {
        TaskStatus::Done(r) => assert_eq!(r.segment_id, 2, "B's result must be the one observed"),
        other => panic!("expected Done from B, got {other:?}"),
    }
}

#[test]
fn reap_exceeding_retry_cap_fails_terminally() {
    let region = Region::for_capacity(4);
    let cap_retries = 1u32;
    // SAFETY: region outlives the queue.
    let queue =
        unsafe { TaskQueue::init_with_max_retries(region.base(), region.len(), 4, cap_retries) }
            .expect("init");

    let past = now_nanos().saturating_sub(1);
    let h = queue.submit(request(0), past).expect("submit");

    // First lapse → requeue (retry 0 -> 1).
    let _a = queue.claim(1).expect("A claims");
    assert_eq!(queue.reap(now_nanos()).requeued, 1);

    // Second lapse → retry cap (1) exhausted → terminal FAILED.
    let _b = queue.claim(2).expect("B claims");
    let report = queue.reap(now_nanos());
    assert_eq!(report.requeued, 0);
    assert_eq!(report.failed, 1, "retries exhausted must fail terminally");

    assert_eq!(queue.poll(h).expect("poll"), TaskStatus::Failed);
}

#[test]
fn stale_handle_after_slot_reuse() {
    // Capacity 1 forces the next submit to reuse the single slot.
    let region = Region::for_capacity(1);
    // SAFETY: region outlives the queue.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 1) }.expect("init");

    let h1 = queue.submit(request(0), future()).expect("submit 1");
    let t = queue.claim(1).expect("claim");
    t.complete(result_for(request(0), 1)).expect("complete");
    assert!(matches!(queue.poll(h1).unwrap(), TaskStatus::Done(_)));

    // Reuse the (terminal) slot for a new task: h1's seq is now stale.
    let h2 = queue
        .submit(request(1), future())
        .expect("submit 2 reuses slot");
    assert_eq!(h1.slot_idx, h2.slot_idx, "same slot reused");

    assert!(matches!(queue.poll(h1), Err(shm_task::Error::StaleHandle)));
    assert!(matches!(
        queue.cancel(h1),
        Err(shm_task::Error::StaleHandle)
    ));
    // The fresh handle still resolves.
    assert_eq!(queue.poll(h2).expect("poll h2"), TaskStatus::Queued);
}

#[test]
fn queue_full_backpressure() {
    let capacity = 4u32;
    let region = Region::for_capacity(capacity);
    // SAFETY: region outlives the queue.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");

    let mut handles = Vec::new();
    for i in 0..capacity {
        handles.push(
            queue
                .submit(request(i), future())
                .expect("submit fills queue"),
        );
    }
    // All slots live → full.
    assert!(matches!(
        queue.submit(request(99), future()),
        Err(shm_task::Error::QueueFull)
    ));

    // A CLAIMED task is still not reusable.
    let t = queue.claim(1).expect("claim");
    assert!(matches!(
        queue.submit(request(99), future()),
        Err(shm_task::Error::QueueFull)
    ));

    // Completing frees exactly one slot for reuse.
    t.complete(result_for(request(0), 1)).expect("complete");
    assert!(
        queue.submit(request(99), future()).is_ok(),
        "freed slot must accept a submit"
    );
}

#[test]
fn requester_wait_blocks_until_done() {
    let region = Region::for_capacity(8);
    let db = doorbell_pair().expect("pair");
    let write_fd = db.write.as_raw_fd();
    // SAFETY: region outlives the queue and both threads (joined below).
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 8) }
        .expect("init")
        .with_done_notifier(DoorbellNotifier::new(write_fd));

    let h = queue.submit(request(3), future()).expect("submit");

    // Requester blocks on the done doorbell until a worker finishes the task.
    let req_q = queue.clone();
    let parker = DoorbellParker::new(db.read.try_clone().expect("dup read"));
    let requester = thread::spawn(move || req_q.wait(h, &parker).expect("wait"));

    // Worker runs a bit later, then completes (rings the done doorbell).
    thread::sleep(Duration::from_millis(60));
    let task = queue.claim(1).expect("claim");
    task.complete(result_for(request(3), 1)).expect("complete");

    match requester.join().unwrap() {
        Outcome::Done(r) => assert_eq!(r.schema_id, 3),
        other => panic!("expected Done, got {other:?}"),
    }
    drop(db);
}

#[test]
fn doorbell_blocked_worker_wakes_on_submit() {
    let capacity = 16u32;
    let region = Region::for_capacity(capacity);
    let db = doorbell_pair().expect("pair");
    let write_fd = db.write.as_raw_fd();
    // SAFETY: region outlives the queue and the worker thread (joined below).
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }
        .expect("init")
        .with_work_notifier(DoorbellNotifier::new(write_fd));

    let worker_q = queue.clone();
    let parker = DoorbellParker::new(db.read.try_clone().expect("dup read"));
    let worker = thread::spawn(move || {
        let start = Instant::now();
        let task = worker_q.claim_blocking(1, &parker);
        let req = task.request();
        task.complete(result_for(req, 1)).expect("complete");
        (req.schema_id, start.elapsed())
    });

    // Let the worker park on the empty queue, then submit (rings the doorbell).
    thread::sleep(Duration::from_millis(80));
    let h = queue.submit(request(5), future()).expect("submit");

    let (got, elapsed) = worker.join().unwrap();
    assert_eq!(got, 5, "worker processed the submitted task");
    assert!(
        elapsed < Duration::from_secs(2),
        "worker woke slowly: {elapsed:?}"
    );
    assert!(matches!(queue.poll(h).unwrap(), TaskStatus::Done(_)));

    drop(db);
}

#[test]
fn cancelled_slot_recycles_through_claim_to_submit() {
    // ADR-0009: a slot cancelled while QUEUED rides its READY node until a
    // claim pop transfers it to the FREE stack; a later submit must get it back.
    let capacity = 8u32;
    let region = Region::for_capacity(capacity);
    // SAFETY: region outlives the queue.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");

    // Fill the queue, cancel everything (a cancel storm), then drain: one claim
    // sees only CANCELLED nodes, transfers them all to FREE, and returns None.
    let mut handles = Vec::new();
    for i in 0..capacity {
        handles.push(queue.submit(request(i), future()).expect("submit"));
    }
    for h in &handles {
        queue.cancel(*h).expect("cancel");
    }
    assert!(queue.claim(1).is_none(), "all tasks are cancelled");

    // Stack hygiene: every slot came back — the queue refills to capacity and
    // fully drains, with no slot lost and no double membership.
    let mut round2 = Vec::new();
    for i in 0..capacity {
        round2.push(
            queue
                .submit(request(100 + i), future())
                .expect("cancelled slots must be reusable"),
        );
    }
    assert!(matches!(
        queue.submit(request(999), future()),
        Err(shm_task::Error::QueueFull)
    ));
    for _ in 0..capacity {
        let t = queue.claim(1).expect("claim refill");
        let req = t.request();
        t.complete(result_for(req, 1)).expect("complete");
    }
    assert!(queue.claim(1).is_none(), "drained");
    for h in &round2 {
        assert!(matches!(queue.poll(*h).expect("poll"), TaskStatus::Done(_)));
    }
    // The old handles are stale (slots reused), not resurrected.
    for h in &handles {
        assert!(matches!(queue.poll(*h), Err(shm_task::Error::StaleHandle)));
    }
}

#[test]
fn queue_full_with_only_cancelled_slots_recovers() {
    // ADR-0009 queue-full edge fallback: with the FREE stack empty and every
    // slot CANCELLED (still riding READY nodes), submit must still succeed —
    // the pre-ADR behavior, where a CANCELLED slot was directly reusable.
    let capacity = 4u32;
    let region = Region::for_capacity(capacity);
    // SAFETY: region outlives the queue.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");

    let mut handles = Vec::new();
    for i in 0..capacity {
        handles.push(queue.submit(request(i), future()).expect("submit"));
    }
    for h in &handles {
        queue.cancel(*h).expect("cancel");
    }
    // FREE is empty; all four slots are CANCELLED on READY. Each submit
    // recovers one via the bounded fallback pop.
    for i in 0..capacity {
        queue
            .submit(request(200 + i), future())
            .expect("cancel-heavy queue must not report full");
    }
    // Now the queue is genuinely full of live QUEUED tasks.
    assert!(matches!(
        queue.submit(request(999), future()),
        Err(shm_task::Error::QueueFull)
    ));
}

#[test]
fn submit_claim_complete_at_large_capacity() {
    // Correctness (not perf) at a deep queue: the index stacks must route every
    // one of `n` tasks exactly once through 4 workers at capacity 4096.
    let capacity = 4096u32;
    let n = 1000u32;
    let m = 4u32;
    let region = Region::for_capacity(capacity);
    // SAFETY: `region` outlives every handle/thread below (joined before drop).
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");

    let mut handles = Vec::new();
    for i in 0..n {
        handles.push(queue.submit(request(i), future()).expect("submit"));
    }
    let claims: Arc<Vec<AtomicU32>> = Arc::new((0..n).map(|_| AtomicU32::new(0)).collect());
    let completed = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for w in 1..=m {
        let q = queue.clone();
        let claims = claims.clone();
        let completed = completed.clone();
        workers.push(thread::spawn(move || {
            while completed.load(Ordering::Acquire) < n as usize {
                match q.claim(w) {
                    Some(task) => {
                        let req = task.request();
                        claims[req.schema_id as usize].fetch_add(1, Ordering::AcqRel);
                        task.complete(result_for(req, w)).expect("complete");
                        completed.fetch_add(1, Ordering::AcqRel);
                    }
                    None => thread::yield_now(),
                }
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }
    for (i, c) in claims.iter().enumerate() {
        assert_eq!(c.load(Ordering::Acquire), 1, "task {i} not claimed once");
    }
    for h in &handles {
        assert!(matches!(queue.poll(*h).expect("poll"), TaskStatus::Done(_)));
    }
}

/// **The P0.2 property test** (fails on the pre-ADR-0009 O(capacity) scan):
/// probing an empty queue for work must cost the same at capacity 2^16 as at
/// capacity 2^9. The old `claim_inner` scanned all `capacity` slots before
/// returning `None`, so the large queue cost ~128x the small one; the READY
/// stack pop is O(1), so the ratio collapses to ~1. The 16x threshold leaves
/// an order of magnitude of headroom for machine noise on either side.
#[test]
fn empty_claim_probe_cost_is_flat_in_capacity() {
    fn best_probe_batch(capacity: u32) -> Duration {
        let region = Region::for_capacity(capacity);
        // SAFETY: region outlives the queue (dropped at end of scope).
        let queue =
            unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");
        for _ in 0..100 {
            assert!(queue.claim(1).is_none(), "queue is empty");
        }
        let mut best = Duration::MAX;
        for _ in 0..3 {
            let t0 = Instant::now();
            for _ in 0..2000 {
                std::hint::black_box(queue.claim(1));
            }
            best = best.min(t0.elapsed());
        }
        best
    }
    let small = best_probe_batch(1 << 9);
    let large = best_probe_batch(1 << 16);
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-9);
    assert!(
        ratio < 16.0,
        "empty-claim probe scales with capacity: cap 2^9 -> {small:?}, cap 2^16 -> {large:?} \
         (ratio {ratio:.1}x; an O(1) claim is ~1x, the O(capacity) scan ~128x)"
    );
}

// Bring `AsRawFd` into scope for `db.write.as_raw_fd()`.
use std::os::fd::AsRawFd;

// ---- P0.3 (ADR-0010): the lease side table — task-lifecycle-tied bindings ----

/// The whole binding lifecycle: input armed at submit, output armed by the
/// worker under `CLAIMED`, both released **exactly once** at the requester's
/// ack (idempotent thereafter), with `NotTerminal` protecting a live task.
#[test]
fn task_binding_lifecycle_arms_at_submit_and_releases_exactly_once_at_ack() {
    use shm_task::{Error, LeaseBinding};
    let region = Region::for_capacity(8);
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 8) }.expect("init");

    let input = LeaseBinding {
        artifact_id: 77,
        incarnation: 3,
        version: 5,
    };
    let output = LeaseBinding {
        artifact_id: 88,
        incarnation: 4,
        version: 9,
    };

    let h = queue
        .submit_with_binding(request(1), future(), input)
        .expect("submit with input binding");

    // A live task's bindings cannot be acked out from under it.
    assert!(matches!(queue.ack(h), Err(Error::NotTerminal)), "queued = live");
    let task = queue.claim(11).expect("claim");
    assert!(matches!(queue.ack(h), Err(Error::NotTerminal)), "claimed = live");

    // The worker ties its retained output to the task, then completes.
    task.bind_output(output).expect("bind output");
    task.complete(result_for(request(1), 11)).expect("complete");

    // Terminal: the ack hands both bindings to the coordinator, exactly once,
    // and the next reap (zero grace, no liveness check) releases them.
    assert_eq!(queue.ack(h).expect("ack"), 2);
    assert_eq!(queue.ack(h).expect("ack twice"), 0, "idempotent");
    let mut got = queue.reap_bindings(0, 0);
    got.sort_by_key(|b| b.artifact_id);
    assert_eq!(got, vec![input, output]);

    // The records went back to the free list: a fresh task can arm again.
    let h2 = queue
        .submit_with_binding(request(2), future(), input)
        .expect("records recycled");
    let t2 = queue.claim(11).expect("claim 2");
    t2.fail().expect("fail");
    // A FAILED outcome is ackable too (the input is no longer needed).
    assert_eq!(queue.ack(h2).expect("ack failed task"), 1);
    assert_eq!(queue.reap_bindings(0, 0), vec![input]);
}

/// The reap backstop: a live (`QUEUED`/`CLAIMED`) task's bindings are never
/// touched, however late; a terminal task's bindings are released only past
/// `deadline + grace`; a racing ack and reap release each binding exactly once.
#[test]
fn task_binding_reap_backstop_respects_liveness_and_grace() {
    use shm_task::LeaseBinding;
    let region = Region::for_capacity(4);
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 4) }.expect("init");

    let b = LeaseBinding {
        artifact_id: 501,
        incarnation: 1,
        version: 2,
    };
    let deadline = 1_000_000u64;
    let grace = 1_000u64;
    let h = queue
        .submit_with_binding(request(1), deadline, b)
        .expect("submit");

    // QUEUED: protected even arbitrarily far past deadline + grace.
    assert_eq!(queue.reap_bindings(u64::MAX, grace), vec![]);
    let task = queue.claim(9).expect("claim");
    // CLAIMED: still protected.
    assert_eq!(queue.reap_bindings(u64::MAX, grace), vec![]);
    task.complete(ChunkDesc::ZERO).expect("complete");

    // Terminal, but inside the requester's ack window: untouched.
    assert_eq!(queue.reap_bindings(deadline + grace, grace), vec![]);
    // Past the window: the backstop wins the binding.
    assert_eq!(queue.reap_bindings(deadline + grace + 1, grace), vec![b]);
    // The ack that never came finds nothing left (exactly-once).
    assert_eq!(queue.ack(h).expect("late ack"), 0);
}

/// Slot reuse cannot cross-release: bindings are tied to `{slot_idx, seq}`, so
/// an old task's unacked binding survives the slot being recycled for a new
/// task, the old handle's ack releases only the old binding, and the new
/// task's binding stays armed. Also: the lease table backpressures with
/// `LeaseTableFull` when every record is armed.
#[test]
fn task_binding_survives_slot_reuse_and_table_full_backpressures() {
    use shm_task::{Error, LeaseBinding};
    let region = Region::for_capacity(1); // one slot: reuse is immediate
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), 1) }.expect("init");

    let old = LeaseBinding {
        artifact_id: 601,
        incarnation: 1,
        version: 1,
    };
    let new = LeaseBinding {
        artifact_id: 602,
        incarnation: 1,
        version: 1,
    };

    let h_old = queue
        .submit_with_binding(request(1), future(), old)
        .expect("submit old");
    queue
        .claim(5)
        .expect("claim old")
        .complete(ChunkDesc::ZERO)
        .expect("complete old");

    // The slot recycles for a NEW task before the old requester acks.
    let h_new = queue
        .submit_with_binding(request(2), future(), new)
        .expect("submit new into the same slot");
    assert_eq!(h_new.slot_idx, h_old.slot_idx, "same slot reused");
    assert_ne!(h_new.seq, h_old.seq, "fresh incarnation");

    // With capacity 1 the lease table holds 2 records, both now armed:
    // arming a third binding backpressures.
    let t_new = queue.claim(5).expect("claim new");
    assert!(matches!(
        t_new.bind_output(new),
        Err(Error::LeaseTableFull)
    ));

    // The old handle's ack releases ONLY the old binding (seq-matched), even
    // though its slot now hosts a live task (StaleHandle from poll's view).
    assert!(matches!(queue.poll(h_old), Err(Error::StaleHandle)));
    assert_eq!(queue.ack(h_old).expect("ack old"), 1);
    assert_eq!(queue.reap_bindings(0, 0), vec![old]);

    // The new task's binding is untouched and still releasable at its own ack.
    t_new.complete(ChunkDesc::ZERO).expect("complete new");
    assert_eq!(queue.ack(h_new).expect("ack new"), 1);
    assert_eq!(queue.reap_bindings(0, 0), vec![new]);
}
