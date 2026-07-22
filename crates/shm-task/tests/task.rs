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
        Region { buf: vec![0u64; bytes.div_ceil(8)] }
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
    ChunkDesc { schema_id: i, offset: i.wrapping_mul(3), ..ChunkDesc::ZERO }
}

/// The result a worker `w` produces for a request (encodes the owner + task).
fn result_for(req: ChunkDesc, w: u32) -> ChunkDesc {
    ChunkDesc { schema_id: req.schema_id, segment_id: w, offset: req.schema_id.wrapping_mul(2), ..ChunkDesc::ZERO }
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
        assert_eq!(c.load(Ordering::Acquire), 1, "task {i} not claimed exactly once");
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
    assert!(queue.claim(1).is_none(), "cancelled task must not be claimable");
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
    assert!(task.is_cancelled(), "worker must observe the cooperative cancel flag");

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
    assert!(a.complete(result_for(request(0), 1)).is_err(), "reaped claim must be Lost");

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
    let queue = unsafe {
        TaskQueue::init_with_max_retries(region.base(), region.len(), 4, cap_retries)
    }
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
    let h2 = queue.submit(request(1), future()).expect("submit 2 reuses slot");
    assert_eq!(h1.slot_idx, h2.slot_idx, "same slot reused");

    assert!(matches!(queue.poll(h1), Err(shm_task::Error::StaleHandle)));
    assert!(matches!(queue.cancel(h1), Err(shm_task::Error::StaleHandle)));
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
        handles.push(queue.submit(request(i), future()).expect("submit fills queue"));
    }
    // All slots live → full.
    assert!(matches!(queue.submit(request(99), future()), Err(shm_task::Error::QueueFull)));

    // A CLAIMED task is still not reusable.
    let t = queue.claim(1).expect("claim");
    assert!(matches!(queue.submit(request(99), future()), Err(shm_task::Error::QueueFull)));

    // Completing frees exactly one slot for reuse.
    t.complete(result_for(request(0), 1)).expect("complete");
    assert!(queue.submit(request(99), future()).is_ok(), "freed slot must accept a submit");
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
    assert!(elapsed < Duration::from_secs(2), "worker woke slowly: {elapsed:?}");
    assert!(matches!(queue.poll(h).unwrap(), TaskStatus::Done(_)));

    drop(db);
}

// Bring `AsRawFd` into scope for `db.write.as_raw_fd()`.
use std::os::fd::AsRawFd;
