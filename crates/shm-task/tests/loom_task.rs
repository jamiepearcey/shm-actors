//! loom model of the **task-claim CAS state machine** (ADR-0004 stage L, core 3).
//!
//! Runs the *production* [`TaskSlot`] transition methods — the exact
//! `compare_exchange` bodies `TaskQueue::claim`, `ClaimedTask::complete`, and
//! `TaskQueue::reap` drive — over a `TaskSlot` built in ordinary memory and shared
//! across two `loom::thread`s. All transitions are single-location CAS out of one
//! state, so no cross-location fence is needed (unlike the pin handshake).
//!
//! # Scenario 1 — exactly-once claim
//!
//! A slot in `QUEUED`; two workers each call [`try_claim`](TaskSlot::try_claim)
//! (`QUEUED→CLAIMED`). **Invariant:** *exactly one* wins — claiming is
//! exactly-once, so a task is never executed by two workers at once.
//!
//! # Scenario 2 — no double terminal transition (completer vs. reaper)
//!
//! A slot in `CLAIMED`; a completer calls
//! [`try_begin_complete`](TaskSlot::try_begin_complete) (`CLAIMED→COMPLETING`)
//! while the reaper calls
//! [`try_begin_reap_requeue`](TaskSlot::try_begin_reap_requeue)
//! (`CLAIMED→RESERVED`). **Invariant:** *exactly one* wins — a lapsed-lease reap
//! and an in-flight completion never both transition the same claim (the CAS out
//! of `CLAIMED` is the sole arbitration point).
//!
//! Only compiled/run under `--cfg loom`; a no-op otherwise.
#![cfg(loom)]

use loom::sync::Arc;

use shm_core::{ChunkDesc, ShmU32, ShmU64};
use shm_task::queue::{TaskSlot, CLAIMED, QUEUED};

/// A `TaskSlot` in `state`, incarnation `seq = 1`, everything else zeroed.
fn slot_in(state: u32) -> TaskSlot {
    TaskSlot {
        state: ShmU32::new(state),
        owner: ShmU32::new(0),
        seq: ShmU64::new(1),
        deadline: ShmU64::new(0),
        retry: ShmU32::new(0),
        cancel: ShmU32::new(0),
        request: ChunkDesc::ZERO,
        result: ChunkDesc::ZERO,
    }
}

static ITERS_A: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ITERS_B: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[test]
fn loom_task_claim_exactly_once() {
    loom::model(|| {
        ITERS_A.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let slot = Arc::new(slot_in(QUEUED));

        let a = slot.clone();
        let w1 = loom::thread::spawn(move || a.try_claim());
        let b = slot.clone();
        let w2 = loom::thread::spawn(move || b.try_claim());

        let won1 = w1.join().unwrap();
        let won2 = w2.join().unwrap();

        // Exactly one worker claims the task.
        assert!(won1 ^ won2, "task claimed by {} workers, expected exactly 1", (won1 as u8) + (won2 as u8));
        // And the slot is now CLAIMED.
        assert_eq!(slot.state.load(std::sync::atomic::Ordering::SeqCst), CLAIMED);
    });
    eprintln!(
        "loom_task_claim_exactly_once: explored {} interleavings",
        ITERS_A.load(std::sync::atomic::Ordering::Relaxed)
    );
}

#[test]
fn loom_task_complete_vs_reap() {
    loom::model(|| {
        ITERS_B.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let slot = Arc::new(slot_in(CLAIMED));

        // Completer: the winning worker publishing its result.
        let c = slot.clone();
        let completer = loom::thread::spawn(move || c.try_begin_complete());
        // Reaper: the coordinator requeueing a lapsed lease.
        let r = slot.clone();
        let reaper = loom::thread::spawn(move || r.try_begin_reap_requeue());

        let completed = completer.join().unwrap();
        let requeued = reaper.join().unwrap();

        // Exactly one transition out of CLAIMED: a task is never both completed
        // and requeued (no double terminal/transition).
        assert!(
            completed ^ requeued,
            "CLAIMED transitioned {} ways, expected exactly 1 (completer vs reaper race)",
            (completed as u8) + (requeued as u8)
        );
    });
    eprintln!(
        "loom_task_complete_vs_reap: explored {} interleavings",
        ITERS_B.load(std::sync::atomic::Ordering::Relaxed)
    );
}
