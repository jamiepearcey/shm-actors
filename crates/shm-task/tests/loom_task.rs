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
//! # Scenarios 3–5 — the O(1) claim index (ADR-0009, P0.2)
//!
//! Three models of the two lock-free interactions the FREE/READY Treiber index
//! added, each driving the *production* composition ([`claim_pop`],
//! [`publish_queued`], [`TaskSlot::try_cancel_queued`]) over slots and stack
//! heads built in ordinary memory (the Treiber loops themselves are
//! [`shm_core::pool::treiber_pop`]/[`treiber_push`], already loom-covered):
//!
//! - **3 — dual popper**: two workers race [`claim_pop`] over one `QUEUED`
//!   node. Exactly one claims; the node is consumed exactly once.
//! - **4 — claim vs cancel**: [`claim_pop`] races a pre-claim cancel. The slot
//!   ends `CLAIMED`, xor ends `CANCELLED` *and* its node has been transferred
//!   to the FREE stack exactly once — never lost, never double-pushed.
//! - **5 — publish vs pop**: a submitter's publish (`QUEUED` store, *then*
//!   READY push) races a claimer. The claimer never claims (or discards) a
//!   `RESERVED` slot, and the task is never lost — it is claimed now or
//!   remains claimable after.
//!
//! Only compiled/run under `--cfg loom`; a no-op otherwise.
#![cfg(loom)]

use loom::sync::Arc;

use shm_core::{ChunkDesc, ShmU32, ShmU64};
use shm_task::queue::{
    claim_pop, pack_stack_head, publish_queued, TaskSlot, CANCELLED, CLAIMED, QUEUED, STACK_NIL,
};

/// A `TaskSlot` in `state`, incarnation `seq = 1`, everything else zeroed and
/// its intrusive stack link at [`STACK_NIL`].
fn slot_in(state: u32) -> TaskSlot {
    TaskSlot {
        state: ShmU32::new(state),
        owner: ShmU32::new(0),
        seq: ShmU64::new(1),
        deadline: ShmU64::new(0),
        retry: ShmU32::new(0),
        cancel: ShmU32::new(0),
        next: ShmU32::new(STACK_NIL),
        reserved: 0,
        request: ChunkDesc::ZERO,
        result: ChunkDesc::ZERO,
        _pad: [0; 40],
    }
}

/// The queue's discovery state rebuilt in ordinary memory, loom_reclaim.rs
/// style: the two Treiber heads plus a slot array whose intrusive `next` links
/// are the production ones ([`TaskSlot::next`]). `slots[0]` starts in `state`;
/// READY holds slot 0 iff `on_ready` (matching a published submit), FREE starts
/// empty.
struct MiniQueue {
    ready: ShmU64,
    free: ShmU64,
    cancelled: ShmU32,
    slots: Vec<TaskSlot>,
}

impl MiniQueue {
    fn one_slot(state: u32, on_ready: bool) -> MiniQueue {
        let head = if on_ready { 0 } else { STACK_NIL };
        MiniQueue {
            ready: ShmU64::new(pack_stack_head(head, 0)),
            free: ShmU64::new(pack_stack_head(STACK_NIL, 0)),
            cancelled: ShmU32::new(0),
            slots: vec![slot_in(state)],
        }
    }

    /// Production claim discovery over this queue's heads and slots.
    fn claim(&self) -> Option<u32> {
        claim_pop(&self.ready, &self.free, &self.cancelled, |i| {
            &self.slots[i as usize]
        })
    }

    /// The production pre-claim cancel: the winning `QUEUED→CANCELLED` CAS
    /// bumps the cancelled-riding count (`TaskQueue::cancel`'s composition).
    fn cancel(&self) -> bool {
        if self.slots[0].try_cancel_queued() {
            self.cancelled
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            true
        } else {
            false
        }
    }

    /// Pop the FREE stack (the submit side's reuse pop).
    fn pop_free(&self) -> Option<u32> {
        shm_core::pool::treiber_pop(&self.free, |i| {
            self.slots[i as usize]
                .next
                .load(std::sync::atomic::Ordering::Acquire)
        })
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
        assert!(
            won1 ^ won2,
            "task claimed by {} workers, expected exactly 1",
            (won1 as u8) + (won2 as u8)
        );
        // And the slot is now CLAIMED.
        assert_eq!(
            slot.state.load(std::sync::atomic::Ordering::SeqCst),
            CLAIMED
        );
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

static ITERS_C: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ITERS_D: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ITERS_E: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Scenario 3 — two workers race `claim_pop` over one queued task: node
/// exclusivity (the READY pop hands the node to exactly one popper) composed
/// with the exactly-once claim CAS. Exactly one worker claims; nothing is
/// transferred to FREE (no cancel happened); the node is fully consumed.
#[test]
fn loom_claim_index_dual_popper() {
    loom::model(|| {
        ITERS_C.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let q = Arc::new(MiniQueue::one_slot(QUEUED, true));

        let a = q.clone();
        let w1 = loom::thread::spawn(move || a.claim());
        let b = q.clone();
        let w2 = loom::thread::spawn(move || b.claim());

        let r1 = w1.join().unwrap();
        let r2 = w2.join().unwrap();

        // Exactly one worker got the task.
        assert!(
            r1.is_some() ^ r2.is_some(),
            "one queued task claimed by {} workers",
            (r1.is_some() as u8) + (r2.is_some() as u8)
        );
        assert_eq!(
            q.slots[0].state.load(std::sync::atomic::Ordering::SeqCst),
            CLAIMED
        );
        // The node was consumed: both stacks are empty.
        assert!(q.claim().is_none(), "READY must be empty");
        assert!(q.pop_free().is_none(), "FREE must be empty (no cancel ran)");
    });
    eprintln!(
        "loom_claim_index_dual_popper: explored {} interleavings",
        ITERS_C.load(std::sync::atomic::Ordering::Relaxed)
    );
}

/// Scenario 4 — `claim_pop` vs a concurrent pre-claim cancel: the slot ends
/// `CLAIMED` (cancel lost), xor ends `CANCELLED` with its READY node
/// transferred to the FREE stack exactly once — never lost (a leaked slot is
/// dead capacity forever), never double-pushed (which would corrupt the
/// intrusive links).
#[test]
fn loom_claim_index_vs_cancel() {
    loom::model(|| {
        ITERS_D.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let q = Arc::new(MiniQueue::one_slot(QUEUED, true));

        let a = q.clone();
        let claimer = loom::thread::spawn(move || a.claim());
        let b = q.clone();
        let canceller = loom::thread::spawn(move || b.cancel());

        let claimed = claimer.join().unwrap();
        let cancelled = canceller.join().unwrap();

        assert!(
            claimed.is_some() ^ cancelled,
            "QUEUED exited {} ways, expected exactly 1 (claim vs cancel race)",
            (claimed.is_some() as u8) + (cancelled as u8)
        );
        if cancelled {
            // The claimer (which always pops the node — nothing else does here)
            // found the CAS failed and must have transferred the node to FREE.
            assert_eq!(
                q.slots[0].state.load(std::sync::atomic::Ordering::SeqCst),
                CANCELLED
            );
            assert_eq!(q.pop_free(), Some(0), "cancelled node must land on FREE");
            assert!(q.pop_free().is_none(), "and exactly once");
            assert_eq!(
                q.cancelled.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "the transfer must pay down the winning cancel's count"
            );
        } else {
            assert_eq!(
                q.slots[0].state.load(std::sync::atomic::Ordering::SeqCst),
                CLAIMED
            );
            assert!(q.pop_free().is_none(), "no transfer on a won claim");
        }
        assert!(q.claim().is_none(), "READY must end empty either way");
    });
    eprintln!(
        "loom_claim_index_vs_cancel: explored {} interleavings",
        ITERS_D.load(std::sync::atomic::Ordering::Relaxed)
    );
}

/// Scenario 5 — a submitter's publish (`QUEUED` store, then READY push — the
/// order `publish_queued` fixes) vs a concurrent `claim_pop`: the claimer can
/// never observe (and discard) the slot in `RESERVED`, and the task is never
/// lost — it is claimed by the racing worker or remains claimable afterwards.
#[test]
fn loom_claim_index_publish_vs_pop() {
    loom::model(|| {
        ITERS_E.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Submitter holds slot 0 exclusively (RESERVED, off both stacks),
        // fields written — about to publish.
        let q = Arc::new(MiniQueue::one_slot(
            6, /* RESERVED (internal transient) */
            false,
        ));

        let s = q.clone();
        let submitter = loom::thread::spawn(move || {
            publish_queued(&s.ready, |i| &s.slots[i as usize], 0);
        });
        let w = q.clone();
        let claimer = loom::thread::spawn(move || w.claim());

        submitter.join().unwrap();
        let got = claimer.join().unwrap();

        match got {
            Some(idx) => {
                assert_eq!(idx, 0);
                assert_eq!(
                    q.slots[0].state.load(std::sync::atomic::Ordering::SeqCst),
                    CLAIMED
                );
            }
            None => {
                // Raced ahead of the push: the task must still be claimable.
                assert_eq!(q.claim(), Some(0), "published task must not be lost");
                assert_eq!(
                    q.slots[0].state.load(std::sync::atomic::Ordering::SeqCst),
                    CLAIMED
                );
            }
        }
        // A RESERVED slot must never have been mistaken for CANCELLED and
        // transferred to FREE (that is the push-before-publish bug).
        assert!(q.pop_free().is_none(), "FREE must stay empty");
    });
    eprintln!(
        "loom_claim_index_publish_vs_pop: explored {} interleavings",
        ITERS_E.load(std::sync::atomic::Ordering::Relaxed)
    );
}

// ---- Scenario 6 — the lease side table (ADR-0010, P0.3) ----

use shm_task::queue::{
    lease_word_gen, lease_word_state, pack_lease_word, LeaseBinding, LeaseSlot, LEASE_ARMED,
    LEASE_NONE,
};

/// A fresh, free lease record (generation 0, `NONE`).
fn lease_record() -> LeaseSlot {
    LeaseSlot {
        word: ShmU64::new(pack_lease_word(0, LEASE_NONE)),
        version: ShmU64::new(0),
        seq: ShmU64::new(0),
        deadline: ShmU64::new(0),
        artifact_id: ShmU32::new(0),
        incarnation: ShmU32::new(0),
        slot_idx: ShmU32::new(0),
        next: ShmU32::new(STACK_NIL),
    }
}

/// **Scenario 6 — exactly-once binding release + generation ABA-safety.**
///
/// The one genuinely new concurrent state machine P0.3 adds: an `ARMED` lease
/// record raced by two releasers (the requester's `ack` and the coordinator's
/// `reap_bindings` backstop), both driving the production
/// [`LeaseSlot::try_release`] CAS against the same observed word.
///
/// Invariants:
/// - **exactly one** releaser wins (the retained pin is decremented once,
///   never twice);
/// - after the winner retires the record and a fresh submit re-arms it for a
///   **different** task (generation bump), a stale releaser still holding the
///   old observation can never release the new task's binding — the
///   generation in the packed word is what makes the CAS ABA-safe.
#[test]
fn loom_lease_release_exactly_once_and_gen_aba_safe() {
    static ITERS: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);
    loom::model(|| {
        ITERS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        let rec = Arc::new(lease_record());
        let binding = LeaseBinding {
            artifact_id: 7,
            incarnation: 1,
            version: 3,
        };
        // Armed against task {slot 0, seq 1} by an exclusive armer.
        rec.arm(binding, 0, 1, 0);
        let armed_word = rec.word.load(core::sync::atomic::Ordering::Acquire);
        assert_eq!(lease_word_state(armed_word), LEASE_ARMED);
        assert_eq!(lease_word_gen(armed_word), 1);

        let acker = {
            let rec = Arc::clone(&rec);
            loom::thread::spawn(move || rec.try_release(armed_word))
        };
        let reaper = {
            let rec = Arc::clone(&rec);
            loom::thread::spawn(move || rec.try_release(armed_word))
        };
        let a = acker.join().unwrap();
        let b = reaper.join().unwrap();
        assert!(
            a ^ b,
            "exactly one releaser must win the ARMED -> RELEASED election"
        );

        // The winner retires the record; a fresh submit re-arms it for a
        // DIFFERENT task (gen 1 -> 2).
        rec.retire();
        rec.arm(binding, 0, /* new task incarnation */ 2, 0);

        // A stale releaser replaying its old observation must fail: same
        // state bits, different generation.
        assert!(
            !rec.try_release(armed_word),
            "a stale-generation release must never touch the new task's binding"
        );
        assert_eq!(
            lease_word_gen(rec.word.load(core::sync::atomic::Ordering::Acquire)),
            2
        );
        assert_eq!(
            lease_word_state(rec.word.load(core::sync::atomic::Ordering::Acquire)),
            LEASE_ARMED
        );
    });
    eprintln!(
        "loom_lease_release_exactly_once_and_gen_aba_safe: explored {} interleavings",
        ITERS.swap(0, core::sync::atomic::Ordering::Relaxed)
    );
}
