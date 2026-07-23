//! The shared-memory MPMC **task queue** ABI and the [`TaskQueue`] handle.
//!
//! Unlike [`shm_ring`]'s SPMC broadcast (wait-free, no CAS), a task queue is a
//! **multi-producer / multi-consumer** work queue with **exactly-once claiming**:
//! many submitters enqueue task descriptors, many workers each try to *claim* a
//! task, and a per-slot CAS state machine guarantees at most one worker owns any
//! task at a time. It is therefore given its **own** slot ABI (ADR-0002): the
//! ring's wait-free `Slot` must never carry an owner/deadline CAS state machine.
//!
//! # State machine (per slot)
//!
//! ```text
//!   submit()                 claim(w)              complete(r)
//!  ┌────────┐  reserve   ┌────────┐  CAS      ┌─────────┐  publish ┌──────┐
//!  │ EMPTY  │ ─────────▶ │ QUEUED │ ────────▶ │ CLAIMED │ ───────▶ │ DONE │
//!  │/terminal          ▲ └────────┘           └─────────┘          └──────┘
//!  └────────┘           │      │                  │  │  fail()      ┌────────┐
//!       ▲               │      │ cancel()         │  └────────────▶ │ FAILED │
//!       │ reuse         │      ▼                  │                 └────────┘
//!       │            reap│ ┌───────────┐          │  reap (deadline<now,
//!  (terminal slot     requeue │CANCELLED │        │   retry<cap) requeue ─┐
//!   reused by a       │    └───────────┘          │                       │
//!   later submit)     └───────────────────────────┴───────────────────────┘
//! ```
//!
//! `QUEUED→CLAIMED` is the single arbitration point: the CAS makes claiming
//! **exactly-once**. A `CLAIMED` task whose worker's lease lapses (its
//! `deadline` passes) is reset to `QUEUED` by [`TaskQueue::reap`] with an
//! incremented retry counter — **at-least-once** — until a retry cap is
//! exceeded, at which point it becomes terminally `FAILED`. Timeout/cancel is a
//! cooperative `cancel` flag the worker polls via [`ClaimedTask::is_cancelled`].
//!
//! # Correlation id = slot seq
//!
//! A [`TaskHandle`] is `{slot_idx, seq}`. `seq` is a per-slot monotonic counter
//! bumped on every **fresh** submit (never on a reap requeue, so the id is
//! stable across retries). Every requester/cancel op re-validates `seq`, so a
//! slot reused for a new task under an old handle is detected as
//! [`Error::StaleHandle`] — the ABA guard.

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use shm_core::{ChunkDesc, ShmU32, ShmU64};
use shm_ring::{NoopNotifier, Notifier, Parker};

use crate::error::{Error, Result};

/// Task-queue magic: little-endian bytes of `b"SHMTASK1"`.
pub const TASK_MAGIC: u64 = u64::from_le_bytes(*b"SHMTASK1");

/// Slot is unused and available for a fresh submit.
pub const EMPTY: u32 = 0;
/// Task is enqueued and awaiting a worker; the only claimable state.
pub const QUEUED: u32 = 1;
/// Task is exclusively owned by `owner`, executing.
pub const CLAIMED: u32 = 2;
/// Task finished successfully; `result` holds the worker's output descriptor.
pub const DONE: u32 = 3;
/// Task failed (worker called `fail`, was cancelled, or exceeded its retries).
pub const FAILED: u32 = 4;
/// Task was cancelled before any worker began executing it.
pub const CANCELLED: u32 = 5;

/// Internal transient: a submitter (or a reap requeue) has exclusively reserved
/// the slot and is writing its fields; not yet claimable. Never handed to a
/// [`TaskHandle`] holder (a live handle's `seq` only pairs with `QUEUED` or
/// later), so it is invisible to public [`TaskStatus`].
const RESERVED: u32 = 6;
/// Internal transient: the winning worker has exclusively reserved the slot to
/// publish its result; only one worker can be here per claim, so the 24-byte
/// `result` descriptor is never written by two workers at once (no torn write).
const COMPLETING: u32 = 7;

/// Sentinel meaning "no owner" in `owner`.
const OWNER_NONE: u32 = 0;

/// Default retry cap: a lapsed task is requeued at most this many times before
/// [`TaskQueue::reap`] gives up and marks it terminally `FAILED`.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Fixed header at the base of a task-queue region.
///
/// # Layout (frozen ABI — 40 bytes, 8-aligned)
///
/// | field          | type        | meaning                                   |
/// |----------------|-------------|-------------------------------------------|
/// | `magic`        | `u64`       | must equal [`TASK_MAGIC`]                  |
/// | `capacity`     | `u32`       | slot count (power of two)                  |
/// | `mask`         | `u32`       | `capacity - 1`                            |
/// | `enqueue_head` | `AtomicU64` | round-robin submit cursor (a hint)         |
/// | `waiters_work` | `AtomicU32` | idle workers parked on the work doorbell   |
/// | `waiters_done` | `AtomicU32` | requesters parked on the done doorbell     |
/// | `max_retries`  | `u32`       | reap retry cap                             |
/// | `doorbell_seq` | `AtomicU32` | reserved for v0.4 futex doorbell           |
#[repr(C)]
pub struct TaskQueueHeader {
    /// Must equal [`TASK_MAGIC`].
    pub magic: u64,
    /// Slot count; always a power of two.
    pub capacity: u32,
    /// `capacity - 1`, used to map the enqueue cursor to a slot index.
    pub mask: u32,
    /// Monotonic round-robin hint spreading concurrent submitters across slots.
    pub enqueue_head: AtomicU64,
    /// Number of workers currently parked awaiting work (a submit rings the work
    /// doorbell iff `> 0`).
    pub waiters_work: AtomicU32,
    /// Number of requesters currently parked awaiting a terminal outcome (a
    /// worker rings the done doorbell iff `> 0`).
    pub waiters_done: AtomicU32,
    /// Retry cap consulted by [`TaskQueue::reap`].
    pub max_retries: u32,
    /// **Reserved for v0.4's futex-based doorbell (ADR-0003).** Unused in v0.3:
    /// initialized to `0` and never read or written on the hot path. It consumes
    /// the former `_pad` word, so the header size and slot offsets are unchanged.
    pub doorbell_seq: AtomicU32,
}

const _: () = assert!(core::mem::size_of::<TaskQueueHeader>() == 40);
const _: () = assert!(core::mem::align_of::<TaskQueueHeader>() == 8);

/// One task slot: the CAS state machine plus request/result descriptors.
///
/// # Layout (frozen ABI — 80 bytes, 8-aligned)
///
/// | field      | type        | meaning                                        |
/// |------------|-------------|------------------------------------------------|
/// | `state`    | `AtomicU32` | `EMPTY`/`QUEUED`/`CLAIMED`/`DONE`/`FAILED`/`CANCELLED` |
/// | `owner`    | `AtomicU32` | claiming worker id (`0` = none)                |
/// | `seq`      | `AtomicU64` | incarnation id; correlation id of the handle    |
/// | `deadline` | `AtomicU64` | lease deadline (nanos); reap requeues if passed |
/// | `retry`    | `AtomicU32` | reap requeue count (capped by `max_retries`)    |
/// | `cancel`   | `AtomicU32` | cooperative cancel flag (`0` = live)            |
/// | `request`  | `ChunkDesc` | 24-byte submitted request descriptor            |
/// | `result`   | `ChunkDesc` | 24-byte worker output descriptor (when `DONE`)  |
///
/// The `request`/`result` payloads live in pool chunks; the slot only carries
/// their 24-byte descriptors. Each is written under an exclusive transient state
/// (`RESERVED`/`COMPLETING`) and read after an `Acquire` load of the guarding
/// `state`, so neither is ever observed torn.
#[repr(C)]
pub struct TaskSlot {
    /// Lifecycle state (see the module state machine).
    pub state: ShmU32,
    /// Exclusive owner worker id, or `0` when unclaimed.
    pub owner: ShmU32,
    /// Incarnation counter; bumped on every fresh submit, stable across reaps.
    pub seq: ShmU64,
    /// Absolute lease deadline in nanoseconds (same clock domain as `reap`'s
    /// `now`); a `CLAIMED` slot past its deadline is reaped.
    pub deadline: ShmU64,
    /// Number of times a lapsed claim has been requeued.
    pub retry: ShmU32,
    /// Cooperative cancel flag: nonzero asks the owning worker to abort.
    pub cancel: ShmU32,
    /// The submitted request descriptor (guarded by `state`).
    pub request: ChunkDesc,
    /// The worker's result descriptor, valid once `state == DONE`.
    pub result: ChunkDesc,
}

// 80-byte frozen ABI. Gated on `not(loom)`: the atomic fields are
// `#[repr(transparent)]` substrate newtypes (byte-identical to the bare atomics in
// production, loom's fat twin under `--cfg loom`); the loom build reconstructs the
// claim state machine in ordinary memory and never overlays these bytes on shm.
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<TaskSlot>() == 80);
#[cfg(not(loom))]
const _: () = assert!(core::mem::align_of::<TaskSlot>() == 8);

impl TaskSlot {
    /// **The single claim arbitration point.** CAS `QUEUED → CLAIMED`; `true` iff
    /// this caller won the task. The CAS makes claiming **exactly-once**: at most
    /// one worker transitions any given `QUEUED` slot, so a second concurrent
    /// claimer of the same slot always sees `false`. Extracted so the production
    /// [`TaskQueue::claim`] path and the loom harness (ADR-0004 stage L, core 3)
    /// run the *same* CAS body.
    #[inline]
    pub fn try_claim(&self) -> bool {
        self.state
            .compare_exchange(QUEUED, CLAIMED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// **Completer's exclusive-publish election.** CAS `CLAIMED → COMPLETING`;
    /// `true` iff this worker won the right to publish its result. Races the reaper
    /// out of `CLAIMED` — whichever CAS wins is the *sole* transition, so a task is
    /// never both completed and requeued/failed. (Model-checked in core 3.)
    #[inline]
    pub fn try_begin_complete(&self) -> bool {
        self.state
            .compare_exchange(CLAIMED, COMPLETING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// **Reaper's requeue election.** CAS `CLAIMED → RESERVED` (a lapsed lease's
    /// exclusive requeue reservation); `true` iff the reaper won. Loses to a
    /// concurrent [`try_begin_complete`](Self::try_begin_complete) — the single-
    /// transition guarantee.
    #[inline]
    pub fn try_begin_reap_requeue(&self) -> bool {
        self.state
            .compare_exchange(CLAIMED, RESERVED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// **Terminal-fail election.** CAS `CLAIMED → FAILED` — used both by a worker
    /// giving up ([`ClaimedTask::fail`]) and by the reaper when the retry cap is
    /// exhausted; `true` iff this caller won. Loses to a concurrent completer, so a
    /// task is never both failed and completed.
    #[inline]
    pub fn try_fail(&self) -> bool {
        self.state
            .compare_exchange(CLAIMED, FAILED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

/// Round `x` up to a multiple of `a` (a power of two).
#[inline]
const fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

/// Byte offset of the slot array within a region (64-byte aligned).
#[inline]
pub const fn slots_offset() -> usize {
    align_up(core::mem::size_of::<TaskQueueHeader>(), 64)
}

/// Total bytes a task queue of `capacity` slots needs.
#[inline]
pub const fn required_bytes(capacity: u32) -> usize {
    slots_offset() + capacity as usize * core::mem::size_of::<TaskSlot>()
}

/// The correlation id of a submitted task: `{slot_idx, seq}`.
///
/// There is no separate correlation table — the id *is* the slot index plus the
/// slot's incarnation `seq`. Every requester/cancel op re-validates `seq` so a
/// reused slot never resolves an old handle (the ABA guard).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskHandle {
    /// Index of the slot holding this task.
    pub slot_idx: u32,
    /// The slot incarnation the task was submitted at.
    pub seq: u64,
}

/// The observable status of a task (from a requester's [`TaskQueue::poll`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    /// Enqueued, not yet claimed.
    Queued,
    /// Claimed by a worker and executing.
    Claimed,
    /// Completed successfully; carries the worker's result descriptor.
    Done(ChunkDesc),
    /// Failed (worker `fail`, cancel, or retries exhausted).
    Failed,
    /// Cancelled before any worker executed it.
    Cancelled,
}

/// A terminal task outcome (from a requester's [`TaskQueue::wait`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Success; carries the worker's result descriptor.
    Done(ChunkDesc),
    /// The task failed.
    Failed,
    /// The task was cancelled.
    Cancelled,
}

/// What one [`TaskQueue::reap`] sweep did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReapReport {
    /// Lapsed claims reset to `QUEUED` for another attempt (at-least-once).
    pub requeued: u32,
    /// Lapsed claims that exceeded the retry cap and were failed terminally.
    pub failed: u32,
}

/// A shared, injected notifier (work-available or task-done doorbell).
type NotifierRef = Arc<dyn Notifier + Send + Sync>;

/// A process-local handle to an MPMC task queue placed in shared memory.
///
/// Cheap to clone (cached pointers + geometry + two `Arc` doorbell notifiers);
/// every clone refers to the same shared queue. Cross-process safety comes from
/// the atomics in the mapped [`TaskQueueHeader`]/[`TaskSlot`]s. Submitters,
/// workers, and requesters all operate through this one type.
#[derive(Clone)]
pub struct TaskQueue {
    header: *mut TaskQueueHeader,
    slots: *mut TaskSlot,
    capacity: usize,
    mask: u64,
    /// Rung by a submitter when idle workers are parked.
    work: NotifierRef,
    /// Rung by a worker on `complete`/`fail` when requesters are parked.
    done: NotifierRef,
}

// SAFETY: the handle only holds pointers into a `MAP_SHARED` (or heap) region;
// all shared mutation is via atomics with explicit ordering, and each `desc`
// payload is guarded by the per-slot `state` transitions. The `Arc<dyn Notifier
// + Send + Sync>` fields are themselves `Send + Sync`. The handle is safe to
// send/share across threads and processes.
unsafe impl Send for TaskQueue {}
unsafe impl Sync for TaskQueue {}

impl TaskQueue {
    /// Initialize a fresh queue of `capacity` slots into the region at `base`,
    /// using [`DEFAULT_MAX_RETRIES`].
    ///
    /// # Safety
    ///
    /// `base` must point at `region_len` writable bytes that stay mapped for the
    /// lifetime of every handle derived from it, and no other party may
    /// concurrently initialize the same region.
    pub unsafe fn init(base: *mut u8, region_len: usize, capacity: u32) -> Result<TaskQueue> {
        // SAFETY: forwarded to the primary initializer under the same contract.
        unsafe { Self::init_with_max_retries(base, region_len, capacity, DEFAULT_MAX_RETRIES) }
    }

    /// Initialize a fresh queue with an explicit retry cap.
    ///
    /// # Safety
    ///
    /// Same contract as [`TaskQueue::init`].
    pub unsafe fn init_with_max_retries(
        base: *mut u8,
        region_len: usize,
        capacity: u32,
        max_retries: u32,
    ) -> Result<TaskQueue> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(Error::BadCapacity(capacity));
        }
        if !(base as usize).is_multiple_of(8) {
            return Err(Error::Misaligned);
        }
        let need = required_bytes(capacity);
        if region_len < need {
            return Err(Error::RegionTooSmall { need, have: region_len });
        }

        // SAFETY: `base` is 8-aligned and `region_len >= need`, so the header and
        // every slot offset below stays within the writable region.
        unsafe {
            let header = base.cast::<TaskQueueHeader>();
            header.write(TaskQueueHeader {
                magic: TASK_MAGIC,
                capacity,
                mask: capacity - 1,
                enqueue_head: AtomicU64::new(0),
                waiters_work: AtomicU32::new(0),
                waiters_done: AtomicU32::new(0),
                max_retries,
                doorbell_seq: AtomicU32::new(0),
            });
            let slots = base.add(slots_offset()).cast::<TaskSlot>();
            for i in 0..capacity as usize {
                slots.add(i).write(TaskSlot {
                    state: ShmU32::new(EMPTY),
                    owner: ShmU32::new(OWNER_NONE),
                    seq: ShmU64::new(0),
                    deadline: ShmU64::new(0),
                    retry: ShmU32::new(0),
                    cancel: ShmU32::new(0),
                    request: ChunkDesc::ZERO,
                    result: ChunkDesc::ZERO,
                });
            }
            Ok(TaskQueue {
                header,
                slots,
                capacity: capacity as usize,
                mask: (capacity - 1) as u64,
                work: Arc::new(NoopNotifier),
                done: Arc::new(NoopNotifier),
            })
        }
    }

    /// Attach a handle onto an already-initialized queue region.
    ///
    /// # Safety
    ///
    /// `base` must point at a region previously initialized by [`TaskQueue::init`]
    /// that stays mapped for the lifetime of the returned handle.
    pub unsafe fn attach(base: *mut u8, region_len: usize) -> Result<TaskQueue> {
        if !(base as usize).is_multiple_of(8) {
            return Err(Error::Misaligned);
        }
        if region_len < core::mem::size_of::<TaskQueueHeader>() {
            return Err(Error::RegionTooSmall {
                need: core::mem::size_of::<TaskQueueHeader>(),
                have: region_len,
            });
        }
        let header = base.cast::<TaskQueueHeader>();
        // SAFETY: header lives at the region base; the region is at least header-
        // sized. Reading the scalar fields is in-bounds.
        let (magic, capacity, mask) = unsafe {
            let h = &*header;
            (h.magic, h.capacity, h.mask)
        };
        if magic != TASK_MAGIC {
            return Err(Error::BadMagic);
        }
        let need = required_bytes(capacity);
        if region_len < need {
            return Err(Error::RegionTooSmall { need, have: region_len });
        }
        // SAFETY: `capacity` was validated at init; the slot array is in-bounds.
        let slots = unsafe { base.add(slots_offset()).cast::<TaskSlot>() };
        Ok(TaskQueue {
            header,
            slots,
            capacity: capacity as usize,
            mask: mask as u64,
            work: Arc::new(NoopNotifier),
            done: Arc::new(NoopNotifier),
        })
    }

    /// Attach the **work** doorbell notifier (rung to wake idle workers on a
    /// submit). Builder-style; consumes and returns `self`.
    #[must_use]
    pub fn with_work_notifier<N: Notifier + Send + Sync + 'static>(mut self, n: N) -> TaskQueue {
        self.work = Arc::new(n);
        self
    }

    /// Attach the **done** doorbell notifier (rung to wake parked requesters when
    /// a task reaches a terminal state). Builder-style.
    #[must_use]
    pub fn with_done_notifier<N: Notifier + Send + Sync + 'static>(mut self, n: N) -> TaskQueue {
        self.done = Arc::new(n);
        self
    }

    /// The number of slots (a power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The reap retry cap this queue was initialized with.
    #[inline]
    pub fn max_retries(&self) -> u32 {
        self.header().max_retries
    }

    #[inline]
    fn header(&self) -> &TaskQueueHeader {
        // SAFETY: `header` points at the mapped region for this handle's life.
        unsafe { &*self.header }
    }

    #[inline]
    fn slot(&self, idx: usize) -> &TaskSlot {
        debug_assert!(idx < self.capacity);
        // SAFETY: `idx < capacity`; the slot array has `capacity` entries.
        unsafe { &*self.slots.add(idx) }
    }

    #[inline]
    fn slot_ptr(&self, idx: usize) -> *mut TaskSlot {
        debug_assert!(idx < self.capacity);
        // SAFETY: `idx < capacity`; the slot array has `capacity` entries.
        unsafe { self.slots.add(idx) }
    }

    // ---- Submitter API ----

    /// Enqueue a task and return its correlation [`TaskHandle`].
    ///
    /// Finds a reusable slot (an `EMPTY` or terminal `DONE`/`FAILED`/`CANCELLED`
    /// slot) via the round-robin enqueue cursor, exclusively reserves it, bumps
    /// and publishes a fresh `seq`, writes the `request` and `deadline`, clears
    /// `cancel`/`retry`, marks it `QUEUED`, and rings the work doorbell iff idle
    /// workers are parked. Returns [`Error::QueueFull`] if every slot holds a
    /// live (`QUEUED`/`CLAIMED`) task.
    ///
    /// `deadline_nanos` is an absolute deadline in the same clock domain
    /// [`TaskQueue::reap`] compares against (see [`crate::now_nanos`]); a
    /// `CLAIMED` task past it is presumed dead and requeued.
    pub fn submit(&self, request: ChunkDesc, deadline_nanos: u64) -> Result<TaskHandle> {
        let cap = self.capacity;
        let start = self.header().enqueue_head.fetch_add(1, Ordering::Relaxed);
        for i in 0..cap {
            let idx = ((start.wrapping_add(i as u64)) & self.mask) as usize;
            let slot = self.slot(idx);
            let st = slot.state.load(Ordering::Acquire);
            let reusable = matches!(st, EMPTY | DONE | FAILED | CANCELLED);
            if !reusable {
                continue;
            }
            // Exclusively reserve the slot so no worker can claim a half-written
            // task and no second submitter races us for the same slot.
            if slot
                .state
                .compare_exchange(st, RESERVED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue; // lost the race for this slot; keep scanning.
            }

            // We own the slot. Publish a fresh incarnation *first* so any old
            // handle to this (now reused) slot immediately reads a mismatched
            // `seq` and resolves as `StaleHandle`.
            let new_seq = slot.seq.load(Ordering::Relaxed).wrapping_add(1);
            slot.seq.store(new_seq, Ordering::Release);
            slot.owner.store(OWNER_NONE, Ordering::Release);
            slot.retry.store(0, Ordering::Release);
            slot.cancel.store(0, Ordering::Release);
            slot.deadline.store(deadline_nanos, Ordering::Release);
            // SAFETY: we hold the exclusive `RESERVED` reservation, so no other
            // party reads or writes these descriptors until we store `QUEUED`.
            unsafe {
                let p = self.slot_ptr(idx);
                core::ptr::addr_of_mut!((*p).request).write(request);
                core::ptr::addr_of_mut!((*p).result).write(ChunkDesc::ZERO);
            }
            // Publish: the task is now claimable. Release pairs with the worker's
            // Acquire in `claim`, so it observes the fields written above.
            slot.state.store(QUEUED, Ordering::Release);

            if self.header().waiters_work.load(Ordering::Acquire) > 0 {
                self.work.notify();
            }
            return Ok(TaskHandle { slot_idx: idx as u32, seq: new_seq });
        }
        Err(Error::QueueFull)
    }

    // ---- Worker API ----

    /// Try to claim one queued task. Returns `None` if none is queued.
    ///
    /// Scans for a `QUEUED` slot and CASes it `QUEUED→CLAIMED`. The CAS is the
    /// single arbitration point: at most one worker transitions any given task,
    /// so claiming is **exactly-once** — a task in `CLAIMED` cannot be claimed by
    /// a second worker. `worker_id` should be nonzero (`0` means "no owner").
    ///
    /// The claim inherits the deadline the task was **submitted** with; use
    /// [`claim_with_lease`](TaskQueue::claim_with_lease) to (re)set a fresh lease
    /// deadline at claim time.
    pub fn claim(&self, worker_id: u32) -> Option<ClaimedTask> {
        self.claim_inner(worker_id, None)
    }

    /// Claim one queued task and (re)set its lease `deadline`, returning `None`
    /// if none is queued.
    ///
    /// Identical to [`claim`](TaskQueue::claim) but stamps `deadline_nanos` onto
    /// the slot on a successful claim, so the worker's lease starts *now* rather
    /// than at submit time. This is what a lease-driven runtime wants: a task
    /// re-dispatched after a dead worker was reaped gets a **fresh** deadline in
    /// its new claimant's hands, so the next [`reap`](TaskQueue::reap) does not
    /// immediately re-requeue a healthy worker whose task happened to be submitted
    /// with a since-elapsed deadline. `deadline_nanos` is in the same clock domain
    /// [`reap`](TaskQueue::reap) compares against (see [`crate::now_nanos`]).
    pub fn claim_with_lease(&self, worker_id: u32, deadline_nanos: u64) -> Option<ClaimedTask> {
        self.claim_inner(worker_id, Some(deadline_nanos))
    }

    /// The shared claim body: CAS a `QUEUED` slot to `CLAIMED`, optionally
    /// stamping a fresh lease `deadline`.
    fn claim_inner(&self, worker_id: u32, deadline: Option<u64>) -> Option<ClaimedTask> {
        debug_assert_ne!(worker_id, OWNER_NONE, "worker id must be nonzero");
        for idx in 0..self.capacity {
            let slot = self.slot(idx);
            if slot.state.load(Ordering::Acquire) != QUEUED {
                continue;
            }
            if !slot.try_claim() {
                continue; // another worker claimed it first.
            }
            slot.owner.store(worker_id, Ordering::Release);
            if let Some(deadline) = deadline {
                // Fresh lease: the claim's clock starts now, not at submit.
                slot.deadline.store(deadline, Ordering::Release);
            }
            let seq = slot.seq.load(Ordering::Acquire);
            // SAFETY: the `QUEUED→CLAIMED` Acquire pairs with the submitter's
            // Release store of `QUEUED`, so the `request` written before it is
            // visible; we read it through the raw pointer as a whole POD.
            let request = unsafe { core::ptr::addr_of!((*self.slot_ptr(idx)).request).read() };
            return Some(ClaimedTask {
                queue: self.clone(),
                idx,
                seq,
                worker_id,
                request,
            });
        }
        None
    }

    /// Claim a task, parking on the work doorbell while none is queued.
    ///
    /// Registers as an idle worker (so submitters know to ring the doorbell),
    /// re-checks (closing the lost-wakeup window), then parks. Loops until a task
    /// is claimed. The bounded [`Parker`] timeout guarantees liveness even if a
    /// wakeup is missed.
    pub fn claim_blocking<P: Parker>(&self, worker_id: u32, parker: &P) -> ClaimedTask {
        loop {
            if let Some(task) = self.claim(worker_id) {
                return task;
            }
            self.header().waiters_work.fetch_add(1, Ordering::AcqRel);
            // Re-check after registering: a submit that raced our registration is
            // still observed here before we sleep.
            let task = self.claim(worker_id);
            if task.is_none() {
                parker.park();
            }
            self.header().waiters_work.fetch_sub(1, Ordering::AcqRel);
            if let Some(task) = task {
                return task;
            }
        }
    }

    /// Claim a task, parking on the work doorbell while none is queued, stamping
    /// a fresh lease deadline `lease_nanos` into the future at the moment of the
    /// successful claim.
    ///
    /// The lease-refreshing counterpart of
    /// [`claim_blocking`](TaskQueue::claim_blocking): the deadline is computed as
    /// `now_nanos() + lease_nanos` *when the claim succeeds*, so a re-dispatched
    /// task (whose submit-time deadline has long elapsed) is not immediately
    /// re-reaped out from under the healthy worker that just picked it up. Uses
    /// [`crate::now_nanos`] for the clock, matching [`reap`](TaskQueue::reap).
    pub fn claim_blocking_with_lease<P: Parker>(
        &self,
        worker_id: u32,
        lease_nanos: u64,
        parker: &P,
    ) -> ClaimedTask {
        loop {
            if let Some(task) =
                self.claim_with_lease(worker_id, crate::now_nanos().wrapping_add(lease_nanos))
            {
                return task;
            }
            self.header().waiters_work.fetch_add(1, Ordering::AcqRel);
            let task =
                self.claim_with_lease(worker_id, crate::now_nanos().wrapping_add(lease_nanos));
            if task.is_none() {
                parker.park();
            }
            self.header().waiters_work.fetch_sub(1, Ordering::AcqRel);
            if let Some(task) = task {
                return task;
            }
        }
    }

    // ---- Requester API ----

    /// Read a task's current status, validating the handle's `seq`.
    ///
    /// Returns [`Error::StaleHandle`] if the slot was reused for a newer task
    /// (its `seq` moved past the handle's), otherwise the live/terminal
    /// [`TaskStatus`].
    pub fn poll(&self, handle: TaskHandle) -> Result<TaskStatus> {
        let idx = handle.slot_idx as usize;
        if idx >= self.capacity {
            return Err(Error::StaleHandle);
        }
        let slot = self.slot(idx);
        if slot.seq.load(Ordering::Acquire) != handle.seq {
            return Err(Error::StaleHandle);
        }
        let status = match slot.state.load(Ordering::Acquire) {
            // A live handle's `seq` only pairs with `QUEUED` or later; the
            // transient `RESERVED` is mapped defensively (a reap is requeueing).
            QUEUED | RESERVED => TaskStatus::Queued,
            CLAIMED | COMPLETING => TaskStatus::Claimed,
            DONE => {
                // SAFETY: `state == DONE` was Acquire-loaded and pairs with the
                // worker's Release store after writing `result`; the descriptor
                // is a fully-published POD read through the raw pointer.
                let result = unsafe { core::ptr::addr_of!((*self.slot_ptr(idx)).result).read() };
                // Re-validate `seq` to reject a slot reused mid-read.
                if slot.seq.load(Ordering::Acquire) != handle.seq {
                    return Err(Error::StaleHandle);
                }
                TaskStatus::Done(result)
            }
            FAILED => TaskStatus::Failed,
            CANCELLED => TaskStatus::Cancelled,
            // `EMPTY` under a matching seq cannot occur (a fresh submit bumps
            // seq); treat any unexpected state as gone.
            _ => return Err(Error::StaleHandle),
        };
        Ok(status)
    }

    /// Block until a task reaches a terminal state, parking on the done doorbell.
    ///
    /// Registers as a parked requester (so workers know to ring the doorbell),
    /// re-checks, then parks; loops until [`TaskStatus`] is terminal. Returns
    /// [`Error::StaleHandle`] if the slot is reused out from under the handle.
    pub fn wait<P: Parker>(&self, handle: TaskHandle, parker: &P) -> Result<Outcome> {
        loop {
            match self.poll(handle)? {
                TaskStatus::Done(r) => return Ok(Outcome::Done(r)),
                TaskStatus::Failed => return Ok(Outcome::Failed),
                TaskStatus::Cancelled => return Ok(Outcome::Cancelled),
                TaskStatus::Queued | TaskStatus::Claimed => {}
            }
            self.header().waiters_done.fetch_add(1, Ordering::AcqRel);
            // Re-check after registering to close the lost-wakeup window.
            let recheck = self.poll(handle)?;
            let terminal = !matches!(recheck, TaskStatus::Queued | TaskStatus::Claimed);
            if !terminal {
                parker.park();
            }
            self.header().waiters_done.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Request cancellation of a task, validating the handle's `seq`.
    ///
    /// If still `QUEUED`, CASes it `QUEUED→CANCELLED` (no worker ever runs it).
    /// If `CLAIMED`, sets the cooperative `cancel` flag: the owning worker
    /// observes it via [`ClaimedTask::is_cancelled`] and should `fail`/exit.
    /// Terminal tasks are left unchanged. Returns [`Error::StaleHandle`] on a
    /// reused slot.
    pub fn cancel(&self, handle: TaskHandle) -> Result<()> {
        let idx = handle.slot_idx as usize;
        if idx >= self.capacity {
            return Err(Error::StaleHandle);
        }
        let slot = self.slot(idx);
        if slot.seq.load(Ordering::Acquire) != handle.seq {
            return Err(Error::StaleHandle);
        }
        // Try the pre-claim cancel first.
        if slot
            .state
            .compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // A requester might be waiting on this handle.
            if self.header().waiters_done.load(Ordering::Acquire) > 0 {
                self.done.notify();
            }
            return Ok(());
        }
        // Otherwise, if it is (still) claimed, raise the cooperative flag. Re-
        // validate seq to avoid flagging a task that was reused after our load.
        if slot.state.load(Ordering::Acquire) == CLAIMED
            && slot.seq.load(Ordering::Acquire) == handle.seq
        {
            slot.cancel.store(1, Ordering::Release);
        }
        Ok(())
    }

    // ---- Coordinator / lease reap ----

    /// Reap lapsed claims: requeue (at-least-once) or fail them.
    ///
    /// For each `CLAIMED` slot whose `deadline < now_nanos` (its worker is
    /// presumed dead or stuck), the rule is:
    ///
    /// - if its `retry` count is **below** `max_retries`, reset it to `QUEUED`,
    ///   increment `retry`, clear `owner` (keeping the same `seq`, so the
    ///   correlation id is stable across retries), and it becomes claimable
    ///   again — **at-least-once**;
    /// - otherwise the retry cap is exhausted: transition it terminally to
    ///   `FAILED`.
    ///
    /// So a task is attempted at most `max_retries + 1` times before failing.
    /// Rings the work doorbell if any task was requeued and the done doorbell if
    /// any was failed (subject to parked-waiter hints). Concurrency-safe: each
    /// transition is a CAS out of `CLAIMED`, so it never races a worker that is
    /// completing (whichever CAS out of `CLAIMED` wins is the sole transition).
    pub fn reap(&self, now_nanos: u64) -> ReapReport {
        let max_retries = self.header().max_retries;
        let mut report = ReapReport::default();
        for idx in 0..self.capacity {
            let slot = self.slot(idx);
            if slot.state.load(Ordering::Acquire) != CLAIMED {
                continue;
            }
            if slot.deadline.load(Ordering::Acquire) >= now_nanos {
                continue; // lease still valid.
            }
            let retry = slot.retry.load(Ordering::Acquire);
            if retry >= max_retries {
                // Retry cap exhausted → terminal failure.
                if slot.try_fail() {
                    report.failed += 1;
                }
                continue;
            }
            // Requeue via an exclusive reservation so a new claimer cannot set
            // `owner` before we clear it (which would otherwise be clobbered).
            if slot.try_begin_reap_requeue() {
                slot.retry.store(retry + 1, Ordering::Release);
                slot.owner.store(OWNER_NONE, Ordering::Release);
                // Keep `seq` — the correlation id must survive the retry.
                slot.state.store(QUEUED, Ordering::Release);
                report.requeued += 1;
            }
        }
        if report.requeued > 0 && self.header().waiters_work.load(Ordering::Acquire) > 0 {
            self.work.notify();
        }
        if report.failed > 0 && self.header().waiters_done.load(Ordering::Acquire) > 0 {
            self.done.notify();
        }
        report
    }

    /// Ring the done doorbell for a terminal transition iff requesters are parked.
    fn notify_done_if_waiters(&self) {
        if self.header().waiters_done.load(Ordering::Acquire) > 0 {
            self.done.notify();
        }
    }
}

/// A task exclusively claimed by a worker, holding it until completion.
///
/// Exposes the request payload, a cooperative cancel poll, and the two terminal
/// transitions ([`complete`](Self::complete)/[`fail`](Self::fail)). Dropping a
/// `ClaimedTask` without completing it leaves the slot `CLAIMED`; the
/// coordinator's [`reap`](TaskQueue::reap) will eventually requeue it once its
/// deadline lapses (at-least-once).
pub struct ClaimedTask {
    queue: TaskQueue,
    idx: usize,
    seq: u64,
    worker_id: u32,
    request: ChunkDesc,
}

impl ClaimedTask {
    /// The submitted request descriptor to operate on.
    #[inline]
    pub fn request(&self) -> ChunkDesc {
        self.request
    }

    /// The correlation id of this task (`{slot_idx, seq}`), for dedup/logging.
    #[inline]
    pub fn task_id(&self) -> TaskHandle {
        TaskHandle { slot_idx: self.idx as u32, seq: self.seq }
    }

    /// The id of the worker that claimed this task.
    #[inline]
    pub fn worker_id(&self) -> u32 {
        self.worker_id
    }

    /// Whether cancellation has been requested (the cooperative poll).
    ///
    /// A cooperative worker calls this periodically and, when it returns `true`,
    /// stops work and calls [`fail`](Self::fail).
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        // Only meaningful while we still hold the claim; a reused slot's flag is
        // guarded by the `seq` check callers of `poll`/`cancel` perform.
        self.queue.slot(self.idx).cancel.load(Ordering::Acquire) != 0
    }

    /// Complete the task with `result`, transitioning `CLAIMED→DONE`.
    ///
    /// Exclusively reserves the slot (`CLAIMED→COMPLETING`) so only this worker
    /// writes the 24-byte `result` (no torn write even if a reaped duplicate
    /// worker also runs), publishes the result, marks `DONE`, and rings the done
    /// doorbell. Returns [`Error::Lost`] if the claim was already reaped/re-
    /// dispatched (its lease lapsed) — the caller's work is discarded and another
    /// attempt owns the task (at-least-once; dedup on [`task_id`](Self::task_id)).
    pub fn complete(self, result: ChunkDesc) -> Result<()> {
        let slot = self.queue.slot(self.idx);
        if slot.seq.load(Ordering::Acquire) != self.seq {
            return Err(Error::StaleHandle);
        }
        // Win the exclusive right to publish. Only one worker (and never the
        // reaper, which CASes `CLAIMED→RESERVED/FAILED`) can win this.
        if !slot.try_begin_complete() {
            return Err(Error::Lost);
        }
        // SAFETY: we hold the exclusive `COMPLETING` reservation, so no other
        // party reads or writes `result` until we publish `DONE` below.
        unsafe {
            core::ptr::addr_of_mut!((*self.queue.slot_ptr(self.idx)).result).write(result);
        }
        // Publish: Release pairs with the requester's Acquire load of `DONE`.
        slot.state.store(DONE, Ordering::Release);
        self.queue.notify_done_if_waiters();
        Ok(())
    }

    /// Fail the task, transitioning `CLAIMED→FAILED` and ringing the done
    /// doorbell.
    ///
    /// Returns [`Error::Lost`] if the claim was already reaped/re-dispatched.
    pub fn fail(self) -> Result<()> {
        let slot = self.queue.slot(self.idx);
        if slot.seq.load(Ordering::Acquire) != self.seq {
            return Err(Error::StaleHandle);
        }
        if !slot.try_fail() {
            return Err(Error::Lost);
        }
        self.queue.notify_done_if_waiters();
        Ok(())
    }
}
