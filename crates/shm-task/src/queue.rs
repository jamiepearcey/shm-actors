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
//! # O(1) discovery: the FREE / READY index stacks (ADR-0009, P0.2)
//!
//! Which slot to *touch* is found in O(1) through two intrusive, ABA-safe
//! Treiber stacks, each sharded [`SHARDS`] ways, whose heads live in the header
//! ([`TaskQueueHeader::free_heads`] / [`TaskQueueHeader::ready_heads`]) and whose links ride each slot's
//! [`TaskSlot::next`] — the exact loom-checked
//! [`treiber_pop`](shm_core::pool::treiber_pop)/[`treiber_push`](shm_core::pool::treiber_push)
//! loops the chunk pools and the `shm-store` catalog run:
//!
//! - **FREE** holds slots a submit may reuse; a terminal transition
//!   (`complete`/`fail`/reap-fail) pushes its slot here.
//! - **READY** holds `QUEUED` slots awaiting a claim; `submit` and a reap
//!   requeue push here *after* storing `QUEUED`.
//!
//! The stacks are only a **discovery index** under a strict single-membership
//! discipline: a slot is on exactly one stack, or held exclusively by one party
//! (a submitter in `RESERVED`, a worker in `CLAIMED`/`COMPLETING`, a terminal
//! transition mid-push). One documented exception: a slot cancelled while
//! `QUEUED` keeps riding its READY node until the next claim pop (or a
//! queue-full submit fallback) transfers it to FREE. Exactly-once arbitration is
//! **still** the per-slot [`try_claim`](TaskSlot::try_claim) CAS — a claim pops
//! READY and *revalidates* with the CAS; on failure the slot is provably
//! `CANCELLED` (the only other exit from `QUEUED`, since a resubmission needs a
//! FREE pop the slot has not had) and the popper transfers it to FREE. Each
//! discarded node is paid for by the cancel that created it, so claim is
//! amortized O(1) and never scans the slot array.
//!
//! # Correlation id = slot seq
//!
//! A [`TaskHandle`] is `{slot_idx, seq}`. `seq` is a per-slot monotonic counter
//! bumped on every **fresh** submit (never on a reap requeue, so the id is
//! stable across retries). Every requester/cancel op re-validates `seq`, so a
//! slot reused for a new task under an old handle is detected as
//! [`Error::StaleHandle`] — the ABA guard.

use core::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use shm_core::{ChunkDesc, ShmU32, ShmU64};
use shm_ring::{NoopNotifier, Notifier, Parker};

use crate::error::{Error, Result};

/// Task-queue magic: little-endian bytes of `b"SHMTASK4"`.
///
/// Bumped from `SHMTASK3` by ADR-0012: the header grew from one cache line to
/// seventeen (control line + 8 sharded FREE heads + 8 sharded READY heads) and
/// `TaskSlot` from 88 to 128 bytes, so a stale `SHMTASK3` region fails loudly.
/// (`SHMTASK3` replaced `SHMTASK2` at ADR-0010 / P0.3: the region gained the **lease
/// side table** (task-lifecycle-tied retained-ref bindings, appended after the
/// slot array) and the header gained its Treiber free-list head, so a stale
/// `SHMTASK2` region fails loudly with [`Error::BadMagic`] instead of being
/// misread. (`SHMTASK2` itself replaced `SHMTASK1` at ADR-0009 / P0.2, when
/// the round-robin `enqueue_head` became the FREE/READY stack heads and every
/// slot grew an intrusive `next` link.)
pub const TASK_MAGIC: u64 = u64::from_le_bytes(*b"SHMTASK4");

/// Intrusive-stack link sentinel: "no next slot" / an empty stack head. The
/// same `u32::MAX` sentinel [`treiber_pop`](shm_core::pool::treiber_pop)
/// terminates on (and the `shm-store` catalog's `FREE_NIL`).
pub const STACK_NIL: u32 = u32::MAX;

/// Pack a Treiber stack head word: `{tag:32 | idx:32}`. The tag is bumped on
/// every push and pop, which is what makes the stack ABA-safe — the same shape
/// [`Pool`](shm_core::Pool) uses for its chunk free lists.
#[inline]
pub const fn pack_stack_head(idx: u32, tag: u32) -> u64 {
    ((tag as u64) << 32) | (idx as u64)
}

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
/// # Layout (frozen ABI — 1088 bytes = seventeen cache lines, 8-aligned; `SHMTASK4`)
///
/// **Line 0 — control (cold or read-mostly):**
///
/// | field             | type        | meaning                                   |
/// |-------------------|-------------|-------------------------------------------|
/// | `magic`           | `u64`       | must equal [`TASK_MAGIC`]                  |
/// | `capacity`        | `u32`       | slot count (power of two)                  |
/// | `mask`            | `u32`       | `capacity - 1`                            |
/// | `cancelled`       | `ShmU32`    | CANCELLED slots still riding READY nodes   |
/// | `reserved2`       | `u32`       | reserved padding (zero)                    |
/// | `waiters_work`    | `AtomicU32` | idle workers parked on the work doorbell   |
/// | `waiters_done`    | `AtomicU32` | requesters parked on the done doorbell     |
/// | `max_retries`     | `u32`       | reap retry cap                             |
/// | `doorbell_seq`    | `AtomicU32` | futex doorbell word (ADR-0011)             |
/// | `lease_free_head` | `ShmU64`    | Treiber head of the lease side table       |
/// | `_pad0`           | `[u8; 16]`  | to the line boundary                       |
///
/// **Lines 1–8 — `free_heads[0..8]`**, one Treiber head per line.
/// **Lines 9–16 — `ready_heads[0..8]`**, one per line. Total 1088 bytes.
///
/// # Why sharded (`SHMTASK3` → `SHMTASK4`, ADR-0012)
///
/// At `SHMTASK3` there was one `free_head` and one `ready_head`, and they shared
/// a single 64-byte header line with the doorbell and both waiter counts. The
/// measured consequence was aggregate throughput that *fell* as workers were
/// added: 6.9 → 2.3 M/s from one worker to four on disjoint tasks.
///
/// Padding the two heads onto their own lines was tried first and changed
/// nothing measurable. The isolating experiment (`shm-bench task`, "drain":
/// pre-filled queue, N workers, no producers, no shared counter, **claim
/// only**) then put it beyond argument: 59 M/s with one worker, 17.6 with
/// two, 6.3 with four, 5.3 with eight — a 9× collapse on nothing but
/// `treiber_pop` against one word. A single Treiber head is O(1) per
/// operation and O(N) in contention: every successful CAS invalidates every
/// other core's copy of the line. P0.2 removed the O(capacity) discovery scan
/// and left an O(cores) serialization point in its place.
///
/// Sharding each stack [`SHARDS`] ways spreads that serialization over
/// independent lines. Submitters and completers use a per-thread round-robin
/// cursor; workers pop their home shard and steal. Single-membership and the
/// per-slot CAS arbitration (ADR-0009) are untouched — a shard is just *which*
/// head a node rides, so [`claim_pop`] / [`publish_queued`] and their loom
/// models are unchanged and simply run against one shard's head.
#[repr(C)]
pub struct TaskQueueHeader {
    /// Must equal [`TASK_MAGIC`].
    pub magic: u64,
    /// Slot count; always a power of two.
    pub capacity: u32,
    /// `capacity - 1`; kept as recorded geometry (the round-robin cursor that
    /// used to mask through it was retired by the FREE/READY stacks).
    pub mask: u32,
    /// Count of `CANCELLED` slots still riding READY nodes (incremented by the
    /// winning `QUEUED→CANCELLED` cancel, decremented by the exactly-once
    /// transfer of that node — a [`claim_pop`] discard or the queue-full
    /// drain's reserve). A **hint** that lets the queue-full path skip the
    /// READY drain entirely when there is nothing to recover, keeping a
    /// backpressured submit-retry loop O(1).
    pub cancelled: ShmU32,
    /// Reserved padding (zero).
    pub reserved2: u32,
    /// Number of workers currently parked awaiting work (a submit rings the work
    /// doorbell iff `> 0`).
    pub waiters_work: AtomicU32,
    /// Number of requesters currently parked awaiting a terminal outcome (a
    /// worker rings the done doorbell iff `> 0`).
    pub waiters_done: AtomicU32,
    /// Retry cap consulted by [`TaskQueue::reap`].
    pub max_retries: u32,
    /// The futex doorbell word (reserved by ADR-0003, activated by ADR-0011 on
    /// Linux via [`TaskQueue::doorbell_word`]). `0` and untouched elsewhere.
    pub doorbell_seq: AtomicU32,
    /// Treiber head (`{tag:32 | idx:32}`, [`pack_stack_head`]) of the **lease
    /// side table's** free list (ADR-0010, P0.3): [`LeaseSlot`] records
    /// available to arm a task-lifecycle-tied retained-ref binding. Touched
    /// only by the cold binding paths ([`TaskQueue::submit_with_binding`],
    /// [`ClaimedTask::bind_output`], [`TaskQueue::ack`],
    /// [`TaskQueue::reap_bindings`]) — never by claim/poll/wait.
    pub lease_free_head: ShmU64,
    /// Pads line 0 to the cache-line boundary. Zero.
    pub _pad0: [u8; 16],
    /// The **FREE** stack, sharded [`SHARDS`] ways — one Treiber head
    /// (`{tag:32 | idx:32}`, [`pack_stack_head`]) per cache line. A slot lives
    /// on exactly one shard at a time; which one is a locality hint, never a
    /// correctness property (every pop sweeps all shards before reporting
    /// empty). `idx ==` [`STACK_NIL`] when a shard is empty. [`ShmU64`] so the
    /// pop/push loops are the loom-checked ones (ADR-0004 stage L).
    pub free_heads: [PaddedHead; SHARDS],
    /// The **READY** stack, sharded the same way: `QUEUED` slots awaiting a
    /// claim. A worker pops its home shard (`worker_id % SHARDS`) and steals
    /// round-robin from the rest on empty.
    pub ready_heads: [PaddedHead; SHARDS],
}

/// How many live READY nodes the queue-full drain may hold aside per shard
/// while digging for a `CANCELLED` one (ADR-0012 addendum). Bounds the
/// submitter-crash window to this many stranded nodes instead of `capacity`.
pub const DRAIN_DIG: usize = 4;

/// Number of shards each of the FREE and READY stacks is split into. Eight
/// covers the core counts this substrate targets without making an
/// empty-claim sweep (one Acquire load per shard) noticeable; it is a frozen
/// ABI constant, not a tunable.
pub const SHARDS: usize = 8;

/// One Treiber head padded to a 64-byte stride. Not `align(64)`: the header's
/// alignment is part of the ABI (8), and callers hand `init` 8-aligned regions
/// in tests; real segments are page-aligned, so there each head does land on
/// its own line — which is where it matters.
#[repr(C)]
pub struct PaddedHead {
    /// The `{tag:32 | idx:32}` head word.
    pub head: ShmU64,
    _pad: [u8; 56],
}

impl PaddedHead {
    fn new(idx: u32) -> PaddedHead {
        PaddedHead {
            head: ShmU64::new(pack_stack_head(idx, 0)),
            _pad: [0; 56],
        }
    }
}

// 1088-byte frozen ABI: one control line + 8 FREE lines + 8 READY lines
// (`SHMTASK4`; one 64-byte line at `SHMTASK3`). Gated on `not(loom)` like
// `TaskSlot`'s asserts: the stack heads are `#[repr(transparent)]` substrate
// newtypes whose loom twin is fat.
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<PaddedHead>() == 64);
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<TaskQueueHeader>() == 64 + 16 * 64);
#[cfg(not(loom))]
const _: () = assert!(core::mem::offset_of!(TaskQueueHeader, cancelled) == 16);
#[cfg(not(loom))]
const _: () = assert!(core::mem::offset_of!(TaskQueueHeader, waiters_work) == 24);
#[cfg(not(loom))]
const _: () = assert!(core::mem::offset_of!(TaskQueueHeader, doorbell_seq) == 36);
#[cfg(not(loom))]
const _: () = assert!(core::mem::offset_of!(TaskQueueHeader, lease_free_head) == 40);
#[cfg(not(loom))]
const _: () = assert!(core::mem::offset_of!(TaskQueueHeader, free_heads) == 64);
#[cfg(not(loom))]
const _: () = assert!(core::mem::offset_of!(TaskQueueHeader, ready_heads) == 64 + 8 * 64);

thread_local! {
    /// Per-thread round-robin shard cursor for submit / complete. Seeded from
    /// the cell's own address so threads start on different shards; wraps
    /// freely. Process-local by construction — nothing about it is ABI.
    static SHARD_CURSOR: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

/// The next shard for this thread's submit/complete traffic.
#[inline]
fn next_shard() -> usize {
    SHARD_CURSOR.with(|c| {
        let mut v = c.get();
        if v == 0 {
            // First use on this thread: seed from the cell's address.
            v = ((c as *const _ as usize) >> 6) as u32 | 1;
        }
        c.set(v.wrapping_add(1));
        v as usize % SHARDS
    })
}
#[cfg(not(loom))]
const _: () = assert!(core::mem::align_of::<TaskQueueHeader>() == 8);

/// One task slot: the CAS state machine plus request/result descriptors.
///
/// # Layout (frozen ABI — 128 bytes = two cache lines, 8-aligned; `SHMTASK4`, ADR-0012)
///
/// | field      | type        | meaning                                        |
/// |------------|-------------|------------------------------------------------|
/// | `state`    | `AtomicU32` | `EMPTY`/`QUEUED`/`CLAIMED`/`DONE`/`FAILED`/`CANCELLED` |
/// | `owner`    | `AtomicU32` | claiming worker id (`0` = none)                |
/// | `seq`      | `AtomicU64` | incarnation id; correlation id of the handle    |
/// | `deadline` | `AtomicU64` | lease deadline (nanos); reap requeues if passed |
/// | `retry`    | `AtomicU32` | reap requeue count (capped by `max_retries`)    |
/// | `cancel`   | `AtomicU32` | cooperative cancel flag (`0` = live)            |
/// | `next`     | `AtomicU32` | intrusive FREE/READY stack link ([`STACK_NIL`]) |
/// | `reserved` | `u32`       | reserved padding (zero)                         |
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
    /// Intrusive Treiber-stack link: the next slot below this one on the FREE or
    /// READY stack ([`STACK_NIL`] terminates). Written only by the exclusive
    /// holder of the slot's stack node, immediately before the head CAS that
    /// publishes it (single-membership discipline: a slot is on at most one
    /// stack, so one link suffices). Meaningless while the slot is held.
    pub next: ShmU32,
    /// Reserved padding (keeps the descriptors 8-aligned); always zero.
    pub reserved: u32,
    /// The submitted request descriptor (guarded by `state`).
    pub request: ChunkDesc,
    /// The worker's result descriptor, valid once `state == DONE`.
    pub result: ChunkDesc,
    /// Pads the slot from 88 to 128 bytes — two whole cache lines — so
    /// neighbouring slots never share a line (`SHMTASK4`, ADR-0012). At 88
    /// bytes, slots `i` and `i+1` straddled a line, and two workers on
    /// different READY shards claiming adjacent slots false-shared on every
    /// `state`/`owner` write. Always zero.
    pub _pad: [u8; 40],
}

// 128-byte frozen ABI (88 through `SHMTASK3`). Gated on `not(loom)`: the atomic fields are
// `#[repr(transparent)]` substrate newtypes (byte-identical to the bare atomics in
// production, loom's fat twin under `--cfg loom`); the loom build reconstructs the
// claim state machine in ordinary memory and never overlays these bytes on shm.
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<TaskSlot>() == 128);
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

    /// **Pre-claim cancel election.** CAS `QUEUED → CANCELLED`; `true` iff this
    /// caller cancelled the task before any worker claimed it. This is the only
    /// exit from `QUEUED` besides [`try_claim`](Self::try_claim) — the fact
    /// [`claim_pop`] leans on when a popped READY node's claim CAS fails.
    /// Extracted (like the other elections) so [`TaskQueue::cancel`] and the
    /// loom models run the same CAS body.
    #[inline]
    pub fn try_cancel_queued(&self) -> bool {
        self.state
            .compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

/// **The O(1) claim discovery loop** (ADR-0009), shared verbatim by the
/// production [`TaskQueue::claim`] path and the loom models: pop the READY
/// stack, revalidate with the exactly-once [`try_claim`](TaskSlot::try_claim)
/// CAS, and on a CAS failure — the slot is provably `CANCELLED`, the only other
/// exit from `QUEUED` (a resubmission would need a FREE pop this node has not
/// had) — transfer the node to the FREE stack and keep popping. Returns the
/// claimed slot index (its state is now `CLAIMED`), or `None` when READY is
/// empty. Amortized O(1): every discarded node is paid for by the cancel that
/// created it.
///
/// Each transfer decrements `cancelled` (the header's cancelled-riding count)
/// exactly once — the transferer holds the node exclusively, and `CANCELLED`
/// can only be exited by the node holder, so the count stays balanced against
/// the winning cancel's increment.
///
/// `slot_at(i)` resolves a slot index to its [`TaskSlot`]; in production that is
/// pointer arithmetic over the mapped region, in a loom harness an ordinary
/// in-memory slot array (the composition under test is identical).
pub fn claim_pop<'s>(
    ready_head: &ShmU64,
    free_head: &ShmU64,
    cancelled: &ShmU32,
    slot_at: impl Fn(u32) -> &'s TaskSlot,
) -> Option<u32> {
    loop {
        let idx = shm_core::pool::treiber_pop(ready_head, |i| {
            slot_at(i).next.load(Ordering::Acquire)
        })?;
        let slot = slot_at(idx);
        if slot.try_claim() {
            return Some(idx);
        }
        // The pop granted exclusive ownership of the node, so nothing else can
        // transition this slot out of CANCELLED until we release it: safe to
        // assert, then transfer the node to FREE (the cancelled slot becomes
        // reusable capacity, exactly as it was under the scan).
        debug_assert_eq!(
            slot.state.load(Ordering::Acquire),
            CANCELLED,
            "READY node failed try_claim in a non-CANCELLED state"
        );
        shm_core::pool::treiber_push(free_head, idx, |i, next| {
            slot_at(i).next.store(next, Ordering::Release);
        });
        cancelled.fetch_sub(1, Ordering::AcqRel);
    }
}

/// **The submit/requeue publish step** (ADR-0009), shared by the production
/// paths and the loom models: store `QUEUED` (Release — pairs with the
/// claimer's Acquire so the fields written under `RESERVED` are visible), then
/// push the slot onto the READY stack. **Push strictly after publish**: a
/// popper that found the node before the store would fail `try_claim` on
/// `RESERVED`, conclude "cancelled", and transfer the node to FREE — losing the
/// task. The caller must hold the slot exclusively in `RESERVED`.
pub fn publish_queued<'s>(ready_head: &ShmU64, slot_at: impl Fn(u32) -> &'s TaskSlot, idx: u32) {
    slot_at(idx).state.store(QUEUED, Ordering::Release);
    shm_core::pool::treiber_push(ready_head, idx, |i, next| {
        slot_at(i).next.store(next, Ordering::Release);
    });
}

// ---- The lease side table (ADR-0010, P0.3 / ADR-0005 G12) ----

/// Lease-record state (low 32 bits of [`LeaseSlot::word`]): the record is free
/// (on the lease free list, or exclusively held by an armer mid-write).
pub const LEASE_NONE: u32 = 0;
/// Lease-record state: the record holds a live task-tied retained-ref binding.
pub const LEASE_ARMED: u32 = 1;
/// Lease-record state (transient): a releaser won the exactly-once
/// `ARMED → RELEASED` CAS and is extracting the binding before retiring the
/// record back to the free list.
pub const LEASE_RELEASED: u32 = 2;
/// Lease-record state: the requester acknowledged the task's outcome and
/// handed the binding to the **coordinator** to release (ADR-0010 addendum).
/// The record stays in the table until the coordinator's `reap_bindings`
/// wins it — with zero grace and no liveness check — so a requester that dies
/// after acking leaks nothing, and no actor ever needs the store's raw
/// segments to release a pin.
pub const LEASE_ACKED: u32 = 3;

/// Pack a [`LeaseSlot::word`]: `{gen:32 | state:32}`. `gen` advances on every
/// arm, which is what makes the release CAS ABA-safe: a releaser that observed
/// `{g, ARMED}` can never win against a record that was meanwhile released,
/// retired, and re-armed for a different task (now `{g+1, ARMED}`).
#[inline]
pub const fn pack_lease_word(gen: u32, state: u32) -> u64 {
    ((gen as u64) << 32) | (state as u64)
}

/// The generation half of a packed lease word.
#[inline]
pub const fn lease_word_gen(word: u64) -> u32 {
    (word >> 32) as u32
}

/// The state half of a packed lease word.
#[inline]
pub const fn lease_word_state(word: u64) -> u32 {
    word as u32
}

/// One task-lifecycle-tied retained-ref binding (ADR-0010, P0.3): the opaque
/// `{artifact_id, incarnation, version}` triple naming a **retained pin** on a
/// keyed-store entry's version. `shm-task` never interprets these words — it
/// only guarantees the exactly-once handoff from "armed against task
/// `{slot_idx, seq}`" to "returned to exactly one releaser" (the requester's
/// [`TaskQueue::ack`], or the coordinator's [`TaskQueue::reap_bindings`]
/// backstop). The releaser routes the triple back to the entry
/// (`attach_at_incarnation` + `release_leaked_pin` — the item-J crash route),
/// where a stale incarnation is dropped rather than touching the slot's next
/// occupant: the lease dies with the **entry** and with the **task**, never
/// with the actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseBinding {
    /// The keyed-store lineage id (routes to a catalog slot).
    pub artifact_id: u32,
    /// The entry occupant the retained pin was taken against (ADR-0008 P0.1).
    pub incarnation: u32,
    /// The pinned version.
    pub version: u64,
}

/// One record of the lease side table: a retained-ref binding tied to one task
/// incarnation `{slot_idx, seq}`.
///
/// # Layout (frozen ABI — 48 bytes, 8-aligned; `SHMTASK3`, ADR-0010)
///
/// | field         | type     | meaning                                        |
/// |---------------|----------|------------------------------------------------|
/// | `word`        | `ShmU64` | `{gen:32 \| state:32}` ([`pack_lease_word`])    |
/// | `version`     | `ShmU64` | binding: pinned version                         |
/// | `seq`         | `ShmU64` | the task incarnation this binding is tied to    |
/// | `deadline`    | `ShmU64` | the task's submit deadline (reap grace anchor)  |
/// | `artifact_id` | `ShmU32` | binding: keyed-store lineage id                 |
/// | `incarnation` | `ShmU32` | binding: entry occupant (ADR-0008)              |
/// | `slot_idx`    | `ShmU32` | the task slot this binding is tied to           |
/// | `next`        | `ShmU32` | intrusive lease-free-list link ([`STACK_NIL`])  |
///
/// The payload fields are written only by the **armer** while it holds the
/// record exclusively (popped off the lease free list, state `NONE`), before
/// the `Release` store of the gen-bumped `ARMED` word — so any reader that
/// `Acquire`-loads `{g, ARMED}` sees generation `g`'s fields, and the
/// exactly-once `ARMED → RELEASED` CAS ([`try_release`](Self::try_release))
/// hands the stable fields to a single releaser.
#[repr(C)]
pub struct LeaseSlot {
    /// Packed `{gen, state}`; the record's entire concurrency protocol.
    pub word: ShmU64,
    /// Binding: the pinned version.
    pub version: ShmU64,
    /// The task-slot `seq` this binding is tied to (dies with the task).
    pub seq: ShmU64,
    /// The task's submit `deadline_nanos` — the anchor the reap backstop adds
    /// its grace period to.
    pub deadline: ShmU64,
    /// Binding: the keyed-store lineage id.
    pub artifact_id: ShmU32,
    /// Binding: the entry occupant the pin was taken against.
    pub incarnation: ShmU32,
    /// The task slot this binding is tied to.
    pub slot_idx: ShmU32,
    /// Intrusive lease-free-list link ([`STACK_NIL`] terminates).
    pub next: ShmU32,
}

// 48-byte frozen ABI, gated like `TaskSlot`'s asserts.
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<LeaseSlot>() == 48);
#[cfg(not(loom))]
const _: () = assert!(core::mem::align_of::<LeaseSlot>() == 8);

impl LeaseSlot {
    /// **Armer.** Write the binding fields, then publish the gen-bumped
    /// `ARMED` word (`Release` — pairs with every releaser's `Acquire` load).
    /// The caller must hold the record exclusively (a fresh lease-free-list
    /// pop; state `NONE`).
    pub fn arm(&self, binding: LeaseBinding, slot_idx: u32, seq: u64, deadline: u64) {
        debug_assert_eq!(
            lease_word_state(self.word.load(Ordering::Acquire)),
            LEASE_NONE,
            "arming a lease record that is not exclusively held"
        );
        self.version.store(binding.version, Ordering::Relaxed);
        self.seq.store(seq, Ordering::Relaxed);
        self.deadline.store(deadline, Ordering::Relaxed);
        self.artifact_id.store(binding.artifact_id, Ordering::Relaxed);
        self.incarnation.store(binding.incarnation, Ordering::Relaxed);
        self.slot_idx.store(slot_idx, Ordering::Relaxed);
        let gen = lease_word_gen(self.word.load(Ordering::Relaxed));
        self.word.store(
            pack_lease_word(gen.wrapping_add(1), LEASE_ARMED),
            Ordering::Release,
        );
    }

    /// **The exactly-once release election.** CAS the exact `{gen, ARMED}`
    /// word the caller observed to `{gen, RELEASED}`; `true` iff this caller
    /// is the single winner and now holds the record (and its stable fields)
    /// exclusively. The generation makes it ABA-safe: a record that was
    /// released, retired, and re-armed for a different task carries `gen + 1`,
    /// so a stale observer's CAS fails. Shared with the loom model
    /// (`tests/loom_task.rs`), which proves exactly-once and the stale-gen
    /// rejection.
    /// Requester half of the ack handoff: CAS `ARMED → ACKED` at the observed
    /// gen. `false` if the record moved (released by reap, or re-armed).
    #[inline]
    pub fn try_ack(&self, observed: u64) -> bool {
        debug_assert_eq!(lease_word_state(observed), LEASE_ARMED);
        self.word
            .compare_exchange(
                observed,
                pack_lease_word(lease_word_gen(observed), LEASE_ACKED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Releaser half: CAS `ARMED | ACKED → RELEASED` at the observed gen. The
    /// exactly-once arbitration between a reap and any other releaser.
    #[inline]
    pub fn try_release(&self, observed: u64) -> bool {
        debug_assert!(matches!(
            lease_word_state(observed),
            LEASE_ARMED | LEASE_ACKED
        ));
        self.word
            .compare_exchange(
                observed,
                pack_lease_word(lease_word_gen(observed), LEASE_RELEASED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// **Winner.** Return the record to `NONE` (keeping its generation) so the
    /// caller can push it back onto the lease free list. Only the
    /// [`try_release`](Self::try_release) winner may call this.
    #[inline]
    pub fn retire(&self) {
        let w = self.word.load(Ordering::Relaxed);
        debug_assert_eq!(lease_word_state(w), LEASE_RELEASED);
        self.word
            .store(pack_lease_word(lease_word_gen(w), LEASE_NONE), Ordering::Release);
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

/// Byte offset of the lease side table within a region (64-byte aligned,
/// appended after the slot array; ADR-0010).
#[inline]
pub const fn lease_table_offset(capacity: u32) -> usize {
    align_up(
        slots_offset() + capacity as usize * core::mem::size_of::<TaskSlot>(),
        64,
    )
}

/// Number of lease records a queue of `capacity` slots carries: two per slot
/// (one input + one output binding for every concurrently-live task).
#[inline]
pub const fn lease_capacity(capacity: u32) -> u32 {
    capacity.saturating_mul(2)
}

/// Total bytes a task queue of `capacity` slots needs (header + slot array +
/// lease side table).
#[inline]
pub const fn required_bytes(capacity: u32) -> usize {
    lease_table_offset(capacity)
        + lease_capacity(capacity) as usize * core::mem::size_of::<LeaseSlot>()
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
    /// The lease side table (ADR-0010; `lease_capacity(capacity)` records).
    leases: *mut LeaseSlot,
    capacity: usize,
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
            return Err(Error::RegionTooSmall {
                need,
                have: region_len,
            });
        }

        // SAFETY: `base` is 8-aligned and `region_len >= need`, so the header and
        // every slot offset below stays within the writable region.
        unsafe {
            let header = base.cast::<TaskQueueHeader>();
            header.write(TaskQueueHeader {
                magic: TASK_MAGIC,
                capacity,
                mask: capacity - 1,
                // Every slot starts on the FREE stack, linked 0 → 1 → … → NIL
                // (the links are seeded below); READY starts empty.
                cancelled: ShmU32::new(0),
                reserved2: 0,
                waiters_work: AtomicU32::new(0),
                waiters_done: AtomicU32::new(0),
                max_retries,
                doorbell_seq: AtomicU32::new(0),
                // Every lease record starts on the lease free list, linked
                // 0 → 1 → … → NIL (links seeded below).
                lease_free_head: ShmU64::new(pack_stack_head(0, 0)),
                _pad0: [0; 16],
                // Every slot starts on FREE shard 0 as one chain (links seeded
                // below); the other FREE shards and every READY shard start
                // empty. Distribution across shards happens naturally as slots
                // complete and are pushed back by their completing thread's
                // cursor.
                free_heads: core::array::from_fn(|i| {
                    PaddedHead::new(if i == 0 { 0 } else { STACK_NIL })
                }),
                ready_heads: core::array::from_fn(|_| PaddedHead::new(STACK_NIL)),
            });
            let slots = base.add(slots_offset()).cast::<TaskSlot>();
            for i in 0..capacity as usize {
                let next = if i + 1 < capacity as usize {
                    (i + 1) as u32
                } else {
                    STACK_NIL
                };
                slots.add(i).write(TaskSlot {
                    state: ShmU32::new(EMPTY),
                    owner: ShmU32::new(OWNER_NONE),
                    seq: ShmU64::new(0),
                    deadline: ShmU64::new(0),
                    retry: ShmU32::new(0),
                    cancel: ShmU32::new(0),
                    next: ShmU32::new(next),
                    reserved: 0,
                    request: ChunkDesc::ZERO,
                    result: ChunkDesc::ZERO,
                    _pad: [0; 40],
                });
            }
            let leases = base.add(lease_table_offset(capacity)).cast::<LeaseSlot>();
            let lease_cap = lease_capacity(capacity) as usize;
            for i in 0..lease_cap {
                let next = if i + 1 < lease_cap {
                    (i + 1) as u32
                } else {
                    STACK_NIL
                };
                leases.add(i).write(LeaseSlot {
                    word: ShmU64::new(pack_lease_word(0, LEASE_NONE)),
                    version: ShmU64::new(0),
                    seq: ShmU64::new(0),
                    deadline: ShmU64::new(0),
                    artifact_id: ShmU32::new(0),
                    incarnation: ShmU32::new(0),
                    slot_idx: ShmU32::new(0),
                    next: ShmU32::new(next),
                });
            }
            Ok(TaskQueue {
                header,
                slots,
                leases,
                capacity: capacity as usize,
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
        let (magic, capacity) = unsafe {
            let h = &*header;
            (h.magic, h.capacity)
        };
        if magic != TASK_MAGIC {
            return Err(Error::BadMagic);
        }
        let need = required_bytes(capacity);
        if region_len < need {
            return Err(Error::RegionTooSmall {
                need,
                have: region_len,
            });
        }
        // SAFETY: `capacity` was validated at init; the slot array and the
        // lease side table are in-bounds (`required_bytes` covers both).
        let slots = unsafe { base.add(slots_offset()).cast::<TaskSlot>() };
        // SAFETY: as above.
        let leases = unsafe { base.add(lease_table_offset(capacity)).cast::<LeaseSlot>() };
        Ok(TaskQueue {
            header,
            slots,
            leases,
            capacity: capacity as usize,
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

    /// The queue's ABI-reserved futex doorbell word
    /// ([`TaskQueueHeader::doorbell_seq`], reserved by ADR-0003, activated by
    /// ADR-0011's Linux fast paths) — the twin of
    /// [`Ring::doorbell_word`](shm_ring::Ring::doorbell_word), for wiring a
    /// `FutexNotifier`/`FutexParker` pair as this queue's work or done wake
    /// hook. Available on every platform (the word is plain shared memory);
    /// only the futex hooks are Linux-gated. Borrows the mapped header, so it
    /// lives as long as this handle.
    pub fn doorbell_word(&self) -> &AtomicU32 {
        &self.header().doorbell_seq
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

    // ---- FREE/READY index stacks (ADR-0009) ----

    /// Pop a reusable slot off the FREE stack — the exclusive claim of that
    /// slot (the P0.1 catalog pattern: the pop *is* the reservation, so no CAS
    /// on `state` is needed to fence out rival submitters).
    ///
    /// Tries this thread's cursor shard first, then sweeps the rest: empty is
    /// only reported once every shard has been seen empty.
    #[inline]
    fn pop_free(&self) -> Option<u32> {
        let header = self.header();
        let home = next_shard();
        for k in 0..SHARDS {
            let s = (home + k) % SHARDS;
            if let Some(idx) = shm_core::pool::treiber_pop(&header.free_heads[s].head, |i| {
                self.slot(i as usize).next.load(Ordering::Acquire)
            }) {
                return Some(idx);
            }
        }
        None
    }

    /// Push a slot the caller exclusively holds onto this thread's cursor FREE
    /// shard.
    #[inline]
    fn push_free(&self, idx: u32) {
        let s = next_shard();
        shm_core::pool::treiber_push(&self.header().free_heads[s].head, idx, |i, next| {
            self.slot(i as usize).next.store(next, Ordering::Release);
        });
    }

    // ---- Lease side table (ADR-0010, P0.3) ----

    /// The number of lease records this queue's side table holds.
    #[inline]
    pub fn lease_table_capacity(&self) -> usize {
        lease_capacity(self.capacity as u32) as usize
    }

    #[inline]
    fn lease(&self, idx: usize) -> &LeaseSlot {
        debug_assert!(idx < self.lease_table_capacity());
        // SAFETY: `idx < lease_capacity`; the table has that many records.
        unsafe { &*self.leases.add(idx) }
    }

    /// Pop a free lease record — the exclusive claim of that record.
    #[inline]
    fn pop_lease(&self) -> Option<u32> {
        shm_core::pool::treiber_pop(&self.header().lease_free_head, |i| {
            self.lease(i as usize).next.load(Ordering::Acquire)
        })
    }

    /// Push a lease record the caller exclusively holds back onto the free list.
    #[inline]
    fn push_lease(&self, idx: u32) {
        shm_core::pool::treiber_push(&self.header().lease_free_head, idx, |i, next| {
            self.lease(i as usize).next.store(next, Ordering::Release);
        });
    }

    /// Win the exactly-once release of lease record `i` (observed as `word`),
    /// extract its binding, retire the record to the free list, and return the
    /// binding. `None` iff another releaser won first (or the record moved on).
    fn release_lease_record(&self, i: usize, word: u64) -> Option<LeaseBinding> {
        let rec = self.lease(i);
        if !rec.try_release(word) {
            return None;
        }
        // The CAS win hands us the record exclusively; the fields are the
        // arming generation's, stable until we retire.
        let binding = LeaseBinding {
            artifact_id: rec.artifact_id.load(Ordering::Acquire),
            incarnation: rec.incarnation.load(Ordering::Acquire),
            version: rec.version.load(Ordering::Acquire),
        };
        rec.retire();
        self.push_lease(i as u32);
        Some(binding)
    }

    // ---- Submitter API ----

    /// Enqueue a task and return its correlation [`TaskHandle`].
    ///
    /// Pops a reusable slot (an `EMPTY` or terminal `DONE`/`FAILED`/`CANCELLED`
    /// slot) off the FREE stack — O(1); the pop *is* the exclusive reservation —
    /// bumps and publishes a fresh `seq`, writes the `request` and `deadline`,
    /// clears `cancel`/`retry`, marks it `QUEUED`, pushes it onto the READY
    /// stack, and rings the work doorbell iff idle workers are parked. Returns
    /// [`Error::QueueFull`] if no reusable slot exists (every slot holds a live
    /// `QUEUED`/`CLAIMED` task); a slot cancelled while `QUEUED` still riding
    /// its READY node is recovered by a bounded READY drain first
    /// ([`reserve_cancelled_ready`](Self::reserve_cancelled_ready)), so
    /// cancel-heavy workloads keep the pre-ADR-0009 full/not-full behavior.
    ///
    /// `deadline_nanos` is an absolute deadline in the same clock domain
    /// [`TaskQueue::reap`] compares against (see [`crate::now_nanos`]); a
    /// `CLAIMED` task past it is presumed dead and requeued.
    pub fn submit(&self, request: ChunkDesc, deadline_nanos: u64) -> Result<TaskHandle> {
        self.submit_inner(request, deadline_nanos, None)
    }

    /// **P0.3 (ADR-0010, G12).** Enqueue a task whose retained **input** ref is
    /// tied to the task's lifecycle: `input` (a retained pin the submitter took
    /// via the keyed store, e.g. `Entry::retain_current`) is armed into the
    /// lease side table against this task's `{slot_idx, seq}` **before** the
    /// task becomes claimable, so the input stays pinned for as long as the
    /// task lives — surviving the submitter's death — and is released
    /// exactly-once at requester [`ack`](TaskQueue::ack) or by the
    /// coordinator's [`reap_bindings`](TaskQueue::reap_bindings) backstop.
    /// Fails [`Error::LeaseTableFull`] (backpressure: unacked bindings hold
    /// records) or [`Error::QueueFull`] without arming anything.
    pub fn submit_with_binding(
        &self,
        request: ChunkDesc,
        deadline_nanos: u64,
        input: LeaseBinding,
    ) -> Result<TaskHandle> {
        self.submit_inner(request, deadline_nanos, Some(input))
    }

    /// The shared submit body ([`submit`] / [`submit_with_binding`]).
    fn submit_inner(
        &self,
        request: ChunkDesc,
        deadline_nanos: u64,
        input: Option<LeaseBinding>,
    ) -> Result<TaskHandle> {
        // Reserve the lease record first (it is the scarcer resource under
        // unacked-binding backpressure): the pop is the exclusive claim, and a
        // failed slot reservation below just pushes it back untouched.
        let lease_idx = match input {
            Some(_) => Some(self.pop_lease().ok_or(Error::LeaseTableFull)?),
            None => None,
        };
        let reserved = match self.reserve_submit_slot() {
            Some(idx) => idx,
            None => {
                if let Some(li) = lease_idx {
                    self.push_lease(li);
                }
                return Err(Error::QueueFull);
            }
        };
        let idx = reserved;
        let slot = self.slot(idx as usize);

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
            let p = self.slot_ptr(idx as usize);
            core::ptr::addr_of_mut!((*p).request).write(request);
            core::ptr::addr_of_mut!((*p).result).write(ChunkDesc::ZERO);
        }
        // Arm the input binding against this task incarnation BEFORE the task
        // becomes claimable, so no worker can ever observe the task without
        // its lifecycle-tied input lease (ADR-0010). We hold the record from
        // the pop above.
        if let (Some(binding), Some(li)) = (input, lease_idx) {
            self.lease(li as usize)
                .arm(binding, idx, new_seq, deadline_nanos);
        }
        // Publish (`QUEUED`, Release — pairs with the claimer's Acquire), then
        // push onto READY. Strictly in that order: see [`publish_queued`].
        let s = next_shard();
        publish_queued(&self.header().ready_heads[s].head, |i| self.slot(i as usize), idx);

        // Dekker against `claim_blocking`'s register-then-sweep: the house rule
        // (`substrate::fence` docs, and the pin handshake) is an explicit
        // `SeqCst` fence between the publishing write and the waiter check.
        // Sound on x86/AArch64 without it; required for the C11 model and any
        // loom model of this path.
        shm_core::substrate::fence(Ordering::SeqCst);
        if self.header().waiters_work.load(Ordering::Acquire) > 0 {
            self.work.notify();
        }
        Ok(TaskHandle {
            slot_idx: idx,
            seq: new_seq,
        })
    }

    /// Exclusively reserve a reusable slot for a fresh submit: pop the FREE
    /// stack (the pop *is* the reservation), falling back to the queue-full
    /// cancelled-READY recovery. Returns the slot index in `RESERVED`, or
    /// `None` when the queue is genuinely full of live tasks.
    fn reserve_submit_slot(&self) -> Option<u32> {
        match self.pop_free() {
            Some(idx) => {
                let slot = self.slot(idx as usize);
                debug_assert!(
                    matches!(
                        slot.state.load(Ordering::Acquire),
                        EMPTY | DONE | FAILED | CANCELLED
                    ),
                    "FREE stack held a live slot"
                );
                // The pop granted exclusivity; no CAS needed to fence rivals.
                slot.state.store(RESERVED, Ordering::Release);
                Some(idx)
            }
            None => self.reserve_cancelled_ready(),
        }
    }

    /// The queue-full edge fallback (ADR-0009): with FREE empty, a `CANCELLED`
    /// slot still riding its READY node is the one kind of reusable capacity
    /// the FREE stack cannot see — and fresh `QUEUED` nodes may sit on top of
    /// it. Gated on the header's `cancelled` count: zero means nothing to
    /// recover, so a genuinely-full queue answers `QueueFull` in O(1) (a
    /// backpressured submit-retry loop must not churn the READY stack).
    /// Otherwise pop READY, holding popped `QUEUED` nodes aside, until a
    /// `CANCELLED` slot turns up (`CANCELLED → RESERVED`; we hold its node, so
    /// nothing else can transition it — and we pay down the count) or a
    /// `capacity` pop budget is spent; then push every held `QUEUED` node back
    /// and ring the work doorbell (closing the transient no-node window the
    /// drain opened for a racing
    /// [`claim_blocking`](TaskQueue::claim_blocking) parker). `None` means the
    /// queue is genuinely full of live tasks.
    ///
    /// The drain is bounded by `capacity` — but it runs **only** on the
    /// FREE-empty edge with cancels actually outstanding, where v1's submit
    /// scanned O(capacity) too and the caller's next move is backpressure;
    /// the hot submit path never enters it. Claiming stays scan-free. The
    /// count is a hint with the same tiny race v1's scan had: a cancel whose
    /// CAS lands after the zero-check may see one spurious `QueueFull`.
    fn reserve_cancelled_ready(&self) -> Option<u32> {
        let header = self.header();
        let mut found = None;
        // **Bounded hold-aside.** An earlier cut dug through up to `capacity`
        // live QUEUED nodes per shard, holding them in a process-local `Vec`;
        // a submitter killed mid-dig stranded every one — other actors' tasks,
        // off every stack forever. Digging only the top of each shard closed
        // that window but could not reach a CANCELLED node with even one live
        // node re-published on top of it, which breaks the recovery guarantee
        // for small queues. The compromise is a **constant** dig depth: the
        // crash window is at most `DRAIN_DIG` nodes per submitter (not
        // `capacity`), and a CANCELLED node buried deeper than that is left
        // for the next `claim_pop` discard to transfer.
        'shards: for s in 0..SHARDS {
            // Nothing (left) to recover: a genuinely full queue bails on the
            // first shard, keeping a backpressured submit-retry loop O(1). A
            // count above `capacity` is a transient underflow and means the
            // same thing.
            let c = header.cancelled.load(Ordering::Acquire);
            if c == 0 || c as usize > self.capacity {
                break;
            }
            let ready_head = &header.ready_heads[s].head;
            let mut held: [u32; DRAIN_DIG] = [STACK_NIL; DRAIN_DIG];
            let mut n_held = 0usize;
            for _ in 0..DRAIN_DIG {
                let Some(idx) = shm_core::pool::treiber_pop(ready_head, |i| {
                    self.slot(i as usize).next.load(Ordering::Acquire)
                }) else {
                    break;
                };
                let slot = self.slot(idx as usize);
                if slot
                    .state
                    .compare_exchange(CANCELLED, RESERVED, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    header.cancelled.fetch_sub(1, Ordering::AcqRel);
                    found = Some(idx);
                    break;
                }
                // Live QUEUED: hold it (we own its node, so single membership
                // holds) and dig one deeper.
                held[n_held] = idx;
                n_held += 1;
            }
            // Push held nodes back in reverse pop order, restoring the shard's
            // original order beneath whatever was pushed meanwhile.
            for k in (0..n_held).rev() {
                shm_core::pool::treiber_push(ready_head, held[k], |i, next| {
                    self.slot(i as usize).next.store(next, Ordering::Release);
                });
            }
            if found.is_some() {
                break 'shards;
            }
        }
        if found.is_none() {
            // A concurrent `claim_pop` may have transferred the very node we
            // were digging for to FREE while we swept; one more FREE pop turns
            // that spurious `QueueFull` into a submit. The pop grants
            // exclusivity, exactly as in the primary FREE path.
            found = self.pop_free().inspect(|&idx| {
                self.slot(idx as usize).state.store(RESERVED, Ordering::Release);
            });
        }
        // Re-ring the doorbell for any worker that raced the drain, saw READY
        // empty, and parked.
        if header.waiters_work.load(Ordering::Acquire) > 0 {
            self.work.notify();
        }
        found
    }

    // ---- Worker API ----

    /// Try to claim one queued task. Returns `None` if none is queued.
    ///
    /// Pops the READY stack — O(1), no scan — and CASes the popped slot
    /// `QUEUED→CLAIMED` ([`claim_pop`]). The CAS remains the single arbitration
    /// point: at most one worker transitions any given task, so claiming is
    /// **exactly-once** — a task in `CLAIMED` cannot be claimed by a second
    /// worker. `worker_id` should be nonzero (`0` means "no owner").
    ///
    /// The claim inherits the deadline the task was **submitted** with; use
    /// [`claim_with_lease`](TaskQueue::claim_with_lease) to (re)set a fresh lease
    /// deadline at claim time.
    pub fn claim(&self, worker_id: u32) -> Option<ClaimedTask<'_>> {
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
    pub fn claim_with_lease(&self, worker_id: u32, deadline_nanos: u64) -> Option<ClaimedTask<'_>> {
        self.claim_inner(worker_id, Some(deadline_nanos))
    }

    /// The shared claim body: pop READY / CAS `QUEUED→CLAIMED` ([`claim_pop`]),
    /// optionally stamping a fresh lease `deadline`.
    fn claim_inner(&self, worker_id: u32, deadline: Option<u64>) -> Option<ClaimedTask<'_>> {
        debug_assert_ne!(worker_id, OWNER_NONE, "worker id must be nonzero");
        let header = self.header();
        // Home shard first, then steal round-robin. Empty is reported only
        // once every shard has been seen empty; a push landing on a shard
        // already passed is caught by the caller's retry or the doorbell
        // re-check, exactly as a single-head empty was.
        // Steal order is a per-worker *stride*, not `home+1, home+2, …`: with
        // a shared sequential order, two workers that exhaust their homes
        // converge on the same next shard and serialize on its head (measured:
        // both workers at 12 M/s against 73 M/s alone). An odd stride coprime
        // with `SHARDS` visits every shard exactly once in a worker-specific
        // order, so neighbours diverge instead of colliding.
        let home = worker_id as usize % SHARDS;
        // Odd, so coprime with SHARDS (a power of two): the sweep visits every
        // shard exactly once. Derived from `worker_id % (SHARDS/2)` — NOT
        // `worker_id / SHARDS`, which is 0 for every id below 8 and made the
        // first cut of this fix inert for every worker the bench ran.
        let stride = 1 + 2 * (worker_id as usize % (SHARDS / 2));
        let mut popped = None;
        for k in 0..SHARDS {
            let s = (home + k * stride) % SHARDS;
            if let Some(i) = claim_pop(
                &header.ready_heads[s].head,
                &header.free_heads[s].head,
                &header.cancelled,
                |i| self.slot(i as usize),
            ) {
                popped = Some(i);
                break;
            }
        }
        let idx = popped? as usize;
        let slot = self.slot(idx);
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
        Some(ClaimedTask {
            queue: self,
            idx,
            seq,
            worker_id,
            request,
        })
    }

    /// Claim a task, parking on the work doorbell while none is queued.
    ///
    /// Registers as an idle worker (so submitters know to ring the doorbell),
    /// re-checks (closing the lost-wakeup window), then parks. Loops until a task
    /// is claimed. The bounded [`Parker`] timeout guarantees liveness even if a
    /// wakeup is missed.
    pub fn claim_blocking<P: Parker>(&self, worker_id: u32, parker: &P) -> ClaimedTask<'_> {
        loop {
            if let Some(task) = self.claim(worker_id) {
                return task;
            }
            self.header().waiters_work.fetch_add(1, Ordering::AcqRel);
            // Re-check after registering: a submit that raced our registration is
            // still observed here before we sleep. The fence is the worker half
            // of the Dekker pairing with `submit` (see there).
            shm_core::substrate::fence(Ordering::SeqCst);
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
    ) -> ClaimedTask<'_> {
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
        // Try the pre-claim cancel first. The CANCELLED slot keeps riding its
        // READY node; the next claim pop (or a queue-full submit fallback)
        // transfers it to the FREE stack (ADR-0009 single-membership rule).
        // The winning cancel is unique per QUEUED incarnation, so the
        // cancelled-riding count is balanced by that transfer's decrement.
        // Count BEFORE the CAS and uncount on failure: incrementing after a
        // win let a racing transfer's decrement land first and read the hint
        // as `u32::MAX` — "many" instead of "none".
        self.header().cancelled.fetch_add(1, Ordering::AcqRel);
        if slot.try_cancel_queued() {
            // A requester might be waiting on this handle.
            if self.header().waiters_done.load(Ordering::Acquire) > 0 {
                self.done.notify();
            }
            return Ok(());
        }
        // Lost the pre-claim cancel (already claimed, or gone): uncount.
        self.header().cancelled.fetch_sub(1, Ordering::AcqRel);
        // Otherwise, if it is (still) claimed, raise the cooperative flag. Re-
        // validate seq to avoid flagging a task that was reused after our load.
        if slot.state.load(Ordering::Acquire) == CLAIMED
            && slot.seq.load(Ordering::Acquire) == handle.seq
        {
            slot.cancel.store(1, Ordering::Release);
        }
        Ok(())
    }

    /// **P0.3 (ADR-0010, G12) — consume a terminal outcome and take its
    /// bindings.** The requester's half of "clear on ack": after observing a
    /// terminal [`Outcome`] (via [`wait`](TaskQueue::wait)/[`poll`](TaskQueue::poll)),
    /// `ack` wins the exactly-once `ARMED → RELEASED` election on every lease
    /// record tied to `{handle.slot_idx, handle.seq}` and returns the bindings
    /// **for the caller to release** against the keyed store
    /// (`shm-store::release_task_binding`) — `shm-task` never interprets the
    /// words. Idempotent: a second ack (or one racing the reap backstop)
    /// returns only the bindings this caller actually won, possibly none.
    ///
    /// Fails [`Error::NotTerminal`] while the task is still live (releasing a
    /// running task's input out from under it is exactly the bug this table
    /// exists to prevent). A slot already recycled for a newer task
    /// (`StaleHandle` from `poll`) is *ackable* — the outcome was consumed,
    /// the bindings are still tied to the old `seq` and still this caller's to
    /// release.
    pub fn ack(&self, handle: TaskHandle) -> Result<usize> {
        let idx = handle.slot_idx as usize;
        if idx >= self.capacity {
            return Err(Error::StaleHandle);
        }
        let slot = self.slot(idx);
        if slot.seq.load(Ordering::Acquire) == handle.seq {
            match slot.state.load(Ordering::Acquire) {
                DONE | FAILED | CANCELLED => {}
                _ => return Err(Error::NotTerminal),
            }
        }
        // The slot moved on (seq mismatch): the task is terminal by
        // construction — a reuse needs a FREE pop only a terminal transition
        // (or the cancelled-READY transfer) provides.
        // Hand each matching binding to the coordinator (`ARMED → ACKED`)
        // rather than returning it: an actor has no supported way to release a
        // pin against the store, so a returned binding leaked on the intended
        // path, and a requester dying between "ack" and "release" leaked one
        // with no record left to reap. `ACKED` records are released by the
        // next `reap_bindings` with zero grace. Returns how many were handed
        // over (0 on a repeat ack: idempotent).
        let mut acked = 0usize;
        for i in 0..self.lease_table_capacity() {
            let rec = self.lease(i);
            let w = rec.word.load(Ordering::Acquire);
            if lease_word_state(w) != LEASE_ARMED {
                continue;
            }
            if rec.slot_idx.load(Ordering::Acquire) != handle.slot_idx
                || rec.seq.load(Ordering::Acquire) != handle.seq
            {
                continue;
            }
            // The gen in `w` makes the CAS ABA-safe: if the record was
            // meanwhile released and re-armed for a different task, we fail
            // and skip it.
            if rec.try_ack(w) {
                acked += 1;
            }
        }
        Ok(acked)
    }

    /// **P0.3 (ADR-0010, G12) — the reap backstop.** Release the bindings of
    /// tasks whose requester never acked: an `ARMED` record is released when
    /// its task no longer needs it — the task slot's `seq` moved on (recycled)
    /// or sits in a state that can never run again (terminal, or a
    /// crashed-submitter `RESERVED` wedge) — **and** `now_nanos` is past the
    /// task's submit deadline plus `grace_nanos` (the requester's ack window;
    /// policy set by the coordinator, see ADR-0010). A live `QUEUED`/`CLAIMED`
    /// task is never touched, however late: its input is still needed.
    ///
    /// Returns the bindings this sweep won (exactly-once against a racing
    /// `ack`) for the caller — the coordinator — to release against the keyed
    /// store. O(lease-table) once per monitor tick; touches nothing on the
    /// claim/poll paths.
    pub fn reap_bindings(&self, now_nanos: u64, grace_nanos: u64) -> Vec<LeaseBinding> {
        let mut out = Vec::new();
        for i in 0..self.lease_table_capacity() {
            let rec = self.lease(i);
            let w = rec.word.load(Ordering::Acquire);
            match lease_word_state(w) {
                // Acked: the requester handed it over. Release now, no grace,
                // no liveness check — the task is terminal by `ack`'s gate.
                LEASE_ACKED => {
                    if let Some(binding) = self.release_lease_record(i, w) {
                        out.push(binding);
                    }
                    continue;
                }
                LEASE_ARMED => {}
                _ => continue,
            }
            if now_nanos <= rec.deadline.load(Ordering::Acquire).saturating_add(grace_nanos) {
                continue; // still inside the requester's ack window
            }
            let sidx = rec.slot_idx.load(Ordering::Acquire) as usize;
            if sidx < self.capacity {
                let slot = self.slot(sidx);
                if slot.seq.load(Ordering::Acquire) == rec.seq.load(Ordering::Acquire) {
                    match slot.state.load(Ordering::Acquire) {
                        // The task can still run (or is mid-publish): the
                        // binding must outlive it. A retried CLAIMED task's
                        // slot deadline was refreshed at claim, but the record
                        // anchors to the submit deadline — the state check is
                        // what protects the long-running retry here.
                        QUEUED | CLAIMED | COMPLETING => continue,
                        // Terminal, EMPTY, or a RESERVED submitter-crash wedge
                        // (a live submitter's mid-submit RESERVED is excluded
                        // by the fresh deadline above): releasable.
                        _ => {}
                    }
                }
            }
            if let Some(binding) = self.release_lease_record(i, w) {
                out.push(binding);
            }
        }
        out
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
                // Retry cap exhausted → terminal failure. Winning the CAS makes
                // this reaper the slot's unique holder, so it is the one party
                // that returns the slot to the FREE stack.
                if slot.try_fail() {
                    self.push_free(idx as u32);
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
                // Publish `QUEUED`, then push onto READY (strictly in that
                // order — see `publish_queued`): the requeued task is
                // rediscoverable in O(1) like any fresh submit.
                let s = idx % SHARDS;
                publish_queued(
                    &self.header().ready_heads[s].head,
                    |i| self.slot(i as usize),
                    idx as u32,
                );
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
pub struct ClaimedTask<'q> {
    /// A borrow, deliberately. Cloning the `TaskQueue` handle here — as this
    /// did through `SHMTASK3` — cloned its two `Arc<dyn Notifier>`s, which put
    /// two contended refcount `fetch_add`s on every claim and two `fetch_sub`s
    /// on every completion. Measured: pure-claim aggregate throughput fell
    /// 59 → 5 M/s from one worker to two. A claimed task cannot outlive the
    /// queue it was claimed from, so the borrow costs nothing and removes the
    /// only cores-wide serialization point on the claim path.
    queue: &'q TaskQueue,
    idx: usize,
    seq: u64,
    worker_id: u32,
    request: ChunkDesc,
}

impl ClaimedTask<'_> {
    /// The submitted request descriptor to operate on.
    #[inline]
    pub fn request(&self) -> ChunkDesc {
        self.request
    }

    /// The correlation id of this task (`{slot_idx, seq}`), for dedup/logging.
    #[inline]
    pub fn task_id(&self) -> TaskHandle {
        TaskHandle {
            slot_idx: self.idx as u32,
            seq: self.seq,
        }
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

    /// **P0.3 (ADR-0010, G12).** Tie a retained **output** ref to this task's
    /// lifecycle: the worker retains its output in the keyed store
    /// (`Entry::retain_current`), arms the binding here against the claimed
    /// `{slot_idx, seq}`, and then [`complete`](Self::complete)s — so the
    /// output stays pinned until the requester [`ack`](TaskQueue::ack)s (or
    /// the coordinator's [`reap_bindings`](TaskQueue::reap_bindings) backstop
    /// fires), even if this worker dies in between. Call before `complete`.
    /// A reaped-and-retried task simply arms one binding per attempt: each
    /// attempt took its own retained pin, and every armed record tied to the
    /// task is released at ack, so the counts stay balanced.
    pub fn bind_output(&self, binding: LeaseBinding) -> Result<()> {
        let li = self.queue.pop_lease().ok_or(Error::LeaseTableFull)?;
        let deadline = self.queue.slot(self.idx).deadline.load(Ordering::Acquire);
        self.queue
            .lease(li as usize)
            .arm(binding, self.idx as u32, self.seq, deadline);
        Ok(())
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
        // Terminal slots are reusable capacity: hand the node to the FREE stack
        // (we won `COMPLETING`, so we are the slot's unique holder). A late
        // `poll` of the result still races reuse exactly as before — the
        // seq-revalidation StaleHandle contract is unchanged.
        self.queue.push_free(self.idx as u32);
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
        // Winning the `CLAIMED→FAILED` CAS makes us the unique holder: return
        // the slot to the FREE stack (terminal slots are reusable capacity).
        self.queue.push_free(self.idx as u32);
        self.queue.notify_done_if_waiters();
        Ok(())
    }
}
