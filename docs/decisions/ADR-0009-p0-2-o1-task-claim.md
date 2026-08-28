# ADR-0009 — P0.2: O(1) task claim via FREE/READY index stacks

- Status: Accepted (implemented)
- Date: 2026-08-28
- Context: ADR-0008 P0.2. Under Holon every actor mailbox drains through
  `shm-task`'s claim path, and both `TaskQueue::claim` and `TaskQueue::submit`
  were O(capacity) scans over the slot array (ADR-0005 item S / gap G6) — a
  per-message tax that also concentrated CAS contention on low slot indices.

## Decision

Replace the two discovery scans with two intrusive, ABA-safe **Treiber stacks**
whose heads live in the queue header and whose links ride each slot:

- **FREE** (`TaskQueueHeader::free_head`) — slots a submit may reuse.
- **READY** (`TaskQueueHeader::ready_head`) — `QUEUED` slots awaiting a claim.

Both run the loom-checked `shm_core::pool::treiber_pop`/`treiber_push`
(`{tag:32 | idx:32}` head, tag bumped every push/pop — the exact primitive the
chunk pools use and P0.1's catalog reclamation already reuses), with the link in
the new `TaskSlot::next` word and `STACK_NIL = u32::MAX` terminating.

### What the old scan was for, and how each use is kept without it

The scan was pure **discovery** — *which slot should this operation touch* —
never arbitration. Arbitration always was, and remains, the per-slot CAS state
machine:

| old scan | replaced by | guarantee kept by |
|---|---|---|
| `claim_inner` scanning for a `QUEUED` slot | pop READY, then `try_claim` | the `QUEUED→CLAIMED` CAS stays the **sole exactly-once arbitration point**; the stack only nominates candidates |
| `submit` scanning for an `EMPTY`/terminal slot (round-robin `enqueue_head` cursor) | pop FREE | the pop **is** the exclusive reservation (P0.1 pattern), so the old `CAS state→RESERVED` fence against rival submitters is no longer needed; seq-bump-first StaleHandle discipline unchanged |
| `submit` reusing a `CANCELLED` slot in place | bounded READY drain on the FREE-empty edge (below) | `QueueFull` still means "no reusable capacity" |

`reap` remains an O(capacity) periodic sweep **by design** — it is
coordinator-periodic, not per-message; ADR-0005 item S and ADR-0008 P0.2 target
the claim path only.

### Single-membership discipline

The stacks are a discovery index only, kept sane by one rule: **a slot is on
exactly one stack, or held exclusively by exactly one party** (a submitter in
`RESERVED`, a worker in `CLAIMED`/`COMPLETING`, a terminal-transition winner
mid-push). One documented exception: a slot cancelled while `QUEUED`
(`try_cancel_queued`, the only exit from `QUEUED` besides `try_claim`) keeps
riding its READY node until a claim pop — or a queue-full submit drain —
transfers it to FREE. This is what makes the claim path's CAS-failure case
*provable*: a popped READY node whose `try_claim` fails can only be `CANCELLED`
(a resubmission would need a FREE pop the slot has not had), so the popper
transfers it to FREE and keeps popping. Amortized O(1): every discarded node is
paid for by the cancel that created it.

### The moving parts

- **submit** (`TaskQueue::submit`): pop FREE (→ `RESERVED` by plain store; the
  pop granted exclusivity) → seq-bump-first field writes, verbatim →
  `publish_queued`: store `QUEUED` (Release), **then** push READY → work
  doorbell iff waiters. Push strictly after publish: push-first would let a
  popper CAS-fail on `RESERVED`, conclude "cancelled", and transfer the node to
  FREE — losing the task (model-checked; see below).
- **queue-full edge** (`reserve_cancelled_ready`): FREE empty → if the
  header's `cancelled` count is zero, `QueueFull` immediately; otherwise pop
  READY, holding live `QUEUED` nodes aside, until a `CANCELLED` slot turns up
  (`CANCELLED→RESERVED`) or a `capacity` budget is spent; push held nodes back,
  re-ring the work doorbell (closes the transient no-node window for a racing
  `claim_blocking` parker), else `QueueFull`. **Two deviations from the
  approved design, both forced by measurement/tests**: (1) a single bounded pop
  is not enough — a fresh submit's `QUEUED` node buries earlier `CANCELLED`
  nodes, so a cancel-storm-with-repeated-submits workload would see `QueueFull`
  where v1 succeeded (`queue_full_with_only_cancelled_slots_recovers` caught
  it); the drain is bounded by `capacity`. (2) An unconditional drain made a
  backpressured submit-retry loop (spinning on `QueueFull` against a full
  256-slot queue) churn the whole READY stack per retry — the A/B showed it;
  hence `TaskQueueHeader::cancelled`, a count of cancelled-riding nodes:
  incremented by the winning `QUEUED→CANCELLED` cancel, decremented exactly
  once by that node's transfer (`claim_pop` discard or the drain's reserve —
  the transferer holds the node exclusively, so the count stays balanced). Zero
  → skip the drain: the genuinely-full retry path is O(1). The count is a hint
  with the same tiny race v1's scan had (a cancel winning its CAS after the
  check reads 0 → one spurious `QueueFull`); the increment is AcqRel and the
  check Acquire, so a cancel that happens-before the submit is always seen.
- **claim** (`claim_pop`, shared verbatim with the loom models): loop { pop
  READY → `None` ⇒ no work; `try_claim` ⇒ claimed; else `CANCELLED` ⇒ transfer
  node to FREE }. Tail (owner store, optional lease stamp, Acquire seq load,
  request read) unchanged; all four claim wrappers unchanged.
- **terminal transitions push FREE**: `complete` after `DONE`, `fail` after a
  winning `try_fail`, reap after a winning retry-cap `try_fail` — in each case
  the CAS winner is the slot's unique holder, hence the unique pusher.
  `cancel`'s `QUEUED→CANCELLED` touches no stack (it does not hold the node).
- **reap requeue**: sweep, deadline check, retry cap, both CAS elections
  byte-for-byte; the requeue publishes through the same `publish_queued`
  (store `QUEUED`, then push READY). `ReapReport` semantics unchanged.

## ABI

On-shm break, pre-authorized by ADR-0008's batched-break clause:

- `TASK_MAGIC` `b"SHMTASK1"` → `b"SHMTASK2"` — a stale region fails loudly with
  `Error::BadMagic`.
- `TaskQueueHeader` 40 → 56 bytes: the dead round-robin `enqueue_head`
  (`AtomicU64`) removed; `free_head`/`ready_head` (`ShmU64`) and the
  `cancelled` count (`ShmU32` + pad) added. `mask` kept as recorded geometry.
- `TaskSlot` 80 → 88 bytes: `next: ShmU32` (intrusive link) + `reserved: u32`
  pad added between `cancel` and `request`.
- `slots_offset()` stays 64 (`align_up(56, 64)`); a queue region grows by
  `capacity * 8` bytes. No other crate's ABI touched; all consumers size
  regions through `required_bytes`.

The header size asserts are now `#[cfg(not(loom))]`-gated like the slot's (the
stack heads are substrate newtypes whose loom twin is fat).

## Semantic deltas, stated

- **Claim order is LIFO** (stack order). This loses nothing: v1 guaranteed no
  order (claim scanned in index order from 0 while submit spread by cursor —
  arrival order was never honored). **Flag for Holon Phase 1**: if mailboxes
  need FIFO, swap the READY structure for a Vyukov-style seq-numbered MPMC
  index ring behind the same `publish_queued`/`claim_pop` seam — an isolated
  change; do not silently bake LIFO into mailbox semantics.
- **Reuse timing shifts** from cursor-spread to LIFO-immediate: a completed
  slot is the next one submitted into. The observable contract
  (StaleHandle-after-reuse, result readback racing reuse) is unchanged in kind
  and still covered by `stale_handle_after_slot_reuse`; the principled fix
  (explicit ack releasing the slot, ArrowRef clear-on-ack / G12) is P0.3 scope.
- **Transient discovery misses**: a `claim` may return `None` while another
  party transiently holds the only READY node (a racing claimer mid-CAS, or a
  queue-full drain). The same race existed under the scan (a slot mid-CAS was
  skipped); `claim_blocking` liveness is unaffected (bounded parker + the
  drain's doorbell re-ring).

## Crash window (accepted, same class as v1)

Enqueue is now two steps (store `QUEUED`, then push READY), so a submitter
dying between them leaves one `QUEUED` slot undiscoverable. This is the same
blast radius and class as the pre-existing accepted wedge of a submitter dying
in `RESERVED` mid-submit (reap never touches non-`CLAIMED` slots, so that slot
was already permanently lost in v1). A reap-side rescue was designed and
rejected: it cannot distinguish "on READY" from "crashed before push" without
racing concurrent reap callers into a duplicate push, which corrupts the
intrusive links. Proper crash-tied cleanup belongs to P0.3 (lifecycle-tied
leases) / a journaled submit.

## Proof

- **Loom** (`crates/shm-task/tests/loom_task.rs`, scenarios 3–5, driving the
  production `claim_pop`/`publish_queued`/`try_cancel_queued` over slots and
  heads in ordinary memory): dual-popper node exclusivity + exactly-once (54
  interleavings); claim-pop vs cancel — slot ends `CLAIMED` xor (`CANCELLED`,
  on FREE exactly once, and the `cancelled` count paid back to 0), never lost,
  never double-pushed (27); publish→push vs concurrent pop — no claim/discard
  of a `RESERVED` slot, task never lost (45). Bite-verified three ways:
  inverting the publish order fails scenario 5; leaking the cancelled node
  instead of transferring fails scenario 4; skipping the count decrement fails
  scenario 4.
- **Integration** (`crates/shm-task/tests/task.rs`): all 8 v1 tests unchanged
  and green (they encode every preserved property); new — cancel-storm recycle
  + stack hygiene, queue-full recovery with only `CANCELLED` slots, 4-worker
  exactly-once at capacity 4096, and `empty_claim_probe_cost_is_flat_in_capacity`,
  the P0.2 property test: probe cost at capacity 2^16 vs 2^9 must be <16x
  (measured ~1x here; the v1 scan measured **117x** and fails the test).
- **Bench** (`shm-bench -- task`): the "benchmark small so we don't measure the
  scan" caveat is deleted; a capacity-scaling section (256 vs 65536,
  claim+complete+submit p50 and empty-probe p50) is the standing flatness
  evidence.

## Measured (same-session A/B, M4 Max, release, 2 runs/side; machine drifts ±20-40%)

`shm-bench -- task`, old scan (A) vs stacks (B):

| metric | A (scan) | B (stacks) |
|---|---|---|
| round-trip p50, single thread, cap 256 | 83–125 ns | 42–83 ns |
| claim+complete+submit p50, cap 256, 1 task | 83 ns | 42 ns |
| claim+complete+submit p50, cap 65536, 1 task | **19,041–19,250 ns** | **42 ns** (flat) |
| empty-claim probe p50, cap 256 | 83 ns | ~0 ns (mean ~13.5) |
| empty-claim probe p50, cap 65536 | **40,292–40,916 ns** | ~0 ns (mean ~13.5) |
| pipelined throughput, 1 worker, cap 256 | 10.7–12.2 M/s | 5.8–6.6 M/s |
| pipelined throughput, 4 workers, cap 256 | 7.5–9.0 M/s | 3.3–3.8 M/s |

The claim path is capacity-independent (the P0.2 goal: at cap 65536 the v1
scan capped a worker at ~0.05 M claims/s; the stacks hold p50 at 42 ns). The
**saturated small-queue pipelined throughput is ~2x lower**: every task now
crosses two shared Treiber heads (submit pop+push, claim pop, complete push —
4 head CASes on 2 cache lines) where v1 spread per-slot CASes across the
array. This is the contention profile the design predicted (same as pool
alloc/free) and is stated, not hidden: for Holon's per-message claim tax at
realistic queue depths the trade is decisively right; if a saturated
small-queue workload ever matters, the remedy is the same seam as the FIFO
question (a seq-numbered index ring, or head backoff), not a return to the
scan.

`shm-bench -- artifact` (descriptor-path no-regression gate, 2 runs, new
side): pin p50 42 ns flat 1→10k versions; as_arrow p50 208–250 ns;
Replace commit p50 ~1.2 µs flat — at the documented reference numbers.
