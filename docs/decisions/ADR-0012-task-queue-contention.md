# ADR-0012 — Task-queue contention: what P0.2 left behind, and the four fixes

- Status: Accepted (implemented)
- Date: 2026-08-28
- Builds on: [ADR-0009](ADR-0009-p0-2-o1-task-claim.md) (the FREE/READY stacks),
  [ADR-0008](ADR-0008-holon-phase-0-substrate-prerequisites.md).

## Context

The post-Phase-0 benchmark pass found that task-queue **aggregate** throughput
*fell* as workers were added — 6.9 → 4.3 → 2.3 M/s at 1/2/4 workers on disjoint
tasks. P0.2 had removed the O(capacity) discovery scan; the review's hypothesis
was that it had bought O(contention) serialization in its place. This ADR
records the investigation and what it found. The method throughout was: add
the counter that isolates one cause, fix it, re-measure, repeat — never fix by
theory.

## Findings, in the order they were isolated

1. **The bench shape hid the question.** One producer feeding N spinning
   workers is producer-bound by construction, and each idle worker's empty
   claim loaded the same line the producer was CASing. Replaced by an
   N-producer/N-worker shape and a *drain* shape (pre-filled queue, N workers,
   no producers, per-worker counters) that isolates pure claim contention.
2. **`TaskQueueHeader` was one cache line.** `free_head`, `ready_head`, the
   doorbell and both waiter counts all shared it. Every submit, claim, complete
   and idle spin touched the same line. Padded — and it changed **nothing
   measurable**, which is why it is not the headline.
3. **The drain isolated a single Treiber head.** Claim-only, one head: 59 M/s
   with one worker, 17.6 with two, 6.3 with four. A Treiber head is O(1) per
   operation and O(cores) in contention. Sharded [`SHARDS`] = 8 ways, one
   head per line, worker home shard + steal.
4. **Sharding barely moved the drain.** So the serialization point was not
   the head. It was **`ClaimedTask { queue: TaskQueue }`** — every claim cloned
   the queue handle, and the handle holds two `Arc<dyn Notifier>`. Two
   contended refcount `fetch_add`s per claim, two `fetch_sub`s per completion,
   on lines shared by every worker in the process. This is the real defect.
   Fixed by making `ClaimedTask<'q>` **borrow** the queue — the design doc's
   "honest `Send`/`!Send`" rule applied: a claimed task cannot outlive the
   queue it came from, so the borrow costs nothing and removes the only
   cores-wide atomic on the claim path. End-to-end scaling went from
   4.5 → 2.0 M/s (negative) to flat.
5. **`TaskSlot` was 88 bytes**, so adjacent slots shared a line and two
   workers on different shards false-shared on `state`/`owner` writes. Padded
   to 128 — two whole lines.
6. **Steal order was shared.** `home+1, home+2, …` meant two workers that
   exhausted their homes converged on the same next shard. Per-worker counters
   showed both at 12 M/s against 73 M/s alone — uniformly slow, so contention,
   not core placement. Steal order is now a per-worker odd stride.

## What remains, stated rather than hidden

The drain worst case — every worker racing to empty a pre-filled queue —
still roughly halves per-worker throughput at two workers. That is inherent
to shared LIFO heads: any two workers that meet on a shard serialize on its
CAS, and a drain race always ends with everyone on the last non-empty shards.
Sharding and stride reduce how often they meet; they cannot make meeting free.

The structural fix is **owner-local deques** (Chase-Lev shape: the owner pops
without a CAS, only a stealer CASes), with the shared sharded stacks demoted to
an ingress path. That is a Phase 1 scheduler decision, not a substrate patch,
and this ADR exists so it is made on this data rather than re-derived.

Also unmeasured: everything on Linux. This machine's P/E-core scheduling
adds variance at 4 workers (per-worker counts of `[18k, 6k, 5k, 30k]` are a
placement signature, not a contention one); those runs are reported with
their spread.

## ABI

`SHMTASK3 → SHMTASK4`: header 64 → 1088 bytes (control line + 8 FREE lines +
8 READY lines, `offset_of` asserts on both arrays), `TaskSlot` 88 → 128. Both
`const`-asserted. `slots_offset` already rounded the header to 64, so nothing
else moved. No consumer outside this workspace pins the old layout; the
kellnr republish is already semver-major for the P0.1 breaks.

## Non-changes

`claim_pop`, `publish_queued` and every loom model (`loom_task.rs`, 6 models)
are unchanged and run against one shard's head — sharding is *which* head a
node rides, and the per-slot CAS arbitration of ADR-0009 is untouched.

## Addendum — post-review fixes (same day)

An adversarial read of the sharded queue found no single-membership violation
and no lost-task interleaving, and four things worth fixing:

- **The stride was inert.** `1 + 2 * ((worker_id / SHARDS) % (SHARDS/2))` is
  stride 1 for every `worker_id < 8` — every worker the bench ran. Now
  `1 + 2 * (worker_id % (SHARDS/2))`. The drain numbers above were measured
  *with the inert stride*; finding 6's fix was real in intent and null in
  effect until this addendum.
- **The queue-full drain held other actors' tasks in process memory.** It
  popped live `QUEUED` nodes into a `Vec` while digging for a `CANCELLED` one.
  A submitter `kill -9`ed mid-dig stranded every held node — up to `capacity`
  of them, none its own — off every stack forever. Redesigned: pop the top of
  each shard only; a live node goes straight back; a buried `CANCELLED` node is
  left for `claim_pop` to transfer. Crash window is back to ADR-0009's one
  node; cost is O(SHARDS), not O(SHARDS × capacity).
- **`cancelled` could transiently read `u32::MAX`** (increment after the
  cancel CAS, racing a transfer's decrement), which read as "many" and drove a
  full dig. Now counted before the CAS and uncounted on failure; the drain also
  treats `> capacity` as zero.
- **Spurious `QueueFull`** when a concurrent `claim_pop` transferred the sought
  node to FREE mid-sweep: the drain now retries `pop_free` once.
- The `submit` ↔ `claim_blocking` waiters handshake gained the explicit
  `fence(SeqCst)` the substrate's house rule prescribes for Dekker pairings.
  Sound before on x86/AArch64; required for the C11 model.

Not done: a loom model of the drain. With hold-aside gone the drain is a pop,
one CAS, and a push-back on a single shard — the same shape `claim_pop`'s
models already cover.
