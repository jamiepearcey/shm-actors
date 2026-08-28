# ADR-0010 — Holon P0.3: lifecycle-tied leases + evict-current

- Status: Accepted
- Date: 2026-08-28
- Builds on: [ADR-0008](ADR-0008-holon-phase-0-substrate-prerequisites.md) (P0.3
  scope; the P0.1 recycle handshake this item must preserve),
  [ADR-0009](ADR-0009-p0-2-o1-task-claim.md) (P0.2's FREE/READY stacks — whose
  immediate slot recycling reshaped this design), and
  [ADR-0005](ADR-0005-arrowref-task-fabric-spike.md) (gaps G4 and G12).

## The two gaps, read precisely

- **G4** (ADR-0005 §4): clear-on-ack cannot retire the **current** artifact
  version — `try_retire_version` early-outs while `current == version` — so the
  spike had to demonstrate clear-on-ack on the *input*, never the output.
- **G12** (ADR-0005 §4): chunk/version holds are **manual and actor-scoped**,
  not tied to the task or the entry. A submitter that dies leaks its input hold
  unless journaled — and a journaled hold dies with the *actor*, which is
  exactly wrong for a task input the still-running task needs. On the entry
  side, a tombstoned entry whose fenced write lease is held by a
  live-but-idle committer never goes quiescent: the lease died with the actor's
  heartbeat, not with the entry.

The P0.3 thesis: **a hold must die with the thing it protects** — the write
lease with the *entry*, a task's retained input/output with the *task*.

## Decision 1 — `evict_current` is an **empty Replace commit** (G4)

`Artifact::evict_current_optimistic(owner, expect)` /
`Committer::evict_current()` / `Entry::evict_current()` commit an **empty**
version (`staged = &[]`, `batch_spans = &[]`) through the unchanged
`commit_staged_inner`; the install-path retire then reclaims the evicted
version by the standard ADR-0003a handshake (a pinned reader drains via pin
drop). The entry stays `LIVE`: the key resolves, the next commit continues the
sequence at `target + 1`, and a reader of the evicted-current entry sees
`VersionGone` from `as_arrow`'s existing zero-batch arm — identical to a
never-committed entry, with **zero** new branches on the pin hot path.

**Rejected: CAS `current: v → NO_VERSION`.** It reissues version numbers (the
next commit targets `expect + 1 = 1`) while the old same-numbered slot may
still be `SLOT_LIVE` under a reader's pin — breaking the "version numbers are
monotonic and never reissued" invariant that `find_slot`/`accept_pin`
disambiguation rests on. Fixing that needs a version-floor word, and
`ArtifactHead` has zero padding left (`SHMAHEA3` is exactly full) — an ABI
break for nothing. The empty-commit form needs no head change, no new atomics,
and **is** the commit registration `loom_reclaim_vs_claim` already models:
the recycle handshake gains no new registration point. `write_manifest`
handles `chunk_count = 0` structurally (32-byte header-only manifest; every
pool class is ≥ 64 bytes, so it always fits).

Cost per evict-current: one 32-byte manifest chunk, freed when the empty
version is itself superseded or torn down. Quiescence is untouched:
`is_quiescent` still requires `current == NO_VERSION`, which only `evict_all`
produces — evict-current never makes a slot reclaimable, by construction.
Semantics: evicting an empty entry is `VersionGone`; a lost race is
`Conflict`; repeated evicts stack empty versions monotonically.

## Decision 2 — the write lease dies with the entry (G12a)

`Artifact::evict_all` (the teardown both the evictor and the sweep's
re-teardown run) now also calls `force_release_write_lease()` — the exact
item-K crash-release CAS, fence bump included — after storing `NO_VERSION`.
A tombstoned entry held by a live-but-idle committer therefore converges: the
lease is revoked at evict time instead of at that actor's death.

**The refined lease invariant, stated carefully.** The sweep may now reclaim
over a lease it revoked. A revoked holder's late commit that *observes* the
fence bump fails the step-0 `lease_held_by` gate (`Error::Fenced`) before
staging anything — and the force-release also turns the straggler-resurrect
race (eviction is a level, not an edge) into a clean rejection for leased
commits. But the step-0 gate is an `Acquire` load, and the loom model built
for this ADR **found the interleaving where a maximally-stale zombie reads a
stale lease word and passes the gate**. The load-bearing guarantee is
therefore *not* "the gate fails"; it is: **no interleaving installs onto a
reclaimed head** — a zombie that slips the gate still registers at
`claim_slot` and fails the fenced step-4b revalidation (`Error::Stale`), or
loses the install CAS (`Conflict`). `loom_reclaim_vs_fenced_lease` models the
full shape (gate + registration) and proves exactly this.

**Dekker preservation (the ADR-0008 mandate).** The sweep keeps its exact
shape — teardown, then `retire()` (SeqCst), then the fenced SeqCst quiescence
scan. The force-release happens in the *teardown* phase, before `retire`, and
is a **de-registration** (like pin drop, which never validates); it moves no
fence and adds no registration point. Every operation side is byte-identical:
pin registers via `publish_pin` → `accept_pin`'s fence → `is_incarnation`;
commit via `claim_slot` (SeqCst CAS) → `revalidate_incarnation`; lease via
`acquire_write_lease` → `revalidate_incarnation`. The three P0.1 loom models
pass unchanged; removing the `fence(SeqCst)` in `revalidate_incarnation`
still fails `vs_claim`, `vs_lease`, **and** the new `vs_fenced_lease`
(bite-verified).

## Decision 3 — task-lifecycle-tied retained refs (G12b)

### The retained pin

`Artifact::retain_pin()` takes a **guard-less, unjournaled** pin on the
current version: the count is left incremented with no RAII guard and no
borrow-journal entry, deliberately — a task input binding must *survive* the
submitter's death and die with the **task**. `Entry::retain_current()` wraps
it and returns the opaque `{artifact_id, incarnation, version}` triple.
Registration-wise this is the already-loom-modeled pin handshake run twice
(once by the inner pin, once for the retained increment taken while it is
held), so no new head-side registration point exists. While a binding is
armed, the entry cannot go quiescent — its slot cannot recycle out from under
the binding.

Release is `shm_store::release_task_binding(...)`: route `artifact_id`
through the catalog, `attach_at_incarnation`, `release_leaked_pin(version)` —
byte-for-byte the coordinator's item-J crash route, so a binding whose
incarnation no longer matches is **dropped**, never applied to the slot's
next occupant.

### The lease side table (`SHMTASK3`)

The task-queue region gains a side table of `LeaseSlot` records (48 B each,
`2 × capacity` of them, appended after the slot array) plus a Treiber
free-list head in the header (56 → 64 B; `slots_offset` stays 64 — the slot
array did not move). A record ties one binding to one task incarnation
`{slot_idx, seq}`:

- `TaskQueue::submit_with_binding(request, deadline, input)` arms the input
  binding under the submit reservation, **before** `publish_queued` — no
  worker can ever observe the task without its input lease.
- `ClaimedTask::bind_output(binding)` arms the worker's retained output under
  `CLAIMED`, before `complete`.
- `TaskQueue::ack(handle)` — the requester consumes a terminal outcome and
  wins the bindings (for the caller to release against the store).
  `NotTerminal` protects a live task; a recycled slot (`StaleHandle` to
  `poll`) is still ackable. Idempotent.
- `TaskQueue::reap_bindings(now, grace)` — the coordinator's backstop: an
  armed record is released only when its task no longer needs it (slot `seq`
  moved on, or a state that can never run again: terminal, or a
  crashed-submitter `RESERVED` wedge) **and** `now` is past the record's
  deadline anchor plus the grace window. A live `QUEUED`/`CLAIMED` task is
  never touched, however late.

**Exactly-once + ABA.** A record's whole protocol is one packed word
`{gen:32 | state:32}`: fields are written by the armer under free-list
exclusivity, then the gen-bumped `ARMED` word is `Release`-stored; a releaser
CASes the exact observed word `{g, ARMED} → {g, RELEASED}`. The generation
makes a racing ack-vs-reap release exactly-once and makes a stale releaser
(record meanwhile released, retired, and re-armed for a different task) fail
its CAS. Modeled in `loom_task.rs::loom_lease_release_exactly_once_and_gen_aba_safe`;
bite-verified (a gen-ignoring CAS fails the model).

### Deviation from the approved design, and why

The design proposed a **parallel per-slot array** "guarded by the existing
slot state machine". P0.2 (ADR-0009) invalidated that: `complete`/`fail` now
push the slot to the FREE stack immediately, so a slot recycles for a new
task *before* the requester acks — a per-slot row would be overwritten by the
next occupant's binding while the old one is still armed, which is precisely
the "lease outlives the occupant" bug class P0.3 exists to kill. The record
pool with `{slot_idx, seq}` correlation and a generation-guarded word keeps
slot recycling untouched (zero changes to `claim_pop`/`publish_queued`/
`complete`/`fail`/`cancel` — the P0.2 loom models stand as-is) and lets an
old task's binding coexist with the slot's new occupant. ADR-0009's sketch of
"explicit ack releasing the slot" was likewise **not** adopted: ack releases
the *bindings*, never the slot — slot capacity and binding retention are
independent backpressure axes (`QueueFull` vs `LeaseTableFull`).

### Reap grace policy (ratified)

`RuntimeConfig::task_binding_grace` (default 2 s = 4 × the default
`lease_deadline`) is added to the coordinator's config; the lease-monitor tick
runs `reap_bindings(now, grace)` and releases won bindings against the store
(`task_bindings_reclaimed` counts them). The anchor is the record's stored
deadline: the submit deadline for an input binding, the claim-lease deadline
for an output binding — so an unacked completed task's retained output stays
readable for (claim lease + grace), bounded, and a requester always gets at
least the grace window after the task's lease horizon to ack. This is
ArrowRef's retention/TTL surfacing in the substrate, as predicted; richer
policy (per-group TTLs) stays in the envelope/store layer above.

## ABI / magic

One on-shm break, batched per ADR-0008 style: `TASK_MAGIC` `SHMTASK2 →
SHMTASK3` (a stale region fails `BadMagic` loudly), `TaskQueueHeader` 56 →
64 B (`lease_free_head`), region grows by `align + 2 × capacity × 48` B for
the lease table. `TaskSlot` stays 88 B; `slots_offset` stays 64. Deliberately
**no** change to `ArtifactHead` (`SHMAHEA3`, zero padding left — the
empty-commit form of evict-current was chosen specifically to avoid growing
it), `CatalogSlot`/`SHMSTOR2`, `JournalEntry`/`SHMJRNL3` (bindings live in
the queue segment, not the journal), or the manifest format (`chunk_count =
0` was already representable). Consumers size via `required_bytes`.

## Proof

- **Loom, artifact side** (`/tmp/loom-target`, `--cfg loom`):
  `loom_reclaim` 4/4 — the three P0.1 models **unchanged** (isolated runs:
  pin 63, claim 252, lease 279 interleavings) plus
  `loom_reclaim_vs_fenced_lease` (3609 interleavings isolated; full commit
  shape: gate + claim + fenced revalidate vs
  force-release → retire → scan). Bite: removing `revalidate_incarnation`'s
  fence fails `vs_claim`, `vs_lease`, and `vs_fenced_lease`; restored, all
  green. The model also *earned its keep during development*: asserted on the
  step-0 gate alone it failed, exposing that the gate can pass on a stale
  `Acquire` load — which is why the refined invariant above names the
  registered revalidation, not the gate, as load-bearing.
- **Loom, task side**: `loom_task` 6/6 — the five ADR-0009 models unchanged
  plus the lease-record model (27 interleavings: dual-releaser exactly-once +
  stale-generation rejection after re-arm). Bite: a gen-ignoring
  `try_release` fails it; restored, green.
- **Failing-first test** (watched fail before the G12a fix):
  `store_local.rs::a_tombstoned_entry_with_a_held_write_lease_converges` —
  pre-fix the fenced holder's late commit *succeeded* (resurrect) and the
  slot never reclaimed; post-fix the commit is `Fenced` and the slot
  converges to `SLOT_FREE` with a zero-leak census.
- **Unit/integration**: evict-current reclaims + sequence continues + empty
  read is `VersionGone` + conflict loser stages nothing + pinned reader
  drains on drop (rcu.rs); a 400-version commit/evict-current churn past
  `MAX_LIVE_VERSIONS` under a concurrent reader ends 1 manifest chunk from
  baseline; `Entry::evict_current` keeps the entry `LIVE` (store_local.rs);
  retained bindings survive the actor, drop on wrong incarnation, block then
  unblock slot reclaim (store_local.rs); the full binding lifecycle,
  reap-backstop liveness/grace rules, slot-reuse isolation and
  `LeaseTableFull` backpressure (task.rs); end-to-end over a real
  coordinator, the submitter's `kill -9` analogue releases its journaled pin
  by replay while the task's bindings survive and release by reap, to a
  zero-leak census (`shm-runtime/tests/task_bindings.rs`).
- **Spike**: `shm-arrowref-spike` now demonstrates clear-on-ack on the
  **output** via `evict_current` (ADR-0005 §3 recorded it had to use the
  input); `output_cleared_on_ack` is asserted in its test.

## Measured (same-session A/B, release, ≥ 2 runs/side, M4 Max)

Descriptor paths are untouched by design and measured at reference:

- artifact: pin p50 **42 ns** flat 1 → 10k versions; `as_arrow` p50
  208–250 ns; Replace commit p50 ~1.2–1.29 µs flat (both sides of the A/B).
- task queue (the region grew by the lease table): round-trip p50 **83 ns**
  (= before); claim+complete+submit p50 **42 ns** flat 256 → 65536
  (= before); empty-claim probe p50 0 ns / mean ~13–14 ns flat (= before).
  Saturated pipelined throughput: 1-worker 5.9–7.1 M/s (before 6.0–6.2);
  4-worker 2.3–3.1 M/s over five runs vs 3.0–4.1 over the two before-runs —
  the before-samples' own spread (37 %) exceeds the difference and the claim
  and complete paths read none of the new words, so this is judged
  within-noise, but it is flagged rather than hidden: the 4-worker contended
  number has been the noisiest metric since ADR-0009's A/B.

The binding paths (`submit_with_binding`, `bind_output`, `ack`,
`reap_bindings`) are cold-path by construction: claim/poll/wait read none of
the new words.

## Left undone, stated

- The claim-lease anchor for output bindings means a very long claim lease
  delays the output's reap window by the same amount; a terminal timestamp
  in the slot would tighten it, but writes a word on the completion path for
  a policy refinement — deferred until evidence.
- `ack` scans the lease table (O(2 × capacity), cold requester path); an
  index (e.g. per-slot record chains) is deferred until a workload shows it.
- The P0.2 submitter-crash windows (die between `QUEUED` store and READY
  push; die in `RESERVED`) are unchanged; the binding reap now at least
  releases the *bindings* of a `RESERVED`-wedged task past its window.
- Typed output refs / `OutputPolicy` / lifecycle groups (G1/G3 envelope work)
  stay out, deliberately: the binding carries only the three opaque words so
  the envelope layer can land independently.

## Addendum — post-review fixes (2026-08-28)

An adversarial read found the recycle handshake preserved (no new registration
point; `loom_reclaim_vs_fenced_lease` faithful) and the evict-current design
sound, and eight things beyond that, of which six are now fixed:

- **F1 (high) — `evict_all` tore down without registering.** Its stores were
  invisible to a sweep's quiescence scan, so a preempted evictor could have its
  slot reclaimed and re-created underneath it and then land `current =
  NO_VERSION`, a lease revocation and a version retire on the *next occupant*.
  Now: force-release, **acquire the write lease as `EVICTOR_OWNER`**, fenced
  `revalidate_incarnation`, tear down, release. Holding the lease is exactly
  what the scan refuses to reclaim under; the sweep's own `evict_all` takes
  the same lease, so the two teardowns serialize.
- **F2 (high) — `ack` returned bindings nobody could release.** An actor has no
  route to the store's raw segments, so on the intended `wait → ack` path every
  binding leaked its version and slot forever; a requester dying between ack
  and release leaked with no record left to reap. Now `ack` CASes each record
  `ARMED → ACKED` (new state 3) and returns a count; the coordinator's
  `reap_bindings` releases `ACKED` records with zero grace and no liveness
  check. One releaser, crash-proof, exactly-once by the same gen word.
- **F3 (medium) — the lease fence reset on every recycle**, so a stale
  `Committer` from a slot's previous occupant could, on drop, CAS the new
  occupant's identical `{owner, 0}` lease word and revoke an innocent writer.
  `create_at` now carries the prior head's fence into the fresh one: monotonic
  per region, a stale token can never match.
- **F4 (medium-low) — the retain → arm window was untracked.** A crash, or a
  `LeaseTableFull`/`QueueFull` return, between `retain_current` and the arm
  leaked the pin. `retain_current` now journals an `ArtifactPin` (reclaimed by
  the item-J replay like any reader pin); `binding_armed` releases it at the
  handoff; `release_retained` undoes a failed arm.
- **F7 (low) — `release_leaked_pin` had no underflow guard.** Now
  `PinSlot::try_unpin` (CAS, refuses below zero), so a double release is a
  lost release, never a free-under-reader or a wrap.
- **F6 (doc)** — "identical to a never-committed entry" is narrowed to "reads
  see `VersionGone`": `pin()` succeeds on an empty version, `current_version`
  is `N`, and the watch sink reports `Replace`.

Left as recorded: **F5** — `reap_bindings` can release a mid-submit input
binding if a caller passes an already-elapsed deadline and the submitter stalls
past grace between `arm` and `publish_queued` (production passes `now + lease`;
tests pass elapsed deadlines deliberately); **F8** — a straggler optimistic
commit on a tombstoned entry after `evict_all` can transiently reissue version
1 while the old v1 slot is pinned (a reader spins until it drains; the fence
gate closes it for leased commits only).
