# ADR-0008 — Holon Phase 0: substrate prerequisites

- Status: Accepted
- Date: 2026-08-28
- Context: [ACTOR-FRAMEWORK-DESIGN.md](../ACTOR-FRAMEWORK-DESIGN.md) proposes
  **Holon**, an actor framework on this substrate in which *state lives in memory
  nodes and actors own none of it*. Phase 0 is the set of substrate changes that
  must land before any actor code, because an actor system creates and destroys
  addressable state constantly and the substrate currently cannot survive that.

## Scope

| Item | Change | Why it is Phase 0 |
|---|---|---|
| **P0.1** | `shm-store` catalog slot **reclamation** | hard blocker: today `store_capacity` caps entries created *for the coordinator's lifetime* |
| **P0.2** | `shm-task` claim in O(1) — **done, ADR-0009** | claim was an O(capacity) scan; an actor mailbox drains it every message |
| **P0.3** | lifecycle-tied leases + `evict-current` — **done, ADR-0010** | a reclaimed slot needs a lease that dies with the entry, not with the actor |
| **P0.4** | Linux fast paths: futex doorbell, `eventfd`, `pidfd`, `memfd` sealing — **done, ADR-0011** | per-actor doorbells make the POSIX 1-byte-UDS wakeup the hot path |
| **P0.5** | ratify the **envelope-only ring** doctrine | decides, once, that `shm-ring` never carries payload |

Everything is additive to the *public shape* of the substrate. Where an on-shm
ABI must change it is batched into this one break, in the v0.3 (S0) style, and
the affected magic is bumped so a stale segment fails loudly rather than
silently.

## P0.1 — Catalog slot reclamation

### The problem

`Catalog::alloc_slot` is a monotonic `next_slot` bump and `evict` CASes the slot
`LIVE → TOMBSTONE` and never returns it. Three quantities are pure functions of
the slot index — `head_off = idx * head_stride`, `artifact_id = base + idx`, and
the slot's identity itself — which is exactly what made append-only safe: no two
creators ever contend for an index, and a journaled record routes back to a head
by arithmetic alone.

Reuse breaks all three at once, so the decision is not "add a free list"; it is
**what identifies an entry once an index no longer does**.

### Decision — the lineage is the pair `(artifact_id, incarnation)`

`artifact_id` keeps its present meaning and its present derivation
(`base + idx`): it names a **slot**, and it stays O(1)-routable from a journal
record. A new `incarnation: u32` names **which occupant of that slot** an
operation believes it is talking to.

- The **`ArtifactHead` is authoritative** for the incarnation — it is what every
  read, pin, commit and lease already touches, so validation costs one `SeqCst`
  load on paths that already carry `SeqCst` fences (a plain load on x86-64;
  measured at zero on the pin path). The load must be `SeqCst`, not relaxed:
  it is half of the reclaim handshake below.
- The catalog slot **mirrors** it (`CatalogSlot::gen`) so a sweep can hand the
  next creator the right value without attaching to the head first.
- Journal records (`ArtifactPin`, `WriteLease`) carry it in words already
  reserved in the 32-byte `JournalEntry`, so crash routing can reject a record
  belonging to a previous occupant. `JOURNAL_MAGIC` is bumped: those two tags
  change payload layout.

Rejected: making `artifact_id` a globally unique monotonic counter. It is the
tidier model, but it costs `slot_for_artifact_id` its O(1) derivation, widens
`artifact_id` to `u64` across four crates to avoid a 2^32 lifetime-create wall,
and buys nothing the pair does not already give.

### The state machine

```text
  FREE ──alloc+publish──▶ LIVE ──evict──▶ TOMBSTONE ──sweep──▶ RECLAIMING
    ▲                                         ▲                    │
    └──────── free list push ─────────────────┴── not quiescent ────┘
```

`TOMBSTONE → FREE` is **deferred, never immediate**. `evict` already tears down
versions through `Artifact::evict_all`, but retirement is *conditional*: a
version still pinned by a live reader is left for that reader's pin-drop to
retire. So the slot becomes reclaimable only once the artifact is quiescent, and
the sweep — not the evictor — decides that.

**Quiescence** is: `head.current == NO_VERSION`, every pin slot `FREE` with a
zero pin count, and `write_lease` unowned. The sweep proves it under
`RECLAIMING`, which is a state no creator will allocate from, so the check
cannot race a new occupant.

### Why a stale handle cannot corrupt a new occupant

A handle obtained before reclamation holds a raw pointer to the head region,
which is reused verbatim. The incarnation closes the window on both sides:

- **Reclaim side.** The sweep swaps `head.incarnation` to `NO_INCARNATION`
  (`SeqCst`) **before** it scans for quiescence, and restores the same value if
  it then aborts. The advance to the *next* incarnation happens separately, in
  the catalog slot's `gen`, and is read by the next `create`.
- **Operation side.** Every operation follows the existing Dekker shape already
  used for the `FREEING`/pin hazard handshake: **register first, then
  re-validate the incarnation**, and back out on a mismatch. "Register" is
  whatever the sweep's quiescence check can see — publishing a pin, claiming a
  version slot, taking the write lease. A late operation either registers before
  the retirement, and is then seen by the scan, which aborts the reclaim; or it
  registers after, sees `NO_INCARNATION`, and fails `Error::Stale`. There is no
  ordering in which both miss each other.

  The proof needs all four accesses in the `SeqCst` total order, so the
  registering **writes** are `SeqCst` RMWs (`publish_pin`'s `fetch_add`,
  `claim_slot`'s install CAS, `acquire_write_lease`'s CAS) and the sweep's
  quiescence loads are `SeqCst`. An `AcqRel` registration is not enough — the
  model admits an execution where the scan misses the registration *and* the
  operation misses the retirement. As in the pin hazard handshake, each side
  also carries an explicit `fence(SeqCst)` between its store and its load
  (`revalidate_incarnation` / `is_quiescent`; the pin path rides
  `accept_pin`'s existing fence) — the form loom can model. Model-checked in
  `shm-artifact/tests/loom_reclaim.rs`, one model per registration point.

An operation that fails `Stale` against a sweep that then aborts has failed
spuriously — but truthfully: the slot is `TOMBSTONE`, so its entry was evicted
either way, and the caller's answer is the same.

Two consequences worth stating, because they are load-bearing:

- **`attach_at` adopts, `attach_at_incarnation` validates.** Resolving by key
  means "whatever is live now", so a plain attach takes the incarnation it
  finds; a caller that already knows which occupant it means pins the
  expectation — the coordinator routing a journal record, and the evictor
  tearing down the occupant it tombstoned (see *Who sweeps*). The sweep's own
  plain attach is safe because it holds the slot in `RECLAIMING`, where the
  occupant cannot change.
- **Pin *drop* never validates.** `PinState::drop` works on the head directly,
  so retiring an incarnation can never strand a pin or block the retire it
  drives. That is what makes deferred reclamation converge.

### Allocation

The header gains a Treiber free list (`free_head: ShmU64`, `{tag:32 | idx:32}`,
ABA-safe by the tag — literally `shm_core::pool::treiber_pop`/`treiber_push`,
the loom-checked loops the chunk pools run), and each slot gains a `next_free`
link. `alloc_slot` pops first and falls back to the `next_slot`
bump, so an un-churned store behaves exactly as it does today and `next_slot`
remains the high-water mark that bounds `find_live_by_key`. A refused allocation
now un-bumps `next_slot`, so a full catalog does not run the high-water mark
past capacity.

The pop *is* the exclusive claim, which is why no extra `CLAIMED` state is
needed: `publish_slot` may keep writing its fields before the `FREE → LIVE` CAS,
exactly as it did when indices came only from the monotonic bump.

### Who sweeps

`evict` attempts an immediate sweep of the slot it just tombstoned — the common
case, where nothing was pinned, then costs nothing extra. Anything still busy
is picked up by `Catalog::reclaim_tombstones`, called from the coordinator's
existing lease-monitor tick. No new thread, no new timer.

Two consequences of slots recycling that the implementation must (and does)
carry:

- **The evictor binds to an occupant, not a slot.** Between the key scan and
  the `LIVE → TOMBSTONE` CAS the slot can in principle be evicted by someone
  else, swept, and re-created; blindly attaching to "whatever is live now" and
  tearing it down would destroy the *next* occupant. So `evict` reads the
  slot's `gen` before the CAS, re-checks `gen` and `key_id` after winning it
  (undoing the tombstone on a mismatch), and runs the teardown through
  `attach_at_incarnation` against that proven incarnation.
- **The sweep re-runs the teardown.** Eviction is a level, not an edge: a
  straggler handle held across the evict can commit a fresh version *after*
  the evictor's `evict_all` (its registration re-validates an incarnation that
  is still in service, so it succeeds), and such an entry would otherwise never
  go quiescent — the slot would leak forever. The sweep therefore calls
  `evict_all` (idempotent, cheap when already empty) on every tombstoned slot
  before judging quiescence, so tombstones converge no matter how late the
  straggler; the straggler's next operation after the reclaim fails `Stale`.

One residual window is accepted and stated: if a full recycle lands inside the
evictor's scan→CAS window *and* a concurrent sweep claims the wrongly
tombstoned slot in the nanoseconds before the evictor's `gen`/`key_id` check
undoes it, an innocent occupant can be torn down. Closing it entirely needs the
occupant identity inside the state word (a packed `{state, gen}` CAS); deferred
until evidence says the triple race exists in practice.

### Known limits, stated rather than hidden

- `find_live_by_key` remains an O(high-water) scan. Reclamation makes the
  high-water mark *stop growing*, which is the property Phase 0 needs; making
  the lookup itself O(1) needs open addressing, and open addressing needs probe
  chains that conflict with returning slots to a free list. Deferred to a later
  phase, on evidence.
- `incarnation` is `u32`. It counts recycles **of a single slot**, so exhaustion
  is 2^32 reincarnations of the same key. A slot whose incarnation would wrap is
  retired permanently instead of being freed: one slot lost, no id reuse, ever.

## P0.5 — The envelope-only ring doctrine

`shm-ring` hands a subscriber a bare `ChunkDesc` and has no pin, so a producer
that laps the slot races a reader mid-copy. `shm-store` has exactly the pin the
ring lacks.

**Decision: `shm-ring` never carries payload, and no pin is added to it.** A ring
slot carries a fixed-size control word — in Holon, the 64-byte `Envelope` — and
the payload it names lives in a pinned cell. The consequence is stated as a
guarantee rather than a caveat: **lapping loses a message, and can never tear
one.** Overflow is therefore always a delivery-policy question
(`Backpressure` or `DropOldest` + `Lagged`), never a memory-safety question.

This is the cheaper half of a choice that was already made twice in practice —
by `arrowref_messaging::shm_topics`, which rejected payload-in-ring for this
reason, and by the v0.2 doorbell, which carries no data at all.

## Consequences

- One batched on-shm ABI break: `CatalogSlot` gains `gen` and `next_free`
  (20 B → 28 B), the catalog header gains the Treiber `free_head`,
  `ArtifactHead` gains `incarnation` (in what was padding — head size and
  `head_stride` unchanged), `JournalEntry` reinterprets reserved words for two
  tags. `CATALOG_MAGIC` → `SHMSTOR2`, `HEAD_MAGIC` and `JOURNAL_MAGIC` bumped.
  P0.2 rides the same batch on the task-queue side (ADR-0009): `TASK_MAGIC` →
  `SHMTASK2`, `TaskQueueHeader` 40 → 48 B (round-robin `enqueue_head` out,
  FREE/READY Treiber heads in), `TaskSlot` 80 → 88 B (intrusive `next` link).
  P0.3 (ADR-0010) extends the same task-queue break: `TASK_MAGIC` →
  `SHMTASK3`, `TaskQueueHeader` → 64 B (`lease_free_head`), and the region
  gains the lease side table (`2 × capacity` × 48 B records) after the slot
  array; `ArtifactHead`, the catalog, and the journal are deliberately
  untouched. No coordinate outside this workspace pins these; ArrowRef
  consumes the crates behind a default-off feature. P0.4 (ADR-0011) rides the
  batch with **zero** ABI change by design: the futex doorbells activate the
  words ADR-0003 already reserved, and every other fast path is cfg-gated
  implementation only.
- `Error::Stale` becomes a normal, expected outcome for any handle held across
  an eviction. Callers re-resolve by key; that is what keys are for.
- The proof obligations are the existing ones, extended: loom on the
  register-then-validate handshake, a `kill -9` census that now asserts slots
  return to `FREE`, and a churn soak that creates and evicts far past
  `store_capacity` — the exact workload that fails today.
