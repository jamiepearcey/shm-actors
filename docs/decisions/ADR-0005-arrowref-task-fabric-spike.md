# ADR-0005 — ArrowRef task-fabric integration spike (v0.4 stage Q)

- Status: Accepted
- Date: 2026-07-23
- Decider: architecture (Fable), implemented by the Opus pipeline
- Builds on: [ADR-0004](ADR-0004-v0.4-adversarial-release.md) (stage Q ruling)

## Context

ADR-0004 stage Q ordered a **spike** — learn + prove the mapping, not a port —
that maps ONE ArrowRef surface, the **task fabric** (smaller API area than the L2
cache), onto shm-actors' published primitives, to validate that shm-actors is a
viable substrate for a stable ArrowRef rewrite and to surface concrete API gaps
feeding v0.5. Fable's cut-line rationale explicitly wanted evidence on *which*
Tier-3 perf item actually matters (suspected: **R**, the Append `O(table)` fix)
rather than a guess.

Hard constraints followed literally: shm-actors stays **clean-room** (no ArrowRef
dependency, no library-crate changes); query-cache is **read-only** (untouched);
the gaps return as this ADR, and the proof-of-concept is a self-contained,
committed, runnable example crate (`crates/shm-arrowref-spike/`) depending only on
shm-actors + arrow + a hand-written mock of ArrowRef's task contract
(`src/arrowref_mock.rs`). All cargo used `CARGO_TARGET_DIR=/tmp/shm-actors-v04-target`.

## 1. The ArrowRef task-fabric contract, as it exists today (file refs)

Read from `~/projects/infrastructure/query-cache/repo/src` on 2026-07-23. One
line: **a descriptor-first retained-execution cache; queue/control messages never
carry large payloads — Arrow/Parquet is retained once, referenced by descriptors,
moved only by explicit data-plane fetches.** The task fabric is that invariant
applied to work: *submit task → worker executes → retained output ref → ack.*

| Concept | Where | What it is |
|---|---|---|
| Submit | `runtime.rs::TaskRuntime::submit` / `submit_batch` (l.338, l.581) | Register a `TaskMessage`, enqueue on a crossbeam descriptor queue, journal `Submitted`. |
| Task message | `model.rs::TaskMessage` (l.82) | `task_id`, `plugin`, `input: InputRef`, `output: OutputPolicy`, `execution: ExecutionPolicy`, `deadline_ms`, `plugin_config` (JSON). |
| Input ref (descriptor-only) | `model.rs::InputRef` (l.95) | Tagged union: `ArrowRef` / `DatasetQuery` (projection+predicate+limit) / `BytesRef` / `ParquetRef` / `StreamRef`. Never the payload. |
| Execute | `runtime.rs` worker pool (l.1239 `spawn_worker`), panic-isolated | Native/DuckDB engine runs the plugin against the retained input. |
| Output as retained ref | `runtime.rs` `output_dataset` + `output_chunks: Vec<ChunkRef>` (l.104, l.428); `OutputPolicy::Dataset { dataset, group, retention, ack }` (`model.rs` l.130) | Result is retained as a versioned dataset (chunk refs) **or** an inline `Stream` (content-type + len). |
| Ack / clear-on-ack | `runtime.rs::ack` (l.458), `validate_ackable` (l.435); `model.rs::AckPolicy::clear_on_ack` (l.144) | Ack after completion; if `clear_on_ack`, the retained output dataset is evicted (`remove_dataset_if_unleased`). |
| Wait | `runtime.rs::wait_for_tasks` (l.746); HTTP `/tasks/wait` | Block on a terminal outcome (per-task or batch). |
| Crash/retry | `runtime.rs` restore (`restore_recovered_tasks` l.232) + journal | Task lifecycle is journaled and recovered on restart. |
| Chunk leases | README "chunk leases/ref-counts so referenced chunks stay valid while tasks run" | Referenced chunks are pinned for the task's duration. |
| Durability | `task_journal.rs` | Append-only, hash-framed, group-commit-fsync WAL of descriptor-only `Submitted`/`Completed`/`Failed`/`Released` records; torn-tail-tolerant replay + compaction. Payload bytes never enter the journal. |
| Lifecycle groups | `model.rs::LifecycleGroupConfig` (l.478) | Named groups with memory/spill budgets, TTL, `clear_on_ack_default`, eviction policy; `DEFAULT_TASK_RESULT_GROUP = "task_results"`. |
| Transport | `http.rs` `/tasks`, `/tasks/ack`, `/tasks/wait`, `/tasks/:id/output` (l.1175) | HTTP-facing; also a two-plane model (abort-proof node plane + disposable DuckDB workers). |
| Cross-host | `model.rs::ChunkRef.node_id` (l.40) + `cluster/` | Chunk refs are node-addressed; clustering replicates/places across hosts. |

## 2. Mapping onto shm-actors' primitives

Read from `crates/{shm-task,shm-artifact,shm-arrow,shm-core,shm-runtime}/src`.

| ArrowRef task-fabric concept | shm-actors primitive | Covered? |
|---|---|---|
| Descriptor-only queue message | `shm-task` `TaskSlot.request: ChunkDesc` — **24 bytes** (`shm-core/desc.rs`, `size_of::<ChunkDesc>()==24`) | ✅ core invariant holds |
| Submit task | `TaskQueue::submit(request: ChunkDesc, deadline_nanos)` → `TaskHandle{slot,seq}` (`queue.rs` l.493) | ✅ (payload is a bare chunk desc — see G1) |
| Claim / execute (competing consumers) | `TaskQueue::claim`/`claim_with_lease`/`claim_blocking` — CAS `QUEUED→CLAIMED`, **exactly-once** (`queue.rs` l.554) | ✅ excellent fit (disposable-worker plane) |
| At-least-once retry on worker death | `TaskQueue::reap(now)` — lapsed-lease `CLAIMED→QUEUED`, retry cap → `FAILED` (`queue.rs` l.788) | ✅ maps to the lease reaper |
| Cooperative cancel / deadline | `slot.cancel` flag + `ClaimedTask::is_cancelled` + `deadline` (`queue.rs` l.738) | ✅ |
| Retained input ref | a retained (`PUBLISHED`, refcount≥1) pool chunk; its `ChunkDesc` is the ref | ✅ (single chunk only — G1) |
| Zero-copy read of retained input | `shm-arrow::read_batch(owner, ctrl, desc, registry)` — buffers point into the mapping (`read.rs` l.73) | ✅ proven zero-copy |
| Task-duration chunk lease | `ChunkCtrl::borrow_shared()` / `release_shared()` (`shm-core/ctrl.rs`) | ✅ manual (not auto-tied to the task — G12) |
| Output as retained versioned ref | `shm-artifact::Artifact` RCU/MVCC version — `commit_optimistic(...)→version` (`artifact.rs` l.264); read via `pin().as_arrow()` | ⚠️ single lineage, not a keyed multi-output store (G3) |
| Clear-on-ack eviction | drop last ref → chunk `FREE` (refcount→0); artifact version retire | ⚠️ can't retire the **current** version (G4) |
| Descriptor-only result | `ClaimedTask::complete(result: ChunkDesc)` → requester `poll`/`wait` reads it (`queue.rs` l.887) | ⚠️ result is a chunk desc, not a typed dataset/version ref (G1) |
| Wait for terminal outcome | `TaskQueue::wait(handle, parker)` → `Outcome::Done(desc)`/`Failed`/`Cancelled` | ✅ |
| Doorbell wakeup | work/done doorbells wired by `shm-runtime::Node::task_queue` (`node.rs` l.590) | ✅ |
| Schema agreement across processes | coordinator `intern_schema`/`resolve_schema` (`node.rs` l.177) | ✅ (but artifact schema is fixed at v1 — G10) |
| Crash-safe task **lifecycle** durability | *(none — the queue segment is volatile; only the borrow-journal reclaims leaked pins/leases)* | ❌ gap G8 |
| Cross-host placement/replication | *(none by design — same-host SCM_RIGHTS substrate)* | ➖ boundary G9 |
| HTTP/browser facade, DuckDB workers | *(none — in-process Rust API; lives in ArrowRef's plane above)* | ➖ boundary G11 |

## 3. What the spike PROVED (runnable, measured)

`cargo run -p shm-arrowref-spike` (and its `#[test]`) run an end-to-end task: a
requester retains one Arrow input in shared memory and submits **only its 24-byte
descriptor**; a worker thread (standing in for a separate process — the handles
are the same ones a cross-process actor gets) claims it exactly-once, reads the
input **zero-copy**, doubles a column, retains the output as a new **artifact
version**, and completes with a 24-byte result descriptor naming that version; the
requester waits, reads the retained output **zero-copy**, verifies the values,
then clears-on-ack (releases the retained input → reclaimed).

Measured on the dev box (macOS/Apple Silicon):

| Metric | Value |
|---|---|
| Arrow input payload retained once | **32 896 bytes** |
| Control message that crossed the queue | **24 bytes** (`ChunkDesc`) |
| Payload : control ratio | **1370×** |
| Full task-queue slot | 80 bytes (state machine + request + result desc) |
| Retained output | `dataset="task_results"`, **version 1**, 4096 rows |
| Input read zero-copy (buffer ptr inside the mapping) | **true** |
| Output read zero-copy | **true** |
| Cleared-on-ack (refcount→0→FREE) | **true** |
| End-to-end round trip | ~1.4 ms |

**The invariant holds:** control stays 24 bytes; the payload is written exactly
once and thereafter only referenced (never copied through the queue) and read
zero-copy on both planes. The exactly-once claim + at-least-once reap + RCU
versioned output are exactly the shape the task fabric needs. **No ABI change to
the four lock-free cores is implied by this surface.**

Honesty about coverage: the spike is single-worker, single-task, single output
version, in-process (shared `SchemaRegistry` and shared `Arc<Segment>`s standing
in for the coordinator's fd-passing). It proves the *mapping*, not throughput; the
gaps below come from code-reading the primitives plus what the spike had to work
around.

## 4. Ranked API-gap list (feeds v0.5)

Ranked by how much each blocks a **real** migration of the task fabric.

### G1 — Typed retained-ref envelope vs a bare 24-byte `ChunkDesc` — **HIGH; first**
`submit(request: ChunkDesc)` and `complete(result: ChunkDesc)` carry one raw chunk
descriptor naming one pool chunk. ArrowRef's input is a typed `InputRef` union
(ArrowRef/DatasetQuery/BytesRef/ParquetRef/StreamRef) and its output is a *dataset*
(a `Vec<ChunkRef>` + version + content-type). A single `ChunkDesc` can express
none of: a multi-chunk dataset, an artifact **version**, a `DatasetQuery`
(projection/predicate/limit), or an inline-stream output. **Evidence:** the spike
had to *encode a version number into the `offset`/`len` fields* of the result
`ChunkDesc` (`encode_version`, marked with a `VERSION_MARKER` sentinel) — an abuse
of the descriptor. → v0.5 item: a **typed task-payload/ref envelope** (a
tagged-union carried by the queue, e.g. the request `ChunkDesc` points at a
retained chunk holding a serialized `TaskMessage`, and a first-class
`RetainedRef` result type `{artifact_id, version} | {dataset, chunks}`). *New
item; not R/S/H.*

### G3 — Single-lineage `Artifact` vs a keyed multi-output result store — **HIGH**
`shm-artifact` is one versioned lineage per artifact name. A task-output store is
*many* independent, short-lived, keyed outputs (`DEFAULT_TASK_RESULT_GROUP`). Two
bad options today: one `Artifact` (= one segment pair) per task output
(heavyweight), or all outputs as versions of one shared artifact — but then only
the latest is `current`, older ones retire unless pinned, and there is no
`name→version` index to fetch "task X's output". **Evidence:** `artifact.rs`
`head.claim_slot`/`current` model one lineage; no keyed lookup exists. → v0.5
item: a **keyed retained-result store** over the artifact/chunk primitives (a
name-indexed set of independent refcounted chunk-sets), not raw artifact versions.

### G8 — No durable task-lifecycle recovery — **HIGH**
ArrowRef's task fabric is crash-recoverable via `task_journal.rs`
(Submitted/Completed/Failed/Released, group-commit fsync, torn-tail-tolerant
replay, compaction). shm-task state lives entirely in the (volatile) queue
segment: it survives a **worker** crash (reap requeues via lease) but not a
**host/coordinator restart**. The borrow-journal reclaims leaked *pins/leases*,
not *task lifecycle*. **Evidence:** `shm-task` has no WAL; recovery is memory-only.
→ v0.5 item: an **optional per-queue task WAL** mirroring ArrowRef's (or an
explicit "tasks are ephemeral" contract). *New durability item; not R/S/H.*

### G12 + G4 — Task-lifecycle-tied chunk leases & evict-current — **MEDIUM-HIGH**
This is the concrete task-fabric read of stage O's *ephemeral-broadcast-loan-not-
journaled* note. ArrowRef ties chunk leases to the task ("valid while tasks run,
cleared on ack"). In shm-actors the input/output chunk refcounts are **manual**
and not bound to the task slot: a submitter that dies after retaining input but
before the task completes **leaks** the chunk unless it took a `pin_journaled`
pin. And `clear_on_ack` can't evict the **current** artifact version —
`try_retire_version` returns early while `current` (`artifact.rs` l.908), so the
spike had to demonstrate clear-on-ack on the *input* chunk, not the output. →
v0.5 item: **auto-tie input/output chunk leases to the task slot** (release on
terminal/ack, journal for crash reclamation) + an **evict-current / drop-to-empty**
artifact op. *Maps to "lifecycle/TTL/clear-on-ack groups".*

### G6 — item S: `O(capacity)` claim scan + head-of-array contention — **MEDIUM**
`submit` scans `0..capacity` for a reusable slot (`queue.rs` l.496; spread by
`enqueue_head`), but `claim_inner` scans `0..capacity` **starting at index 0 for
every worker** (l.577) and `reap` scans all slots. With many workers this is
`O(capacity)` per claim plus CAS contention on the low slots. ArrowRef's crossbeam
queue is `O(1)` MPMC. **Evidence:** code-read; the spike (capacity 8, one worker)
did not stress it. → v0.5 item **S**: a real MPMC index / per-worker claim start
offset (cheap first step: spread the claim cursor like `enqueue_head`) / free-slot
free-list. Only bites at high fan-out.

### G5 — item R: Append commit is `O(prior chunk count)` — **MEDIUM, but avoidable here**
Confirmed in `artifact.rs::commit_staged_inner` (l.447–479): an `Append` commit
re-`borrow_shared`s **every** prior chunk into the new manifest, so version N costs
`O(N)` and the manifest grows unboundedly — quadratic for an append-accumulate
store. **But the evidence refines the ADR-0004 guess:** a task-output store wants
*independent* outputs, i.e. `Commit::Replace` (`O(new data)`) or standalone
refcounted chunk-sets — which never hit R. R bites **only** if task outputs are
accumulated into one growing dataset. So for *this* surface R is real but
lower-priority than G1/G3/G8. → v0.5 item **R** stays deferred until a real
workload shows an append-accumulate output model.

### G10 — Schema fixed at v1 — **MEDIUM (mostly moot for independent outputs)**
`commit_staged_inner` sets the artifact's `schema_id` once
(`compare_exchange(0, schema_id, …)`, l.514), so a single artifact lineage cannot
evolve schema across versions. Moot if each task output is its own ref; a gap for
an evolving dataset. → folds into G3's keyed-store design.

### G7 — No submit backpressure — **LOW-MEDIUM**
`submit` returns `Error::QueueFull` when all slots are live (l.539); there is a
`claim_blocking` but no `submit_blocking`/admission long-poll (ArrowRef has both).
→ ties to S.

### Boundaries (not shm-actors gaps — kept in ArrowRef's plane above)
- **G9 cross-host** (`ChunkRef.node_id`, clustering): shm-actors is the *same-host*
  substrate by design; the migration keeps ArrowRef's cluster layer and swaps only
  the same-host L2/task core.
- **G11 HTTP/browser facade + DuckDB workers**: shm-actors is an in-process Rust
  API; ArrowRef's HTTP surface and disposable DuckDB workers layer on top. The
  **disposable-worker model maps cleanly** — shm-task's claim + lease-reap *is* the
  abort-proof-node / disposable-worker discipline.

## 5. Verdict and recommended v0.5 sequence

**Viability: YES.** shm-actors is a viable substrate for ArrowRef's task-fabric
surface. Its core invariant (descriptor-only control, payload retained once,
zero-copy) maps directly and is *proven*; exactly-once claim + at-least-once
lease-reap + fenced write + RCU versioned output are precisely the primitives the
fabric needs; the disposable-worker plane is a natural fit. **Crucially, every gap
is an additive envelope/store/durability layer ABOVE the primitives — none is a
flaw in, or an ABI change to, the four lock-free cores.**

The evidence answers ADR-0004's cut-line question directly: the first thing a real
task-fabric consumer needs is **not** the suspected perf item **R**, but the
**typed-ref envelope (G1) + keyed result store (G3) + lifecycle-tied leases &
durability (G12/G4/G8)** — structure/feature, not perf. R is avoidable for this
surface; S matters only at high fan-out. *Get evidence, don't guess* paid off:
build R/S last, gated on a measured workload.

Recommended v0.5 sequence (most-blocking first):

1. **Typed task-payload/ref envelope** (G1, G2) — the request `ChunkDesc` points at
   a retained encoded `TaskMessage`; a first-class `RetainedRef` result type
   (`{artifact_id, version}` | `{dataset, chunks}`). Unblocks everything.
2. **Keyed retained-result store** (G3, G10) — name-indexed independent refcounted
   chunk-sets over the artifact/chunk primitives; `Replace`/independent semantics
   (sidesteps R).
3. **Task-lifecycle-tied leases + evict-current** (G12, G4) — auto-release
   input/output leases on terminal/ack, journalled; drop-to-empty artifact op.
4. **Optional durable task WAL** (G8) — mirror `task_journal.rs`, or declare tasks
   ephemeral.
5. **Then, gated on measured fan-out:** **S** (claim scan/contention; cheap first
   step = spread the claim cursor) and — only if an append-accumulate output model
   appears — **R** (Append `O(table)`).

Same-host boundary (G9) and the HTTP/DuckDB plane (G11) stay in ArrowRef's layer
above the substrate; **H** (Linux futex doorbell) remains deferred per ADR-0004 —
the spike surfaced no doorbell bottleneck.

## Artifacts

- Runnable spike: `crates/shm-arrowref-spike/` (clean-room; `run_spike()` +
  `arrowref-spike` bin + a green `#[test]`). Library crates unchanged; workspace
  builds and per-crate tests stay green; clippy clean.
