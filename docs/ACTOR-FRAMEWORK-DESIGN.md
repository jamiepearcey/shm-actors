# Holon — a zero-copy actor framework on the shm-actors substrate

Status: **proposal / design**. Nothing implemented. Supersedes nothing; sits *on top of*
`shm-actors` v0.5 (ADR-0001..0007) and generalises the two zero-copy ideas ArrowRef already
proved in production: the **descriptor** (`ChunkDesc` / `PackedRef` / `TypedRef`) and the
**bus** (`shm-ring` same-host, `src/tcp_control.rs` cross-host).

---

## 0. One-line thesis

> **Ray's object store, Erlang's supervision, and Rust's borrow checker as the cross-process
> pin — actors that own no state, memory nodes that own no code.**

Every existing actor framework (Akka, Erlang/OTP, Actix, Ractor, Orleans) puts *state inside
the actor*. That single choice forces serialization on every message and makes actor death
equal state death. Invert it:

- **State lives in memory nodes** — RCU/MVCC versioned cells in shared memory, addressable,
  snapshottable, replicable, adoptable by a successor process.
- **Actors are stateless handlers bound to cells.** An actor is cheap, restartable, and holds
  nothing but descriptors and local scratch.
- **Messages are descriptors, not bytes.** 64 bytes on the wire whether the payload is 1 KiB or
  1 GiB, same-host or cross-host.

That inversion is what makes the user requirement — *"memory contained in a memory node that
guarantees that if it dies it can be copied"* — expressible at all.

---

## 1. What already exists (do not rebuild)

| Piece | Where | What it gives |
|---|---|---|
| `ChunkDesc` 24 B, `PackedRef` `[seg:32\|off:32]` | `shm-core` | the descriptor |
| `Pool` (Treiber size-class), `ChunkCtrl` FREE/LOANED/PUBLISHED + gen | `shm-core` | allocation + stale-descriptor detection |
| `BorrowJournal` | `shm-core` | crash-reclaim of pins held by a dead process |
| `Platform` seam (POSIX baseline, Linux fast paths reserved) | `shm-core` | portability |
| zero-copy Arrow read/write, `SchemaRegistry` | `shm-arrow` | typed payloads, fuzz-hardened `read_batch_layout` |
| SPMC broadcast ring, `Msg::{Sample,Lagged}`, reliable mode | `shm-ring` | the same-host bus (**no pin** — envelope-only) |
| RCU/MVCC artifact, pin hazard handshake (Dekker), fenced write lease | `shm-artifact` | the versioned cell |
| staged append + atomic `commit_staged` | `shm-stream` | transactional writes |
| MPMC queue, exactly-once CAS claim, lease/deadline reap | `shm-task` | work dispatch |
| keyed store, key interning, `resolve_and_pin` | `shm-store` | the cell catalog |
| Coordinator (UDS + `SCM_RIGHTS` fd grant, lease monitor) | `shm-runtime` | control plane |
| `TypedRef` 56 B envelope | `shm-store` | typed cross-process reference |
| binary descriptor bus: batched, pipelined, range-compressed | ArrowRef `src/tcp_control.rs` | 63.8 M ops/s measured (M4 Max) |
| host-authoritative pin table + epoch guard | ArrowRef `shared_arena.rs` | the safe-slot discipline |

Measured baselines to design against (M4 Max, `shm-bench` + ArrowRef proof artifacts):
ring 1-way **~83 ns** · publish **504 M/s** · `write_batch` **~80 GB/s** · pin/`as_arrow` flat O(1) ·
in-process `queue_descriptor` **2.3 M tasks/s** · TCP numeric pipelined compressed **63.8 M/s**.

## 2. What is genuinely missing

1. No actor abstraction at all: no `Actor`/`Handler` trait, no typed address, no mailbox, no
   ask/tell, no lifecycle hooks, no supervision, no registry, no placement. `shm-runtime::Node`
   is a *connection handle*, not an actor.
2. No cross-host transport in the substrate (boundary rule: descriptors never leave the host).
3. Memory is coordinator-owned and dies conceptually with it. There is no *adopt*, no
   *snapshot*, no *replicate*, no *promote*. Data-plane survival of coordinator death was proven
   in v0.4 item O — but nothing turns that into a lifecycle.
4. Substrate blockers for actor churn (see §8 Phase 0): `shm-store` catalog is **append-only**
   (`evict` tombstones and never returns a slot), task claim is O(capacity) scan, leases are not
   lifecycle-tied.

---

## 3. Architecture — three planes

```
                 ┌───────────────────────────────────────────────┐
  CONTROL PLANE  │ registry · placement · supervision · leases    │   small, deterministic,
  (no data)      │ epochs/fencing · membership · SQLite kernel+WAL│   itself replicated
                 └───────────────────────────────────────────────┘
                        │ grants (fd = capability)      │ fences (epoch)
                        ▼                               ▼
  EXECUTION PLANE                              MEMORY PLANE
  ┌──────────────────────────┐  descriptors   ┌─────────────────────────────┐
  │ actor procs (fault dom.) │◄──────────────►│ MemoryNode: segments + pool │
  │  N actors / OS thread    │   (64 B env.)  │  cells = RCU/MVCC artifacts │
  │  stateless handlers      │                │  manifest = self-describing │
  │  RAII Pinned<T> guards   │  PROT_READ map │  adopt / snapshot / replicate│
  └──────────────────────────┘                └─────────────────────────────┘
```

**Rule: no user code ever runs in the memory plane.** The memory node is a data structure with
a lifecycle, not a service. That is why it can be adopted by any process that can map its
segments.

**Rule: actors follow memory, never the reverse.** Default placement is `SameHostAs(cell)`.
Zero-copy only pays if compute is co-located with the arena.

### Crate split (proposed)

| Crate | Contents | Est. LOC |
|---|---|---|
| `holon-core` | `Envelope` ABI, `Payload`/`ShmView` traits, `Pinned<T>`, dispatch table, `LocalRef`/`GlobalRef` types | ~1.5 k |
| `holon-mem` | MemoryNode lifecycle: adopt, snapshot, restore, replicate, promote, durability classes, spill floor | ~3 k |
| `holon-actor` | Actor/Handler, `ActorRef<A>`, mailbox, scheduler, ctx, timers, supervision, registry, dead letters | ~3.5 k |
| `holon-net` | multiplexed TCP: envelope lane + payload lane, credit flow control, `GlobalRef` pull-through | ~2 k |
| `holon-macros` | `#[derive(Actor)]`, `#[handler]`, schema-id interning, dense dispatch codegen | ~1 k |

~11 k LOC on top of the existing 26 k. Everything additive; `shm-actors` ABI unchanged except
Phase 0 items.

---

## 4. The Envelope — one cache line, both transports

Extend `TypedRef` (56 B) to a 64-byte, cache-line-sized, `SharedPod` control word:

```rust
#[repr(C, align(64))]
pub struct Envelope {
    to:        ActorId,   // u64  — global actor id (host_id:32 | local:32)
    from:      ActorId,   // u64
    corr:      u64,       // correlation id for ask/reply; 0 = tell
    payload:   RefWord,   // u64  — PackedRef (local) or cell handle (global)
    schema_id: u32,       // interned by the coordinator (FNV-1a of canonical Arrow IPC)
    version:   u32,       // cell version the payload was committed at
    kind:      u16,       // Tell | Ask | Reply | Err | Signal | Stream | Sys
    flags:     u16,       // INLINE_PAYLOAD | GLOBAL_REF | URGENT | NO_REPLY | ...
    deadline:  u32,       // coarse ms since epoch base; lease monitor reaps
    epoch:     u32,       // fencing token of the owning memory node
}
```

**The same 64 B struct is the shm ring slot and the TCP frame header.** Location transparency
falls out of the ABI rather than out of a serialization layer. Only *payload resolution* differs:

- same host → `PackedRef` → mmap + pin → `&T`. **Zero copies.**
- cross host → `GlobalRef{host, mem_node, cell, version}` → resolved by the peer, either from a
  local replica (zero copies) or pulled once and cached as a local cell (**one copy on the wire —
  the theoretical minimum**). `writev`/`sendfile` straight from the mmap avoids the send-side
  user copy.

**Typed invariant enforced by the compiler:** `LocalRef` and `GlobalRef` are *different types*,
and `LocalRef: !Serialize`. A host-scoped descriptor cannot physically be written to a socket.

---

## 5. Rust paradigms this design leans on

This is the part a port of Akka would miss.

**`Pinned<T>` — the borrow checker is the cross-process lifetime enforcer.**

```rust
pub struct Pinned<'p, T: ShmView> { view: T::View<'p>, _pin: VersionPin, _j: JournalGuard }
impl<T: ShmView> Deref for Pinned<'_, T> { type Target = T::View<'_>; }
```

Drop releases the RCU pin *and* the borrow-journal entry. You cannot hold a `&T` into shared
memory past its pin, because the lifetime is tied to the guard — and if the process dies holding
it, journal replay reclaims it. No other language can express this; it is the whole reason to
build this in Rust rather than wrap iceoryx2.

**Typestate for the cell write path.** `Cell<Uninit> → Cell<Staged> → Cell<Committed>`;
`commit()` consumes the staged token. `shm-stream` already implements the runtime; make the
states types so a forgotten commit is a compile error, not a leaked LOANED chunk.

**Zero-`dyn` dispatch.** `#[derive(Actor)]` generates a dense `schema_id → fn(&mut A, Envelope)`
jump table at compile time. No `Box<dyn Any>`, no downcast, no vtable in the hot path — the
dispatch cost is a bounds check and an indirect call.

**Honest `Send`/`!Send`.** Actor state is `!Send` and pinned to its thread; only `Envelope`
(a `SharedPod`) crosses threads and processes. `ActorRef<A>: Send + Clone`, and its `Handler`
bounds are checked at the call site, so `tell` of a message the actor cannot handle does not
compile.

**Payloads: three shapes, one trait.** `Payload` is implemented for (a) Arrow `RecordBatch`
(the existing `shm-arrow` path), (b) `#[repr(C)] SharedPod` structs (control messages, zero
decode), (c) `rkyv` archived types (arbitrary Rust structs, zero-copy read). Schema id is
interned once at startup by the existing coordinator.

**API sketch:**

```rust
#[derive(Actor)]
#[holon(mailbox = 4096, overflow = Backpressure, placement = SameHostAs(self.curve))]
struct Pricer {
    curve: CellRef<CurveTable>,          // durable state, in the memory plane
    hits:  u64,                          // local scratch, lost on restart, by design
}

#[handler]
impl Pricer {
    async fn on_price(&mut self, req: Pinned<'_, PriceRequest>, cx: &mut Cx<Self>)
        -> Result<Reply<PriceResponse>>
    {
        let curve = cx.pin(&self.curve)?;              // RAII, zero-copy, version-checked
        let out   = price(&*req, &*curve);             // plain Rust, plain references
        cx.reply(out)                                   // commits into a cell, returns a ref
    }

    async fn on_start(&mut self, cx: &mut Cx<Self>) -> Result<()> { /* reattach, no rehydrate */ }
    async fn on_stop(&mut self, why: StopReason)      -> Result<()> { Ok(()) }
}

let pricer: ActorRef<Pricer> = sys.spawn_named("risk/eu/pricer", Pricer::new(curve))?;
let resp = pricer.ask(PriceRequest { .. }).await?;      // typed, zero-copy both directions
```

---

## 6. Mailbox — envelopes in the ring, payloads in cells

`shm-ring` has no pin, so a lapping producer races a mid-copy reader. Do **not** add a pin to the
ring; split the concerns instead:

- **Mailbox = MPSC ring of 64 B envelopes**, per-actor doorbell (futex on Linux — the word is
  already reserved in the ring padding; 1-byte UDS write elsewhere). Lock-free, fixed size.
  Lapping loses an *envelope*, never tears a payload.
- **Payload lives in a refcounted cell**, pinned by the sender until the receiver's dequeue pin
  lands. That handshake is exactly the existing v0.3 hazard protocol.
- **Overflow policy is per-mailbox and always observable**: `Backpressure` (producer parks —
  `would_backpressure` exists) or `DropOldest` + a `Lagged{n}` signal delivered to the actor.
  Silent loss is forbidden (invariant I6).
- **Ask/reply**: `corr` slot + oneshot parked on the doorbell; `deadline` reaped by the existing
  lease monitor, so a dead peer times out rather than hanging.

Target: enqueue → handler entry **< 200 ns** same-host — aspirational until a native Linux
futex wake is measured. Busy-poll ring 1-way is ~83 ns, but the parked POSIX wake is ~4.4 µs
(measured), and a parked futex wake is a syscall plus a scheduler decision. The property to
design for is a wake cost flat across thousands of idle actors; 500 ns–1 µs with that shape is
a good result.

---

## 7. The memory node — the crown jewel

A `MemoryNode` is an addressable, versioned arena with a lifecycle independent of any process.

**Composition:** segment set (file/`memfd` backed, named by stable `mem_node_id`) + `Pool` +
catalog of cells (each an RCU/MVCC `shm-artifact`) + the **self-describing manifest**
(`SHMMFST4`, chained per ADR-0013 — each manifest lists its own chunks with a per-batch span table and links to its predecessor — self-validating `{artifact_id, version}`).

The manifest is the load-bearing part: **any process that can map the segments can reconstruct
the full catalog with no help from the owner.** That property, already built in v0.3, is what
makes every guarantee below cheap.

### 7.1 Survive-in-place (owner death, no copy)

1. Control plane detects owner death (lease miss / `pidfd`).
2. It **fences** the epoch, so a zombie owner's late commit fails with `Fenced` (v0.3 write-lease
   mechanism, generalised from artifact to node).
3. A successor process maps the same segments and runs `recover()`: replay borrow journals →
   free orphaned pins; drop `LOANED`/staged chunks → no partial versions; validate manifest →
   committed versions intact.
4. Registry re-points `CellRef`s; actors continue at the last committed version.

**RPO = 0 for every committed version. No copy, no serialization, no rehydrate.** This is the
common case and it is fast — bounded by journal size, not arena size.

### 7.2 Snapshot without quiescing (the "it can be copied" guarantee)

Because cells are RCU/MVCC, a snapshot is: pin the current version of every cell, then stream
the pinned chunks out. **Writers keep committing new versions while the snapshot streams the
pinned ones.** No lock, no stop-the-world, no fork/COW trick.

Sinks: another memory node (same host = share segments; cross host = `holon-net` bytes), a file
(Arrow IPC / Parquet — ArrowRef's existing writers), or object storage. Result is a consistent
**version cut**, not a smear, because each cell contributes exactly the version pinned at cut
time and a manifest records the cut.

### 7.3 Replication and promotion

Every commit already emits a `VersionEvent`. A follower memory node applies that stream:
same-host followers share chunks by refcount (free); cross-host followers receive bytes.

- `Volatile` — RAM only. Survives process death (§7.1); lost if the host dies.
- `Replicated{n}` — commit returns only after n memory nodes acknowledge. Survives host loss.
- `Durable` — commit is fsync'd to the descriptor WAL first. Group-commit is already implemented
  and measured (145/s serial → 582/s with a 1 ms commit-delay window), so this is explicitly the
  expensive class, opt-in **per cell**, never global.

Promotion = control-plane epoch bump + registry re-point. The epoch is the split-brain fence:
the demoted leader's commits fail, they do not diverge.

### 7.4 Tenancy = capability

Per-tenant segments granted by fd over `SCM_RIGHTS`. An actor process **cannot name, let alone
map**, another tenant's arena — isolation by construction rather than by check. Consumers map
`PROT_READ`, so a buggy consumer physically cannot corrupt the arena it reads.

---

## 8. Invariants

- **I1** One copy in, zero after (same host). One copy on the wire (cross host).
- **I2** Descriptors are host-scoped; envelopes are global. Enforced by types (`LocalRef: !Serialize`).
- **I3** State outlives actors; memory nodes outlive processes; only host loss is fatal, and only
  to `Volatile` cells.
- **I4** A pin is an RAII guard. A raw descriptor never appears in user code.
- **I5** Commit is a CAS on `(cell, expected_version)`. Concurrent writers **conflict**, never corrupt.
- **I6** Every mailbox overflow is observable (`Lagged`), never silent.
- **I7** No user code runs in the memory plane.
- **I8** At-least-once delivery + idempotent version-CAS commit = **effectively-once** effects.
  Exactly-once delivery is not offered, because it cannot be.

---

## 9. Prior art and the delta

| System | State | Message | Failure | Delta |
|---|---|---|---|---|
| Erlang/OTP | in actor heap | **copied** per send | supervisor restarts from `init` — state gone | we keep state, no copy |
| Akka / Actix / Ractor | in actor heap | copied; serialized on remoting | same | same |
| Orleans | virtual actors + storage provider | serialized | rehydrate from storage (slow) | we reattach to a live version |
| Ray / Plasma | **object store** (closest ancestor) | refcounted shm objects | object store loss = task replay | we add versioning, replication, adoption, supervision |
| iceoryx2 | none (pub/sub only) | true zero-copy | n/a | we add the state + actor model it deliberately omits |

The combination — an object store that is *versioned and adoptable*, actors that are *stateless
and supervised*, and a borrow checker that *enforces cross-process pins* — is unoccupied.

---

## 10. Implementation workflow

Each phase ends at a **proof gate** in the house style (loom on lock-free cores, miri on ABI
parsers, `cargo-fuzz` on wire decoders, `kill -9` matrix with an exact zero-leak pool census,
`shm-bench` numbers on a quiet host, one ADR per phase). Method as established: Fable writes the
ADR from a tight self-contained brief; Opus agents implement; sequential on one workspace to
avoid concurrent-cargo races.

### Phase 0 — substrate prerequisites (in `shm-actors`, additive, ABI-stable)

These are blockers, not nice-to-haves. An actor system creates and destroys addresses constantly.

- **P0.1 `shm-store` catalog slot reclamation.** Generation-tagged free list; `evict` returns the
  slot. Today `store_capacity` is a cap on *cells created for the coordinator's lifetime* —
  under actor churn that is a guaranteed outage. **Hard blocker.**
- **P0.2** Task claim O(1) (ADR-0005 item S — was an O(capacity) scan). **Done — ADR-0009**:
  FREE/READY Treiber index stacks; claim order is LIFO, so revisit the READY structure (a
  Vyukov-style seq ring behind the same seam) before Phase 1 freezes mailbox FIFO semantics.
- **P0.3** Lifecycle-tied leases + `evict-current` (G4/G12). **Done — ADR-0010**:
  `evict_current` = empty Replace commit; entry-tied write leases
  (`evict_all` force-releases); task-tied retained-ref bindings in a
  `SHMTASK3` lease side table, ack/reap released.
- **P0.4** Linux fast paths (ADR-0004 item H): futex doorbell (word pre-reserved), `eventfd`,
  `pidfd` death detection, `memfd` sealing, `CLOCK_MONOTONIC`. **Done — ADR-0011**: one
  cfg(linux) module behind the `Platform` seam; futex hooks on the reserved words
  (`FutexNotifier`/`FutexParker` + `doorbell_word()` accessors), eventfd behind the existing
  `doorbell_pair`, sealed-memfd coordinator segments, pidfd-accelerated lease monitor,
  monotonic `now_nanos`. Verified by cross-target check/clippy + the full suite executed in a
  Linux container (`scripts/linux-check.sh` / `linux-test.sh`); native-Linux perf unmeasured.
- **P0.5** Ratify the **envelope-only ring doctrine** in an ADR (cheaper and safer than adding a
  pin to `shm-ring`; §6).

*Gate:* existing 147 workspace tests + loom + miri + TSan + kill-9 census still green; no ABI
size assertion changes.

### Phase 1 — `holon-core`: envelope + dispatch

64 B `Envelope` ABI, `Payload`/`ShmView` traits, `Pinned<T>` RAII, `LocalRef`/`GlobalRef` types,
dense dispatch table, schema-id interning through the existing coordinator.

*Gate:* miri-clean ABI parse; `cargo-fuzz` target on the envelope decoder (the existing fuzzing
found 2 real OOB bugs — expect the same here); loom on the mailbox handshake; bench: envelope
round-trip vs the 83 ns ring baseline.

### Phase 2 — `holon-actor` (single process) + `holon-macros`

Actor/Handler traits, `ActorRef<A>`, tell/ask/stream, MPSC mailbox + doorbell, per-thread
scheduler with work stealing, `Cx`, timers, `#[derive(Actor)]` / `#[handler]`.

*Gate:* ping-pong and payload benches against Ractor and Actix. Targets: within 2× on 64 B
messages (they have no descriptor overhead to amortise), **order-of-magnitude ahead above ~4 KiB**
payloads, and flat with payload size where they are linear.

### Phase 3 — cross-process actors + supervision

Registry (`tenant/project/env/name`, reusing `FabricTenant::prefix`), placement
(`SameHostAs(cell)` default), supervision trees (OneForOne / OneForAll / RestForOne / Escalate +
restart-intensity windows), dead letters, **poison-message quarantine** (an envelope that kills a
handler N times is moved to a dead-letter cell with crash context — mandatory here, because a bad
payload in shared memory can otherwise kill every consumer in a loop).

*Gate:* `kill -9` matrix — kill mid-handle, kill the supervisor, kill mid-`ask`, kill the sender
between pin and enqueue — each with an exact zero-leak pool census; restart-storm soak.

### Phase 4 — `holon-mem`: the memory-node lifecycle

`adopt()` (§7.1), `snapshot()`/`restore()` (§7.2), durability classes, spill floor, epoch fencing
generalised from artifact to node.

*Gate:* kill the owner under sustained write load → successor adopts → committed set
byte-identical → zero leak; measure adopt latency vs arena size (must be journal-bound, not
arena-bound). Snapshot-under-write: writers running throughout, snapshot must be a consistent
version cut, verified by a differential replay.

### Phase 5 — `holon-net`: the TCP plane

Multiplexed envelope lane + payload lane (large frames must not head-of-line-block envelopes),
credit-based flow control per lane, batching/pipelining and range compression lifted from
`tcp_control.rs`, `GlobalRef` resolution with **pull-through caching** into the local memory node
(the L1/L2 idea, generalised — a second reference to a remote cell is free).

*Gate:* cross-host ping-pong latency and throughput vs raw TCP floor; fault injection
(partition, delay, reorder, half-open); a fuzzed wire decoder; assert I2 — no `LocalRef` can be
serialized (a compile-fail test, `trybuild`).

### Phase 6 — replication and failover

Follower memory nodes applying the commit stream, the `Replicated{n}` commit path, promotion with
epoch fencing, actor re-resolution after promotion.

*Gate:* kill the leader host mid-commit → no split-brain (demoted leader's commits return
`Fenced`) → RPO = 0 for `Replicated` cells; measure failover time and the commit-latency cost of
n = 2, 3.

### Phase 7 — re-front ArrowRef onto Holon

Reimplement `arrowref-shm-substrate`, `arrowref_tasks::shm_fabric`, and
`arrowref_messaging::shm_topics` as Holon actors. Keep the existing flag shape:
`task_fabric = legacy | shm | actors`, default unchanged.

*Gate:* the existing differential harness (legacy ≡ actors) green; 455/455 ArrowRef tests on the
actors path; bench parity or better on `queue_descriptor`, `arrow_ref_task`, and the numeric TCP
lanes.

---

## 11. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | Append-only catalog exhausts under actor churn | Phase 0.1, hard blocker, done first |
| R2 | One bad payload kills every consumer repeatedly | poison quarantine (Phase 3) + bounds-checked readers (already fuzz-hardened) |
| R3 | Shared memory = shared fate for corruption | consumers map `PROT_READ`; per-tenant fd capability; `memfd` sealing on Linux |
| R4 | A host-scoped descriptor leaks cross-host | type-level: `LocalRef: !Serialize`, `trybuild` compile-fail test |
| R5 | Scope creep into distributed consensus | control plane stays single-writer SQLite + fencing epochs; no Raft, ever, in this lib |
| R6 | macOS lacks futex / `pidfd` / `memfd` | `Platform` seam already exists — develop on macOS, measure on Linux, CI both |
| R7 | The framework becomes ArrowRef-shaped | Phase 7 is *last*; Phases 1–6 must be provable with a non-ArrowRef demo (a chat/matching-engine sample) |

---

## 12. Open questions for the owner

1. **Name.** "Holon" (a whole that is also a part) is a placeholder — it maps well onto
   memory nodes but the repo has a naming style worth respecting.
2. **Async or sync handlers?** The sketch shows `async fn`. A sync-first core with an async
   adapter is faster and simpler; async-first is more ergonomic for I/O-bound actors. Recommend
   **sync core + `async` opt-in per actor**.
3. **Does Holon live in the `shm-actors` workspace or its own repo?** Recommend the same
   workspace — it shares the loom/miri/fuzz CI, and the crates are published to `kellnr` anyway.
4. **Is cross-host (Phases 5–6) actually needed for the first product use, or is same-host
   multi-process the shipping target?** If same-host, Phases 5–6 defer and the timeline halves.
