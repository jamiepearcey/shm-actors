# ADR-0001 — Foundation, build-vs-adopt, and v0.1 scope

- Status: Accepted
- Date: 2026-07-21
- Decider: architecture (Fable 5), transcribed by implementation

## Context

`shm-actors` is a zero-copy cross-process actor system in Rust, Arrow-first,
intended to become the substrate underneath a more stable rewrite of the
existing **ArrowRef / query-cache** product (`infrastructure/query-cache`).

The design (design doc v0.1) specifies four primitives — pub/sub, tasks,
streams, artifacts — over a shared-memory arena of `#[repr(C)]` POD chunks
referenced only by 24-byte descriptors, with cross-process borrowing modelled
on Rust's own discipline (exclusive `Loan`, shared `Sample`), and crash
reclamation driven by leases + a per-actor borrow journal + generation
counters.

Two prior-art inputs were weighed:

- **iceoryx2** (Apache-2.0, pure Rust) already implements decentralized shm
  pub/sub, loan/publish zero-copy, fixed-pool allocation, crash-tolerant
  reclamation, and request/response — but has no streams-with-commit, no
  versioned RCU artifacts, no Arrow-native layout, and no actor runtime.
- iceoryx2's **blackboard** pattern (its closest analogue to our Artifact) is
  single-writer/multi-reader key-value for *small* global state, per-key
  updates. It is explicitly **not** for large payloads and has **no**
  versioning/RCU, no arbitrary schemas, no Arrow, no transactional commit — so
  it does not cover our Artifact requirements.

## Decision

1. **Build the substrate from scratch; use iceoryx2 as an oracle, not a
   dependency.** All four primitives share one load-bearing contract —
   `ChunkDesc` + generation + `ChunkCtrl` refcounts — that artifacts, streams,
   and pub/sub must all speak so chunks can be shared across versions and
   topics. Adopting iceoryx2 would split the system into two memory models
   (theirs for transport, ours for artifacts), and its loan/publish owns
   allocation in ways that block chunk-level RCU sharing. iceoryx2 remains a
   design/test oracle for reclamation edge cases.

2. **Five crates for v0.1** (down from the proposed eight):
   - `shm-core` — POD/`SharedPod`, segments, size-class pools, `ChunkCtrl`,
     borrow journal, platform seam. (Absorbs the proposed `shm-sync`.)
   - `shm-ring` — SPMC broadcast ring + subscriber cursors. (Absorbs `shm-pubsub`.)
   - `shm-arrow` — Arrow buffer layout over chunks, zero-copy `RecordBatch`
     views, schema interning.
   - `shm-artifact` — `VersionManifest`, pin/commit, RCU version head.
   - `shm-runtime` — coordinator + actor host, UDS/fd-passing, leases.

   **Deferred to v0.2:** `shm-task` (MPMC task ring) and `shm-stream`
   (transactional commit — it is staged-manifest-install *on* artifacts, so
   artifacts land first).

3. **First vertical slice (walking skeleton):** Coordinator + two actors. The
   coordinator grants one segment over UDS/`SCM_RIGHTS`; the producer loans a
   chunk, writes a `RecordBatch` via `shm-arrow`, and publishes the descriptor
   on one SPMC topic; the consumer takes a `Sample`, reconstructs the
   `RecordBatch` zero-copy via `Buffer::from_custom_allocation`, and drops the
   pin. Then `kill -9` the consumer while pinned and verify lease expiry +
   journal replay frees the pin, and that a stale descriptor read returns
   `Err(StaleDescriptor)`. This exercises every invariant: segments, pools,
   loan/publish, ring, generations, Arrow layout, crash reclaim.

4. **Platform seam from day one; Linux defines semantics, POSIX is the
   baseline.** One `Platform` trait in `shm-core` (segment create/map, doorbell
   wakeup, death detection). v0.1 assumes only the POSIX baseline:
   `shm_open` + `ftruncate` + `mmap`; doorbell = one-byte UDS write; death
   detection = **coordinator leases** (not `pidfd`). Sealing, `futex`,
   `eventfd`, and `pidfd` are Linux hardening/fast-path specializations landed
   behind the seam in v0.2 — correctness must never depend on them.

5. **Borrow journal = fixed-slot bitmap.** A fixed table of N `ChunkDesc` slots
   plus an occupancy bitmap per actor (N configurable at registration, default
   1024). O(1) crash replay, pure POD, and the bounded pin count is a feature
   (natural backpressure, hard bound on reclamation work). An append log is
   rejected: unbounded shm growth and O(n) replay in the crash path.

6. **Clean-room standalone workspace.** `shm-actors` is a new sibling folder
   (`infrastructure/shm-actors`); ArrowRef will later consume it as a path/git
   dependency. No ArrowRef types leak in — it must stand as an Apache-2 substrate
   on its own. It does adopt ArrowRef's chaos-test discipline (abort injection)
   from day one.

## Consequences

- The descriptor/generation/ChunkCtrl ABI in `shm-core` is the keystone; it is
  frozen early and every other crate speaks it.
- macOS (dev) is a correct-but-slower profile; production Linux gets the fast
  paths later without changing semantics.
- v0.2 adds `shm-task`, `shm-stream`, and the Linux fast-path specializations.
