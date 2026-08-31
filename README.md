# Holon / shm-actors

**Actors own no state. Memory owns no code.**

**Holon** is a zero-copy actor framework for Rust: 64-byte envelopes over a
lock-free mailbox, payloads in versioned Apache Arrow cells, crash recovery by
lease + journal replay. It stands on **shm-actors**, the shared-memory
substrate this repo also contains — one copy in, zero copies after.

[![ci](https://github.com/jamiepearcey/shm-actors/actions/workflows/ci.yml/badge.svg)](https://github.com/jamiepearcey/shm-actors/actions/workflows/ci.yml)
[![site](https://github.com/jamiepearcey/shm-actors/actions/workflows/site.yml/badge.svg)](https://jamiepearcey.github.io/shm-actors/)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

The substrate writes a payload once into shared memory; everything downstream —
pub/sub, tasks, streams, versioned tables — moves a **24-byte descriptor**.
Safety is Rust's borrowing extended across process boundaries: exclusive loans
for writers, pinned shared views for readers, and crash reclamation by journal
replay that provably leaks nothing.

**Website & docs: <https://jamiepearcey.github.io/shm-actors/>** — the guide,
every benchmark, and all sixteen ADRs.

## The numbers (measured, not promised)

| claim | measured |
|---|---:|
| ring hop, cross-process, p50 | 83.5 ns |
| version pin, 1 → 10 000 versions | ≤ 1 tick |
| zero-copy table read, 1 k → 1 M rows | 208–250 ns, flat |
| append commit, version 0 → 100 000 | 1.4–1.6 µs, flat |
| stream delta read, any history | ~290 ns, flat |
| actor ask round trip (parked / spinning) | 9.5 µs / 1.2 µs |
| messages lost across a `kill -9` mid-message | 0 |

Apple M4 Max, macOS dev profile, release builds, ≥ 2 runs. “≤ 1 tick” = at the
41.7 ns timer quantum. No comparison baseline against other IPC systems has
been run, so no “N× faster than X” is claimed. Reproduce with
`cargo run -p shm-bench --release -- all` — the suite prints its own hardware
profile first, and the shapes that *punish* the design ship in it.

## Model

- **Coordinator** — control plane only. Owns the registry (topics, artifacts,
  actors, leases), passes segment fds over a Unix socket (`SCM_RIGHTS`), and
  drives crash reclamation. Never on the data path: if it dies, existing
  channels keep flowing.
- **Actors** — ordinary Rust processes that map the granted segments, then
  message through shared memory with no hot-path syscalls.

Four primitives over one substrate: **pub/sub** (SPMC broadcast ring),
**tasks** (MPMC queue, O(1) claim, leases + redelivery), **streams**
(transactional multi-batch commits), **artifacts** (RCU-versioned Arrow tables
with chained manifests, windowed append, and exact delta reads).

On top: **Holon**, the actor layer — actors own no state, memory owns no code.
A 64-byte envelope (one cache line, frozen ABI) rides the task queue; handlers
pin the exact cell version their message names; a supervisor-injected
`kill -9` mid-message recovers in lease + one tick with zero losses and a pool
census that balances to the chunk.

## Quick start

```sh
cargo build && cargo test --workspace       # the adversarial test suite
cargo run -p shm-bench --release -- all     # measured numbers on your machine
cargo run -p holon-demo --release -- bench  # actors, throughput, the crash row
```

## Workspace

| Crate | Responsibility |
|---|---|
| `shm-core` | Segments, size-class pools, `ChunkCtrl` state machine, borrow journal, platform seam |
| `shm-ring` | SPMC broadcast ring + subscriber cursors |
| `shm-arrow` | Arrow buffer layout over chunks, zero-copy `RecordBatch` views, schema interning |
| `shm-artifact` | Versioned tables: RCU head, chained manifests, pin/commit, windowed append, delta read |
| `shm-task` | MPMC task queue: O(1) claim, leases, redelivery, output binding |
| `shm-stream` | Transactional multi-batch writers |
| `shm-store` | Keyed store: catalog, typed refs, entry lifecycle |
| `shm-runtime` | Coordinator + actor host: fd passing, leases, crash reclamation |
| `holon-core` / `holon-actor` / `holon-demo` | The actor layer and its measured end-to-end demo |
| `shm-bench` | The measured-numbers suite |

## Why trust it

239 tests that attack (lineage races, crash replays, exact-census invariants),
4 `loom` models of the lock-free protocols — shown to fail against broken
orderings — 5 fuzz targets on every untrusted-input parser, a hostile
`kill -9` churn loop, and frozen, `const`-asserted ABIs. Every capability
landed through an [ADR](docs/decisions/) with measured evidence; start with
[ADR-0001](docs/decisions/ADR-0001-foundation-and-scope.md) and read forward.

## Platform

Dev is macOS on a portable POSIX baseline (`shm_open` + `mmap`, UDS doorbells,
coordinator leases). Production target is Linux; the futex/memfd fast paths
are built behind a platform seam (clippy-clean cross-target) and honestly
unmeasured until they run on a Linux box. Pinned stable toolchain; fuzzing on
nightly.

## License

[Apache-2.0](LICENSE).
