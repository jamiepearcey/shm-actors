# shm-actors

A zero-copy cross-process actor system in Rust, with Apache Arrow as the
first-class payload format. Actors are ordinary Rust processes that communicate
through shared memory: **one copy in, zero copies after** — payloads are written
once and everything downstream moves a 24-byte descriptor.

`shm-actors` is being built as the substrate underneath a more stable rewrite of
the **ArrowRef / query-cache** retained-execution fabric. See
[`docs/decisions/ADR-0001`](docs/decisions/ADR-0001-foundation-and-scope.md) for
the build-vs-adopt ruling and v0.1 scope.

## Model

- **Coordinator** — control plane. Owns the registry (topics, artifacts, actors,
  leases), never touches the data path, passes segment fds over a Unix domain
  socket (`SCM_RIGHTS`), and drives crash reclamation. If it dies, existing
  channels keep flowing; only new registrations and reclamation pause.
- **Actors** — ordinary processes that map granted segments and then message
  through shared memory with no hot-path syscalls.

Four primitives, all over the same substrate: **pub/sub**, **tasks**,
**streams**, **artifacts**. Safety comes from extending Rust's borrowing across
processes: exclusive `Loan`s for writers, shared pinned `Sample`s for readers,
reclamation driven by leases + a per-actor borrow journal + generation counters.

## v0.1 crates

| Crate          | Responsibility |
|----------------|----------------|
| `shm-core`     | POD/`SharedPod`, segments, size-class pools, `ChunkCtrl`, borrow journal, platform seam |
| `shm-ring`     | SPMC broadcast ring + subscriber cursors (pub/sub) |
| `shm-arrow`    | Arrow buffer layout over chunks, zero-copy `RecordBatch` views, schema interning |
| `shm-artifact` | `VersionManifest`, RCU version head, pin/commit |
| `shm-runtime`  | Coordinator + actor host, UDS/fd-passing, leases, the walking-skeleton example |

`shm-task` (MPMC task ring) and `shm-stream` (transactional commit) are deferred
to v0.2.

## Platform

v0.1 targets a portable POSIX baseline (`shm_open` + `mmap`, doorbell = one-byte
UDS write, death detection via coordinator leases) behind a `Platform` seam.
Linux fast paths (memfd sealing, futex, eventfd, pidfd) land in v0.2 without
changing semantics. Dev is macOS; production is Linux.

## Build

```sh
cargo build
cargo test
```
