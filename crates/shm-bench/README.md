# shm-bench

A first-class, committable performance harness for the `shm-actors` substrate.
Every number it prints is **measured on the machine it runs on** (warmup + timed
loop + percentiles); nothing is extrapolated or fabricated.

## Run

Always build **outside** the repo tree and in **release**:

```sh
CARGO_TARGET_DIR=/tmp/shm-actors-bench-target \
  cargo run --release -p shm-bench -- [suite]
```

`suite` is one of `xproc`, `ring`, `pool`, `artifact`, `arrow`, `task`, or
`all` (default). With `all`, the fork-based cross-process bench runs **first**,
before any bench thread is spawned, so the child inherits a quiescent
single-threaded address space.

## What each suite measures

| suite      | metrics |
|------------|---------|
| `xproc`    | cross-process (fork + `shm_open`/`Ring::attach` re-map) busy-poll one-way latency (RTT/2, single-clock ping-pong) and streaming throughput |
| `ring`     | in-process busy-poll latency (same-core hot path + cross-core RTT/2), single-producer publish throughput, 1→4-consumer broadcast throughput, and parked-doorbell (`poll(2)`) wakeup latency |
| `pool`     | `Pool::alloc` / `Pool::free` ns per op and ops/sec (amortized over batches of 4096 to beat the timer floor) |
| `artifact` | `pin()+drop` latency vs committed-version count (O(1) check), `as_arrow()` zero-copy reconstruct vs row count (O(1) check), and commit latency for `Replace` vs `Append` vs table depth |
| `arrow`    | `write_batch` "one copy in" GB/s and rows/s vs size, and `read_batch` zero-copy reconstruct vs row count |
| `task`     | `submit→claim→complete→poll` round-trip latency and pipelined multi-worker throughput |

## Methodology

- Latency stats (min / p50 / p99 / max / mean, all ns) come from a warmup phase
  followed by a timed loop; each iteration is bracketed by `Instant::now()` and
  the result fed through `black_box`. Percentiles are nearest-rank on the sorted
  sample. See `src/stats.rs`.
- The clock is `std::time::Instant` (mach_absolute_time on Apple Silicon). Its
  read resolution is ~40 ns here, so any p50 that prints `0.0` (e.g. the
  same-core ring hot path) is **below timer resolution**, not literally zero.
- Sub-100 ns ops (pool alloc/free) are timed as batches of 4096 and divided, so
  the per-op figure is not dominated by the `Instant` read overhead.
- Cross-process and cross-core latency use a **ping-pong** and report RTT/2, so
  there is no cross-process/cross-thread clock-sync problem — the round-trip is
  timed entirely by one clock in one thread.

## Caveats

- **macOS dev profile, NOT the Linux target.** This host uses the POSIX baseline
  (`shm_open` + `poll`/pipe doorbell), with no futex or memfd sealing. The
  design's production target is a Linux x86/ARM server; the busy-poll and
  (especially) the microsecond-scale doorbell numbers should not be read as the
  Linux target.
- Always run `--release`; a debug build prints a warning and is not
  representative.
