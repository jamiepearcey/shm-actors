# ADR-0015 — The Holon demo: one message, one mailbox, one handler, one pinned cell

- Status: Accepted; implemented (`holon-core`, `holon-actor`, `holon-demo`), numbers below
- Date: 2026-08-29
- Builds on: everything in Phase 0 (ADR-0008..0014).

## Decision

Build the **smallest actor layer that proves the thesis end-to-end, across
processes, under `kill -9`, with a measured number** — and nothing else. The
architecture document's own risk R7 and the external review both say Phases
1–6 are optional expansion until this exists.

The thesis being demonstrated: *actors own no state, memory nodes own no
code.* Concretely: a `Pricer` actor process handles `PriceRequest` messages
against a `curve` cell that lives in the keyed store. Kill the pricer with
`SIGKILL` mid-stream; a successor process attaches to the **same** cell and
continues serving the **same** clients, whose in-flight asks are redelivered
(at-least-once) by the task queue's lease reap. No copy, no rehydrate: the
state never moved.

## What is reused (the demo introduces no new substrate)

| Need | Substrate |
|---|---|
| mailbox with exactly-once claim, lease reap, parked wake | `shm-task` (sharded stacks, futex doorbell word) |
| 64-byte envelope that rides a 24-byte descriptor | envelope-in-a-chunk, the ADR-0007 G1 pattern |
| state cell, versioned, pinned zero-copy | `shm-store` `Entry` + `resolve_and_pin` |
| ask/reply | `submit` → `wait` on the `TaskHandle`; reply is the task's result descriptor |
| process death → redelivery | the coordinator's lease monitor + `reap` |
| crash reclaim of pins/envelopes | borrow journal replay (ADR-0014 election) |

The honest deviation from the design document: the design's mailbox is a
dedicated ring whose **slot is** the 64-byte envelope. That ring does not
exist yet. The demo pays one 64-byte chunk allocation per message to carry the
envelope through the task queue's `ChunkDesc`. Its cost is measured and
reported, not hidden; building the envelope ring is Phase 1 proper.

## Crates

- **`holon-core`** — `Envelope` (64 B, `#[repr(C, align(64))]`, `SharedPod`,
  the ADR field set: `to, from, corr, payload, schema_id, version, kind,
  flags, deadline, epoch`), `ActorId`, `MessageKind`, `Payload` for
  `#[repr(C)]` POD messages, `LocalRef` (the host-scoped payload descriptor;
  deliberately *not* `Serialize`), dense dispatch: `schema_id → handler`.
- **`holon-actor`** — `Actor` trait (`fn handle(&mut self, msg: &Envelope,
  cx: &mut Cx) -> Reply`), `ActorSystem` (owns a `Node`, its task queue and
  store; hosts **any number of actors of any types**, `spawn`ed by name and
  routed by the envelope's `to` — a binary search over the sorted host table,
  one vtable call at the actor boundary, each actor keeping its own zero-`dyn`
  schema table; `run` = `claim_blocking` loop → decode envelope → route by
  `to` → dispatch by `schema_id` → write the reply chunk → complete), `ActorRef`
  (`tell`, `ask` = submit + wait; `ask` allocates the envelope chunk and the
  reply chunk), `Cx::pin(cell)` → `Pinned` (RAII over `VersionPin`), `CellRef`
  by key. Routing is by `to`, not by schema: the same schema id may mean
  different things to different actors, a schema an actor does not accept
  fails the ask, and so does a `to` nobody hosts.
- **`holon-demo`** (binary) — roles `coordinator`, `curve-publish`, `pricer`
  (hosts two actors over its one mailbox: `pricer` → `PriceReply`, and `risk`
  → `RiskReply`, the DV01 off the same pinned `curve` cell), `client`
  (`--mix` alternates asks between the two by `to` and verifies every DV01
  against the closed form `−P·sinh(t·1e−4)`), `supervisor` (spawns `pricer`,
  restarts it on exit), plus `--kill-after <n>` on the pricer for the crash
  demo.

## The numbers the demo must print

1. `ask` round trip, client ↔ pricer in different processes, pricer **parked**
   on the doorbell (the honest path): p50 / p99 / max, N ≥ 100 000.
2. The same with the pricer busy-polling, for the floor.
3. Throughput: 1 client / 1 pricer; 4 clients / 1 pricer; 4 / 4; and (3b)
   4 clients alternating between two actors hosted in one process, to price
   the routing.
4. The crash: time from `SIGKILL` of the pricer to the first reply from its
   successor, and the count of asks redelivered vs lost (must be 0 lost).
5. The zero-leak census after the crash (pool free count back to baseline).

Every number on a quiet machine, ≥ 2 runs, reported with its spread. The
wake path is macOS `poll(2)` here — the futex doorbell is Linux-only and
unmeasured — so the parked number is stated as the POSIX baseline.

## Gate

`cargo test --workspace` stays green (191 → 191 + the demo's tests), clippy
clean on macOS and cross-target Linux, and one multi-process test that runs
the crash scenario with an exact census. No loom models: the demo adds no
lock-free protocol, and the claim is that it needs none.

## Measured (macOS, `poll(2)` pipe doorbell; the POSIX baseline, not the Linux futex target)

`cargo run --release -p holon-demo -- bench` (n = 100 000 asks per configuration,
2 runs, crash run n = 2 000 with `--kill-after 50`, lease 500 ms). Apple
silicon dev machine, quiet but not isolated.

| # | number | run 1 | run 2 |
|---|--------|-------|-------|
| 1 | ask round trip, pricer **parked** (client ↔ pricer, separate processes): p50 / p99 / max | 9.6 / 21.6 / 312.5 µs | 9.4 / 18.9 / 130.5 µs |
| 2 | same, pricer **and** client busy-polling (the floor): p50 / p99 / max | 1.2 / 2.2 / 60.8 µs | 1.2 / 2.1 / 50.7 µs |
| 3 | throughput, parked: 1 client/1 pricer · 4/1 · 4/4 | 99.0 K · 146.8 K · 168.1 K asks/s | 105.3 K · 160.2 K · 185.7 K asks/s |
| 3b | two actors in **one** process (`pricer` + `risk`), 4 clients alternating by `to`: throughput · p50 — vs the single-actor 4/1 row of the same run | 126.8 K · 13.0 µs — vs 122.2 K · 12.1 µs | 98.9 K · 11.7 µs — vs 98.7 K · 11.7 µs |
| 4 | crash: `_exit(137)` mid-handle (claim + journaled pin held) → first reply from the successor; redelivered / lost | 521.9 ms; 1 / 0 | 542.6 ms; 1 / 0 |
| 5 | census after the crash (store-pool free chunks, baseline with curve = 190) | 190, zero leak | 190, zero leak |

Final census after evicting the curve: 192 = the empty baseline, both runs.
Row 3b (a later run than rows 1–5's; its own 4/1 control is in the row) prices
the routing: 50 000 of the 100 000 asks went to `risk`, every DV01 verified
client-side, 0 errors, and the `to → host` binary search + one vtable call is
within run-to-run noise of the single-actor mailbox (p50 +0.9 µs / +0.0 µs;
throughput +4 % / +0.3 %). The p99 of the mixed run (306 / 712 µs) was not
compared against a single-actor p99 in the same run.
The crash number is the lease (500 ms) plus one monitor tick: the successor is
spawned ~0.3 ms after the death, connects and parks, and the reap redelivers
the in-flight ask when the dead claim's lease lapses — at-least-once, no copy
of the curve, the successor pinned the same cell version.

**The envelope-in-a-chunk detour**, measured the honest way — the same
cross-process round trip with no envelope, no pin and no handler (`--bare`:
`submit(ZERO) → claim → complete(ZERO) → wait`) subtracted from the full ask:

| | bare p50 | full p50 | detour (envelope alloc/write/read/free ×2 + cell open/pin/read + handler) |
|---|---|---|---|
| parked | 7.4 / 7.5 µs | 9.6 / 9.4 µs | **+2.2 / +1.9 µs** |
| busy-poll | 0.5 / 0.4 µs | 1.2 / 1.2 µs | **+0.7 / +0.8 µs** |

So the message costs ~0.7–0.8 µs of real work per round trip (two 256-byte
chunk pop/push pairs, a catalog scan, a journaled version pin and the Arrow
reconstruction); everything above that in the parked number is the two
`poll(2)` wakes. The parked wake dominates 7:1 — the design's futex/mailbox
work (Phase 1) is where the next order of magnitude is, not the envelope.

## What the demo found in the substrate

A task slot is reusable capacity the instant it completes (ADR-0009/0012:
`complete` pushes it onto the LIFO FREE stack). With **one** requester that is
invisible; with two or more, the very next `submit` from another client takes
the slot the first client has not yet `poll`ed, and the requester's `wait`
returns `StaleHandle` instead of its result descriptor — every 4-client ask
failed on the first bench run, and the orphaned reply chunks then exhausted the
256-byte class. The slot's result word is therefore **not** a usable ask/reply
channel under concurrency. The demo's answer, within the existing protocol: the
asker allocates the reply chunk too and names it in the request envelope
(`payload` = `LocalRef`, `FLAG_LOCAL_REF`); the handler writes the reply
*into* it and completes with a zero result; a `StaleHandle` after a wait is
provably terminal (`seq` only advances on a submit, which only pops a slot that
went terminal), so the asker then reads its own chunk. A lease-lapsed handler
must not write into a chunk its asker may have freed, so the handler abandons
the message past half its lease — a time fence standing in for the design's
`epoch` until `holon-mem` owns it. The real fix is the Phase 1 mailbox whose
slot *is* the envelope and whose reply is the correlated slot.

Substrate accessors added (read-only, off the descriptor path):
`KeyedStore::data_segment()` (the shared pool the envelopes live in),
`ClaimedTask::attempt()` (redelivery count, stamped into the reply so the
client can count redeliveries exactly), and `TaskQueueHandle::{work_parker,
done_parker}` (one parker per loop instead of a `dup`+`close` per message).
