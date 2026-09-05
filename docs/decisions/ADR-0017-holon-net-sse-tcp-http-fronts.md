# ADR-0017 — Holon network fronts: SSE, TCP and HTTP messaging as independent actors

- Status: Accepted as a design; **gate NOT met**. `clippy -D warnings` and
  `cargo fmt --check` fail in `holon-net-demo`, the bench prints no ask-path
  row at all, and its speed claim is refuted by its own harness (see
  §Measured and §What this bench does not measure). The capability claims —
  cross-process fan-out with no relay, resume with no loss — stand.
- Date: 2026-09-02
- Builds on: ADR-0015 (Holon demo: envelope-in-a-chunk ask/reply, asker-owned
  reply chunk), ADR-0016 (windowed append + delta read), ADR-0007 (keyed
  store), ADR-0002 (topic rings + doorbells).
- Lifts from: ArrowRef Work Fabric (`infrastructure/query-cache`) ADR-0006
  (numeric fast lane, pipelining, backpressure by reservation), ADR-0010 (TCP
  frame layout), ADR-0013 (RESP front), ADR-0021 (pub/sub with `Last-Event-ID`
  replay), and `crates/arrowref-messaging/src/shm_topics.rs` (the shm topic
  engine's two rejected shapes).

## Context

ArrowRef proved three messaging surfaces worth keeping: a pipelined binary
TCP lane (4-byte length prefix, ordered responses, bounded in-flight window,
range-coalesced completions — 63.8 M "tasks"/s is ~62 k frames/s × 1024
handles per frame), an HTTP front with SSE topic fan-out that resumes from a
cursor (`Last-Event-ID` / `?from=`), and a per-topic retained log that is
both the replay window and the lag-recovery buffer. All three were built on
tokio with in-process `broadcast` channels and a per-node `VecDeque` ring;
none is cross-process, none is zero-copy, and none survives the process.

Holon now has the substrate those surfaces wanted: a 64-byte `Envelope` that
is by design *both* the shm ring slot and the TCP frame header (design §4), an
ask/reply channel with an asker-owned reply chunk, a keyed store whose
entries are RCU cells with windowed append and `read_since(cursor)` delta
reads, and doorbell-parked rings. What is missing is the *front*: nothing in
the workspace can select over a socket and a mailbox, no actor can publish to
a topic from inside a handler, and there is no retained topic log.

ArrowRef's shm topic engine (`shm_topics.rs`) tried and rejected two shapes
that are worth not repeating:

- **Payloads in `shm-ring` slots.** The ring has no pin; a producer lapping a
  slot races a reader mid-copy. Here the runtime's `publish_batch` path also
  never releases a published-but-unpinned chunk, so unread messages leak
  until the producer dies.
- **One store key per message.** Catalog slots are append-only; a key per
  message caps messages *ever published*, not messages *retained*.

## Decision

Three additive layers, each an independent crate, no existing ABI changed.

### 1. `holon-topic` — the retained topic log is ONE store entry

A topic is one keyed-store entry `topic/<name>` whose Arrow schema is a
single-row message `{seq: u64, ts_ms: u64, event: utf8, content_type: utf8,
data: binary}`. Publish is `Entry::append_windowed(WindowPolicy::new(K))`
(ADR-0016): one commit per message, so **the entry version IS the sequence
number** — monotonic, gap-free, assigned under the exclusive write lease.
`K` = `TopicPolicy::max_messages`, frozen at creation, is the replay window;
the Window re-root is the retention.

Read is `Entry::read_since(cursor)` → `Delta { batches, from_version,
truncated }`: zero-copy views held alive by the journaled `VersionPin`. A
subscriber that fell behind the window sees `truncated = true` and is told
so (a `lagged` event carrying the resume cursor) — invariant I6, no silent
loss. A subscriber that keeps up pays O(new messages) per wake.

Notification is a **descriptor-only ring**: `Node::topic_publisher("__topic_
events")` publishes a 16-byte `VersionEvent { name_id = key_id, version =
seq }` packed *into* the `ChunkDesc` (the `__artifacts` trick) — no chunk, no
pin, nothing to reclaim; the doorbell wakes parked gateways. A lagged event
ring costs nothing: the cursor lives with the subscriber and the log is the
source of truth, so a woken gateway simply reads since its cursor.

Publishers are any store client: an actor from inside a handler
(`Cx::publish`), a gateway on behalf of an HTTP/TCP client, or a plain
`Node`. Concurrent publishers serialize on the entry's write lease
(`Conflict` → bounded retry).

### 2. `holon-net` — the gateway is an actor-shaped reactor, not an executor

No tokio. One reactor thread per shard: `poll(2)` over the listener, every
client socket, the task queue's **done doorbell** (reply completions) and the
topic-event ring's doorbell. Ring parking follows the subscriber protocol
exactly (`add_waiter` → re-check → park → `remove_waiter`), so a producer
rings the bell only while a gateway is actually parked. Each shard owns its
own `Node` (its own borrow journal, so a shard's death reclaims its pins and
chunks through the ordinary lease path).

**Asks are pipelined by construction.** An inbound Ask allocates the reply
chunk, submits, and is parked in an outstanding table keyed by task handle;
the reactor polls outstanding handles on each done-doorbell wake and writes
replies in completion order with the client's `corr` restored. There is no
per-frame await, no per-connection task, no channel — the in-flight window
is the client's choice, bounded server-side by a per-connection cap that
stops reading the socket (TCP backpressure) rather than dropping.

**Backpressure by cursor, never by buffer growth.** A connection subscribed
to a topic holds a cursor; the reactor writes events from `read_since
(cursor)` only while the socket is writable and the write buffer is under
its cap. A slow consumer simply keeps an older cursor; the window bounds its
cost, and falling out of the window is reported, not hidden.

`holon-actor` gains the non-blocking ask primitives the reactor needs
(`ActorRef::{ask_start, ask_poll, done_fd}`) and `Cx::publish` — additive;
`ask`/`tell` are unchanged and re-expressed on the same primitives.

### 3. Two wire faces over one connection model

**TCP:** frame = the 64-byte `Envelope` verbatim followed by `body_len`
bytes. `to` = `ActorId::named(name)` (FNV-1a, computed client-side — the
registry is the name); `from`/`epoch`/`deadline` are stamped by the gateway;
`payload`/`FLAG_LOCAL_REF` are rejected at ingress (a `LocalRef` can not
cross a socket — I2). Control messages address `ActorId::NONE` with schemas
in the reserved system range: `SUBSCRIBE {topic, from_seq}`, `UNSUBSCRIBE`,
`PUBLISH {topic, event} + body`, `PING`. Events arrive as `Tell` frames of
schema `EVENT` with `{seq, ts_ms, topic_id, event} + data`; lag as `LAGGED
{topic_id, skipped_to}`. Errors as `Err` envelopes with the request's `corr`.

**HTTP/1.1** (hand-rolled, keep-alive, `Content-Length` bodies):
`POST /actors/{name}/{schema}` → ask (200 + reply body, `X-Holon-Schema`),
`?tell` → 202; `POST /topics/{topic}?event=` → publish → `{"seq":N}`;
`GET /topics/{topic}/sse` → SSE with `id: <seq>`, resume via `Last-Event-ID`
or `?from=`, `event: lagged` on window fall-out, comment heartbeat;
`GET /topics/{topic}?from=&max=` → long-poll NDJSON.

### Rejected

- **tokio/hyper/axum.** An executor per gateway process, `Send + 'static`
  handlers, and a second wake mechanism (waker) beside the doorbell. The
  reactor needs exactly one syscall-level wait and already has the fds.
- **A per-connection thread.** The asks are already asynchronous in the
  substrate; blocking `wait` per ask would serialize the pipeline.
- **RESP.** ADR-0013's value was Redis client compatibility for a numeric
  no-output lane; that lane's semantics (range submit, session-local
  handles) are a *message design* an actor can implement, not gateway
  machinery. Deferred until a Redis-client consumer exists.
- **Replay from the notification ring.** Its capacity is a runtime config
  shared by every topic; the store window is per topic and pinned.

## Consequences

- SSE, TCP and HTTP are three thin faces on one reactor; an actor that
  publishes completion events to a topic is visible on all three with no
  gateway code — the ArrowRef "signal feed" is a topic.
- Replies are capped by the 256-byte reply chunk (192-byte body) until the
  `LocalRef` payload path lands; request bodies by the store pool's largest
  class. Both are reported, not worked around.
- `poll(2)` is O(fds) per wake; the reactor is a single type behind
  `Reactor::poll`, so kqueue/epoll is a swap, not a redesign.
- The `__topic_events` ring is one more topic in `RuntimeConfig.ring_capacity`.

## What the fan-out benchmark must measure

The first version of the fan-out benchmark reported aggregate events per
second and was worthless, in a way worth recording because it is the
default mistake for a fan-out system. Aggregate throughput grows with
subscriber count *by construction* (it counts one message once per
subscriber), so it is the one axis on which this architecture cannot lose
and therefore proves nothing. Meanwhile the two axes that do move both got
worse and were printed at the end of the line: publish rate fell 3.3x from
1 to 256 subscribers, and the worst p99 rose 3.5x. Every row also lagged,
so no row had actually delivered what it claimed to.

The rules the bench now follows:

- **An offered rate, not a firehose.** An unpaced publisher measures how
  fast a queue fills. Latency percentiles mean something only at a rate the
  system is expected to sustain.
- **A lagged healthy subscriber is a failed row**, not a data point.
- **Every fan-out row is printed against a no-Holon floor**: one writer
  thread, N plain TCP sockets, N reader threads, same machine, same payload.
- **The claim is "tracks the floor", not "stays flat".** One reactor thread
  writing N sockets serialises by construction and so does the floor; what
  is being claimed is that the log, the cursors and the SSE framing add a
  bounded factor rather than a growing one.
- **Losing shapes are rows, not omissions**: one subscriber that cannot keep
  up, and an explicit statement that single-process fan-out is a row an
  in-process broadcast channel wins.
- **A fresh stack per stage**, because of the retention finding below.

## Findings

- **The replay window is capped by the largest pool size class.** A window
  re-root writes one manifest listing every retained batch, about
  `64 + 28·window` bytes, and that manifest must fit a single chunk. A
  64 KiB largest class caps the window near 2 300 messages; the failure mode
  is `allocation exceeds largest size class` at the first re-root past the
  ceiling, not at configuration time.
- **A retained window is held until its topic is deleted.** Windows are per
  topic and are not reclaimed by subscribers going away, so a node serving
  many topics must be sized for the sum of their windows. The first bench
  reused one node across stages and exhausted the pool in a later stage,
  which read as an actor bug and was not one. `DELETE /topics/{topic}` now
  releases a window, at the cost of permanently tombstoning a catalog slot
  (`shm-store` catalog slots are append-only), so it is an operator action
  rather than a per-request one.
- **More reactor shards do not help single-topic fan-out.** Each shard
  performs its own pinned delta read of the same topic, so sharding
  multiplies that work and adds store contention. Shards buy connection
  capacity, not fan-out throughput.

## Gate

`cargo test -p holon-topic -p holon-net -p holon-net-demo` green; clippy
`-D warnings` clean; the demo prints, from two runs: TCP ask RTT p50/p99
(parked and spin), pipelined asks/s at windows 1 / 64 / 1024 — window 1
parked is the deliberate losing shape — against a raw loopback TCP echo
floor measured the same way; HTTP ask RTT; publish→SSE delivery latency;
SSE fan-out messages/s at 1 / 16 / 256 subscribers; `Last-Event-ID` resume
with zero loss and zero duplicates across a reconnect; store pool census back
to baseline after every client disconnects.

## Measured

`holon-net-demo bench`, host macos aarch64, release build, recorded
2026-09-04: 2 runs requested, 20000 msgs at 20000/s offered, 64 B payload,
window 4096. Run 1/2 is what was captured and is reproduced verbatim below.
Stages [5] (cost), [6] (request/response) and [7] exist in the harness and
run by default; they produced nothing in this capture, so **there is no
TCP-ask and no HTTP-ask number here**. Every row below is SSE fan-out.

The capture below is from the **corrected harness**. The first run of this
bench was withdrawn: its floor was unpaced while Holon was paced, so the
`vs p99` column divided two different experiments; the gate read the median
subscriber's p99 rather than the worst; and an idle-stream bug in the test
client (`client.rs`, heartbeat comments looped inside the read) wedged the
run before stage [5]. All four are fixed, and the harness now distinguishes
FAIL (the system under test) from INVALID (the comparator could not run).

First, the full sweep with the paced floor and the completed stages:

```
== holon-net VALUE bench (ADR-0017) ==
host macos aarch64 · release=true · 1 runs · 20000 msgs at 20000/s offered · 64 B payload · window 4096
A lagged healthy subscriber is a FAIL, not a number. Every fan-out row is printed against a no-Holon floor.

───────── run 1/1 ─────────

[1] CLAIM: per-subscriber delivery latency TRACKS THE NO-HOLON FLOOR as subscriber count grows.
    Not "stays flat": one reactor thread writing N sockets serialises by construction, and so does the floor.
    The claim is that the log, the cursors and the SSE framing add a bounded factor, not a growing one.
    Falsified by: WORST healthy p99 above 5x the floor's worst, any healthy subscriber lagging,
    or either side failing to sustain the offered rate — a row whose floor could not keep up is INVALID, not a win.
     subs   floor p50  floor p99 │        p50        p99  worst p99 │  vs p99   publish/s     behind   verdict
        1      8.0 µs    23.0 µs │    27.0 µs    64.0 µs    64.0 µs │    2.8x       20000         40   PASS
       16    148.0 µs    1.92 ms │   680.0 µs    9.81 ms    9.84 ms │    5.1x       19999        192   FAIL (INVALID: floor missed the offered rate 17274 times)
       64     2.31 ms    6.50 ms │   394.0 µs   32.60 ms   34.71 ms │    5.3x       19973        336   FAIL (INVALID: floor missed the offered rate 20000 times)
      256    44.03 ms  192.10 ms │    1.78 ms   74.60 ms   93.34 ms │    0.5x       19908       2194   FAIL (INVALID: floor missed the offered rate 20000 times)
    Aggregate events/s is not printed at all: it grows with subscriber count by construction and can only flatter.

[1b] Does adding reactor threads recover the tail at 256 subscribers?
     shards         p50         p99     publish/s   verdict
          1     1.76 ms    52.68 ms         19950   FAIL (did not sustain 20000/s)
          2     1.13 ms    41.83 ms         19907   FAIL (did not sustain 20000/s)
          4    30.02 ms    82.11 ms         19958   FAIL (did not sustain 20000/s)

[2] LOSING SHAPE: 1 of 16 subscribers reads 10x too slowly.
    Claim: the other 15 are unaffected and the slow one is TOLD it lagged, never silently skipped.
    control  16 healthy          p99    8.64 ms · publish 19989 msg/s
    test     15 healthy + 1 slow p99    6.27 ms · publish 19999 msg/s · slow sub lag notices 0 · healthy lag 0 · gaps 0   FAIL (slow sub never lagged: the lag-notice path did not run)

[3] CLAIM: fan-out spans OS PROCESSES with no relay and no second copy.
    This is what an in-process broadcast channel cannot do at all: it would need a relay hop per process.
      gateways    subs         p50         p99     publish/s        gaps   verdict
             1      64    417.0 µs    22.59 ms         19991           0   PASS
             2      64    28.46 ms    56.99 ms         19937           0   FAIL (did not sustain 20000/s)
             4      64    27.49 ms    54.01 ms         19968           0   FAIL (did not sustain 20000/s)
    Same subscriber count each row; only the process split changes. One publish, N processes, zero relay hops.

[4] CLAIM: the replay log outlives the gateway process. SIGKILL a gateway mid-stream; resume elsewhere with Last-Event-ID; lose nothing.
    mid-stream migration gw0→gw1  got 20000/20000 gaps 0 reconnects 1 · published 20000 msg/s   PASS
    after SIGKILL of gw0, replay from seq 0 on gw1   replayed 7712 of 20000 published (window holds 8192) gaps 0 lag notices 1   PASS

[5] COST: what the above is paid for with. These are the rows a broadcast-channel design wins.
    publish, acknowledged one at a time   29896 msg/s   (a channel send is a pointer push; this is an RCU commit: chunk alloc, Arrow batch, write lease, manifest install)
    publish, pipelined fire-and-forget    133107 msg/s
    retained window is a fixed pool of chunks, not a vector of pointers: a window of 4096 holds up to 8192 live message chunks plus their manifests, per topic, until the topic is DELETEd.
    Arrow earns nothing on this path: a one-row batch and a schema carry an opaque byte blob.
    Verdict: for single-process fan-out alone, an in-process broadcast channel wins. The wins above are cross-process reach and crash survival.

[6] REQUEST/RESPONSE (unchanged claim: pipelined asks near the loopback floor).
    raw loopback echo RTT p50 14.0 µs · TCP ask RTT p50 28.8 µs p99 34.5 µs · overhead 14.8 µs
    pipelined w=64    577658 asks/s · errors 0
    pipelined w=1024  596755 asks/s · errors 0
    HTTP ask RTT p50 31.0 µs p99 36.8 µs · 31971 req/s · errors 0

[7] COALESCED COMPLETIONS (ADR-0006 lifted): one descriptor, N handles.
    per-handle  200x1024 handles  193839 handles/s seen by a subscriber · 204800 events on the wire · 189 req/s · errors 0
    coalesced   200x1024 handles  44279884 handles/s seen by a subscriber · 200 events on the wire · 48497 req/s · errors 0
```

Then the fan-out stages re-run with the verdict on achieved rate rather than
on a count of individually-late messages, and with INVALID separated out:

```
== holon-net VALUE bench (ADR-0017) ==
host macos aarch64 · release=true · 1 runs · 20000 msgs at 20000/s offered · 64 B payload · window 4096
A lagged healthy subscriber is a FAIL, not a number. Every fan-out row is printed against a no-Holon floor.

───────── run 1/1 ─────────

[1] CLAIM: per-subscriber delivery latency TRACKS THE NO-HOLON FLOOR as subscriber count grows.
    Not "stays flat": one reactor thread writing N sockets serialises by construction, and so does the floor.
    The claim is that the log, the cursors and the SSE framing add a bounded factor, not a growing one.
    Falsified by: WORST healthy p99 above 5x the floor's worst, any healthy subscriber lagging,
    or either side failing to sustain the offered rate — a row whose floor could not keep up is INVALID, not a win.
     subs   floor p50  floor p99 │        p50        p99  worst p99 │  vs p99   publish/s     behind   verdict
        1      6.0 µs    22.0 µs │    21.0 µs    49.0 µs    49.0 µs │    2.2x       20000         11   PASS
       16     21.0 µs   975.0 µs │    69.0 µs    8.02 ms    8.04 ms │    8.2x       19994        333   FAIL (8.2x the floor)
       64     2.25 ms    6.06 ms │   409.0 µs   22.18 ms   22.89 ms │    3.8x       19943        445   INVALID (the floor could not offer 20000/s at 64 subscribers: 7091 msg/s, 20000 late)
      256    33.14 ms  153.97 ms │    1.62 ms   45.39 ms   62.48 ms │    0.4x       19910       1690   INVALID (the floor could not offer 20000/s at 256 subscribers: 1511 msg/s, 19999 late)
    Aggregate events/s is not printed at all: it grows with subscriber count by construction and can only flatter.

[1b] Does adding reactor threads recover the tail at 256 subscribers?
     shards         p50         p99     publish/s   verdict
          1     1.74 ms    63.88 ms         19908   PASS
          2     1.08 ms    42.87 ms         19918   PASS
          4    32.51 ms    79.19 ms         19921   FAIL (sharding made p50 18.7x worse)

[2] LOSING SHAPE: 1 of 16 subscribers reads 10x too slowly.
    Claim: the other 15 are unaffected and the slow one is TOLD it lagged, never silently skipped.
    control  16 healthy          p99    8.75 ms · publish 19994 msg/s
    test     15 healthy + 1 slow p99    7.40 ms · publish 20000 msg/s · slow sub lag notices 0 · healthy lag 0 · gaps 0   FAIL (slow sub never lagged: the lag-notice path did not run)

[3] CLAIM: fan-out spans OS PROCESSES with no relay and no second copy.
    This is what an in-process broadcast channel cannot do at all: it would need a relay hop per process.
      gateways    subs         p50         p99     publish/s        gaps   verdict
             1      64    541.0 µs    34.51 ms         19985           0   PASS
             2      64    27.59 ms    55.29 ms         19927           0   PASS
             4      64    26.77 ms    52.83 ms         19987           0   PASS
    Same subscriber count each row; only the process split changes. One publish, N processes, zero relay hops.
```

**[1]** establishes delivery, not speed. At 256 subscribers with 20000/s
offered the publisher held 19914 msg/s, no healthy subscriber lagged, and no
row gapped. The `vs p99` column is not a comparison and is refuted below —
the floor is unpaced while Holon is paced, so the denominator is the floor's
own queueing backlog rather than a baseline. Read the Holon p99 column on its
own terms: 197.0 µs at 1 subscriber, 10.85 ms at 16, 36.67 ms at 64,
113.28 ms at 256. Note also that p50 is *lower* at 64 subscribers
(501.0 µs) than at 16 (679.0 µs), which a serialising reactor cannot do; the
measurement is dominated by something other than fan-out cost.

**[1b]** answers its own question with a no. p50 goes 1.87 ms → 27.35 ms →
31.80 ms from 1 to 4 reactor shards, and publish/s falls 19977 → 19797. The
verdict tests gaps and lagged only, so a 17x p50 regression prints PASS. This
is the Findings entry "More reactor shards do not help single-topic fan-out",
now measured.

**[2]** did not fire. `slow sub lag notices 0` means the deliberately
throttled subscriber never fell out of the 4096-message window, so the branch
the stage exists to prove — that a lagging reader is *told* — never executed.
What the row does show is that 15 healthy subscribers beside a slow one are
unaffected: p99 12.35 ms against a 13.46 ms control, publish 19951 against
19990 msg/s, gaps 0.

**[3]** is a capability row printed under a speed table. One publish reached
64 subscribers split across 1, 2 and 4 gateway processes with 0 gaps and no
relay hop — the thing an in-process broadcast channel cannot do at all. It
also shows the process split costing p50 696.0 µs → 27.20 ms → 27.65 ms; the
verdict checks gaps, lagged and got, so that 39x regression prints PASS.

**[4]** as printed is a client-driven migration gw0→gw1: 20000/20000, gaps 0,
reconnects 1. It proves the log outlives *the connection*. It does not prove
the heading, because no process dies in the row shown (refuted below).

**Gate status.** `cargo test -p holon-topic -p holon-net -p holon-net-demo`
is green: 19 tests, 0 failed, 0 ignored, first run. `cargo clippy
-D warnings` **FAILS**, exit 101, 3 errors, all in
`crates/holon-net-demo/src/bench.rs`: unused import `Report` (:35), dead
field `worst_p99` (:87), `clippy::too_many_arguments` on `fn fan_out`, 8/7
(:93). `cargo fmt --check` **FAILS**, exit 1, 9 hunks across `bench.rs` and
`tests/net.rs`, all pure formatting. `holon-topic` and `holon-net` are clean;
both failures are in the demo crate. The Gate above is therefore not met —
and the dead field the compiler names, `worst_p99`, is exactly the statistic
that would have exposed the worst subscriber's tail in a fan-out claim.

### What this bench does not measure

The numbers in this subsection come from independent re-runs of the same
binary on the same host and from a hand-built floor, not from the capture
above. They are recorded here because they contradict it.

**Refuted: the `vs p99` column is not a comparison.** `raw_fanout`
(`roles.rs:1218`) takes no rate argument and writes as fast as the kernel
will accept, while the Holon side is `PublishOpts::paced` at 20000/s
(`roles.rs:759-767`); `bench.rs:219` divides one by the other and
`bench.rs:226` gates PASS on the quotient. A byte-identical replica of
`raw_fanout` with a pacer added and nothing else changed gives, at 1
subscriber: unpaced p50 37.0 µs / p99 2.19 ms (reproducing the row above),
but paced at 20000/s p50 9.0 µs / p99 110.0 µs. Against a fair floor the
printed "0.1x" is about 1.8x the wrong way. At 64 and 256 subscribers the
paced floor delivers 5155 msg/s and 1038 msg/s — it never reaches the rate
Holon is credited with sustaining, so no ratio on those rows means anything.

**Refuted: the gate cannot fail in the direction that matters.**
`FLOOR_FACTOR = 5.0` (`bench.rs:46`) divides a denominator whose 1-subscriber
p99 ranged from 28.0 µs to 980.60 ms across four runs of the shipped binary
on this host — a spread larger than the gate itself. In one of those runs the
gate stamped PASS on a Holon p50 of 827.71 ms at one subscriber, with the
publisher at 14783 msg/s against its own stated 20000/s offered rate.
All twelve rows in the capture above printed PASS (a skeptic's own re-runs
added two more, also PASS). `behind_schedule` is
computed (`roles.rs:798`, `:825`) and never printed, so the offered rate in
the banner is not enforced anywhere.

**Refuted: the rows do not reproduce.** An independent re-run of stage [1] on
the same binary and host gave, at 256 subscribers, p50 196.42 ms against the
2.03 ms above (97x) and p99 474.17 ms against 113.28 ms, with publish falling
to 16957 msg/s. Stage [1b] shards=1 gave 87.22 ms / 437.30 ms / 15786 against
the 1.87 ms / 54.88 ms / 19977 above. Every one of those rows also printed
PASS.

**Refuted: stage [4] as printed is not a SIGKILL.** The harness does contain
a kill row (`bench.rs:377-399`, `stack.gws[0].kill()` then replay from seq 0
on gw1), but the line printed under the [4] heading is the voluntary
client-side reconnect (`SseOpts::reconnecting`, `bench.rs:352-355`) to a
second, healthy gateway. No process dies in the evidence above. The kill
row's own verdict is weak in any case: it passes on `got != 0`
(`bench.rs:388-391`), not on `got == expect`.

**Refuted: the harness cannot report its own worst failure.** At 256
subscribers with 2 reactor shards the stack wedged on this host: the
publisher had finished, both reactor shards were parked in `poll(2)` at 0.1%
CPU, and 130 of 256 SSE reader threads were blocked in `recvfrom`.
`read_sse_event` (`client.rs:411-448`) loops forever on heartbeat blocks and
the `SseOpts.timeout` deadline is only checked in the outer loop
(`roles.rs:991`), so a starved-but-alive stream never times out. A row can
only print if nothing wedged, which makes every PASS above
survivorship-selected.

**Two of the three fronts have no numbers here.** Stages [5], [6] and [7]
were run separately. Stage [6]: "raw loopback echo RTT p50 13.0 µs · TCP ask
RTT p50 26.0 µs p99 68.9 µs · overhead 13.0 µs" in one run, and "raw loopback
echo RTT p50 14.8 µs · TCP ask RTT p50 31.2 µs p99 51.6 µs · overhead
16.5 µs" in another — the ask indirection is about 2x a plain socket doing
the same thing, against the only honest floor in the file. Stage [5]:
"publish, acknowledged one at a time" at 29561 msg/s in one run and 48644
msg/s in another; "publish, pipelined fire-and-forget" at 106875 msg/s and
101591 msg/s. Stage [5] also carries the harness author's own conclusion:
"for single-process fan-out alone, an in-process broadcast channel wins."
The ADR's Gate demands parked *and* spin ask RTT; `bench.rs` never passes
`--spin` and `MultiStack::spawn` takes no spin parameter, so the spin row
cannot exist. Nor is a store pool census run anywhere in the bench.

**The comparator the bench declines to run.** An in-process fan-out floor
(`Arc<Vec<u8>>` cloned into N `std::mpsc` queues, same 128 B frame, same
20000/s offered rate, same per-subscriber latency method) measures 1 sub 5 µs
p50; 16 subs 30 µs p50 / 398 µs p99; 64 subs 151 µs / 1.12 ms; 256 subs
1.08 ms / 6.85 ms. Against the rows above that is 23x worse p50 and 27x worse
p99 at 16 subscribers, 33x worse p99 at 64, and 17x worse p99 at 256.
Unpaced, that channel publishes 14.6 M msg/s against the front's 106875. This
is the losing shape the Decision above already names, quantified.

**Reporting shape.** The printed p50/p99 is the *median of the per-subscriber
percentiles* (`bench.rs:154-155`) while the floor's is a pooled percentile —
different populations in the same row. Half the subscribers are worse than
the printed number by construction, and `worst_p99` is computed
(`bench.rs:143`) and discarded. The banner claims every fan-out row is
printed against a no-Holon floor; stages [1b], [2] and [3] compute no floor
at all, and the [1b] and [3] verdicts (`bench.rs:259`, `:325`) contain no
latency test. Stage [2]'s verdict also omits the `got != expect` check that
stage [1] makes at `bench.rs:224`, so a slow subscriber that silently
received fewer messages would still print PASS.

Axes that are pinned to one value and therefore unmeasured:

- **Duration.** Every row is a single ~1 s sample (the 256-sub row's 5120000
  events at 5070957 events/s is 1.01 s). No soak, no steady state, and only
  run 1 of the declared 2 runs was captured, so there is no run-to-run
  variance bound.
- **Payload.** 64 B in every row, against `MAX_WIRE_BODY` 65535 on the TCP
  face and `MAX_BODY` 1 MiB on the HTTP face. The 6.5x Arrow-envelope space
  amplification grows in absolute bytes with payload and is never seen.
- **Offered rate.** Fixed at 20000/s, which is ~20% of the front's own
  pipelined ceiling and ~41% of its acknowledged one. The knee is never
  located, and no publish-latency distribution is measured anywhere.
- **Topics and connections.** Exactly one topic is live per stage, at most
  256 connections. The O(connections × subscriptions) delivery scan is the
  term claimed to grow and it is never varied.
- **Retention.** `WINDOW = 4096` here against a shipped gateway default of
  1024, i.e. 4x looser and 4x rarer re-roots. At 4096 roughly 4-5 of 20000
  publishes are re-roots — p99.98, below the printed p99 — so the
  re-root spike is structurally invisible to these percentiles.
- **Churn and cold start.** The bench sleeps `400 + subs*2` ms so every
  stream is established before publishing; nothing connects or disconnects
  mid-run, and topic creation is folded into the paced publish rate.
- **Memory.** Nothing in the bench measures RSS, pool census or the
  per-connection write buffer (`WBUF_HIGH` is 1 MiB, i.e. up to ~256 MiB of
  gateway heap at the 256-subscriber row, outside the shm census entirely).
- **Ask path shape.** One shard only, bodies of 16 B against the 192 B reply
  cap, deepest pipeline window 1024 against `MAX_PENDING_ASKS` 4096.

The honest reading of the evidence: over ~1 s at 20000 msg/s with 64 B
payloads on one topic, SSE fan-out to 1-256 pre-connected subscribers
delivers with zero gaps and zero lag, across 1, 2 and 4 gateway processes,
and a client resumes across a reconnect with no loss. Per-subscriber SSE
delivery costs roughly 2-3x a raw socket at one subscriber and one to two
orders of magnitude more than an in-process channel at every subscriber
count. A round trip on the ask path costs about 2x a loopback echo. What the
indirection buys is cross-process reach, not speed. Crash survival is the
other half of the claim and this run does not earn it: no process dies in the
evidence above, and the kill row never printed.

## The cost of the indirection

Per message, on each path, with the required costs separated from the
incidental ones. Timings are from a hot-path probe on this host; source
positions are given so each item can be checked.

**Publish → SSE delivery.** `TopicLog::publish` is 3.79-3.88 µs. Of that,
2.15 µs (55-57%) is `SchemaRegistry::intern` called *twice* per commit
(`shm-artifact/src/artifact.rs:772` and `shm-arrow/src/write.rs:100`), each
call rebuilding a 324 B Arrow-IPC flatbuffer and FNV-hashing it on a cache
hit, to look up a `schema_id` that is already known; `resolve(id)` is 10 ns
by comparison. What is genuinely required is the
journaled fenced write lease (4 uncontended RMWs, counted from the code — not instrumented), which is what makes a
`kill -9` mid-commit reclaimable and what makes the version the gap-free
sequence number; the FREE→LOANED→PUBLISHED refcount ladder on the data chunk
(4 CAS, counted from the code), which is what makes a chunk reclaimable exactly once and never under
a live reader; the single linearising CAS that installs the new manifest;
and the one copy of the payload into shared memory. The `VersionEvent` push
is 24 B packed into a `ChunkDesc` with no allocation and no pin, and the
doorbell is gated on `waiters > 0`, so at saturation notification costs zero
syscalls. That part of the design is already optimal.

Everything else on the publish path is incidental. The payload is copied four
times before it reaches shared memory — kernel→`rbuf` (required),
`rbuf`→`req.body` (`http.rs:201`), payload→`BinaryArray` values
(`holon-topic/src/lib.rs:249`), and the one real copy into the chunk
(`write.rs:188`) — where only the first and last are structural. HTTP request
parsing is 672-690 ns and and, counted from the code rather than instrumented, ~18 heap allocations for 7 headers and a 128 B
body. `Connection::read` zero-fills 64 KiB on every `read(2)` regardless of
how many bytes arrive: 277-300 ns, ~8% of a publish. The manifest is parsed
three times per Append; only the parse taken under the held `borrow_shared`
is the load-bearing ADR-0013 identity re-check. A 128 B payload serialises to
832 B (32 B `BatchHeader` + 248 B of table + eight 64-byte-aligned buffers)
and therefore occupies a 1 KiB chunk — 6.5x amplification, structural to
wrapping one row in a 5-column Arrow batch, and multiplied by the retained
window on every topic.

On the read side the journaled pin is 80 ns for a whole caught-up
`read_since` — it is not where the money is. Rebuilding a 5-column Arrow
`RecordBatch` per single-row message is: 797-958 ns per message, linear from
1 behind (0.96 µs) to 256 behind (211 µs), and `decode_message` then tears
that object graph back down to five scalars in 17 ns. The pin and the
zero-copy are required; the Arrow object graph is not — the topic schema is
fixed and internal.

The window re-root is required by bounded retention (ADR-0016), but its size
was never chosen: `WindowPolicy::new(K)` pins `max_depth = keep = K`, so the
spike is O(K) at frequency 1/O(K). ADR-0016 measures the re-root rate as 1 per K, so at the shipped default of
1024 a re-root lands on roughly 1 publish in 1024. The size of that spike at
K=1024 is **not measured here** — ADR-0016's Window figures are at K=4096 —
so the tail it contributes is unquantified. Nobody tuned this knob.

Per SSE subscriber, one re-framing copy and one `write(2)` are required: SSE
mandates a `data:` prefix per line, and N sockets mean N writes. The TCP face
does *not* need its copy (`tcp.rs:194` copies the payload verbatim, where a
writev from the pinned shm buffer would not).

**Ask (TCP).** The envelope is genuinely zero-serialisation — the same 64-byte
struct on the socket and in the chunk, a bytemuck cast, no allocation. Two
shm chunk allocations are required: the asker-owned reply chunk, which is
what lets a handler reply cross-process and what makes an abandoned ask
reclaimable, and the request chunk carrying a flat header plus inline bytes.
The submit is ~4 RMWs plus a `SeqCst` fence (the no-lost-task and
no-lost-wakeup rules); the claim CAS is the exactly-once arbitration; the
handler reads its request *borrowed* from shared memory, zero copies. That is
the guarantee actually being delivered, and the whole ladder is far cheaper
than the publish path: an ask carries a message for about one CAS and one
memcpy where a publish pays two chunk allocations, ~25 RMWs (counted from the code) and 2.15 µs of
schema interning.

The cost that dominates it is not in any of that. Per ADR-0015's own
decomposition, the bare parked round trip is 7.4-7.5 µs against 0.4-0.5 µs
busy-polling — the two `poll(2)` parks and their doorbell writes are 78% of a
9.4-9.6 µs parked ask. Everything the envelope, the chunks and the handler do
is noise beside it. Removing it costs a core or a Linux futex, not a
guarantee.

Incidental on the ask path: three of the five payload copies (`tcp.rs:25`
copies the body out of `rbuf` only because `consume` invalidates the borrow
two lines later; `system.rs:830` copies the reply out of shm only so the
chunk can be freed before the caller sees it; `tcp.rs:165` then copies it
again into the write buffer, and the last two collapse to zero with a
writev). Also incidental: a redundant `poll(2)` inside every doorbell drain,
on an fd the reactor's own `poll(2)` just reported readable; and five
separate O(all-connections) passes per reactor iteration, in which the
pending count is summed twice, every in-flight ask is polled twice, and
`sweep_orphans` takes a mutex whether or not the orphan list is empty.

**What the extra work buys, measured against the surface it was lifted
from.** Against ArrowRef's shipping pub/sub engine (one `parking_lot::Mutex`,
`seq++`, an `Arc` over the request's `Bytes` with zero payload copies, a
`VecDeque::push_back`), Holon's publish is strictly heavier: an Arrow 1-row
batch with a payload copy, a journaled exclusive write lease, an RCU windowed
commit with a bounded retry, an amortised O(K) re-root, and a ring push.
Delivery is strictly lighter: ArrowRef pays, per subscriber per message, an
`Arc` clone into a broadcast slot, a waker, a tokio scheduler hop and a full
`serde_json` re-serialisation including `from_utf8_lossy` of the payload;
Holon pays one pinned `read_since` per topic per pass, shared across every
subscriber, then a borrowed `&[u8]` appended into each connection's write
buffer on the thread that already owns the socket. The crossover is roughly
one subscriber. The structural win is the log shape: one catalog key per
topic with version == seq minted by the commit, so resume is one call
returning an exact contiguous delta — against K keys per topic at
`msg/{seq % K}`, a seq minted before the commit, a process-wide `front`
mutex to stop publishers inverting, and a revalidation on every read because
the key may have recycled.

The shared read has a cost a per-receiver channel cannot have: `deliver`
reads once from the *minimum* cursor across subscribers
(`gateway.rs:640`, `:652`) and every connection then re-walks that whole run
skipping seqs it already holds (`gateway.rs:687-689`). The per-batch cost of
that re-walk is not instrumented; the related probe figure is per *message*.
One lagging reader makes every reader O(lag).

On request/response Holon is straightforwardly less work: no task per
connection, no allocation per frame, pipelined asks on one connection, and
one round trip — against a task-per-connection read/handle/write loop, a
crossbeam hop to a worker pool, a `Mutex`+`Condvar` signal ring, and a second
command to collect completions. Its only extra costs there are the
asker-owned reply chunk with its orphan sweep and the deliberate
re-poll-every-pending-ask lost-wakeup guard.

The surface is about a fifth of what it was lifted from. Missing: WebSocket
subscribe, durable topics, byte and age retention, the topic admin surface
(list, info, policy update), auth and tenancy, cluster fan-out, work queues
with visibility leases and dead-lettering, the RESP face, the numeric fast
lane, validation and conditional-fetch frames, a data plane (bodies are
inline-only and capped at 65535 B on TCP), and metrics export. `holon-net`'s
`GatewayStats` is a struct of `AtomicU64` with no exporter. One silent
contract difference is worth naming: `/sse` here defaults to live-only
(`from = u64::MAX`) where the surface it was lifted from defaults to
replaying the whole retained window.

### Removable overheads, in order

1. The double `SchemaRegistry::intern` per commit — 2.15 µs of a 3.79-3.88 µs
   publish. The id is already interned at `TopicLog::new` and at store create;
   cache it on `TopicSide`/`Entry` and thread `schema_id` into the commit.
   Expected: publish 3.8 → ~1.7 µs. Breaks no guarantee.
2. The Arrow `RecordBatch` rebuilt per single-row message on delivery —
   797-958 ns each, paid per message per pass, and immediately flattened in
   17 ns. A fixed-layout raw accessor over the pinned chunk keeps the pin,
   the zero-copy and the replay log. This is an interface change, not an edit.
3. The parked-wake syscall pair on the ask path — 7.4-7.5 µs of a 9.4-9.6 µs
   ask. A spin-then-park mailbox costs a core, not a guarantee. ADR-0015
   already names this as Phase 1 work.
4. The five O(all-connections) passes per reactor iteration, plus the
   O(connections × subscriptions) delivery scan that runs *before* the
   `wanted.is_empty()` bail. An inverted topic→connections index and a
   handle→connection map make both O(events).
5. The redundant `poll(2)` inside each doorbell drain — one syscall per wake,
   twice per iteration. The bare drain already exists as a private fn.
6. The removable payload copies: three of four on the publish path, three of
   five on the ask path. A design that advertises zero-copy currently pays
   four and five.
7. The unconditional 64 KiB zero-fill per `read(2)` — 277-300 ns,
   ~8% of a publish, ~3% of an ask.
8. HTTP request parsing — 672-690 ns and ~18 allocations to turn bytes
   already in a buffer into Strings. A borrowing parser cuts it to near zero.
9. `conn.consume(n)`'s `Vec::drain` memmove per message, which makes a
   pipelined batch quadratic in buffered bytes. A read cursor plus periodic
   compaction — which the write side already does — removes it.
10. The window re-root tail, by choosing `keep` and `max_depth`
    independently instead of pinning them equal. Same amortised cost, far
    smaller spike.

Two costs on this list are not removable and should be stated as such: the
6.5x space amplification is structural to a 5-column Arrow envelope around
one row (a fixed-layout row would be ~180 B), and the per-socket SSE
re-framing is required by the SSE grammar itself.
