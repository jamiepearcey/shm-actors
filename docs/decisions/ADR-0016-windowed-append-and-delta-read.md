# ADR-0016 — Windowed append and delta read: bounded high-churn streams

- Status: Accepted — implemented 2026-08-30 (see §Measured)
- Date: 2026-08-30
- Builds on: ADR-0013 (chained manifests: `Append` in O(new data)),
  ADR-0003a (manifest self-validation), ADR-0014 (staged-manifest journal),
  ADR-0015 (Holon demo — actors append to cells per message).

## Context

ADR-0013 made `Commit::Append` O(new data) and the bench confirms it: Append
p50 sits at 1.3–1.6 µs from version 0 to version 100 000, flat within noise
of `Replace`. What it left to "a policy of the layer above" is everything
else a **high-churn append-only stream** needs, and nothing above the
substrate implemented it:

1. **The chain is unbounded.** Every Append pins one data chunk and one
   manifest chunk forever, until a `Replace`. A cell fed at 100 K appends/s
   exhausts any pool in seconds; the reachable history is the whole history.
2. **A read walks the whole chain.** `as_arrow_batches` is O(batches):
   ~220 ns per batch — 22 µs at depth 100, 227 µs at 1 000, 2.7 ms at
   10 000. `as_arrow` then copies all of it.
3. **There is no delta read.** A consumer that only wants "what changed
   since I last looked" has to walk from the head to the root every time.
4. The only compaction available was `Replace` of a concatenated batch —
   a copy of the whole window, and a hard `truncated` for every reader.

This ADR closes those four in the substrate, keeping the invariant that
matters: **no byte of payload is ever copied**, and the reference-counting
ownership rule of ADR-0013 is unchanged.

## Decision

### 1. `Commit::Window { keep_batches }` — re-root by reference

The new version is a fresh chain **root** whose manifest lists the newest
`keep_batches` batches of the predecessor's table — *re-listed* with one
`borrow_shared` per kept data chunk — followed by the newly staged chunks.
The predecessor's chain is not linked. When the predecessor version retires,
the cascade of ADR-0013 releases its manifests; kept data chunks survive on
their second reference (the new root's), everything older than the window
returns to the pool.

Cost: O(keep + new data) reference RMWs plus a **newest-first, early-stop**
walk over only the chain members holding the window (never O(depth)); the
manifest is `64 + 24·chunks + 4·batches + 16·members` bytes and must fit a
pool class. `keep_batches == 0` or no predecessor degenerates to `Replace`.

The ownership rule is unchanged: *a manifest owns one reference on each
data chunk it lists*. A data chunk's refcount is the number of manifests
listing it — now legitimately 2 for the window's lifetime, which the
cascade already handles (`release_chunk` decrements, frees at 0). Every
kept chunk is generation-validated **before and after** its borrow, exactly
like the Append link: a chunk recycled across the borrow is a genuine
reference on a stranger's occupant and is released straight back
(`release_chunk_owned`); any failure releases every reference taken and maps
to `Conflict`. Rollback (`rollback_staged`) gained a `kept` list and
releases those references generation-checked.

### 2. `WindowPolicy` — the amortised shape

```
WindowPolicy { keep_batches, max_depth }
commit_for_depth(depth) = Append while depth + 1 < max_depth, else Window
WindowPolicy::new(k) = { keep_batches: k, max_depth: k }
```

`Committer::commit_windowed(&policy, batch)` reads `Artifact::current_depth()`
(an unpinned, identity-checked read of the head manifest — a *hint*; every
commit re-validates under its own rules) and picks the kind. The table then
always holds between `keep` and `keep + max_depth` batches; live chunks,
manifest size and read cost are bounded by that regardless of how many
versions were ever committed; and the amortised cost per commit is
`O(1 + keep / max_depth)` reference RMWs — with `new(k)`, one Window per `k`
commits. `KeyedStore::Entry::append_windowed` is the actor-facing wrapper
(journaled exclusive lease, as `commit`).

### 3. `VersionPin::batches_since(since)` — the consumer's delta

Walks **newest-first from the head and stops** at the first manifest whose
predecessor is `since` or older (`walk_chain_newest_first`, the primitive
`walk_chain_with` is now the full-walk reversal of). O(new batches),
zero-copy, one `RecordBatch` per batch. Returns
`Delta { batches, from_version, truncated }`.

### 4. A Window is transparent to a reader that keeps up

The first cut of `batches_since` flagged `truncated` at every root newer
than `since` — so a follower one version behind was told to resynchronise
at every Window, re-reading `keep` batches it already held. The store test
(`entry_append_windowed_stays_bounded_and_read_since_follows_it`) caught it.
The manifest now carries what a delta reader needs (ABI `SHMMFST4 →
SHMMFST5`):

- `kept_count` (the former `_reserved` word) and a trailing
  `[KeptMember { version: u64, batches: u32, _pad: u32 }; kept_count]`
  table: which version each run of the root's leading (kept) batches came
  from, oldest first.
- `prev_version` on a **root** is the **window base**: the version after
  which every batch is present in the root (`0` for a plain `Replace`
  root). Root strictness becomes `prev_ref == 0 ⇔ depth == 0` (+
  `prev_gen == 0`); a linked manifest has `0 < prev_version < version` and
  no members. `window_ok` (shared by writer and parser) enforces:
  `window_base < version`; members strictly increasing, `< version`, `≥ 1`
  batch, zero pad, `Σ batches ≤ batch_count`, `window_base ≤ members[0]`.

`Manifest::kept_batches_before(since)` = `Σ batches` of members with
`version ≤ since`, defined iff `since ≥ window_base`. `batches_since` at a
root newer than `since`: exact (skip that many leading batches,
`truncated = false`) when defined; otherwise the whole table from the root,
`truncated = true`.

The base rule, per kept chain member (only the oldest kept member may be a
partial tail):

| oldest kept member `m`, kept `take` batches | window base |
|---|---|
| partial (`take` < the batches `m` added itself) | `m.version` |
| whole, `m` linked | `m.version − 1` |
| whole, `m` a plain `Replace` root | `m.version` — a reader that missed the Replace must resync |
| whole, `m` a window root, none of its inherited run | `m.version − 1` |
| into `m`'s inherited run | the run's own rule: `k.version − 1` if `k` whole (and not partial in `m`), else `k.version`; all of it → `m.window_base` |

A kept window root's member table is **flattened into the new root** (its
inherited run first, then `(m.version, own new batches)`), so nesting
windows keeps every reader at or past the base exact. `Replace` is never
transparent, and a Window that keeps a whole Replace root carries that
root's own version as base.

`Entry::read_since(since)` is the store wrapper; the consumer passes the
returned pin's `version()` back as its next `since`. **Note** the delta's
batches are zero-copy views that keep the pin alive.

## Rejected

- **Copy-compaction** (`Replace` of a concatenated window). O(window bytes)
  memcpy per compaction, needs a chunk class as large as the window, and
  every reader is told to resync. The substrate's whole identity is
  "descriptor movement is cheap, payload never moves".
- **Mutating the tail** (cutting an old manifest's link). Manifests are
  immutable by construction — that is what makes the pin path a single
  parse and the cascade exactly-once.
- **A per-version batch count in every manifest** so a delta could be
  computed without the member table. Cheaper per manifest, but the old
  chain is freed at the window, so the information has to travel with the
  root anyway.

## Consequences

- Manifest ABI `SHMMFST4 → SHMMFST5`. No users, no migration (pre-launch).
- `write_manifest` takes a `window: Option<(u64, &[KeptMember])>`;
  `manifest_len` takes `kept_count`. `CommitKind::Window = 3` on the
  `__artifacts` topic.
- A Window's manifest is O(window); the pool needs a class for it (the
  bench sizes `64 + 44·(keep + max_depth + 1)`).
- A reader pinned before a Window holds the whole old chain until it drops:
  the bound is per *reachable* version, not per artifact (bench row (c)).
- The Window's p50 grows ~180 ns per kept chunk (member parse + two
  validates + one CAS + descriptor write); at `keep = 4096` that is a
  ~0.7 ms commit once every 4096 commits — the tail-latency price of the
  amortisation, reported honestly below. A stream that cannot afford it
  picks a smaller `keep` or a `max_depth` above `keep` (fewer, larger
  windows are *not* cheaper per window; more frequent, smaller ones are).

## Verification

`crates/shm-artifact/tests/rcu.rs` (w1)–(w7): keep-the-newest + free the
tail with an exact census; `WindowPolicy` over 10 000 commits in a pool of
256 small chunks (a plain Append lineage needs 20 000) with depth, table
size, live chunks and row order checked throughout; `batches_since` across
plain roots, windows, nested windows, a Replace and a window over a Replace;
a reader pinned across a Window (kept chunks at refcount 2, exact cascade on
drop); optimistic `Window` vs `Append` racing 500 rounds with an exact census
per round; multi-chunk batches kept whole; schema mismatch rejected with
nothing leaked. `manifest.rs` unit tests: member-table round trip and every
`window_ok` rejection. `crates/shm-store/tests/store_local.rs`: an entry
driven 5 000 commits under `new(8)` with a follower that is never told to
resync and sees every row exactly once, a far-behind reader that is, and a
zero-leak evict. Full workspace: see §Measured for the gate lines.

## Measured

`cargo run -p shm-bench --release -- artifact`, Apple M4 Max, macOS 26.1,
2026-08-30, four runs (two before and two after the losing-shape rows were
added; the windowed rows agree across all four). 4-row `Int64` batches,
`WindowPolicy::new(keep)`, N = 100 000 commits. Max columns carry machine
noise (one run shows a 183 ms Append max under a foreground build) — read
p50/p99.

| keep | Append p50 / p99 | Window p50 (n) | delta read, 1 behind | full read | live chunks max (bound) |
|---|---|---|---|---|---|
| 16 | 1.38–1.54 µs / 1.9–3.7 µs | 3.6–4.2 µs (6250) | 0.29–0.33 µs | 7.1–8.4 µs (32 batches) | 48 (66) |
| 256 | 1.38–1.50 µs / 1.75–3.3 µs | 30.7–35.6 µs (391) | 0.29 µs | 90–99 µs (416 batches) | 576 (1026) |
| 4096 | 1.46–1.63 µs / 2.3–3.8 µs | 0.74–1.01 ms (25) | 0.25–0.29 µs | 1.31–1.66 ms (5792 batches) | 7488 (16386) |

Reading: **Append is flat at every window size** (within noise of ADR-0013's
1.4–1.6 µs), **the delta read is flat at ~290 ns** regardless of the
window, the full read stays ~220 ns/batch and the live chunk count stays
under the policy bound. A Window costs **~7.5 µs / 64 kept chunks ≈ 120
ns/chunk at 256 and ~180 ns/chunk at 4096** (member parse + two validates
+ one CAS + a 24 B descriptor write; at 4096 the ~180 KB manifest write
shows). Amortised over `keep` commits that is 0.2 µs per commit at 16, 0.12
at 256, 0.2 at 4096 — one seventh of an Append.

**Losing shapes (both runs):**

- **Window on every commit** (`keep 256, max_depth 1`): p50 6.4–6.8 µs per
  commit — 4.5× an Append, no amortisation — and the delta read doubles to
  ~0.7 µs because a 257-member table is summed per read. Live chunks 258.
  Use `max_depth ≥ keep`; the policy is the amortisation.
- **Unbounded Append at N = 20 000**: commit p50 1.4–1.5 µs (fine), live
  chunks 40 000 and growing 2 per commit, full read 8.4–8.5 ms — the row
  the policy exists to replace. The delta read is flat here too (0.33 µs):
  `batches_since` alone would have fixed the consumer, not the memory.
- **Slow reader** (`keep 256`, a pin held across 4096 commits): live chunks
  768 steady → 1536 pinned → 768 dropped. A stale pin costs exactly the
  one window it pinned — bounded at 2× the policy, never the history.

**Gates** (2026-08-30): `cargo test --workspace` green; `cargo clippy
--workspace --all-targets` clean natively and for
`x86_64-unknown-linux-gnu` with `-D warnings`. Tree uncommitted, no
`cargo fmt`. Fuzz targets (`manifest`, `manifest_chain`) are nightly-only
and were not run; their input contract is unchanged (`parse_manifest_bytes`
still never panics on arbitrary bytes — the 200 000-iteration in-crate
property test covers the new member table).
