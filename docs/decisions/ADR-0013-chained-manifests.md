# ADR-0013 — Chained manifests: `Commit::Append` in O(new data)

- Status: Accepted — implemented 2026-08-28 (see §Implementation notes)
- Date: 2026-08-28
- Builds on: ADR-0003 (S4 multi-chunk manifests), ADR-0003a (pin hazard
  handshake + manifest self-validation), ADR-0005 item R, ADR-0008 (recycle
  handshake), ADR-0010 (evict-current = empty Replace).

## Context

`Commit::Append` is documented as "turnover costs O(new data), not O(table)"
and measured as the opposite: p50 ~1.8 µs at version 0, ~11.2 µs at version
990, 6.2× and climbing; `Replace` is flat at ~1.2 µs. Holon actors append to
long-lived cells per message, so this is a per-message tax that grows with a
cell's own history.

The O(table) is in four places, not one:

1. **Commit** — `commit_staged_inner`'s `is_append` block re-parses the entire
   prior manifest, does one `borrow_shared` RMW per retained chunk, then
   writes a new manifest listing *every* chunk (`32 + 24·chunks + 4·batches`
   bytes; 27 KB at v990, rounded to the 32 KiB pool class).
2. **Retire** — `try_retire_version` re-parses and `release_chunk`s every entry.
   Version n retires when n+1 installs, so every Append pays both the O(table)
   borrow and the O(table) release of its predecessor.
3. **Pin** — `read_manifest_checked` copies the whole descriptor array; a pin on
   an Append lineage is an O(table) memcpy. The "pin is O(1)" bench measures
   Replace lineages only.
4. **Read** — `as_arrow` reconstructs each batch zero-copy, then for >1 batch
   calls `concat_batches`, which copies every byte. "as_arrow never copies" is
   already false for any multi-batch version.

Refcount model today: a data chunk's refcount = number of live versions listing
it; a version owns one ref per listed chunk plus its manifest chunk.

## Options

| | Commit | Pin | Read | Retire | Verdict |
|---|---|---|---|---|---|
| 1. Chained manifests | O(new) + 1 RMW | O(own) | O(batches) walk | O(1) amortised, cascade once | **chosen** |
| 2. Refcounted chunk-run object | as 1 | as 1 | as 1 | as 1 | folded into 1: the prior manifest *is* the run object |
| 3. Bounded chain + flat every K | O(new), O(table) burst per K | O(table/K) | walk ≤ K | cascade + flat | rejected: bounds a walk that is not the bottleneck; puts a policy constant in the ABI |
| 4. Policy only (document O(table); actors compact) | 11 µs @v990, 100 µs @v10k | O(table) | O(table) | O(table) | rejected as the substrate answer; kept as the batch-count answer (§Read) |

Why 1 for Holon: commit and pin become independent of history; the chain *is*
a delta log (a snapshot follower at version k walks from head until
`prev_version == k` and receives exactly the new batches, O(delta) — a flat
manifest cannot provide that); adopt still reconstructs everything from
manifests alone; chain length is bounded by refcount, not by
`MAX_LIVE_VERSIONS`.

## Decision

A version's manifest lists only its **own** chunks and carries a validated link
to its predecessor's manifest. Ownership becomes:

> A version owns exactly one reference: its manifest chunk. A manifest owns one
> reference on each data chunk it lists and one on its predecessor manifest (if
> any). Whoever releases the last reference on a manifest releases the
> manifest's own references (cascade).

No new registration point (ADR-0008); the pin hazard handshake (ADR-0003a) is
unchanged.

### Layout — `SHMMFST4`, 64-byte header

```
off  size  field            notes
 0    8    magic            b"SHMMFST4"
 8    8    version          self-id (as today)
16    4    artifact_id      self-id (as today)
20    4    schema_id        own chunks' schema; MUST equal prev's when linked
24    4    chunk_count      OWN data chunks only
28    4    batch_count      OWN batches only
32    8    prev_version     0 = root; else < version
40    8    prev_ref         PackedRef bits of predecessor manifest; 0 = root
48    4    prev_gen         ChunkCtrl generation of prev at link time; 0 if root
52    4    depth            0 for root, else prev.depth + 1 — the walk bound
56    4    total_batches    own + prev.total_batches (saturating); re-validated by the walk
60    4    _reserved        0
64   24·chunk_count  ChunkDesc[] (own, row order)
..    4·batch_count  u32 span[] (sum == chunk_count)
```

Root-strictness (parse-time, fuzzed): `prev_ref == 0 ⇔ prev_version == 0 ⇔
prev_gen == 0 ⇔ depth == 0`; when linked `prev_version < version`; always
`total_batches >= batch_count`; `_reserved == 0`. Violations → `VersionGone`.
An Append of one single-chunk batch is 92 B (128 B class); an empty
evict-current manifest is 64 B and still fits every class ≥ 64.

### Commit (replaces `commit_staged_inner` steps 2–5)

1. Gate + publish staged chunks (unchanged).
2. If Append with a prior: `read_manifest_checked(prior)`; reject on schema
   mismatch (`Unsupported`); `link = {mref, version: expect}`.
3. **`borrow_shared` on the prior *manifest* chunk — one RMW** — record its
   generation into the link; re-validate the prior by identity after the
   borrow (our ref freezes the bytes; a recycled chunk fails identity →
   rollback → `Conflict`).
4. Write + stage the new manifest (own chunks, link, `depth+1`,
   `total_batches`).
5. Claim slot, fenced revalidate, install CAS — unchanged. On failure
   `rollback_staged` releases the link through the cascade (a concurrent
   Replace may have made our link the last reference).

### Retire / cascade (replaces the `try_retire_version` tail)

```
release_manifest_ref(mref):
  loop:
    parsed = read_manifest(mref)          // under our held ref: immutable, mapped
    if !release_chunk_freed(mref): return // still referenced (a successor links it)
    for c in parsed.chunks: release_chunk(c)
    mref = parsed.prev or return
```

Per-version retire is one `release_shared`. The cascade runs exactly once per
manifest, elected by `try_reclaim`'s `PUBLISHED → FREE` CAS. A freed manifest
is never ghost-read: `Pool::free` overwrites its first word with the Treiber
link (magic fails), `prev_gen` fails `ctrl.validate`, and identity fails.

### Pin + read

`pin_inner` is byte-for-byte the same; it parses only the head manifest.
`VersionPin` gains `chain()` (oldest-first, via a pure `walk_chain_with`
bounded by `head.depth` with strictly decreasing versions and depth
continuity), `data_chunks()`, and `as_arrow_batches()` (zero-copy, one
`RecordBatch` per batch). `as_arrow()` on >1 batch copies via `concat_batches`
and says so. Under a pin every chain member is `PUBLISHED` with `refcount ≥ 1`
by the transitive link argument.

The honest contract: **commit and pin are O(new data); read is O(batches);
zero-copy per batch.** Controlling batch count is a policy of the layer above
(compact by `Replace` of a concatenated batch past a depth threshold).

## ABI

`VersionManifest` 32 → 64 B, `SHMMFST3 → SHMMFST4`. `ArtifactHead`, `PinSlot`,
`ChunkDesc`, `PackedRef`, `JournalEntry`, `TypedRef`, catalog, task queue:
unchanged. A data chunk's refcount now means "manifests listing it" (1 in
practice) — not on-shm ABI, but stated in the retire and manifest docs.

## Interleaving risks

- Ghost read: unchanged (pin is the only registration).
- Append vs concurrent Replace: loser's rollback cascades → leak-free.
- ChunkCtrl split-word window (`refcount == 0` check then `PUBLISHED → FREE`
  CAS as separate words): pre-existing; now exposes one chunk per Append
  instead of O(table). Mitigated by validate-after-borrow + `ctrl.validate`
  before rollback release. Real fix (packed `{state, refcount}` word) is a
  separate shm-core item.
- Two releasers cascading one manifest: each ref released exactly once by its
  sole owner; add `loom_ctrl` — two `release_shared` on a refcount-2 chunk,
  exactly one observes `true`.
- Reader pinned on an old version while the chain ahead dies: cascade stops
  at the pinned manifest (refcount 1 from the pin's version ref).
- Committer death between borrow and install: leaks one manifest + own chunks
  + one link ref. Today's equivalent leaks a phantom ref on *every* prior
  chunk. Strictly narrower; root cause (inline staging unjournaled) is a
  follow-up.
- Adopt / census: roots = every `SLOT_LIVE` `PinSlot.manifest`; mark = walk
  chain. Everything reachable from manifests alone.

## Test plan

Unit: 64-B round trip with/without link; root-strictness rejections;
short-buffer loop; miri as today. Walker: happy path, cycle, depth overrun,
non-decreasing version, schema drift, `total_batches` mismatch. Fuzz: existing
`manifest` target + new `manifest_chain` target (never panics, never loops).
Integration (`rcu.rs`): (a) 1000 appends in a pool whose largest class is
256 B — impossible today; (b) Replace after a long lineage frees the whole
chain to baseline; (c) pinned prefix survives the cascade; (d) optimistic
Append vs Replace conflict ×1000, census exact; (e) per-batch zero-copy
pointer check; (f) self-link rejected; (g) leaked-pin replay on an Append
lineage; (h) `evict_all` on a lineage; (i) schema mismatch rejected. Runtime:
add Append to the `shm-cacheloop` churn mix so the kill-9 census covers chains.
Bench: Append series expected flat within noise of Replace; pin and
`as_arrow_batches` vs depth 1/100/1000/10000 — pin flat, read linear in
batches only.

## Implementation order

1. `manifest.rs`: header, `ManifestLink`, strict parse, `write_manifest`
   signature, `walk_chain_with`, unit tests, fuzz target.
2. `artifact.rs`: `release_chunk → bool`, `release_manifest_ref`, retire tail,
   commit steps 2–5 + rollback, `VersionPin` accessors, `as_arrow_batches`.
3. `rcu.rs` updates + new tests; `loom_ctrl.rs`.
4. `shm-stream` doc, cacheloop op mix, bench, spike check.
5. Supersede ADR-0003 S4's "manifests unchanged"; record the two follow-ups
   (journal the inline-commit manifest; pack the ChunkCtrl word).

## Implementation notes (2026-08-28)

Landed as designed, with two deviations from the text above:

- **`prev_gen` is not part of the root iff.** `ChunkCtrl` generations start at
  `0` (`Pool::create` → `ChunkCtrl::init_at(_, 0)`), so a link to a
  never-recycled manifest chunk legitimately records `prev_gen == 0`. The
  parse-time rule is therefore `prev_ref == 0 ⇔ prev_version == 0 ⇔ depth == 0`,
  plus `prev_gen == 0` *when root*; a linked manifest's `prev_gen` is any value.
- **Rollback of the link is generation-guarded.** The committer samples the
  prior manifest chunk's generation *before* `borrow_shared`, re-validates the
  prior by identity after it, and requires the generation unchanged; a
  rollback releases the link only if the chunk still carries that generation.
  Across the pre-existing `ChunkCtrl` split-word window the chunk's next
  occupant's `try_loan` resets the refcount, so a release there would take a
  reference that is not ours. This is the "validate-after-borrow +
  `ctrl.validate` before rollback release" mitigation, made concrete.

Also recorded: `ManifestLink` carries the predecessor's `depth` and
`total_batches` (derived from the header on parse) so the walker checks depth
continuity and the batch total at every hop; `walk_chain_with` additionally
requires a root's `total_batches == batch_count`. `VersionPin::chain` compares
each link's `generation` against the live `ChunkCtrl` before the identity read.

Verification: `rcu.rs` (a)–(i) green; `loom_ctrl` (two concurrent
`release_shared` on one `refcount == 2` chunk — exactly one election, the
chunk ends `FREE` at generation `+1`) green and shown to fail against a non-RMW
`release_shared`; `manifest_chain` fuzz target builds. Bench:
Append p50 went from ~1.8/2.7/6.1/11.2 µs (version index 0/100/500/990) to flat
within noise of Replace; pin p50 stays at the timer floor at depth 10 000.

### Follow-ups (not in this ADR)

1. **Journal the inline-commit manifest.** A committer dying between the link
   borrow and the install CAS leaks one manifest + its own chunks + one link
   reference (strictly narrower than the pre-ADR phantom ref on every prior
   chunk). Root cause: inline staging is unjournaled.
2. **Pack `{state, refcount}` into one `ChunkCtrl` word** (shm-core). The
   `refcount == 0` check and the `PUBLISHED → FREE` CAS in `try_reclaim` are
   separate words; ADR-0013 exposes one chunk per Append to that window instead
   of O(table), and guards it as described above, but the real fix is the
   packed word. A loom model of `borrow_shared` vs `release_shared` on a
   `refcount == 1` chunk would find the window today and is deliberately not
   added until it does.

## Addendum — post-review fixes (2026-08-28)

An adversarial read found the ownership rule and the cascade sound under
successful installs, and three medium holes in the **failed-commit corners**,
all fixed:

- **F1 — a held reference dropped.** Step 3's early-fail path (identity or
  generation re-check failed after a *successful* `borrow_shared`) rolled back
  with `link = None`, never releasing the +1. When the generation still
  matched, that +1 was provably on a still-`PUBLISHED` occupant — e.g. the
  prior manifest chunk freed, re-popped (LIFO) and republished as a different
  manifest before our sample — and skipping it leaked that occupant and
  everything it would ever cascade: O(depth), not one chunk. Now released
  through the cascade whenever the sampled generation still matches; only a
  moved generation (the split-word case, where `try_loan` already wiped the
  bump) is skipped.
- **F2 — rollback double-release into a sibling's chunk.** `evict_all`
  retires every `SLOT_LIVE` slot, including a committer's claimed-but-
  uninstalled one (optimistic committers hold no lease, so `EVICTOR_OWNER`
  does not exclude them). The straggler's rollback then released chunks the
  eviction had already freed — and, in the shared store pool, possibly
  re-published to a sibling entry. `release_chunk` now **validates the
  descriptor's generation** before decrementing, and rollback frees the
  claimed slot only if it still holds the committer's version.
- **F3 — retiring a stale duplicate slot.** `find_slot` returned the *first*
  live slot for a version; a late optimistic committer's duplicate claim for
  the same version could be retired in place of the endorsed one, stranding
  the real version's manifest and its whole chain. The post-install retire
  now targets the slot whose manifest is the one the install replaced
  (`find_slot_with_manifest`).
- **F5/F7 — cascade hops validate.** `release_manifest_ref` carries the
  generation at every hop (a link's recorded `generation`; a rollback's
  sampled one); only the retire's first hop — a reference the caller provably
  owns — is unvalidated by construction.
- **F6 (doc correction).** "Committer death between borrow and install leaks
  one manifest + own chunks + one link ref" understated it: the leaked link
  pins the prior manifest, so the **entire chain beneath it** leaks. Pre-ADR
  leaked every prior data chunk but no manifests; this is comparable in data
  and worse in manifests, not "strictly narrower". Follow-up #1 (journal the
  inline-commit manifest) is therefore not optional.
- A leftover `DIAG` print block in `churn_soak` was removed.

Left recorded: **F4** — `borrow_shared` on a just-freed chunk can wrap the
refcount to `u32::MAX` if `try_loan` resets it mid-borrow, so the next
occupant's first `borrow_shared` wraps to 0 and its publish frees the chunk it
is installing. Pre-existing, one chunk per Append now instead of O(table);
belongs with follow-up #2 (pack `{state, refcount}` into one word). **F8** —
`evict_current` stamps the set-once `head.schema_id` onto the empty root, so
an Append with the newer schema after a Replace-then-evict is refused. A wart,
not a safety issue.
