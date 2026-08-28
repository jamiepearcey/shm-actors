# ADR-0014 — Closing the four residual correctness contracts

- Status: Accepted (implemented)
- Date: 2026-08-28
- Builds on: ADR-0008 (recycle handshake), ADR-0010 (lifecycle leases),
  ADR-0013 (chained manifests), ADR-0004 stage L (loom discipline).

## Context

After Phase 0 the architecture document's §14 listed four known races as
"residual". They were not caveats; each contradicted a stated invariant
(I3 *state outlives actors*, I5 *concurrent writers conflict, never
corrupt*). A Phase 0 that documents them is not complete. This ADR closes
all four with existing primitives — no new protocol, one new journal tag,
two packed atomic words.

## 1. Recycle race — occupant identity inside the state word (`SHMSTOR3`)

**Was:** `evict` CASed a catalog slot `LIVE → TOMBSTONE` by *state alone*,
then re-checked `gen`/`key_id`. A full recycle landing inside that window,
plus a sweep claiming the wrongly-tombstoned slot before the undo, could
tear down an innocent new occupant. The undo itself was a bounded retry
loop against a slot that might be parked in `RECLAIMING` forever.

**Now:** `CatalogSlot.word = {gen:32 | state:32}` — one atomic. `evict`
does one CAS `{gen, LIVE} → {gen, TOMBSTONE}` on the occupant it found by
key. A recycled slot carries a different gen; the CAS fails; nothing was
touched; there is nothing to undo. `finish_reclaim` advances the gen and
returns the slot to `FREE` in one store. `untombstone` and its retry loop
are deleted. Slot 28 → 32 bytes, align 8.

## 2. `ChunkCtrl` split-word window — `{state | refcount}` in one word (`SHMPOOL2`)

**Was:** `borrow_shared` = `fetch_add`, load state, undo on mismatch;
`try_reclaim` = check `refcount == 0`, then CAS `PUBLISHED → FREE` on a
*different* word; `try_loan` reset the count with a plain store. Three
consequences: a borrow could land after the reclaimer's zero-check and hold
a reference onto a chunk one CAS from `FREE`; `try_loan`'s reset could wrap
a racing borrow's undo to `u32::MAX`, so the next occupant's first borrow
wrapped to zero and its publish freed the chunk it was installing; and
every release path had to validate generations defensively.

**Now:** `ChunkCtrl.word = {state:32 | refcount:32}`. `borrow_shared` is
`CAS {PUBLISHED, n} → {PUBLISHED, n+1}` — it cannot succeed on a chunk that
is not `PUBLISHED` at the instant of the CAS. `try_reclaim` is `CAS
{PUBLISHED, 0} → {FREE, 0}` — a borrow before it makes the count non-zero,
a borrow after it fails. `try_loan` is `CAS {FREE, 0} → {LOANED, 0}` — no
separate count reset exists. `release_shared` frees in the same CAS when it
takes the count from 1 to 0 with the owner gone, and re-runs the reclaim
behind a `SeqCst` fence otherwise; `owner_release` stores, fences,
reclaims — the Dekker pairing the substrate's own fence doc prescribes.

Loom: `loom_ctrl` gained `borrow_vs_reclaim_never_both` (the closed window,
stated as `borrowed ^ reclaimed`) and `release_vs_owner_release_exactly_one_frees`.
The latter **failed on the first cut** — the fences were missing, and
loom's acq/rel model of unfenced `SeqCst` found the store-buffering
interleaving where neither side frees. That is the harness doing its job.

## 3. Committer death before the install CAS — journal the staged manifest

**Was:** the inline commit path loaned data chunks, borrowed the prior
manifest link, staged its own manifest and claimed a slot, all unjournaled.
Death before the install CAS leaked all of it — and with chained manifests
the leaked link pinned the **entire chain** beneath it.

**Now:** a new journal tag `ENTRY_STAGED_MANIFEST` (`{artifact_id,
incarnation, manifest PackedRef bits, generation}`) is recorded the moment
the manifest chunk is staged and released after the install CAS (or after
rollback). Coordinator replay routes it by `(artifact_id, incarnation)` and
calls `reclaim_staged_manifest`, which releases the manifest **iff no live
pin slot endorses it** — an installed version's manifest is referenced by
its slot, so a record that outlived a successful install (death between
install and journal release) is a no-op, and the release is
generation-validated so a rolled-back-and-reallocated chunk is never
touched. The release cascades: own data chunks, then the link. A new tag
within the existing slot layout — `JOURNAL_MAGIC` unchanged.

Every commit path that has a journal now uses it: the exclusive committer
(its `LeaseJournal`), `shm-stream`'s staged commits, and the keyed store's
`Entry::commit`. The bare optimistic entry points remain for callers with
no journal and are documented as such.

## 4. Zombie double-decrement — the journal slot *is* ownership

**Was:** an actor declared dead by lease expiry but still running would,
on its eventual pin drop, decrement a count the coordinator's replay had
already decremented. `try_unpin` bounded it to a stolen reference rather
than a wrap; it did not remove it.

**Now:** `BorrowJournal::release` returns whether *this caller* cleared the
bit (`fetch_and` on the occupancy word — a CAS election). Replay clears
each bit **before** reclaiming it (`replay_indexed`); every clean release
path — `PinState::drop`, `Committer::drop`, `shm-stream`'s staged chunks,
`Node::release_pin` — performs its shared-memory decrement **only if it won
the bit**. Exactly one party ever decrements. For the retain → arm handoff
the order is reversed: the journal bit is released *before* arming, and a
lost election refuses the arm (`Error::Stale`) — correctness over a
few-instruction leak window, which is documented.

## Invariants restored

I3 and I5 hold without footnotes. The remaining documented limits are
performance shapes (drain worst case on shared LIFO heads; `find_live_by_key`
O(high-water)) and platform coverage (Linux unexecuted) — not correctness.

## ABI

`SHMSTOR2 → SHMSTOR3` (slot 28 → 32 B, align 8); `SHMPOOL1 → SHMPOOL2`
(`ChunkCtrl` 16 B unchanged, align 4 → 8; every consumer uses the
accessors). `ENTRY_STAGED_MANIFEST = 4` within `SHMJRNL3`. No other
structure changes.
