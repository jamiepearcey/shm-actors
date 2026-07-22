# ADR-0003a — PackedRef repack + pin hazard handshake

- Status: Accepted
- Date: 2026-07-22
- Decider: architecture (Fable 5), transcribed by implementation
- Companion to: [ADR-0003](ADR-0003-v0.3-scope.md) (S0 detail)

## Context

`PackedRef` packs `[segment_id:16 | generation:16 | offset:32]` into the
`AtomicU64` `ArtifactHead.manifest_desc`, read with one `Acquire` load on the RCU
read fast path. `ChunkDesc.segment_id` is already `u32`; the 2^16 artifact
data-segment-id cap lives only in `PackedRef`. Widening seg→32 cannot keep
gen(16)+off(32) in 64 bits.

## Decision

**Repack `PackedRef` as `[segment_id:32 | offset:32]`, dropping the generation
field.** (Rejected: a double-width / seqlock `manifest_desc` — it buys protection
the pin protocol must provide anyway.) The single-atomic, single-`Acquire` read
fast path is preserved.

**Version-as-ABA-guard alone is NOT sufficient.** There is a ghost-read window:
if a reader's pin publishes *after* the reclaimer's pin scan, the manifest chunk
can be freed and recycled while still holding intact old bytes; a naive
`manifest.version == pinned` check then validates against ghost data whose
`ChunkDesc`s reference freed chunks. The dropped generation never guarded this
either — closing it is mandatory regardless of the repack.

### Mandates (implemented in S1/J, the pin-lifecycle work)

1. **Hazard-pointer handshake.** Reader: publish pin (`SeqCst`) →
   re-validate the live-version-table slot `{version == v, state == LIVE}` →
   only then `Acquire`-load `manifest_desc` and deref. Reclaimer: mark the slot
   `FREEING` (`SeqCst`) *before* scanning pins / freeing. Result: either the
   reclaimer observes the pin, or the reader observes `FREEING` and retries.
2. **Manifest self-validation.** Each `VersionManifest` carries and validates
   `{artifact_id, version}`. That pair is monotonic and never reissued, so it
   subsumes generation's ABA role for the manifest pointer.

## Consequences

- S0 (shm-core) does the `PackedRef` repack and adds `artifact_id` to the
  manifest self-check surface; it leaves the workspace green.
- S1 (J) implements the full reader/reclaimer `SeqCst` hazard handshake together
  with the `BorrowJournal` `ArtifactPin` crash-reclaim, since both are the
  artifact pin lifecycle. Landing S0 and S1 in sequence before any release keeps
  the tree sound.
- Cap lifts from 2^16 to 2^32 artifact data-segment ids.
