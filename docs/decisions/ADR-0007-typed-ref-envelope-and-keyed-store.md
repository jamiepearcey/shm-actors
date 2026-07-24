# ADR-0007 — Typed ref envelope (G1) + keyed store (G3)

- Status: Accepted
- Date: 2026-07-24
- Decider: architecture (Fable 5, pure-reasoning), transcribed by implementation
- Context: the two ADR-0005 gaps prerequisite to the ArrowRef task-fabric wedge
  (see [ARROWREF-TARGET-ARCHITECTURE.md](../ARROWREF-TARGET-ARCHITECTURE.md)):
  fronts translate wire requests into typed descriptor envelopes and dispatch
  them zero-copy; workers resolve them against a keyed store.

## Decision

### G1 — Typed ref envelope: envelope-in-a-chunk (option a)

The wire unit stays the **24B `ChunkDesc`**. A typed ref is a payload chunk
holding a POD envelope, tagged by a reserved system schema id `SCHEMA_TYPED_REF`.

```rust
#[repr(C)] pub struct TypedRef {          // 56 bytes, Copy, explicit fields
    magic: u32,        // 0x54524546 "TREF"
    abi_version: u16,  // = 1
    kind: u16,         // RefKind: 0=RawChunk,1=Object,2=Artifact,3=Dataset,4=Result
    key_id: u32,       // coordinator-interned key (0 = none, kind=RawChunk)
    schema_id: u32,    // schema of the REFERENT payload (0 = raw)
    version: u64,      // 0 = current; else assert-match
    locator: ChunkDesc,// optional resolved fast path (all-zero = resolve by key)
    manifest: u64,     // PackedRef to a VersionManifest; 0 = none
}
```

`key_id` is authoritative; `locator`/`manifest` are an optional resolved fast path.

### G1 — Key representation: coordinator-interned `key_id: u32`

Keys are **opaque byte strings (≤1024B)** interned over UDS (`InternKey`/
`ResolveKey`), nodes cache both directions — the proven `schema_id` precedent,
keeping every shm struct fixed-size POD. Tenant/scope prefixes are just bytes
inside the key, so P3 tenancy costs **zero ABI change**. (Fixed `[u8;N]` bloats
and caps; key-in-chunk adds a lifetime/refcount problem.)

### G3 — Keyed store: new crate `shm-store`

A keyed collection **of** shm-artifact artifacts; **do not modify shm-artifact**
(RCU is reused, not reinvented). Registry of record in the **coordinator**
(`key_id → {artifact_id, head segment_id+offset, kind, state}`), with an shm
**catalog region** (store-owned management segment) as the fast path:

```rust
#[repr(C)] struct CatalogSlot {           // append-only, CAS state machine
    key_id: u32, artifact_id: u32, head_off: u32,
    kind: u16, _pad: u16, state: AtomicU32, // FREE→LIVE→TOMBSTONE
}
```

Workers resolve key→head with no UDS round-trip after warm-up. Lifecycle:
- `create(key)` — coordinator interns key, allocates an `ArtifactHead` in a store
  management segment, publishes the slot.
- `open(key_id)` → head pointer.
- commit / pin / read / append — the **unchanged** shm-artifact RCU APIs (write
  lease, hazard pins, refcounted chunk sharing).
- `evict(key_id)` — TOMBSTONE the slot, drain pins via the existing hazard scan,
  reclaim chunks by refcount, retire the artifact_id.
- crash reclaim = the existing lease-sweep, now also clearing catalog slots owned
  by dead nodes.

### G1×G3 composition

A front builds `TypedRef{kind:Dataset, key_id, version}`, writes it as a 56B
chunk, dispatches its `ChunkDesc` (schema_id=`SCHEMA_TYPED_REF`) through a
ring/task slot. The worker sees the system schema id, reads the envelope,
resolves `key_id` via catalog (UDS on miss) to the `ArtifactHead`, pins, checks
`version` (0=current; mismatch → `VersionMismatch`), zero-copy reads via the
manifest, unpins on completion.

### ABI impact: pure additive

**Zero change** to `ChunkDesc`, `PackedRef`, `ArtifactHead`, `VersionManifest`,
ring `Slot`, `TaskSlot`. New: `TypedRef`, `RefKind`, `shm-store` crate,
`CatalogSlot`, UDS `InternKey`/`ResolveKey`/`CreateEntry`/`OpenEntry`/
`EvictEntry`. One flagged convention: **reserve schema_id 1–15 as system ids**
(envelope = 1); coordinator starts user issuance at 16 and advertises
`SCHEMA_TYPED_REF` in the handshake.

### Build order

**G3 first** (an envelope with nothing to resolve proves nothing). Walking
skeleton: (a) process A `shm-store` creates `"dataset/X"`, commits v1–v3; process
B opens by key, pins, asserts bytes+version; (b) G1: front sends a `TypedRef`
through the task queue, worker resolves→pins→reads, commits a `Result`-kind entry
`"result/X"`; (c) crash: `kill -9` the worker mid-pin, coordinator lease-sweep
clears the pin, `evict("dataset/X")` completes, chunk census shows zero leaks
(mirror the hostile-cache-loop test).
