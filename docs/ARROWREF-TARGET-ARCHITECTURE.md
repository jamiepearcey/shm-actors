# ArrowRef Target Architecture — tenancy + zero-copy translator front

- Status: Proposed (owner sign-off pending)
- Date: 2026-07-24
- Architect: Fable 5 (pure-reasoning ruling)
- Companion: [ARROWREF-INTERFACE-COLLAPSED.md](ARROWREF-INTERFACE-COLLAPSED.md) (current interface),
  [ADR-0005](decisions/ADR-0005-arrowref-task-fabric-spike.md) (shm-actors task-fabric gaps)

## The vision (owner)

The current UI defines the **contract shape**, not the capability scope — existing
backend capability is **retained**, not discarded. The shape is: **one
environment, many projects, each project a TENANT addressed as a sub-domain**
(first-class multi-tenancy). The system becomes a **fast TCP/HTTP translator
front** that terminates the wire and uses **zero-copy dispatch (shm-actors)** to a
**multi-process worker cluster**.

## Verdict: RE-FRONT + generalize-the-dispatch (not a rebuild)

ArrowRef already *is* this shape in embryo — a two-plane fd-passing dispatch
(abort-proof node + disposable DuckDB workers over mmap segments + `SCM_RIGHTS`,
ADR-0031), three protocol fronts (HTTP, S3, RESP/TCP), disposable workers. A
ground-up rebuild would discard ~46.6k LOC, the 440-test stability program, and
the hardest-won containment property to re-implement a pattern it already has.
So: **keep every capability, re-front behind tenant-resolving translators, and
swap the dispatch spine underneath (hand-rolled mmap+SCM_RIGHTS → shm-actors
general fabric) via strangler migration.** shm-actors is the spine of **one plane
of five (~20% of the system)** — not the whole thing.

## Target architecture — five planes

| Plane | Role | Job |
|---|---|---|
| **P1 Translator Front** | front | Thin, stateless protocol terminators (HTTP/SSE/WS, S3 face, RESP/TCP). Terminate wire, authenticate, resolve **subdomain→tenant**, translate to *typed descriptor envelopes*, stream responses. No retained state, no DuckDB. The RESP lane is a first-class front here, **not dead code**. |
| **P2 Dispatch Fabric** | dispatch | **shm-actors** — the four primitives over the descriptor arena. Fronts enqueue zero-copy; workers consume zero-copy. **Per-host only.** |
| **P3 Node / Control + Tenancy** | control | The existing abort-proof node: SQLite CAS catalog, CDC log-first, leases, budgets — now **also the tenancy catalog** (env/project→tenant) and the **durability anchor** for the volatile shm fabric (WAL). The shm-actors coordinator lives beside/inside it, off the data path. |
| **P4 Worker Cluster** | workers | Disposable DuckDB workers + dataset/object/task workers, mapped to **per-tenant segments via fd grants**. Containment invariant unchanged: no direct durable-write path. |
| **P5 Cross-Host Cluster** | cluster | Existing gossip + rendezvous placement, unchanged role: place tenants/scopes on home hosts, replicate bytes. |

## Multi-tenancy (project = tenant = subdomain)

- **Catalog:** a thin backend catalog **now** — tenant/env/project tables in the
  SQLite node kernel; migrate the UI's localStorage registry onto it. Scopes
  become children of tenants.
- **Front:** Host-header/subdomain → `tenant_id`; per-tenant keys + rate limits;
  S3 bucket namespace and RESP `AUTH` both select tenant.
- **Dispatch:** **capability-based isolation, not check-based** — per-tenant shm
  segments; a worker can only map what the coordinator granted. This is
  shm-actors' single strongest gift to the design.
- **Workers:** shared pools with per-tenant segment grants by default; dedicated
  pools as a policy tier for isolation-paying tenants.

## Same-host vs multi-host — the boundary rule

**Descriptors never cross a host; bytes do.** The shm-actors fabric (P2) is
strictly per-host. The existing P5 cluster places each tenant/scope on a home
host (rendezvous) and moves Arrow-IPC/replication **bytes** between hosts. No
cross-host shm ambitions, ever. ("multi-process cluster" = same-host
multi-process, shm-actors' sweet spot; multi-host stays on P5.)

## Sequence (strangler order)

1. **Tenancy catalog + subdomain resolution** in the front — purely additive; a
   default tenant preserves all 129 routes unchanged.
2. **Close shm-actors G1 (typed ref envelope) + G3 (keyed result store)** — in
   the shm-actors repo, isolated. (Prerequisites for any real surface swap.)
3. **First wedge: Tasks + batches** — back the task fabric with `shm-task`;
   node-plane WAL supplies durability (log-first, replay into shm on restart —
   bounds G8).
4. **Topics → `shm-ring`** behind the SSE/WS front. (There is no `shm-pubsub`
   crate — ADR-0001 folded that role into `shm-ring`, the SPMC broadcast ring
   with per-subscriber cursors. Note `shm-stream` is *not* pub/sub; it is the
   transactional multi-batch artifact writer.) The resumable-cursor prerequisite
   is now in place: `Subscriber::from_seq` / `Node::subscribe_from` seek to an
   explicit sequence, which is what serves `Last-Event-ID` / `?from=`.
   Still node-plane work, not substrate: the per-topic `seq` sequencer,
   retention policy (`max_messages`/`max_bytes`/`max_age_ms`), durability
   (WAL-is-truth, ring volatile), tenant prefixing, and serialising the
   multi-caller publish onto the ring's single-producer role.
5. **Only after 3–4 prove out in production:** migrate the DuckDB worker segment
   path.

**Explicitly NOT touched yet:** the ADR-0031 DuckDB containment path, S3
internals, CDC connectors, cross-host cluster.

## Biggest risk

Re-plumbing the abort-proof DuckDB worker transport onto shm-actors prematurely
and losing containment. **Bound it:** that plane migrates **last**, behind the
existing trait boundary, gated on the full 440-test suite plus the hostile
kill-9 zero-leak census. Durability always lives in the control plane, never in
shm.
