# ArrowRef / query-cache — Collapsed Interface Contract

> **Purpose.** A distilled, **UI-driven** contract of what ArrowRef's interface
> *currently* is — derived from what the console actually calls, not from the
> backend's 131 declared routes. The backend implementation has sprawled
> (~46.6k LOC across ~40 modules, ~131 `.route()` registrations); the UI needs a
> far smaller surface. This is the reference point for the strategic decision:
> **rebuild ArrowRef's same-host core on the shm-actors substrate vs incrementally
> integrate.** This document does not make that call — it supplies the honest
> inputs.
>
> **Provenance & scope.** Evidence read on 2026-07-24. `query-cache` was treated
> **read-only** (nothing modified). Ground truth:
> - UI client: `query-cache/repo/ui/src/lib/arrowref.ts` (1727 LOC — every export is a capability the UI needs)
> - Console product registry: `query-cache/repo/ui/src/lib/infra.ts` (515 LOC — localStorage-only, maps product names → node calls)
> - Views: `query-cache/repo/ui/src/components/views/*.tsx` (22 views)
> - Backend routers: `src/http.rs`, `src/scope/http.rs`, `src/cluster/http.rs`
> - shm-actors: `README.md`, `docs/decisions/ADR-0005-arrowref-task-fabric-spike.md`, crate tree

---

## 0. Headline numbers

| Measure | Value | Source |
|---|---|---|
| Backend `.route()` registrations | **131** (129 excl. `/hang`,`/fast` test routes) | `src/http.rs` (113 same-line + multi-line), `src/scope/http.rs` (15), `src/cluster/http.rs` (8). The oft-cited "113" counts only same-line `.route("…"` literals; multi-line registrations add 18. |
| Distinct HTTP method-handlers | **~202** (verbs on route builders) | many routes are multi-method (`get().put().delete()`) |
| Backend `src` LOC | **~46,612** | `find src -name '*.rs' \| wc -l` |
| UI client exports (capabilities) | **~113** functions/consts | `arrowref.ts` — **all referenced by ≥1 view; no dead client exports** |
| Routes **USED** by the UI | **91 / 129** (71%) | client + view (SSE/WS) reference audit |
| Routes **UNUSED** by the UI | **38 / 129** (29%) | legacy execution path, server↔server cluster, test/probe |
| shm-actors substrate LOC | **~21,360** | 4 primitives + coordinator; **zero** HTTP/SQL/SSE/network deps (only `libc`,`nix`,`bytemuck`,`arrow`,`thiserror`) |

The UI-required interface is **~13 product capabilities backed by 91 routes**. The
implementation-vs-interface gap is large: the 38 dead routes plus the entire
RESP/TCP control lane (`tcp_control.rs`, 2729 LOC, never touched by the UI) and
the legacy `frames`/`queries`/`results`/`transforms` execution path are
implementation that the current product face does not exercise.

---

## A. Current Interface Contract (product vocabulary)

The console thinks in **Environments → Projects → Caches / Scratchpads / Topics /
Tasks / Queues / Sources**. *Environments and Projects are a **client-only**
construct* (`infra.ts`, localStorage key `arrowref.infra.v1`); the node has no
such concept — it sees a flat `env.project.name` string namespace. Everything
below is what the node must actually serve.

### 1. Objects — opaque key → bytes cache
Backing store: `object_store.rs` (1404) + `multipart_upload.rs` (317).
| Operation | Client fn | Route |
|---|---|---|
| GET bytes (ETag, content-type, status) | `getObject` | `GET /objects/*key` |
| PUT bytes (+ optional `ttl_ms`) | `putObject`,`putObjectBytes` | `PUT /objects/*key` |
| DELETE | `deleteObject` | `DELETE /objects/*key` |
| Conditional revalidate (If-None-Match → 304) | `revalidateArrow` | `GET` with `if-none-match` |
| Multipart upload (deflate transport, ≥4 MiB) | `multipartUploadObject` | `POST /uploads`, `PUT /uploads/:id/parts/:n`, `POST /uploads/:id/complete`, `DELETE /uploads/:id` |
**Load-bearing HTTP semantics:** ETag/`If-None-Match`/304, `ttl_ms`, chunked+`content-encoding: deflate` multipart. Not actor semantics — HTTP object-store semantics.

### 2. Artifacts — versioned view over the same store (ADR-0019)
| Operation | Client fn | Route |
|---|---|---|
| GET by tag/version (+ `x-artifact-version/latest/tags` headers) | `getArtifact` | `GET /artifacts/:key` |
| List versions | `listArtifactVersions` | `GET /artifacts/:key?versions` |
| List tags | `listArtifactTags` | `GET /artifacts/:key/tags` |
Object (`/objects`) and Artifact (`/artifacts`) are **two faces of one store**; the UI treats "object ↔ dataset ↔ artifact" as one thing with three representations.

### 3. Datasets / Caches — Arrow, scope-backed retained tables
Backing: `storage.rs` (2321) + `shared_arena.rs` (574).
| Operation | Client fn | Route |
|---|---|---|
| Read whole dataset (Arrow IPC, ETag) | `fetchArrow`,`refToPath` | `GET /datasets/:name` |
| Append/put chunk (Arrow IPC) | `putDataset`,`putDatasetIpc`,`appendDatasetChunk` | `PUT /datasets/:name/chunks` |
| Append-only delta read (cursor) | `datasetChunksDelta` | `GET /datasets/:name/chunks/delta` |
| Keyed replica (CDC): declare/read/change/snapshot | `createReplica`,`getReplica`,`deleteReplica`,`readReplica`,`replicaChanges`,`replicaSnapshot` | `PUT/GET/DELETE /datasets/:name/keyed`, `GET /datasets/:name/read`, `POST /datasets/:name/changes`, `POST /datasets/:name/snapshot` |
| Object↔dataset promotion | `promoteObjectToDataset`,`materializeDatasetToObject` | `POST /datasets/:name/from-object`, `POST /datasets/:name/to-object` |
| Read frame/result/query as Arrow | `refToPath` | `GET /frames/:id/arrow`, `GET /results/:id/arrow`, `GET /queries/:id/result.arrow` |
**Vocabulary note:** the UI "Cache" (`CacheRef.scopeKey`, `infra.ts`) is a **scope**, and its data unit is a **dataset** (`scope/name`) — see §4.

### 4. Scopes — the workspace primitive (ADR-0023; UI "Cache")
Backing: `scope/` (6616 LOC — the single biggest module area).
| Operation | Client fn | Route |
|---|---|---|
| List / create / get / update / delete | `listScopes`,`createScope`,`getScope`,`updateScope`,`deleteScope` | `GET/POST /scopes`, `GET/PUT/DELETE /scopes/:scope` |
| Catalog: items / variants / warm-queries | `listItems`,`listVariants`,`listWarmQueries`,`addItem` | `GET/POST /scopes/:scope/items`, `GET /scopes/:scope/variants`, `GET /scopes/:scope/warm-queries` |
| Variant lifecycle: pin / refresh / delete | `pinVariant`,`refreshVariant`,`deleteVariant` | `POST …/variants/:id/pin`, `POST …/refresh`, `DELETE …/variants/:id` |
| Metadata reload / scope reload / scan | `reloadMetadata`,`reloadScope`,`scanScope` | `POST /metadata/reload`, `POST /scopes/:scope/reload`, `POST /scopes/:scope/scan` |
| Scope query (Arrow) | `scopeQuery` | `POST /scopes/:scope/query` |
| **Working area (DuckDB):** open / info / quack card / exec / query / promote / list+preview datasets | `openScope`,`scopeWorkInfo`,`scopeWorkQuack`,`scopeWorkExec`,`scopeWorkQuery`,`scopePromote`,`listScopeDatasets`,`scopeDatasetPreview` | `POST /scopes/:scope/open`, `GET …/work`, `GET …/work/quack`, `POST …/work/exec`, `POST …/work/query`, `POST …/promote`, `GET …/datasets`, `POST …/datasets/:name/query` |
**Load-bearing:** scope "work" area is **DuckDB SQL execution** + a "Quack" connection card (ADR-0028). Not actor semantics — a SQL engine.

### 5. Scratchpads — DuckDB SQL sandboxes (ADR-0020)
Backing: `scratchpad.rs` (1180) + `duckdb_exec.rs` (424).
| Operation | Client fn | Route |
|---|---|---|
| List | `listScratchpads` | `GET /scratchpads` |
| Exec DDL/DML | `scratchpadExec` | `POST /scratchpad/:name/exec` |
| Query (Arrow) | `scratchpadQuery` | `POST /scratchpad/:name/query` |
| Attach artifact/source as table | `scratchpadAttach` | `POST /scratchpad/:name/attach` |
| Refresh bound tables | `scratchpadRefresh` | `POST /scratchpad/:name/refresh` |
| Promote query → versioned artifact + arena snapshot | `scratchpadPromote` | `POST /scratchpad/:name/promote` |
**Load-bearing:** entirely a **DuckDB SQL** capability.

### 6. Topics — pub/sub over SSE/WS (ADR-0021/0022)
Backing: `pubsub.rs` (502).
| Operation | Client fn | Route |
|---|---|---|
| List / get / put / delete | `listTopics`,`getTopic`,`putTopic`,`deleteTopic` | `GET /topics`, `GET/PUT/DELETE /topics/:topic` |
| Publish | `publishTopic` | `POST /topics/:topic/publish` |
| Subscribe (resumable, `from=`/`Last-Event-ID`) | `topicSseUrl`,`topicWsUrl` | `GET /topics/:topic/sse`, `GET /topics/:topic/ws` |
**Load-bearing:** browser-native **SSE + WebSocket** transport with cursor resume.

### 7. Tasks + Task-batches — descriptor-only work fabric (UI "durable messages")
Backing: `runtime.rs` (2552) + `browser.rs` (1652) + `task_journal.rs` (1045).
| Operation | Client fn | Route |
|---|---|---|
| Submit (descriptor + retained `InputRef`, never payload) | `submitTask` | `POST /tasks` |
| Submit batch | `submitTaskBatch` | `POST /task-batches` |
| Get / wait (long-poll to terminal) | `getTask`,`waitTask`,`waitTaskBatch` | `GET /tasks/:id`, `GET /tasks/:id/wait`, `GET /task-batches/:id/wait` |
| Ack (+ clear-on-ack) | `ackTask` | `POST /tasks/:id/ack` |
| Fetch retained output (Arrow/bytes) | `getTaskOutput` | `GET /tasks/:id/output` |
`InputRef` union: `dataset_query` \| `arrow_ref` \| `parquet_ref` \| `bytes_ref` \| `stream_ref` (`arrowref.ts` l.359). UI "durable message" desugars to `plugin: task.echo` (`durableMessageBody`). **This is exactly the surface ADR-0005 spiked.**

### 8. Queues — durable work queues (ADR-0027/0030)
Backing: `queue.rs` (1142) + `wal.rs` (642).
| Operation | Client fn | Route |
|---|---|---|
| List / ensure(PUT) / stats / delete | `listQueues`,`ensureQueue`,`getQueueStats`,`deleteQueue` | `GET /queues`, `PUT/GET/DELETE /queues/:name` |
| Enqueue / peek / receive(lease) / ack / nack | `enqueueMessage`,`peekQueue`,`receiveMessages`,`ackMessages`,`nackMessages` | `POST …/messages`, `GET …/peek`, `POST …/receive`, `POST …/ack`, `POST …/nack` |
| DLQ list / requeue | `dlqMessages`,`dlqRequeue` | `GET …/dlq/messages`, `POST …/dlq/requeue` |
Console policy (`infra.ts QueuePolicy`) is mapped to node wire policy by `toNodeQueuePolicy` (`order`, `visibility_timeout_ms`, `max_receive_count`, DLQ, durable).

### 9. Sources — L3 named ingestion (ADR-0020)
Backing: `source.rs` (1328) + `cdc_replica.rs` (1134) + `pg_cdc.rs` (257).
| Operation | Client fn | Route |
|---|---|---|
| List / put / delete | `listSources`,`putSource`,`deleteSource` | `GET /sources`, `PUT/DELETE /sources/:name` |
| Test saved / dry-run spec | `testSource`,`testSourceSpec` | `POST /sources/:name/test`, `POST /sources/test` |
| Overview inventory | `sourceOverview` | `GET /sources/:name/overview` |
`SourceSpec` kinds: `artifact` \| `file` \| `webhook`(+OAuth2) \| `scope` \| `s3` \| `postgres`(CDC). **Load-bearing:** live connectors incl. Postgres logical-replication CDC.

### 10. Policies — reusable governance (ADR-0020)
| Operation | Client fn | Route |
|---|---|---|
| List / get / put / delete | `listPolicies`,`getPolicy`,`putPolicy`,`deletePolicy` | `GET /policies`, `GET/PUT/DELETE /policies/:name` |

### 11. Groups — lifecycle budgets
| Operation | Client fn | Route |
|---|---|---|
| List / usage / upsert | `listGroups`,`listGroupUsage`,`putGroup` | `GET /groups`, `GET /groups/usage`, `PUT /groups/:name` |
Memory/spill budgets, TTL, eviction, `clear_on_ack_default` (`model.rs LifecycleGroupConfig`).

### 12. Metrics / health / cluster / S3 / worker-plane
| Operation | Client fn | Route |
|---|---|---|
| Liveness / storage+runtime+group metrics / catalog / plugins / prometheus | `health`,`getStorageMetrics`,`getRuntimeMetrics`,`getStorageGroupMetrics`,`getCatalog`,`getPlugins`,`getMetricsText` | `GET /healthz`,`/storage/metrics`,`/runtime/metrics`,`/storage/metrics/groups`,`/metadata/catalog`,`/plugins`,`/metrics` |
| Cluster status + placement | `getCluster`,`getPlacement` | `GET /cluster`, `GET /cluster/placement` |
| S3 face status | `fetchS3Info` | `GET /s3/info` |
| DuckDB worker-plane: stats / events(SSE) / chaos / demo-load | `getWorkerStats`,`workerEventsUrl`,`postWorkerChaos`,`postWorkerDemoLoad` | `GET /duckdb-workers/stats`, `GET …/events`, `POST …/chaos`, `POST …/demo-load` |
| Fabric ops stream (SSE) | Activity.tsx `EventSource` | `GET /events` |

---

## B. Drift map

### B.1 Counts
- **USED by UI: 91 / 129 routes (71%).**
- **UNUSED by UI: 38 / 129 routes (29%).**
- **UI client dead code: 0** — every one of ~113 `arrowref.ts` exports is referenced by a view.

### B.2 The 38 UNUSED routes (dead-to-UI / legacy / server-only)
**Legacy execution path (frames/queries/results/transforms) — 12** — the original
"submit query → poll result" model. The UI only ever *reads* a result via
`refToPath` (`…/arrow`), never submits: `POST /queries`, `GET /queries/:id`,
`GET /queries/:id/wait`, `POST /transforms/sql`, `GET /results/:id/meta`,
`GET /results/:id/wait`, `POST /results/ack`, `PUT /frames/:id`,
`GET /frames/:id/meta`, `POST /scopes/:scope/transforms/ingest`,
`POST /datasets/:name/query`, `GET /datasets/:name/chunks/:chunk_id`.

**Legacy task/queue polling (superseded by per-id + batch) — 5** —
`POST /tasks/ack`, `POST /tasks/wait`, `POST /tasks/batch`,
`POST /task-signals/next`, `POST /events/next`, plus `GET /task-batches/:id`
(UI uses `…/wait` only).

**`artifact.parquet` materializers — 5** — `GET …/artifact.parquet` on datasets,
frames, results, scopes, variants. (Parquet export path; UI reads Arrow IPC.)

**Server↔server cluster — 7** — `/cluster/gossip`, `/cluster/inventory`,
`/cluster/inventory/datasets/:name`, `/cluster/leave`, `/cluster/pubsub/publish`,
`/cluster/pubsub/stream`, `/cluster/replica/datasets/:name` (inter-node, not a
browser surface).

**Scope catalog leftovers — 4** — `GET /scopes/:scope/item` (singular),
`/scopes/:scope/catalog/variants`, `/scopes/:scope/l3/list`,
`/scopes/:scope/variants/:id/artifact.parquet`.

**Misc — 5** — `GET /groups/:name/usage`, `GET /artifacts/:key/query`,
`PUT/DELETE /artifacts/:key/tags/:tag`, `POST /scratchpad/:name/checkpoint`,
`GET /readyz`.

> Not counted above but note: `tcp_control.rs` (2729 LOC) is a **whole RESP/TCP
> control lane** with **zero** UI routes — a large sprawl surface entirely outside
> the console's interface.

### B.3 Vocabulary divergence (UI term ≠ backend primitive)
| UI product term | Backend primitive | Mismatch |
|---|---|---|
| **Environment / Project** | *(none)* | Pure console construct (`infra.ts`, localStorage). Node sees a flat `env.project.name` string namespace only. |
| **Cache** (`CacheRef.scopeKey`) | **scope** + its **datasets** | One UI "cache" = one backend *scope*; its rows are a *dataset* (`scope/name`). A scope also exposes *items/variants/warm-queries* (a second, older catalog model) that the "cache" UI mostly hides. |
| **Durable message / Task lane** | **task** (`plugin: task.echo`) + `group="task_results"` | "Message" is a `POST /tasks`; "lane" is a console-only folder — no node concept. |
| **Queue** | **queue** (1:1) | but console `QueuePolicy` shape ≠ node `NodeQueuePolicy` (mapped by `toNodeQueuePolicy`). |
| **Object / Artifact** | one store, two faces (`/objects` opaque, `/artifacts` versioned) | UI presents object ↔ dataset ↔ artifact as one entity with three representations. |
| **Scratchpad / Scope "work"** | **DuckDB** connection + SQL exec | product framing hides that this is a live SQL engine + "Quack" card. |

### B.4 Backend module sprawl (LOC — where the implementation lives)
| Area | Modules | LOC |
|---|---|---|
| HTTP facade / browser translation | `http.rs` 6010, `browser.rs` 1652, `range_http.rs` 153, `request_timeout.rs`,`ratelimit.rs`,`validation.rs` | ~8.6k |
| Scope service (UI "caches") | `scope/` | 6616 |
| **RESP/TCP control lane (no UI)** | `tcp_control.rs` | 2729 |
| Storage / objects / arena | `storage.rs` 2321, `object_store.rs` 1404, `shared_arena.rs` 574, `snapshot_export.rs` 261, `multipart_upload.rs` 317 | ~4.9k |
| Task/queue fabric | `runtime.rs` 2552, `queue.rs` 1142, `task_journal.rs` 1045, `wal.rs` 642, `model.rs` 1206 | ~6.6k |
| Sources / CDC | `source.rs` 1328, `cdc_replica.rs` 1134, `replicas.rs` 565, `pg_cdc.rs` 257 | ~3.3k |
| Scratchpad + DuckDB worker plane | `scratchpad.rs` 1180, `duckdb_worker/` 2344, `duckdb_exec.rs` 424 | ~3.9k |
| Cluster | `cluster/` | 2762 |
| S3 face | `s3/` | 1147 |
| Query/frames legacy | `query.rs` 573, `formats.rs` 497, `plugins.rs` 471, `plugin_host/` 1339 | ~2.9k |
| Other | `ipc/` 665, `client.rs`,`ops.rs`,`l1.rs`,`main.rs`,`promote.rs`,`policy.rs`,`error.rs`,`logging.rs`,`pubsub.rs` 502 … | ~3k |
| **Total `src`** | | **~46,612** |

---

## C. Collapsed interface → shm-actors coverage / gap table

**What shm-actors *is*:** four same-host, zero-copy primitives + a coordinator —
**pub/sub** (SPMC ring), **tasks** (MPMC exactly-once claim + at-least-once
lease-reap), **streams** (transactional commit), **artifacts** (RCU versioned);
coordinator = UDS + `SCM_RIGHTS` fd-passing + leases + crash reclamation.
**What it is *not* (proven by deps — only `libc`,`nix`,`bytemuck`,`arrow`):** not
an HTTP server, no SQL engine, no SSE/WS, no S3, no cross-host, no HTTP object
semantics. It replaces the **same-host L2/task CORE**, nothing above it.

| UI capability (§A) | shm-actors primitive | Coverage | Gap / who must build it |
|---|---|---|---|
| **7. Tasks/batches** | `shm-task` claim + reap + `wait` | ✅ **proven** (ADR-0005: 24-byte ctrl, 1370× payload:ctrl, zero-copy both planes, no ABI change) | **G1** typed ref envelope; **G3** keyed result store; **G8** durable task WAL; **G12/G4** lifecycle-tied leases + evict-current |
| **2. Artifacts (versioned)** | `shm-artifact` RCU/MVCC | ✅ versioning proven | **G3** keyed multi-output; **G4** cannot evict *current* version; **G10** schema fixed at v1 |
| **3. Datasets — Arrow refs, chunk append** | retained pool chunks + `shm-arrow` zero-copy read; `shm-stream` transactional append | ✅ zero-copy proven | **G1** multi-chunk/dataset ref (single `ChunkDesc` today); **G5(R)** Append `O(prior chunks)` if accumulating |
| **6. Topics pub/sub** | `shm-ring` SPMC broadcast + cursors | ✅ **same-host** ring | ❌ **SSE + WebSocket** browser transport (no HTTP in substrate); resumable `from=`/`Last-Event-ID` bridge |
| **8. Queues (durable)** | `shm-task` lease/reap/DLQ-shaped | ⚠️ delivery shape maps | ❌ **durable WAL** (G8); visibility-timeout/DLQ/nack policy layer; `receive/peek` HTTP verbs |
| **1. Objects (opaque bytes)** | `shm-artifact`/chunks store bytes | ⚠️ storage only | ❌ **HTTP object semantics**: ETag/`If-None-Match`/304, byte-range, TTL, **multipart+deflate upload** — all HTTP, none in substrate |
| **5. Scratchpads / 4. scope "work"** | — | ❌ **none** | ❌ **DuckDB SQL engine + orchestration** (exec/query/attach/refresh/promote, Quack card) — shm-actors has *no SQL* |
| **9. Sources (L3 ingestion)** | — | ❌ none | ❌ connectors: file/http/webhook/OAuth2/s3/**Postgres CDC** |
| **10. Policies / 11. Groups** | coordinator leases (partial) | ⚠️ leases≈pins | ❌ memory/spill **budgets**, TTL, eviction, `clear_on_ack` groups |
| **12. Cluster** | *(same-host by design — G9)* | ➖ boundary | ❌ gossip/placement/replication stays in ArrowRef's layer |
| **12. S3 face** | — | ❌ none | ❌ SigV4/XML protocol (`s3/` 1147 LOC) |
| **A. HTTP framing itself / metrics / auth** | *(in-process Rust API — G11)* | ➖ boundary | ❌ the entire `/cache/*` HTTP server, bearer auth, Prometheus |

**ADR-0005's ranked task-fabric gaps** (the *narrowest* surface, already spiked):
**G1** typed ref envelope → **G3** keyed result store → **G12/G4** lifecycle-tied
leases + evict-current → **G8** durable task WAL; **R/S** (perf) deferred until a
measured workload. Its verdict for the task fabric: *viable, every gap is additive
above the four cores, no ABI change.* This document extends that ranked list with
the **whole-UI non-actor concerns** (rows shaded ❌ above): **SQL engine, SSE/WS,
HTTP object semantics, S3, cluster, source connectors** — none of which ADR-0005
scoped, because it deliberately mapped only the task fabric.

---

## D. Rebuild vs Integrate — evidence-based inputs (not a decision)

### D.1 How much of the 131-route backend is load-bearing for the UI?
- **91/129 routes (71%)** are live; **38 (29%)** are dead-to-UI. A ground-up
  rebuild sheds the dead 29% (legacy frames/queries/results/transforms +
  server↔server cluster + parquet materializers) **for free**, plus the entire
  RESP/TCP lane (`tcp_control.rs`, 2729 LOC) that has no UI surface at all.
- But by **LOC**, the live surface is dominated by layers *above* shm-actors'
  boundary: HTTP facade (~8.6k) + scope/SQL "work" (6.6k) + scratchpad/DuckDB
  worker plane (3.9k) + objects/uploads HTTP semantics (~4.9k) + sources/CDC
  (3.3k) + S3 (1.1k) + cluster (2.8k). The **same-host L2/task core** shm-actors
  would replace (`runtime.rs`+`storage`/arena+task journal/queue) is on the order
  of **~25–35% of the load-bearing code**.

### D.2 A rebuild on shm-actors gets *for free* vs must *re-implement*
**For free (proven in ADR-0005 spike):** descriptor-only task queue (exactly-once
claim + at-least-once reap + deadline/cancel), zero-copy Arrow retained refs, RCU
versioned artifacts, SPMC pub/sub ring, transactional stream commit, coordinator
leases + per-actor borrow journal + crash reclamation. These are *exactly* the
descriptor-first invariant the current backend hand-rolls in `runtime.rs` (2552) /
`storage.rs` (2321) / `shared_arena.rs` (574) / `task_journal.rs` (1045).

**Must add above the substrate — the non-actor concerns (this is the bulk of the
UI surface):**
1. **DuckDB SQL engine + orchestration** — scratchpads, scope "work" query/exec,
   dataset query, promote. shm-actors has *no SQL*. (~4–6k LOC of orchestration,
   plus DuckDB itself.) **Biggest single gap.**
2. **HTTP server + framing + bearer auth + Prometheus** — the whole `/cache/*`
   surface (`http.rs` 6010 + `browser.rs` 1652). shm-actors is in-process only.
3. **SSE + WebSocket** transport for topics/events/worker-stream with cursor
   resume. Substrate has a ring, not a browser transport.
4. **HTTP object semantics** — ETag/304/byte-range/TTL/multipart+deflate upload.
5. **S3 face** (SigV4/XML), **cluster** (gossip/placement/replication — G9
   same-host boundary), **L3 source connectors** incl. Postgres CDC.
6. From ADR-0005: **typed ref envelope (G1)**, **keyed result store (G3)**,
   **lifecycle leases + evict-current (G12/G4)**, **durable task WAL (G8)**.

### D.3 The single biggest argument each way
- **FOR a ground-up rebuild:** the *core invariant* the whole product is built on
  — payload retained once, moved only as a 24-byte descriptor, read zero-copy — is
  **precisely shm-actors' proven wheelhouse**, and ADR-0005 demonstrated the task
  mapping end-to-end with **no ABI change to the four lock-free cores** (every gap
  additive). The current backend carries that invariant in ~6.6k LOC of
  hand-rolled runtime/journal/arena code that shm-actors implements more rigorously
  (RCU pins, borrow journal, coordinator crash-reclamation). A rebuild also sheds
  ~29% dead routes + the 2.7k-LOC RESP lane for free.
- **AGAINST a ground-up rebuild:** shm-actors replaces **0%** of what dominates the
  UI's *actual* required surface — a **DuckDB SQL engine**, an **HTTP/SSE/WS
  server**, **HTTP object semantics**, **S3**, **cluster**, and **source/CDC
  ingestion**. Most live routes (all scratchpad + scope-work + dataset-query +
  promote = SQL; all `/objects`+`/uploads`+`/artifacts` = HTTP object semantics;
  topics = SSE/WS; sources; s3; cluster) sit **above** the substrate boundary. So
  "rebuild on shm-actors" is really "rebuild the ~30% same-host core **and
  re-implement the other ~70% from scratch**" — exactly the layers the *integrate*
  path keeps intact.

### D.4 Biggest risk each way
- **Rebuild risk:** re-implementing the SQL/HTTP/SSE/S3/cluster/CDC layers (the
  majority of the UI surface) is a multi-quarter effort with regression exposure in
  code that *already works and the UI depends on today*; the substrate wins buy a
  minority of the surface.
- **Integrate risk:** swapping only the same-host L2/task core under a live 46.6k-
  LOC backend means a two-substrate seam (ArrowRef primitives ↔ shm-actors chunks)
  and carrying the 4 ADR-0005 gaps (G1/G3/G8/G12) as *new* glue — while the sprawl
  (dead routes, RESP lane, dual scope catalogs) is left standing unless separately
  pruned.

---

*Read-only guarantee: no file under `~/projects/infrastructure/query-cache` was
modified in producing this document.*
