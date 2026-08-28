//! The v0.2 walking-skeleton demo binary — "the cache loop" (ADR-0002 §3).
//!
//! Roles:
//!
//! ```text
//! shm-cacheloop coordinator   --uds <path> [--seg-base <n>]
//! shm-cacheloop producer      --uds <path> --result <file>
//! shm-cacheloop producer-hang --uds <path> --result <file>   # stages, never commits
//! shm-cacheloop watcher       --uds <path> --result <file> [--lease-ms <n>]
//! shm-cacheloop worker        --uds <path> --result <file> [--lease-ms <n>]
//! shm-cacheloop worker-hang   --uds <path> --result <file> [--lease-ms <n>]  # claims, hangs
//! shm-cacheloop worker-pin-hang --uds <path> --result <file>  # journal-pins a version, hangs
//! shm-cacheloop writer-hang   --uds <path> --result <file>   # takes the exclusive lease, hangs
//! shm-cacheloop kill-at       --uds <path> --result <file> --kill-at <point> [--art <name>]
//! shm-cacheloop churn         --uds <path> --seed <n> [--art <name>]
//! ```
//!
//! The v0.4 stage O roles (`tests/crash_matrix.rs`, `tests/churn_soak.rs`):
//!
//! - `kill-at` drives one primitive to a precise, reclaimable shared state
//!   (`--kill-at` = `stage-1`|`stage-2`|`lease-only`|`lease-stage`|`art-pin`|
//!   `payload-pin`|`task-claim`|`task-submit`; also `SHM_KILL_AT`), writes a
//!   `READY` marker, then `std::process::abort()`s — dying with the resource held
//!   and no destructor run, so only the coordinator's journal replay can reclaim
//!   it.
//! - `churn` runs a deterministic (`--seed`) loop of reclaimable operations until
//!   the driver `kill -9`s it, for the churn-soak zero-leak census.
//!
//! The `--nested` flag (ADR-0003 S5 "hostile cache loop") routes the
//! `producer`, `worker-pin-hang`, and `writer-hang` roles at the **nested
//! Struct/List multi-chunk** artifact ([`NESTED_ARTIFACT`]) under its
//! coordinator-negotiated schema, instead of the flat demo one — so the S5
//! integration test can chain a real nested producer, a mid-pin crash, and a
//! mid-write crash on one multi-chunk artifact and take a zero-leak census.
//!
//! The integration test runs the coordinator **in-process** (so it can inspect
//! artifact segments + the task queue) and spawns the other roles as separate OS
//! processes. The cache loop:
//!
//! 1. `producer` stages the demo batch into a **stream** and `commit()`s →
//!    installs artifact version 1 → a `VersionEvent` is published on
//!    `__artifacts`.
//! 2. `watcher` (parked on the `__artifacts` doorbell) wakes and enqueues a
//!    **task** describing "recompute for version N".
//! 3. `worker` CAS-claims the task, pins version N zero-copy, derives a result,
//!    and marks the task `DONE`.
//!
//! `worker-hang` / `producer-hang` drive the two crash scenarios.

use std::sync::Arc;
use std::time::{Duration, Instant};

use shm_arrow::SchemaRegistry;
use shm_core::ChunkDesc;
use shm_ring::Msg;
use shm_runtime::demo::{
    demo_batch, demo_derive, demo_schema, nested_batch, nested_schema, CACHE_ARTIFACT, DEMO_TOPIC,
    NESTED_ARTIFACT,
};
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_stream::{Commit, Coordination};
use shm_task::{now_nanos, Outcome};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let role = args.get(1).map(String::as_str).unwrap_or("");
    let opts = Opts::parse(&args);

    let code = match role {
        "coordinator" => run_coordinator(&opts),
        "producer" => run_producer(&opts, /*commit=*/ true),
        "producer-hang" => run_producer(&opts, /*commit=*/ false),
        "watcher" => run_watcher(&opts),
        "worker" => run_worker(&opts, /*hang=*/ false),
        "worker-hang" => run_worker(&opts, /*hang=*/ true),
        "worker-pin-hang" => run_pin_hang(&opts),
        "writer-hang" => run_writer_hang(&opts),
        // v0.4 stage O: self-aborting fault injection at a specific transition.
        "kill-at" => run_kill_at(&opts),
        // v0.4 stage O: seeded churn-soak worker (registers/publishes/pins/
        // commits/submits in a deterministic loop until the driver kills it).
        "churn" => run_churn(&opts),
        other => {
            eprintln!(
                "unknown role {other:?}; expected coordinator|producer|producer-hang|\
                 watcher|worker|worker-hang|worker-pin-hang|writer-hang|kill-at|churn"
            );
            2
        }
    };
    std::process::exit(code);
}

/// Parsed command-line options.
struct Opts {
    uds: String,
    result: Option<String>,
    seg_base: u32,
    lease_ms: u64,
    /// S5: operate on the nested Struct/List multi-chunk artifact under its
    /// coordinator-negotiated schema, rather than the flat demo one.
    nested: bool,
    /// v0.4/O: the fault-injection point the `kill-at` role drives to before
    /// `std::process::abort()`ing (also readable from `SHM_KILL_AT`).
    kill_at: Option<String>,
    /// v0.4/O: the artifact name a `kill-at`/`churn` actor operates on (defaults
    /// to the flat cache artifact; lets each matrix point use an isolated name).
    art: Option<String>,
    /// v0.4/O: deterministic PRNG seed for the `churn` role.
    seed: u64,
}

impl Opts {
    fn parse(args: &[String]) -> Opts {
        let mut uds = String::new();
        let mut result = None;
        let mut seg_base = 1u32;
        let mut lease_ms = 500u64;
        let mut nested = false;
        let mut kill_at = None;
        let mut art = None;
        let mut seed = 0u64;
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str() {
                "--uds" => {
                    uds = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--result" => {
                    result = args.get(i + 1).cloned();
                    i += 2;
                }
                "--seg-base" => {
                    seg_base = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(1);
                    i += 2;
                }
                "--lease-ms" => {
                    lease_ms = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(500);
                    i += 2;
                }
                "--nested" => {
                    nested = true;
                    i += 1;
                }
                "--kill-at" => {
                    kill_at = args.get(i + 1).cloned();
                    i += 2;
                }
                "--art" => {
                    art = args.get(i + 1).cloned();
                    i += 2;
                }
                "--seed" => {
                    seed = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(0);
                    i += 2;
                }
                _ => i += 1,
            }
        }
        Opts {
            uds,
            result,
            seg_base,
            lease_ms,
            nested,
            kill_at,
            art,
            seed,
        }
    }
}

/// The artifact name this actor operates on: the nested Struct/List multi-chunk
/// artifact when `--nested` (S5), else the flat cache-loop artifact.
fn art_name(opts: &Opts) -> &'static str {
    if opts.nested {
        NESTED_ARTIFACT
    } else {
        CACHE_ARTIFACT
    }
}

/// The Arrow schema this actor negotiates + writes: nested Struct/List (S5) or
/// the flat demo schema.
fn art_schema(opts: &Opts) -> arrow_schema::SchemaRef {
    if opts.nested {
        nested_schema()
    } else {
        demo_schema()
    }
}

/// The batch this actor stages: the nested multi-chunk batch (S5) or the flat
/// demo batch.
fn art_batch(opts: &Opts) -> arrow_array::RecordBatch {
    if opts.nested {
        nested_batch()
    } else {
        demo_batch()
    }
}

/// An **empty** local schema cache. Since ADR-0003 item E, actors no longer seed
/// identical registries: the coordinator issues `schema_id`s, and each actor
/// fills its cache via `Node::intern_schema` (producers) / `Node::resolve_schema`
/// (consumers). The cache loop proving out E therefore starts every actor with a
/// blank registry and lets the coordinator be the single source of truth.
fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::new())
}

/// Write the role's result marker file (atomically enough for the test).
fn write_result(opts: &Opts, msg: &str) {
    if let Some(path) = &opts.result {
        let _ = std::fs::write(path, msg);
    }
}

/// Encode a derived `(sum, rows)` result into the task-result descriptor's
/// scalar fields, matching the test's decoder.
fn result_desc(sum: i64, rows: i64) -> ChunkDesc {
    ChunkDesc {
        offset: sum as u32,
        len: rows as u32,
        ..ChunkDesc::ZERO
    }
}

fn run_coordinator(opts: &Opts) -> i32 {
    let config = RuntimeConfig::with_seg_base(opts.seg_base);
    let mut coord = match Coordinator::bind(&opts.uds, config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("coordinator bind failed: {e}");
            return 1;
        }
    };
    if let Err(e) = coord.start() {
        eprintln!("coordinator start failed: {e}");
        return 1;
    }
    println!("coordinator ready on {}", opts.uds);
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Producer: stage the demo batch into a stream. If `commit`, install version 1
/// (publishing a `VersionEvent`); otherwise stay mid-stage forever (scenario b).
fn run_producer(opts: &Opts, commit: bool) -> i32 {
    let mut node = match Node::connect(&opts.uds, "producer", registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("producer connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));
    let art = art_name(opts);
    if let Err(e) = node.open_artifact(art) {
        eprintln!("producer open_artifact failed: {e}");
        return 1;
    }
    // ADR-0003 item E: negotiate the schema id with the coordinator BEFORE
    // writing, so the id stamped into the batch chunk + version manifest is the
    // coordinator's globally-agreed id (this process was seeded with an EMPTY
    // registry). The subsequent `write_batch` interns the same schema into this
    // node's cache and transparently picks up that id. With `--nested` this is a
    // Struct/List schema whose batch spans several chunks (S5, item F).
    if let Err(e) = node.intern_schema(&art_schema(opts)) {
        eprintln!("producer intern_schema failed: {e}");
        return 1;
    }

    let stream = match node.stream(art) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("producer stream failed: {e}");
            return 1;
        }
    };
    // Replace-optimistic over the empty artifact (expect current == 0).
    let mut writer = match stream.writer(
        Commit::Replace,
        Coordination::Optimistic { expect_version: 0 },
    ) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("producer writer failed: {e}");
            return 1;
        }
    };
    if let Err(e) = writer.append_batch(&art_batch(opts)) {
        eprintln!("producer append failed: {e}");
        return 1;
    }

    if !commit {
        // Scenario (b): staged but never committed. Hold the writer alive (no
        // Drop, no commit) and idle until `kill -9`, so the coordinator's lease
        // monitor replays our journal and frees the staged chunks.
        write_result(opts, "STAGED");
        println!("producer-hang staged (no commit)");
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    match writer.commit() {
        Ok(v) => {
            write_result(opts, &format!("OK {v}"));
            println!("producer committed version {v}");
        }
        Err(e) => {
            write_result(opts, &format!("ERR commit: {e}"));
            return 1;
        }
    }

    // Stay alive (heartbeating) so the coordinator does not reclaim us.
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Watcher: park on `__artifacts`, and on the commit event enqueue a task, then
/// wait for its terminal outcome.
fn run_watcher(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "watcher", registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("watcher connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));

    let mut watcher = match node.watch_artifacts() {
        Ok(w) => w,
        Err(e) => {
            eprintln!("watcher subscribe failed: {e}");
            return 1;
        }
    };
    // Block (parked on the doorbell) until the producer's commit event.
    let event = watcher.recv();
    println!("watcher woke on version {}", event.version);

    let queue = match node.task_queue() {
        Ok(q) => q,
        Err(e) => {
            eprintln!("watcher task_queue failed: {e}");
            return 1;
        }
    };
    // The request carries the version to recompute (the event, re-encoded).
    let deadline = now_nanos() + Duration::from_millis(opts.lease_ms).as_nanos() as u64;
    let handle = match queue.submit(event.to_desc(), deadline) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("watcher submit failed: {e}");
            return 1;
        }
    };
    write_result(
        opts,
        &format!("SUBMIT {} {}\n", handle.slot_idx, handle.seq),
    );
    println!("watcher submitted task {handle:?}");

    match queue.wait(handle) {
        Ok(Outcome::Done(r)) => {
            write_result(
                opts,
                &format!(
                    "SUBMIT {} {}\nOUTCOME DONE {} {} {} {}\n",
                    handle.slot_idx, handle.seq, handle.slot_idx, handle.seq, r.offset, r.len
                ),
            );
            println!("watcher observed DONE: sum={} rows={}", r.offset, r.len);
        }
        Ok(other) => {
            write_result(opts, &format!("OUTCOME {other:?}\n"));
            eprintln!("watcher observed non-Done outcome: {other:?}");
            return 1;
        }
        Err(e) => {
            eprintln!("watcher wait failed: {e}");
            return 1;
        }
    }

    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Worker: claim a task, and (unless `hang`) pin the artifact version zero-copy,
/// derive the result, and complete the task.
fn run_worker(opts: &Opts, hang: bool) -> i32 {
    let name = if hang { "worker-hang" } else { "worker" };
    let mut node = match Node::connect(&opts.uds, name, registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("{name} connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));
    if !hang {
        if let Err(e) = node.open_artifact(CACHE_ARTIFACT) {
            eprintln!("{name} open_artifact failed: {e}");
            return 1;
        }
    }

    let lease = Duration::from_millis(opts.lease_ms).as_nanos() as u64;
    let queue = match node.task_queue() {
        Ok(q) => q,
        Err(e) => {
            eprintln!("{name} task_queue failed: {e}");
            return 1;
        }
    };

    let task = match queue.claim_blocking(lease) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{name} claim failed: {e}");
            return 1;
        }
    };
    let id = task.task_id();

    if hang {
        // Scenario (a): claimed, then hang forever (never complete). `kill -9`
        // from the test → the claim's lease lapses → the coordinator's reap
        // requeues it (at-least-once) for a second worker.
        write_result(opts, &format!("CLAIMED {} {}\n", id.slot_idx, id.seq));
        println!("worker-hang claimed {id:?}; hanging");
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    // Pin the current artifact version zero-copy and derive the result.
    let (sum, rows) = match compute(&node) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("worker compute failed: {e}");
            let _ = task.fail();
            return 1;
        }
    };
    match task.complete(result_desc(sum, rows)) {
        Ok(()) => {
            write_result(
                opts,
                &format!("DONE {} {} {} {}\n", id.slot_idx, id.seq, sum, rows),
            );
            println!("worker completed {id:?}: sum={sum} rows={rows}");
        }
        Err(e) => {
            // Lost the claim (reaped): another attempt owns the task.
            write_result(opts, &format!("LOST {} {}\n", id.slot_idx, id.seq));
            eprintln!("worker complete rejected (Lost): {e}");
            return 1;
        }
    }

    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Pin the artifact's current version and derive `(sum(id), rows)` zero-copy.
/// Takes a **journalled** pin (item J) so a crash mid-compute is crash-reclaimed.
fn compute(node: &Node) -> shm_runtime::Result<(i64, i64)> {
    let pin = node.pin_artifact(CACHE_ARTIFACT)?;
    // ADR-0003 item E: learn the schema from the coordinator using the id read
    // from the pinned version's manifest — this process was seeded with an EMPTY
    // registry, so agreement rests entirely on the coordinator, not on identical
    // seeding. `resolve_schema` caches it so `as_arrow`'s read path resolves it.
    node.resolve_schema(pin.manifest().schema_id)?;
    let batch = pin.as_arrow(node.registry())?;
    demo_derive(&batch).map_err(|m| shm_runtime::Error::Protocol(leak(m)))
}

/// `worker-pin-hang` (item J): open the artifact, take a **journalled** pin on
/// the current version, then hang forever holding it. A `kill -9` from the test
/// leaves the pin's `ArtifactPin` journal entry (and its +1 version pin count)
/// leaked, exactly as a crashed reader would; the coordinator's lease-monitor
/// replay then decrements the pin table and retires the version.
fn run_pin_hang(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "worker-pin-hang", registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("worker-pin-hang connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));
    let art = art_name(opts);
    if let Err(e) = node.open_artifact(art) {
        eprintln!("worker-pin-hang open_artifact failed: {e}");
        return 1;
    }

    // Wait until a version exists, then journal-pin it and hold it forever.
    loop {
        match node.pin_artifact(art) {
            Ok(pin) => {
                write_result(opts, &format!("PINNED {}\n", pin.version()));
                println!("worker-pin-hang pinned version {}", pin.version());
                // Hold the pin (never dropped) until kill -9.
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// `writer-hang` (item K): open the artifact, take the **exclusive** write lease
/// (opened journalled, so it lands in this actor's borrow journal), stage one
/// batch under it, then hang forever holding the lease. A `kill -9` from the test
/// leaves the lease held (and its `WriteLease` journal entry leaked), exactly as a
/// crashed exclusive writer would; the coordinator's lease-monitor replay then
/// force-releases + fences the lease so a second writer can take over.
fn run_writer_hang(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "writer-hang", registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("writer-hang connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));
    let art = art_name(opts);
    if let Err(e) = node.open_artifact(art) {
        eprintln!("writer-hang open_artifact failed: {e}");
        return 1;
    }
    // Negotiate the schema id before staging (empty local registry; item E).
    // With `--nested` this stages a Struct/List multi-chunk batch (S5, item F).
    if let Err(e) = node.intern_schema(&art_schema(opts)) {
        eprintln!("writer-hang intern_schema failed: {e}");
        return 1;
    }

    let stream = match node.stream(art) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("writer-hang stream failed: {e}");
            return 1;
        }
    };
    let mut writer = match stream.writer(Commit::Replace, Coordination::Exclusive) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("writer-hang writer (exclusive open) failed: {e}");
            return 1;
        }
    };
    if let Err(e) = writer.append_batch(&art_batch(opts)) {
        eprintln!("writer-hang append failed: {e}");
        return 1;
    }

    // Lease acquired + one chunk staged; hold both forever (never commit/drop)
    // until `kill -9`.
    write_result(opts, "LEASED");
    println!("writer-hang holds the exclusive lease (no commit)");
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The churn-soak artifact name (v0.4 stage O §4).
pub const CHURN_ARTIFACT: &str = "churn";

/// v0.4 stage O §1 — **kill at a specific state transition**.
///
/// The actor connects, drives one primitive to a precise, *reclaimable* shared
/// state (a staged stream chunk, a held write lease, a held artifact/chunk pin, a
/// claimed/submitted task), writes a `READY` marker so the driving test knows the
/// state was reached, and then `std::process::abort()`s — dying with the resource
/// still held and **no destructor run**, exactly as a `kill -9` would. Because
/// `abort()` never unwinds, every in-scope guard (the `StreamWriter`, `Committer`,
/// `VersionPin`, `Pin`, `ClaimedTask`) leaks its shm state rather than releasing
/// it, so the coordinator's lease-monitor journal replay is the only thing that
/// can reclaim it — which is the whole point.
///
/// The point is selected by `--kill-at <point>` or the `SHM_KILL_AT` env var; the
/// artifact it operates on is `--art <name>` (default the flat cache artifact).
/// See `tests/crash_matrix.rs` for the transition ⇄ census assertion mapping.
fn run_kill_at(opts: &Opts) -> i32 {
    let point = match opts
        .kill_at
        .clone()
        .or_else(|| std::env::var("SHM_KILL_AT").ok())
    {
        Some(p) => p,
        None => {
            eprintln!("kill-at: no injection point (pass --kill-at <point> or SHM_KILL_AT)");
            return 2;
        }
    };
    let art = opts.art.as_deref().unwrap_or(CACHE_ARTIFACT);
    let name = format!("kill-at-{point}");
    // The v0.1 payload broadcast path (`payload-pin`) has no coordinator schema
    // resolve, so its consumer seeds the demo schema identically (the v0.1
    // contract); every other point negotiates the id through the coordinator
    // (item E) and so starts from an empty local cache.
    let reg = if point == "payload-pin" {
        Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
    } else {
        registry()
    };
    let mut node = match Node::connect(&opts.uds, &name, reg) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("{name} connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));

    // Points that stage/commit an artifact need its schema negotiated first.
    let ready = |extra: &str| write_result(opts, &format!("READY {point}{extra}\n"));

    match point.as_str() {
        // --- Stream staging (artifact data pool): staged chunk(s) LOANED +
        //     journalled, never committed. Journal replay frees them. ---
        "stage-1" | "stage-2" => {
            if let Err(e) = open_and_intern(&mut node, art, opts) {
                eprintln!("{name} setup failed: {e}");
                return 1;
            }
            let expect = node.artifact(art).map(|a| a.current_version()).unwrap_or(0);
            let stream = node.stream(art).expect("stream");
            let mut writer = stream
                .writer(
                    Commit::Replace,
                    Coordination::Optimistic {
                        expect_version: expect,
                    },
                )
                .expect("optimistic writer");
            writer.append_batch(&art_batch(opts)).expect("append 1");
            if point == "stage-2" {
                writer.append_batch(&art_batch(opts)).expect("append 2");
            }
            ready(&format!(" staged={}", writer.staged_len()));
            // `writer` is still in scope → abort() leaks the staged loans.
            std::process::abort();
        }

        // --- Exclusive write lease held, NOTHING staged (item K, lease only). ---
        "lease-only" => {
            if let Err(e) = open_and_intern(&mut node, art, opts) {
                eprintln!("{name} setup failed: {e}");
                return 1;
            }
            let stream = node.stream(art).expect("stream");
            let _writer = stream
                .writer(Commit::Replace, Coordination::Exclusive)
                .expect("exclusive writer (lease held)");
            ready("");
            std::process::abort();
        }

        // --- Exclusive write lease held AND a batch staged (item K + stream). ---
        "lease-stage" => {
            if let Err(e) = open_and_intern(&mut node, art, opts) {
                eprintln!("{name} setup failed: {e}");
                return 1;
            }
            let stream = node.stream(art).expect("stream");
            let mut writer = stream
                .writer(Commit::Replace, Coordination::Exclusive)
                .expect("exclusive writer");
            writer.append_batch(&art_batch(opts)).expect("append");
            ready("");
            std::process::abort();
        }

        // --- Artifact version pin held on a (soon-superseded) version (item J).
        //     Waits until the driver supersedes the pinned version so the crash
        //     reclaim genuinely retires a NON-current pinned version. ---
        "art-pin" => {
            if let Err(e) = node.open_artifact(art) {
                eprintln!("{name} open_artifact failed: {e}");
                return 1;
            }
            let pin = loop {
                match node.pin_artifact(art) {
                    Ok(p) => break p,
                    Err(_) => std::thread::sleep(Duration::from_millis(15)),
                }
            };
            let pinned = pin.version();
            ready(&format!(" v={pinned}"));
            // Hold the pin (heartbeating) until the driver commits a newer version,
            // so at abort time the pinned version is non-current → its reclaim
            // must retire it. Bounded so a stuck driver never hangs the actor.
            let start = Instant::now();
            while node
                .artifact(art)
                .map(|a| a.current_version())
                .unwrap_or(pinned)
                <= pinned
            {
                if start.elapsed() > Duration::from_secs(30) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(15));
            }
            // `pin` is still in scope → abort() leaks its ArtifactPin journal entry.
            let _ = &pin;
            std::process::abort();
        }

        // --- Payload shared pin held (v0.1 path). Subscribe, receive the driver's
        //     published batch, journal-pin it zero-copy, then abort holding it. ---
        "payload-pin" => {
            let mut sub = match node.subscribe(DEMO_TOPIC) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("{name} subscribe failed: {e}");
                    return 1;
                }
            };
            let desc = loop {
                match sub.recv() {
                    Msg::Sample(d) => break d,
                    Msg::Lagged(_) => continue,
                }
            };
            let pin = match node.pin_and_read(&desc) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("{name} pin_and_read failed: {e}");
                    return 1;
                }
            };
            ready(&format!(" off={}", pin.desc.offset));
            // `pin` in scope → abort() leaks the journalled shared pin.
            let _ = &pin;
            std::process::abort();
        }

        // --- Task claimed, never completed (worker death). Reap requeues it. ---
        "task-claim" => {
            let queue = match node.task_queue() {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("{name} task_queue failed: {e}");
                    return 1;
                }
            };
            let lease = Duration::from_millis(opts.lease_ms).as_nanos() as u64;
            let task = match queue.claim_blocking(lease) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("{name} claim failed: {e}");
                    return 1;
                }
            };
            let id = task.task_id();
            ready(&format!(" {} {}", id.slot_idx, id.seq));
            let _ = &task;
            std::process::abort();
        }

        // --- Task submitted, submitter dies before any worker claims. The task
        //     must stay claimable (a dead submitter wedges nothing). ---
        "task-submit" => {
            let queue = match node.task_queue() {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("{name} task_queue failed: {e}");
                    return 1;
                }
            };
            let request = ChunkDesc {
                schema_id: 7,
                ..ChunkDesc::ZERO
            };
            let deadline = now_nanos() + Duration::from_secs(3600).as_nanos() as u64;
            let handle = match queue.submit(request, deadline) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("{name} submit failed: {e}");
                    return 1;
                }
            };
            ready(&format!(" {} {}", handle.slot_idx, handle.seq));
            std::process::abort();
        }

        other => {
            eprintln!("kill-at: unknown point {other:?}");
            2
        }
    }
}

/// Open `art` and negotiate its schema id with the coordinator (empty local
/// registry, item E) — the setup a staging/lease injection point needs.
fn open_and_intern(node: &mut Node, art: &str, opts: &Opts) -> shm_runtime::Result<()> {
    node.open_artifact(art)?;
    node.intern_schema(&art_schema(opts))?;
    Ok(())
}

/// A tiny deterministic xorshift64* PRNG so the churn soak is reproducible from a
/// fixed seed (no external `rand` dependency in this substrate).
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Rng {
        // Avoid the zero fixed-point; still fully seed-determined.
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15 | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

/// v0.4 stage O §4 — a **churn-soak worker**.
///
/// Registers against the coordinator and runs a deterministic (seeded) loop of
/// reclaimable operations against the shared churn artifact + task queue: commit a
/// version (optimistic or exclusive), take + drop a journalled version pin, submit
/// a task, claim + (maybe) complete a task. Every hold is journalled, so whenever
/// the driver `kill -9`s this worker mid-operation the coordinator's replay
/// reclaims it. Runs until killed; the driver's periodic census asserts the pool
/// never trends downward and returns to baseline at quiescence.
fn run_churn(opts: &Opts) -> i32 {
    let art = opts.art.as_deref().unwrap_or(CHURN_ARTIFACT);
    let name = format!("churn-{}", opts.seed);
    let mut node = match Node::connect(&opts.uds, &name, registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("{name} connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));
    if let Err(e) = node.open_artifact(art) {
        eprintln!("{name} open_artifact failed: {e}");
        return 1;
    }
    if let Err(e) = node.intern_schema(&art_schema(opts)) {
        eprintln!("{name} intern_schema failed: {e}");
        return 1;
    }
    if let Err(e) = node.open_task_queue() {
        eprintln!("{name} open_task_queue failed: {e}");
        return 1;
    }

    let mut rng = Rng::new(opts.seed);
    write_result(opts, "CHURNING\n");
    loop {
        let op = rng.below(6);
        match op {
            0 | 1 => {
                // Optimistic commit over the current version — Replace (a new
                // chain root) or Append (links the prior manifest, ADR-0013) —
                // so the kill -9 census covers manifest chains and their
                // cascade. Concurrent committers race; the loser rolls back
                // cleanly (freeing staged, releasing its link).
                let kind = if op == 0 {
                    Commit::Replace
                } else {
                    Commit::Append
                };
                let expect = node.artifact(art).map(|a| a.current_version()).unwrap_or(0);
                if let Ok(stream) = node.stream(art) {
                    if let Ok(mut w) = stream.writer(
                        kind,
                        Coordination::Optimistic {
                            expect_version: expect,
                        },
                    ) {
                        if w.append_batch(&art_batch(opts)).is_ok() {
                            let _ = w.commit();
                        }
                    }
                }
            }
            2 => {
                // Exclusive commit; most callers see WriteLocked and back off.
                if let Ok(stream) = node.stream(art) {
                    if let Ok(mut w) = stream.writer(Commit::Replace, Coordination::Exclusive) {
                        if w.append_batch(&art_batch(opts)).is_ok() {
                            let _ = w.commit();
                        }
                    }
                }
            }
            3 => {
                // Take a journalled pin and hold it briefly, then drop it (a clean
                // retire path). A crash while holding it is reclaimed by replay.
                if let Ok(pin) = node.pin_artifact(art) {
                    std::thread::sleep(Duration::from_millis(rng.below(8)));
                    drop(pin);
                }
            }
            4 => {
                // Submit a short-lived task (reaped if unclaimed by its deadline).
                if let Ok(queue) = node.task_queue() {
                    let deadline =
                        now_nanos() + Duration::from_millis(200 + rng.below(200)).as_nanos() as u64;
                    let _ = queue.submit(
                        ChunkDesc {
                            schema_id: 1,
                            ..ChunkDesc::ZERO
                        },
                        deadline,
                    );
                }
            }
            _ => {
                // Claim a task if one is queued; complete it half the time (the
                // rest are dropped, exercising the lease-driven reap/requeue).
                if let Ok(queue) = node.task_queue() {
                    if let Some(task) = queue.claim(Duration::from_millis(300).as_nanos() as u64) {
                        if rng.below(2) == 0 {
                            let _ = task.complete(ChunkDesc::ZERO);
                        }
                    }
                }
            }
        }
        // Small deterministic jitter so workers interleave rather than lockstep.
        std::thread::sleep(Duration::from_millis(rng.below(6)));
    }
}

/// Leak a derive-error string into a `'static str` for the `Protocol` variant
/// (only ever hit on a malformed batch, which the demo never produces).
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
