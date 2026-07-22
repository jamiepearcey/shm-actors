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
//! ```
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
use std::time::Duration;

use shm_arrow::SchemaRegistry;
use shm_core::ChunkDesc;
use shm_runtime::demo::{
    demo_batch, demo_derive, demo_schema, nested_batch, nested_schema, CACHE_ARTIFACT,
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
        other => {
            eprintln!(
                "unknown role {other:?}; expected coordinator|producer|producer-hang|\
                 watcher|worker|worker-hang|worker-pin-hang|writer-hang"
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
}

impl Opts {
    fn parse(args: &[String]) -> Opts {
        let mut uds = String::new();
        let mut result = None;
        let mut seg_base = 1u32;
        let mut lease_ms = 500u64;
        let mut nested = false;
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
                _ => i += 1,
            }
        }
        Opts {
            uds,
            result,
            seg_base,
            lease_ms,
            nested,
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
    let mut writer = match stream.writer(Commit::Replace, Coordination::Optimistic { expect_version: 0 }) {
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
    write_result(opts, &format!("SUBMIT {} {}\n", handle.slot_idx, handle.seq));
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
            write_result(opts, &format!("DONE {} {} {} {}\n", id.slot_idx, id.seq, sum, rows));
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

/// Leak a derive-error string into a `'static str` for the `Protocol` variant
/// (only ever hit on a malformed batch, which the demo never produces).
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
