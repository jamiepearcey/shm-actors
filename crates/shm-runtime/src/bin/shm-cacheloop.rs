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
//! ```
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
use shm_runtime::demo::{demo_batch, demo_derive, demo_schema, CACHE_ARTIFACT};
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
        other => {
            eprintln!(
                "unknown role {other:?}; expected \
                 coordinator|producer|producer-hang|watcher|worker|worker-hang"
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
}

impl Opts {
    fn parse(args: &[String]) -> Opts {
        let mut uds = String::new();
        let mut result = None;
        let mut seg_base = 1u32;
        let mut lease_ms = 500u64;
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
                _ => i += 1,
            }
        }
        Opts {
            uds,
            result,
            seg_base,
            lease_ms,
        }
    }
}

/// Seed a registry identically to every other actor (the v0.1 schema contract).
fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
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
    if let Err(e) = node.open_artifact(CACHE_ARTIFACT) {
        eprintln!("producer open_artifact failed: {e}");
        return 1;
    }

    let stream = match node.stream(CACHE_ARTIFACT) {
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
    if let Err(e) = writer.append_batch(&demo_batch()) {
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
fn compute(node: &Node) -> shm_runtime::Result<(i64, i64)> {
    let art = node.artifact(CACHE_ARTIFACT)?;
    let pin = art.pin()?;
    let batch = pin.as_arrow(node.registry())?;
    demo_derive(&batch).map_err(|m| shm_runtime::Error::Protocol(leak(m)))
}

/// Leak a derive-error string into a `'static str` for the `Protocol` variant
/// (only ever hit on a malformed batch, which the demo never produces).
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
