//! The ADR-0007 **G1** typed-ref dispatch/resolve walking-skeleton binary
//! (mirrors `shm-store-skeleton` and `shm-cacheloop`).
//!
//! Roles (the coordinator + the **front** run in the integration-test process so
//! it can dispatch the typed task and inspect the store; these children are
//! separate OS processes so a `kill -9` mid-pin is real):
//!
//! ```text
//! shm-g1-skeleton producer --uds <path> --dataset <k> [--result <file>]
//! shm-g1-skeleton worker   --uds <path> --dataset <k> --result-key <k>
//!                          [--crash-first] [--result <file>]
//! ```
//!
//! - **producer** creates `<dataset>` as a `Dataset` and commits the demo batch
//!   once (v1), then idles (heartbeating).
//! - **worker** claims one dispatched task, reads its [`TypedRef`] envelope,
//!   `resolve_and_pin`s the dataset zero-copy, and derives `sum(id)`. In normal
//!   mode it commits a `Result` entry `<result-key>` and `complete`s the task
//!   with a by-key `TypedRef` to it (then frees the request envelope). With
//!   `--crash-first` it stops **while holding the dataset pin** (writes `PINNED`
//!   and idles) so the test can `kill -9` it mid-pin — the coordinator's
//!   lease-sweep releases the leaked entry pin and requeues the task
//!   (at-least-once) for a second worker.

use std::sync::Arc;
use std::time::Duration;

use shm_arrow::SchemaRegistry;
use shm_runtime::demo::{demo_derive, demo_schema, result_batch, result_schema};
use shm_runtime::Node;
use shm_store::{RefKind, TypedRef};

/// Lease a worker stamps on its claim: long enough that a healthy worker finishes
/// well inside it, short enough that the test's `kill -9`ed worker's task is
/// requeued promptly (at-least-once).
const CLAIM_LEASE: Duration = Duration::from_millis(1000);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let role = args.get(1).map(String::as_str).unwrap_or("");
    let opts = Opts::parse(&args);
    let code = match role {
        "producer" => run_producer(&opts),
        "worker" => run_worker(&opts),
        other => {
            eprintln!("unknown role {other:?}; expected producer|worker");
            2
        }
    };
    std::process::exit(code);
}

struct Opts {
    uds: String,
    dataset: String,
    result_key: String,
    crash_first: bool,
    result: Option<String>,
}

impl Opts {
    fn parse(args: &[String]) -> Opts {
        let mut o = Opts {
            uds: String::new(),
            dataset: "dataset/X".to_string(),
            result_key: "result/X".to_string(),
            crash_first: false,
            result: None,
        };
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str() {
                "--uds" => {
                    o.uds = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--dataset" => {
                    o.dataset = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--result-key" => {
                    o.result_key = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--crash-first" => {
                    o.crash_first = true;
                    i += 1;
                }
                "--result" => {
                    o.result = args.get(i + 1).cloned();
                    i += 2;
                }
                _ => i += 1,
            }
        }
        o
    }
}

fn write_result(opts: &Opts, msg: &str) {
    if let Some(path) = &opts.result {
        let _ = std::fs::write(path, msg);
    }
}

fn idle_forever() -> ! {
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn run_producer(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "producer", Arc::new(SchemaRegistry::new())) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("producer connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));
    if let Err(e) = node.intern_schema(&demo_schema()) {
        eprintln!("producer intern_schema failed: {e}");
        return 1;
    }
    let key = opts.dataset.as_bytes().to_vec();
    let entry = match node
        .store()
        .and_then(|s| Ok(s.create(&key, RefKind::Dataset, &demo_schema())?))
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("producer create failed: {e}");
            return 1;
        }
    };
    if let Err(e) = entry.commit_replace(&shm_runtime::demo::demo_batch()) {
        eprintln!("producer commit failed: {e}");
        return 1;
    }
    write_result(opts, "OK");
    println!("producer committed {} to v1", opts.dataset);
    idle_forever();
}

fn run_worker(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "worker", Arc::new(SchemaRegistry::new())) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("worker connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));

    // Map the store + task queue and take a queue handle (owned; does not borrow
    // the node, so we can still call node methods below).
    if let Err(e) = node.open_store().and_then(|()| node.open_task_queue()) {
        eprintln!("worker open store/queue failed: {e}");
        return 1;
    }
    let tq = match node.task_queue() {
        Ok(q) => q,
        Err(e) => {
            eprintln!("worker task_queue failed: {e}");
            return 1;
        }
    };

    // Claim one dispatched task, parking on the work doorbell until one arrives.
    let claimed = match tq.claim_blocking(CLAIM_LEASE.as_nanos() as u64) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("worker claim failed: {e}");
            return 1;
        }
    };

    // Read the typed-ref envelope carried by the task's request descriptor.
    let tref = match node.task_ref(&claimed) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("worker read envelope failed: {e}");
            return 1;
        }
    };
    // The envelope carries the referent's schema id; learn it before reading.
    if let Err(e) = node.resolve_schema(tref.schema_id) {
        eprintln!("worker resolve_schema failed: {e}");
        return 1;
    }

    // Resolve → journaled pin → zero-copy batch.
    let (_entry, pin, batch) = match node.store().and_then(|s| Ok(s.resolve_and_pin(&tref)?)) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("worker resolve_and_pin failed: {e}");
            return 1;
        }
    };

    if opts.crash_first {
        // Stop mid-pin, holding the dataset entry pin, until the test `kill -9`s
        // us. The leaked journaled pin is what the coordinator lease-sweep frees.
        write_result(opts, "PINNED");
        println!("worker pinned {} and is idling (crash-first)", opts.dataset);
        let _hold = (pin, batch);
        idle_forever();
    }

    // Derive the result, then release the dataset pin (clean).
    let (sum, _rows) = match demo_derive(&batch) {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("worker derive failed: {msg}");
            return 1;
        }
    };
    drop((pin, batch));

    // Commit the derived scalar as a Result-kind keyed entry.
    let result_key = opts.result_key.as_bytes().to_vec();
    let result_schema_id = match node.intern_schema(&result_schema()) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("worker intern result schema failed: {e}");
            return 1;
        }
    };
    let result_key_id = match node.intern_key(&result_key) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("worker intern result key failed: {e}");
            return 1;
        }
    };
    let committed = node
        .store()
        .and_then(|s| Ok(s.create(&result_key, RefKind::Result, &result_schema())?))
        .and_then(|e| Ok(e.commit_replace(&result_batch(sum))?));
    if let Err(e) = committed {
        eprintln!("worker commit result failed: {e}");
        return 1;
    }

    // Complete the task with a by-key TypedRef to the result entry, then free the
    // request envelope (no longer needed by anyone once we complete).
    let result_ref = TypedRef::by_key(RefKind::Result, result_key_id, result_schema_id, 0);
    let result_desc = match node.write_ref_chunk(&result_ref) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("worker write result envelope failed: {e}");
            return 1;
        }
    };
    let request_desc = claimed.request();
    if let Err(e) = claimed.complete(result_desc) {
        eprintln!("worker complete failed: {e}");
        return 1;
    }
    let _ = node.free_ref_chunk(&request_desc);

    write_result(opts, "DONE");
    println!("worker completed with {} = {sum}", opts.result_key);
    idle_forever();
}
