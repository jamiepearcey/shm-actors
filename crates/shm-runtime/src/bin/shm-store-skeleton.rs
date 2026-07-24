//! The ADR-0007 G3 keyed-store walking-skeleton binary.
//!
//! Roles (the coordinator runs **in-process** inside the integration test so it
//! can inspect the store catalog + data pool; these children are separate OS
//! processes so a `kill -9` mid-pin is real):
//!
//! ```text
//! shm-store-skeleton creator --uds <path> --key <k> [--result <file>]
//! shm-store-skeleton pinner  --uds <path> --key <k>  --result <file>
//! ```
//!
//! - **creator** opens the store, `create`s `<key>` as a `Dataset`, and commits
//!   the demo batch three times (v1→v3), then idles (heartbeating).
//! - **pinner** opens `<key>`, waits until it is at v3, pins it through its borrow
//!   journal, verifies the zero-copy batch + version, writes `OK`, and then idles
//!   **holding the pin** until the test `kill -9`s it — so the coordinator's
//!   lease-sweep + journal replay is what releases the leaked entry pin.

use std::sync::Arc;
use std::time::{Duration, Instant};

use shm_arrow::SchemaRegistry;
use shm_runtime::demo::{demo_batch, demo_schema, verify_demo_batch};
use shm_runtime::{Node, Result};
use shm_store::{Entry, RefKind};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let role = args.get(1).map(String::as_str).unwrap_or("");
    let opts = Opts::parse(&args);
    let code = match role {
        "creator" => run_creator(&opts),
        "pinner" => run_pinner(&opts),
        other => {
            eprintln!("unknown role {other:?}; expected creator|pinner");
            2
        }
    };
    std::process::exit(code);
}

struct Opts {
    uds: String,
    key: String,
    result: Option<String>,
}

impl Opts {
    fn parse(args: &[String]) -> Opts {
        let mut uds = String::new();
        let mut key = "dataset/X".to_string();
        let mut result = None;
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str() {
                "--uds" => {
                    uds = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--key" => {
                    key = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--result" => {
                    result = args.get(i + 1).cloned();
                    i += 2;
                }
                _ => i += 1,
            }
        }
        Opts { uds, key, result }
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

fn run_creator(opts: &Opts) -> i32 {
    // Unseeded registry: negotiate the schema id through the coordinator so it
    // agrees with the pinner without identical seeding.
    let mut node = match Node::connect(&opts.uds, "creator", Arc::new(SchemaRegistry::new())) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("creator connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));
    if let Err(e) = node.intern_schema(&demo_schema()) {
        eprintln!("creator intern_schema failed: {e}");
        return 1;
    }

    let key = opts.key.as_bytes().to_vec();
    // Owned `Entry` escapes the borrowed store handle.
    let created: Result<Entry> = node
        .store()
        .and_then(|s| Ok(s.create(&key, RefKind::Dataset, &demo_schema())?));
    let entry = match created {
        Ok(e) => e,
        Err(e) => {
            eprintln!("creator create failed: {e}");
            return 1;
        }
    };
    for v in 1..=3 {
        if let Err(e) = entry.commit_replace(&demo_batch()) {
            eprintln!("creator commit v{v} failed: {e}");
            return 1;
        }
    }
    write_result(opts, "OK");
    println!("creator committed {} to v3", opts.key);
    idle_forever();
}

fn run_pinner(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "pinner", Arc::new(SchemaRegistry::new())) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("pinner connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));

    let key = opts.key.as_bytes().to_vec();

    // Wait until the entry exists and is at v3. The store handle borrows the node,
    // so scope it per attempt and let the owned `Entry` escape.
    let deadline = Instant::now() + Duration::from_secs(20);
    let entry = loop {
        let opened: Option<Entry> = node
            .store()
            .ok()
            .and_then(|s| s.open(&key).ok())
            .filter(|e| e.current_version() == 3);
        if let Some(e) = opened {
            break e;
        }
        if Instant::now() > deadline {
            write_result(opts, "ERR: entry never reached v3");
            return 1;
        }
        std::thread::sleep(Duration::from_millis(20));
    };

    // Pin through the journal (crash-reclaimable), resolve the schema via the
    // coordinator, then reconstruct the batch zero-copy.
    let pin = match entry.pin() {
        Ok(p) => p,
        Err(e) => {
            write_result(opts, &format!("ERR pin: {e}"));
            return 1;
        }
    };
    let schema_id = pin.manifest().schema_id;
    if let Err(e) = node.resolve_schema(schema_id) {
        write_result(opts, &format!("ERR resolve: {e}"));
        return 1;
    }
    let batch = match pin.as_arrow(node.registry()) {
        Ok(b) => b,
        Err(e) => {
            write_result(opts, &format!("ERR read: {e}"));
            return 1;
        }
    };
    if pin.version() != 3 {
        write_result(opts, &format!("ERR version {} != 3", pin.version()));
        return 1;
    }
    if let Err(msg) = verify_demo_batch(&batch) {
        write_result(opts, &format!("ERR verify: {msg}"));
        return 1;
    }

    // Signal the parent, then idle holding the pin until `kill -9`.
    write_result(opts, "OK");
    println!("pinner pinned + verified {} at v3", opts.key);
    idle_forever();
}
