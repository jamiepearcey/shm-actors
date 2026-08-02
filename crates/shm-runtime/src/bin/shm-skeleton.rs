//! The walking-skeleton demo binary.
//!
//! Roles:
//!
//! ```text
//! shm-skeleton coordinator --uds <path> [--seg-base <n>]
//! shm-skeleton producer    --uds <path> [--topic <t>]
//! shm-skeleton consumer     --uds <path> [--topic <t>] --result <file>
//! ```
//!
//! The integration test runs the coordinator **in-process** (so it can inspect
//! the payload segment's `ChunkCtrl` directly) and spawns the producer and
//! consumer as child processes via this binary — the consumer being the process
//! that is `kill -9`ed while holding its pin. Actors learn every segment id from
//! the coordinator's `Registered`/`Granted` replies, so children need only the
//! socket path.

use std::sync::Arc;
use std::time::Duration;

use shm_arrow::SchemaRegistry;
use shm_ring::Msg;
use shm_runtime::demo::{demo_batch, demo_schema, verify_demo_batch, DEMO_TOPIC};
use shm_runtime::{Coordinator, Node, RuntimeConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let role = args.get(1).map(String::as_str).unwrap_or("");
    let opts = Opts::parse(&args);

    let code = match role {
        "coordinator" => run_coordinator(&opts),
        "producer" => run_producer(&opts),
        "consumer" => run_consumer(&opts),
        other => {
            eprintln!("unknown role {other:?}; expected coordinator|producer|consumer");
            2
        }
    };
    std::process::exit(code);
}

/// Parsed command-line options.
struct Opts {
    uds: String,
    topic: String,
    result: Option<String>,
    seg_base: u32,
}

impl Opts {
    fn parse(args: &[String]) -> Opts {
        let mut uds = String::new();
        let mut topic = DEMO_TOPIC.to_string();
        let mut result = None;
        let mut seg_base = 1u32;
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str() {
                "--uds" => {
                    uds = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--topic" => {
                    topic = args.get(i + 1).cloned().unwrap_or_default();
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
                _ => i += 1,
            }
        }
        Opts {
            uds,
            topic,
            result,
            seg_base,
        }
    }
}

/// Seed a registry identically to every other actor (the v0.1 schema contract).
fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
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

fn run_producer(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "producer", registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("producer connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));

    let batch = demo_batch();
    match node.publish_batch(&opts.topic, &batch) {
        Ok(desc) => println!("producer published {desc:?}"),
        Err(e) => {
            eprintln!("producer publish failed: {e}");
            return 1;
        }
    }

    // Stay alive (heartbeating) so the coordinator does not reclaim us; the test
    // only kills the consumer.
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn run_consumer(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "consumer", registry()) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("consumer connect failed: {e}");
            return 1;
        }
    };
    let mut sub = match node.subscribe(&opts.topic) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("consumer subscribe failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));

    // Block until a sample arrives (skip any lag notices).
    let desc = loop {
        match sub.recv() {
            Msg::Sample(desc) => break desc,
            Msg::Lagged(_) => continue,
        }
    };

    // Pin + zero-copy reconstruct + verify contents.
    let pin = match node.pin_and_read(&desc) {
        Ok(p) => p,
        Err(e) => {
            write_result(opts, &format!("ERR pin: {e}"));
            return 1;
        }
    };
    if let Err(msg) = verify_demo_batch(&pin.batch) {
        write_result(opts, &format!("ERR verify: {msg}"));
        return 1;
    }

    // Signal the parent test that we have pinned + verified. We deliberately do
    // NOT release the pin — we idle holding it until `kill -9`, so the
    // coordinator's lease + journal replay is what frees it.
    write_result(opts, "OK");
    println!("consumer pinned + verified {desc:?}");

    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Write the consumer's result marker file (atomically enough for the test).
fn write_result(opts: &Opts, msg: &str) {
    if let Some(path) = &opts.result {
        let _ = std::fs::write(path, msg);
    }
}
