//! The ADR-0007 G3 keyed-store walking skeleton (mirrors `walking_skeleton.rs`
//! and the hostile cache-loop census).
//!
//! - [`store_crash_reclaim_multiprocess`] — the target: a real coordinator
//!   (in-process, so it can read the store catalog + data pool) grants the store
//!   segments to a **creator** and a **pinner** spawned as separate OS processes.
//!   The creator `create`s `"dataset/X"` and commits v1→v3; the pinner opens it,
//!   pins v3 through its borrow journal, and is `kill -9`ed while holding the pin.
//!   The coordinator's lease-sweep + journal replay releases the leaked *entry*
//!   pin; a subsequent `evict` tears the entry down and the data-pool census
//!   returns to its baseline (zero leak).
//! - [`store_crash_reclaim_same_process_deterministic`] — the same reclaim +
//!   evict + census, driven without the lease clock (a leaked pin simulated with
//!   `mem::forget`, released via `force_reclaim`) for a fast, timing-immune proof.

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_runtime::demo::{demo_batch, demo_schema, verify_demo_batch};
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_store::RefKind;

const KEY: &str = "dataset/X";

fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    cond()
}

struct Reaper(Child);
impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn store_crash_reclaim_multiprocess() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let creator_result = dir.path().join("creator.result");
    let pinner_result = dir.path().join("pinner.result");
    let uds_s = uds.to_str().unwrap().to_string();

    // Coordinator in-process (so we can read the store catalog + data pool).
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // Empty-store data-pool census baseline (before anything is created).
    let baseline = coord
        .store_data_free_total()
        .expect("store data pool baseline");

    let exe = env!("CARGO_BIN_EXE_shm-store-skeleton");

    // --- Creator: create dataset/X + commit v1..v3. ---
    let creator = Command::new(exe)
        .args([
            "creator",
            "--uds",
            &uds_s,
            "--key",
            KEY,
            "--result",
            creator_result.to_str().unwrap(),
        ])
        .spawn()
        .expect("spawn creator");
    let mut creator = Reaper(creator);

    // Wait until the entry exists and reached v3 (observed via the coordinator).
    let at_v3 = wait_until(Duration::from_secs(20), || {
        coord.store_entry_version(KEY.as_bytes()) == Some(3)
    });
    assert!(
        at_v3,
        "creator never drove dataset/X to v3 (result: {:?})",
        std::fs::read_to_string(&creator_result).ok()
    );
    assert!(
        coord.store_data_free_total().unwrap() < baseline,
        "committing v1..v3 must have consumed store chunks"
    );

    // --- Pinner: open dataset/X, pin v3, hold it. ---
    let pinner = Command::new(exe)
        .args([
            "pinner",
            "--uds",
            &uds_s,
            "--key",
            KEY,
            "--result",
            pinner_result.to_str().unwrap(),
        ])
        .spawn()
        .expect("spawn pinner");
    let mut pinner = Reaper(pinner);

    let pinned = wait_until(Duration::from_secs(20), || {
        std::fs::read_to_string(&pinner_result)
            .map(|s| s.trim() == "OK")
            .unwrap_or(false)
    });
    assert!(
        pinned,
        "pinner never confirmed pin+verify (result: {:?})",
        std::fs::read_to_string(&pinner_result).ok()
    );

    // The entry pin is live in shared memory.
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.store_entry_pins(KEY.as_bytes(), 3).unwrap_or(0) >= 1
        }),
        "pinner's entry pin was never observed live"
    );

    // --- kill -9 the pinner WHILE it holds the entry pin. ---
    pinner.0.kill().expect("kill -9 pinner");
    let _ = pinner.0.wait();

    // The lease-sweep + journal replay releases the leaked entry pin.
    let released = wait_until(Duration::from_secs(5), || {
        coord.store_entry_pins(KEY.as_bytes(), 3) == Some(0)
    });
    assert!(
        released,
        "leaked entry pin was not released by lease-sweep (pins: {:?})",
        coord.store_entry_pins(KEY.as_bytes(), 3)
    );

    // --- Evict from the test process, then prove a zero-leak census. ---
    let mut evictor =
        Node::connect(&uds, "evictor", Arc::new(SchemaRegistry::new())).expect("evictor connect");
    evictor.start_heartbeat(Duration::from_millis(150));
    evictor
        .store()
        .expect("open store")
        .evict(KEY.as_bytes())
        .expect("evict dataset/X");

    assert_eq!(
        coord.store_entry_version(KEY.as_bytes()),
        None,
        "evicted key has no live entry"
    );
    let after = coord.store_data_free_total().expect("store census after evict");
    assert_eq!(
        after, baseline,
        "evict + reclaim returns the data pool to baseline (zero leak)"
    );

    let _ = creator.0.kill();
}

#[test]
fn store_crash_reclaim_same_process_deterministic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let baseline = coord.store_data_free_total().expect("baseline");

    // Creator: unseeded registry; negotiate the schema id via the coordinator.
    let mut creator =
        Node::connect(&uds, "creator", Arc::new(SchemaRegistry::new())).expect("creator connect");
    creator.start_heartbeat(Duration::from_millis(150));
    creator.intern_schema(&demo_schema()).expect("intern schema");

    // create + commit v1..v3 (owned Entry escapes the borrowed store handle).
    let entry = {
        let store = creator.store().expect("open store");
        store
            .create(KEY.as_bytes(), RefKind::Dataset, &demo_schema())
            .expect("create")
    };
    for _ in 0..3 {
        entry.commit_replace(&demo_batch()).expect("commit");
    }
    assert_eq!(entry.current_version(), 3);

    // Pinner: open, pin (journaled), resolve schema, verify the zero-copy batch.
    let mut pinner =
        Node::connect(&uds, "pinner", Arc::new(SchemaRegistry::new())).expect("pinner connect");
    pinner.start_heartbeat(Duration::from_millis(150));
    let pentry = {
        let store = pinner.store().expect("open store");
        store.open(KEY.as_bytes()).expect("open dataset/X")
    };
    let pin = pentry.pin().expect("pin v3");
    let schema_id = pin.manifest().schema_id;
    pinner.resolve_schema(schema_id).expect("resolve schema");
    let batch = pin.as_arrow(pinner.registry()).expect("zero-copy read");
    assert_eq!(pin.version(), 3);
    verify_demo_batch(&batch).expect("batch verifies");
    assert_eq!(
        coord.store_entry_pins(KEY.as_bytes(), 3),
        Some(1),
        "entry pin is live"
    );

    // Simulate a crash: leak the pin guard (its Drop never runs), so the shm pin +
    // its journal entry remain — exactly a `kill -9` mid-pin.
    std::mem::forget(pin);
    drop(batch);

    // Drive the exact crash-reclaim path deterministically (simulated expiry).
    coord
        .force_reclaim(pinner.actor_id())
        .expect("force reclaim pinner");
    assert_eq!(
        coord.store_entry_pins(KEY.as_bytes(), 3),
        Some(0),
        "leaked entry pin decremented by journal replay"
    );
    assert_eq!(
        coord.artifact_pins_reclaimed(),
        1,
        "the coordinator counted one leaked entry pin reclaim"
    );

    // Evict + census: back to baseline (zero leak).
    creator
        .store()
        .expect("open store")
        .evict(KEY.as_bytes())
        .expect("evict");
    assert_eq!(coord.store_entry_version(KEY.as_bytes()), None);
    assert_eq!(
        coord.store_data_free_total().unwrap(),
        baseline,
        "evict + reclaim returns the data pool to baseline"
    );
}
