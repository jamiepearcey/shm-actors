//! v0.4 stage O §3 — **torn writes** (a producer dying mid-write).
//!
//! A payload chunk is written under an **exclusive loan** (`FREE → LOANED`) and
//! only becomes reachable by a reader once it transitions to `PUBLISHED` (pub/sub)
//! or is named by an installed version manifest (artifacts). Dereference is gated
//! by that state (plus the recycle `generation`), so a half-written / garbage
//! chunk that never reached `PUBLISHED`/installed is **structurally unreachable**
//! by any subscriber or pin: a reader sees the prior valid version or a clean
//! "no message", never torn bytes.
//!
//! Two tests:
//!
//! - [`raw_torn_write_is_never_observed`]: a producer literally writes **half** a
//!   loaned payload chunk with garbage and never publishes it, while a racing
//!   subscriber runs. The subscriber only ever observes the *prior valid* message,
//!   never the torn chunk. Dropping the loan recycles it (`LOANED → FREE`, bumped
//!   generation) — the isolation + recycle proof at the raw `ChunkCtrl` layer.
//! - [`torn_stream_write_isolated_and_reclaimed`]: an artifact writer stages an
//!   **incomplete** (never-committed) version, a reader pinning during that window
//!   sees only the committed v1, and the writer then "crashes" — journal replay
//!   frees the staged chunk and `current_version` never advanced. This is the
//!   isolation + **journal-replay reclaim** proof at the artifact layer.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::{ChunkAllocator, PoolAllocator, SchemaRegistry};
use shm_core::{Pool, FREE, LOANED};
use shm_ring::Msg;
use shm_runtime::demo::{demo_batch, demo_schema, verify_demo_batch, CACHE_ARTIFACT, DEMO_TOPIC};
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_stream::{Commit, Coordination};

/// A per-run segment-id base with a process-local counter so parallel test
/// threads in this binary never share a base (see `crash_matrix.rs`).
fn unique_seg_base() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    1_100_000 + n * 100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 50_000) as u32
}

fn demo_registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::with_schemas(&[demo_schema()]))
}

fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::new())
}

#[test]
fn raw_torn_write_is_never_observed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    let mut producer = Node::connect(&uds, "producer", demo_registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    let mut consumer = Node::connect(&uds, "consumer", demo_registry()).expect("consumer connect");
    consumer.start_heartbeat(Duration::from_millis(150));
    let mut sub = consumer.subscribe(DEMO_TOPIC).expect("subscribe");

    // A prior VALID publish the racing reader is allowed to see.
    let valid = producer
        .publish_batch(DEMO_TOPIC, &demo_batch())
        .expect("publish valid");
    assert_eq!(
        recv_sample(&mut sub, Duration::from_secs(5)),
        Some(valid.offset),
        "reader sees the prior valid message"
    );

    // --- The torn write: loan a fresh chunk, write only HALF of it with garbage,
    //     and NEVER publish it (no ring stamp). ---
    let pool = Pool::attach(producer.payload_segment()).expect("attach pool");
    let torn = pool.alloc(1024).expect("alloc torn chunk");
    let ctrl = pool.ctrl(&torn).expect("ctrl");
    ctrl.try_loan(producer.actor_id())
        .expect("loan the torn chunk");
    assert_eq!(ctrl.state(), LOANED, "the torn chunk is exclusively loaned");
    let gen_before = ctrl.generation();
    {
        let alloc = PoolAllocator::new(&pool, producer.payload_segment());
        let base = alloc.resolve(&torn);
        let half = (torn.len as usize) / 2;
        // SAFETY: `base` is the writable first byte of a `torn.len`-byte chunk we
        // exclusively own (LOANED); writing `half <= len` bytes stays in bounds.
        unsafe {
            std::ptr::write_bytes(base, 0xAB, half);
        }
        // The other half is deliberately left un-set: the chunk is half-written.
    }

    // A racing reader must observe NOTHING further — the torn chunk was never
    // published, so it is not on the ring and no `pin` can reach it.
    let saw_torn = recv_sample(&mut sub, Duration::from_millis(600));
    assert_eq!(
        saw_torn, None,
        "the reader must never observe the torn (never-published) chunk; got {saw_torn:?}"
    );
    assert_eq!(
        ctrl.state(),
        LOANED,
        "the torn chunk is still LOANED (never published), so it stayed invisible"
    );

    // Recycle: dropping the loan (as a crash reclaim or abort would) returns it to
    // FREE with a bumped generation, invalidating any descriptor minted over it.
    ctrl.drop_loan().expect("drop the torn loan");
    assert_eq!(ctrl.state(), FREE, "the torn chunk recycles to FREE");
    assert!(
        ctrl.generation() > gen_before,
        "recycle bumps the generation ({} -> {})",
        gen_before,
        ctrl.generation()
    );
}

#[test]
fn torn_stream_write_isolated_and_reclaimed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");

    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // A producer commits a clean v1 (item E: negotiate the schema via coordinator).
    let mut producer = Node::connect(&uds, "producer", registry()).expect("producer connect");
    producer.start_heartbeat(Duration::from_millis(150));
    producer
        .open_artifact(CACHE_ARTIFACT)
        .expect("open_artifact");
    producer
        .intern_schema(&demo_schema())
        .expect("intern schema");
    {
        let stream = producer.stream(CACHE_ARTIFACT).expect("stream");
        let mut w = stream
            .writer(
                Commit::Replace,
                Coordination::Optimistic { expect_version: 0 },
            )
            .expect("writer");
        w.append_batch(&demo_batch()).expect("append");
        assert_eq!(w.commit().expect("commit"), 1);
    }
    let one_version_free = coord.artifact_free_total(CACHE_ARTIFACT).expect("artifact");

    // A "torn" writer stages an INCOMPLETE version (never committed): its chunks
    // are LOANED + journalled but appear in no installed manifest.
    let torn_writer = Node::connect(&uds, "torn-writer", registry()).expect("torn connect");
    // (torn_writer has no heartbeat: we drive its reclaim deterministically.)
    let mut torn = torn_writer;
    torn.open_artifact(CACHE_ARTIFACT).expect("open_artifact");
    torn.intern_schema(&demo_schema()).expect("intern schema");
    {
        let stream = torn.stream(CACHE_ARTIFACT).expect("stream");
        let mut w = stream
            .writer(
                Commit::Replace,
                Coordination::Optimistic { expect_version: 1 },
            )
            .expect("writer");
        w.append_batch(&demo_batch())
            .expect("append staged (torn/incomplete) batch");
        assert!(
            coord.artifact_free_total(CACHE_ARTIFACT).unwrap() < one_version_free,
            "the staged incomplete version must consume chunks"
        );

        // --- ISOLATION: a reader pinning DURING the staged window sees only the
        //     committed v1 — never the incomplete staged data. ---
        let reader = Node::connect(&uds, "reader", registry()).expect("reader connect");
        let mut reader = reader;
        reader.open_artifact(CACHE_ARTIFACT).expect("open_artifact");
        let pin = reader.pin_artifact(CACHE_ARTIFACT).expect("pin");
        assert_eq!(
            pin.version(),
            1,
            "the reader is isolated to the committed v1, never the staged torn v2"
        );
        reader
            .resolve_schema(pin.manifest().schema_id)
            .expect("resolve schema");
        let batch = pin.as_arrow(reader.registry()).expect("read v1 zero-copy");
        verify_demo_batch(&batch).expect("the reader's view is the clean v1, not torn bytes");
        assert_eq!(
            coord.artifact_current_version(CACHE_ARTIFACT),
            Some(1),
            "no partial/torn version is ever installed as current"
        );

        // Simulate the writer dying mid-write: forget the writer so its staged
        // loans + journal entries leak exactly as a `kill -9` leaves them.
        std::mem::forget(w);
    }

    // Journal replay (crash reclaim) frees the torn staged chunk(s); the pool
    // returns to the one-version baseline and current_version never advanced.
    let reclaimed = coord
        .force_reclaim(torn.actor_id())
        .expect("force reclaim torn writer");
    assert!(
        !reclaimed.is_empty(),
        "the torn staged chunk must be reclaimed by journal replay"
    );
    assert_eq!(
        coord.artifact_free_total(CACHE_ARTIFACT),
        Some(one_version_free),
        "every torn staged chunk returned to the pool (zero leak)"
    );
    assert_eq!(
        coord.artifact_current_version(CACHE_ARTIFACT),
        Some(1),
        "current_version never advanced past the last CLEAN commit"
    );

    // The producer can still install a clean v2 — the artifact is not wedged.
    {
        let stream = producer.stream(CACHE_ARTIFACT).expect("stream");
        let mut w = stream
            .writer(
                Commit::Replace,
                Coordination::Optimistic { expect_version: 1 },
            )
            .expect("writer");
        w.append_batch(&demo_batch()).expect("append");
        assert_eq!(
            w.commit().expect("commit"),
            2,
            "a clean v2 installs after the torn write"
        );
    }
    assert_eq!(
        coord.artifact_free_total(CACHE_ARTIFACT),
        Some(one_version_free),
        "the clean v2 has the same one-version footprint (no residual torn chunks)"
    );
}

/// Receive the next real `Sample` offset within `timeout`, skipping lag notices.
fn recv_sample(
    sub: &mut shm_ring::Subscriber<shm_ring::DoorbellParker>,
    timeout: Duration,
) -> Option<u32> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        match sub.try_recv() {
            Some(Msg::Sample(d)) => return Some(d.offset),
            Some(Msg::Lagged(_)) => continue,
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    None
}
