//! Large objects: the split a process allocator makes at `M_MMAP_THRESHOLD`,
//! applied to the object store.
//!
//! An object the shared pool cannot hold does not get cut into segments — it
//! gets a coordinator-granted segment of its own, holding one pool of one class
//! of one chunk, and reads back as a single contiguous buffer. The threshold is
//! not a tunable: it is exactly `ObjectStore::pooled_ceiling` — the largest
//! payload the shared pool can hold — because below that pooled is strictly
//! cheaper (no round trip, no `mmap` per reader, no segment to unlink
//! afterwards).
//!
//! The properties pinned down here:
//!
//! - a payload several times the pooled ceiling round-trips **byte for byte**
//!   and arrives as **one** buffer, not a sequence of segments;
//! - replacing that version releases the segment it was using **through the
//!   chunk reaching `FREE`** — the coordinator's sweep takes it, no call site
//!   unlinks anything;
//! - an object at or under the ceiling still comes out of the shared pool and
//!   costs no segment at all;
//! - deleting a large object returns both censuses — the store's data pool and
//!   the live-segment table — to their baselines.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use holon_object::{ObjectSide, ObjectStore, Version};
use shm_arrow::SchemaRegistry;
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// Process-unique segment bases so parallel tests never collide on shm names.
static SEG: AtomicU32 = AtomicU32::new(0);

fn seg_base() -> u32 {
    let pid = std::process::id();
    let n = SEG.fetch_add(1, Ordering::Relaxed);
    3_500_000 + ((pid % 1000) * 100) + n * 20_000
}

/// A deliberately small shared pool: 16 chunks per class up to 4 KiB, so the
/// pooled ceiling is a few tens of KiB and "several times the ceiling" is a
/// payload a test can build in a millisecond.
fn config() -> RuntimeConfig {
    let mut c = RuntimeConfig::with_seg_base(seg_base());
    c.store_pool = shm_core::PoolConfig::power_of_two(256, 4096, 16);
    c.store_data_size = 1 << 20;
    c
}

fn connect(uds: &std::path::Path, name: &str) -> Node {
    let mut n = Node::connect(uds, name, Arc::new(SchemaRegistry::new())).expect("connect");
    n.start_heartbeat(Duration::from_millis(150));
    n
}

/// An index-dependent pattern, so a truncation, a repeat or an off-by-one chunk
/// boundary all show up as a mismatch rather than as more of the same byte.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i.wrapping_mul(31) % 251) as u8).collect()
}

const CT: &str = "application/octet-stream";

#[test]
fn an_object_several_times_the_pooled_ceiling_round_trips_as_one_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("coord.sock");
    let mut coord = Coordinator::bind(&uds, config()).unwrap();
    coord.start().unwrap();

    let mut node = connect(&uds, "writer");
    let side = ObjectSide::open(&mut node).unwrap();
    let registry = node.registry().clone();
    node.open_store().unwrap();
    let store = node.opened_store().unwrap();
    let objects = ObjectStore::new(&store, &side, registry, &node);

    let ceiling = objects.max_object().unwrap();
    let payload = pattern(ceiling as usize * 4);
    assert_eq!(coord.payload_segments_live(), 0, "nothing granted yet");

    assert_eq!(objects.put("big", CT, &payload).unwrap(), 1);
    assert_eq!(
        coord.payload_segments_live(),
        1,
        "an object the pool cannot hold gets a segment of its own"
    );

    let read = objects.get("big", Version::Latest).unwrap();
    assert_eq!(read.version(), 1);
    assert_eq!(read.content_type(), CT);
    // The point of the dedicated path: contiguous. A pooled object this size
    // would arrive as `ceiling * 4 / per_segment` separate buffers.
    assert_eq!(
        read.segment_count(),
        1,
        "a dedicated object is one contiguous buffer, not a sequence of segments"
    );
    assert_eq!(read.len(), payload.len());
    assert_eq!(
        read.segment(0).expect("the one buffer"),
        payload.as_slice(),
        "byte for byte"
    );
    drop(read);

    // The whole point of a segment of its own: it cannot fragment the shared
    // arena, because it was never carved from it. Only the manifest chunk came
    // out of the pool, and one more object of the same size still fits.
    assert_eq!(objects.put("big2", CT, &payload).unwrap(), 1);
    assert_eq!(coord.payload_segments_live(), 2);
    let second = objects.get("big2", Version::Latest).unwrap();
    assert_eq!(second.segment(0).unwrap(), payload.as_slice());
}

#[test]
fn replacing_a_large_object_releases_the_segment_the_old_version_used() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("coord.sock");
    let mut coord = Coordinator::bind(&uds, config()).unwrap();
    coord.start().unwrap();

    let mut node = connect(&uds, "writer");
    let side = ObjectSide::open(&mut node).unwrap();
    let registry = node.registry().clone();
    node.open_store().unwrap();
    let store = node.opened_store().unwrap();
    let objects = ObjectStore::new(&store, &side, registry, &node);

    let ceiling = objects.max_object().unwrap();
    let v1 = pattern(ceiling as usize * 3);
    let v2 = pattern(ceiling as usize * 5);

    assert_eq!(objects.put("big", CT, &v1).unwrap(), 1);
    assert_eq!(coord.payload_segments_live(), 1);
    // Nothing to sweep while the version is installed and current.
    assert_eq!(coord.sweep_payload_segments(), 0);

    assert_eq!(objects.put("big", CT, &v2).unwrap(), 2);
    // Both segments are still *named*: the second put unlinked nothing itself.
    // The commit's retire cascade drove v1's chunk to `FREE`, and that — the
    // control word, not the call site — is the only thing that authorises an
    // unlink. Were the release explicit at the call site, this would already
    // read 1 and the sweep below would find nothing.
    assert_eq!(
        coord.payload_segments_live(),
        2,
        "the superseded segment is released by the sweep, not by the writer"
    );
    assert_eq!(
        coord.sweep_payload_segments(),
        1,
        "the replaced version's chunk reached FREE, so its segment is unlinked"
    );
    assert_eq!(coord.payload_segments_live(), 1);
    // Idempotent: the live one is still held by v2's manifest.
    assert_eq!(coord.sweep_payload_segments(), 0);

    let read = objects.get("big", Version::Latest).unwrap();
    assert_eq!(read.version(), 2);
    assert_eq!(read.segment_count(), 1);
    assert_eq!(read.segment(0).unwrap(), v2.as_slice());
}

#[test]
fn an_object_the_pool_can_hold_still_comes_out_of_the_pool() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("coord.sock");
    let mut coord = Coordinator::bind(&uds, config()).unwrap();
    coord.start().unwrap();

    let mut node = connect(&uds, "writer");
    let side = ObjectSide::open(&mut node).unwrap();
    let registry = node.registry().clone();
    node.open_store().unwrap();
    let store = node.opened_store().unwrap();
    let objects = ObjectStore::new(&store, &side, registry, &node);

    // The exact switch-over point for *this* content type: `max_object` is the
    // number that holds for any content type, which is this or a little less.
    let ceiling = objects.pooled_ceiling(CT).unwrap() as usize;
    assert!(ceiling >= objects.max_object().unwrap() as usize);
    let baseline = coord.store_data_free_total().expect("baseline");

    // A small one.
    let small = pattern(37);
    assert_eq!(objects.put("small", CT, &small).unwrap(), 1);
    assert_eq!(
        coord.payload_segments_live(),
        0,
        "a small object costs no segment"
    );
    assert_eq!(objects.get("small", Version::Latest).unwrap().len(), 37);
    assert!(
        coord.store_data_free_total().unwrap() < baseline,
        "it came out of the shared pool"
    );

    // The threshold is the pooled ceiling itself, not a fraction of it: exactly
    // `max_object` is still pooled, and one byte more is not.
    let at = pattern(ceiling);
    assert_eq!(objects.put("edge", CT, &at).unwrap(), 1);
    assert_eq!(
        coord.payload_segments_live(),
        0,
        "an object of exactly the ceiling still fits the pool"
    );
    let read = objects.get("edge", Version::Latest).unwrap();
    assert!(
        read.segment_count() > 1,
        "a pooled object of the ceiling is cut into segments"
    );
    assert_eq!(read.len(), ceiling);
    drop(read);
    objects.delete("edge").unwrap();

    let over = pattern(ceiling + 1);
    assert_eq!(objects.put("over", CT, &over).unwrap(), 1);
    assert_eq!(
        coord.payload_segments_live(),
        1,
        "one byte past the ceiling takes a segment of its own"
    );
    let read = objects.get("over", Version::Latest).unwrap();
    assert_eq!(read.segment_count(), 1);
    assert_eq!(read.segment(0).unwrap(), over.as_slice());
}

#[test]
fn deleting_a_large_object_returns_both_censuses_to_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("coord.sock");
    let mut coord = Coordinator::bind(&uds, config()).unwrap();
    coord.start().unwrap();
    let baseline = coord.store_data_free_total().expect("baseline");

    let mut node = connect(&uds, "writer");
    let side = ObjectSide::open(&mut node).unwrap();
    let registry = node.registry().clone();
    node.open_store().unwrap();
    let store = node.opened_store().unwrap();
    let objects = ObjectStore::new(&store, &side, registry, &node);

    let ceiling = objects.max_object().unwrap() as usize;
    for v in 1..=3u64 {
        let body = pattern(ceiling * (1 + v as usize));
        assert_eq!(objects.put("big", CT, &body).unwrap(), v);
    }
    // Two superseded versions plus the current one; only the current one is
    // still held.
    assert_eq!(coord.sweep_payload_segments(), 2);
    assert_eq!(coord.payload_segments_live(), 1);
    assert!(
        coord.store_data_free_total().unwrap() < baseline,
        "the live version's manifest holds a chunk"
    );

    objects.delete("big").unwrap();
    coord.sweep_store_tombstones();
    assert_eq!(
        coord.sweep_payload_segments(),
        1,
        "the deleted object's chunk reached FREE, so its segment is unlinked"
    );
    assert_eq!(coord.payload_segments_live(), 0);
    assert_eq!(
        coord.store_data_free_total().unwrap(),
        baseline,
        "delete returns the shared pool to baseline too"
    );
    assert!(
        coord
            .store_slot_states()
            .iter()
            .all(|&s| s == shm_store::SLOT_FREE),
        "every catalog slot returned to FREE: {:?}",
        coord.store_slot_states()
    );
}

#[test]
fn a_second_node_reads_a_large_object_it_never_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("coord.sock");
    let mut coord = Coordinator::bind(&uds, config()).unwrap();
    coord.start().unwrap();

    let mut writer = connect(&uds, "writer");
    let wside = ObjectSide::open(&mut writer).unwrap();
    let wreg = writer.registry().clone();
    writer.open_store().unwrap();
    let wstore = writer.opened_store().unwrap();
    let wobjects = ObjectStore::new(&wstore, &wside, wreg, &writer);

    let ceiling = wobjects.max_object().unwrap();
    let payload = pattern(ceiling as usize * 4);
    assert_eq!(wobjects.put("shared", CT, &payload).unwrap(), 1);

    // A second node that has never seen this segment. Its pin is refused until
    // it maps it — and the refusal names the segment, which is how `get` knows
    // what to ask the coordinator for. The order is the safe one: the version
    // pin is taken first, so the manifest's reference already holds the chunk
    // against `FREE` before the mapping is attempted.
    let mut reader = connect(&uds, "reader");
    let rside = ObjectSide::open(&mut reader).unwrap();
    let rreg = reader.registry().clone();
    reader.open_store().unwrap();
    let rstore = reader.opened_store().unwrap();
    let robjects = ObjectStore::new(&rstore, &rside, rreg, &reader);

    let read = robjects.get("shared", Version::Latest).unwrap();
    assert_eq!(read.segment_count(), 1);
    assert_eq!(read.len(), payload.len());
    assert_eq!(read.segment(0).unwrap(), payload.as_slice());
    // The reader's mapping is its own, and a second read is a cache hit.
    let again = robjects.get("shared", Version::Latest).unwrap();
    assert_eq!(again.segment(0).unwrap(), payload.as_slice());
}
