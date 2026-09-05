//! Dedicated payload segments: the large-object path.
//!
//! The shared data segment is carved into fixed size classes, so a value larger
//! than the biggest class cannot live in it contiguously. This is the same split
//! a process allocator makes — size-class bins for small allocations, a fresh
//! mapping for large ones — and it is why a large object gets a segment of its
//! own: contiguous by construction, and unable to fragment the shared arena
//! because it is never carved out of it.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// This binary's own segment-id band, with the per-call counter owning the low
/// slots so two tests in this binary cannot draw overlapping ranges.
fn unique_seg_base() -> u32 {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    140_000_000
        + n * 16_384
        + ((((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 16) as u32) * 16_384 * 16
}

fn coordinator() -> (tempfile::TempDir, Coordinator, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let uds = dir.path().join("coord.sock");
    let mut coord =
        Coordinator::bind(&uds, RuntimeConfig::with_seg_base(unique_seg_base())).unwrap();
    coord.start().unwrap();
    (dir, coord, uds)
}

fn connect(uds: &std::path::Path, name: &str) -> Node {
    Node::connect(uds, name, Arc::new(SchemaRegistry::new())).expect("connect")
}

#[test]
fn a_large_payload_gets_its_own_contiguous_segment() {
    let (_dir, _coord, uds) = coordinator();
    let mut node = connect(&uds, "writer");

    // Far larger than any size class the shared pool offers, and not a round
    // number so an off-by-one in sizing shows up.
    const BYTES: u64 = (48 << 20) + 12_345;
    let seg = node.create_payload_segment(BYTES).expect("grant");

    assert!(
        seg.size() >= BYTES as usize,
        "granted {} bytes for a {BYTES}-byte request",
        seg.size()
    );

    // Contiguous: one mapping, one base pointer, the whole payload addressable
    // from it. Write a pattern across the entire span and read it back.
    // SAFETY: this node exclusively owns the freshly-created segment; nothing
    // else has been handed its fd yet.
    let payload = unsafe { std::slice::from_raw_parts_mut(seg.payload_ptr(), BYTES as usize) };
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    assert!(
        payload
            .iter()
            .enumerate()
            .all(|(i, b)| *b == (i % 251) as u8),
        "the segment did not hold what was written across its whole span"
    );

    // It is resolvable by id, which is what lets a `ChunkDesc` pointing outside
    // the shared data segment be read at all.
    let id = seg.id();
    assert!(node.mapped_payload_segment(id).is_some());

    node.release_payload_segment(id).unwrap();
    assert!(
        node.mapped_payload_segment(id).is_none(),
        "released segment still resolves"
    );
}

#[test]
fn segments_are_independent_and_ids_are_recycled() {
    let (_dir, _coord, uds) = coordinator();
    let mut node = connect(&uds, "writer");

    let a = node.create_payload_segment(1 << 20).unwrap();
    let b = node.create_payload_segment(1 << 20).unwrap();
    assert_ne!(a.id(), b.id(), "two objects shared one segment");

    // Independent: writing one does not disturb the other.
    // SAFETY: both are exclusively owned by this node.
    unsafe {
        std::slice::from_raw_parts_mut(a.payload_ptr(), 1 << 20).fill(0xAA);
        std::slice::from_raw_parts_mut(b.payload_ptr(), 1 << 20).fill(0x55);
        assert!(std::slice::from_raw_parts(a.payload_ptr(), 1 << 20)
            .iter()
            .all(|x| *x == 0xAA));
        assert!(std::slice::from_raw_parts(b.payload_ptr(), 1 << 20)
            .iter()
            .all(|x| *x == 0x55));
    }

    // A released id comes back: the range is bounded, so it has to be reused
    // rather than run into the next coordinator's base.
    let freed = a.id();
    drop(a);
    node.release_payload_segment(freed).unwrap();
    // Give the coordinator a moment to process the fire-and-forget release.
    std::thread::sleep(std::time::Duration::from_millis(100));
    let c = node.create_payload_segment(1 << 20).unwrap();
    assert_eq!(c.id(), freed, "a released segment id was not recycled");
}

#[test]
fn a_segment_held_by_a_dead_owner_is_reclaimed_by_journal_replay() {
    // The gap this closes: nothing in the shared pool describes a dedicated
    // segment, so the chunk reclamation path cannot see it. Without a journal
    // entry a crashed writer's segment would survive until the coordinator shut
    // down. This asserts it does not.
    let (_dir, coord, uds) = coordinator();
    let mut node = connect(&uds, "doomed");
    let actor_id = node.actor_id();

    let seg = node.create_payload_segment(4 << 20).unwrap();
    let seg_id = seg.id();
    assert_eq!(coord.payload_segments_live(), 1);

    // Die holding it: no release, no goodbye. `forget` keeps the mapping alive
    // exactly as a killed process would, and the node is dropped without the
    // release path running.
    std::mem::forget(seg);
    std::mem::forget(node);

    coord.force_reclaim(actor_id).expect("reclaim");
    assert_eq!(
        coord.payload_segments_live(),
        0,
        "a dead owner's segment survived reclamation"
    );

    // And its id is available again, so the bounded range is not consumed by a
    // crash.
    let mut next = connect(&uds, "after");
    let again = next.create_payload_segment(1 << 20).unwrap();
    assert_eq!(
        again.id(),
        seg_id,
        "the crashed segment's id was not recycled"
    );
}
