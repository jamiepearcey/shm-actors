//! Dedicated payload segments: the large-object path.
//!
//! The shared data segment is carved into fixed size classes, so a value larger
//! than the biggest class cannot live in it contiguously. This is the same split
//! a process allocator makes — size-class bins for small allocations, a fresh
//! mapping for large ones — and it is why a large object gets a segment of its
//! own: contiguous by construction, and unable to fragment the shared arena
//! because it is never carved out of it.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_core::{BorrowJournal, JournalEntry, JournalRecord, Pool, PoolConfig, SizeClass};
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// This binary's own segment-id band, with the per-call counter owning the low
/// slots so two tests in this binary cannot draw overlapping ranges.
fn unique_seg_base() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed) as u64;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    // Every coordinator owns `base .. base + 2 MiB`: dedicated payload segment
    // ids start at `base + 1 MiB` and are never reused, so two bands must be at
    // least that far apart. This binary owns 32 draws; the per-call counter
    // and the pid/clock hash together pick one, so neither two tests in this
    // binary nor two binaries can overlap.
    569762048 + (((n + ((pid.wrapping_mul(2_654_435_761)) ^ nanos)) % 32) as u32) * 2097152
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

/// Unwrap a map that must have been refused, reporting the granted segment if
/// it was not. `Segment` is deliberately not `Debug`, so `expect_err` cannot be
/// used on this shape.
fn refusal(r: shm_runtime::Result<Arc<shm_core::Segment>>, ctx: &str) -> shm_runtime::Error {
    match r {
        Ok(seg) => panic!("{ctx} (segment {} was mapped)", seg.id()),
        Err(e) => e,
    }
}

/// Poll `cond` until it holds or `timeout` elapses, for the few assertions that
/// depend on a fire-and-forget request having reached the coordinator.
fn wait_until(timeout: std::time::Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    cond()
}

#[test]
fn a_large_payload_gets_its_own_contiguous_segment() {
    let (_dir, _coord, uds) = coordinator();
    let node = connect(&uds, "writer");

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
fn segments_are_independent_and_ids_are_never_reused() {
    let (_dir, _coord, uds) = coordinator();
    let node = connect(&uds, "writer");

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

    // A released id is NEVER handed out again. A segment carries no incarnation
    // counter and a fresh pool starts its chunk generations at zero, so a stale
    // descriptor naming a reused id would pass the generation check against
    // whatever object was granted that id next — the ABA the chunk generation
    // exists to prevent, reopened one level up. A never-reused id fails to
    // resolve instead, which readers already refuse.
    let freed = a.id();
    drop(a);
    node.release_payload_segment(freed).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
    let c = node.create_payload_segment(1 << 20).unwrap();
    assert_ne!(c.id(), freed, "a released segment id was handed out again");
    assert_ne!(c.id(), b.id());
}

#[test]
fn a_segment_held_by_a_dead_owner_is_reclaimed_by_journal_replay() {
    // The gap this closes: nothing in the shared pool describes a dedicated
    // segment, so the chunk reclamation path cannot see it. Without a journal
    // entry a crashed writer's segment would survive until the coordinator shut
    // down. This asserts it does not.
    let (_dir, coord, uds) = coordinator();
    let node = connect(&uds, "doomed");
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

    // And the id it held is not handed out again either: reclamation frees the
    // segment, not the identity. A reused id would let a stale descriptor from
    // the dead owner's object resolve against its successor.
    let next = connect(&uds, "after");
    let again = next.create_payload_segment(1 << 20).unwrap();
    assert_ne!(
        again.id(),
        seg_id,
        "a crashed segment's id was handed out again"
    );
}

// ---------------------------------------------------------------------------
// Lifetime follows the version, not the creator.
//
// A dedicated segment's object is installed as a *version*: the version's
// manifest holds the `+1` on its chunk and readers pin the version, never the
// segment. So the creator's borrow journal can only cover the **staging
// window** — create → install — exactly as it does for a staged manifest.
// After the install the object's chunk control word is the only thing that
// authorises an unlink, and the coordinator's sweep is the only thing that
// performs one.
// ---------------------------------------------------------------------------

/// The one-class/one-chunk pool a large object's dedicated segment carries.
///
/// Deliberately a real [`Pool`] rather than a bespoke "segment = chunk" header:
/// it reuses the control state machine, the recycle generations and the journal
/// kinds unchanged, so an object in its own segment is reclaimed by the same
/// machinery as one in the shared arena.
fn one_chunk_pool(chunk_size: u32) -> PoolConfig {
    PoolConfig {
        classes: vec![SizeClass {
            chunk_size,
            chunk_count: 1,
        }],
    }
}

/// Create a dedicated segment big enough for a one-chunk pool of `chunk_size`.
fn dedicated_segment(node: &Node, chunk_size: u32) -> Arc<shm_core::Segment> {
    let bytes = shm_core::required_bytes(&one_chunk_pool(chunk_size)) as u64;
    node.create_payload_segment(bytes).expect("grant")
}

#[test]
fn an_installed_objects_segment_outlives_its_creator() {
    let (_dir, coord, uds) = coordinator();
    let node = connect(&uds, "writer");
    let actor_id = node.actor_id();

    const CHUNK: u32 = 8 << 20;
    let seg = dedicated_segment(&node, CHUNK);
    let seg_id = seg.id();

    // Write the object and publish it, then take the reference an installed
    // version's manifest holds and hand off exclusive ownership — the state a
    // committed large object is actually in.
    let pool = Pool::create(&seg, &one_chunk_pool(CHUNK)).expect("lay pool");
    let desc = pool.alloc(CHUNK).expect("alloc");
    let ctrl = pool.ctrl(&desc).expect("ctrl");
    ctrl.try_loan(actor_id).expect("loan");
    ctrl.publish().expect("publish");
    ctrl.borrow_shared().expect("the manifest's +1");
    assert!(!ctrl.owner_release(), "still referenced, so not recycled");

    // The install closes the staging window: from here the chunk holds the
    // segment, not this node.
    node.install_payload_segment(seg_id).expect("install");

    // The staging entry is gone, exactly as a staged manifest's is after its
    // install CAS: the journal covered the window, not the object.
    let journal = BorrowJournal::attach(node.journal_segment()).expect("journal");
    assert!(
        !journal
            .replay()
            .any(|r| matches!(r, JournalRecord::PayloadSegment { .. })),
        "the staging entry outlived the install, so the creator still owns the object's lifetime"
    );

    // The creator dies without releasing anything.
    std::mem::forget(node);
    coord.force_reclaim(actor_id).expect("reclaim");

    assert_eq!(
        coord.payload_segments_live(),
        1,
        "an installed object's segment was unlinked when its creator died — \
         readers pin the version, which is still installed and still holding it"
    );
    assert_eq!(
        coord.sweep_payload_segments(),
        0,
        "the sweep unlinked a segment whose object is still held"
    );
    assert_eq!(ctrl.state(), shm_core::PUBLISHED);

    // Nor does an explicit release reach it. After the install the object's
    // chunk owns the segment's lifetime, so a release is a staging-window
    // operation that no longer applies — otherwise one careless caller could
    // unlink an installed object out from under every reader pinning it.
    let late = connect(&uds, "late-releaser");
    late.release_payload_segment(seg_id).expect("release");
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert_eq!(
        coord.payload_segments_live(),
        1,
        "an explicit release unlinked an installed object's segment"
    );

    // Now the version is superseded and the last pin dropped: the retire
    // cascade ends in exactly this pair, and the chunk goes FREE.
    assert!(
        ctrl.release_shared(),
        "the last reference should recycle it"
    );
    pool.free(&desc).expect("free");

    assert_eq!(
        coord.sweep_payload_segments(),
        1,
        "a quiescent installed segment was not unlinked"
    );
    assert_eq!(
        coord.payload_segments_live(),
        0,
        "the segment survived its object going FREE"
    );
    // Idempotent: only the coordinator unlinks, only from FREE, and the table
    // remove happens once.
    assert_eq!(coord.sweep_payload_segments(), 0);
}

#[test]
fn journal_replay_never_blind_unlinks_a_segment_whose_object_is_held() {
    // The creator crashed *inside* the staging window, after the object was
    // published and referenced but before it could report the install. Its
    // journal entry is still there — and replaying it as an unlink would drop
    // the object out from under whatever holds it.
    let (_dir, coord, uds) = coordinator();
    let node = connect(&uds, "doomed-committer");
    let actor_id = node.actor_id();

    const CHUNK: u32 = 4 << 20;
    let seg = dedicated_segment(&node, CHUNK);

    let pool = Pool::create(&seg, &one_chunk_pool(CHUNK)).expect("lay pool");
    let desc = pool.alloc(CHUNK).expect("alloc");
    let ctrl = pool.ctrl(&desc).expect("ctrl");
    ctrl.try_loan(actor_id).expect("loan");
    ctrl.publish().expect("publish");
    ctrl.borrow_shared().expect("the manifest's +1");
    assert!(!ctrl.owner_release());

    std::mem::forget(node);
    coord.force_reclaim(actor_id).expect("reclaim");

    assert_eq!(
        coord.payload_segments_live(),
        1,
        "replay unlinked a segment whose object was still referenced"
    );
    assert_eq!(ctrl.state(), shm_core::PUBLISHED);
    assert_eq!(
        ctrl.refcount(),
        1,
        "replay dropped a reference that was not the dead actor's"
    );

    // Replay handed the segment to the sweep (nobody is left to install it), so
    // dropping the last reference is now enough to reclaim it.
    assert_eq!(coord.sweep_payload_segments(), 0);
    assert!(ctrl.release_shared());
    pool.free(&desc).expect("free");
    assert_eq!(coord.sweep_payload_segments(), 1);
    assert_eq!(coord.payload_segments_live(), 0);
}

#[test]
fn journal_replay_reclaims_a_staged_objects_loan_and_unlinks() {
    // The other half of the same rule: a creator that died before anything
    // could reference the object leaves a chunk loaned to a dead actor. Replay
    // releases that loan and *then* finds the segment quiescent, so it unlinks.
    let (_dir, coord, uds) = coordinator();
    let node = connect(&uds, "doomed-writer");
    let actor_id = node.actor_id();

    const CHUNK: u32 = 4 << 20;
    let seg = dedicated_segment(&node, CHUNK);

    let pool = Pool::create(&seg, &one_chunk_pool(CHUNK)).expect("lay pool");
    let desc = pool.alloc(CHUNK).expect("alloc");
    pool.ctrl(&desc).expect("ctrl").try_loan(actor_id).unwrap();

    std::mem::forget(node);
    coord.force_reclaim(actor_id).expect("reclaim");

    assert_eq!(
        pool.ctrl(&desc).expect("ctrl").state(),
        shm_core::FREE,
        "a dead creator's loan was not released"
    );
    assert_eq!(
        coord.payload_segments_live(),
        0,
        "a segment holding nothing but a dead creator's loan was not unlinked"
    );
}

#[test]
fn a_chunk_pinned_in_a_dedicated_segment_is_reclaimed_on_death() {
    // A dedicated segment carries a real pool, so a reader's pin there is
    // journaled exactly like a pin in the shared arena. The coordinator routes
    // a journaled ChunkPin to the pool of *its own* segment, so it has to
    // consider the dedicated segments too — otherwise this pin is silently
    // dropped and the object never comes back.
    let (_dir, coord, uds) = coordinator();
    let writer = connect(&uds, "writer");
    let reader = connect(&uds, "reader");
    let reader_id = reader.actor_id();

    const CHUNK: u32 = 2 << 20;
    let seg = dedicated_segment(&writer, CHUNK);

    let pool = Pool::create(&seg, &one_chunk_pool(CHUNK)).expect("lay pool");
    let desc = pool.alloc(CHUNK).expect("alloc");
    let ctrl = pool.ctrl(&desc).expect("ctrl");
    ctrl.try_loan(writer.actor_id()).expect("loan");
    ctrl.publish().expect("publish");

    // The reader pins the object and journals the pin, then dies holding it.
    ctrl.borrow_shared().expect("pin");
    let journal = BorrowJournal::attach(reader.journal_segment()).expect("journal");
    journal
        .record_entry(JournalEntry::chunk_pin(desc))
        .expect("record");
    assert!(!ctrl.owner_release(), "the reader's pin still holds it");

    std::mem::forget(reader);
    let reclaimed = coord.force_reclaim(reader_id).expect("reclaim");

    assert_eq!(
        reclaimed,
        vec![desc],
        "a pin journaled against a dedicated segment was not reclaimed"
    );
    assert_eq!(pool.ctrl(&desc).expect("ctrl").state(), shm_core::FREE);
}

#[test]
fn installing_a_segment_that_holds_nothing_is_refused() {
    // An install asserts "something now holds this object". If nothing does,
    // honouring it would arm the sweep to unlink the segment on its next tick —
    // under the writer still filling it. Refusing keeps the staging window open,
    // which is the safe side.
    let (_dir, coord, uds) = coordinator();
    let node = connect(&uds, "writer");

    const CHUNK: u32 = 1 << 20;
    let seg = dedicated_segment(&node, CHUNK);
    let seg_id = seg.id();
    let pool = Pool::create(&seg, &one_chunk_pool(CHUNK)).expect("lay pool");

    assert!(
        node.install_payload_segment(seg_id).is_err(),
        "installing a segment with no live chunk was accepted"
    );
    assert_eq!(coord.sweep_payload_segments(), 0);
    assert_eq!(coord.payload_segments_live(), 1);

    // With the object actually held, the install is accepted.
    let desc = pool.alloc(CHUNK).expect("alloc");
    pool.ctrl(&desc).unwrap().try_loan(node.actor_id()).unwrap();
    node.install_payload_segment(seg_id).expect("install");
    assert_eq!(coord.sweep_payload_segments(), 0);
    assert_eq!(coord.payload_segments_live(), 1);
}

// ---------------------------------------------------------------------------
// The read side: a reader must be able to map a segment it did not create.
//
// A version's manifest names its chunks by `(segment_id, offset)`, and a
// descriptor naming a segment the reader has not mapped is *refused* rather
// than resolved against some other segment that happens to contain that offset.
// So without a way to obtain the fd, a second process can see that a large
// object exists and never read a byte of it.
// ---------------------------------------------------------------------------

#[test]
fn mapping_an_unknown_segment_id_is_refused() {
    // "Unknown" and "already unlinked" are the same answer, and deliberately so:
    // a segment leaves the coordinator's table exactly when it is unlinked, and
    // payload ids are never reused. An id that does not resolve names an object
    // that is gone — there is no fd to hand back and none is invented.
    let (_dir, _coord, uds) = coordinator();
    let node = connect(&uds, "reader");

    let err = refusal(
        node.map_payload_segment(12_345),
        "an id this coordinator never granted was answered with a mapping anyway",
    );
    assert!(
        matches!(&err, shm_runtime::Error::Rejected(m) if m.contains("12345")),
        "unexpected error for an unknown id: {err:?}"
    );
    assert!(
        node.mapped_payload_segment(12_345).is_none(),
        "a refused map left a mapping behind"
    );
}

#[test]
fn mapping_an_unlinked_segment_is_refused_rather_than_answered() {
    let (_dir, coord, uds) = coordinator();
    let writer = connect(&uds, "writer");
    let reader = connect(&uds, "reader");

    // Abandoned inside its staging window, so the explicit release unlinks it.
    let seg = writer.create_payload_segment(1 << 20).unwrap();
    let seg_id = seg.id();
    drop(seg);
    writer.release_payload_segment(seg_id).unwrap();
    assert!(wait_until(std::time::Duration::from_secs(5), || coord
        .payload_segments_live()
        == 0));

    let err = refusal(
        reader.map_payload_segment(seg_id),
        "an unlinked segment's id was still granted an fd",
    );
    assert!(
        matches!(&err, shm_runtime::Error::Rejected(m) if m.contains(&seg_id.to_string())),
        "unexpected error for an unlinked id: {err:?}"
    );
}

#[test]
fn a_staged_segment_is_not_mappable_but_an_installed_one_is() {
    // Before the install, the segment's lifetime is tied to its creator's
    // liveness: journal replay may unlink it and recycle its chunk at any
    // instant, and a reader holds nothing that would stop it. Nothing outside
    // the creator can legitimately know a staged segment exists — a reader
    // learns of one by pinning the version that names it, which cannot happen
    // until after the install.
    let (_dir, _coord, uds) = coordinator();
    let writer = connect(&uds, "writer");
    let reader = connect(&uds, "reader");

    const CHUNK: u32 = 1 << 20;
    let seg = dedicated_segment(&writer, CHUNK);
    let seg_id = seg.id();

    let err = refusal(
        reader.map_payload_segment(seg_id),
        "a staged segment was handed to a reader",
    );
    assert!(
        matches!(&err, shm_runtime::Error::Rejected(m) if m.contains("staged")),
        "unexpected error for a staged segment: {err:?}"
    );

    // Fill and install it: now the chunk, not the creator, owns the lifetime.
    let pool = Pool::create(&seg, &one_chunk_pool(CHUNK)).expect("lay pool");
    let desc = pool.alloc(CHUNK).expect("alloc");
    let ctrl = pool.ctrl(&desc).expect("ctrl");
    ctrl.try_loan(writer.actor_id()).expect("loan");
    // SAFETY: the chunk is exclusively loaned to the writer, and `desc.len`
    // bytes at `desc.offset` lie inside the segment the pool was laid in.
    unsafe {
        std::slice::from_raw_parts_mut(
            seg.payload_ptr().add(desc.offset as usize),
            desc.len as usize,
        )
        .fill(0xC3);
    }
    ctrl.publish().expect("publish");
    ctrl.borrow_shared().expect("the manifest's +1");
    assert!(!ctrl.owner_release());
    writer.install_payload_segment(seg_id).expect("install");

    let mapped = reader.map_payload_segment(seg_id).expect("map installed");
    assert_eq!(mapped.id(), seg_id);
    assert!(
        !std::ptr::eq(mapped.payload_ptr(), seg.payload_ptr()),
        "the reader got the creator's own mapping, so the fd never crossed"
    );
    // SAFETY: the chunk is PUBLISHED with the manifest's `+1` held, and the
    // span is inside the reader's own mapping of the same segment.
    let seen = unsafe {
        std::slice::from_raw_parts(
            mapped.payload_ptr().add(desc.offset as usize) as *const u8,
            desc.len as usize,
        )
    };
    assert!(
        seen.iter().all(|b| *b == 0xC3),
        "a separately-mapped reader did not see the object's bytes"
    );

    // Idempotent for a segment this node already holds: no second round-trip,
    // and one mapping per id so descriptor resolution stays stable.
    let again = reader.map_payload_segment(seg_id).expect("map again");
    assert!(Arc::ptr_eq(&again, &mapped));
    let own = writer
        .map_payload_segment(seg_id)
        .expect("creator maps its own");
    assert!(Arc::ptr_eq(&own, &seg));
}

#[test]
fn a_swept_segment_stops_being_mappable() {
    // The end of the object's life, from a late reader's point of view: once the
    // retire cascade drives the chunk to FREE and the coordinator's sweep
    // unlinks the segment, a reader that arrives afterwards is refused. It is
    // never handed a mapping onto an object that is gone.
    let (_dir, coord, uds) = coordinator();
    let writer = connect(&uds, "writer");
    let early = connect(&uds, "early-reader");
    let late = connect(&uds, "late-reader");

    const CHUNK: u32 = 1 << 20;
    let seg = dedicated_segment(&writer, CHUNK);
    let seg_id = seg.id();
    let pool = Pool::create(&seg, &one_chunk_pool(CHUNK)).expect("lay pool");
    let desc = pool.alloc(CHUNK).expect("alloc");
    let ctrl = pool.ctrl(&desc).expect("ctrl");
    ctrl.try_loan(writer.actor_id()).expect("loan");
    ctrl.publish().expect("publish");
    ctrl.borrow_shared().expect("the manifest's +1");
    assert!(!ctrl.owner_release());
    writer.install_payload_segment(seg_id).expect("install");

    let mapped = early.map_payload_segment(seg_id).expect("map installed");

    // The version is retired: the cascade ends in exactly this pair.
    assert!(
        ctrl.release_shared(),
        "the last reference should recycle it"
    );
    pool.free(&desc).expect("free");
    assert_eq!(coord.sweep_payload_segments(), 1);

    let err = refusal(
        late.map_payload_segment(seg_id),
        "a swept segment was still mappable",
    );
    assert!(
        matches!(&err, shm_runtime::Error::Rejected(m) if m.contains(&seg_id.to_string())),
        "unexpected error for a swept segment: {err:?}"
    );
    // The early reader's mapping is unharmed — unlink removes the name, not the
    // mapping — and its id is still never reused.
    assert_eq!(mapped.id(), seg_id);
    let next = writer.create_payload_segment(1 << 20).unwrap();
    assert_ne!(next.id(), seg_id);
}

// ---------------------------------------------------------------------------
// Across real processes.
//
// Everything above runs in one address space, where a mapping can be shared by
// handing over an `Arc`. The claim that actually matters is the one an
// `Arc` cannot fake: a large object written by one OS process, whose writer has
// **exited**, read back byte for byte by another OS process that only ever knew
// the segment's id — the fd travelling over `SCM_RIGHTS` on the coordinator's
// control socket.
// ---------------------------------------------------------------------------

/// Kill-and-reap on drop, so a failed assertion never leaves a child behind.
struct Reaper(std::process::Child);
impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn a_large_object_crosses_processes_byte_for_byte() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let writer_result = dir.path().join("writer.result");
    let reader_result = dir.path().join("reader.result");
    let refused_result = dir.path().join("refused.result");

    // Coordinator in-process, so the test can census `payload_segments` and
    // drive the sweep; the writer and reader are separate OS processes.
    let mut coord =
        Coordinator::bind(&uds, RuntimeConfig::with_seg_base(unique_seg_base())).unwrap();
    coord.start().unwrap();

    let exe = env!("CARGO_BIN_EXE_shm-payload-skeleton");
    // 96× the largest shared size class (`PoolConfig::power_of_two(4096,
    // 65536, 8)`), so this object could not have lived in the shared arena
    // contiguously at all.
    const BYTES: u32 = 6 << 20;

    // --- Writer: create, fill, publish, install, exit. ---
    let writer = Reaper(
        std::process::Command::new(exe)
            .args([
                "writer",
                "--uds",
                &uds_s,
                "--bytes",
                &BYTES.to_string(),
                "--result",
                writer_result.to_str().unwrap(),
            ])
            .spawn()
            .expect("spawn writer"),
    );
    let mut writer = writer;
    let status = writer.0.wait().expect("wait writer");
    assert!(
        status.success(),
        "writer exited {status:?} (result: {:?})",
        std::fs::read_to_string(&writer_result).ok()
    );

    let reported = std::fs::read_to_string(&writer_result).expect("writer result");
    let fields: Vec<&str> = reported.split_whitespace().collect();
    assert_eq!(
        fields.first().copied(),
        Some("OK"),
        "writer said {reported:?}"
    );
    let seg_id: u32 = fields[1].parse().unwrap();
    let offset: u32 = fields[2].parse().unwrap();
    let len: u32 = fields[3].parse().unwrap();
    let generation: u32 = fields[4].parse().unwrap();
    let sum: u64 = fields[5].parse().unwrap();
    assert_eq!(len, BYTES, "the writer did not fill the whole object");

    // The writer is gone, and the object is not: readers pin the version, whose
    // manifest holds the `+1` on the chunk. Nothing about the creator's death
    // reaches the segment.
    assert_eq!(
        coord.payload_segments_live(),
        1,
        "the object's segment did not outlive the process that wrote it"
    );
    assert_eq!(
        coord.sweep_payload_segments(),
        0,
        "the sweep unlinked a segment whose object is still held"
    );

    // --- Reader: a different process, which only knows the id. ---
    let reader = Reaper(
        std::process::Command::new(exe)
            .args([
                "reader",
                "--uds",
                &uds_s,
                "--seg-id",
                &seg_id.to_string(),
                "--offset",
                &offset.to_string(),
                "--len",
                &len.to_string(),
                "--generation",
                &generation.to_string(),
                "--checksum",
                &sum.to_string(),
                "--result",
                reader_result.to_str().unwrap(),
            ])
            .spawn()
            .expect("spawn reader"),
    );
    let mut reader = reader;
    let status = reader.0.wait().expect("wait reader");
    assert!(
        status.success(),
        "reader exited {status:?} (result: {:?})",
        std::fs::read_to_string(&reader_result).ok()
    );
    assert_eq!(
        std::fs::read_to_string(&reader_result).expect("reader result"),
        format!("OK {len} {sum}"),
        "the reader did not read back the writer's bytes"
    );

    // --- Retire the object, and the segment stops resolving. ---
    // This process now plays the actor that supersedes the version: it maps the
    // segment itself (the same grant the reader used), drops the manifest's
    // `+1`, and returns the chunk to the object's own pool.
    let node = connect(&uds, "retirer");
    let seg = node.map_payload_segment(seg_id).expect("map for retire");
    let pool = Pool::attach(&seg).expect("attach the object's pool");
    let desc = pool.desc_by_index(0).expect("the object's one chunk");
    let ctrl = pool.ctrl(&desc).expect("ctrl");
    assert_eq!(ctrl.refcount(), 1, "the reader left a pin behind");
    assert!(
        ctrl.release_shared(),
        "the last reference should recycle it"
    );
    pool.free(&desc).expect("free");
    assert_eq!(
        coord.sweep_payload_segments(),
        1,
        "a quiescent installed segment was not unlinked"
    );

    // A reader arriving now is refused — never handed a stale fd. The id is
    // never reused, so "gone" is the only thing it can ever mean again.
    let refused = Reaper(
        std::process::Command::new(exe)
            .args([
                "reader",
                "--expect-refused",
                "--uds",
                &uds_s,
                "--seg-id",
                &seg_id.to_string(),
                "--result",
                refused_result.to_str().unwrap(),
            ])
            .spawn()
            .expect("spawn late reader"),
    );
    let mut refused = refused;
    let status = refused.0.wait().expect("wait late reader");
    assert!(
        status.success(),
        "late reader exited {status:?} (result: {:?})",
        std::fs::read_to_string(&refused_result).ok()
    );
    let said = std::fs::read_to_string(&refused_result).expect("late reader result");
    assert!(
        said.starts_with("REFUSED") && said.contains(&seg_id.to_string()),
        "late reader said {said:?}"
    );
}
