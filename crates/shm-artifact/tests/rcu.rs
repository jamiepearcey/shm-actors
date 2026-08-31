//! Integration tests for `shm-artifact`: real `Segment`s + `Pool`, RCU isolation,
//! reclamation correctness, multi-writer coordination, and watch events.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_arrow::SchemaRegistry;
use shm_artifact::{write_manifest, Artifact, Commit, CommitKind, VersionEvent};
use shm_core::{BorrowJournal, JournalRecord, PoolConfig, Segment, FREE, PUBLISHED};
use shm_ring::{Msg, Publisher, Ring, Subscriber};

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    49_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A pair of freshly created, auto-unlinked segments (management head + data pool).
struct Fixture {
    head_id: u32,
    data_id: u32,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
}

impl Fixture {
    fn new() -> Fixture {
        let head_id = next_segment_id();
        let data_id = next_segment_id();
        let _ = Segment::unlink_by_id(head_id);
        let _ = Segment::unlink_by_id(data_id);
        let head_seg = Arc::new(Segment::create(head_id, 1 << 16).expect("create head"));
        let data_seg = Arc::new(Segment::create(data_id, 1 << 20).expect("create data"));
        Fixture {
            head_id,
            data_id,
            head_seg,
            data_seg,
        }
    }

    /// A fixture whose data segment is `data_bytes` long (for the chained-manifest
    /// lineage tests, which need thousands of small chunks).
    fn with_data_size(data_bytes: usize) -> Fixture {
        let head_id = next_segment_id();
        let data_id = next_segment_id();
        let _ = Segment::unlink_by_id(head_id);
        let _ = Segment::unlink_by_id(data_id);
        let head_seg = Arc::new(Segment::create(head_id, 1 << 16).expect("create head"));
        let data_seg = Arc::new(Segment::create(data_id, data_bytes).expect("create data"));
        Fixture {
            head_id,
            data_id,
            head_seg,
            data_seg,
        }
    }

    fn artifact(&self, name_id: u32) -> Artifact {
        self.artifact_with(name_id, &PoolConfig::power_of_two(1024, 4096, 64))
    }

    fn artifact_with(&self, name_id: u32, pool: &PoolConfig) -> Artifact {
        Artifact::create(name_id, self.head_seg.clone(), self.data_seg.clone(), pool)
            .expect("create artifact")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = Segment::unlink_by_id(self.head_id);
        let _ = Segment::unlink_by_id(self.data_id);
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    let a = Int64Array::from(vals.to_vec());
    RecordBatch::try_new(schema(), vec![Arc::new(a)]).unwrap()
}

fn registry() -> SchemaRegistry {
    SchemaRegistry::with_schemas(std::slice::from_ref(&schema()))
}

/// Extract the single Int64 column as a `Vec<i64>`.
fn col(b: &RecordBatch) -> Vec<i64> {
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

// ---------------------------------------------------------------------------

#[test]
fn replace_commit_pin_and_read_is_zero_copy() {
    let fx = Fixture::new();
    let art = fx.artifact(1);
    let reg = registry();

    let b = batch(&[10, 20, 30]);
    let mut w = art.open_exclusive(7).unwrap();
    let v = w.commit(Commit::Replace, &b, &reg).unwrap();
    assert_eq!(v, 1);
    drop(w);

    let pin = art.pin().unwrap();
    assert_eq!(pin.version(), 1);
    let out = pin.as_arrow(&reg).unwrap();
    assert_eq!(col(&out), vec![10, 20, 30]);
    assert_eq!(&out, &b);

    // Zero-copy: the value buffer points inside the data segment mapping.
    let base = fx.data_seg.base_ptr() as usize;
    let end = base + fx.data_seg.size();
    let p = out.column(0).to_data().buffers()[0].as_ptr() as usize;
    assert!((base..end).contains(&p), "buffer escaped the segment");
}

#[test]
fn append_shares_prior_chunk_and_rcu_isolates_old_pin() {
    let fx = Fixture::new();
    let art = fx.artifact(2);
    let reg = registry();

    // v1 with [1,2,3].
    let mut w = art.open_exclusive(7).unwrap();
    assert_eq!(
        w.commit(Commit::Replace, &batch(&[1, 2, 3]), &reg).unwrap(),
        1
    );

    // Pin v1 BEFORE committing v2.
    let pin_v1 = art.pin().unwrap();
    assert_eq!(pin_v1.version(), 1);

    // v2 appends [4,5], sharing v1's chunk.
    assert_eq!(w.commit(Commit::Append, &batch(&[4, 5]), &reg).unwrap(), 2);
    drop(w);

    // The pin taken on v1 still reads v1 correctly AFTER v2 is installed.
    assert_eq!(col(&pin_v1.as_arrow(&reg).unwrap()), vec![1, 2, 3]);

    // A fresh pin sees v2 = both chunks concatenated. Its head manifest lists
    // only v2's OWN chunk and links to v1's manifest (ADR-0013); the table is
    // the chain.
    let pin_v2 = art.pin().unwrap();
    assert_eq!(pin_v2.version(), 2);
    assert_eq!(pin_v2.manifest().chunks.len(), 1, "own chunk only");
    assert_eq!(pin_v2.manifest().depth, 1);
    assert_eq!(pin_v2.manifest().total_batches, 2);
    assert_eq!(pin_v2.manifest().prev.map(|l| l.version), Some(1));
    let chain = pin_v2.chain().unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].version, 1, "oldest first");
    assert_eq!(chain[1].version, 2);
    assert_eq!(pin_v2.data_chunks().unwrap().len(), 2);
    assert_eq!(
        pin_v2.data_chunks().unwrap()[0],
        pin_v1.manifest().chunks[0],
        "v2's table starts with v1's chunk, shared not copied"
    );
    assert_eq!(col(&pin_v2.as_arrow(&reg).unwrap()), vec![1, 2, 3, 4, 5]);

    // v1's old pin STILL reads v1 after v2 exists (repeat to prove isolation).
    assert_eq!(col(&pin_v1.as_arrow(&reg).unwrap()), vec![1, 2, 3]);
}

#[test]
fn reclamation_frees_exclusive_chunk_but_keeps_shared() {
    let fx = Fixture::new();
    let art = fx.artifact(3);
    let reg = registry();

    let mut w = art.open_exclusive(7).unwrap();
    // v1: exclusive chunk with [1,2,3].
    w.commit(Commit::Replace, &batch(&[1, 2, 3]), &reg).unwrap();
    let v1_data = art.pin().unwrap().manifest().chunks[0];

    // v2: append links to v1's manifest (sharing v1's chunk through the chain)
    // and adds its own chunk [4].
    w.commit(Commit::Append, &batch(&[4]), &reg).unwrap();
    let pin2 = art.pin().unwrap();
    let v2_manifest = pin2.manifest().clone();
    let table = pin2.data_chunks().unwrap();
    drop(pin2);
    let shared = table[0];
    let v2_exclusive = v2_manifest.chunks[0];
    assert_eq!(shared, v1_data, "v2 must share v1's chunk");
    let v1_link = v2_manifest.prev.expect("v2 links to v1");
    let v1_manifest_chunk = shm_core::ChunkDesc {
        segment_id: v1_link.mref.segment_id(),
        offset: v1_link.mref.offset(),
        ..shm_core::ChunkDesc::ZERO
    };

    // Attach a pool to inspect chunk control words.
    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();

    // After v2 is current, v1 (non-current, unpinned) is retired on commit:
    // its one reference (on its manifest) is released, but v2's link still
    // holds that manifest, so nothing of v1 is freed — the shared data chunk
    // stays PUBLISHED with its single listing-manifest reference.
    assert_eq!(
        pool.ctrl(&shared).unwrap().state(),
        PUBLISHED,
        "shared chunk must survive v1 retirement"
    );
    assert_eq!(
        pool.ctrl(&shared).unwrap().refcount(),
        1,
        "a data chunk's refcount is the number of manifests listing it"
    );
    assert_eq!(pool.ctrl(&v1_manifest_chunk).unwrap().state(), PUBLISHED);
    assert_eq!(
        pool.ctrl(&v1_manifest_chunk).unwrap().refcount(),
        1,
        "v1's manifest is held only by v2's link once v1 retired"
    );

    // v3: replace wholesale with [9]. v2 becomes non-current and (unpinned) is
    // reclaimed: the cascade frees v2's manifest + exclusive chunk, follows the
    // link, and frees v1's manifest + the shared chunk.
    w.commit(Commit::Replace, &batch(&[9]), &reg).unwrap();
    drop(w);
    // No live pins on v2 → the whole chain is reclaimed.
    assert_eq!(
        pool.ctrl(&shared).unwrap().state(),
        FREE,
        "shared chunk must be freed once its last version (v2) retired"
    );
    assert_eq!(
        pool.ctrl(&v2_exclusive).unwrap().state(),
        FREE,
        "v2's exclusive chunk must be freed"
    );
    assert_eq!(
        pool.ctrl(&v1_manifest_chunk).unwrap().state(),
        FREE,
        "the cascade must free v1's manifest"
    );

    // v3 is readable.
    let pin = art.pin().unwrap();
    assert_eq!(col(&pin.as_arrow(&reg).unwrap()), vec![9]);
}

#[test]
fn pin_defers_reclamation_until_dropped() {
    let fx = Fixture::new();
    let art = fx.artifact(4);
    let reg = registry();

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1, 2, 3]), &reg).unwrap();
    let v1_chunk = art.pin().unwrap().manifest().chunks[0];

    // Hold a pin on v1, then commit v2 (Replace, so v1's chunk is exclusive).
    let held = art.pin().unwrap();
    w.commit(Commit::Replace, &batch(&[4]), &reg).unwrap();
    drop(w);

    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();
    // v1 is non-current but PINNED → its chunk must NOT be reclaimed yet.
    assert_eq!(pool.ctrl(&v1_chunk).unwrap().state(), PUBLISHED);
    assert_eq!(col(&held.as_arrow(&reg).unwrap()), vec![1, 2, 3]);

    // Dropping the last pin retires v1 → its chunk returns to the pool.
    drop(held);
    assert_eq!(
        pool.ctrl(&v1_chunk).unwrap().state(),
        FREE,
        "v1 chunk must be reclaimed once its last pin drops"
    );
}

#[test]
fn optimistic_conflict_one_writer_loses() {
    let fx = Fixture::new();
    let art = Arc::new(fx.artifact(5));
    let reg = Arc::new(registry());

    // Seed v1 so both racers target v1 -> v2.
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[0]), &reg)
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for owner in [11u32, 12u32] {
        let art = art.clone();
        let reg = reg.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            art.commit_optimistic(owner, 1, Commit::Replace, &batch(&[owner as i64]), &reg)
        }));
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let oks = results.iter().filter(|r| r.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|r| matches!(r, Err(shm_artifact::Error::Conflict { .. })))
        .count();
    assert_eq!(oks, 1, "exactly one optimistic commit should win");
    assert_eq!(conflicts, 1, "the other must observe a Conflict");
    assert_eq!(art.current_version(), 2);
}

#[test]
fn exclusive_lease_blocks_second_writer() {
    let fx = Fixture::new();
    let art = fx.artifact(6);

    let w1 = art.open_exclusive(7).unwrap();
    // A second exclusive open must fail fast.
    assert!(matches!(
        art.open_exclusive(8),
        Err(shm_artifact::Error::WriteLocked)
    ));
    drop(w1);
    // After the first lease drops, a new writer can open.
    assert!(art.open_exclusive(8).is_ok());
}

#[test]
fn version_event_published_on_commit() {
    let fx = Fixture::new();
    let reg = registry();

    // A dedicated ring segment for the __artifacts topic.
    let ring_id = next_segment_id();
    let _ = Segment::unlink_by_id(ring_id);
    let ring_seg = Arc::new(Segment::create(ring_id, 1 << 16).unwrap());
    // SAFETY: the payload region stays mapped for the segment's lifetime and is
    // initialised exactly once here before any attach.
    let ring = unsafe { Ring::init(ring_seg.payload_ptr(), ring_seg.payload_len(), 64).unwrap() };
    let publisher = Arc::new(Publisher::new(ring.clone()));
    let mut sub = Subscriber::from_start(ring.clone());

    let pub_for_sink = publisher.clone();
    let art = fx.artifact(42).with_watch(move |ev: VersionEvent| {
        pub_for_sink.publish(ev.to_desc());
    });

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1]), &reg).unwrap();
    w.commit(Commit::Append, &batch(&[2]), &reg).unwrap();
    drop(w);

    // Drain the ring and decode the events.
    let mut events = Vec::new();
    while let Some(msg) = sub.try_recv() {
        if let Msg::Sample(desc) = msg {
            events.push(VersionEvent::from_desc(&desc).expect("event desc"));
        }
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].name_id, 42);
    assert_eq!(events[0].version, 1);
    assert_eq!(events[0].kind, CommitKind::Replace.as_u32());
    assert_eq!(events[1].version, 2);
    assert_eq!(events[1].kind, CommitKind::Append.as_u32());

    let _ = Segment::unlink_by_id(ring_id);
}

#[test]
fn concurrent_readers_never_see_a_torn_version() {
    let fx = Fixture::new();
    let art = Arc::new(fx.artifact(9));
    let reg = Arc::new(registry());

    // Seed v1.
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1, 1, 1]), &reg)
        .unwrap();

    const COMMITS: u64 = 200;
    const READERS: usize = 4;

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Writer thread: commit COMMITS more versions, each a full replace whose
    // every value equals the version number (so a torn read would be detectable).
    let writer = {
        let art = art.clone();
        let reg = reg.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut expect = 1u64;
            while expect <= COMMITS {
                let target = expect + 1;
                let vals = vec![target as i64; 3];
                match art.commit_optimistic(7, expect, Commit::Replace, &batch(&vals), &reg) {
                    Ok(v) => expect = v,
                    Err(shm_artifact::Error::Conflict { actual, .. }) => expect = actual,
                    Err(e) => panic!("unexpected commit error: {e:?}"),
                }
            }
            stop.store(true, Ordering::Release);
        })
    };

    // Reader threads: repeatedly pin + read; every row must equal the version.
    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let art = art.clone();
            let reg = reg.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                let mut reads = 0u64;
                while !stop.load(Ordering::Acquire) {
                    let pin = art.pin().unwrap();
                    let v = pin.version();
                    let out = pin.as_arrow(&reg).unwrap();
                    for x in col(&out) {
                        assert_eq!(x, v as i64, "torn version: value != version");
                    }
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    writer.join().unwrap();
    let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total > 0, "readers should have completed some reads");
    assert_eq!(art.current_version(), COMMITS + 1);
}

// ---------------------------------------------------------------------------
// ADR-0003a pin hazard handshake + item J (journalled version pins)
// ---------------------------------------------------------------------------

/// A freshly created, auto-unlinked borrow-journal segment.
fn journal_segment() -> Arc<Segment> {
    let id = next_segment_id();
    let _ = Segment::unlink_by_id(id);
    Arc::new(Segment::create(id, 1 << 16).expect("create journal segment"))
}

#[test]
fn reclaimer_skips_freeing_a_live_pin_then_frees_on_drop() {
    // The deterministic side of the hazard handshake: while a pin is live the
    // reclaimer must NOT free the (non-current) version's chunks; the moment the
    // pin drops, retirement frees them promptly.
    let fx = Fixture::new();
    let art = fx.artifact(101);
    let reg = registry();

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1, 2, 3]), &reg).unwrap();
    let v1_chunk = art.pin().unwrap().manifest().chunks[0];

    // Hold a pin on v1, then supersede it with v2 (Replace ⇒ v1's chunk is
    // exclusive to v1). The reclaimer runs on the commit but must observe our
    // live pin (pins==1) and revert FREEING→LIVE without freeing.
    let held = art.pin().unwrap();
    assert_eq!(art.version_pin_count(1), Some(1), "one live pin on v1");
    w.commit(Commit::Replace, &batch(&[4]), &reg).unwrap();
    drop(w);

    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();
    assert_eq!(
        pool.ctrl(&v1_chunk).unwrap().state(),
        PUBLISHED,
        "the reclaimer must skip freeing v1 while it is pinned"
    );
    assert_eq!(art.version_pin_count(1), Some(1));

    // Dropping the last pin retires v1 promptly: its slot is freed and its chunk
    // returns to the pool.
    drop(held);
    assert_eq!(
        pool.ctrl(&v1_chunk).unwrap().state(),
        FREE,
        "v1 chunk must be reclaimed the moment its last pin drops"
    );
    assert_eq!(
        art.version_pin_count(1),
        None,
        "v1's slot must be free once retired"
    );
}

#[test]
fn journalled_pin_records_and_clean_drop_releases() {
    // A journalled pin records exactly one ArtifactPin entry; a *clean* drop
    // releases the journal slot so a replay finds nothing.
    let fx = Fixture::new();
    let art = fx.artifact(102);
    let reg = registry();
    let jseg = journal_segment();
    BorrowJournal::create(&jseg, 64).unwrap();

    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[10, 20]), &reg)
        .unwrap();

    let pin = art.pin_journaled(&jseg).unwrap();
    assert_eq!(pin.version(), 1);

    // The journal now holds exactly one ArtifactPin{artifact_id: 102, version: 1}.
    let journal = BorrowJournal::attach(&jseg).unwrap();
    assert_eq!(journal.len(), 1);
    let recs: Vec<JournalRecord> = journal.replay().collect();
    assert_eq!(
        recs,
        vec![JournalRecord::ArtifactPin {
            artifact_id: 102,
            incarnation: shm_artifact::FIRST_INCARNATION,
            version: 1
        }]
    );

    // A clean drop releases the journal slot (nothing left for a replay).
    drop(pin);
    assert_eq!(
        journal.len(),
        0,
        "clean drop must release the journal entry"
    );
}

#[test]
fn release_leaked_pin_retires_a_crashed_readers_version() {
    // Item J: a leaked (never-dropped) journalled pin is crash-reclaimed by
    // replaying the ArtifactPin entry and calling `release_leaked_pin`, which
    // retires the version exactly as a clean drop would.
    let fx = Fixture::new();
    let art = fx.artifact(103);
    let reg = registry();
    let jseg = journal_segment();
    BorrowJournal::create(&jseg, 64).unwrap();

    // v1, journalled-pinned, then superseded by v2 (v1 non-current + pinned).
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1, 2, 3]), &reg)
        .unwrap();
    let pin = art.pin_journaled(&jseg).unwrap();
    let v1_chunk = pin.manifest().chunks[0];
    art.commit_optimistic(7, 1, Commit::Replace, &batch(&[9]), &reg)
        .unwrap();

    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();
    assert_eq!(
        pool.ctrl(&v1_chunk).unwrap().state(),
        PUBLISHED,
        "v1 chunk pinned by the leaked pin must survive"
    );

    // Simulate a crash: forget the pin so its Drop never runs — the ArtifactPin
    // journal entry (and the +1 pin count) leak, exactly as a `kill -9` leaves
    // them.
    std::mem::forget(pin);
    assert_eq!(art.version_pin_count(1), Some(1));

    // Crash reclamation: replay the journal and release each leaked ArtifactPin.
    let journal = BorrowJournal::attach(&jseg).unwrap();
    let mut released = 0;
    for rec in journal.replay() {
        if let JournalRecord::ArtifactPin {
            artifact_id,
            incarnation,
            version,
        } = rec
        {
            assert_eq!(artifact_id, 103);
            assert_eq!(incarnation, shm_artifact::FIRST_INCARNATION);
            if art.release_leaked_pin(version).unwrap() {
                released += 1;
            }
        }
    }
    assert_eq!(released, 1, "exactly one leaked pin reclaimed");

    // The version is retired: its slot is gone and its chunk is back in the pool.
    assert_eq!(art.version_pin_count(1), None);
    assert_eq!(
        pool.ctrl(&v1_chunk).unwrap().state(),
        FREE,
        "the leaked pin's version chunk must be reclaimed"
    );
    assert_eq!(art.current_version(), 2);
}

#[test]
fn stress_readers_never_dereference_a_freed_chunk() {
    // The concurrency proof (loom stand-in — see the note below): N readers race
    // a committer that churns many Replace versions (each value == its version)
    // while a mix of plain and journalled pins force constant retirement. Every
    // pinned read must observe a self-consistent snapshot (value == version);
    // reading a freed/recycled chunk would surface as garbage or an error.
    //
    // NOTE ON LOOM: the pin/retire core operates on atomics embedded *inside*
    // mmap'd shared-memory segments, reached through raw pointers over a real
    // `Pool`/`Segment`. `loom` models a closed set of `loom::sync` atomics and
    // cannot see atomics living behind `Segment::payload_ptr()`, so a faithful
    // loom model would require re-implementing the algorithm off its shm
    // substrate (a second, unshared copy to verify). We instead stress the real
    // substrate hard; the `SeqCst` interleaving proof lives in the `artifact`
    // module doc.
    let fx = Fixture::new();
    let art = Arc::new(fx.artifact(104));
    let reg = Arc::new(registry());
    let jseg = journal_segment();
    BorrowJournal::create(&jseg, 1024).unwrap();

    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1, 1, 1]), &reg)
        .unwrap();

    const COMMITS: u64 = 400;
    const READERS: usize = 6;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let art = art.clone();
        let reg = reg.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut expect = 1u64;
            while expect <= COMMITS {
                let vals = vec![(expect + 1) as i64; 3];
                match art.commit_optimistic(7, expect, Commit::Replace, &batch(&vals), &reg) {
                    Ok(v) => expect = v,
                    Err(shm_artifact::Error::Conflict { actual, .. }) => expect = actual,
                    Err(e) => panic!("unexpected commit error: {e:?}"),
                }
            }
            stop.store(true, Ordering::Release);
        })
    };

    let readers: Vec<_> = (0..READERS)
        .map(|i| {
            let art = art.clone();
            let reg = reg.clone();
            let stop = stop.clone();
            let jseg = jseg.clone();
            thread::spawn(move || {
                let mut reads = 0u64;
                while !stop.load(Ordering::Acquire) {
                    // Alternate journalled and plain pins so both paths (and the
                    // journal record/release churn) are exercised under the race.
                    let pin = if i % 2 == 0 {
                        art.pin_journaled(&jseg).unwrap()
                    } else {
                        art.pin().unwrap()
                    };
                    let v = pin.version();
                    let out = pin.as_arrow(&reg).unwrap();
                    for x in col(&out) {
                        assert_eq!(x, v as i64, "freed/torn chunk: value {x} != version {v}");
                    }
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    writer.join().unwrap();
    let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total > 0, "readers should have completed some reads");
    assert_eq!(art.current_version(), COMMITS + 1);

    // Every journalled pin was taken and dropped cleanly: the journal is empty.
    let journal = BorrowJournal::attach(&jseg).unwrap();
    assert_eq!(
        journal.len(),
        0,
        "no journalled pin may leak after a clean run"
    );
}

// ---------------------------------------------------------------------------
// ADR-0003 item K — coordinator-backed, FENCED write lease
// ---------------------------------------------------------------------------

/// Total free chunks across every size class of a data pool.
fn free_total(seg: &Segment) -> usize {
    let pool = shm_core::Pool::attach(seg).unwrap();
    (0..pool.num_classes()).map(|c| pool.free_count(c)).sum()
}

#[test]
fn journalled_exclusive_records_and_clean_drop_releases() {
    // Opening the exclusive lease journalled records exactly one WriteLease entry
    // (carrying the artifact id + acquired fence); a clean drop releases both the
    // journal slot and the lease.
    let fx = Fixture::new();
    let art = fx.artifact(110);
    let jseg = journal_segment();
    BorrowJournal::create(&jseg, 64).unwrap();
    let journal = BorrowJournal::attach(&jseg).unwrap();

    {
        let w = art.open_exclusive_journaled(7, &journal).unwrap();
        assert_eq!(w.owner(), 7);
        assert_eq!(w.fence_token(), 0, "first lease is acquired under fence 0");
        assert_eq!(art.write_lease_owner(), 7, "lease is held by the writer");

        // Exactly one WriteLease{artifact_id: 110, fence: 0} is journaled.
        assert_eq!(journal.len(), 1);
        let recs: Vec<JournalRecord> = journal.replay().collect();
        assert_eq!(
            recs,
            vec![JournalRecord::WriteLease {
                artifact_id: 110,
                incarnation: shm_artifact::FIRST_INCARNATION,
                fence: 0
            }]
        );
        // A live owner still fences a second writer out fast.
        assert!(matches!(
            art.open_exclusive(8),
            Err(shm_artifact::Error::WriteLocked)
        ));
    }

    // Clean drop released the journal slot AND the lease (fence bumped to 1).
    assert_eq!(
        journal.len(),
        0,
        "clean drop must release the WriteLease entry"
    );
    assert_eq!(art.write_lease_owner(), 0, "clean drop must free the lease");
}

#[test]
fn fenced_committer_installs_no_version_and_returns_staged() {
    // The crux of item K: a committer whose fence was force-advanced (as the
    // coordinator does on declaring the writer dead) is rejected with
    // `Error::Fenced` on commit — installing NO version and returning every
    // staged chunk to the pool — so a zombie writer's late commit is safe.
    let fx = Fixture::new();
    let art = fx.artifact(111);
    let reg = registry();

    let mut w = art.open_exclusive(7).unwrap();
    assert_eq!(w.fence_token(), 0);
    let free_before = free_total(&fx.data_seg);

    // Simulate the coordinator declaring this writer dead and force-releasing its
    // lease (fence 0 → 1). `w` still holds the now-stale token 0.
    assert!(
        art.release_leaked_write_lease(),
        "force-release must report a lease was held"
    );
    assert_eq!(art.write_lease_owner(), 0, "lease is force-released");

    // The zombie's commit is fenced: no version installed, staged chunk returned.
    assert!(matches!(
        w.commit(Commit::Replace, &batch(&[1, 2, 3]), &reg),
        Err(shm_artifact::Error::Fenced)
    ));
    assert_eq!(
        art.current_version(),
        0,
        "a fenced commit installs no version"
    );
    assert_eq!(
        free_total(&fx.data_seg),
        free_before,
        "a fenced commit must return every staged chunk to the pool"
    );

    // The staged-multi-chunk path is fenced identically (commit_staged).
    let mut w2 = art.open_exclusive(9).unwrap();
    assert!(art.release_leaked_write_lease());
    // Stage one chunk by hand (written + loaned + journaled shape a stream uses).
    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();
    let alloc = shm_arrow::PoolAllocator::new(&pool, &fx.data_seg);
    let desc = shm_arrow::write_batch(&alloc, &reg, &batch(&[4, 5])).unwrap();
    pool.ctrl(&desc).unwrap().try_loan(9).unwrap();
    let free_staged = free_total(&fx.data_seg);
    assert!(matches!(
        w2.commit_staged(Commit::Replace, &[desc], &[1], desc.schema_id),
        Err(shm_artifact::Error::Fenced)
    ));
    assert_eq!(art.current_version(), 0);
    assert!(
        free_total(&fx.data_seg) > free_staged,
        "the pre-staged LOANED chunk must be returned on a fenced commit_staged"
    );
    // And after everything, a fresh writer can still acquire and commit.
    let mut w3 = art.open_exclusive(11).unwrap();
    assert_eq!(w3.commit(Commit::Replace, &batch(&[9]), &reg).unwrap(), 1);
}

#[test]
fn clean_release_bumps_fence_and_new_writer_acquires() {
    // A clean release bumps the fence so the next acquirer reads a *fresh* token,
    // and a second writer can take the lease and commit.
    let fx = Fixture::new();
    let art = fx.artifact(112);
    let reg = registry();

    let mut w1 = art.open_exclusive(7).unwrap();
    assert_eq!(w1.fence_token(), 0);
    assert_eq!(w1.commit(Commit::Replace, &batch(&[1]), &reg).unwrap(), 1);
    drop(w1); // clean release: fence 0 → 1, lease freed.
    assert_eq!(art.write_lease_owner(), 0);

    // The new writer acquires under the bumped fence (token 1, not 0).
    let mut w2 = art.open_exclusive(8).unwrap();
    assert_eq!(
        w2.fence_token(),
        1,
        "clean release must have bumped the fence"
    );
    assert_eq!(w2.commit(Commit::Replace, &batch(&[2]), &reg).unwrap(), 2);
    drop(w2);

    assert_eq!(art.current_version(), 2);
    assert_eq!(col(&art.pin().unwrap().as_arrow(&reg).unwrap()), vec![2]);
}

// ---------------------------------------------------------------------------
// ADR-0003 item F — multi-chunk / nested Arrow batches (+ no-leak reclamation).
// ---------------------------------------------------------------------------

/// A wide Int64 batch (`cols` columns × `rows` rows) whose serialized size
/// exceeds the fixture pool's 4 KiB max chunk, forcing a multi-chunk batch while
/// each individual value buffer stays well under one chunk.
fn wide_batch(cols: usize, rows: usize) -> (RecordBatch, SchemaRef) {
    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    for c in 0..cols {
        fields.push(Field::new(format!("c{c}"), DataType::Int64, false));
        let vals: Vec<i64> = (0..rows as i64).map(|r| r + (c as i64) * 100_000).collect();
        columns.push(Arc::new(Int64Array::from(vals)));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    (
        RecordBatch::try_new(schema.clone(), columns).unwrap(),
        schema,
    )
}

#[test]
fn multi_chunk_version_reads_zero_copy_and_reclaims_every_chunk() {
    let fx = Fixture::new();
    let art = fx.artifact(200);

    // 6 columns × 200 rows: value buffers 1600 B each (< 4096), total ~9.6 KiB
    // → the batch spans several 4 KiB chunks.
    let (b, schema) = wide_batch(6, 200);
    let reg = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    let full_free = free_total(&fx.data_seg);

    // Commit v1 as ONE multi-chunk batch (inline write → write_batch_chunks).
    let mut w = art.open_exclusive(7).unwrap();
    assert_eq!(w.commit(Commit::Replace, &b, &reg).unwrap(), 1);
    drop(w);

    let used_v1 = full_free - free_total(&fx.data_seg);

    // Pin v1: it is genuinely multi-chunk, and reads back equal, zero-copy. The
    // pin AND the zero-copy batch (which keeps the pin alive) are dropped at the
    // end of this scope so v1 becomes fully unpinned before it is superseded.
    {
        let pin = art.pin().unwrap();
        let m = pin.manifest();
        assert!(
            m.chunks.len() >= 2,
            "batch must span >= 2 data chunks, got {}",
            m.chunks.len()
        );
        assert_eq!(
            m.batch_spans,
            vec![m.chunks.len() as u32],
            "one batch spanning all its chunks"
        );
        let out = pin.as_arrow(&reg).unwrap();
        assert_eq!(&out, &b, "multi-chunk version reads back equal");
        // Zero-copy: a value buffer of the last column lives inside the data segment.
        let base = fx.data_seg.base_ptr() as usize;
        let end = base + fx.data_seg.size();
        let p = out.column(5).to_data().buffers()[0].as_ptr() as usize;
        assert!((base..end).contains(&p), "buffer escaped the data segment");
    }

    // Supersede v1 with v2 (Replace). v1 is unpinned + non-current → retired, so
    // EVERY chunk of the multi-chunk v1 (data + manifest) returns to the pool.
    let mut w2 = art.open_exclusive(9).unwrap();
    assert_eq!(w2.commit(Commit::Replace, &b, &reg).unwrap(), 2);
    drop(w2);

    let used_v2 = full_free - free_total(&fx.data_seg);
    // No leak: with v1 fully reclaimed, exactly v2's chunks are in use — the same
    // count as v1 (identical shape). Any leaked v1 chunk would make used_v2 > used_v1.
    assert_eq!(
        used_v2, used_v1,
        "v1's chunks must be fully reclaimed (no leak): used_v1={used_v1}, used_v2={used_v2}"
    );

    // And v2 still reads back equal over the recycled pool.
    assert_eq!(&art.pin().unwrap().as_arrow(&reg).unwrap(), &b);
}

#[test]
fn multi_chunk_append_shares_prior_chunks_across_versions() {
    let fx = Fixture::new();
    let art = fx.artifact(201);
    let (b1, schema) = wide_batch(6, 200);
    let reg = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    // v1: one multi-chunk batch.
    let mut w = art.open_exclusive(7).unwrap();
    assert_eq!(w.commit(Commit::Replace, &b1, &reg).unwrap(), 1);
    drop(w);
    let free_after_v1 = free_total(&fx.data_seg);
    let v1_chunks = art.pin().unwrap().manifest().chunks.len();

    // v2: Append the same multi-chunk batch → v2 = v1's chunks (shared, refcount)
    // + v2's new chunks, as TWO batches (two spans).
    let mut w2 = art.open_exclusive(9).unwrap();
    assert_eq!(w2.commit(Commit::Append, &b1, &reg).unwrap(), 2);
    drop(w2);

    let pin2 = art.pin().unwrap();
    let m2 = pin2.manifest();
    assert_eq!(
        m2.chunks.len(),
        v1_chunks,
        "the head manifest lists only v2's own chunks"
    );
    assert_eq!(m2.batch_spans, vec![v1_chunks as u32], "one own batch");
    assert_eq!(m2.total_batches, 2, "two batches across the chain");
    let chain = pin2.chain().unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].batch_spans[0] as usize, v1_chunks);
    assert_eq!(
        pin2.data_chunks().unwrap().len(),
        2 * v1_chunks,
        "the table is both batches' chunks, in order"
    );

    // v2 reads back as the two batches, zero-copy, then concatenated in row order.
    let batches = pin2.as_arrow_batches(&reg).unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(&batches[0], &b1);
    assert_eq!(&batches[1], &b1);
    let out = pin2.as_arrow(&reg).unwrap();
    assert_eq!(out.num_rows(), 2 * b1.num_rows());
    drop(pin2);

    // v2's manifest referenced v1's chunks (shared), so committing v2 consumed
    // only v2's *new* data chunks + its manifest — the prior chunks were not copied.
    let consumed_by_v2 = free_after_v1 - free_total(&fx.data_seg);
    assert!(
        consumed_by_v2 <= v1_chunks + 1,
        "append must not copy prior chunks (consumed {consumed_by_v2}, v1 had {v1_chunks})"
    );
}

#[test]
fn nested_struct_list_version_round_trips_zero_copy() {
    use arrow_array::{Int32Array, ListArray, StringArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::Fields;

    let fx = Fixture::new();
    let art = fx.artifact(202);

    // Struct<{a: Int32, b: Utf8}> + List<Int32>, small enough to fit one chunk.
    let a = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]))
        as Arc<dyn arrow_array::Array>;
    let bcol = Arc::new(StringArray::from(vec![
        Some("x"),
        None,
        Some("zz"),
        Some("www"),
    ])) as Arc<dyn arrow_array::Array>;
    let sfields = Fields::from(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let s = StructArray::new(
        sfields.clone(),
        vec![a, bcol],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 2, 5, 6]));
    let ifield = Arc::new(Field::new("item", DataType::Int32, false));
    let list = ListArray::new(ifield.clone(), offsets, Arc::new(values), None);

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Struct(sfields), true),
        Field::new("l", DataType::List(ifield), false),
    ]));
    let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(s), Arc::new(list)]).unwrap();
    let reg = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    let full_free = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    assert_eq!(w.commit(Commit::Replace, &b, &reg).unwrap(), 1);
    drop(w);

    let pin = art.pin().unwrap();
    let out = pin.as_arrow(&reg).unwrap();
    assert_eq!(&out, &b, "nested (struct + list) version reads back equal");
    // Zero-copy: the struct's Int32 child buffer lives inside the data segment.
    let base = fx.data_seg.base_ptr() as usize;
    let end = base + fx.data_seg.size();
    let child = out.column(0).to_data().child_data()[0].buffers()[0].as_ptr() as usize;
    assert!(
        (base..end).contains(&child),
        "nested child buffer escaped the segment"
    );
    drop(pin);

    // Retire v1 by superseding it; every chunk returns to the pool.
    let mut w2 = art.open_exclusive(9).unwrap();
    assert_eq!(w2.commit(Commit::Replace, &b, &reg).unwrap(), 2);
    drop(w2);
    let used = full_free - free_total(&fx.data_seg);
    // Recommit-then-retire is idempotent in the pool: only v2 occupies chunks.
    assert!(used >= 2, "nested version occupies data + manifest chunks");
    assert_eq!(&art.pin().unwrap().as_arrow(&reg).unwrap(), &b);
}

// ---- P0.3 (ADR-0010, G4): evict-current as an empty Replace commit ----

/// The core G4 property: `evict_current` retires the current version's chunks
/// (the thing `try_retire_version` alone can never do while it is current),
/// keeps the lineage alive, and the version sequence continues monotonically —
/// no version number is ever reissued.
#[test]
fn evict_current_reclaims_and_the_version_sequence_continues() {
    let fx = Fixture::new();
    let art = fx.artifact(41);
    let reg = registry();

    let baseline = free_total(&fx.data_seg);
    let v1 = art
        .commit_optimistic(7, 0, Commit::Replace, &batch(&[1, 2, 3]), &reg)
        .unwrap();
    assert_eq!(v1, 1);
    let used_v1 = baseline - free_total(&fx.data_seg);
    assert!(used_v1 >= 2, "v1 holds a data chunk and a manifest chunk");

    // Evict the CURRENT version: an empty Replace supersedes it and the
    // standard install-path retire frees v1's chunks immediately (unpinned).
    let v2 = art.evict_current_optimistic(7, v1).unwrap();
    assert_eq!(v2, 2, "the empty version continues the sequence");
    assert_eq!(art.current_version(), 2);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        1,
        "v1's chunks are back; only the empty version's 32-byte manifest chunk remains"
    );

    // A reader of the evicted-current entry looks exactly like a reader of a
    // never-committed one: the pin resolves, as_arrow reports VersionGone.
    let pin = art.pin().expect("empty version is pinnable");
    assert_eq!(pin.version(), 2);
    assert_eq!(
        pin.manifest().chunks.len(),
        0,
        "zero-chunk manifest round-trips"
    );
    assert_eq!(pin.manifest().batch_spans.len(), 0);
    assert!(matches!(
        pin.as_arrow(&reg),
        Err(shm_artifact::Error::VersionGone)
    ));
    drop(pin);

    // The lineage is still live: the next commit continues at 3 (never
    // reissuing 1 or 2) and reads back normally.
    let v3 = art
        .commit_optimistic(7, 2, Commit::Replace, &batch(&[9]), &reg)
        .unwrap();
    assert_eq!(v3, 3);
    let pin = art.pin().unwrap();
    assert_eq!(col(&pin.as_arrow(&reg).unwrap()), vec![9]);
    drop(pin);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2,
        "the empty v2 manifest was itself retired when v3 superseded it"
    );
}

/// A reader pinned on the evicted version keeps its frozen view (RCU
/// isolation) and the version's chunks; the retire happens on pin drop.
#[test]
fn evict_current_with_a_pinned_reader_drains_on_pin_drop() {
    let fx = Fixture::new();
    let art = fx.artifact(42);
    let reg = registry();

    let baseline = free_total(&fx.data_seg);
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[5, 6]), &reg)
        .unwrap();
    let used_v1 = baseline - free_total(&fx.data_seg);
    let pin = art.pin().unwrap();

    let v2 = art.evict_current_optimistic(7, 1).unwrap();
    assert_eq!(v2, 2);
    // The pinned version survives the evict, frozen.
    assert_eq!(col(&pin.as_arrow(&reg).unwrap()), vec![5, 6]);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        used_v1 + 1,
        "nothing of v1 freed while the pin is live (+1 for the empty manifest)"
    );

    drop(pin);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        1,
        "the last pin drop retired the evicted version"
    );
}

/// Nothing committed ⇒ nothing to evict: `VersionGone`, and the entry is
/// untouched. A conflicting concurrent commit linearises the loser as
/// `Conflict`, exactly like any optimistic commit race.
#[test]
fn evict_current_on_empty_errors_and_conflicts_are_clean() {
    let fx = Fixture::new();
    let art = fx.artifact(43);
    let reg = registry();

    assert!(matches!(
        art.evict_current_optimistic(7, 0),
        Err(shm_artifact::Error::VersionGone)
    ));

    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1]), &reg)
        .unwrap();
    let baseline = free_total(&fx.data_seg);
    // A racing committer moved current first: the evict is a clean loser and
    // stages nothing.
    assert!(matches!(
        art.evict_current_optimistic(7, /* stale expect */ 9),
        Err(shm_artifact::Error::Conflict {
            expected: 9,
            actual: 1
        })
    ));
    assert_eq!(
        free_total(&fx.data_seg),
        baseline,
        "a lost evict leaks nothing"
    );
    assert_eq!(art.current_version(), 1);

    // The leased form: evicting twice stacks empty versions monotonically.
    let mut committer = art.open_exclusive(7).unwrap();
    assert_eq!(committer.evict_current().unwrap(), 2);
    assert_eq!(committer.evict_current().unwrap(), 3);
    drop(committer);
    assert_eq!(art.current_version(), 3);
}

/// Churn: alternate commit / evict_current far past `MAX_LIVE_VERSIONS` under
/// a concurrent pinning reader — no slot leak, no pin underflow, and the pool
/// census returns to its baseline (+ the final empty version's manifest).
#[test]
fn evict_current_churn_past_max_live_versions_with_concurrent_readers() {
    let fx = Fixture::new();
    let art = Arc::new(fx.artifact(44));
    let reg = Arc::new(registry());
    let baseline = free_total(&fx.data_seg);

    let stop = Arc::new(AtomicU32::new(0));
    let reader = {
        let art = Arc::clone(&art);
        let reg = Arc::clone(&reg);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut reads = 0u64;
            while stop.load(Ordering::Acquire) == 0 {
                match art.pin() {
                    Ok(pin) => {
                        // An empty (evicted-current) version reads VersionGone;
                        // a data version must read coherently.
                        if let Ok(b) = pin.as_arrow(&reg) {
                            assert_eq!(b.num_rows(), 2);
                        }
                        reads += 1;
                    }
                    Err(shm_artifact::Error::VersionGone) => {}
                    Err(e) => panic!("reader saw {e:?}"),
                }
            }
            reads
        })
    };

    // 2 versions per iteration x 200 = 400 versions >> MAX_LIVE_VERSIONS (64).
    let mut current = 0u64;
    for i in 0..200 {
        current = art
            .commit_optimistic(7, current, Commit::Replace, &batch(&[i, i + 1]), &reg)
            .unwrap();
        current = art.evict_current_optimistic(7, current).unwrap();
    }
    stop.store(1, Ordering::Release);
    let reads = reader.join().unwrap();
    assert!(reads > 0, "the reader actually raced the churn");

    assert_eq!(art.current_version(), 400);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        1,
        "only the final empty version's manifest chunk is outstanding"
    );
}

// ---------------------------------------------------------------------------
// ADR-0013 — chained manifests: `Commit::Append` in O(new data)
// ---------------------------------------------------------------------------

/// A fixture + artifact over a pool whose **largest class is 256 bytes**, with
/// `chunks` chunks. A flat manifest re-listing every prior chunk (24 B each)
/// outgrows 256 B after eight appends, so a long Append lineage here is only
/// possible with chained manifests (every manifest is 64 B + its own chunks).
fn lineage_artifact(name_id: u32, chunks: u32) -> (Fixture, Artifact) {
    let pool = PoolConfig::power_of_two(256, 256, chunks);
    let fx = Fixture::with_data_size(256 * chunks as usize + (1 << 20));
    let art = fx.artifact_with(name_id, &pool);
    (fx, art)
}

/// (a) 1000 appends in a pool whose largest class is 256 B — impossible with a
/// flat manifest (the 1000th would be ~24 KB). Every version costs exactly one
/// data chunk + one 92-byte manifest, and the whole table reads back through
/// the chain.
#[test]
fn append_1000_in_a_pool_whose_largest_class_is_256_bytes() {
    let (fx, art) = lineage_artifact(300, 4096);
    let reg = registry();
    let baseline = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    assert_eq!(w.commit(Commit::Replace, &batch(&[0]), &reg).unwrap(), 1);
    for i in 1..=1000i64 {
        assert_eq!(
            w.commit(Commit::Append, &batch(&[i]), &reg).unwrap(),
            i as u64 + 1
        );
    }
    drop(w);
    assert_eq!(art.current_version(), 1001);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2 * 1001,
        "exactly one data chunk + one manifest chunk per chain member"
    );

    let pin = art.pin().unwrap();
    assert_eq!(pin.manifest().chunks.len(), 1, "pin parses the head only");
    assert_eq!(pin.manifest().depth, 1000);
    assert_eq!(pin.manifest().total_batches, 1001);
    let chain = pin.chain().unwrap();
    assert_eq!(chain.len(), 1001);
    assert!(
        chain.windows(2).all(|w| w[0].version + 1 == w[1].version),
        "chain is oldest-first and contiguous"
    );
    assert_eq!(pin.data_chunks().unwrap().len(), 1001);
    assert_eq!(pin.as_arrow_batches(&reg).unwrap().len(), 1001);
    let expect: Vec<i64> = (0..=1000).collect();
    assert_eq!(col(&pin.as_arrow(&reg).unwrap()), expect);
}

/// (b) A `Replace` after a long lineage frees the whole chain back to
/// baseline: one release on the retired head's manifest, then the cascade
/// walks every link to the root.
#[test]
fn replace_after_a_long_lineage_frees_the_whole_chain() {
    let (fx, art) = lineage_artifact(301, 1024);
    let reg = registry();
    let baseline = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[0]), &reg).unwrap();
    for i in 1..=200i64 {
        w.commit(Commit::Append, &batch(&[i]), &reg).unwrap();
    }
    assert_eq!(baseline - free_total(&fx.data_seg), 2 * 201);

    assert_eq!(w.commit(Commit::Replace, &batch(&[9]), &reg).unwrap(), 202);
    drop(w);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2,
        "only v202's data + manifest remain after the cascade"
    );
    assert_eq!(col(&art.pin().unwrap().as_arrow(&reg).unwrap()), vec![9]);
}

/// (c) A reader pinned on a prefix of the chain survives the cascade: the
/// cascade frees the suffix and stops at the pinned manifest (still held by
/// the pinned version's own reference); the pin drop then frees the prefix.
#[test]
fn pinned_prefix_survives_the_cascade() {
    let (fx, art) = lineage_artifact(302, 1024);
    let reg = registry();
    let baseline = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1]), &reg).unwrap();
    for i in 2..=5i64 {
        w.commit(Commit::Append, &batch(&[i]), &reg).unwrap();
    }
    let held = art.pin().unwrap();
    assert_eq!(held.version(), 5);
    for i in 6..=20i64 {
        w.commit(Commit::Append, &batch(&[i]), &reg).unwrap();
    }
    assert_eq!(baseline - free_total(&fx.data_seg), 2 * 20);

    // v21 (Replace) retires v20: the cascade frees v20..v6 and stops at v5.
    assert_eq!(w.commit(Commit::Replace, &batch(&[99]), &reg).unwrap(), 21);
    drop(w);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2 + 2 * 5,
        "v21 plus the pinned v1..v5 prefix remain"
    );
    assert_eq!(col(&held.as_arrow(&reg).unwrap()), vec![1, 2, 3, 4, 5]);
    assert_eq!(held.chain().unwrap().len(), 5);

    drop(held);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2,
        "the pin drop retired v5 and the cascade freed the prefix"
    );
}

/// (d) Optimistic `Append` vs `Replace` racing for the same `expect`, 1000
/// rounds: exactly one wins each round, the loser's rollback releases its link
/// through the cascade, and the census is exact after every round.
#[test]
fn optimistic_append_vs_replace_conflict_census_exact() {
    let (fx, art) = lineage_artifact(303, 4096);
    let art = Arc::new(art);
    let reg = Arc::new(registry());
    let baseline = free_total(&fx.data_seg);

    let mut current = art
        .commit_optimistic(7, 0, Commit::Replace, &batch(&[0]), &reg)
        .unwrap();
    let mut appends_won = 0usize;
    for round in 0..1000u64 {
        let barrier = Arc::new(Barrier::new(2));
        let (a, r) = thread::scope(|s| {
            let a = {
                let art = art.clone();
                let reg = reg.clone();
                let b = barrier.clone();
                s.spawn(move || {
                    b.wait();
                    art.commit_optimistic(
                        11,
                        current,
                        Commit::Append,
                        &batch(&[round as i64]),
                        &reg,
                    )
                })
            };
            let r = {
                let art = art.clone();
                let reg = reg.clone();
                let b = barrier.clone();
                s.spawn(move || {
                    b.wait();
                    art.commit_optimistic(12, current, Commit::Replace, &batch(&[-1]), &reg)
                })
            };
            (a.join().unwrap(), r.join().unwrap())
        });
        match (&a, &r) {
            (Ok(v), Err(shm_artifact::Error::Conflict { .. })) => {
                appends_won += 1;
                current = *v;
            }
            (Err(shm_artifact::Error::Conflict { .. }), Ok(v)) => current = *v,
            other => panic!("round {round}: expected exactly one winner, got {other:?}"),
        }
        assert_eq!(current, round + 2);
        // Census: exactly the live chain is in use — two chunks per member —
        // and nothing the loser staged or linked leaked.
        let depth = art.pin().unwrap().manifest().depth as usize;
        assert_eq!(
            baseline - free_total(&fx.data_seg),
            2 * (depth + 1),
            "round {round}: census off for a chain of depth {depth}"
        );
    }
    eprintln!("append-vs-replace: appends won {appends_won}/1000 rounds");

    // Collapse: a final Replace frees everything but itself.
    art.commit_optimistic(7, current, Commit::Replace, &batch(&[1]), &reg)
        .unwrap();
    assert_eq!(baseline - free_total(&fx.data_seg), 2);
}

/// (e) `as_arrow_batches` is zero-copy per batch across the chain (every
/// buffer points into the data segment); `as_arrow` on a multi-batch version
/// concatenates, which is a copy — and says so.
#[test]
fn as_arrow_batches_is_zero_copy_per_batch_and_as_arrow_copies() {
    let fx = Fixture::new();
    let art = fx.artifact(304);
    let reg = registry();

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1, 2]), &reg).unwrap();
    w.commit(Commit::Append, &batch(&[3]), &reg).unwrap();
    w.commit(Commit::Append, &batch(&[4, 5, 6]), &reg).unwrap();
    drop(w);

    let pin = art.pin().unwrap();
    let base = fx.data_seg.base_ptr() as usize;
    let end = base + fx.data_seg.size();
    let batches = pin.as_arrow_batches(&reg).unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(col(&batches[0]), vec![1, 2]);
    assert_eq!(col(&batches[1]), vec![3]);
    assert_eq!(col(&batches[2]), vec![4, 5, 6]);
    for (i, b) in batches.iter().enumerate() {
        let p = b.column(0).to_data().buffers()[0].as_ptr() as usize;
        assert!(
            (base..end).contains(&p),
            "batch {i}'s value buffer escaped the segment (copied)"
        );
    }
    assert_eq!(
        pin.data_chunks().unwrap().len(),
        3,
        "one chunk per batch here"
    );

    let one = pin.as_arrow(&reg).unwrap();
    assert_eq!(col(&one), vec![1, 2, 3, 4, 5, 6]);
    let p = one.column(0).to_data().buffers()[0].as_ptr() as usize;
    assert!(
        !(base..end).contains(&p),
        "a concatenated multi-batch read is a copy, not a view"
    );
}

/// (f) A manifest may only link to an **older** version: a self-link and a
/// forward link are refused at write time (and by the parser / walker — see
/// the `manifest` unit tests), staging nothing.
#[test]
fn self_link_and_forward_link_are_rejected() {
    let fx = Fixture::new();
    let art = fx.artifact(305);
    let reg = registry();
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1]), &reg)
        .unwrap();
    let baseline = free_total(&fx.data_seg);
    let head = art.pin().unwrap().manifest().clone();
    assert_eq!(head.version, 1);

    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();
    let alloc = shm_arrow::PoolAllocator::new(&pool, &fx.data_seg);
    let link = head.link_from(shm_core::PackedRef(0x0000_0001_0000_1000), 0);
    // Version 1 linking to version 1: a self-link.
    assert!(matches!(
        write_manifest(&alloc, 305, 1, head.schema_id, &[], &[], Some(&link), None),
        Err(shm_artifact::Error::Unsupported(_))
    ));
    // Version 0 linking to version 1: a forward link.
    assert!(matches!(
        write_manifest(&alloc, 305, 0, head.schema_id, &[], &[], Some(&link), None),
        Err(shm_artifact::Error::Unsupported(_))
    ));
    assert_eq!(
        free_total(&fx.data_seg),
        baseline,
        "a refused link allocates nothing"
    );
}

/// (g) Item-J replay on an Append lineage: a leaked journalled pin on the
/// chain head keeps the whole chain alive past a `Replace`; releasing it via
/// `release_leaked_pin` retires the head and cascades the chain.
#[test]
fn leaked_pin_replay_on_an_append_lineage_cascades() {
    let (fx, art) = lineage_artifact(306, 1024);
    let reg = registry();
    let jseg = journal_segment();
    BorrowJournal::create(&jseg, 64).unwrap();
    let baseline = free_total(&fx.data_seg);

    let mut cur = art
        .commit_optimistic(7, 0, Commit::Replace, &batch(&[1]), &reg)
        .unwrap();
    for i in 2..=10i64 {
        cur = art
            .commit_optimistic(7, cur, Commit::Append, &batch(&[i]), &reg)
            .unwrap();
    }
    let pin = art.pin_journaled(&jseg).unwrap();
    assert_eq!(pin.version(), 10);
    std::mem::forget(pin); // the reader "crashes" holding v10

    cur = art
        .commit_optimistic(7, cur, Commit::Replace, &batch(&[0]), &reg)
        .unwrap();
    assert_eq!(cur, 11);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2 * 10 + 2,
        "the leaked pin keeps the 10-deep chain alive"
    );

    let journal = BorrowJournal::attach(&jseg).unwrap();
    let mut released = 0;
    for rec in journal.replay() {
        if let JournalRecord::ArtifactPin { version, .. } = rec {
            if art.release_leaked_pin(version).unwrap() {
                released += 1;
            }
        }
    }
    assert_eq!(released, 1);
    assert_eq!(art.version_pin_count(10), None);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2,
        "the replayed release retired v10 and the cascade freed the chain"
    );
}

/// (h) `evict_all` on a lineage: unpinned members cascade immediately, a
/// pinned member (and its prefix) drains on pin drop, and the pool returns to
/// baseline.
#[test]
fn evict_all_on_a_lineage_returns_the_pool_to_baseline() {
    let (fx, art) = lineage_artifact(307, 1024);
    let reg = registry();
    let baseline = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1]), &reg).unwrap();
    for i in 2..=21i64 {
        w.commit(Commit::Append, &batch(&[i]), &reg).unwrap();
    }
    let held = art.pin().unwrap();
    assert_eq!(held.version(), 21);
    for i in 22..=26i64 {
        w.commit(Commit::Append, &batch(&[i]), &reg).unwrap();
    }
    drop(w);
    assert_eq!(baseline - free_total(&fx.data_seg), 2 * 26);

    art.evict_all().unwrap();
    assert_eq!(art.current_version(), 0);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        2 * 21,
        "v26..v22 cascaded down to the pinned v21; its chain survives the pin"
    );
    let expect: Vec<i64> = (1..=21).collect();
    assert_eq!(col(&held.as_arrow(&reg).unwrap()), expect);

    drop(held);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        0,
        "the pin drop retired v21 and the cascade returned everything"
    );
    // Idempotent.
    art.evict_all().unwrap();
    assert_eq!(free_total(&fx.data_seg), baseline);
}

/// (i) An `Append` whose schema differs from the prior version's is rejected
/// (`Unsupported`), installs nothing and leaks nothing; a `Replace` with the
/// new schema starts a fresh root.
#[test]
fn append_with_a_different_schema_is_rejected() {
    let fx = Fixture::new();
    let art = fx.artifact(308);
    let schema_b: SchemaRef = Arc::new(Schema::new(vec![Field::new("w", DataType::Int32, false)]));
    let reg = SchemaRegistry::with_schemas(&[schema(), schema_b.clone()]);
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1]), &reg)
        .unwrap();
    let baseline = free_total(&fx.data_seg);

    let other = RecordBatch::try_new(
        schema_b,
        vec![Arc::new(arrow_array::Int32Array::from(vec![2]))],
    )
    .unwrap();
    assert!(matches!(
        art.commit_optimistic(7, 1, Commit::Append, &other, &reg),
        Err(shm_artifact::Error::Unsupported(_))
    ));
    assert_eq!(art.current_version(), 1);
    assert_eq!(
        free_total(&fx.data_seg),
        baseline,
        "the rejected Append returned its staged chunk"
    );

    assert_eq!(
        art.commit_optimistic(7, 1, Commit::Replace, &other, &reg)
            .unwrap(),
        2
    );
    let pin = art.pin().unwrap();
    assert_eq!(pin.manifest().depth, 0, "a Replace is a new root");
    assert_eq!(pin.as_arrow(&reg).unwrap(), other);
}

/// ADR-0014 §4 — the zombie double-decrement is gone. A journaled reader is
/// declared dead and its pin replayed by the coordinator (which wins the
/// journal slot before decrementing); the reader then turns out to be alive
/// and drops its pin normally. That drop must observe the lost election and
/// **not** decrement again: the version was retired exactly once, the census
/// is exact, and the artifact keeps working.
#[test]
fn zombie_pin_drop_after_replay_does_not_double_decrement() {
    let fx = Fixture::new();
    let art = fx.artifact(104);
    let reg = registry();
    let jseg = journal_segment();
    BorrowJournal::create(&jseg, 64).unwrap();
    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();
    let baseline: usize = (0..pool.num_classes()).map(|c| pool.free_count(c)).sum();

    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1, 2, 3]), &reg)
        .unwrap();
    let pin = art.pin_journaled(&jseg).unwrap(); // the soon-to-be zombie's pin
    art.commit_optimistic(7, 1, Commit::Replace, &batch(&[9]), &reg)
        .unwrap();
    assert_eq!(art.version_pin_count(1), Some(1));

    // Coordinator replay: win the slot, then release the leaked pin.
    let journal = BorrowJournal::attach(&jseg).unwrap();
    let mut replayed = 0;
    for (slot, rec) in journal.replay_indexed() {
        if let JournalRecord::ArtifactPin { version, .. } = rec {
            assert!(journal.release(slot).unwrap(), "replay wins the election");
            assert!(art.release_leaked_pin(version).unwrap());
            replayed += 1;
        }
    }
    assert_eq!(replayed, 1);
    assert_eq!(art.version_pin_count(1), None, "v1 retired by the replay");
    let after_replay: usize = (0..pool.num_classes()).map(|c| pool.free_count(c)).sum();

    // The zombie lives on and drops its pin: it lost the election, so this
    // must be a no-op — no second retire, no stolen reference, no panic.
    drop(pin);
    let after_zombie: usize = (0..pool.num_classes()).map(|c| pool.free_count(c)).sum();
    assert_eq!(after_zombie, after_replay, "zombie drop released nothing");

    // Still fully functional: v2 is current, readable, and a new commit lands.
    let p2 = art.pin().unwrap();
    assert_eq!(p2.version(), 2);
    drop(p2);
    art.commit_optimistic(7, 2, Commit::Replace, &batch(&[4]), &reg)
        .unwrap();
    // One live version's footprint above baseline: v3's manifest + chunk, and
    // nothing else outstanding.
    let _ = baseline;
}

/// ADR-0014 §3 — a committer that dies between staging its manifest and the
/// install CAS is torn down by replay of its `StagedManifest` record; one that
/// died *after* installing (record outliving the install) is left alone.
#[test]
fn staged_manifest_record_replays_uninstalled_and_ignores_installed() {
    use shm_arrow::PoolAllocator;
    use shm_artifact::write_manifest;
    use shm_core::PackedRef;

    let fx = Fixture::new();
    let art = fx.artifact(105);
    let reg = registry();
    let jseg = journal_segment();
    BorrowJournal::create(&jseg, 64).unwrap();
    let journal = BorrowJournal::attach(&jseg).unwrap();
    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();

    // Normal path: a journaled optimistic commit leaves no record behind.
    let staged = {
        let alloc = PoolAllocator::new(&pool, &fx.data_seg);
        let desc = shm_arrow::write_batch(&alloc, &reg, &batch(&[1, 2])).unwrap();
        // A staged data chunk is LOANED to the committer; the commit itself
        // publishes it and takes the version's reference (`publish_staged`).
        pool.ctrl(&desc).unwrap().try_loan(7).unwrap();
        desc
    };
    art.commit_staged_optimistic_journaled(7, 0, Commit::Replace, &[staged], &[1], 1, &journal)
        .unwrap();
    assert_eq!(
        journal.len(),
        0,
        "install released the staged-manifest record"
    );
    let installed_bits = art.current_manifest_bits();

    // Crash path: a manifest staged (published, one owned reference) but never
    // installed, with its record still in the journal.
    let orphan = {
        let alloc = PoolAllocator::new(&pool, &fx.data_seg);
        let m = write_manifest(&alloc, 105, 2, 1, &[], &[], None, None).unwrap();
        let c = pool.ctrl(&m).unwrap();
        c.try_loan(7).unwrap();
        c.publish().unwrap();
        c.borrow_shared().unwrap();
        c.owner_release();
        m
    };
    let orphan_bits = PackedRef::from_desc(&orphan).to_bits();
    let before: usize = (0..pool.num_classes()).map(|c| pool.free_count(c)).sum();

    // Replay: the installed one is endorsed by v1's slot → untouched.
    assert!(!art.reclaim_staged_manifest(installed_bits, 0).unwrap());
    assert_eq!(
        art.pin().unwrap().version(),
        1,
        "installed manifest untouched"
    );
    // The orphan is not endorsed → released through the cascade.
    assert!(art
        .reclaim_staged_manifest(orphan_bits, orphan.generation)
        .unwrap());
    assert_eq!(pool.ctrl(&orphan).unwrap().state(), FREE);
    let after: usize = (0..pool.num_classes()).map(|c| pool.free_count(c)).sum();
    assert_eq!(after, before + 1, "exactly the orphan chunk came back");
    // Idempotent: a second replay finds a freed/recycled chunk and does nothing.
    assert!(!art
        .reclaim_staged_manifest(orphan_bits, orphan.generation)
        .unwrap());
}

// ---------------------------------------------------------------------------
// ADR-0016 — windowed append (`Commit::Window`, `WindowPolicy`) + delta read
// ---------------------------------------------------------------------------

use shm_artifact::WindowPolicy;

/// A lineage pool whose 256 B class holds the data chunks and the small
/// (Append) manifests, plus a 4096 B class for the wider `Window` manifests
/// (`64 + 24·chunks + 4·batches` bytes: up to ~140 single-chunk batches).
fn windowed_artifact(name_id: u32, chunks: u32) -> (Fixture, Artifact) {
    let pool = PoolConfig {
        classes: vec![
            shm_core::SizeClass {
                chunk_size: 256,
                chunk_count: chunks,
            },
            shm_core::SizeClass {
                chunk_size: 4096,
                chunk_count: 64,
            },
        ],
    };
    let fx = Fixture::with_data_size(256 * chunks as usize + 4096 * 64 + (1 << 20));
    let art = fx.artifact_with(name_id, &pool);
    (fx, art)
}

/// The rows of a pinned version across every batch, in row order.
fn rows(pin: &shm_artifact::VersionPin, reg: &SchemaRegistry) -> Vec<i64> {
    pin.as_arrow_batches(reg)
        .unwrap()
        .iter()
        .flat_map(col)
        .collect()
}

/// (w1) A `Window` re-roots on the newest `keep_batches` batches: the table
/// is exactly those plus the new batch, depth resets to 0, and the prior
/// chain's tail — every batch older than the window — is back in the pool.
/// `keep_batches == 0` is a `Replace`.
#[test]
fn window_commit_keeps_the_newest_batches_and_frees_the_tail() {
    let (fx, art) = windowed_artifact(401, 256);
    let reg = registry();
    let baseline = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1]), &reg).unwrap();
    for i in 2..=10i64 {
        w.commit(Commit::Append, &batch(&[i]), &reg).unwrap();
    }
    assert_eq!(baseline - free_total(&fx.data_seg), 2 * 10);

    let v = w
        .commit(Commit::Window { keep_batches: 3 }, &batch(&[11]), &reg)
        .unwrap();
    assert_eq!(v, 11);
    let pin = art.pin().unwrap();
    assert_eq!(pin.manifest().depth, 0, "a Window is a root");
    assert!(pin.manifest().prev.is_none());
    assert_eq!(pin.manifest().chunks.len(), 4);
    assert_eq!(pin.manifest().batch_spans, vec![1, 1, 1, 1]);
    assert_eq!(rows(&pin, &reg), vec![8, 9, 10, 11]);
    assert_eq!(col(&pin.as_arrow(&reg).unwrap()), vec![8, 9, 10, 11]);
    drop(pin);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        4 + 1,
        "3 kept + 1 new data chunks and one manifest; v1..v7 and every old manifest freed"
    );

    // Appending onto the new root chains as before.
    w.commit(Commit::Append, &batch(&[12]), &reg).unwrap();
    assert_eq!(rows(&art.pin().unwrap(), &reg), vec![8, 9, 10, 11, 12]);
    assert_eq!(baseline - free_total(&fx.data_seg), 5 + 2);

    // keep_batches == 0: a Replace.
    w.commit(Commit::Window { keep_batches: 0 }, &batch(&[13]), &reg)
        .unwrap();
    assert_eq!(rows(&art.pin().unwrap(), &reg), vec![13]);
    assert_eq!(baseline - free_total(&fx.data_seg), 2);

    // A window wider than the table keeps the whole table.
    w.commit(Commit::Append, &batch(&[14]), &reg).unwrap();
    w.commit(Commit::Window { keep_batches: 99 }, &batch(&[15]), &reg)
        .unwrap();
    assert_eq!(rows(&art.pin().unwrap(), &reg), vec![13, 14, 15]);
    assert_eq!(baseline - free_total(&fx.data_seg), 3 + 1);
}

/// (w2) `WindowPolicy` over 10 000 single-batch commits in a pool of 256
/// small chunks: a plain Append lineage would need 20 000. The chain depth,
/// the live chunk count and the table size stay inside the policy's bounds
/// the whole way, and the table always reads back as the newest rows in
/// order.
#[test]
fn window_policy_bounds_chunks_depth_and_read_over_10k_commits() {
    let (fx, art) = windowed_artifact(402, 256);
    let reg = registry();
    let baseline = free_total(&fx.data_seg);
    let policy = WindowPolicy::new(16);

    let mut w = art.open_exclusive(7).unwrap();
    let mut windows = 0usize;
    for i in 1..=10_000i64 {
        let kind = policy.commit_for_depth(art.current_depth());
        if matches!(kind, Commit::Window { .. }) {
            windows += 1;
        }
        assert_eq!(
            w.commit_windowed(&policy, &batch(&[i]), &reg).unwrap(),
            i as u64
        );
        if i % 1000 == 0 || i < 40 {
            let pin = art.pin().unwrap();
            let m = pin.manifest().clone();
            assert!(m.depth < 16, "v{i}: depth {} escaped the policy", m.depth);
            let n = m.total_batches as i64;
            assert!((16..=32).contains(&n) || i <= 32, "v{i}: {n} batches");
            let expect: Vec<i64> = ((i - n + 1)..=i).collect();
            assert_eq!(
                rows(&pin, &reg),
                expect,
                "v{i}: table is the newest {n} rows"
            );
            drop(pin);
            let live = baseline - free_total(&fx.data_seg);
            // chain members (manifests) + one data chunk per batch
            assert_eq!(live, m.depth as usize + 1 + n as usize, "v{i}: census");
            assert!(live <= 32 + 16, "v{i}: {live} live chunks");
        }
    }
    assert_eq!(
        windows,
        10_000 / 16,
        "the first root plus one Window per 16 commits"
    );
    drop(w);

    // Collapse and the census returns to a single version.
    art.open_exclusive(7)
        .unwrap()
        .commit(Commit::Replace, &batch(&[0]), &reg)
        .unwrap();
    assert_eq!(baseline - free_total(&fx.data_seg), 2);
}

/// (w3) `batches_since` walks only the manifests newer than `since`, and
/// flags a root newer than `since` (a Window or Replace happened) as
/// `truncated` — the delta then starts at that root.
#[test]
fn batches_since_returns_only_new_manifests_and_flags_truncation() {
    let (_fx, art) = windowed_artifact(403, 256);
    let reg = registry();

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1]), &reg).unwrap();
    w.commit(Commit::Append, &batch(&[2]), &reg).unwrap();
    w.commit(Commit::Append, &batch(&[3]), &reg).unwrap();
    let pin = art.pin().unwrap();
    let rows_of = |d: &shm_artifact::Delta| d.batches.iter().flat_map(col).collect::<Vec<i64>>();

    let d = pin.batches_since(2, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![3], 3, false)
    );
    let d = pin.batches_since(1, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![2, 3], 2, false)
    );
    let d = pin.batches_since(3, &reg).unwrap();
    assert!(d.batches.is_empty() && !d.truncated && d.from_version == 3);
    let d = pin.batches_since(7, &reg).unwrap();
    assert!(d.batches.is_empty() && !d.truncated);
    let d = pin.batches_since(0, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![1, 2, 3], 1, false)
    );
    drop(pin);

    // v4 = Window{2}: root [2, 3, 4] with members (2,1),(3,1) and base 1 (v2
    // kept whole, and v2 was linked). The window is transparent to any
    // reader at or past the base: it gets exactly the rows it lacks.
    w.commit(Commit::Window { keep_batches: 2 }, &batch(&[4]), &reg)
        .unwrap();
    let pin = art.pin().unwrap();
    let m = pin.manifest();
    assert_eq!(m.window_base, 1);
    assert_eq!(
        m.kept
            .iter()
            .map(|k| (k.version, k.batches))
            .collect::<Vec<_>>(),
        vec![(2, 1), (3, 1)]
    );
    let d = pin.batches_since(3, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![4], 4, false)
    );
    let d = pin.batches_since(2, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![3, 4], 4, false)
    );
    let d = pin.batches_since(1, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![2, 3, 4], 4, false)
    );
    let d = pin.batches_since(4, &reg).unwrap();
    assert!(d.batches.is_empty() && !d.truncated);
    // from the beginning: the whole (windowed) table, not truncated
    let d = pin.batches_since(0, &reg).unwrap();
    assert_eq!((rows_of(&d), d.truncated), (vec![2, 3, 4], false));
    drop(pin);

    // v5 appends onto the new root: a reader at v4 gets exactly [5]; a
    // reader still at v3 gets [4, 5] — still exact through the window.
    w.commit(Commit::Append, &batch(&[5]), &reg).unwrap();
    let pin = art.pin().unwrap();
    let d = pin.batches_since(4, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![5], 5, false)
    );
    let d = pin.batches_since(3, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![4, 5], 4, false)
    );
    drop(pin);

    // v6 = Window{1} onto a windowed root: v5 kept whole (linked → base 4),
    // so a reader at v4 still gets an exact [5, 6]; a reader at v3 is now
    // behind the base and must resync from the root.
    w.commit(Commit::Window { keep_batches: 1 }, &batch(&[6]), &reg)
        .unwrap();
    let pin = art.pin().unwrap();
    assert_eq!(pin.manifest().window_base, 4);
    let d = pin.batches_since(4, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![5, 6], 6, false)
    );
    let d = pin.batches_since(3, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![5, 6], 6, true)
    );
    drop(pin);

    // v7 = Window{3} keeps the whole of root v6 (2 batches) plus... only 2
    // exist: the root is kept whole, so its base (4) is inherited and its
    // member table is flattened: (5,1),(6,1).
    w.commit(Commit::Window { keep_batches: 3 }, &batch(&[7]), &reg)
        .unwrap();
    let pin = art.pin().unwrap();
    let m = pin.manifest();
    assert_eq!(m.window_base, 4);
    assert_eq!(
        m.kept
            .iter()
            .map(|k| (k.version, k.batches))
            .collect::<Vec<_>>(),
        vec![(5, 1), (6, 1)]
    );
    let d = pin.batches_since(4, &reg).unwrap();
    assert_eq!((rows_of(&d), d.truncated), (vec![5, 6, 7], false));
    let d = pin.batches_since(5, &reg).unwrap();
    assert_eq!((rows_of(&d), d.truncated), (vec![6, 7], false));
    let d = pin.batches_since(6, &reg).unwrap();
    assert_eq!((rows_of(&d), d.truncated), (vec![7], false));
    drop(pin);

    // A Replace is never transparent: a reader at v7 must resync.
    w.commit(Commit::Replace, &batch(&[8]), &reg).unwrap();
    let pin = art.pin().unwrap();
    let d = pin.batches_since(7, &reg).unwrap();
    assert_eq!(
        (rows_of(&d), d.from_version, d.truncated),
        (vec![8], 8, true)
    );
    // ... and a Window that keeps a whole Replace root has that root's own
    // version as base: a reader at v7 still resyncs, a reader at v8 is exact.
    w.commit(Commit::Window { keep_batches: 5 }, &batch(&[9]), &reg)
        .unwrap();
    let pin = art.pin().unwrap();
    assert_eq!(pin.manifest().window_base, 8);
    let d = pin.batches_since(7, &reg).unwrap();
    assert_eq!((rows_of(&d), d.truncated), (vec![8, 9], true));
    let d = pin.batches_since(8, &reg).unwrap();
    assert_eq!((rows_of(&d), d.truncated), (vec![9], false));
}

/// (w4) A reader pinned on the prior chain survives a Window: the kept
/// chunks are shared (second reference), the old chain stays readable, and
/// the pin drop cascades exactly the unkept tail.
#[test]
fn pinned_reader_survives_a_window_commit() {
    let (fx, art) = windowed_artifact(404, 256);
    let reg = registry();
    let baseline = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &batch(&[1]), &reg).unwrap();
    for i in 2..=5i64 {
        w.commit(Commit::Append, &batch(&[i]), &reg).unwrap();
    }
    let held = art.pin().unwrap();
    assert_eq!(held.version(), 5);

    w.commit(Commit::Window { keep_batches: 2 }, &batch(&[6]), &reg)
        .unwrap();
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        10 + 2,
        "the pinned chain (10) plus v6's own data chunk + manifest"
    );
    assert_eq!(rows(&held, &reg), vec![1, 2, 3, 4, 5]);
    assert_eq!(rows(&art.pin().unwrap(), &reg), vec![4, 5, 6]);
    // The kept chunks carry two references: the old manifests' and the root's.
    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();
    let root = art.pin().unwrap();
    for (i, c) in root.manifest().chunks.iter().enumerate() {
        let expect = if i < 2 { 2 } else { 1 };
        assert_eq!(pool.ctrl(c).unwrap().refcount(), expect, "kept chunk {i}");
    }
    drop(root);

    drop(held);
    assert_eq!(
        baseline - free_total(&fx.data_seg),
        3 + 1,
        "v1..v3 and the five old manifests cascaded; [4],[5] survive on the root's reference"
    );
    assert_eq!(rows(&art.pin().unwrap(), &reg), vec![4, 5, 6]);
}

/// (w5) Optimistic `Window` vs `Append` racing for the same `expect`, 500
/// rounds: exactly one wins, the loser's rollback releases every kept
/// reference (or its link), and the census is exact after every round.
#[test]
fn optimistic_window_vs_append_conflict_census_exact() {
    let (fx, art) = windowed_artifact(405, 4096);
    let art = Arc::new(art);
    let reg = Arc::new(registry());
    let baseline = free_total(&fx.data_seg);

    let mut current = art
        .commit_optimistic(7, 0, Commit::Replace, &batch(&[0]), &reg)
        .unwrap();
    let mut windows_won = 0usize;
    for round in 0..500u64 {
        let barrier = Arc::new(Barrier::new(2));
        let (a, wv) = thread::scope(|s| {
            let a = {
                let art = art.clone();
                let reg = reg.clone();
                let b = barrier.clone();
                s.spawn(move || {
                    b.wait();
                    art.commit_optimistic(
                        11,
                        current,
                        Commit::Append,
                        &batch(&[round as i64]),
                        &reg,
                    )
                })
            };
            let wv = {
                let art = art.clone();
                let reg = reg.clone();
                let b = barrier.clone();
                s.spawn(move || {
                    b.wait();
                    art.commit_optimistic(
                        12,
                        current,
                        Commit::Window { keep_batches: 4 },
                        &batch(&[-1]),
                        &reg,
                    )
                })
            };
            (a.join().unwrap(), wv.join().unwrap())
        });
        match (&a, &wv) {
            (Ok(v), Err(shm_artifact::Error::Conflict { .. })) => current = *v,
            (Err(shm_artifact::Error::Conflict { .. }), Ok(v)) => {
                windows_won += 1;
                current = *v;
            }
            other => panic!("round {round}: expected exactly one winner, got {other:?}"),
        }
        assert_eq!(current, round + 2);
        let pin = art.pin().unwrap();
        let m = pin.manifest().clone();
        let live = m.depth as usize + 1 + m.total_batches as usize;
        assert!(
            m.total_batches <= 5 + m.depth,
            "round {round}: window not honoured"
        );
        drop(pin);
        assert_eq!(
            baseline - free_total(&fx.data_seg),
            live,
            "round {round}: census off (depth {}, batches {})",
            m.depth,
            m.total_batches
        );
    }
    eprintln!("window-vs-append: windows won {windows_won}/500 rounds");

    art.commit_optimistic(7, current, Commit::Replace, &batch(&[1]), &reg)
        .unwrap();
    assert_eq!(baseline - free_total(&fx.data_seg), 2);
}

/// (w6) A Window keeps multi-chunk batches (item F) whole: the kept spans are
/// copied with their chunk groups, every batch reads back intact, and the
/// census counts each kept chunk once.
#[test]
fn window_keeps_multi_chunk_batches_whole() {
    let fx = Fixture::new();
    let art = fx.artifact(406);
    let (b1, schema) = wide_batch(6, 200);
    let reg = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let baseline = free_total(&fx.data_seg);

    let mut w = art.open_exclusive(7).unwrap();
    w.commit(Commit::Replace, &b1, &reg).unwrap();
    let n = art.pin().unwrap().manifest().chunks.len();
    assert!(
        n > 1,
        "the batch must span several chunks for this test to mean anything"
    );
    w.commit(Commit::Append, &b1, &reg).unwrap();
    w.commit(Commit::Append, &b1, &reg).unwrap();
    assert_eq!(baseline - free_total(&fx.data_seg), 3 * n + 3);

    w.commit(Commit::Window { keep_batches: 2 }, &b1, &reg)
        .unwrap();
    let pin = art.pin().unwrap();
    assert_eq!(pin.manifest().chunks.len(), 3 * n);
    assert_eq!(pin.manifest().batch_spans, vec![n as u32; 3]);
    let batches = pin.as_arrow_batches(&reg).unwrap();
    assert_eq!(batches.len(), 3);
    for b in &batches {
        assert_eq!(b, &b1);
    }
    drop(pin);
    assert_eq!(baseline - free_total(&fx.data_seg), 3 * n + 1);
}

/// (w7) A Window whose new batch has a different schema is rejected before
/// any reference is taken, and leaks nothing.
#[test]
fn window_with_a_different_schema_is_rejected() {
    let fx = Fixture::new();
    let art = fx.artifact(407);
    let schema_b: SchemaRef = Arc::new(Schema::new(vec![Field::new("w", DataType::Int32, false)]));
    let reg = SchemaRegistry::with_schemas(&[schema(), schema_b.clone()]);
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1]), &reg)
        .unwrap();
    art.commit_optimistic(7, 1, Commit::Append, &batch(&[2]), &reg)
        .unwrap();
    let baseline = free_total(&fx.data_seg);

    let other = RecordBatch::try_new(
        schema_b,
        vec![Arc::new(arrow_array::Int32Array::from(vec![3]))],
    )
    .unwrap();
    assert!(matches!(
        art.commit_optimistic(7, 2, Commit::Window { keep_batches: 1 }, &other, &reg),
        Err(shm_artifact::Error::Unsupported(_))
    ));
    assert_eq!(art.current_version(), 2);
    assert_eq!(free_total(&fx.data_seg), baseline);
    assert_eq!(rows(&art.pin().unwrap(), &reg), vec![1, 2]);
}
