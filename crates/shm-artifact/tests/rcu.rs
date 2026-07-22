//! Integration tests for `shm-artifact`: real `Segment`s + `Pool`, RCU isolation,
//! reclamation correctness, multi-writer coordination, and watch events.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_artifact::{Artifact, Commit, CommitKind, VersionEvent};
use shm_arrow::SchemaRegistry;
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

    fn artifact(&self, name_id: u32) -> Artifact {
        Artifact::create(
            name_id,
            self.head_seg.clone(),
            self.data_seg.clone(),
            &PoolConfig::power_of_two(1024, 4096, 64),
        )
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
    assert_eq!(w.commit(Commit::Replace, &batch(&[1, 2, 3]), &reg).unwrap(), 1);

    // Pin v1 BEFORE committing v2.
    let pin_v1 = art.pin().unwrap();
    assert_eq!(pin_v1.version(), 1);

    // v2 appends [4,5], sharing v1's chunk.
    assert_eq!(w.commit(Commit::Append, &batch(&[4, 5]), &reg).unwrap(), 2);
    drop(w);

    // The pin taken on v1 still reads v1 correctly AFTER v2 is installed.
    assert_eq!(col(&pin_v1.as_arrow(&reg).unwrap()), vec![1, 2, 3]);

    // A fresh pin sees v2 = both chunks concatenated.
    let pin_v2 = art.pin().unwrap();
    assert_eq!(pin_v2.version(), 2);
    assert_eq!(pin_v2.manifest().chunks.len(), 2);
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

    // v2: append shares v1's chunk, adds an exclusive chunk [4].
    w.commit(Commit::Append, &batch(&[4]), &reg).unwrap();
    let v2_manifest = art.pin().unwrap().manifest().clone();
    let shared = v2_manifest.chunks[0];
    let v2_exclusive = v2_manifest.chunks[1];
    assert_eq!(shared, v1_data, "v2 must share v1's chunk");

    // Attach a pool to inspect chunk control words.
    let pool = shm_core::Pool::attach(&fx.data_seg).unwrap();

    // After v2 is current, v1 (non-current, unpinned) is reclaimed on commit.
    // The shared chunk survives (still referenced by v2); v1's manifest chunk is
    // freed. The shared data chunk is still PUBLISHED.
    assert_eq!(
        pool.ctrl(&shared).unwrap().state(),
        PUBLISHED,
        "shared chunk must survive v1 retirement"
    );
    // The shared chunk's refcount is exactly 1 now (only v2 references it).
    assert_eq!(pool.ctrl(&shared).unwrap().refcount(), 1);

    // v3: replace wholesale with [9]. v2 becomes non-current and (unpinned) is
    // reclaimed: BOTH the shared chunk and v2's exclusive chunk lose their last
    // reference and return to FREE.
    w.commit(Commit::Replace, &batch(&[9]), &reg).unwrap();
    drop(w);
    // No live pins on v2 → its chunks are reclaimed.
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
    let ring = unsafe {
        Ring::init(ring_seg.payload_ptr(), ring_seg.payload_len(), 64).unwrap()
    };
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
            version: 1
        }]
    );

    // A clean drop releases the journal slot (nothing left for a replay).
    drop(pin);
    assert_eq!(journal.len(), 0, "clean drop must release the journal entry");
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
            version,
        } = rec
        {
            assert_eq!(artifact_id, 103);
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
    assert_eq!(journal.len(), 0, "no journalled pin may leak after a clean run");
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
    assert_eq!(journal.len(), 0, "clean drop must release the WriteLease entry");
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
    assert_eq!(art.current_version(), 0, "a fenced commit installs no version");
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
    assert_eq!(w2.fence_token(), 1, "clean release must have bumped the fence");
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
    (RecordBatch::try_new(schema.clone(), columns).unwrap(), schema)
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
        assert!(m.chunks.len() >= 2, "batch must span >= 2 data chunks, got {}", m.chunks.len());
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
    assert_eq!(m2.chunks.len(), 2 * v1_chunks, "append concatenates both batches' chunks");
    assert_eq!(m2.batch_spans.len(), 2, "two batches after append");
    assert_eq!(m2.batch_spans[0] as usize, v1_chunks);
    assert_eq!(m2.batch_spans[1] as usize, v1_chunks);

    // v2 reads back as the two batches concatenated in row order.
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
    let a = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as Arc<dyn arrow_array::Array>;
    let bcol = Arc::new(StringArray::from(vec![Some("x"), None, Some("zz"), Some("www")])) as Arc<dyn arrow_array::Array>;
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
    assert!((base..end).contains(&child), "nested child buffer escaped the segment");
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
