//! Integration tests for `shm-artifact`: real `Segment`s + `Pool`, RCU isolation,
//! reclamation correctness, multi-writer coordination, and watch events.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_artifact::{Artifact, Commit, CommitKind, VersionEvent};
use shm_arrow::SchemaRegistry;
use shm_core::{PoolConfig, Segment, FREE, PUBLISHED};
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
