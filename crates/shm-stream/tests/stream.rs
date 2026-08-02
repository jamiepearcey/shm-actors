//! Integration tests for `shm-stream`: transactional isolation, Append/Replace
//! install, abort/drop reclamation, crash-equivalent journal replay, and
//! optimistic/exclusive coordination — all over real `Segment`s + `Pool`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_arrow::SchemaRegistry;
use shm_artifact::{Artifact, Commit};
use shm_core::{BorrowJournal, ChunkDesc, Pool, PoolConfig, Segment, FREE, LOANED, PUBLISHED};
use shm_stream::{Coordination, StreamWriter};

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    52_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A freshly created artifact (head + data segments) plus a borrow journal.
struct Fixture {
    head_id: u32,
    data_id: u32,
    journal_id: u32,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
    journal_seg: Arc<Segment>,
}

impl Fixture {
    fn new() -> Fixture {
        let head_id = next_segment_id();
        let data_id = next_segment_id();
        let journal_id = next_segment_id();
        for id in [head_id, data_id, journal_id] {
            let _ = Segment::unlink_by_id(id);
        }
        let head_seg = Arc::new(Segment::create(head_id, 1 << 16).expect("create head"));
        let data_seg = Arc::new(Segment::create(data_id, 1 << 20).expect("create data"));
        let journal_seg = Arc::new(Segment::create(journal_id, 1 << 16).expect("create journal"));
        BorrowJournal::create(&journal_seg, 256).expect("create journal");
        Fixture {
            head_id,
            data_id,
            journal_id,
            head_seg,
            data_seg,
            journal_seg,
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

    fn journal(&self) -> BorrowJournal<'_> {
        BorrowJournal::attach(&self.journal_seg).expect("attach journal")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for id in [self.head_id, self.data_id, self.journal_id] {
            let _ = Segment::unlink_by_id(id);
        }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

fn registry() -> SchemaRegistry {
    SchemaRegistry::with_schemas(std::slice::from_ref(&schema()))
}

fn col(b: &RecordBatch) -> Vec<i64> {
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

/// Replicate the coordinator's crash-reclaim core (`shm-runtime`'s
/// `replay_and_reclaim`): replay a dead actor's journal and reclaim every chunk
/// it still held. Returns the descriptors actually recycled + freed.
fn replay_and_reclaim(pool: &Pool, journal: &BorrowJournal, actor_id: u32) -> Vec<ChunkDesc> {
    let mut reclaimed = Vec::new();
    for rec in journal.replay() {
        // A stream only ever journals chunk pins; ignore any other entry kind.
        let desc = match rec {
            shm_core::JournalRecord::ChunkPin(d) => d,
            shm_core::JournalRecord::ArtifactPin { .. }
            | shm_core::JournalRecord::WriteLease { .. } => continue,
        };
        let ctrl = match pool.ctrl(&desc) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if ctrl.validate(&desc).is_err() {
            continue;
        }
        let recycled = match ctrl.state() {
            PUBLISHED => {
                if ctrl.refcount() > 0 {
                    ctrl.release_shared()
                } else {
                    ctrl.owner_release()
                }
            }
            LOANED => {
                if ctrl.owner_actor.load(Ordering::Acquire) == actor_id {
                    ctrl.drop_loan().is_ok()
                } else {
                    false
                }
            }
            _ => false,
        };
        if recycled {
            let _ = pool.free(&desc);
            reclaimed.push(desc);
        }
    }
    reclaimed
}

// ---------------------------------------------------------------------------

/// Staged batches are invisible to a reader that pins before commit; after
/// commit a fresh pin sees the new version with BOTH batches concatenated.
#[test]
fn isolation_staged_batches_invisible_until_commit() {
    let fx = Fixture::new();
    let art = fx.artifact(1);
    let reg = registry();
    let journal = fx.journal();

    let mut stream = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        7,
        Commit::Append,
        Coordination::Exclusive,
    )
    .unwrap();
    stream.append_batch(&batch(&[10, 20])).unwrap();
    stream.append_batch(&batch(&[30])).unwrap();
    assert_eq!(stream.staged_len(), 2);

    // Reader pinning BEFORE commit: nothing is installed → no version visible.
    assert!(matches!(art.pin(), Err(shm_artifact::Error::VersionGone)));
    assert_eq!(art.current_version(), 0);

    let v = stream.commit().unwrap();
    assert_eq!(v, 1);

    // A fresh pin now sees v1 = both staged batches concatenated in append order.
    let pin = art.pin().unwrap();
    assert_eq!(pin.version(), 1);
    assert_eq!(pin.manifest().chunks.len(), 2);
    assert_eq!(col(&pin.as_arrow(&reg).unwrap()), vec![10, 20, 30]);

    assert!(journal.is_empty(), "journal slots released after commit");
}

/// Append mode: the committed version's manifest is the prior version's chunk
/// (shared, reference-counted) plus the staged chunk.
#[test]
fn append_shares_prior_chunk() {
    let fx = Fixture::new();
    let art = fx.artifact(2);
    let reg = registry();
    let journal = fx.journal();

    // Seed v1 = [1,2,3] via a plain optimistic commit.
    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1, 2, 3]), &reg)
        .unwrap();
    let pin_v1 = art.pin().unwrap();
    let v1_chunk = pin_v1.manifest().chunks[0];

    // Stream appends [4,5] → v2.
    let mut stream = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        7,
        Commit::Append,
        Coordination::Optimistic { expect_version: 1 },
    )
    .unwrap();
    stream.append_batch(&batch(&[4, 5])).unwrap();
    assert_eq!(stream.commit().unwrap(), 2);

    let pin_v2 = art.pin().unwrap();
    assert_eq!(pin_v2.version(), 2);
    assert_eq!(pin_v2.manifest().chunks.len(), 2);
    // v2's first chunk IS v1's chunk (shared, not copied).
    assert_eq!(
        pin_v2.manifest().chunks[0],
        v1_chunk,
        "v2 must share v1's chunk"
    );
    assert_eq!(col(&pin_v2.as_arrow(&reg).unwrap()), vec![1, 2, 3, 4, 5]);

    // The shared chunk is PUBLISHED and referenced by both live versions.
    let pool = Pool::attach(&fx.data_seg).unwrap();
    assert_eq!(pool.ctrl(&v1_chunk).unwrap().state(), PUBLISHED);
    assert_eq!(pool.ctrl(&v1_chunk).unwrap().refcount(), 2);
}

/// Replace mode: the committed version contains ONLY the staged batches.
#[test]
fn replace_contains_only_staged() {
    let fx = Fixture::new();
    let art = fx.artifact(3);
    let reg = registry();
    let journal = fx.journal();

    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[1, 2, 3]), &reg)
        .unwrap();

    let mut stream = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        7,
        Commit::Replace,
        Coordination::Optimistic { expect_version: 1 },
    )
    .unwrap();
    stream.append_batch(&batch(&[99])).unwrap();
    assert_eq!(stream.commit().unwrap(), 2);

    let pin = art.pin().unwrap();
    assert_eq!(
        pin.manifest().chunks.len(),
        1,
        "Replace keeps only staged chunks"
    );
    assert_eq!(col(&pin.as_arrow(&reg).unwrap()), vec![99]);
}

/// Aborting frees every staged chunk (back to the pool) and installs no version.
#[test]
fn abort_frees_staged() {
    let fx = Fixture::new();
    let art = fx.artifact(4);
    let reg = registry();
    let journal = fx.journal();
    let pool = Pool::attach(&fx.data_seg).unwrap();
    let free_before = pool.free_count(0);

    let mut stream = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        7,
        Commit::Append,
        Coordination::Exclusive,
    )
    .unwrap();
    stream.append_batch(&batch(&[1])).unwrap();
    stream.append_batch(&batch(&[2])).unwrap();
    // Two chunks are now loaned out of class 0.
    assert_eq!(pool.free_count(0), free_before - 2);

    stream.abort();

    assert_eq!(art.current_version(), 0, "abort installs no version");
    assert_eq!(
        pool.free_count(0),
        free_before,
        "both staged chunks returned"
    );
    assert!(journal.is_empty(), "journal slots released on abort");
    // A fresh exclusive open succeeds: abort released the lease.
    assert!(art.open_exclusive(8).is_ok());
}

/// Dropping a stream without commit is equivalent to abort.
#[test]
fn drop_without_commit_frees_staged() {
    let fx = Fixture::new();
    let art = fx.artifact(5);
    let reg = registry();
    let journal = fx.journal();
    let pool = Pool::attach(&fx.data_seg).unwrap();
    let free_before = pool.free_count(0);

    {
        let mut stream = StreamWriter::open(
            &art,
            &fx.data_seg,
            &journal,
            &reg,
            7,
            Commit::Append,
            Coordination::Optimistic { expect_version: 0 },
        )
        .unwrap();
        stream.append_batch(&batch(&[1])).unwrap();
        stream.append_batch(&batch(&[2])).unwrap();
        assert_eq!(pool.free_count(0), free_before - 2);
        // stream dropped here without commit/abort.
    }

    assert_eq!(art.current_version(), 0);
    assert_eq!(
        pool.free_count(0),
        free_before,
        "Drop == abort: staged freed"
    );
    assert!(journal.is_empty());
}

/// Crash-equivalent: after appending (no commit), a `mem::forget`-ed writer
/// leaves its staged chunks LOANED + journaled; driving the coordinator's
/// journal-replay reclaim (as a lease monitor would) frees them with nothing
/// published and no version installed.
#[test]
fn crash_equivalent_journal_replay_frees_staged() {
    let fx = Fixture::new();
    let art = fx.artifact(6);
    let reg = registry();
    let journal = fx.journal();
    let pool = Pool::attach(&fx.data_seg).unwrap();
    let free_before = pool.free_count(0);
    let owner = 7u32;

    let mut stream = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        owner,
        Commit::Append,
        Coordination::Optimistic { expect_version: 0 },
    )
    .unwrap();
    stream.append_batch(&batch(&[1])).unwrap();
    stream.append_batch(&batch(&[2])).unwrap();
    assert_eq!(pool.free_count(0), free_before - 2);
    assert_eq!(journal.len(), 2, "two staged chunks journaled");

    // Simulate the writer process dying: no Drop, no abort, no commit.
    std::mem::forget(stream);

    // The coordinator's lease monitor replays the dead writer's journal.
    let reclaimed = replay_and_reclaim(&pool, &journal, owner);
    assert_eq!(reclaimed.len(), 2, "both staged chunks reclaimed by replay");

    assert_eq!(art.current_version(), 0, "no version ever published");
    assert_eq!(
        pool.free_count(0),
        free_before,
        "staged chunks returned to pool"
    );
    // Every reclaimed chunk was recycled to FREE (its descriptor now stale).
    for desc in &reclaimed {
        assert_eq!(pool.ctrl(desc).unwrap().state(), FREE);
    }
}

/// Optimistic conflict: two streams open at v1; the first commit wins (v2), the
/// second observes Conflict and its staged chunk is returned to the pool.
#[test]
fn optimistic_conflict_second_stream_loses() {
    let fx = Fixture::new();
    let art = fx.artifact(7);
    let reg = registry();
    let journal = fx.journal();
    let pool = Pool::attach(&fx.data_seg).unwrap();

    art.commit_optimistic(7, 0, Commit::Replace, &batch(&[0]), &reg)
        .unwrap();
    let free_after_seed = pool.free_count(0);

    let mut a = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        11,
        Commit::Replace,
        Coordination::Optimistic { expect_version: 1 },
    )
    .unwrap();
    let mut b = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        12,
        Commit::Replace,
        Coordination::Optimistic { expect_version: 1 },
    )
    .unwrap();
    a.append_batch(&batch(&[11])).unwrap();
    b.append_batch(&batch(&[12])).unwrap();

    assert_eq!(a.commit().unwrap(), 2, "first stream wins the install");
    let err = b.commit().unwrap_err();
    assert!(
        matches!(
            err,
            shm_stream::Error::Artifact(shm_artifact::Error::Conflict { .. })
        ),
        "second stream must observe a Conflict, got {err:?}"
    );

    assert_eq!(art.current_version(), 2);
    // The loser's staged chunk was returned; net pool usage is the one winning
    // chunk (v2 replaced v1, whose chunk was reclaimed).
    assert_eq!(pool.free_count(0), free_after_seed);
    assert!(
        journal.is_empty(),
        "both streams released their journal slots"
    );
    assert_eq!(col(&art.pin().unwrap().as_arrow(&reg).unwrap()), vec![11]);
}

/// Exclusive mode: a second stream (or writer) cannot open while the lease is
/// held; it opens once the first stream releases it.
#[test]
fn exclusive_second_open_is_write_locked() {
    let fx = Fixture::new();
    let art = fx.artifact(8);
    let reg = registry();
    let journal = fx.journal();

    let a = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        11,
        Commit::Append,
        Coordination::Exclusive,
    )
    .unwrap();

    let locked = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        12,
        Commit::Append,
        Coordination::Exclusive,
    );
    assert!(
        matches!(
            locked.err(),
            Some(shm_stream::Error::Artifact(
                shm_artifact::Error::WriteLocked
            ))
        ),
        "second exclusive open must fail WriteLocked"
    );

    a.abort();
    // Lease released → a new exclusive stream opens.
    let c = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        13,
        Commit::Append,
        Coordination::Exclusive,
    );
    assert!(c.is_ok());
}

/// A ranged Patch stream is rejected at open (deferred to v0.3).
#[test]
fn patch_stream_rejected_at_open() {
    let fx = Fixture::new();
    let art = fx.artifact(9);
    let reg = registry();
    let journal = fx.journal();

    let opened = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        7,
        Commit::Patch(0..1),
        Coordination::Exclusive,
    );
    assert!(matches!(
        opened.err(),
        Some(shm_stream::Error::Unsupported(_))
    ));
    // Rejecting at open takes no lease.
    assert!(art.open_exclusive(8).is_ok());
}

// ---------------------------------------------------------------------------
// ADR-0003 item F — multi-chunk batches through the stream.
// ---------------------------------------------------------------------------

/// A wide Int64 batch that serializes past the fixture pool's 4 KiB max chunk,
/// so one appended batch stages several chunks.
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

/// Total free chunks across every size class of the data pool.
fn free_all(pool: &Pool) -> usize {
    (0..pool.num_classes()).map(|c| pool.free_count(c)).sum()
}

#[test]
fn multi_chunk_batch_commits_and_reads_equal() {
    let fx = Fixture::new();
    let art = fx.artifact(20);
    let (b, schema) = wide_batch(6, 200);
    let reg = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let journal = fx.journal();

    let mut stream = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        7,
        Commit::Replace,
        Coordination::Exclusive,
    )
    .unwrap();
    // One append of a wide batch stages SEVERAL chunks (one batch).
    stream.append_batch(&b).unwrap();
    assert!(
        stream.staged_len() >= 2,
        "a multi-chunk batch stages >= 2 chunks"
    );
    assert_eq!(stream.commit().unwrap(), 1);

    let pin = art.pin().unwrap();
    let m = pin.manifest();
    assert!(m.chunks.len() >= 2, "committed batch spans >= 2 chunks");
    assert_eq!(
        m.batch_spans,
        vec![m.chunks.len() as u32],
        "one batch spanning all chunks"
    );
    assert_eq!(
        &pin.as_arrow(&reg).unwrap(),
        &b,
        "multi-chunk stream commit reads equal"
    );
    assert!(journal.is_empty(), "journal slots released after commit");
}

#[test]
fn crash_replay_frees_every_chunk_of_a_multi_chunk_batch() {
    let fx = Fixture::new();
    let art = fx.artifact(21);
    let (b, schema) = wide_batch(6, 200);
    let reg = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let journal = fx.journal();
    let pool = Pool::attach(&fx.data_seg).unwrap();
    let free_before = free_all(&pool);
    let owner = 7u32;

    let mut stream = StreamWriter::open(
        &art,
        &fx.data_seg,
        &journal,
        &reg,
        owner,
        Commit::Replace,
        Coordination::Optimistic { expect_version: 0 },
    )
    .unwrap();
    stream.append_batch(&b).unwrap();
    let staged_chunks = stream.staged_len();
    assert!(staged_chunks >= 2, "the batch staged >= 2 chunks");
    // EVERY chunk of the multi-chunk batch is journaled (not just the first).
    assert_eq!(
        journal.len(),
        staged_chunks,
        "every staged chunk is journaled"
    );
    assert_eq!(
        free_before - free_all(&pool),
        staged_chunks,
        "all staged chunks loaned"
    );

    // Simulate the writer dying mid-stage: no Drop, no abort, no commit.
    std::mem::forget(stream);

    // Journal replay must free ALL of the batch's chunks, not just the primary.
    let reclaimed = replay_and_reclaim(&pool, &journal, owner);
    assert_eq!(
        reclaimed.len(),
        staged_chunks,
        "replay freed every staged chunk"
    );
    assert_eq!(art.current_version(), 0, "no version ever published");
    assert_eq!(
        free_all(&pool),
        free_before,
        "every staged chunk returned to the pool"
    );
}
