//! A version whose data lives in a **dedicated** segment (ADR: an object larger
//! than the shared pool's biggest size class gets a segment of its own, exactly
//! the split a process allocator makes at `M_MMAP_THRESHOLD`).
//!
//! These tests drive `shm-artifact` through the [`SegmentMap`] seam: every
//! control-word lookup and every reconstructed Arrow buffer must be resolved
//! against the segment the descriptor actually names, never against the
//! artifact's own data segment. The three hazards they pin down:
//!
//! - a recycled **chunk generation** inside a dedicated segment is still caught
//!   by `ChunkCtrl::validate`, so a stale descriptor cannot steal the next
//!   occupant's reference;
//! - a pin keeps every segment it resolved mapped for the pin's whole life,
//!   even after the map that produced it stops answering;
//! - a retire returns a dedicated chunk to **its own** pool exactly once, which
//!   is what drives its control word to `FREE` (and so what lets the
//!   coordinator's sweep unlink the segment).

use std::panic::RefUnwindSafe;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_arrow::{write_batch_chunks, PoolAllocator, SchemaRegistry};
use shm_artifact::{write_manifest, Artifact, Commit, Error, SegmentMap};
use shm_core::{
    required_bytes, ChunkDesc, PackedRef, Pool, PoolConfig, Segment, SizeClass, FREE, PUBLISHED,
};

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    50_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

const OWNER: u32 = 7;

/// The bytes a dedicated segment needs to hold a one-class, one-chunk pool of
/// `chunk_size` — the shape a coordinator-granted payload segment carries.
fn dedicated_config(chunk_size: u32) -> PoolConfig {
    PoolConfig {
        classes: vec![SizeClass {
            chunk_size,
            chunk_count: 1,
        }],
    }
}

/// Head + shared-pool data segments, plus a dedicated payload segment holding
/// its own one-chunk pool.
struct Fixture {
    ids: Vec<u32>,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
    dedicated: Arc<Segment>,
}

impl Fixture {
    /// The shared pool's largest class is 4 KiB; the dedicated segment's single
    /// chunk is 64 KiB — an object no size class could ever hold.
    fn new() -> Fixture {
        let (head_id, data_id, ded_id) = (next_segment_id(), next_segment_id(), next_segment_id());
        for id in [head_id, data_id, ded_id] {
            let _ = Segment::unlink_by_id(id);
        }
        let head_seg = Arc::new(Segment::create(head_id, 1 << 16).expect("create head"));
        let data_seg = Arc::new(Segment::create(data_id, 1 << 20).expect("create data"));
        let cfg = dedicated_config(1 << 16);
        let ded_bytes = required_bytes(&cfg) + 4096;
        let dedicated = Arc::new(Segment::create(ded_id, ded_bytes).expect("create dedicated"));
        Pool::create(&dedicated, &cfg).expect("lay the dedicated pool");
        Fixture {
            ids: vec![head_id, data_id, ded_id],
            head_seg,
            data_seg,
            dedicated,
        }
    }

    fn artifact(&self, name_id: u32, map: Arc<dyn SegmentMap>) -> Artifact {
        Artifact::create(
            name_id,
            self.head_seg.clone(),
            self.data_seg.clone(),
            &PoolConfig::power_of_two(1024, 4096, 64),
        )
        .expect("create artifact")
        .with_segments(map)
    }

    /// An artifact that was never told about the dedicated segment.
    fn artifact_without_map(&self, name_id: u32) -> Artifact {
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
        for id in &self.ids {
            let _ = Segment::unlink_by_id(*id);
        }
    }
}

/// A [`SegmentMap`] over one dedicated segment whose answer can be **revoked**,
/// so a test can prove a pin retained what it resolved rather than re-asking.
struct RevocableMap {
    seg: Mutex<Option<Arc<Segment>>>,
}

impl RevocableMap {
    fn new(seg: Arc<Segment>) -> Arc<RevocableMap> {
        Arc::new(RevocableMap {
            seg: Mutex::new(Some(seg)),
        })
    }

    fn revoke(&self) {
        *self.seg.lock().unwrap() = None;
    }
}

impl SegmentMap for RevocableMap {
    fn segment(&self, segment_id: u32) -> Option<Arc<Segment>> {
        self.seg
            .lock()
            .unwrap()
            .as_ref()
            .filter(|s| s.id() == segment_id)
            .cloned()
    }
}

// `Segment` holds raw pointers into its mapping, so the auto-impl does not
// apply; every access below is through atomics or immutable bytes.
impl RefUnwindSafe for RevocableMap {}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

fn registry() -> SchemaRegistry {
    SchemaRegistry::with_schemas(std::slice::from_ref(&schema()))
}

fn values(b: &RecordBatch) -> Vec<i64> {
    b.column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column")
        .values()
        .to_vec()
}

/// Write `b` into `seg`'s pool and leave every chunk `LOANED` by `OWNER` — the
/// pre-staged shape `commit_staged_optimistic` installs.
fn stage_into(seg: &Segment, registry: &SchemaRegistry, b: &RecordBatch) -> Vec<ChunkDesc> {
    let pool = Pool::attach(seg).expect("attach pool");
    let alloc = PoolAllocator::new(&pool, seg);
    let chunks = write_batch_chunks(&alloc, registry, b).expect("write batch");
    for d in &chunks {
        pool.ctrl(d).expect("ctrl").try_loan(OWNER).expect("loan");
    }
    chunks
}

#[test]
fn a_version_whose_data_lives_in_a_dedicated_segment_reads_back() {
    let fx = Fixture::new();
    let map = RevocableMap::new(fx.dedicated.clone());
    let art = fx.artifact(1, map);
    let reg = registry();
    let b = batch(&[1, 2, 3, 4]);
    let schema_id = reg.intern(&b.schema());

    let staged = stage_into(&fx.dedicated, &reg, &b);
    assert!(
        staged.iter().all(|d| d.segment_id == fx.dedicated.id()),
        "the batch must be staged in the dedicated segment, not the store pool"
    );
    let spans = [staged.len() as u32];
    let v = art
        .commit_staged_optimistic(OWNER, 0, Commit::Replace, &staged, &spans, schema_id)
        .expect("commit a version whose chunk lives in its own segment");
    assert_eq!(v, 1);

    let pin = art.pin().expect("pin");
    let out = pin.as_arrow(&reg).expect("read back");
    assert_eq!(values(&out), vec![1, 2, 3, 4]);
}

#[test]
fn one_batch_may_span_the_shared_pool_and_a_dedicated_segment() {
    let fx = Fixture::new();
    let map = RevocableMap::new(fx.dedicated.clone());
    let art = fx.artifact(2, map);
    let reg = registry();
    let small = batch(&[10, 11]);
    let large = batch(&[20, 21, 22]);
    let schema_id = reg.intern(&small.schema());

    // Two batches in one version: the first out of the artifact's shared pool,
    // the second out of a segment of its own.
    let mut staged = stage_into(&fx.data_seg, &reg, &small);
    let store_chunks = staged.len() as u32;
    let ded = stage_into(&fx.dedicated, &reg, &large);
    let ded_chunks = ded.len() as u32;
    staged.extend_from_slice(&ded);
    let spans = [store_chunks, ded_chunks];

    art.commit_staged_optimistic(OWNER, 0, Commit::Replace, &staged, &spans, schema_id)
        .expect("commit a version spanning two segments");

    let pin = art.pin().expect("pin");
    let batches = pin.as_arrow_batches(&reg).expect("read back both batches");
    assert_eq!(batches.len(), 2);
    // Each batch must be reconstructed against the segment its descriptors
    // name: resolving both against the store segment would read the second at a
    // foreign offset.
    assert_eq!(values(&batches[0]), vec![10, 11]);
    assert_eq!(values(&batches[1]), vec![20, 21, 22]);
}

#[test]
fn a_pin_keeps_its_dedicated_segment_resolvable_for_the_pins_whole_life() {
    let fx = Fixture::new();
    let map = RevocableMap::new(fx.dedicated.clone());
    let art = fx.artifact(3, map.clone());
    let reg = registry();
    let b = batch(&[5, 6, 7]);
    let schema_id = reg.intern(&b.schema());
    let staged = stage_into(&fx.dedicated, &reg, &b);
    let spans = [staged.len() as u32];
    art.commit_staged_optimistic(OWNER, 0, Commit::Replace, &staged, &spans, schema_id)
        .expect("commit");

    // The pin resolves the segments its manifest names up front and holds them.
    let pin = art.pin().expect("pin");

    // The map stops answering, and every other handle on the segment goes away:
    // only what the pin retained can keep the mapping addressable.
    map.revoke();
    assert!(
        map.segment(fx.dedicated.id()).is_none(),
        "the map must no longer resolve the dedicated segment"
    );

    let batches = pin
        .as_arrow_batches(&reg)
        .expect("a pin reads through the segments it resolved at pin time");
    assert_eq!(values(&batches[0]), vec![5, 6, 7]);

    // The buffers outlive the pin AND the artifact: each holds a clone of the
    // pin state, which owns the mapping.
    drop(pin);
    drop(art);
    assert_eq!(values(&batches[0]), vec![5, 6, 7]);
}

#[test]
fn a_pin_is_refused_when_its_version_names_a_segment_the_reader_has_not_mapped() {
    let fx = Fixture::new();
    let reg = registry();
    let b = batch(&[1, 2]);
    let schema_id = reg.intern(&b.schema());
    let staged = stage_into(&fx.dedicated, &reg, &b);
    let spans = [staged.len() as u32];

    // Commit through a mapped artifact, then read through an unmapped one over
    // the same segments.
    let writer = fx.artifact(4, RevocableMap::new(fx.dedicated.clone()));
    writer
        .commit_staged_optimistic(OWNER, 0, Commit::Replace, &staged, &spans, schema_id)
        .expect("commit");

    let reader = Artifact::attach(4, fx.head_seg.clone(), fx.data_seg.clone()).expect("attach");
    match reader.pin() {
        Err(Error::Core(shm_core::Error::WrongSegment { desc, .. })) => {
            assert_eq!(desc, fx.dedicated.id());
        }
        Err(e) => panic!("expected WrongSegment, got {e:?}"),
        Ok(_) => panic!("an unmapped reader must not be handed a pin it cannot read"),
    }
    // The refusal must not have leaked the speculative pin bump.
    assert_eq!(writer.version_pin_count(1), Some(0));
    // ... and the version is still readable by a reader that DOES map it.
    let pin = writer.pin().expect("pin through the mapped artifact");
    assert_eq!(values(&pin.as_arrow(&reg).expect("read")), vec![1, 2]);
}

#[test]
fn staging_into_an_unmapped_segment_is_refused_rather_than_misresolved() {
    let fx = Fixture::new();
    let art = fx.artifact_without_map(5);
    let reg = registry();
    let b = batch(&[9]);
    let schema_id = reg.intern(&b.schema());
    let staged = stage_into(&fx.dedicated, &reg, &b);
    let spans = [staged.len() as u32];

    let err = art
        .commit_staged_optimistic(OWNER, 0, Commit::Replace, &staged, &spans, schema_id)
        .expect_err("a map-less artifact must refuse a foreign descriptor");
    match err {
        Error::Core(shm_core::Error::WrongSegment { desc, pool }) => {
            assert_eq!(desc, fx.dedicated.id());
            assert_eq!(pool, fx.data_seg.id());
        }
        other => panic!("expected WrongSegment, got {other:?}"),
    }
    assert_eq!(art.current_version(), 0, "nothing may have been installed");

    // The refusal is fail-closed, not destructive: the chunk is still exactly
    // as the caller left it, in its own segment.
    let ded_pool = Pool::attach(&fx.dedicated).expect("attach dedicated pool");
    let ctrl = ded_pool.ctrl(&staged[0]).expect("dedicated ctrl");
    assert_eq!(ctrl.generation(), staged[0].generation);
}

#[test]
fn retiring_a_version_returns_its_dedicated_chunk_to_its_own_pool_exactly_once() {
    let fx = Fixture::new();
    let art = fx.artifact(6, RevocableMap::new(fx.dedicated.clone()));
    let reg = registry();
    let b = batch(&[1, 2, 3]);
    let schema_id = reg.intern(&b.schema());

    let ded_pool = Pool::attach(&fx.dedicated).expect("attach dedicated pool");
    assert_eq!(ded_pool.free_count(0), 1, "the segment holds one chunk");

    let staged = stage_into(&fx.dedicated, &reg, &b);
    assert_eq!(
        staged.len(),
        1,
        "the object is one chunk in its own segment"
    );
    let desc = staged[0];
    art.commit_staged_optimistic(OWNER, 0, Commit::Replace, &staged, &[1], schema_id)
        .expect("commit v1");
    assert_eq!(ded_pool.free_count(0), 0);

    // Supersede it with a version out of the shared pool. v1 is then non-current
    // and unpinned, so the retire cascade runs.
    let b2 = batch(&[4]);
    let staged2 = stage_into(&fx.data_seg, &reg, &b2);
    let spans2 = [staged2.len() as u32];
    art.commit_staged_optimistic(OWNER, 1, Commit::Replace, &staged2, &spans2, schema_id)
        .expect("commit v2");

    // The cascade must have driven the dedicated segment's sole chunk to FREE —
    // that control word reaching FREE is the ONLY thing that authorises the
    // coordinator to unlink the segment.
    let ctrl = ded_pool.ctrl(&desc).expect("dedicated ctrl");
    assert_eq!(ctrl.state(), FREE, "the dedicated chunk must be reclaimed");

    // Exactly once. A double free would push the same slot onto the free list
    // twice, so the class would hand out two chunks where it owns one.
    let a = ded_pool
        .alloc(1 << 16)
        .expect("the chunk is back on the list");
    assert_eq!(a.offset, desc.offset);
    assert!(
        matches!(ded_pool.alloc(1 << 16), Err(shm_core::Error::PoolExhausted)),
        "a one-chunk segment must not hand out a second chunk"
    );
}

#[test]
fn a_recycled_chunk_generation_in_a_dedicated_segment_is_still_caught() {
    let fx = Fixture::new();
    let art = fx.artifact(7, RevocableMap::new(fx.dedicated.clone()));
    let reg = registry();
    let ded_pool = Pool::attach(&fx.dedicated).expect("attach dedicated pool");

    // Occupant #1 of the segment's sole chunk: published, then fully released
    // and returned to the free list. `stale` is the descriptor a manifest
    // written before that recycle would carry.
    let stale = ded_pool.alloc(1 << 16).expect("alloc occupant 1");
    {
        let c = ded_pool.ctrl(&stale).expect("ctrl");
        c.try_loan(OWNER).expect("loan");
        c.publish().expect("publish");
        c.borrow_shared().expect("borrow");
        c.owner_release();
        assert!(c.release_shared(), "occupant 1 reclaims on its last ref");
    }
    ded_pool.free(&stale).expect("return to the free list");

    // Occupant #2: the SAME chunk, one generation on, holding one live
    // reference — the stranger a stale descriptor must never touch.
    let live = ded_pool.alloc(1 << 16).expect("alloc occupant 2");
    assert_eq!(live.offset, stale.offset, "the segment holds one chunk");
    assert_ne!(
        live.generation, stale.generation,
        "recycling must bump the generation"
    );
    {
        let c = ded_pool.ctrl(&live).expect("ctrl");
        c.try_loan(OWNER).expect("loan");
        c.publish().expect("publish");
        c.borrow_shared().expect("borrow");
        c.owner_release();
    }

    // A manifest staged before the recycle, listing the stale descriptor, that
    // its committer died before installing (ADR-0014 §3). Replay releases the
    // manifest and cascades through the chunks it lists.
    let store_pool = Pool::attach(&fx.data_seg).expect("attach store pool");
    let alloc = PoolAllocator::new(&store_pool, &fx.data_seg);
    let schema_id = reg.intern(&schema());
    let mdesc = write_manifest(&alloc, 7, 1, schema_id, &[stale], &[1], None, None)
        .expect("write the staged manifest");
    {
        let c = store_pool.ctrl(&mdesc).expect("manifest ctrl");
        c.try_loan(OWNER).expect("loan");
        c.publish().expect("publish");
        c.borrow_shared().expect("borrow");
        c.owner_release();
    }
    let mref = PackedRef::from_desc(&mdesc);

    assert!(
        art.reclaim_staged_manifest(mref.to_bits(), mdesc.generation)
            .expect("replay the staged manifest"),
        "the uninstalled manifest is released by replay"
    );

    // The cascade resolved the stale descriptor against the DEDICATED pool (not
    // the store's) and then refused it on the generation: occupant #2 keeps its
    // reference and its chunk.
    let c = ded_pool.ctrl(&live).expect("ctrl");
    assert_eq!(c.state(), PUBLISHED, "occupant 2 must still be published");
    assert_eq!(c.refcount(), 1, "its reference must not have been stolen");
    assert_eq!(c.generation(), live.generation);
    assert_eq!(
        ded_pool.free_count(0),
        0,
        "the live chunk must not be back on the free list"
    );
    // The manifest chunk itself, which was genuinely ours, is gone.
    let mc = store_pool.ctrl(&mdesc).expect("manifest ctrl");
    assert_eq!(mc.state(), FREE);
}
