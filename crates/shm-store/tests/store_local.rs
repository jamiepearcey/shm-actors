//! Single-process proof of the keyed store (ADR-0007 G3), with no coordinator:
//! the test builds the three store segments by hand and injects a trivial
//! in-memory [`KeyResolver`]. It exercises create / open-by-key / open-by-id,
//! commit v1→v3 + pin/read equality, evict (tombstone + full chunk reclaim to a
//! zero-leak census), and clean errors for an absent / tombstoned key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use shm_arrow::SchemaRegistry;
use shm_artifact::{ArtifactHead, Commit};
use shm_core::{segment::HEADER_SIZE, BorrowJournal, Pool, PoolConfig, Segment};
use shm_store::{Catalog, Error, KeyResolver, KeyedStore, RefKind};

/// A trivial in-memory key interner (the coordinator's job in the real system):
/// same bytes → same monotonic id, different bytes → different id.
#[derive(Default)]
struct MemResolver {
    inner: Mutex<(HashMap<Vec<u8>, u32>, u32)>,
}

impl KeyResolver for MemResolver {
    fn intern_key(&self, key: &[u8]) -> shm_store::Result<u32> {
        let mut g = self.inner.lock().unwrap();
        if let Some(&id) = g.0.get(key) {
            return Ok(id);
        }
        g.1 += 1;
        let id = g.1;
        g.0.insert(key.to_vec(), id);
        Ok(id)
    }
}

/// The store's three owned segments plus the actor journal, kept mapped for the
/// test's duration.
struct Harness {
    catalog: Arc<Segment>,
    head: Arc<Segment>,
    data: Arc<Segment>,
    journal: Arc<Segment>,
    registry: Arc<SchemaRegistry>,
    resolver: MemResolver,
}

impl Harness {
    fn new(base: u32, schema: &SchemaRef) -> Harness {
        let cap = 16u32;
        let head_stride = (ArtifactHead::region_bytes() + 63) & !63;
        for id in base..base + 4 {
            let _ = Segment::unlink_by_id(id);
        }
        let catalog = Segment::create(base, Catalog::segment_bytes(cap)).expect("catalog seg");
        Catalog::init(&catalog, cap, head_stride as u32, 1 << 28).expect("catalog init");
        let head = Segment::create(base + 1, HEADER_SIZE + cap as usize * head_stride)
            .expect("head seg");
        let data = Segment::create(base + 2, 1 << 20).expect("data seg");
        Pool::create(&data, &PoolConfig::power_of_two(256, 8192, 32)).expect("pool");
        let journal = Segment::create(base + 3, 64 * 1024).expect("journal seg");
        BorrowJournal::create(&journal, 256).expect("journal");
        Harness {
            catalog: Arc::new(catalog),
            head: Arc::new(head),
            data: Arc::new(data),
            journal: Arc::new(journal),
            registry: Arc::new(SchemaRegistry::with_schemas(std::slice::from_ref(schema))),
            resolver: MemResolver::default(),
        }
    }

    fn store(&self) -> KeyedStore<'_> {
        KeyedStore::new(
            self.catalog.clone(),
            self.head.clone(),
            self.data.clone(),
            self.journal.clone(),
            self.registry.clone(),
            /* owner */ 7,
            &self.resolver,
        )
    }

    /// Total free chunks across the data pool's classes (the leak census).
    fn free_total(&self) -> usize {
        let pool = Pool::attach(&self.data).expect("pool attach");
        (0..pool.num_classes()).map(|c| pool.free_count(c)).sum()
    }

    fn unlink(&self) {
        self.catalog.unlink().ok();
        self.head.unlink().ok();
        self.data.unlink().ok();
        self.journal.unlink().ok();
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch(schema: &SchemaRef, vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
}

#[test]
fn create_commit_v1_to_v3_open_by_key_and_id_read_equal() {
    let sch = schema();
    let h = Harness::new(80_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();

    let entry = store
        .create(b"dataset/X", RefKind::Dataset, &sch)
        .expect("create");
    assert_eq!(entry.kind(), RefKind::Dataset);
    assert_eq!(entry.current_version(), 0, "nothing committed yet");
    let key_id = entry.key_id();

    let b1 = batch(&sch, &[1, 2, 3]);
    let b2 = batch(&sch, &[10, 20, 30]);
    let b3 = batch(&sch, &[100, 200, 300]);
    assert_eq!(entry.commit(Commit::Replace, &b1).expect("v1"), 1);
    assert_eq!(entry.commit(Commit::Replace, &b2).expect("v2"), 2);
    assert_eq!(entry.commit_replace(&b3).expect("v3"), 3);
    assert_eq!(entry.current_version(), 3);

    // Open by key in a *fresh* handle and read the current version zero-copy.
    let reopened = store.open(b"dataset/X").expect("open by key");
    let (pin, got) = reopened.read().expect("read");
    assert_eq!(pin.version(), 3);
    assert_eq!(got, b3, "the pinned current version equals the last commit");
    drop(pin);

    // Open by interned id resolves to the same live entry.
    let by_id = store.open_id(key_id).expect("open_id");
    assert_eq!(by_id.artifact_id(), entry.artifact_id());
    assert_eq!(by_id.current_version(), 3);

    h.unlink();
}

#[test]
fn create_is_idempotent_and_keys_are_distinct() {
    let sch = schema();
    let h = Harness::new(81_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();

    let a = store.create(b"a", RefKind::Dataset, &sch).expect("create a");
    let a_again = store.create(b"a", RefKind::Dataset, &sch).expect("re-create a");
    assert_eq!(
        a.artifact_id(),
        a_again.artifact_id(),
        "same key returns the same live entry (get-or-create)"
    );

    let b = store.create(b"b", RefKind::Result, &sch).expect("create b");
    assert_ne!(a.artifact_id(), b.artifact_id(), "different keys, different entries");
    assert_ne!(a.key_id(), b.key_id());

    h.unlink();
}

#[test]
fn evict_tombstones_reclaims_to_baseline_and_absent_open_errors() {
    let sch = schema();
    let h = Harness::new(82_000 + (std::process::id() & 0x3ff), &sch);

    // Open of an absent key errors cleanly, before anything exists.
    assert!(matches!(h.store().open(b"dataset/X"), Err(Error::NotFound)));

    let baseline = h.free_total();

    let store = h.store();
    let entry = store
        .create(b"dataset/X", RefKind::Dataset, &sch)
        .expect("create");
    entry.commit_replace(&batch(&sch, &[1, 2, 3])).unwrap();
    entry.commit_replace(&batch(&sch, &[4, 5, 6])).unwrap();
    entry.commit_replace(&batch(&sch, &[7, 8, 9])).unwrap();
    assert!(
        h.free_total() < baseline,
        "committing versions consumed chunks ({} < {})",
        h.free_total(),
        baseline
    );

    // Evict: tombstone + full teardown. The census returns to baseline (zero leak).
    store.evict(b"dataset/X").expect("evict");
    assert_eq!(
        h.free_total(),
        baseline,
        "evict reclaimed every chunk by refcount"
    );

    // Open of a tombstoned key errors cleanly; evict is idempotent.
    assert!(matches!(store.open(b"dataset/X"), Err(Error::NotFound)));
    store.evict(b"dataset/X").expect("second evict is a no-op");

    // A re-create after eviction appends a fresh entry with a new lineage id.
    let e2 = store.create(b"dataset/X", RefKind::Dataset, &sch).expect("re-create");
    assert_eq!(e2.current_version(), 0, "fresh reincarnation");

    h.unlink();
}
