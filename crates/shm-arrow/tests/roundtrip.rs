//! Integration tests for `shm-arrow`: real `Segment` + `Pool`, zero-copy proof.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_arrow::{read_batch, write_batch, PinGuard, PoolAllocator, SchemaRegistry};
use shm_core::{Pool, PoolConfig, Segment};

/// Process-unique segment ids so parallel tests never collide on shm names.
fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    // Base well above anything the rest of the workspace uses.
    47_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A freshly created, auto-unlinked segment with a pool laid out in it.
struct Fixture {
    id: u32,
    segment: Arc<Segment>,
}

impl Fixture {
    fn new(size: usize) -> Fixture {
        let id = next_segment_id();
        // Clean any leftover from a crashed prior run, then create fresh.
        let _ = Segment::unlink_by_id(id);
        let segment = Segment::create(id, size).expect("create segment");
        Fixture { id, segment: Arc::new(segment) }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = Segment::unlink_by_id(self.id);
    }
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
    ]))
}

fn sample_batch(schema: &SchemaRef) -> RecordBatch {
    let i = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let f = Float64Array::from(vec![Some(1.5), None, Some(3.5), Some(4.5), None]);
    let s = StringArray::from(vec![Some("alpha"), Some("beta"), None, Some("delta"), Some("")]);
    RecordBatch::try_new(schema.clone(), vec![Arc::new(i), Arc::new(f), Arc::new(s)]).unwrap()
}

#[test]
fn roundtrip_equality() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    let schema = sample_schema();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let batch = sample_batch(&schema);

    // Write path: loan a chunk, serialize, publish.
    let desc = write_batch(&alloc, &registry, &batch).unwrap();
    let ctrl = pool.ctrl(&desc).unwrap();
    ctrl.try_loan(7).unwrap();
    ctrl.publish().unwrap();

    // Read path: zero-copy reconstruction.
    let pin = Arc::new(PinGuard::new(fx.segment.clone()));
    let out = read_batch(pin, ctrl, &desc, &registry).unwrap();

    assert_eq!(out.schema(), batch.schema());
    assert_eq!(out.num_rows(), batch.num_rows());
    assert_eq!(&out, &batch);
}

#[test]
fn read_buffers_point_inside_segment() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    let schema = sample_schema();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let batch = sample_batch(&schema);

    let desc = write_batch(&alloc, &registry, &batch).unwrap();
    let ctrl = pool.ctrl(&desc).unwrap();
    ctrl.try_loan(7).unwrap();
    ctrl.publish().unwrap();

    let pin = Arc::new(PinGuard::new(fx.segment.clone()));
    let out = read_batch(pin, ctrl, &desc, &registry).unwrap();

    let base = fx.segment.base_ptr() as usize;
    let end = base + fx.segment.size();
    // Every column's every buffer must live inside the mapping — proving no
    // payload was copied to the heap during reconstruction.
    let mut checked = 0;
    for col in out.columns() {
        let data = col.to_data();
        if let Some(nulls) = data.nulls() {
            let p = nulls.buffer().as_ptr() as usize;
            assert!((base..end).contains(&p), "validity buffer escaped the segment");
            checked += 1;
        }
        for b in data.buffers() {
            let p = b.as_ptr() as usize;
            assert!((base..end).contains(&p), "data buffer escaped the segment");
            checked += 1;
        }
    }
    assert!(checked >= 4, "expected several in-segment buffers, saw {checked}");
}

#[test]
fn stale_descriptor_is_rejected() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    let schema = sample_schema();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let batch = sample_batch(&schema);

    let desc = write_batch(&alloc, &registry, &batch).unwrap();
    let ctrl = pool.ctrl(&desc).unwrap();
    ctrl.try_loan(7).unwrap();
    ctrl.publish().unwrap();

    // Recycle the chunk out from under the descriptor: force it back to FREE,
    // which bumps the generation. The old `desc` is now stale.
    ctrl.force_free();

    let pin = Arc::new(PinGuard::new(fx.segment.clone()));
    let err = read_batch(pin, ctrl, &desc, &registry).unwrap_err();
    assert!(
        matches!(err, shm_arrow::Error::Core(shm_core::Error::StaleDescriptor)),
        "expected StaleDescriptor, got {err:?}"
    );
}

#[test]
fn schema_interning_is_deterministic() {
    let a = sample_schema();
    let b: SchemaRef = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));

    let r1 = SchemaRegistry::with_schemas(&[a.clone(), b.clone()]);
    let r2 = SchemaRegistry::with_schemas(&[a.clone(), b.clone()]);

    // Same seeding order => identical ids across two independent registries.
    assert_eq!(r1.intern(&a), r2.intern(&a));
    assert_eq!(r1.intern(&b), r2.intern(&b));
    assert_eq!(r1.intern(&a), 1);
    assert_eq!(r1.intern(&b), 2);

    // Interning is idempotent and never yields the reserved raw id 0.
    assert_ne!(r1.intern(&a), 0);
    assert_eq!(r1.intern(&a), r1.intern(&a));

    // resolve() round-trips; id 0 is reserved (raw bytes) and resolves to None.
    assert_eq!(r1.resolve(1).unwrap(), a);
    assert_eq!(r1.resolve(2).unwrap(), b);
    assert!(r1.resolve(0).is_none());
    assert!(r1.resolve(999).is_none());
}

#[test]
fn unknown_schema_id_is_rejected() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    let schema = sample_schema();
    let writer_registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let batch = sample_batch(&schema);

    let desc = write_batch(&alloc, &writer_registry, &batch).unwrap();
    let ctrl = pool.ctrl(&desc).unwrap();
    ctrl.try_loan(7).unwrap();
    ctrl.publish().unwrap();

    // A reader that was NOT seeded with the schema cannot resolve the id.
    let empty_registry = SchemaRegistry::new();
    let pin = Arc::new(PinGuard::new(fx.segment.clone()));
    let err = read_batch(pin, ctrl, &desc, &empty_registry).unwrap_err();
    assert!(matches!(err, shm_arrow::Error::UnknownSchema(_)), "got {err:?}");
}
