//! Integration tests for `shm-arrow`: real `Segment` + `Pool`, zero-copy proof.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use shm_arrow::{
    read_batch, read_batch_chunks, write_batch, write_batch_chunks, PinGuard, PoolAllocator,
    SchemaRegistry,
};
use shm_core::{ChunkCtrl, ChunkDesc, Pool, PoolConfig, Segment};

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

// ---------------------------------------------------------------------------
// v0.3 (ADR-0003 item F): nested, sliced, and multi-chunk batches.
// ---------------------------------------------------------------------------

/// Stage a whole batch (possibly multi-chunk): loan + publish every chunk, then
/// reconstruct it zero-copy and return it together with the chunk descriptors.
fn stage_and_read(
    pool: &Pool<'_>,
    alloc: &PoolAllocator<'_>,
    registry: &SchemaRegistry,
    segment: &Arc<Segment>,
    batch: &RecordBatch,
) -> (RecordBatch, Vec<ChunkDesc>) {
    let descs = write_batch_chunks(alloc, registry, batch).unwrap();
    for d in &descs {
        let ctrl = pool.ctrl(d).unwrap();
        ctrl.try_loan(7).unwrap();
        ctrl.publish().unwrap();
    }
    let ctrls: Vec<&ChunkCtrl> = descs.iter().map(|d| pool.ctrl(d).unwrap()).collect();
    let pin = Arc::new(PinGuard::new(segment.clone()));
    let out = read_batch_chunks(pin, &descs, &ctrls, registry).unwrap();
    (out, descs)
}

/// Assert every buffer of every column (recursively) points inside `segment`.
fn assert_buffers_in_segment(batch: &RecordBatch, segment: &Segment) -> usize {
    fn walk(data: &arrow_data::ArrayData, base: usize, end: usize) -> usize {
        let mut n = 0;
        if let Some(nulls) = data.nulls() {
            let p = nulls.buffer().as_ptr() as usize;
            assert!((base..end).contains(&p), "validity buffer escaped the segment");
            n += 1;
        }
        for b in data.buffers() {
            let p = b.as_ptr() as usize;
            assert!((base..end).contains(&p), "data buffer escaped the segment");
            n += 1;
        }
        for c in data.child_data() {
            n += walk(c, base, end);
        }
        n
    }
    let base = segment.base_ptr() as usize;
    let end = base + segment.size();
    batch.columns().iter().map(|c| walk(&c.to_data(), base, end)).sum()
}

#[test]
fn struct_column_round_trips_zero_copy() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    // A Struct<{a: Int32, b: Utf8}> column, with a null in the struct.
    let a = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
    let b = Arc::new(StringArray::from(vec![Some("x"), Some("yy"), Some("zzz"), None])) as ArrayRef;
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let nulls = arrow_buffer::NullBuffer::from(vec![true, true, false, true]);
    let s = StructArray::new(fields.clone(), vec![a, b], Some(nulls));
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(fields),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(s)]).unwrap();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    let (out, descs) = stage_and_read(&pool, &alloc, &registry, &fx.segment, &batch);
    assert_eq!(descs.len(), 1, "the small struct fits one chunk");
    assert_eq!(&out, &batch, "struct column must round-trip exactly");
    assert!(assert_buffers_in_segment(&out, &fx.segment) >= 4);
}

#[test]
fn list_int32_column_round_trips_zero_copy() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    // List<Int32>: [[1,2,3], [], [4,5]].
    let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 3, 3, 5]));
    let field = Arc::new(Field::new("item", DataType::Int32, false));
    let list = ListArray::new(field.clone(), offsets, Arc::new(values), None);
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "l",
        DataType::List(field),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    let (out, _descs) = stage_and_read(&pool, &alloc, &registry, &fx.segment, &batch);
    assert_eq!(&out, &batch, "list column must round-trip exactly");
    assert!(assert_buffers_in_segment(&out, &fx.segment) >= 2);
}

#[test]
fn sliced_primitive_equals_logical_slice() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    // Sliced Int64 + sliced Utf8: arrow bakes the slice into a non-zero *buffer*
    // internal offset (the values/offsets buffers are sub-windows of the parent
    // allocation). The writer must copy each buffer's logical `as_slice()`, not
    // the parent allocation, so the read side gets exactly the logical slice.
    let full_i = Int64Array::from(vec![10, 20, 30, 40, 50, 60]);
    let full_s = StringArray::from(vec!["a", "bb", "ccc", "dddd", "e", "ff"]);
    let sliced_i = full_i.slice(2, 3); // [30, 40, 50]
    let sliced_s = full_s.slice(2, 3); // ["ccc","dddd","e"]
    // The value buffer really is a sub-window (its data pointer is past the
    // parent's start), proving the input is genuinely sliced.
    assert_ne!(
        sliced_i.to_data().buffers()[0].as_ptr(),
        full_i.to_data().buffers()[0].as_ptr(),
        "the sliced buffer is a non-zero sub-window of the parent"
    );

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(sliced_i.clone()), Arc::new(sliced_s.clone())],
    )
    .unwrap();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    let (out, _descs) = stage_and_read(&pool, &alloc, &registry, &fx.segment, &batch);
    assert_eq!(&out, &batch, "result equals the logical slice");
    assert_eq!(
        col_i64(&out, 0),
        vec![30, 40, 50],
        "the exact logical window survived"
    );
    assert!(assert_buffers_in_segment(&out, &fx.segment) >= 3);
}

/// Read an Int64 column as a `Vec<i64>`.
fn col_i64(b: &RecordBatch, c: usize) -> Vec<i64> {
    b.column(c)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn batch_spanning_multiple_chunks_round_trips() {
    // A small max chunk size (2 KiB) forces a multi-column batch across chunks,
    // while every individual buffer stays well under 2 KiB.
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(256, 2048, 32)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    // Six Int64 columns of 180 rows: each values buffer = 1440 bytes (< 2048),
    // total ~8.6 KiB → several chunks, one buffer per chunk.
    let n = 180usize;
    let mut fields = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for c in 0..6 {
        fields.push(Field::new(format!("c{c}"), DataType::Int64, false));
        let vals: Vec<i64> = (0..n as i64).map(|r| r + c as i64 * 1000).collect();
        cols.push(Arc::new(Int64Array::from(vals)));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    let (out, descs) = stage_and_read(&pool, &alloc, &registry, &fx.segment, &batch);
    assert!(descs.len() >= 2, "batch must span >= 2 chunks, spanned {}", descs.len());
    assert_eq!(&out, &batch, "multi-chunk batch must round-trip exactly");
    // Every buffer reachable and inside the mapping (buffers land in >1 chunk).
    let checked = assert_buffers_in_segment(&out, &fx.segment);
    assert_eq!(checked, 6, "all six value buffers reachable across chunks");
}

#[test]
fn nested_sliced_combined_round_trips() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    // List<Int32> of length 5, sliced to the middle 3 rows — a sliced nested
    // column whose child values/offsets are sub-windows the writer must copy
    // faithfully so the logical slice survives.
    let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4, 6, 7, 8]));
    let field = Arc::new(Field::new("item", DataType::Int32, false));
    let list = ListArray::new(field.clone(), offsets, Arc::new(values), None);
    let sliced = list.slice(1, 3);

    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "l",
        DataType::List(field),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(sliced.clone())]).unwrap();
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));

    let (out, _descs) = stage_and_read(&pool, &alloc, &registry, &fx.segment, &batch);
    assert_eq!(&out, &batch, "sliced nested column round-trips to its logical slice");
    // The reconstructed list holds exactly the middle 3 lists: [[3,4],[5,6],[7]].
    let got = out
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(got.len(), 3);
}

/// Regression (v0.4 stage N): a corrupt node table whose `data_buffers` count
/// exceeds the buffer table must be rejected with a clean `Err`, never panic.
///
/// Before the fix, `make_buffer(idx)` indexed `entries[idx]` directly, so a
/// node claiming more buffers than `buffer_count` triggered an out-of-bounds
/// index panic mid-walk (before the end-of-walk consistency check could fire).
#[test]
fn corrupt_node_buffer_count_is_rejected_not_paniced() {
    let fx = Fixture::new(1 << 20);
    let pool = Pool::create(&fx.segment, &PoolConfig::power_of_two(1024, 1 << 16, 8)).unwrap();
    let alloc = PoolAllocator::new(&pool, &fx.segment);

    // Single non-nullable Int64 column: node_count = 1, buffer_count = 1.
    let schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let registry = SchemaRegistry::with_schemas(std::slice::from_ref(&schema));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))])
            .unwrap();

    let desc = write_batch(&alloc, &registry, &batch).unwrap();
    let ctrl = pool.ctrl(&desc).unwrap();
    ctrl.try_loan(7).unwrap();
    ctrl.publish().unwrap();

    // Corrupt the single NodeEntry's `data_buffers` field (4th u32 of the 24-byte
    // node, which sits at chunk offset 32) to demand 99 buffers.
    let base = fx.segment.resolve(desc.offset, desc.len).unwrap();
    // node table starts at size_of::<BatchHeader>() == 32; data_buffers at +12.
    unsafe {
        base.add(32 + 12).cast::<u32>().write_unaligned(99u32);
    }

    let ctrl = pool.ctrl(&desc).unwrap();
    let pin = Arc::new(PinGuard::new(fx.segment.clone()));
    let out = read_batch_chunks(pin, std::slice::from_ref(&desc), &[ctrl], &registry);
    assert!(
        matches!(out, Err(shm_arrow::Error::Layout(_))),
        "corrupt node table must be a clean Layout error, got {out:?}"
    );
}
