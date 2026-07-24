//! Shared demo payload: the one schema + batch the walking skeleton moves.
//!
//! The producer and consumer are separate OS processes, so they must seed their
//! [`SchemaRegistry`](shm_arrow::SchemaRegistry)s *identically* for the interned
//! `schema_id` to agree across the socket (v0.1's in-process registry contract,
//! ADR-0001). This module is the single source of that schema and of the batch
//! contents, so both sides — and the test — stay in lockstep.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

/// The demo topic name.
pub const DEMO_TOPIC: &str = "demo";

/// The cache-loop artifact name (v0.2 walking skeleton).
pub const CACHE_ARTIFACT: &str = "cache";

/// The demo batch's row count.
pub const DEMO_ROWS: usize = 4;

/// The value a worker derives from a pinned version of [`demo_batch`]: the sum
/// of the `id` column (`10+20+30+40 = 100`).
///
/// Computing it requires the worker to have pinned the version and reconstructed
/// the batch zero-copy via [`VersionPin::as_arrow`](shm_artifact::VersionPin),
/// so a result matching this proves the full read path.
pub const DEMO_ID_SUM: i64 = 100;

/// Derive the cache-loop result from a reconstructed batch: `(sum(id), rows)`.
///
/// Returns an error string if the batch is not the expected shape.
pub fn demo_derive(batch: &RecordBatch) -> std::result::Result<(i64, i64), String> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("column 0 is not Int64")?;
    let sum: i64 = ids.values().iter().copied().sum();
    Ok((sum, batch.num_rows() as i64))
}

/// The one schema the walking skeleton uses: `(id: Int64, name: Utf8)`.
///
/// Both processes call this to seed their registries identically.
pub fn demo_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Build the exact `RecordBatch` the producer writes and the consumer verifies.
pub fn demo_batch() -> RecordBatch {
    let ids = Int64Array::from(vec![10i64, 20, 30, 40]);
    let names = StringArray::from(vec!["alpha", "bravo", "charlie", "delta"]);
    RecordBatch::try_new(
        demo_schema(),
        vec![Arc::new(ids), Arc::new(names)],
    )
    .expect("demo batch is well formed")
}

/// The G1 result schema: a single derived scalar `(sum: Int64)` (ADR-0007 G1).
///
/// A G1 worker resolves a dispatched [`TypedRef`](shm_store::TypedRef) to a
/// dataset, derives [`DEMO_ID_SUM`] from it, and commits this one-row batch as a
/// `Result`-kind keyed entry the requester then reads back.
pub fn result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("sum", DataType::Int64, false)]))
}

/// Build the one-row [`result_schema`] batch carrying the derived `sum`.
pub fn result_batch(sum: i64) -> RecordBatch {
    RecordBatch::try_new(result_schema(), vec![Arc::new(Int64Array::from(vec![sum]))])
        .expect("result batch is well formed")
}

/// Read the single `sum` scalar back from a reconstructed [`result_batch`], or an
/// error string if the shape is wrong.
pub fn result_value(batch: &RecordBatch) -> std::result::Result<i64, String> {
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("result column 0 is not Int64")?;
    if col.len() != 1 {
        return Err(format!("result batch has {} rows, expected 1", col.len()));
    }
    Ok(col.value(0))
}

/// The nested cache-loop artifact name (item-F walking-skeleton half).
pub const NESTED_ARTIFACT: &str = "cache-nested";

/// Row count of [`nested_batch`] — chosen so its serialized buffers exceed one
/// artifact chunk (the runtime's max artifact chunk is 8 KiB), forcing a
/// **multi-chunk** version, while every individual buffer stays under one chunk.
pub const NESTED_ROWS: usize = 800;

/// The nested demo schema (item F): `s: Struct<{id: Int64, tag: Utf8}>` plus
/// `vals: List<Int64>`.
///
/// Exercises the recursive nested layout (`Struct` + `List` with child buffers)
/// through the coordinator-negotiated schema path (item E) and the multi-chunk
/// writer/reader (item F).
pub fn nested_schema() -> SchemaRef {
    let struct_fields = Fields::from(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
    ]);
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    Arc::new(Schema::new(vec![
        Field::new("s", DataType::Struct(struct_fields), false),
        Field::new("vals", DataType::List(item), false),
    ]))
}

/// Build the nested, **multi-chunk** demo batch the item-F skeleton commits.
///
/// Row `i` holds `s = {id: i, tag: "t{i}"}` and `vals = [i]` (one element per
/// row). At [`NESTED_ROWS`] rows the serialized buffers span several artifact
/// chunks, while every individual buffer stays under one chunk.
pub fn nested_batch() -> RecordBatch {
    let n = NESTED_ROWS;
    let ids = Int64Array::from((0..n as i64).collect::<Vec<_>>());
    let tags = StringArray::from((0..n).map(|i| format!("t{i}")).collect::<Vec<_>>());
    let struct_fields = match nested_schema().field(0).data_type() {
        DataType::Struct(f) => f.clone(),
        _ => unreachable!("field 0 is a struct"),
    };
    let s = StructArray::new(
        struct_fields,
        vec![Arc::new(ids), Arc::new(tags)],
        None,
    );

    // One Int64 element per row: values [0, 1, 2, ...], offsets 0,1,2,...,n.
    let values = Int64Array::from((0..n as i64).collect::<Vec<_>>());
    let offsets = OffsetBuffer::new(ScalarBuffer::from(
        (0..=n as i32).collect::<Vec<_>>(),
    ));
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    let list = ListArray::new(item, offsets, Arc::new(values), None);

    RecordBatch::try_new(nested_schema(), vec![Arc::new(s), Arc::new(list)])
        .expect("nested demo batch is well formed")
}

/// Verify a reconstructed [`nested_batch`] matches exactly (used by the consumer
/// to prove the multi-chunk nested zero-copy read produced the right bytes).
pub fn verify_nested_batch(batch: &RecordBatch) -> std::result::Result<(), String> {
    if batch.num_rows() != NESTED_ROWS {
        return Err(format!("row count {} != {NESTED_ROWS}", batch.num_rows()));
    }
    if batch.schema() != nested_schema() {
        return Err("schema mismatch vs nested_schema()".to_string());
    }
    if *batch != nested_batch() {
        return Err("nested batch content mismatch".to_string());
    }
    Ok(())
}

/// Verify a reconstructed batch matches [`demo_batch`] exactly (used by the
/// consumer to prove the zero-copy read produced the right bytes).
///
/// Returns `Ok(())` on an exact match, or a descriptive error string.
pub fn verify_demo_batch(batch: &RecordBatch) -> std::result::Result<(), String> {
    if batch.num_rows() != DEMO_ROWS {
        return Err(format!("row count {} != {DEMO_ROWS}", batch.num_rows()));
    }
    if batch.num_columns() != 2 {
        return Err(format!("column count {} != 2", batch.num_columns()));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("column 0 is not Int64")?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("column 1 is not Utf8")?;

    let expect_ids = [10i64, 20, 30, 40];
    let expect_names = ["alpha", "bravo", "charlie", "delta"];
    for i in 0..DEMO_ROWS {
        if ids.value(i) != expect_ids[i] {
            return Err(format!("id[{i}] = {} != {}", ids.value(i), expect_ids[i]));
        }
        if names.value(i) != expect_names[i] {
            return Err(format!(
                "name[{i}] = {:?} != {:?}",
                names.value(i),
                expect_names[i]
            ));
        }
    }
    Ok(())
}
