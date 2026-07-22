//! Shared demo payload: the one schema + batch the walking skeleton moves.
//!
//! The producer and consumer are separate OS processes, so they must seed their
//! [`SchemaRegistry`](shm_arrow::SchemaRegistry)s *identically* for the interned
//! `schema_id` to agree across the socket (v0.1's in-process registry contract,
//! ADR-0001). This module is the single source of that schema and of the batch
//! contents, so both sides — and the test — stay in lockstep.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

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
