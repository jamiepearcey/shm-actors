//! The `curve` cell: a 64-tenor zero curve as an Arrow batch, and the
//! interpolation the pricer runs against a pinned version of it.

use std::sync::Arc;

use arrow_array::{Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// The cell's key in the keyed store.
pub const CURVE_KEY: &[u8] = b"curve";

/// Number of tenor points.
pub const CURVE_TENORS: usize = 64;

/// The curve schema: `(tenor: Float64 years, rate: Float64 continuously compounded)`.
pub fn curve_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tenor", DataType::Float64, false),
        Field::new("rate", DataType::Float64, false),
    ]))
}

/// Tenor `i` of the grid: 0.25y … 30y, evenly spaced.
fn tenor_at(i: usize) -> f64 {
    0.25 + i as f64 * (30.0 - 0.25) / (CURVE_TENORS as f64 - 1.0)
}

/// Build the curve with a parallel shift of `bump_bp` basis points over the
/// base curve (`3% + 2bp per point`), so a re-publish is a visible market move.
pub fn curve_batch(bump_bp: f64) -> RecordBatch {
    let tenors: Vec<f64> = (0..CURVE_TENORS).map(tenor_at).collect();
    let rates: Vec<f64> = (0..CURVE_TENORS)
        .map(|i| 0.03 + 0.0002 * i as f64 + bump_bp * 1e-4)
        .collect();
    RecordBatch::try_new(
        curve_schema(),
        vec![
            Arc::new(Float64Array::from(tenors)),
            Arc::new(Float64Array::from(rates)),
        ],
    )
    .expect("curve batch is well formed")
}

/// Linearly interpolate the rate at `tenor` from a curve batch (flat
/// extrapolation beyond the ends).
pub fn interpolate(batch: &RecordBatch, tenor: f64) -> Result<f64, String> {
    let t = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("column 0 is not Float64")?;
    let r = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("column 1 is not Float64")?;
    let n = t.len();
    if n == 0 || r.len() != n {
        return Err("empty or ragged curve".into());
    }
    let tv = t.values();
    let rv = r.values();
    if tenor <= tv[0] {
        return Ok(rv[0]);
    }
    if tenor >= tv[n - 1] {
        return Ok(rv[n - 1]);
    }
    // Partition point: first tenor >= requested.
    let hi = tv.partition_point(|&x| x < tenor);
    let lo = hi - 1;
    let w = (tenor - tv[lo]) / (tv[hi] - tv[lo]);
    Ok(rv[lo] + w * (rv[hi] - rv[lo]))
}

/// Discount `notional` at `rate` for `tenor` years.
#[inline]
pub fn price(rate: f64, tenor: f64, notional: f64) -> f64 {
    notional * (-rate * tenor).exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolation_is_linear_and_flat_at_ends() {
        let b = curve_batch(0.0);
        assert_eq!(b.num_rows(), CURVE_TENORS);
        let r0 = interpolate(&b, 0.0).unwrap();
        assert!((r0 - 0.03).abs() < 1e-12);
        let r_end = interpolate(&b, 100.0).unwrap();
        assert!((r_end - (0.03 + 0.0002 * 63.0)).abs() < 1e-12);
        let mid = (tenor_at(10) + tenor_at(11)) / 2.0;
        let rm = interpolate(&b, mid).unwrap();
        assert!((rm - (0.03 + 0.0002 * 10.5)).abs() < 1e-12);
        let bumped = curve_batch(25.0);
        assert!((interpolate(&bumped, mid).unwrap() - rm - 0.0025).abs() < 1e-12);
        assert!((price(0.05, 2.0, 100.0) - 100.0 * (-0.1f64).exp()).abs() < 1e-9);
    }
}
