//! Artifact benchmarks: pin cost (O(1) vs version count), zero-copy `as_arrow`
//! reconstruct (O(1) vs row count), and commit cost for Replace vs Append.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_artifact::{Artifact, Commit};
use shm_arrow::SchemaRegistry;
use shm_core::{PoolConfig, Segment, SizeClass};

use crate::stats::{measure, Stats};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn registry() -> SchemaRegistry {
    SchemaRegistry::with_schemas(std::slice::from_ref(&schema()))
}

fn batch(rows: usize) -> RecordBatch {
    let a = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
    RecordBatch::try_new(schema(), vec![Arc::new(a)]).unwrap()
}

fn next_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    62_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Build an artifact over fresh head+data segments; returns it plus the segment
/// ids so the caller can unlink them.
fn make_artifact(data_bytes: usize, pool: &PoolConfig) -> (Artifact, u32, u32) {
    let head_id = next_id();
    let data_id = next_id();
    let _ = Segment::unlink_by_id(head_id);
    let _ = Segment::unlink_by_id(data_id);
    let head_seg = Arc::new(Segment::create(head_id, 1 << 16).unwrap());
    let data_seg = Arc::new(Segment::create(data_id, data_bytes).unwrap());
    let art = Artifact::create(1, head_seg, data_seg, pool).unwrap();
    (art, head_id, data_id)
}

fn cleanup(a: u32, b: u32) {
    let _ = Segment::unlink_by_id(a);
    let _ = Segment::unlink_by_id(b);
}

pub fn run() {
    println!("\n== ARTIFACT (macOS dev profile) ==");
    let reg = registry();

    // ---- pin()+drop cost vs number of committed versions ----
    println!("pin()+drop latency vs committed-version count (expect ~constant, O(1)):");
    for &versions in &[1u64, 100, 1000, 10_000] {
        let (art, h, d) = make_artifact(1 << 20, &PoolConfig::power_of_two(1024, 4096, 64));
        let small = batch(4);
        {
            let mut w = art.open_exclusive(7).unwrap();
            while art.current_version() < versions {
                w.commit(Commit::Replace, &small, &reg).unwrap();
            }
        }
        let s = measure(20_000, 100_000, || art.pin().unwrap());
        println!("  versions={versions:>6}: {}", s.line_ns());
        cleanup(h, d);
    }

    // ---- as_arrow() reconstruct cost vs row count (zero-copy => O(1)) ----
    println!("as_arrow() zero-copy reconstruct latency vs row count (expect ~constant):");
    for &rows in &[1_000usize, 100_000, 1_000_000] {
        // Int64 column: 8 bytes/row. Give the pool a class big enough for one chunk.
        let need = rows * 8 + 4096;
        let cls = (need as u32).next_power_of_two();
        let (art, h, d) = make_artifact((cls as usize) * 4 + (1 << 20), &PoolConfig::power_of_two(cls, cls, 3));
        {
            let mut w = art.open_exclusive(7).unwrap();
            w.commit(Commit::Replace, &batch(rows), &reg).unwrap();
        }
        let pin = art.pin().unwrap();
        let s = measure(2_000, 20_000, || pin.as_arrow(&reg).unwrap());
        println!("  rows={rows:>8}: {}", s.line_ns());
        drop(pin);
        cleanup(h, d);
    }

    // ---- commit latency: Replace vs Append, vs prior table size ----
    println!("commit latency vs prior version index (Replace should be flat; Append flat => O(new data)):");
    commit_series(Commit::Replace, "Replace", &reg);
    commit_series(Commit::Append, "Append", &reg);
}

/// Commit a small batch `count` times, recording each commit's latency, then
/// print the median at several table sizes to show flatness (O(1) turnover).
fn commit_series(kind: Commit, label: &str, reg: &SchemaRegistry) {
    let count = 1000usize;
    // Append accumulates a small data chunk per commit (they pile up: ~`count`
    // live at once) and rewrites the version manifest, which grows O(#chunks) —
    // so late manifests need progressively larger chunk classes (only a couple
    // are live at a time). Small classes get many chunks; big classes get few.
    let pool = PoolConfig {
        classes: vec![
            SizeClass { chunk_size: 256, chunk_count: 4096 },
            SizeClass { chunk_size: 512, chunk_count: 4096 },
            SizeClass { chunk_size: 1024, chunk_count: 64 },
            SizeClass { chunk_size: 2048, chunk_count: 64 },
            SizeClass { chunk_size: 4096, chunk_count: 64 },
            SizeClass { chunk_size: 8192, chunk_count: 32 },
            SizeClass { chunk_size: 16384, chunk_count: 32 },
            SizeClass { chunk_size: 32768, chunk_count: 16 },
            SizeClass { chunk_size: 65536, chunk_count: 16 },
        ],
    };
    let (art, h, d) = make_artifact(1 << 26, &pool);
    let small = batch(4);
    let mut samples: Vec<f64> = Vec::with_capacity(count);
    {
        let mut w = art.open_exclusive(7).unwrap();
        for _ in 0..count {
            let t0 = Instant::now();
            w.commit(kind.clone(), &small, reg).unwrap();
            samples.push(t0.elapsed().as_nanos() as f64);
        }
    }
    // Windowed medians to expose any growth with table size.
    let windows = [(0usize, 10usize), (100, 110), (500, 510), (count - 10, count)];
    print!("  {label:8}: ");
    for (lo, hi) in windows {
        let mut w: Vec<f64> = samples[lo..hi].to_vec();
        let s = Stats::from_ns(std::mem::take(&mut w));
        print!("[#{lo:>4}..{hi:>4} p50={:>8.0}ns] ", s.p50);
    }
    println!();
    cleanup(h, d);
}
