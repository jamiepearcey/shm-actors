//! shm-arrow benchmarks: the "one copy in" write_batch cost (GB/s, rows/s) vs
//! the zero-copy read_batch reconstruct (independent of row count).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_arrow::{read_batch, serialized_len, write_batch, PinGuard, PoolAllocator, SchemaRegistry};
use shm_core::{Pool, PoolConfig, Segment};

use crate::stats::{fmt_rate, Stats};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Float64, false),
        Field::new("d", DataType::Float64, false),
    ]))
}

/// 4 fixed-width columns = 32 payload bytes/row.
fn batch(rows: usize) -> RecordBatch {
    let a = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
    let b = Int64Array::from((0..rows as i64).map(|x| x * 2).collect::<Vec<_>>());
    let c = Float64Array::from((0..rows).map(|x| x as f64).collect::<Vec<_>>());
    let d = Float64Array::from((0..rows).map(|x| x as f64 * 0.5).collect::<Vec<_>>());
    RecordBatch::try_new(schema(), vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)]).unwrap()
}

fn next_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    63_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

fn make_seg(size: usize) -> (Arc<Segment>, u32) {
    let id = next_id();
    let _ = Segment::unlink_by_id(id);
    (Arc::new(Segment::create(id, size).unwrap()), id)
}

pub fn run() {
    println!("\n== SHM-ARROW write/read (macOS dev profile) ==");
    let reg = SchemaRegistry::with_schemas(std::slice::from_ref(&schema()));

    println!("write_batch (one copy in) — bytes copied = serialized_len:");
    for &rows in &[1_000usize, 100_000, 1_000_000] {
        let b = batch(rows);
        let bytes = serialized_len(&b).expect("serialized_len") as f64;
        let chunk = (bytes as u32 + 4096).next_power_of_two();
        let (seg, id) = make_seg(chunk as usize * 4 + (1 << 20));
        let pool = Pool::create(&seg, &PoolConfig::power_of_two(chunk, chunk, 3)).unwrap();
        let alloc = PoolAllocator::new(&pool, &seg);

        // Warmup.
        for _ in 0..8 {
            let d = write_batch(&alloc, &reg, &b).unwrap();
            pool.free(&d).unwrap();
        }
        let rounds = if rows >= 1_000_000 { 200 } else { 2000 };
        let mut samples = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let t0 = Instant::now();
            let d = write_batch(&alloc, &reg, &b).unwrap();
            let dt = t0.elapsed().as_nanos() as f64;
            pool.free(&d).unwrap();
            samples.push(dt);
        }
        let s = Stats::from_ns(samples);
        let gbps = bytes / s.p50; // bytes/ns == GB/s
        let rows_s = rows as f64 / (s.p50 / 1e9);
        println!(
            "  rows={rows:>8} bytes={:>10.0}: p50={:>10.0}ns  {:.2} GB/s  {} rows/s",
            bytes,
            s.p50,
            gbps,
            fmt_rate(rows_s)
        );
        let _ = Segment::unlink_by_id(id);
    }

    println!("read_batch (zero-copy reconstruct) — expect ~constant vs row count:");
    for &rows in &[1_000usize, 100_000, 1_000_000] {
        let b = batch(rows);
        let bytes = serialized_len(&b).expect("serialized_len") as u32;
        let chunk = (bytes + 4096).next_power_of_two();
        let (seg, id) = make_seg(chunk as usize * 4 + (1 << 20));
        let pool = Pool::create(&seg, &PoolConfig::power_of_two(chunk, chunk, 3)).unwrap();
        let alloc = PoolAllocator::new(&pool, &seg);

        // Publish one chunk to read from.
        let desc = write_batch(&alloc, &reg, &b).unwrap();
        let ctrl = pool.ctrl(&desc).unwrap();
        ctrl.try_loan(0).unwrap();
        ctrl.publish().unwrap();
        let pin = Arc::new(PinGuard::new(seg.clone()));

        // Warmup + measure the zero-copy reconstruct.
        for _ in 0..2_000 {
            let out = read_batch(pin.clone(), ctrl, &desc, &reg).unwrap();
            std::hint::black_box(out);
        }
        let iters = 20_000;
        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t0 = Instant::now();
            let out = read_batch(pin.clone(), ctrl, &desc, &reg).unwrap();
            let dt = t0.elapsed().as_nanos() as f64;
            std::hint::black_box(out);
            samples.push(dt);
        }
        let s = Stats::from_ns(samples);
        println!("  rows={rows:>8}: {}", s.line_ns());
        let _ = Segment::unlink_by_id(id);
    }
}
