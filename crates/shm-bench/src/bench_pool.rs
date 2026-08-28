//! Pool alloc/free benchmarks: the single-CAS Treiber free-list hot path.
//!
//! alloc and free are each one compare-exchange on the free-list head, so their
//! cost is far below the [`std::time::Instant`] read floor. We therefore time a
//! whole batch of `k` operations per round and divide, reporting per-op stats
//! across rounds. alloc and free are timed *separately* (the counterpart op is
//! run untimed to restore pool state), so neither number includes the other.

use std::time::Instant;

use shm_core::{ChunkDesc, PoolConfig};

use crate::fixtures::PoolFixture;
use crate::stats::{fmt_rate, Stats};

const CHUNK: u32 = 256;
const K: usize = 4096;

/// (alloc stats, free stats) in ns/op plus their ops/sec (from the medians).
pub fn run() {
    println!("\n== POOL alloc/free (macOS dev profile) ==");

    // One size class of K chunks; a full round allocates then frees all K.
    // Budget: K*(chunk payload + a ChunkCtrl word + slack) rounded up.
    let seg_size = (K * (CHUNK as usize + 64) + (1 << 16)).next_power_of_two();
    let fx = PoolFixture::new(seg_size);
    let pool = fx.pool(&PoolConfig {
        classes: vec![shm_core::SizeClass {
            chunk_size: CHUNK,
            chunk_count: K as u32,
        }],
    });

    let rounds = 2000usize;
    let warm = 100usize;
    let mut scratch: Vec<ChunkDesc> = Vec::with_capacity(K);

    // ---- alloc: time K pops, free them back untimed. ----
    let mut alloc_samples = Vec::with_capacity(rounds);
    for r in 0..(rounds + warm) {
        let t0 = Instant::now();
        for _ in 0..K {
            scratch.push(pool.alloc(CHUNK).expect("alloc"));
        }
        let dt = t0.elapsed().as_nanos() as f64 / K as f64;
        for d in scratch.drain(..) {
            pool.free(&d).expect("free");
        }
        if r >= warm {
            alloc_samples.push(dt);
        }
    }

    // ---- free: fill untimed, then time K pushes. ----
    let mut free_samples = Vec::with_capacity(rounds);
    for r in 0..(rounds + warm) {
        for _ in 0..K {
            scratch.push(pool.alloc(CHUNK).expect("alloc"));
        }
        let t0 = Instant::now();
        for d in scratch.drain(..) {
            pool.free(&d).expect("free");
        }
        let dt = t0.elapsed().as_nanos() as f64 / K as f64;
        if r >= warm {
            free_samples.push(dt);
        }
    }

    let a = Stats::from_ns(alloc_samples);
    let f = Stats::from_ns(free_samples);
    println!("alloc (amortized {K}/round): {}", a.line_ns());
    println!("      throughput ~= {} (from p50)", fmt_rate(1e9 / a.p50));
    println!("free  (amortized {K}/round): {}", f.line_ns());
    println!("      throughput ~= {} (from p50)", fmt_rate(1e9 / f.p50));
}
