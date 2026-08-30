//! Artifact benchmarks: pin cost (O(1) vs version count), zero-copy `as_arrow`
//! reconstruct (O(1) vs row count), and commit cost for Replace vs Append.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_arrow::SchemaRegistry;
use shm_artifact::{Artifact, Commit, WindowPolicy};
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
        let (art, h, d) = make_artifact(
            (cls as usize) * 4 + (1 << 20),
            &PoolConfig::power_of_two(cls, cls, 3),
        );
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

    // ---- pin + per-batch read cost vs Append chain depth (ADR-0013) ----
    println!(
        "pin()+drop and as_arrow_batches() vs Append depth (pin flat = O(own manifest); read linear in batches only):"
    );
    for &depth in &[1u64, 100, 1000, 10_000] {
        // One 256 B class holds both a 4-row data chunk and a 92 B manifest;
        // every chain member costs exactly two chunks, so size for 2·depth.
        let pool = PoolConfig {
            classes: vec![
                SizeClass {
                    chunk_size: 256,
                    chunk_count: 2 * depth as u32 + 64,
                },
                SizeClass {
                    chunk_size: 512,
                    chunk_count: 16,
                },
            ],
        };
        let (art, h, d) = make_artifact(1 << 26, &pool);
        let small = batch(4);
        {
            let mut w = art.open_exclusive(7).unwrap();
            w.commit(Commit::Replace, &small, &reg).unwrap();
            while art.current_version() < depth {
                w.commit(Commit::Append, &small, &reg).unwrap();
            }
        }
        let pin_stats = measure(20_000, 100_000, || art.pin().unwrap());
        let pin = art.pin().unwrap();
        debug_assert_eq!(pin.manifest().depth as u64 + 1, depth);
        let read_iters = (2_000_000 / depth as usize).clamp(200, 20_000);
        let read_stats = measure(read_iters / 10, read_iters, || {
            pin.as_arrow_batches(&reg).unwrap()
        });
        println!(
            "  depth={depth:>6}: pin  {}\n                read {}",
            pin_stats.line_ns(),
            read_stats.line_ns()
        );
        drop(pin);
        cleanup(h, d);
    }

    // ---- commit latency: Replace vs Append, vs prior table size ----
    println!("commit latency vs prior version index (Replace should be flat; Append flat => O(new data)):");
    commit_series(Commit::Replace, "Replace", &reg);
    commit_series(Commit::Append, "Append", &reg);

    // ---- ADR-0016: windowed append stream ----
    println!(
        "windowed append stream (ADR-0016) — N=100000 commits of a 4-row batch under WindowPolicy::new(keep):\n\
         \x20 Append commits should be flat, a Window commit costs O(keep) reference RMWs, live chunks and reads stay bounded:"
    );
    for &keep in &[16u32, 256, 4096] {
        stream_series(keep, keep, 100_000, &reg);
    }
    println!("losing shapes (the rows that punish the design):");
    // (a) Window on EVERY commit: O(keep) RMWs per commit, no amortisation.
    stream_series(256, 1, 20_000, &reg);
    // (b) Unbounded Append at the same N as a windowed run: memory and read
    //     cost grow with history; the point of the policy.
    unbounded_series(20_000, &reg);
    // (c) A slow reader pinned before a Window keeps the whole old chain
    //     alive until it drops: the bound is per *reachable* version.
    slow_reader_series(256, 4096, &reg);
}

/// Commit `n` small batches under `WindowPolicy { keep_batches: keep, max_depth }`,
/// timing every commit and splitting the samples by kind. Then a consumer's
/// delta read (one batch behind) and the full window read.
fn stream_series(keep: u32, max_depth: u32, n: usize, reg: &SchemaRegistry) {
    let policy = WindowPolicy {
        keep_batches: keep,
        max_depth,
    };
    // A windowed root lists up to keep + max_depth single-chunk batches:
    // 24 B descriptor + 4 B span + 16 B kept member each (ADR-0016).
    let window_bytes = (64 + 44 * (keep as usize + max_depth as usize + 1)).next_power_of_two() as u32;
    let small_chunks = 4 * keep + 4 * max_depth + 256;
    let pool = PoolConfig {
        classes: vec![
            SizeClass {
                chunk_size: 256,
                chunk_count: small_chunks,
            },
            SizeClass {
                chunk_size: window_bytes.max(512),
                chunk_count: 8,
            },
        ],
    };
    let bytes = 256 * small_chunks as usize + 8 * window_bytes.max(512) as usize + (1 << 20);
    let (art, h, d) = make_artifact(bytes, &pool);
    let baseline = free_total(&art);
    let small = batch(4);
    let mut appends: Vec<f64> = Vec::with_capacity(n);
    let mut windows: Vec<f64> = Vec::with_capacity(n / max_depth.max(1) as usize + 1);
    let mut max_live = 0usize;
    {
        let mut w = art.open_exclusive(7).unwrap();
        for i in 0..n {
            let kind = policy.commit_for_depth(art.current_depth());
            let t0 = Instant::now();
            w.commit(kind.clone(), &small, reg).unwrap();
            let ns = t0.elapsed().as_nanos() as f64;
            if matches!(kind, Commit::Window { .. }) {
                windows.push(ns);
            } else {
                appends.push(ns);
            }
            if i % 4096 == 0 {
                max_live = max_live.max(baseline - free_total(&art));
            }
        }
    }
    max_live = max_live.max(baseline - free_total(&art));
    let line = |label: &str, v: Vec<f64>| -> String {
        if v.is_empty() {
            return format!("{label} (none)");
        }
        let s = Stats::from_ns(v);
        format!("{label} p50={:>8.0} p99={:>8.0} max={:>8.0}ns (n={})", s.p50, s.p99, s.max, s.n)
    };
    let a = line("Append", appends);
    let wst = line("Window", windows);
    let pin = art.pin().unwrap();
    let batches = pin.manifest().total_batches;
    let depth = pin.manifest().depth;
    // A consumer one version behind: the delta is one batch.
    let since = pin.version() - 1;
    let delta = measure(2_000, 20_000, || pin.batches_since(since, reg).unwrap());
    let full_iters = (4_000_000 / batches as usize).clamp(200, 20_000);
    let full = measure(full_iters / 10, full_iters, || pin.as_arrow_batches(reg).unwrap());
    println!("  keep={keep:>5} max_depth={max_depth:>5}: {a}  {wst}");
    println!(
        "                              live chunks max={max_live:>6} (bound {})  final table {batches} batches depth {depth}",
        2 * keep as usize + 2 * max_depth as usize + 2
    );
    println!(
        "                              delta read (1 batch behind) {}\n                              full read ({batches} batches)       {}",
        delta.line_ns(),
        full.line_ns()
    );
    drop(pin);
    cleanup(h, d);
}

/// Plain `Append` for `n` commits in a pool sized to hold them all: the
/// unbounded baseline the policy replaces.
fn unbounded_series(n: usize, reg: &SchemaRegistry) {
    let pool = PoolConfig {
        classes: vec![SizeClass {
            chunk_size: 256,
            chunk_count: 2 * n as u32 + 64,
        }],
    };
    let (art, h, d) = make_artifact(256 * (2 * n + 64) + (1 << 20), &pool);
    let baseline = free_total(&art);
    let small = batch(4);
    let mut samples: Vec<f64> = Vec::with_capacity(n);
    {
        let mut w = art.open_exclusive(7).unwrap();
        for _ in 0..n {
            let t0 = Instant::now();
            w.commit(Commit::Append, &small, reg).unwrap();
            samples.push(t0.elapsed().as_nanos() as f64);
        }
    }
    let s = Stats::from_ns(samples);
    let live = baseline - free_total(&art);
    let pin = art.pin().unwrap();
    let since = pin.version() - 1;
    let delta = measure(2_000, 20_000, || pin.batches_since(since, reg).unwrap());
    let full = measure(20, 200, || pin.as_arrow_batches(reg).unwrap());
    println!(
        "  unbounded Append N={n}: commit p50={:>6.0} p99={:>6.0}ns  live chunks {live} (grows 2/commit)\n                              delta read {}\n                              full read ({n} batches) {}",
        s.p50,
        s.p99,
        delta.line_ns(),
        full.line_ns()
    );
    drop(pin);
    cleanup(h, d);
}

/// A reader pins a version, then `extra` more commits land under the policy:
/// the pinned chain cannot be freed, so live chunks grow past the bound until
/// the pin drops — then the census falls back to the window.
fn slow_reader_series(keep: u32, extra: usize, reg: &SchemaRegistry) {
    let policy = WindowPolicy::new(keep);
    let small_chunks = 4 * keep + 2 * extra as u32 + 256;
    let window_bytes = (64 + 44 * (2 * keep as usize + 1)).next_power_of_two() as u32;
    let pool = PoolConfig {
        classes: vec![
            SizeClass {
                chunk_size: 256,
                chunk_count: small_chunks,
            },
            SizeClass {
                chunk_size: window_bytes,
                chunk_count: 64,
            },
        ],
    };
    let bytes = 256 * small_chunks as usize + 64 * window_bytes as usize + (1 << 20);
    let (art, h, d) = make_artifact(bytes, &pool);
    let baseline = free_total(&art);
    let small = batch(4);
    let mut w = art.open_exclusive(7).unwrap();
    for _ in 0..(2 * keep) {
        w.commit_windowed(&policy, &small, reg).unwrap();
    }
    let steady = baseline - free_total(&art);
    let held = art.pin().unwrap();
    for _ in 0..extra {
        w.commit_windowed(&policy, &small, reg).unwrap();
    }
    let with_pin = baseline - free_total(&art);
    drop(held);
    let after = baseline - free_total(&art);
    drop(w);
    println!(
        "  slow reader keep={keep} pinned across {extra} commits: live chunks steady {steady} -> pinned {with_pin} -> dropped {after}"
    );
    cleanup(h, d);
}

/// Total free chunks across every size class of the artifact's data pool.
fn free_total(art: &Artifact) -> usize {
    let pool = shm_core::Pool::attach(art.data_segment()).unwrap();
    (0..pool.num_classes()).map(|c| pool.free_count(c)).sum()
}

/// Commit a small batch `count` times, recording each commit's latency, then
/// print the median at several table sizes to show flatness (O(1) turnover).
fn commit_series(kind: Commit, label: &str, reg: &SchemaRegistry) {
    let count = 1000usize;
    // Append accumulates a small data chunk + a 92 B manifest per commit (they
    // pile up: ~`count` chain members live at once). With chained manifests
    // (ADR-0013) a manifest lists only its own chunks, so no class larger than
    // the data chunk is ever needed; the larger classes below are kept only so
    // the pool shape matches the pre-ADR-0013 series this compares against.
    let pool = PoolConfig {
        classes: vec![
            SizeClass {
                chunk_size: 256,
                chunk_count: 4096,
            },
            SizeClass {
                chunk_size: 512,
                chunk_count: 4096,
            },
            SizeClass {
                chunk_size: 1024,
                chunk_count: 64,
            },
            SizeClass {
                chunk_size: 2048,
                chunk_count: 64,
            },
            SizeClass {
                chunk_size: 4096,
                chunk_count: 64,
            },
            SizeClass {
                chunk_size: 8192,
                chunk_count: 32,
            },
            SizeClass {
                chunk_size: 16384,
                chunk_count: 32,
            },
            SizeClass {
                chunk_size: 32768,
                chunk_count: 16,
            },
            SizeClass {
                chunk_size: 65536,
                chunk_count: 16,
            },
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
    let windows = [
        (0usize, 10usize),
        (100, 110),
        (500, 510),
        (count - 10, count),
    ];
    print!("  {label:8}: ");
    for (lo, hi) in windows {
        let mut w: Vec<f64> = samples[lo..hi].to_vec();
        let s = Stats::from_ns(std::mem::take(&mut w));
        print!("[#{lo:>4}..{hi:>4} p50={:>8.0}ns] ", s.p50);
    }
    println!();
    cleanup(h, d);
}
