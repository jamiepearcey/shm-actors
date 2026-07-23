//! Measurement helpers: percentile stats over nanosecond samples and a
//! warmup+measure latency loop.
//!
//! Every number this harness prints comes through here, so the methodology is
//! in one place: collect per-iteration durations (or per-round amortized costs),
//! sort, and report min / median / p99 / max / mean. Percentiles are the
//! *nearest-rank* value on the sorted sample (`sorted[ceil(q*n)-1]`), which for
//! p50/p99 over thousands of samples is indistinguishable from interpolation.

use std::hint::black_box;
use std::time::Instant;

/// Latency/where-cost summary over a sample of nanosecond measurements.
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    /// Number of samples.
    pub n: usize,
    /// Smallest sample (ns).
    pub min: f64,
    /// Median / p50 (ns).
    pub p50: f64,
    /// p99 (ns).
    pub p99: f64,
    /// Largest sample (ns).
    pub max: f64,
    /// Arithmetic mean (ns).
    pub mean: f64,
}

impl Stats {
    /// Summarize a sample of per-op nanosecond costs.
    pub fn from_ns(mut samples: Vec<f64>) -> Stats {
        assert!(!samples.is_empty(), "no samples");
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = samples.len();
        let rank = |q: f64| {
            let i = ((q * n as f64).ceil() as usize).max(1) - 1;
            samples[i.min(n - 1)]
        };
        let mean = samples.iter().sum::<f64>() / n as f64;
        Stats { n, min: samples[0], p50: rank(0.50), p99: rank(0.99), max: samples[n - 1], mean }
    }

    /// Render as a compact one-line latency summary in nanoseconds.
    pub fn line_ns(&self) -> String {
        format!(
            "n={:>7} min={:>9.1} p50={:>9.1} p99={:>9.1} max={:>10.1} mean={:>9.1}  (ns)",
            self.n, self.min, self.p50, self.p99, self.max, self.mean
        )
    }
}

/// Run `warmup` untimed iterations, then time `iters` iterations of `op`,
/// returning the per-iteration nanosecond [`Stats`].
///
/// `op` is called once per timed iteration and its result is fed to
/// [`black_box`] so the optimizer cannot elide the work. The clock is
/// [`Instant`] (mach_absolute_time on macOS), read immediately around `op`.
pub fn measure<T>(warmup: usize, iters: usize, mut op: impl FnMut() -> T) -> Stats {
    for _ in 0..warmup {
        black_box(op());
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let r = op();
        let dt = t0.elapsed().as_nanos() as f64;
        black_box(r);
        samples.push(dt);
    }
    Stats::from_ns(samples)
}

/// Format a throughput figure (ops/sec) with a human-readable magnitude suffix.
pub fn fmt_rate(ops_per_sec: f64) -> String {
    if ops_per_sec >= 1e9 {
        format!("{:.2} G/s", ops_per_sec / 1e9)
    } else if ops_per_sec >= 1e6 {
        format!("{:.2} M/s", ops_per_sec / 1e6)
    } else if ops_per_sec >= 1e3 {
        format!("{:.2} K/s", ops_per_sec / 1e3)
    } else {
        format!("{ops_per_sec:.1} /s")
    }
}
