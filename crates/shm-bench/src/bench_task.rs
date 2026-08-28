//! Task-queue benchmarks: submit->claim->complete->poll round-trip latency
//! (single thread) and end-to-end throughput with a few worker threads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use shm_core::ChunkDesc;
use shm_task::{now_nanos, required_bytes, Error as TaskError, TaskQueue, TaskStatus};

use crate::stats::{fmt_rate, measure};

/// An 8-byte-aligned heap region standing in for a shm segment payload (matches
/// how the crate's own tests drive `TaskQueue`).
struct Region {
    buf: Vec<u64>,
}
impl Region {
    fn for_capacity(capacity: u32) -> Region {
        let bytes = required_bytes(capacity) + 64;
        Region {
            buf: vec![0u64; bytes.div_ceil(8)],
        }
    }
    fn base(&self) -> *mut u8 {
        self.buf.as_ptr() as *mut u8
    }
    fn len(&self) -> usize {
        self.buf.len() * 8
    }
}

fn future() -> u64 {
    now_nanos() + Duration::from_secs(3600).as_nanos() as u64
}

fn req(i: u32) -> ChunkDesc {
    ChunkDesc {
        schema_id: i,
        ..ChunkDesc::ZERO
    }
}

pub fn run() {
    println!("\n== TASK queue (macOS dev profile) ==");

    // ADR-0009 (P0.2): `submit` and `claim` discover slots through the O(1)
    // FREE/READY Treiber stacks, so queue capacity no longer taxes either path;
    // the capacity-scaling section below is the direct evidence (must be flat).

    // ---- single-thread round-trip latency ----
    let capacity = 256u32;
    let region = Region::for_capacity(capacity);
    // SAFETY: `region` outlives the queue handle (dropped at end of function).
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");
    let result = ChunkDesc {
        schema_id: 999,
        ..ChunkDesc::ZERO
    };
    let mut i = 0u32;
    let s = measure(20_000, 200_000, || {
        i = i.wrapping_add(1);
        let h = queue.submit(req(i), future()).expect("submit");
        let claimed = queue.claim(1).expect("claim");
        claimed.complete(result).expect("complete");
        matches!(queue.poll(h).expect("poll"), TaskStatus::Done(_))
    });
    println!("submit->claim->complete->poll round-trip, single thread:");
    println!("  {}", s.line_ns());

    // ---- multi-worker throughput ----
    // One producer feeding N workers is bounded by the producer, so this shape
    // can only ever show whether extra workers *hurt* it.
    println!("throughput, 1 producer -> N workers (producer-bound by construction):");
    for workers in [1usize, 2, 4] {
        let rate = throughput(200_000, workers);
        println!(
            "  {workers} worker(s): {} tasks/sec",
            fmt_rate(rate)
        );
    }
    // N producers feeding N workers is the shape that can scale. At capacity
    // 256 producers spin on QueueFull; 4096 removes that backpressure so the
    // queue itself is what is measured.
    for cap in [256u32, 4096] {
        println!("throughput, N producers -> N workers (aggregate), capacity {cap}:");
        for pairs in [1usize, 2, 4] {
            let rate = throughput_pairs_cap(200_000, pairs, cap);
            println!("  {pairs} pair(s): {} tasks/sec", fmt_rate(rate));
        }
    }

    // ---- isolation: pure READY-head contention ----
    // Pre-fill, then N workers drain with NO producers and NO shared counter.
    // If aggregate still falls with N, the single Treiber head is the
    // serialization point and nothing else is.
    println!("drain, pre-filled 60000, N workers, no producers, per-worker counters:");
    for workers in [1usize, 2, 4, 8] {
        let rate = drain(60_000, workers, true);
        println!("  {workers} worker(s), claim+complete: {} tasks/sec", fmt_rate(rate));
    }
    for workers in [1usize, 2, 4, 8] {
        let rate = drain(60_000, workers, false);
        println!("  {workers} worker(s), claim only:     {} tasks/sec", fmt_rate(rate));
    }

    // ---- capacity scaling (ADR-0009: claim must be O(1), flat in capacity) ----
    println!("capacity scaling, one queued task (claim p50 must be flat):");
    for cap in [256u32, 65_536] {
        let region = Region::for_capacity(cap);
        // SAFETY: `region` outlives the queue handle (dropped each iteration).
        let queue = unsafe { TaskQueue::init(region.base(), region.len(), cap) }.expect("init");
        let result = ChunkDesc {
            schema_id: 999,
            ..ChunkDesc::ZERO
        };
        // Keep exactly one task queued: claim+complete it, then resubmit, so
        // the timed claim always has one READY node to pop.
        let mut i = 0u32;
        queue.submit(req(0), future()).expect("seed submit");
        let s = measure(20_000, 200_000, || {
            let claimed = queue.claim(1).expect("claim");
            claimed.complete(result).expect("complete");
            i = i.wrapping_add(1);
            queue.submit(req(i), future()).expect("resubmit")
        });
        println!("  capacity {cap:>6}: claim+complete+submit {}", s.line_ns());
        // Drain the one outstanding task so the probe below measures an
        // actually-empty queue.
        if let Some(t) = queue.claim(2) {
            t.complete(result).expect("drain");
        }
        let s = measure(20_000, 200_000, || queue.claim(2).is_none());
        println!("  capacity {cap:>6}: empty-claim probe     {}", s.line_ns());
    }
}

/// Pipelined throughput: the main thread streams `n` submits under backpressure
/// (retrying on `QueueFull`) into a small queue while `workers` threads claim +
/// complete concurrently. Returns tasks/sec from first submit to all `n` done.
fn throughput(n: usize, workers: usize) -> f64 {
    let capacity = 256u32; // small so claim/submit scans stay cheap and realistic
    let region = Arc::new(Region::for_capacity(capacity));
    // SAFETY: `region` (Arc) is kept alive until after every worker joins.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");
    let done = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(workers + 1));
    let result = ChunkDesc {
        schema_id: 7,
        ..ChunkDesc::ZERO
    };
    let mut handles = Vec::new();
    for w in 1..=workers as u32 {
        let q = queue.clone();
        let done = done.clone();
        let barrier = barrier.clone();
        let keep = region.clone();
        handles.push(thread::spawn(move || {
            let _keep = keep; // hold the region alive in this thread
            barrier.wait();
            while done.load(Ordering::Acquire) < n {
                if let Some(task) = q.claim(w) {
                    task.complete(result).expect("complete");
                    done.fetch_add(1, Ordering::AcqRel);
                } else {
                    // An idle worker that hammers the READY head is measuring
                    // its own cache-line contention, not the queue.
                    std::hint::spin_loop();
                }
            }
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    let mut i = 0u32;
    let mut submitted = 0usize;
    while submitted < n {
        match queue.submit(req(i), future()) {
            Ok(_) => {
                submitted += 1;
                i = i.wrapping_add(1);
            }
            Err(TaskError::QueueFull) => std::hint::spin_loop(),
            Err(e) => panic!("submit failed: {e:?}"),
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    n as f64 / dt
}

/// `pairs` producer threads each stream `n / pairs` submits under backpressure
/// while `pairs` worker threads claim + complete. Aggregate tasks/sec across
/// all pairs — the shape that can actually scale with cores, so a fall here is
/// the queue's fault, not the harness's.
fn throughput_pairs_cap(n: usize, pairs: usize, capacity: u32) -> f64 {
    let region = Arc::new(Region::for_capacity(capacity));
    // SAFETY: `region` (Arc) is kept alive until after every thread joins.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");
    let done = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(pairs * 2 + 1));
    let result = ChunkDesc {
        schema_id: 7,
        ..ChunkDesc::ZERO
    };
    let per = n / pairs;
    let total = per * pairs;
    let mut handles = Vec::new();
    for w in 1..=pairs as u32 {
        let q = queue.clone();
        let done = done.clone();
        let barrier = barrier.clone();
        let keep = region.clone();
        handles.push(thread::spawn(move || {
            let _keep = keep;
            barrier.wait();
            while done.load(Ordering::Acquire) < total {
                if let Some(task) = q.claim(w) {
                    task.complete(result).expect("complete");
                    done.fetch_add(1, Ordering::AcqRel);
                } else {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    for p in 0..pairs as u32 {
        let q = queue.clone();
        let barrier = barrier.clone();
        let keep = region.clone();
        handles.push(thread::spawn(move || {
            let _keep = keep;
            barrier.wait();
            let mut i = p;
            let mut submitted = 0usize;
            while submitted < per {
                match q.submit(req(i), future()) {
                    Ok(_) => {
                        submitted += 1;
                        i = i.wrapping_add(pairs as u32);
                    }
                    Err(TaskError::QueueFull) => std::hint::spin_loop(),
                    Err(e) => panic!("submit failed: {e:?}"),
                }
            }
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    total as f64 / dt
}

/// Pre-fill `n` tasks into a 65536-slot queue, then `workers` threads drain it
/// with no producer and no shared counter (each keeps a local count; the main
/// thread sums after join). `complete` toggles whether the worker also pushes
/// the slot back to FREE, isolating READY-head contention from FREE-head.
fn drain(n: usize, workers: usize, complete: bool) -> f64 {
    let capacity = 65_536u32;
    let region = Arc::new(Region::for_capacity(capacity));
    // SAFETY: `region` (Arc) is kept alive until after every worker joins.
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");
    for i in 0..n as u32 {
        queue.submit(req(i), future()).expect("prefill");
    }
    let barrier = Arc::new(Barrier::new(workers + 1));
    let result = ChunkDesc {
        schema_id: 7,
        ..ChunkDesc::ZERO
    };
    let mut handles = Vec::new();
    for w in 1..=workers as u32 {
        let q = queue.clone();
        let barrier = barrier.clone();
        let keep = region.clone();
        handles.push(thread::spawn(move || {
            let _keep = keep;
            barrier.wait();
            let t0 = Instant::now();
            let mut local = 0usize;
            let mut claimed: Vec<_> = Vec::new();
            while let Some(task) = q.claim(w) {
                local += 1;
                if complete {
                    task.complete(result).expect("complete");
                } else {
                    claimed.push(task);
                }
            }
            let dt = t0.elapsed().as_secs_f64();
            drop(claimed);
            (local, dt)
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    let per: Vec<(usize, f64)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let dt = t0.elapsed().as_secs_f64();
    let total: usize = per.iter().map(|p| p.0).sum();
    assert_eq!(total, n, "every prefilled task was claimed exactly once");
    // Per-worker rates: the counter that separates contention (every worker
    // uniformly slow) from core heterogeneity / placement (workers unequal).
    let mut rates: Vec<f64> = per.iter().map(|&(c, d)| c as f64 / d).collect();
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let counts: Vec<usize> = per.iter().map(|p| p.0).collect();
    println!(
        "      per-worker rate min={} max={}  counts={:?}",
        fmt_rate(rates[0]),
        fmt_rate(rates[rates.len() - 1]),
        counts
    );
    total as f64 / dt
}
