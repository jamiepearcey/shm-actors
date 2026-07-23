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
        Region { buf: vec![0u64; bytes.div_ceil(8)] }
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
    ChunkDesc { schema_id: i, ..ChunkDesc::ZERO }
}

pub fn run() {
    println!("\n== TASK queue (macOS dev profile) ==");

    // NOTE: `claim` (and `submit`) scan the slot array for a QUEUED/reusable
    // slot, so both are O(capacity). We therefore benchmark at a small, realistic
    // queue capacity — a deep queue would measure the scan, not the state machine.

    // ---- single-thread round-trip latency ----
    let capacity = 256u32;
    let region = Region::for_capacity(capacity);
    // SAFETY: `region` outlives the queue handle (dropped at end of function).
    let queue = unsafe { TaskQueue::init(region.base(), region.len(), capacity) }.expect("init");
    let result = ChunkDesc { schema_id: 999, ..ChunkDesc::ZERO };
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
    for workers in [1usize, 2, 4] {
        let rate = throughput(200_000, workers);
        println!("throughput, {workers} worker(s): {} tasks/sec", fmt_rate(rate));
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
    let result = ChunkDesc { schema_id: 7, ..ChunkDesc::ZERO };
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
