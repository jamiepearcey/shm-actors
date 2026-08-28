//! Ring pub/sub benchmarks — the headline latency and throughput numbers.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use shm_core::{doorbell_pair, ChunkDesc};
use shm_ring::{DoorbellNotifier, DoorbellParker, Msg, Publisher, Subscriber};

use crate::fixtures::RingFixture;
use crate::stats::{fmt_rate, measure, Stats};

fn desc(i: u64) -> ChunkDesc {
    ChunkDesc {
        offset: i as u32,
        ..ChunkDesc::ZERO
    }
}

/// Single-thread publish→try_recv of one 24-byte descriptor. This is the pure
/// instruction cost of the hot path (release store + acquire load) with both
/// ends on the same core (cache-hot): a lower bound on ring latency.
pub fn same_core_latency(warmup: usize, iters: usize) -> Stats {
    let fx = RingFixture::new(1024);
    let publisher = Publisher::new(fx.ring.clone());
    let mut sub = Subscriber::new(fx.ring.clone());
    let mut i = 0u64;
    measure(warmup, iters, || {
        i += 1;
        publisher.publish(desc(i));
        sub.try_recv().expect("sample")
    })
}

/// Two-thread busy-poll ping-pong across cores: producer publishes a token on
/// ring A and busy-polls ring B for the echo; the peer thread busy-polls A and
/// echoes on B. We time the full round-trip in the producer thread (one clock,
/// no cross-thread clock skew) and report **RTT/2** as the one-way cross-core
/// ring-hop latency. This is the truest in-process latency for "producer on one
/// core, consumer on another".
pub fn cross_core_latency(warmup: usize, iters: usize) -> Stats {
    let a = RingFixture::new(1024);
    let b = RingFixture::new(1024);
    let ring_a = a.ring.clone();
    let ring_b = b.ring.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_peer = stop.clone();
    let ready = Arc::new(AtomicBool::new(false));
    let ready_peer = ready.clone();

    // Peer: busy-poll A (from_start so no ping is missed), echo on B.
    let peer = thread::spawn(move || {
        let pub_b = Publisher::new(ring_b);
        let mut sub_a = Subscriber::from_start(ring_a);
        ready_peer.store(true, Ordering::Release);
        while !stop_peer.load(Ordering::Relaxed) {
            if let Some(Msg::Sample(d)) = sub_a.try_recv() {
                pub_b.publish(d);
            }
        }
    });
    // Ensure the peer has subscribed before the first publish.
    while !ready.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }

    let pub_a = Publisher::new(a.ring.clone());
    let mut sub_b = Subscriber::from_start(b.ring.clone());
    let mut i = 0u64;
    let mut round = || {
        i += 1;
        let t0 = Instant::now();
        pub_a.publish(desc(i));
        loop {
            if let Some(Msg::Sample(d)) = sub_b.try_recv() {
                black_box(d);
                break;
            }
        }
        // Half the round-trip = one-way ring hop.
        (t0.elapsed().as_nanos() as f64) / 2.0
    };
    for _ in 0..warmup {
        round();
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        samples.push(round());
    }
    stop.store(true, Ordering::Relaxed);
    // Unblock the peer's busy loop by publishing once more if needed.
    pub_a.publish(desc(i + 1));
    peer.join().unwrap();
    Stats::from_ns(samples)
}

/// Sustained single-thread publish throughput: how fast one producer can push
/// 24-byte descriptors through the ring (no consumer). This is the "descriptors
/// per second per SPMC ring" figure — publish is wait-free and never blocks.
pub fn publish_throughput(n: u64) -> f64 {
    let fx = RingFixture::new(1 << 16);
    let publisher = Publisher::new(fx.ring.clone());
    // Warmup.
    for i in 0..(n / 8) {
        publisher.publish(desc(i));
    }
    let t0 = Instant::now();
    for i in 0..n {
        publisher.publish(desc(i));
    }
    let dt = t0.elapsed().as_secs_f64();
    n as f64 / dt
}

/// Sustained 1-producer / `consumers`-consumer end-to-end throughput with all
/// ends busy-polling: producer publishes `n` descriptors while each consumer
/// drains to `n` (counting lag skips). Returns descriptors/sec measured from
/// the producer's first publish until every consumer has observed `n`.
pub fn broadcast_throughput(n: u64, consumers: usize) -> (f64, u64) {
    let fx = RingFixture::new(1 << 16);
    let barrier = Arc::new(Barrier::new(consumers + 1));
    let total_skipped = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..consumers {
        // Subscribe from start so no message is missed before the producer runs.
        let mut sub = Subscriber::from_start(fx.ring.clone());
        let barrier = barrier.clone();
        let total_skipped = total_skipped.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut got = 0u64;
            let mut skipped = 0u64;
            while got < n {
                match sub.try_recv() {
                    Some(Msg::Sample(_)) => got += 1,
                    Some(Msg::Lagged(s)) => {
                        got += s;
                        skipped += s;
                    }
                    None => {}
                }
            }
            total_skipped.fetch_add(skipped, Ordering::Relaxed);
        }));
    }
    let publisher = Publisher::new(fx.ring.clone());
    barrier.wait();
    let t0 = Instant::now();
    for i in 0..n {
        publisher.publish(desc(i));
    }
    for h in handles {
        h.join().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    (n as f64 / dt, total_skipped.load(Ordering::Relaxed))
}

/// Doorbell wakeup latency: a subscriber parked in `recv` on a pipe-backed
/// doorbell (the idle path — a `poll(2)` syscall), woken by a publish on another
/// thread. Both timestamps are taken in the *same process* (producer captures t0
/// right before publish; consumer captures t1 on receive and returns the delta),
/// so there is no cross-thread clock skew. This is the honest microsecond-scale
/// idle wakeup path, distinct from the busy-poll nanosecond path above.
pub fn doorbell_latency(rounds: usize) -> Stats {
    let fx = RingFixture::new(1024);
    let db = doorbell_pair().expect("doorbell pair");
    let write_fd = {
        use std::os::fd::AsRawFd;
        db.write.as_raw_fd()
    };
    let parker = DoorbellParker::new(db.read.try_clone().expect("dup read"));
    let mut sub = Subscriber::from_start_with_parker(fx.ring.clone(), parker);

    // Channel carries the measured wakeup delta (ns) back to the driver, and a
    // go-signal so the consumer arms itself round by round.
    let (dt_tx, dt_rx) = std::sync::mpsc::channel::<f64>();
    let (t0_tx, t0_rx) = std::sync::mpsc::channel::<Instant>();
    let ready = Arc::new(AtomicBool::new(false));
    let ready_c = ready.clone();

    let consumer = thread::spawn(move || {
        loop {
            ready_c.store(true, Ordering::Release);
            let msg = sub.recv();
            let t1 = Instant::now();
            if let Msg::Sample(d) = msg {
                if d.offset == u32::MAX {
                    break; // shutdown sentinel
                }
            }
            let t0 = t0_rx.recv().unwrap();
            let _ = dt_tx.send(t1.duration_since(t0).as_nanos() as f64);
        }
    });

    let publisher = Publisher::with_notifier(fx.ring.clone(), DoorbellNotifier::new(write_fd));
    let mut samples = Vec::with_capacity(rounds);
    // A few warmup rounds absorbed by measuring extra and dropping the first.
    let warm = 20usize;
    for i in 0..(rounds + warm) {
        // Wait until the consumer has (re)entered recv and is about to park.
        while !ready.swap(false, Ordering::AcqRel) {
            std::hint::spin_loop();
        }
        // Give the bounded spin time to exhaust and the poll() to arm.
        thread::sleep(Duration::from_millis(2));
        let t0 = Instant::now();
        publisher.publish(desc(i as u64 & 0x00ff_ffff));
        t0_tx.send(t0).unwrap();
        let dt = dt_rx.recv().unwrap();
        if i >= warm {
            samples.push(dt);
        }
    }
    // Shutdown: publish the sentinel and wake.
    ready.store(false, Ordering::Release);
    thread::sleep(Duration::from_millis(2));
    Publisher::with_notifier(fx.ring.clone(), DoorbellNotifier::new(write_fd)).publish(ChunkDesc {
        offset: u32::MAX,
        ..ChunkDesc::ZERO
    });
    consumer.join().unwrap();
    drop(db);
    Stats::from_ns(samples)
}

/// Run and print the whole ring suite.
pub fn run() {
    use std::io::Write;
    let flush = || std::io::stdout().flush().ok();

    println!("\n== RING pub/sub (macOS dev profile) ==");
    flush();

    let sc = same_core_latency(50_000, 500_000);
    println!("busy-poll latency, same-core (publish->try_recv):");
    println!("  {}", sc.line_ns());
    flush();

    let cc = cross_core_latency(50_000, 200_000);
    println!("busy-poll latency, cross-core (RTT/2, 2 threads):");
    println!("  {}", cc.line_ns());
    flush();

    let rate = publish_throughput(20_000_000);
    println!("publish throughput, single producer (no consumer):");
    println!("  {} descriptors/sec", fmt_rate(rate));
    flush();

    for c in [1usize, 2, 4] {
        let (r, skipped) = broadcast_throughput(10_000_000, c);
        println!(
            "end-to-end throughput, 1 producer / {c} consumer(s), busy-poll: {} (skipped={skipped})",
            fmt_rate(r)
        );
        flush();
    }

    let db = doorbell_latency(300);
    println!("doorbell wakeup latency, subscriber PARKED on poll(2) (idle path):");
    println!("  {}", db.line_ns());
    flush();
}
