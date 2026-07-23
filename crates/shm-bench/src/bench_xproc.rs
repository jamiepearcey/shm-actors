//! Cross-process ring benchmark: a real 2-process harness over shared memory.
//!
//! The parent creates two rings (A: parent->child, B: child->parent) in POSIX
//! shm, then `fork(2)`s. The child **re-attaches** both segments by id
//! (`shm_open` + `Ring::attach`), so it maps the same physical shm object into a
//! *distinct* address space — a genuine cross-process, cross-core measurement,
//! not a shared-thread illusion.
//!
//! - **Latency** is a busy-poll ping-pong: the parent times the full round-trip
//!   with a single clock (its own `Instant`) and reports **RTT/2** as the
//!   one-way hop, so there is no cross-process clock-sync problem.
//! - **Throughput** streams `THRU_N` descriptors A->child; the child counts to
//!   `THRU_N` (folding in any lag skips) and echoes a done marker on B carrying
//!   the skip count. The parent measures elapsed with its own clock.

use std::time::Instant;

use shm_core::{ChunkDesc, Segment};
use shm_ring::{required_bytes, Msg, Publisher, Ring, Subscriber};

use crate::fixtures::next_segment_id;
use crate::stats::{fmt_rate, Stats};

const WARMUP: u64 = 20_000;
const LAT_ROUNDS: u64 = 200_000;
const THRU_N: u64 = 5_000_000;
const CAP: u32 = 1 << 16;

fn desc(i: u64) -> ChunkDesc {
    ChunkDesc { offset: i as u32, ..ChunkDesc::ZERO }
}

/// Child: echo pings for the latency phase, then count the throughput stream and
/// signal done on ring B. Runs in the forked child; never returns to the driver.
fn child_role(ring_a: Ring, ring_b: Ring) -> ! {
    let pub_b = Publisher::new(ring_b);
    let mut sub_a = Subscriber::from_start(ring_a);

    // Phase 1: echo every ping back on B.
    let total = WARMUP + LAT_ROUNDS;
    let mut done = 0u64;
    while done < total {
        if let Some(Msg::Sample(d)) = sub_a.try_recv() {
            pub_b.publish(d);
            done += 1;
        }
    }

    // Phase 2: count THRU_N descriptors (folding in lag skips), then done marker.
    let mut got = 0u64;
    let mut skipped = 0u64;
    while got < THRU_N {
        match sub_a.try_recv() {
            Some(Msg::Sample(_)) => got += 1,
            Some(Msg::Lagged(s)) => {
                got += s;
                skipped += s;
            }
            None => {}
        }
    }
    pub_b.publish(ChunkDesc { offset: skipped as u32, ..ChunkDesc::ZERO });

    // Exit without running destructors (which is what a real worker teardown or
    // a crash would do); the parent owns unlink.
    unsafe { libc::_exit(0) }
}

/// Parent latency phase: busy-poll ping-pong, RTT/2 one-way stats.
fn parent_latency(ring_a: &Ring, ring_b: &Ring) -> Stats {
    let pub_a = Publisher::new(ring_a.clone());
    let mut sub_b = Subscriber::from_start(ring_b.clone());
    let mut i = 0u64;
    let mut round = || -> f64 {
        i += 1;
        let t0 = Instant::now();
        pub_a.publish(desc(i));
        loop {
            if let Some(Msg::Sample(_)) = sub_b.try_recv() {
                break;
            }
        }
        (t0.elapsed().as_nanos() as f64) / 2.0
    };
    for _ in 0..WARMUP {
        round();
    }
    let mut samples = Vec::with_capacity(LAT_ROUNDS as usize);
    for _ in 0..LAT_ROUNDS {
        samples.push(round());
    }
    Stats::from_ns(samples)
}

/// Parent throughput phase: publish THRU_N as fast as possible, wait for the
/// child's done marker. Returns (descriptors/sec, child-reported skip count).
fn parent_throughput(ring_a: &Ring, ring_b: &Ring) -> (f64, u32) {
    let pub_a = Publisher::new(ring_a.clone());
    // Start at the current head so we wait only for the child's *done* marker,
    // not the 220k latency-phase echoes still live in ring B.
    let mut sub_b = Subscriber::new(ring_b.clone());
    let t0 = Instant::now();
    for i in 0..THRU_N {
        pub_a.publish(desc(i));
    }
    let skipped = loop {
        if let Some(Msg::Sample(d)) = sub_b.try_recv() {
            break d.offset;
        }
    };
    let dt = t0.elapsed().as_secs_f64();
    (THRU_N as f64 / dt, skipped)
}

pub fn run() {
    println!("\n== RING cross-process (2 processes, fork; macOS dev profile) ==");

    let id_a = next_segment_id();
    let id_b = next_segment_id();
    let _ = Segment::unlink_by_id(id_a);
    let _ = Segment::unlink_by_id(id_b);
    let size = (required_bytes(CAP) + 4096).next_power_of_two();
    let seg_a = Segment::create(id_a, size).expect("seg a");
    let seg_b = Segment::create(id_b, size).expect("seg b");
    // SAFETY: fresh, exclusively-owned payloads; no concurrent initializer.
    let ring_a = unsafe { Ring::init(seg_a.payload_ptr(), seg_a.payload_len(), CAP) }.expect("ring a");
    let ring_b = unsafe { Ring::init(seg_b.payload_ptr(), seg_b.payload_len(), CAP) }.expect("ring b");

    // Fork. This function is only called before any bench thread is spawned, so
    // the child inherits a single-threaded, quiescent address space.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        panic!("fork failed");
    }
    if pid == 0 {
        // CHILD: re-attach the same shm objects into this process's own address
        // space (distinct mappings, same physical pages).
        let c_seg_a = Segment::attach(id_a).expect("attach a");
        let c_seg_b = Segment::attach(id_b).expect("attach b");
        // SAFETY: both were Ring::init'd by the parent before the fork.
        let c_ring_a = unsafe { Ring::attach(c_seg_a.payload_ptr(), c_seg_a.payload_len()) }.expect("attach ring a");
        let c_ring_b = unsafe { Ring::attach(c_seg_b.payload_ptr(), c_seg_b.payload_len()) }.expect("attach ring b");
        child_role(c_ring_a, c_ring_b);
    }

    // PARENT.
    let lat = parent_latency(&ring_a, &ring_b);
    let (rate, skipped) = parent_throughput(&ring_a, &ring_b);

    // Reap the child.
    let mut status: libc::c_int = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }

    println!("busy-poll one-way latency (RTT/2), producer proc -> consumer proc:");
    println!("  {}", lat.line_ns());
    println!(
        "streaming throughput, {} descriptors A->child: {} descriptors/sec (child skipped={skipped})",
        THRU_N,
        fmt_rate(rate)
    );

    // Clean up shm names (child never unlinks).
    let _ = seg_a.unlink();
    let _ = seg_b.unlink();
    let _ = Segment::unlink_by_id(id_a);
    let _ = Segment::unlink_by_id(id_b);
}
