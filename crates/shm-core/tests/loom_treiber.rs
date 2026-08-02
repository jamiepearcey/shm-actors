//! loom model of the pool's **Treiber free-list** (ADR-0004 stage L, core 2).
//!
//! Runs the *production* [`shm_core::pool::treiber_pop`] /
//! [`treiber_push`](shm_core::pool::treiber_push) CAS loops — the exact code
//! `Pool::alloc`/`Pool::free` drive — over an ordinary in-memory node table
//! (heap-allocated loom atomics) instead of `mmap`'d chunk payload, so loom's
//! exhaustive scheduler can observe every atomic. The addressing layer (chunk
//! pointer arithmetic) is replaced by a plain index into the link table; the
//! lock-free algorithm under test is byte-for-byte the same.
//!
//! # Modeled scenarios (2-node stack: slots `0`,`1`, `head={tag:0,slot:0}`, `0→1→EMPTY`)
//!
//! 1. **Concurrent pops — no double-alloc.** Two threads each `pop()` once. The
//!    two pops must return *distinct* slots: a chunk is never handed to two
//!    allocators at once. (Pushing back is deliberately omitted here — with a
//!    push, a serialized `pop;push;pop` would legitimately reuse a slot, which is
//!    correct, so distinctness only characterises the simultaneous case.)
//! 2. **Concurrent pop+push — no lost node / no ABA corruption.** Two threads each
//!    `pop()` then `push()` the node back. After both finish the free list must
//!    still be *exactly* the two original nodes (walking it yields `{0,1}` with no
//!    cycle, duplicate, or dropped node). The generation tag is what makes this
//!    hold under the ABA interleavings loom drives — a tagless head would let a
//!    stale CAS splice a recycled node and corrupt the list.
//!
//! Only compiled/run under `--cfg loom`; a no-op otherwise.
#![cfg(loom)]

use loom::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use shm_core::pool::{treiber_pop, treiber_push};
use shm_core::ShmU32;
use shm_core::ShmU64;

const EMPTY: u32 = u32::MAX;
const N: usize = 2;

#[inline]
fn head_pack(tag: u32, slot: u32) -> u64 {
    (u64::from(tag) << 32) | u64::from(slot)
}
#[inline]
fn head_slot(v: u64) -> u32 {
    (v & 0xffff_ffff) as u32
}

/// The shared stack: a tagged head plus one next-link per node. In production the
/// link lives in the chunk payload; here it is a loom atomic so the scheduler can
/// interleave the (production-non-atomic-but-head-synchronised) link accesses too
/// — a strictly weaker ordering than the real code, so a pass here is sound.
struct Stack {
    head: ShmU64,
    links: Vec<ShmU32>,
}

impl Stack {
    fn new() -> Stack {
        let links = vec![ShmU32::new(1), ShmU32::new(EMPTY)];
        Stack {
            head: ShmU64::new(head_pack(0, 0)),
            links,
        }
    }
    fn pop(&self) -> Option<u32> {
        treiber_pop(&self.head, |slot| self.links[slot as usize].load(Relaxed))
    }
    fn push(&self, slot: u32) {
        treiber_push(&self.head, slot, |slot, next| {
            self.links[slot as usize].store(next, Relaxed)
        });
    }
    /// Walk the list, returning the slots in order (or `None` on a cycle/overrun).
    fn walk(&self) -> Option<Vec<u32>> {
        let mut out = Vec::new();
        let mut slot = head_slot(self.head.load(Relaxed));
        while slot != EMPTY {
            if out.len() > N || out.contains(&slot) {
                return None; // cycle or too long: corruption
            }
            out.push(slot);
            slot = self.links[slot as usize].load(Relaxed);
        }
        Some(out)
    }
}

static ITERS_A: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ITERS_B: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[test]
fn loom_treiber_no_double_alloc() {
    loom::model(|| {
        ITERS_A.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stack = Arc::new(Stack::new());

        let s1 = stack.clone();
        let t1 = loom::thread::spawn(move || s1.pop());
        let s2 = stack.clone();
        let t2 = loom::thread::spawn(move || s2.pop());

        let a = t1.join().unwrap();
        let b = t2.join().unwrap();

        // Both concurrent pops succeed (2 nodes, 2 poppers) with distinct slots:
        // no chunk is popped by two allocators at once.
        assert!(
            a.is_some() && b.is_some(),
            "both pops must succeed on a 2-node stack"
        );
        assert_ne!(
            a, b,
            "a chunk was popped by two allocators at once (double-alloc)"
        );
        // The list is now empty and uncorrupted.
        assert_eq!(
            stack.walk().expect("free-list corrupted"),
            Vec::<u32>::new()
        );
    });
    eprintln!(
        "loom_treiber_no_double_alloc: explored {} interleavings",
        ITERS_A.load(std::sync::atomic::Ordering::Relaxed)
    );
}

#[test]
fn loom_treiber_pop_push_integrity() {
    loom::model(|| {
        ITERS_B.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stack = Arc::new(Stack::new());

        let s1 = stack.clone();
        let t1 = loom::thread::spawn(move || {
            if let Some(slot) = s1.pop() {
                s1.push(slot);
            }
        });
        let s2 = stack.clone();
        let t2 = loom::thread::spawn(move || {
            if let Some(slot) = s2.pop() {
                s2.push(slot);
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // No lost node / ABA corruption: every popped node was pushed back, so the
        // free list is exactly the two original nodes.
        let mut walked = stack.walk().expect("free-list corrupted (cycle/overrun)");
        walked.sort_unstable();
        assert_eq!(
            walked,
            vec![0, 1],
            "a node was lost or duplicated (ABA corruption)"
        );
    });
    eprintln!(
        "loom_treiber_pop_push_integrity: explored {} interleavings",
        ITERS_B.load(std::sync::atomic::Ordering::Relaxed)
    );
}
