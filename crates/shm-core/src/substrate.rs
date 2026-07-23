//! The **atomic substrate**: `#[repr(transparent)]` newtypes over the real
//! atomics that let the lock-free cores run under `loom` without any on-shm
//! change (ADR-0004 stage L).
//!
//! # The problem this solves
//!
//! `loom` verifies lock-free code by replacing `std`'s atomics with instrumented
//! twins and exhaustively exploring every legal thread interleaving. But it can
//! only observe atomics it *owns* — atomics living in `mmap`'d shared memory
//! reached by raw pointers are invisible to it, which is why the pre-v0.4 cores
//! could only be *stress-tested*, never model-checked.
//!
//! # The swap
//!
//! Every field of a modeled control struct ([`ChunkCtrl`](crate::ctrl::ChunkCtrl),
//! the pool free-list head, `shm-artifact`'s `PinSlot`/`ArtifactHead`,
//! `shm-task`'s `TaskSlot`) is one of these newtypes rather than a bare
//! [`core::sync::atomic`] type. A newtype wraps:
//!
//! - **production** (`cfg(not(loom))`): [`core::sync::atomic::AtomicU32`] /
//!   [`AtomicU64`](core::sync::atomic::AtomicU64) — the *real* atomic;
//! - **loom** (`--cfg loom`): [`loom::sync::atomic`]'s instrumented twin.
//!
//! # The `repr(transparent)` contract (zero ABI drift)
//!
//! In production each newtype is `#[repr(transparent)]` over the real atomic, so
//! it has **byte-identical layout** to the atomic it wraps: a struct built from
//! these newtypes overlays raw shared-memory bytes exactly as one built from bare
//! atomics did. That is what keeps the `const _: () = assert!(size_of::<…>()==N)`
//! ABI guards on every on-shm control struct passing **unchanged**. (Under
//! `--cfg loom` the inner twin is a fat instrumented struct, so those size asserts
//! are `#[cfg(not(loom))]`-gated — the loom build never overlays shm, it
//! reconstructs the algorithm in ordinary heap/stack memory.)
//!
//! # Same method bodies, two substrates
//!
//! Both twins expose the identical method surface
//! (`new`/`load`/`store`/`compare_exchange`/`compare_exchange_weak`/`fetch_add`/
//! `fetch_sub`, each taking a [`core::sync::atomic::Ordering`]), so a core's
//! method bodies — `self.field.compare_exchange(…)` and friends — compile and run
//! **unchanged** against either substrate. Production overlays the struct on shm;
//! a loom harness constructs the *same* struct in ordinary memory and shares it
//! across `loom::thread`s. There is no separately hand-written model to drift.

use core::sync::atomic::Ordering;

#[cfg(not(loom))]
use core::sync::atomic::{AtomicU32 as InnerU32, AtomicU64 as InnerU64};
#[cfg(loom)]
use loom::sync::atomic::{AtomicU32 as InnerU32, AtomicU64 as InnerU64};

/// A memory [`fence`](core::sync::atomic::fence), cfg-swapped like the atomics:
/// [`core::sync::atomic::fence`] in production, [`loom::sync::atomic::fence`]
/// under `--cfg loom`.
///
/// A `fence(SeqCst)` between a store and a later load is a **StoreLoad barrier**
/// — the explicit form of a Dekker / hazard-pointer handshake. It is the *only*
/// construct through which loom enforces the sequentially-consistent total order
/// across two different locations (loom models SeqCst *operations* as
/// acquire/release for synchronisation and does not, by itself, forbid the
/// store-buffering litmus). On the C11 abstract machine and every target ISA the
/// surrounding SeqCst operations already imply this ordering, so the fence is a
/// no-op strengthening in production — but it makes the barrier explicit and lets
/// loom model-check the handshake (ADR-0004 stage L).
#[cfg(not(loom))]
pub use core::sync::atomic::fence;
#[cfg(loom)]
pub use loom::sync::atomic::fence;

/// A shared-memory 32-bit atomic word.
///
/// `#[repr(transparent)]` over [`core::sync::atomic::AtomicU32`] in production, so
/// it is layout-identical to a bare `AtomicU32` and safe to overlay on shm bytes;
/// over [`loom::sync::atomic::AtomicU32`] under `--cfg loom`. See the module docs
/// for the substrate-swap contract.
#[repr(transparent)]
pub struct ShmU32(InnerU32);

/// A shared-memory 64-bit atomic word.
///
/// `#[repr(transparent)]` over [`core::sync::atomic::AtomicU64`] in production, so
/// it is layout-identical to a bare `AtomicU64` and safe to overlay on shm bytes;
/// over [`loom::sync::atomic::AtomicU64`] under `--cfg loom`. See the module docs
/// for the substrate-swap contract.
#[repr(transparent)]
pub struct ShmU64(InnerU64);

impl ShmU32 {
    /// Construct a new word holding `v`.
    ///
    /// Not `const`: the loom twin's constructor is not `const`, and this newtype
    /// keeps one signature across both substrates. Every on-shm use site
    /// constructs at run time (inside an `init_at`), so `const` is not needed.
    #[inline]
    pub fn new(v: u32) -> ShmU32 {
        ShmU32(InnerU32::new(v))
    }

    /// Atomically load the value with ordering `order`.
    #[inline]
    pub fn load(&self, order: Ordering) -> u32 {
        self.0.load(order)
    }

    /// Atomically store `v` with ordering `order`.
    #[inline]
    pub fn store(&self, v: u32, order: Ordering) {
        self.0.store(v, order);
    }

    /// Strong CAS: store `new` iff the value is `current`, returning the previous
    /// value (`Ok` on success, `Err` on failure). `success`/`failure` are the
    /// orderings applied on the respective outcomes.
    #[inline]
    pub fn compare_exchange(
        &self,
        current: u32,
        new: u32,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u32, u32> {
        self.0.compare_exchange(current, new, success, failure)
    }

    /// Weak CAS (may fail spuriously; cheaper on some ISAs). Semantics otherwise
    /// match [`compare_exchange`](Self::compare_exchange).
    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: u32,
        new: u32,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u32, u32> {
        self.0.compare_exchange_weak(current, new, success, failure)
    }

    /// Atomically add `v`, returning the previous value.
    #[inline]
    pub fn fetch_add(&self, v: u32, order: Ordering) -> u32 {
        self.0.fetch_add(v, order)
    }

    /// Atomically subtract `v`, returning the previous value.
    #[inline]
    pub fn fetch_sub(&self, v: u32, order: Ordering) -> u32 {
        self.0.fetch_sub(v, order)
    }
}

impl ShmU64 {
    /// Construct a new word holding `v`. Not `const`; see [`ShmU32::new`].
    #[inline]
    pub fn new(v: u64) -> ShmU64 {
        ShmU64(InnerU64::new(v))
    }

    /// Atomically load the value with ordering `order`.
    #[inline]
    pub fn load(&self, order: Ordering) -> u64 {
        self.0.load(order)
    }

    /// Atomically store `v` with ordering `order`.
    #[inline]
    pub fn store(&self, v: u64, order: Ordering) {
        self.0.store(v, order);
    }

    /// Strong CAS: store `new` iff the value is `current`, returning the previous
    /// value. See [`ShmU32::compare_exchange`].
    #[inline]
    pub fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.0.compare_exchange(current, new, success, failure)
    }

    /// Weak CAS; see [`ShmU32::compare_exchange_weak`].
    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.0.compare_exchange_weak(current, new, success, failure)
    }

    /// Atomically add `v`, returning the previous value.
    #[inline]
    pub fn fetch_add(&self, v: u64, order: Ordering) -> u64 {
        self.0.fetch_add(v, order)
    }

    /// Atomically subtract `v`, returning the previous value.
    #[inline]
    pub fn fetch_sub(&self, v: u64, order: Ordering) -> u64 {
        self.0.fetch_sub(v, order)
    }
}

// The production substrate is byte-identical to the bare atomic it wraps. These
// guards are the substrate-level proof of that; the on-shm control structs carry
// their own full-struct size asserts. (Under `--cfg loom` the inner twin is a fat
// instrumented type, so these are gated off — the loom build never overlays shm.)
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<ShmU32>() == 4);
#[cfg(not(loom))]
const _: () = assert!(core::mem::align_of::<ShmU32>() == 4);
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<ShmU64>() == 8);
#[cfg(not(loom))]
const _: () = assert!(core::mem::align_of::<ShmU64>() == 8);
