//! Per-holder chunk reservations over a shared [`Pool`].
//!
//! # The failure this exists for
//!
//! A pool is one arena shared by every process attached to the segment, and
//! [`Pool::alloc`] has no idea who is calling it. So a single runaway holder can
//! drain a size class and every *other* holder starts getting
//! [`Error::PoolExhausted`] — for a fault that was not theirs, with nothing in
//! the error to say whose it was. Shared memory turns one actor's leak into
//! everybody's outage, and the shared free list is exactly the mechanism.
//!
//! That is the cost side of shared memory, and it is why a shared arena is not
//! on its own an isolation story: process separation stops one worker's *heap*
//! killing another, and does nothing about the arena they share.
//!
//! # What a reservation changes
//!
//! A [`QuotaPool`] bounds how many chunks one holder may have outstanding. Past
//! its own limit it gets [`Error::QuotaExceeded`] and **the pool still has
//! chunks for everyone else**.
//!
//! The distinct error is most of the value. Today a runaway is diagnosed by
//! every victim reporting `PoolExhausted` and nobody reporting anything that
//! names the cause. With reservations the culprit gets an error that is about
//! *it* — attributable, at the moment it happens, in the process responsible.
//!
//! # The property that makes it isolation, and the one that does not
//!
//! Reservations only guarantee non-interference when they are **planned**:
//! `sum(limits) <= pool.total_chunks()`. [`Reservations`] exists to do that
//! arithmetic up front rather than discover it under load; without it, two
//! holders each "within quota" can still empty the arena between them.
//!
//! And enforcement is **per-process and cooperative**. A holder that reaches
//! past its `QuotaPool` to the raw [`Pool`] is not stopped. That makes this a
//! guard against a runaway or a leak — the realistic failure — and not a
//! security boundary. A hostile or untrusted tenant needs its own segment;
//! nothing enforced inside a shared arena can contain something that can write
//! to all of it anyway.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::desc::ChunkDesc;
use crate::error::{Error, Result};
use crate::pool::Pool;

/// A planner for dividing one pool's chunks between holders.
///
/// The point is to make over-subscription a mistake you cannot make quietly:
/// [`Reservations::grant`] refuses the grant that would take the total past the
/// pool, instead of leaving two holders to discover it under load.
#[derive(Debug)]
pub struct Reservations {
    total: usize,
    granted: usize,
}

impl Reservations {
    /// Plan reservations over a pool of `total_chunks`.
    #[must_use]
    pub fn new(total_chunks: usize) -> Self {
        Self {
            total: total_chunks,
            granted: 0,
        }
    }

    /// Reserve `chunks` for one holder.
    ///
    /// [`Error::QuotaExceeded`] when the grant would over-subscribe the pool —
    /// refused rather than clamped, because a holder silently given less than it
    /// asked for would fail later, somewhere else, for no visible reason.
    pub fn grant(&mut self, chunks: usize) -> Result<PoolQuota> {
        let next = self.granted.saturating_add(chunks);
        if next > self.total {
            return Err(Error::QuotaExceeded);
        }
        self.granted = next;
        Ok(PoolQuota::new(chunks))
    }

    /// Chunks not yet reserved. Leaving headroom is legitimate: an unreserved
    /// remainder is what a holder with no quota at all draws from.
    #[must_use]
    pub fn unreserved(&self) -> usize {
        self.total - self.granted
    }
}

/// One holder's allowance: how many chunks it may have outstanding at once.
#[derive(Debug)]
pub struct PoolQuota {
    limit: usize,
    outstanding: AtomicUsize,
}

impl PoolQuota {
    /// A quota of `limit` chunks.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            outstanding: AtomicUsize::new(0),
        }
    }

    /// Chunks currently held under this quota.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::Acquire)
    }

    /// The ceiling.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Take one chunk's worth of allowance, or fail.
    ///
    /// A CAS loop rather than `fetch_add` then compare: adding first would let a
    /// burst of concurrent allocators each see a count already past the limit
    /// and roll back, and a rollback that races another holder's take can leave
    /// the count briefly wrong in the direction that *admits* work. The check
    /// and the increment have to be one step.
    fn take(&self) -> Result<()> {
        let mut current = self.outstanding.load(Ordering::Acquire);
        loop {
            if current >= self.limit {
                return Err(Error::QuotaExceeded);
            }
            match self.outstanding.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(seen) => current = seen,
            }
        }
    }

    /// Return one chunk's worth of allowance.
    ///
    /// Saturating: a double free must not wrap the count to `usize::MAX` and
    /// hand the holder an unlimited quota. The pool's own generation check is
    /// what rejects the double free itself; this only refuses to make it worse.
    fn give_back(&self) {
        let _ = self
            .outstanding
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            });
    }
}

/// A [`Pool`] view that will not let one holder exceed its [`PoolQuota`].
///
/// Every allocation goes through the quota first, so a holder at its limit is
/// refused while the pool still serves everyone else.
pub struct QuotaPool<'s> {
    pool: &'s Pool<'s>,
    quota: PoolQuota,
}

impl<'s> QuotaPool<'s> {
    /// Bind `quota` to `pool` for one holder.
    #[must_use]
    pub fn new(pool: &'s Pool<'s>, quota: PoolQuota) -> Self {
        Self { pool, quota }
    }

    /// This holder's quota.
    #[must_use]
    pub fn quota(&self) -> &PoolQuota {
        &self.quota
    }

    /// The pool underneath. Allocating through this bypasses the quota, which is
    /// exactly what makes enforcement cooperative — see the module docs.
    #[must_use]
    pub fn pool(&self) -> &'s Pool<'s> {
        self.pool
    }

    /// Allocate within this holder's allowance.
    ///
    /// [`Error::QuotaExceeded`] when the holder is at its limit — distinct from
    /// [`Error::PoolExhausted`], which still means the arena itself is empty.
    /// Telling those apart is the difference between "you have a leak" and
    /// "somebody else does".
    pub fn alloc(&self, size: u32) -> Result<ChunkDesc> {
        self.quota.take()?;
        match self.pool.alloc(size) {
            Ok(desc) => Ok(desc),
            // The allowance was taken before the pool was asked, so it has to go
            // back on every failing path or a holder leaks its own quota down to
            // zero without ever holding a chunk.
            Err(e) => {
                self.quota.give_back();
                Err(e)
            }
        }
    }

    /// Free a chunk and return its allowance.
    ///
    /// The allowance is returned only when the pool accepted the free: a
    /// rejected free (stale generation, bad descriptor) did not release a chunk,
    /// so crediting it would let the holder allocate past its share.
    pub fn free(&self, desc: &ChunkDesc) -> Result<()> {
        self.pool.free(desc)?;
        self.quota.give_back();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::PoolConfig;
    use crate::segment::Segment;

    static NEXT: AtomicUsize = AtomicUsize::new(0);

    /// A fresh segment per test: the shm namespace is global, so parallel tests
    /// that shared an id would stomp each other's arenas.
    fn pool_segment(chunks: u32) -> (Segment, PoolConfig) {
        let id = (((std::process::id() & 0x7fff) << 8)
            | ((NEXT.fetch_add(1, Ordering::Relaxed) as u32) & 0xff))
            | 0x4000_0000;
        let _ = Segment::attach(id).and_then(|s| s.unlink());
        let seg = Segment::create(id, 1 << 20).expect("segment");
        (seg, PoolConfig::power_of_two(64, 64, chunks))
    }

    #[test]
    fn a_holder_at_its_limit_is_refused_while_others_still_allocate() {
        let (seg, config) = pool_segment(16);
        let pool = Pool::create(&seg, &config).expect("pool");

        let mut plan = Reservations::new(pool.total_chunks());
        let a = QuotaPool::new(&pool, plan.grant(2).expect("grant a"));
        let b = QuotaPool::new(&pool, plan.grant(2).expect("grant b"));

        let _a1 = a.alloc(64).expect("a's first");
        let _a2 = a.alloc(64).expect("a's second");

        // A is done; the pool is not.
        assert!(matches!(a.alloc(64), Err(Error::QuotaExceeded)));

        // THE POINT: B is unaffected by A's runaway.
        let _b1 = b.alloc(64).expect("b still allocates");
        assert_eq!(a.quota().outstanding(), 2);
        assert_eq!(b.quota().outstanding(), 1);
    }

    #[test]
    fn freeing_returns_the_allowance() {
        let (seg, config) = pool_segment(8);
        let pool = Pool::create(&seg, &config).expect("pool");
        let q = QuotaPool::new(&pool, PoolQuota::new(1));

        let desc = q.alloc(64).expect("first");
        assert!(matches!(q.alloc(64), Err(Error::QuotaExceeded)));
        q.free(&desc).expect("free");
        assert_eq!(q.quota().outstanding(), 0);
        q.alloc(64).expect("the allowance came back");
    }

    #[test]
    fn a_failed_allocation_does_not_consume_the_allowance() {
        // Quota above what the pool can serve, so the pool is what fails. The
        // holder must not be charged for a chunk it never received.
        let (seg, config) = pool_segment(1);
        let pool = Pool::create(&seg, &config).expect("pool");
        let q = QuotaPool::new(&pool, PoolQuota::new(4));

        let _held = q.alloc(64).expect("the only chunk");
        assert!(matches!(q.alloc(64), Err(Error::PoolExhausted)));
        assert_eq!(
            q.quota().outstanding(),
            1,
            "a pool failure must not charge the quota"
        );
    }

    #[test]
    fn over_subscription_is_refused_at_plan_time() {
        let (seg, config) = pool_segment(4);
        let pool = Pool::create(&seg, &config).expect("pool");
        let mut plan = Reservations::new(pool.total_chunks());

        plan.grant(3).expect("fits");
        assert_eq!(plan.unreserved(), pool.total_chunks() - 3);
        assert!(
            matches!(plan.grant(2), Err(Error::QuotaExceeded)),
            "reservations that exceed the arena are a planning error, not a runtime surprise"
        );
        plan.grant(1).expect("the remainder still fits");
    }

    #[test]
    fn a_double_free_cannot_inflate_the_allowance() {
        let (seg, config) = pool_segment(8);
        let pool = Pool::create(&seg, &config).expect("pool");
        let q = QuotaPool::new(&pool, PoolQuota::new(2));

        let desc = q.alloc(64).expect("one");
        q.free(&desc).expect("free");
        // Whatever the pool says about the second free, the count must not wrap.
        let _ = q.free(&desc);
        assert_eq!(q.quota().outstanding(), 0);
    }
}
