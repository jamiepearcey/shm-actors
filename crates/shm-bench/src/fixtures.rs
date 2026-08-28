//! Shared-memory fixtures for the benchmarks: auto-unlinked segments, a pool
//! segment, and a ring segment. These mirror the construction the crate tests
//! use so the benchmarks exercise the real allocation / init paths.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use shm_core::{Pool, PoolConfig, Segment};
use shm_ring::{required_bytes, Ring};

/// Process-unique segment ids, well above the ranges the crate tests use
/// (47_000–49_999) so a parallel `cargo test` never collides on shm names.
pub fn next_segment_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    60_000 + NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A freshly created, auto-unlinked segment.
pub struct SegFixture {
    id: u32,
    pub segment: Arc<Segment>,
}

impl SegFixture {
    /// Create a segment of `size` bytes with a fresh id.
    pub fn new(size: usize) -> SegFixture {
        let id = next_segment_id();
        let _ = Segment::unlink_by_id(id);
        let segment = Arc::new(Segment::create(id, size).expect("create segment"));
        SegFixture { id, segment }
    }
}

impl Drop for SegFixture {
    fn drop(&mut self) {
        let _ = Segment::unlink_by_id(self.id);
    }
}

/// A segment with a ring initialized in its payload.
pub struct RingFixture {
    _seg: SegFixture,
    pub ring: Ring,
}

impl RingFixture {
    /// Initialize a ring of `capacity` slots in a fresh segment.
    pub fn new(capacity: u32) -> RingFixture {
        let size = (required_bytes(capacity) + 4096).next_power_of_two();
        let seg = SegFixture::new(size);
        // SAFETY: the payload region stays mapped for the fixture's lifetime and
        // no other party initializes it concurrently.
        let ring = unsafe {
            Ring::init(
                seg.segment.payload_ptr(),
                seg.segment.payload_len(),
                capacity,
            )
            .expect("init ring")
        };
        RingFixture { _seg: seg, ring }
    }
}

/// A segment carved into a [`Pool`]. Holds the `Arc<Segment>` so callers can
/// clone it for `PinGuard` keep-alives; the `Pool` borrows the segment.
pub struct PoolFixture {
    _id_guard: SegFixture,
    pub segment: Arc<Segment>,
}

impl PoolFixture {
    /// Create a segment of `seg_size` bytes and lay `config` into it.
    pub fn new(seg_size: usize) -> PoolFixture {
        let seg = SegFixture::new(seg_size);
        let segment = seg.segment.clone();
        PoolFixture {
            _id_guard: seg,
            segment,
        }
    }

    /// Attach a [`Pool`] over this fixture's segment for `config`.
    pub fn pool<'s>(&'s self, config: &PoolConfig) -> Pool<'s> {
        Pool::create(&self.segment, config).expect("create pool")
    }
}
