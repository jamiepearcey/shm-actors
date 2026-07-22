//! Per-chunk control words and the cross-process borrow state machine.

use core::sync::atomic::{AtomicU32, Ordering};

use crate::desc::ChunkDesc;
use crate::error::{Error, Result};

/// Chunk is unowned and available for allocation.
pub const FREE: u32 = 0;
/// Chunk is exclusively loaned to `owner_actor` (a writer holds a `Loan`).
pub const LOANED: u32 = 1;
/// Chunk has been published; readers may pin shared `Sample`s of it.
pub const PUBLISHED: u32 = 2;

/// Sentinel meaning "no owner" (also "owner has released") in `owner_actor`.
pub const OWNER_NONE: u32 = 0;

/// Cross-process control word for one chunk.
///
/// `ChunkCtrl`s live in a dedicated **control array** indexed by chunk index,
/// kept in a management region that is *not* interleaved with payload bytes (see
/// [`crate::pool`]). Because every field is mutated concurrently by multiple
/// processes, all four are atomics; consequently `ChunkCtrl` is deliberately
/// **not** `Pod`/`SharedPod` and is placed into shared memory by hand.
///
/// # Layout (frozen ABI — 16 bytes)
///
/// | field         | type        | meaning                                  |
/// |---------------|-------------|------------------------------------------|
/// | `state`       | `AtomicU32` | `FREE` / `LOANED` / `PUBLISHED`          |
/// | `refcount`    | `AtomicU32` | number of shared `Sample` pins           |
/// | `owner_actor` | `AtomicU32` | exclusive owner id (`0` = none/released) |
/// | `generation`  | `AtomicU32` | bumped on every recycle to `FREE`        |
///
/// # State machine
///
/// ```text
///                 try_loan(owner)              publish()
///     FREE  ───────────────────────▶  LOANED ───────────▶  PUBLISHED
///      ▲                                 │                     │  ▲
///      │        drop_loan() (bump gen)   │                     │  │ borrow_shared()
///      └─────────────────────────────────┘                     │  │ release_shared()
///                                                               │  │
///      ▲   try_reclaim(): owner released && refcount == 0       │  │
///      └───────────────────────────────────────────────────────┘  │
///                        (bump gen)                                ▼
/// ```
///
/// A published chunk returns to `FREE` only once the owner has released it
/// (`owner_actor == OWNER_NONE`) **and** every shared pin is gone
/// (`refcount == 0`). Each recycle to `FREE` bumps `generation`, so any
/// [`ChunkDesc`] minted at the old generation now fails [`ChunkCtrl::validate`]
/// with [`Error::StaleDescriptor`].
#[repr(C)]
pub struct ChunkCtrl {
    /// Lifecycle state: [`FREE`], [`LOANED`], or [`PUBLISHED`].
    pub state: AtomicU32,
    /// Number of live shared `Sample` pins (only meaningful when `PUBLISHED`).
    pub refcount: AtomicU32,
    /// Exclusive owner actor id, or [`OWNER_NONE`] when unowned/released.
    pub owner_actor: AtomicU32,
    /// Recycle generation; bumped whenever the chunk transitions back to `FREE`.
    pub generation: AtomicU32,
}

// The 16-byte size / 4-byte alignment is part of the frozen ABI.
const _: () = assert!(core::mem::size_of::<ChunkCtrl>() == 16);
const _: () = assert!(core::mem::align_of::<ChunkCtrl>() == 4);

impl ChunkCtrl {
    /// Initializes a control word in place to a fresh `FREE` state at the given
    /// starting generation.
    ///
    /// # Safety
    ///
    /// `ptr` must point to 16 writable, `ChunkCtrl`-aligned bytes.
    #[inline]
    pub unsafe fn init_at(ptr: *mut ChunkCtrl, generation: u32) {
        // SAFETY: caller guarantees `ptr` is valid and aligned for `ChunkCtrl`.
        // Atomics are `#[repr(C)]` over their integer, so an in-place write of
        // the atomic wrapper values is well-defined.
        unsafe {
            ptr.write(ChunkCtrl {
                state: AtomicU32::new(FREE),
                refcount: AtomicU32::new(0),
                owner_actor: AtomicU32::new(OWNER_NONE),
                generation: AtomicU32::new(generation),
            });
        }
    }

    /// Current lifecycle state.
    #[inline]
    pub fn state(&self) -> u32 {
        self.state.load(Ordering::Acquire)
    }

    /// Current generation.
    #[inline]
    pub fn generation(&self) -> u32 {
        self.generation.load(Ordering::Acquire)
    }

    /// Current shared-pin refcount.
    #[inline]
    pub fn refcount(&self) -> u32 {
        self.refcount.load(Ordering::Acquire)
    }

    /// `FREE -> LOANED`: claim the chunk exclusively for `owner`.
    ///
    /// `owner` must not be [`OWNER_NONE`]. Returns [`Error::InvalidState`] if the
    /// chunk was not `FREE`.
    pub fn try_loan(&self, owner: u32) -> Result<()> {
        debug_assert_ne!(owner, OWNER_NONE, "owner actor id must be non-zero");
        self.state
            .compare_exchange(FREE, LOANED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::InvalidState)?;
        self.owner_actor.store(owner, Ordering::Release);
        self.refcount.store(0, Ordering::Release);
        Ok(())
    }

    /// `LOANED -> PUBLISHED`: make a written chunk visible to subscribers.
    ///
    /// Returns [`Error::InvalidState`] if the chunk was not `LOANED`.
    pub fn publish(&self) -> Result<()> {
        self.state
            .compare_exchange(LOANED, PUBLISHED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::InvalidState)?;
        Ok(())
    }

    /// `LOANED -> FREE`: drop a loan that was never published. Bumps generation.
    ///
    /// Returns [`Error::InvalidState`] if the chunk was not `LOANED`.
    pub fn drop_loan(&self) -> Result<()> {
        self.state
            .compare_exchange(LOANED, FREE, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::InvalidState)?;
        self.owner_actor.store(OWNER_NONE, Ordering::Release);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Take a shared pin on a published chunk (`refcount += 1`).
    ///
    /// Only valid while `PUBLISHED`. Returns [`Error::InvalidState`] otherwise.
    pub fn borrow_shared(&self) -> Result<()> {
        // Optimistically bump, then verify the chunk is still published. If a
        // recycle raced us we undo the bump. This keeps `refcount` an honest
        // upper bound so `try_reclaim` never frees a chunk with a live pin.
        self.refcount.fetch_add(1, Ordering::AcqRel);
        if self.state.load(Ordering::Acquire) != PUBLISHED {
            self.refcount.fetch_sub(1, Ordering::AcqRel);
            return Err(Error::InvalidState);
        }
        Ok(())
    }

    /// Release a shared pin (`refcount -= 1`) and attempt reclamation.
    ///
    /// Returns `true` if this release recycled the chunk back to `FREE`.
    pub fn release_shared(&self) -> bool {
        let prev = self.refcount.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "release_shared underflow");
        self.try_reclaim()
    }

    /// The exclusive owner releases a published chunk. Attempts reclamation.
    ///
    /// Returns `true` if this release recycled the chunk back to `FREE`.
    pub fn owner_release(&self) -> bool {
        self.owner_actor.store(OWNER_NONE, Ordering::Release);
        self.try_reclaim()
    }

    /// Recycle to `FREE` iff `PUBLISHED && owner released && refcount == 0`.
    ///
    /// Bumps `generation` on success so stale descriptors are invalidated.
    ///
    /// Note: `state`, `owner_actor`, and `refcount` are separate words, so the
    /// pre-checks are not a single atomic snapshot. The reclaim only commits via
    /// a `PUBLISHED -> FREE` CAS, so at most one caller wins; the caveat is that
    /// a pin taken *after* the checks but before the CAS is prevented by
    /// [`borrow_shared`](Self::borrow_shared) rejecting non-`PUBLISHED` chunks.
    /// A v0.2 hardening packs `refcount`+`owner-released` into one word.
    pub fn try_reclaim(&self) -> bool {
        if self.state.load(Ordering::Acquire) != PUBLISHED {
            return false;
        }
        if self.owner_actor.load(Ordering::Acquire) != OWNER_NONE {
            return false;
        }
        if self.refcount.load(Ordering::Acquire) != 0 {
            return false;
        }
        if self
            .state
            .compare_exchange(PUBLISHED, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.generation.fetch_add(1, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Force the chunk back to `FREE`, bumping generation, regardless of state.
    ///
    /// Used by the coordinator's crash-reclamation path once a dead actor's pins
    /// have been accounted for. Returns the new generation.
    pub fn force_free(&self) -> u32 {
        self.state.store(FREE, Ordering::Release);
        self.owner_actor.store(OWNER_NONE, Ordering::Release);
        self.refcount.store(0, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Validate a descriptor's generation against this live control word.
    ///
    /// Returns [`Error::StaleDescriptor`] when the descriptor was minted before
    /// a recycle (its generation no longer matches).
    pub fn validate(&self, desc: &ChunkDesc) -> Result<()> {
        if desc.generation == self.generation() {
            Ok(())
        } else {
            Err(Error::StaleDescriptor)
        }
    }
}
