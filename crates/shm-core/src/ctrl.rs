//! Per-chunk control words and the cross-process borrow state machine.

use core::sync::atomic::Ordering;

use crate::desc::ChunkDesc;
use crate::error::{Error, Result};
use crate::substrate::{ShmU32, ShmU64};

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
/// | field         | type        | meaning                                              |
/// |---------------|-------------|------------------------------------------------------|
/// | `word`        | `AtomicU64` | `{state:32 (hi) | refcount:32 (lo)}` — one atomic    |
/// | `owner_actor` | `AtomicU32` | exclusive owner id (`0` = none/released)             |
/// | `generation`  | `AtomicU32` | bumped on every recycle to `FREE`                    |
///
/// `SHMPOOL2`: state and refcount share one word so every transition that
/// depends on both is a single CAS (see the field doc).
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
    /// **`{state:32 | refcount:32}` in ONE word** (state in the high half).
    /// Packed so `borrow_shared`, `release_shared`, `try_loan` and
    /// `try_reclaim` are each a single CAS that observes state and count
    /// together. With two separate words (through `SHMPOOL1`) a borrow could
    /// `fetch_add` the count of a chunk a reclaimer had already checked at
    /// zero and was one CAS away from freeing — a reference onto a freed chunk
    /// — and `try_loan`'s count reset could wrap a racing borrow's undo to
    /// `u32::MAX`. Neither interleaving exists when state and count move
    /// atomically.
    pub word: ShmU64,
    /// Exclusive owner actor id (`OWNER_NONE` = none / released).
    pub owner_actor: ShmU32,
    /// Bumped on every recycle to `FREE`; the `ChunkDesc` staleness guard.
    pub generation: ShmU32,
}

// The 16-byte size / 4-byte alignment is part of the frozen ABI. Gated on
// `not(loom)` because each field is a `#[repr(transparent)]` [`ShmU32`] over the
// real `AtomicU32` (byte-identical in production) but over loom's fat instrumented
// twin under `--cfg loom`; the loom build reconstructs the algorithm in ordinary
// memory and never overlays these bytes on shm, so the ABI size is immaterial there.
#[cfg(not(loom))]
const _: () = assert!(core::mem::size_of::<ChunkCtrl>() == 16);
#[cfg(not(loom))]
const _: () = assert!(core::mem::align_of::<ChunkCtrl>() == 8);

/// Pack `{state (hi) | refcount (lo)}` into the control word.
#[inline]
pub const fn pack_word(state: u32, refcount: u32) -> u64 {
    ((state as u64) << 32) | (refcount as u64)
}
/// The state half of a packed control word.
#[inline]
pub const fn word_state(word: u64) -> u32 {
    (word >> 32) as u32
}
/// The refcount half of a packed control word.
#[inline]
pub const fn word_refcount(word: u64) -> u32 {
    word as u32
}

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
                word: ShmU64::new(pack_word(FREE, 0)),
                owner_actor: ShmU32::new(OWNER_NONE),
                generation: ShmU32::new(generation),
            });
        }
    }

    /// Current lifecycle state.
    #[inline]
    pub fn state(&self) -> u32 {
        word_state(self.word.load(Ordering::Acquire))
    }

    /// Current generation.
    #[inline]
    pub fn generation(&self) -> u32 {
        self.generation.load(Ordering::Acquire)
    }

    /// Current shared-pin count.
    #[inline]
    pub fn refcount(&self) -> u32 {
        word_refcount(self.word.load(Ordering::Acquire))
    }

    /// `FREE → LOANED` for `owner`. One CAS on `{FREE, 0}`: a chunk is only
    /// `FREE` with a zero count, so no separate count reset exists to race a
    /// borrow's undo.
    pub fn try_loan(&self, owner: u32) -> Result<()> {
        debug_assert_ne!(owner, OWNER_NONE, "owner actor id must be non-zero");
        self.word
            .compare_exchange(
                pack_word(FREE, 0),
                pack_word(LOANED, 0),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| Error::InvalidState)?;
        self.owner_actor.store(owner, Ordering::Release);
        Ok(())
    }

    /// `LOANED → PUBLISHED`, preserving the count.
    pub fn publish(&self) -> Result<()> {
        let mut cur = self.word.load(Ordering::Acquire);
        loop {
            if word_state(cur) != LOANED {
                return Err(Error::InvalidState);
            }
            match self.word.compare_exchange(
                cur,
                pack_word(PUBLISHED, word_refcount(cur)),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(now) => cur = now,
            }
        }
    }

    /// `LOANED → FREE` (bumps the generation): the owner abandoned the loan.
    pub fn drop_loan(&self) -> Result<()> {
        let mut cur = self.word.load(Ordering::Acquire);
        loop {
            if word_state(cur) != LOANED {
                return Err(Error::InvalidState);
            }
            match self.word.compare_exchange(
                cur,
                pack_word(FREE, 0),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(now) => cur = now,
            }
        }
        self.owner_actor.store(OWNER_NONE, Ordering::Release);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Take a shared reference: CAS `{PUBLISHED, n} → {PUBLISHED, n+1}`. Fails
    /// iff the chunk is not `PUBLISHED` **at the instant of the CAS** — there is
    /// no window in which a count is bumped on a chunk that is being freed.
    pub fn borrow_shared(&self) -> Result<()> {
        let mut cur = self.word.load(Ordering::Acquire);
        loop {
            if word_state(cur) != PUBLISHED {
                return Err(Error::InvalidState);
            }
            let n = word_refcount(cur);
            debug_assert!(n < u32::MAX, "refcount overflow");
            match self.word.compare_exchange(
                cur,
                pack_word(PUBLISHED, n + 1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(now) => cur = now,
            }
        }
    }

    /// Release a shared reference. If this was the last one and the owner has
    /// already released, the same CAS moves the chunk to `FREE` (`{PUBLISHED,
    /// 1} → {FREE, 0}`) and returns `true`; otherwise `{n} → {n-1}` and `false`.
    ///
    /// The owner check is `SeqCst` and re-run after a decrement to zero so that
    /// `release_shared` and [`owner_release`](Self::owner_release) racing on a
    /// count-1 chunk elect exactly one freer (Dekker: each publishes its own
    /// step, then observes the other's).
    pub fn release_shared(&self) -> bool {
        let mut cur = self.word.load(Ordering::SeqCst);
        loop {
            debug_assert_eq!(word_state(cur), PUBLISHED, "release_shared on non-PUBLISHED");
            let n = word_refcount(cur);
            debug_assert!(n > 0, "release_shared underflow");
            let owner_gone = self.owner_actor.load(Ordering::SeqCst) == OWNER_NONE;
            let new = if n == 1 && owner_gone {
                pack_word(FREE, 0)
            } else {
                pack_word(PUBLISHED, n - 1)
            };
            match self
                .word
                .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    if word_state(new) == FREE {
                        self.generation.fetch_add(1, Ordering::Release);
                        return true;
                    }
                    // Decremented to zero while the owner still held it: the
                    // owner may have released between our check and our CAS.
                    // Re-run the reclaim so the last party out frees it. The
                    // fence is the Dekker pairing with `owner_release`'s
                    // store-then-fence: each side publishes its own step and
                    // then observes the other's, so at least one sees both
                    // (`substrate::fence` docs; loom-checked in `loom_ctrl`).
                    if n == 1 {
                        crate::substrate::fence(Ordering::SeqCst);
                        return self.try_reclaim();
                    }
                    return false;
                }
                Err(now) => cur = now,
            }
        }
    }

    /// The exclusive owner releases; reclaims iff no shared reference remains.
    pub fn owner_release(&self) -> bool {
        self.owner_actor.store(OWNER_NONE, Ordering::SeqCst);
        // Owner half of the Dekker pairing with `release_shared` (see there).
        crate::substrate::fence(Ordering::SeqCst);
        self.try_reclaim()
    }

    /// CAS `{PUBLISHED, 0} → {FREE, 0}` iff the owner has released. One CAS:
    /// a borrow landing after our load fails our CAS, and a borrow landing
    /// before it makes the count non-zero — nothing can be borrowed *and* freed.
    pub fn try_reclaim(&self) -> bool {
        if self.owner_actor.load(Ordering::SeqCst) != OWNER_NONE {
            return false;
        }
        if self
            .word
            .compare_exchange(
                pack_word(PUBLISHED, 0),
                pack_word(FREE, 0),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            self.generation.fetch_add(1, Ordering::Release);
            true
        } else {
            false
        }
    }

    /// Unconditionally reset to `FREE` (crash/administrative reclaim), bumping
    /// and returning the new generation.
    pub fn force_free(&self) -> u32 {
        self.word.store(pack_word(FREE, 0), Ordering::Release);
        self.owner_actor.store(OWNER_NONE, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Check that `desc` is not stale: its generation must match the current one.
    pub fn validate(&self, desc: &ChunkDesc) -> Result<()> {
        if desc.generation == self.generation() {
            Ok(())
        } else {
            Err(Error::StaleDescriptor)
        }
    }
}
