//! `shm-core` — the keystone crate of the `shm-actors` substrate.
//!
//! Every other crate speaks the ABI defined here: 24-byte [`ChunkDesc`]
//! descriptors, per-chunk [`ChunkCtrl`] generation/refcount control words,
//! size-class [`Pool`]s over POSIX [`Segment`]s, a per-actor fixed-slot
//! [`BorrowJournal`], and a [`Platform`] seam. See ADR-0001 for scope.
//!
//! # The load-bearing contract
//!
//! - A [`Segment`] is a `MAP_SHARED` POSIX shm region with a validated
//!   [`SegmentHeader`] at its base.
//! - A [`Pool`] carves a segment into fixed size classes; allocation is one CAS
//!   on a Treiber free-list and returns a [`ChunkDesc`].
//! - A [`ChunkCtrl`] tracks each chunk's borrow state (`FREE`/`LOANED`/
//!   `PUBLISHED`), refcount, owner, and a generation that bumps on every
//!   recycle. A [`ChunkDesc`] minted at an old generation fails
//!   [`ChunkCtrl::validate`] with [`Error::StaleDescriptor`].
//! - A [`BorrowJournal`] records every pin so a dead actor's borrows can be
//!   replayed and released.
//!
//! # POD discipline
//!
//! Only types implementing the unsafe [`SharedPod`] marker may be placed in
//! shared memory as plain data. Coordination types with atomics (like
//! [`ChunkCtrl`]) are intentionally *not* `SharedPod` and are placed by hand.

#![deny(missing_docs)]

pub mod ctrl;
pub mod desc;
pub mod error;
pub mod journal;
pub mod platform;
pub mod pod;
pub mod pool;
pub mod segment;

pub use ctrl::{ChunkCtrl, FREE, LOANED, OWNER_NONE, PUBLISHED};
pub use desc::{ChunkDesc, PackedRef};
pub use error::{Error, Result};
pub use journal::{
    BorrowJournal, JournalEntry, JournalRecord, DEFAULT_CAPACITY, ENTRY_ARTIFACT_PIN,
    ENTRY_CHUNK_PIN, ENTRY_EMPTY, JOURNAL_MAGIC,
};
pub use platform::{
    doorbell_pair, doorbell_park, doorbell_ring, DeathDetection, DoorbellPair, Platform,
    PosixPlatform,
};
pub use pod::SharedPod;
pub use pool::{Pool, PoolConfig, SizeClass};
pub use segment::{Segment, SegmentHeader, LAYOUT_VERSION, SEGMENT_MAGIC};
