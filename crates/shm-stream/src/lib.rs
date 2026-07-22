//! `shm-stream` — ordered, chunked, **transactional** sequences over
//! `shm-artifact` (design doc §5.3, ADR-0002 stage C — the 4th `shm-actors`
//! primitive).
//!
//! A stream is the **write path** for artifacts. Where a raw
//! [`Committer::commit`](shm_artifact::Committer::commit) installs one
//! `RecordBatch` as a version in a single call, a [`StreamWriter`] lets a
//! producer accumulate *many* batches — invisibly — and then install them as
//! **one** atomic version:
//!
//! ```text
//! let mut stream = StreamWriter::open(..)?;   // opens a txn against an artifact
//! stream.append_batch(&batch_a)?;             // Arrow batch -> staged chunk, INVISIBLE
//! stream.append_batch(&batch_b)?;             // staged
//! stream.commit()?;                           // atomic: staged chunks become v(n+1)
//! ```
//!
//! # Transactional semantics
//!
//! - **Isolation.** Staged chunks are `LOANED` (owned by the writer) and named
//!   by no installed manifest, so no reader can reach them. A reader pinning the
//!   artifact mid-transaction sees the previously installed version — or none.
//! - **Atomicity.** [`commit`](StreamWriter::commit) installs all staged chunks
//!   as version `n+1` via `shm-artifact`'s single linearising CAS. Readers only
//!   ever observe `n` then `n+1`.
//! - **Abort / crash.** [`abort`](StreamWriter::abort) (or `Drop` without
//!   commit) frees every staged chunk with nothing published. If the writer
//!   *process dies* mid-stage, the coordinator's lease monitor replays the
//!   borrow journal and frees the staged chunks — the stream needs no extra
//!   crash machinery (ADR-0002: "staged chunks are LOANED + journal-recorded;
//!   crash replay already frees them").
//!
//! # Commit kinds
//!
//! - [`Commit::Append`](shm_artifact::Commit::Append) — the staged batches
//!   **extend** the table: `v(n+1)` shares `v(n)`'s chunks (reference-counted,
//!   O(new data)) plus the staged chunks.
//! - [`Commit::Replace`](shm_artifact::Commit::Replace) — the new version
//!   supersedes wholesale: its manifest is the staged chunks alone.
//! - [`Commit::Patch`](shm_artifact::Commit::Patch) — deferred to v0.3; rejected
//!   at [`open`](StreamWriter::open) with [`Error::Unsupported`].
//!
//! # Coordination
//!
//! [`Coordination::Exclusive`] holds `shm-artifact`'s write lease for the whole
//! transaction (a second exclusive `open` fails [`WriteLocked`](shm_artifact::Error::WriteLocked));
//! [`Coordination::Optimistic`] takes no lease and races on the install CAS,
//! a loser observing [`Conflict`](shm_artifact::Error::Conflict).
//!
//! # Relationship to `shm-artifact`
//!
//! `commit()` installs pre-staged chunks through the additive
//! [`Committer::commit_staged`](shm_artifact::Committer::commit_staged) /
//! [`Artifact::commit_staged_optimistic`](shm_artifact::Artifact::commit_staged_optimistic)
//! entry points (v0.2 stage C), which share `shm-artifact`'s single RCU install
//! path with ordinary single-batch commits. No on-shm `StreamHead` is
//! introduced: a transaction is fully described by writer memory + the borrow
//! journal, so no cross-process stream metadata is needed.
//!
//! # Scope (v0.2)
//!
//! Single-process (threads over one mapping) is exercised, matching the rest of
//! the substrate. All batches in one transaction must share one Arrow schema.

#![deny(missing_docs)]

pub mod error;
pub mod stream;

pub use error::{Error, Result};
pub use stream::{Coordination, StreamWriter};

// Re-exported for ergonomics: a caller almost always names the commit kind.
pub use shm_artifact::Commit;
