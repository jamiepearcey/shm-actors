//! `shm-artifact` — versioned, RCU/MVCC shared state over `shm-core` chunks.
//!
//! An **artifact** is a named, versioned, shared-memory object (design doc
//! v0.1 §5.4). Readers never block; writers never mutate in place. Concurrency is
//! **RCU / MVCC**, not locking: a writer builds the next version off to the side
//! and installs it with a single atomic swap, while readers pin whatever version
//! was current when they looked and read it, zero-copy, until they let go.
//!
//! # Pieces
//!
//! - [`ArtifactHead`] — the atomic RCU control block (`current` version +
//!   `manifest_desc` + a fixed pin table), placed by hand at the base of a
//!   management segment. See [`head`] for its frozen ABI.
//! - [`VersionManifest`] — an immutable on-chunk record listing a version's data
//!   chunk(s); itself a chunk, referenced by a [`ChunkDesc`](shm_core::ChunkDesc)
//!   and pointed at by a [`PackedRef`](shm_core::PackedRef). See [`manifest`].
//! - [`Artifact`] — the handle: [`create`](Artifact::create) /
//!   [`attach`](Artifact::attach), [`pin`](Artifact::pin) (read),
//!   [`open_exclusive`](Artifact::open_exclusive) +
//!   [`commit`](Committer::commit) or
//!   [`commit_optimistic`](Artifact::commit_optimistic) (write).
//! - [`VersionPin`] — a reader's frozen-version guard;
//!   [`as_arrow`](VersionPin::as_arrow) reconstructs the batch zero-copy with the
//!   pin as the Arrow buffer keep-alive.
//! - [`VersionEvent`] — the commit notification delivered to an injected watch
//!   sink; a [`to_desc`](VersionEvent::to_desc)/[`from_desc`](VersionEvent::from_desc)
//!   codec lets it ride an [`shm-ring`](shm_ring) `__artifacts` topic. See
//!   [`event`].
//!
//! # RCU / MVCC protocol
//!
//! - **Commit** stages new data + a new manifest chunk (loaned, written once,
//!   published), reference-counts any shared chunks into the new version, then
//!   installs with one `SeqCst` CAS of `current` (`n → n+1`) followed by a
//!   `Release` store of `manifest_desc`. Nothing already published is mutated, so
//!   a torn version is impossible; readers additionally validate
//!   `manifest.version` so the two-word head update is never observed torn.
//! - **Commit kinds**: [`Commit::Replace`] (supersede wholesale),
//!   [`Commit::Append`] (share the predecessor's chunks + new data),
//!   [`Commit::Patch`] (deferred to v0.2 — [`Error::Unsupported`]).
//! - **Multi-writer**: exclusive mode takes a lease (second writer →
//!   [`Error::WriteLocked`]); optimistic mode races on the install CAS (loser →
//!   [`Error::Conflict`]).
//! - **Reclamation**: a version's chunks are released only when its pin count is
//!   `0` **and** it is not `current`. Chunk `refcount`s count *referencing
//!   versions*, so an Append-shared chunk survives until the last referencing
//!   version is reclaimed. See [`artifact`]'s `try_retire_version` for the exact
//!   rule.
//!
//! # Scope (v0.1)
//!
//! Single-process (threads over one mapping) is exercised. The exclusive write
//! lease is a **fenced** `{owner, fence}` atomic (ADR-0003 item K):
//! [`open_exclusive`](Artifact::open_exclusive) is the unjournalled single-process
//! open, while [`open_exclusive_journaled`](Artifact::open_exclusive_journaled)
//! records a crash-ledger entry so `shm-runtime`'s coordinator can force-release
//! (and fence) a dead writer's lease. `Patch` is deferred. Multi-data-chunk
//! Append versions reconstruct via `concat`.

#![deny(missing_docs)]

pub mod artifact;
pub mod error;
pub mod event;
pub mod head;
pub mod manifest;

pub use artifact::{Artifact, Commit, Committer, VersionPin};
pub use error::{Error, Result};
pub use event::{CommitKind, VersionEvent, ARTIFACTS_TOPIC, EVENT_MAGIC};
pub use head::{ArtifactHead, PinSlot, MAX_LIVE_VERSIONS};
pub use manifest::{
    manifest_len, read_manifest, read_manifest_checked, write_manifest, Manifest, VersionManifest,
    MANIFEST_MAGIC,
};
