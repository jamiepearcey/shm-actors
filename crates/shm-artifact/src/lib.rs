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
//! - [`VersionManifest`] — an immutable on-chunk record listing the data
//!   chunk(s) a version **added** plus a link to its predecessor's manifest
//!   (ADR-0013 chained manifests); itself a chunk, referenced by a
//!   [`ChunkDesc`](shm_core::ChunkDesc) and pointed at by a
//!   [`PackedRef`](shm_core::PackedRef). See [`manifest`].
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
//!   published) listing only the new data, takes one reference on the
//!   predecessor's manifest for an `Append` link, then installs with one
//!   `SeqCst` CAS of `current` (`n → n+1`) followed by a `Release` store of
//!   `manifest_desc`. Nothing already published is mutated, so a torn version
//!   is impossible; readers additionally validate `manifest.version` so the
//!   two-word head update is never observed torn.
//! - **Commit kinds**: [`Commit::Replace`] (supersede wholesale — a chain
//!   root), [`Commit::Append`] (link to the predecessor's manifest + new data —
//!   O(new data) commit and pin, ADR-0013), [`Commit::Window`] (a fresh root
//!   re-listing the newest `keep_batches` batches by reference + new data —
//!   the bounded, zero-copy shape of a high-churn stream, amortised by
//!   [`WindowPolicy`], ADR-0016), [`Commit::Patch`] (deferred to v0.2 —
//!   [`Error::Unsupported`]).
//! - **Multi-writer**: exclusive mode takes a lease (second writer →
//!   [`Error::WriteLocked`]); optimistic mode races on the install CAS (loser →
//!   [`Error::Conflict`]).
//! - **Reclamation**: a version owns one reference — its manifest chunk — and
//!   releases it only when its pin count is `0` **and** it is not `current`. A
//!   manifest owns one reference on each data chunk it lists and one on its
//!   predecessor manifest; whoever releases a manifest's last reference
//!   cascades through those. See [`artifact`]'s `try_retire_version` for the
//!   exact rule.
//! - **Read**: [`VersionPin::as_arrow_batches`] is zero-copy, one
//!   [`RecordBatch`](arrow_array::RecordBatch) per batch across the chain;
//!   [`VersionPin::as_arrow`] concatenates (copies) when a version holds more
//!   than one batch; [`VersionPin::batches_since`] is the stream consumer's
//!   O(new batches) delta, exact across a `Window` (ADR-0016).
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

pub use artifact::{Artifact, Commit, Committer, Delta, VersionPin, WindowPolicy};
pub use error::{Error, Result};
pub use event::{CommitKind, VersionEvent, ARTIFACTS_TOPIC, EVENT_MAGIC};
pub use head::{
    ArtifactHead, PinSlot, FIRST_INCARNATION, MAX_LIVE_VERSIONS, NO_INCARNATION,
};
pub use manifest::{
    manifest_len, parse_manifest_bytes, read_manifest, read_manifest_checked,
    walk_chain_newest_first, walk_chain_with, write_manifest, KeptMember, Manifest,
    ManifestLink, VersionManifest, MANIFEST_MAGIC,
};
