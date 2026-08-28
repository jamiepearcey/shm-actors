//! The on-chunk [`VersionManifest`] ABI and its read/write helpers.
//!
//! A manifest is an **immutable** chunk that names the data chunk(s) making up
//! one version of an artifact **and links to its predecessor's manifest**
//! (ADR-0013). It is itself a chunk (obtained from a [`ChunkAllocator`]),
//! referenced by a [`ChunkDesc`], and pointed at from the
//! [`ArtifactHead`](crate::ArtifactHead) by a [`PackedRef`](shm_core::PackedRef).
//!
//! # Layout (frozen ABI — ADR-0013, `SHMMFST4`)
//!
//! Written at the start of a loaned chunk (i.e. at `segment_base +
//! ChunkDesc::offset`), all offsets chunk-relative:
//!
//! ```text
//! off  size  field            notes
//!  0    8    magic            b"SHMMFST4"
//!  8    8    version          self-id
//! 16    4    artifact_id      self-id
//! 20    4    schema_id        own chunks' schema; equals prev's when linked
//! 24    4    chunk_count      OWN data chunks only
//! 28    4    batch_count      OWN batches only
//! 32    8    prev_version     0 = root; else < version
//! 40    8    prev_ref         PackedRef bits of the predecessor manifest; 0 = root
//! 48    4    prev_gen         ChunkCtrl generation of prev at link time; 0 if root
//! 52    4    depth            0 for root, else prev.depth + 1 — the walk bound
//! 56    4    total_batches    own + prev.total_batches (saturating)
//! 60    4    _reserved        0
//! 64   24·chunk_count  ChunkDesc[] (own, row order)
//! ..    4·batch_count  u32 span[] (sum == chunk_count)
//! ```
//!
//! # Chained manifests (ADR-0013)
//!
//! A manifest lists only the chunks its **own** version added. An `Append`
//! version's manifest carries a validated link (`prev_*`) to its predecessor's
//! manifest, so the full table of a version is the chain of manifests walked
//! oldest-first ([`walk_chain_with`]); a `Replace` (or the first) version is a
//! **root** (`depth == 0`, no link). Ownership follows the chain:
//!
//! > A version owns exactly one reference: its manifest chunk. A manifest owns
//! > one reference on each data chunk it lists and one on its predecessor
//! > manifest (if any). Whoever releases the last reference on a manifest
//! > releases the manifest's own references (the retire cascade).
//!
//! This makes commit and pin O(own data) regardless of the lineage's length,
//! while a read is O(batches) across the chain.
//!
//! # Root-strictness (parse-time, fuzzed)
//!
//! `prev_ref == 0 ⇔ prev_version == 0 ⇔ depth == 0` (a root); a root also has
//! `prev_gen == 0`. When linked, `prev_version < version`. Always
//! `total_batches >= batch_count` and `_reserved == 0`. Any violation parses as
//! [`Error::VersionGone`]. (`prev_gen` is *not* part of the root iff: a chunk's
//! generation starts at `0`, so a link to a never-recycled manifest legitimately
//! records `prev_gen == 0`.)
//!
//! # Batch boundaries (item F)
//!
//! A manifest's own data chunks are a **flat** list, but with multi-chunk
//! batches (item F) a single Arrow batch may span several consecutive chunks.
//! The `batch_span` array partitions the flat `chunk` list into batches: batch
//! `b` occupies `batch_span[b]` consecutive chunks, and `sum(batch_span) ==
//! chunk_count`. Reclamation stays trivial because **every** chunk — primary or
//! continuation — is a flat `chunk` entry, so releasing the manifest frees them
//! all with no per-batch bookkeeping.
//!
//! The records are plain `#[repr(C)]` PODs written unaligned; a manifest chunk
//! is never mutated after publication, so no atomics / `SharedPod` machinery is
//! needed here (mirroring `shm-arrow`'s batch layout).

use shm_arrow::ChunkAllocator;
use shm_core::{ChunkDesc, PackedRef, Segment};

use crate::error::{Error, Result};

/// Manifest magic: little-endian bytes of `b"SHMMFST4"`.
///
/// Bumped from `SHMMFST3` for the ADR-0013 chained layout: the header grew from
/// 32 to 64 bytes with the `prev_version` / `prev_ref` / `prev_gen` / `depth` /
/// `total_batches` link fields, and `chunk_count` / `batch_count` now count the
/// version's **own** chunks only. A `SHMMFST3`-or-older manifest is rejected by
/// the magic check.
pub const MANIFEST_MAGIC: u64 = u64::from_le_bytes(*b"SHMMFST4");

/// The fixed header at the start of a manifest chunk (64 bytes), followed by a
/// flat `[ChunkDesc; chunk_count]` array and a `[u32; batch_count]` span array.
///
/// # Layout (frozen ABI — 64 bytes, ADR-0013)
///
/// | field           | type  | meaning                                          |
/// |-----------------|-------|--------------------------------------------------|
/// | `magic`         | `u64` | must equal [`MANIFEST_MAGIC`]                    |
/// | `version`       | `u64` | the artifact version this manifest describes     |
/// | `artifact_id`   | `u32` | interned artifact name id this manifest is for   |
/// | `schema_id`     | `u32` | interned Arrow schema id of the data chunks      |
/// | `chunk_count`   | `u32` | number of **own** `ChunkDesc`s after the header  |
/// | `batch_count`   | `u32` | number of **own** `u32` batch spans              |
/// | `prev_version`  | `u64` | predecessor version; `0` = root                  |
/// | `prev_ref`      | `u64` | `PackedRef` bits of the predecessor manifest; `0` = root |
/// | `prev_gen`      | `u32` | predecessor chunk's generation at link time      |
/// | `depth`         | `u32` | `0` for a root, else `prev.depth + 1`            |
/// | `total_batches` | `u32` | `batch_count + prev.total_batches` (saturating)  |
/// | `_reserved`     | `u32` | must be `0`                                      |
///
/// The `{artifact_id, version}` pair is monotonic and never reissued, so a
/// manifest self-identifies; [`read_manifest_checked`] validates both against
/// the reader's expectation, subsuming the ABA role the dropped `PackedRef`
/// generation used to play for the manifest pointer.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionManifest {
    /// Must equal [`MANIFEST_MAGIC`].
    pub magic: u64,
    /// The artifact version number this manifest describes.
    pub version: u64,
    /// Interned artifact name id this manifest belongs to (its self-identifier).
    pub artifact_id: u32,
    /// Interned Arrow schema id shared by every data chunk in this version (and,
    /// when linked, by the whole chain).
    pub schema_id: u32,
    /// Number of **own** [`ChunkDesc`] entries that follow this header.
    pub chunk_count: u32,
    /// Number of **own** `u32` batch-span entries after the `ChunkDesc` array.
    /// Their sum equals `chunk_count`; each span is the chunk count of one Arrow
    /// batch.
    pub batch_count: u32,
    /// The predecessor's version, or `0` for a root manifest.
    pub prev_version: u64,
    /// The predecessor manifest's [`PackedRef`] bits, or `0` for a root.
    pub prev_ref: u64,
    /// The predecessor manifest chunk's `ChunkCtrl` generation when the link was
    /// taken (`0` for a root).
    pub prev_gen: u32,
    /// Chain depth: `0` for a root, else `prev.depth + 1`. Bounds a chain walk.
    pub depth: u32,
    /// Batches in the whole chain: `batch_count + prev.total_batches`
    /// (saturating). Re-validated by the walk.
    pub total_batches: u32,
    /// Reserved; must be `0`.
    pub _reserved: u32,
}

const _: () = assert!(core::mem::size_of::<VersionManifest>() == 64);
const _: () = assert!(core::mem::align_of::<VersionManifest>() == 8);

/// Byte offset of the `ChunkDesc` array within a manifest chunk.
#[inline]
const fn chunks_offset() -> usize {
    core::mem::size_of::<VersionManifest>()
}

/// Byte offset of the batch-span (`u32`) array within a manifest chunk.
#[inline]
const fn spans_offset(chunk_count: usize) -> usize {
    chunks_offset() + chunk_count * core::mem::size_of::<ChunkDesc>()
}

/// The total serialized byte length of a manifest listing `chunk_count` own
/// chunks partitioned into `batch_count` own batches.
#[inline]
pub const fn manifest_len(chunk_count: usize, batch_count: usize) -> usize {
    spans_offset(chunk_count) + batch_count * core::mem::size_of::<u32>()
}

/// A validated link from a manifest to its predecessor's manifest (ADR-0013).
///
/// On the write side the committer fills it in after taking its reference on
/// the predecessor manifest chunk (`generation` is the chunk's generation at
/// that moment). On the read side it is what the manifest *claims* about its
/// predecessor — `depth` and `total_batches` are derived from the header
/// (`depth - 1`, `total_batches - batch_count`) so [`walk_chain_with`] can
/// check depth continuity and the batch total at every hop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestLink {
    /// The predecessor's version (`< ` the linking manifest's version).
    pub version: u64,
    /// Where the predecessor manifest chunk lives.
    pub mref: PackedRef,
    /// The predecessor manifest chunk's `ChunkCtrl` generation at link time.
    pub generation: u32,
    /// The predecessor's chain depth (the linking manifest's `depth - 1`).
    pub depth: u32,
    /// The predecessor's `total_batches` (the linking manifest's
    /// `total_batches - batch_count`).
    pub total_batches: u32,
}

/// A parsed, owned view of one manifest chunk.
///
/// Produced by [`read_manifest`]; carries the fixed header fields plus a copy of
/// the (small, 24-byte-each) **own** data-chunk descriptor list. The bulk payload
/// is never copied — these descriptors merely *name* the data chunks. A version's
/// full table is this manifest plus everything reachable through [`prev`](Self::prev)
/// (see [`walk_chain_with`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    /// The interned artifact name id this manifest self-identifies as.
    pub artifact_id: u32,
    /// The artifact version this manifest describes.
    pub version: u64,
    /// Interned Arrow schema id of the version's data chunks.
    pub schema_id: u32,
    /// The data-chunk descriptors this version **added**, **flat** and in row
    /// order. Reclamation frees every entry here — including the continuation
    /// chunks of any multi-chunk batch. Chunks inherited from the predecessor are
    /// reached through [`prev`](Self::prev), never duplicated here.
    pub chunks: Vec<ChunkDesc>,
    /// Batch boundaries over `chunks`: `batch_spans[b]` consecutive `chunks`
    /// form own Arrow batch `b`, and `sum(batch_spans) == chunks.len()`. One
    /// entry per batch; a single-chunk batch has span `1`.
    pub batch_spans: Vec<u32>,
    /// The predecessor link, or `None` for a root (Replace / first) version.
    pub prev: Option<ManifestLink>,
    /// Chain depth: `0` for a root, else `prev.depth + 1`.
    pub depth: u32,
    /// Batches in the whole chain: `batch_spans.len() + prev.total_batches`
    /// (saturating).
    pub total_batches: u32,
}

impl Manifest {
    /// The link this manifest would hand to a successor that chains onto it,
    /// given where it lives (`mref`) and its chunk's current `generation`.
    pub fn link_from(&self, mref: PackedRef, generation: u32) -> ManifestLink {
        ManifestLink {
            version: self.version,
            mref,
            generation,
            depth: self.depth,
            total_batches: self.total_batches,
        }
    }
}

/// Serialize a manifest into a freshly loaned chunk from `alloc`, returning the
/// manifest chunk's descriptor.
///
/// `artifact_id` is the interned artifact name id the manifest self-identifies
/// as; together with `version` it forms the `{artifact_id, version}` pair a
/// reader validates via [`read_manifest_checked`]. `chunks` / `batch_spans` are
/// the version's **own** data chunks and their batch partition. `prev` is the
/// predecessor link for an `Append` (the committer must already hold a
/// reference on that manifest chunk) or `None` for a root; `depth` and
/// `total_batches` are derived from it.
///
/// The caller is responsible for the chunk's [`ChunkCtrl`](shm_core::ChunkCtrl)
/// lifecycle (`try_loan` → `publish`) exactly as with an `shm-arrow` batch; the
/// chunk is written **once** and then immutable.
pub fn write_manifest<A: ChunkAllocator>(
    alloc: &A,
    artifact_id: u32,
    version: u64,
    schema_id: u32,
    chunks: &[ChunkDesc],
    batch_spans: &[u32],
    prev: Option<&ManifestLink>,
) -> Result<ChunkDesc> {
    let chunk_count = u32::try_from(chunks.len())
        .map_err(|_| Error::Unsupported("too many chunks in manifest"))?;
    let batch_count = u32::try_from(batch_spans.len())
        .map_err(|_| Error::Unsupported("too many batches in manifest"))?;
    debug_assert_eq!(
        batch_spans.iter().map(|&s| s as usize).sum::<usize>(),
        chunks.len(),
        "batch spans must partition the chunk list"
    );
    let (prev_version, prev_ref, prev_gen, depth, total_batches) = match prev {
        Some(link) => {
            if link.version >= version || link.version == 0 || link.mref.to_bits() == 0 {
                return Err(Error::Unsupported("manifest link must name an older version"));
            }
            let depth = link
                .depth
                .checked_add(1)
                .ok_or(Error::Unsupported("manifest chain too deep"))?;
            (
                link.version,
                link.mref.to_bits(),
                link.generation,
                depth,
                batch_count.saturating_add(link.total_batches),
            )
        }
        None => (0, 0, 0, 0, batch_count),
    };
    let total = manifest_len(chunks.len(), batch_spans.len());

    let desc = alloc.alloc(total)?;
    if (desc.len as usize) < total {
        // The allocator returned a smaller chunk than requested.
        return Err(Error::Arrow(shm_arrow::Error::ChunkTooSmall {
            need: total,
            have: desc.len as usize,
        }));
    }

    let base = alloc.resolve(&desc);
    let header = VersionManifest {
        magic: MANIFEST_MAGIC,
        version,
        artifact_id,
        schema_id,
        chunk_count,
        batch_count,
        prev_version,
        prev_ref,
        prev_gen,
        depth,
        total_batches,
        _reserved: 0,
    };
    // SAFETY: `base` points at a loaned chunk of `desc.len >= total` bytes,
    // exclusively owned by the caller. The header, each `ChunkDesc`, and each
    // batch-span `u32` write land at offsets `< total`, all within the chunk.
    // Records are `#[repr(C)]` PODs written unaligned to stay layout-agnostic.
    unsafe {
        base.cast::<VersionManifest>().write_unaligned(header);
        let arr = base.add(chunks_offset()).cast::<ChunkDesc>();
        for (i, c) in chunks.iter().enumerate() {
            arr.add(i).write_unaligned(*c);
        }
        let spans = base.add(spans_offset(chunks.len())).cast::<u32>();
        for (i, s) in batch_spans.iter().enumerate() {
            spans.add(i).write_unaligned(*s);
        }
    }
    Ok(desc)
}

/// Parse and validate the manifest chunk `mref` points at within `segment`.
///
/// Validates the magic and that the whole `header + [ChunkDesc; chunk_count] +
/// [u32; batch_count]` region lies inside the segment before reading the
/// descriptor array, then applies the root-strictness rules (module doc).
/// Returns [`Error::BadMagic`] on a wrong magic and [`Error::VersionGone`] /
/// [`shm_core::Error::OutOfBounds`] when the region does not fit or the header
/// is inconsistent (a sign the chunk was recycled out from under the reference).
pub fn read_manifest(segment: &Segment, mref: PackedRef) -> Result<Manifest> {
    let offset = mref.offset();

    // Bounds-check + resolve the fixed header first.
    let hdr_ptr = segment.resolve(offset, core::mem::size_of::<VersionManifest>() as u32)?;
    // SAFETY: `resolve` verified `[offset, offset + 64)` is inside the mapping;
    // a `VersionManifest` is a POD read unaligned from those bytes.
    let header = unsafe { hdr_ptr.cast::<VersionManifest>().read_unaligned() };
    if header.magic != MANIFEST_MAGIC {
        return Err(Error::BadMagic);
    }

    let chunk_count = header.chunk_count as usize;
    let batch_count = header.batch_count as usize;
    let total = manifest_len(chunk_count, batch_count);
    let total = u32::try_from(total).map_err(|_| Error::VersionGone)?;
    // Bounds-check the whole record (header + descriptor array + span array),
    // then parse it out of the now-validated byte region. Sharing
    // [`parse_manifest_bytes`] keeps the real (segment-backed) path and the
    // fuzzed/slice path on identical validation logic.
    let base = segment.resolve(offset, total)?;
    // SAFETY: `resolve` verified `[offset, offset + total)` is mapped, so `base`
    // is valid for `total` bytes; the slice does not outlive this call.
    let bytes = unsafe { core::slice::from_raw_parts(base, total as usize) };
    parse_manifest_bytes(bytes)
}

/// Parse and validate a manifest **purely over its chunk bytes** — the
/// untrusted-input boundary a corrupt or recycled manifest chunk reaches across.
///
/// `bytes` are the raw bytes of the manifest chunk (starting at the
/// [`VersionManifest`] header). Every count is untrusted, so this function:
///
/// - rejects a `bytes` shorter than the 64-byte header ([`Error::VersionGone`]);
/// - checks the magic ([`Error::BadMagic`]);
/// - computes the full `header + [ChunkDesc; chunk_count] + [u32; batch_count]`
///   length with **checked** arithmetic and rejects any input that does not hold
///   the whole record ([`Error::VersionGone`]);
/// - enforces root-strictness: `prev_ref == 0 ⇔ prev_version == 0 ⇔ depth == 0`,
///   `prev_gen == 0` for a root, `prev_version < version` when linked,
///   `total_batches >= batch_count`, `_reserved == 0` ([`Error::VersionGone`]).
///
/// It never panics and never reads out of bounds for **any** input — see the
/// `manifest` / `manifest_chain` fuzz targets and the `manifest_parser_*`
/// property tests.
///
/// [`read_manifest`] calls this after `Segment::resolve` has already proven the
/// region is mapped; exposing it lets the fuzzer exercise the exact same
/// validation without a real shared-memory segment.
pub fn parse_manifest_bytes(bytes: &[u8]) -> Result<Manifest> {
    use core::mem::size_of;

    if bytes.len() < size_of::<VersionManifest>() {
        return Err(Error::VersionGone);
    }
    // SAFETY: length checked above; `VersionManifest` is an all-integer POD.
    let header: VersionManifest = unsafe { read_pod(bytes, 0) };
    if header.magic != MANIFEST_MAGIC {
        return Err(Error::BadMagic);
    }

    // Root-strictness (ADR-0013).
    let is_root = header.prev_ref == 0;
    if is_root != (header.prev_version == 0)
        || is_root != (header.depth == 0)
        || (is_root && header.prev_gen != 0)
        || (!is_root && header.prev_version >= header.version)
        || header.total_batches < header.batch_count
        || header._reserved != 0
    {
        return Err(Error::VersionGone);
    }

    let chunk_count = header.chunk_count as usize;
    let batch_count = header.batch_count as usize;

    // Whole-record extent with checked math (attacker counts are ~4e9 each).
    let chunk_bytes = chunk_count
        .checked_mul(size_of::<ChunkDesc>())
        .ok_or(Error::VersionGone)?;
    let spans_off = chunks_offset()
        .checked_add(chunk_bytes)
        .ok_or(Error::VersionGone)?;
    let span_bytes = batch_count
        .checked_mul(size_of::<u32>())
        .ok_or(Error::VersionGone)?;
    let total = spans_off
        .checked_add(span_bytes)
        .ok_or(Error::VersionGone)?;
    debug_assert_eq!(total, manifest_len(chunk_count, batch_count));
    if total > bytes.len() {
        return Err(Error::VersionGone);
    }

    let mut chunks = Vec::with_capacity(chunk_count);
    for i in 0..chunk_count {
        // SAFETY: `total <= bytes.len()` proved the descriptor array is in bounds.
        chunks.push(unsafe {
            read_pod::<ChunkDesc>(bytes, chunks_offset() + i * size_of::<ChunkDesc>())
        });
    }
    let mut batch_spans = Vec::with_capacity(batch_count);
    for i in 0..batch_count {
        // SAFETY: as above; the span array sits at `spans_off < total`.
        batch_spans.push(unsafe { read_pod::<u32>(bytes, spans_off + i * size_of::<u32>()) });
    }

    let prev = if is_root {
        None
    } else {
        Some(ManifestLink {
            version: header.prev_version,
            mref: PackedRef(header.prev_ref),
            generation: header.prev_gen,
            // Both subtractions are guarded by the strictness checks above
            // (`depth >= 1` when linked; `total_batches >= batch_count` always).
            depth: header.depth - 1,
            total_batches: header.total_batches - header.batch_count,
        })
    };

    Ok(Manifest {
        artifact_id: header.artifact_id,
        version: header.version,
        schema_id: header.schema_id,
        chunks,
        batch_spans,
        prev,
        depth: header.depth,
        total_batches: header.total_batches,
    })
}

/// Copy a fixed-size POD `T` out of `buf` at byte `off`, unaligned.
///
/// # Safety
///
/// The caller must guarantee `off + size_of::<T>() <= buf.len()` and that every
/// bit pattern is a valid `T` (true for the all-integer POD records here).
#[inline]
unsafe fn read_pod<T: Copy>(buf: &[u8], off: usize) -> T {
    debug_assert!(off + core::mem::size_of::<T>() <= buf.len());
    let mut v = core::mem::MaybeUninit::<T>::uninit();
    // SAFETY: bounds guaranteed by the caller; regions don't overlap.
    unsafe {
        core::ptr::copy_nonoverlapping(
            buf.as_ptr().add(off),
            v.as_mut_ptr().cast::<u8>(),
            core::mem::size_of::<T>(),
        );
        v.assume_init()
    }
}

/// Read the manifest `mref` points at and validate it self-identifies as
/// `{expect_artifact_id, expect_version}`.
///
/// This is [`read_manifest`] plus the manifest self-validation mandated by
/// ADR-0003a: the `{artifact_id, version}` pair is monotonic and never reissued,
/// so a stale or foreign manifest (a recycled/ghost manifest chunk that still
/// holds intact-but-wrong bytes) is rejected with [`Error::StaleManifest`]
/// rather than validated against by a bare `version` check. Wrong magic /
/// out-of-bounds still surface as [`Error::BadMagic`] / [`Error::VersionGone`]
/// from [`read_manifest`].
///
/// This is the identity guard on every manifest dereference: the pin path, the
/// `Append` link, and each hop of a chain walk.
pub fn read_manifest_checked(
    segment: &Segment,
    mref: PackedRef,
    expect_artifact_id: u32,
    expect_version: u64,
) -> Result<Manifest> {
    let manifest = read_manifest(segment, mref)?;
    if manifest.artifact_id != expect_artifact_id || manifest.version != expect_version {
        return Err(Error::StaleManifest);
    }
    Ok(manifest)
}

/// Walk a manifest chain from `head` back to its root, returning the manifests
/// **oldest-first** (the root at index `0`, `head` last) — the row order of the
/// version's table.
///
/// `read_prev` resolves one link to the manifest it names; the walker owns every
/// structural check so a caller (or a fuzzer feeding it arbitrary manifests)
/// never loops or over-reads:
///
/// - the walk performs at most `head.depth` hops (the bound the header carries);
/// - each hop's manifest must self-identify as the link's `version`, which is
///   strictly smaller than the linking manifest's (so a cycle is impossible);
/// - depth is continuous (`prev.depth == cur.depth - 1`) and the chain ends
///   exactly at a root (`depth == 0`, no link);
/// - `artifact_id` and `schema_id` are constant along the chain;
/// - `total_batches` telescopes (`prev.total_batches == cur.total_batches -
///   cur.batch_count`) and the root's equals its own `batch_count`.
///
/// Any violation is [`Error::VersionGone`] (or whatever `read_prev` returned).
pub fn walk_chain_with<F>(head: &Manifest, mut read_prev: F) -> Result<Vec<Manifest>>
where
    F: FnMut(&ManifestLink) -> Result<Manifest>,
{
    let mut chain: Vec<Manifest> = Vec::with_capacity(head.depth as usize + 1);
    let mut cur = head.clone();
    let mut hops = 0u32;
    loop {
        // Own consistency of the current manifest.
        let own_batches = u32::try_from(cur.batch_spans.len()).map_err(|_| Error::VersionGone)?;
        if cur.total_batches < own_batches {
            return Err(Error::VersionGone);
        }
        if cur.artifact_id != head.artifact_id || cur.schema_id != head.schema_id {
            return Err(Error::VersionGone);
        }
        match cur.prev {
            None => {
                // A root: the chain ends here, exactly `head.depth` hops in.
                if cur.depth != 0 || hops != head.depth || cur.total_batches != own_batches {
                    return Err(Error::VersionGone);
                }
                chain.push(cur);
                break;
            }
            Some(link) => {
                if hops >= head.depth || cur.depth == 0 {
                    return Err(Error::VersionGone);
                }
                if link.version >= cur.version
                    || link.depth != cur.depth - 1
                    || link.total_batches != cur.total_batches - own_batches
                {
                    return Err(Error::VersionGone);
                }
                let prev = read_prev(&link)?;
                if prev.version != link.version
                    || prev.depth != link.depth
                    || prev.total_batches != link.total_batches
                {
                    return Err(Error::VersionGone);
                }
                chain.push(cur);
                cur = prev;
                hops += 1;
            }
        }
    }
    chain.reverse();
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift PRNG — committed, seed-stable fuzz stand-in.
    struct Rng(u64);
    impl Rng {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn byte(&mut self) -> u8 {
            (self.next_u64() & 0xff) as u8
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// A well-formed **root** manifest byte image with `chunk_count`/`batch_count`
    /// entries laid into a buffer of `len` bytes (as a mutation seed).
    fn valid_manifest(chunk_count: u32, batch_count: u32, len: usize) -> Vec<u8> {
        let mut b = vec![0u8; len];
        if len >= 64 {
            b[0..8].copy_from_slice(&MANIFEST_MAGIC.to_le_bytes());
            b[8..16].copy_from_slice(&5u64.to_le_bytes()); // version
            b[16..20].copy_from_slice(&7u32.to_le_bytes()); // artifact_id
            b[20..24].copy_from_slice(&1u32.to_le_bytes()); // schema_id
            b[24..28].copy_from_slice(&chunk_count.to_le_bytes());
            b[28..32].copy_from_slice(&batch_count.to_le_bytes());
            // prev_version / prev_ref / prev_gen / depth = 0 (root)
            b[56..60].copy_from_slice(&batch_count.to_le_bytes()); // total_batches
        }
        b
    }

    /// Link a valid image to `{prev_version, prev_ref, prev_gen}` at `depth`
    /// with `total_batches`.
    fn link(b: &mut [u8], prev_version: u64, prev_ref: u64, prev_gen: u32, depth: u32, total: u32) {
        b[32..40].copy_from_slice(&prev_version.to_le_bytes());
        b[40..48].copy_from_slice(&prev_ref.to_le_bytes());
        b[48..52].copy_from_slice(&prev_gen.to_le_bytes());
        b[52..56].copy_from_slice(&depth.to_le_bytes());
        b[56..60].copy_from_slice(&total.to_le_bytes());
    }

    #[test]
    fn manifest_parser_rejects_oversized_counts_no_oob() {
        // The threat: a huge chunk_count/batch_count must not walk off the buffer.
        let mut b = valid_manifest(0, 0, 96);
        b[24..28].copy_from_slice(&u32::MAX.to_le_bytes()); // chunk_count
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));

        let mut b = valid_manifest(0, 0, 96);
        b[28..32].copy_from_slice(&u32::MAX.to_le_bytes()); // batch_count
        b[56..60].copy_from_slice(&u32::MAX.to_le_bytes()); // total_batches >= batch_count
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));

        // A count that fits the u32 length cap but overruns the actual bytes.
        let mut b = valid_manifest(0, 0, 96);
        b[24..28].copy_from_slice(&3u32.to_le_bytes()); // 3 * 24 = 72 > 96-64
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
    }

    #[test]
    fn manifest_parser_rejects_short_and_bad_magic() {
        for n in 0..64usize {
            assert!(matches!(
                parse_manifest_bytes(&vec![0u8; n]),
                Err(Error::VersionGone)
            ));
        }
        assert!(matches!(
            parse_manifest_bytes(&[0u8; 96]),
            Err(Error::BadMagic)
        ));
    }

    #[test]
    fn manifest_parser_round_trips_a_root_image() {
        // 2 chunks in 2 single-chunk batches: total = 64 + 2*24 + 2*4 = 120.
        let total = manifest_len(2, 2);
        assert_eq!(total, 120);
        let mut b = valid_manifest(2, 2, total);
        // Fill the two ChunkDesc entries + spans with recognizable values.
        for i in 0..2u32 {
            let at = 64 + i as usize * 24;
            b[at..at + 4].copy_from_slice(&(100 + i).to_le_bytes()); // segment_id
            b[at + 12..at + 16].copy_from_slice(&4096u32.to_le_bytes()); // len
        }
        let sp = 64 + 2 * 24;
        b[sp..sp + 4].copy_from_slice(&1u32.to_le_bytes());
        b[sp + 4..sp + 8].copy_from_slice(&1u32.to_le_bytes());
        let m = parse_manifest_bytes(&b).expect("valid manifest parses");
        assert_eq!(m.chunks.len(), 2);
        assert_eq!(m.batch_spans, vec![1, 1]);
        assert_eq!(m.chunks[0].segment_id, 100);
        assert_eq!(m.artifact_id, 7);
        assert_eq!(m.version, 5);
        assert_eq!(m.prev, None);
        assert_eq!(m.depth, 0);
        assert_eq!(m.total_batches, 2);
    }

    #[test]
    fn manifest_parser_round_trips_a_linked_image() {
        let total = manifest_len(1, 1);
        let mut b = valid_manifest(1, 1, total);
        let sp = 64 + 24;
        b[sp..sp + 4].copy_from_slice(&1u32.to_le_bytes());
        // version 5 links to version 3 at depth 2 (prev depth 1), 4 batches total.
        link(&mut b, 3, 0x0000_0001_0000_1000, 9, 2, 4);
        let m = parse_manifest_bytes(&b).expect("linked manifest parses");
        assert_eq!(m.depth, 2);
        assert_eq!(m.total_batches, 4);
        assert_eq!(
            m.prev,
            Some(ManifestLink {
                version: 3,
                mref: PackedRef(0x0000_0001_0000_1000),
                generation: 9,
                depth: 1,
                total_batches: 3,
            })
        );
        // A never-recycled predecessor legitimately has generation 0.
        link(&mut b, 3, 0x0000_0001_0000_1000, 0, 2, 4);
        assert!(parse_manifest_bytes(&b).is_ok());
    }

    #[test]
    fn manifest_parser_enforces_root_strictness() {
        let total = manifest_len(0, 0);
        let ok = valid_manifest(0, 0, total);
        assert!(parse_manifest_bytes(&ok).is_ok());

        // prev_ref without prev_version / depth.
        let mut b = ok.clone();
        b[40..48].copy_from_slice(&1u64.to_le_bytes());
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // prev_version without prev_ref.
        let mut b = ok.clone();
        b[32..40].copy_from_slice(&1u64.to_le_bytes());
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // depth without a link.
        let mut b = ok.clone();
        b[52..56].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // prev_gen on a root.
        let mut b = ok.clone();
        b[48..52].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // Linked but prev_version >= version (a self-link, and a future link).
        let mut b = ok.clone();
        link(&mut b, 5, 1, 0, 1, 0);
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        let mut b = ok.clone();
        link(&mut b, 6, 1, 0, 1, 0);
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // Linked with depth 0.
        let mut b = ok.clone();
        link(&mut b, 4, 1, 0, 0, 0);
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // total_batches < batch_count.
        let mut b = valid_manifest(1, 1, manifest_len(1, 1));
        b[56..60].copy_from_slice(&0u32.to_le_bytes());
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // Reserved word set.
        let mut b = ok.clone();
        b[60..64].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
        // A well-formed link still parses.
        let mut b = ok.clone();
        link(&mut b, 4, 1, 0, 1, 0);
        assert!(parse_manifest_bytes(&b).is_ok());
    }

    /// Property-fuzz: many deterministic iterations of random + mutated-valid
    /// bytes must never panic and never read out of bounds.
    #[test]
    fn manifest_parser_never_panics_on_arbitrary_bytes() {
        let mut rng = Rng(0x5eed_dead_beef_0002);
        // miri interprets ~100x slower; 2k iterations still cover every branch.
        let iters = if cfg!(miri) { 2_000 } else { 200_000 };
        for _ in 0..iters {
            let b: Vec<u8> = if rng.below(2) == 0 {
                let len = rng.below(160);
                (0..len).map(|_| rng.byte()).collect()
            } else {
                let cc = rng.below(6) as u32;
                let bc = rng.below(6) as u32;
                let len = 64 + rng.below(160);
                let mut b = valid_manifest(cc, bc, len);
                if rng.below(2) == 0 {
                    link(&mut b, 1 + rng.below(4) as u64, 1 + rng.next_u64() % 1000, rng.byte() as u32, 1 + rng.below(3) as u32, bc + rng.below(4) as u32);
                }
                for _ in 0..rng.below(8) {
                    if !b.is_empty() {
                        let i = rng.below(b.len());
                        b[i] ^= rng.byte();
                    }
                }
                b
            };
            let _ = parse_manifest_bytes(&b);
        }
    }

    // ---- walk_chain_with ----

    fn mref(v: u64) -> PackedRef {
        PackedRef(0x1_0000_0000 | (v << 8))
    }

    fn root(version: u64, batches: u32) -> Manifest {
        Manifest {
            artifact_id: 7,
            version,
            schema_id: 1,
            chunks: vec![ChunkDesc::ZERO; batches as usize],
            batch_spans: vec![1; batches as usize],
            prev: None,
            depth: 0,
            total_batches: batches,
        }
    }

    fn linked(version: u64, batches: u32, prev: &Manifest) -> Manifest {
        Manifest {
            artifact_id: prev.artifact_id,
            version,
            schema_id: prev.schema_id,
            chunks: vec![ChunkDesc::ZERO; batches as usize],
            batch_spans: vec![1; batches as usize],
            prev: Some(prev.link_from(mref(prev.version), 0)),
            depth: prev.depth + 1,
            total_batches: prev.total_batches + batches,
        }
    }

    /// A chain `store` keyed by manifest ref, as `read_prev` would resolve it.
    fn store(ms: &[Manifest]) -> impl FnMut(&ManifestLink) -> Result<Manifest> + '_ {
        move |l: &ManifestLink| {
            ms.iter()
                .find(|m| mref(m.version) == l.mref)
                .cloned()
                .ok_or(Error::VersionGone)
        }
    }

    #[test]
    fn walk_chain_happy_path_is_oldest_first() {
        let a = root(1, 2);
        let b = linked(2, 1, &a);
        let c = linked(3, 3, &b);
        let all = [a.clone(), b.clone(), c.clone()];
        let chain = walk_chain_with(&c, store(&all)).unwrap();
        assert_eq!(chain, vec![a.clone(), b, c]);
        assert_eq!(chain.last().unwrap().total_batches, 6);
        // A root walks to itself.
        assert_eq!(walk_chain_with(&a, store(&all)).unwrap(), vec![a]);
    }

    #[test]
    fn walk_chain_rejects_cycle_depth_overrun_and_ordering() {
        let a = root(1, 1);
        let b = linked(2, 1, &a);
        let mut c = linked(3, 1, &b);
        let all = [a.clone(), b.clone(), c.clone()];

        // Cycle: c → b → c (b's link points forward). Strict ordering catches it.
        let mut b_cycle = b.clone();
        b_cycle.prev = Some(c.link_from(mref(3), 0));
        assert!(matches!(
            walk_chain_with(&c, store(&[a.clone(), b_cycle, c.clone()])),
            Err(Error::VersionGone)
        ));

        // Depth overrun: head claims depth 1 but the chain is 2 deep.
        c.depth = 1;
        c.prev = Some(ManifestLink { depth: 0, ..c.prev.unwrap() });
        assert!(matches!(
            walk_chain_with(&c, store(&all)),
            Err(Error::VersionGone)
        ));

        // Non-decreasing version on the link itself.
        let mut d = linked(4, 1, &b);
        d.prev = Some(ManifestLink { version: 4, ..d.prev.unwrap() });
        assert!(matches!(
            walk_chain_with(&d, store(&all)),
            Err(Error::VersionGone)
        ));

        // The resolved predecessor does not self-identify as the linked version.
        let mut e = linked(5, 1, &b);
        e.prev = Some(ManifestLink { mref: mref(1), ..e.prev.unwrap() });
        assert!(matches!(
            walk_chain_with(&e, store(&all)),
            Err(Error::VersionGone)
        ));

        // Missing predecessor propagates the resolver's error.
        let f = linked(6, 1, &b);
        assert!(matches!(
            walk_chain_with(&f, store(std::slice::from_ref(&f))),
            Err(Error::VersionGone)
        ));
    }

    #[test]
    fn walk_chain_rejects_schema_drift_and_batch_total_mismatch() {
        let a = root(1, 2);
        let b = linked(2, 1, &a);
        let mut a_drift = a.clone();
        a_drift.schema_id = 2;
        assert!(matches!(
            walk_chain_with(&b, store(&[a_drift, b.clone()])),
            Err(Error::VersionGone)
        ));

        // total_batches does not telescope: b claims 5 total over a's 2.
        let mut b_bad = b.clone();
        b_bad.total_batches = 5;
        b_bad.prev = Some(ManifestLink { total_batches: 4, ..b_bad.prev.unwrap() });
        assert!(matches!(
            walk_chain_with(&b_bad, store(&[a.clone(), b_bad.clone()])),
            Err(Error::VersionGone)
        ));

        // A root whose total_batches != its own batch_count.
        let mut a_bad = a.clone();
        a_bad.total_batches = 3;
        assert!(matches!(
            walk_chain_with(&a_bad, store(std::slice::from_ref(&a_bad))),
            Err(Error::VersionGone)
        ));
    }
}
