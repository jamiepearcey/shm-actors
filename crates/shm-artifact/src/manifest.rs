//! The on-chunk [`VersionManifest`] ABI and its read/write helpers.
//!
//! A manifest is an **immutable** chunk that names the data chunk(s) making up
//! one version of an artifact. It is itself a chunk (obtained from a
//! [`ChunkAllocator`]), referenced by a [`ChunkDesc`], and pointed at from the
//! [`ArtifactHead`](crate::ArtifactHead) by a [`PackedRef`](shm_core::PackedRef).
//!
//! # Layout (frozen ABI — v0.3 / ADR-0003 item F, `SHMMFST3`)
//!
//! Written at the start of a loaned chunk (i.e. at `segment_base +
//! ChunkDesc::offset`), all offsets chunk-relative:
//!
//! ```text
//! +----------------------------------------------------------------+
//! | VersionManifest    (32 B, at chunk offset 0)                   |
//! |   magic:u64  version:u64                                       |
//! |   artifact_id:u32  schema_id:u32  chunk_count:u32  batch_count |
//! | ChunkDesc * chunk_count   (24 B each, at chunk offset 32)      |
//! | u32 batch_span * batch_count   (4 B each)                      |
//! +----------------------------------------------------------------+
//! ```
//!
//! # Batch boundaries (item F)
//!
//! A version's data chunks are a **flat** list, but with multi-chunk batches
//! (item F) a single Arrow batch may span several consecutive chunks. The
//! `batch_span` array partitions the flat `chunk` list into batches: batch `b`
//! occupies `batch_span[b]` consecutive chunks, and `sum(batch_span) ==
//! chunk_count`. [`Artifact::as_arrow`](crate::VersionPin::as_arrow) uses the
//! spans to hand each batch's chunks to
//! [`read_batch_chunks`](shm_arrow::read_batch_chunks); reclamation stays trivial
//! because **every** chunk — primary or continuation — is a flat `chunk` entry,
//! so retiring the version frees them all with no per-batch bookkeeping.
//!
//! The records are plain `#[repr(C)]` PODs written unaligned; a manifest chunk
//! is never mutated after publication, so no atomics / `SharedPod` machinery is
//! needed here (mirroring `shm-arrow`'s batch layout).

use shm_arrow::ChunkAllocator;
use shm_core::{ChunkDesc, PackedRef, Segment};

use crate::error::{Error, Result};

/// Manifest magic: little-endian bytes of `b"SHMMFST3"`.
///
/// Bumped from `SHMMFST2` for the v0.3 item-F layout: the trailing `_pad`
/// header word became `batch_count`, and a `[u32; batch_count]` batch-span array
/// now follows the `ChunkDesc` array (partitioning the flat chunk list into
/// multi-chunk batches). A `SHMMFST2`/`SHMMFST1` manifest is rejected by the
/// magic check.
pub const MANIFEST_MAGIC: u64 = u64::from_le_bytes(*b"SHMMFST3");

/// The fixed header at the start of a manifest chunk (32 bytes), followed by a
/// flat `[ChunkDesc; chunk_count]` array.
///
/// # Layout (frozen ABI — 32 bytes, v0.3 / ADR-0003a)
///
/// | field         | type  | meaning                                       |
/// |---------------|-------|-----------------------------------------------|
/// | `magic`       | `u64` | must equal [`MANIFEST_MAGIC`]                 |
/// | `version`     | `u64` | the artifact version this manifest describes  |
/// | `artifact_id` | `u32` | interned artifact name id this manifest is for|
/// | `schema_id`   | `u32` | interned Arrow schema id of the data chunks   |
/// | `chunk_count` | `u32` | number of `ChunkDesc`s that follow the header |
/// | `batch_count` | `u32` | number of `u32` batch spans after the chunks   |
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
    /// Interned Arrow schema id shared by every data chunk in this version.
    pub schema_id: u32,
    /// Number of [`ChunkDesc`] entries that follow this header.
    pub chunk_count: u32,
    /// Number of `u32` batch-span entries after the `ChunkDesc` array. Their sum
    /// equals `chunk_count`; each span is the chunk count of one Arrow batch.
    pub batch_count: u32,
}

const _: () = assert!(core::mem::size_of::<VersionManifest>() == 32);
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

/// The total serialized byte length of a manifest listing `chunk_count` chunks
/// partitioned into `batch_count` batches.
#[inline]
pub const fn manifest_len(chunk_count: usize, batch_count: usize) -> usize {
    spans_offset(chunk_count) + batch_count * core::mem::size_of::<u32>()
}

/// A parsed, owned view of a manifest chunk.
///
/// Produced by [`read_manifest`]; carries the fixed header fields plus a copy of
/// the (small, 24-byte-each) data-chunk descriptor list. The bulk payload is
/// never copied — these descriptors merely *name* the data chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    /// The interned artifact name id this manifest self-identifies as.
    pub artifact_id: u32,
    /// The artifact version this manifest describes.
    pub version: u64,
    /// Interned Arrow schema id of the version's data chunks.
    pub schema_id: u32,
    /// The data-chunk descriptors making up this version, **flat** and in row
    /// order. Reclamation frees every entry here — including the continuation
    /// chunks of any multi-chunk batch.
    pub chunks: Vec<ChunkDesc>,
    /// Batch boundaries: `batch_spans[b]` consecutive `chunks` form Arrow batch
    /// `b`, and `sum(batch_spans) == chunks.len()`. One entry per batch; a
    /// single-chunk batch has span `1`.
    pub batch_spans: Vec<u32>,
}

/// Serialize a manifest into a freshly loaned chunk from `alloc`, returning the
/// manifest chunk's descriptor.
///
/// `artifact_id` is the interned artifact name id the manifest self-identifies
/// as; together with `version` it forms the `{artifact_id, version}` pair a
/// reader validates via [`read_manifest_checked`].
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
) -> Result<ChunkDesc> {
    let chunk_count =
        u32::try_from(chunks.len()).map_err(|_| Error::Unsupported("too many chunks in manifest"))?;
    let batch_count = u32::try_from(batch_spans.len())
        .map_err(|_| Error::Unsupported("too many batches in manifest"))?;
    debug_assert_eq!(
        batch_spans.iter().map(|&s| s as usize).sum::<usize>(),
        chunks.len(),
        "batch spans must partition the chunk list"
    );
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
/// Validates the magic and that the whole `header + [ChunkDesc; chunk_count]`
/// region lies inside the segment before reading the descriptor array. Returns
/// [`Error::BadMagic`] on a wrong magic and [`Error::VersionGone`] /
/// [`shm_core::Error::OutOfBounds`] when the region does not fit (a sign the
/// chunk was recycled out from under the reference).
pub fn read_manifest(segment: &Segment, mref: PackedRef) -> Result<Manifest> {
    let offset = mref.offset();

    // Bounds-check + resolve the fixed header first.
    let hdr_ptr = segment.resolve(offset, core::mem::size_of::<VersionManifest>() as u32)?;
    // SAFETY: `resolve` verified `[offset, offset + 24)` is inside the mapping;
    // a `VersionManifest` is a POD read unaligned from those bytes.
    let header = unsafe { hdr_ptr.cast::<VersionManifest>().read_unaligned() };
    if header.magic != MANIFEST_MAGIC {
        return Err(Error::BadMagic);
    }

    let chunk_count = header.chunk_count as usize;
    let batch_count = header.batch_count as usize;
    let total = manifest_len(chunk_count, batch_count);
    let total = u32::try_from(total).map_err(|_| Error::VersionGone)?;
    // Bounds-check the whole record (header + descriptor array + span array).
    let base = segment.resolve(offset, total)?;

    let mut chunks = Vec::with_capacity(chunk_count);
    let mut batch_spans = Vec::with_capacity(batch_count);
    // SAFETY: `resolve` verified the full `total`-byte region is mapped; the
    // descriptor array begins at `chunks_offset()` (`chunk_count` 24-byte PODs)
    // and the span array at `spans_offset(chunk_count)` (`batch_count` `u32`s),
    // all within that region. Reads are unaligned-safe.
    unsafe {
        let arr = base.add(chunks_offset()).cast::<ChunkDesc>();
        for i in 0..chunk_count {
            chunks.push(arr.add(i).read_unaligned());
        }
        let spans = base.add(spans_offset(chunk_count)).cast::<u32>();
        for i in 0..batch_count {
            batch_spans.push(spans.add(i).read_unaligned());
        }
    }

    Ok(Manifest {
        artifact_id: header.artifact_id,
        version: header.version,
        schema_id: header.schema_id,
        chunks,
        batch_spans,
    })
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
/// S0 populates the field and wires this checked read into the pin / commit
/// paths; the full reader/reclaimer `SeqCst` hazard handshake it anchors is S1's
/// job (ADR-0003a).
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
