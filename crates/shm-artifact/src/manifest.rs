//! The on-chunk [`VersionManifest`] ABI and its read/write helpers.
//!
//! A manifest is an **immutable** chunk that names the data chunk(s) making up
//! one version of an artifact. It is itself a chunk (obtained from a
//! [`ChunkAllocator`]), referenced by a [`ChunkDesc`], and pointed at from the
//! [`ArtifactHead`](crate::ArtifactHead) by a [`PackedRef`](shm_core::PackedRef).
//!
//! # Layout (frozen ABI)
//!
//! Written at the start of a loaned chunk (i.e. at `segment_base +
//! ChunkDesc::offset`), all offsets chunk-relative:
//!
//! ```text
//! +--------------------------------------------------------------+
//! | VersionManifest    (24 B, at chunk offset 0)                 |
//! |   magic:u64  version:u64  schema_id:u32  chunk_count:u32     |
//! | ChunkDesc * chunk_count   (24 B each, at chunk offset 24)    |
//! +--------------------------------------------------------------+
//! ```
//!
//! The records are plain `#[repr(C)]` PODs written unaligned; a manifest chunk
//! is never mutated after publication, so no atomics / `SharedPod` machinery is
//! needed here (mirroring `shm-arrow`'s batch layout).

use shm_arrow::ChunkAllocator;
use shm_core::{ChunkDesc, PackedRef, Segment};

use crate::error::{Error, Result};

/// Manifest magic: little-endian bytes of `b"SHMMFST1"`.
pub const MANIFEST_MAGIC: u64 = u64::from_le_bytes(*b"SHMMFST1");

/// The fixed header at the start of a manifest chunk (24 bytes), followed by a
/// flat `[ChunkDesc; chunk_count]` array.
///
/// # Layout (frozen ABI — 24 bytes)
///
/// | field         | type  | meaning                                       |
/// |---------------|-------|-----------------------------------------------|
/// | `magic`       | `u64` | must equal [`MANIFEST_MAGIC`]                 |
/// | `version`     | `u64` | the artifact version this manifest describes  |
/// | `schema_id`   | `u32` | interned Arrow schema id of the data chunks   |
/// | `chunk_count` | `u32` | number of `ChunkDesc`s that follow the header |
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionManifest {
    /// Must equal [`MANIFEST_MAGIC`].
    pub magic: u64,
    /// The artifact version number this manifest describes.
    pub version: u64,
    /// Interned Arrow schema id shared by every data chunk in this version.
    pub schema_id: u32,
    /// Number of [`ChunkDesc`] entries that follow this header.
    pub chunk_count: u32,
}

const _: () = assert!(core::mem::size_of::<VersionManifest>() == 24);
const _: () = assert!(core::mem::align_of::<VersionManifest>() == 8);

/// Byte offset of the `ChunkDesc` array within a manifest chunk.
#[inline]
const fn chunks_offset() -> usize {
    core::mem::size_of::<VersionManifest>()
}

/// The total serialized byte length of a manifest listing `chunk_count` chunks.
#[inline]
pub const fn manifest_len(chunk_count: usize) -> usize {
    chunks_offset() + chunk_count * core::mem::size_of::<ChunkDesc>()
}

/// A parsed, owned view of a manifest chunk.
///
/// Produced by [`read_manifest`]; carries the fixed header fields plus a copy of
/// the (small, 24-byte-each) data-chunk descriptor list. The bulk payload is
/// never copied — these descriptors merely *name* the data chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    /// The artifact version this manifest describes.
    pub version: u64,
    /// Interned Arrow schema id of the version's data chunks.
    pub schema_id: u32,
    /// The data-chunk descriptors making up this version, in row order.
    pub chunks: Vec<ChunkDesc>,
}

/// Serialize a manifest into a freshly loaned chunk from `alloc`, returning the
/// manifest chunk's descriptor.
///
/// The caller is responsible for the chunk's [`ChunkCtrl`](shm_core::ChunkCtrl)
/// lifecycle (`try_loan` → `publish`) exactly as with an `shm-arrow` batch; the
/// chunk is written **once** and then immutable.
pub fn write_manifest<A: ChunkAllocator>(
    alloc: &A,
    version: u64,
    schema_id: u32,
    chunks: &[ChunkDesc],
) -> Result<ChunkDesc> {
    let chunk_count =
        u32::try_from(chunks.len()).map_err(|_| Error::Unsupported("too many chunks in manifest"))?;
    let total = manifest_len(chunks.len());

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
        schema_id,
        chunk_count,
    };
    // SAFETY: `base` points at a loaned chunk of `desc.len >= total` bytes,
    // exclusively owned by the caller. The header write and each `ChunkDesc`
    // write below land at offsets `< total`, all within the chunk. Records are
    // `#[repr(C)]` PODs written unaligned to stay layout-agnostic.
    unsafe {
        base.cast::<VersionManifest>().write_unaligned(header);
        let arr = base.add(chunks_offset()).cast::<ChunkDesc>();
        for (i, c) in chunks.iter().enumerate() {
            arr.add(i).write_unaligned(*c);
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

    let total = manifest_len(header.chunk_count as usize);
    let total = u32::try_from(total).map_err(|_| Error::VersionGone)?;
    // Bounds-check the whole record (header + descriptor array).
    let base = segment.resolve(offset, total)?;

    let mut chunks = Vec::with_capacity(header.chunk_count as usize);
    // SAFETY: `resolve` verified the full `total`-byte region is mapped; the
    // descriptor array begins at `chunks_offset()` and holds `chunk_count`
    // 24-byte PODs, all within that region. Reads are unaligned-safe.
    unsafe {
        let arr = base.add(chunks_offset()).cast::<ChunkDesc>();
        for i in 0..header.chunk_count as usize {
            chunks.push(arr.add(i).read_unaligned());
        }
    }

    Ok(Manifest {
        version: header.version,
        schema_id: header.schema_id,
        chunks,
    })
}
