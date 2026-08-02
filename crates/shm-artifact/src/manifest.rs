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
    let chunk_count = u32::try_from(chunks.len())
        .map_err(|_| Error::Unsupported("too many chunks in manifest"))?;
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
/// - rejects a `bytes` shorter than the 32-byte header ([`Error::VersionGone`]);
/// - checks the magic ([`Error::BadMagic`]);
/// - computes the full `header + [ChunkDesc; chunk_count] + [u32; batch_count]`
///   length with **checked** arithmetic and rejects any input that does not hold
///   the whole record ([`Error::VersionGone`]).
///
/// It never panics and never reads out of bounds for **any** input — see the
/// `fuzz_manifest` target and the `manifest_parser_*` property tests.
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

    Ok(Manifest {
        artifact_id: header.artifact_id,
        version: header.version,
        schema_id: header.schema_id,
        chunks,
        batch_spans,
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

    /// A well-formed manifest byte image with `chunk_count`/`batch_count` entries
    /// laid into a buffer of `len` bytes (as a mutation seed).
    fn valid_manifest(chunk_count: u32, batch_count: u32, len: usize) -> Vec<u8> {
        let mut b = vec![0u8; len];
        if len >= 32 {
            b[0..8].copy_from_slice(&MANIFEST_MAGIC.to_le_bytes());
            b[8..16].copy_from_slice(&5u64.to_le_bytes()); // version
            b[16..20].copy_from_slice(&7u32.to_le_bytes()); // artifact_id
            b[20..24].copy_from_slice(&1u32.to_le_bytes()); // schema_id
            b[24..28].copy_from_slice(&chunk_count.to_le_bytes());
            b[28..32].copy_from_slice(&batch_count.to_le_bytes());
        }
        b
    }

    #[test]
    fn manifest_parser_rejects_oversized_counts_no_oob() {
        // The threat: a huge chunk_count/batch_count must not walk off the buffer.
        let mut b = valid_manifest(0, 0, 64);
        b[24..28].copy_from_slice(&u32::MAX.to_le_bytes()); // chunk_count
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));

        let mut b = valid_manifest(0, 0, 64);
        b[28..32].copy_from_slice(&u32::MAX.to_le_bytes()); // batch_count
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));

        // A count that fits the u32 length cap but overruns the actual bytes.
        let mut b = valid_manifest(0, 0, 64);
        b[24..28].copy_from_slice(&3u32.to_le_bytes()); // 3 * 24 = 72 > 64-32
        assert!(matches!(parse_manifest_bytes(&b), Err(Error::VersionGone)));
    }

    #[test]
    fn manifest_parser_rejects_short_and_bad_magic() {
        for n in 0..32usize {
            assert!(matches!(
                parse_manifest_bytes(&vec![0u8; n]),
                Err(Error::VersionGone)
            ));
        }
        assert!(matches!(
            parse_manifest_bytes(&[0u8; 64]),
            Err(Error::BadMagic)
        ));
    }

    #[test]
    fn manifest_parser_round_trips_a_valid_image() {
        // 2 chunks in 2 single-chunk batches: total = 32 + 2*24 + 2*4 = 88.
        let total = manifest_len(2, 2);
        let mut b = valid_manifest(2, 2, total);
        // Fill the two ChunkDesc entries + spans with recognizable values.
        for i in 0..2u32 {
            let at = 32 + i as usize * 24;
            b[at..at + 4].copy_from_slice(&(100 + i).to_le_bytes()); // segment_id
            b[at + 12..at + 16].copy_from_slice(&4096u32.to_le_bytes()); // len
        }
        let sp = 32 + 2 * 24;
        b[sp..sp + 4].copy_from_slice(&1u32.to_le_bytes());
        b[sp + 4..sp + 8].copy_from_slice(&1u32.to_le_bytes());
        let m = parse_manifest_bytes(&b).expect("valid manifest parses");
        assert_eq!(m.chunks.len(), 2);
        assert_eq!(m.batch_spans, vec![1, 1]);
        assert_eq!(m.chunks[0].segment_id, 100);
        assert_eq!(m.artifact_id, 7);
        assert_eq!(m.version, 5);
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
                let len = rng.below(128);
                (0..len).map(|_| rng.byte()).collect()
            } else {
                let cc = rng.below(6) as u32;
                let bc = rng.below(6) as u32;
                let len = 32 + rng.below(160);
                let mut b = valid_manifest(cc, bc, len);
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
}
