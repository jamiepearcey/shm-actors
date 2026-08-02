//! The zero-copy read path: reconstruct a `RecordBatch` over the mapped chunk(s).

use std::panic::RefUnwindSafe;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::{make_array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;

use crate::error::{Error, Result};
use crate::layout::{
    buffers_offset, nodes_offset, BatchHeader, BufferEntry, NodeEntry, BATCH_FORMAT, BATCH_MAGIC,
};
use crate::schema::SchemaRegistry;
use shm_core::{ChunkCtrl, ChunkDesc};

/// The segment-base resolver a [`read_batch`] owner must provide.
///
/// The owner both (a) keeps the mapping alive for the lifetime of every buffer
/// built over it and (b) resolves the segment base a [`ChunkDesc`]'s `offset`
/// is measured from. Coupling the two removes any raw-pointer argument from
/// [`read_batch`]: the keep-alive *is* the resolver.
pub trait SegmentBase {
    /// Base pointer of the mapped segment the descriptor's `offset` is relative
    /// to. Must stay valid for as long as `self` is alive.
    fn base_ptr(&self) -> *const u8;
}

/// An example pin guard: the owner moved into every reconstructed buffer.
///
/// It keeps the mapping the batch points into alive (here, by owning an
/// `Arc<Segment>`); in a full runtime it would additionally hold the shared
/// [`ChunkCtrl`] pin (refcount) so the chunk cannot be recycled while a reader
/// holds a `RecordBatch` over it. Any `'static` type that is `SegmentBase +
/// Send + Sync + RefUnwindSafe` works as the [`read_batch`] owner — this is
/// just the canonical one.
pub struct PinGuard {
    segment: Arc<shm_core::Segment>,
}

impl PinGuard {
    /// Wrap a shared segment handle so buffers built over it keep it mapped.
    pub fn new(segment: Arc<shm_core::Segment>) -> PinGuard {
        PinGuard { segment }
    }

    /// The base pointer of the pinned segment mapping.
    pub fn base_ptr(&self) -> *const u8 {
        self.segment.base_ptr()
    }

    /// The pinned segment.
    pub fn segment(&self) -> &shm_core::Segment {
        &self.segment
    }
}

impl SegmentBase for PinGuard {
    fn base_ptr(&self) -> *const u8 {
        self.segment.base_ptr()
    }
}

impl RefUnwindSafe for PinGuard {}

/// Reconstruct a `RecordBatch` from a **single-chunk** batch, zero-copy.
///
/// A convenience wrapper over [`read_batch_chunks`] for the common case (the
/// batch occupies exactly one chunk). `ctrl`/`desc` name that chunk; the
/// reconstructed batch's buffers point directly into the mapped segment.
pub fn read_batch<O>(
    owner: Arc<O>,
    ctrl: &ChunkCtrl,
    desc: &ChunkDesc,
    registry: &SchemaRegistry,
) -> Result<RecordBatch>
where
    O: SegmentBase + Send + Sync + RefUnwindSafe + 'static,
{
    read_batch_chunks(owner, &[*desc], &[ctrl], registry)
}

/// Reconstruct a `RecordBatch` that points **directly** at the mapped chunk(s) —
/// no payload is copied.
///
/// `chunks` is the batch's ordered chunk list (`chunks[0]` the primary, holding
/// the [`BatchHeader`] + tables); `ctrls` are their control words (parallel to
/// `chunks`). Each Arrow [`Buffer`] is built with
/// [`Buffer::from_custom_allocation`](arrow_buffer::Buffer::from_custom_allocation)
/// over a pointer into the segment, taking a clone of `owner` as its keep-alive.
///
/// # Validation order
///
/// 1. `ctrl.validate(desc)` for **every** chunk — generation match, else
///    [`shm_core::Error::StaleDescriptor`].
/// 2. [`BatchHeader`] magic + format, else [`Error::BadMagic`].
/// 3. header `schema_id` vs `chunks[0].schema_id`, else [`Error::SchemaMismatch`].
/// 4. `chunk_count` vs `chunks.len()`, and every buffer entry in-bounds of its
///    chunk, else [`Error::Layout`].
/// 5. registry lookup, else [`Error::UnknownSchema`].
///
/// # Safety-relevant contract
///
/// The caller must hold every chunk in `PUBLISHED` state with a shared pin for
/// the lifetime of `owner` (the borrow discipline of [`ChunkCtrl`]). Given that,
/// the mapped bytes are immutable and the buffers are sound to read.
pub fn read_batch_chunks<O>(
    owner: Arc<O>,
    chunks: &[ChunkDesc],
    ctrls: &[&ChunkCtrl],
    registry: &SchemaRegistry,
) -> Result<RecordBatch>
where
    O: SegmentBase + Send + Sync + RefUnwindSafe + 'static,
{
    if chunks.is_empty() {
        return Err(Error::Layout("empty chunk list"));
    }
    if chunks.len() != ctrls.len() {
        return Err(Error::Layout("chunk / ctrl length mismatch"));
    }

    // 1. Generation check on every chunk — a recycled chunk is rejected here.
    for (desc, ctrl) in chunks.iter().zip(ctrls.iter()) {
        ctrl.validate(desc)?;
    }

    // Per-chunk base pointers within the (single) mapped segment.
    let base_ptr = owner.base_ptr();
    // SAFETY: each `desc` was minted for this segment; `offset` is in-bounds of
    // the mapping the caller pinned via `owner`.
    let bases: Vec<*const u8> = chunks
        .iter()
        .map(|d| unsafe { base_ptr.add(d.offset as usize) })
        .collect();
    let primary = bases[0];

    // 2/4. Parse + validate the header and the node/buffer tables purely over
    // the primary chunk's bytes. This is the untrusted-input boundary: every
    // table read and every buffer-entry extent is bounds-checked against the
    // real chunk lengths before any pointer is formed, so a corrupt/malicious
    // header (huge `node_count`/`buffer_count`, out-of-range `chunk_index`,
    // overflowing extents) returns `Err` rather than reading out of bounds.
    // SAFETY: `primary` is valid for `chunks[0].len` bytes (the caller pinned
    // the mapping via `owner`); the slice never outlives this call.
    let primary_bytes = unsafe { std::slice::from_raw_parts(primary, chunks[0].len as usize) };
    let chunk_lens: Vec<usize> = chunks.iter().map(|c| c.len as usize).collect();
    let (header, nodes, entries) = read_batch_layout(primary_bytes, &chunk_lens)?;

    // 3. schema_id agreement between descriptor and header.
    if header.schema_id != chunks[0].schema_id {
        return Err(Error::SchemaMismatch {
            desc: chunks[0].schema_id,
            header: header.schema_id,
        });
    }

    // 5. Resolve the schema.
    let schema = registry
        .resolve(header.schema_id)
        .ok_or(Error::UnknownSchema(header.schema_id))?;

    let node_count = header.node_count as usize;
    let buffer_count = header.buffer_count as usize;

    // Build one zero-copy Arrow buffer per table entry. `read_batch_layout`
    // bounds-checked every entry's extent against its chunk; this closure
    // additionally guards the *index* (`idx`), because the number of buffers the
    // schema-driven walk demands is derived from the untrusted per-node
    // `has_validity`/`data_buffers` counts and may exceed `buffer_count`.
    let make_buffer = |idx: usize| -> Result<Buffer> {
        let e = entries.get(idx).ok_or(Error::Layout(
            "schema demands more buffers than the buffer table holds",
        ))?;
        // SAFETY: entry `idx` was bounds-checked in `read_batch_layout` to
        // address `e.len` bytes inside chunk `e.chunk_index` (also range-checked);
        // the region stays mapped and immutable while `owner` is alive, and a
        // clone of `owner` is handed to the buffer as its deallocation guard.
        unsafe {
            let ptr = NonNull::new_unchecked(
                bases[e.chunk_index as usize].add(e.offset as usize) as *mut u8
            );
            let keep_alive: Arc<dyn arrow_buffer::alloc::Allocation> = owner.clone();
            Ok(Buffer::from_custom_allocation(
                ptr,
                e.len as usize,
                keep_alive,
            ))
        }
    };

    // Walk the schema's field DataTypes in lockstep with the flattened node table.
    let mut ni = 0usize;
    let mut bi = 0usize;
    let mut arrays = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let data = build_node(field.data_type(), &nodes, &mut ni, &mut bi, &make_buffer)?;
        arrays.push(make_array(data));
    }
    if ni != node_count || bi != buffer_count {
        return Err(Error::Layout(
            "node/buffer table not fully consumed by schema",
        ));
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Copy a fixed-size POD `T` out of `buf` at byte `off`, unaligned.
///
/// # Safety
///
/// The caller must guarantee `off + size_of::<T>() <= buf.len()` and that every
/// bit pattern of `size_of::<T>()` bytes is a valid `T` (true for the plain
/// all-`u32`/`u64` layout records here — no padding, no niches).
#[inline]
unsafe fn read_pod<T: Copy>(buf: &[u8], off: usize) -> T {
    debug_assert!(off + core::mem::size_of::<T>() <= buf.len());
    let mut v = core::mem::MaybeUninit::<T>::uninit();
    // SAFETY: bounds guaranteed by the caller; source and dest don't overlap.
    unsafe {
        core::ptr::copy_nonoverlapping(
            buf.as_ptr().add(off),
            v.as_mut_ptr().cast::<u8>(),
            core::mem::size_of::<T>(),
        );
        v.assume_init()
    }
}

/// Parse and validate a batch's [`BatchHeader`] and its flattened node + buffer
/// tables **purely over the primary chunk's bytes** — the untrusted-input
/// boundary a corrupt or malicious peer reaches across.
///
/// `primary` is the raw byte content of the primary chunk (chunk 0, which holds
/// the header + tables); `chunk_lens` is the byte length of *every* chunk in the
/// batch (`chunk_lens[0] == primary.len()`), used to bounds-check each buffer
/// entry's extent against the chunk it names.
///
/// Every field derived from the header is untrusted, so this function:
///
/// - rejects a `primary` shorter than the 32-byte header ([`Error::Layout`]);
/// - checks the magic + format ([`Error::BadMagic`]);
/// - checks `chunk_count` equals the supplied chunk list length;
/// - computes the node/buffer table extents with **checked** arithmetic (so a
///   huge `node_count`/`buffer_count` cannot wrap) and rejects any table that
///   overruns `primary`;
/// - bounds-checks every [`BufferEntry`]: `chunk_index` in range and
///   `offset + len` within that chunk.
///
/// On success the returned `nodes`/`entries` are safe to walk and every entry
/// addresses bytes that exist. It never panics and never reads out of bounds for
/// **any** input — see the `fuzz_arrow_layout` target and the `layout_parser_*`
/// property tests.
pub fn read_batch_layout(
    primary: &[u8],
    chunk_lens: &[usize],
) -> Result<(BatchHeader, Vec<NodeEntry>, Vec<BufferEntry>)> {
    use core::mem::size_of;

    // Header must fit before we read a single field from it.
    if primary.len() < size_of::<BatchHeader>() {
        return Err(Error::Layout("primary chunk smaller than batch header"));
    }
    // SAFETY: length checked above; `BatchHeader` is an all-integer POD.
    let header: BatchHeader = unsafe { read_pod(primary, 0) };
    if header.magic != BATCH_MAGIC || header.format != BATCH_FORMAT {
        return Err(Error::BadMagic);
    }
    if header.chunk_count as usize != chunk_lens.len() {
        return Err(Error::Layout("header chunk_count != chunk list length"));
    }

    let node_count = header.node_count as usize;
    let buffer_count = header.buffer_count as usize;

    // Node + buffer tables must fit inside the primary chunk. Checked math keeps
    // an attacker's ~4-billion counts from wrapping the offset computation.
    let node_bytes = node_count
        .checked_mul(size_of::<NodeEntry>())
        .ok_or(Error::Layout("node table size overflow"))?;
    let buffers_off = nodes_offset()
        .checked_add(node_bytes)
        .ok_or(Error::Layout("node table offset overflow"))?;
    debug_assert_eq!(buffers_off, buffers_offset(node_count));
    let buffer_bytes = buffer_count
        .checked_mul(size_of::<BufferEntry>())
        .ok_or(Error::Layout("buffer table size overflow"))?;
    let tables_end = buffers_off
        .checked_add(buffer_bytes)
        .ok_or(Error::Layout("buffer table offset overflow"))?;
    if tables_end > primary.len() {
        return Err(Error::Layout("node/buffer tables overrun primary chunk"));
    }

    let mut nodes = Vec::with_capacity(node_count);
    for i in 0..node_count {
        // SAFETY: `tables_end <= primary.len()` proved the whole node table is in
        // bounds; entry `i` sits at `nodes_offset() + i*24 < buffers_off`.
        nodes.push(unsafe {
            read_pod::<NodeEntry>(primary, nodes_offset() + i * size_of::<NodeEntry>())
        });
    }
    let mut entries = Vec::with_capacity(buffer_count);
    for i in 0..buffer_count {
        // SAFETY: as above; entry `i` sits at `buffers_off + i*12 < tables_end`.
        entries.push(unsafe {
            read_pod::<BufferEntry>(primary, buffers_off + i * size_of::<BufferEntry>())
        });
    }

    // Every buffer entry must address bytes that exist within its named chunk.
    for e in &entries {
        let cidx = e.chunk_index as usize;
        let clen = *chunk_lens
            .get(cidx)
            .ok_or(Error::Layout("buffer chunk_index out of range"))?;
        let end = (e.offset as usize)
            .checked_add(e.len as usize)
            .ok_or(Error::Layout("buffer extent overflow"))?;
        if end > clen {
            return Err(Error::Layout("buffer extends past its chunk"));
        }
    }

    Ok((header, nodes, entries))
}

/// Rebuild one `ArrayData` node (and, recursively, its children) from the flat
/// node table + a buffer factory, advancing the node cursor `ni` and buffer
/// cursor `bi` in pre-order lockstep with the writer.
fn build_node(
    dt: &DataType,
    nodes: &[NodeEntry],
    ni: &mut usize,
    bi: &mut usize,
    make_buffer: &dyn Fn(usize) -> Result<Buffer>,
) -> Result<arrow_data::ArrayData> {
    let node = *nodes.get(*ni).ok_or(Error::Layout("node table underrun"))?;
    *ni += 1;

    let mut builder = ArrayDataBuilder::new(dt.clone())
        .len(node.len as usize)
        .null_count(node.null_count as usize);

    if node.has_validity != 0 {
        builder = builder.null_bit_buffer(Some(make_buffer(*bi)?));
        *bi += 1;
    }
    for _ in 0..node.data_buffers {
        builder = builder.add_buffer(make_buffer(*bi)?);
        *bi += 1;
    }

    let child_types = child_data_types(dt);
    if child_types.len() != node.child_count as usize {
        return Err(Error::Layout("schema child count != stored child count"));
    }
    for child_dt in &child_types {
        let child = build_node(child_dt, nodes, ni, bi, make_buffer)?;
        builder = builder.add_child_data(child);
    }

    builder.build().map_err(Error::from)
}

/// The child `DataType`s of a nested type, in field order (empty for a flat type).
fn child_data_types(dt: &DataType) -> Vec<DataType> {
    match dt {
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            vec![f.data_type().clone()]
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{buffers_offset, nodes_offset, BATCH_FORMAT, BATCH_MAGIC, BUFFER_ALIGN};

    /// Tiny deterministic xorshift PRNG — a committed, seed-stable stand-in for a
    /// fuzzer so the property test is reproducible in CI on stable.
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

    /// Lay a well-formed header + node/buffer tables into a primary buffer of
    /// `chunk_len` bytes. Used as a "valid" seed the property test then mutates.
    fn valid_primary(node_count: u32, buffer_count: u32, chunk_len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; chunk_len];
        let hdr = BatchHeader {
            magic: BATCH_MAGIC,
            format: BATCH_FORMAT,
            schema_id: 1,
            row_count: 3,
            node_count,
            buffer_count,
            chunk_count: 1,
        };
        // Serialize the header by field (mirrors the writer's byte order).
        buf[0..8].copy_from_slice(&hdr.magic.to_le_bytes());
        buf[8..12].copy_from_slice(&hdr.format.to_le_bytes());
        buf[12..16].copy_from_slice(&hdr.schema_id.to_le_bytes());
        buf[16..20].copy_from_slice(&hdr.row_count.to_le_bytes());
        buf[20..24].copy_from_slice(&hdr.node_count.to_le_bytes());
        buf[24..28].copy_from_slice(&hdr.buffer_count.to_le_bytes());
        buf[28..32].copy_from_slice(&hdr.chunk_count.to_le_bytes());
        // Buffer entries all point at a valid in-chunk region [align, align+4).
        let boff = buffers_offset(node_count as usize);
        let payload = crate::layout::payload_start(node_count as usize, buffer_count as usize);
        for i in 0..buffer_count as usize {
            let at = boff + i * 12;
            if at + 12 <= buf.len() {
                buf[at..at + 4].copy_from_slice(&0u32.to_le_bytes()); // chunk_index
                buf[at + 4..at + 8].copy_from_slice(&(payload as u32).to_le_bytes()); // offset
                buf[at + 8..at + 12].copy_from_slice(&4u32.to_le_bytes()); // len
            }
        }
        let _ = nodes_offset();
        let _ = BUFFER_ALIGN;
        buf
    }

    #[test]
    fn layout_parser_rejects_oversized_tables_no_oob() {
        // The historical bug: a header claiming a huge node/buffer count made the
        // reader walk the node/buffer tables out of bounds of the primary chunk.
        // The parser must now reject it cleanly, never read OOB, never panic.
        let mut buf = valid_primary(0, 0, 64);
        // node_count = u32::MAX
        buf[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
        let err = read_batch_layout(&buf, &[buf.len()]);
        assert!(matches!(err, Err(Error::Layout(_))), "got {err:?}");

        // buffer_count = u32::MAX
        let mut buf = valid_primary(0, 0, 64);
        buf[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            read_batch_layout(&buf, &[buf.len()]),
            Err(Error::Layout(_))
        ));
    }

    #[test]
    fn layout_parser_rejects_short_and_bad_magic() {
        // Shorter than the 32-byte header.
        for n in 0..32usize {
            assert!(matches!(
                read_batch_layout(&vec![0u8; n], &[n]),
                Err(Error::Layout(_))
            ));
        }
        // Header-sized but zero magic.
        assert!(matches!(
            read_batch_layout(&[0u8; 64], &[64]),
            Err(Error::BadMagic)
        ));
    }

    #[test]
    fn layout_parser_rejects_out_of_range_buffer_entry() {
        // One buffer entry naming chunk_index 9 (only one chunk exists).
        let mut buf = valid_primary(0, 1, 128);
        let at = buffers_offset(0);
        buf[at..at + 4].copy_from_slice(&9u32.to_le_bytes());
        assert!(matches!(
            read_batch_layout(&buf, &[buf.len()]),
            Err(Error::Layout(_))
        ));

        // Entry extent past the end of its chunk.
        let mut buf = valid_primary(0, 1, 128);
        let at = buffers_offset(0);
        buf[at + 4..at + 8].copy_from_slice(&120u32.to_le_bytes()); // offset
        buf[at + 8..at + 12].copy_from_slice(&100u32.to_le_bytes()); // len -> 220 > 128
        assert!(matches!(
            read_batch_layout(&buf, &[buf.len()]),
            Err(Error::Layout(_))
        ));
    }

    /// Property-fuzz: many deterministic iterations of random + mutated-valid
    /// bytes must never panic and never read out of bounds (a clean `Ok`/`Err` is
    /// the only acceptable outcome). Seed-stable so a failure is reproducible.
    #[test]
    fn layout_parser_never_panics_on_arbitrary_bytes() {
        let mut rng = Rng(0x5eed_1234_abcd_0001);
        // miri interprets ~100x slower; 2k iterations still cover every branch.
        let iters = if cfg!(miri) { 2_000 } else { 200_000 };
        for _ in 0..iters {
            let mode = rng.below(3);
            let (bytes, lens): (Vec<u8>, Vec<usize>) = match mode {
                // Pure random bytes of a small random length.
                0 => {
                    let len = rng.below(96);
                    let b: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
                    (b, vec![len])
                }
                // A valid-ish frame with random small counts, then bit-mutated.
                _ => {
                    let nc = rng.below(6) as u32;
                    let bc = rng.below(6) as u32;
                    let len = 32 + rng.below(160);
                    let mut b = valid_primary(nc, bc, len);
                    // Flip a handful of random bytes.
                    for _ in 0..rng.below(8) {
                        if !b.is_empty() {
                            let i = rng.below(b.len());
                            b[i] ^= rng.byte();
                        }
                    }
                    // Sometimes fuzz the reported chunk count / lengths too.
                    let lens = if rng.below(2) == 0 {
                        vec![b.len()]
                    } else {
                        vec![b.len(), rng.below(64)]
                    };
                    (b, lens)
                }
            };
            // The contract under test: this returns, it does not panic/UB.
            let _ = read_batch_layout(&bytes, &lens);
        }
    }
}
