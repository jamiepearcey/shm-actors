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

    // 2. Header + magic + format.
    // SAFETY: a published batch's primary chunk begins with a `BatchHeader`;
    // `primary` is valid for `chunks[0].len` bytes (>= header) and the read is
    // unaligned-safe.
    let header = unsafe { primary.cast::<BatchHeader>().read_unaligned() };
    if header.magic != BATCH_MAGIC || header.format != BATCH_FORMAT {
        return Err(Error::BadMagic);
    }

    // 3. schema_id agreement between descriptor and header.
    if header.schema_id != chunks[0].schema_id {
        return Err(Error::SchemaMismatch {
            desc: chunks[0].schema_id,
            header: header.schema_id,
        });
    }

    // 4a. chunk-count agreement.
    if header.chunk_count as usize != chunks.len() {
        return Err(Error::Layout("header chunk_count != chunk list length"));
    }

    // 5. Resolve the schema.
    let schema = registry
        .resolve(header.schema_id)
        .ok_or(Error::UnknownSchema(header.schema_id))?;

    let node_count = header.node_count as usize;
    let buffer_count = header.buffer_count as usize;

    // Read the node + buffer tables out of the primary chunk (small copies of
    // the fixed-size descriptor records — never the payload).
    let mut nodes = Vec::with_capacity(node_count);
    // SAFETY: the node table sits at `nodes_offset()` after the header, within
    // the serialized region the writer laid out in the primary chunk.
    unsafe {
        let node_ptr = primary.add(nodes_offset()).cast::<NodeEntry>();
        for i in 0..node_count {
            nodes.push(node_ptr.add(i).read_unaligned());
        }
    }
    let mut entries = Vec::with_capacity(buffer_count);
    // SAFETY: as above; the buffer table follows the node table.
    unsafe {
        let buf_ptr = primary.add(buffers_offset(node_count)).cast::<BufferEntry>();
        for i in 0..buffer_count {
            entries.push(buf_ptr.add(i).read_unaligned());
        }
    }

    // 4b. Bounds-check every buffer entry against its chunk so the buffer builder
    // below is infallible and never points outside the mapping.
    for e in &entries {
        let cidx = e.chunk_index as usize;
        if cidx >= chunks.len() {
            return Err(Error::Layout("buffer chunk_index out of range"));
        }
        let end = e.offset as usize + e.len as usize;
        if end > chunks[cidx].len as usize {
            return Err(Error::Layout("buffer extends past its chunk"));
        }
    }

    // Build one zero-copy Arrow buffer per table entry.
    let make_buffer = |idx: usize| -> Buffer {
        let e = &entries[idx];
        // SAFETY: entry `idx` was bounds-checked above to address `e.len` bytes
        // inside chunk `e.chunk_index`; the region stays mapped and immutable
        // while `owner` is alive, and a clone of `owner` is handed to the buffer
        // as its deallocation guard.
        unsafe {
            let ptr =
                NonNull::new_unchecked(bases[e.chunk_index as usize].add(e.offset as usize) as *mut u8);
            let keep_alive: Arc<dyn arrow_buffer::alloc::Allocation> = owner.clone();
            Buffer::from_custom_allocation(ptr, e.len as usize, keep_alive)
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
        return Err(Error::Layout("node/buffer table not fully consumed by schema"));
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Rebuild one `ArrayData` node (and, recursively, its children) from the flat
/// node table + a buffer factory, advancing the node cursor `ni` and buffer
/// cursor `bi` in pre-order lockstep with the writer.
fn build_node(
    dt: &DataType,
    nodes: &[NodeEntry],
    ni: &mut usize,
    bi: &mut usize,
    make_buffer: &dyn Fn(usize) -> Buffer,
) -> Result<arrow_data::ArrayData> {
    let node = *nodes.get(*ni).ok_or(Error::Layout("node table underrun"))?;
    *ni += 1;

    let mut builder = ArrayDataBuilder::new(dt.clone())
        .len(node.len as usize)
        .null_count(node.null_count as usize);

    if node.has_validity != 0 {
        builder = builder.null_bit_buffer(Some(make_buffer(*bi)));
        *bi += 1;
    }
    for _ in 0..node.data_buffers {
        builder = builder.add_buffer(make_buffer(*bi));
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
