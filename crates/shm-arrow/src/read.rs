//! The zero-copy read path: reconstruct a `RecordBatch` over the mapped chunk.

use std::panic::RefUnwindSafe;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::{make_array, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayDataBuilder;

use crate::error::{Error, Result};
use crate::layout::{buffers_offset, columns_offset, BatchHeader, BufferEntry, ColumnEntry, BATCH_MAGIC};
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

/// Reconstruct a `RecordBatch` that points **directly** at the mapped chunk —
/// no payload is copied.
///
/// Each Arrow [`Buffer`] is built with
/// [`Buffer::from_custom_allocation`](arrow_buffer::Buffer::from_custom_allocation)
/// over a pointer into the segment, taking a clone of `owner` as its keep-alive.
/// While any returned buffer lives, `owner` (and therefore the mapping) lives.
///
/// # Arguments
///
/// - `owner` — the pin guard kept alive by every buffer; a clone is moved into
///   each. It must keep its [`SegmentBase::base_ptr`] valid for as long as the
///   batch (or any buffer sliced from it) is reachable. The chunk lives at
///   `owner.base_ptr() + desc.offset`.
/// - `ctrl` — the chunk's live control word, used to reject a recycled chunk.
/// - `desc` — the descriptor naming the chunk.
/// - `registry` — resolves `desc.schema_id` to the Arrow schema; must have been
///   seeded identically to the writer's (v0.1 in-process contract).
///
/// # Validation order
///
/// 1. `ctrl.validate(desc)` — generation match, else
///    [`shm_core::Error::StaleDescriptor`].
/// 2. [`BatchHeader`] magic, else [`Error::BadMagic`].
/// 3. header `schema_id` vs `desc.schema_id`, else [`Error::SchemaMismatch`].
/// 4. registry lookup, else [`Error::UnknownSchema`].
///
/// # Safety-relevant contract
///
/// The caller must hold the chunk in `PUBLISHED` state with a shared pin for the
/// lifetime of `owner` (the borrow discipline of [`ChunkCtrl`]). Given that,
/// the mapped bytes are immutable and the buffers are sound to read.
pub fn read_batch<O>(
    owner: Arc<O>,
    ctrl: &ChunkCtrl,
    desc: &ChunkDesc,
    registry: &SchemaRegistry,
) -> Result<RecordBatch>
where
    O: SegmentBase + Send + Sync + RefUnwindSafe + 'static,
{
    // 1. Generation check — a recycled chunk is rejected here.
    ctrl.validate(desc)?;

    // The chunk starts `desc.offset` into the segment.
    let base_ptr = owner.base_ptr();
    // SAFETY: `desc` was minted for this segment; `offset` is in-bounds of the
    // mapping the caller pinned via `owner`.
    let chunk = unsafe { base_ptr.add(desc.offset as usize) };

    // 2. Header + magic.
    // SAFETY: a published batch chunk begins with a `BatchHeader`; `chunk` is
    // valid for `desc.len` bytes (>= header) and the read is unaligned-safe.
    let header = unsafe { chunk.cast::<BatchHeader>().read_unaligned() };
    if header.magic != BATCH_MAGIC {
        return Err(Error::BadMagic);
    }

    // 3. schema_id agreement between descriptor and header.
    if header.schema_id != desc.schema_id {
        return Err(Error::SchemaMismatch {
            desc: desc.schema_id,
            header: header.schema_id,
        });
    }

    // 4. Resolve the schema.
    let schema = registry
        .resolve(header.schema_id)
        .ok_or(Error::UnknownSchema(header.schema_id))?;

    let column_count = header.column_count as usize;
    let buffer_count = header.buffer_count as usize;

    // Read the column + buffer tables out of the chunk (small copies of the
    // 16-/8-byte descriptor records — never the payload).
    let mut column_entries = Vec::with_capacity(column_count);
    // SAFETY: the tables sit at fixed offsets after the header, all within the
    // serialized region the writer laid out inside this chunk.
    unsafe {
        let col_ptr = chunk.add(columns_offset()).cast::<ColumnEntry>();
        for i in 0..column_count {
            column_entries.push(col_ptr.add(i).read_unaligned());
        }
    }
    let mut buffer_entries = Vec::with_capacity(buffer_count);
    // SAFETY: as above; the buffer table follows the column table.
    unsafe {
        let buf_ptr = chunk.add(buffers_offset(column_count)).cast::<BufferEntry>();
        for i in 0..buffer_count {
            buffer_entries.push(buf_ptr.add(i).read_unaligned());
        }
    }

    if column_count != schema.fields().len() {
        return Err(Error::Arrow(arrow_schema::ArrowError::InvalidArgumentError(
            format!(
                "column count {column_count} != schema field count {}",
                schema.fields().len()
            ),
        )));
    }

    // Build one zero-copy Arrow buffer per table entry, walking the flat list in
    // the same order the writer emitted it.
    let make_buffer = |entry: &BufferEntry| -> Buffer {
        // SAFETY: `entry.offset` addresses a 64-byte-aligned region of
        // `entry.len` bytes inside this chunk (writer invariant); the region
        // stays mapped and immutable while `owner` is alive, and a clone of
        // `owner` is handed to the buffer as its deallocation guard.
        unsafe {
            let ptr = NonNull::new_unchecked(chunk.add(entry.offset as usize) as *mut u8);
            let keep_alive: Arc<dyn arrow_buffer::alloc::Allocation> = owner.clone();
            Buffer::from_custom_allocation(ptr, entry.len as usize, keep_alive)
        }
    };

    let mut arrays = Vec::with_capacity(column_count);
    let mut next = 0usize; // cursor into the flat buffer table
    for (col, field) in column_entries.iter().zip(schema.fields().iter()) {
        let mut builder = ArrayDataBuilder::new(field.data_type().clone()).len(col.len as usize);

        if col.has_validity != 0 {
            let entry = buffer_entries
                .get(next)
                .ok_or(Error::BadMagic)
                .copied()?;
            next += 1;
            builder = builder
                .null_bit_buffer(Some(make_buffer(&entry)))
                .null_count(col.null_count as usize);
        }

        for _ in 0..col.data_buffers {
            let entry = buffer_entries
                .get(next)
                .ok_or(Error::BadMagic)
                .copied()?;
            next += 1;
            builder = builder.add_buffer(make_buffer(&entry));
        }

        let data = builder.build()?;
        arrays.push(make_array(data));
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}
