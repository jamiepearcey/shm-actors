//! The write path: serialize a `RecordBatch`'s buffers into one loaned chunk.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;

use crate::alloc::ChunkAllocator;
use crate::error::{Error, Result};
use crate::layout::{
    align_up, buffers_offset, columns_offset, payload_start, BatchHeader, BufferEntry, ColumnEntry,
    BATCH_MAGIC, BUFFER_ALIGN,
};
use crate::schema::SchemaRegistry;
use shm_core::ChunkDesc;

/// Per-column plan gathered from the source batch.
///
/// Buffers are held as owned [`Buffer`] handles (an `Arc` bump over the source
/// allocation — **no** payload copy); the bytes are copied into shared memory
/// exactly once, later, in the write step.
struct PlannedColumn {
    len: u32,
    null_count: u32,
    validity: Option<Buffer>,
    data: Vec<Buffer>,
}

/// Serialize `batch` into a single chunk obtained from `alloc`, returning the
/// descriptor (with `schema_id` set) to be published by the caller.
///
/// The batch is written **once**; everything downstream moves the returned
/// 24-byte [`ChunkDesc`]. See [`crate::layout`] for the exact byte layout.
///
/// # v0.1 limitations
///
/// - **Single chunk.** The whole batch must fit in one allocated chunk;
///   otherwise [`Error::ChunkTooSmall`] is returned. Multi-chunk batches (a
///   descriptor list) are a documented v0.2 extension.
/// - **Unsliced input.** Columns (and their null buffers) must have offset `0`.
///   A sliced array returns [`Error::Unsupported`]; callers should
///   materialize with `arrow`'s compute kernels first.
/// - **Flat columns.** Primitive and variable-length (`Utf8`/`Binary`) columns
///   are supported; nested types (`List`/`Struct`) that carry child data are a
///   v0.2 extension and also surface as [`Error::Unsupported`].
pub fn write_batch<A: ChunkAllocator>(
    alloc: &A,
    registry: &SchemaRegistry,
    batch: &RecordBatch,
) -> Result<ChunkDesc> {
    let schema_id = registry.intern(&batch.schema());
    let columns = plan_columns(batch)?;

    // ---- Layout: assign each buffer a 64-byte-aligned chunk offset. ----
    let column_count = columns.len();
    let buffer_count: usize = columns
        .iter()
        .map(|c| usize::from(c.validity.is_some()) + c.data.len())
        .sum();

    let mut cursor = payload_start(column_count, buffer_count);
    let mut buffer_entries: Vec<BufferEntry> = Vec::with_capacity(buffer_count);
    let mut ordered_buffers: Vec<&Buffer> = Vec::with_capacity(buffer_count);
    let mut column_entries: Vec<ColumnEntry> = Vec::with_capacity(column_count);

    for c in &columns {
        if let Some(v) = &c.validity {
            cursor = align_up(cursor, BUFFER_ALIGN);
            buffer_entries.push(BufferEntry { offset: cursor as u32, len: v.len() as u32 });
            ordered_buffers.push(v);
            cursor += v.len();
        }
        for d in &c.data {
            cursor = align_up(cursor, BUFFER_ALIGN);
            buffer_entries.push(BufferEntry { offset: cursor as u32, len: d.len() as u32 });
            ordered_buffers.push(d);
            cursor += d.len();
        }
        column_entries.push(ColumnEntry {
            len: c.len,
            null_count: c.null_count,
            has_validity: u32::from(c.validity.is_some()),
            data_buffers: c.data.len() as u32,
        });
    }

    let total_bytes = cursor;

    // ---- Allocate and write. ----
    let mut desc = alloc.alloc(total_bytes)?;
    if (desc.len as usize) < total_bytes {
        // Allocator returned a smaller chunk than requested — treat as a fit
        // failure rather than corrupting memory.
        return Err(Error::ChunkTooSmall { need: total_bytes, have: desc.len as usize });
    }

    let base = alloc.resolve(&desc);
    let header = BatchHeader {
        magic: BATCH_MAGIC,
        schema_id,
        row_count: u32::try_from(batch.num_rows())
            .map_err(|_| Error::Unsupported("row count exceeds u32"))?,
        column_count: column_count as u32,
        buffer_count: buffer_count as u32,
    };

    // SAFETY: `base` points at a loaned chunk of `desc.len >= total_bytes`
    // exclusively owned by the caller (LOANED state). Every offset written
    // below is `< total_bytes`, so all writes stay within the chunk. The
    // records are `#[repr(C)]` PODs written unaligned to be layout-agnostic.
    unsafe {
        base.cast::<BatchHeader>().write_unaligned(header);

        let col_ptr = base.add(columns_offset()).cast::<ColumnEntry>();
        for (i, entry) in column_entries.iter().enumerate() {
            col_ptr.add(i).write_unaligned(*entry);
        }

        let buf_ptr = base.add(buffers_offset(column_count)).cast::<BufferEntry>();
        for (i, entry) in buffer_entries.iter().enumerate() {
            buf_ptr.add(i).write_unaligned(*entry);
        }

        for (entry, buffer) in buffer_entries.iter().zip(ordered_buffers.iter()) {
            let bytes = buffer.as_slice();
            let dst = base.add(entry.offset as usize);
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        }
    }

    desc.schema_id = schema_id;
    Ok(desc)
}

/// Gather each column's owned buffers, rejecting sliced/nested input.
fn plan_columns(batch: &RecordBatch) -> Result<Vec<PlannedColumn>> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for array in batch.columns() {
        let data = array.to_data();
        if data.offset() != 0 {
            return Err(Error::Unsupported("sliced column (array offset != 0)"));
        }
        if !data.child_data().is_empty() {
            return Err(Error::Unsupported("nested column with child data"));
        }

        let validity = match data.nulls() {
            Some(nulls) => {
                if nulls.offset() != 0 {
                    return Err(Error::Unsupported("sliced validity buffer"));
                }
                Some(nulls.buffer().clone())
            }
            None => None,
        };

        columns.push(PlannedColumn {
            len: u32::try_from(data.len())
                .map_err(|_| Error::Unsupported("column length exceeds u32"))?,
            null_count: u32::try_from(data.null_count())
                .map_err(|_| Error::Unsupported("null count exceeds u32"))?,
            validity,
            data: data.buffers().to_vec(),
        });
    }
    Ok(columns)
}

/// The serialized byte length of `batch` under this crate's on-chunk layout —
/// enough to size an arena without a dry-run write. Returns `None` on an
/// unsupported (sliced/nested) input.
pub fn serialized_len(batch: &RecordBatch) -> Option<usize> {
    let columns = plan_columns(batch).ok()?;
    let buffer_count: usize = columns
        .iter()
        .map(|c| usize::from(c.validity.is_some()) + c.data.len())
        .sum();
    let mut cursor = payload_start(columns.len(), buffer_count);
    for c in &columns {
        if let Some(v) = &c.validity {
            cursor = align_up(cursor, BUFFER_ALIGN) + v.len();
        }
        for d in &c.data {
            cursor = align_up(cursor, BUFFER_ALIGN) + d.len();
        }
    }
    Some(cursor)
}
