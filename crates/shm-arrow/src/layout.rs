//! The on-chunk Arrow batch layout ABI.
//!
//! A written batch occupies **one** loaned chunk with this byte layout, all
//! measured from the chunk's start (i.e. `segment_base + ChunkDesc::offset`):
//!
//! ```text
//! +------------------------------------------------------------------+
//! | BatchHeader        (24 B, at chunk offset 0)                      |
//! | ColumnEntry * column_count   (16 B each)                         |
//! | BufferEntry * buffer_count   (8 B each)                          |
//! | <pad to 64>                                                       |
//! | buffer 0 bytes     (64-byte aligned)                             |
//! | <pad to 64>                                                       |
//! | buffer 1 bytes     (64-byte aligned)                             |
//! | ...                                                               |
//! +------------------------------------------------------------------+
//! ```
//!
//! The **buffer table** is a flat list. Buffers appear in column order; within
//! a column, the validity buffer (if [`ColumnEntry::has_validity`] is `1`)
//! comes first, followed by [`ColumnEntry::data_buffers`] data buffers (values,
//! and for variable-length types the offsets buffer). Every payload buffer is
//! padded so it begins on a 64-byte boundary, satisfying Arrow's buffer
//! alignment expectation for the zero-copy read path.
//!
//! These records are plain `#[repr(C)]` PODs placed into shared memory by hand
//! (raw `ptr::write`/`ptr::read`); they carry no atomics and are never mutated
//! after the chunk is published, so — unlike [`shm_core::ChunkCtrl`] — no
//! `SharedPod`/`bytemuck` machinery is required here.

/// Arrow batch magic: little-endian bytes of `b"SHMARBT1"`.
pub const BATCH_MAGIC: u64 = u64::from_le_bytes(*b"SHMARBT1");

/// Arrow's required buffer alignment; every payload buffer starts here.
pub const BUFFER_ALIGN: usize = 64;

/// Fixed header at the start of a batch chunk (24 bytes).
///
/// # Layout (frozen ABI — 24 bytes)
///
/// | field          | type  | meaning                                       |
/// |----------------|-------|-----------------------------------------------|
/// | `magic`        | `u64` | must equal [`BATCH_MAGIC`]                     |
/// | `schema_id`    | `u32` | interned schema id (mirrors `ChunkDesc`)      |
/// | `row_count`    | `u32` | number of rows in the batch                   |
/// | `column_count` | `u32` | number of columns (== schema fields)          |
/// | `buffer_count` | `u32` | total entries in the buffer table             |
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchHeader {
    /// Must equal [`BATCH_MAGIC`].
    pub magic: u64,
    /// Interned schema id; mirrors [`shm_core::ChunkDesc::schema_id`].
    pub schema_id: u32,
    /// Number of rows in the batch.
    pub row_count: u32,
    /// Number of columns (equals the schema's field count).
    pub column_count: u32,
    /// Total number of entries in the buffer table.
    pub buffer_count: u32,
}

const _: () = assert!(core::mem::size_of::<BatchHeader>() == 24);
const _: () = assert!(core::mem::align_of::<BatchHeader>() == 8);

/// Per-column record describing how to rebuild one column's `ArrayData` (16 B).
///
/// # Layout (frozen ABI — 16 bytes)
///
/// | field           | type  | meaning                                      |
/// |-----------------|-------|----------------------------------------------|
/// | `len`           | `u32` | logical array length (rows)                  |
/// | `null_count`    | `u32` | number of nulls                              |
/// | `has_validity`  | `u32` | `1` if a validity buffer precedes the data   |
/// | `data_buffers`  | `u32` | count of data buffers (excludes validity)    |
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnEntry {
    /// Logical array length (row count of this column).
    pub len: u32,
    /// Number of nulls in the column.
    pub null_count: u32,
    /// `1` if a validity (null) bitmap buffer precedes this column's data
    /// buffers in the buffer table, `0` otherwise.
    pub has_validity: u32,
    /// Number of data buffers for this column, excluding the validity buffer
    /// (e.g. `1` for a primitive values buffer, `2` for `Utf8`/`Binary`
    /// offsets + values).
    pub data_buffers: u32,
}

const _: () = assert!(core::mem::size_of::<ColumnEntry>() == 16);

/// One entry in the buffer table: a `(offset, len)` pair, chunk-relative (8 B).
///
/// `offset` is measured from the chunk start; the absolute address is
/// `segment_base + ChunkDesc::offset + BufferEntry::offset`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferEntry {
    /// Byte offset of the buffer from the chunk start (64-byte aligned).
    pub offset: u32,
    /// Byte length of the buffer.
    pub len: u32,
}

const _: () = assert!(core::mem::size_of::<BufferEntry>() == 8);

/// Round `x` up to a multiple of `a` (a power of two).
#[inline]
pub(crate) const fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

/// Byte offset of the column-entry table within a batch chunk.
#[inline]
pub(crate) const fn columns_offset() -> usize {
    core::mem::size_of::<BatchHeader>()
}

/// Byte offset of the buffer table within a batch chunk.
#[inline]
pub(crate) const fn buffers_offset(column_count: usize) -> usize {
    columns_offset() + column_count * core::mem::size_of::<ColumnEntry>()
}

/// Byte offset where the first padded payload buffer begins.
#[inline]
pub(crate) const fn payload_start(column_count: usize, buffer_count: usize) -> usize {
    let after_tables = buffers_offset(column_count) + buffer_count * core::mem::size_of::<BufferEntry>();
    align_up(after_tables, BUFFER_ALIGN)
}
