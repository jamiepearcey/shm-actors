//! The on-chunk Arrow batch layout ABI (v0.3 / ADR-0003 item F — `SHMARBT2`).
//!
//! A written batch occupies **one or more** loaned chunks. The **first** chunk
//! (the *primary*) begins with a [`BatchHeader`] followed by two flat tables:
//!
//! ```text
//! primary chunk (chunk_index 0):
//! +------------------------------------------------------------------+
//! | BatchHeader        (32 B, at chunk offset 0)                      |
//! | NodeEntry * node_count       (24 B each)                          |
//! | BufferEntry * buffer_count   (12 B each)                          |
//! | <pad to 64>                                                       |
//! | buffer bytes (64-byte aligned, packed until the chunk fills)      |
//! +------------------------------------------------------------------+
//! continuation chunks (chunk_index 1..):
//! +------------------------------------------------------------------+
//! | buffer bytes (64-byte aligned from chunk offset 0)               |
//! +------------------------------------------------------------------+
//! ```
//!
//! # Schema-driven, recursive (arrow-IPC-style) encoding
//!
//! The writer performs a **pre-order** depth-first walk of each column's
//! [`ArrayData`](arrow_data::ArrayData) tree, emitting one [`NodeEntry`] per node
//! and one [`BufferEntry`] per physical buffer, recursing into `child_data`. The
//! reader walks the **same** schema `DataType` tree in lockstep, consuming
//! `NodeEntry`s and `BufferEntry`s and rebuilding `ArrayData` (with `child_data`)
//! zero-copy. This mirrors Arrow IPC's RecordBatch encoding: the tree shape comes
//! from the schema, so only per-node runtime facts (`len`, `null_count`, whether
//! a validity buffer is present) and the physical buffers are stored.
//!
//! # Multi-chunk packing
//!
//! Each [`BufferEntry`] names `{chunk_index, offset, len}`. Buffers are packed
//! 64-byte-aligned into the primary chunk (after the header + tables) until the
//! next buffer would overflow the chunk, at which point a new continuation chunk
//! is opened. **Every individual buffer is contiguous within a single chunk** —
//! a buffer never straddles a chunk boundary (that would break zero-copy). A
//! single buffer larger than the pool's max chunk size is rejected with
//! [`Error::Unsupported`](crate::Error::Unsupported).
//!
//! # Slicing
//!
//! Sliced input (`ArrayData::offset() != 0`, at any nesting depth) is
//! **normalized on write**: the array is compacted (via `arrow-select`'s
//! `concat`) so the in-chunk representation has offset `0`, and the read side
//! never sees a non-zero offset.
//!
//! These records are plain `#[repr(C)]` PODs placed into shared memory by hand
//! (raw `ptr::write`/`ptr::read`); they carry no atomics and are never mutated
//! after the chunk is published, so no `SharedPod`/`bytemuck` machinery is needed.

/// Arrow batch magic: little-endian bytes of `b"SHMARBT2"`.
///
/// Bumped from `SHMARBT1` for the v0.3 layout (ADR-0003 item F): recursive
/// nested nodes, per-buffer chunk indices, and a multi-chunk batch. A v0.2
/// (`SHMARBT1`) chunk is rejected by the magic check.
pub const BATCH_MAGIC: u64 = u64::from_le_bytes(*b"SHMARBT2");

/// The batch layout format version stamped in [`BatchHeader::format`].
pub const BATCH_FORMAT: u32 = 2;

/// Arrow's required buffer alignment; every payload buffer starts here.
pub const BUFFER_ALIGN: usize = 64;

/// Fixed header at the start of a batch's **primary** chunk (32 bytes).
///
/// # Layout (versioned ABI — 32 bytes, v0.3 `SHMARBT2`)
///
/// | field          | type  | meaning                                          |
/// |----------------|-------|--------------------------------------------------|
/// | `magic`        | `u64` | must equal [`BATCH_MAGIC`]                        |
/// | `format`       | `u32` | layout version (equals [`BATCH_FORMAT`])          |
/// | `schema_id`    | `u32` | interned schema id (mirrors `ChunkDesc`)          |
/// | `row_count`    | `u32` | number of rows in the batch                       |
/// | `node_count`   | `u32` | flattened pre-order [`NodeEntry`] count           |
/// | `buffer_count` | `u32` | flattened [`BufferEntry`] count                   |
/// | `chunk_count`  | `u32` | chunks this batch spans (primary + continuations) |
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchHeader {
    /// Must equal [`BATCH_MAGIC`].
    pub magic: u64,
    /// Layout format version; equals [`BATCH_FORMAT`].
    pub format: u32,
    /// Interned schema id; mirrors [`shm_core::ChunkDesc::schema_id`].
    pub schema_id: u32,
    /// Number of rows in the batch.
    pub row_count: u32,
    /// Number of [`NodeEntry`]s in the flattened pre-order node table.
    pub node_count: u32,
    /// Number of [`BufferEntry`]s in the flattened buffer table.
    pub buffer_count: u32,
    /// Number of chunks this batch spans (`>= 1`; primary + continuations).
    pub chunk_count: u32,
}

const _: () = assert!(core::mem::size_of::<BatchHeader>() == 32);
const _: () = assert!(core::mem::align_of::<BatchHeader>() == 8);

/// One node in the flattened pre-order array tree (24 bytes).
///
/// The node's [`DataType`](arrow_schema::DataType) is *not* stored — it is read
/// from the schema during the lockstep walk. Only per-node runtime facts and the
/// counts needed to consume the flat tables are recorded.
///
/// # Layout (versioned ABI — 24 bytes)
///
/// | field          | type  | meaning                                          |
/// |----------------|-------|--------------------------------------------------|
/// | `len`          | `u32` | logical array length of this node                 |
/// | `null_count`   | `u32` | number of nulls                                   |
/// | `has_validity` | `u32` | `1` if a validity buffer precedes the data buffers|
/// | `data_buffers` | `u32` | count of non-validity buffers for this node       |
/// | `child_count`  | `u32` | count of child nodes (next in pre-order)          |
/// | `_pad`         | `u32` | reserved, zero (keeps the array 8-aligned)        |
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeEntry {
    /// Logical array length (row count) of this node.
    pub len: u32,
    /// Number of nulls in this node.
    pub null_count: u32,
    /// `1` if a validity (null) bitmap buffer precedes this node's data buffers.
    pub has_validity: u32,
    /// Number of data (non-validity) buffers for this node — e.g. `1` for a
    /// primitive values buffer, `2` for `Utf8` offsets + values, `1` for a
    /// `List` offsets buffer, `0` for a `Struct`.
    pub data_buffers: u32,
    /// Number of child nodes (the immediately following pre-order entries) —
    /// e.g. the field count for a `Struct`, `1` for a `List`.
    pub child_count: u32,
    /// Reserved padding, kept zero to keep the node array 8-aligned.
    pub _pad: u32,
}

const _: () = assert!(core::mem::size_of::<NodeEntry>() == 24);
const _: () = assert!(core::mem::align_of::<NodeEntry>() == 4);

/// One entry in the buffer table: which chunk a buffer lives in, and where (12 B).
///
/// The absolute address of the buffer is
/// `segment_base + chunks[chunk_index].offset + offset`, valid for `len` bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferEntry {
    /// Index (into the batch's ordered chunk list) of the chunk holding this
    /// buffer. `0` is the primary chunk; `>= 1` a continuation chunk.
    pub chunk_index: u32,
    /// Byte offset of the buffer from the start of its chunk (64-byte aligned).
    pub offset: u32,
    /// Byte length of the buffer.
    pub len: u32,
}

const _: () = assert!(core::mem::size_of::<BufferEntry>() == 12);
const _: () = assert!(core::mem::align_of::<BufferEntry>() == 4);

/// Round `x` up to a multiple of `a` (a power of two).
#[inline]
pub(crate) const fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

/// Byte offset of the node table within the primary chunk.
#[inline]
pub(crate) const fn nodes_offset() -> usize {
    core::mem::size_of::<BatchHeader>()
}

/// Byte offset of the buffer table within the primary chunk.
#[inline]
pub(crate) const fn buffers_offset(node_count: usize) -> usize {
    nodes_offset() + node_count * core::mem::size_of::<NodeEntry>()
}

/// Byte offset where the first padded payload buffer begins in the primary chunk.
#[inline]
pub(crate) const fn payload_start(node_count: usize, buffer_count: usize) -> usize {
    let after_tables =
        buffers_offset(node_count) + buffer_count * core::mem::size_of::<BufferEntry>();
    align_up(after_tables, BUFFER_ALIGN)
}
