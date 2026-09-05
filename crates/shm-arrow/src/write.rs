//! The write path: serialize a `RecordBatch`'s buffers into one or more chunks.
//!
//! The batch is copied **once** into shared memory ("one copy in, zero after").
//! Nested (`Struct`/`List`/…) columns are serialized by a schema-driven pre-order
//! walk; sliced columns are normalized to offset `0` during the copy; a batch
//! whose buffers exceed one chunk is packed across multiple chunks. See
//! [`crate::layout`] for the exact byte layout.

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::DataType;

use crate::alloc::ChunkAllocator;
use crate::error::{Error, Result};
use crate::layout::{
    align_up, buffers_offset, nodes_offset, payload_start, BatchHeader, BufferEntry, NodeEntry,
    BATCH_FORMAT, BATCH_MAGIC, BUFFER_ALIGN,
};
use crate::schema::SchemaRegistry;
use shm_core::ChunkDesc;

/// The flattened, normalized plan of a batch: the pre-order node table, the
/// ordered physical buffers, and the row count. Buffer handles are held as owned
/// [`Buffer`] clones (`Arc` bumps — **no** payload copy) so their bytes stay
/// valid until copied into shared memory.
struct Plan {
    nodes: Vec<NodeEntry>,
    buffers: Vec<Buffer>,
    row_count: usize,
    /// Normalized column arrays kept alive so every `buffers` handle stays valid
    /// (a `concat`-materialized array owns buffers the clones point into).
    _keepalive: Vec<ArrayRef>,
}

/// The chunk assignment of every buffer, plus the used byte length of each chunk.
struct Packing {
    /// `(chunk_index, offset_in_chunk)` for each buffer, parallel to `Plan::buffers`.
    locs: Vec<(u32, u32)>,
    /// High-water end offset (== bytes needed) of each chunk.
    chunk_used: Vec<usize>,
}

/// Serialize `batch` into **exactly one** loaned chunk from `alloc`, returning
/// its descriptor (with `schema_id` set).
///
/// This is the convenience entry point for the single-chunk case (the pub/sub
/// data path, small artifact commits): nested and sliced columns are supported,
/// but the whole serialized batch must fit in one chunk. A batch that would span
/// multiple chunks returns [`Error::ChunkTooSmall`]; use
/// [`write_batch_chunks`] for those.
pub fn write_batch<A: ChunkAllocator>(
    alloc: &A,
    registry: &SchemaRegistry,
    batch: &RecordBatch,
) -> Result<ChunkDesc> {
    let chunks = write_batch_chunks(alloc, registry, batch)?;
    if chunks.len() == 1 {
        Ok(chunks[0])
    } else {
        // Roll back every allocated (still-`FREE`) chunk and report the fit.
        for c in &chunks {
            alloc.free(c);
        }
        Err(Error::ChunkTooSmall {
            need: serialized_len(batch).unwrap_or(usize::MAX),
            have: alloc.max_chunk_size(),
        })
    }
}

/// Serialize `batch` into one or more loaned chunks, returning their descriptors
/// (each with `schema_id` set). `chunks[0]` is the **primary** chunk (it holds
/// the [`BatchHeader`] + tables); `chunks[1..]` are continuations.
///
/// The batch is written **once**. Every returned chunk is `FREE` (freshly popped
/// from the pool, never loaned); the caller loans + publishes them (and lists
/// them all in the version manifest so every chunk is reclaimed).
///
/// # Supported inputs
///
/// - **Nested** columns (`Struct`, `List`, `LargeList`, `FixedSizeList`, and
///   their recursive children) with correct validity/offset/child buffers.
/// - **Sliced** columns (`offset != 0` at any depth) — normalized to offset `0`
///   during the copy.
/// - **Multi-chunk** batches — buffers packed across chunks (each buffer stays
///   contiguous within one chunk).
///
/// # Errors
///
/// - [`Error::Unsupported`] for a Dictionary/Union/Map/RunEndEncoded type, or for
///   a single buffer larger than `alloc.max_chunk_size()`.
/// - [`Error::ChunkTooSmall`] if even the header + tables exceed one chunk.
/// - Pool exhaustion / overflow bubble up from `alloc`.
pub fn write_batch_chunks<A: ChunkAllocator>(
    alloc: &A,
    registry: &SchemaRegistry,
    batch: &RecordBatch,
) -> Result<Vec<ChunkDesc>> {
    let schema_id = registry.intern(&batch.schema());
    let plan = plan_batch(batch)?;

    let cap = alloc.max_chunk_size();
    let packing = pack(&plan, cap)?;
    let chunk_count = packing.chunk_used.len();

    let row_count =
        u32::try_from(plan.row_count).map_err(|_| Error::Unsupported("row count exceeds u32"))?;
    let node_count = u32::try_from(plan.nodes.len())
        .map_err(|_| Error::Unsupported("node count exceeds u32"))?;
    let buffer_count = u32::try_from(plan.buffers.len())
        .map_err(|_| Error::Unsupported("buffer count exceeds u32"))?;

    // ---- Allocate every chunk up front; roll back partial allocation. ----
    let mut chunks: Vec<ChunkDesc> = Vec::with_capacity(chunk_count);
    for (i, &used) in packing.chunk_used.iter().enumerate() {
        match alloc.alloc(used) {
            Ok(mut desc) => {
                if (desc.len as usize) < used {
                    // The allocator returned a smaller chunk than requested.
                    alloc.free(&desc);
                    for c in &chunks {
                        alloc.free(c);
                    }
                    return Err(Error::ChunkTooSmall {
                        need: used,
                        have: desc.len as usize,
                    });
                }
                desc.schema_id = schema_id;
                chunks.push(desc);
            }
            Err(e) => {
                for c in &chunks {
                    alloc.free(c);
                }
                let _ = i;
                return Err(e);
            }
        }
    }

    // Resolve each chunk's writable base pointer.
    let bases: Vec<*mut u8> = chunks.iter().map(|d| alloc.resolve(d)).collect();

    let header = BatchHeader {
        magic: BATCH_MAGIC,
        format: BATCH_FORMAT,
        schema_id,
        row_count,
        node_count,
        buffer_count,
        chunk_count: chunk_count as u32,
    };

    // SAFETY: `bases[0]` points at the loaned primary chunk of `chunks[0].len`
    // bytes (>= `chunk_used[0]`), exclusively owned by the caller. The header,
    // node table, and buffer table all land at offsets `< payload_start` (<=
    // `chunk_used[0]`), so every write stays within the primary chunk. Each
    // payload buffer is copied to `bases[chunk_index] + offset`, a region of
    // `len` bytes verified by `pack` to lie inside that chunk. Records are
    // `#[repr(C)]` PODs written unaligned to stay layout-agnostic.
    unsafe {
        let primary = bases[0];
        primary.cast::<BatchHeader>().write_unaligned(header);

        let node_ptr = primary.add(nodes_offset()).cast::<NodeEntry>();
        for (i, n) in plan.nodes.iter().enumerate() {
            node_ptr.add(i).write_unaligned(*n);
        }

        let buf_ptr = primary
            .add(buffers_offset(plan.nodes.len()))
            .cast::<BufferEntry>();
        for (i, ((chunk_index, offset), buffer)) in
            packing.locs.iter().zip(plan.buffers.iter()).enumerate()
        {
            buf_ptr.add(i).write_unaligned(BufferEntry {
                chunk_index: *chunk_index,
                offset: *offset,
                len: buffer.len() as u32,
            });
        }

        for ((chunk_index, offset), buffer) in packing.locs.iter().zip(plan.buffers.iter()) {
            let bytes = buffer.as_slice();
            let dst = bases[*chunk_index as usize].add(*offset as usize);
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
        }
    }

    Ok(chunks)
}

/// A batch whose chunks are allocated and whose header, node table and buffer
/// table are already written — but whose payload buffers are **empty and
/// waiting to be filled**.
///
/// The producer-writes-in-place path. Instead of building an Arrow buffer and
/// copying it into shared memory, a producer asks for the space, is handed
/// exactly the regions its buffers occupy, and writes once into final position.
///
/// Safety rests on what is *not* returned: no pointer, no offset, no chunk
/// index. [`Reserved::buffers`] hands back `&mut [u8]` already sliced to each
/// buffer's region, so a producer cannot address outside it — the slice length
/// is the bound and the compiler enforces it. Every offset is still computed by
/// [`pack`], the same function the copying path uses.
pub struct Reserved {
    chunks: Vec<ChunkDesc>,
    /// `(chunk_index, offset, len)` per Arrow buffer, from `pack`.
    locs: Vec<(u32, u32, usize)>,
}

impl Reserved {
    /// The chunks holding this batch, ready to stage once the buffers are
    /// filled.
    pub fn chunks(&self) -> &[ChunkDesc] {
        &self.chunks
    }

    /// One writable slice per Arrow buffer, in the batch's buffer order.
    ///
    /// # Safety
    ///
    /// The caller must hold these chunks exclusively — they are freshly
    /// allocated and not yet published, which is exactly the state
    /// [`reserve_batch_like`] returns them in — and must not call this twice
    /// while a previous set of slices is alive.
    pub unsafe fn buffers<'a, A: ChunkAllocator>(&self, alloc: &A) -> Vec<&'a mut [u8]> {
        let bases: Vec<*mut u8> = self.chunks.iter().map(|d| alloc.resolve(d)).collect();
        self.locs
            .iter()
            .map(|(ci, off, len)| {
                // SAFETY: `pack` placed a buffer of `len` bytes at `off` inside
                // chunk `ci`, and the chunk was allocated with at least that
                // many bytes used (checked at allocation). The caller
                // guarantees exclusive ownership.
                unsafe {
                    core::slice::from_raw_parts_mut(bases[*ci as usize].add(*off as usize), *len)
                }
            })
            .collect()
    }
}

/// Allocate and lay out a batch shaped like `template`, with the payload
/// buffers left empty for a producer to fill.
///
/// `template` supplies the *structure* — schema, node table, null counts, and
/// every buffer length except the ones named in `override_lens`, which replace
/// the length of the buffer at that index. That is deliberate: reusing
/// [`plan_batch`] means the node and buffer tables are byte-for-byte what the
/// copying path would have produced, so the reader cannot tell the two apart.
/// Nothing here invents a layout.
pub fn reserve_batch_like<A: ChunkAllocator>(
    alloc: &A,
    registry: &SchemaRegistry,
    template: &RecordBatch,
    override_lens: &[(usize, usize)],
) -> Result<Reserved> {
    let schema_id = registry.intern(&template.schema());
    let plan = plan_batch(template)?;

    let mut lens: Vec<usize> = plan.buffers.iter().map(|b| b.len()).collect();
    for (i, len) in override_lens {
        if *i >= lens.len() {
            return Err(Error::Unsupported(
                "override names a buffer that does not exist",
            ));
        }
        lens[*i] = *len;
    }

    let cap = alloc.max_chunk_size();
    let packing = pack_lens(&lens, plan.nodes.len(), cap)?;
    let chunk_count = packing.chunk_used.len();

    let row_count =
        u32::try_from(plan.row_count).map_err(|_| Error::Unsupported("row count exceeds u32"))?;
    let node_count = u32::try_from(plan.nodes.len())
        .map_err(|_| Error::Unsupported("node count exceeds u32"))?;
    let buffer_count =
        u32::try_from(lens.len()).map_err(|_| Error::Unsupported("buffer count exceeds u32"))?;

    let mut chunks: Vec<ChunkDesc> = Vec::with_capacity(chunk_count);
    for &used in packing.chunk_used.iter() {
        match alloc.alloc(used) {
            Ok(mut desc) => {
                if (desc.len as usize) < used {
                    alloc.free(&desc);
                    for c in &chunks {
                        alloc.free(c);
                    }
                    return Err(Error::ChunkTooSmall {
                        need: used,
                        have: desc.len as usize,
                    });
                }
                desc.schema_id = schema_id;
                chunks.push(desc);
            }
            Err(e) => {
                for c in &chunks {
                    alloc.free(c);
                }
                return Err(e);
            }
        }
    }

    let bases: Vec<*mut u8> = chunks.iter().map(|d| alloc.resolve(d)).collect();
    let header = BatchHeader {
        magic: BATCH_MAGIC,
        format: BATCH_FORMAT,
        schema_id,
        row_count,
        node_count,
        buffer_count,
        chunk_count: chunk_count as u32,
    };

    // SAFETY: identical to `write_batch_chunks` — the header, node table and
    // buffer table all land below `payload_start`, inside the primary chunk,
    // which was allocated with at least `chunk_used[0]` bytes. Only the payload
    // copy is missing, because the producer will write it.
    unsafe {
        let primary = bases[0];
        primary.cast::<BatchHeader>().write_unaligned(header);

        let node_ptr = primary.add(nodes_offset()).cast::<NodeEntry>();
        for (i, n) in plan.nodes.iter().enumerate() {
            node_ptr.add(i).write_unaligned(*n);
        }

        let buf_ptr = primary
            .add(buffers_offset(plan.nodes.len()))
            .cast::<BufferEntry>();
        for (i, ((chunk_index, offset), len)) in packing.locs.iter().zip(lens.iter()).enumerate() {
            buf_ptr.add(i).write_unaligned(BufferEntry {
                chunk_index: *chunk_index,
                offset: *offset,
                len: *len as u32,
            });
        }
    }

    let locs = packing
        .locs
        .iter()
        .zip(lens.iter())
        .map(|((c, o), l)| (*c, *o, *l))
        .collect();
    Ok(Reserved { chunks, locs })
}

/// [`pack`] over buffer *lengths* rather than buffers.
///
/// The same arithmetic, factored so the reserve path cannot drift from the
/// copying one.
fn pack_lens(lens: &[usize], node_count: usize, cap: usize) -> Result<Packing> {
    let start = payload_start(node_count, lens.len());
    if start > cap {
        return Err(Error::ChunkTooSmall {
            need: start,
            have: cap,
        });
    }
    let mut locs = Vec::with_capacity(lens.len());
    let mut chunk_used = vec![start];
    let mut cur = 0usize;
    let mut cursor = start;
    for &blen in lens {
        if blen > cap {
            return Err(Error::Unsupported("buffer exceeds max chunk size"));
        }
        let mut off = align_up(cursor, BUFFER_ALIGN);
        if off + blen > cap {
            cur += 1;
            chunk_used.push(0);
            off = 0;
        }
        locs.push((cur as u32, off as u32));
        cursor = off + blen;
        chunk_used[cur] = cursor;
    }
    Ok(Packing { locs, chunk_used })
}

/// Flatten `batch` into its normalized pre-order plan (nodes + buffers).
fn plan_batch(batch: &RecordBatch) -> Result<Plan> {
    let mut nodes = Vec::new();
    let mut buffers = Vec::new();
    let mut keepalive = Vec::with_capacity(batch.num_columns());
    for array in batch.columns() {
        let normalized = normalize(array)?;
        plan_node(&normalized.to_data(), &mut nodes, &mut buffers)?;
        keepalive.push(normalized);
    }
    Ok(Plan {
        nodes,
        buffers,
        row_count: batch.num_rows(),
        _keepalive: keepalive,
    })
}

/// Append one `ArrayData` node (and, recursively, its children) to the flattened
/// pre-order plan. `data` must already be normalized (offset `0` at every depth).
fn plan_node(
    data: &ArrayData,
    nodes: &mut Vec<NodeEntry>,
    buffers: &mut Vec<Buffer>,
) -> Result<()> {
    ensure_supported(data.data_type())?;

    let len =
        u32::try_from(data.len()).map_err(|_| Error::Unsupported("array length exceeds u32"))?;
    let null_count = u32::try_from(data.null_count())
        .map_err(|_| Error::Unsupported("null count exceeds u32"))?;
    let has_validity = data.nulls().is_some();
    let data_buffers = data.buffers().len();
    let child_count = data.child_data().len();

    nodes.push(NodeEntry {
        len,
        null_count,
        has_validity: u32::from(has_validity),
        data_buffers: u32::try_from(data_buffers)
            .map_err(|_| Error::Unsupported("too many data buffers in a node"))?,
        child_count: u32::try_from(child_count)
            .map_err(|_| Error::Unsupported("too many children in a node"))?,
        _pad: 0,
    });

    // Validity first, then data buffers, matching the read-side consume order.
    if let Some(nulls) = data.nulls() {
        buffers.push(nulls.buffer().clone());
    }
    for b in data.buffers() {
        buffers.push(b.clone());
    }
    for child in data.child_data() {
        plan_node(child, nodes, buffers)?;
    }
    Ok(())
}

/// Reject types this layout does not round-trip (their child/buffer conventions
/// differ from the generic node model). The supported nested set is
/// `Struct`/`List`/`LargeList`/`FixedSizeList`; everything else must be flat.
fn ensure_supported(dt: &DataType) -> Result<()> {
    match dt {
        DataType::Dictionary(_, _) => Err(Error::Unsupported("dictionary type not supported")),
        DataType::Union(_, _) => Err(Error::Unsupported("union type not supported")),
        DataType::Map(_, _) => Err(Error::Unsupported("map type not supported")),
        DataType::RunEndEncoded(_, _) => {
            Err(Error::Unsupported("run-end-encoded type not supported"))
        }
        _ => Ok(()),
    }
}

/// Normalize `array` so its (and every descendant's) `ArrayData::offset()` is `0`.
///
/// Returns the array unchanged when it is already normalized; otherwise compacts
/// it with a single-input `concat`, which materializes fresh, offset-`0`,
/// tightly-sized buffers at every nesting level.
fn normalize(array: &ArrayRef) -> Result<ArrayRef> {
    if is_normalized(&array.to_data()) {
        Ok(array.clone())
    } else {
        Ok(arrow_select::concat::concat(&[array.as_ref()])?)
    }
}

/// Whether `data` (recursively) has offset `0` and an offset-`0` validity buffer.
fn is_normalized(data: &ArrayData) -> bool {
    data.offset() == 0
        && data.nulls().is_none_or(|n| n.offset() == 0)
        && data.child_data().iter().all(is_normalized)
}

/// Assign every buffer to a chunk, opening a new continuation chunk whenever the
/// next buffer would overflow the current one.
///
/// `cap` is the per-chunk capacity (the pool's max chunk size). Fails with
/// [`Error::Unsupported`] if a single buffer (or the header + tables) exceeds
/// `cap` — such a buffer cannot be stored contiguously in any chunk.
fn pack(plan: &Plan, cap: usize) -> Result<Packing> {
    let start = payload_start(plan.nodes.len(), plan.buffers.len());
    if start > cap {
        return Err(Error::ChunkTooSmall {
            need: start,
            have: cap,
        });
    }

    let mut locs = Vec::with_capacity(plan.buffers.len());
    let mut chunk_used = vec![start];
    let mut cur = 0usize;
    let mut cursor = start;

    for buffer in &plan.buffers {
        let blen = buffer.len();
        if blen > cap {
            return Err(Error::Unsupported("buffer exceeds max chunk size"));
        }
        let mut off = align_up(cursor, BUFFER_ALIGN);
        if off + blen > cap {
            // Overflow: open a fresh continuation chunk (offset 0 is 64-aligned).
            cur += 1;
            chunk_used.push(0);
            off = 0;
        }
        locs.push((cur as u32, off as u32));
        cursor = off + blen;
        chunk_used[cur] = cursor;
    }

    Ok(Packing { locs, chunk_used })
}

/// The serialized byte length of `batch` if it fit in a **single** chunk — enough
/// to size an arena without a dry-run write. Returns `None` on an unsupported
/// (e.g. Dictionary/Union) input.
pub fn serialized_len(batch: &RecordBatch) -> Option<usize> {
    let plan = plan_batch(batch).ok()?;
    let mut cursor = payload_start(plan.nodes.len(), plan.buffers.len());
    for b in &plan.buffers {
        cursor = align_up(cursor, BUFFER_ALIGN) + b.len();
    }
    Some(cursor)
}
