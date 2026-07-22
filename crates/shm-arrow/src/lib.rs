//! `shm-arrow` — Arrow buffer layout over `shm-core` chunks.
//!
//! The design invariant of the whole substrate is *payloads are written once,
//! everything downstream moves the 24-byte descriptor*. This crate realises
//! that for Apache Arrow: it serializes a [`RecordBatch`](arrow_array::RecordBatch)'s
//! buffers into **one** loaned [`ChunkDesc`](shm_core::ChunkDesc) chunk on the
//! write side, and on the read side reconstructs a `RecordBatch` whose buffers
//! point *directly* at the mapped shared segment — one copy in, zero after.
//!
//! # Pieces
//!
//! - [`SchemaRegistry`] — interns `SchemaRef` <-> `schema_id: u32` (id `0`
//!   reserved for raw bytes). v0.1 is in-process; identical seeding via
//!   [`SchemaRegistry::with_schemas`] keeps a writer and reader agreeing.
//! - [`ChunkAllocator`] — the allocation seam; [`PoolAllocator`] implements it
//!   over a [`shm_core::Pool`] + [`shm_core::Segment`].
//! - [`write_batch`] — serialize a batch into a loaned chunk (see [`layout`]
//!   for the on-chunk [`BatchHeader`]/[`ColumnEntry`]/[`BufferEntry`] ABI).
//! - [`read_batch`] — zero-copy reconstruction, validating generation via
//!   [`ChunkCtrl::validate`](shm_core::ChunkCtrl::validate) and the batch magic
//!   first. Buffers borrow the mapping through a caller-supplied pin guard
//!   (e.g. [`PinGuard`]).
//!
//! # Scope (v0.1)
//!
//! Single-chunk batches of flat columns (primitive + `Utf8`/`Binary`), unsliced
//! input, in-process schema interning. Multi-chunk batches, nested types,
//! sliced arrays, and a cross-process schema coordinator are v0.2 (ADR-0001).

#![deny(missing_docs)]

pub mod alloc;
pub mod error;
pub mod layout;
pub mod read;
pub mod schema;
pub mod write;

pub use alloc::{ChunkAllocator, PoolAllocator};
pub use error::{Error, Result};
pub use layout::{BatchHeader, BufferEntry, ColumnEntry, BATCH_MAGIC, BUFFER_ALIGN};
pub use read::{read_batch, PinGuard, SegmentBase};
pub use schema::{SchemaRegistry, RAW_SCHEMA_ID};
pub use write::{serialized_len, write_batch};
