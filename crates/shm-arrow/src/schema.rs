//! Schema interning: `SchemaRef` <-> `schema_id: u32`, plus a process-independent
//! schema wire form and content hash.
//!
//! A [`shm_core::ChunkDesc`] carries only a 4-byte `schema_id`; the full Arrow
//! [`SchemaRef`] it names is looked up here. Id `0` is reserved for untyped raw
//! bytes (a non-Arrow chunk) and never assigned to a schema.
//!
//! # Scope (v0.3, ADR-0003 item E)
//!
//! The [`SchemaRegistry`] is a **local cache**. Two flavours of population exist:
//!
//! - **Single process / tests** seed it deterministically with
//!   [`SchemaRegistry::with_schemas`] (ids `1, 2, 3, …` in slice order), or let
//!   [`intern`](SchemaRegistry::intern) mint ids on first sight.
//! - **Cross process** — the [`shm-runtime`] coordinator is the *authoritative*
//!   catalog; a node learns each `(schema_id, SchemaRef)` mapping over UDS and
//!   records it here with [`insert`](SchemaRegistry::insert). The registry then
//!   answers [`resolve`](SchemaRegistry::resolve) / [`id_for`](SchemaRegistry::id_for)
//!   from the cache with no further round-trips.
//!
//! This crate stays **transport-agnostic**: it knows how to serialize a schema to
//! bytes and hash them (so a coordinator can intern by content), but it never
//! speaks UDS — the runtime wires the cache to the coordinator.
//!
//! ## Content hash — the cross-process interning key
//!
//! [`serialize_schema`] encodes a schema to the Arrow-IPC *schema message* — the
//! canonical, version-stable wire form — and [`schema_content_hash`] folds those
//! bytes with FNV-1a into a `u64`. Because the IPC encoding is a pure function of
//! the schema (fields, types, nullability, metadata) and FNV-1a is a fixed
//! algorithm, two *different processes* running the same Arrow version produce
//! **identical** bytes and therefore the **identical** hash for an equal schema —
//! this is what lets the coordinator dedup (intern) a schema submitted by one
//! process against the same schema submitted by another. `std`'s `DefaultHasher`
//! is explicitly *not* used: it is unspecified and unstable across builds.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::root_as_message;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{ArrowError, SchemaRef};

use crate::error::{Error, Result};

/// The reserved id meaning "untyped raw bytes" (not an Arrow batch).
pub const RAW_SCHEMA_ID: u32 = 0;

/// Serialize `schema` to its canonical Arrow-IPC schema-message bytes.
///
/// This is the version-stable wire form the runtime ships over UDS and the input
/// to [`schema_content_hash`]. It is a pure function of the schema, so it is
/// identical byte-for-byte across processes on the same Arrow version — see the
/// module docs for the cross-process determinism argument. Infallible: the IPC
/// schema encoder cannot fail for a well-formed [`arrow_schema::Schema`].
pub fn serialize_schema(schema: &SchemaRef) -> Vec<u8> {
    let generator = IpcDataGenerator::default();
    // `false` = do not error if a dictionary field reappears; a schema message
    // carries no dictionary data, so the tracker is inert here.
    let mut tracker = DictionaryTracker::new(false);
    let options = IpcWriteOptions::default();
    generator
        .schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options)
        .ipc_message
}

/// Reconstruct a [`SchemaRef`] from bytes produced by [`serialize_schema`].
///
/// The inverse of [`serialize_schema`]; a round-trip yields a schema equal to the
/// original. Returns [`Error::Arrow`](crate::Error::Arrow) if the bytes are not a
/// valid Arrow-IPC schema message.
pub fn deserialize_schema(bytes: &[u8]) -> Result<SchemaRef> {
    // `serialize_schema` emits the bare IPC `Message` flatbuffer (no stream
    // length-prefix / continuation framing), so parse the message root directly
    // rather than through `try_schema_from_ipc_buffer` (which expects framing).
    let message = root_as_message(bytes).map_err(|e| {
        Error::Arrow(ArrowError::ParseError(format!(
            "invalid schema IPC message: {e}"
        )))
    })?;
    let ipc_schema = message.header_as_schema().ok_or_else(|| {
        Error::Arrow(ArrowError::ParseError(
            "IPC message header is not a Schema".to_string(),
        ))
    })?;
    Ok(Arc::new(fb_to_schema(ipc_schema)))
}

/// A stable, process-independent FNV-1a (64-bit) hash of a schema's serialized
/// bytes (as produced by [`serialize_schema`]).
///
/// Deterministic across processes and builds by construction (fixed algorithm
/// over the canonical IPC bytes), which is why it can serve as the coordinator's
/// interning key. Equal schemas hash equal; different schemas hash different
/// (barring an astronomically unlikely 64-bit collision).
pub fn schema_content_hash(bytes: &[u8]) -> u64 {
    // FNV-1a, 64-bit. Not `std::hash::DefaultHasher` (unspecified/unstable).
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The content hash of a live schema (serialize then hash) — the key both the
/// registry and the coordinator intern on.
fn content_key(schema: &SchemaRef) -> u64 {
    schema_content_hash(&serialize_schema(schema))
}

/// A bidirectional cache between Arrow schemas and compact `schema_id`s.
///
/// Cloneable-by-`Arc` in callers; internally guarded by a [`Mutex`] so it can be
/// shared and populated concurrently. Keyed on the process-independent
/// [`schema_content_hash`] so that an id learned from the coordinator and an id
/// minted locally by [`intern`](Self::intern) resolve to the same slot for the
/// same schema.
#[derive(Debug)]
pub struct SchemaRegistry {
    inner: Mutex<Inner>,
}

impl Default for SchemaRegistry {
    fn default() -> SchemaRegistry {
        SchemaRegistry::new()
    }
}

#[derive(Debug)]
struct Inner {
    /// `schema_id` -> schema. Sparse: ids may be learned out of order from the
    /// coordinator, so this is a map rather than a dense `Vec`.
    by_id: HashMap<u32, SchemaRef>,
    /// [`schema_content_hash`] -> `schema_id`, for dedup on intern / lookup.
    by_hash: HashMap<u64, u32>,
    /// Next id to mint for a *locally* interned schema (never issued to the
    /// coordinator; single-process/test path). Starts at 1 so [`RAW_SCHEMA_ID`]
    /// is never minted.
    next_id: u32,
}

impl SchemaRegistry {
    /// Create an empty registry (only [`RAW_SCHEMA_ID`] is defined).
    pub fn new() -> SchemaRegistry {
        SchemaRegistry {
            inner: Mutex::new(Inner {
                by_id: HashMap::new(),
                by_hash: HashMap::new(),
                next_id: 1,
            }),
        }
    }

    /// Create a registry pre-seeded with `schemas`, assigning ids `1, 2, 3, …` in
    /// slice order.
    ///
    /// Two processes constructed from the same slice agree on every id — the
    /// single-process / test contract. Duplicate schemas in the slice collapse to
    /// their first id. (Cross-process agreement no longer *requires* this; the
    /// coordinator issues ids at runtime — see the module docs.)
    pub fn with_schemas(schemas: &[SchemaRef]) -> SchemaRegistry {
        let registry = SchemaRegistry::new();
        for schema in schemas {
            registry.intern(schema);
        }
        registry
    }

    /// Intern `schema`, returning its id (minting a fresh **local** one on first
    /// sight). Never returns [`RAW_SCHEMA_ID`].
    ///
    /// When the cache has already learned this schema's id — whether from a prior
    /// `intern`, a [`with_schemas`](Self::with_schemas) seed, or an
    /// [`insert`](Self::insert) of a coordinator-issued id — the existing id is
    /// returned. This is the seam that lets the write path pick up a
    /// coordinator-issued id transparently: a node calls
    /// `Node::intern_schema` (which `insert`s the coordinator's id), and the
    /// subsequent `write_batch` `intern` hits the cache and stamps that same id.
    pub fn intern(&self, schema: &SchemaRef) -> u32 {
        let key = content_key(schema);
        let mut inner = self.inner.lock().expect("schema registry poisoned");
        if let Some(&id) = inner.by_hash.get(&key) {
            return id;
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner.by_hash.insert(key, id);
        inner.by_id.insert(id, schema.clone());
        id
    }

    /// Record an explicit `(schema_id, schema)` mapping learned from the
    /// coordinator, keying it by content hash so a later [`intern`](Self::intern)
    /// or [`id_for`](Self::id_for) of the same schema returns this id.
    ///
    /// Idempotent: re-inserting the same id/schema is a no-op, and an existing
    /// mapping is never overwritten. Ignores [`RAW_SCHEMA_ID`]. Bumps the local
    /// mint counter past `id` so a future *local* intern cannot collide with a
    /// coordinator-issued id already known here.
    pub fn insert(&self, id: u32, schema: SchemaRef) {
        if id == RAW_SCHEMA_ID {
            return;
        }
        let key = content_key(&schema);
        let mut inner = self.inner.lock().expect("schema registry poisoned");
        inner.by_hash.entry(key).or_insert(id);
        inner.by_id.entry(id).or_insert(schema);
        if id >= inner.next_id {
            inner.next_id = id + 1;
        }
    }

    /// The id already cached for `schema` (by content hash), or `None` if this
    /// registry has not learned it yet. Does **not** mint a new id — the
    /// local-cache probe a node runs before asking the coordinator to intern.
    pub fn id_for(&self, schema: &SchemaRef) -> Option<u32> {
        let key = content_key(schema);
        self.inner
            .lock()
            .expect("schema registry poisoned")
            .by_hash
            .get(&key)
            .copied()
    }

    /// Resolve an id back to its [`SchemaRef`], or `None` for an unknown id or for
    /// [`RAW_SCHEMA_ID`].
    pub fn resolve(&self, id: u32) -> Option<SchemaRef> {
        if id == RAW_SCHEMA_ID {
            return None;
        }
        self.inner
            .lock()
            .expect("schema registry poisoned")
            .by_id
            .get(&id)
            .cloned()
    }

    /// Number of interned schemas.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("schema registry poisoned")
            .by_id
            .len()
    }

    /// Whether no schemas have been interned yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn schema_a() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn schema_b() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    #[test]
    fn serialize_round_trip_equals_original() {
        for schema in [schema_a(), schema_b()] {
            let bytes = serialize_schema(&schema);
            let back = deserialize_schema(&bytes).expect("deserialize");
            assert_eq!(back, schema, "schema must survive an IPC round trip");
        }
    }

    #[test]
    fn content_hash_is_equal_for_equal_and_stable_on_recompute() {
        let h1 = schema_content_hash(&serialize_schema(&schema_a()));
        // A second, independently constructed but equal schema must hash equal.
        let h2 = schema_content_hash(&serialize_schema(&schema_a()));
        assert_eq!(h1, h2, "equal schemas must hash equal (stable recompute)");
        // A different schema must hash differently.
        let hb = schema_content_hash(&serialize_schema(&schema_b()));
        assert_ne!(h1, hb, "different schemas must hash differently");
    }

    #[test]
    fn insert_then_lookup_by_id_and_by_content_hash() {
        let reg = SchemaRegistry::new();
        // Learn a coordinator-issued mapping (id 7, out of the local 1,2,3 order).
        reg.insert(7, schema_a());
        assert_eq!(reg.resolve(7), Some(schema_a()), "resolve by id");
        assert_eq!(reg.id_for(&schema_a()), Some(7), "lookup by content hash");
        // A subsequent intern of the same schema returns the inserted id, not a
        // freshly minted local one.
        assert_eq!(
            reg.intern(&schema_a()),
            7,
            "intern hits the inserted mapping"
        );
        // Idempotent re-insert is a no-op.
        reg.insert(7, schema_a());
        assert_eq!(reg.resolve(7), Some(schema_a()));
    }

    #[test]
    fn with_schemas_and_raw_id_still_work() {
        let reg = SchemaRegistry::with_schemas(&[schema_a(), schema_b()]);
        assert_eq!(reg.intern(&schema_a()), 1, "first schema is id 1");
        assert_eq!(reg.intern(&schema_b()), 2, "second schema is id 2");
        assert_eq!(reg.resolve(1), Some(schema_a()));
        assert_eq!(reg.resolve(2), Some(schema_b()));
        // RAW_SCHEMA_ID is reserved and never resolves to a schema.
        assert_eq!(reg.resolve(RAW_SCHEMA_ID), None);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn intern_dedups_and_mints_monotonically() {
        let reg = SchemaRegistry::new();
        assert_eq!(reg.intern(&schema_a()), 1);
        assert_eq!(reg.intern(&schema_a()), 1, "same schema dedups to its id");
        assert_eq!(reg.intern(&schema_b()), 2, "new schema mints the next id");
    }
}
