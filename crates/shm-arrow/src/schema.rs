//! Schema interning: `SchemaRef` <-> `schema_id: u32`.
//!
//! A [`shm_core::ChunkDesc`] carries only a 4-byte `schema_id`; the full Arrow
//! [`SchemaRef`] it names is looked up here. Id `0` is reserved for untyped raw
//! bytes (a non-Arrow chunk) and never assigned to a schema.
//!
//! # Scope (v0.1)
//!
//! This is an **in-process** registry. Two processes that must agree on ids
//! seed themselves identically with [`SchemaRegistry::with_schemas`] (ids are
//! assigned `1, 2, 3, …` in slice order). Cross-process interning — where a
//! coordinator hands out globally-agreed ids at runtime — is deferred to v0.2;
//! see ADR-0001.

use std::collections::HashMap;
use std::sync::Mutex;

use arrow_schema::SchemaRef;

/// The reserved id meaning "untyped raw bytes" (not an Arrow batch).
pub const RAW_SCHEMA_ID: u32 = 0;

/// A bidirectional map between Arrow schemas and compact `schema_id`s.
///
/// Cloneable-by-`Arc` in callers; internally guarded by a [`Mutex`] so it can
/// be shared and interned into concurrently.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Interned schemas; `by_id[id - 1]` holds the schema for `id`.
    by_id: Vec<SchemaRef>,
    /// Fingerprint (`{:?}` of the schema) -> id, for dedup on intern.
    by_key: HashMap<String, u32>,
}

impl SchemaRegistry {
    /// Create an empty registry (only [`RAW_SCHEMA_ID`] is defined).
    pub fn new() -> SchemaRegistry {
        SchemaRegistry::default()
    }

    /// Create a registry pre-seeded with `schemas`, assigning ids `1, 2, 3, …`
    /// in slice order.
    ///
    /// Two processes constructed from the same slice agree on every id, which
    /// is how the v0.1 walking skeleton keeps a reader and writer in sync.
    /// Duplicate schemas in the slice collapse to their first id.
    pub fn with_schemas(schemas: &[SchemaRef]) -> SchemaRegistry {
        let registry = SchemaRegistry::new();
        for schema in schemas {
            registry.intern(schema);
        }
        registry
    }

    /// Intern `schema`, returning its stable id (minting a fresh one on first
    /// sight). Never returns [`RAW_SCHEMA_ID`].
    pub fn intern(&self, schema: &SchemaRef) -> u32 {
        let key = fingerprint(schema);
        let mut inner = self.inner.lock().expect("schema registry poisoned");
        if let Some(&id) = inner.by_key.get(&key) {
            return id;
        }
        inner.by_id.push(schema.clone());
        let id = inner.by_id.len() as u32; // ids are 1-based
        inner.by_key.insert(key, id);
        id
    }

    /// Resolve an id back to its [`SchemaRef`], or `None` for an unknown id or
    /// for [`RAW_SCHEMA_ID`].
    pub fn resolve(&self, id: u32) -> Option<SchemaRef> {
        if id == RAW_SCHEMA_ID {
            return None;
        }
        let inner = self.inner.lock().expect("schema registry poisoned");
        inner.by_id.get((id - 1) as usize).cloned()
    }

    /// Number of interned schemas.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("schema registry poisoned").by_id.len()
    }

    /// Whether no schemas have been interned yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A deterministic, in-process fingerprint of a schema.
///
/// `arrow_schema::Schema` implements neither `Hash` nor a stable wire encoding
/// we want to depend on here, so v0.1 keys on the `Debug` rendering, which is
/// total over fields, data types, nullability, and metadata. This is
/// process-local only (the ADR-0001 deferral); a v0.2 cross-process registry
/// will use a content hash of the Arrow IPC schema message.
fn fingerprint(schema: &SchemaRef) -> String {
    format!("{schema:?}")
}
