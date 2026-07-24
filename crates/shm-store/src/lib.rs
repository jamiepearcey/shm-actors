//! `shm-store` — a keyed collection **of** `shm-artifact` artifacts (ADR-0007 G3).
//!
//! The store adds three things on top of the *unchanged* `shm-artifact` RCU/MVCC
//! (which it reuses, never reinvents — write lease, hazard pins, refcounted chunk
//! sharing, crash reclaim):
//!
//! 1. **Keying.** An opaque byte-string key (≤ [`MAX_KEY_LEN`]) is interned by the
//!    coordinator to a stable `key_id: u32` (the proven `schema_id` precedent), so
//!    every on-shm struct stays fixed-size POD and tenant/scope prefixes cost zero
//!    ABI change.
//! 2. **A shm catalog** ([`Catalog`]/[`CatalogSlot`]) — an append-only, CAS
//!    state-machine index (`FREE → LIVE → TOMBSTONE`) placed by hand into a
//!    store-owned management segment. A worker resolves
//!    `key_id → CatalogSlot → ArtifactHead` with **no** UDS round-trip once the
//!    catalog is mapped.
//! 3. **A store handle** ([`KeyedStore`]) over three coordinator-granted segments
//!    (catalog + head-management + shared data pool), whose [`Entry`] handles
//!    [`create`](KeyedStore::create) / [`open`](KeyedStore::open) /
//!    [`evict`](KeyedStore::evict) keyed artifacts and delegate
//!    commit / pin / read straight to `shm-artifact`.
//!
//! The coordinator (`shm-runtime`) is the registry of record: it owns the store's
//! segments, interns keys, and — via the catalog it also maps — routes a dead
//! actor's leaked entry pins / write leases back to their `ArtifactHead` on crash.
//!
//! # G1 readiness
//!
//! This is G3 only; the typed-ref envelope (G1) is the next stage. The key
//! interning and store APIs it will call are in place, and [`SCHEMA_TYPED_REF`] +
//! [`FIRST_USER_SCHEMA_ID`] fix the reserved system-schema-id convention now so G1
//! can rely on it (ADR-0007 §ABI).

#![deny(missing_docs)]

pub mod catalog;
pub mod error;
pub mod store;

pub use catalog::{
    Catalog, CatalogSlot, RefKind, CATALOG_MAGIC, SLOT_FREE, SLOT_LIVE, SLOT_TOMBSTONE,
};
pub use error::{Error, Result};
pub use store::{Entry, KeyResolver, KeyedStore, MAX_KEY_LEN};

/// Reserved system schema id for the G1 typed-ref envelope (ADR-0007 §ABI).
///
/// Schema ids `1..=15` are reserved as **system** ids; the coordinator issues
/// user schema ids starting at [`FIRST_USER_SCHEMA_ID`]. `0` remains
/// [`RAW_SCHEMA_ID`](shm_arrow::RAW_SCHEMA_ID) (untyped bytes). A G1 `TypedRef`
/// envelope chunk is tagged with this id so a worker recognises the envelope
/// before resolving the referent.
pub const SCHEMA_TYPED_REF: u32 = 1;

/// The first schema id the coordinator issues to **user** schemas (ADR-0007
/// §ABI). Ids `1..=15` are reserved for system envelopes (see
/// [`SCHEMA_TYPED_REF`]); `0` is raw bytes.
pub const FIRST_USER_SCHEMA_ID: u32 = 16;
