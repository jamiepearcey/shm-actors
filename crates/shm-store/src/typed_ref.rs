//! The **typed-ref envelope** (ADR-0007 G1): a 56-byte POD that names a keyed
//! store entry, carried on the wire as an ordinary 24-byte
//! [`ChunkDesc`](shm_core::ChunkDesc) pointing at the chunk that holds it.
//!
//! # The envelope-in-a-chunk (ADR-0007 §G1, option a)
//!
//! The substrate's wire unit stays the 24-byte descriptor. A *typed* ref is a
//! payload chunk holding a [`TypedRef`] POD, tagged by the reserved system schema
//! id [`SCHEMA_TYPED_REF`](crate::SCHEMA_TYPED_REF) so a peer recognises the
//! chunk as an envelope (not raw payload) before reading it. A front translates a
//! wire request into a `TypedRef`, [`write_typed_ref`]s it into a chunk, and
//! dispatches the chunk's `ChunkDesc` through a ring/task slot; a worker reads the
//! system schema id, [`read_typed_ref`]s the envelope back, and resolves its
//! `key_id` against the keyed store ([`KeyedStore::resolve_and_pin`](crate::KeyedStore::resolve_and_pin)).
//!
//! `key_id` is **authoritative**; `locator`/`manifest` are an optional
//! pre-resolved fast path (all-zero = resolve by key).

use shm_arrow::{ChunkAllocator, SegmentBase};
use shm_core::{ChunkDesc, PackedRef, SharedPod};

use crate::catalog::RefKind;
use crate::error::{Error, Result};
use crate::SCHEMA_TYPED_REF;

/// Envelope magic `0x5452_4546` — the ASCII of `"TREF"` read most-significant
/// byte first (`T`=0x54, `R`=0x52, `E`=0x45, `F`=0x46), the exact frozen value
/// ADR-0007 §G1 specifies.
pub const TYPED_REF_MAGIC: u32 = 0x5452_4546;

/// The envelope ABI version this build reads and writes.
pub const TYPED_REF_ABI_VERSION: u16 = 1;

/// A 56-byte POD envelope that names a keyed-store referent by `key_id`, with an
/// optional pre-resolved fast path (ADR-0007 G1).
///
/// # Layout (frozen ABI — 56 bytes, `#[repr(C)]`, `Copy`, no implicit padding)
///
/// | field         | type        | offset | meaning                                            |
/// |---------------|-------------|-------:|----------------------------------------------------|
/// | `magic`       | `u32`       | 0      | [`TYPED_REF_MAGIC`] (`"TREF"`)                      |
/// | `abi_version` | `u16`       | 4      | [`TYPED_REF_ABI_VERSION`] (`= 1`)                  |
/// | `kind`        | `u16`       | 6      | [`RefKind`] discriminant                           |
/// | `key_id`      | `u32`       | 8      | coordinator-interned key (`0` = none / raw)        |
/// | `schema_id`   | `u32`       | 12     | schema of the **referent** payload (`0` = raw)     |
/// | `version`     | `u64`       | 16     | `0` = current; else assert-match                   |
/// | `locator`     | [`ChunkDesc`]| 24    | optional resolved fast path (all-zero = by key)    |
/// | `manifest`    | `u64`       | 48     | [`PackedRef`] to a `VersionManifest`; `0` = none   |
///
/// The 24-byte `locator` at offset 24 (4-aligned) and the two `u64`s at 8-aligned
/// offsets 16/48 pack with **no** implicit padding, so the struct is a clean POD
/// (see the `const _` size + offset assertions below).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct TypedRef {
    /// [`TYPED_REF_MAGIC`]; validated on read.
    pub magic: u32,
    /// [`TYPED_REF_ABI_VERSION`]; validated on read.
    pub abi_version: u16,
    /// The [`RefKind`] discriminant of the referent.
    pub kind: u16,
    /// Coordinator-interned key id of the referent (`0` = none, kind `RawChunk`).
    pub key_id: u32,
    /// Schema id of the referent's payload (`0` = raw bytes). Carried on the
    /// envelope so a consumer can resolve the schema without prior agreement.
    pub schema_id: u32,
    /// Requested version: `0` = the entry's current version; else an exact match
    /// is asserted at resolve time.
    pub version: u64,
    /// Optional resolved fast path: the referent version's primary
    /// [`ChunkDesc`]. All-zero ([`ChunkDesc::ZERO`]) means "resolve by `key_id`".
    pub locator: ChunkDesc,
    /// Optional [`PackedRef`] (as raw bits) to the referent's `VersionManifest`;
    /// `0` = none.
    pub manifest: u64,
}

// SAFETY: `#[repr(C)]`, all-POD fields, no padding (asserted below), no pointers,
// no `Drop` — a legitimate cross-process shared POD.
unsafe impl SharedPod for TypedRef {}

/// The wire size of a [`TypedRef`] envelope, in bytes (frozen ABI).
pub const TYPED_REF_SIZE: usize = 56;

const _: () = assert!(core::mem::size_of::<TypedRef>() == TYPED_REF_SIZE);
const _: () = assert!(core::mem::align_of::<TypedRef>() == 8);
// Explicit field offsets — the frozen ABI (no implicit padding anywhere).
const _: () = assert!(core::mem::offset_of!(TypedRef, magic) == 0);
const _: () = assert!(core::mem::offset_of!(TypedRef, abi_version) == 4);
const _: () = assert!(core::mem::offset_of!(TypedRef, kind) == 6);
const _: () = assert!(core::mem::offset_of!(TypedRef, key_id) == 8);
const _: () = assert!(core::mem::offset_of!(TypedRef, schema_id) == 12);
const _: () = assert!(core::mem::offset_of!(TypedRef, version) == 16);
const _: () = assert!(core::mem::offset_of!(TypedRef, locator) == 24);
const _: () = assert!(core::mem::offset_of!(TypedRef, manifest) == 48);

impl TypedRef {
    /// Build a **resolve-by-key** envelope: the `key_id` is authoritative and the
    /// `locator`/`manifest` fast path is empty (resolve via the catalog).
    ///
    /// This is what a front dispatches when it has only the logical identity of
    /// the referent (its key + desired version), not a pre-resolved chunk.
    pub fn by_key(kind: RefKind, key_id: u32, schema_id: u32, version: u64) -> TypedRef {
        TypedRef {
            magic: TYPED_REF_MAGIC,
            abi_version: TYPED_REF_ABI_VERSION,
            kind: kind.as_u16(),
            key_id,
            schema_id,
            version,
            locator: ChunkDesc::ZERO,
            manifest: 0,
        }
    }

    /// Build a **pre-resolved** envelope carrying the fast-path `locator` (the
    /// referent version's primary chunk) and `manifest` (its `VersionManifest`
    /// [`PackedRef`] bits; `0` = none). `key_id` remains authoritative — the fast
    /// path is a hint a resolver may use when present and valid, else it falls
    /// back to resolving by key.
    pub fn resolved(
        kind: RefKind,
        key_id: u32,
        schema_id: u32,
        version: u64,
        locator: ChunkDesc,
        manifest: u64,
    ) -> TypedRef {
        TypedRef {
            magic: TYPED_REF_MAGIC,
            abi_version: TYPED_REF_ABI_VERSION,
            kind: kind.as_u16(),
            key_id,
            schema_id,
            version,
            locator,
            manifest,
        }
    }

    /// The decoded [`RefKind`] of the referent, or [`Error::BadKind`] if the
    /// stored discriminant is out of range.
    #[inline]
    pub fn kind(&self) -> Result<RefKind> {
        RefKind::from_u16(self.kind)
    }

    /// Whether a non-empty `locator` fast path is present.
    #[inline]
    pub fn has_locator(&self) -> bool {
        !self.locator.is_zero()
    }

    /// The `manifest` [`PackedRef`] if present (`None` when `0`).
    #[inline]
    pub fn manifest_ref(&self) -> Option<PackedRef> {
        (self.manifest != 0).then_some(PackedRef(self.manifest))
    }

    /// Validate the envelope's `magic` and `abi_version`, returning
    /// [`Error::BadEnvelope`] on mismatch. Called by [`read_typed_ref`] /
    /// [`from_bytes`](Self::from_bytes) before an envelope is trusted.
    #[inline]
    pub fn validate(&self) -> Result<()> {
        if self.magic != TYPED_REF_MAGIC || self.abi_version != TYPED_REF_ABI_VERSION {
            return Err(Error::BadEnvelope);
        }
        Ok(())
    }

    /// The envelope as its 56 wire bytes.
    #[inline]
    pub fn to_bytes(&self) -> [u8; TYPED_REF_SIZE] {
        let mut out = [0u8; TYPED_REF_SIZE];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Decode + validate an envelope from at least 56 bytes, or
    /// [`Error::BadEnvelope`] if too short / the magic/abi do not match.
    pub fn from_bytes(bytes: &[u8]) -> Result<TypedRef> {
        if bytes.len() < TYPED_REF_SIZE {
            return Err(Error::BadEnvelope);
        }
        // `pod_read_unaligned` copies the bytes out into an aligned `TypedRef`, so
        // no alignment assumption is placed on `bytes`.
        let tref: TypedRef = bytemuck::pod_read_unaligned(&bytes[..TYPED_REF_SIZE]);
        tref.validate()?;
        Ok(tref)
    }
}

/// Which path a [`resolve`](crate::KeyedStore::resolve_and_pin) took (ADR-0007
/// G1: "prefer the resolved fast path when present and valid, else by key").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvePath {
    /// The envelope carried a valid pre-resolved `locator`/`manifest`, so the
    /// referent could be located without a catalog scan.
    FastPath,
    /// The envelope was resolved by `key_id` through the catalog (a cold key
    /// miss would hit the coordinator; here `key_id` is already interned).
    ByKey,
}

/// Decide which resolve path a `TypedRef` would take: the fast path when a
/// non-empty `locator` is present **and** self-consistent with the envelope's
/// `schema_id`, else by key. Pure (does not touch the store); used to document
/// and test the decision.
pub fn resolve_path(tref: &TypedRef) -> ResolvePath {
    if tref.has_locator() && tref.locator.schema_id == tref.schema_id {
        ResolvePath::FastPath
    } else {
        ResolvePath::ByKey
    }
}

/// Allocate a chunk (≥ 56 bytes) from `alloc`, write `tref`'s envelope into it,
/// and return a [`ChunkDesc`] tagged with
/// [`SCHEMA_TYPED_REF`](crate::SCHEMA_TYPED_REF) so a peer recognises the chunk as
/// an envelope rather than raw payload (ADR-0007 G1).
///
/// The chunk is freshly popped from the pool's free list (still `FREE`, never
/// loaned), so the caller exclusively owns its bytes until the chunk is either
/// dispatched (and eventually freed by the consumer) or freed directly. The
/// returned descriptor's `segment_id`/`offset`/`generation` are the allocator's;
/// only its `schema_id` is overwritten.
pub fn write_typed_ref<A: ChunkAllocator>(alloc: &A, tref: &TypedRef) -> Result<ChunkDesc> {
    let mut desc = alloc.alloc(TYPED_REF_SIZE)?;
    let base = alloc.resolve(&desc);
    let bytes = tref.to_bytes();
    // SAFETY: `alloc` guarantees `base` addresses at least `desc.len` (>= 56)
    // writable bytes of a chunk this caller exclusively owns (freshly popped,
    // still FREE). `bytes` is a distinct 56-byte stack array — no overlap.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), base, TYPED_REF_SIZE);
    }
    desc.schema_id = SCHEMA_TYPED_REF;
    Ok(desc)
}

/// Read + validate a [`TypedRef`] from the chunk `desc` names, resolving the
/// chunk's segment base through `resolver` (ADR-0007 G1).
///
/// Asserts `desc.schema_id == `[`SCHEMA_TYPED_REF`](crate::SCHEMA_TYPED_REF)
/// ([`Error::NotEnvelope`] otherwise), then copies the 56 bytes out and validates
/// the magic/abi ([`Error::BadEnvelope`] on mismatch, or if the chunk is shorter
/// than the envelope).
pub fn read_typed_ref<S: SegmentBase>(resolver: &S, desc: &ChunkDesc) -> Result<TypedRef> {
    if desc.schema_id != SCHEMA_TYPED_REF {
        return Err(Error::NotEnvelope(desc.schema_id));
    }
    if (desc.len as usize) < TYPED_REF_SIZE {
        return Err(Error::BadEnvelope);
    }
    let base = resolver.base_ptr();
    // SAFETY: `desc` was minted for the segment `resolver` keeps mapped; its
    // `offset` is in-bounds and the chunk is >= 56 bytes (checked above). The
    // borrowed slice never outlives this call, and `from_bytes` copies out.
    let bytes =
        unsafe { core::slice::from_raw_parts(base.add(desc.offset as usize), TYPED_REF_SIZE) };
    TypedRef::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use shm_arrow::{PoolAllocator, SchemaRegistry};
    use shm_artifact::{ArtifactHead, Commit};
    use shm_core::segment::HEADER_SIZE;
    use shm_core::{BorrowJournal, Pool, PoolConfig, Segment};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::{Catalog, KeyResolver, KeyedStore};

    #[test]
    fn size_and_field_offsets_are_frozen() {
        assert_eq!(core::mem::size_of::<TypedRef>(), 56);
        assert_eq!(core::mem::align_of::<TypedRef>(), 8);
        assert_eq!(core::mem::offset_of!(TypedRef, magic), 0);
        assert_eq!(core::mem::offset_of!(TypedRef, abi_version), 4);
        assert_eq!(core::mem::offset_of!(TypedRef, kind), 6);
        assert_eq!(core::mem::offset_of!(TypedRef, key_id), 8);
        assert_eq!(core::mem::offset_of!(TypedRef, schema_id), 12);
        assert_eq!(core::mem::offset_of!(TypedRef, version), 16);
        assert_eq!(core::mem::offset_of!(TypedRef, locator), 24);
        assert_eq!(core::mem::offset_of!(TypedRef, manifest), 48);
        assert_eq!(TYPED_REF_MAGIC, 0x5452_4546);
    }

    #[test]
    fn magic_and_abi_validation() {
        let good = TypedRef::by_key(RefKind::Dataset, 7, 16, 0);
        assert!(good.validate().is_ok());
        assert_eq!(TypedRef::from_bytes(&good.to_bytes()).unwrap(), good);

        // Corrupt magic.
        let mut bad = good;
        bad.magic ^= 0xdead;
        assert!(matches!(
            TypedRef::from_bytes(&bad.to_bytes()),
            Err(Error::BadEnvelope)
        ));

        // Corrupt abi_version.
        let mut bad = good;
        bad.abi_version = 99;
        assert!(matches!(
            TypedRef::from_bytes(&bad.to_bytes()),
            Err(Error::BadEnvelope)
        ));

        // Too short.
        assert!(matches!(
            TypedRef::from_bytes(&[0u8; 40]),
            Err(Error::BadEnvelope)
        ));
    }

    #[test]
    fn resolve_path_prefers_valid_locator() {
        let by_key = TypedRef::by_key(RefKind::Dataset, 1, 16, 0);
        assert_eq!(resolve_path(&by_key), ResolvePath::ByKey);

        let locator = ChunkDesc {
            segment_id: 5,
            generation: 1,
            offset: 128,
            len: 256,
            schema_id: 16,
            _pad: 0,
        };
        let fast = TypedRef::resolved(RefKind::Dataset, 1, 16, 3, locator, 0x1234);
        assert_eq!(resolve_path(&fast), ResolvePath::FastPath);
        assert!(fast.has_locator());
        assert_eq!(fast.manifest_ref(), Some(PackedRef(0x1234)));

        // A locator whose schema disagrees with the envelope is not trusted.
        let mut mismatched = fast;
        mismatched.locator.schema_id = 99;
        assert_eq!(resolve_path(&mismatched), ResolvePath::ByKey);
    }

    // ---- Local write/read round-trip + resolve_and_pin harness ----

    #[derive(Default)]
    struct MemResolver {
        inner: Mutex<(HashMap<Vec<u8>, u32>, u32)>,
    }
    impl KeyResolver for MemResolver {
        fn intern_key(&self, key: &[u8]) -> Result<u32> {
            let mut g = self.inner.lock().unwrap();
            if let Some(&id) = g.0.get(key) {
                return Ok(id);
            }
            g.1 += 1;
            let id = g.1;
            g.0.insert(key.to_vec(), id);
            Ok(id)
        }
    }

    struct Harness {
        catalog: Arc<Segment>,
        head: Arc<Segment>,
        data: Arc<Segment>,
        journal: Arc<Segment>,
        registry: Arc<SchemaRegistry>,
        resolver: MemResolver,
    }

    impl Harness {
        fn new(base: u32, schema: &SchemaRef) -> Harness {
            let cap = 16u32;
            let head_stride = (ArtifactHead::region_bytes() + 63) & !63;
            for id in base..base + 4 {
                let _ = Segment::unlink_by_id(id);
            }
            let catalog = Segment::create(base, Catalog::segment_bytes(cap)).unwrap();
            Catalog::init(&catalog, cap, head_stride as u32, 1 << 28).unwrap();
            let head = Segment::create(base + 1, HEADER_SIZE + cap as usize * head_stride).unwrap();
            let data = Segment::create(base + 2, 1 << 20).unwrap();
            Pool::create(&data, &PoolConfig::power_of_two(256, 8192, 32)).unwrap();
            let journal = Segment::create(base + 3, 64 * 1024).unwrap();
            BorrowJournal::create(&journal, 256).unwrap();
            Harness {
                catalog: Arc::new(catalog),
                head: Arc::new(head),
                data: Arc::new(data),
                journal: Arc::new(journal),
                registry: Arc::new(SchemaRegistry::with_schemas(std::slice::from_ref(schema))),
                resolver: MemResolver::default(),
            }
        }
        fn store(&self) -> KeyedStore<'_> {
            KeyedStore::new(
                self.catalog.clone(),
                self.head.clone(),
                self.data.clone(),
                self.journal.clone(),
                self.registry.clone(),
                7,
                &self.resolver,
            )
        }
        fn unlink(&self) {
            self.catalog.unlink().ok();
            self.head.unlink().ok();
            self.data.unlink().ok();
            self.journal.unlink().ok();
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }
    fn batch(schema: &SchemaRef, vals: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vals.to_vec()))],
        )
        .unwrap()
    }

    #[test]
    fn write_read_round_trip_through_a_chunk() {
        let h = Harness::new(83_000 + (std::process::id() & 0x3ff), &schema());
        let pool = Pool::attach(&h.data).unwrap();
        let alloc = PoolAllocator::new(&pool, &h.data);

        let tref = TypedRef::by_key(RefKind::Dataset, 42, 16, 7);
        let desc = write_typed_ref(&alloc, &tref).unwrap();
        assert_eq!(
            desc.schema_id, SCHEMA_TYPED_REF,
            "envelope chunk is system-tagged"
        );

        let guard = shm_arrow::PinGuard::new(h.data.clone());
        let got = read_typed_ref(&guard, &desc).unwrap();
        assert_eq!(got, tref, "envelope round-trips through a chunk");

        // A raw (schema_id 0) descriptor is rejected as not-an-envelope.
        let mut raw = desc;
        raw.schema_id = 0;
        assert!(matches!(
            read_typed_ref(&guard, &raw),
            Err(Error::NotEnvelope(0))
        ));

        h.unlink();
    }

    #[test]
    fn resolve_and_pin_reads_pinned_batch_and_checks_version() {
        let sch = schema();
        let h = Harness::new(84_000 + (std::process::id() & 0x3ff), &sch);
        let store = h.store();

        let entry = store.create(b"dataset/X", RefKind::Dataset, &sch).unwrap();
        let key_id = entry.key_id();
        entry
            .commit(Commit::Replace, &batch(&sch, &[1, 2, 3]))
            .unwrap();
        entry
            .commit_replace(&batch(&sch, &[100, 200, 300]))
            .unwrap();
        assert_eq!(entry.current_version(), 2);

        // Resolve current (version 0 = current) → the pinned v2 batch.
        let tref = TypedRef::by_key(RefKind::Dataset, key_id, 16, 0);
        let (rentry, pin, got) = store.resolve_and_pin(&tref).unwrap();
        assert_eq!(rentry.artifact_id(), entry.artifact_id());
        assert_eq!(pin.version(), 2);
        assert_eq!(got, batch(&sch, &[100, 200, 300]));
        drop(pin);

        // Exact-version match resolves too.
        let tref_v2 = TypedRef::by_key(RefKind::Dataset, key_id, 16, 2);
        let (_e, pin2, _b) = store.resolve_and_pin(&tref_v2).unwrap();
        assert_eq!(pin2.version(), 2);
        drop(pin2);

        // A version that is not current → VersionMismatch.
        let tref_v1 = TypedRef::by_key(RefKind::Dataset, key_id, 16, 1);
        assert!(matches!(
            store.resolve_and_pin(&tref_v1),
            Err(Error::VersionMismatch {
                expected: 1,
                actual: 2
            })
        ));

        // A RawChunk kind is not resolvable.
        let raw = TypedRef::by_key(RefKind::RawChunk, 0, 0, 0);
        assert!(matches!(store.resolve(&raw), Err(Error::NotResolvable)));
        assert!(matches!(
            store.resolve_and_pin(&raw),
            Err(Error::NotResolvable)
        ));

        h.unlink();
    }
}
