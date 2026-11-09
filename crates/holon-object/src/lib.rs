//! `holon-object` — opaque, versioned objects in the keyed store; the sibling
//! of [`holon-topic`](holon_topic).
//!
//! An object is **one** keyed-store entry, `object/<key>`, whose Arrow schema is
//! a single row `{content_type, data}`. [`ObjectStore::put`] installs it as a
//! [`Commit::Replace`](shm_artifact::Commit::Replace), so the entry's version
//! *is* the object version: monotonic, gap-free, assigned under the entry's
//! exclusive write lease. Every version is immutable — a `put` never mutates the
//! bytes a reader is looking at, it installs a new version beside them.
//!
//! A payload larger than one chunk is split across **several Arrow batches**
//! which are staged invisibly and installed as **one** version by a single
//! `commit_staged` CAS (`shm-stream`'s transaction, [`StreamWriter`]). Splitting
//! across *rows* would not raise the ceiling — Arrow lays a `Binary` column's
//! values out back to back in one contiguous values buffer, and a buffer larger
//! than a chunk is refused by `shm-arrow` — whereas each *batch* gets its own
//! buffers in its own chunks. Because the N batches arrive as one version, a
//! reader can never see a partially written object: it sees version `n`, then
//! version `n+1`, and nothing between.
//!
//! # Large objects
//!
//! Cutting only works while the shared pool has chunks to cut from. An object
//! **larger than [`ObjectStore::pooled_ceiling`]** is therefore not cut at all:
//! it is written into a coordinator-granted segment of its own, holding one
//! [`Pool`] of one class of one chunk, and it reads back as a single contiguous
//! buffer. That is the same split a process allocator makes at
//! `M_MMAP_THRESHOLD`, and it has the same two payoffs — the object is
//! contiguous by construction, and it cannot fragment the shared arena because
//! it was never carved from it.
//!
//! The threshold is deliberately **not a tunable**. The dedicated path is taken
//! exactly when the pool cannot hold the object, because below that pooled is
//! strictly cheaper: no coordinator round trip, no `mmap` per reader, and no
//! segment to unlink afterwards.
//!
//! Lifetime follows the object, not the writer. The segment is *staged* while
//! it is being filled — the writer's borrow-journal entry reclaims it if the
//! writer dies — and [`Node::install_payload_segment`] closes that window once
//! the version is installed. From then on the version's manifest holds the `+1`
//! on the object's chunk, so a `put` that replaces the object releases the old
//! segment without anyone unlinking it: the retire cascade drives the old chunk
//! to `FREE`, and the coordinator's sweep reads that control word and unlinks.
//! A reader in another process maps the segment on demand — its pin names the
//! segment it is missing — *after* taking the version pin, so the manifest's
//! reference already holds the chunk against `FREE` and the sweep cannot unlink
//! underneath the mapping.
//!
//! Reading is [`ObjectStore::get`]: a journaled pin plus a zero-copy view of the
//! payload straight out of the shared data segment. The pin travels inside the
//! returned [`ObjectRead`], so a version stays readable for exactly as long as
//! the reader holds it, however many `put`s land meanwhile.
//!
//! Notification is descriptor-only and rides the **same ring as topics**
//! ([`holon_topic::TOPIC_EVENTS`]): each `put` drops a
//! [`VersionEvent`] `{name_id = key_id, version, kind = Replace}`. Nothing is
//! allocated, copied or pinned for it — a watcher learns *that* a version exists
//! and calls `get` if it wants the bytes. A lapped ring loses a wake-up, which
//! [`ObjectStore::head`] recovers.
//!
//! # What this deliberately is not
//!
//! - **There is no time travel.** The substrate lets a reader pin only an
//!   entry's *current* version ([`Entry::pin`]), so [`Version::IfCurrent`] is an
//!   assert-match, not a seek: it succeeds iff the named version is still
//!   current, and otherwise reports [`Error::VersionMismatch`] with what is.
//!   A reader that wants to *hold* version `v` must `get` it while it is current
//!   and keep the [`ObjectRead`] alive.
//! - **`delete` is not notified.** The events ring carries version arrivals; a
//!   deletion has no version to announce, and inventing one would give watchers
//!   a sequence number that names nothing.
//! - **A payload is not one contiguous slice — unless it earned a segment.**
//!   A pooled object arrives as several buffers; only a dedicated one is
//!   contiguous. Consumers walk [`ObjectRead::segments`] either way.
//! - **The *pooled* ceiling is the version manifest and the pool, not one size
//!   class.** A version's manifest is itself one chunk listing 24 bytes per data
//!   chunk plus 4 per batch, so a store pool whose largest class is `C` bytes
//!   holds roughly `C/28` segments of just under `C` bytes each, bounded also by
//!   how many `C`-sized chunks the pool was built with.
//!   [`ObjectStore::max_object`] computes it. Past it an object is not refused —
//!   it goes to a segment of its own; what is refused is a single dedicated
//!   object over 4 GiB serialized, which a chunk's `u32` length cannot address
//!   ([`Error::TooLarge`]).

#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use holon_topic::TOPIC_EVENTS;
use shm_arrow::{write_batch_chunks, PoolAllocator, SchemaRegistry};
use shm_artifact::{Commit, CommitKind, VersionEvent, VersionPin};
use shm_core::{
    required_bytes, BorrowJournal, ChunkDesc, Pool, PoolConfig, Segment, SizeClass, LOANED,
};
use shm_runtime::{Node, TopicPublisher};
use shm_store::{Entry, KeyedStore, RefKind};
use shm_stream::{Coordination, StreamWriter};

/// Key prefix of object entries in the keyed store.
pub const KEY_PREFIX: &str = "object/";

/// Longest object key accepted (leaves ample room under `shm_store::MAX_KEY_LEN`
/// once [`KEY_PREFIX`] is applied).
pub const MAX_OBJECT_KEY: usize = 256;

/// Longest content type accepted.
pub const MAX_CONTENT_TYPE: usize = 128;

/// Retries on a lost write-lease race before giving up.
const PUT_RETRIES: u32 = 64;

/// How many distinct dedicated segments one operation will fault in before it
/// gives up and reports the refusal.
///
/// A version names at most one segment per batch, and a manifest holds far
/// fewer than this, so exceeding it means something other than a cold mapping
/// is wrong.
const FAULT_IN_LIMIT: u32 = 64;

/// Errors of the object store.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The store's pool has no size class big enough to hold one segment.
    #[error("no usable size class for an object segment")]
    NoUsableClass,
    /// The payload is too large for **one chunk of its own segment**: a chunk
    /// is addressed by a `u32` length, so a single dedicated object cannot
    /// exceed 4 GiB serialized. Nothing was allocated.
    #[error("object of {0} serialized bytes exceeds the 4 GiB dedicated-chunk limit")]
    TooLarge(usize),
    /// A descriptor named a dedicated segment this node could not map, and
    /// retrying did not converge.
    #[error("could not map dedicated segment {0}")]
    Unmapped(u32),
    /// The producer handed to [`ObjectStore::write_with`] failed, or staging a
    /// segment did. Nothing was installed.
    #[error("streaming write failed: {0}")]
    Sink(#[source] std::io::Error),
    /// The object key is empty, too long, or not in `[A-Za-z0-9._/-]`.
    #[error("bad object key: {0}")]
    BadKey(&'static str),
    /// The content type is too long or not printable ASCII.
    #[error("bad content type: {0}")]
    BadContentType(&'static str),
    /// No live entry tracks this key (never written, or deleted).
    #[error("no such object")]
    NotFound,
    /// The requested version is not the entry's current one. There is no seek:
    /// see the crate docs on [`Version::IfCurrent`].
    #[error("object is at version {current}, not {requested}")]
    VersionMismatch {
        /// The version the caller asked for.
        requested: u64,
        /// The version that is actually current.
        current: u64,
    },
    /// The stored batch did not have the object schema.
    #[error("stored batch has an unexpected shape")]
    BadObject,
    /// The write lease stayed contended for every retry.
    #[error("put lost the write lease {0} times")]
    Contended(u32),
    /// The staging transaction failed (a chunk could not be written, loaned or
    /// journaled, or the install was rejected). Nothing is installed.
    #[error("stream: {0}")]
    Stream(#[from] shm_stream::Error),
    /// The shared-memory substrate rejected the operation.
    #[error("core: {0}")]
    Core(#[from] shm_core::Error),
    /// A batch could not be serialized into shared memory.
    #[error("arrow: {0}")]
    Arrow(#[from] shm_arrow::Error),
    /// The store rejected the operation.
    #[error("store: {0}")]
    Store(#[from] shm_store::Error),
    /// The runtime rejected the operation.
    #[error("runtime: {0}")]
    Runtime(#[from] shm_runtime::Error),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Which version [`ObjectStore::get`] should return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    /// Whatever is current at the moment of the pin.
    Latest,
    /// Only if this is still the current version — a precondition, not a seek.
    /// not a seek. [`Error::VersionMismatch`] if the object has moved on.
    IfCurrent(u64),
}

/// Validate an object key: `1..=MAX_OBJECT_KEY` bytes of `[A-Za-z0-9._/-]`.
pub fn check_object_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::BadKey("empty"));
    }
    if key.len() > MAX_OBJECT_KEY {
        return Err(Error::BadKey("too long"));
    }
    if !key
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-' || b == b'/')
    {
        return Err(Error::BadKey("only [A-Za-z0-9._/-] allowed"));
    }
    Ok(())
}

/// Validate a content type: `0..=MAX_CONTENT_TYPE` bytes of printable ASCII
/// (empty means "unspecified").
pub fn check_content_type(content_type: &str) -> Result<()> {
    if content_type.len() > MAX_CONTENT_TYPE {
        return Err(Error::BadContentType("too long"));
    }
    if !content_type
        .bytes()
        .all(|b| b.is_ascii_graphic() || b == b' ')
    {
        return Err(Error::BadContentType("only printable ASCII allowed"));
    }
    Ok(())
}

/// Store key of an object.
pub fn object_key(key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(KEY_PREFIX.len() + key.len());
    k.extend_from_slice(KEY_PREFIX.as_bytes());
    k.extend_from_slice(key.as_bytes());
    k
}

/// The Arrow schema of one object segment (exactly one row).
pub fn object_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("content_type", DataType::Utf8, false),
        Field::new("data", DataType::Binary, false),
    ]))
}

/// Build the one-row batch for **one segment** of an object.
///
/// Crate-private: see [`borrowed_buffer`] for why the visibility is the
/// soundness argument.
///
/// One row per batch, and one batch per segment, deliberately. Splitting a
/// payload across *rows* does not raise the ceiling: Arrow lays a `Binary`
/// column's values out back to back in a single contiguous values buffer, so N
/// rows still allocate one buffer of the total size, and `shm-arrow` refuses a
/// buffer larger than one chunk. N *batches* is the axis that works — each batch
/// gets its own buffers and its own chunks — and all N install as one version.
///
/// `content_type` is repeated in every segment; it is the object's, not the
/// segment's, and readers take it from the first.
pub(crate) fn encode_segment(schema: &SchemaRef, content_type: &str, data: &[u8]) -> RecordBatch {
    // `BinaryArray::from(vec![data])` would copy the whole payload into an
    // Arrow-owned buffer, and `write_batch_chunks` then copies that into the
    // chunk — two passes over every byte for one stored object. Measured on a
    // 16 MiB put: 225 us each, together 98.5% of the call, against a 221 us
    // raw-memcpy floor. So the values buffer is built as a *view* of the
    // caller's bytes and only the copy into shared memory remains.
    let values = borrowed_buffer(data);
    let offsets = arrow_buffer::Buffer::from_slice_ref([0i32, data.len() as i32]);
    // SAFETY: the layout is Binary's own — one i32 offsets buffer of two
    // entries for one row, and a values buffer of exactly `data.len()` bytes,
    // which is what the offsets describe. No nulls, so no validity buffer.
    let array = unsafe {
        arrow_data::ArrayData::new_unchecked(
            DataType::Binary,
            1,
            Some(0),
            None,
            0,
            vec![offsets, values],
            vec![],
        )
    };
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![content_type])),
            Arc::new(BinaryArray::from(array)),
        ],
    )
    .expect("two same-length columns matching the object schema")
}

/// An Arrow buffer that *borrows* `data` instead of copying it.
///
/// **Crate-private on purpose.** The soundness argument is that the batch never
/// outlives the call that built it — it is written into a chunk and dropped —
/// and that is only checkable while every caller is in this file. Exporting
/// [`encode_segment`] would let a caller hold the batch past the slice it
/// borrows, which is undefined behaviour that no test would catch. If this ever
/// needs to be public it must return something that ties the two lifetimes
/// together, not a bare `RecordBatch`.
fn borrowed_buffer(data: &[u8]) -> arrow_buffer::Buffer {
    // `Allocation` is blanket-implemented for anything Send + Sync + unwind
    // safe, so an empty guard is all that is needed: it owns nothing and frees
    // nothing when the buffer drops.
    struct NoOp;

    if data.is_empty() {
        return arrow_buffer::Buffer::from_vec(Vec::<u8>::new());
    }
    let keep: Arc<dyn arrow_buffer::alloc::Allocation> = Arc::new(NoOp);
    // SAFETY: `ptr` is `data`'s own base, non-null because `data` is non-empty,
    // and `data.len()` bytes are readable and immutable for the lifetime of the
    // borrow. The buffer is consumed within this call, so it cannot outlive
    // `data`.
    unsafe {
        arrow_buffer::Buffer::from_custom_allocation(
            std::ptr::NonNull::new_unchecked(data.as_ptr() as *mut u8),
            data.len(),
            keep,
        )
    }
}

/// Split a one-row object batch into its two columns.
///
/// The returned arrays are refcounted clones — still pointing at the pinned
/// shared-memory buffers, still zero-copy.
pub fn split_segment(batch: &RecordBatch) -> Result<(StringArray, BinaryArray)> {
    if batch.num_columns() != 2 || batch.num_rows() != 1 {
        return Err(Error::BadObject);
    }
    let content_type = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(Error::BadObject)?
        .clone();
    let data = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(Error::BadObject)?
        .clone();
    Ok((content_type, data))
}

/// One pinned object version: the payload borrowed zero-copy out of the shared
/// data segment, plus the pin that keeps it alive.
///
/// Holding an `ObjectRead` holds the version. Later `put`s install new versions
/// beside it and do not disturb these bytes; the version is retired only once
/// the last reader drops (or, after a `kill -9`, once the coordinator replays
/// this actor's borrow journal).
///
/// The payload is a **sequence of segments**, not one slice: see
/// [`segments`](Self::segments).
pub struct ObjectRead {
    pin: VersionPin,
    content_type: StringArray,
    /// One entry per segment of the version, in commit order.
    data: Vec<BinaryArray>,
    version: u64,
}

impl ObjectRead {
    /// The version these bytes belong to.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The content type the writer declared (may be empty).
    #[inline]
    pub fn content_type(&self) -> &str {
        self.content_type.value(0)
    }

    /// How many segments the payload is stored in (always at least one).
    #[inline]
    pub fn segment_count(&self) -> usize {
        self.data.len()
    }

    /// Every segment in order, each borrowed in place out of shared memory.
    ///
    /// There is deliberately **no method handing back the whole payload as one
    /// `&[u8]`**. The segments are separate Arrow buffers in separate chunks of
    /// the shared pool, so a contiguous view would have to copy the payload —
    /// and avoiding that copy is the entire reason this type exists. A consumer
    /// walks the segments (an HTTP body writer, a hasher and a `write_vectored`
    /// all take them as they are); one that truly needs a single buffer
    /// concatenates explicitly, where the cost is visible in its own code.
    pub fn segments(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.data.iter().map(|a| a.value(0))
    }

    /// The `i`-th segment, or `None` past the end — random access for a
    /// consumer that resumes a walk across parks instead of draining the
    /// iterator in one pass.
    #[inline]
    pub fn segment(&self, i: usize) -> Option<&[u8]> {
        self.data.get(i).map(|a| a.value(0))
    }

    /// Total payload length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.segments().map(|s| s.len()).sum()
    }

    /// `true` when the payload is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The pin holding this version alive.
    #[inline]
    pub fn pin(&self) -> &VersionPin {
        &self.pin
    }
}

impl std::fmt::Debug for ObjectRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectRead")
            .field("version", &self.version)
            .field("content_type", &self.content_type())
            .field("segments", &self.segment_count())
            .field("len", &self.len())
            .finish()
    }
}

/// Everything an object client needs that does **not** borrow the node: the
/// notification publisher and the interned schema. Build it before
/// `Node::store()`, then pair it with the store in an [`ObjectStore`].
pub struct ObjectSide {
    events: TopicPublisher,
    schema: SchemaRef,
}

impl ObjectSide {
    /// Coordinator-intern the object schema (so the id stamped into chunks and
    /// manifests is globally agreed) and open the notification ring.
    pub fn open(node: &mut Node) -> Result<ObjectSide> {
        let schema = object_schema();
        node.intern_schema(&schema)?;
        let events = node.topic_publisher(TOPIC_EVENTS)?;
        Ok(ObjectSide { events, schema })
    }

    /// The notification publisher.
    #[inline]
    pub fn events(&self) -> &TopicPublisher {
        &self.events
    }

    /// The interned object schema.
    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

/// Opaque versioned objects over one keyed store.
///
/// Entries are cached per key (an [`Entry`] is a cheap artifact handle; caching
/// skips the catalog probe and the key-intern round trip). [`delete`](Self::delete)
/// drops its own cache line; a key evicted by *another* handle can leave a stale
/// cached entry whose next operation fails `Stale`, exactly as in `holon-topic`.
pub struct ObjectStore<'a> {
    store: &'a KeyedStore<'a>,
    side: &'a ObjectSide,
    registry: Arc<SchemaRegistry>,
    /// The node, for the **large-object** path only: an object the shared pool
    /// cannot hold needs a segment granted, installed and (for a reader) mapped,
    /// and only the coordinator can do any of that.
    ///
    /// Borrowed shared alongside the store, which is why the store must come
    /// from [`Node::opened_store`] rather than `Node::store` — the latter's
    /// `&mut` borrow would live as long as the handle and lock the node out of
    /// granting anything.
    node: &'a Node,
    entries: Mutex<HashMap<String, (Arc<Entry>, u32)>>,
}

impl<'a> ObjectStore<'a> {
    /// Bind an object store to a keyed store, its node-independent side, and the
    /// node that grants dedicated segments for large objects.
    pub fn new(
        store: &'a KeyedStore<'a>,
        side: &'a ObjectSide,
        registry: Arc<SchemaRegistry>,
        node: &'a Node,
    ) -> ObjectStore<'a> {
        registry.intern(&side.schema);
        ObjectStore {
            store,
            side,
            registry,
            node,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// The keyed store this object store reads and writes.
    #[inline]
    pub fn store(&self) -> &'a KeyedStore<'a> {
        self.store
    }

    /// The schema registry (readers of raw batches need it).
    #[inline]
    pub fn registry(&self) -> &Arc<SchemaRegistry> {
        &self.registry
    }

    /// The store pool's largest chunk, in bytes — the size **one segment** of an
    /// object is cut to fit, not the ceiling on an object.
    ///
    /// Read from the data segment's pool rather than assumed: Arrow places a
    /// segment's `data` buffer contiguously, so it is a chunk that bounds a
    /// segment. A payload larger than this is split across several batches and
    /// installed as one version — for the ceiling on an *object*, ask
    /// [`max_object`](Self::max_object).
    pub fn max_payload(&self) -> Result<u32> {
        let pool =
            shm_core::Pool::attach(self.store.data_segment()).map_err(shm_store::Error::from)?;
        Ok(pool.max_chunk_size())
    }

    /// **The largest payload the shared pool can hold whatever content type is
    /// declared, in bytes** — and so the point past which an object is
    /// certainly given a dedicated segment of its own.
    ///
    /// It is no longer a ceiling on an *object*: a payload above it is not
    /// refused, it is placed in a segment of its own. And it is deliberately
    /// not a tunable — the dedicated path is taken precisely when the pool
    /// cannot hold the object, because below that pooled is strictly cheaper:
    /// no coordinator round trip, no `mmap` per reader, and no segment to
    /// unlink afterwards.
    ///
    /// Worst-case in the content type (see below), so the number holds whatever
    /// the caller ends up declaring; the *exact* switch-over point for one
    /// declared content type is [`pooled_ceiling`](Self::pooled_ceiling), which
    /// is this number or a little more.
    ///
    /// Derived, not assumed, from the two things that actually bound a
    /// segmented object:
    ///
    /// 1. **The version manifest**, which is itself one chunk listing 24 bytes
    ///    per data chunk and 4 per batch — so a pool whose largest class is `C`
    ///    admits about `C/28` single-chunk segments;
    /// 2. **the largest size class's chunk count**, since every segment takes
    ///    one of those chunks and one more holds the manifest.
    ///
    /// The smaller of the two, times the segment payload of a worst-case
    /// ([`MAX_CONTENT_TYPE`]-long) content type — so the number holds whatever
    /// content type the caller ends up declaring. It is a **structural** ceiling:
    /// a payload under it can still fail to stage if other live objects have the
    /// pool's largest class exhausted, which surfaces as a clean staging failure
    /// that installs nothing (see [`put`](Self::put)).
    pub fn max_object(&self) -> Result<u64> {
        let ct = " ".repeat(MAX_CONTENT_TYPE);
        self.pooled_ceiling(&ct)
    }

    /// The largest payload the shared pool can hold for **this** content type:
    /// the exact byte at which [`put`](Self::put) stops cutting segments out of
    /// the pool and asks for a segment of its own.
    ///
    /// It depends on the content type because every segment of a pooled object
    /// repeats it — the content type is the object's, not the segment's — so a
    /// longer one leaves less room for payload in each chunk. A caller that
    /// wants one number for every content type asks
    /// [`max_object`](Self::max_object) instead.
    pub fn pooled_ceiling(&self, content_type: &str) -> Result<u64> {
        Ok(self.pooled_shape(content_type)?.ceiling)
    }

    /// The largest `data` slice one segment can carry: the chunk cap less the
    /// exact serialized overhead of a segment batch carrying `content_type`
    /// (batch header, node/buffer tables, both offset buffers, the content-type
    /// bytes and inter-buffer alignment). Derived by asking `shm-arrow` to size
    /// an empty segment, so nothing here is a guess or a hardcoded fraction, and
    /// each segment lands in exactly one chunk.
    fn segment_size(&self, content_type: &str) -> Result<usize> {
        Ok(self.pooled_shape(content_type)?.per_segment)
    }

    /// The size class to cut segments from, and the payload one segment holds.
    ///
    /// **Not the largest class.** A segment takes one whole chunk, so the
    /// object's ceiling is `usable chunks x payload per segment`, and the
    /// largest class is often not the one that maximises it: a pool typically
    /// gives its biggest class very few chunks. RAE's shape has four 8 MiB
    /// chunks and eight of 4 MiB, so cutting at 8 MiB yields three segments
    /// (one chunk holds the manifest) for 24 MiB, while cutting at 4 MiB yields
    /// seven for 28 MiB — a bigger object from a smaller chunk.
    ///
    /// Ties go to the larger chunk: fewer, bigger segments mean a shorter
    /// manifest and fewer descriptors to walk on every read.
    fn pooled_shape(&self, content_type: &str) -> Result<PooledShape> {
        let pool = Pool::attach(self.store.data_segment()).map_err(shm_store::Error::from)?;
        let probe = encode_segment(&self.side.schema, content_type, &[]);
        let overhead =
            shm_arrow::serialized_len(&probe).expect("the object schema is serializable");
        let by_manifest = Self::segments_per_manifest(pool.max_chunk_size());

        let mut best: Option<PooledShape> = None;
        for i in 0..pool.num_classes() {
            let Some((size, count)) = pool.class_info(i) else {
                continue;
            };
            let per_segment = (size as usize).saturating_sub(overhead);
            if per_segment == 0 || count == 0 {
                continue;
            }
            // One chunk of *some* class holds the manifest; charging it to this
            // class is the conservative reading and keeps the number honest for
            // a pool whose classes are unevenly stocked.
            let usable = (count as usize).saturating_sub(1).min(by_manifest);
            let shape = PooledShape {
                class: size,
                per_segment,
                ceiling: usable as u64 * per_segment as u64,
            };
            if best.is_none_or(|b| {
                shape.ceiling > b.ceiling || (shape.ceiling == b.ceiling && shape.class > b.class)
            }) {
                best = Some(shape);
            }
        }
        best.ok_or(Error::NoUsableClass)
    }

    /// How many segments' descriptors and spans fit one manifest chunk.
    ///
    /// `manifest_len` is monotone in the segment count, so this bisects rather
    /// than assuming a closed form for a layout `shm-artifact` owns.
    fn segments_per_manifest(cap: u32) -> usize {
        let (mut lo, mut hi) = (0usize, cap as usize);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if shm_artifact::manifest_len(mid, mid, 0) <= cap as usize {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        lo
    }

    /// Get-or-create the entry for `key`. Only [`put`](Self::put) takes this
    /// path: a catalog slot is spent the first time a key is written, never by a
    /// read of a key that does not exist.
    fn entry_or_create(&self, key: &str) -> Result<(Arc<Entry>, u32)> {
        check_object_key(key)?;
        let mut cache = self.entries.lock().expect("entry cache poisoned");
        if let Some((e, kid)) = cache.get(key) {
            return Ok((e.clone(), *kid));
        }
        // `create` is get-or-create and never appends a catalog slot for an
        // existing key: an object costs exactly one slot, once.
        let entry = Arc::new(self.store.create(
            &object_key(key),
            RefKind::Object,
            &self.side.schema,
        )?);
        let kid = entry.key_id();
        cache.insert(key.to_string(), (entry.clone(), kid));
        Ok((entry, kid))
    }

    /// Open the live entry for `key`, or `None` if none is live. Never creates.
    fn entry_open(&self, key: &str) -> Result<Option<(Arc<Entry>, u32)>> {
        check_object_key(key)?;
        let mut cache = self.entries.lock().expect("entry cache poisoned");
        if let Some((e, kid)) = cache.get(key) {
            return Ok(Some((e.clone(), *kid)));
        }
        let entry = match self.store.open(&object_key(key)) {
            Ok(e) => Arc::new(e),
            Err(shm_store::Error::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let kid = entry.key_id();
        cache.insert(key.to_string(), (entry.clone(), kid));
        Ok(Some((entry, kid)))
    }

    /// Create `key`'s entry if it does not exist and cache it, returning its
    /// interned `key_id`. Commits nothing: the object exists with no version.
    ///
    /// It is here so that a process with a *known, fixed* set of keys can pay
    /// their key-interning round trips up front — at startup, on its own
    /// thread, with nothing else in flight — instead of discovering them one at
    /// a time on the hot path. Every later [`put`](Self::put) or
    /// [`get`](Self::get) of a warmed key is then pure shared memory: a catalog
    /// scan, no control socket. For a writer whose key space is unbounded this
    /// is not available and `put` interns lazily, as before.
    pub fn ensure(&self, key: &str) -> Result<u32> {
        Ok(self.entry_or_create(key)?.1)
    }

    /// The interned `key_id` of `key` — the `name_id` its notification events
    /// carry — or `None` if the object does not exist. A watcher maps its keys
    /// with this once and then matches events without touching the store.
    pub fn key_id(&self, key: &str) -> Result<Option<u32>> {
        Ok(self.entry_open(key)?.map(|(_, kid)| kid))
    }

    /// The current version of `key`, or `None` if the object does not exist.
    /// `Some(0)` means the entry exists but nothing is committed in it.
    pub fn head(&self, key: &str) -> Result<Option<u64>> {
        Ok(self.entry_open(key)?.map(|(e, _)| e.current_version()))
    }

    /// Commit a new immutable version of `key`; returns its version number.
    ///
    /// An object the shared pool can hold is cut into segments that each fit one
    /// chunk, staged invisibly, and installed as **one** version — one
    /// `Commit::Replace` CAS over N batches — so a reader never observes half an
    /// object and the previous version's chunks are released wholesale.
    ///
    /// An object **larger than [`max_object`](Self::max_object)** is not cut at
    /// all: it goes into a coordinator-granted segment of its own, holding one
    /// pool of one class of one chunk, and reads back as a single contiguous
    /// buffer. That split is not a tunable — it is taken exactly when the pool
    /// cannot hold the object, since below that pooled costs strictly less.
    ///
    /// Concurrent writers serialize on the entry's write lease; a lost race is
    /// retried, bounded. The version is assigned by the commit, so it is exact
    /// even under contention. Readers never block and are never disturbed: they
    /// keep reading whatever version they pinned.
    pub fn put(&self, key: &str, content_type: &str, bytes: &[u8]) -> Result<u64> {
        check_content_type(content_type)?;
        let (entry, kid) = self.entry_or_create(key)?;
        let version = self.install(&entry, content_type, bytes)?;
        // Descriptor-only: the watcher learns a version exists, not what is in
        // it. A lapped ring costs a wake-up, not data.
        let _ =
            self.side
                .events
                .publish_event(VersionEvent::new(kid, version, CommitKind::Replace));
        Ok(version)
    }

    /// Place and install `bytes` as one version, retrying a lost write lease a
    /// bounded number of times.
    ///
    /// Every failure path — a chunk that will not allocate, a fenced lease, a
    /// rejected install — unwinds the staging it had already done and installs
    /// nothing, so the pool census (and the live-segment census) returns to
    /// exactly where it was and the object keeps the version it had.
    fn install(&self, entry: &Entry, content_type: &str, bytes: &[u8]) -> Result<u64> {
        self.install_coordinated(entry, content_type, bytes, Coordination::Exclusive)
    }

    fn install_coordinated(
        &self,
        entry: &Entry,
        content_type: &str,
        bytes: &[u8],
        coordination: Coordination,
    ) -> Result<u64> {
        let optimistic = matches!(coordination, Coordination::Optimistic { .. });
        let shape = self.pooled_shape(content_type)?;
        let dedicated = bytes.len() as u64 > shape.ceiling;
        let mut tries = 0u32;
        loop {
            let attempt = if dedicated {
                self.install_dedicated(entry, content_type, bytes, coordination)
            } else {
                self.install_pooled(entry, content_type, bytes, shape.per_segment, coordination)
            };
            match attempt {
                Ok(v) => return Ok(v),
                Err(e) if is_contended(&e, optimistic) => {
                    if tries >= PUT_RETRIES {
                        return Err(Error::Contended(tries));
                    }
                    tries += 1;
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// One attempt at the **pooled** path: cut the payload into segments that
    /// each fit one chunk of the shared pool and install them as one version.
    ///
    /// The transaction is `shm-stream`'s: each batch is serialized into fresh
    /// chunks, loaned to this writer and recorded in its borrow journal *before*
    /// it counts as staged, and the whole set is installed by a single
    /// `Committer::commit_staged`.
    fn install_pooled(
        &self,
        entry: &Entry,
        content_type: &str,
        bytes: &[u8],
        segment_size: usize,
        coordination: Coordination,
    ) -> Result<u64> {
        // An empty payload is one empty segment, not zero segments: a version
        // with no batches would read back as a missing object.
        let parts: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[]]
        } else {
            bytes.chunks(segment_size).collect()
        };
        let journal = BorrowJournal::attach(self.store.journal_segment())?;
        let mut writer = StreamWriter::open(
            entry.artifact(),
            self.store.data_segment(),
            &journal,
            &self.registry,
            self.store.owner(),
            Commit::Replace,
            coordination,
        )?;
        // The version this commit supersedes may name a dedicated segment this
        // node has never mapped, and the retire cascade has to return those
        // chunks to *their own* pools. Done here rather than before the open so
        // that under `Exclusive` the lease is already held: no other writer can
        // slip a version naming a different segment in between.
        if let Err(e) = self.map_current_segments(entry) {
            writer.abort();
            return Err(e);
        }
        // One batch per segment, built as it is staged so only one segment is
        // ever duplicated in this process's heap.
        for part in &parts {
            let batch = encode_segment(&self.side.schema, content_type, part);
            if let Err(e) = writer.append_batch(&batch) {
                // Frees every chunk staged so far and releases their journal
                // slots; nothing was ever visible to a reader.
                writer.abort();
                return Err(e.into());
            }
        }
        Ok(writer.commit()?)
    }

    /// One attempt at the **dedicated** path: the whole payload as one batch in
    /// one chunk of a segment granted for it alone.
    ///
    /// The same split a process allocator makes at `M_MMAP_THRESHOLD`. The
    /// segment is contiguous by construction and cannot fragment the shared
    /// arena, because it is never carved from it.
    ///
    /// The segment is **staged** for the whole write: until the version is
    /// installed, the creator's `ENTRY_PAYLOAD_SEGMENT` journal entry is what
    /// reclaims it, so a `kill -9` mid-write leaks nothing. Once the commit
    /// lands, [`Node::install_payload_segment`] closes that window and the
    /// object's chunk governs the segment's life instead — which is why a later
    /// `put` releases this segment without anyone unlinking it explicitly: the
    /// retire cascade drives the chunk to `FREE` and the coordinator's sweep
    /// takes it from there.
    ///
    /// A failure of the install *after* a successful commit is reported, not
    /// swallowed: the version is durable either way (replay of the creator's
    /// journal entry finds the chunk held and un-stages the segment rather than
    /// unlinking it), but until the coordinator is told, the segment's life is
    /// still tied to this node's, and that is a degradation the caller should
    /// hear about rather than discover later.
    fn install_dedicated(
        &self,
        entry: &Entry,
        content_type: &str,
        bytes: &[u8],
        coordination: Coordination,
    ) -> Result<u64> {
        let batch = encode_segment(&self.side.schema, content_type, bytes);
        let need = shm_arrow::serialized_len(&batch).ok_or(Error::BadObject)?;
        // A chunk is addressed by a `u32` length, so one dedicated object tops
        // out at 4 GiB serialized. Refuse before asking for a segment.
        let chunk = u32::try_from(need).map_err(|_| Error::TooLarge(need))?;
        let cfg = one_chunk_pool(chunk);
        let journal = BorrowJournal::attach(self.store.journal_segment())?;
        let seg = self
            .node
            .create_payload_segment(required_bytes(&cfg) as u64)?;
        let seg_id = seg.id();
        match self.stage_and_commit(entry, &seg, &cfg, &batch, coordination, &journal) {
            Ok(version) => {
                // The version's manifest now holds the `+1` on the chunk, so the
                // object outlives this process. Telling the coordinator is what
                // stops replay from reclaiming a segment nothing is accountable
                // for any more.
                self.node.install_payload_segment(seg_id)?;
                Ok(version)
            }
            Err(e) => {
                // Still inside the staging window, so the explicit release is
                // legal and unlinks immediately — no waiting for a sweep.
                let _ = self.node.release_payload_segment(seg_id);
                Err(e)
            }
        }
    }

    /// Lay the one-class, one-chunk pool inside `seg`, write `batch` into its
    /// single chunk, and install it as the entry's next version.
    ///
    /// On any failure the chunk is returned to its own pool, so the segment is
    /// quiescent by the time the caller releases it.
    fn stage_and_commit(
        &self,
        entry: &Entry,
        seg: &Segment,
        cfg: &PoolConfig,
        batch: &RecordBatch,
        coordination: Coordination,
        journal: &BorrowJournal<'_>,
    ) -> Result<u64> {
        let owner = self.store.owner();
        let pool = Pool::create(seg, cfg)?;
        let chunks = {
            let alloc = PoolAllocator::new(&pool, seg);
            write_batch_chunks(&alloc, &self.registry, batch)?
        };
        // The pool was sized from this batch's own `serialized_len`, so a
        // continuation chunk means the sizing is wrong, not the payload.
        if chunks.len() != 1 {
            for d in &chunks {
                let _ = pool.free(d);
            }
            return Err(Error::TooLarge(
                shm_arrow::serialized_len(batch).unwrap_or(0),
            ));
        }
        let desc = chunks[0];
        pool.ctrl(&desc)?.try_loan(owner)?;

        let result = self.commit_one_chunk(entry, desc, coordination, journal);
        if result.is_err() {
            // A commit that got as far as staging already returned the chunk to
            // this pool; one that never took the lease, or that never got past
            // the segment probe, did not. Free only a chunk still loaned to us,
            // so neither case double-frees.
            if let Ok(ctrl) = pool.ctrl(&desc) {
                if ctrl.state() == LOANED && ctrl.drop_loan().is_ok() {
                    let _ = pool.free(&desc);
                }
            }
        }
        result
    }

    /// Install one already-loaned chunk as the entry's next version.
    ///
    /// The predecessor's dedicated segments are mapped **after** the write lease
    /// is taken, for the reason [`install_pooled`](Self::install_pooled) gives:
    /// under `Exclusive` no other writer can install a version naming a segment
    /// this node has not mapped between the probe and the commit. Under
    /// `Optimistic` the install CAS is the same guarantee — a version that
    /// landed after the probe moves `current`, so the commit conflicts and
    /// installs nothing.
    fn commit_one_chunk(
        &self,
        entry: &Entry,
        desc: ChunkDesc,
        coordination: Coordination,
        journal: &BorrowJournal<'_>,
    ) -> Result<u64> {
        let owner = self.store.owner();
        let staged = [desc];
        let spans = [1u32];
        let schema_id = desc.schema_id;
        let installed = match coordination {
            Coordination::Exclusive => {
                let mut committer = entry
                    .artifact()
                    .open_exclusive_journaled(owner, journal)
                    .map_err(|e| Error::Store(shm_store::Error::Artifact(e)))?;
                self.map_current_segments(entry)?;
                committer.commit_staged(Commit::Replace, &staged, &spans, schema_id)
            }
            Coordination::Optimistic { expect_version } => {
                self.map_current_segments(entry)?;
                entry.artifact().commit_staged_optimistic_journaled(
                    owner,
                    expect_version,
                    Commit::Replace,
                    &staged,
                    &spans,
                    schema_id,
                    journal,
                )
            }
        };
        installed.map_err(|e| Error::Store(shm_store::Error::Artifact(e)))
    }

    /// Map every dedicated segment the entry's **current** version names.
    ///
    /// Descriptors are addressed by offset alone, so `shm-artifact` refuses one
    /// naming an unmapped segment rather than resolving it against the store's
    /// — which is what keeps a foreign descriptor from aliasing a real,
    /// unrelated control block. The consequence is that a writer superseding a
    /// large object, and an evictor tearing one down, need the mapping just as
    /// much as a reader does: without it the retire fails closed and the chunk
    /// stays published forever.
    ///
    /// The pin is the probe: it is the operation that resolves a version's
    /// segments, and its refusal names the one that is missing. Each miss maps
    /// exactly one segment, so the loop is bounded by how many the version
    /// names.
    fn map_current_segments(&self, entry: &Entry) -> Result<()> {
        if entry.current_version() == 0 {
            return Ok(());
        }
        let mut last = 0u32;
        for _ in 0..FAULT_IN_LIMIT {
            match entry.pin() {
                // Dropped immediately: all this wanted was the resolution.
                Ok(_pin) => return Ok(()),
                Err(e) => match unmapped_segment(&Error::from(e)) {
                    Some(seg_id) => {
                        last = seg_id;
                        self.node.map_payload_segment(seg_id)?;
                    }
                    // Not a mapping problem (nothing committed yet, a stale
                    // handle): leave it to the operation itself to report.
                    None => return Ok(()),
                },
            }
        }
        Err(Error::Unmapped(last))
    }

    /// Replace `key` only if its current version is still `expect`.
    ///
    /// The compare-and-set a read-modify-write needs. Without it, two writers
    /// that each read a value, change it and write it back silently lose one of
    /// the two updates — which is exactly how a shared index loses an entry.
    ///
    /// `expect` of 0 means "only if this key holds no version yet", so a
    /// create can be made to fail rather than clobber.
    ///
    /// Returns [`Error::VersionMismatch`] when it lost: the caller re-reads and
    /// retries. Nothing is installed and no chunk is left staged.
    pub fn put_if(&self, key: &str, content_type: &str, bytes: &[u8], expect: u64) -> Result<u64> {
        check_content_type(content_type)?;
        let (entry, kid) = self.entry_or_create(key)?;
        let version = match self.install_coordinated(
            &entry,
            content_type,
            bytes,
            Coordination::Optimistic {
                expect_version: expect,
            },
        ) {
            Ok(v) => v,
            Err(Error::Stream(shm_stream::Error::Artifact(shm_artifact::Error::Conflict {
                ..
            })))
            | Err(Error::Store(shm_store::Error::Artifact(shm_artifact::Error::Conflict {
                ..
            }))) => {
                return Err(Error::VersionMismatch {
                    requested: expect,
                    current: entry.current_version(),
                })
            }
            Err(e) => return Err(e),
        };
        let _ =
            self.side
                .events
                .publish_event(VersionEvent::new(kid, version, CommitKind::Replace));
        Ok(version)
    }

    /// Write an object by **streaming into it**, never materialising the whole
    /// payload.
    ///
    /// `f` is handed a sink that implements [`std::io::Write`], so any encoder
    /// writes straight at it — an Arrow IPC `StreamWriter`, a parquet writer,
    /// a serialiser. Bytes are cut into segments and staged as they arrive, and
    /// the whole set installs as one version when `f` returns.
    ///
    /// This is what a producer should use instead of building an answer in its
    /// own buffer and calling [`put`](Self::put): a producer streaming a
    /// gigabyte holds one segment at a time, not a gigabyte. Nothing is visible
    /// to a reader until the commit, and a failure part-way stages nothing.
    ///
    /// It is not yet *zero*-copy: a segment is still copied from the sink's
    /// buffer into its chunk, because Arrow builds the batch's buffers itself.
    /// Removing that last copy needs a batch constructed **over** chunk memory
    /// (`Buffer::from_custom_allocation`), which is a change in `shm-arrow`, not
    /// here. What this removes is the copy of the *whole payload*, which is the
    /// one that scales.
    pub fn write_with<F>(&self, key: &str, content_type: &str, f: F) -> Result<u64>
    where
        F: FnOnce(&mut SegmentSink<'_, '_>) -> std::io::Result<()>,
    {
        check_content_type(content_type)?;
        let (entry, kid) = self.entry_or_create(key)?;
        let seg = self.segment_size(content_type)?;
        let journal = BorrowJournal::attach(self.store.journal_segment())?;
        let mut writer = StreamWriter::open(
            entry.artifact(),
            self.store.data_segment(),
            &journal,
            &self.registry,
            self.store.owner(),
            Commit::Replace,
            Coordination::Exclusive,
        )?;
        // Under the lease, as in `install_pooled`: the version this stream will
        // supersede may live in a dedicated segment this node has never mapped,
        // and the retire cascade has to reach its pool.
        if let Err(e) = self.map_current_segments(&entry) {
            writer.abort();
            return Err(e);
        }

        let mut sink = SegmentSink {
            writer: &mut writer,
            schema: &self.side.schema,
            content_type,
            seg,
            buf: Vec::with_capacity(seg),
            batches: 0,
            failed: None,
        };
        let outcome = f(&mut sink).and_then(|()| sink.finish());
        if let Err(e) = outcome {
            writer.abort();
            return Err(Error::Sink(e));
        }
        let version = writer.commit()?;
        let _ =
            self.side
                .events
                .publish_event(VersionEvent::new(kid, version, CommitKind::Replace));
        Ok(version)
    }

    /// Read `key` at `want`, returning the pinned payload.
    ///
    /// [`Version::Latest`] pins whatever is current. [`Version::IfCurrent`] pins the
    /// current version and *checks* it — there is no seek — so a version already
    /// superseded reports [`Error::VersionMismatch`] naming the current one.
    pub fn get(&self, key: &str, want: Version) -> Result<ObjectRead> {
        let (entry, _) = self.entry_open(key)?.ok_or(Error::NotFound)?;
        if entry.current_version() == 0 {
            return Err(Error::NotFound);
        }
        // Every batch of the pinned version, in commit order. A `Replace` root
        // has no predecessor, so this walks exactly one manifest: the object's
        // own segments and nothing else.
        //
        // A version whose payload is in a dedicated segment can only be pinned
        // once this node has mapped it, and the pin is what says which. The
        // order is the safe one: the version pin is taken first, so the
        // manifest's reference already holds the chunk against `FREE` and the
        // coordinator's sweep cannot unlink the segment underneath the mapping.
        let (pin, delta) = self.read_current(&entry)?;
        let version = pin.version();
        if let Version::IfCurrent(requested) = want {
            if requested != version {
                return Err(Error::VersionMismatch {
                    requested,
                    current: version,
                });
            }
        }
        if delta.batches.is_empty() {
            return Err(Error::NotFound);
        }
        let mut data = Vec::with_capacity(delta.batches.len());
        let mut content_type = None;
        for b in &delta.batches {
            let (ct, d) = split_segment(b)?;
            content_type.get_or_insert(ct);
            data.push(d);
        }
        Ok(ObjectRead {
            pin,
            content_type: content_type.expect("a non-empty version has a first segment"),
            data,
            version,
        })
    }

    /// Pin the entry's current version and read its batches, mapping any
    /// dedicated segment the version names on the way.
    ///
    /// A pin that names a segment this node has not mapped is *refused* — never
    /// resolved against the store segment — so the refusal itself is the lookup:
    /// it says which segment to ask the coordinator for. The speculative pin is
    /// undone by the refusal, so retrying is not a leak.
    fn read_current(&self, entry: &Entry) -> Result<(VersionPin, shm_artifact::Delta)> {
        let mut last = 0u32;
        for _ in 0..FAULT_IN_LIMIT {
            match entry.read_since(0) {
                Ok(read) => return Ok(read),
                Err(e) => {
                    let err = Error::from(e);
                    match unmapped_segment(&err) {
                        Some(seg_id) => {
                            last = seg_id;
                            self.node.map_payload_segment(seg_id)?;
                        }
                        None => return Err(err),
                    }
                }
            }
        }
        Err(Error::Unmapped(last))
    }

    /// Delete `key`: tombstone its catalog slot and tear down every version.
    /// Idempotent — deleting an absent object is a clean no-op.
    ///
    /// Readers holding an [`ObjectRead`] keep their bytes until they drop; the
    /// slot is reclaimed then, by the coordinator's sweep.
    ///
    /// **The catalog slot is spent while the entry is not quiescent** and
    /// `store_capacity` bounds how many objects a node can have live at once, so
    /// deleting is an operator-scale action, not a per-request one.
    pub fn delete(&self, key: &str) -> Result<()> {
        check_object_key(key)?;
        // The teardown must return the current version's chunks to their own
        // pools, so map any dedicated segment it names before tombstoning.
        // Without this the retire fails closed and the chunk — and so the whole
        // segment — is never reclaimed.
        if let Some((entry, _)) = self.entry_open(key)? {
            self.map_current_segments(&entry)?;
        }
        self.entries
            .lock()
            .expect("entry cache poisoned")
            .remove(key);
        match self.store.evict(&object_key(key)) {
            Ok(()) | Err(shm_store::Error::NotFound) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// How the shared pool would hold this object: which size class its segments
/// are cut from, how much payload one segment carries, and the largest object
/// the pool can hold that way.
#[derive(Clone, Copy)]
struct PooledShape {
    /// The size class segments are cut from.
    class: u32,
    /// The `data` bytes one segment carries.
    per_segment: usize,
    /// The largest payload the pool can hold — the dedicated threshold.
    ceiling: u64,
}

/// The pool a dedicated segment carries: one class, one chunk, sized to the
/// object it was granted for.
///
/// A real [`Pool`] rather than a bespoke "the segment *is* the chunk" header,
/// deliberately. The chunk control word is what a version's manifest holds its
/// `+1` on, what the retire cascade drives to `FREE`, and what the coordinator
/// reads to decide the segment may be unlinked. Reusing it means the size split
/// changes *where* a chunk lives and nothing else — the state machine, the
/// generations and the journal kinds are all unchanged.
fn one_chunk_pool(chunk_size: u32) -> PoolConfig {
    PoolConfig {
        classes: vec![SizeClass {
            chunk_size,
            chunk_count: 1,
        }],
    }
}

/// The dedicated segment an error says this node has not mapped, if that is
/// what the error says.
///
/// `WrongSegment` is `shm-artifact`'s refusal to resolve a descriptor naming a
/// segment it has no mapping for — a hard failure by design, since chunk
/// addressing is by offset alone and answering with the store segment would
/// find a real, unrelated control block. It names the segment, which is exactly
/// what a caller needs to go and map it.
fn unmapped_segment(e: &Error) -> Option<u32> {
    let artifact = match e {
        Error::Store(shm_store::Error::Artifact(a)) => a,
        Error::Stream(shm_stream::Error::Artifact(a)) => a,
        Error::Core(shm_core::Error::WrongSegment { desc, .. }) => return Some(*desc),
        _ => return None,
    };
    match artifact {
        shm_artifact::Error::Core(shm_core::Error::WrongSegment { desc, .. }) => Some(*desc),
        _ => None,
    }
}

/// Is this failure worth retrying?
///
/// A lost **write lease** is: another writer holds it and will let go. A lost
/// **install CAS** is only contention when nobody asked for a precondition —
/// under [`Coordination::Optimistic`] a `Conflict` IS the answer, and retrying
/// it would turn a compare-and-set into a loop that eventually overwrites the
/// value the caller was guarding against.
fn is_contended(e: &Error, optimistic: bool) -> bool {
    let artifact = match e {
        Error::Stream(shm_stream::Error::Artifact(a)) => a,
        Error::Store(shm_store::Error::Artifact(a)) => a,
        _ => return false,
    };
    match artifact {
        shm_artifact::Error::WriteLocked => true,
        shm_artifact::Error::Conflict { .. } => !optimistic,
        _ => false,
    }
}

/// Where [`ObjectStore::write_with`]'s producer writes.
///
/// Holds at most one segment. Each time it fills, that segment is staged as a
/// batch and the buffer is reused, so a producer's memory is bounded by the
/// segment size however large the object becomes.
pub struct SegmentSink<'w, 'a> {
    writer: &'w mut StreamWriter<'a>,
    schema: &'w SchemaRef,
    content_type: &'w str,
    seg: usize,
    buf: Vec<u8>,
    batches: usize,
    /// The staging error that stopped this sink, kept so the caller sees the
    /// real cause rather than the `io::Error` it was wrapped in to satisfy
    /// `Write`.
    failed: Option<String>,
}

impl SegmentSink<'_, '_> {
    fn stage(&mut self) -> std::io::Result<()> {
        let batch = encode_segment(self.schema, self.content_type, &self.buf);
        self.writer.append_batch(&batch).map_err(|e| {
            self.failed = Some(e.to_string());
            std::io::Error::other(e.to_string())
        })?;
        self.batches += 1;
        self.buf.clear();
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        // An empty payload is one empty segment, not zero: a version with no
        // batches reads back as a missing object.
        if !self.buf.is_empty() || self.batches == 0 {
            self.stage()?;
        }
        Ok(())
    }

    /// Why staging failed, when it did.
    pub fn failure(&self) -> Option<&str> {
        self.failed.as_deref()
    }
}

impl std::io::Write for SegmentSink<'_, '_> {
    fn write(&mut self, mut data: &[u8]) -> std::io::Result<usize> {
        let n = data.len();
        while !data.is_empty() {
            let room = self.seg - self.buf.len();
            let take = room.min(data.len());
            self.buf.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.buf.len() == self.seg {
                self.stage()?;
            }
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Deliberately not a stage: `flush` on a partial segment would cut the
        // object at whatever boundary the encoder happened to flush at, and a
        // segment boundary is this crate's business, not the encoder's.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shm_runtime::{Coordinator, RuntimeConfig};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    /// Test-only: the payload as one buffer. Named for what it costs —
    /// production code walks `segments()` and never pays this.
    fn joined(r: &ObjectRead) -> Vec<u8> {
        r.segments().flat_map(|s| s.to_vec()).collect()
    }

    static SEG: AtomicU32 = AtomicU32::new(0);

    fn seg_base() -> u32 {
        let pid = std::process::id();
        let n = SEG.fetch_add(1, Ordering::Relaxed);
        3_000_000 + ((pid % 1000) * 100) + n * 20_000
    }

    fn config() -> RuntimeConfig {
        let mut c = RuntimeConfig::with_seg_base(seg_base());
        c.store_pool = shm_core::PoolConfig::power_of_two(256, 8192, 256);
        c.store_data_size = 4 << 20;
        c
    }

    fn connect(uds: &std::path::Path, name: &str) -> Node {
        let mut n = Node::connect(uds, name, Arc::new(SchemaRegistry::new())).expect("connect");
        n.start_heartbeat(Duration::from_millis(150));
        n
    }

    fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        cond()
    }

    #[test]
    fn keys_and_content_types_are_checked() {
        assert!(check_object_key("reports/2026/q1.parquet").is_ok());
        assert!(check_object_key("").is_err());
        assert!(check_object_key("a b").is_err());
        assert!(check_object_key(&"x".repeat(MAX_OBJECT_KEY + 1)).is_err());
        assert!(check_content_type("").is_ok());
        assert!(check_content_type("application/vnd.apache.arrow.file").is_ok());
        assert!(check_content_type("text/\u{1}plain").is_err());
        assert!(check_content_type(&"m".repeat(MAX_CONTENT_TYPE + 1)).is_err());
        assert_eq!(object_key("a/b"), b"object/a/b".to_vec());
    }

    #[test]
    fn object_round_trip() {
        let s = object_schema();
        let b = encode_segment(&s, "application/octet-stream", b"payload");
        let (ct, data) = split_segment(&b).unwrap();
        assert_eq!(ct.value(0), "application/octet-stream");
        assert_eq!(data.value(0), b"payload");
        // A batch of the wrong shape is rejected, never misread.
        let wrong = holon_topic::encode_message(&holon_topic::message_schema(), 1, 2, "", "", b"x");
        assert!(matches!(split_segment(&wrong), Err(Error::BadObject)));
    }

    #[test]
    fn ensure_creates_an_empty_entry_and_is_idempotent() {
        // The property a fixed key ring depends on: a process can pay every key's interning round
        // trip up front, once, and the entry is then a catalog hit forever after.
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();
        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        assert!(objects.head("ring/000").unwrap().is_none());
        let kid = objects.ensure("ring/000").unwrap();
        // The entry exists and holds no version — `Some(0)`, not `None` and not `Some(1)`.
        assert_eq!(objects.head("ring/000").unwrap(), Some(0));
        assert_eq!(objects.key_id("ring/000").unwrap(), Some(kid));
        assert!(matches!(
            objects.get("ring/000", Version::Latest),
            Err(Error::NotFound)
        ));
        // Idempotent, and it does not spend a second catalog slot: the same key id comes back.
        assert_eq!(objects.ensure("ring/000").unwrap(), kid);
        assert_ne!(objects.ensure("ring/001").unwrap(), kid);
        // And a warmed key still takes version 1 on its first write.
        assert_eq!(objects.put("ring/000", "text/plain", b"v1").unwrap(), 1);
        assert!(objects.ensure("").is_err(), "a bad key is still refused");
    }

    #[test]
    fn put_get_head_delete_and_notification() {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();

        // A watcher on the shared notification ring, in a second node.
        let mut watcher_node = connect(&uds, "watcher");
        let mut sub = watcher_node.subscribe(TOPIC_EVENTS).unwrap();

        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        // A read of a key that was never written creates nothing.
        assert!(objects.head("missing").unwrap().is_none());
        assert!(objects.key_id("missing").unwrap().is_none());
        assert!(matches!(
            objects.get("missing", Version::Latest),
            Err(Error::NotFound)
        ));

        assert_eq!(objects.put("a/b", "text/plain", b"v1").unwrap(), 1);
        assert_eq!(objects.head("a/b").unwrap(), Some(1));
        let r = objects.get("a/b", Version::Latest).unwrap();
        assert_eq!(
            (r.version(), r.content_type(), joined(&r).as_slice()),
            (1, "text/plain", &b"v1"[..])
        );
        assert_eq!((r.len(), r.segment_count()), (2, 1));
        assert!(!r.is_empty());
        drop(r);

        assert_eq!(objects.put("a/b", "text/plain", b"version two").unwrap(), 2);
        let r = objects.get("a/b", Version::IfCurrent(2)).unwrap();
        assert_eq!(joined(&r).as_slice(), &b"version two"[..]);
        drop(r);
        // Exact is an assert-match against the current version, not a seek.
        assert!(matches!(
            objects.get("a/b", Version::IfCurrent(1)),
            Err(Error::VersionMismatch {
                requested: 1,
                current: 2
            })
        ));

        // A second key is independent, and carries a distinct key id.
        assert_eq!(objects.put("c", "", b"").unwrap(), 1);
        let r = objects.get("c", Version::Latest).unwrap();
        assert!(r.is_empty() && r.content_type().is_empty());
        // Empty is one empty segment, never zero.
        assert_eq!(r.segment_count(), 1);
        drop(r);
        assert_eq!(objects.head("a/b").unwrap(), Some(2));
        let kid_ab = objects.key_id("a/b").unwrap().unwrap();
        let kid_c = objects.key_id("c").unwrap().unwrap();
        assert_ne!(kid_ab, kid_c);

        // Three descriptor-only notifications, in publish order.
        let expect = [(kid_ab, 1u64), (kid_ab, 2), (kid_c, 1)];
        for want in expect {
            let ev = loop {
                match sub.recv() {
                    shm_ring::Msg::Sample(d) => {
                        if let Some(ev) = VersionEvent::from_desc(&d) {
                            break ev;
                        }
                    }
                    shm_ring::Msg::Lagged(_) => {}
                }
            };
            assert_eq!((ev.name_id, ev.version), want);
            assert_eq!(ev.kind, CommitKind::Replace.as_u32());
        }

        // Bad input never reaches the store.
        assert!(matches!(
            objects.put("", "text/plain", b"x"),
            Err(Error::BadKey(_))
        ));
        assert!(matches!(
            objects.put("k", "text/\u{1}plain", b"x"),
            Err(Error::BadContentType(_))
        ));

        // Delete is idempotent and takes the key out of the namespace.
        objects.delete("a/b").unwrap();
        objects.delete("a/b").unwrap();
        assert!(objects.head("a/b").unwrap().is_none());
        assert!(matches!(
            objects.get("a/b", Version::Latest),
            Err(Error::NotFound)
        ));
        // ...and the key can be written again from version 1 (a fresh slot).
        assert_eq!(objects.put("a/b", "text/plain", b"reborn").unwrap(), 1);
    }

    /// A held read keeps reading its version while later versions are committed
    /// over it — the RCU guarantee, from the object API's point of view.
    #[test]
    fn a_pinned_version_survives_later_commits() {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        assert_eq!(objects.put("blob", "text/plain", b"first").unwrap(), 1);
        let held = objects.get("blob", Version::Latest).unwrap();
        assert_eq!(
            (held.version(), joined(&held).as_slice()),
            (1, &b"first"[..])
        );
        assert_eq!(
            coord.store_entry_pins(&object_key("blob"), 1),
            Some(1),
            "the read holds a journaled pin on v1"
        );

        // Commit over it, repeatedly, with differently-sized payloads.
        for (i, body) in [&b"second"[..], &b"third, longer"[..], &b"4"[..]]
            .into_iter()
            .enumerate()
        {
            let v = objects
                .put("blob", "application/octet-stream", body)
                .unwrap();
            assert_eq!(v as usize, i + 2);
            // The held read is untouched: same version, same bytes, same type.
            assert_eq!(held.version(), 1);
            assert_eq!(joined(&held).as_slice(), &b"first"[..]);
            assert_eq!(held.content_type(), "text/plain");
        }
        assert_eq!(objects.head("blob").unwrap(), Some(4));

        // A fresh read sees the new version while the old one is still pinned.
        let latest = objects.get("blob", Version::Latest).unwrap();
        assert_eq!(
            (latest.version(), joined(&latest).as_slice()),
            (4, &b"4"[..])
        );
        assert_eq!(
            joined(&held).as_slice(),
            &b"first"[..],
            "still v1 after a concurrent read"
        );

        // v1 is retired only when its last reader lets go.
        drop(held);
        assert!(
            wait_until(Duration::from_secs(5), || {
                coord.store_entry_pins(&object_key("blob"), 1) != Some(1)
            }),
            "v1's pin was released on drop"
        );
        drop(latest);
    }

    /// The same guarantee under a genuinely concurrent writer *in another
    /// node*: a reader holding version 1 keeps reading version 1's bytes while
    /// a second process-shaped client commits over it as fast as it can.
    ///
    /// (`KeyedStore` borrows a `&dyn KeyResolver` and so is not `Sync`; the
    /// writer therefore brings its own node, which is the real cross-process
    /// shape anyway.)
    #[test]
    fn a_pinned_version_survives_a_concurrent_writer() {
        const ROUNDS: u64 = 200;

        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "reader");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        assert_eq!(objects.put("blob", "text/plain", b"first").unwrap(), 1);
        let held = objects.get("blob", Version::Latest).unwrap();
        assert_eq!(
            (held.version(), joined(&held).as_slice()),
            (1, &b"first"[..])
        );

        let uds2 = uds.clone();
        let writer = std::thread::spawn(move || {
            let mut node = connect(&uds2, "writer");
            let side = ObjectSide::open(&mut node).unwrap();
            let registry = node.registry().clone();
            node.open_store().unwrap();
            let store = node.opened_store().unwrap();
            let objects = ObjectStore::new(&store, &side, registry, &node);
            for v in 2..=ROUNDS + 1 {
                let body = vec![b'y'; (v as usize % 97) + 1];
                assert_eq!(
                    objects
                        .put("blob", "application/octet-stream", &body)
                        .unwrap(),
                    v
                );
            }
        });

        // Read the pinned bytes the whole time the versions churn underneath.
        while !writer.is_finished() {
            assert_eq!(held.version(), 1);
            assert_eq!(joined(&held).as_slice(), &b"first"[..]);
            assert_eq!(held.content_type(), "text/plain");
        }
        writer.join().expect("writer thread");

        assert_eq!(
            joined(&held).as_slice(),
            &b"first"[..],
            "v1 outlived every commit over it"
        );
        assert_eq!(objects.head("blob").unwrap(), Some(ROUNDS + 1));
        let latest = objects.get("blob", Version::Latest).unwrap();
        assert_eq!(latest.version(), ROUNDS + 1);
        drop(latest);
        drop(held);
    }

    /// The property the segmented commit exists for: one batch per segment, all
    /// N installed as one version, so an object many times the pool's largest
    /// size class round-trips byte-for-byte — and the next, smaller version
    /// releases every one of its segments.
    #[test]
    fn a_payload_far_larger_than_one_chunk_round_trips() {
        let cfg = config();
        let max = cfg.store_pool.max_chunk_size() as usize;

        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, cfg).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        // 150 times the largest size class, and deliberately not a multiple of
        // the segment size: the last segment is short, which is where an
        // off-by-one would hide.
        let n = max * 150 + 12345;
        let big: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        assert_eq!(
            objects
                .put("huge", "application/octet-stream", &big)
                .unwrap(),
            1
        );

        let r = objects.get("huge", Version::Latest).unwrap();
        assert_eq!(r.len(), n, "length changed across the round trip");
        assert!(
            r.segment_count() > 1,
            "expected several segments for {n} bytes, got {}",
            r.segment_count()
        );
        assert!(joined(&r) == big, "payload differs across the round trip");
        assert_eq!(r.content_type(), "application/octet-stream");
        drop(r);

        // A second, smaller put releases the first one's segments rather than
        // leaving them to be read as part of the new object.
        let small = vec![7u8; 10];
        assert_eq!(objects.put("huge", "text/plain", &small).unwrap(), 2);
        let r2 = objects.get("huge", Version::Latest).unwrap();
        assert_eq!(
            (r2.len(), r2.segment_count()),
            (10, 1),
            "the previous object's segments leaked into the new version"
        );
        assert_eq!(joined(&r2), small);
        assert_eq!(r2.content_type(), "text/plain");
        drop(r2);

        // And the first big version's chunks really went back to the pool. The
        // pool holds `count_per_class` (256) chunks of the largest class and one
        // of these payloads needs 150-odd of them, so a *second* big version can
        // only be staged if the first one's chunks were released by the small
        // commit that superseded it.
        assert_eq!(
            objects
                .put("huge", "application/octet-stream", &big)
                .unwrap(),
            3
        );
        let r3 = objects.get("huge", Version::Latest).unwrap();
        assert!(joined(&r3) == big);
    }

    /// The advertised ceiling is real: a payload of exactly `max_object()`
    /// bytes round-trips, and it is far more than one chunk.
    #[test]
    fn an_object_of_exactly_the_advertised_ceiling_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        let cap = objects.max_object().unwrap();
        let seg = objects.max_payload().unwrap() as u64;
        assert!(
            cap > seg * 100,
            "the object ceiling ({cap}) should be many segments ({seg}), not one"
        );

        // Exactly the advertised number, to the byte.
        let big: Vec<u8> = (0..cap as usize).map(|i| (i % 251) as u8).collect();
        let v = objects
            .put("ceiling", "text/plain", &big)
            .expect("a payload of exactly max_object() must fit");
        let r = objects.get("ceiling", Version::Latest).unwrap();
        assert_eq!((r.version(), r.len()), (v, cap as usize));
        assert!(joined(&r) == big, "payload differs across the round trip");
        assert!(r.segment_count() > 100, "{}", r.segment_count());
    }

    /// A `put` that fails partway through staging installs nothing and leaks
    /// nothing: the pool census is exactly where it was before the call.
    #[test]
    fn a_failed_put_stages_nothing_and_leaves_the_census_at_baseline() {
        let cfg = config();

        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, cfg).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        // A live object beside it, so the failing put has to unwind back to a
        // non-trivial census rather than to an empty pool — and one big enough
        // to hold 64 chunks of the very size class the doomed put cuts from,
        // which is what makes the pool run out partway through it.
        //
        // The trigger is exhaustion of the *shared pool*, not sheer size: a
        // payload larger than `max_object` no longer fails at all, it gets a
        // dedicated segment. So the doomed put asks for exactly the pooled
        // ceiling — the largest thing that still has to come out of the pool —
        // against a pool that no longer has room for it.
        let ct = "application/octet-stream";
        let seg = objects.segment_size(ct).unwrap();
        objects.put("neighbour", ct, &vec![7u8; seg * 64]).unwrap();
        objects.ensure("doomed").unwrap();
        let before = coord.store_data_free_total().expect("census");

        let huge = vec![9u8; objects.max_object().unwrap() as usize];
        let err = objects
            .put("doomed", ct, &huge)
            .expect_err("the pooled ceiling cannot be staged beside a 64-chunk neighbour");
        assert!(
            matches!(err, Error::Stream(_)),
            "expected a staging failure, got {err:?}"
        );

        assert_eq!(
            coord.store_data_free_total(),
            Some(before),
            "a failed put returned every staged chunk to the pool"
        );
        // Nothing was installed, and the neighbour is untouched.
        assert_eq!(objects.head("doomed").unwrap(), Some(0));
        assert!(matches!(
            objects.get("doomed", Version::Latest),
            Err(Error::NotFound)
        ));
        let n = objects.get("neighbour", Version::Latest).unwrap();
        assert_eq!(joined(&n), vec![7u8; seg * 64]);
        drop(n);
        // And the entry still works: the next, sane put takes version 1.
        assert_eq!(objects.put("doomed", "text/plain", b"ok").unwrap(), 1);
    }

    /// Put, read, delete — and the store's data pool is exactly where it
    /// started, with every catalog slot back to FREE.
    #[test]
    fn census_returns_to_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();
        let baseline = coord.store_data_free_total().expect("baseline");

        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        let keys = ["one", "two/deep", "three.bin"];
        for key in keys {
            for v in 1..=4u64 {
                // The last version spans several segments, so the baseline
                // covers a multi-batch version's chunks too.
                let body = vec![b'x'; 64 * v as usize * v as usize * 400];
                assert_eq!(
                    objects.put(key, "application/octet-stream", &body).unwrap(),
                    v
                );
            }
            let r = objects.get(key, Version::Latest).unwrap();
            assert_eq!(r.len(), 64 * 16 * 400);
            assert!(r.segment_count() > 1, "the last version is multi-segment");
            drop(r);
        }
        let held = coord.store_data_free_total().expect("census");
        assert!(held < baseline, "live objects hold chunks");

        for key in keys {
            objects.delete(key).unwrap();
        }
        assert!(
            wait_until(Duration::from_secs(5), || {
                coord.sweep_store_tombstones();
                coord.store_data_free_total() == Some(baseline)
            }),
            "after delete the data pool is back to baseline: {:?} vs {}",
            coord.store_data_free_total(),
            baseline
        );
        assert_eq!(
            coord.store_data_free_total().unwrap(),
            baseline,
            "delete + reclaim returns the data pool to baseline"
        );
        assert!(
            coord
                .store_slot_states()
                .iter()
                .all(|&s| s == shm_store::SLOT_FREE),
            "every catalog slot returned to FREE: {:?}",
            coord.store_slot_states()
        );
    }

    #[test]
    fn a_streamed_write_never_holds_the_whole_payload() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();
        let baseline = coord.store_data_free_total().unwrap();

        let mut node = connect(&uds, "w");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        // Written in many small pushes, as an encoder would: the sink never sees
        // the payload as one buffer and neither does the producer.
        let chunk = vec![b'q'; 1_000];
        let pushes = 300;
        let v = objects
            .write_with("streamed", "application/octet-stream", |sink| {
                for _ in 0..pushes {
                    sink.write_all(&chunk)?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(v, 1);

        let read = objects.get("streamed", Version::Latest).unwrap();
        assert_eq!(read.len(), chunk.len() * pushes);
        assert!(
            read.segment_count() > 1,
            "expected the payload to span segments, got {}",
            read.segment_count()
        );
        assert!(
            read.segments().flat_map(|s| s.iter()).all(|b| *b == b'q'),
            "streamed bytes differ"
        );
        drop(read);

        // A producer that fails part-way installs nothing and leaks nothing.
        let err = objects
            .write_with("aborted", "application/octet-stream", |sink| {
                sink.write_all(&chunk)?;
                Err(std::io::Error::other("producer gave up"))
            })
            .unwrap_err();
        assert!(matches!(err, Error::Sink(_)), "{err:?}");
        assert_eq!(objects.head("aborted").unwrap(), Some(0));

        objects.delete("streamed").unwrap();
        objects.delete("aborted").unwrap();
        let _ = node.say_bye();
        drop(node);
        assert!(
            wait_until(Duration::from_secs(5), || {
                coord.sweep_store_tombstones();
                coord.store_data_free_total() == Some(baseline)
            }),
            "a streamed write leaked: {:?} vs {baseline}",
            coord.store_data_free_total()
        );
    }

    #[test]
    fn the_segment_class_is_chosen_by_reach_not_by_size() {
        // A pool shaped like RAE's: a few very large chunks, more medium ones.
        // Cutting at the largest class would give 3 segments (one chunk pays for
        // the manifest); the 4 MiB class gives 7. The bigger object comes from
        // the smaller chunk, which is the whole point of choosing by reach.
        let mut cfg = config();
        cfg.store_pool = shm_core::PoolConfig {
            classes: vec![
                shm_core::SizeClass {
                    chunk_size: 256,
                    chunk_count: 64,
                },
                shm_core::SizeClass {
                    chunk_size: 4 << 20,
                    chunk_count: 8,
                },
                shm_core::SizeClass {
                    chunk_size: 8 << 20,
                    chunk_count: 4,
                },
            ],
        };
        cfg.store_data_size = 96 << 20;

        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, cfg).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "w");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        let ceiling = objects.max_object().unwrap();
        let largest_class_reach = 3 * (8u64 << 20);
        assert!(
            ceiling > largest_class_reach,
            "cutting at the largest class would give {largest_class_reach}; \
             choosing by reach gave {ceiling}"
        );

        // And the ceiling is real, not advertised: write one segment past what
        // the largest class could have held.
        let n = (largest_class_reach + (1 << 20)) as usize;
        let big: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        objects
            .put("wide", "application/octet-stream", &big)
            .unwrap();
        let read = objects.get("wide", Version::Latest).unwrap();
        assert_eq!(read.len(), n);
        assert!(
            read.segments().flat_map(|s| s.iter()).eq(big.iter()),
            "payload differs"
        );
    }

    #[test]
    fn put_if_is_a_compare_and_set_so_a_read_modify_write_cannot_lose_an_update() {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "w");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry, &node);

        // Create-if-absent: version 0 means "nothing committed yet".
        assert_eq!(objects.put_if("cas", "text/plain", b"one", 0).unwrap(), 1);
        // The same create again must lose rather than clobber.
        assert!(matches!(
            objects.put_if("cas", "text/plain", b"other", 0),
            Err(Error::VersionMismatch { .. })
        ));
        assert_eq!(
            joined(&objects.get("cas", Version::Latest).unwrap()).as_slice(),
            &b"one"[..]
        );

        // The interleaving that loses an update without a precondition: two
        // writers each read v1, each write back. The second must be refused.
        let seen = objects.get("cas", Version::Latest).unwrap().version();
        assert_eq!(
            objects.put_if("cas", "text/plain", b"two", seen).unwrap(),
            2
        );
        assert!(
            matches!(
                objects.put_if("cas", "text/plain", b"three", seen),
                Err(Error::VersionMismatch { requested, current })
                    if requested == seen && current == 2
            ),
            "a stale writer overwrote a newer version"
        );
        assert_eq!(
            joined(&objects.get("cas", Version::Latest).unwrap()).as_slice(),
            &b"two"[..]
        );

        // A refused write installs nothing and stages nothing.
        assert_eq!(objects.head("cas").unwrap(), Some(2));
    }

    /// Where a large `put` actually spends its time.
    ///
    /// Run with `cargo test -p holon-object --release -- --ignored --nocapture
    /// where_a_put_spends`. Ignored because it is a measurement, not an
    /// assertion: it exists to decide whether removing the last copy is worth
    /// invasive unsafe work, and the repo's own rule is to measure first.
    #[test]
    #[ignore]
    fn where_a_put_spends_its_time() {
        use std::time::Instant;

        let mut cfg = config();
        cfg.store_pool = shm_core::PoolConfig {
            classes: vec![
                shm_core::SizeClass {
                    chunk_size: 256,
                    chunk_count: 256,
                },
                shm_core::SizeClass {
                    chunk_size: 4 << 20,
                    chunk_count: 16,
                },
            ],
        };
        cfg.store_data_size = 192 << 20;

        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, cfg).unwrap();
        coord.start().unwrap();
        let mut node = connect(&uds, "probe");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        node.open_store().unwrap();
        let store = node.opened_store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry.clone(), &node);

        let ct = "application/octet-stream";
        let seg = objects.segment_size(ct).unwrap();
        const N: usize = 16 << 20;
        let payload: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        let parts: Vec<&[u8]> = payload.chunks(seg).collect();
        let reps = 5;

        // A raw memcpy of the same bytes: the floor any copy is measured against.
        let mut sink = vec![0u8; N];
        let mut memcpy = u128::MAX;
        for _ in 0..reps {
            let t = Instant::now();
            sink.copy_from_slice(&payload);
            memcpy = memcpy.min(t.elapsed().as_micros());
        }
        std::hint::black_box(&sink);

        // Phase 1: building the Arrow batch. This is copy #1 — the caller's
        // slice into an Arrow-owned buffer.
        let mut encode = u128::MAX;
        for _ in 0..reps {
            let t = Instant::now();
            let batches: Vec<_> = parts
                .iter()
                .map(|part| encode_segment(&side.schema, ct, part))
                .collect();
            encode = encode.min(t.elapsed().as_micros());
            std::hint::black_box(batches);
        }

        // Phase 2: serialising into chunks. This is copy #2 — the one removing
        // would take a new shm-arrow API and fresh unsafe.
        let batches: Vec<_> = parts
            .iter()
            .map(|part| encode_segment(&side.schema, ct, part))
            .collect();
        let data_seg = store.data_segment().clone();
        let pool = shm_core::Pool::attach(&data_seg).unwrap();
        let alloc = shm_arrow::PoolAllocator::new(&pool, &data_seg);
        let mut stage = u128::MAX;
        for _ in 0..reps {
            let t = Instant::now();
            let mut all = Vec::new();
            for b in &batches {
                all.extend(shm_arrow::write_batch_chunks(&alloc, &registry, b).unwrap());
            }
            stage = stage.min(t.elapsed().as_micros());
            for c in &all {
                shm_arrow::ChunkAllocator::free(&alloc, c);
            }
        }

        // Phase 3: the whole thing, including the staged commit.
        let mut total = u128::MAX;
        for _ in 0..reps {
            let t = Instant::now();
            objects.put("probe", ct, &payload).unwrap();
            total = total.min(t.elapsed().as_micros());
        }

        let rest = total.saturating_sub(encode + stage);
        let pct = |v: u128| (v as f64) * 100.0 / (total as f64);
        println!(
            "\n  {} MiB in {} segments of {seg} B, best of {reps}",
            N >> 20,
            parts.len()
        );
        println!("  raw memcpy of the payload   {memcpy:>7} us   (the floor)");
        println!(
            "  1 build arrow batches       {encode:>7} us   {:>5.1}%",
            pct(encode)
        );
        println!(
            "  2 serialise into chunks     {stage:>7} us   {:>5.1}%",
            pct(stage)
        );
        println!(
            "  3 everything else           {rest:>7} us   {:>5.1}%",
            pct(rest)
        );
        println!("  ----------------------------------------------");
        println!("  put, end to end             {total:>7} us");
        println!(
            "\n  Removing the last copy targets phase 2 only, and cannot beat\n  \
             the memcpy floor within it. Upper bound on the win: {:.1}% of a put.\n",
            pct(stage)
        );

        objects.delete("probe").unwrap();
    }
}
