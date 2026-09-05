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
//! - **A payload is not one contiguous slice.** [`ObjectRead`] hands back
//!   segments, never a single `&[u8]`; see [`ObjectRead::segments`].
//! - **The ceiling is the version manifest and the pool, not one size class.**
//!   A version's manifest is itself one chunk listing 24 bytes per data chunk
//!   plus 4 per batch, so a store pool whose largest class is `C` bytes holds
//!   roughly `C/28` segments of just under `C` bytes each, bounded also by how
//!   many `C`-sized chunks the pool was built with.
//!   [`ObjectStore::max_object`] computes it; past it the substrate reports its
//!   own `ChunkTooSmall` or pool exhaustion, rather than a limit invented here.

#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use holon_topic::TOPIC_EVENTS;
use shm_arrow::SchemaRegistry;
use shm_artifact::{Commit, CommitKind, VersionEvent, VersionPin};
use shm_core::BorrowJournal;
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

/// Errors of the object store.
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
/// One row per batch, and one batch per segment, deliberately. Splitting a
/// payload across *rows* does not raise the ceiling: Arrow lays a `Binary`
/// column's values out back to back in a single contiguous values buffer, so N
/// rows still allocate one buffer of the total size, and `shm-arrow` refuses a
/// buffer larger than one chunk. N *batches* is the axis that works — each batch
/// gets its own buffers and its own chunks — and all N install as one version.
///
/// `content_type` is repeated in every segment; it is the object's, not the
/// segment's, and readers take it from the first.
pub fn encode_segment(schema: &SchemaRef, content_type: &str, data: &[u8]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![content_type])),
            Arc::new(BinaryArray::from(vec![data])),
        ],
    )
    .expect("two same-length columns matching the object schema")
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
    entries: Mutex<HashMap<String, (Arc<Entry>, u32)>>,
}

impl<'a> ObjectStore<'a> {
    /// Bind an object store to a keyed store and its node-independent side.
    pub fn new(
        store: &'a KeyedStore<'a>,
        side: &'a ObjectSide,
        registry: Arc<SchemaRegistry>,
    ) -> ObjectStore<'a> {
        registry.intern(&side.schema);
        ObjectStore {
            store,
            side,
            registry,
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

    /// **The largest payload one object may hold, in bytes.** Ask this before
    /// encoding an answer: a caller that would exceed it can refuse with the
    /// number in the message instead of doing the work first.
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
        let pool =
            shm_core::Pool::attach(self.store.data_segment()).map_err(shm_store::Error::from)?;
        let cap = pool.max_chunk_size();
        // Chunks of the largest class, one of which the manifest itself takes.
        let class_chunks = (0..pool.num_classes())
            .filter_map(|i| pool.class_info(i))
            .find(|(size, _)| *size == cap)
            .map(|(_, count)| count as usize)
            .unwrap_or(0)
            .saturating_sub(1);
        // Segments whose descriptors + spans still fit one manifest chunk.
        // `manifest_len` is monotone in the segment count, so bisect it rather
        // than assume a closed form for a layout `shm-artifact` owns.
        let (mut lo, mut hi) = (0usize, cap as usize);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if shm_artifact::manifest_len(mid, mid, 0) <= cap as usize {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        let by_manifest = lo;
        let segments = by_manifest.min(class_chunks);
        let per_segment = self.segment_size(&" ".repeat(MAX_CONTENT_TYPE))?;
        Ok(segments as u64 * per_segment as u64)
    }

    /// The largest `data` slice one segment can carry: the chunk cap less the
    /// exact serialized overhead of a segment batch carrying `content_type`
    /// (batch header, node/buffer tables, both offset buffers, the content-type
    /// bytes and inter-buffer alignment). Derived by asking `shm-arrow` to size
    /// an empty segment, so nothing here is a guess or a hardcoded fraction, and
    /// each segment lands in exactly one chunk.
    fn segment_size(&self, content_type: &str) -> Result<usize> {
        let cap = self.max_payload()? as usize;
        let probe = encode_segment(&self.side.schema, content_type, &[]);
        let overhead =
            shm_arrow::serialized_len(&probe).expect("the object schema is serializable");
        Ok(cap.saturating_sub(overhead).max(1))
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
    /// The payload is cut into segments that each fit one chunk, staged
    /// invisibly, and installed as **one** version — one `Commit::Replace` CAS
    /// over N batches — so a reader never observes half an object and the
    /// previous version's chunks are released wholesale.
    ///
    /// Concurrent writers serialize on the entry's write lease; a lost race is
    /// retried, bounded. The version is assigned by the commit, so it is exact
    /// even under contention. Readers never block and are never disturbed: they
    /// keep reading whatever version they pinned.
    pub fn put(&self, key: &str, content_type: &str, bytes: &[u8]) -> Result<u64> {
        check_content_type(content_type)?;
        let (entry, kid) = self.entry_or_create(key)?;
        let seg = self.segment_size(content_type)?;
        // An empty payload is one empty segment, not zero segments: a version
        // with no batches would read back as a missing object.
        let parts: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[]]
        } else {
            bytes.chunks(seg).collect()
        };
        let version = self.install(&entry, content_type, &parts)?;
        // Descriptor-only: the watcher learns a version exists, not what is in
        // it. A lapped ring costs a wake-up, not data.
        let _ =
            self.side
                .events
                .publish_event(VersionEvent::new(kid, version, CommitKind::Replace));
        Ok(version)
    }

    /// Stage every segment and install them as one version, retrying a lost
    /// write lease a bounded number of times.
    ///
    /// The transaction is `shm-stream`'s: each batch is serialized into fresh
    /// chunks, loaned to this writer and recorded in its borrow journal *before*
    /// it counts as staged, and the whole set is installed by a single
    /// `Committer::commit_staged`. Every failure path — a chunk that will not
    /// allocate, a fenced lease, a rejected install — unwinds the staging it had
    /// already done (dropping loans, releasing journal slots, returning chunks
    /// to the pool) and installs nothing, so the pool census returns to exactly
    /// where it was and the object keeps the version it had.
    fn install(&self, entry: &Entry, content_type: &str, parts: &[&[u8]]) -> Result<u64> {
        let journal = BorrowJournal::attach(self.store.journal_segment())?;
        let mut tries = 0u32;
        loop {
            let mut writer = match StreamWriter::open(
                entry.artifact(),
                self.store.data_segment(),
                &journal,
                &self.registry,
                self.store.owner(),
                Commit::Replace,
                Coordination::Exclusive,
            ) {
                Ok(w) => w,
                Err(e) if is_contention(&e) => {
                    if tries >= PUT_RETRIES {
                        return Err(Error::Contended(tries));
                    }
                    tries += 1;
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            // One batch per segment, built as it is staged so only one segment
            // is ever duplicated in this process's heap.
            for part in parts {
                let batch = encode_segment(&self.side.schema, content_type, part);
                if let Err(e) = writer.append_batch(&batch) {
                    // Frees every chunk staged so far and releases their journal
                    // slots; nothing was ever visible to a reader.
                    writer.abort();
                    return Err(e.into());
                }
            }
            match writer.commit() {
                Ok(v) => return Ok(v),
                // The install itself lost the lease (fenced/contended); the
                // staged chunks were already returned to the pool by the commit.
                Err(e) if is_contention(&e) => {
                    if tries >= PUT_RETRIES {
                        return Err(Error::Contended(tries));
                    }
                    tries += 1;
                    std::thread::yield_now();
                }
                Err(e) => return Err(e.into()),
            }
        }
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
        let (pin, delta) = entry.read_since(0)?;
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

/// Whether a staging/commit failure is a lost race for the entry's write lease
/// — the one class of failure a `put` retries.
fn is_contention(e: &shm_stream::Error) -> bool {
    matches!(
        e,
        shm_stream::Error::Artifact(
            shm_artifact::Error::WriteLocked | shm_artifact::Error::Conflict { .. }
        )
    )
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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
            let store = node.store().unwrap();
            let objects = ObjectStore::new(&store, &side, registry);
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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
        let pool_bytes = cfg.store_data_size;

        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, cfg).unwrap();
        coord.start().unwrap();

        let mut node = connect(&uds, "writer");
        let side = ObjectSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

        // A live object beside it, so the failing put has to unwind back to a
        // non-trivial census rather than to an empty pool.
        objects
            .put("neighbour", "text/plain", b"untouched")
            .unwrap();
        objects.ensure("doomed").unwrap();
        let before = coord.store_data_free_total().expect("census");

        // Twice the entire data segment: staging runs out of chunks partway
        // through, with hundreds of segments already loaned and journaled.
        let huge = vec![9u8; pool_bytes * 2];
        let err = objects
            .put("doomed", "application/octet-stream", &huge)
            .expect_err("a payload twice the data segment cannot be staged");
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
        assert_eq!(joined(&n).as_slice(), &b"untouched"[..]);
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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
        let store = node.store().unwrap();
        let objects = ObjectStore::new(&store, &side, registry);

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
}
