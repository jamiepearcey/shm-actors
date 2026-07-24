//! The actor-side host: connect to a coordinator, map granted segments, and
//! loan / publish / subscribe over shared memory with no hot-path syscalls.
//!
//! A [`Node`] is an ordinary process's handle onto the substrate. It registers
//! over the control socket, adopts the payload + borrow-journal segment fds the
//! coordinator passes ([`Segment::from_raw_fd`]), and then drives the pub/sub
//! data path directly in shared memory. The only syscalls on the data path are
//! the optional heartbeat (a lease renewal, off the payload path).

use std::collections::HashMap;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use shm_arrow::{read_batch, write_batch, PinGuard, PoolAllocator, SchemaRegistry};
use shm_artifact::{Artifact, VersionEvent, ARTIFACTS_TOPIC};
use shm_core::{BorrowJournal, ChunkDesc, Pool, Segment};
use shm_store::{read_typed_ref, write_typed_ref, KeyResolver, KeyedStore, TypedRef};
use shm_ring::{DoorbellNotifier, DoorbellParker, Msg, Publisher, Ring, Subscriber};
use shm_stream::{Commit, Coordination, StreamWriter};
use shm_task::{
    now_nanos, ClaimedTask, Outcome, TaskHandle, TaskQueue, TaskStatus,
};

use crate::error::{Error, Result};
use crate::protocol::{Request, Response};
use crate::uds::{recv_frame, send_frame};

/// A mapped topic ring segment, its region length, and the topic's doorbell fd.
///
/// A `Node` uses a topic in a single role: the doorbell end fixed at the first
/// [`Node::ensure_topic`] is the write-end (for a publisher) or the read-end
/// (for a subscriber). The publisher's [`DoorbellNotifier`] borrows this fd; a
/// subscriber's [`DoorbellParker`] gets its own duplicate so it can outlive
/// nothing but still own its wakeup fd.
struct TopicRing {
    segment: Arc<Segment>,
    region_len: usize,
    doorbell: OwnedFd,
}

/// A coordinator-granted artifact: its handle plus the two segments backing it.
///
/// The `Artifact` carries a watch sink that, on every successful commit,
/// publishes the [`VersionEvent`] onto the coordinator's `__artifacts` ring —
/// so a stream commit through this artifact is observable by any
/// [`watch_artifacts`](Node::watch_artifacts) subscriber. The segments are kept
/// mapped for the artifact's (and every pin's) lifetime.
struct ArtifactMap {
    artifact: Artifact,
    #[allow(dead_code)]
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
}

/// A coordinator-granted task-queue mapping: the segment plus every doorbell end
/// (work/done, read/write) so this node can act as worker, submitter, and
/// requester over the one shared queue.
struct TaskQueueMap {
    segment: Arc<Segment>,
    region_len: usize,
    work_read: OwnedFd,
    work_write: OwnedFd,
    done_read: OwnedFd,
    done_write: OwnedFd,
}

/// A coordinator-granted keyed-store mapping: the three store segments (catalog,
/// head-management, shared data pool). ADR-0007 G3.
struct StoreMap {
    catalog_seg: Arc<Segment>,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
}

/// A node's local bidirectional **key cache** (opaque key bytes ↔ interned
/// `key_id`), filled from the coordinator, so a warm key resolves with no UDS
/// round-trip — the `schema_id` precedent applied to keys (ADR-0007 G3).
#[derive(Default)]
struct KeyCache {
    by_bytes: HashMap<Vec<u8>, u32>,
    by_id: HashMap<u32, Vec<u8>>,
}

/// An actor's handle onto a running coordinator's substrate.
pub struct Node {
    name: String,
    actor_id: u32,
    payload_seg: Arc<Segment>,
    journal_seg: Arc<Segment>,
    registry: Arc<SchemaRegistry>,
    topics: HashMap<String, TopicRing>,
    artifacts: HashMap<String, ArtifactMap>,
    task_queue: Option<TaskQueueMap>,
    /// The mapped keyed-store segments (ADR-0007 G3), on first
    /// [`open_store`](Node::open_store).
    store: Option<StoreMap>,
    /// The local key cache (filled by [`intern_key`](Node::intern_key)).
    keys: Mutex<KeyCache>,
    /// Guarded send side (shared with the heartbeat thread).
    send: Arc<Mutex<UnixStream>>,
    /// Read side (responses only; main thread).
    read_stream: UnixStream,
    heartbeat: Option<JoinHandle<()>>,
}

impl Node {
    /// Connect to the coordinator at `uds_path`, register as `name`, and adopt
    /// the payload + journal segments the coordinator passes back.
    ///
    /// `registry` is this node's **local schema cache**. Since ADR-0003 item E it
    /// no longer needs to be seeded identically to other actors': pass an empty
    /// [`SchemaRegistry::new`] and let [`intern_schema`](Self::intern_schema) /
    /// [`resolve_schema`](Self::resolve_schema) fill it from the coordinator, the
    /// authoritative catalog. A pre-seeded ([`with_schemas`](SchemaRegistry::with_schemas))
    /// registry still works for single-process use.
    pub fn connect(
        uds_path: impl AsRef<Path>,
        name: &str,
        registry: Arc<SchemaRegistry>,
    ) -> Result<Node> {
        let stream = UnixStream::connect(uds_path)?;
        let send = Arc::new(Mutex::new(stream.try_clone()?));
        let read_stream = stream;

        // --- Register handshake (expects the two segment fds). ---
        {
            let guard = send.lock().expect("send mutex poisoned");
            send_frame(&*guard, &Request::Register { name: name.into() }.encode(), &[])?;
        }
        let frame = recv_frame(&read_stream)?;
        let resp = Response::decode(&frame.body)?;
        let (actor_id, payload_seg_id, journal_seg_id) = match resp {
            Response::Registered {
                actor_id,
                payload_seg_id,
                journal_seg_id,
                ..
            } => (actor_id, payload_seg_id, journal_seg_id),
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected Registered")),
        };
        if frame.fds.len() != 2 {
            return Err(Error::MissingFds {
                expected: 2,
                received: frame.fds.len(),
            });
        }
        // Adopt the passed fds. Order matches the coordinator: payload, journal.
        let mut fds = frame.fds;
        let journal_fd = fds.pop().unwrap();
        let payload_fd = fds.pop().unwrap();
        // SAFETY: these fds were freshly received via SCM_RIGHTS from the
        // coordinator, each naming a live shm object; ownership transfers here.
        let payload_seg = Arc::new(unsafe {
            Segment::from_raw_fd(into_raw(payload_fd), payload_seg_id)?
        });
        let journal_seg = Arc::new(unsafe {
            Segment::from_raw_fd(into_raw(journal_fd), journal_seg_id)?
        });

        Ok(Node {
            name: name.into(),
            actor_id,
            payload_seg,
            journal_seg,
            registry,
            topics: HashMap::new(),
            artifacts: HashMap::new(),
            task_queue: None,
            store: None,
            keys: Mutex::new(KeyCache::default()),
            send,
            read_stream,
            heartbeat: None,
        })
    }

    /// This node's local schema-registry **cache** (filled by
    /// [`intern_schema`](Self::intern_schema) / [`resolve_schema`](Self::resolve_schema)).
    #[inline]
    pub fn registry(&self) -> &Arc<SchemaRegistry> {
        &self.registry
    }

    /// Intern `schema` at the coordinator (ADR-0003 item E), returning its stable
    /// coordinator-issued `schema_id` and caching the `(id, schema)` mapping
    /// locally both ways.
    ///
    /// Probes the local cache by content hash first (no syscall on a hit);
    /// otherwise serializes the schema ([`shm_arrow::serialize_schema`]), asks the
    /// coordinator to intern it over UDS, and records the result. A **producer**
    /// calls this before writing so the id it stamps into batch chunks and version
    /// manifests is the coordinator's globally-agreed id — not a process-local one.
    /// Because the subsequent [`write_batch`] interns the same schema into this
    /// same cache, it transparently picks up the coordinator id.
    pub fn intern_schema(&self, schema: &SchemaRef) -> Result<u32> {
        if let Some(id) = self.registry.id_for(schema) {
            return Ok(id);
        }
        let schema_bytes = shm_arrow::serialize_schema(schema);
        let (resp, _fds) = self.request(&Request::InternSchema { schema_bytes })?;
        let schema_id = match resp {
            Response::SchemaInterned { schema_id } => schema_id,
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected SchemaInterned")),
        };
        self.registry.insert(schema_id, schema.clone());
        Ok(schema_id)
    }

    /// Resolve a coordinator-issued `schema_id` to its [`SchemaRef`] (ADR-0003
    /// item E), caching the mapping locally.
    ///
    /// Local cache first; otherwise asks the coordinator over UDS, deserializes
    /// the returned bytes ([`shm_arrow::deserialize_schema`]), and records the
    /// mapping. A **consumer** calls this with the id it read from a version
    /// manifest (or batch header) before reconstructing the batch — proving two
    /// processes agree on the schema without ever seeding identical registries.
    pub fn resolve_schema(&self, schema_id: u32) -> Result<SchemaRef> {
        if let Some(schema) = self.registry.resolve(schema_id) {
            return Ok(schema);
        }
        let (resp, _fds) = self.request(&Request::ResolveSchema { schema_id })?;
        let schema_bytes = match resp {
            Response::SchemaResolved { schema_bytes } => schema_bytes,
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected SchemaResolved")),
        };
        let schema = shm_arrow::deserialize_schema(&schema_bytes)?;
        self.registry.insert(schema_id, schema.clone());
        Ok(schema)
    }

    /// Intern an opaque store `key` at the coordinator (ADR-0007 G3), returning
    /// its stable `key_id` and caching the mapping both ways. Probes the local
    /// cache first (no syscall on a hit) — the `schema_id` precedent for keys.
    pub fn intern_key(&self, key: &[u8]) -> Result<u32> {
        {
            let c = self.keys.lock().expect("key cache poisoned");
            if let Some(&id) = c.by_bytes.get(key) {
                return Ok(id);
            }
        }
        let (resp, _fds) = self.request(&Request::InternKey {
            key_bytes: key.to_vec(),
        })?;
        let key_id = match resp {
            Response::KeyInterned { key_id } => key_id,
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected KeyInterned")),
        };
        let mut c = self.keys.lock().expect("key cache poisoned");
        c.by_bytes.insert(key.to_vec(), key_id);
        c.by_id.insert(key_id, key.to_vec());
        Ok(key_id)
    }

    /// Resolve a coordinator-issued `key_id` back to its opaque key bytes
    /// (ADR-0007 G3), caching the mapping. Local cache first, then UDS.
    pub fn resolve_key(&self, key_id: u32) -> Result<Vec<u8>> {
        {
            let c = self.keys.lock().expect("key cache poisoned");
            if let Some(bytes) = c.by_id.get(&key_id) {
                return Ok(bytes.clone());
            }
        }
        let (resp, _fds) = self.request(&Request::ResolveKey { key_id })?;
        let key_bytes = match resp {
            Response::KeyResolved { key_bytes } => key_bytes,
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected KeyResolved")),
        };
        let mut c = self.keys.lock().expect("key cache poisoned");
        c.by_bytes.insert(key_bytes.clone(), key_id);
        c.by_id.insert(key_id, key_bytes.clone());
        Ok(key_bytes)
    }

    /// Map the built-in keyed store's three segments (idempotent). Usually called
    /// implicitly by [`store`](Node::store).
    pub fn open_store(&mut self) -> Result<()> {
        if self.store.is_some() {
            return Ok(());
        }
        let (resp, mut fds) = self.request(&Request::OpenStore)?;
        let (catalog_seg_id, head_seg_id, data_seg_id) = match resp {
            Response::StoreGranted {
                catalog_seg_id,
                head_seg_id,
                data_seg_id,
                ..
            } => (catalog_seg_id, head_seg_id, data_seg_id),
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected StoreGranted")),
        };
        if fds.len() != 3 {
            return Err(Error::MissingFds {
                expected: 3,
                received: fds.len(),
            });
        }
        // fd order (coordinator send order): catalog, head, data.
        let data_fd = fds.pop().unwrap();
        let head_fd = fds.pop().unwrap();
        let catalog_fd = fds.pop().unwrap();
        // SAFETY: freshly-received SCM_RIGHTS fds naming the store's catalog / head
        // / data shm segments; ownership transfers to the mapped `Segment`s.
        let catalog_seg =
            Arc::new(unsafe { Segment::from_raw_fd(into_raw(catalog_fd), catalog_seg_id)? });
        let head_seg = Arc::new(unsafe { Segment::from_raw_fd(into_raw(head_fd), head_seg_id)? });
        let data_seg = Arc::new(unsafe { Segment::from_raw_fd(into_raw(data_fd), data_seg_id)? });
        self.store = Some(StoreMap {
            catalog_seg,
            head_seg,
            data_seg,
        });
        Ok(())
    }

    /// A handle onto the built-in keyed store (ADR-0007 G3), mapping it on first
    /// use. Use it to [`create`](shm_store::KeyedStore::create) /
    /// [`open`](shm_store::KeyedStore::open) / [`evict`](shm_store::KeyedStore::evict)
    /// keyed artifacts — e.g. `node.store()?.create(key, kind, &schema)?`.
    ///
    /// The handle borrows this node (as the key-interning
    /// [`KeyResolver`](shm_store::KeyResolver)) but its [`Entry`](shm_store::Entry)
    /// results are self-owned and outlive it.
    pub fn store(&mut self) -> Result<KeyedStore<'_>> {
        self.open_store()?;
        // Reborrow `&mut self` as `&Node` for the rest: `open_store` already ran,
        // so no further mutation is needed and the returned handle borrows shared.
        let this: &Node = self;
        let m = this.store.as_ref().expect("store mapped above");
        Ok(KeyedStore::new(
            m.catalog_seg.clone(),
            m.head_seg.clone(),
            m.data_seg.clone(),
            this.journal_seg.clone(),
            this.registry.clone(),
            this.actor_id,
            this,
        ))
    }

    // ---- v0.5 G1: typed-ref envelope dispatch + resolve (ADR-0007) ----

    /// Write a [`TypedRef`] envelope into a chunk of the **shared store data
    /// pool** and return its [`ChunkDesc`] (tagged with `SCHEMA_TYPED_REF`), so
    /// the descriptor is resolvable by any actor that has mapped the store
    /// (ADR-0007 G1). The store must already be open (see
    /// [`open_store`](Self::open_store)); each actor's own payload segment is
    /// *not* shared, so an envelope destined for another process must live in the
    /// store's shared pool.
    pub fn write_ref_chunk(&self, tref: &TypedRef) -> Result<ChunkDesc> {
        let m = self.store.as_ref().ok_or(Error::NotFound("store not open"))?;
        let pool = Pool::attach(&m.data_seg)?;
        let alloc = PoolAllocator::new(&pool, &m.data_seg);
        Ok(write_typed_ref(&alloc, tref)?)
    }

    /// Read + validate the [`TypedRef`] envelope the descriptor `desc` names from
    /// the shared store data pool (ADR-0007 G1). Errors if `desc` is not tagged
    /// `SCHEMA_TYPED_REF` or fails envelope validation. The store must be open.
    pub fn read_ref_chunk(&self, desc: &ChunkDesc) -> Result<TypedRef> {
        let m = self.store.as_ref().ok_or(Error::NotFound("store not open"))?;
        let guard = PinGuard::new(m.data_seg.clone());
        Ok(read_typed_ref(&guard, desc)?)
    }

    /// Read the [`TypedRef`] envelope a claimed task's `request` descriptor points
    /// at — the ergonomic worker-side accessor (ADR-0007 G1). Equivalent to
    /// `node.read_ref_chunk(&claimed.request())`; provided so a worker gets the
    /// envelope from its claim without touching the store pool directly.
    pub fn task_ref(&self, claimed: &ClaimedTask) -> Result<TypedRef> {
        self.read_ref_chunk(&claimed.request())
    }

    /// Return an envelope chunk (allocated by [`write_ref_chunk`](Self::write_ref_chunk))
    /// to the shared store data pool's free list. The chunk was never loaned, so
    /// this simply re-links it; call it once the envelope is no longer needed
    /// (e.g. by the requester after reading a result, or by the worker that
    /// completed a task) to keep the store data-pool census leak-free.
    pub fn free_ref_chunk(&self, desc: &ChunkDesc) -> Result<()> {
        let m = self.store.as_ref().ok_or(Error::NotFound("store not open"))?;
        let pool = Pool::attach(&m.data_seg)?;
        let _ = pool.free(desc);
        Ok(())
    }

    /// Dispatch a [`TypedRef`] through the built-in task queue (ADR-0007 G1): write
    /// the envelope into a shared store-pool chunk, then submit its 24-byte
    /// `ChunkDesc` as the task request with an absolute `deadline_nanos` (in
    /// [`now_nanos`]'s clock domain). Control stays a descriptor; the referent is
    /// never copied. Returns the correlation [`TaskHandle`].
    ///
    /// Maps both the store (for the envelope chunk) and the task queue on first
    /// use. A worker claims the task, reads the envelope with
    /// [`task_ref`](Self::task_ref), and resolves it via
    /// [`store()`](Self::store)`.resolve_and_pin(..)`.
    pub fn submit_typed_task(&mut self, tref: &TypedRef, deadline_nanos: u64) -> Result<TaskHandle> {
        self.open_store()?;
        self.open_task_queue()?;
        let desc = self.write_ref_chunk(tref)?;
        self.task_queue()?.submit(desc, deadline_nanos)
    }

    /// The actor id assigned by the coordinator.
    #[inline]
    pub fn actor_id(&self) -> u32 {
        self.actor_id
    }

    /// The actor's name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A shared handle to the payload segment (e.g. to inspect a `ChunkCtrl`).
    #[inline]
    pub fn payload_segment(&self) -> &Arc<Segment> {
        &self.payload_seg
    }

    /// Send a fire-and-forget request (no response is read).
    fn fire(&self, req: &Request) -> Result<()> {
        let guard = self.send.lock().expect("send mutex poisoned");
        send_frame(&*guard, &req.encode(), &[])
    }

    /// Send a request and read the single response it elicits.
    fn request(&self, req: &Request) -> Result<(Response, Vec<std::os::fd::OwnedFd>)> {
        {
            let guard = self.send.lock().expect("send mutex poisoned");
            send_frame(&*guard, &req.encode(), &[])?;
        }
        let frame = recv_frame(&self.read_stream)?;
        Ok((Response::decode(&frame.body)?, frame.fds))
    }

    /// Ensure `topic` exists (creating its ring), mapping the ring segment.
    fn ensure_topic(&mut self, topic: &str, subscribe: bool) -> Result<()> {
        if self.topics.contains_key(topic) {
            return Ok(());
        }
        let req = if subscribe {
            Request::Subscribe { topic: topic.into() }
        } else {
            Request::CreateTopic { topic: topic.into() }
        };
        let (resp, mut fds) = self.request(&req)?;
        let (ring_seg_id, region_len) = match resp {
            Response::Granted {
                ring_seg_id,
                region_len,
            } => (ring_seg_id, region_len as usize),
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected Granted")),
        };
        if fds.len() != 2 {
            return Err(Error::MissingFds {
                expected: 2,
                received: fds.len(),
            });
        }
        // fd order (coordinator send order): ring segment, then doorbell.
        let doorbell = fds.pop().unwrap();
        let ring_fd = fds.pop().unwrap();
        // SAFETY: a freshly-received SCM_RIGHTS fd naming the topic's ring shm
        // segment; ownership transfers to the mapped `Segment`.
        let segment = Arc::new(unsafe { Segment::from_raw_fd(into_raw(ring_fd), ring_seg_id)? });
        self.topics.insert(
            topic.into(),
            TopicRing {
                segment,
                region_len,
                doorbell,
            },
        );
        Ok(())
    }

    /// Attach a [`Ring`] over a mapped topic's ring region.
    fn ring_for(&self, topic: &str) -> Result<Ring> {
        let t = self
            .topics
            .get(topic)
            .ok_or(Error::NotFound("topic not mapped"))?;
        // The ring lives in the ring segment's payload region.
        let base = t.segment.payload_ptr();
        // SAFETY: `base` points at `region_len` writable bytes the coordinator
        // initialized with `Ring::init`, mapped and kept alive by `t.segment`.
        Ok(unsafe { Ring::attach(base, t.region_len)? })
    }

    /// Loan a chunk, serialize `batch` into it, publish it to `topic`, and
    /// notify the coordinator. Returns the published descriptor.
    ///
    /// This is the full producer data path: allocate from the pool, mark the
    /// chunk `LOANED` (exclusive write), fill it via [`write_batch`], transition
    /// it to `PUBLISHED`, and broadcast the 24-byte descriptor on the ring — the
    /// payload is written exactly once.
    pub fn publish_batch(&mut self, topic: &str, batch: &RecordBatch) -> Result<ChunkDesc> {
        self.ensure_topic(topic, false)?;
        let seg = &self.payload_seg;
        let pool = Pool::attach(seg)?;
        let allocator = PoolAllocator::new(&pool, seg);

        // Serialize into a freshly-popped chunk (ctrl still FREE).
        let desc = write_batch(&allocator, &self.registry, batch)?;

        // FREE -> LOANED (exclusive) -> PUBLISHED (visible to subscribers).
        let ctrl = pool.ctrl(&desc)?;
        ctrl.try_loan(self.actor_id)?;
        ctrl.publish()?;

        // Broadcast the descriptor, ringing the topic's doorbell to wake any
        // parked subscribers. The write-end fd is owned by `TopicRing` (kept
        // alive by `self`), so the borrowed `RawFd` is valid for this call.
        let ring = self.ring_for(topic)?;
        let write_fd = self
            .topics
            .get(topic)
            .expect("topic ensured above")
            .doorbell
            .as_raw_fd();
        Publisher::with_notifier(ring, DoorbellNotifier::new(write_fd)).publish(desc);

        // Tell the coordinator (control plane) which chunk is now published so it
        // can manage the exclusive-ownership handoff on the pinned ack.
        self.fire(&Request::Published { desc })?;
        Ok(desc)
    }

    /// Subscribe to `topic` from the start of its live ring history, parking on
    /// the topic's doorbell so an idle `recv` blocks instead of spinning.
    ///
    /// Using `from_start` makes the consumer robust to subscribing after the
    /// producer has already published (the message is still live in the ring).
    /// The returned [`Subscriber`] owns a duplicate of the doorbell read-end via
    /// its [`DoorbellParker`], so its wakeup fd closes when the subscriber drops.
    pub fn subscribe(&mut self, topic: &str) -> Result<Subscriber<DoorbellParker>> {
        self.ensure_topic(topic, true)?;
        let ring = self.ring_for(topic)?;
        let read = self
            .topics
            .get(topic)
            .expect("topic ensured above")
            .doorbell
            .try_clone()?;
        Ok(Subscriber::from_start_with_parker(ring, DoorbellParker::new(read)))
    }

    /// Take a shared pin on a received `desc`, record it in the borrow journal,
    /// and reconstruct the `RecordBatch` **zero-copy** over the mapped chunk.
    ///
    /// The returned batch's Arrow buffers point directly into the shared segment
    /// (kept alive by an internal [`PinGuard`]); the pin (refcount + journal
    /// slot) persists in shared memory until [`release_pin`](Self::release_pin)
    /// or — if this actor dies holding it — the coordinator's crash reclaim.
    pub fn pin_and_read(&self, desc: &ChunkDesc) -> Result<Pin> {
        let seg = &self.payload_seg;
        let pool = Pool::attach(seg)?;
        let ctrl = pool.ctrl(desc)?;

        // Take the shared pin FIRST (refcount++), so the chunk cannot be
        // reclaimed out from under the read.
        ctrl.borrow_shared()?;

        // Record the pin in this actor's borrow journal for crash replay.
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        let slot = match journal.record(*desc) {
            Ok(slot) => slot,
            Err(e) => {
                // Undo the refcount bump if we cannot journal the pin.
                ctrl.release_shared();
                return Err(e.into());
            }
        };

        // Reconstruct zero-copy. The PinGuard keeps the mapping alive for every
        // buffer built over it.
        let owner = Arc::new(PinGuard::new(seg.clone()));
        let batch = read_batch(owner, ctrl, desc, &self.registry)?;

        // Notify the coordinator that the chunk is now pinned so it can release
        // the producer's exclusive ownership without racing reclamation.
        self.fire(&Request::Pinned { desc: *desc })?;

        Ok(Pin {
            desc: *desc,
            slot,
            batch,
        })
    }

    /// Release a pin taken by [`pin_and_read`](Self::pin_and_read): drop the
    /// shared refcount and clear the journal slot.
    pub fn release_pin(&self, pin: &Pin) -> Result<()> {
        let pool = Pool::attach(&self.payload_seg)?;
        if let Ok(ctrl) = pool.ctrl(&pin.desc) {
            ctrl.release_shared();
        }
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        journal.release(pin.slot)?;
        Ok(())
    }

    // ---- v0.2 cache-loop API (ADR-0002 item A + walking skeleton) ----

    /// Open (creating on first reference) the named artifact, mapping its head +
    /// data segments and wiring a commit **watch sink** that publishes a
    /// [`VersionEvent`] on the coordinator's `__artifacts` ring.
    ///
    /// Idempotent: a second call for the same name is a no-op. After opening,
    /// [`artifact`](Node::artifact) reads (pin) and [`stream`](Node::stream)
    /// writes. A stream commit fires the watch sink, so any
    /// [`watch_artifacts`](Node::watch_artifacts) subscriber sees the new
    /// version — this is ADR-0002 item A (watch-on-commit).
    pub fn open_artifact(&mut self, name: &str) -> Result<()> {
        if self.artifacts.contains_key(name) {
            return Ok(());
        }
        // Map the built-in `__artifacts` ring as a publisher (we take the
        // write-end) so the watch sink can broadcast commit events.
        self.ensure_topic(ARTIFACTS_TOPIC, false)?;

        let (resp, mut fds) = self.request(&Request::OpenArtifact { name: name.into() })?;
        let (name_id, head_seg_id, data_seg_id) = match resp {
            Response::ArtifactGranted {
                name_id,
                head_seg_id,
                data_seg_id,
            } => (name_id, head_seg_id, data_seg_id),
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected ArtifactGranted")),
        };
        if fds.len() != 2 {
            return Err(Error::MissingFds {
                expected: 2,
                received: fds.len(),
            });
        }
        // fd order (coordinator send order): head segment, then data segment.
        let data_fd = fds.pop().unwrap();
        let head_fd = fds.pop().unwrap();
        // SAFETY: freshly-received SCM_RIGHTS fds naming the artifact's head/data
        // shm segments; ownership transfers to the mapped `Segment`s.
        let head_seg = Arc::new(unsafe { Segment::from_raw_fd(into_raw(head_fd), head_seg_id)? });
        let data_seg = Arc::new(unsafe { Segment::from_raw_fd(into_raw(data_fd), data_seg_id)? });

        // Build the watch sink over an independent, owned view of the
        // `__artifacts` ring (segment Arc + a duplicated doorbell write-end), so
        // it is `Send + Sync + 'static` and keeps its wakeup fd alive itself.
        let art_topic = self.topics.get(ARTIFACTS_TOPIC).expect("ensured above");
        let ring_seg = art_topic.segment.clone();
        let region_len = art_topic.region_len;
        let write_fd = art_topic.doorbell.try_clone()?;
        let artifact = Artifact::attach(name_id, head_seg.clone(), data_seg.clone())?.with_watch(
            move |ev: VersionEvent| {
                // SAFETY: `ring_seg` (an owned Arc) keeps the ring segment mapped
                // for this closure's life; `payload_ptr()` backs `region_len`
                // bytes the coordinator initialised with `Ring::init`.
                if let Ok(ring) = unsafe { Ring::attach(ring_seg.payload_ptr(), region_len) } {
                    Publisher::with_notifier(ring, DoorbellNotifier::new(write_fd.as_raw_fd()))
                        .publish(ev.to_desc());
                }
            },
        );

        self.artifacts.insert(
            name.into(),
            ArtifactMap {
                artifact,
                head_seg,
                data_seg,
            },
        );
        Ok(())
    }

    /// Borrow a previously [opened](Node::open_artifact) artifact, e.g. to
    /// [`pin`](shm_artifact::Artifact::pin) and read a version zero-copy.
    pub fn artifact(&self, name: &str) -> Result<&Artifact> {
        self.artifacts
            .get(name)
            .map(|a| &a.artifact)
            .ok_or(Error::NotFound("artifact not opened"))
    }

    /// Pin a previously [opened](Node::open_artifact) artifact's current version
    /// **through this actor's borrow journal** (ADR-0003 item J), so a `kill -9`
    /// mid-pin is crash-reclaimed by the coordinator's lease-monitor replay
    /// (which decrements the artifact's per-version pin count).
    ///
    /// Prefer this over `node.artifact(name)?.pin()` for any pin held across an
    /// await/blocking point in a cross-process actor: the plain
    /// [`pin`](shm_artifact::Artifact::pin) leaks its version forever if the
    /// holder dies. The returned [`VersionPin`](shm_artifact::VersionPin)
    /// releases its journal entry on a clean drop.
    pub fn pin_artifact(&self, name: &str) -> Result<shm_artifact::VersionPin> {
        let art = self.artifact(name)?;
        Ok(art.pin_journaled(&self.journal_seg)?)
    }

    /// Open a write transaction against a previously [opened](Node::open_artifact)
    /// artifact, returning a [`NodeStream`] whose
    /// [`writer`](NodeStream::writer) is an ordinary [`shm_stream::StreamWriter`]
    /// bound to this node's borrow journal (so a crash mid-stage is reclaimed by
    /// the coordinator) and this artifact's commit watch sink (so `commit()`
    /// publishes a [`VersionEvent`]).
    pub fn stream(&self, name: &str) -> Result<NodeStream<'_>> {
        let am = self
            .artifacts
            .get(name)
            .ok_or(Error::NotFound("artifact not opened"))?;
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        Ok(NodeStream {
            artifact: &am.artifact,
            data_seg: am.data_seg.as_ref(),
            journal,
            registry: self.registry.as_ref(),
            owner: self.actor_id,
        })
    }

    /// Subscribe to the built-in `__artifacts` topic, parking on its doorbell so
    /// an idle watcher blocks (≈0% CPU) until a commit event arrives — ADR-0002
    /// stage D applied to item A.
    ///
    /// The returned [`ArtifactWatcher`] yields decoded [`VersionEvent`]s (via
    /// [`VersionEvent::from_desc`]).
    pub fn watch_artifacts(&mut self) -> Result<ArtifactWatcher> {
        let sub = self.subscribe(ARTIFACTS_TOPIC)?;
        Ok(ArtifactWatcher { sub })
    }

    /// Map the built-in task queue (idempotent). Usually called implicitly by
    /// [`task_queue`](Node::task_queue).
    pub fn open_task_queue(&mut self) -> Result<()> {
        if self.task_queue.is_some() {
            return Ok(());
        }
        let (resp, mut fds) = self.request(&Request::OpenTaskQueue)?;
        let (queue_seg_id, region_len) = match resp {
            Response::TaskQueueGranted {
                queue_seg_id,
                region_len,
            } => (queue_seg_id, region_len as usize),
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected TaskQueueGranted")),
        };
        if fds.len() != 5 {
            return Err(Error::MissingFds {
                expected: 5,
                received: fds.len(),
            });
        }
        // fd order: queue seg, work read, work write, done read, done write.
        let done_write = fds.pop().unwrap();
        let done_read = fds.pop().unwrap();
        let work_write = fds.pop().unwrap();
        let work_read = fds.pop().unwrap();
        let queue_fd = fds.pop().unwrap();
        // SAFETY: a freshly-received SCM_RIGHTS fd naming the task-queue shm
        // segment; ownership transfers to the mapped `Segment`.
        let segment = Arc::new(unsafe { Segment::from_raw_fd(into_raw(queue_fd), queue_seg_id)? });
        self.task_queue = Some(TaskQueueMap {
            segment,
            region_len,
            work_read,
            work_write,
            done_read,
            done_write,
        });
        Ok(())
    }

    /// A handle onto the built-in task queue for submit / claim / wait / complete
    /// (mapping it on first use). The handle wires the work/done doorbells so
    /// workers block on the work doorbell and requesters on the done doorbell.
    pub fn task_queue(&mut self) -> Result<TaskQueueHandle> {
        self.open_task_queue()?;
        let m = self.task_queue.as_ref().expect("opened above");
        // Own our own duplicated write-ends so the queue's notifiers reference
        // fds that live exactly as long as the returned handle.
        let work_write = m.work_write.try_clone()?;
        let done_write = m.done_write.try_clone()?;
        // SAFETY: the cloned `segment` Arc keeps the region mapped for the
        // handle's life; the coordinator initialised it with `TaskQueue::init`.
        let queue = unsafe { TaskQueue::attach(m.segment.payload_ptr(), m.region_len)? }
            .with_work_notifier(DoorbellNotifier::new(work_write.as_raw_fd()))
            .with_done_notifier(DoorbellNotifier::new(done_write.as_raw_fd()));
        Ok(TaskQueueHandle {
            queue,
            _segment: m.segment.clone(),
            work_read: m.work_read.try_clone()?,
            done_read: m.done_read.try_clone()?,
            _work_write: work_write,
            _done_write: done_write,
            worker_id: self.actor_id,
        })
    }

    /// Start a background heartbeat thread that renews this actor's lease every
    /// `interval`. Call once, after all request/response handshakes are done.
    pub fn start_heartbeat(&mut self, interval: Duration) {
        let send = self.send.clone();
        let handle = std::thread::spawn(move || {
            let body = Request::Heartbeat.encode();
            loop {
                std::thread::sleep(interval);
                let guard = match send.lock() {
                    Ok(g) => g,
                    Err(_) => break,
                };
                // Once the coordinator/socket is gone, stop heartbeating.
                if send_frame(&*guard, &body, &[]).is_err() {
                    break;
                }
            }
        });
        self.heartbeat = Some(handle);
    }

    /// Send a graceful goodbye so the coordinator drops the actor without
    /// treating it as a crash.
    pub fn say_bye(&self) -> Result<()> {
        self.fire(&Request::Bye)
    }
}

/// The [`Node`] is the keyed store's key-interning seam: it resolves opaque key
/// bytes to a coordinator-issued `key_id` over UDS (with a local cache), so
/// `shm-store` never speaks the control protocol itself (ADR-0007 G3).
impl KeyResolver for Node {
    fn intern_key(&self, key: &[u8]) -> shm_store::Result<u32> {
        Node::intern_key(self, key).map_err(|e| shm_store::Error::Intern(e.to_string()))
    }
}

/// A live shared pin plus the zero-copy batch reconstructed over it.
pub struct Pin {
    /// The pinned descriptor.
    pub desc: ChunkDesc,
    /// The borrow-journal slot recording the pin.
    pub slot: usize,
    /// The reconstructed record batch (buffers point into shared memory).
    pub batch: RecordBatch,
}

/// A write transaction opened via [`Node::stream`].
///
/// It owns the node's [`BorrowJournal`] handle for the transaction's duration
/// and lends out an ordinary [`shm_stream::StreamWriter`] through
/// [`writer`](NodeStream::writer). Keeping the journal here (rather than inside
/// the writer) sidesteps a self-referential borrow: the `NodeStream` borrows the
/// node, and the `StreamWriter` borrows the `NodeStream`.
pub struct NodeStream<'a> {
    artifact: &'a Artifact,
    data_seg: &'a Segment,
    journal: BorrowJournal<'a>,
    registry: &'a SchemaRegistry,
    owner: u32,
}

impl NodeStream<'_> {
    /// Open a [`StreamWriter`] for this transaction with the given commit kind
    /// and coordination discipline. Its `commit()` installs a new artifact
    /// version and fires the artifact's watch sink (publishing a
    /// [`VersionEvent`] on `__artifacts`).
    pub fn writer(&self, kind: Commit, coordination: Coordination) -> Result<StreamWriter<'_>> {
        Ok(StreamWriter::open(
            self.artifact,
            self.data_seg,
            &self.journal,
            self.registry,
            self.owner,
            kind,
            coordination,
        )?)
    }
}

/// A doorbell-parked subscriber to the `__artifacts` topic that yields decoded
/// [`VersionEvent`]s. Built by [`Node::watch_artifacts`].
pub struct ArtifactWatcher {
    sub: Subscriber<DoorbellParker>,
}

impl ArtifactWatcher {
    /// Block (parked on the doorbell) until the next commit [`VersionEvent`],
    /// decoding it from the ring descriptor. Skips ring-lag notices and any
    /// non-event descriptor.
    pub fn recv(&mut self) -> VersionEvent {
        loop {
            match self.sub.recv() {
                Msg::Sample(desc) => {
                    if let Some(ev) = VersionEvent::from_desc(&desc) {
                        return ev;
                    }
                }
                Msg::Lagged(_) => {}
            }
        }
    }

    /// Access the underlying [`Subscriber`] (e.g. to observe park behaviour or
    /// drive a non-blocking receive).
    #[inline]
    pub fn subscriber(&mut self) -> &mut Subscriber<DoorbellParker> {
        &mut self.sub
    }
}

/// A handle onto the built-in task queue, wiring the work/done doorbells so
/// workers block on the work doorbell and requesters on the done doorbell.
/// Built by [`Node::task_queue`].
pub struct TaskQueueHandle {
    queue: TaskQueue,
    _segment: Arc<Segment>,
    work_read: OwnedFd,
    done_read: OwnedFd,
    _work_write: OwnedFd,
    _done_write: OwnedFd,
    worker_id: u32,
}

impl TaskQueueHandle {
    /// This node's worker/owner id (the correlation owner stamped on a claim).
    #[inline]
    pub fn worker_id(&self) -> u32 {
        self.worker_id
    }

    /// The underlying [`TaskQueue`] (for advanced/rare operations).
    #[inline]
    pub fn queue(&self) -> &TaskQueue {
        &self.queue
    }

    /// Submit a task with an absolute `deadline_nanos` (in [`now_nanos`]'s clock
    /// domain), returning its correlation [`TaskHandle`]. Rings the work doorbell
    /// iff workers are parked.
    pub fn submit(&self, request: ChunkDesc, deadline_nanos: u64) -> Result<TaskHandle> {
        Ok(self.queue.submit(request, deadline_nanos)?)
    }

    /// Read a task's current [`TaskStatus`], re-validating the handle.
    pub fn poll(&self, handle: TaskHandle) -> Result<TaskStatus> {
        Ok(self.queue.poll(handle)?)
    }

    /// Non-blocking claim with a fresh lease `lease_nanos` into the future (its
    /// deadline is stamped on the claim), or `None` if the queue is empty.
    pub fn claim(&self, lease_nanos: u64) -> Option<ClaimedTask> {
        self.queue
            .claim_with_lease(self.worker_id, now_nanos().wrapping_add(lease_nanos))
    }

    /// Claim a task, parking on the work doorbell while none is queued, stamping
    /// a fresh `lease_nanos` lease on the successful claim.
    pub fn claim_blocking(&self, lease_nanos: u64) -> Result<ClaimedTask> {
        let parker = DoorbellParker::new(self.work_read.try_clone()?);
        Ok(self
            .queue
            .claim_blocking_with_lease(self.worker_id, lease_nanos, &parker))
    }

    /// Block (parked on the done doorbell) until the task reaches a terminal
    /// [`Outcome`].
    pub fn wait(&self, handle: TaskHandle) -> Result<Outcome> {
        let parker = DoorbellParker::new(self.done_read.try_clone()?);
        Ok(self.queue.wait(handle, &parker)?)
    }
}

/// Consume an [`OwnedFd`] into its raw fd, transferring ownership onward.
fn into_raw(fd: std::os::fd::OwnedFd) -> std::os::fd::RawFd {
    // `IntoRawFd` releases the fd from the OwnedFd without closing it; the
    // receiver (`Segment::from_raw_fd`) takes over ownership.
    use std::os::fd::IntoRawFd;
    fd.into_raw_fd()
}
