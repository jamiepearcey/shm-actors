//! The coordinator: control plane, fd granting, leases, and crash reclamation.
//!
//! The coordinator owns the shared substrate but never touches the payload data
//! path. It:
//!
//! 1. creates + owns the payload segment (and its [`Pool`]), each topic's ring
//!    segment, and one borrow-journal segment per actor;
//! 2. hands those segments to actors by **passing their fds** over a Unix domain
//!    socket ([`SCM_RIGHTS`](crate::uds));
//! 3. tracks a per-actor **lease** (last heartbeat) and, when one expires,
//!    declares the actor dead and **replays its borrow journal**, releasing every
//!    chunk it still held so the pool can reclaim it.
//!
//! Death detection is lease-based by design (ADR-0001): a closed control socket
//! (EOF) is *not* treated as death — only a stopped heartbeat is — so the same
//! mechanism works whether an actor exits cleanly, hangs, or is `kill -9`ed.

use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use core::sync::atomic::Ordering;

use shm_artifact::{Artifact, ArtifactHead};
use shm_core::{
    doorbell_pair, BorrowJournal, ChunkCtrl, ChunkDesc, DoorbellPair, JournalRecord, Pool, Segment,
    FREE, LOANED, PUBLISHED,
};
use shm_ring::{required_bytes, DoorbellNotifier, Ring};
use shm_store::Catalog;
use shm_task::{now_nanos, ReapReport, TaskQueue};

use crate::config::{RuntimeConfig, STORE_ARTIFACT_ID_BASE};
use crate::error::{Error, Result};
use crate::protocol::{Request, Response};
use crate::uds::{recv_frame, send_frame};

/// A snapshot of a chunk's live control word (for tests / observability).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkSnapshot {
    /// Lifecycle state: [`FREE`], [`LOANED`], or [`PUBLISHED`].
    pub state: u32,
    /// Current recycle generation.
    pub generation: u32,
    /// Live shared-pin refcount.
    pub refcount: u32,
    /// Exclusive owner actor id, or [`OWNER_NONE`].
    pub owner: u32,
}

/// The liveness of a registered actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Liveness {
    /// Renewing its lease.
    Alive,
    /// Said goodbye; dropped without reclamation.
    Left,
    /// Lease expired; its journal has been replayed and reclaimed.
    Dead,
}

/// A registered actor's coordinator-side record.
struct ActorEntry {
    #[allow(dead_code)]
    name: String,
    journal_seg: Arc<Segment>,
    last_heartbeat: Instant,
    liveness: Liveness,
    /// Linux (ADR-0011): a pidfd watching the actor's process, obtained at
    /// registration (`SO_PEERCRED` → `pidfd_open`). The lease monitor parks its
    /// tick in `poll(2)` over these, so an actor's death is noticed
    /// near-instantly instead of a lease-deadline later. `None` when the pid or
    /// pidfd could not be obtained — leases remain the correctness backstop
    /// either way (a pidfd detects exit, never a wedged-but-alive actor, and
    /// the pid→pidfd_open window races pid reuse).
    #[cfg(target_os = "linux")]
    pidfd: Option<std::os::fd::OwnedFd>,
}

/// A topic's ring segment plus its broadcast doorbell.
struct TopicEntry {
    #[allow(dead_code)]
    index: u32,
    segment: Arc<Segment>,
    region_len: usize,
    /// The per-topic wakeup doorbell. The coordinator keeps **both** ends open
    /// for the topic's lifetime, granting a duplicate of the read-end to each
    /// subscriber and of the write-end to each publisher via `SCM_RIGHTS`.
    /// Retaining the write-end keeps subscribers' `poll` from ever seeing
    /// `POLLHUP` when the last publisher exits (liveness then rests on the
    /// bounded parker timeout).
    doorbell: DoorbellPair,
}

/// A coordinator-hosted artifact: its two segments plus its interned name id.
///
/// The coordinator [creates](shm_artifact::Artifact::create) the artifact's
/// head + data segments on first `OpenArtifact`, then grants their fds; every
/// [`Node`](crate::Node) merely [attaches](shm_artifact::Artifact::attach). The
/// coordinator keeps both segments mapped for the artifact's lifetime so it can
/// reap a dead producer's staged data chunks (which live in the *data* pool, not
/// the shared payload pool) and inspect the artifact's version/pool for tests.
struct ArtifactEntry {
    name_id: u32,
    #[allow(dead_code)]
    index: u32,
    head_seg: Arc<Segment>,
    data_seg: Arc<Segment>,
}

/// Mutable coordinator state, guarded by one mutex.
struct CoordState {
    next_actor_id: u32,
    next_topic_index: u32,
    next_artifact_index: u32,
    next_name_id: u32,
    actors: HashMap<u32, ActorEntry>,
    topics: HashMap<String, TopicEntry>,
    artifacts: HashMap<String, ArtifactEntry>,
    /// Interned artifact `name_id` → artifact name, so a replayed
    /// [`ArtifactPin`](shm_core::JournalRecord::ArtifactPin) (which carries only
    /// the id) can be routed back to its hosted [`ArtifactEntry`] (item J).
    artifact_name_by_id: HashMap<u32, String>,
    last_published: Option<ChunkDesc>,
    armed: bool,
    reclaimed: Vec<ChunkDesc>,
    /// Count of leaked artifact **version pins** reclaimed by journal replay
    /// (item J observability).
    artifact_pins_reclaimed: u64,
    /// Count of leaked artifact **write leases** force-released by journal replay
    /// (item K observability).
    write_leases_reclaimed: u64,
    /// Count of task-lifecycle-tied retained-ref **bindings** released by the
    /// coordinator's reap backstop (ADR-0010 P0.3 observability).
    task_bindings_reclaimed: u64,
    created_seg_ids: Vec<u32>,
    /// Master schema catalog (ADR-0003 item E): the process-independent
    /// [`shm_arrow::schema_content_hash`] → issued `schema_id`. The coordinator
    /// is the *authoritative* registry; nodes cache their learned mappings
    /// locally. Interning is by content hash, so an equal schema submitted by any
    /// process converges on one id — no identical seeding required.
    schema_ids: HashMap<u64, u32>,
    /// The reverse map `schema_id` → serialized schema bytes, answering
    /// [`ResolveSchema`](crate::protocol::Request::ResolveSchema).
    schema_bytes: HashMap<u32, Vec<u8>>,
    /// Next schema id to issue (monotonic). Starts at
    /// [`FIRST_USER_SCHEMA_ID`](shm_store::FIRST_USER_SCHEMA_ID) (16): ids `1..=15`
    /// are reserved as **system** ids (ADR-0007 §ABI — envelope =
    /// [`SCHEMA_TYPED_REF`](shm_store::SCHEMA_TYPED_REF)) and `0` is
    /// [`RAW_SCHEMA_ID`](shm_arrow::RAW_SCHEMA_ID).
    next_schema_id: u32,
    /// Master **key** catalog (ADR-0007 G3): opaque key bytes → issued `key_id`.
    /// The coordinator is authoritative; nodes cache learned mappings. Keying is
    /// by exact bytes, so an equal key from any process converges on one id.
    key_ids: HashMap<Vec<u8>, u32>,
    /// The reverse map `key_id` → key bytes, answering
    /// [`ResolveKey`](crate::protocol::Request::ResolveKey).
    key_bytes: HashMap<u32, Vec<u8>>,
    /// Next key id to issue (monotonic, starts at 1; `0` is reserved for "none").
    next_key_id: u32,
}

/// State shared across the accept thread, handler threads, and lease monitor.
struct CoordShared {
    config: RuntimeConfig,
    payload_seg: Arc<Segment>,
    /// The built-in [`shm_task`] task-queue segment.
    task_queue_seg: Arc<Segment>,
    /// A coordinator handle onto the task queue, wired to the work/done
    /// doorbells so a lease-driven [`reap`](TaskQueue::reap) wakes parked
    /// workers/requesters.
    task_queue: TaskQueue,
    /// The task queue's work doorbell (wakes idle workers on submit/requeue).
    /// The coordinator keeps both ends open for the queue's lifetime and grants
    /// duplicates to actors.
    work_db: DoorbellPair,
    /// The task queue's done doorbell (wakes parked requesters on completion).
    done_db: DoorbellPair,
    /// The keyed store's **catalog** segment (ADR-0007 G3). The coordinator
    /// created + initialised it and keeps it mapped so it can (a) grant its fd and
    /// (b) read the catalog to route a dead actor's leaked entry pins / write
    /// leases back to their `ArtifactHead` on crash.
    store_catalog_seg: Arc<Segment>,
    /// The keyed store's **head** (management) segment (packs every entry's
    /// `ArtifactHead`).
    store_head_seg: Arc<Segment>,
    /// The keyed store's **data** (shared pool) segment.
    store_data_seg: Arc<Segment>,
    state: Mutex<CoordState>,
    running: AtomicBool,
}

impl CoordShared {
    fn running(&self) -> bool {
        self.running.load(AtomicOrdering::Acquire)
    }
}

/// A running coordinator. Dropping it stops the background threads and unlinks
/// every segment name it created.
pub struct Coordinator {
    shared: Arc<CoordShared>,
    uds_path: PathBuf,
    threads: Vec<JoinHandle<()>>,
}

impl Coordinator {
    /// Bind a coordinator: create the payload segment + pool, and listen on
    /// `uds_path`. Background threads are not started until [`start`](Self::start).
    pub fn bind(uds_path: impl AsRef<Path>, config: RuntimeConfig) -> Result<Coordinator> {
        let uds_path = uds_path.as_ref().to_path_buf();
        // A stale socket path would make bind fail with EADDRINUSE.
        let _ = std::fs::remove_file(&uds_path);

        let payload_id = config.payload_seg_id();
        let payload_seg = Arc::new(create_segment(payload_id, config.payload_size)?);
        // Lay out the pool inside the payload segment.
        Pool::create(&payload_seg, &config.pool)?;

        // Built-in task queue: one segment sized to hold its header + slot array,
        // plus its two doorbells (work = wake workers, done = wake requesters).
        let task_queue_id = config.task_queue_seg_id();
        let taskq_region = shm_task::required_bytes(config.task_capacity);
        let task_queue_seg = Arc::new(create_segment(
            task_queue_id,
            shm_core::segment::HEADER_SIZE + taskq_region,
        )?);
        // SAFETY: `payload_ptr()` is 64-byte aligned and backs `payload_len()`
        // writable bytes we exclusively own here; nothing else initializes it.
        unsafe {
            TaskQueue::init_with_max_retries(
                task_queue_seg.payload_ptr(),
                task_queue_seg.payload_len(),
                config.task_capacity,
                config.task_max_retries,
            )?;
        }
        let work_db = doorbell_pair()?;
        let done_db = doorbell_pair()?;

        // Built-in keyed store (ADR-0007 G3): three segments — the catalog (shm
        // fast-path index), the head management segment (packs every entry's
        // ArtifactHead), and the data segment (one shared pool). The coordinator
        // initialises the catalog + data pool; entries' heads are laid lazily on
        // `create`. Each head slot is 64-byte aligned for Arrow-friendly access.
        let head_stride = (ArtifactHead::region_bytes() + 63) & !63;
        let store_catalog_id = config.store_catalog_seg_id();
        let store_head_id = config.store_head_seg_id();
        let store_data_id = config.store_data_seg_id();
        let store_catalog_seg = Arc::new(create_segment(
            store_catalog_id,
            Catalog::segment_bytes(config.store_capacity),
        )?);
        Catalog::init(
            &store_catalog_seg,
            config.store_capacity,
            head_stride as u32,
            STORE_ARTIFACT_ID_BASE,
        )?;
        let store_head_seg = Arc::new(create_segment(
            store_head_id,
            shm_core::segment::HEADER_SIZE + config.store_capacity as usize * head_stride,
        )?);
        let store_data_seg = Arc::new(create_segment(store_data_id, config.store_data_size)?);
        Pool::create(&store_data_seg, &config.store_pool)?;
        // The coordinator's own queue handle, wired so reap can ring the
        // doorbells. Its notifiers borrow the write-ends the coordinator retains.
        // SAFETY: the region was just initialized above and stays mapped for the
        // shared struct's lifetime (it owns `task_queue_seg`).
        let task_queue = unsafe {
            TaskQueue::attach(task_queue_seg.payload_ptr(), task_queue_seg.payload_len())?
        }
        .with_work_notifier(DoorbellNotifier::new(work_db.write.as_raw_fd()))
        .with_done_notifier(DoorbellNotifier::new(done_db.write.as_raw_fd()));

        let shared = Arc::new(CoordShared {
            config,
            payload_seg,
            task_queue_seg,
            task_queue,
            work_db,
            done_db,
            store_catalog_seg,
            store_head_seg,
            store_data_seg,
            state: Mutex::new(CoordState {
                next_actor_id: 1,
                next_topic_index: 0,
                next_artifact_index: 0,
                next_name_id: 1,
                actors: HashMap::new(),
                topics: HashMap::new(),
                artifacts: HashMap::new(),
                artifact_name_by_id: HashMap::new(),
                last_published: None,
                armed: false,
                reclaimed: Vec::new(),
                artifact_pins_reclaimed: 0,
                write_leases_reclaimed: 0,
                task_bindings_reclaimed: 0,
                created_seg_ids: vec![
                    payload_id,
                    task_queue_id,
                    store_catalog_id,
                    store_head_id,
                    store_data_id,
                ],
                schema_ids: HashMap::new(),
                schema_bytes: HashMap::new(),
                next_schema_id: shm_store::FIRST_USER_SCHEMA_ID,
                key_ids: HashMap::new(),
                key_bytes: HashMap::new(),
                next_key_id: 1,
            }),
            running: AtomicBool::new(true),
        });

        Ok(Coordinator {
            shared,
            uds_path,
            threads: Vec::new(),
        })
    }

    /// Start the accept loop and the lease monitor on background threads.
    pub fn start(&mut self) -> Result<()> {
        // Pre-create the built-in `__artifacts` topic so a watcher can subscribe
        // (and park on its doorbell) before any producer has committed. This is
        // just an ordinary broadcast ring the runtime routes `VersionEvent`s on.
        {
            let mut st = self.shared.state.lock().unwrap();
            let _ =
                get_or_create_topic(&mut st, &self.shared.config, shm_artifact::ARTIFACTS_TOPIC)?;
        }

        let listener = UnixListener::bind(&self.uds_path)?;
        listener.set_nonblocking(true)?;

        // Accept loop.
        let accept_shared = self.shared.clone();
        let accept = std::thread::spawn(move || accept_loop(accept_shared, listener));

        // Lease monitor.
        let monitor_shared = self.shared.clone();
        let monitor = std::thread::spawn(move || lease_monitor(monitor_shared));

        self.threads.push(accept);
        self.threads.push(monitor);
        Ok(())
    }

    /// The control socket path.
    pub fn uds_path(&self) -> &Path {
        &self.uds_path
    }

    /// The runtime configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.shared.config
    }

    /// The most recently published descriptor the coordinator was told about.
    pub fn last_published(&self) -> Option<ChunkDesc> {
        self.shared.state.lock().unwrap().last_published
    }

    /// Whether a consumer has pinned the published chunk and the coordinator has
    /// released the producer's exclusive ownership (the reclaim is now "armed":
    /// only the consumer's pin keeps the chunk alive).
    pub fn is_armed(&self) -> bool {
        self.shared.state.lock().unwrap().armed
    }

    /// Descriptors reclaimed so far by lease-driven journal replay.
    pub fn reclaimed(&self) -> Vec<ChunkDesc> {
        self.shared.state.lock().unwrap().reclaimed.clone()
    }

    /// The number of registered actors currently alive.
    pub fn alive_actor_count(&self) -> usize {
        self.shared
            .state
            .lock()
            .unwrap()
            .actors
            .values()
            .filter(|a| a.liveness == Liveness::Alive)
            .count()
    }

    /// Snapshot a chunk's live control word by descriptor (reads the pool ctrl).
    pub fn chunk_snapshot(&self, desc: &ChunkDesc) -> Option<ChunkSnapshot> {
        let pool = Pool::attach(&self.shared.payload_seg).ok()?;
        let ctrl = pool.ctrl(desc).ok()?;
        Some(snapshot_ctrl(ctrl))
    }

    /// Validate a descriptor against the live chunk generation (mirrors
    /// [`ChunkCtrl::validate`]); a recycled chunk yields
    /// [`shm_core::Error::StaleDescriptor`].
    pub fn validate_desc(&self, desc: &ChunkDesc) -> Result<()> {
        let pool = Pool::attach(&self.shared.payload_seg)?;
        let ctrl = pool.ctrl(desc)?;
        ctrl.validate(desc)?;
        Ok(())
    }

    /// Free chunks currently on the given size class's free list (test helper).
    pub fn free_count(&self, class_idx: usize) -> Option<usize> {
        let pool = Pool::attach(&self.shared.payload_seg).ok()?;
        Some(pool.free_count(class_idx))
    }

    /// Force an immediate lease expiry + reclaim of `actor_id` (drives the exact
    /// crash-reclaim code path without waiting on the lease clock; used by the
    /// deterministic same-process test).
    pub fn force_reclaim(&self, actor_id: u32) -> Result<Vec<ChunkDesc>> {
        let jseg = {
            let mut st = self.shared.state.lock().unwrap();
            let entry = st
                .actors
                .get_mut(&actor_id)
                .ok_or(Error::NotFound("actor"))?;
            entry.liveness = Liveness::Dead;
            entry.journal_seg.clone()
        };
        let reclaimed = reclaim_dead(&self.shared, actor_id, &jseg);
        let mut st = self.shared.state.lock().unwrap();
        st.reclaimed.extend(reclaimed.iter().copied());
        Ok(reclaimed)
    }

    /// The current (latest installed) version of a hosted artifact, or `None` if
    /// the artifact is not (yet) known to the coordinator. `0` means "no version
    /// committed yet".
    pub fn artifact_current_version(&self, name: &str) -> Option<u64> {
        let (name_id, head, data) = {
            let st = self.shared.state.lock().unwrap();
            let a = st.artifacts.get(name)?;
            (a.name_id, a.head_seg.clone(), a.data_seg.clone())
        };
        Artifact::attach(name_id, head, data)
            .ok()
            .map(|a| a.current_version())
    }

    /// Free-chunk count of a hosted artifact's data pool for `class_idx` (test /
    /// observability helper), or `None` if the artifact is unknown.
    pub fn artifact_free_count(&self, name: &str, class_idx: usize) -> Option<usize> {
        let data = {
            let st = self.shared.state.lock().unwrap();
            st.artifacts.get(name)?.data_seg.clone()
        };
        Pool::attach(&data).ok().map(|p| p.free_count(class_idx))
    }

    /// Total free chunks across **all** size classes of a hosted artifact's data
    /// pool, or `None` if the artifact is unknown. Useful when a caller does not
    /// know which class a staged chunk landed in.
    pub fn artifact_free_total(&self, name: &str) -> Option<usize> {
        let data = {
            let st = self.shared.state.lock().unwrap();
            st.artifacts.get(name)?.data_seg.clone()
        };
        let pool = Pool::attach(&data).ok()?;
        Some((0..pool.num_classes()).map(|c| pool.free_count(c)).sum())
    }

    /// The live pin count on a hosted artifact's slot for `version`, or `None`
    /// if the artifact is unknown or no live slot tracks that version (never
    /// committed, or already reclaimed). Lets a test prove a leaked version pin
    /// was decremented by crash replay (item J).
    pub fn artifact_slot_pins(&self, name: &str, version: u64) -> Option<u32> {
        let (name_id, head, data) = {
            let st = self.shared.state.lock().unwrap();
            let a = st.artifacts.get(name)?;
            (a.name_id, a.head_seg.clone(), a.data_seg.clone())
        };
        Artifact::attach(name_id, head, data)
            .ok()
            .and_then(|a| a.version_pin_count(version))
    }

    /// Count of leaked artifact **version pins** reclaimed by lease-driven
    /// journal replay so far (item J observability).
    pub fn artifact_pins_reclaimed(&self) -> u64 {
        self.shared.state.lock().unwrap().artifact_pins_reclaimed
    }

    /// Count of leaked artifact **write leases** force-released by lease-driven
    /// journal replay so far (item K observability).
    pub fn write_leases_reclaimed(&self) -> u64 {
        self.shared.state.lock().unwrap().write_leases_reclaimed
    }

    /// Count of task-lifecycle-tied retained-ref bindings released by the
    /// coordinator's reap backstop (ADR-0010 P0.3 observability).
    pub fn task_bindings_reclaimed(&self) -> u64 {
        self.shared.state.lock().unwrap().task_bindings_reclaimed
    }

    /// Drive one task-**binding** reap sweep (ADR-0010 P0.3) with an explicit
    /// grace period, releasing each won binding against the keyed store and
    /// returning how many were released. Exposes the exact backstop path the
    /// lease monitor runs each tick (which uses
    /// [`RuntimeConfig::task_binding_grace`]) so a test can drive it
    /// deterministically.
    pub fn reap_task_bindings_with_grace(&self, now_nanos: u64, grace_nanos: u64) -> usize {
        reap_task_bindings(&self.shared, now_nanos, grace_nanos)
    }

    /// The current exclusive write-lease owner actor id on a hosted artifact, or
    /// `None` if the artifact is unknown. `0` means the lease is free. Lets a test
    /// prove a dead exclusive writer's lease was force-released by crash replay
    /// (item K).
    pub fn artifact_write_lease_owner(&self, name: &str) -> Option<u32> {
        let (name_id, head, data) = {
            let st = self.shared.state.lock().unwrap();
            let a = st.artifacts.get(name)?;
            (a.name_id, a.head_seg.clone(), a.data_seg.clone())
        };
        Artifact::attach(name_id, head, data)
            .ok()
            .map(|a| a.write_lease_owner())
    }

    /// Intern serialized schema bytes directly in the coordinator's master
    /// catalog (ADR-0003 item E), returning the stable id. Exposes the exact
    /// interning path a node's `InternSchema` request drives, for deterministic
    /// same-process tests of the idempotency contract.
    pub fn intern_schema_bytes(&self, schema_bytes: &[u8]) -> u32 {
        intern_schema(&self.shared, schema_bytes)
    }

    /// Intern store key bytes directly in the coordinator's master key catalog
    /// (ADR-0007 G3), returning the stable id — the exact path a node's
    /// `InternKey` request drives, for deterministic same-process tests.
    pub fn intern_key_bytes(&self, key_bytes: &[u8]) -> u32 {
        intern_key(&self.shared, key_bytes)
    }

    /// Resolve a coordinator-issued `key_id` back to its key bytes, or `None`.
    pub fn resolve_key_bytes(&self, key_id: u32) -> Option<Vec<u8>> {
        resolve_key_bytes(&self.shared, key_id)
    }

    /// Total free chunks across **all** size classes of the keyed store's shared
    /// data pool (ADR-0007 G3), for a leak census in tests. A create + commit
    /// consumes chunks; a full evict + reclaim returns this to its baseline.
    pub fn store_data_free_total(&self) -> Option<usize> {
        let pool = Pool::attach(&self.shared.store_data_seg).ok()?;
        Some((0..pool.num_classes()).map(|c| pool.free_count(c)).sum())
    }

    /// The current (latest) version of the keyed-store entry for `key`, or `None`
    /// if no live entry exists. `0` means "created but nothing committed".
    pub fn store_entry_version(&self, key: &[u8]) -> Option<u64> {
        self.store_entry_artifact(key).map(|a| a.current_version())
    }

    /// The live pin count on the keyed-store entry for `key` at `version`, or
    /// `None` if no live entry / no live slot tracks it. Lets a test prove a
    /// leaked entry pin was decremented by crash replay (ADR-0007 G3 × item J).
    pub fn store_entry_pins(&self, key: &[u8], version: u64) -> Option<u32> {
        self.store_entry_artifact(key)
            .and_then(|a| a.version_pin_count(version))
    }

    /// Attach an [`Artifact`] handle onto the live keyed-store entry for `key` (by
    /// interned key id → catalog slot → `ArtifactHead`), or `None` if none is live.
    fn store_entry_artifact(&self, key: &[u8]) -> Option<Artifact> {
        let key_id = *self.shared.state.lock().unwrap().key_ids.get(key)?;
        let cat = Catalog::attach(&self.shared.store_catalog_seg).ok()?;
        let idx = cat.find_live_by_key(key_id)?;
        let head_off = cat.slot(idx).head_off() as usize;
        Artifact::attach_at(
            cat.artifact_id_for(idx),
            self.shared.store_head_seg.clone(),
            head_off,
            self.shared.store_data_seg.clone(),
        )
        .ok()
    }

    /// Resolve a coordinator-issued `schema_id` back to its serialized schema
    /// bytes (test/observability), or `None` if never interned.
    pub fn resolve_schema_bytes(&self, schema_id: u32) -> Option<Vec<u8>> {
        resolve_schema_bytes(&self.shared, schema_id)
    }

    /// Drive one task-queue [`reap`](TaskQueue::reap) sweep at `now_nanos`,
    /// returning what it did. Exposes the exact lease-driven requeue path the
    /// monitor runs, for deterministic same-process tests.
    pub fn reap_tasks(&self, now_nanos: u64) -> ReapReport {
        self.shared.task_queue.reap(now_nanos)
    }

    /// Drive one keyed-store tombstone sweep, returning how many slots it
    /// returned to the free list (ADR-0008 P0.1). Exposes the exact path the
    /// monitor runs, for deterministic same-process tests.
    pub fn sweep_store_tombstones(&self) -> usize {
        sweep_store_tombstones(&self.shared)
    }

    /// The lifecycle state of every **appended** store catalog slot
    /// (`SLOT_FREE`/`SLOT_LIVE`/`SLOT_TOMBSTONE`/`SLOT_RECLAIMING`), in slot
    /// order. Observability for the ADR-0008 P0.1 reclamation census: a test
    /// proves an evicted slot *returned to `FREE`* — not merely that its chunks
    /// came back — before trusting the store to survive churn.
    pub fn store_slot_states(&self) -> Vec<u32> {
        let Ok(cat) = Catalog::attach(&self.shared.store_catalog_seg) else {
            return Vec::new();
        };
        let n = cat.next_slot().min(cat.capacity() as u32);
        (0..n).map(|i| cat.slot(i).state()).collect()
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        self.shared.running.store(false, AtomicOrdering::Release);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
        // Unlink every segment name we created (existing mappings stay valid).
        let ids: Vec<u32> = {
            let st = self.shared.state.lock().unwrap();
            st.created_seg_ids.clone()
        };
        for id in ids {
            let _ = Segment::unlink_by_id(id);
        }
        let _ = std::fs::remove_file(&self.uds_path);
    }
}

// ---- Accept + per-connection handling ----

/// Accept connections until shutdown, spawning a handler thread per actor.
fn accept_loop(shared: Arc<CoordShared>, listener: UnixListener) {
    let tick = shared.config.monitor_tick;
    while shared.running() {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let s = shared.clone();
                std::thread::spawn(move || {
                    if let Err(err) = handle_connection(s, stream) {
                        // Connection errors are non-fatal; the lease monitor
                        // governs liveness, not the socket state. Surface them
                        // only under an opt-in debug env var.
                        if std::env::var_os("SHM_DEBUG").is_some() {
                            eprintln!("coordinator: handler error: {err}");
                        }
                    }
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(tick);
            }
            Err(_) => break,
        }
    }
}

/// Drive one actor connection: register, then service requests until goodbye,
/// EOF, or shutdown. EOF does **not** declare death (leases do).
fn handle_connection(shared: Arc<CoordShared>, stream: UnixStream) -> Result<()> {
    // A read timeout lets the handler notice shutdown and lets a `kill -9`ed
    // actor's socket simply go quiet without us mistaking it for death.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(shared.config.monitor_tick))?;

    let mut actor_id: Option<u32> = None;

    while shared.running() {
        let frame = match recv_frame(&stream) {
            Ok(f) => f,
            Err(Error::PeerClosed) => break,
            Err(Error::Nix(errno)) if is_timeout(errno) => continue,
            Err(Error::Io(ref e)) if is_io_timeout(e) => continue,
            Err(e) => return Err(e),
        };
        let req = Request::decode(&frame.body)?;
        match req {
            Request::Register { name } => {
                let id = register_actor(&shared, &name, &stream)?;
                actor_id = Some(id);
            }
            Request::Heartbeat => {
                if let Some(id) = actor_id {
                    let mut st = shared.state.lock().unwrap();
                    if let Some(a) = st.actors.get_mut(&id) {
                        a.last_heartbeat = Instant::now();
                    }
                }
            }
            Request::CreateTopic { topic } => {
                grant_topic(&shared, &topic, &stream, false)?;
            }
            Request::Subscribe { topic } => {
                grant_topic(&shared, &topic, &stream, true)?;
            }
            Request::Published { desc } => {
                shared.state.lock().unwrap().last_published = Some(desc);
            }
            Request::Pinned { desc } => {
                handle_pinned(&shared, &desc);
            }
            Request::OpenArtifact { name } => {
                grant_artifact(&shared, &name, &stream)?;
            }
            Request::OpenTaskQueue => {
                grant_task_queue(&shared, &stream)?;
            }
            Request::InternSchema { schema_bytes } => {
                let schema_id = intern_schema(&shared, &schema_bytes);
                let resp = Response::SchemaInterned { schema_id };
                send_frame(&stream, &resp.encode(), &[])?;
            }
            Request::ResolveSchema { schema_id } => {
                let resp = match resolve_schema_bytes(&shared, schema_id) {
                    Some(schema_bytes) => Response::SchemaResolved { schema_bytes },
                    None => Response::Error {
                        message: format!("unknown schema_id {schema_id}"),
                    },
                };
                send_frame(&stream, &resp.encode(), &[])?;
            }
            Request::InternKey { key_bytes } => {
                let key_id = intern_key(&shared, &key_bytes);
                send_frame(&stream, &Response::KeyInterned { key_id }.encode(), &[])?;
            }
            Request::ResolveKey { key_id } => {
                let resp = match resolve_key_bytes(&shared, key_id) {
                    Some(key_bytes) => Response::KeyResolved { key_bytes },
                    None => Response::Error {
                        message: format!("unknown key_id {key_id}"),
                    },
                };
                send_frame(&stream, &resp.encode(), &[])?;
            }
            Request::OpenStore => {
                grant_store(&shared, &stream)?;
            }
            Request::Bye => {
                if let Some(id) = actor_id {
                    let mut st = shared.state.lock().unwrap();
                    if let Some(a) = st.actors.get_mut(&id) {
                        a.liveness = Liveness::Left;
                    }
                }
                break;
            }
        }
    }
    Ok(())
}

/// Register a new actor: allocate an id, create + pass its journal segment plus
/// the shared payload segment.
fn register_actor(shared: &Arc<CoordShared>, name: &str, stream: &UnixStream) -> Result<u32> {
    let config = &shared.config;
    let (actor_id, journal_seg_id) = {
        let mut st = shared.state.lock().unwrap();
        let id = st.next_actor_id;
        st.next_actor_id += 1;
        (id, config.journal_seg_id(id))
    };

    // One borrow-journal segment per actor.
    let jsize = journal_segment_size(config.journal_capacity);
    let journal_seg = Arc::new(create_segment(journal_seg_id, jsize)?);
    BorrowJournal::create(&journal_seg, config.journal_capacity)?;

    {
        let mut st = shared.state.lock().unwrap();
        st.created_seg_ids.push(journal_seg_id);
        st.actors.insert(
            actor_id,
            ActorEntry {
                name: name.into(),
                journal_seg: journal_seg.clone(),
                last_heartbeat: Instant::now(),
                liveness: Liveness::Alive,
                #[cfg(target_os = "linux")]
                pidfd: peer_pidfd(stream),
            },
        );
    }

    // Pass the payload fd then the journal fd (order the actor relies on).
    let resp = Response::Registered {
        actor_id,
        journal_idx: actor_id,
        payload_seg_id: config.payload_seg_id(),
        journal_seg_id,
    };
    let fds = [shared.payload_seg.as_raw_fd(), journal_seg.as_raw_fd()];
    send_frame(stream, &resp.encode(), &fds)?;
    Ok(actor_id)
}

/// Ensure a topic's ring segment (and doorbell) exists and pass their fds to
/// the requester.
///
/// Get-or-create is performed under a single lock hold so two actors racing to
/// reference the same topic (a producer's `CreateTopic` and a consumer's
/// `Subscribe`) always converge on **one** ring segment and **one** doorbell —
/// otherwise a publisher and subscriber could end up on different rings/pipes
/// and never meet.
///
/// The requester's doorbell end depends on its role: a `subscribe`r receives
/// the pipe **read-end** (to `poll`/drain), a publisher the **write-end** (to
/// ring). `SCM_RIGHTS` installs a *duplicate* in the receiver; the coordinator
/// retains its own copy of both ends.
fn grant_topic(
    shared: &Arc<CoordShared>,
    topic: &str,
    stream: &UnixStream,
    subscribe: bool,
) -> Result<()> {
    let (ring_seg_id, ring_fd, region_len, read_fd, write_fd) = {
        let mut st = shared.state.lock().unwrap();
        get_or_create_topic(&mut st, &shared.config, topic)?
    };
    // The requester's doorbell end depends on its role.
    let doorbell_fd = if subscribe { read_fd } else { write_fd };

    let resp = Response::Granted {
        ring_seg_id,
        region_len: region_len as u32,
    };
    // fd order the actor relies on: ring segment first, then the doorbell.
    send_frame(stream, &resp.encode(), &[ring_fd, doorbell_fd])?;
    Ok(())
}

/// Get-or-create a topic's ring segment + doorbell under the held state lock,
/// returning `(ring_seg_id, ring_fd, region_len, read_fd, write_fd)`.
///
/// Factored so both [`grant_topic`] and the coordinator's `start` (which
/// pre-creates the built-in `__artifacts` topic) converge on **one** ring +
/// **one** doorbell per name. The returned fds are the coordinator's retained
/// ends; `SCM_RIGHTS` installs duplicates in receivers.
fn get_or_create_topic(
    st: &mut CoordState,
    config: &RuntimeConfig,
    topic: &str,
) -> Result<(u32, RawFd, usize, RawFd, RawFd)> {
    if let Some(t) = st.topics.get(topic) {
        return Ok((
            t.segment.id(),
            t.segment.as_raw_fd(),
            t.region_len,
            t.doorbell.read.as_raw_fd(),
            t.doorbell.write.as_raw_fd(),
        ));
    }
    let region_len = required_bytes(config.ring_capacity);
    let idx = st.next_topic_index;
    st.next_topic_index += 1;
    let ring_seg_id = config.ring_seg_id(idx);
    let seg_size = shm_core::segment::HEADER_SIZE + region_len;
    let ring_seg = Arc::new(create_segment(ring_seg_id, seg_size)?);
    // SAFETY: `payload_ptr()` is 64-byte aligned and backs `region_len` writable
    // bytes we exclusively own here; no other party initializes this region (we
    // hold the state lock).
    unsafe {
        Ring::init(ring_seg.payload_ptr(), region_len, config.ring_capacity)?;
    }
    let ring_fd = ring_seg.as_raw_fd();
    // One broadcast doorbell per topic, created alongside the ring.
    let doorbell = doorbell_pair()?;
    let read_fd = doorbell.read.as_raw_fd();
    let write_fd = doorbell.write.as_raw_fd();
    st.created_seg_ids.push(ring_seg_id);
    st.topics.insert(
        topic.into(),
        TopicEntry {
            index: idx,
            segment: ring_seg,
            region_len,
            doorbell,
        },
    );
    Ok((ring_seg_id, ring_fd, region_len, read_fd, write_fd))
}

/// Get-or-create the named artifact and grant its head + data segment fds.
///
/// On first reference the coordinator interns a stable `name_id`, creates the
/// head + data segments, and runs [`Artifact::create`] (laying the pool + head)
/// so that every actor merely [attaches](shm_artifact::Artifact::attach). The
/// two fds are sent head-first, data-second — the order the actor relies on.
fn grant_artifact(shared: &Arc<CoordShared>, name: &str, stream: &UnixStream) -> Result<()> {
    let config = &shared.config;
    let (name_id, head_seg_id, data_seg_id, head_fd, data_fd) = {
        let mut st = shared.state.lock().unwrap();
        if let Some(a) = st.artifacts.get(name) {
            (
                a.name_id,
                a.head_seg.id(),
                a.data_seg.id(),
                a.head_seg.as_raw_fd(),
                a.data_seg.as_raw_fd(),
            )
        } else {
            let index = st.next_artifact_index;
            st.next_artifact_index += 1;
            let name_id = st.next_name_id;
            st.next_name_id += 1;

            let head_seg_id = config.artifact_head_seg_id(index);
            let data_seg_id = alloc_artifact_data_id();
            let head_size =
                shm_core::segment::HEADER_SIZE + shm_artifact::ArtifactHead::region_bytes();
            let head_seg = Arc::new(create_segment(head_seg_id, head_size)?);
            let data_seg = Arc::new(create_segment(data_seg_id, config.artifact_data_size)?);
            // Initialise the artifact (pool + head) once, coordinator-side.
            Artifact::create(
                name_id,
                head_seg.clone(),
                data_seg.clone(),
                &config.artifact_pool,
            )?;

            let head_fd = head_seg.as_raw_fd();
            let data_fd = data_seg.as_raw_fd();
            st.created_seg_ids.push(head_seg_id);
            st.created_seg_ids.push(data_seg_id);
            st.artifact_name_by_id.insert(name_id, name.to_string());
            st.artifacts.insert(
                name.into(),
                ArtifactEntry {
                    name_id,
                    index,
                    head_seg,
                    data_seg,
                },
            );
            (name_id, head_seg_id, data_seg_id, head_fd, data_fd)
        }
    };

    let resp = Response::ArtifactGranted {
        name_id,
        head_seg_id,
        data_seg_id,
    };
    send_frame(stream, &resp.encode(), &[head_fd, data_fd])?;
    Ok(())
}

/// Grant the built-in task queue: its segment fd plus both doorbells' read and
/// write ends (work read, work write, done read, done write), in that order.
///
/// A general actor is both a worker and a submitter/requester, so it receives
/// every end; the [`Node`](crate::Node) picks read-ends for parking and
/// write-ends for its notifiers.
fn grant_task_queue(shared: &Arc<CoordShared>, stream: &UnixStream) -> Result<()> {
    let seg = &shared.task_queue_seg;
    let resp = Response::TaskQueueGranted {
        queue_seg_id: seg.id(),
        region_len: seg.payload_len() as u32,
    };
    let fds = [
        seg.as_raw_fd(),
        shared.work_db.read.as_raw_fd(),
        shared.work_db.write.as_raw_fd(),
        shared.done_db.read.as_raw_fd(),
        shared.done_db.write.as_raw_fd(),
    ];
    send_frame(stream, &resp.encode(), &fds)?;
    Ok(())
}

// ---- Schema coordinator (ADR-0003 item E) ----

/// Intern a serialized Arrow schema in the coordinator's master catalog,
/// returning its stable coordinator-issued id.
///
/// Interning is keyed by the process-independent
/// [`shm_arrow::schema_content_hash`] of the received bytes, so the operation is
/// **idempotent**: the same schema (from the same or a *different* process)
/// always yields the same id, while a never-seen schema mints the next monotonic
/// id (>= 1; `0` is reserved for raw bytes). The coordinator stores the bytes
/// verbatim for a later [`resolve_schema_bytes`] — it never needs to deserialize.
fn intern_schema(shared: &Arc<CoordShared>, schema_bytes: &[u8]) -> u32 {
    let hash = shm_arrow::schema_content_hash(schema_bytes);
    let mut st = shared.state.lock().unwrap();
    if let Some(&id) = st.schema_ids.get(&hash) {
        return id;
    }
    let id = st.next_schema_id;
    st.next_schema_id += 1;
    st.schema_ids.insert(hash, id);
    st.schema_bytes.insert(id, schema_bytes.to_vec());
    id
}

/// Resolve a coordinator-issued `schema_id` back to its serialized schema bytes,
/// or `None` if the id was never interned.
fn resolve_schema_bytes(shared: &Arc<CoordShared>, schema_id: u32) -> Option<Vec<u8>> {
    shared
        .state
        .lock()
        .unwrap()
        .schema_bytes
        .get(&schema_id)
        .cloned()
}

// ---- Key coordinator (ADR-0007 G3) ----

/// Intern opaque store key bytes in the coordinator's master catalog, returning a
/// stable id. Idempotent by exact bytes (the same key from any process converges
/// on one id), mirroring the schema-interning path; `0` is reserved for "none".
fn intern_key(shared: &Arc<CoordShared>, key_bytes: &[u8]) -> u32 {
    let mut st = shared.state.lock().unwrap();
    if let Some(&id) = st.key_ids.get(key_bytes) {
        return id;
    }
    let id = st.next_key_id;
    st.next_key_id += 1;
    st.key_ids.insert(key_bytes.to_vec(), id);
    st.key_bytes.insert(id, key_bytes.to_vec());
    id
}

/// Resolve a coordinator-issued `key_id` back to its key bytes, or `None`.
fn resolve_key_bytes(shared: &Arc<CoordShared>, key_id: u32) -> Option<Vec<u8>> {
    shared.state.lock().unwrap().key_bytes.get(&key_id).cloned()
}

/// Grant the built-in keyed store: its catalog, head, and data segment fds (in
/// that order), plus each segment's id + payload length (ADR-0007 G3).
///
/// The node reads the catalog's capacity / head-stride / lineage base from the
/// catalog header after mapping, so those are not carried in the frame.
fn grant_store(shared: &Arc<CoordShared>, stream: &UnixStream) -> Result<()> {
    let cat = &shared.store_catalog_seg;
    let head = &shared.store_head_seg;
    let data = &shared.store_data_seg;
    let resp = Response::StoreGranted {
        catalog_seg_id: cat.id(),
        catalog_len: cat.payload_len() as u32,
        head_seg_id: head.id(),
        head_len: head.payload_len() as u32,
        data_seg_id: data.id(),
        data_len: data.payload_len() as u32,
    };
    // fd order the node relies on: catalog, head, data.
    let fds = [cat.as_raw_fd(), head.as_raw_fd(), data.as_raw_fd()];
    send_frame(stream, &resp.encode(), &fds)?;
    Ok(())
}

/// Handle a consumer's `Pinned`: the chunk now has a live shared pin, so it is
/// safe to release the producer's exclusive ownership (no reclaim race). This
/// "arms" the chunk so that when the pinning actor dies, its pin is the only
/// hold and the chunk reclaims cleanly.
fn handle_pinned(shared: &Arc<CoordShared>, desc: &ChunkDesc) {
    if let Ok(pool) = Pool::attach(&shared.payload_seg) {
        if let Ok(ctrl) = pool.ctrl(desc) {
            // Only release ownership while a pin is actually held.
            if ctrl.state() == PUBLISHED && ctrl.refcount() > 0 {
                ctrl.owner_release();
            }
        }
    }
    shared.state.lock().unwrap().armed = true;
}

// ---- Lease monitor + crash reclamation ----

/// Park the lease monitor for one tick.
///
/// Non-Linux: a plain sleep; kernel death notification does not exist, so the
/// returned set is always empty and leases alone govern death (the
/// [`DeathDetection::LeaseBased`](shm_core::DeathDetection) policy).
#[cfg(not(target_os = "linux"))]
fn wait_tick(_shared: &Arc<CoordShared>, tick: Duration) -> Vec<u32> {
    std::thread::sleep(tick);
    Vec::new()
}

/// Park the lease monitor for one tick — in `poll(2)` over every alive,
/// registered actor's pidfd (Linux, ADR-0011), so a watched actor's exit ends
/// the tick early. Returns the actor ids whose pidfd became readable (the
/// kernel's exit notification); the caller drives them through the exact same
/// mark-dead → journal-replay reclaim path a lease expiry does.
#[cfg(target_os = "linux")]
fn wait_tick(shared: &Arc<CoordShared>, tick: Duration) -> Vec<u32> {
    use std::os::fd::{AsRawFd, RawFd};
    let watched: Vec<(u32, RawFd)> = {
        let st = shared.state.lock().unwrap();
        st.actors
            .iter()
            .filter(|(_, a)| a.liveness == Liveness::Alive)
            .filter_map(|(id, a)| a.pidfd.as_ref().map(|fd| (*id, fd.as_raw_fd())))
            .collect()
    };
    if watched.is_empty() {
        std::thread::sleep(tick);
        return Vec::new();
    }
    let mut pfds: Vec<libc::pollfd> = watched
        .iter()
        .map(|(_, fd)| libc::pollfd {
            fd: *fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    let ms = tick.as_millis().min(i32::MAX as u128) as libc::c_int;
    // SAFETY: `pfds` is a valid, exclusively-borrowed pollfd array; `poll` only
    // reads/writes those entries. The fds stay open for the call: the OwnedFds
    // live in `state.actors`, which only drops entries under the same lock, and
    // even a racing close would only yield POLLNVAL (filtered out below).
    let n = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, ms) };
    if n <= 0 {
        // Timeout, EINTR, or error: report nothing; the lease clock still runs.
        return Vec::new();
    }
    watched
        .iter()
        .zip(&pfds)
        // A pidfd reports exit as POLLIN; include POLLERR/POLLHUP defensively.
        .filter(|(_, p)| p.revents & (libc::POLLIN | libc::POLLERR | libc::POLLHUP) != 0)
        .map(|((id, _), _)| *id)
        .collect()
}

/// Obtain a pidfd for the peer of a freshly-registered actor's control socket
/// (Linux, ADR-0011): `SO_PEERCRED` names the pid, `pidfd_open` watches it.
/// Best-effort: `None` on any failure (leases are the backstop). An in-process
/// node yields a pidfd on the coordinator's own pid — harmless, it never fires.
#[cfg(target_os = "linux")]
fn peer_pidfd(stream: &UnixStream) -> Option<std::os::fd::OwnedFd> {
    use std::os::fd::AsRawFd;
    let pid = shm_core::platform_linux::socket_peer_pid(stream.as_raw_fd()).ok()?;
    shm_core::platform_linux::pidfd_open(pid).ok()
}

/// Periodically expire leases, reclaim dead actors' journals, and reap lapsed
/// task claims.
///
/// Task reap is tied to the same lease clock: a worker refreshes its task's
/// deadline on claim (see [`TaskQueue::claim_with_lease`]), so a task whose
/// deadline has passed is one whose claiming worker's lease has lapsed. Each
/// tick runs a [`reap`](TaskQueue::reap); a freshly-detected actor death also
/// triggers an immediate reap so a dead worker's `CLAIMED` task is requeued
/// promptly (at-least-once) rather than waiting a whole tick.
fn lease_monitor(shared: Arc<CoordShared>) {
    let tick = shared.config.monitor_tick;
    let deadline = shared.config.lease_deadline;
    while shared.running() {
        // One monitor tick. On Linux the sleep is a `poll(2)` over the
        // registered actors' pidfds (ADR-0011), so a watched actor's exit ends
        // the tick early and names it; elsewhere it is a plain sleep and the
        // returned set is always empty. Either way the lease clock below stays
        // the correctness backstop — a pidfd never fires for a wedged-but-alive
        // actor, and not every actor has one.
        let kernel_dead = wait_tick(&shared, tick);
        let now = Instant::now();

        // Collect actors whose lease has expired — or whose death the kernel
        // just reported — marking them Dead under lock.
        let mut dead: Vec<(u32, Arc<Segment>)> = Vec::new();
        {
            let mut st = shared.state.lock().unwrap();
            for (id, a) in st.actors.iter_mut() {
                let lapsed = now.duration_since(a.last_heartbeat) > deadline;
                if a.liveness == Liveness::Alive && (lapsed || kernel_dead.contains(id)) {
                    a.liveness = Liveness::Dead;
                    dead.push((*id, a.journal_seg.clone()));
                }
            }
        }

        let had_death = !dead.is_empty();

        // Reclaim outside the lock (touches shm atomics, not the mutex).
        for (id, jseg) in dead {
            let reclaimed = reclaim_dead(&shared, id, &jseg);
            if !reclaimed.is_empty() {
                shared.state.lock().unwrap().reclaimed.extend(reclaimed);
            }
        }

        // Requeue lapsed task claims (a dead worker's claim lapses with its
        // lease). Run every tick, and again right after a death for promptness.
        shared.task_queue.reap(now_nanos());
        if had_death {
            shared.task_queue.reap(now_nanos());
        }

        // Release task-lifecycle-tied retained-ref bindings whose requester
        // never acked (ADR-0010 P0.3): the backstop that makes a task's
        // input/output hold die with the TASK rather than leak. Runs before
        // the store sweep below so a binding released this tick can already
        // unblock a tombstoned entry's reclamation in the same tick.
        let grace = shared.config.task_binding_grace.as_nanos() as u64;
        reap_task_bindings(&shared, now_nanos(), grace);

        // Return keyed-store slots whose entries have gone quiescent (ADR-0008
        // P0.1). Eviction sweeps its own slot inline, so this only collects
        // entries that were still pinned when they were evicted — including by a
        // reader just declared dead above, which is why it runs after the
        // reclaim pass rather than before it.
        sweep_store_tombstones(&shared);
    }
}

/// One task-binding reap sweep (ADR-0010 P0.3): win the exactly-once release
/// of every stale armed binding in the task queue's lease side table, then
/// release each against the keyed store's entry (`attach_at_incarnation` +
/// `release_leaked_pin` — the item-J route; a stale incarnation drops the
/// binding). Returns how many bindings were released against the store.
fn reap_task_bindings(shared: &Arc<CoordShared>, now_nanos: u64, grace_nanos: u64) -> usize {
    let won = shared.task_queue.reap_bindings(now_nanos, grace_nanos);
    if won.is_empty() {
        return 0;
    }
    let mut released = 0usize;
    for b in &won {
        if shm_store::release_task_binding(
            &shared.store_catalog_seg,
            &shared.store_head_seg,
            &shared.store_data_seg,
            b.artifact_id,
            b.incarnation,
            b.version,
        ) {
            released += 1;
        }
    }
    let mut st = shared.state.lock().unwrap();
    st.task_bindings_reclaimed += released as u64;
    released
}

/// Sweep the keyed store's catalog for tombstoned slots whose entries have gone
/// quiescent, returning them to the free list (ADR-0008 P0.1).
///
/// Best-effort by design: a store that is not provisioned, or a catalog that
/// momentarily will not attach, simply reclaims nothing this tick. There is
/// always another tick, and nothing is lost by skipping one.
fn sweep_store_tombstones(shared: &Arc<CoordShared>) -> usize {
    shm_store::sweep_tombstones(
        &shared.store_catalog_seg,
        &shared.store_head_seg,
        &shared.store_data_seg,
    )
    .unwrap_or(0)
}

/// Replay a dead actor's borrow journal and reclaim everything it held: every
/// pinned **chunk** (across every pool the coordinator hosts), every leaked
/// artifact **version pin** (item J), **and** every leaked exclusive **write
/// lease** (item K, force-released with a fence bump).
///
/// A dead actor's journal can name chunks in more than one segment: shared-pin
/// chunks in the payload pool (v0.1) **and** a producer's staged stream chunks
/// in an artifact's *data* pool (v0.2 stage C). Because [`Pool::ctrl`] locates a
/// chunk by `offset` alone (ignoring `segment_id`), each chunk descriptor must
/// be reclaimed against the pool of its own segment; this routes by
/// `segment_id`. An [`ArtifactPin`](JournalRecord::ArtifactPin) is instead
/// routed by `artifact_id` to its hosted artifact, whose per-version pin count
/// is decremented through the same retire path a clean pin drop would take.
///
/// Returns the chunk descriptors that were reclaimed (recycled + freed);
/// artifact-pin reclamations are counted into `artifact_pins_reclaimed`.
fn reclaim_dead(
    shared: &Arc<CoordShared>,
    actor_id: u32,
    journal_seg: &Arc<Segment>,
) -> Vec<ChunkDesc> {
    let journal = match BorrowJournal::attach(journal_seg) {
        Ok(j) => j,
        Err(_) => return Vec::new(),
    };
    // Every segment whose pool could hold a chunk this actor journaled: the
    // shared payload pool plus every hosted artifact's data pool. `segs` must
    // outlive `pools` (each `Pool` borrows its `Segment`).
    let segs: Vec<Arc<Segment>> = {
        let st = shared.state.lock().unwrap();
        let mut v = vec![shared.payload_seg.clone()];
        for a in st.artifacts.values() {
            v.push(a.data_seg.clone());
        }
        // The keyed store's shared data pool can also hold a dead actor's
        // journaled chunks (ADR-0007 G3), so route by its segment id too.
        v.push(shared.store_data_seg.clone());
        v
    };
    let pools: Vec<(u32, Pool)> = segs
        .iter()
        .filter_map(|s| Pool::attach(s).ok().map(|p| (s.id(), p)))
        .collect();

    let mut reclaimed = Vec::new();
    let mut artifact_pins = 0u64;
    let mut write_leases = 0u64;
    for rec in journal.replay() {
        match rec {
            JournalRecord::ChunkPin(desc) => {
                if let Some((_, pool)) = pools.iter().find(|(id, _)| *id == desc.segment_id) {
                    if let Some(d) = reclaim_one(pool, &desc, actor_id) {
                        reclaimed.push(d);
                    }
                }
            }
            JournalRecord::ArtifactPin {
                artifact_id,
                incarnation,
                version,
            } => {
                if reclaim_artifact_pin(shared, artifact_id, incarnation, version) {
                    artifact_pins += 1;
                }
            }
            JournalRecord::WriteLease {
                artifact_id,
                incarnation,
                ..
            } => {
                if reclaim_write_lease(shared, artifact_id, incarnation) {
                    write_leases += 1;
                }
            }
        }
    }
    if artifact_pins > 0 || write_leases > 0 {
        let mut st = shared.state.lock().unwrap();
        st.artifact_pins_reclaimed += artifact_pins;
        st.write_leases_reclaimed += write_leases;
    }
    reclaimed
}

/// Reclaim one leaked artifact **version pin** (item J): route `artifact_id` to
/// its hosted artifact and decrement its per-version pin count through the same
/// retire path a clean [`VersionPin`](shm_artifact::VersionPin) drop takes, so a
/// leaked pin's version is retired (and its chunks reclaimed) exactly as if the
/// reader had dropped it. Returns `true` iff a live slot was decremented.
fn reclaim_artifact_pin(
    shared: &Arc<CoordShared>,
    artifact_id: u32,
    incarnation: u32,
    version: u64,
) -> bool {
    match attach_artifact_by_id(shared, artifact_id, incarnation) {
        Some(art) => art.release_leaked_pin(version).unwrap_or(false),
        None => false,
    }
}

/// Reclaim one leaked artifact **write lease** (item K): route `artifact_id` to
/// its hosted artifact and force-release its fenced exclusive write lease (with a
/// fence bump), so a dead exclusive writer no longer wedges the artifact and its
/// late commit is fenced out. Returns `true` iff a lease was actually held.
fn reclaim_write_lease(shared: &Arc<CoordShared>, artifact_id: u32, incarnation: u32) -> bool {
    match attach_artifact_by_id(shared, artifact_id, incarnation) {
        Some(art) => art.release_leaked_write_lease(),
        None => false,
    }
}

/// Route an interned `artifact_id` back to its hosted artifact and attach a
/// handle onto it (shared by the item-J and item-K reclaim paths).
///
/// Lineage ids `>= STORE_ARTIFACT_ID_BASE` name a **keyed-store entry** rather
/// than a per-name artifact, so those are routed through the store's catalog
/// (ADR-0007 G3): the store is the registry of record via the segments the
/// coordinator owns, so a dead actor's leaked entry pin / write lease is released
/// against the entry's `ArtifactHead` exactly like a normal artifact's.
/// `incarnation` is the occupant the record was written against (ADR-0008 P0.1).
/// A keyed-store slot can be reclaimed and re-created between a crash and its
/// replay, so routing by `artifact_id` alone would release a borrow belonging to
/// the slot's *next* occupant; a mismatch drops the record instead.
fn attach_artifact_by_id(
    shared: &Arc<CoordShared>,
    artifact_id: u32,
    incarnation: u32,
) -> Option<Artifact> {
    if artifact_id >= STORE_ARTIFACT_ID_BASE {
        return attach_store_entry_by_id(shared, artifact_id, incarnation);
    }
    let (name_id, head, data) = {
        let st = shared.state.lock().unwrap();
        let name = st.artifact_name_by_id.get(&artifact_id)?.clone();
        let a = st.artifacts.get(&name)?;
        (a.name_id, a.head_seg.clone(), a.data_seg.clone())
    };
    let artifact = Artifact::attach(name_id, head, data).ok()?;
    (artifact.incarnation() == incarnation).then_some(artifact)
}

/// Route a keyed-store lineage `artifact_id` to its entry's `ArtifactHead` by
/// reading the store catalog (which the coordinator maps): find the slot for the
/// id, take its `head_off`, and attach a handle at that offset over the store's
/// shared head + data segments.
fn attach_store_entry_by_id(
    shared: &Arc<CoordShared>,
    artifact_id: u32,
    incarnation: u32,
) -> Option<Artifact> {
    let cat = Catalog::attach(&shared.store_catalog_seg).ok()?;
    let idx = cat.slot_for_artifact_id(artifact_id)?;
    let head_off = cat.slot(idx).head_off() as usize;
    Artifact::attach_at_incarnation(
        artifact_id,
        incarnation,
        shared.store_head_seg.clone(),
        head_off,
        shared.store_data_seg.clone(),
    )
    .ok()
}

/// The single-pool **chunk** reclaim core, factored out so it can be unit-tested
/// in isolation (and reused by the segmented path).
///
/// For every chunk descriptor a dead actor still had journaled, release the hold
/// it represents and, if that recycles the chunk to `FREE`, return it to the
/// pool. Returns the descriptors that were actually reclaimed (recycled + freed).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn replay_and_reclaim(
    pool: &Pool,
    journal: &BorrowJournal,
    actor_id: u32,
) -> Vec<ChunkDesc> {
    journal
        .replay()
        .filter_map(|rec| match rec {
            JournalRecord::ChunkPin(desc) => reclaim_one(pool, &desc, actor_id),
            JournalRecord::ArtifactPin { .. } | JournalRecord::WriteLease { .. } => None,
        })
        .collect()
}

/// Reclaim one journaled descriptor against `pool`, returning it iff the hold it
/// represented recycled the chunk to `FREE`.
fn reclaim_one(pool: &Pool, desc: &ChunkDesc, actor_id: u32) -> Option<ChunkDesc> {
    let ctrl = pool.ctrl(desc).ok()?;
    // A generation mismatch means this chunk was already recycled (its hold is
    // long gone); skip to avoid a spurious refcount underflow.
    if ctrl.validate(desc).is_err() {
        return None;
    }
    let recycled = match ctrl.state() {
        PUBLISHED => {
            if ctrl.refcount() > 0 {
                // Drop this actor's shared pin; reclaims iff it was the last hold
                // and the owner has already released.
                ctrl.release_shared()
            } else {
                // No shared pins: the dead actor was the exclusive owner of a
                // published-but-undelivered chunk; release ownership.
                ctrl.owner_release()
            }
        }
        LOANED => {
            // A never-published exclusive loan held by the dead actor.
            if ctrl.owner_actor.load(Ordering::Acquire) == actor_id {
                ctrl.drop_loan().is_ok()
            } else {
                false
            }
        }
        FREE => false,
        _ => false,
    };
    if recycled {
        let _ = pool.free(desc);
        Some(*desc)
    } else {
        None
    }
}

// ---- helpers ----

/// Snapshot a control word's four fields atomically-per-field.
fn snapshot_ctrl(ctrl: &ChunkCtrl) -> ChunkSnapshot {
    ChunkSnapshot {
        state: ctrl.state(),
        generation: ctrl.generation(),
        refcount: ctrl.refcount(),
        owner: ctrl.owner_actor.load(Ordering::Acquire),
    }
}

/// Byte size for an actor's journal segment: header + a comfortable bound over
/// the fixed slot table + bitmap for `capacity` pins.
fn journal_segment_size(capacity: usize) -> usize {
    // Each slot is a 24-byte ChunkDesc; the bitmap is capacity/8 bytes; add
    // header slack. 32 bytes/pin + 4 KiB is a safe upper bound.
    shm_core::segment::HEADER_SIZE + 4096 + capacity * 32
}

/// Allocate a process-unique segment id for an artifact's data segment.
///
/// Since v0.3 (ADR-0003a) a manifest [`PackedRef`](shm_core::PackedRef) carries a
/// full 32-bit `segment_id`, so ids are no longer capped at 2^16; this allocator
/// still hands them out from a modest range purely for shm-name-space hygiene. A
/// process-global counter guarantees uniqueness among artifacts created by one
/// coordinator
/// process (only the coordinator ever *creates* these segments; actors attach by
/// fd); a randomized per-process base keeps the small shared shm-name space from
/// clashing with a prior crashed run's leftovers. `create_segment`'s
/// unlink-and-retry is the final backstop.
fn alloc_artifact_data_id() -> u32 {
    use std::sync::atomic::AtomicU32;
    use std::sync::OnceLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    static BASE: OnceLock<u32> = OnceLock::new();
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    let base = *BASE.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let pid = std::process::id() as u64;
        // A base in [1, 50_000], leaving a comfortable per-process run of ids.
        1 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 50_000) as u32
    });
    let n = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    // Keep ids in a modest range for shm-name-space hygiene; never hand out 0
    // (the ZERO sentinel). The 2^16 ABI cap was lifted in v0.3 (ADR-0003a).
    (base.wrapping_add(n) % 65_536).max(1)
}

/// Create a segment, clearing a stale name from a prior crashed run first.
///
/// Goes through the [`Platform`] seam's hardened creation (ADR-0011): on Linux
/// this yields an anonymous **sealed memfd** — no namespace entry to go stale,
/// and a hostile `ftruncate` on any granted fd fails `EPERM` instead of
/// SIGBUS-ing mapped readers. On POSIX platforms it is a plain named
/// `shm_open` segment, where `EEXIST` means a leftover from a crashed prior
/// run (unlink and retry once). Every coordinator segment is distributed by fd
/// over `SCM_RIGHTS`, so namelessness on Linux costs nothing.
fn create_segment(id: u32, size: usize) -> Result<Segment> {
    use shm_core::{NativePlatform, Platform};
    let plat = NativePlatform::new();
    match plat.segment_create_sealed(id, size) {
        Ok(seg) => Ok(seg),
        Err(shm_core::Error::Nix(nix::errno::Errno::EEXIST)) => {
            // Leftover from a previous run; unlink and retry once. (Unreachable
            // on Linux: a memfd has no name to collide on.)
            let _ = Segment::unlink_by_id(id);
            Ok(plat.segment_create_sealed(id, size)?)
        }
        Err(e) => Err(e.into()),
    }
}

/// Whether a `nix::Errno` is a non-fatal read-timeout / interrupt.
fn is_timeout(errno: nix::errno::Errno) -> bool {
    // On this platform EWOULDBLOCK == EAGAIN, so EAGAIN covers both.
    matches!(errno, nix::errno::Errno::EAGAIN | nix::errno::Errno::EINTR)
}

/// Whether an `io::Error` is a read-timeout / would-block / interrupt.
fn is_io_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use shm_arrow::{write_batch, PoolAllocator, SchemaRegistry};

    // Build a small payload segment + pool + a journal segment, then exercise the
    // reclaim core directly: pin a published chunk, journal it, and prove replay
    // recycles it (FREE + bumped generation) and staleness kicks in.
    #[test]
    fn replay_reclaims_a_pinned_chunk() {
        let base: u32 = 40_000 + (std::process::id() & 0x7ff);
        let payload = Segment::create(base, 1 << 18).expect("payload seg");
        let pool = Pool::create(
            &payload,
            &shm_core::PoolConfig::power_of_two(4096, 16384, 4),
        )
        .expect("pool");
        let jseg = Segment::create(base + 1, 64 * 1024).expect("journal seg");
        let journal = BorrowJournal::create(&jseg, 64).expect("journal");

        // Producer side: write a batch, loan, publish.
        let registry = SchemaRegistry::with_schemas(&[crate::demo::demo_schema()]);
        let batch = crate::demo::demo_batch();
        let allocator = PoolAllocator::new(&pool, &payload);
        let desc = write_batch(&allocator, &registry, &batch).expect("write");
        let ctrl = pool.ctrl(&desc).unwrap();
        ctrl.try_loan(1).unwrap();
        ctrl.publish().unwrap();
        let gen_before = ctrl.generation();

        // Consumer side: pin + journal, then producer releases ownership.
        ctrl.borrow_shared().unwrap();
        let _slot = journal.record(desc).unwrap();
        ctrl.owner_release(); // owner NONE, refcount 1 -> not yet reclaimed
        assert_eq!(ctrl.state(), PUBLISHED);
        assert_eq!(ctrl.refcount(), 1);

        // Simulated lease expiry: replay the (consumer's) journal.
        let reclaimed = replay_and_reclaim(&pool, &journal, /*actor*/ 2);
        assert_eq!(reclaimed.len(), 1, "exactly one chunk reclaimed");
        assert_eq!(reclaimed[0], desc);

        // Chunk is back to FREE with a bumped generation.
        assert_eq!(ctrl.state(), FREE);
        assert!(ctrl.generation() > gen_before, "generation must bump");

        // The pre-reclaim descriptor is now stale.
        assert!(matches!(
            ctrl.validate(&desc),
            Err(shm_core::Error::StaleDescriptor)
        ));

        payload.unlink().ok();
        jseg.unlink().ok();
    }

    #[test]
    fn replay_drops_an_unpublished_loan() {
        let base: u32 = 42_000 + (std::process::id() & 0x7ff);
        let payload = Segment::create(base, 1 << 17).expect("payload seg");
        let pool = Pool::create(&payload, &shm_core::PoolConfig::power_of_two(4096, 8192, 4))
            .expect("pool");
        let jseg = Segment::create(base + 1, 32 * 1024).expect("journal seg");
        let journal = BorrowJournal::create(&jseg, 32).expect("journal");

        // Loan a chunk (never published) and journal it, as a crashed writer.
        let desc = pool.alloc(4096).unwrap();
        let ctrl = pool.ctrl(&desc).unwrap();
        ctrl.try_loan(9).unwrap();
        let gen_before = ctrl.generation();
        journal.record(desc).unwrap();

        let reclaimed = replay_and_reclaim(&pool, &journal, 9);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(ctrl.state(), FREE);
        assert!(ctrl.generation() > gen_before);

        payload.unlink().ok();
        jseg.unlink().ok();
    }
}
