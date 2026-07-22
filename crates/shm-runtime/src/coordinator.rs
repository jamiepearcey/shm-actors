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
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

use core::sync::atomic::Ordering;

use shm_core::{BorrowJournal, ChunkCtrl, ChunkDesc, Pool, Segment, FREE, LOANED, PUBLISHED};
use shm_ring::{required_bytes, Ring};

use crate::config::RuntimeConfig;
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
}

/// A topic's ring segment.
struct TopicEntry {
    #[allow(dead_code)]
    index: u32,
    segment: Arc<Segment>,
    region_len: usize,
}

/// Mutable coordinator state, guarded by one mutex.
struct CoordState {
    next_actor_id: u32,
    next_topic_index: u32,
    actors: HashMap<u32, ActorEntry>,
    topics: HashMap<String, TopicEntry>,
    last_published: Option<ChunkDesc>,
    armed: bool,
    reclaimed: Vec<ChunkDesc>,
    created_seg_ids: Vec<u32>,
}

/// State shared across the accept thread, handler threads, and lease monitor.
struct CoordShared {
    config: RuntimeConfig,
    payload_seg: Arc<Segment>,
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

        let shared = Arc::new(CoordShared {
            config,
            payload_seg,
            state: Mutex::new(CoordState {
                next_actor_id: 1,
                next_topic_index: 0,
                actors: HashMap::new(),
                topics: HashMap::new(),
                last_published: None,
                armed: false,
                reclaimed: Vec::new(),
                created_seg_ids: vec![payload_id],
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
                    if let Err(_e) = handle_connection(s, stream) {
                        // Connection errors are non-fatal; the lease monitor
                        // governs liveness, not the socket state.
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
            Request::CreateTopic { topic } | Request::Subscribe { topic } => {
                grant_topic(&shared, &topic, &stream)?;
            }
            Request::Published { desc } => {
                shared.state.lock().unwrap().last_published = Some(desc);
            }
            Request::Pinned { desc } => {
                handle_pinned(&shared, &desc);
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

/// Ensure a topic's ring segment exists and pass its fd to the requester.
///
/// Get-or-create is performed under a single lock hold so two actors racing to
/// reference the same topic (a producer's `CreateTopic` and a consumer's
/// `Subscribe`) always converge on **one** ring segment — otherwise a publisher
/// and subscriber could end up on different rings and never meet.
fn grant_topic(shared: &Arc<CoordShared>, topic: &str, stream: &UnixStream) -> Result<()> {
    let config = &shared.config;
    let region_len = required_bytes(config.ring_capacity);

    let (ring_seg_id, ring_fd, region_len) = {
        let mut st = shared.state.lock().unwrap();
        if let Some(t) = st.topics.get(topic) {
            (t.segment.id(), t.segment.as_raw_fd(), t.region_len)
        } else {
            let idx = st.next_topic_index;
            st.next_topic_index += 1;
            let ring_seg_id = config.ring_seg_id(idx);
            let seg_size = shm_core::segment::HEADER_SIZE + region_len;
            let ring_seg = Arc::new(create_segment(ring_seg_id, seg_size)?);
            // SAFETY: `payload_ptr()` is 64-byte aligned and backs `region_len`
            // writable bytes we exclusively own here; no other party initializes
            // this region (we hold the state lock).
            unsafe {
                Ring::init(ring_seg.payload_ptr(), region_len, config.ring_capacity)?;
            }
            let ring_fd = ring_seg.as_raw_fd();
            st.created_seg_ids.push(ring_seg_id);
            st.topics.insert(
                topic.into(),
                TopicEntry {
                    index: idx,
                    segment: ring_seg,
                    region_len,
                },
            );
            (ring_seg_id, ring_fd, region_len)
        }
    };

    let resp = Response::Granted {
        ring_seg_id,
        region_len: region_len as u32,
    };
    send_frame(stream, &resp.encode(), &[ring_fd])?;
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

/// Periodically expire leases and reclaim dead actors' journals.
fn lease_monitor(shared: Arc<CoordShared>) {
    let tick = shared.config.monitor_tick;
    let deadline = shared.config.lease_deadline;
    while shared.running() {
        std::thread::sleep(tick);
        let now = Instant::now();

        // Collect actors whose lease has expired, marking them Dead under lock.
        let mut dead: Vec<(u32, Arc<Segment>)> = Vec::new();
        {
            let mut st = shared.state.lock().unwrap();
            for (id, a) in st.actors.iter_mut() {
                if a.liveness == Liveness::Alive
                    && now.duration_since(a.last_heartbeat) > deadline
                {
                    a.liveness = Liveness::Dead;
                    dead.push((*id, a.journal_seg.clone()));
                }
            }
        }

        // Reclaim outside the lock (touches shm atomics, not the mutex).
        for (id, jseg) in dead {
            let reclaimed = reclaim_dead(&shared, id, &jseg);
            if !reclaimed.is_empty() {
                shared
                    .state
                    .lock()
                    .unwrap()
                    .reclaimed
                    .extend(reclaimed);
            }
        }
    }
}

/// Replay a dead actor's borrow journal and reclaim every chunk it held.
fn reclaim_dead(shared: &Arc<CoordShared>, actor_id: u32, journal_seg: &Arc<Segment>) -> Vec<ChunkDesc> {
    let pool = match Pool::attach(&shared.payload_seg) {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let journal = match BorrowJournal::attach(journal_seg) {
        Ok(j) => j,
        Err(_) => return Vec::new(),
    };
    replay_and_reclaim(&pool, &journal, actor_id)
}

/// The reclaim core, factored out so it can be unit-tested in isolation.
///
/// For every descriptor a dead actor still had journaled, release the hold it
/// represents and, if that recycles the chunk to `FREE`, return it to the pool.
/// Returns the descriptors that were actually reclaimed (recycled + freed).
pub(crate) fn replay_and_reclaim(
    pool: &Pool,
    journal: &BorrowJournal,
    actor_id: u32,
) -> Vec<ChunkDesc> {
    let mut reclaimed = Vec::new();
    for desc in journal.replay() {
        let ctrl = match pool.ctrl(&desc) {
            Ok(c) => c,
            Err(_) => continue,
        };
        // A generation mismatch means this chunk was already recycled (its hold
        // is long gone); skip to avoid a spurious refcount underflow.
        if ctrl.validate(&desc).is_err() {
            continue;
        }
        let recycled = match ctrl.state() {
            PUBLISHED => {
                if ctrl.refcount() > 0 {
                    // Drop this actor's shared pin; reclaims iff it was the last
                    // hold and the owner has already released.
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
            let _ = pool.free(&desc);
            reclaimed.push(desc);
        }
    }
    reclaimed
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

/// Create a segment, clearing a stale name from a prior crashed run first.
fn create_segment(id: u32, size: usize) -> Result<Segment> {
    match Segment::create(id, size) {
        Ok(seg) => Ok(seg),
        Err(shm_core::Error::Nix(nix::errno::Errno::EEXIST)) => {
            // Leftover from a previous run; unlink and retry once.
            let _ = Segment::unlink_by_id(id);
            Ok(Segment::create(id, size)?)
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
        let pool = Pool::create(&payload, &shm_core::PoolConfig::power_of_two(4096, 16384, 4))
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
