//! [`Actor`], [`ActorSystem`] (N actors over one mailbox, routed by `to`),
//! [`ActorRef`], and the per-message [`Cx`] with its RAII [`Pinned`] cell view.

use std::ops::Deref;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use holon_core::{
    ActorId, Dispatch, Envelope, LocalRef, MessageKind, Payload, Reply, ENVELOPE_SIZE,
    MAX_REPLY_BODY,
};
use holon_object::{ObjectSide, ObjectStore};
use holon_topic::{TopicLog, TopicPolicy, TopicSide};
use shm_arrow::SchemaRegistry;
use shm_artifact::VersionPin;
use shm_core::{ChunkDesc, PackedRef, Segment};
use shm_ring::DoorbellParker;
use shm_runtime::{Node, TaskQueueHandle};
use shm_store::KeyedStore;
use shm_task::{now_nanos, Error as TaskError, Outcome, TaskHandle, TaskStatus};

use crate::chunk::{MessagePool, DEFAULT_REPLY_BODY};
use crate::error::{Error, Result};

/// The default claim lease a running system stamps on each claim: a handler
/// that has not completed within it is presumed dead and its message is
/// redelivered (at-least-once) by the coordinator's lease reap.
pub const DEFAULT_LEASE: Duration = Duration::from_millis(500);

/// How far ahead of `now` an ask's submit deadline is set. Only bounds the
/// pre-claim window: the claim re-stamps a fresh lease (see [`DEFAULT_LEASE`]).
const SUBMIT_HORIZON: Duration = Duration::from_secs(60);

/// A durable cell in the keyed store, named by key. The actor holds the
/// *name*; the state lives in the memory plane and survives the actor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellRef {
    key: Vec<u8>,
}

impl CellRef {
    /// Name a cell.
    pub fn new(key: impl Into<Vec<u8>>) -> CellRef {
        CellRef { key: key.into() }
    }

    /// The cell's key bytes.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

/// A zero-copy, version-pinned view of a cell (design §5's `Pinned<T>`).
///
/// The batch's buffers point into the shared data segment; the journaled
/// [`VersionPin`] keeps that version alive and is released on drop — and by
/// the coordinator's journal replay if the holder dies first.
pub struct Pinned {
    batch: RecordBatch,
    version: u64,
    _pin: VersionPin,
}

impl Pinned {
    /// The pinned cell version.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The batch (also reachable through `Deref`).
    #[inline]
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

impl Deref for Pinned {
    type Target = RecordBatch;
    #[inline]
    fn deref(&self) -> &RecordBatch {
        &self.batch
    }
}

/// The per-message context the system lends a handler.
pub struct Cx<'a> {
    store: &'a KeyedStore<'a>,
    topics: &'a TopicLog<'a>,
    objects: &'a ObjectStore<'a>,
    self_id: ActorId,
    sender: ActorId,
    corr: u64,
    attempt: u32,
    reply_cap: u32,
}

impl<'a> Cx<'a> {
    /// Pin `cell`'s current version zero-copy: `open(key)` (a key-cache hit +
    /// catalog scan) → journaled `pin()` → `read()`.
    pub fn pin(&self, cell: &CellRef) -> holon_core::Result<Pinned> {
        let entry = self
            .store
            .open(cell.key())
            .map_err(|e| holon_core::Error::Cell(e.to_string()))?;
        let (pin, batch) = entry
            .read()
            .map_err(|e| holon_core::Error::Cell(e.to_string()))?;
        Ok(Pinned {
            batch,
            version: pin.version(),
            _pin: pin,
        })
    }

    /// This actor's id.
    /// Publish `data` to `topic` under the default [`TopicPolicy`]; returns the
    /// message seq (ADR-0017). Visible to every SSE/TCP/HTTP subscriber of the
    /// topic through the gateways — the actor never sees a socket.
    pub fn publish(&self, topic: &str, event: &str, data: &[u8]) -> holon_core::Result<u64> {
        self.publish_with(topic, TopicPolicy::default(), event, "", data)
    }

    /// [`Cx::publish`] with an explicit policy and content type.
    pub fn publish_with(
        &self,
        topic: &str,
        policy: TopicPolicy,
        event: &str,
        content_type: &str,
        data: &[u8],
    ) -> holon_core::Result<u64> {
        self.topics
            .publish(topic, policy, event, content_type, data)
            .map_err(|e| holon_core::Error::Cell(e.to_string()))
    }

    /// The topic log (for reads from inside a handler).
    #[inline]
    pub fn topics(&self) -> &'a TopicLog<'a> {
        self.topics
    }

    /// The object store ([`holon_object`]): opaque, versioned payloads keyed in
    /// the same keyed store the topics live in.
    ///
    /// This is the handler's answer to the one thing a reply chunk cannot do —
    /// hand a *remote* caller a large body. A `LocalRef` is refused at the
    /// holon-net wire guards, so an edge caller cannot be given a reference to
    /// shared memory; it can be given a **handle** (a key and a version), which
    /// is not an address and crosses fine. The handler commits the bytes here
    /// and replies with the handle; the gateway serves them from
    /// `GET /objects/{key}?version=`, streamed out of the same pinned pages.
    #[inline]
    pub fn objects(&self) -> &'a ObjectStore<'a> {
        self.objects
    }

    /// Commit a new immutable version of object `key`; returns its version.
    ///
    /// Descriptor-only notification rides [`holon_topic::TOPIC_EVENTS`] for
    /// free — nothing is allocated, copied or pinned for it.
    pub fn put_object(
        &self,
        key: &str,
        content_type: &str,
        bytes: &[u8],
    ) -> holon_core::Result<u64> {
        self.objects
            .put(key, content_type, bytes)
            .map_err(|e| holon_core::Error::Cell(e.to_string()))
    }

    /// The largest payload one object may hold, read from the store pool rather
    /// than assumed — see [`ObjectStore::max_object`].
    ///
    /// A handler that asks before it encodes can refuse an oversized answer as
    /// *data*, with the number in the message, instead of discovering the
    /// ceiling as a store error after the work is done. This is the **object**
    /// ceiling — a payload is split across as many segments as it needs and all
    /// of them install as one version — not one chunk's worth; a handler that
    /// wants the per-segment figure asks [`ObjectStore::max_payload`].
    pub fn max_object_bytes(&self) -> holon_core::Result<u64> {
        self.objects
            .max_object()
            .map_err(|e| holon_core::Error::Cell(e.to_string()))
    }

    /// The id of the actor handling this message.
    #[inline]
    pub fn self_id(&self) -> ActorId {
        self.self_id
    }

    /// The sender of the message being handled.
    #[inline]
    pub fn sender(&self) -> ActorId {
        self.sender
    }

    /// The correlation id of the message being handled (`0` for a tell).
    #[inline]
    pub fn corr(&self) -> u64 {
        self.corr
    }

    /// How many times this message was redelivered by the lease reap before
    /// reaching this handler (`0` = first delivery).
    #[inline]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The largest [`Reply::Bytes`] body this ask can carry, in bytes: the
    /// asker's reply chunk minus the envelope (`0` for a tell).
    ///
    /// Ask before encoding. A handler that over-runs this gets its task failed
    /// rather than a truncated answer delivered as a whole one, so the budget
    /// is a decision the handler makes up front, not a limit it discovers.
    #[inline]
    pub fn max_reply(&self) -> u32 {
        self.reply_cap
    }
}

/// An actor: process-local scratch plus handlers over messages whose state
/// lives in cells.
pub trait Actor: Sized + 'static {
    /// The message schema ids this actor accepts; each is registered to
    /// [`handle`](Self::handle) by the default [`dispatch`](Self::dispatch).
    fn accepts() -> &'static [u32];

    /// Handle one message. `body` is the inline POD payload
    /// ([`Payload::from_bytes`]); return a [`Reply`] or an error (which fails the
    /// task — the asker sees [`Error::Failed`]).
    fn handle(&mut self, msg: &Envelope, body: &[u8], cx: &mut Cx<'_>)
        -> holon_core::Result<Reply>;

    /// Build the dispatch table. The default routes every id in
    /// [`accepts`](Self::accepts) to [`handle`](Self::handle); an actor with
    /// per-schema handlers overrides this (what `#[derive(Actor)]` will emit).
    fn dispatch<'a>() -> Dispatch<Self, Cx<'a>> {
        let mut d = Dispatch::new();
        for &id in Self::accepts() {
            d.register(id, Self::handle);
        }
        d
    }
}

/// A spawned actor before [`ActorSystem::run`] binds it to the store: erased
/// at the actor boundary only, so one system hosts actors of different types.
trait Spawnable {
    fn into_hosted<'s>(self: Box<Self>) -> Box<dyn Hosted<'s> + 's>;
}

/// A hosted actor bound to the store lifetime `'s`: its state plus its own
/// zero-`dyn` dispatch table. Routing costs one vtable call per message at
/// the `to → host` boundary; the schema lookup inside is the fn-pointer table.
trait Hosted<'s> {
    fn handle(&mut self, env: &Envelope, body: &[u8], cx: &mut Cx<'s>)
        -> holon_core::Result<Reply>;
}

struct Host<'s, A: Actor> {
    actor: A,
    dispatch: Dispatch<A, Cx<'s>>,
}

impl<A: Actor> Spawnable for A {
    fn into_hosted<'s>(self: Box<Self>) -> Box<dyn Hosted<'s> + 's> {
        Box::new(Host {
            actor: *self,
            dispatch: A::dispatch(),
        })
    }
}

impl<'s, A: Actor> Hosted<'s> for Host<'s, A> {
    fn handle(
        &mut self,
        env: &Envelope,
        body: &[u8],
        cx: &mut Cx<'s>,
    ) -> holon_core::Result<Reply> {
        let handler = self
            .dispatch
            .lookup(env.schema_id)
            .ok_or(holon_core::Error::UnknownSchema(env.schema_id))?;
        handler(&mut self.actor, env, body, cx)
    }
}

/// Bind every spawned actor to the store lifetime of one `run`.
fn bind<'s>(actors: Vec<(ActorId, Box<dyn Spawnable>)>) -> Vec<(u64, Box<dyn Hosted<'s> + 's>)> {
    actors
        .into_iter()
        .map(|(id, a)| (id.0, a.into_hosted()))
        .collect()
}

/// One process's actor host: a [`Node`] plus the mailbox (task queue) and the
/// memory plane (keyed store), running any number of actors.
///
/// Every actor spawned here shares the one mailbox; a message is routed to
/// the actor whose id equals its `to` (a binary search over the sorted host
/// table), and each actor keeps its own schema → handler table, so the same
/// schema id can mean different things to different actors. Several
/// *processes* spawning the same names form a pool over the one mailbox (the
/// demo's `4 pricers` shape). A per-actor mailbox — so that one slow actor
/// cannot head-of-line block another — is Phase 1's work, not this layer's.
pub struct ActorSystem {
    node: Node,
    /// Sorted by id.
    actors: Vec<(ActorId, Box<dyn Spawnable>)>,
    lease: Duration,
    spin: bool,
    stop: Option<Arc<AtomicBool>>,
}

impl ActorSystem {
    /// Connect to the coordinator at `uds`, registering as `name`, start the
    /// heartbeat, and map the store + task queue.
    pub fn connect(uds: impl AsRef<std::path::Path>, name: &str) -> Result<ActorSystem> {
        let mut node = Node::connect(uds, name, Arc::new(SchemaRegistry::new()))?;
        node.start_heartbeat(Duration::from_millis(150));
        node.open_store()?;
        node.open_task_queue()?;
        Ok(ActorSystem {
            node,
            actors: Vec::new(),
            lease: DEFAULT_LEASE,
            spin: false,
            stop: None,
        })
    }

    /// The underlying node (e.g. to intern a cell's schema before `run`).
    #[inline]
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Mutable access to the underlying node.
    #[inline]
    pub fn node_mut(&mut self) -> &mut Node {
        &mut self.node
    }

    /// Intern `schema` at the coordinator so a cell committed under it can be
    /// reconstructed by this process (the typed `CellRef<T>` of the design,
    /// spelled out by hand).
    pub fn intern_schema(&self, schema: &SchemaRef) -> Result<u32> {
        Ok(self.node.intern_schema(schema)?)
    }

    /// Spawn `actor` under `name` (its [`ActorId`] is [`ActorId::named`]).
    /// Any number of actors, of any types, may be spawned on one system; a
    /// second spawn under the same name is [`Error::DuplicateActor`].
    pub fn spawn<A: Actor>(&mut self, name: &str, actor: A) -> Result<ActorId> {
        let id = ActorId::named(name);
        match self.actors.binary_search_by_key(&id.0, |(i, _)| i.0) {
            Ok(_) => Err(Error::DuplicateActor(id.0)),
            Err(at) => {
                self.actors.insert(at, (id, Box::new(actor)));
                Ok(id)
            }
        }
    }

    /// The ids of the spawned actors, ascending.
    pub fn actor_ids(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.actors.iter().map(|(id, _)| *id)
    }

    /// Set the per-claim lease (default [`DEFAULT_LEASE`]).
    pub fn set_lease(&mut self, lease: Duration) {
        self.lease = lease;
    }

    /// Busy-poll the mailbox instead of parking on the doorbell (the floor
    /// measurement; burns a core).
    pub fn set_spin(&mut self, spin: bool) {
        self.spin = spin;
    }

    /// A flag [`run`](Self::run) checks after each handled message; set it and
    /// send one more message to make an idle system return.
    pub fn set_stop(&mut self, stop: Arc<AtomicBool>) {
        self.stop = Some(stop);
    }

    /// The mailbox loop: claim → decode the envelope chunk → route by `to` →
    /// dispatch by `schema_id` → write the reply chunk → `complete` (or
    /// `fail`) → free the request chunk. Returns only when the stop flag is
    /// set (after a message).
    pub fn run(self) -> Result<()> {
        let ActorSystem {
            mut node,
            actors,
            lease,
            spin,
            stop,
        } = self;
        if actors.is_empty() {
            return Err(Error::NoActor);
        }
        let lease_nanos = lease.as_nanos() as u64;
        let tq: TaskQueueHandle = node.task_queue()?;
        let parker: DoorbellParker = tq.work_parker()?;
        let worker_id = tq.worker_id();
        let side = TopicSide::open(&mut node).map_err(|e| Error::Topic(e.to_string()))?;
        // Interned and opened before `node.store()` takes the borrow, exactly
        // as the topic side is: `ObjectSide` is the half of the object store
        // that needs the node, and `ObjectStore` is the half that needs the
        // keyed store. Every actor in this process shares one.
        let oside = ObjectSide::open(&mut node)?;
        let registry = node.registry().clone();
        let store: KeyedStore<'_> = node.store()?;
        let store_ref = &store;
        let topics = TopicLog::new(store_ref, &side, registry.clone());
        let objects = ObjectStore::new(store_ref, &oside, registry);
        let planes = Planes {
            store: store_ref,
            topics: &topics,
            objects: &objects,
        };
        let mut hosts = bind(actors);
        let msgs = MessagePool::new(store.data_segment().clone());
        let stopped = || stop.as_ref().is_some_and(|s| s.load(Ordering::Acquire));
        // `HOLON_TRACE=1` prints handler errors (they cross the wire as a bare
        // `Err` envelope, so this is the only place the text is visible).
        let trace = std::env::var_os("HOLON_TRACE").is_some();

        loop {
            if stopped() {
                return Ok(());
            }
            // Idle workers re-check the stop flag every park cycle (the
            // parker's timeout bounds the wait), so a system stops without
            // needing one more message to arrive.
            let claimed = if spin {
                loop {
                    if let Some(t) = tq.claim(lease_nanos) {
                        break t;
                    }
                    if stopped() {
                        return Ok(());
                    }
                    std::hint::spin_loop();
                }
            } else {
                match tq
                    .queue()
                    .claim_once_parked(worker_id, lease_nanos, &parker)
                {
                    Some(t) => t,
                    None => continue,
                }
            };
            let req = claimed.request();
            let attempt = claimed.attempt();

            // Decode; a non-message request is failed and its chunk left alone
            // (we do not know who owns it).
            let (env, body) = match msgs.read_message(&req) {
                Ok(v) => v,
                Err(_) => {
                    let _ = claimed.fail();
                    continue;
                }
            };

            // Resolve the asker's reply chunk BEFORE dispatch: its real size is
            // the handler's byte budget, and its live generation says whether
            // the asker is still there to read the answer.
            let rdesc = if env.no_reply() {
                None
            } else {
                // An ask names the asker's reply chunk; one without is malformed.
                match env.payload_ref().map(|r| msgs.reply_desc(r)) {
                    Some(Ok(d)) => Some(d),
                    _ => {
                        if claimed.fail().is_ok() {
                            let _ = msgs.free(&req);
                        }
                        continue;
                    }
                }
            };
            // Epoch check. A reply chunk is loaned, so the only thing that can
            // move its generation is a release — the asker's own, or the
            // coordinator reclaiming a node it has declared dead. Either way
            // the asker is gone: fail the task rather than write into a chunk
            // that now belongs to someone else.
            if let Some(d) = &rdesc {
                if d.generation != env.reply_gen {
                    if claimed.fail().is_ok() {
                        let _ = msgs.free(&req);
                    }
                    continue;
                }
            }
            let reply_cap = rdesc.as_ref().map_or(0, |d| d.len - ENVELOPE_SIZE as u32);

            let outcome = route(&mut hosts, &planes, &env, body, attempt, reply_cap);
            if let Err(e) = &outcome {
                if trace {
                    eprintln!(
                        "[holon-actor] handler error to={:#x} schema={} attempt={attempt}: {e}",
                        env.to, env.schema_id
                    );
                }
            }

            // A tell: nothing to write back; complete/fail and free the request.
            let Some(rdesc) = rdesc else {
                let done = match outcome {
                    Ok(_) => claimed.complete(ChunkDesc::ZERO),
                    Err(_) => claimed.fail(),
                };
                if done.is_ok() {
                    let _ = msgs.free(&req);
                }
                continue;
            };

            // A body past the asker's chunk is refused, never truncated: a
            // short answer that decodes is worse than no answer.
            let over_cap = match &outcome {
                Ok(Reply::Bytes(b)) => b.len() > reply_cap as usize,
                _ => false,
            };
            if over_cap && trace {
                eprintln!(
                    "[holon-actor] reply too large to={:#x} schema={} cap={reply_cap}",
                    env.to, env.schema_id
                );
            }
            let wrote = match &outcome {
                Ok(Reply::Inline(b)) => msgs.write_message_into(
                    &rdesc,
                    &Envelope::reply_to(&env, b.schema_id(), b.len()),
                    b.bytes(),
                ),
                Ok(Reply::Bytes(b)) if !over_cap => msgs.write_message_into(
                    &rdesc,
                    &Envelope::reply_to(&env, b.schema_id(), b.len() as u16),
                    b.bytes(),
                ),
                Ok(Reply::None) => {
                    msgs.write_message_into(&rdesc, &Envelope::reply_to(&env, 0, 0), &[])
                }
                _ => msgs.write_message_into(&rdesc, &Envelope::err_to(&env), &[]),
            };
            let done = if outcome.is_ok() && !over_cap && wrote.is_ok() {
                claimed.complete(ChunkDesc::ZERO)
            } else {
                claimed.fail()
            };
            // We went terminal: the request chunk is ours to free. `Lost` /
            // stale: another attempt owns the task and its request chunk.
            if done.is_ok() {
                let _ = msgs.free(&req);
            }
        }
    }
}

/// The three shared-memory planes every handler in this process is lent, built
/// once in [`ActorSystem::run`] and borrowed by every [`Cx`].
///
/// A struct rather than three parameters because they are one thing — the
/// process's view of the store — and because a fourth plane should not mean a
/// fourth argument at every call site between `run` and a handler.
struct Planes<'s> {
    store: &'s KeyedStore<'s>,
    topics: &'s TopicLog<'s>,
    objects: &'s ObjectStore<'s>,
}

/// Validate the envelope's kind, find the host `to` names, and dispatch.
fn route<'s>(
    hosts: &mut [(u64, Box<dyn Hosted<'s> + 's>)],
    planes: &Planes<'s>,
    env: &Envelope,
    body: &[u8],
    attempt: u32,
    reply_cap: u32,
) -> holon_core::Result<Reply> {
    match env.kind()? {
        MessageKind::Tell | MessageKind::Ask => {}
        other => return Err(holon_core::Error::BadKind(other.as_u16())),
    }
    let i = hosts
        .binary_search_by_key(&env.to, |(id, _)| *id)
        .map_err(|_| holon_core::Error::NoSuchActor(env.to))?;
    let (id, host) = &mut hosts[i];
    let mut cx = Cx {
        store: planes.store,
        topics: planes.topics,
        objects: planes.objects,
        self_id: ActorId(*id),
        sender: env.from(),
        corr: env.corr,
        attempt,
        reply_cap,
    };
    host.handle(env, body, &mut cx)
}

/// A handle to send messages to an actor from this process: `tell` and `ask`.
///
/// Holds the sender's task-queue handle and message pool; `Send` (moves to a
/// thread) but not `Sync` — one ref per sending thread.
pub struct ActorRef {
    tq: TaskQueueHandle,
    done_parker: DoorbellParker,
    msgs: MessagePool,
    /// This node's borrow journal — the crash ledger every reply chunk is
    /// recorded in, and the election that makes its release exactly-once.
    journal_seg: Arc<Segment>,
    /// The coordinator-issued id of this node, stamped as the reply chunk's
    /// loan owner. It is what `reclaim_dead` compares against.
    owner: u32,
    to: ActorId,
    from: ActorId,
    corr: AtomicU64,
    spin: bool,
    /// Reply chunks of asks that were dropped before going terminal, with the
    /// journal slot that owns each. A handler may still write into them, so
    /// they are released only once the task is observed terminal
    /// (`sweep_orphans`) — and through the journal election, never blind.
    orphans: Mutex<Vec<(TaskHandle, ChunkDesc, usize)>>,
}

/// An ask in flight, started by [`ActorRef::ask_start`] and driven by
/// [`ActorRef::ask_poll`] — the non-blocking half of the ask protocol that a
/// reactor multiplexing many asks needs (ADR-0017 §2).
///
/// The reply chunk is owned by the asker. Dropping a `PendingAsk` before it
/// went terminal does **not** free the chunk (a handler may still write into
/// it); it is cancelled if still queued and otherwise parked as an orphan on
/// the `ActorRef`, freed when observed terminal.
#[derive(Debug)]
pub struct PendingAsk {
    handle: TaskHandle,
    reply: ChunkDesc,
    /// The journal slot the reply chunk was recorded in: the ownership token.
    journal_slot: usize,
    /// The request chunk. A worker frees this when it claims the task, so the
    /// asker normally never touches it — but a cancel that wins the pre-claim
    /// race means no worker ever will, and then this is the only handle left
    /// to it.
    request: ChunkDesc,
    corr: u64,
    done: bool,
}

impl PendingAsk {
    /// The task handle (for diagnostics; opaque otherwise).
    #[inline]
    pub fn handle(&self) -> TaskHandle {
        self.handle
    }

    /// The correlation id stamped in the request envelope.
    #[inline]
    pub fn corr(&self) -> u64 {
        self.corr
    }

    /// The largest reply body this ask can carry, in bytes (the chunk the
    /// asker allocated, minus the envelope).
    #[inline]
    pub fn reply_cap(&self) -> u32 {
        self.reply.len - ENVELOPE_SIZE as u32
    }
}

/// The asker's view of a by-reference reply: the reply chunk it allocated,
/// loaned and journaled, borrowed **in place**.
///
/// [`bytes`](ReplyRef::bytes) points straight into the store's data segment;
/// nothing is copied. The guard is what makes the release exactly-once across
/// every exit path of the code that reads it, which a bare descriptor could
/// not: dropping it runs the journal election and, if it wins, drops the loan
/// and returns the chunk to the pool.
pub struct ReplyRef<'a> {
    msgs: &'a MessagePool,
    journal_seg: &'a Arc<Segment>,
    desc: ChunkDesc,
    slot: usize,
    env: Envelope,
    body: &'a [u8],
    released: bool,
}

impl<'a> ReplyRef<'a> {
    /// The reply envelope.
    #[inline]
    pub fn envelope(&self) -> &Envelope {
        &self.env
    }

    /// The reply body's schema id.
    #[inline]
    pub fn schema_id(&self) -> u32 {
        self.env.schema_id
    }

    /// The reply body length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.body.len()
    }

    /// Whether the reply body is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    /// The reply body, borrowed from the shared data segment.
    #[inline]
    pub fn bytes(&self) -> &'a [u8] {
        self.body
    }

    /// Decode the body as the POD message `R`.
    pub fn payload<R: Payload>(&self) -> Result<R> {
        if self.env.schema_id != R::SCHEMA_ID {
            return Err(holon_core::Error::SchemaMismatch {
                expected: R::SCHEMA_ID,
                got: self.env.schema_id,
            }
            .into());
        }
        Ok(R::from_bytes(self.body)?)
    }

    /// Release the chunk now instead of at drop. Identical work; explicit.
    pub fn release(mut self) {
        self.release_now();
    }

    fn release_now(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let _ = self
            .msgs
            .release_reply(&self.desc, self.slot, self.journal_seg);
    }
}

impl Drop for ReplyRef<'_> {
    fn drop(&mut self) {
        self.release_now();
    }
}

impl core::fmt::Debug for ReplyRef<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ReplyRef")
            .field("schema_id", &self.env.schema_id)
            .field("len", &self.body.len())
            .field("slot", &self.slot)
            .finish()
    }
}

impl ActorRef {
    /// Build a ref to `to` over `node` (maps the store + task queue on first
    /// use). `node` stays usable afterwards; the ref owns its own handles.
    pub fn new(node: &mut Node, to: ActorId) -> Result<ActorRef> {
        let owner = node.actor_id();
        let from = ActorId::new(0, owner);
        let tq = node.task_queue()?;
        let done_parker = tq.done_parker()?;
        let seg = node.store()?.data_segment().clone();
        let journal_seg = node.journal_segment().clone();
        Ok(ActorRef {
            tq,
            done_parker,
            msgs: MessagePool::new(seg),
            journal_seg,
            owner,
            to,
            from,
            corr: AtomicU64::new(1),
            spin: false,
            orphans: Mutex::new(Vec::new()),
        })
    }

    /// The done-doorbell fd: readable when some task went terminal. A reactor
    /// polls it alongside its sockets and then `ask_poll`s its pending asks.
    /// Drain it with [`ActorRef::drain_done_fd`] after a wake.
    #[inline]
    pub fn done_fd(&self) -> RawFd {
        self.done_parker.read_fd()
    }

    /// Drain the done doorbell (non-blocking) after `poll(2)` reported it
    /// readable. The doorbell is level-signalled by bytes; leaving them makes
    /// the next poll return immediately.
    pub fn drain_done_fd(&self) {
        let _ = shm_core::doorbell_park(self.done_parker.read_fd(), Duration::ZERO);
    }

    /// Register as a done-doorbell waiter (a reactor parks on
    /// [`ActorRef::done_fd`] only after this, and re-polls its pending asks
    /// in between — the lost-wakeup rule of `TaskQueue::wait`).
    #[inline]
    pub fn add_done_waiter(&self) {
        self.tq.queue().add_done_waiter();
    }

    /// Pairs with [`ActorRef::add_done_waiter`].
    #[inline]
    pub fn remove_done_waiter(&self) {
        self.tq.queue().remove_done_waiter();
    }

    /// Re-target this ref at another actor (same node, same queue).
    pub fn retarget(&mut self, to: ActorId) {
        self.to = to;
    }

    /// Busy-poll for replies instead of parking on the done doorbell.
    pub fn set_spin(&mut self, spin: bool) {
        self.spin = spin;
    }

    /// The destination.
    #[inline]
    pub fn to(&self) -> ActorId {
        self.to
    }

    /// The sender id this ref stamps.
    #[inline]
    pub fn from(&self) -> ActorId {
        self.from
    }

    /// The message pool (for census/diagnostics).
    #[inline]
    pub fn pool(&self) -> &MessagePool {
        &self.msgs
    }

    /// Write the envelope chunk and submit it; returns the task handle and the
    /// correlation id stamped on the envelope.
    fn submit_to(
        &self,
        to: ActorId,
        kind: MessageKind,
        schema_id: u32,
        body: &[u8],
        reply: Option<(LocalRef, u32)>,
    ) -> Result<(TaskHandle, u64, ChunkDesc)> {
        let corr = match kind {
            MessageKind::Tell => 0,
            _ => self.corr.fetch_add(1, Ordering::Relaxed),
        };
        let deadline = now_nanos().wrapping_add(SUBMIT_HORIZON.as_nanos() as u64);
        let mut env = Envelope::inline(kind, to, self.from, corr, schema_id, body.len() as u16)
            .with_deadline_nanos(deadline);
        if let Some((r, generation)) = reply {
            env = env.with_reply_ref(r, generation);
        }
        let desc = self.msgs.write_message(&env, body)?;
        match self.tq.submit(desc, deadline) {
            Ok(h) => Ok((h, corr, desc)),
            Err(e) => {
                let _ = self.msgs.free(&desc);
                Err(e.into())
            }
        }
    }

    /// Fire-and-forget: one envelope chunk + one submit.
    pub fn tell<P: Payload>(&self, msg: &P) -> Result<()> {
        self.submit_to(
            self.to,
            MessageKind::Tell,
            P::SCHEMA_ID,
            msg.as_bytes(),
            None,
        )?;
        Ok(())
    }

    /// Request/reply: submit the envelope chunk, wait for the task's terminal
    /// outcome, read + free the reply chunk, decode `R`.
    pub fn ask<P: Payload, R: Payload>(&self, msg: &P) -> Result<R> {
        let (env, body) = self.ask_raw(P::SCHEMA_ID, msg.as_bytes())?;
        if env.schema_id != R::SCHEMA_ID {
            return Err(holon_core::Error::SchemaMismatch {
                expected: R::SCHEMA_ID,
                got: env.schema_id,
            }
            .into());
        }
        Ok(R::from_bytes(&body)?)
    }

    /// Untyped ask: returns the reply envelope and a copy of its body.
    ///
    /// Allocates the reply chunk, names it in the request envelope, submits,
    /// waits for the task to go terminal (a `StaleHandle` counts: the slot was
    /// reused, so it went terminal first), then reads and frees the reply chunk.
    pub fn ask_raw(&self, schema_id: u32, body: &[u8]) -> Result<(Envelope, Vec<u8>)> {
        self.ask_raw_to(self.to, schema_id, body)
    }

    /// [`ActorRef::ask_raw`] addressed to `to` instead of this ref's target.
    pub fn ask_raw_to(
        &self,
        to: ActorId,
        schema_id: u32,
        body: &[u8],
    ) -> Result<(Envelope, Vec<u8>)> {
        let mut pending = self.ask_start_to(to, schema_id, body)?;
        let terminal = match self.await_terminal(&pending) {
            Ok(t) => t,
            Err(e) => {
                // Unknown state: keep the chunk until observed terminal.
                self.orphan(&mut pending);
                return Err(e.into());
            }
        };
        self.finish(&mut pending, terminal)
    }

    /// Start an ask without waiting: allocate the reply chunk, submit, and
    /// hand back the [`PendingAsk`] to drive with [`ActorRef::ask_poll`].
    pub fn ask_start(&self, schema_id: u32, body: &[u8]) -> Result<PendingAsk> {
        self.ask_start_to(self.to, schema_id, body)
    }

    /// [`ActorRef::ask_start`] addressed to `to`.
    pub fn ask_start_to(&self, to: ActorId, schema_id: u32, body: &[u8]) -> Result<PendingAsk> {
        self.ask_start_sized(to, schema_id, body, DEFAULT_REPLY_BODY)
    }

    /// [`ActorRef::ask_start_to`] with an explicit reply-body cap in bytes:
    /// allocate a chunk for `64 + max_reply`, loan it to this node, journal it,
    /// and name it (with its generation) in the request envelope.
    ///
    /// Failures are all real and none are silent: `LayoutOverflow` when no size
    /// class fits `max_reply` (a deployment shaped its store pool too small —
    /// never a truncated answer), `PoolExhausted` when the pool is full, and
    /// `JournalFull` as backpressure when this node already holds every borrow
    /// slot. Every arm unwinds the loan, the journal record and the chunk.
    pub fn ask_start_sized(
        &self,
        to: ActorId,
        schema_id: u32,
        body: &[u8],
        max_reply: u32,
    ) -> Result<PendingAsk> {
        // One envelope describes one body through a `u16` `body_len`, so a cap
        // past that could not be honoured — refuse it here rather than promise
        // a handler capacity the reply envelope cannot express.
        if max_reply as usize > MAX_REPLY_BODY {
            return Err(Error::ReplyTooLarge {
                got: max_reply as usize,
                cap: MAX_REPLY_BODY,
            });
        }
        let reply = self.msgs.alloc_reply(max_reply)?;
        let slot = match self.msgs.loan_reply(&reply, self.owner, &self.journal_seg) {
            Ok(slot) => slot,
            Err(e) => {
                let _ = self.msgs.free(&reply);
                return Err(e);
            }
        };
        let rref = LocalRef(PackedRef::from_desc(&reply));
        match self.submit_to(
            to,
            MessageKind::Ask,
            schema_id,
            body,
            Some((rref, reply.generation)),
        ) {
            Ok((handle, corr, request)) => Ok(PendingAsk {
                handle,
                reply,
                journal_slot: slot,
                request,
                corr,
                done: false,
            }),
            Err(e) => {
                let _ = self.msgs.release_reply(&reply, slot, &self.journal_seg);
                Err(e)
            }
        }
    }

    /// A tell addressed to `to`.
    pub fn tell_raw_to(&self, to: ActorId, schema_id: u32, body: &[u8]) -> Result<()> {
        self.submit_to(to, MessageKind::Tell, schema_id, body, None)?;
        Ok(())
    }

    /// Poll a pending ask. `Ok(None)` = still in flight. `Ok(Some(..))` /
    /// `Err(..)` = terminal; the reply chunk has been freed and the
    /// `PendingAsk` must not be polled again.
    pub fn ask_poll(&self, pending: &mut PendingAsk) -> Result<Option<(Envelope, Vec<u8>)>> {
        match self.poll_terminal(pending)? {
            Some(terminal) => self.finish(pending, terminal).map(Some),
            None => Ok(None),
        }
    }

    /// `true` when the ask is terminal (a following `ask_poll` will not
    /// return `Ok(None)`). Does not consume anything.
    pub fn ask_ready(&self, pending: &PendingAsk) -> bool {
        !matches!(
            self.tq.queue().poll(pending.handle),
            Ok(TaskStatus::Queued | TaskStatus::Claimed)
        )
    }

    /// Free the reply chunks of dropped asks that have since gone terminal.
    /// NOT called implicitly: `holon-net`'s reactor calls it each pass, and any other caller that abandons asks must call it too or its orphaned reply chunks are only reclaimed when the node dies; a reactor may call it on a
    /// timer too. Returns how many were freed.
    pub fn sweep_orphans(&self) -> usize {
        let mut orphans = self.orphans.lock().expect("orphans poisoned");
        let before = orphans.len();
        orphans.retain(|(h, desc, slot)| match self.tq.queue().poll(*h) {
            Ok(TaskStatus::Queued | TaskStatus::Claimed) => true,
            _ => {
                let _ = self.msgs.release_reply(desc, *slot, &self.journal_seg);
                false
            }
        });
        before - orphans.len()
    }

    /// Number of orphaned reply chunks still waiting for their task to end.
    pub fn orphan_count(&self) -> usize {
        self.orphans.lock().expect("orphans poisoned").len()
    }

    fn orphan(&self, pending: &mut PendingAsk) {
        if pending.done {
            return;
        }
        pending.done = true;
        // Still queued: cancel wins → terminal now → release now. A won
        // pre-claim cancel also means the task will never be claimed, so no
        // worker will ever free the request chunk; without this the chunk is
        // lost for the life of the coordinator, and a peer that fires asks and
        // disconnects drains the store pool monotonically.
        if let Ok(true) = self.tq.queue().cancel(pending.handle) {
            let _ = self.msgs.free(&pending.request);
        }
        match self.tq.queue().poll(pending.handle) {
            Ok(TaskStatus::Queued | TaskStatus::Claimed) => self
                .orphans
                .lock()
                .expect("orphans poisoned")
                .push((pending.handle, pending.reply, pending.journal_slot)),
            _ => {
                let _ = self.msgs.release_reply(
                    &pending.reply,
                    pending.journal_slot,
                    &self.journal_seg,
                );
            }
        }
    }

    fn finish(&self, pending: &mut PendingAsk, terminal: Outcome) -> Result<(Envelope, Vec<u8>)> {
        let guard = self.finish_ref(pending, terminal)?;
        Ok((*guard.envelope(), guard.bytes().to_vec()))
    }

    /// The by-reference half of [`finish`](Self::finish): read the reply chunk
    /// in place and hand back the guard that owns it.
    ///
    /// On the success path nothing is freed — the guard does that on drop. On
    /// every failing path the chunk is released here, through the journal
    /// election, before the error is returned.
    fn finish_ref(&self, pending: &mut PendingAsk, terminal: Outcome) -> Result<ReplyRef<'_>> {
        pending.done = true;
        let release = |e: Error| -> Error {
            let _ =
                self.msgs
                    .release_reply(&pending.reply, pending.journal_slot, &self.journal_seg);
            e
        };
        match terminal {
            Outcome::Cancelled => return Err(release(Error::Cancelled)),
            Outcome::Failed => return Err(release(Error::Failed)),
            Outcome::Done(_) => {}
        }
        // A chunk nobody wrote (zeroed) fails validation: the task went
        // terminal without a handler answering (reaped out of retries).
        let (renv, rbody) = match self.msgs.read_message(&pending.reply) {
            Ok(v) => v,
            Err(_) => return Err(release(Error::Failed)),
        };
        match renv.kind()? {
            MessageKind::Reply if renv.corr == pending.corr => Ok(ReplyRef {
                msgs: &self.msgs,
                journal_seg: &self.journal_seg,
                desc: pending.reply,
                slot: pending.journal_slot,
                env: renv,
                body: rbody,
                released: false,
            }),
            MessageKind::Err if renv.corr == pending.corr => Err(release(Error::Failed)),
            _ => Err(release(Error::BadReply {
                kind: renv.kind,
                got: renv.corr,
                expected: pending.corr,
            })),
        }
    }

    /// Request/reply **by reference**: the answer stays in the reply chunk this
    /// ref allocated, and the guard borrows those bytes in place.
    ///
    /// `max_reply` is the body cap in bytes; the chunk allocated holds
    /// `64 + max_reply` rounded up to a pool size class, and the handler is
    /// told the real figure through [`Cx::max_reply`].
    pub fn ask_ref(&self, schema_id: u32, body: &[u8], max_reply: u32) -> Result<ReplyRef<'_>> {
        self.ask_ref_to(self.to, schema_id, body, max_reply)
    }

    /// [`ActorRef::ask_ref`] addressed to `to`.
    pub fn ask_ref_to(
        &self,
        to: ActorId,
        schema_id: u32,
        body: &[u8],
        max_reply: u32,
    ) -> Result<ReplyRef<'_>> {
        let mut pending = self.ask_start_sized(to, schema_id, body, max_reply)?;
        let terminal = match self.await_terminal(&pending) {
            Ok(t) => t,
            Err(e) => {
                // Unknown state: keep the chunk until observed terminal.
                self.orphan(&mut pending);
                return Err(e.into());
            }
        };
        self.finish_ref(&mut pending, terminal)
    }

    /// [`ActorRef::ask_poll`] by reference: `Ok(None)` = still in flight;
    /// `Ok(Some(guard))` = the answer, borrowed in place.
    pub fn ask_poll_ref(&self, pending: &mut PendingAsk) -> Result<Option<ReplyRef<'_>>> {
        match self.poll_terminal(pending)? {
            Some(terminal) => self.finish_ref(pending, terminal).map(Some),
            None => Ok(None),
        }
    }

    /// Block until `pending`'s task is terminal. A `StaleHandle` counts: the
    /// slot was reused, which only happens after it went terminal.
    fn await_terminal(&self, pending: &PendingAsk) -> core::result::Result<Outcome, TaskError> {
        if self.spin {
            loop {
                match self.tq.queue().poll(pending.handle) {
                    Ok(TaskStatus::Done(_)) => break Ok(Outcome::Done(ChunkDesc::ZERO)),
                    Ok(TaskStatus::Failed) => break Ok(Outcome::Failed),
                    Ok(TaskStatus::Cancelled) => break Ok(Outcome::Cancelled),
                    Ok(TaskStatus::Queued | TaskStatus::Claimed) => std::hint::spin_loop(),
                    Err(TaskError::StaleHandle) => break Ok(Outcome::Done(ChunkDesc::ZERO)),
                    Err(e) => break Err(e),
                }
            }
        } else {
            match self.tq.queue().wait(pending.handle, &self.done_parker) {
                Err(TaskError::StaleHandle) => Ok(Outcome::Done(ChunkDesc::ZERO)),
                other => other,
            }
        }
    }

    /// One non-blocking look at `pending`'s task. `Ok(None)` = still in flight.
    /// An unknown state orphans the ask (its chunk is kept until terminal).
    fn poll_terminal(&self, pending: &mut PendingAsk) -> Result<Option<Outcome>> {
        if pending.done {
            return Err(Error::Failed);
        }
        Ok(Some(match self.tq.queue().poll(pending.handle) {
            Ok(TaskStatus::Done(_)) => Outcome::Done(ChunkDesc::ZERO),
            Ok(TaskStatus::Failed) => Outcome::Failed,
            Ok(TaskStatus::Cancelled) => Outcome::Cancelled,
            Ok(TaskStatus::Queued | TaskStatus::Claimed) => return Ok(None),
            // The slot was reused: that only happens after terminal.
            Err(TaskError::StaleHandle) => Outcome::Done(ChunkDesc::ZERO),
            Err(e) => {
                self.orphan(pending);
                return Err(e.into());
            }
        }))
    }

    /// Abandon a pending ask (connection gone). Safe at any point.
    pub fn ask_abandon(&self, mut pending: PendingAsk) {
        self.orphan(&mut pending);
    }
}
