//! [`Actor`], [`ActorSystem`] (N actors over one mailbox, routed by `to`),
//! [`ActorRef`], and the per-message [`Cx`] with its RAII [`Pinned`] cell view.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use holon_core::{ActorId, Dispatch, Envelope, LocalRef, MessageKind, Payload, Reply};
use shm_arrow::SchemaRegistry;
use shm_artifact::VersionPin;
use shm_core::{ChunkDesc, PackedRef};
use shm_ring::DoorbellParker;
use shm_runtime::{Node, TaskQueueHandle};
use shm_store::KeyedStore;
use shm_task::{now_nanos, Error as TaskError, Outcome, TaskStatus};

use crate::chunk::MessagePool;
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
    self_id: ActorId,
    sender: ActorId,
    corr: u64,
    attempt: u32,
}

impl Cx<'_> {
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
        let store: KeyedStore<'_> = node.store()?;
        let store_ref = &store;
        let mut hosts = bind(actors);
        let msgs = MessagePool::new(store.data_segment().clone());
        let stopped = || stop.as_ref().is_some_and(|s| s.load(Ordering::Acquire));

        loop {
            if stopped() {
                return Ok(());
            }
            let claimed = if spin {
                loop {
                    if let Some(t) = tq.claim(lease_nanos) {
                        break t;
                    }
                    std::hint::spin_loop();
                }
            } else {
                tq.queue()
                    .claim_blocking_with_lease(worker_id, lease_nanos, &parker)
            };
            let claimed_at = Instant::now();
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
            let outcome = route(&mut hosts, store_ref, &env, body, attempt);

            // A tell: nothing to write back; complete/fail and free the request.
            if env.no_reply() {
                let done = match outcome {
                    Ok(_) => claimed.complete(ChunkDesc::ZERO),
                    Err(_) => claimed.fail(),
                };
                if done.is_ok() {
                    let _ = msgs.free(&req);
                }
                continue;
            }

            // An ask names the asker's reply chunk; one without is malformed.
            let Some(reply_ref) = env.payload_ref() else {
                if claimed.fail().is_ok() {
                    let _ = msgs.free(&req);
                }
                continue;
            };
            // Time fence: past half the lease this claim may already have been
            // reaped and redelivered, and the asker may have been answered by
            // the successor and freed the chunk — do not write into it. The
            // reap (re)delivers the message; the claim is left to lapse.
            if claimed_at.elapsed() > lease / 2 {
                continue;
            }
            let rdesc = msgs.reply_desc(reply_ref);
            let wrote = match &outcome {
                Ok(Reply::Inline(b)) => msgs.write_message_into(
                    &rdesc,
                    &Envelope::reply_to(&env, b.schema_id(), b.len()),
                    b.bytes(),
                ),
                Ok(Reply::None) => {
                    msgs.write_message_into(&rdesc, &Envelope::reply_to(&env, 0, 0), &[])
                }
                Err(_) => msgs.write_message_into(&rdesc, &Envelope::err_to(&env), &[]),
            };
            let done = if outcome.is_ok() && wrote.is_ok() {
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

/// Validate the envelope's kind, find the host `to` names, and dispatch.
fn route<'s>(
    hosts: &mut [(u64, Box<dyn Hosted<'s> + 's>)],
    store: &'s KeyedStore<'s>,
    env: &Envelope,
    body: &[u8],
    attempt: u32,
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
        store,
        self_id: ActorId(*id),
        sender: env.from(),
        corr: env.corr,
        attempt,
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
    to: ActorId,
    from: ActorId,
    corr: AtomicU64,
    spin: bool,
}

impl ActorRef {
    /// Build a ref to `to` over `node` (maps the store + task queue on first
    /// use). `node` stays usable afterwards; the ref owns its own handles.
    pub fn new(node: &mut Node, to: ActorId) -> Result<ActorRef> {
        let from = ActorId::new(0, node.actor_id());
        let tq = node.task_queue()?;
        let done_parker = tq.done_parker()?;
        let seg = node.store()?.data_segment().clone();
        Ok(ActorRef {
            tq,
            done_parker,
            msgs: MessagePool::new(seg),
            to,
            from,
            corr: AtomicU64::new(1),
            spin: false,
        })
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
    fn submit(
        &self,
        kind: MessageKind,
        schema_id: u32,
        body: &[u8],
        reply: Option<LocalRef>,
    ) -> Result<(shm_task::TaskHandle, u64)> {
        let corr = match kind {
            MessageKind::Tell => 0,
            _ => self.corr.fetch_add(1, Ordering::Relaxed),
        };
        let deadline = now_nanos().wrapping_add(SUBMIT_HORIZON.as_nanos() as u64);
        let mut env =
            Envelope::inline(kind, self.to, self.from, corr, schema_id, body.len() as u16)
                .with_deadline_nanos(deadline);
        if let Some(r) = reply {
            env = env.with_reply_ref(r);
        }
        let desc = self.msgs.write_message(&env, body)?;
        match self.tq.submit(desc, deadline) {
            Ok(h) => Ok((h, corr)),
            Err(e) => {
                let _ = self.msgs.free(&desc);
                Err(e.into())
            }
        }
    }

    /// Fire-and-forget: one envelope chunk + one submit.
    pub fn tell<P: Payload>(&self, msg: &P) -> Result<()> {
        self.submit(MessageKind::Tell, P::SCHEMA_ID, msg.as_bytes(), None)?;
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
        let reply = self.msgs.alloc_reply()?;
        let rref = LocalRef(PackedRef::from_desc(&reply));
        let (handle, corr) = match self.submit(MessageKind::Ask, schema_id, body, Some(rref)) {
            Ok(v) => v,
            Err(e) => {
                let _ = self.msgs.free(&reply);
                return Err(e);
            }
        };
        let terminal = if self.spin {
            loop {
                match self.tq.queue().poll(handle) {
                    Ok(TaskStatus::Done(_)) => break Ok(Outcome::Done(ChunkDesc::ZERO)),
                    Ok(TaskStatus::Failed) => break Ok(Outcome::Failed),
                    Ok(TaskStatus::Cancelled) => break Ok(Outcome::Cancelled),
                    Ok(TaskStatus::Queued | TaskStatus::Claimed) => std::hint::spin_loop(),
                    Err(TaskError::StaleHandle) => break Ok(Outcome::Done(ChunkDesc::ZERO)),
                    Err(e) => break Err(e),
                }
            }
        } else {
            match self.tq.queue().wait(handle, &self.done_parker) {
                Err(TaskError::StaleHandle) => Ok(Outcome::Done(ChunkDesc::ZERO)),
                other => other,
            }
        };
        // Whatever happened, the reply chunk is ours: read it, then free it.
        let read = self.msgs.read_message(&reply).map(|(e, b)| (e, b.to_vec()));
        let _ = self.msgs.free(&reply);
        match terminal? {
            Outcome::Cancelled => return Err(Error::Cancelled),
            Outcome::Failed => return Err(Error::Failed),
            Outcome::Done(_) => {}
        }
        // A chunk nobody wrote (zeroed) fails validation: the task went
        // terminal without a handler answering (reaped out of retries).
        let (renv, rbody) = read.map_err(|_| Error::Failed)?;
        match renv.kind()? {
            MessageKind::Reply if renv.corr == corr => Ok((renv, rbody)),
            MessageKind::Err if renv.corr == corr => Err(Error::Failed),
            _ => Err(Error::BadReply {
                kind: renv.kind,
                got: renv.corr,
                expected: corr,
            }),
        }
    }
}
