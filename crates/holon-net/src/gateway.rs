//! The gateway shard: one reactor thread over listeners, client sockets,
//! the done doorbell and the topic-event doorbell (ADR-0017 §2).

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use holon_actor::ActorRef;
use holon_core::{ActorId, Envelope, MessageKind};
use holon_object::{ObjectSide, ObjectStore};
use holon_topic::{TopicLog, TopicPolicy, TopicRead, TopicSide, TOPIC_EVENTS};
use shm_arrow::SchemaRegistry;
use shm_artifact::VersionEvent;
use shm_ring::{DoorbellParker, Msg, Subscriber};
use shm_runtime::Node;

use crate::conn::{AskOrigin, Connection, Face, SubMode, Subscription};
use crate::error::{Error, Result};
use crate::http;
use crate::object;
use crate::reactor::{Interest, Reactor};
use crate::tcp;
use crate::wire;

const TOKEN_TCP_LISTEN: usize = 0;
const TOKEN_HTTP_LISTEN: usize = 1;
const TOKEN_DONE: usize = 2;
const TOKEN_EVENTS: usize = 3;
const FIRST_CONN_TOKEN: usize = 16;

/// The default reply-body cap a gateway sizes each ask's reply chunk to:
/// the largest chunk of the stock `RuntimeConfig::store_pool` (8 KiB) minus the
/// 64-byte envelope, so a gateway on an unshaped deployment works as-is. A
/// deployment with a shaped pool raises [`GatewayConfig::max_reply`] toward
/// [`holon_core::MAX_REPLY_BODY`].
pub const DEFAULT_MAX_REPLY: u32 = 8192 - holon_core::ENVELOPE_SIZE as u32;

/// Default byte budget for one long-poll answer (8 MiB).
///
/// `?max=` bounds a long-poll by *message count*, which bounds nothing when a
/// message can be megabytes: 65536 messages is a legal request, and the body is
/// accumulated in memory before a single byte is written. The budget is the
/// bound that actually holds, and it costs the caller nothing because the
/// protocol is already resumable — a short answer carries `X-Next-Cursor` and
/// the caller polls again from there, exactly as it would after a `max` cut.
pub const DEFAULT_MAX_LONGPOLL_BYTES: usize = 8 * 1024 * 1024;

/// Default stall budget for one streamed object body (30 s).
///
/// A transfer holds a journaled version pin for its whole life — that is what
/// makes its slices safe to read across reactor passes — so a reader that stops
/// reading would otherwise hold a store version alive for as long as it keeps
/// the socket open. This bounds *stall*, not size: the clock resets whenever a
/// slice is queued, so an arbitrarily large object streaming at any rate at all
/// is never cut off.
pub const DEFAULT_OBJECT_STALL: Duration = Duration::from_secs(30);

/// Gateway configuration.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Coordinator UDS path.
    pub uds: PathBuf,
    /// Node name prefix (each shard registers as `<name>-<shard>`).
    pub name: String,
    /// TCP (envelope frame) bind address; `None` = no TCP face.
    pub tcp: Option<SocketAddr>,
    /// HTTP bind address; `None` = no HTTP face.
    pub http: Option<SocketAddr>,
    /// Reactor threads. Each owns a node connection and shares the listeners.
    pub shards: usize,
    /// Longest park in `poll(2)`; also the timer tick.
    pub poll_timeout: Duration,
    /// Replay window for topics created through this gateway.
    pub topic_window: u32,
    /// SSE comment heartbeat interval.
    pub sse_heartbeat: Duration,
    /// Asks older than this are abandoned and answered with an error.
    pub ask_timeout: Duration,
    /// The reply-body cap, in bytes, that every socket-borne ask sizes its
    /// reply chunk to ([`DEFAULT_MAX_REPLY`]).
    ///
    /// Deployment-shaped, and it must be honest about two things: the store
    /// pool needs a size class of at least `64 + max_reply` or every ask fails
    /// at submit with `LayoutOverflow` (loudly — never a truncated answer), and
    /// each in-flight ask holds a chunk of that class, so raising it multiplies
    /// the pool pressure of a deep pipeline.
    pub max_reply: u32,
    /// Byte budget for one long-poll answer ([`DEFAULT_MAX_LONGPOLL_BYTES`]).
    /// Delivery stops at the first message that would cross it, and the answer
    /// carries the cursor to resume from.
    pub max_longpoll_bytes: usize,
    /// Longest a streamed object body may make no progress before the transfer
    /// is abandoned and its version pin released ([`DEFAULT_OBJECT_STALL`]).
    pub object_stall: Duration,
}

impl GatewayConfig {
    /// Defaults: no faces bound, one shard, 50 ms tick, 1024-message window,
    /// 15 s SSE heartbeat, 30 s ask timeout, [`DEFAULT_MAX_REPLY`] reply cap.
    pub fn new(uds: impl Into<PathBuf>, name: &str) -> GatewayConfig {
        GatewayConfig {
            uds: uds.into(),
            name: name.to_string(),
            tcp: None,
            http: None,
            shards: 1,
            poll_timeout: Duration::from_millis(50),
            topic_window: 1024,
            sse_heartbeat: Duration::from_secs(15),
            ask_timeout: Duration::from_secs(30),
            max_reply: DEFAULT_MAX_REPLY,
            max_longpoll_bytes: DEFAULT_MAX_LONGPOLL_BYTES,
            object_stall: DEFAULT_OBJECT_STALL,
        }
    }
}

/// Counters shared by every shard.
#[derive(Debug, Default)]
pub struct GatewayStats {
    /// Connections accepted.
    pub accepted: AtomicU64,
    /// Connections currently open.
    pub open: AtomicU64,
    /// Inbound frames / HTTP requests.
    pub requests: AtomicU64,
    /// Asks submitted to actors.
    pub asks: AtomicU64,
    /// Replies written.
    pub replies: AtomicU64,
    /// Tells submitted.
    pub tells: AtomicU64,
    /// Publishes.
    pub publishes: AtomicU64,
    /// Topic messages delivered (frames or SSE events).
    pub events: AtomicU64,
    /// Lag notices sent.
    pub lagged: AtomicU64,
    /// Errors sent to clients.
    pub errors: AtomicU64,
    /// Reactor wake-ups.
    pub wakes: AtomicU64,
}

impl GatewayStats {
    #[inline]
    pub(crate) fn inc(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }
}

/// A running gateway (its shard threads).
pub struct Gateway {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<Result<()>>>,
    stats: Arc<GatewayStats>,
    tcp_addr: Option<SocketAddr>,
    http_addr: Option<SocketAddr>,
}

impl Gateway {
    /// Bind the faces and start the shards.
    pub fn start(cfg: GatewayConfig) -> Result<Gateway> {
        let tcp = cfg.tcp.map(TcpListener::bind).transpose()?;
        let http = cfg.http.map(TcpListener::bind).transpose()?;
        let tcp_addr = tcp.as_ref().map(|l| l.local_addr()).transpose()?;
        let http_addr = http.as_ref().map(|l| l.local_addr()).transpose()?;
        for l in tcp.iter().chain(http.iter()) {
            l.set_nonblocking(true)?;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(GatewayStats::default());
        let mut threads = Vec::new();
        for i in 0..cfg.shards.max(1) {
            let tcp = tcp.as_ref().map(TcpListener::try_clone).transpose()?;
            let http = http.as_ref().map(TcpListener::try_clone).transpose()?;
            let cfg = cfg.clone();
            let stop = stop.clone();
            let stats = stats.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("holon-gw-{i}"))
                    .spawn(move || Shard::run(cfg, i, tcp, http, stop, stats))?,
            );
        }
        Ok(Gateway {
            stop,
            threads,
            stats,
            tcp_addr,
            http_addr,
        })
    }

    /// The bound TCP address.
    #[inline]
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.tcp_addr
    }

    /// The bound HTTP address.
    #[inline]
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// Shared counters.
    #[inline]
    pub fn stats(&self) -> &Arc<GatewayStats> {
        &self.stats
    }

    /// The stop flag (settable from a signal handler).
    #[inline]
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    /// Ask every shard to stop and wait for them.
    pub fn stop(self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        for t in self.threads {
            match t.join() {
                Ok(r) => r?,
                Err(_) => return Err(Error::Protocol("shard thread panicked")),
            }
        }
        Ok(())
    }

    /// Block until the stop flag is set, then join.
    pub fn join(self) -> Result<()> {
        while !self.stop.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(100));
        }
        self.stop()
    }
}

/// Everything a request handler needs from its shard.
pub struct ShardCtx<'s> {
    /// The shard's actor ref (target re-addressed per frame).
    pub aref: &'s ActorRef,
    /// The topic log.
    pub topics: &'s TopicLog<'s>,
    /// The object store behind `GET /objects/{key}`.
    pub objects: &'s ObjectStore<'s>,
    /// The configuration.
    pub cfg: &'s GatewayConfig,
    /// Counters.
    pub stats: Arc<GatewayStats>,
    /// Topic ids with new messages since the last delivery pass.
    pub dirty: HashSet<u32>,
    /// Topic id → name (for every topic some connection follows).
    pub topic_names: HashMap<u32, String>,
    /// Shard index.
    pub shard: usize,
}

impl ShardCtx<'_> {
    /// Default topic policy of this gateway.
    #[inline]
    pub fn policy(&self, window: u32) -> TopicPolicy {
        TopicPolicy {
            max_messages: if window == 0 {
                self.cfg.topic_window
            } else {
                window
            },
        }
    }

    /// Forget a deleted topic: its key id now names a tombstoned slot, and
    /// every subscription to it is dropped at the next delivery pass.
    pub fn forget_topic(&mut self, topic: &str) {
        self.topic_names.retain(|_, name| name != topic);
    }

    /// Resolve a topic name to its key id, remembering the mapping.
    pub fn topic_id(&mut self, topic: &str) -> Result<u32> {
        let id = self.topics.key_id(topic)?;
        self.topic_names
            .entry(id)
            .or_insert_with(|| topic.to_string());
        Ok(id)
    }

    /// Register a subscription on `conn` and schedule its first delivery.
    pub fn subscribe(
        &mut self,
        conn: &mut Connection,
        topic: &str,
        from_seq: u64,
        mode: SubMode,
    ) -> Result<(u32, u64, u64)> {
        let topic_id = self.topic_id(topic)?;
        let head = self.topics.head(topic)?;
        let cursor = if from_seq == u64::MAX {
            head
        } else {
            from_seq.min(head)
        };
        conn.subs.retain(|s| s.topic_id != topic_id);
        conn.subs.push(Subscription {
            topic: topic.to_string(),
            topic_id,
            cursor,
            dirty: true,
            mode,
        });
        self.dirty.insert(topic_id);
        Ok((topic_id, head, cursor))
    }
}

struct Shard;

impl Shard {
    fn run(
        cfg: GatewayConfig,
        shard: usize,
        tcp: Option<TcpListener>,
        http: Option<TcpListener>,
        stop: Arc<AtomicBool>,
        stats: Arc<GatewayStats>,
    ) -> Result<()> {
        let name = format!("{}-{shard}", cfg.name);
        let mut node = Node::connect(&cfg.uds, &name, Arc::new(SchemaRegistry::new()))?;
        node.start_heartbeat(Duration::from_millis(150));
        node.open_store()?;
        node.open_task_queue()?;
        let aref = ActorRef::new(&mut node, ActorId::NONE)?;
        let mut events: Subscriber<DoorbellParker> = node.subscribe(TOPIC_EVENTS)?;
        let side = TopicSide::open(&mut node)?;
        let oside = ObjectSide::open(&mut node)?;
        let registry = node.registry().clone();
        // Shared, not `&mut`: the object store needs `&Node` alongside the
        // store handle so it can have a dedicated segment granted for an object
        // the shared pool cannot hold.
        let node_ref = &node;
        let store = node_ref.opened_store()?;
        let topics = TopicLog::new(&store, &side, registry.clone());
        // Topics and objects are two shapes over the *same* keyed store: one
        // entry that appends within a window, one entry that replaces.
        let objects = ObjectStore::new(&store, &oside, registry, node_ref);
        let mut ctx = ShardCtx {
            aref: &aref,
            topics: &topics,
            objects: &objects,
            cfg: &cfg,
            stats: stats.clone(),
            dirty: HashSet::new(),
            topic_names: HashMap::new(),
            shard,
        };

        let mut reactor = Reactor::new();
        if let Some(l) = &tcp {
            reactor.register(
                std::os::fd::AsRawFd::as_raw_fd(l),
                TOKEN_TCP_LISTEN,
                Interest::READ,
            );
        }
        if let Some(l) = &http {
            reactor.register(
                std::os::fd::AsRawFd::as_raw_fd(l),
                TOKEN_HTTP_LISTEN,
                Interest::READ,
            );
        }
        reactor.register(aref.done_fd(), TOKEN_DONE, Interest::READ);
        let events_fd = events.parker().read_fd();
        reactor.register(events_fd, TOKEN_EVENTS, Interest::READ);
        let ring = events.ring().clone();

        let mut conns: HashMap<usize, Connection> = HashMap::new();
        let mut next_token = FIRST_CONN_TOKEN;
        let mut done_dirty = true;
        let mut last_tick = Instant::now();

        while !stop.load(Ordering::Acquire) {
            // 1. Topic notifications → dirty topic ids.
            while let Some(m) = events.try_recv() {
                match m {
                    Msg::Sample(d) => {
                        if let Some(ev) = VersionEvent::from_desc(&d) {
                            if ctx.topic_names.contains_key(&ev.name_id) {
                                ctx.dirty.insert(ev.name_id);
                            }
                        }
                    }
                    // Lost wake-ups: every followed topic may have moved.
                    Msg::Lagged(_) => {
                        let all: Vec<u32> = ctx.topic_names.keys().copied().collect();
                        ctx.dirty.extend(all);
                    }
                }
            }

            // 2. Reply completions: after a done-doorbell wake, and always
            //    while asks are pending (the poll is a few atomic loads per
            //    ask; a missed doorbell must never strand a reply).
            let pending: usize = conns.values().map(|c| c.asks.len()).sum();
            if done_dirty || pending > 0 {
                done_dirty = false;
                for conn in conns.values_mut() {
                    poll_asks(&mut ctx, conn);
                }
                aref.sweep_orphans();
            }

            // 3. Topic delivery (dirty topics + connections paused by
            //    backpressure that have since drained).
            deliver(&mut ctx, &mut conns);

            let now = Instant::now();

            // 3b. Streamed object bodies: each resumes from its own offset,
            //     bounded by the write high-water mark, exactly as a paused
            //     subscription resumes from its cursor. A completed transfer
            //     drops its pin here.
            for conn in conns.values_mut() {
                if !conn.closed {
                    object::advance(conn, now);
                }
            }

            // 3b. Pipelining: a request that arrived in the same packet as an
            // earlier one sits in the read buffer while that one is being
            // answered, and `dispatch` is only ever entered from a readability
            // event. Once the connection is free again there is no further
            // readability to come — the bytes are already here — so nothing
            // would ever answer it and the connection (and its fd) would be
            // stranded for good. One `write(2)` carrying two GETs is enough.
            let ready_again: Vec<usize> = conns
                .iter()
                .filter(|(_, c)| {
                    c.face == Face::Http
                        && !c.closed
                        && !c.http.busy
                        && !c.http.sse
                        && !c.close_after_flush
                        && !c.inbound().is_empty()
                })
                .map(|(t, _)| *t)
                .collect();
            for tok in ready_again {
                if let Some(conn) = conns.get_mut(&tok) {
                    if let Err(_e) = http::dispatch(&mut ctx, conn) {
                        conn.closed = true;
                    }
                }
            }

            // 4. Timers.
            if now.duration_since(last_tick) >= cfg.poll_timeout {
                last_tick = now;
                for conn in conns.values_mut() {
                    tick(&mut ctx, conn, now);
                }
            }

            // 5. Flush eagerly, then set interests; drop dead connections.
            let mut dead = Vec::new();
            for (tok, conn) in conns.iter_mut() {
                if conn.wants_write() {
                    let _ = conn.flush();
                }
                if conn.closed {
                    dead.push(*tok);
                    continue;
                }
                let mut i = Interest::NONE;
                if conn.wants_read() {
                    i = Interest::READ;
                }
                if conn.wants_write() {
                    i = if i == Interest::READ {
                        Interest::READ_WRITE
                    } else {
                        Interest::WRITE
                    };
                }
                reactor.set_interest(*tok, i);
            }
            for tok in dead {
                close_conn(&mut ctx, &mut conns, &mut reactor, tok);
            }

            // 6. Park: the ring's waiter protocol, so a producer rings the
            //    doorbell only while we are actually about to sleep.
            //    Same handshake for the task queue's done doorbell: register,
            //    re-check every pending ask, park, withdraw.
            ring.add_waiter();
            let pending: usize = conns.values().map(|c| c.asks.len()).sum();
            let mut done_now = false;
            if pending > 0 {
                aref.add_done_waiter();
                done_now = conns
                    .values_mut()
                    .any(|c| c.asks.iter_mut().any(|a| aref.ask_ready(&a.pending)));
            }
            // A body stream that is not throttled has bytes to queue right
            // now. Without this the loop would park for `poll_timeout` between
            // slices whenever a flush emptied the buffer, because an empty
            // buffer asks for no `POLLOUT`.
            let streaming = conns
                .values()
                .any(|c| c.body.is_some() && !c.closed && !c.throttled());
            // A pipelined request already in the read buffer is work in hand;
            // parking on it would stall it until the next unrelated event.
            let buffered_request = conns.values().any(|c| {
                c.face == Face::Http && !c.closed && !c.http.busy && !c.inbound().is_empty()
            });
            let timeout = if ring.head() > events.cursor()
                || !ctx.dirty.is_empty()
                || done_now
                || streaming
                || buffered_request
            {
                Duration::ZERO
            } else {
                cfg.poll_timeout
            };
            let ready = match reactor.poll(timeout) {
                Ok(r) => r.to_vec(),
                Err(e) => {
                    ring.remove_waiter();
                    if pending > 0 {
                        aref.remove_done_waiter();
                    }
                    return Err(e.into());
                }
            };
            ring.remove_waiter();
            if pending > 0 {
                aref.remove_done_waiter();
                done_dirty = true;
            }
            GatewayStats::inc(&stats.wakes);

            // 7. Readiness.
            for r in ready {
                match r.token {
                    TOKEN_TCP_LISTEN => {
                        if let Some(l) = &tcp {
                            accept_all(
                                &mut ctx,
                                l,
                                Face::Tcp,
                                &mut conns,
                                &mut reactor,
                                &mut next_token,
                            );
                        }
                    }
                    TOKEN_HTTP_LISTEN => {
                        if let Some(l) = &http {
                            accept_all(
                                &mut ctx,
                                l,
                                Face::Http,
                                &mut conns,
                                &mut reactor,
                                &mut next_token,
                            );
                        }
                    }
                    TOKEN_DONE => {
                        aref.drain_done_fd();
                        done_dirty = true;
                    }
                    TOKEN_EVENTS => {
                        let _ = shm_core::doorbell_park(events_fd, Duration::ZERO);
                    }
                    tok => {
                        let Some(conn) = conns.get_mut(&tok) else {
                            continue;
                        };
                        if r.writable {
                            let _ = conn.flush();
                        }
                        if r.readable && !conn.closed {
                            match conn.read() {
                                Ok(_) => {
                                    if let Err(e) = dispatch(&mut ctx, conn) {
                                        // Protocol violation: close.
                                        let _ = e;
                                        conn.closed = true;
                                    }
                                }
                                Err(_) => conn.closed = true,
                            }
                        }
                        // A hung-up peer will never read the rest of a body
                        // stream: close now rather than holding its version pin
                        // until a write finally fails.
                        if r.hup && (conn.outbound_len() == 0 || conn.body.is_some()) {
                            conn.closed = true;
                        }
                        if conn.closed {
                            close_conn(&mut ctx, &mut conns, &mut reactor, tok);
                        }
                    }
                }
            }
        }

        // Shutdown: abandon in-flight asks so their reply chunks are reclaimed
        // once terminal, and drop every connection.
        let toks: Vec<usize> = conns.keys().copied().collect();
        for tok in toks {
            close_conn(&mut ctx, &mut conns, &mut reactor, tok);
        }
        Ok(())
    }
}

fn accept_all(
    ctx: &mut ShardCtx<'_>,
    l: &TcpListener,
    face: Face,
    conns: &mut HashMap<usize, Connection>,
    reactor: &mut Reactor,
    next_token: &mut usize,
) {
    loop {
        match l.accept() {
            Ok((stream, _)) => {
                let tok = *next_token;
                *next_token += 1;
                match Connection::new(tok, stream, face) {
                    Ok(conn) => {
                        reactor.register(conn.fd(), tok, Interest::READ);
                        conns.insert(tok, conn);
                        GatewayStats::inc(&ctx.stats.accepted);
                        GatewayStats::inc(&ctx.stats.open);
                    }
                    Err(_) => continue,
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
}

/// Drop a connection and release everything it holds: in-flight asks and, if
/// one is running, the streamed object body's version pin.
fn close_conn(
    ctx: &mut ShardCtx<'_>,
    conns: &mut HashMap<usize, Connection>,
    reactor: &mut Reactor,
    tok: usize,
) {
    reactor.deregister(tok);
    if let Some(conn) = conns.remove(&tok) {
        for a in conn.asks {
            ctx.aref.ask_abandon(a.pending);
        }
        // The teardown funnel for the streamed-object pin: disconnect,
        // write error, protocol error, HUP and shard shutdown all arrive here.
        // Explicit rather than implicit so the release is visible next to the
        // ask's, even though `VersionPin` releases itself on drop.
        drop(conn.body);
        ctx.stats.open.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Parse and handle every complete frame / request in the read buffer.
fn dispatch(ctx: &mut ShardCtx<'_>, conn: &mut Connection) -> Result<()> {
    match conn.face {
        Face::Tcp => tcp::dispatch(ctx, conn),
        Face::Http => http::dispatch(ctx, conn),
    }
}

/// Poll every in-flight ask of `conn`; write the completed ones.
///
/// This is the single point where a socket-borne ask's answer is read back,
/// and therefore where a by-reference reply is **materialised and released**:
/// the guard borrows the shared-memory chunk, one copy goes into the
/// connection's out buffer, and the descriptor never approaches the socket
/// (invariant I2). The guard's `Drop` covers every exit below, which is why the
/// API hands back a guard rather than a bare descriptor.
fn poll_asks(ctx: &mut ShardCtx<'_>, conn: &mut Connection) {
    if conn.asks.is_empty() {
        return;
    }
    let mut i = 0;
    while i < conn.asks.len() {
        let outcome = ctx.aref.ask_poll_ref(&mut conn.asks[i].pending);
        match outcome {
            Ok(None) => {
                i += 1;
                continue;
            }
            Ok(Some(guard)) => {
                let done = conn.asks.swap_remove(i);
                GatewayStats::inc(&ctx.stats.replies);
                match done.origin {
                    AskOrigin::Frame(req) => {
                        tcp::write_reply(conn, &req, guard.envelope(), guard.bytes())
                    }
                    AskOrigin::Http => http::write_ask_reply(conn, guard.envelope(), guard.bytes()),
                }
            }
            Err(e) => {
                let done = conn.asks.swap_remove(i);
                GatewayStats::inc(&ctx.stats.errors);
                let (code, msg) = match e {
                    holon_actor::Error::Cancelled => (wire::code::CANCELLED, "cancelled"),
                    _ => (wire::code::FAILED, "ask failed"),
                };
                match done.origin {
                    AskOrigin::Frame(req) => tcp::write_error(conn, &req, code, msg),
                    AskOrigin::Http => http::write_ask_error(conn, code, msg),
                }
            }
        }
    }
}

/// Timer pass: ask timeouts, SSE heartbeats, long-poll deadlines.
fn tick(ctx: &mut ShardCtx<'_>, conn: &mut Connection, now: Instant) {
    let timeout = ctx.cfg.ask_timeout;
    let mut i = 0;
    while i < conn.asks.len() {
        if now.duration_since(conn.asks[i].started) > timeout {
            let a = conn.asks.swap_remove(i);
            ctx.aref.ask_abandon(a.pending);
            GatewayStats::inc(&ctx.stats.errors);
            match a.origin {
                AskOrigin::Frame(req) => {
                    tcp::write_error(conn, &req, wire::code::CANCELLED, "ask timed out")
                }
                AskOrigin::Http => http::write_ask_error(conn, wire::code::CANCELLED, "timeout"),
            }
        } else {
            i += 1;
        }
    }
    if conn.face == Face::Http {
        http::tick(ctx, conn, now);
        object::tick(conn, now, ctx.cfg.object_stall);
    }
}

/// Deliver topic messages to every connection whose subscription is dirty
/// (new messages, or paused by backpressure and now drained).
///
/// One pinned read per topic per pass: the read starts at the smallest
/// cursor among the deliverable subscribers and each subscriber takes the
/// messages past its own cursor. A cursor below the retained window is
/// reported as lag and moved to the window base.
fn deliver(ctx: &mut ShardCtx<'_>, conns: &mut HashMap<usize, Connection>) {
    // Which topics need a read, and from where.
    let mut wanted: HashMap<u32, u64> = HashMap::new();
    for conn in conns.values() {
        if conn.closed || conn.throttled() {
            continue;
        }
        for s in &conn.subs {
            if s.dirty || ctx.dirty.contains(&s.topic_id) {
                wanted
                    .entry(s.topic_id)
                    .and_modify(|c| *c = (*c).min(s.cursor))
                    .or_insert(s.cursor);
            }
        }
    }
    ctx.dirty.clear();
    if wanted.is_empty() {
        return;
    }
    for (topic_id, min_cursor) in wanted {
        let Some(topic) = ctx.topic_names.get(&topic_id).cloned() else {
            continue;
        };
        let read: TopicRead = match ctx.topics.read_since(&topic, min_cursor) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for conn in conns.values_mut() {
            if conn.closed {
                continue;
            }
            let Some(si) = conn.subs.iter().position(|s| s.topic_id == topic_id) else {
                continue;
            };
            let (dirty, cursor) = (conn.subs[si].dirty, conn.subs[si].cursor);
            if !dirty && cursor >= read.head {
                continue;
            }
            if conn.throttled() {
                conn.subs[si].dirty = true;
                continue;
            }
            let mode = conn.subs[si].mode;
            let mut cursor = cursor;
            // Lag: the window no longer holds `cursor + 1`.
            if read.truncated && cursor < read.from_seq {
                match conn.face {
                    Face::Tcp => tcp::write_lagged(conn, topic_id, read.from_seq, read.head),
                    Face::Http => http::write_lagged(conn, &topic, read.from_seq, read.head),
                }
                GatewayStats::inc(&ctx.stats.lagged);
                cursor = read.from_seq;
            }
            let mut delivered = 0u32;
            let mut paused = false;
            let max = match mode {
                SubMode::Stream => u32::MAX,
                SubMode::LongPoll { max, .. } => max,
            };
            let mut ndjson: Vec<u8> = Vec::new();
            for m in read.messages() {
                let Ok(m) = m else { continue };
                if m.seq <= cursor {
                    continue;
                }
                if delivered >= max {
                    break;
                }
                if conn.throttled() {
                    paused = true;
                    break;
                }
                match (conn.face, mode) {
                    (Face::Tcp, _) => tcp::write_event(conn, topic_id, &m),
                    (Face::Http, SubMode::Stream) => http::write_sse_event(conn, &m),
                    (Face::Http, SubMode::LongPoll { .. }) => http::push_ndjson(&mut ndjson, &m),
                }
                cursor = m.seq;
                delivered += 1;
                GatewayStats::inc(&ctx.stats.events);
                // The byte budget, not `max`, is what bounds the answer: one
                // message is enough to cross it. Checked *after* the push so a
                // single oversized message is still delivered rather than
                // wedging the cursor forever.
                if ndjson.len() >= ctx.cfg.max_longpoll_bytes {
                    break;
                }
            }
            conn.subs[si].cursor = cursor;
            conn.subs[si].dirty = paused;
            if let SubMode::LongPoll { .. } = mode {
                if delivered > 0 {
                    http::finish_longpoll(conn, si, ndjson, cursor);
                }
            }
        }
    }
}

/// Build the reply envelope for a client request: the client's `to`/`from`
/// swapped, its `corr` kept.
pub(crate) fn reply_env(req: &Envelope, schema_id: u32, body_len: usize) -> Envelope {
    let mut e = Envelope::inline(
        MessageKind::Reply,
        ActorId(req.from),
        ActorId(req.to),
        req.corr,
        schema_id,
        body_len as u16,
    );
    e.epoch = req.epoch;
    e
}
