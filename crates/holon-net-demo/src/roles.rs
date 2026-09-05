//! The role entry points behind the `holon-net-demo` binary, plus the
//! in-process helpers the tests share.

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use holon_actor::ActorSystem;
use holon_core::{ActorId, Payload};
use holon_net::client::{FrameClient, HttpClient, Inbound};
use holon_net::{Gateway, GatewayConfig};
use shm_arrow::SchemaRegistry;
use shm_core::PoolConfig;
use shm_runtime::{Coordinator, Node, RuntimeConfig};

use crate::actors::{
    Count, Counter, Echo, Get, Inc, Range, RangeAck, RangeReq, Sink, Work, WorkDone, COUNTER,
    COUNTER_GET, COUNTER_INC, DONE_TOPIC, ECHO, ECHO_REQ, RANGE, RANGE_REQ, SINK, WORK_REQ,
};

/// Parsed command line: `<role> [--key value | --flag]…`.
#[derive(Clone, Debug, Default)]
pub struct Opts {
    /// The role name.
    pub role: String,
    values: HashMap<String, String>,
    flags: Vec<String>,
}

const BOOL_FLAGS: &[&str] = &["--spin", "--parent-watch", "--compress", "--quiet"];

impl Opts {
    /// Parse `args` (including `argv[0]`).
    pub fn parse(args: &[String]) -> Opts {
        let mut o = Opts {
            role: args.get(1).cloned().unwrap_or_default(),
            ..Opts::default()
        };
        let mut i = 2;
        while i < args.len() {
            let a = &args[i];
            if BOOL_FLAGS.contains(&a.as_str()) {
                o.flags.push(a.clone());
                i += 1;
            } else if let Some(k) = a.strip_prefix("--") {
                if let Some(v) = args.get(i + 1) {
                    o.values.insert(k.to_string(), v.clone());
                }
                i += 2;
            } else {
                i += 1;
            }
        }
        o
    }

    /// A string option.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// A parsed numeric option.
    pub fn num<T: std::str::FromStr>(&self, key: &str) -> Option<T> {
        self.get(key).and_then(|v| v.parse().ok())
    }

    /// A boolean flag.
    pub fn flag(&self, key: &str) -> bool {
        self.flags.iter().any(|f| f == key)
    }

    fn uds(&self) -> String {
        self.get("uds").unwrap_or_default().to_string()
    }
}

/// Dispatch a role; returns the process exit code.
pub fn run(opts: &Opts) -> i32 {
    let r = match opts.role.as_str() {
        "coordinator" => run_coordinator(opts),
        "host" => run_host(opts),
        "gateway" => run_gateway(opts),
        "tcp-client" => run_tcp_client(opts).map(|r| println!("{}", r.to_kv())),
        "tcp-sub" => run_tcp_sub(opts).map(|r| println!("{}", r.to_kv())),
        "publisher" => run_publisher(opts).map(|r| println!("{}", r.to_kv())),
        "sse-client" => run_sse_client(opts).map(|r| println!("{}", r.to_kv())),
        "http-client" => run_http_client(opts).map(|r| println!("{}", r.to_kv())),
        "raw-echo" => run_raw_echo(opts).map(|r| println!("{}", r.to_kv())),
        "bench" => crate::bench::run(opts),
        "bench-legacy" => run_bench(opts),
        other => Err(format!(
            "unknown role {other:?}; expected coordinator|host|gateway|tcp-client|tcp-sub|publisher|sse-client|http-client|raw-echo|bench|bench-legacy"
        )),
    };
    match r {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{}: {e}", opts.role);
            1
        }
    }
}

// ---- shared helpers ----

/// Wall-clock nanoseconds since the Unix epoch (same-host latency stamps).
pub fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// A segment base unlikely to collide with other runs on this host.
pub fn unique_seg_base() -> u32 {
    static N: AtomicU64 = AtomicU64::new(0);
    // A coordinator spans ~8.2k segment ids past its base (store ids sit at
    // +8192); processes and in-process stacks must never overlap.
    let pid = std::process::id() % 2000;
    let n = N.fetch_add(1, Ordering::Relaxed) as u32;
    10_000_000 + pid * 200_000 + n * 10_000
}

/// The runtime config every role agrees on.
pub fn runtime_config(seg_base: u32, task_capacity: u32, store_mb: usize) -> RuntimeConfig {
    let mut c = RuntimeConfig::with_seg_base(seg_base);
    c.task_capacity = task_capacity.next_power_of_two().max(256);
    c.ring_capacity = 4096;
    c.store_capacity = 1024;
    c.payload_size = 4 << 20;
    // The pool is SHAPED, not uniform, and every class matters for a
    // different reason:
    //
    // - 256 B carries an ask's request and reply chunk (two per in-flight
    //   ask) and a small topic message.
    // - 512 B–4 KiB carry topic messages and their per-version manifests.
    // - The largest classes exist only for the **window re-root manifest**,
    //   which lists every retained batch in one chunk: roughly
    //   `64 + 28·window` bytes. That makes the largest size class a hard
    //   ceiling on the replay window: 64 KiB caps it near 2 300 messages,
    //   128 KiB near 4 600. Raising the window without raising the class
    //   fails the next re-root with "allocation exceeds largest size class".
    const SHAPE: [(u32, u32); 11] = [
        (256, 32_768),
        (512, 8_192),
        (1024, 8_192),
        (2048, 4_096),
        (4096, 1_024),
        (8192, 512),
        (16384, 256),
        (32768, 128),
        (65536, 64),
        (131_072, 32),
        (262_144, 16),
    ];
    let set_bytes: usize = SHAPE
        .iter()
        .map(|&(size, n)| size as usize * n as usize)
        .sum();
    // Leave a tenth of the segment for the pool's own control array.
    let sets = (((store_mb << 20) * 9 / 10) / set_bytes).max(1) as u32;
    c.store_data_size = store_mb << 20;
    c.store_pool = PoolConfig {
        classes: SHAPE
            .iter()
            .map(|&(chunk_size, weight)| shm_core::SizeClass {
                chunk_size,
                chunk_count: weight * sets,
            })
            .collect(),
    };
    c
}

/// The largest replay window a pool with `max_class` bytes as its biggest
/// size class can re-root, in messages (see [`runtime_config`]).
pub fn max_window_for(max_class: u32) -> u32 {
    max_class.saturating_sub(64) / 28
}

/// Connect a node with a heartbeat.
pub fn connect(uds: &Path, name: &str) -> Result<Node, String> {
    let mut node = Node::connect(uds, name, Arc::new(SchemaRegistry::new()))
        .map_err(|e| format!("{name} connect failed: {e}"))?;
    node.start_heartbeat(Duration::from_millis(150));
    Ok(node)
}

/// Poll `cond` until it holds or `timeout` elapses.
pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    cond()
}

/// Nearest-rank percentile of a sorted sample.
pub fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

/// Format nanoseconds as a short human string.
pub fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1} µs", ns as f64 / 1e3)
    } else {
        format!("{ns} ns")
    }
}

/// Start a coordinator in-process.
pub fn start_coordinator(
    dir: &Path,
    task_capacity: u32,
    store_mb: usize,
) -> (Coordinator, PathBuf) {
    let uds = dir.join("coord.sock");
    let cfg = runtime_config(unique_seg_base(), task_capacity, store_mb);
    let mut coord = Coordinator::bind(&uds, cfg).expect("coordinator bind");
    coord.start().expect("coordinator start");
    (coord, uds)
}

/// Host the four demo actors on one mailbox (blocking).
pub fn host_actors(
    uds: &Path,
    name: &str,
    spin: bool,
    stop: Option<Arc<AtomicBool>>,
) -> Result<(), String> {
    let mut sys = ActorSystem::connect(uds, name).map_err(|e| format!("connect: {e}"))?;
    sys.spawn(ECHO, Echo).map_err(|e| e.to_string())?;
    sys.spawn(COUNTER, Counter::default())
        .map_err(|e| e.to_string())?;
    sys.spawn(SINK, Sink::default())
        .map_err(|e| e.to_string())?;
    sys.spawn(RANGE, Range).map_err(|e| e.to_string())?;
    sys.set_spin(spin);
    if let Some(s) = stop {
        sys.set_stop(s);
    }
    sys.run().map_err(|e| format!("run: {e}"))
}

/// Host the actors on a thread (tests).
pub fn spawn_host_thread(
    uds: &Path,
    name: &str,
    spin: bool,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let uds = uds.to_path_buf();
    let name = name.to_string();
    std::thread::spawn(move || {
        if let Err(e) = host_actors(&uds, &name, spin, Some(stop)) {
            eprintln!("host {name}: {e}");
        }
    })
}

/// Start a gateway with both faces on ephemeral loopback ports.
pub fn start_gateway(uds: &Path, name: &str, shards: usize) -> Gateway {
    let mut cfg = GatewayConfig::new(uds, name);
    cfg.tcp = Some("127.0.0.1:0".parse().unwrap());
    cfg.http = Some("127.0.0.1:0".parse().unwrap());
    cfg.shards = shards;
    Gateway::start(cfg).expect("gateway start")
}

/// Wait until a TCP port answers a ping.
pub fn wait_gateway(addr: SocketAddr) -> bool {
    wait_until(Duration::from_secs(10), || {
        FrameClient::connect(addr)
            .and_then(|mut c| c.ping())
            .is_ok()
    })
}

/// A child that is killed on drop.
pub struct Reaper(pub Child);

impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A key=value report line.
#[derive(Clone, Debug, Default)]
pub struct Report {
    /// Ordered fields.
    pub fields: Vec<(String, String)>,
}

impl Report {
    /// Add a field.
    pub fn put(&mut self, k: &str, v: impl std::fmt::Display) -> &mut Self {
        self.fields.push((k.to_string(), v.to_string()));
        self
    }

    /// A field.
    pub fn get(&self, k: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(kk, _)| kk == k)
            .map(|(_, v)| v.as_str())
    }

    /// A numeric field.
    pub fn num<T: std::str::FromStr>(&self, k: &str) -> Option<T> {
        self.get(k).and_then(|v| v.parse().ok())
    }

    /// `k=v k=v …`
    pub fn to_kv(&self) -> String {
        self.fields
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Parse `k=v …` (the last such line of `text`).
    pub fn from_kv(text: &str) -> Report {
        let mut r = Report::default();
        if let Some(line) = text.lines().rev().find(|l| l.contains('=')) {
            for kv in line.split_whitespace() {
                if let Some((k, v)) = kv.split_once('=') {
                    r.put(k, v);
                }
            }
        }
        r
    }
}

fn parent_watch(enabled: bool) {
    if !enabled {
        return;
    }
    // SAFETY: getppid has no preconditions.
    let parent = unsafe { libc::getppid() };
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(200));
        // SAFETY: as above.
        if unsafe { libc::getppid() } != parent {
            // SAFETY: immediate exit is the point.
            unsafe { libc::_exit(0) };
        }
    });
}

// ---- roles ----

fn run_coordinator(opts: &Opts) -> Result<(), String> {
    let seg_base = opts.num("seg-base").unwrap_or_else(unique_seg_base);
    let cfg = runtime_config(
        seg_base,
        opts.num("task-capacity").unwrap_or(4096),
        opts.num("store-mb").unwrap_or(64),
    );
    let mut coord = Coordinator::bind(opts.uds(), cfg).map_err(|e| format!("bind: {e}"))?;
    coord.start().map_err(|e| format!("start: {e}"))?;
    parent_watch(opts.flag("--parent-watch"));
    println!("coordinator ready on {} (seg base {seg_base})", opts.uds());
    if let Some(path) = opts.get("ready-file") {
        let _ = std::fs::write(path, "ready");
    }
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

fn run_host(opts: &Opts) -> Result<(), String> {
    parent_watch(opts.flag("--parent-watch"));
    let name = opts.get("name").unwrap_or("host").to_string();
    host_actors(Path::new(&opts.uds()), &name, opts.flag("--spin"), None)
}

fn run_gateway(opts: &Opts) -> Result<(), String> {
    parent_watch(opts.flag("--parent-watch"));
    let mut cfg = GatewayConfig::new(opts.uds(), opts.get("name").unwrap_or("gateway"));
    cfg.tcp = opts
        .get("tcp")
        .map(|a| a.parse().map_err(|e| format!("--tcp: {e}")))
        .transpose()?;
    cfg.http = opts
        .get("http")
        .map(|a| a.parse().map_err(|e| format!("--http: {e}")))
        .transpose()?;
    cfg.shards = opts.num("shards").unwrap_or(1);
    if let Some(w) = opts.num("window") {
        cfg.topic_window = w;
    }
    let gw = Gateway::start(cfg).map_err(|e| format!("start: {e}"))?;
    println!(
        "gateway ready tcp={} http={}",
        gw.tcp_addr().map(|a| a.to_string()).unwrap_or_default(),
        gw.http_addr().map(|a| a.to_string()).unwrap_or_default()
    );
    if let Some(path) = opts.get("ready-file") {
        let _ = std::fs::write(
            path,
            format!(
                "tcp={}\nhttp={}\n",
                gw.tcp_addr().map(|a| a.to_string()).unwrap_or_default(),
                gw.http_addr().map(|a| a.to_string()).unwrap_or_default()
            ),
        );
    }
    gw.join().map_err(|e| e.to_string())
}

/// TCP client modes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpMode {
    /// Asks to `echo`.
    Ask,
    /// Tells to `counter`, then one `GET` to verify.
    Tell,
    /// Pings.
    Ping,
    /// Range requests to `range`.
    Range {
        /// Handles per request.
        count: u32,
        /// Coalesced completions.
        compress: bool,
    },
}

/// Options of [`tcp_client`].
#[derive(Clone, Debug)]
pub struct TcpClientOpts {
    /// Requests.
    pub n: u64,
    /// In-flight window.
    pub window: usize,
    /// Body bytes for asks.
    pub body: usize,
    /// Mode.
    pub mode: TcpMode,
}

/// Run a TCP client: `n` requests with `window` in flight.
pub fn tcp_client(addr: SocketAddr, o: &TcpClientOpts) -> Result<Report, String> {
    let mut c = FrameClient::connect(addr).map_err(|e| e.to_string())?;
    c.set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    let body = vec![0xabu8; o.body.min(192)];
    let echo = ActorId::named(ECHO);
    let counter = ActorId::named(COUNTER);
    let range = ActorId::named(RANGE);
    let mut lat: Vec<u64> = Vec::with_capacity(if o.window == 1 { o.n as usize } else { 0 });
    let mut errors = 0u64;
    let mut first_error: Option<String> = None;
    let mut sent = 0u64;
    let mut done = 0u64;
    let mut inflight: HashMap<u64, Instant> = HashMap::new();
    let start = Instant::now();
    if let TcpMode::Tell = o.mode {
        // Fire-and-forget; verify with one ask at the end.
        for i in 0..o.n {
            c.queue_tell(counter, COUNTER_INC, bytemuck::bytes_of(&Inc { by: 1 }))
                .map_err(|e| e.to_string())?;
            if (i + 1) % o.window as u64 == 0 {
                c.flush().map_err(|e| e.to_string())?;
            }
        }
        c.flush().map_err(|e| e.to_string())?;
        let submit = start.elapsed();
        // The count converges once the mailbox drains.
        let mut count = 0u64;
        let ok = wait_until(Duration::from_secs(60), || {
            match c.ask(counter, COUNTER_GET, bytemuck::bytes_of(&Get { _pad: 0 })) {
                Ok((_, b)) => {
                    count = Count::from_bytes(&b).map(|c| c.count).unwrap_or(0);
                    count >= o.n
                }
                Err(_) => false,
            }
        });
        let total = start.elapsed();
        let mut r = Report::default();
        r.put("mode", "tell")
            .put("n", o.n)
            .put("window", o.window)
            .put("count", count)
            .put("ok", ok)
            .put("submit_per_s", (o.n as f64 / submit.as_secs_f64()) as u64)
            .put("per_s", (o.n as f64 / total.as_secs_f64()) as u64)
            .put("elapsed_ms", total.as_millis());
        return Ok(r);
    }
    while done < o.n {
        while sent < o.n && inflight.len() < o.window {
            let corr = match o.mode {
                TcpMode::Ask => c.queue_ask(echo, ECHO_REQ, &body),
                TcpMode::Ping => c.queue_control(holon_net::wire::SCHEMA_PING, &()),
                TcpMode::Range { count, compress } => c.queue_ask(
                    range,
                    RANGE_REQ,
                    bytemuck::bytes_of(&RangeReq {
                        first: sent * u64::from(count),
                        count,
                        compress: u32::from(compress),
                    }),
                ),
                TcpMode::Tell => unreachable!(),
            }
            .map_err(|e| e.to_string())?;
            inflight.insert(corr, Instant::now());
            sent += 1;
        }
        c.flush().map_err(|e| e.to_string())?;
        loop {
            match c.recv().map_err(|e| format!("recv after {done}: {e}"))? {
                Inbound::Reply { env, body: b } => {
                    if let Some(t) = inflight.remove(&env.corr) {
                        if o.window == 1 {
                            lat.push(t.elapsed().as_nanos() as u64);
                        }
                        if o.mode == TcpMode::Ask && b != body {
                            errors += 1;
                        }
                        if let TcpMode::Range { count, .. } = o.mode {
                            if RangeAck::from_bytes(&b).map(|a| a.count).unwrap_or(0) != count {
                                errors += 1;
                            }
                        }
                        done += 1;
                    }
                }
                Inbound::Err { env, code, msg } => {
                    if inflight.remove(&env.corr).is_some() {
                        if first_error.is_none() {
                            first_error = Some(format!("{code}:{msg}"));
                        }
                        errors += 1;
                        done += 1;
                    }
                }
                _ => {}
            }
            if inflight.len() < o.window || done == o.n {
                break;
            }
        }
    }
    let total = start.elapsed();
    lat.sort_unstable();
    let mut r = Report::default();
    r.put(
        "mode",
        match o.mode {
            TcpMode::Ask => "ask",
            TcpMode::Ping => "ping",
            TcpMode::Range { compress: true, .. } => "range-compressed",
            TcpMode::Range { .. } => "range",
            TcpMode::Tell => "tell",
        },
    )
    .put("n", o.n)
    .put("window", o.window)
    .put("errors", errors)
    .put("per_s", (o.n as f64 / total.as_secs_f64()) as u64)
    .put("elapsed_ms", total.as_millis());
    if let Some(e) = first_error {
        r.put("first_error", e.replace(' ', "_"));
    }
    if !lat.is_empty() {
        r.put("p50_ns", percentile(&lat, 0.5))
            .put("p99_ns", percentile(&lat, 0.99))
            .put("min_ns", lat[0]);
    }
    Ok(r)
}

fn run_tcp_client(opts: &Opts) -> Result<Report, String> {
    let addr: SocketAddr = opts
        .get("addr")
        .unwrap_or("")
        .parse()
        .map_err(|e| format!("--addr: {e}"))?;
    let mode = match opts.get("mode").unwrap_or("ask") {
        "tell" => TcpMode::Tell,
        "ping" => TcpMode::Ping,
        "range" => TcpMode::Range {
            count: opts.num("count").unwrap_or(1024),
            compress: opts.flag("--compress"),
        },
        _ => TcpMode::Ask,
    };
    tcp_client(
        addr,
        &TcpClientOpts {
            n: opts.num("n").unwrap_or(10_000),
            window: opts.num("window").unwrap_or(1),
            body: opts.num("body-bytes").unwrap_or(16),
            mode,
        },
    )
}

/// Subscribe over TCP and count events until `expect` arrive (or the read
/// times out). Verifies seq contiguity after the first event.
pub fn tcp_sub(
    addr: SocketAddr,
    topic: &str,
    from: u64,
    expect: u64,
    timeout: Duration,
) -> Result<Report, String> {
    let mut c = FrameClient::connect(addr).map_err(|e| e.to_string())?;
    c.set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    let ack = c.subscribe(topic, from).map_err(|e| e.to_string())?;
    let until_seq = if expect == u64::MAX {
        u64::MAX
    } else {
        ack.cursor.saturating_add(expect)
    };
    let deadline = Instant::now() + timeout;
    let mut got = 0u64;
    let mut lagged = 0u64;
    let mut gaps = 0u64;
    let mut last: Option<u64> = None;
    let mut lat: Vec<u64> = Vec::new();
    let mut first_at: Option<Instant> = None;
    let start = Instant::now();
    let mut handles = 0u64;
    while got < expect && last.unwrap_or(0) < until_seq && Instant::now() < deadline {
        match c.recv() {
            Ok(Inbound::Event { head, data }) => {
                if first_at.is_none() {
                    first_at = Some(Instant::now());
                }
                if let Some(l) = last {
                    if head.seq != l + 1 {
                        gaps += 1;
                    }
                }
                last = Some(head.seq);
                got += 1;
                // `<id>:<sent_nanos>` (sink) or `<first>+<count>` (range).
                if let Ok(s) = std::str::from_utf8(&data) {
                    if let Some((_, ns)) = s.split_once(':') {
                        if let Ok(ns) = ns.parse::<u64>() {
                            lat.push(unix_nanos().saturating_sub(ns));
                        }
                    }
                    if let Some((_, n)) = s.split_once('+') {
                        handles += n.parse::<u64>().unwrap_or(0);
                    } else {
                        handles += 1;
                    }
                }
            }
            Ok(Inbound::Lagged(l)) => {
                lagged += 1;
                last = Some(l.resume_seq);
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    let total = first_at.map(|f| f.elapsed()).unwrap_or_default();
    lat.sort_unstable();
    let mut r = Report::default();
    r.put("topic_id", ack.topic_id)
        .put("head_at_subscribe", ack.head)
        .put("got", got)
        .put("expect", expect)
        .put("lagged", lagged)
        .put("gaps", gaps)
        .put("handles", handles)
        .put("last_seq", last.unwrap_or(0))
        .put(
            "events_per_s",
            if total.as_nanos() > 0 {
                (got as f64 / total.as_secs_f64()) as u64
            } else {
                0
            },
        )
        .put("elapsed_ms", start.elapsed().as_millis());
    if !lat.is_empty() {
        r.put("lat_p50_ns", percentile(&lat, 0.5))
            .put("lat_p99_ns", percentile(&lat, 0.99));
    }
    Ok(r)
}

fn run_tcp_sub(opts: &Opts) -> Result<Report, String> {
    let addr: SocketAddr = opts
        .get("addr")
        .unwrap_or("")
        .parse()
        .map_err(|e| format!("--addr: {e}"))?;
    tcp_sub(
        addr,
        opts.get("topic").unwrap_or(DONE_TOPIC),
        opts.num("from").unwrap_or(u64::MAX),
        opts.num("expect").unwrap_or(u64::MAX),
        Duration::from_millis(opts.num("timeout-ms").unwrap_or(10_000)),
    )
}

/// Publish `n` messages over TCP (`window` tells per flush; the last one an
/// ask so the report sees the final seq). Data = `<i>:<unix_nanos>`.
pub struct PublishOpts {
    /// Messages to publish.
    pub n: u64,
    /// Tells per flush; `0` = one acknowledged ask per message.
    pub window: usize,
    /// Event name.
    pub event: String,
    /// Offered rate in messages/second; `0` = as fast as possible.
    ///
    /// A fan-out measurement without an offered rate measures how fast the
    /// publisher can fill a queue, not how fast the system delivers. Pacing
    /// is what makes the delivery-latency percentiles mean anything.
    pub rate_per_s: u64,
    /// Payload bytes (padded past the `<i>:<nanos>` stamp).
    pub bytes: usize,
}

impl PublishOpts {
    /// `n` messages, `window` tells per flush, unpaced, minimal payload.
    pub fn burst(n: u64, window: usize) -> PublishOpts {
        PublishOpts {
            n,
            window,
            event: "tick".into(),
            rate_per_s: 0,
            bytes: 0,
        }
    }

    /// `n` messages at a fixed offered rate.
    pub fn paced(n: u64, rate_per_s: u64) -> PublishOpts {
        PublishOpts {
            n,
            window: 1,
            event: "tick".into(),
            rate_per_s,
            bytes: 0,
        }
    }

    /// Set the payload size.
    pub fn with_bytes(mut self, bytes: usize) -> PublishOpts {
        self.bytes = bytes;
        self
    }
}

/// Publish over TCP. Data is `<i>:<unix_nanos>` (optionally padded), so a
/// subscriber can compute publish-to-receive latency per message.
pub fn publish_with(addr: SocketAddr, topic: &str, o: &PublishOpts) -> Result<Report, String> {
    let mut c = FrameClient::connect(addr).map_err(|e| e.to_string())?;
    c.set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    let pad = vec![b'.'; o.bytes];
    let gap_ns = if o.rate_per_s > 0 {
        1_000_000_000u64 / o.rate_per_s.max(1)
    } else {
        0
    };
    let start = Instant::now();
    let mut behind = 0u64;
    for i in 0..o.n.saturating_sub(1) {
        if gap_ns > 0 {
            // Absolute schedule, not sleep-per-message: a late message does
            // not push every later one back.
            let due = Duration::from_nanos(gap_ns * i);
            match due.checked_sub(start.elapsed()) {
                Some(wait) if !wait.is_zero() => std::thread::sleep(wait),
                Some(_) => {}
                None => behind += 1,
            }
        }
        let mut data = format!("{i}:{}", unix_nanos()).into_bytes();
        data.extend_from_slice(&pad);
        if o.window == 0 {
            c.publish(topic, &o.event, &data)
                .map_err(|e| e.to_string())?;
            continue;
        }
        c.queue_publish_tell(topic, &o.event, &data)
            .map_err(|e| e.to_string())?;
        if (i + 1) % o.window as u64 == 0 {
            c.flush().map_err(|e| e.to_string())?;
        }
    }
    let mut data = format!("{}:{}", o.n - 1, unix_nanos()).into_bytes();
    data.extend_from_slice(&pad);
    let seq = c
        .publish(topic, &o.event, &data)
        .map_err(|e| e.to_string())?;
    let total = start.elapsed();
    let mut r = Report::default();
    r.put("n", o.n)
        .put("last_seq", seq)
        .put("per_s", (o.n as f64 / total.as_secs_f64()) as u64)
        .put("offered_per_s", o.rate_per_s)
        .put("behind_schedule", behind)
        .put("elapsed_ms", total.as_millis());
    Ok(r)
}

/// [`publish_with`] in the burst shape (back-compatible helper).
pub fn publisher(
    addr: SocketAddr,
    topic: &str,
    n: u64,
    window: usize,
    event: &str,
) -> Result<Report, String> {
    publish_with(
        addr,
        topic,
        &PublishOpts {
            n,
            window,
            event: event.into(),
            rate_per_s: 0,
            bytes: 0,
        },
    )
}

fn run_publisher(opts: &Opts) -> Result<Report, String> {
    let addr: SocketAddr = opts
        .get("addr")
        .unwrap_or("")
        .parse()
        .map_err(|e| format!("--addr: {e}"))?;
    publisher(
        addr,
        opts.get("topic").unwrap_or(DONE_TOPIC),
        opts.num("n").unwrap_or(1000),
        opts.num("window").unwrap_or(64),
        opts.get("event").unwrap_or("tick"),
    )
}

/// One SSE subscriber: read until `expect` events (or timeout); verify ids
/// contiguous; measure publish→receive latency from `<i>:<nanos>` data.
pub struct SseOpts {
    /// Resume cursor; `None` = live only.
    pub from: Option<u64>,
    /// Stop after this many events.
    pub expect: u64,
    /// Give up after this long.
    pub timeout: Duration,
    /// Sleep this long per event, to model a subscriber that cannot keep up.
    /// The claim under test is that the OTHER subscribers do not care.
    pub throttle_us: u64,
    /// After this many events, drop the connection and reconnect with
    /// `Last-Event-ID` (`0` = never). Proves resume-without-loss.
    pub reconnect_after: u64,
    /// Reconnect to this gateway instead of the original one; a different
    /// address proves the log, not the connection, holds the backlog.
    pub reconnect_to: Option<SocketAddr>,
}

impl SseOpts {
    /// Live-only, `expect` events, `timeout`, no throttle, no reconnect.
    pub fn live(expect: u64, timeout: Duration) -> SseOpts {
        SseOpts {
            from: None,
            expect,
            timeout,
            throttle_us: 0,
            reconnect_after: 0,
            reconnect_to: None,
        }
    }

    /// Set the resume cursor.
    pub fn from(mut self, from: Option<u64>) -> SseOpts {
        self.from = from;
        self
    }

    /// Set the per-event throttle.
    pub fn throttled(mut self, us: u64) -> SseOpts {
        self.throttle_us = us;
        self
    }

    /// Reconnect to `to` after `after` events.
    pub fn reconnecting(mut self, after: u64, to: Option<SocketAddr>) -> SseOpts {
        self.reconnect_after = after;
        self.reconnect_to = to;
        self
    }
}

/// [`sse_client_with`] with the plain options (back-compatible helper).
pub fn sse_client(
    http: SocketAddr,
    topic: &str,
    from: Option<u64>,
    expect: u64,
    timeout: Duration,
) -> Result<Report, String> {
    sse_client_with(
        http,
        topic,
        &SseOpts {
            from,
            expect,
            timeout,
            throttle_us: 0,
            reconnect_after: 0,
            reconnect_to: None,
        },
    )
}

/// One SSE subscriber. Verifies id contiguity, records publish-to-receive
/// latency from the `<i>:<nanos>` payload, and reports lag rather than
/// hiding it.
pub fn sse_client_with(http: SocketAddr, topic: &str, o: &SseOpts) -> Result<Report, String> {
    let (from, expect, timeout) = (o.from, o.expect, o.timeout);
    let mut c = HttpClient::connect(http).map_err(|e| e.to_string())?;
    c.set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    // The stop rule is a target seq, not a count: a subscriber that lags
    // (and is told so) still finishes when the topic reaches the target.
    let until_seq = if expect == u64::MAX {
        u64::MAX
    } else {
        let head = c
            .request("GET", &format!("/topics/{topic}/head"), &[], &[])
            .ok()
            .and_then(|r| {
                String::from_utf8_lossy(&r.body)
                    .trim_start_matches("{\"head\":")
                    .trim_end_matches('}')
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or(0);
        from.unwrap_or(head).saturating_add(expect)
    };
    let deadline = Instant::now() + timeout;
    let target = match from {
        Some(f) => format!("/topics/{topic}/sse?from={f}"),
        None => format!("/topics/{topic}/sse"),
    };
    c.send("GET", &target, &[("Accept", "text/event-stream")], &[])
        .map_err(|e| e.to_string())?;
    let (status, headers) = c.read_head().map_err(|e| e.to_string())?;
    if status != 200 {
        return Err(format!("sse status {status}"));
    }
    let ct = headers
        .iter()
        .find(|(k, _)| k == "content-type")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    let mut got = 0u64;
    let mut lagged = 0u64;
    let mut gaps = 0u64;
    let mut last: Option<u64> = None;
    let mut lat = Vec::new();
    let mut first_at: Option<Instant> = None;
    let mut reconnects = 0u64;
    let start = Instant::now();
    while got < expect && last.unwrap_or(0) < until_seq && Instant::now() < deadline {
        let ev = match c.read_sse_event() {
            Ok(ev) => ev,
            Err(_) => break,
        };
        if ev.heartbeat {
            // Nothing to count. The `while` condition above re-checks the
            // deadline, which is the whole point of not looping inside the
            // client.
            continue;
        }
        if ev.event == "lagged" {
            lagged += 1;
            if let Some(r) = ev
                .data
                .split("\"resume\":")
                .nth(1)
                .and_then(|s| s.split(',').next())
                .and_then(|s| s.parse::<u64>().ok())
            {
                last = Some(r);
            }
            continue;
        }
        if first_at.is_none() {
            first_at = Some(Instant::now());
        }
        if let (Some(l), Some(id)) = (last, ev.id) {
            if id != l + 1 {
                gaps += 1;
            }
        }
        last = ev.id;
        got += 1;
        if let Some((_, ns)) = ev.data.split_once(':') {
            if let Ok(ns) = ns.split('.').next().unwrap_or("").parse::<u64>() {
                lat.push(unix_nanos().saturating_sub(ns));
            }
        }
        if o.throttle_us > 0 {
            std::thread::sleep(Duration::from_micros(o.throttle_us));
        }
        if o.reconnect_after > 0 && got == o.reconnect_after {
            // Drop the socket and resume elsewhere with `Last-Event-ID`. The
            // backlog lives in the log, not in this connection or this
            // gateway process.
            let addr = o.reconnect_to.unwrap_or(http);
            let resume = last.unwrap_or(0).to_string();
            let mut c2 = HttpClient::connect(addr).map_err(|e| e.to_string())?;
            c2.set_read_timeout(Some(timeout))
                .map_err(|e| e.to_string())?;
            c2.send(
                "GET",
                &format!("/topics/{topic}/sse"),
                &[("Last-Event-ID", &resume)],
                &[],
            )
            .map_err(|e| e.to_string())?;
            let (st, _) = c2.read_head().map_err(|e| e.to_string())?;
            if st != 200 {
                return Err(format!("resume status {st}"));
            }
            c = c2;
            reconnects += 1;
        }
    }
    let total = first_at.map(|f| f.elapsed()).unwrap_or_default();
    lat.sort_unstable();
    let mut r = Report::default();
    r.put("content_type", ct)
        .put("got", got)
        .put("expect", expect)
        .put("lagged", lagged)
        .put("gaps", gaps)
        .put("last_id", last.unwrap_or(0))
        .put("reconnects", reconnects)
        .put(
            "events_per_s",
            if total.as_nanos() > 0 {
                (got as f64 / total.as_secs_f64()) as u64
            } else {
                0
            },
        )
        .put("elapsed_ms", start.elapsed().as_millis());
    if !lat.is_empty() {
        r.put("lat_p50_ns", percentile(&lat, 0.5))
            .put("lat_p99_ns", percentile(&lat, 0.99));
    }
    Ok(r)
}

fn run_sse_client(opts: &Opts) -> Result<Report, String> {
    let http: SocketAddr = opts
        .get("http")
        .unwrap_or("")
        .parse()
        .map_err(|e| format!("--http: {e}"))?;
    sse_client(
        http,
        opts.get("topic").unwrap_or(DONE_TOPIC),
        opts.num("from"),
        opts.num("expect").unwrap_or(u64::MAX),
        Duration::from_millis(opts.num("timeout-ms").unwrap_or(10_000)),
    )
}

/// Serial HTTP asks to `echo` over one keep-alive connection.
pub fn http_client(http: SocketAddr, n: u64, body: usize) -> Result<Report, String> {
    let mut c = HttpClient::connect(http).map_err(|e| e.to_string())?;
    c.set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| e.to_string())?;
    let payload = vec![0x5au8; body.min(192)];
    let target = format!("/actors/{ECHO}/{ECHO_REQ}");
    let mut lat = Vec::with_capacity(n as usize);
    let mut errors = 0u64;
    let start = Instant::now();
    for _ in 0..n {
        let t = Instant::now();
        let resp = c
            .request(
                "POST",
                &target,
                &[("Content-Type", "application/octet-stream")],
                &payload,
            )
            .map_err(|e| e.to_string())?;
        lat.push(t.elapsed().as_nanos() as u64);
        if resp.status != 200 || resp.body != payload {
            errors += 1;
        }
    }
    let total = start.elapsed();
    lat.sort_unstable();
    let mut r = Report::default();
    r.put("n", n)
        .put("errors", errors)
        .put("per_s", (n as f64 / total.as_secs_f64()) as u64)
        .put("p50_ns", percentile(&lat, 0.5))
        .put("p99_ns", percentile(&lat, 0.99))
        .put("elapsed_ms", total.as_millis());
    Ok(r)
}

fn run_http_client(opts: &Opts) -> Result<Report, String> {
    let http: SocketAddr = opts
        .get("http")
        .unwrap_or("")
        .parse()
        .map_err(|e| format!("--http: {e}"))?;
    http_client(
        http,
        opts.num("n").unwrap_or(5000),
        opts.num("body-bytes").unwrap_or(16),
    )
}

/// The loopback floor: a plain TCP echo server (one thread) and a client
/// sending the same 80-byte frames with the same in-flight window. Nothing
/// of Holon is involved; this is what the kernel + syscalls cost.
pub fn raw_echo(n: u64, window: usize, frame: usize) -> Result<Report, String> {
    use std::io::Read;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let server = std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let _ = s.set_nodelay(true);
            let mut buf = vec![0u8; 1 << 16];
            loop {
                match s.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(k) => {
                        if s.write_all(&buf[..k]).is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });
    let mut c = std::net::TcpStream::connect(addr).map_err(|e| e.to_string())?;
    let _ = c.set_nodelay(true);
    let msg = vec![0x11u8; frame];
    let mut lat = Vec::new();
    let mut rbuf = vec![0u8; frame * window.max(1)];
    let start = Instant::now();
    let mut sent = 0u64;
    let mut done = 0u64;
    while done < n {
        let batch = (window as u64).min(n - sent);
        let t = Instant::now();
        let mut out = Vec::with_capacity(frame * batch as usize);
        for _ in 0..batch {
            out.extend_from_slice(&msg);
        }
        c.write_all(&out).map_err(|e| e.to_string())?;
        sent += batch;
        let need = frame * batch as usize;
        let mut have = 0;
        while have < need {
            let k = c.read(&mut rbuf[have..need]).map_err(|e| e.to_string())?;
            if k == 0 {
                return Err("echo server closed".into());
            }
            have += k;
        }
        if window == 1 {
            lat.push(t.elapsed().as_nanos() as u64);
        }
        done += batch;
    }
    let total = start.elapsed();
    drop(c);
    let _ = server.join();
    lat.sort_unstable();
    let mut r = Report::default();
    r.put("mode", "raw-echo")
        .put("n", n)
        .put("window", window)
        .put("frame", frame)
        .put("per_s", (n as f64 / total.as_secs_f64()) as u64)
        .put("elapsed_ms", total.as_millis());
    if !lat.is_empty() {
        r.put("p50_ns", percentile(&lat, 0.5))
            .put("p99_ns", percentile(&lat, 0.99));
    }
    Ok(r)
}

/// The fan-out floor: one writer thread pushing the same frames to `subs`
/// plain TCP sockets, with `subs` reader threads. No Holon, no log, no
/// replay, no cursors. This is what the kernel and the client threads cost
/// on this machine, and every fan-out number must be read against it.
pub fn raw_fanout(subs: usize, msgs: u64, frame: usize, rate_per_s: u64) -> Result<Report, String> {
    use std::io::Read;
    let frame = frame.max(16);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let readers: Vec<_> = (0..subs)
        .map(|_| {
            std::thread::spawn(move || -> Result<Vec<u64>, String> {
                let mut s = std::net::TcpStream::connect(addr).map_err(|e| e.to_string())?;
                let _ = s.set_nodelay(true);
                let mut lat = Vec::with_capacity(msgs as usize);
                let mut buf = vec![0u8; frame * 64];
                let mut held: Vec<u8> = Vec::with_capacity(frame * 2);
                while (lat.len() as u64) < msgs {
                    let k = match s.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(k) => k,
                    };
                    held.extend_from_slice(&buf[..k]);
                    let mut off = 0;
                    while held.len() - off >= frame {
                        let sent = u64::from_le_bytes(held[off..off + 8].try_into().unwrap());
                        lat.push(unix_nanos().saturating_sub(sent));
                        off += frame;
                    }
                    held.drain(..off);
                }
                Ok(lat)
            })
        })
        .collect();
    let mut socks = Vec::with_capacity(subs);
    for _ in 0..subs {
        let (s, _) = listener.accept().map_err(|e| e.to_string())?;
        let _ = s.set_nodelay(true);
        socks.push(s);
    }
    let mut payload = vec![b'x'; frame];
    // Same absolute schedule as `publish_with`. An unpaced floor measures how
    // fast the kernel drains a socket, not what the same offered load costs
    // without Holon in it, and dividing a paced system by an unpaced floor is
    // not a comparison.
    let gap_ns = if rate_per_s > 0 {
        1_000_000_000u64 / rate_per_s.max(1)
    } else {
        0
    };
    let t = Instant::now();
    let mut behind = 0u64;
    for i in 0..msgs {
        if gap_ns > 0 {
            let due = Duration::from_nanos(gap_ns * i);
            match due.checked_sub(t.elapsed()) {
                Some(wait) if !wait.is_zero() => std::thread::sleep(wait),
                Some(_) => {}
                None => behind += 1,
            }
        }
        payload[..8].copy_from_slice(&unix_nanos().to_le_bytes());
        for s in socks.iter_mut() {
            s.write_all(&payload).map_err(|e| e.to_string())?;
        }
    }
    let mut lat = Vec::new();
    let mut got = 0u64;
    for h in readers {
        let mut l = h.join().map_err(|_| "reader panicked")??;
        got += l.len() as u64;
        lat.append(&mut l);
    }
    let wall = t.elapsed();
    drop(socks);
    lat.sort_unstable();
    let mut r = Report::default();
    r.put("subs", subs)
        .put("msgs", msgs)
        .put("frame", frame)
        .put("got", got)
        .put("expect", msgs * subs as u64)
        .put("events_per_s", (got as f64 / wall.as_secs_f64()) as u64)
        .put("offered_per_s", rate_per_s)
        .put("behind_schedule", behind)
        .put("write_per_s", (msgs as f64 / wall.as_secs_f64()) as u64)
        .put("p50_ns", percentile(&lat, 0.5))
        .put("p99_ns", percentile(&lat, 0.99))
        .put("elapsed_ms", wall.as_millis());
    Ok(r)
}

/// One gateway process and the addresses it serves.
pub struct GwProc {
    /// TCP (envelope frame) address.
    pub tcp: SocketAddr,
    /// HTTP address.
    pub http: SocketAddr,
    child: Child,
    alive: bool,
}

impl GwProc {
    /// `SIGKILL` this gateway. Its connections die; the log does not.
    pub fn kill(&mut self) {
        if self.alive {
            let _ = self.child.kill();
            let _ = self.child.wait();
            self.alive = false;
        }
    }
}

impl Drop for GwProc {
    fn drop(&mut self) {
        self.kill();
    }
}

/// A coordinator, an actor host, and `G` **separate gateway processes**, all
/// reading and writing one shared-memory log.
///
/// This is the shape a broadcast-channel design cannot express at all: every
/// gateway here is its own OS process with its own node, its own borrow
/// journal and its own sockets, yet a publish through any one of them is
/// visible to subscribers on all of them with no relay and no second copy.
pub struct MultiStack {
    _coord: Reaper,
    _host: Reaper,
    /// The gateway processes.
    pub gws: Vec<GwProc>,
    /// Coordinator UDS.
    pub uds: PathBuf,
    _dir: tempfile::TempDir,
}

impl MultiStack {
    /// Stand up the stack. `gateways` processes, `shards` reactor threads each.
    pub fn spawn(
        exe: &str,
        gateways: usize,
        shards: usize,
        window: u32,
        store_mb: usize,
    ) -> Result<MultiStack, String> {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        let uds = dir.path().join("coord.sock");
        let ready = dir.path().join("coord.ready");
        let coord = Reaper(
            Command::new(exe)
                .args(["coordinator", "--uds"])
                .arg(&uds)
                .args(["--task-capacity", "16384", "--store-mb"])
                .arg(store_mb.to_string())
                .arg("--ready-file")
                .arg(&ready)
                .arg("--parent-watch")
                .stdout(Stdio::null())
                .spawn()
                .map_err(|e| format!("spawn coordinator: {e}"))?,
        );
        if !wait_until(Duration::from_secs(10), || ready.exists()) {
            return Err("coordinator did not come up".into());
        }
        let host = Reaper(
            Command::new(exe)
                .args(["host", "--uds"])
                .arg(&uds)
                .arg("--parent-watch")
                .stdout(Stdio::null())
                .spawn()
                .map_err(|e| format!("spawn host: {e}"))?,
        );
        let mut gws = Vec::with_capacity(gateways);
        for i in 0..gateways {
            let gready = dir.path().join(format!("gw{i}.ready"));
            let child = Command::new(exe)
                .args(["gateway", "--uds"])
                .arg(&uds)
                .args([
                    "--tcp",
                    "127.0.0.1:0",
                    "--http",
                    "127.0.0.1:0",
                    "--name",
                    &format!("gw{i}"),
                    "--shards",
                ])
                .arg(shards.to_string())
                .args(["--window"])
                .arg(window.to_string())
                .arg("--ready-file")
                .arg(&gready)
                .arg("--parent-watch")
                .stdout(Stdio::null())
                .spawn()
                .map_err(|e| format!("spawn gateway {i}: {e}"))?;
            if !wait_until(Duration::from_secs(10), || gready.exists()) {
                return Err(format!("gateway {i} did not come up"));
            }
            let text = std::fs::read_to_string(&gready).map_err(|e| e.to_string())?;
            let mut tcp = None;
            let mut http = None;
            for l in text.lines() {
                if let Some(v) = l.strip_prefix("tcp=") {
                    tcp = v.parse().ok();
                }
                if let Some(v) = l.strip_prefix("http=") {
                    http = v.parse().ok();
                }
            }
            let (tcp, http) = (
                tcp.ok_or("gateway printed no tcp addr")?,
                http.ok_or("gateway printed no http addr")?,
            );
            if !wait_gateway(tcp) {
                return Err(format!("gateway {i} not answering"));
            }
            gws.push(GwProc {
                tcp,
                http,
                child,
                alive: true,
            });
        }
        Ok(MultiStack {
            _coord: coord,
            _host: host,
            gws,
            uds,
            _dir: dir,
        })
    }

    /// The first gateway's TCP address (the usual publish entry point).
    pub fn tcp(&self) -> SocketAddr {
        self.gws[0].tcp
    }

    /// The first gateway's HTTP address.
    pub fn http(&self) -> SocketAddr {
        self.gws[0].http
    }

    /// HTTP address of gateway `i`, round-robined over the live ones.
    pub fn http_n(&self, i: usize) -> SocketAddr {
        self.gws[i % self.gws.len()].http
    }
}

fn run_raw_echo(opts: &Opts) -> Result<Report, String> {
    raw_echo(
        opts.num("n").unwrap_or(20_000),
        opts.num("window").unwrap_or(1),
        opts.num("frame").unwrap_or(80),
    )
}

// ---- bench ----

/// The processes a bench run stands up.
pub struct Stack {
    _coord: Reaper,
    _host: Reaper,
    _gateway: Reaper,
    /// TCP face.
    pub tcp: SocketAddr,
    /// HTTP face.
    pub http: SocketAddr,
    /// Coordinator UDS.
    pub uds: PathBuf,
}

/// Spawn coordinator + host + gateway as child processes of `exe`.
pub fn spawn_stack(
    exe: &str,
    dir: &Path,
    host_spin: bool,
    shards: usize,
    window: u32,
) -> Result<Stack, String> {
    let uds = dir.join("coord.sock");
    let ready = dir.join("coord.ready");
    let coord = Reaper(
        Command::new(exe)
            .args(["coordinator", "--uds"])
            .arg(&uds)
            .args([
                "--task-capacity",
                "16384",
                "--store-mb",
                "128",
                "--ready-file",
            ])
            .arg(&ready)
            .arg("--parent-watch")
            .stdout(Stdio::null())
            .spawn()
            .map_err(|e| format!("spawn coordinator: {e}"))?,
    );
    if !wait_until(Duration::from_secs(10), || ready.exists()) {
        return Err("coordinator did not come up".into());
    }
    let mut host_cmd = Command::new(exe);
    host_cmd
        .args(["host", "--uds"])
        .arg(&uds)
        .arg("--parent-watch");
    if host_spin {
        host_cmd.arg("--spin");
    }
    let host = Reaper(
        host_cmd
            .stdout(Stdio::null())
            .spawn()
            .map_err(|e| format!("spawn host: {e}"))?,
    );
    let gready = dir.join("gateway.ready");
    let gateway = Reaper(
        Command::new(exe)
            .args(["gateway", "--uds"])
            .arg(&uds)
            .args(["--tcp", "127.0.0.1:0", "--http", "127.0.0.1:0", "--shards"])
            .arg(shards.to_string())
            .args(["--window"])
            .arg(window.to_string())
            .arg("--ready-file")
            .arg(&gready)
            .arg("--parent-watch")
            .stdout(Stdio::null())
            .spawn()
            .map_err(|e| format!("spawn gateway: {e}"))?,
    );
    if !wait_until(Duration::from_secs(10), || gready.exists()) {
        return Err("gateway did not come up".into());
    }
    let text = std::fs::read_to_string(&gready).map_err(|e| e.to_string())?;
    let mut tcp = None;
    let mut http = None;
    for l in text.lines() {
        if let Some(v) = l.strip_prefix("tcp=") {
            tcp = v.parse().ok();
        }
        if let Some(v) = l.strip_prefix("http=") {
            http = v.parse().ok();
        }
    }
    let (tcp, http) = (tcp.ok_or("no tcp addr")?, http.ok_or("no http addr")?);
    if !wait_gateway(tcp) {
        return Err("gateway not answering".into());
    }
    Ok(Stack {
        _coord: coord,
        _host: host,
        _gateway: gateway,
        tcp,
        http,
        uds,
    })
}

fn run_bench(opts: &Opts) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let exe = exe.to_string_lossy().to_string();
    let runs: u32 = opts.num("runs").unwrap_or(2);
    let n: u64 = opts.num("n").unwrap_or(20_000);
    let big: u64 = opts.num("big").unwrap_or(200_000);
    let quiet = opts.flag("--quiet");
    let only = opts.get("only").unwrap_or("all").to_string();
    let want = |stage: &str| only == "all" || only.split(',').any(|o| o == stage);
    let mut out = String::new();
    macro_rules! line {
        ($($t:tt)*) => {{
            let s = format!($($t)*);
            if !quiet { println!("{s}"); }
            out.push_str(&s); out.push('\n');
        }};
    }
    line!("== holon-net bench (ADR-0017) — {runs} runs, n={n}, big={big} ==");
    line!(
        "host: {} {} · rustc {} · release={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        rustc_version(),
        !cfg!(debug_assertions)
    );

    for run in 1..=runs {
        line!("\n-- run {run}/{runs} --");
        // Floor.
        if !want("asks") {
            let _ = 0;
        }
        let f1 = raw_echo(n, 1, 80)?;
        let f2 = raw_echo(big, 1024, 80)?;
        line!(
            "raw loopback echo (no Holon)     RTT p50 {} p99 {} · window 1024: {} frames/s",
            fmt_ns(f1.num("p50_ns").unwrap_or(0)),
            fmt_ns(f1.num("p99_ns").unwrap_or(0)),
            f2.get("per_s").unwrap_or("?")
        );

        for host_spin in [false, true] {
            let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            let stack = spawn_stack(&exe, dir.path(), host_spin, 1, 1024)?;
            let tag = if host_spin {
                "host spin  "
            } else {
                "host parked"
            };
            if want("asks") {
                // 1. Ask RTT, window 1 — the losing shape.
                let r = tcp_client(
                    stack.tcp,
                    &TcpClientOpts {
                        n,
                        window: 1,
                        body: 16,
                        mode: TcpMode::Ask,
                    },
                )?;
                line!(
                    "TCP ask RTT  w=1     [{tag}]  p50 {} p99 {} min {} · {} asks/s · errors {}",
                    fmt_ns(r.num("p50_ns").unwrap_or(0)),
                    fmt_ns(r.num("p99_ns").unwrap_or(0)),
                    fmt_ns(r.num("min_ns").unwrap_or(0)),
                    r.get("per_s").unwrap_or("?"),
                    r.get("errors").unwrap_or("?")
                );
                // 2. Pipelined.
                for w in [64usize, 1024] {
                    let r = tcp_client(
                        stack.tcp,
                        &TcpClientOpts {
                            n: big,
                            window: w,
                            body: 16,
                            mode: TcpMode::Ask,
                        },
                    )?;
                    line!(
                        "TCP ask pipelined w={w:<4} [{tag}]  {} asks/s · errors {}",
                        r.get("per_s").unwrap_or("?"),
                        r.get("errors").unwrap_or("?")
                    );
                }
                // 3. Four clients × 1024.
                let t = Instant::now();
                let hs: Vec<_> = (0..4)
                    .map(|_| {
                        let addr = stack.tcp;
                        std::thread::spawn(move || {
                            tcp_client(
                                addr,
                                &TcpClientOpts {
                                    n: big / 4,
                                    window: 1024,
                                    body: 16,
                                    mode: TcpMode::Ask,
                                },
                            )
                        })
                    })
                    .collect();
                let mut errs = 0u64;
                for h in hs {
                    let r = h.join().map_err(|_| "client thread panicked")??;
                    errs += r.num::<u64>("errors").unwrap_or(0);
                }
                line!(
                    "TCP ask 4 clients w=1024 [{tag}]  {} asks/s aggregate · errors {errs}",
                    (big as f64 / t.elapsed().as_secs_f64()) as u64
                );
                // 4. Tells.
                let r = tcp_client(
                    stack.tcp,
                    &TcpClientOpts {
                        n: big,
                        window: 1024,
                        body: 8,
                        mode: TcpMode::Tell,
                    },
                )?;
                line!(
                "TCP tell → counter       [{tag}]  submit {} tells/s · drained {} tells/s · count {} ok={}",
                r.get("submit_per_s").unwrap_or("?"),
                r.get("per_s").unwrap_or("?"),
                r.get("count").unwrap_or("?"),
                r.get("ok").unwrap_or("?")
            );
                // 5. HTTP ask RTT.
                let r = http_client(stack.http, n / 4, 16)?;
                line!(
                    "HTTP ask RTT (keep-alive) [{tag}]  p50 {} p99 {} · {} req/s · errors {}",
                    fmt_ns(r.num("p50_ns").unwrap_or(0)),
                    fmt_ns(r.num("p99_ns").unwrap_or(0)),
                    r.get("per_s").unwrap_or("?"),
                    r.get("errors").unwrap_or("?")
                );
            }
            if host_spin {
                continue;
            }
            if want("sse") {
                // 6. Publish → SSE latency (1 subscriber, paced by ask/reply).
                let http = stack.http;
                let sub = std::thread::spawn(move || {
                    sse_client(http, "lat", None, n / 4, Duration::from_secs(30))
                });
                std::thread::sleep(Duration::from_millis(200));
                let p = publisher(stack.tcp, "lat", n / 4, 0, "tick")?;
                let s = sub.join().map_err(|_| "sse thread panicked")??;
                line!(
                "publish→SSE latency (1 sub, paced ask-per-publish)  p50 {} p99 {} · publish {} msg/s · got {}/{} gaps {} lagged {}",
                fmt_ns(s.num("lat_p50_ns").unwrap_or(0)),
                fmt_ns(s.num("lat_p99_ns").unwrap_or(0)),
                p.get("per_s").unwrap_or("?"),
                s.get("got").unwrap_or("?"),
                s.get("expect").unwrap_or("?"),
                s.get("gaps").unwrap_or("?"),
                s.get("lagged").unwrap_or("?")
            );
                // 7. SSE fan-out.
                for subs in [1usize, 16, 256] {
                    let topic = format!("fan{subs}");
                    let msgs = n / 4;
                    let hs: Vec<_> = (0..subs)
                        .map(|_| {
                            let http = stack.http;
                            let topic = topic.clone();
                            std::thread::spawn(move || {
                                sse_client(http, &topic, None, msgs, Duration::from_secs(60))
                            })
                        })
                        .collect();
                    std::thread::sleep(Duration::from_millis(300 + subs as u64));
                    let t = Instant::now();
                    let p = publisher(stack.tcp, &topic, msgs, 64, "tick")?;
                    let mut got = 0u64;
                    let mut gaps = 0u64;
                    let mut lagged = 0u64;
                    let mut p99 = 0u64;
                    for h in hs {
                        let r = h.join().map_err(|_| "sse thread panicked")??;
                        got += r.num::<u64>("got").unwrap_or(0);
                        gaps += r.num::<u64>("gaps").unwrap_or(0);
                        lagged += r.num::<u64>("lagged").unwrap_or(0);
                        p99 = p99.max(r.num::<u64>("lat_p99_ns").unwrap_or(0));
                    }
                    let wall = t.elapsed();
                    line!(
                    "SSE fan-out {subs:>3} subs × {msgs} msgs  {} events/s delivered · publish {} msg/s · got {got}/{} gaps {gaps} lagged {lagged} · worst p99 {}",
                    (got as f64 / wall.as_secs_f64()) as u64,
                    p.get("per_s").unwrap_or("?"),
                    msgs * subs as u64,
                    fmt_ns(p99)
                );
                }
            }
            // 8. Range shape (ADR-0006 lifted): per-handle vs coalesced.
            for compress in [false, true] {
                if !want("range") {
                    break;
                }
                let tcp = stack.tcp;
                let reqs = 200u64;
                let count = 1024u32;
                let expect = if compress {
                    reqs
                } else {
                    reqs * u64::from(count)
                };
                let sub = std::thread::spawn(move || {
                    tcp_sub(tcp, DONE_TOPIC, u64::MAX, expect, Duration::from_secs(60))
                });
                std::thread::sleep(Duration::from_millis(200));
                let t = Instant::now();
                let r = tcp_client(
                    tcp,
                    &TcpClientOpts {
                        n: reqs,
                        window: 16,
                        body: 0,
                        mode: TcpMode::Range { count, compress },
                    },
                )?;
                let s = sub.join().map_err(|_| "sub thread panicked")??;
                let wall = t.elapsed();
                line!(
                    "range {:<10} {reqs}×{count} handles  {} handles/s observed by a TCP subscriber · {} events · publish side {} req/s · errors {}",
                    if compress { "compressed" } else { "per-handle" },
                    (s.num::<u64>("handles").unwrap_or(0) as f64 / wall.as_secs_f64()) as u64,
                    s.get("got").unwrap_or("?"),
                    r.get("per_s").unwrap_or("?"),
                    r.get("errors").unwrap_or("?")
                );
                if let Some(e) = r.get("first_error") {
                    line!("   first error: {e}");
                }
            }
            drop(stack);
        }
    }
    if let Some(path) = opts.get("out") {
        std::fs::write(path, &out).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn rustc_version() -> String {
    option_env!("RUSTC_VERSION").unwrap_or("stable").to_string()
}

/// Re-exported for the bench's tell verification.
pub fn count_of(body: &[u8]) -> u64 {
    Count::from_bytes(body).map(|c| c.count).unwrap_or(0)
}

/// Unused-import guard for types only the tests need through this module.
#[doc(hidden)]
pub fn _types(_: &Work, _: &WorkDone, _: &RangeAck) {}

/// Write a report to a file.
pub fn write_report(path: &Path, r: &Report) {
    if let Ok(mut f) = std::fs::File::create(path) {
        let _ = writeln!(f, "{}", r.to_kv());
    }
}

/// Wait for the sink topic to reach a head (helper for tests).
pub fn work_topic() -> &'static str {
    DONE_TOPIC
}

/// The ask target for the sink (helper for tests).
pub fn sink_target() -> (ActorId, u32) {
    (ActorId::named(SINK), WORK_REQ)
}
