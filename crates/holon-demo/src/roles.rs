//! The role entry points behind the `holon-demo` binary.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use holon_actor::{ActorRef, ActorSystem};
use holon_core::ActorId;
use shm_arrow::SchemaRegistry;
use shm_core::ChunkDesc;
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_store::RefKind;
use shm_task::{now_nanos, Outcome};

use crate::curve::{curve_batch, curve_schema, CURVE_KEY};
use crate::messages::{expected_dv01, PriceReply, PriceRequest, RiskReply, RiskRequest};
use crate::orchestrate::{
    append_line, crash_scenario, percentile, publish_curve, run_client, spawn_pricer,
    unique_seg_base, unix_nanos, wait_until, ClientOpts, ClientReport, PricerOpts, Reaper,
};
use crate::pricer::{Pricer, PRICER_NAME};
use crate::risk::{Risk, RISK_NAME};

/// Parsed command line: `<role> [--key value | --flag]…`.
#[derive(Clone, Debug, Default)]
pub struct Opts {
    /// The role name.
    pub role: String,
    values: HashMap<String, String>,
    flags: Vec<String>,
}

const BOOL_FLAGS: &[&str] = &["--spin", "--bare", "--mix", "--parent-watch"];

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

    fn result(&self) -> Option<PathBuf> {
        self.get("result").map(PathBuf::from)
    }
}

/// Dispatch a role; returns the process exit code.
pub fn run(opts: &Opts) -> i32 {
    match opts.role.as_str() {
        "coordinator" => run_coordinator(opts),
        "curve-publish" => run_curve_publish(opts),
        "pricer" => run_pricer(opts),
        "client" => run_client_role(opts),
        "supervisor" => run_supervisor(opts),
        "bench" => run_bench(opts),
        other => {
            eprintln!(
                "unknown role {other:?}; expected coordinator|curve-publish|pricer|client|supervisor|bench"
            );
            2
        }
    }
}

fn connect(uds: &str, name: &str) -> Result<Node, String> {
    let mut node = Node::connect(uds, name, Arc::new(SchemaRegistry::new()))
        .map_err(|e| format!("{name} connect failed: {e}"))?;
    node.start_heartbeat(Duration::from_millis(150));
    Ok(node)
}

// ---- coordinator ----

fn run_coordinator(opts: &Opts) -> i32 {
    let seg_base = opts.num("seg-base").unwrap_or_else(unique_seg_base);
    let config = RuntimeConfig::with_seg_base(seg_base);
    let mut coord = match Coordinator::bind(opts.uds(), config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("coordinator bind failed: {e}");
            return 1;
        }
    };
    if let Err(e) = coord.start() {
        eprintln!("coordinator start failed: {e}");
        return 1;
    }
    println!("coordinator ready on {} (seg base {seg_base})", opts.uds());
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

// ---- curve-publish ----

/// Create (get-or-create) the `curve` cell and commit one version of it.
pub fn publish_curve_with(node: &mut Node, bump_bp: f64) -> Result<u64, String> {
    node.intern_schema(&curve_schema())
        .map_err(|e| format!("intern schema: {e}"))?;
    let store = node.store().map_err(|e| format!("store: {e}"))?;
    let entry = store
        .create(CURVE_KEY, RefKind::Dataset, &curve_schema())
        .map_err(|e| format!("create: {e}"))?;
    entry
        .commit_replace(&curve_batch(bump_bp))
        .map_err(|e| format!("commit: {e}"))
}

fn run_curve_publish(opts: &Opts) -> i32 {
    let bump = opts.num::<f64>("bump-bp").unwrap_or(0.0);
    let mut node = match connect(&opts.uds(), "curve-publish") {
        Ok(n) => n,
        Err(e) => {
            eprintln!("{e}");
            return 1;
        }
    };
    match publish_curve_with(&mut node, bump) {
        Ok(v) => println!("curve committed v{v} (bump {bump} bp)"),
        Err(e) => {
            eprintln!("curve-publish failed: {e}");
            return 1;
        }
    }
    let _ = node.say_bye();
    0
}

// ---- pricer ----

fn start_parent_watch() {
    // SAFETY: `getppid` has no preconditions.
    let parent = unsafe { libc::getppid() };
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(100));
        // SAFETY: as above.
        if unsafe { libc::getppid() } != parent {
            // Our supervisor is gone: do not outlive it as an orphan.
            // SAFETY: `_exit` is always sound; no destructor is needed here.
            unsafe { libc::_exit(0) }
        }
    });
}

fn run_pricer(opts: &Opts) -> i32 {
    let uds = opts.uds();
    let lease = Duration::from_millis(opts.num("lease-ms").unwrap_or(500));
    let spin = opts.flag("--spin");
    let result = opts.result();
    if opts.flag("--parent-watch") {
        start_parent_watch();
    }
    if opts.flag("--bare") {
        return run_bare_pricer(&uds, lease, spin, result.as_deref());
    }
    let mut sys = match ActorSystem::connect(&uds, PRICER_NAME) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("pricer connect failed: {e}");
            return 1;
        }
    };
    if let Err(e) = sys.intern_schema(&curve_schema()) {
        eprintln!("pricer intern schema failed: {e}");
        return 1;
    }
    sys.set_lease(lease);
    sys.set_spin(spin);
    let kill_after = opts.num::<u64>("kill-after");
    // Two actors, one mailbox: the envelope's `to` picks the host.
    let spawned = sys
        .spawn(PRICER_NAME, Pricer::new(kill_after, result.clone()))
        .and_then(|_| sys.spawn(RISK_NAME, Risk::new()));
    if let Err(e) = spawned {
        eprintln!("pricer spawn failed: {e}");
        return 1;
    }
    if let Some(p) = &result {
        append_line(p, &format!("READY {} {}", std::process::id(), unix_nanos()));
    }
    match sys.run() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("pricer run failed: {e}");
            1
        }
    }
}

/// Bare mode: the same mailbox loop with no envelope and no pin — `claim →
/// complete(ZERO)`. The difference between this and the real pricer is the
/// envelope-in-a-chunk detour plus the pin plus the handler.
fn run_bare_pricer(uds: &str, lease: Duration, spin: bool, result: Option<&Path>) -> i32 {
    let mut node = match connect(uds, "pricer-bare") {
        Ok(n) => n,
        Err(e) => {
            eprintln!("{e}");
            return 1;
        }
    };
    let tq = match node.task_queue() {
        Ok(q) => q,
        Err(e) => {
            eprintln!("pricer-bare task_queue failed: {e}");
            return 1;
        }
    };
    let parker = tq.work_parker().expect("work parker");
    let lease_nanos = lease.as_nanos() as u64;
    if let Some(p) = result {
        append_line(p, &format!("READY {} {}", std::process::id(), unix_nanos()));
    }
    loop {
        let claimed = if spin {
            loop {
                if let Some(t) = tq.claim(lease_nanos) {
                    break t;
                }
                std::hint::spin_loop();
            }
        } else {
            tq.queue()
                .claim_blocking_with_lease(tq.worker_id(), lease_nanos, &parker)
        };
        let _ = claimed.complete(ChunkDesc::ZERO);
    }
}

// ---- client ----

struct ClientThreadReport {
    samples: Vec<u64>,
    replies: u64,
    errors: u64,
    redelivered: u64,
    risk_replies: u64,
    version_changes: u64,
    incarnations: HashMap<u32, (u64, u64)>,
}

fn client_thread(
    uds: &str,
    idx: u32,
    n: u64,
    spin: bool,
    bare: bool,
    mix: bool,
    barrier: &Barrier,
) -> Result<ClientThreadReport, String> {
    let mut node = connect(uds, &format!("client-{idx}"))?;
    let mut r = ClientThreadReport {
        samples: Vec::with_capacity(n as usize),
        replies: 0,
        errors: 0,
        redelivered: 0,
        risk_replies: 0,
        version_changes: 0,
        incarnations: HashMap::new(),
    };
    if bare {
        let tq = node.task_queue().map_err(|e| e.to_string())?;
        let parker = tq.done_parker().map_err(|e| e.to_string())?;
        let req = ChunkDesc {
            schema_id: 0xba5e,
            ..ChunkDesc::ZERO
        };
        barrier.wait();
        for _ in 0..n {
            let t0 = Instant::now();
            let deadline = now_nanos().wrapping_add(60_000_000_000);
            let h = tq.submit(req, deadline).map_err(|e| e.to_string())?;
            // A stale handle after a wait is terminal (the slot was reused, so
            // it completed first); bare mode carries no result, so it counts.
            let outcome = if spin {
                loop {
                    match tq.queue().poll(h) {
                        Ok(shm_task::TaskStatus::Done(d)) => break Outcome::Done(d),
                        Ok(shm_task::TaskStatus::Failed) => break Outcome::Failed,
                        Ok(shm_task::TaskStatus::Cancelled) => break Outcome::Cancelled,
                        Ok(_) => std::hint::spin_loop(),
                        Err(shm_task::Error::StaleHandle) => break Outcome::Done(ChunkDesc::ZERO),
                        Err(e) => return Err(e.to_string()),
                    }
                }
            } else {
                match tq.queue().wait(h, &parker) {
                    Ok(o) => o,
                    Err(shm_task::Error::StaleHandle) => Outcome::Done(ChunkDesc::ZERO),
                    Err(e) => return Err(e.to_string()),
                }
            };
            r.samples.push(t0.elapsed().as_nanos() as u64);
            match outcome {
                Outcome::Done(_) => r.replies += 1,
                _ => r.errors += 1,
            }
        }
        return Ok(r);
    }

    let mut actor =
        ActorRef::new(&mut node, ActorId::named(PRICER_NAME)).map_err(|e| e.to_string())?;
    actor.set_spin(spin);
    // A second ref over the same node + mailbox, addressed to the other actor.
    let mut risk =
        ActorRef::new(&mut node, ActorId::named(RISK_NAME)).map_err(|e| e.to_string())?;
    risk.set_spin(spin);
    let mut last_version: Option<u64> = None;
    barrier.wait();
    for seq in 0..n {
        let tenor = 0.25 + ((seq.wrapping_mul(7919)) % 2975) as f64 / 100.0;
        let notional = 1_000_000.0;
        let to_risk = mix && seq % 2 == 1;
        let t0 = Instant::now();
        // Both arms fold to (curve_version, incarnation, attempt) once verified.
        let res: Result<(u64, u32, u32), String> = if to_risk {
            let req = RiskRequest {
                tenor,
                notional,
                seq,
            };
            risk.ask::<RiskRequest, RiskReply>(&req)
                .map_err(|e| e.to_string())
                .and_then(|rep| {
                    let want = expected_dv01(rep.px, tenor);
                    if (rep.dv01 - want).abs() > 1e-9 * rep.px.abs().max(1.0) {
                        return Err(format!(
                            "dv01 {} != expected {want} (px {})",
                            rep.dv01, rep.px
                        ));
                    }
                    Ok((rep.curve_version, rep.incarnation, rep.attempt))
                })
        } else {
            let req = PriceRequest {
                tenor,
                notional,
                seq,
            };
            actor
                .ask::<PriceRequest, PriceReply>(&req)
                .map(|rep| (rep.curve_version, rep.incarnation, rep.attempt))
                .map_err(|e| e.to_string())
        };
        let dt = t0.elapsed().as_nanos() as u64;
        r.samples.push(dt);
        match res {
            Ok((curve_version, incarnation, attempt)) => {
                r.replies += 1;
                if to_risk {
                    r.risk_replies += 1;
                }
                if attempt > 0 {
                    r.redelivered += 1;
                }
                if last_version.is_some_and(|v| v != curve_version) {
                    r.version_changes += 1;
                }
                last_version = Some(curve_version);
                let e = r
                    .incarnations
                    .entry(incarnation)
                    .or_insert_with(|| (0, unix_nanos()));
                e.0 += 1;
            }
            Err(e) => {
                r.errors += 1;
                eprintln!(
                    "client-{idx} ask {seq} ({}) failed: {e}",
                    if to_risk { "risk" } else { "price" }
                );
            }
        }
    }
    Ok(r)
}

fn run_client_role(opts: &Opts) -> i32 {
    let uds = opts.uds();
    let n: u64 = opts.num("n").unwrap_or(1000);
    let clients: u32 = opts.num("clients").unwrap_or(1).max(1);
    let spin = opts.flag("--spin");
    let bare = opts.flag("--bare");
    let mix = opts.flag("--mix");
    let barrier = Arc::new(Barrier::new(clients as usize + 1));
    let mut handles = Vec::new();
    for i in 0..clients {
        let per = n / clients as u64
            + if i == clients - 1 {
                n % clients as u64
            } else {
                0
            };
        let uds = uds.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            client_thread(&uds, i, per, spin, bare, mix, &barrier)
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    let mut reports = Vec::new();
    for h in handles {
        match h.join().expect("client thread") {
            Ok(r) => reports.push(r),
            Err(e) => {
                eprintln!("client thread failed: {e}");
                return 1;
            }
        }
    }
    let elapsed = t0.elapsed();

    let mut samples: Vec<u64> = reports
        .iter()
        .flat_map(|r| r.samples.iter().copied())
        .collect();
    samples.sort_unstable();
    let mut incarnations: HashMap<u32, (u64, u64)> = HashMap::new();
    for r in &reports {
        for (pid, (count, first)) in &r.incarnations {
            let e = incarnations.entry(*pid).or_insert((0, *first));
            e.0 += count;
            e.1 = e.1.min(*first);
        }
    }
    let mut inc: Vec<(u32, u64, u64)> = incarnations
        .into_iter()
        .map(|(p, (c, f))| (p, c, f))
        .collect();
    inc.sort_by_key(|i| i.2);
    let report = ClientReport {
        asks: n,
        replies: reports.iter().map(|r| r.replies).sum(),
        errors: reports.iter().map(|r| r.errors).sum(),
        redelivered: reports.iter().map(|r| r.redelivered).sum(),
        risk_replies: reports.iter().map(|r| r.risk_replies).sum(),
        p50_ns: percentile(&samples, 0.50),
        p99_ns: percentile(&samples, 0.99),
        max_ns: samples.last().copied().unwrap_or(0),
        elapsed_ns: elapsed.as_nanos() as u64,
        throughput: n as f64 / elapsed.as_secs_f64(),
        version_changes: reports.iter().map(|r| r.version_changes).sum(),
        incarnations: inc,
    };
    println!(
        "client: {} asks over {} client(s){}{}{}: p50={:.1}us p99={:.1}us max={:.1}us  {:.0} asks/s  errors={} redelivered={} risk_replies={} version_changes={} incarnations={}",
        report.asks,
        clients,
        if spin { " [spin]" } else { "" },
        if bare { " [bare]" } else { "" },
        if mix { " [mix pricer+risk]" } else { "" },
        report.p50_ns as f64 / 1e3,
        report.p99_ns as f64 / 1e3,
        report.max_ns as f64 / 1e3,
        report.throughput,
        report.errors,
        report.redelivered,
        report.risk_replies,
        report.version_changes,
        report.incarnations.len()
    );
    if let Some(p) = opts.result() {
        let _ = std::fs::write(p, report.to_kv());
    }
    0
}

// ---- supervisor ----

static TERM: AtomicBool = AtomicBool::new(false);

extern "C" fn on_term(_sig: libc::c_int) {
    TERM.store(true, Ordering::Release);
}

fn run_supervisor(opts: &Opts) -> i32 {
    let uds = opts.uds();
    let log = opts.result();
    let pricer_result = opts.get("pricer-result").map(PathBuf::from);
    let kill_after = opts.num::<u64>("kill-after");
    let lease_ms = opts.num::<u64>("lease-ms");
    let spin = opts.flag("--spin");
    let exe = std::env::current_exe().expect("current exe");
    // SAFETY: installing a signal handler that only stores to an atomic.
    unsafe {
        libc::signal(
            libc::SIGTERM,
            on_term as extern "C" fn(libc::c_int) as libc::sighandler_t,
        );
    }
    let logline = |s: &str| {
        if let Some(p) = &log {
            append_line(p, s);
        }
    };
    let mut incarnation = 0u32;
    loop {
        let mut args = vec![
            "pricer".to_string(),
            "--uds".into(),
            uds.clone(),
            "--parent-watch".into(),
        ];
        if spin {
            args.push("--spin".into());
        }
        if let Some(l) = lease_ms {
            args.push("--lease-ms".into());
            args.push(l.to_string());
        }
        // The crash is injected once: the first incarnation dies, successors run clean.
        if incarnation == 0 {
            if let Some(k) = kill_after {
                args.push("--kill-after".into());
                args.push(k.to_string());
            }
        }
        if let Some(p) = &pricer_result {
            args.push("--result".into());
            args.push(p.display().to_string());
        }
        let mut child = match Command::new(&exe).args(&args).stdout(Stdio::null()).spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("supervisor spawn failed: {e}");
                return 1;
            }
        };
        logline(&format!(
            "{} {incarnation} {} {}",
            if incarnation == 0 { "START" } else { "RESTART" },
            child.id(),
            unix_nanos()
        ));
        loop {
            if TERM.load(Ordering::Acquire) {
                let _ = child.kill();
                let _ = child.wait();
                logline(&format!("TERM {}", unix_nanos()));
                return 0;
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    logline(&format!(
                        "EXIT {incarnation} {} {} {}",
                        child.id(),
                        status.code().unwrap_or(-1),
                        unix_nanos()
                    ));
                    break;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(5)),
                Err(e) => {
                    eprintln!("supervisor wait failed: {e}");
                    return 1;
                }
            }
        }
        incarnation += 1;
    }
}

// ---- bench ----

fn fmt_us(ns: u64) -> String {
    format!("{:.1} µs", ns as f64 / 1e3)
}

fn run_bench(opts: &Opts) -> i32 {
    let n: u64 = opts.num("n").unwrap_or(100_000);
    let runs: u32 = opts.num("runs").unwrap_or(2);
    let crash_n: u64 = opts.num("crash-n").unwrap_or(2_000);
    let kill_after: u64 = opts.num("kill-after").unwrap_or(50);
    let lease_ms: u64 = 500;
    let exe = std::env::current_exe().expect("current exe");
    let exe = exe.to_str().unwrap().to_string();
    let dir = std::env::temp_dir().join(format!("holon-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("bench dir");
    let uds = dir.join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();

    println!("holon-demo bench — MEASURED, macOS poll(2) wake (POSIX pipe doorbell; futex is Linux-only, unmeasured)");
    println!(
        "build: {}   n={n} asks per config, {runs} run(s); crash: n={crash_n}, kill-after={kill_after}, lease={lease_ms} ms",
        if cfg!(debug_assertions) { "DEBUG (use --release)" } else { "release" }
    );

    let mut config = RuntimeConfig::with_seg_base(unique_seg_base());
    config.lease_deadline = Duration::from_millis(lease_ms);
    let mut coord = Coordinator::bind(&uds, config).expect("bind coordinator");
    coord.start().expect("start coordinator");
    let baseline = coord.store_data_free_total().expect("baseline");
    publish_curve(&exe, &uds_s, 0.0);
    let after_publish = coord.store_data_free_total().expect("census");
    println!("store pool: {baseline} free chunks empty, {after_publish} with curve v1");

    for run in 1..=runs {
        println!("\n== run {run}/{runs} ==");

        // 1. parked ask round trip, 1 client / 1 pricer (also the 1/1 throughput).
        let parked = {
            let _p = spawn_pricer(&exe, &uds_s, &dir, "parked", &PricerOpts::default());
            run_client(&exe, &uds_s, &dir, "parked", &ClientOpts::parked(n))
        };
        println!(
            "1. ask round trip, pricer PARKED (poll(2) wake both ways): p50={} p99={} max={}  n={} errors={}",
            fmt_us(parked.p50_ns), fmt_us(parked.p99_ns), fmt_us(parked.max_ns), parked.asks, parked.errors
        );

        // 2. busy-poll floor: pricer spins on claim, client spins on poll.
        let spin = {
            let _p = spawn_pricer(
                &exe,
                &uds_s,
                &dir,
                "spin",
                &PricerOpts {
                    spin: true,
                    ..Default::default()
                },
            );
            run_client(
                &exe,
                &uds_s,
                &dir,
                "spin",
                &ClientOpts {
                    spin: true,
                    ..ClientOpts::parked(n)
                },
            )
        };
        println!(
            "2. ask round trip, pricer + client BUSY-POLLING (the floor): p50={} p99={} max={}  errors={}",
            fmt_us(spin.p50_ns), fmt_us(spin.p99_ns), fmt_us(spin.max_ns), spin.errors
        );

        // 3. throughput.
        let t41 = {
            let _p = spawn_pricer(&exe, &uds_s, &dir, "t41", &PricerOpts::default());
            run_client(
                &exe,
                &uds_s,
                &dir,
                "t41",
                &ClientOpts {
                    clients: 4,
                    ..ClientOpts::parked(n)
                },
            )
        };
        let t44 = {
            let _ps: Vec<Reaper> = (0..4)
                .map(|i| {
                    spawn_pricer(
                        &exe,
                        &uds_s,
                        &dir,
                        &format!("t44-{i}"),
                        &PricerOpts::default(),
                    )
                })
                .collect();
            run_client(
                &exe,
                &uds_s,
                &dir,
                "t44",
                &ClientOpts {
                    clients: 4,
                    ..ClientOpts::parked(n)
                },
            )
        };
        println!(
            "3. throughput (parked): 1 client/1 pricer = {:.0} asks/s; 4 clients/1 pricer = {:.0} asks/s (p50 {}); 4/4 = {:.0} asks/s (p50 {})  errors={}/{}/{}",
            parked.throughput, t41.throughput, fmt_us(t41.p50_ns), t44.throughput, fmt_us(t44.p50_ns),
            parked.errors, t41.errors, t44.errors
        );

        // 3b. two actors per process, routed by `to` over the one mailbox.
        let mix = {
            let _p = spawn_pricer(&exe, &uds_s, &dir, "mix", &PricerOpts::default());
            run_client(
                &exe,
                &uds_s,
                &dir,
                "mix",
                &ClientOpts {
                    clients: 4,
                    mix: true,
                    ..ClientOpts::parked(n)
                },
            )
        };
        println!(
            "3b. two actors in ONE process (pricer + risk), 4 clients alternating by `to`: {:.0} asks/s (p50 {}, p99 {}) vs single-actor 4/1 {:.0} asks/s (p50 {}); risk replies={} (each verified) errors={}",
            mix.throughput, fmt_us(mix.p50_ns), fmt_us(mix.p99_ns), t41.throughput, fmt_us(t41.p50_ns), mix.risk_replies, mix.errors
        );

        // Detour: the same round trip with no envelope, no pin, no handler.
        let bare = {
            let _p = spawn_pricer(
                &exe,
                &uds_s,
                &dir,
                "bare",
                &PricerOpts {
                    bare: true,
                    ..Default::default()
                },
            );
            run_client(
                &exe,
                &uds_s,
                &dir,
                "bare",
                &ClientOpts {
                    bare: true,
                    ..ClientOpts::parked(n)
                },
            )
        };
        let bare_spin = {
            let _p = spawn_pricer(
                &exe,
                &uds_s,
                &dir,
                "bare-spin",
                &PricerOpts {
                    bare: true,
                    spin: true,
                    ..Default::default()
                },
            );
            run_client(
                &exe,
                &uds_s,
                &dir,
                "bare-spin",
                &ClientOpts {
                    bare: true,
                    spin: true,
                    ..ClientOpts::parked(n)
                },
            )
        };
        println!(
            "   bare submit→claim→complete→wait (no envelope/pin/handler): parked p50={} p99={}; spin p50={} p99={}",
            fmt_us(bare.p50_ns), fmt_us(bare.p99_ns), fmt_us(bare_spin.p50_ns), fmt_us(bare_spin.p99_ns)
        );
        println!(
            "   envelope-in-a-chunk detour + pin + handler (full − bare, p50): parked {:+.1} µs; spin {:+.1} µs",
            (parked.p50_ns as f64 - bare.p50_ns as f64) / 1e3,
            (spin.p50_ns as f64 - bare_spin.p50_ns as f64) / 1e3
        );

        // 4. the crash.
        let before_crash = coord.store_data_free_total().expect("census");
        let crash = crash_scenario(
            &exe,
            &uds_s,
            &dir,
            &format!("r{run}"),
            crash_n,
            kill_after,
            lease_ms,
        );
        let lost = crash.client.lost();
        println!(
            "4. crash: SIGKILL(_exit 137) → first reply from successor = {}; restarts={} redelivered={} lost={} errors={} incarnations={:?}",
            crash
                .kill_to_first_reply()
                .map(|d| format!("{:.1} ms", d.as_secs_f64() * 1e3))
                .unwrap_or_else(|| "n/a".into()),
            crash.restarts,
            crash.client.redelivered,
            lost,
            crash.client.errors,
            crash.client.incarnations.iter().map(|i| (i.0, i.1)).collect::<Vec<_>>()
        );

        // 5. census: every child is gone; the dead pricer's pin and its
        // in-flight envelope were reclaimed; free count is back to the
        // with-curve baseline.
        let t0 = Instant::now();
        let settled = wait_until(Duration::from_secs(10), || {
            coord.store_data_free_total() == Some(before_crash)
        });
        let now = coord.store_data_free_total().unwrap_or(0);
        println!(
            "5. census after crash: {now} free (baseline with curve {before_crash}, empty {baseline}) — {} after {:.0} ms",
            if settled { "ZERO LEAK" } else { "LEAK" },
            t0.elapsed().as_secs_f64() * 1e3
        );
    }

    // Final: evict the curve; the pool must return to the empty baseline.
    {
        let mut node = connect(&uds_s, "bench-evict").expect("connect");
        node.store().unwrap().evict(CURVE_KEY).expect("evict curve");
        let _ = node.say_bye();
    }
    let final_free = coord.store_data_free_total().unwrap_or(0);
    println!(
        "\nfinal census after evicting curve: {final_free} free vs empty baseline {baseline} — {}",
        if final_free == baseline {
            "ZERO LEAK"
        } else {
            "LEAK"
        }
    );
    let _ = std::fs::remove_dir_all(&dir);
    0
}
