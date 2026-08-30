//! Multi-process orchestration shared by the bench and the integration tests:
//! spawning roles, result files, waiting without fixed sleeps, and the crash
//! scenario itself.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Wall-clock nanoseconds since the Unix epoch — the one clock every process
/// in the demo stamps into result files, so cross-process deltas are meaningful.
pub fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Append one line to a result file (best effort).
pub fn append_line(path: &Path, line: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "{line}");
    }
}

/// A segment-id base unique to this process + instant, so concurrent runs
/// never collide on shm names (the skeleton tests' pattern).
pub fn unique_seg_base() -> u32 {
    let nanos = unix_nanos();
    let pid = std::process::id() as u64;
    100_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
}

/// Poll `cond` every 10 ms until it holds or `timeout` elapses.
pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    cond()
}

/// A child process killed (SIGKILL) and reaped on drop.
pub struct Reaper(pub Child);

impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A child process asked to exit with SIGTERM on drop (so a supervisor can
/// take its own child down), SIGKILLed if it does not comply within 2 s.
pub struct TermReaper(pub Child);

impl TermReaper {
    /// Send SIGTERM and wait (bounded) for the exit.
    pub fn terminate(&mut self) {
        // SAFETY: `kill(2)` on the pid of a child we spawned and still own.
        unsafe {
            libc::kill(self.0.id() as libc::pid_t, libc::SIGTERM);
        }
        if !wait_until(Duration::from_secs(2), || {
            matches!(self.0.try_wait(), Ok(Some(_)))
        }) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

impl Drop for TermReaper {
    fn drop(&mut self) {
        self.terminate();
    }
}

/// Parse a `key=value` result file into a map (lines without `=` are skipped).
pub fn read_kv(path: &Path) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Ok(s) = std::fs::read_to_string(path) {
        for line in s.lines() {
            if let Some((k, v)) = line.split_once('=') {
                m.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    m
}

/// What the `client` role reports.
#[derive(Clone, Debug, Default)]
pub struct ClientReport {
    /// Asks issued.
    pub asks: u64,
    /// Replies received.
    pub replies: u64,
    /// Asks that returned an error (handler failure / retries exhausted / bad reply).
    pub errors: u64,
    /// Replies stamped `attempt > 0` (served after a lease-reap redelivery).
    pub redelivered: u64,
    /// Replies that came from the `risk` actor (`--mix` alternates by `to`);
    /// each is verified against [`expected_dv01`](crate::expected_dv01).
    pub risk_replies: u64,
    /// Latency percentiles, nanoseconds.
    pub p50_ns: u64,
    /// p99.
    pub p99_ns: u64,
    /// Max.
    pub max_ns: u64,
    /// Wall time of the whole run, nanoseconds.
    pub elapsed_ns: u64,
    /// Asks per second over the whole run.
    pub throughput: f64,
    /// How many times `curve_version` changed between consecutive replies (per client, summed).
    pub version_changes: u64,
    /// `(pid, replies, first_reply_unix_nanos)` per pricer incarnation seen.
    pub incarnations: Vec<(u32, u64, u64)>,
}

impl ClientReport {
    /// Parse a client result file.
    pub fn from_file(path: &Path) -> ClientReport {
        let m = read_kv(path);
        let num = |k: &str| m.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        let mut incarnations = Vec::new();
        if let Some(s) = m.get("incarnations") {
            for part in s.split(';').filter(|p| !p.is_empty()) {
                let f: Vec<&str> = part.split(':').collect();
                if f.len() == 3 {
                    if let (Ok(a), Ok(b), Ok(c)) =
                        (f[0].parse::<u32>(), f[1].parse::<u64>(), f[2].parse::<u64>())
                    {
                        incarnations.push((a, b, c));
                    }
                }
            }
        }
        incarnations.sort_by_key(|i| i.2);
        ClientReport {
            asks: num("asks"),
            replies: num("replies"),
            errors: num("errors"),
            redelivered: num("redelivered"),
            risk_replies: num("risk_replies"),
            p50_ns: num("p50_ns"),
            p99_ns: num("p99_ns"),
            max_ns: num("max_ns"),
            elapsed_ns: num("elapsed_ns"),
            throughput: m
                .get("throughput")
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0),
            version_changes: num("version_changes"),
            incarnations,
        }
    }

    /// Asks that neither errored nor got a reply.
    pub fn lost(&self) -> u64 {
        self.asks.saturating_sub(self.replies + self.errors)
    }

    /// Serialise to the result-file format.
    pub fn to_kv(&self) -> String {
        let inc: Vec<String> = self
            .incarnations
            .iter()
            .map(|(p, n, t)| format!("{p}:{n}:{t}"))
            .collect();
        format!(
            "asks={}\nreplies={}\nerrors={}\nredelivered={}\nrisk_replies={}\np50_ns={}\np99_ns={}\nmax_ns={}\nelapsed_ns={}\nthroughput={:.1}\nversion_changes={}\nincarnations={}\n",
            self.asks,
            self.replies,
            self.errors,
            self.redelivered,
            self.risk_replies,
            self.p50_ns,
            self.p99_ns,
            self.max_ns,
            self.elapsed_ns,
            self.throughput,
            self.version_changes,
            inc.join(";")
        )
    }
}

/// Options for spawning a `pricer` (directly or under the supervisor).
#[derive(Clone, Debug, Default)]
pub struct PricerOpts {
    /// Busy-poll the mailbox.
    pub spin: bool,
    /// Bare mode: no envelope, no pin — `claim → complete(ZERO)`.
    pub bare: bool,
    /// Claim lease in ms (default 500).
    pub lease_ms: Option<u64>,
    /// Die on the n-th handled message.
    pub kill_after: Option<u64>,
    /// Result file (READY/KILL lines).
    pub result: Option<PathBuf>,
}

impl PricerOpts {
    fn args(&self, uds: &str) -> Vec<String> {
        let mut a = vec!["pricer".to_string(), "--uds".to_string(), uds.to_string()];
        if self.spin {
            a.push("--spin".into());
        }
        if self.bare {
            a.push("--bare".into());
        }
        if let Some(l) = self.lease_ms {
            a.push("--lease-ms".into());
            a.push(l.to_string());
        }
        if let Some(k) = self.kill_after {
            a.push("--kill-after".into());
            a.push(k.to_string());
        }
        if let Some(r) = &self.result {
            a.push("--result".into());
            a.push(r.display().to_string());
        }
        a
    }
}

/// Spawn a pricer process and wait until it reports `READY`.
pub fn spawn_pricer(exe: &str, uds: &str, dir: &Path, tag: &str, opts: &PricerOpts) -> Reaper {
    let result = opts
        .result
        .clone()
        .unwrap_or_else(|| dir.join(format!("pricer-{tag}.log")));
    let _ = std::fs::remove_file(&result);
    let opts = PricerOpts {
        result: Some(result.clone()),
        ..opts.clone()
    };
    let child = Command::new(exe)
        .args(opts.args(uds))
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn pricer");
    let child = Reaper(child);
    assert!(
        wait_until(Duration::from_secs(20), || {
            std::fs::read_to_string(&result)
                .map(|s| s.contains("READY"))
                .unwrap_or(false)
        }),
        "pricer {tag} never became READY"
    );
    child
}

/// Options for a `client` run.
#[derive(Clone, Debug)]
pub struct ClientOpts {
    /// Total asks across all client threads.
    pub n: u64,
    /// Client threads (each with its own node + ref).
    pub clients: u32,
    /// Busy-poll for replies.
    pub spin: bool,
    /// Bare mode (no envelope; pairs with a `--bare` pricer).
    pub bare: bool,
    /// Alternate asks between the `pricer` and `risk` actors (odd `seq` → risk).
    pub mix: bool,
}

impl ClientOpts {
    /// `n` asks from one parked client.
    pub fn parked(n: u64) -> ClientOpts {
        ClientOpts {
            n,
            clients: 1,
            spin: false,
            bare: false,
            mix: false,
        }
    }
}

/// Run a `client` to completion and parse its report.
pub fn run_client(exe: &str, uds: &str, dir: &Path, tag: &str, opts: &ClientOpts) -> ClientReport {
    let result = dir.join(format!("client-{tag}.result"));
    let _ = std::fs::remove_file(&result);
    let mut args = vec![
        "client".to_string(),
        "--uds".into(),
        uds.into(),
        "--n".into(),
        opts.n.to_string(),
        "--clients".into(),
        opts.clients.to_string(),
        "--result".into(),
        result.display().to_string(),
    ];
    if opts.spin {
        args.push("--spin".into());
    }
    if opts.bare {
        args.push("--bare".into());
    }
    if opts.mix {
        args.push("--mix".into());
    }
    let status = Command::new(exe)
        .args(&args)
        .stdout(Stdio::null())
        .status()
        .expect("run client");
    assert!(status.success(), "client {tag} exited with {status}");
    ClientReport::from_file(&result)
}

/// Spawn `curve-publish` and wait for it to exit (the commit is synchronous).
pub fn publish_curve(exe: &str, uds: &str, bump_bp: f64) {
    let status = Command::new(exe)
        .args([
            "curve-publish",
            "--uds",
            uds,
            "--bump-bp",
            &bump_bp.to_string(),
        ])
        .stdout(Stdio::null())
        .status()
        .expect("run curve-publish");
    assert!(status.success(), "curve-publish exited with {status}");
}

/// What the crash scenario observed.
#[derive(Clone, Debug)]
pub struct CrashOutcome {
    /// The client's report.
    pub client: ClientReport,
    /// Restarts the supervisor logged.
    pub restarts: u32,
    /// Wall-clock nanos at which the first pricer `_exit`ed.
    pub kill_ns: Option<u64>,
    /// Wall-clock nanos of the first reply from the successor.
    pub first_successor_reply_ns: Option<u64>,
    /// The supervisor's log, for diagnostics.
    pub supervisor_log: String,
}

impl CrashOutcome {
    /// `SIGKILL → first reply from the successor`, if both were observed.
    pub fn kill_to_first_reply(&self) -> Option<Duration> {
        match (self.kill_ns, self.first_successor_reply_ns) {
            (Some(k), Some(f)) if f >= k => Some(Duration::from_nanos(f - k)),
            _ => None,
        }
    }
}

/// The crash scenario: a supervisor runs a pricer that dies on its
/// `kill_after`-th message; a client sends `n` asks; the supervisor respawns;
/// the client's in-flight ask is redelivered by the lease reap and answered by
/// the successor. Returns once the client has finished and the supervisor has
/// been taken down (its child with it).
pub fn crash_scenario(
    exe: &str,
    uds: &str,
    dir: &Path,
    tag: &str,
    n: u64,
    kill_after: u64,
    lease_ms: u64,
) -> CrashOutcome {
    let sup_log = dir.join(format!("supervisor-{tag}.log"));
    let pricer_log = dir.join(format!("pricer-crash-{tag}.log"));
    let _ = std::fs::remove_file(&sup_log);
    let _ = std::fs::remove_file(&pricer_log);
    let sup = Command::new(exe)
        .args([
            "supervisor",
            "--uds",
            uds,
            "--kill-after",
            &kill_after.to_string(),
            "--lease-ms",
            &lease_ms.to_string(),
            "--result",
            sup_log.to_str().unwrap(),
            "--pricer-result",
            pricer_log.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn supervisor");
    let mut sup = TermReaper(sup);
    assert!(
        wait_until(Duration::from_secs(20), || {
            std::fs::read_to_string(&pricer_log)
                .map(|s| s.contains("READY"))
                .unwrap_or(false)
        }),
        "supervised pricer never became READY"
    );

    let client = run_client(exe, uds, dir, &format!("crash-{tag}"), &ClientOpts::parked(n));

    // The restart is logged when the supervisor respawns; it happened before
    // the client's later asks were answered, but give the log a moment.
    let _ = wait_until(Duration::from_secs(5), || {
        std::fs::read_to_string(&sup_log)
            .map(|s| s.contains("RESTART"))
            .unwrap_or(false)
    });
    let supervisor_log = std::fs::read_to_string(&sup_log).unwrap_or_default();
    let restarts = supervisor_log
        .lines()
        .filter(|l| l.starts_with("RESTART"))
        .count() as u32;
    let kill_ns = std::fs::read_to_string(&pricer_log)
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("KILL "))
                .and_then(|l| l[5..].trim().parse::<u64>().ok())
        });
    // Incarnations are sorted by first reply; the second is the successor.
    let first_successor_reply_ns = client.incarnations.get(1).map(|i| i.2);

    sup.terminate();
    CrashOutcome {
        client,
        restarts,
        kill_ns,
        first_successor_reply_ns,
        supervisor_log,
    }
}

/// Nearest-rank percentile over sorted nanosecond samples.
pub fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let n = sorted.len();
    let i = ((q * n as f64).ceil() as usize).max(1) - 1;
    sorted[i.min(n - 1)]
}
