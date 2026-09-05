//! The value bench: what this design claims, what it costs, and the shapes
//! that punish it (ADR-0017 gate).
//!
//! The earlier fan-out bench measured aggregate events per second, which is
//! the one axis where the architecture is irrelevant: it scales trivially
//! with subscriber count because it counts the same message once per
//! subscriber, and it hid the two axes that actually move (publish rate and
//! per-subscriber latency, both of which *degrade*). Every stage here is
//! written the other way round: state a claim, measure the axis that could
//! falsify it, and print the cost next to the win.
//!
//! Rules this bench follows:
//!
//! - **An offered rate, not a firehose.** Publishing as fast as possible
//!   measures how fast a queue fills. Latency percentiles only mean
//!   something at a rate the system is expected to sustain.
//! - **A lagged row is a failed row.** If a healthy subscriber falls out of
//!   the replay window, the run is reported as FAIL, not as a number.
//! - **A floor.** Every fan-out row is printed against the same shape with
//!   no Holon in it: one writer, N plain sockets, N reader threads.
//! - **A losing shape.** One subscriber that cannot keep up, and one row
//!   (single-process fan-out) where a plain broadcast channel would win.
//! - **A fresh stack per stage.** Retained windows are per topic and live
//!   until the topic is deleted, so stages sharing one node poison each
//!   other. That is a real property of the design, and the first version of
//!   this bench fell into it.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use holon_net::client::HttpClient;

use crate::roles::{
    fmt_ns, http_client, publish_with, raw_fanout, sse_client_with, tcp_client, MultiStack, Opts,
    PublishOpts, SseOpts, TcpClientOpts, TcpMode,
};
use crate::DONE_TOPIC;

/// Replay window used by every fan-out stage, in messages.
const WINDOW: u32 = 4096;

/// Shared-memory store per stack, in MiB.
const STORE_MB: usize = 384;

/// How far above the no-Holon floor a p99 may sit before the row fails.
const FLOOR_FACTOR: f64 = 5.0;

/// A stage's verdict. `Invalid` is deliberately distinct from `Fail`: a row
/// whose *comparator* could not run says nothing about the system under test,
/// and collapsing the two would read as a failure that never happened.
enum Verdict {
    Pass,
    Fail(String),
    Invalid(String),
}

impl Verdict {
    fn pass() -> Verdict {
        Verdict::Pass
    }

    fn fail(why: impl Into<String>) -> Verdict {
        Verdict::Fail(why.into())
    }

    fn invalid(why: impl Into<String>) -> Verdict {
        Verdict::Invalid(why.into())
    }

    fn tag(&self) -> String {
        match self {
            Verdict::Pass => "PASS".into(),
            Verdict::Fail(w) => format!("FAIL ({w})"),
            Verdict::Invalid(w) => format!("INVALID ({w})"),
        }
    }
}

/// Did a paced side actually deliver the rate it was offered? Counting
/// individually-late messages is far too strict — one late message in twenty
/// thousand is not a missed rate — so this compares achieved throughput.
fn sustained(achieved_per_s: u64, offered_per_s: u64) -> bool {
    offered_per_s == 0 || achieved_per_s as f64 >= offered_per_s as f64 * 0.95
}

/// One fan-out measurement: `subs` subscribers spread over the stack's
/// gateways, one paced publisher, every subscriber verified for gaps.
struct FanOut {
    got: u64,
    expect: u64,
    gaps: u64,
    lagged: u64,
    p50: u64,
    p99: u64,
    worst_p99: u64,
    publish_per_s: u64,
    /// Delivered to healthy subscribers only, and what they were owed. A
    /// deliberately slow subscriber is *expected* to miss messages once it
    /// falls out of the window — that is the behaviour under test — so it
    /// cannot be counted against completeness.
    healthy_got: u64,
    healthy_expect: u64,
    /// Messages the publisher could not issue on schedule. A run that fell
    /// behind its own offered rate did not measure that rate.
    behind_schedule: u64,
    slow_lagged: u64,
}

/// The shape of one fan-out measurement.
struct FanOutOpts {
    subs: usize,
    msgs: u64,
    rate: u64,
    bytes: usize,
    /// How many of the subscribers read deliberately slowly.
    slow: usize,
    /// Microseconds of throttle applied to each slow subscriber.
    slow_us: u64,
}

impl FanOutOpts {
    fn new(subs: usize, msgs: u64, rate: u64, bytes: usize) -> FanOutOpts {
        FanOutOpts {
            subs,
            msgs,
            rate,
            bytes,
            slow: 0,
            slow_us: 0,
        }
    }

    fn with_slow(mut self, slow: usize, slow_us: u64) -> FanOutOpts {
        self.slow = slow;
        self.slow_us = slow_us;
        self
    }
}

fn fan_out(stack: &MultiStack, topic: &str, o: &FanOutOpts) -> Result<FanOut, String> {
    let (subs, msgs, rate, bytes, slow, slow_us) =
        (o.subs, o.msgs, o.rate, o.bytes, o.slow, o.slow_us);
    let mut handles = Vec::with_capacity(subs);
    for i in 0..subs {
        let http = stack.http_n(i);
        let topic = topic.to_string();
        let throttle = if i < slow { slow_us } else { 0 };
        handles.push(std::thread::spawn(move || {
            (
                throttle > 0,
                sse_client_with(
                    http,
                    &topic,
                    &SseOpts::live(msgs, Duration::from_secs(120)).throttled(throttle),
                ),
            )
        }));
    }
    // Let every subscriber's SSE stream be established before publishing;
    // a subscriber that connects late is measuring a backlog drain.
    std::thread::sleep(Duration::from_millis(400 + subs as u64 * 2));
    let p = publish_with(
        stack.tcp(),
        topic,
        &PublishOpts::paced(msgs, rate).with_bytes(bytes),
    )?;
    let (mut got, mut gaps, mut lagged, mut slow_lagged) = (0u64, 0u64, 0u64, 0u64);
    let (mut healthy_got, mut healthy_subs) = (0u64, 0u64);
    let (mut worst_p99, mut p50s, mut p99s) = (0u64, Vec::new(), Vec::new());
    for h in handles {
        let (was_slow, r) = h.join().map_err(|_| "sse thread panicked")?;
        let r = r?;
        got += r.num::<u64>("got").unwrap_or(0);
        gaps += r.num::<u64>("gaps").unwrap_or(0);
        let l = r.num::<u64>("lagged").unwrap_or(0);
        if was_slow {
            slow_lagged += l;
        } else {
            healthy_got += r.num::<u64>("got").unwrap_or(0);
            healthy_subs += 1;
            lagged += l;
            // Only healthy subscribers define the latency claim.
            p50s.push(r.num::<u64>("lat_p50_ns").unwrap_or(0));
            p99s.push(r.num::<u64>("lat_p99_ns").unwrap_or(0));
            worst_p99 = worst_p99.max(r.num::<u64>("lat_p99_ns").unwrap_or(0));
        }
    }
    p50s.sort_unstable();
    p99s.sort_unstable();
    Ok(FanOut {
        got,
        expect: msgs * subs as u64,
        healthy_got,
        healthy_expect: msgs * healthy_subs,
        gaps,
        lagged,
        slow_lagged,
        p50: p50s.get(p50s.len() / 2).copied().unwrap_or(0),
        p99: p99s.get(p99s.len() / 2).copied().unwrap_or(0),
        worst_p99,
        publish_per_s: p.num::<u64>("per_s").unwrap_or(0),
        behind_schedule: p.num::<u64>("behind_schedule").unwrap_or(0),
    })
}

fn delete_topic(http: SocketAddr, topic: &str) {
    if let Ok(mut c) = HttpClient::connect(http) {
        let _ = c.request("DELETE", &format!("/topics/{topic}"), &[], &[]);
    }
}

/// Run the value bench.
pub fn run(opts: &Opts) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let exe = exe.to_string_lossy().to_string();
    let runs: u32 = opts.num("runs").unwrap_or(2);
    let msgs: u64 = opts.num("n").unwrap_or(20_000);
    let rate: u64 = opts.num("rate").unwrap_or(20_000);
    let bytes: usize = opts.num("bytes").unwrap_or(64);
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

    line!("== holon-net VALUE bench (ADR-0017) ==");
    line!(
        "host {} {} · release={} · {runs} runs · {msgs} msgs at {rate}/s offered · {bytes} B payload · window {WINDOW}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        !cfg!(debug_assertions)
    );
    line!("A lagged healthy subscriber is a FAIL, not a number. Every fan-out row is printed against a no-Holon floor.");

    for run in 1..=runs {
        line!("\n───────── run {run}/{runs} ─────────");

        // ── Claim 1: per-subscriber delivery latency stays flat as
        //    subscribers grow. This is the only fan-out claim worth making;
        //    aggregate events/s grows by construction.
        if want("flat") {
            line!("\n[1] CLAIM: per-subscriber delivery latency TRACKS THE NO-HOLON FLOOR as subscriber count grows.");
            line!("    Not \"stays flat\": one reactor thread writing N sockets serialises by construction, and so does the floor.");
            line!("    The claim is that the log, the cursors and the SSE framing add a bounded factor, not a growing one.");
            line!("    Falsified by: WORST healthy p99 above {FLOOR_FACTOR}x the floor's worst, any healthy subscriber lagging,");
            line!("    or either side failing to sustain the offered rate — a row whose floor could not keep up is INVALID, not a win.");
            line!(
                "    {:>5}  {:>10} {:>10} │ {:>10} {:>10} {:>10} │ {:>7}  {:>10}  {:>9}   {}",
                "subs",
                "floor p50",
                "floor p99",
                "p50",
                "p99",
                "worst p99",
                "vs p99",
                "publish/s",
                "behind",
                "verdict"
            );
            let stack = MultiStack::spawn(&exe, 1, 1, WINDOW, STORE_MB)?;
            for &n in &[1usize, 16, 64, 256] {
                // The floor is offered the SAME rate. An unpaced floor writes
                // as fast as the kernel takes it, which is a different
                // experiment, and dividing by it manufactures a win.
                let floor = raw_fanout(n, msgs.min(20_000), 64 + bytes, rate)?;
                let fp99 = floor.num::<u64>("p99_ns").unwrap_or(1).max(1);
                let floor_behind = floor.num::<u64>("behind_schedule").unwrap_or(0);
                let topic = format!("flat{n}");
                let f = fan_out(&stack, &topic, &FanOutOpts::new(n, msgs, rate, bytes))?;
                // The worst healthy subscriber, not the median one: a fan-out
                // claim is about the reader that fared worst.
                let ratio = f.worst_p99 as f64 / fp99 as f64;
                let v = if f.gaps > 0 {
                    Verdict::fail(format!("{} gaps", f.gaps))
                } else if f.lagged > 0 {
                    Verdict::fail(format!("{} healthy subs lagged", f.lagged))
                } else if f.got != f.expect {
                    Verdict::fail(format!("{}/{} delivered", f.got, f.expect))
                } else if !sustained(floor.num::<u64>("write_per_s").unwrap_or(0), rate) {
                    Verdict::invalid(format!(
                        "the floor could not offer {rate}/s at {n} subscribers: {} msg/s, {floor_behind} late",
                        floor.num::<u64>("write_per_s").unwrap_or(0)
                    ))
                } else if !sustained(f.publish_per_s, rate) {
                    Verdict::fail(format!("did not sustain {rate}/s"))
                } else if ratio > FLOOR_FACTOR {
                    Verdict::fail(format!("{ratio:.1}x the floor"))
                } else {
                    Verdict::pass()
                };
                line!(
                    "    {n:>5}  {:>10} {:>10} │ {:>10} {:>10} {:>10} │ {:>6.1}x  {:>10}  {:>9}   {}",
                    fmt_ns(floor.num::<u64>("p50_ns").unwrap_or(0)),
                    fmt_ns(fp99),
                    fmt_ns(f.p50),
                    fmt_ns(f.p99),
                    fmt_ns(f.worst_p99),
                    ratio,
                    f.publish_per_s,
                    f.behind_schedule,
                    v.tag()
                );
                delete_topic(stack.http(), &topic);
            }
            line!("    Aggregate events/s is not printed at all: it grows with subscriber count by construction and can only flatter.");
            drop(stack);

            // Does the reactor scale? The one knob that could flatten the
            // curve is more reactor threads, so measure it rather than
            // asserting it.
            line!("\n[1b] Does adding reactor threads recover the tail at 256 subscribers?");
            line!(
                "    {:>7}  {:>10}  {:>10}  {:>12}   {}",
                "shards",
                "p50",
                "p99",
                "publish/s",
                "verdict"
            );
            let mut base_p50 = 0u64;
            for &sh in &[1usize, 2, 4] {
                let stack = MultiStack::spawn(&exe, 1, sh, WINDOW, STORE_MB)?;
                let f = fan_out(&stack, "shard", &FanOutOpts::new(256, msgs, rate, bytes))?;
                if sh == 1 {
                    base_p50 = f.p50.max(1);
                }
                // The stage asks a question; the verdict has to answer it.
                // Printing PASS while every latency gets worse is how a
                // negative result hides in a table.
                let v = if f.gaps > 0 || f.lagged > 0 {
                    Verdict::fail(format!("{} gaps, {} lagged", f.gaps, f.lagged))
                } else if !sustained(f.publish_per_s, rate) {
                    Verdict::fail(format!("did not sustain {rate}/s"))
                } else if sh > 1 && f.p50 > base_p50 {
                    Verdict::fail(format!(
                        "sharding made p50 {:.1}x worse",
                        f.p50 as f64 / base_p50 as f64
                    ))
                } else {
                    Verdict::pass()
                };
                line!(
                    "    {sh:>7}  {:>10}  {:>10}  {:>12}   {}",
                    fmt_ns(f.p50),
                    fmt_ns(f.p99),
                    f.publish_per_s,
                    v.tag()
                );
            }
        }

        // ── Losing shape: one subscriber that cannot keep up.
        if want("slow") {
            line!("\n[2] LOSING SHAPE: 1 of 16 subscribers reads 10x too slowly, against a {SLOW_WINDOW}-message window it cannot stay inside.");
            line!("    Claim: the other 15 are unaffected and the slow one is TOLD it lagged, never silently skipped.");
            // A deliberately SHORT window. The claim under test is what happens
            // to a reader that falls out of the retention window, so the shape
            // has to guarantee it falls out: at the default 4096 a reader that
            // is merely 10x slow is still inside the window once the gateway's
            // 1 MiB write buffer is counted, and the row passes while proving
            // nothing. The window is the variable that decides whether the
            // branch runs at all, so it is the one this stage sets.
            const SLOW_WINDOW: u32 = 256;
            let stack = MultiStack::spawn(&exe, 1, 1, SLOW_WINDOW, STORE_MB)?;
            let ctrl = fan_out(&stack, "ctrl", &FanOutOpts::new(16, msgs, rate, bytes))?;
            delete_topic(stack.http(), "ctrl");
            let slow_us = 1_000_000 / (rate / 10).max(1);
            let f = fan_out(
                &stack,
                "slow",
                &FanOutOpts::new(16, msgs, rate, bytes).with_slow(1, slow_us),
            )?;
            delete_topic(stack.http(), "slow");
            let v = if f.gaps > 0 {
                Verdict::fail(format!("{} gaps: loss was silent", f.gaps))
            } else if f.lagged > 0 {
                Verdict::fail(format!("{} healthy subs dragged into lag", f.lagged))
            } else if f.healthy_got != f.healthy_expect {
                Verdict::fail(format!(
                    "healthy subs got {}/{}",
                    f.healthy_got, f.healthy_expect
                ))
            } else if f.slow_lagged == 0 {
                // The claim is that the slow reader is TOLD. If it never fell
                // out of the window, the notification path did not run and
                // this row proves nothing about it.
                Verdict::fail("slow sub never lagged: the lag-notice path did not run".to_string())
            } else if f.p99 > ctrl.p99.max(1) * 4 {
                Verdict::fail(format!(
                    "healthy p99 {} vs {} control",
                    fmt_ns(f.p99),
                    fmt_ns(ctrl.p99)
                ))
            } else {
                Verdict::pass()
            };
            line!(
                "    control  16 healthy          p99 {:>10} · publish {} msg/s",
                fmt_ns(ctrl.p99),
                ctrl.publish_per_s
            );
            line!(
                "    test     15 healthy + 1 slow p99 {:>10} · publish {} msg/s · slow sub lag notices {} · healthy {}/{} · healthy lag {} · gaps {}   {}",
                fmt_ns(f.p99),
                f.publish_per_s,
                f.slow_lagged,
                f.healthy_got,
                f.healthy_expect,
                f.lagged,
                f.gaps,
                v.tag()
            );
        }

        // ── Claim 2: the structural one. Separate OS processes, one log.
        if want("xproc") {
            line!("\n[3] CLAIM: fan-out spans OS PROCESSES with no relay and no second copy.");
            line!("    This is what an in-process broadcast channel cannot do at all: it would need a relay hop per process.");
            line!(
                "    {:>10}  {:>6}  {:>10}  {:>10}  {:>12}  {:>10}   {}",
                "gateways",
                "subs",
                "p50",
                "p99",
                "publish/s",
                "gaps",
                "verdict"
            );
            for &(gws, per) in &[(1usize, 64usize), (2, 32), (4, 16)] {
                let stack = MultiStack::spawn(&exe, gws, 1, WINDOW, STORE_MB)?;
                let subs = gws * per;
                let f = fan_out(&stack, "xproc", &FanOutOpts::new(subs, msgs, rate, bytes))?;
                // This row is a CAPABILITY claim (delivery across processes),
                // not a speed claim. The latency columns are information; the
                // only thing that must hold is that the load was actually
                // offered and everything arrived.
                let v = if f.gaps > 0 || f.lagged > 0 || f.got != f.expect {
                    Verdict::fail(format!(
                        "{}/{} delivered, {} gaps, {} lagged",
                        f.got, f.expect, f.gaps, f.lagged
                    ))
                } else if !sustained(f.publish_per_s, rate) {
                    Verdict::fail(format!("did not sustain {rate}/s"))
                } else {
                    Verdict::pass()
                };
                line!(
                    "    {gws:>10}  {subs:>6}  {:>10}  {:>10}  {:>12}  {:>10}   {}",
                    fmt_ns(f.p50),
                    fmt_ns(f.p99),
                    f.publish_per_s,
                    f.gaps,
                    v.tag()
                );
            }
            line!("    Same subscriber count each row; only the process split changes. One publish, N processes, zero relay hops.");
        }

        // ── Claim 3: the backlog outlives the gateway that served it.
        if want("crash") {
            line!("\n[4] CLAIM: the replay log outlives the gateway process. SIGKILL a gateway mid-stream; resume elsewhere with Last-Event-ID; lose nothing.");
            let mut stack = MultiStack::spawn(&exe, 2, 1, WINDOW, STORE_MB)?;
            let (gw0_http, gw1_http, tcp) = (stack.gws[0].http, stack.gws[1].http, stack.tcp());
            let half = msgs / 2;
            let sub = std::thread::spawn(move || {
                sse_client_with(
                    gw0_http,
                    "crash",
                    &SseOpts::live(msgs, Duration::from_secs(120))
                        .reconnecting(half, Some(gw1_http)),
                )
            });
            std::thread::sleep(Duration::from_millis(400));
            let p = publish_with(
                tcp,
                "crash",
                &PublishOpts::paced(msgs, rate).with_bytes(bytes),
            )?;
            let r = sub.join().map_err(|_| "sse thread panicked")??;
            let migrated = r.num::<u64>("got").unwrap_or(0) == msgs
                && r.num::<u64>("gaps").unwrap_or(1) == 0
                && r.num::<u64>("reconnects").unwrap_or(0) == 1;
            line!(
                "    mid-stream migration gw0→gw1  got {}/{} gaps {} reconnects {} · published {} msg/s   {}",
                r.get("got").unwrap_or("?"),
                msgs,
                r.get("gaps").unwrap_or("?"),
                r.get("reconnects").unwrap_or("?"),
                p.get("per_s").unwrap_or("?"),
                if migrated {
                    Verdict::pass().tag()
                } else {
                    Verdict::fail("lost messages across the migration").tag()
                }
            );
            // Now kill the gateway that served the first half outright, and
            // replay the whole topic through the survivor.
            stack.gws[0].kill();
            let replay = sse_client_with(
                gw1_http,
                "crash",
                &SseOpts::live(u64::MAX, Duration::from_secs(20)).from(Some(0)),
            )?;
            let got: u64 = replay.num("got").unwrap_or(0);
            let expect = msgs.min(u64::from(WINDOW) * 2);
            let v = if replay.num::<u64>("gaps").unwrap_or(1) != 0 {
                Verdict::fail("gaps in the replay")
            } else if got == 0 {
                Verdict::fail("log did not survive the gateway")
            } else {
                Verdict::pass()
            };
            line!(
                "    after SIGKILL of gw0, replay from seq 0 on gw1   replayed {got} of {msgs} published (window holds {expect}) gaps {} lag notices {}   {}",
                replay.get("gaps").unwrap_or("?"),
                replay.get("lagged").unwrap_or("?"),
                v.tag()
            );
        }

        // ── The bill.
        if want("cost") {
            line!("\n[5] COST: what the above is paid for with. These are the rows a broadcast-channel design wins.");
            let stack = MultiStack::spawn(&exe, 1, 1, WINDOW, STORE_MB)?;
            let acked = publish_with(
                stack.tcp(),
                "cost",
                &PublishOpts {
                    n: 2_000,
                    window: 0,
                    event: "tick".into(),
                    rate_per_s: 0,
                    bytes,
                },
            )?;
            let fire = publish_with(stack.tcp(), "cost2", &PublishOpts::burst(20_000, 64))?;
            line!(
                "    publish, acknowledged one at a time   {} msg/s   (a channel send is a pointer push; this is an RCU commit: chunk alloc, Arrow batch, write lease, manifest install)",
                acked.get("per_s").unwrap_or("?")
            );
            line!(
                "    publish, pipelined fire-and-forget    {} msg/s",
                fire.get("per_s").unwrap_or("?")
            );
            line!(
                "    retained window is a fixed pool of chunks, not a vector of pointers: a window of {WINDOW} holds up to {} live message chunks plus their manifests, per topic, until the topic is DELETEd.",
                WINDOW * 2
            );
            line!("    Arrow earns nothing on this path: a one-row batch and a schema carry an opaque byte blob.");
            line!("    Verdict: for single-process fan-out alone, an in-process broadcast channel wins. The wins above are cross-process reach and crash survival.");
        }

        // ── Request/response path, unchanged from the original bench.
        if want("asks") {
            line!(
                "\n[6] REQUEST/RESPONSE (unchanged claim: pipelined asks near the loopback floor)."
            );
            let floor = crate::roles::raw_echo(msgs, 1, 80)?;
            let stack = MultiStack::spawn(&exe, 1, 1, WINDOW, STORE_MB)?;
            let w1 = tcp_client(
                stack.tcp(),
                &TcpClientOpts {
                    n: msgs,
                    window: 1,
                    body: 16,
                    mode: TcpMode::Ask,
                },
            )?;
            line!(
                "    raw loopback echo RTT p50 {} · TCP ask RTT p50 {} p99 {} · overhead {}",
                fmt_ns(floor.num::<u64>("p50_ns").unwrap_or(0)),
                fmt_ns(w1.num::<u64>("p50_ns").unwrap_or(0)),
                fmt_ns(w1.num::<u64>("p99_ns").unwrap_or(0)),
                fmt_ns(
                    w1.num::<u64>("p50_ns")
                        .unwrap_or(0)
                        .saturating_sub(floor.num::<u64>("p50_ns").unwrap_or(0))
                )
            );
            for w in [64usize, 1024] {
                let r = tcp_client(
                    stack.tcp(),
                    &TcpClientOpts {
                        n: msgs * 4,
                        window: w,
                        body: 16,
                        mode: TcpMode::Ask,
                    },
                )?;
                line!(
                    "    pipelined w={w:<5} {} asks/s · errors {}",
                    r.get("per_s").unwrap_or("?"),
                    r.get("errors").unwrap_or("?")
                );
            }
            let h = http_client(stack.http(), msgs / 4, 16)?;
            line!(
                "    HTTP ask RTT p50 {} p99 {} · {} req/s · errors {}",
                fmt_ns(h.num::<u64>("p50_ns").unwrap_or(0)),
                fmt_ns(h.num::<u64>("p99_ns").unwrap_or(0)),
                h.get("per_s").unwrap_or("?"),
                h.get("errors").unwrap_or("?")
            );
        }

        // ── The ADR-0006 shape, on its own stack (it needs the whole pool).
        if want("range") {
            line!("\n[7] COALESCED COMPLETIONS (ADR-0006 lifted): one descriptor, N handles.");
            let stack = MultiStack::spawn(&exe, 1, 1, WINDOW, STORE_MB)?;
            for compress in [false, true] {
                let reqs = 200u64;
                let count = 1024u32;
                let expect = if compress {
                    reqs
                } else {
                    reqs * u64::from(count)
                };
                let tcp = stack.tcp();
                let sub = std::thread::spawn(move || {
                    crate::roles::tcp_sub(
                        tcp,
                        DONE_TOPIC,
                        u64::MAX,
                        expect,
                        Duration::from_secs(60),
                    )
                });
                std::thread::sleep(Duration::from_millis(300));
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
                line!(
                    "    {:<11} {reqs}x{count} handles  {} handles/s seen by a subscriber · {} events on the wire · {} req/s · errors {}",
                    if compress { "coalesced" } else { "per-handle" },
                    (s.num::<u64>("handles").unwrap_or(0) as f64 / t.elapsed().as_secs_f64()) as u64,
                    s.get("got").unwrap_or("?"),
                    r.get("per_s").unwrap_or("?"),
                    r.get("errors").unwrap_or("?")
                );
                if let Some(e) = r.get("first_error") {
                    line!("      first error: {e}");
                }
            }
        }
    }

    if let Some(path) = opts.get("out") {
        std::fs::write(path, &out).map_err(|e| e.to_string())?;
    }
    Ok(())
}
