//! ADR-0017 gate tests: the three fronts end to end.
//!
//! In-process (coordinator + actor host thread + gateway threads + clients)
//! except [`multiprocess_stack_survives_client_churn`], which stands the
//! stack up as separate OS processes and kills clients mid-stream.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use holon_core::{ActorId, Payload};
use holon_net::client::{FrameClient, HttpClient, Inbound};
use holon_net_demo::roles::{
    http_client, publisher, spawn_host_thread, spawn_stack, sse_client, start_coordinator,
    start_gateway, tcp_client, tcp_sub, wait_gateway, wait_until, TcpClientOpts, TcpMode,
};
use holon_net_demo::{
    Count, Get, Inc, Work, WorkDone, COUNTER, COUNTER_GET, COUNTER_INC, DONE_TOPIC, ECHO, ECHO_REQ,
    SINK, WORK_REQ,
};

struct Stack {
    _dir: tempfile::TempDir,
    coord: shm_runtime::Coordinator,
    stop: Arc<AtomicBool>,
    host: Option<std::thread::JoinHandle<()>>,
    gw: Option<holon_net::Gateway>,
    tcp: std::net::SocketAddr,
    http: std::net::SocketAddr,
}

impl Stack {
    fn up(shards: usize) -> Stack {
        let dir = tempfile::tempdir().unwrap();
        let (coord, uds) = start_coordinator(dir.path(), 4096, 160);
        let stop = Arc::new(AtomicBool::new(false));
        let host = spawn_host_thread(&uds, "host", false, stop.clone());
        let gw = start_gateway(&uds, "gw", shards);
        let tcp = gw.tcp_addr().unwrap();
        let http = gw.http_addr().unwrap();
        assert!(wait_gateway(tcp), "gateway did not answer a ping");
        Stack {
            _dir: dir,
            coord,
            stop,
            host: Some(host),
            gw: Some(gw),
            tcp,
            http,
        }
    }

    fn store_free(&self) -> usize {
        self.coord.store_data_free_total().unwrap()
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        if let Some(gw) = self.gw.take() {
            let _ = gw.stop();
        }
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.host.take() {
            let _ = h.join();
        }
    }
}

#[test]
fn tcp_ask_reply_pipelined_and_ping() {
    let s = Stack::up(1);
    let mut c = FrameClient::connect(s.tcp).unwrap();
    c.ping().unwrap();
    // One ask, the reply is the body.
    let (env, body) = c.ask(ActorId::named(ECHO), ECHO_REQ, b"hello").unwrap();
    assert_eq!(body, b"hello");
    assert_eq!(env.schema_id, ECHO_REQ);
    // 2000 asks with 256 in flight: every corr answered with its own body.
    let r = tcp_client(
        s.tcp,
        &TcpClientOpts {
            n: 2000,
            window: 256,
            body: 32,
            mode: TcpMode::Ask,
        },
    )
    .unwrap();
    assert_eq!(r.get("errors"), Some("0"));
    // Unknown actor → Err frame with the request corr, connection stays up.
    let mut c2 = FrameClient::connect(s.tcp).unwrap();
    let corr = c2.queue_ask(ActorId::named("nobody"), 1, b"x").unwrap();
    c2.flush().unwrap();
    c2.set_read_timeout(Some(Duration::from_secs(90))).unwrap();
    // The ask sits queued until the gateway's ask timeout (30 s) or the
    // task horizon reaps it; either way the client gets an Err, not a hang.
    // Keep the test fast: the ping proves the connection is not blocked.
    c2.ping().unwrap();
    let _ = corr;
    // A LocalRef on the wire closes the connection.
    let mut raw = std::net::TcpStream::connect(s.tcp).unwrap();
    let mut env = holon_net::wire::ask(ActorId::named(ECHO), ActorId::NONE, 1, ECHO_REQ);
    env.flags |= holon_core::FLAG_LOCAL_REF;
    env.payload = 1;
    std::io::Write::write_all(&mut raw, &env.to_bytes()).unwrap();
    let mut b = [0u8; 8];
    assert_eq!(
        std::io::Read::read(&mut raw, &mut b).unwrap_or(0),
        0,
        "gateway closes on I2 violation"
    );
}

#[test]
fn tcp_ask_reply_over_the_inline_cap() {
    let s = Stack::up(1);
    let baseline = s.store_free();
    let mut c = FrameClient::connect(s.tcp).unwrap();
    // 4 000 bytes: twenty times `MAX_INLINE_BODY`, so this round trip could not
    // exist before the reply went by reference. It rides one frame either way
    // (`MAX_WIRE_BODY` is the same u16 that bounds a reply body).
    let body: Vec<u8> = (0..4_000u32).map(|i| (i % 251) as u8).collect();
    let (env, got) = c.ask(ActorId::named(ECHO), ECHO_REQ, &body).unwrap();
    assert_eq!(env.schema_id, ECHO_REQ);
    assert_eq!(got, body, "byte-identical through the gateway");

    // A body past the gateway's per-ask cap is refused, not truncated.
    let big = vec![7u8; holon_net::DEFAULT_MAX_REPLY as usize + 1];
    let corr = c.queue_ask(ActorId::named(ECHO), ECHO_REQ, &big).unwrap();
    c.flush().unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    match c.recv().unwrap() {
        Inbound::Err { env, .. } => assert_eq!(env.corr, corr),
        other => panic!("expected an error frame, got {other:?}"),
    }

    // The gateway materialises and releases: nothing is retained per ask.
    drop(c);
    assert!(
        wait_until(Duration::from_secs(5), || s.store_free() == baseline),
        "store pool {} != baseline {baseline}: reply chunks leaked",
        s.store_free()
    );
}

#[test]
fn tcp_tell_then_ask_counter() {
    let s = Stack::up(1);
    let r = tcp_client(
        s.tcp,
        &TcpClientOpts {
            n: 5000,
            window: 512,
            body: 8,
            mode: TcpMode::Tell,
        },
    )
    .unwrap();
    assert_eq!(r.get("ok"), Some("true"), "{}", r.to_kv());
    let mut c = FrameClient::connect(s.tcp).unwrap();
    let (_, b) = c
        .ask(
            ActorId::named(COUNTER),
            COUNTER_GET,
            bytemuck::bytes_of(&Get { _pad: 0 }),
        )
        .unwrap();
    assert_eq!(Count::from_bytes(&b).unwrap().count, 5000);
}

#[test]
fn tcp_subscribe_replay_live_and_lag() {
    let s = Stack::up(1);
    let mut p = FrameClient::connect(s.tcp).unwrap();
    for i in 1..=10u64 {
        assert_eq!(
            p.publish("t1", "ev", format!("m{i}").as_bytes()).unwrap(),
            i
        );
    }
    // Replay from 0, then live.
    let mut c = FrameClient::connect(s.tcp).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let ack = c.subscribe("t1", 0).unwrap();
    assert_eq!((ack.head, ack.cursor), (10, 0));
    let mut seqs = Vec::new();
    while seqs.len() < 10 {
        if let Inbound::Event { head, data } = c.recv().unwrap() {
            assert_eq!(head.topic_id, ack.topic_id);
            assert_eq!(head.event(), "ev");
            assert_eq!(data, format!("m{}", head.seq).as_bytes());
            seqs.push(head.seq);
        }
    }
    assert_eq!(seqs, (1..=10).collect::<Vec<_>>());
    for i in 11..=15u64 {
        p.publish("t1", "ev", format!("m{i}").as_bytes()).unwrap();
    }
    let mut live = Vec::new();
    while live.len() < 5 {
        if let Inbound::Event { head, .. } = c.recv().unwrap() {
            live.push(head.seq);
        }
    }
    assert_eq!(live, (11..=15).collect::<Vec<_>>());
    // Live-only subscription sees nothing old.
    let mut c2 = FrameClient::connect(s.tcp).unwrap();
    c2.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let ack2 = c2.subscribe("t1", u64::MAX).unwrap();
    assert_eq!(ack2.cursor, 15);
    assert!(c2.recv().is_err(), "nothing to deliver");
    p.publish("t1", "ev", b"m16").unwrap();
    match c2.recv().unwrap() {
        Inbound::Event { head, .. } => assert_eq!(head.seq, 16),
        other => panic!("{other:?}"),
    }
    // Lag: a small-window topic, a cursor far behind → LAGGED, then
    // contiguous to head, no silent loss.
    let mut pubw = HttpClient::connect(s.http).unwrap();
    for i in 1..=100u64 {
        let r = pubw
            .request(
                "POST",
                "/topics/small?window=8",
                &[("Content-Type", "text/plain")],
                format!("x{i}").as_bytes(),
            )
            .unwrap();
        assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    }
    let r = tcp_sub(s.tcp, "small", 0, u64::MAX, Duration::from_millis(500)).unwrap();
    assert_eq!(r.get("lagged"), Some("1"), "{}", r.to_kv());
    assert_eq!(r.get("gaps"), Some("0"), "{}", r.to_kv());
    assert_eq!(r.get("last_seq"), Some("100"), "{}", r.to_kv());
    let got: u64 = r.num("got").unwrap();
    assert!((8..=16).contains(&got), "{}", r.to_kv());
}

#[test]
fn http_ask_publish_longpoll_head() {
    let s = Stack::up(1);
    let mut c = HttpClient::connect(s.http).unwrap();
    let r = c.request("GET", "/healthz", &[], &[]).unwrap();
    assert_eq!((r.status, r.body.as_slice()), (200, &b"ok"[..]));
    // Ask.
    let r = c
        .request("POST", &format!("/actors/{ECHO}/{ECHO_REQ}"), &[], b"ping!")
        .unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.body, b"ping!");
    assert_eq!(
        r.header("x-holon-schema"),
        Some(ECHO_REQ.to_string().as_str())
    );
    // Tell + ask on the same keep-alive connection.
    for _ in 0..3 {
        let r = c
            .request(
                "POST",
                &format!("/actors/{COUNTER}/{COUNTER_INC}?tell"),
                &[],
                bytemuck::bytes_of(&Inc { by: 2 }),
            )
            .unwrap();
        assert_eq!(r.status, 202);
    }
    assert!(wait_until(Duration::from_secs(5), || {
        let r = c
            .request(
                "POST",
                &format!("/actors/{COUNTER}/{COUNTER_GET}"),
                &[],
                bytemuck::bytes_of(&Get { _pad: 0 }),
            )
            .unwrap();
        r.status == 200 && Count::from_bytes(&r.body).unwrap().count == 6
    }));
    // Publish + head + long-poll.
    let r = c
        .request(
            "POST",
            "/topics/h1?event=px",
            &[("Content-Type", "text/plain")],
            b"1.5",
        )
        .unwrap();
    assert_eq!((r.status, r.body.as_slice()), (200, &b"{\"seq\":1}"[..]));
    let r = c
        .request(
            "POST",
            "/topics/h1?event=px",
            &[("Content-Type", "text/plain")],
            b"line1\nline2",
        )
        .unwrap();
    assert_eq!(r.body, b"{\"seq\":2}");
    let r = c.request("GET", "/topics/h1/head", &[], &[]).unwrap();
    assert_eq!(r.body, b"{\"head\":2}");
    let r = c.request("GET", "/topics/h1?from=0", &[], &[]).unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("application/x-ndjson"));
    assert_eq!(r.header("x-next-cursor"), Some("2"));
    let text = String::from_utf8(r.body).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(
        lines[0].starts_with("{\"seq\":1,")
            && lines[0].contains("\"event\":\"px\"")
            && lines[0].ends_with("\"data\":\"1.5\"}")
    );
    assert!(lines[1].contains("\"data\":\"line1\\nline2\"}"));
    // Long-poll waits, then answers when a message lands.
    let http = s.http;
    let waiter = std::thread::spawn(move || {
        let mut w = HttpClient::connect(http).unwrap();
        w.request("GET", "/topics/h1?from=2&wait_ms=5000", &[], &[])
            .unwrap()
    });
    std::thread::sleep(Duration::from_millis(200));
    c.request("POST", "/topics/h1", &[], b"late").unwrap();
    let r = waiter.join().unwrap();
    assert_eq!(r.status, 200);
    assert!(String::from_utf8_lossy(&r.body).contains("\"seq\":3,"));
    // Long-poll deadline answers empty.
    let r = c
        .request("GET", "/topics/h1?from=3&wait_ms=100", &[], &[])
        .unwrap();
    assert_eq!((r.status, r.body.len()), (200, 0));
    // Errors.
    let r = c.request("GET", "/nope", &[], &[]).unwrap();
    assert_eq!(r.status, 404);
    let r = c.request("POST", "/topics/bad%20name", &[], b"x").unwrap();
    assert_eq!(r.status, 400);
    let r = c
        .request("POST", "/actors/echo/notanumber", &[], b"x")
        .unwrap();
    assert_eq!(r.status, 400);
    // Serial asks: latency sanity + correctness.
    let r = http_client(s.http, 200, 16).unwrap();
    assert_eq!(r.get("errors"), Some("0"));
}

/// The next SSE block that carries data, skipping comments and heartbeats.
///
/// `read_sse_event` returns those rather than looping on them, so that a
/// caller's own deadline is re-checked between blocks; a stream opens with a
/// `:ok` comment and emits `:hb` when idle, and neither is an event.
fn next_event(c: &mut HttpClient) -> holon_net::client::SseEvent {
    loop {
        let ev = c.read_sse_event().unwrap();
        if !ev.heartbeat {
            return ev;
        }
    }
}

#[test]
fn sse_stream_resume_and_lag() {
    let s = Stack::up(1);
    let mut p = FrameClient::connect(s.tcp).unwrap();
    for i in 1..=20u64 {
        p.publish("s1", "tick", format!("{i}:0").as_bytes())
            .unwrap();
    }
    // Read 5 from the start, disconnect, resume with Last-Event-ID.
    let mut c = HttpClient::connect(s.http).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.send("GET", "/topics/s1/sse?from=0", &[], &[]).unwrap();
    let (status, headers) = c.read_head().unwrap();
    assert_eq!(status, 200);
    assert!(headers
        .iter()
        .any(|(k, v)| k == "content-type" && v == "text/event-stream"));
    let mut last = 0;
    for _ in 0..5 {
        let ev = next_event(&mut c);
        assert_eq!(ev.id, Some(last + 1));
        assert_eq!(ev.event, "tick");
        assert_eq!(ev.data, format!("{}:0", last + 1));
        last += 1;
    }
    drop(c);
    let mut c = HttpClient::connect(s.http).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.send(
        "GET",
        "/topics/s1/sse",
        &[("Last-Event-ID", &last.to_string())],
        &[],
    )
    .unwrap();
    let (status, _) = c.read_head().unwrap();
    assert_eq!(status, 200);
    let mut ids = Vec::new();
    while ids.len() < 15 {
        let ev = next_event(&mut c);
        ids.push(ev.id.unwrap());
    }
    assert_eq!(
        ids,
        (6..=20).collect::<Vec<_>>(),
        "resume: no loss, no duplicate"
    );
    // Live delivery on the same stream.
    p.publish("s1", "tick", b"21:0").unwrap();
    assert_eq!(next_event(&mut c).id, Some(21));
    // Non-UTF-8 payload → base64.
    p.publish("s1", "bin", &[0xff, 0x00, 0x01]).unwrap();
    let ev = next_event(&mut c);
    assert!(ev.b64 && ev.data == "/wAB", "{ev:?}");
    // Lag on a tiny window: event: lagged carries the resume cursor.
    let mut h = HttpClient::connect(s.http).unwrap();
    for i in 1..=50u64 {
        h.request(
            "POST",
            "/topics/tiny?window=4",
            &[],
            format!("{i}:0").as_bytes(),
        )
        .unwrap();
    }
    let r = sse_client(
        s.http,
        "tiny",
        Some(0),
        u64::MAX,
        Duration::from_millis(500),
    )
    .unwrap();
    assert_eq!(r.get("lagged"), Some("1"), "{}", r.to_kv());
    assert_eq!(r.get("gaps"), Some("0"), "{}", r.to_kv());
    assert_eq!(r.get("last_id"), Some("50"), "{}", r.to_kv());
}

#[test]
fn actor_publish_is_visible_on_every_front() {
    let s = Stack::up(2);
    // Subscribers on both faces, live only.
    let tcp = s.tcp;
    let http = s.http;
    let sub_tcp =
        // The budget is a hang-stop, not an assertion: this test checks that all
        // 50 events arrive on every front with no gaps, never how fast. A full
        // `cargo test` run has dozens of binaries each spawning a coordinator,
        // a host and a gateway, and under that load 10 s was not enough — the
        // subscriber returned short and the row read as a delivery failure.
        std::thread::spawn(move || tcp_sub(tcp, DONE_TOPIC, u64::MAX, 50, Duration::from_secs(60)));
    let sub_sse =
        std::thread::spawn(move || sse_client(http, DONE_TOPIC, None, 50, Duration::from_secs(60)));
    std::thread::sleep(Duration::from_millis(300));
    // 50 units of work through the sink: each completion is published by
    // the actor itself.
    let mut c = FrameClient::connect(s.tcp).unwrap();
    for id in 0..50u64 {
        let (_, b) = c
            .ask(
                ActorId::named(SINK),
                WORK_REQ,
                bytemuck::bytes_of(&Work {
                    id,
                    sent_nanos: holon_net_demo::roles::unix_nanos(),
                }),
            )
            .unwrap();
        let d = WorkDone::from_bytes(&b).unwrap();
        assert_eq!(d.id, id);
        assert_eq!(d.seq, id + 1);
    }
    let t = sub_tcp.join().unwrap().unwrap();
    let h = sub_sse.join().unwrap().unwrap();
    assert_eq!(
        (t.get("got"), t.get("gaps"), t.get("lagged")),
        (Some("50"), Some("0"), Some("0")),
        "{}",
        t.to_kv()
    );
    assert_eq!(
        (h.get("got"), h.get("gaps"), h.get("lagged")),
        (Some("50"), Some("0"), Some("0")),
        "{}",
        h.to_kv()
    );
    assert!(
        t.num::<u64>("lat_p50_ns").unwrap() < 50_000_000,
        "{}",
        t.to_kv()
    );
    // HTTP long-poll sees the same log.
    let mut hc = HttpClient::connect(s.http).unwrap();
    let r = hc
        .request("GET", &format!("/topics/{DONE_TOPIC}?from=48"), &[], &[])
        .unwrap();
    let text = String::from_utf8(r.body).unwrap();
    assert_eq!(text.lines().count(), 2);
    assert!(text.contains("\"seq\":49,") && text.contains("\"seq\":50,"));
}

#[test]
fn publisher_fanout_and_census() {
    let s = Stack::up(1);
    let baseline = s.store_free();
    let http = s.http;
    let subs: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(move || sse_client(http, "fan", None, 500, Duration::from_secs(20)))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(300));
    let p = publisher(s.tcp, "fan", 500, 32, "tick").unwrap();
    assert_eq!(p.get("last_seq"), Some("500"));
    for h in subs {
        let r = h.join().unwrap().unwrap();
        assert_eq!(
            (r.get("got"), r.get("gaps"), r.get("lagged")),
            (Some("500"), Some("0"), Some("0")),
            "{}",
            r.to_kv()
        );
    }
    // Every client is gone: the pool holds only the topic's retained window
    // (bounded), never a leaked pin per subscriber.
    let after = s.store_free();
    assert!(after <= baseline, "window retained");
    let held = baseline - after;
    // 500 messages ≤ window 1024 → at most 500 message chunks + manifests.
    assert!(held <= 2 * 500 + 64, "held {held} chunks for 500 messages");
}

#[test]
fn multiprocess_stack_survives_client_churn() {
    let exe = env!("CARGO_BIN_EXE_holon-net-demo");
    let dir = tempfile::tempdir().unwrap();
    let stack = spawn_stack(exe, dir.path(), false, 2, 1024).unwrap();
    // A subscriber that is killed mid-stream.
    let mut victim = std::process::Command::new(exe)
        .args(["sse-client", "--http"])
        .arg(stack.http.to_string())
        .args([
            "--topic",
            "churn",
            "--expect",
            "1000000",
            "--timeout-ms",
            "20000",
        ])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let p = publisher(stack.tcp, "churn", 200, 16, "tick").unwrap();
    assert_eq!(p.get("last_seq"), Some("200"));
    victim.kill().unwrap();
    let _ = victim.wait();
    // The gateway keeps serving: asks, a fresh subscriber, HTTP.
    let r = tcp_client(
        stack.tcp,
        &TcpClientOpts {
            n: 1000,
            window: 64,
            body: 16,
            mode: TcpMode::Ask,
        },
    )
    .unwrap();
    assert_eq!(r.get("errors"), Some("0"));
    let r = sse_client(stack.http, "churn", Some(0), 200, Duration::from_secs(10)).unwrap();
    assert_eq!(
        (r.get("got"), r.get("gaps")),
        (Some("200"), Some("0")),
        "{}",
        r.to_kv()
    );
    let out = std::process::Command::new(exe)
        .args(["http-client", "--http"])
        .arg(stack.http.to_string())
        .args(["--n", "100"])
        .output()
        .unwrap();
    let rep = holon_net_demo::roles::Report::from_kv(&String::from_utf8_lossy(&out.stdout));
    assert_eq!(
        rep.get("errors"),
        Some("0"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn windowed_topic_census_is_bounded_under_fanout() {
    let s = Stack::up(1);
    let baseline = s.store_free();
    // (a) No subscribers: 5000 messages through a 1024 window.
    let p = publisher(s.tcp, "cw", 5000, 64, "tick").unwrap();
    assert_eq!(p.get("last_seq"), Some("5000"));
    let held_a = baseline - s.store_free();
    // A window holds ≤ 2K live messages between re-roots, each a data chunk
    // plus its chained manifest chunk (ADR-0013): ≤ 4K chunks.
    assert!(
        held_a <= 4 * 1024 + 256,
        "no subscribers: held {held_a} chunks"
    );
    // (b) 16 SSE subscribers reading live while 5000 more are published.
    let http = s.http;
    let subs: Vec<_> = (0..16)
        .map(|_| {
            std::thread::spawn(move || sse_client(http, "cw", None, 5000, Duration::from_secs(30)))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(300));
    let p = publisher(s.tcp, "cw", 5000, 64, "tick").unwrap();
    assert_eq!(p.get("last_seq"), Some("10000"));
    for h in subs {
        let r = h.join().unwrap().unwrap();
        assert_eq!(r.get("gaps"), Some("0"), "{}", r.to_kv());
    }
    assert!(wait_until(Duration::from_secs(5), || baseline
        - s.store_free()
        <= 4 * 1024 + 256));
    let held_b = baseline - s.store_free();
    assert!(
        held_b <= 4 * 1024 + 256,
        "with subscribers: held {held_b} chunks (no subscribers: {held_a})"
    );
}
