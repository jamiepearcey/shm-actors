//! ADR-0015 gate tests.
//!
//! - [`ask_reply_single_process`] — the functional loop in one process: an
//!   `ActorSystem` running the `Pricer` on one thread, a client `ActorRef` on
//!   another, a real coordinator; a market move (curve v2) is visible to the
//!   next ask with no restart; evict + zero-leak census.
//! - [`crash_multiprocess`] — the thesis: coordinator + `curve-publish` +
//!   `supervisor(pricer --kill-after 50)` + `client --n 500` as separate OS
//!   processes. The pricer `_exit(137)`s mid-handle holding its claim and its
//!   journaled pin; the lease reap redelivers the in-flight ask to the
//!   successor; 0 lost, 0 errored, ≥ 1 restart, replies from both
//!   incarnations, and the store pool back to baseline after quiescence.
//! - [`four_clients_two_pricers`] — concurrent askers over the one mailbox:
//!   a completed task slot is reused by the next submit (LIFO FREE stack), so
//!   the reply must live in the asker-owned reply chunk, never in the slot's
//!   result word; 0 errors, every ask answered, pool back to baseline.
//! - [`two_actors_one_mailbox_multiprocess`] — routing by `to`: every pricer
//!   process hosts `pricer` and `risk`; 4 clients alternate asks between the
//!   two over the one mailbox; every risk reply is verified against the
//!   closed-form DV01; 0 errors, exact split, pool back to baseline.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use holon_actor::{ActorRef, ActorSystem};
use holon_core::{ActorId, Payload};
use holon_demo::orchestrate::{
    crash_scenario, publish_curve, run_client, spawn_pricer, unique_seg_base, wait_until,
    ClientOpts, PricerOpts,
};
use holon_demo::roles::publish_curve_with;
use holon_demo::{
    curve_batch, expected_dv01, interpolate, price, PriceReply, PriceRequest, Pricer, Risk,
    RiskReply, RiskRequest, CURVE_KEY, PRICER_NAME, RISK_NAME,
};
use shm_arrow::SchemaRegistry;
use shm_runtime::{Coordinator, Node, RuntimeConfig};

fn connect(uds: &std::path::Path, name: &str) -> Node {
    let mut node = Node::connect(uds, name, Arc::new(SchemaRegistry::new())).expect("connect");
    node.start_heartbeat(Duration::from_millis(150));
    node
}

#[test]
fn ask_reply_single_process() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind");
    coord.start().expect("start");
    let baseline = coord.store_data_free_total().expect("baseline");

    // Publisher (this thread): curve v1.
    let mut publisher = connect(&uds, "curve-publish");
    assert_eq!(publish_curve_with(&mut publisher, 0.0).unwrap(), 1);
    assert_eq!(coord.store_entry_version(CURVE_KEY), Some(1));

    // Pricer on its own thread.
    let stop = Arc::new(AtomicBool::new(false));
    let pricer = {
        let uds = uds.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut sys = ActorSystem::connect(&uds, PRICER_NAME).expect("system");
            sys.intern_schema(&holon_demo::curve_schema())
                .expect("intern");
            sys.set_stop(stop);
            sys.spawn(PRICER_NAME, Pricer::new(None, None))
                .expect("spawn pricer");
            sys.spawn(RISK_NAME, Risk::new()).expect("spawn risk");
            assert!(
                matches!(
                    sys.spawn(RISK_NAME, Risk::new()),
                    Err(holon_actor::Error::DuplicateActor(_))
                ),
                "a second spawn under the same name is refused"
            );
            assert_eq!(sys.actor_ids().count(), 2);
            sys.run().expect("run");
        })
    };

    // Client (this thread).
    let mut client = connect(&uds, "client");
    let actor = ActorRef::new(&mut client, ActorId::named(PRICER_NAME)).expect("actor ref");
    let v1 = curve_batch(0.0);
    for seq in 0..20u64 {
        let tenor = 0.5 + seq as f64 * 1.3;
        let req = PriceRequest {
            tenor,
            notional: 1_000_000.0,
            seq,
        };
        let reply: PriceReply = actor.ask(&req).expect("ask");
        let expected = price(interpolate(&v1, tenor).unwrap(), tenor, 1_000_000.0);
        assert!(
            (reply.px - expected).abs() < 1e-6,
            "seq {seq}: px {} != {expected}",
            reply.px
        );
        assert_eq!(reply.curve_version, 1);
        assert_eq!(reply.attempt, 0);
        assert_eq!(reply.incarnation, std::process::id());
    }

    // Market move: commit v2 while the pricer is serving. No restart, no
    // rehydrate: the next ask prices off v2.
    assert_eq!(publish_curve_with(&mut publisher, 25.0).unwrap(), 2);
    let v2 = curve_batch(25.0);
    let req = PriceRequest {
        tenor: 7.0,
        notional: 1.0,
        seq: 99,
    };
    let reply: PriceReply = actor.ask(&req).expect("ask after move");
    assert_eq!(reply.curve_version, 2);
    let expected = price(interpolate(&v2, 7.0).unwrap(), 7.0, 1.0);
    assert!((reply.px - expected).abs() < 1e-12);

    // An unknown schema fails the task: the asker sees an error, nothing leaks.
    let err = actor.ask_raw(4242, &[0u8; 8]).unwrap_err();
    assert!(matches!(err, holon_actor::Error::Failed), "got {err:?}");

    // The second actor, over the same mailbox, picked by `to`.
    let risk = ActorRef::new(&mut client, ActorId::named(RISK_NAME)).expect("risk ref");
    for seq in 0..10u64 {
        let tenor = 1.0 + seq as f64 * 2.5;
        let rep: RiskReply = risk
            .ask(&RiskRequest {
                tenor,
                notional: 1_000_000.0,
                seq,
            })
            .expect("risk ask");
        let px = price(interpolate(&v2, tenor).unwrap(), tenor, 1_000_000.0);
        assert!((rep.px - px).abs() < 1e-6, "risk px {} != {px}", rep.px);
        assert!(
            (rep.dv01 - expected_dv01(px, tenor)).abs() < 1e-9 * px,
            "dv01 {} != {}",
            rep.dv01,
            expected_dv01(px, tenor)
        );
        assert_eq!(rep.curve_version, 2);
        assert_eq!(rep.incarnation, std::process::id());
    }
    // Routing is by `to`, not by schema: the pricer's schema sent to `risk`
    // is refused by `risk`'s own table …
    let err = risk
        .ask_raw(PriceRequest::SCHEMA_ID, &[0u8; 24])
        .unwrap_err();
    assert!(matches!(err, holon_actor::Error::Failed), "got {err:?}");
    // … and an actor nobody hosts fails the ask rather than hanging it.
    let nobody = ActorRef::new(&mut client, ActorId::named("nobody")).expect("ref");
    let err = nobody
        .ask_raw(PriceRequest::SCHEMA_ID, &[0u8; 24])
        .unwrap_err();
    assert!(matches!(err, holon_actor::Error::Failed), "got {err:?}");

    // Stop the system: set the flag, nudge it with one more message.
    stop.store(true, Ordering::Release);
    actor.tell(&req).expect("tell");
    pricer.join().expect("pricer thread");

    // Evict + census → baseline (every envelope and reply chunk was freed).
    client.store().unwrap().evict(CURVE_KEY).expect("evict");
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.store_data_free_total() == Some(baseline)
        }),
        "store pool did not return to baseline: {:?} vs {baseline}",
        coord.store_data_free_total()
    );
    let _ = publisher.say_bye();
    let _ = client.say_bye();
}

#[test]
fn crash_multiprocess() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let lease_ms = 500u64;
    let mut config = RuntimeConfig::with_seg_base(unique_seg_base());
    config.lease_deadline = Duration::from_millis(lease_ms);
    let mut coord = Coordinator::bind(&uds, config).expect("bind");
    coord.start().expect("start");
    let baseline = coord.store_data_free_total().expect("baseline");
    let exe = env!("CARGO_BIN_EXE_holon-demo");

    publish_curve(exe, &uds_s, 0.0);
    assert_eq!(coord.store_entry_version(CURVE_KEY), Some(1));
    let with_curve = coord.store_data_free_total().expect("census");

    let n = 500;
    let kill_after = 50;
    let out = crash_scenario(exe, &uds_s, dir.path(), "t", n, kill_after, lease_ms);
    let c = &out.client;
    eprintln!(
        "crash: restarts={} redelivered={} lost={} errors={} replies={} incarnations={:?} kill→first={:?}\n{}",
        out.restarts,
        c.redelivered,
        c.lost(),
        c.errors,
        c.replies,
        c.incarnations,
        out.kill_to_first_reply(),
        out.supervisor_log
    );
    assert_eq!(c.asks, n);
    assert_eq!(c.errors, 0, "no ask may error");
    assert_eq!(c.lost(), 0, "no ask may be lost");
    assert_eq!(c.replies, n, "every ask was answered");
    assert!(out.restarts >= 1, "the supervisor restarted the pricer");
    assert_eq!(
        c.incarnations.len(),
        2,
        "replies came from exactly two incarnations: {:?}",
        c.incarnations
    );
    assert_eq!(
        c.incarnations[0].1,
        kill_after - 1,
        "the first incarnation answered every ask before the one it died on"
    );
    assert_eq!(
        c.incarnations[1].1,
        n - (kill_after - 1),
        "the successor answered the redelivered ask and every one after it"
    );
    assert_eq!(
        c.redelivered, 1,
        "exactly the in-flight ask was redelivered"
    );
    let k2f = out
        .kill_to_first_reply()
        .expect("kill and first successor reply both observed");
    assert!(
        k2f >= Duration::from_millis(lease_ms / 2) && k2f < Duration::from_secs(5),
        "kill→first successor reply {k2f:?} is outside the lease-bounded window"
    );

    // Census 1: with every child gone, the dead pricer's journaled pin and its
    // in-flight envelope have been reclaimed; the pool is back to the
    // with-curve count.
    assert!(
        wait_until(Duration::from_secs(10), || {
            coord.store_data_free_total() == Some(with_curve)
        }),
        "post-crash census {:?} != with-curve baseline {with_curve}",
        coord.store_data_free_total()
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.store_entry_pins(CURVE_KEY, 1) == Some(0)
        }),
        "curve v1 pins not back to zero: {:?}",
        coord.store_entry_pins(CURVE_KEY, 1)
    );

    // Census 2: evict the cell → the empty baseline.
    {
        let mut node = connect(&uds, "evictor");
        node.store().unwrap().evict(CURVE_KEY).expect("evict");
        let _ = node.say_bye();
    }
    assert!(
        wait_until(Duration::from_secs(10), || {
            coord.store_data_free_total() == Some(baseline)
        }),
        "final census {:?} != empty baseline {baseline}",
        coord.store_data_free_total()
    );
    assert_eq!(coord.store_entry_version(CURVE_KEY), None);
}

#[test]
fn four_clients_two_pricers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind");
    coord.start().expect("start");
    let exe = env!("CARGO_BIN_EXE_holon-demo");
    publish_curve(exe, &uds_s, 0.0);
    let with_curve = coord.store_data_free_total().expect("census");

    let n = 4_000;
    let report = {
        let _pricers: Vec<_> = (0..2)
            .map(|i| {
                spawn_pricer(
                    exe,
                    &uds_s,
                    dir.path(),
                    &format!("p{i}"),
                    &PricerOpts::default(),
                )
            })
            .collect();
        run_client(
            exe,
            &uds_s,
            dir.path(),
            "four",
            &ClientOpts {
                clients: 4,
                ..ClientOpts::parked(n)
            },
        )
    };
    assert_eq!(report.asks, n);
    assert_eq!(report.errors, 0, "no ask may error under concurrent askers");
    assert_eq!(report.replies, n, "every ask answered");
    assert_eq!(report.lost(), 0);
    assert_eq!(report.redelivered, 0);
    assert!(
        wait_until(Duration::from_secs(10), || {
            coord.store_data_free_total() == Some(with_curve)
        }),
        "census {:?} != with-curve baseline {with_curve}: reply chunks leaked",
        coord.store_data_free_total()
    );
}

#[test]
fn two_actors_one_mailbox_multiprocess() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();
    let config = RuntimeConfig::with_seg_base(unique_seg_base());
    let mut coord = Coordinator::bind(&uds, config).expect("bind");
    coord.start().expect("start");
    let exe = env!("CARGO_BIN_EXE_holon-demo");
    publish_curve(exe, &uds_s, 0.0);
    let with_curve = coord.store_data_free_total().expect("census");

    // 4 clients × 1000 asks, odd seqs to `risk`: exactly 500 risk asks each.
    let n = 4_000;
    let report = {
        let _pricers: Vec<_> = (0..2)
            .map(|i| {
                spawn_pricer(
                    exe,
                    &uds_s,
                    dir.path(),
                    &format!("mix{i}"),
                    &PricerOpts::default(),
                )
            })
            .collect();
        run_client(
            exe,
            &uds_s,
            dir.path(),
            "mix",
            &ClientOpts {
                clients: 4,
                mix: true,
                ..ClientOpts::parked(n)
            },
        )
    };
    assert_eq!(report.asks, n);
    assert_eq!(
        report.errors, 0,
        "no ask may error or misroute (each risk reply is verified)"
    );
    assert_eq!(report.replies, n, "every ask answered");
    assert_eq!(
        report.risk_replies,
        n / 2,
        "exactly the odd seqs went to `risk`"
    );
    assert_eq!(report.lost(), 0);
    assert_eq!(report.redelivered, 0);
    assert_eq!(
        report.incarnations.len(),
        2,
        "both processes served (each hosts both actors): {:?}",
        report.incarnations
    );
    assert!(
        wait_until(Duration::from_secs(10), || {
            coord.store_data_free_total() == Some(with_curve)
        }),
        "census {:?} != with-curve baseline {with_curve}",
        coord.store_data_free_total()
    );
}
