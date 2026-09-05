//! The by-reference reply path: an answer the asker reads in place, out of a
//! chunk it allocated, loaned and journaled.
//!
//! Each test ends on the same assertion — the coordinator's store-pool census
//! back at its baseline — because the whole point of the protocol is that the
//! chunk has exactly one owner at every instant, including the instants where
//! something goes wrong.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use holon_actor::{Actor, ActorRef, ActorSystem, Cx};
use holon_core::{ActorId, Envelope, Reply};
use shm_arrow::SchemaRegistry;
use shm_core::BorrowJournal;
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// Request schema: `want` bytes of `fill`, returned by reference.
const BLOB_REQ: u32 = 9001;
/// Reply schema for the blob body.
const BLOB_ROWS: u32 = 9002;

const ACTOR: &str = "blob";

/// An actor whose answer is variable-length and caller-sized: it builds `want`
/// bytes regardless of the budget, so the run loop's cap check — not the
/// handler's good manners — is what these tests exercise.
#[derive(Default)]
struct Blob;

impl Actor for Blob {
    fn accepts() -> &'static [u32] {
        &[BLOB_REQ]
    }

    fn handle(&mut self, _m: &Envelope, body: &[u8], cx: &mut Cx<'_>) -> holon_core::Result<Reply> {
        let want = u32::from_le_bytes(body[0..4].try_into().expect("want")) as usize;
        let fill = body[4];
        assert!(cx.max_reply() > 0, "an ask always names a reply chunk");
        Reply::bytes(BLOB_ROWS, vec![fill; want])
    }
}

fn req(want: u32, fill: u8) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[0..4].copy_from_slice(&want.to_le_bytes());
    b[4] = fill;
    b
}

fn seg_base() -> u32 {
    use std::sync::atomic::AtomicU32;
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    700_000 + ((pid.wrapping_mul(2_654_435_761) ^ nanos) % 1_000_000) as u32 + n * 1_000
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    cond()
}

struct Fixture {
    _dir: tempfile::TempDir,
    uds: std::path::PathBuf,
    coord: Coordinator,
    baseline: usize,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let mut coord =
        Coordinator::bind(&uds, RuntimeConfig::with_seg_base(seg_base())).expect("bind");
    coord.start().expect("start");
    let baseline = coord.store_data_free_total().expect("baseline");
    Fixture {
        _dir: dir,
        uds,
        coord,
        baseline,
    }
}

impl Fixture {
    fn connect(&self, name: &str) -> Node {
        let mut node =
            Node::connect(&self.uds, name, Arc::new(SchemaRegistry::new())).expect("connect");
        node.start_heartbeat(Duration::from_millis(150));
        node
    }

    /// Run a [`Blob`] host on its own thread; the guard stops and joins it.
    fn host(&self) -> Host {
        let stop = Arc::new(AtomicBool::new(false));
        let uds = self.uds.clone();
        let s = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut sys = ActorSystem::connect(&uds, ACTOR).expect("system");
            sys.set_stop(s);
            sys.spawn(ACTOR, Blob).expect("spawn");
            sys.run().expect("run");
        });
        Host {
            stop,
            thread: Some(thread),
        }
    }

    fn settled(&self) -> bool {
        wait_until(Duration::from_secs(5), || {
            self.coord.store_data_free_total() == Some(self.baseline)
        })
    }
}

struct Host {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Host {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

#[test]
fn ask_ref_round_trips_a_body_no_inline_reply_could_carry() {
    let f = fixture();
    let host = f.host();
    let mut node = f.connect("client");
    let actor = ActorRef::new(&mut node, ActorId::named(ACTOR)).expect("ref");

    // 4 000 bytes: twenty times what `MAX_INLINE_BODY` allows.
    let guard = actor
        .ask_ref(BLOB_REQ, &req(4_000, 0x5a), 4_000)
        .expect("ask");
    assert_eq!(guard.schema_id(), BLOB_ROWS);
    assert_eq!(guard.len(), 4_000);
    assert!(!guard.is_empty());
    assert!(guard.bytes().iter().all(|&b| b == 0x5a));

    // The bytes ARE the shared chunk: nothing was copied on the way out.
    let seg = actor.pool().segment();
    let base = seg.base_ptr() as usize;
    let at = guard.bytes().as_ptr() as usize;
    assert!(
        at > base && at + guard.len() <= base + seg.size(),
        "the reply body must point into the store data segment"
    );
    drop(guard);

    // The small POD path is unchanged and shares the protocol.
    let guard = actor
        .ask_ref(BLOB_REQ, &req(16, 7), 192)
        .expect("small ask");
    assert_eq!(guard.len(), 16);
    guard.release();

    drop(host);
    let _ = node.say_bye();
    assert!(
        f.settled(),
        "census {:?} != baseline {}",
        f.coord.store_data_free_total(),
        f.baseline
    );
}

#[test]
fn over_cap_reply_fails_rather_than_truncating() {
    let f = fixture();
    let host = f.host();
    let mut node = f.connect("client");
    let actor = ActorRef::new(&mut node, ActorId::named(ACTOR)).expect("ref");

    // The handler answers with 4 000 bytes into a chunk sized for 192. A
    // truncated answer would decode as a complete one, so the task fails.
    let err = actor
        .ask_ref(BLOB_REQ, &req(4_000, 1), 192)
        .expect_err("must not answer");
    assert!(matches!(err, holon_actor::Error::Failed), "got {err:?}");

    // The ref is still usable afterwards: the failure is per-ask.
    let guard = actor.ask_ref(BLOB_REQ, &req(100, 2), 192).expect("ask");
    assert_eq!(guard.len(), 100);
    guard.release();

    // A cap past what one envelope can describe is refused at ask time.
    let err = actor
        .ask_ref(BLOB_REQ, &req(1, 0), holon_actor::MAX_REPLY_BODY as u32 + 1)
        .expect_err("cap must be refused");
    assert!(
        matches!(err, holon_actor::Error::ReplyTooLarge { cap, .. } if cap == holon_actor::MAX_REPLY_BODY),
        "got {err:?}"
    );

    drop(host);
    let _ = node.say_bye();
    assert!(
        f.settled(),
        "census {:?} != baseline {}",
        f.coord.store_data_free_total(),
        f.baseline
    );
}

#[test]
fn abandoned_ask_releases_its_journal_slot_and_chunk() {
    let f = fixture();
    // No host: the ask stays queued, so abandoning it is the cancel-wins path.
    let mut node = f.connect("client");
    let jseg = node.journal_segment().clone();
    let journal_len = || BorrowJournal::attach(&jseg).expect("journal").len();
    let before = journal_len();
    let actor = ActorRef::new(&mut node, ActorId::named(ACTOR)).expect("ref");

    let pending = actor
        .ask_start_sized(ActorId::named(ACTOR), BLOB_REQ, &req(10, 0), 1_000)
        .expect("start");
    assert_eq!(
        pending.reply_cap(),
        2048 - 64,
        "the class the pool rounded up to"
    );
    assert_eq!(
        journal_len(),
        before + 1,
        "the borrow is on the crash ledger"
    );

    actor.ask_abandon(pending);
    assert_eq!(actor.sweep_orphans(), 0, "cancel won, nothing was parked");
    assert_eq!(actor.orphan_count(), 0);
    assert_eq!(journal_len(), before, "the slot came back");

    // A won pre-claim cancel means no worker will ever claim the task, so the
    // asker frees the request chunk itself. Nothing may be outstanding: this
    // path is reachable from the network edge (a client that disconnects, or an
    // ask that times out), so a chunk left here is a remote pool drain.
    let _ = node.say_bye();
    drop(node);
    assert!(
        wait_until(Duration::from_secs(5), || {
            f.coord.store_data_free_total() == Some(f.baseline)
        }),
        "a cancelled ask must leave nothing outstanding: {:?} vs {}",
        f.coord.store_data_free_total(),
        f.baseline
    );
}

#[test]
fn handler_refuses_a_recycled_reply_chunk() {
    let f = fixture();
    let mut node = f.connect("client");
    let owner = node.actor_id();
    let actor = ActorRef::new(&mut node, ActorId::named(ACTOR)).expect("ref");

    // Submit with nobody listening, then let the coordinator declare this node
    // dead and reclaim its journaled borrow: the chunk is freed and its
    // generation moves under the (not yet started) handler.
    let mut pending = actor
        .ask_start_sized(ActorId::named(ACTOR), BLOB_REQ, &req(64, 3), 192)
        .expect("start");
    let reclaimed = f.coord.force_reclaim(owner).expect("force reclaim");
    assert_eq!(
        reclaimed.len(),
        1,
        "the loaned + journaled reply chunk is exactly what the replay reclaims"
    );

    // Now let a handler claim it. It re-reads the generation, sees it moved,
    // and fails the task instead of writing into a chunk it no longer owns.
    let host = f.host();
    assert!(
        wait_until(Duration::from_secs(5), || matches!(
            actor.ask_poll(&mut pending),
            Err(holon_actor::Error::Failed)
        )),
        "the ask must go terminal Failed, not hang"
    );
    drop(host);

    // Everything is back: the reply chunk by replay, the request chunk by the
    // handler that took the task terminal.
    let _ = node.say_bye();
    drop(node);
    assert!(
        f.settled(),
        "census {:?} != baseline {}",
        f.coord.store_data_free_total(),
        f.baseline
    );
}
