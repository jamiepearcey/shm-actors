//! `GET /objects/{key}` end to end: a coordinator, a gateway shard and a
//! separate writer node, over the real keyed store.
//!
//! What is worth proving about a streamed body, in order of how badly it would
//! hurt to get wrong: it comes out byte-exact when it is many times larger than
//! the write high-water mark; a client that walks away mid-transfer leaves the
//! store census exactly where it found it; a client that stops reading without
//! disconnecting is cut off rather than holding a version forever; an unknown
//! key is 404; and a version the object has moved past is 410, not a silent
//! read of different bytes.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use holon_net::client::HttpClient;
use holon_net::{Gateway, GatewayConfig};
use holon_object::{object_key, ObjectSide, ObjectStore};
use shm_arrow::SchemaRegistry;
use shm_core::{PoolConfig, SizeClass};
use shm_runtime::{Coordinator, Node, RuntimeConfig};

/// Comfortably more than `holon_net::conn::WBUF_HIGH` (1 MiB), and not a round
/// number, so an off-by-a-slice shows up as a length mismatch.
const BIG: usize = (3 << 20) + 7;

static SEG: AtomicU32 = AtomicU32::new(0);

fn seg_base() -> u32 {
    let pid = std::process::id();
    let n = SEG.fetch_add(1, Ordering::Relaxed);
    5_000_000 + ((pid % 500) * 200_000) + n * 20_000
}

/// Arrow lays an object's `data` buffer down contiguously in **one** chunk, so
/// the pool's largest size class is the hard ceiling on object size. 4 MiB
/// leaves room for a body several times the write high-water mark.
fn config() -> RuntimeConfig {
    let mut c = RuntimeConfig::with_seg_base(seg_base());
    c.store_capacity = 256;
    c.store_pool = PoolConfig {
        classes: vec![
            SizeClass {
                chunk_size: 256,
                chunk_count: 2048,
            },
            SizeClass {
                chunk_size: 1024,
                chunk_count: 512,
            },
            SizeClass {
                chunk_size: 8192,
                chunk_count: 128,
            },
            SizeClass {
                chunk_size: 65536,
                chunk_count: 32,
            },
            SizeClass {
                chunk_size: 4 << 20,
                chunk_count: 8,
            },
        ],
    };
    c.store_data_size = 64 << 20;
    c
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

fn payload(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

struct Stack {
    _dir: tempfile::TempDir,
    coord: Coordinator,
    gw: Option<Gateway>,
    uds: PathBuf,
    http: SocketAddr,
}

impl Stack {
    fn up() -> Stack {
        Stack::up_with(holon_net::gateway::DEFAULT_OBJECT_STALL)
    }

    fn up_with(object_stall: Duration) -> Stack {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).expect("coordinator bind");
        coord.start().expect("coordinator start");
        let mut cfg = GatewayConfig::new(&uds, "gw");
        cfg.http = Some("127.0.0.1:0".parse().unwrap());
        cfg.shards = 1;
        cfg.object_stall = object_stall;
        let gw = Gateway::start(cfg).expect("gateway start");
        let http = gw.http_addr().unwrap();
        assert!(
            wait_until(Duration::from_secs(10), || {
                HttpClient::connect(http)
                    .and_then(|mut c| c.request("GET", "/healthz", &[], &[]))
                    .map(|r| r.status == 200)
                    .unwrap_or(false)
            }),
            "gateway never answered /healthz"
        );
        Stack {
            _dir: dir,
            coord,
            gw: Some(gw),
            uds,
            http,
        }
    }

    fn store_free(&self) -> usize {
        self.coord.store_data_free_total().unwrap()
    }

    /// Live pins on `key` at `version` — the leak detector: a transfer that
    /// abandoned its pin leaves this above zero forever.
    fn pins(&self, key: &str, version: u64) -> Option<u32> {
        self.coord.store_entry_pins(&object_key(key), version)
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        if let Some(gw) = self.gw.take() {
            let _ = gw.stop();
        }
    }
}

/// A writer node with its own object store, in a scope.
fn with_writer<R>(uds: &Path, f: impl FnOnce(&ObjectStore<'_>) -> R) -> R {
    let mut node = Node::connect(uds, "writer", Arc::new(SchemaRegistry::new())).expect("connect");
    node.start_heartbeat(Duration::from_millis(150));
    node.open_store().expect("open store");
    let side = ObjectSide::open(&mut node).expect("object side");
    let registry = node.registry().clone();
    node.open_store().expect("open store");
    let store = node.opened_store().expect("store");
    let objects = ObjectStore::new(&store, &side, registry, &node);
    f(&objects)
}

#[test]
fn a_large_object_streams_byte_exactly() {
    let s = Stack::up();
    let data = payload(BIG);
    // A key with a `/` in it: object keys are the whole path tail.
    with_writer(&s.uds, |o| {
        assert_eq!(
            o.put("blobs/big.bin", "application/x-parquet", &data)
                .unwrap(),
            1
        );
    });

    let mut c = HttpClient::connect(s.http).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let r = c
        .request("GET", "/objects/blobs/big.bin", &[], &[])
        .unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("application/x-parquet"));
    assert_eq!(r.header("content-length"), Some(BIG.to_string().as_str()));
    assert_eq!(r.header("etag"), Some("\"1\""));
    assert_eq!(r.body.len(), BIG, "streamed body is short");
    assert!(
        r.body == data,
        "streamed body differs from the stored bytes"
    );

    // Completion released the pin, and the connection is still usable: the
    // response was framed by Content-Length, not by closing.
    assert!(
        wait_until(Duration::from_secs(5), || s.pins("blobs/big.bin", 1)
            == Some(0)),
        "pin still held after a completed transfer: {:?}",
        s.pins("blobs/big.bin", 1)
    );
    let r = c.request("GET", "/healthz", &[], &[]).unwrap();
    assert_eq!((r.status, r.body.as_slice()), (200, &b"ok"[..]));

    // A second version: latest follows it, and the ETag moves with it.
    let v2 = payload(BIG / 3);
    with_writer(&s.uds, |o| {
        assert_eq!(o.put("blobs/big.bin", "text/plain", &v2).unwrap(), 2);
    });
    let r = c
        .request("GET", "/objects/blobs/big.bin", &[], &[])
        .unwrap();
    assert_eq!((r.status, r.header("etag")), (200, Some("\"2\"")));
    assert_eq!(r.header("content-type"), Some("text/plain"));
    assert!(r.body == v2);
    // An explicit `?version=` that matches serves the same bytes.
    let r = c
        .request("GET", "/objects/blobs/big.bin?version=2", &[], &[])
        .unwrap();
    assert_eq!(r.status, 200);
    assert!(r.body == v2);
}

#[test]
fn a_client_that_disconnects_mid_transfer_releases_the_pin() {
    let s = Stack::up();
    let baseline = s.store_free();
    let data = payload(BIG);
    with_writer(&s.uds, |o| {
        assert_eq!(
            o.put("dropme", "application/octet-stream", &data).unwrap(),
            1
        );
    });
    assert!(s.store_free() < baseline, "the object consumed chunks");

    // Read only the head plus a few kilobytes, then walk away. The body is
    // several times the socket buffers, so the gateway is certainly parked
    // mid-transfer with the pin held.
    let mut raw = TcpStream::connect(s.http).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    raw.write_all(b"GET /objects/dropme HTTP/1.1\r\nHost: h\r\n\r\n")
        .unwrap();
    let mut got = 0usize;
    let mut buf = [0u8; 4096];
    while got < 8192 {
        match raw.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => panic!("read failed: {e}"),
        }
    }
    assert!(
        got >= 8192,
        "server sent nothing to disconnect in the middle of"
    );
    // The pin is held for the life of the transfer — which is also the proof
    // that the body is *streamed*: `advance` drops the pin the moment the last
    // byte is queued, so a pin still standing after only 8 KiB have been read
    // means the gateway queued at most one high-water mark of a 3 MiB body and
    // has to resume on a later pass.
    assert!(
        wait_until(Duration::from_secs(5), || s.pins("dropme", 1) == Some(1)),
        "no pin held mid-transfer: {:?}",
        s.pins("dropme", 1)
    );

    drop(raw);

    assert!(
        wait_until(Duration::from_secs(20), || s.pins("dropme", 1) == Some(0)),
        "pin leaked after a mid-transfer disconnect: {:?}",
        s.pins("dropme", 1)
    );

    // And the census returns to exactly where it started: the abandoned
    // transfer holds nothing back from the teardown.
    with_writer(&s.uds, |o| o.delete("dropme").unwrap());
    assert!(
        wait_until(Duration::from_secs(20), || {
            s.coord.sweep_store_tombstones();
            s.store_free() == baseline
        }),
        "store pool {} != baseline {baseline} after the transfer was abandoned",
        s.store_free()
    );
    assert_eq!(s.store_free(), baseline);
}

#[test]
fn an_unknown_key_is_404_and_a_superseded_version_is_410() {
    let s = Stack::up();
    let mut c = HttpClient::connect(s.http).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();

    // Unknown key: 404, and no catalog slot is spent for it.
    let free = s.store_free();
    let r = c.request("GET", "/objects/nothing/here", &[], &[]).unwrap();
    assert_eq!(r.status, 404);
    assert_eq!(s.store_free(), free, "a 404 allocated store chunks");
    assert_eq!(s.pins("nothing/here", 1), None);
    // A key the store would refuse outright is the same 404, not a 500.
    let r = c.request("GET", "/objects/bad%20key", &[], &[]).unwrap();
    assert_eq!(r.status, 404);
    // A method the route does not serve.
    let r = c.request("DELETE", "/objects/nothing", &[], &[]).unwrap();
    assert_eq!(r.status, 405);

    with_writer(&s.uds, |o| {
        assert_eq!(o.put("v", "text/plain", b"one").unwrap(), 1);
        assert_eq!(o.put("v", "text/plain", b"two").unwrap(), 2);
        assert_eq!(o.put("v", "text/plain", b"three").unwrap(), 3);
    });

    // Superseded: gone, never a silent read of different bytes.
    let r = c.request("GET", "/objects/v?version=1", &[], &[]).unwrap();
    assert_eq!(r.status, 410, "{}", String::from_utf8_lossy(&r.body));
    let r = c.request("GET", "/objects/v?version=2", &[], &[]).unwrap();
    assert_eq!(r.status, 410);
    // Current: served.
    let r = c.request("GET", "/objects/v?version=3", &[], &[]).unwrap();
    assert_eq!((r.status, r.body.as_slice()), (200, &b"three"[..]));
    assert_eq!(r.header("etag"), Some("\"3\""));
    // A version this object has not reached is absent, not gone.
    let r = c.request("GET", "/objects/v?version=9", &[], &[]).unwrap();
    assert_eq!(r.status, 404);
    // A malformed version is a client error, not a silent fall back to latest.
    let r = c
        .request("GET", "/objects/v?version=abc", &[], &[])
        .unwrap();
    assert_eq!(r.status, 400);

    // An empty object is a legitimate object: 200 with a zero-length body,
    // completed in the pass that wrote the head.
    with_writer(&s.uds, |o| {
        assert_eq!(o.put("empty", "text/plain", b"").unwrap(), 1);
    });
    let r = c.request("GET", "/objects/empty", &[], &[]).unwrap();
    assert_eq!((r.status, r.body.len()), (200, 0));
    assert_eq!(r.header("content-length"), Some("0"));
    assert_eq!(r.header("etag"), Some("\"1\""));
    assert_eq!(s.pins("empty", 1), Some(0));

    // Every rejection above left the connection usable and nothing pinned.
    let r = c.request("GET", "/healthz", &[], &[]).unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(s.pins("v", 3), Some(0));
}

#[test]
fn a_reader_that_stalls_is_cut_off_and_its_pin_released() {
    // The one release site the other tests do not reach: a client that keeps
    // its socket open but stops reading. Without this the transfer would hold
    // a store version alive for as long as the peer cares to sit there.
    let s = Stack::up_with(Duration::from_millis(250));
    let data = payload(BIG);
    with_writer(&s.uds, |o| {
        assert_eq!(
            o.put("stall", "application/octet-stream", &data).unwrap(),
            1
        );
    });

    let mut raw = TcpStream::connect(s.http).unwrap();
    // Shrink the receive window before asking for anything. Without this the
    // test is a race against the kernel: loopback socket buffers auto-tune into
    // the megabytes, so the whole 3 MiB body can land in the client's buffer
    // plus the gateway's 1 MiB write buffer and the transfer finishes before it
    // can ever stall — which is exactly the ~12% flake this test had.
    {
        use std::os::fd::AsRawFd;
        let want: libc::c_int = 16 << 10;
        // SAFETY: a live socket fd, a correctly sized c_int option value.
        unsafe {
            libc::setsockopt(
                raw.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &want as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    raw.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    raw.write_all(b"GET /objects/stall HTTP/1.1\r\nHost: h\r\n\r\n")
        .unwrap();
    let mut buf = [0u8; 4096];
    let mut got = raw.read(&mut buf).unwrap();
    assert!(got > 0, "no head");
    // Now stop reading. The socket buffers fill, the transfer parks throttled,
    // and its progress clock stops.
    assert!(
        wait_until(Duration::from_secs(10), || s.pins("stall", 1) == Some(0)),
        "a stalled transfer kept its pin: {:?}",
        s.pins("stall", 1)
    );

    // The peer sees the buffered prefix and then EOF — a short body, which
    // `Content-Length` makes unambiguous rather than silently truncated.
    loop {
        match raw.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => break,
        }
    }
    assert!(
        got < BIG,
        "the whole body arrived; the stall budget did nothing"
    );

    // The object itself is untouched: a fresh reader gets all of it.
    let mut c = HttpClient::connect(s.http).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let r = c.request("GET", "/objects/stall", &[], &[]).unwrap();
    assert_eq!(r.status, 200);
    assert!(r.body == data);
    assert!(wait_until(Duration::from_secs(5), || s.pins("stall", 1) == Some(0)));
}

#[test]
fn two_pipelined_gets_in_one_write_are_both_answered() {
    // Both requests arrive in a single `write(2)`, so a single readability
    // event carries them both. The first takes the connection busy while its
    // body streams; the second waits in the read buffer. No further readability
    // is ever coming — the bytes are already here — so if the reactor only
    // enters `dispatch` from a read event, the second request is never answered
    // and the connection and its fd are stranded for the life of the gateway.
    use std::io::{Read, Write};
    let s = Stack::up();
    let data = payload(BIG);
    with_writer(&s.uds, |o| {
        o.put("blobs/a.bin", "application/octet-stream", &data)
            .unwrap();
        o.put("blobs/b.bin", "application/octet-stream", &data)
            .unwrap();
    });

    let mut sock = std::net::TcpStream::connect(s.http).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let both = "GET /objects/blobs/a.bin HTTP/1.1\r\nHost: x\r\n\r\n\
                GET /objects/blobs/b.bin HTTP/1.1\r\nHost: x\r\n\r\n";
    sock.write_all(both.as_bytes()).unwrap();

    // Read until both bodies have arrived, or the read times out.
    let want = 2 * BIG;
    let mut got = Vec::new();
    let mut buf = vec![0u8; 64 << 10];
    while got.len() < want {
        match sock.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    let bodies = got.windows(4).filter(|w| *w == b"\r\n\r\n").count();
    assert!(
        bodies >= 2,
        "only {bodies} response header block(s) arrived: the second pipelined request was never answered"
    );
    assert!(
        got.len() >= want,
        "got {} bytes, want at least {want}: the second body is missing",
        got.len()
    );
}
