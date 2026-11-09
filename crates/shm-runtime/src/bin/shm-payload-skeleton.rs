//! The dedicated-payload-segment walking-skeleton binary: a large object
//! written by one OS process and read, byte for byte, by another.
//!
//! Roles (the coordinator runs **in-process** inside the integration test so it
//! can census `payload_segments` and drive the sweep; these children are
//! separate OS processes, so the fd really does cross a process boundary via
//! `SCM_RIGHTS` and the writer really is gone by the time the reader maps):
//!
//! ```text
//! shm-payload-skeleton writer --uds <path> --bytes <n> --result <file>
//! shm-payload-skeleton reader --uds <path> --seg-id <id> --offset <o> --len <l>
//!                             --generation <g> --checksum <c> --result <file>
//!                             [--expect-refused]
//! ```
//!
//! - **writer** asks for a dedicated segment, lays the one-class/one-chunk pool
//!   a large object carries, fills the chunk with a checkable pattern, publishes
//!   it and takes the `+1` an installed version's manifest holds, then
//!   `install`s the segment and **exits**. The object outlives it.
//! - **reader** maps that segment by id — the fd it never had — attaches the
//!   pool, validates the descriptor's generation, pins the chunk and verifies
//!   every byte against the same pattern and checksum. With `--expect-refused`
//!   it asserts the opposite: an id whose segment is gone (or was never granted)
//!   is refused, not answered with a stale mapping.

use std::sync::Arc;
use std::time::Duration;

use shm_arrow::SchemaRegistry;
use shm_core::{ChunkDesc, Pool, PoolConfig, SizeClass};
use shm_runtime::Node;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let role = args.get(1).map(String::as_str).unwrap_or("");
    let opts = Opts::parse(&args);
    let code = match role {
        "writer" => run_writer(&opts),
        "reader" => run_reader(&opts),
        other => {
            eprintln!("unknown role {other:?}; expected writer|reader");
            2
        }
    };
    std::process::exit(code);
}

/// The byte at index `i` of the object. Index-dependent and non-repeating over
/// any window a truncation or a misaligned base could survive: a reader that
/// resolved the descriptor against the wrong segment, or read from the wrong
/// offset, cannot match it.
fn pattern_byte(i: usize) -> u8 {
    ((i as u64).wrapping_mul(2_654_435_761) >> 13) as u8
}

/// FNV-1a over the object's bytes, so the reader proves it read *these* bytes
/// rather than merely bytes that satisfy [`pattern_byte`].
fn checksum(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The one-class/one-chunk pool a large object's dedicated segment carries.
///
/// A real [`Pool`], deliberately: it reuses the control state machine, the
/// recycle generations and the journal kinds unchanged, so an object in its own
/// segment is reclaimed by the same machinery as one in the shared arena.
fn one_chunk_pool(chunk_size: u32) -> PoolConfig {
    PoolConfig {
        classes: vec![SizeClass {
            chunk_size,
            chunk_count: 1,
        }],
    }
}

struct Opts {
    uds: String,
    bytes: u32,
    seg_id: u32,
    offset: u32,
    len: u32,
    generation: u32,
    checksum: u64,
    expect_refused: bool,
    result: Option<String>,
}

impl Opts {
    fn parse(args: &[String]) -> Opts {
        let mut o = Opts {
            uds: String::new(),
            bytes: 0,
            seg_id: 0,
            offset: 0,
            len: 0,
            generation: 0,
            checksum: 0,
            expect_refused: false,
            result: None,
        };
        let mut i = 2;
        while i < args.len() {
            let val = |i: usize| args.get(i + 1).cloned().unwrap_or_default();
            match args[i].as_str() {
                "--uds" => {
                    o.uds = val(i);
                    i += 2;
                }
                "--bytes" => {
                    o.bytes = val(i).parse().unwrap_or(0);
                    i += 2;
                }
                "--seg-id" => {
                    o.seg_id = val(i).parse().unwrap_or(0);
                    i += 2;
                }
                "--offset" => {
                    o.offset = val(i).parse().unwrap_or(0);
                    i += 2;
                }
                "--len" => {
                    o.len = val(i).parse().unwrap_or(0);
                    i += 2;
                }
                "--generation" => {
                    o.generation = val(i).parse().unwrap_or(0);
                    i += 2;
                }
                "--checksum" => {
                    o.checksum = val(i).parse().unwrap_or(0);
                    i += 2;
                }
                "--expect-refused" => {
                    o.expect_refused = true;
                    i += 1;
                }
                "--result" => {
                    o.result = Some(val(i));
                    i += 2;
                }
                _ => i += 1,
            }
        }
        o
    }
}

fn write_result(opts: &Opts, msg: &str) {
    if let Some(path) = &opts.result {
        let _ = std::fs::write(path, msg);
    }
}

fn run_writer(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "payload-writer", Arc::new(SchemaRegistry::new()))
    {
        Ok(n) => n,
        Err(e) => {
            eprintln!("writer connect failed: {e}");
            return 1;
        }
    };
    // Heartbeat while filling: until the install, the segment's lifetime is tied
    // to this actor's liveness, so a lease expiry mid-write would reclaim the
    // very object we are writing.
    node.start_heartbeat(Duration::from_millis(150));

    let chunk = opts.bytes;
    let seg_bytes = shm_core::required_bytes(&one_chunk_pool(chunk)) as u64;
    let seg = match node.create_payload_segment(seg_bytes) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("writer create_payload_segment failed: {e}");
            return 1;
        }
    };
    let seg_id = seg.id();

    let pool = match Pool::create(&seg, &one_chunk_pool(chunk)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("writer lay pool failed: {e}");
            return 1;
        }
    };
    let desc = match pool.alloc(chunk) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("writer alloc failed: {e}");
            return 1;
        }
    };
    let ctrl = pool.ctrl(&desc).expect("ctrl");
    if let Err(e) = ctrl.try_loan(node.actor_id()) {
        eprintln!("writer loan failed: {e}");
        return 1;
    }

    // Fill the chunk in place — one contiguous span, no staging copy.
    // SAFETY: the chunk is exclusively loaned to this actor and `desc.len`
    // bytes at `desc.offset` are inside the segment we just laid the pool in.
    let body = unsafe {
        std::slice::from_raw_parts_mut(
            seg.payload_ptr().add(desc.offset as usize),
            desc.len as usize,
        )
    };
    for (i, b) in body.iter_mut().enumerate() {
        *b = pattern_byte(i);
    }
    let sum = checksum(body);

    if let Err(e) = ctrl.publish() {
        eprintln!("writer publish failed: {e}");
        return 1;
    }
    // The `+1` an installed version's manifest holds on the object's chunk. It,
    // and not this process, is what keeps the segment alive from here.
    if let Err(e) = ctrl.borrow_shared() {
        eprintln!("writer borrow_shared failed: {e}");
        return 1;
    }
    if ctrl.owner_release() {
        eprintln!("writer: handing off ownership recycled the chunk");
        return 1;
    }

    // Close the staging window: the object is installed, so the chunk's control
    // word governs the segment's lifetime and this process may leave.
    if let Err(e) = node.install_payload_segment(seg_id) {
        eprintln!("writer install failed: {e}");
        return 1;
    }

    write_result(
        opts,
        &format!(
            "OK {seg_id} {} {} {} {sum}",
            desc.offset, desc.len, desc.generation
        ),
    );
    // Exit *without* a goodbye and without releasing: the reader must be able to
    // map this object with its writer already dead.
    0
}

fn run_reader(opts: &Opts) -> i32 {
    let mut node = match Node::connect(&opts.uds, "payload-reader", Arc::new(SchemaRegistry::new()))
    {
        Ok(n) => n,
        Err(e) => {
            eprintln!("reader connect failed: {e}");
            return 1;
        }
    };
    node.start_heartbeat(Duration::from_millis(150));

    let mapped = node.map_payload_segment(opts.seg_id);

    if opts.expect_refused {
        return match mapped {
            Ok(_) => {
                eprintln!("reader: a segment that is gone was mapped anyway");
                write_result(opts, "MAPPED");
                1
            }
            Err(e) => {
                write_result(opts, &format!("REFUSED {e}"));
                0
            }
        };
    }

    let seg = match mapped {
        Ok(s) => s,
        Err(e) => {
            eprintln!("reader map_payload_segment({}) failed: {e}", opts.seg_id);
            write_result(opts, &format!("MAP-FAILED {e}"));
            return 1;
        }
    };
    if seg.id() != opts.seg_id {
        eprintln!("reader: mapped segment {} not {}", seg.id(), opts.seg_id);
        return 1;
    }

    // The pool lives in the segment, so the reader learns the object's geometry
    // from the object itself rather than being told it.
    let pool = match Pool::attach(&seg) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("reader attach pool failed: {e}");
            return 1;
        }
    };
    let desc = ChunkDesc {
        segment_id: opts.seg_id,
        generation: opts.generation,
        offset: opts.offset,
        len: opts.len,
        schema_id: 0,
        _pad: 0,
    };
    match pool.desc_by_index(0) {
        Ok(d) if d.offset == desc.offset && d.len == desc.len => {}
        Ok(d) => {
            eprintln!("reader: pool describes {d:?}, writer reported {desc:?}");
            return 1;
        }
        Err(e) => {
            eprintln!("reader desc_by_index failed: {e}");
            return 1;
        }
    }
    // `ctrl` refuses a descriptor naming another segment, and `validate` refuses
    // a stale generation — so this is also the check that the id we mapped is
    // the object the writer described, not a successor.
    let ctrl = match pool.ctrl(&desc) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("reader ctrl failed: {e}");
            return 1;
        }
    };
    if let Err(e) = ctrl.validate(&desc) {
        eprintln!("reader: descriptor did not validate: {e}");
        return 1;
    }
    if ctrl.state() != shm_core::PUBLISHED {
        eprintln!("reader: object is not published (state {})", ctrl.state());
        return 1;
    }
    // Pin it before reading, exactly as a reader of a shared-pool chunk does.
    if let Err(e) = ctrl.borrow_shared() {
        eprintln!("reader borrow_shared failed: {e}");
        return 1;
    }
    if ctrl.refcount() != 2 {
        eprintln!(
            "reader: refcount {} with the manifest's +1 and this pin",
            ctrl.refcount()
        );
        return 1;
    }

    // SAFETY: the chunk is PUBLISHED and pinned by this reader, and `desc.len`
    // bytes at `desc.offset` are inside the mapped segment (the pool that
    // describes them lives in it).
    let body = unsafe {
        std::slice::from_raw_parts(
            seg.payload_ptr().add(desc.offset as usize) as *const u8,
            desc.len as usize,
        )
    };
    if let Some(i) = (0..body.len()).find(|i| body[*i] != pattern_byte(*i)) {
        eprintln!(
            "reader: byte {i} is {:#04x}, expected {:#04x}",
            body[i],
            pattern_byte(i)
        );
        return 1;
    }
    let sum = checksum(body);
    if sum != opts.checksum {
        eprintln!("reader: checksum {sum} != writer's {}", opts.checksum);
        return 1;
    }

    // Drop the read pin; the manifest's `+1` still holds the object.
    if ctrl.release_shared() {
        eprintln!("reader: dropping a read pin recycled the object");
        return 1;
    }

    write_result(opts, &format!("OK {} {sum}", body.len()));
    0
}
