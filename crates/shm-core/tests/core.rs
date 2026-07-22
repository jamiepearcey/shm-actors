//! Integration tests for the `shm-core` ABI and algorithms.

use core::sync::atomic::{AtomicU32, Ordering};

use shm_core::ctrl::ChunkCtrl;
use shm_core::{
    BorrowJournal, ChunkDesc, Error, PackedRef, Platform, Pool, PoolConfig, PosixPlatform, Segment,
    SharedPod, FREE, LAYOUT_VERSION, LOANED, PUBLISHED, SEGMENT_MAGIC,
};

// ---------------------------------------------------------------------------
// segment-id allocation so parallel tests don't collide on global shm names
// ---------------------------------------------------------------------------

static NEXT: AtomicU32 = AtomicU32::new(0);

fn uid() -> u32 {
    let base = (std::process::id() & 0x7fff) << 8;
    base | (NEXT.fetch_add(1, Ordering::Relaxed) & 0xff)
}

/// Create a fresh segment, best-effort removing any stale leftover first.
fn fresh(size: usize) -> Segment {
    let id = uid();
    // Best-effort: clear a leftover from a previously aborted run.
    let _ = Segment::attach(id).and_then(|s| s.unlink());
    Segment::create(id, size).expect("segment create")
}

// ---------------------------------------------------------------------------
// SharedPod / ABI shape
// ---------------------------------------------------------------------------

#[test]
fn chunkdesc_abi_is_frozen() {
    assert_eq!(core::mem::size_of::<ChunkDesc>(), 24);
    assert_eq!(core::mem::size_of::<ChunkCtrl>(), 16);
    // A downstream type can opt into SharedPod with a bare unsafe impl.
    #[repr(C)]
    #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
    struct Downstream {
        a: u64,
        b: u64,
    }
    unsafe impl SharedPod for Downstream {}
    fn assert_pod<T: SharedPod>() {}
    assert_pod::<ChunkDesc>();
    assert_pod::<Downstream>();
}

#[test]
fn packed_ref_roundtrips_within_widths() {
    let r = PackedRef::pack(0xABCD, 0x1234, 0xDEAD_BEEF);
    assert_eq!(r.unpack(), (0xABCD, 0x1234, 0xDEAD_BEEF));
    assert_eq!(r.segment_id(), 0xABCD);
    assert_eq!(r.generation(), 0x1234);
    assert_eq!(r.offset(), 0xDEAD_BEEF);

    // Generation is stored mod 2^16; the low 16 bits survive.
    let r2 = PackedRef::pack(1, 0x1_0005, 64);
    assert_eq!(r2.generation(), 0x0005);

    let desc = ChunkDesc {
        segment_id: 3,
        generation: 7,
        offset: 128,
        len: 64,
        schema_id: 9,
        _pad: 0,
    };
    let p = PackedRef::from_desc(&desc);
    assert_eq!(p.unpack(), (3, 7, 128));
}

// ---------------------------------------------------------------------------
// Segment: header roundtrip, validation, create->write->attach->read
// ---------------------------------------------------------------------------

#[test]
fn segment_header_roundtrip_and_payload() {
    let seg = fresh(64 * 1024);
    let hdr = seg.header();
    assert_eq!(hdr.magic, SEGMENT_MAGIC);
    assert_eq!(hdr.layout_version, LAYOUT_VERSION);
    assert_eq!(hdr.segment_id, seg.id());
    assert_eq!(hdr.size as usize, seg.size());
    assert_eq!(hdr.generation, 1);

    // Write a POD value through a payload view.
    // SAFETY: single-threaded test, exclusive access to the payload.
    unsafe {
        *seg.view_at_mut::<u64>(0).unwrap() = 0xFEED_FACE_DEAD_BEEF;
        *seg.view_at_mut::<u64>(64).unwrap() = 0x0102_0304_0506_0708;
    }

    // Attach a *second* handle to the same name and read the bytes back.
    let seg2 = Segment::attach(seg.id()).expect("attach");
    // SAFETY: no writer active.
    unsafe {
        assert_eq!(*seg2.view_at::<u64>(0).unwrap(), 0xFEED_FACE_DEAD_BEEF);
        assert_eq!(*seg2.view_at::<u64>(64).unwrap(), 0x0102_0304_0506_0708);
    }

    seg.unlink().unwrap();
}

#[test]
fn segment_view_bounds_and_alignment() {
    let seg = fresh(4096);
    // Out of bounds.
    // SAFETY: read-only view; test checks the error path.
    let oob = unsafe { seg.view_at::<u64>(seg.payload_len()) };
    assert!(matches!(oob, Err(Error::OutOfBounds)));
    // Misaligned: offset 1 for an 8-byte type.
    let mis = unsafe { seg.view_at::<u64>(1) };
    assert!(matches!(mis, Err(Error::Misaligned)));
    seg.unlink().unwrap();
}

#[test]
fn segment_attach_rejects_bad_magic() {
    let seg = fresh(4096);
    let id = seg.id();
    // Corrupt the magic in shared memory.
    // SAFETY: base points at a live mapping; overwriting the first word is fine.
    unsafe {
        *(seg.base_ptr() as *mut u64) = 0;
    }
    let r = Segment::attach(id);
    assert!(matches!(r, Err(Error::LayoutMismatch)));
    seg.unlink().unwrap();
}

// ---------------------------------------------------------------------------
// Pool: alloc/free, exhaustion, Treiber ABA-safety
// ---------------------------------------------------------------------------

fn small_pool_segment() -> Segment {
    fresh(1024 * 1024)
}

#[test]
fn pool_alloc_free_and_exhaustion() {
    let seg = small_pool_segment();
    // Two classes: 64B x 4, 128B x 2.
    let cfg = PoolConfig {
        classes: vec![
            shm_core::SizeClass { chunk_size: 64, chunk_count: 4 },
            shm_core::SizeClass { chunk_size: 128, chunk_count: 2 },
        ],
    };
    let pool = Pool::create(&seg, &cfg).unwrap();
    assert_eq!(pool.total_chunks(), 6);

    // A 100-byte request rounds up to the 128B class (count 2).
    let a = pool.alloc(100).unwrap();
    let b = pool.alloc(100).unwrap();
    assert_eq!(a.len, 128);
    assert_ne!(a.offset, b.offset);
    // Third 128B alloc exhausts that class.
    assert!(matches!(pool.alloc(100), Err(Error::PoolExhausted)));

    // The 64B class is independent and still has chunks.
    let c = pool.alloc(10).unwrap();
    assert_eq!(c.len, 64);

    // Free one 128B chunk, then allocation succeeds again.
    pool.free(&a).unwrap();
    let d = pool.alloc(128).unwrap();
    assert_eq!(d.offset, a.offset);

    // Too-large request has no class.
    assert!(matches!(pool.alloc(4096), Err(Error::LayoutOverflow(_))));

    seg.unlink().unwrap();
}

#[test]
fn pool_treiber_aba_safety() {
    let seg = small_pool_segment();
    let cfg = PoolConfig {
        classes: vec![shm_core::SizeClass { chunk_size: 64, chunk_count: 8 }],
    };
    let pool = Pool::create(&seg, &cfg).unwrap();

    // Allocate every chunk.
    let mut first: Vec<ChunkDesc> = (0..8).map(|_| pool.alloc(64).unwrap()).collect();
    assert!(matches!(pool.alloc(64), Err(Error::PoolExhausted)));
    // All offsets distinct.
    let mut offs: Vec<u32> = first.iter().map(|d| d.offset).collect();
    offs.sort_unstable();
    offs.dedup();
    assert_eq!(offs.len(), 8);

    // Free them all in reverse — this exercises push/pop tag churn.
    while let Some(d) = first.pop() {
        pool.free(&d).unwrap();
    }
    assert_eq!(pool.free_count(0), 8);

    // Re-allocate all and confirm we again get 8 distinct chunks (no ABA
    // duplication would have been possible without the tagged head).
    let again: Vec<ChunkDesc> = (0..8).map(|_| pool.alloc(64).unwrap()).collect();
    let mut offs2: Vec<u32> = again.iter().map(|d| d.offset).collect();
    offs2.sort_unstable();
    offs2.dedup();
    assert_eq!(offs2.len(), 8);
    assert!(matches!(pool.alloc(64), Err(Error::PoolExhausted)));

    seg.unlink().unwrap();
}

// ---------------------------------------------------------------------------
// ChunkCtrl state machine
// ---------------------------------------------------------------------------

#[test]
fn chunkctrl_state_machine() {
    use core::sync::atomic::AtomicU32;
    let ctrl = ChunkCtrl {
        state: AtomicU32::new(FREE),
        refcount: AtomicU32::new(0),
        owner_actor: AtomicU32::new(0),
        generation: AtomicU32::new(0),
    };

    // FREE -> LOANED
    ctrl.try_loan(42).unwrap();
    assert_eq!(ctrl.state(), LOANED);
    // Cannot loan again.
    assert!(matches!(ctrl.try_loan(43), Err(Error::InvalidState)));
    // Cannot borrow_shared while merely LOANED.
    assert!(matches!(ctrl.borrow_shared(), Err(Error::InvalidState)));

    // LOANED -> PUBLISHED
    ctrl.publish().unwrap();
    assert_eq!(ctrl.state(), PUBLISHED);

    // Two shared borrows.
    ctrl.borrow_shared().unwrap();
    ctrl.borrow_shared().unwrap();
    assert_eq!(ctrl.refcount(), 2);

    // Owner releasing while pins remain does NOT reclaim.
    assert!(!ctrl.owner_release());
    assert_eq!(ctrl.state(), PUBLISHED);

    // Release one pin: still published.
    assert!(!ctrl.release_shared());
    assert_eq!(ctrl.state(), PUBLISHED);
    // Release last pin: now owner-released && refcount==0 => reclaim to FREE.
    assert!(ctrl.release_shared());
    assert_eq!(ctrl.state(), FREE);
    assert_eq!(ctrl.generation(), 1); // bumped on recycle
}

#[test]
fn chunkctrl_drop_loan_bumps_generation() {
    use core::sync::atomic::AtomicU32;
    let ctrl = ChunkCtrl {
        state: AtomicU32::new(FREE),
        refcount: AtomicU32::new(0),
        owner_actor: AtomicU32::new(0),
        generation: AtomicU32::new(5),
    };
    ctrl.try_loan(1).unwrap();
    ctrl.drop_loan().unwrap();
    assert_eq!(ctrl.state(), FREE);
    assert_eq!(ctrl.generation(), 6);
}

// ---------------------------------------------------------------------------
// Generation bump makes a stale descriptor fail try_deref
// ---------------------------------------------------------------------------

#[test]
fn stale_descriptor_after_recycle() {
    let seg = small_pool_segment();
    let cfg = PoolConfig {
        classes: vec![shm_core::SizeClass { chunk_size: 64, chunk_count: 2 }],
    };
    let pool = Pool::create(&seg, &cfg).unwrap();

    let desc = pool.alloc(64).unwrap();
    assert_eq!(desc.generation, 0);
    // Loan -> publish -> owner releases with no pins -> reclaim (gen bump).
    let ctrl = pool.ctrl(&desc).unwrap();
    ctrl.try_loan(7).unwrap();
    ctrl.publish().unwrap();
    assert!(ctrl.owner_release()); // reclaimed
    pool.free(&desc).unwrap();

    // The old descriptor is now stale.
    let ctrl_after = pool.ctrl(&desc).unwrap();
    assert!(matches!(ctrl_after.validate(&desc), Err(Error::StaleDescriptor)));

    // A freshly allocated descriptor for the same slot validates fine.
    let desc2 = pool.alloc(64).unwrap();
    assert_eq!(desc2.offset, desc.offset);
    assert_eq!(desc2.generation, 1);
    assert!(pool.ctrl(&desc2).unwrap().validate(&desc2).is_ok());

    seg.unlink().unwrap();
}

// ---------------------------------------------------------------------------
// Borrow journal: record / release / replay / JournalFull
// ---------------------------------------------------------------------------

fn mk_desc(offset: u32) -> ChunkDesc {
    ChunkDesc {
        segment_id: 1,
        generation: 0,
        offset,
        len: 64,
        schema_id: 0,
        _pad: 0,
    }
}

#[test]
fn journal_record_release_replay_and_full() {
    let seg = fresh(64 * 1024);
    let jrn = BorrowJournal::create(&seg, 3).unwrap();
    assert_eq!(jrn.capacity(), 3);
    assert!(jrn.is_empty());

    let s0 = jrn.record(mk_desc(64)).unwrap();
    let s1 = jrn.record(mk_desc(128)).unwrap();
    let s2 = jrn.record(mk_desc(192)).unwrap();
    assert_eq!(jrn.len(), 3);
    assert!(jrn.is_occupied(s0) && jrn.is_occupied(s1) && jrn.is_occupied(s2));

    // Full now => backpressure.
    assert!(matches!(jrn.record(mk_desc(256)), Err(Error::JournalFull)));

    // Replay yields exactly the three pinned descriptors.
    let mut offs: Vec<u32> = jrn.replay().map(|d| d.offset).collect();
    offs.sort_unstable();
    assert_eq!(offs, vec![64, 128, 192]);

    // Release the middle slot, record a new pin (reuses a free slot).
    jrn.release(s1).unwrap();
    assert!(!jrn.is_occupied(s1));
    assert_eq!(jrn.len(), 2);
    let s3 = jrn.record(mk_desc(999)).unwrap();
    assert_eq!(jrn.len(), 3);

    let mut offs2: Vec<u32> = jrn.replay().map(|d| d.offset).collect();
    offs2.sort_unstable();
    assert_eq!(offs2, vec![64, 192, 999]);
    let _ = s3;

    // Attach a second handle and replay identically (POD in shared memory).
    let jrn2 = BorrowJournal::attach(&seg).unwrap();
    let mut offs3: Vec<u32> = jrn2.replay().map(|d| d.offset).collect();
    offs3.sort_unstable();
    assert_eq!(offs3, vec![64, 192, 999]);

    seg.unlink().unwrap();
}

// ---------------------------------------------------------------------------
// Platform seam
// ---------------------------------------------------------------------------

#[test]
fn platform_create_attach_unlink_and_doorbell() {
    let plat = PosixPlatform::new();
    let id = uid();
    let _ = plat.segment_unlink(id); // clear leftovers
    let seg = plat.segment_create(id, 8192).unwrap();
    let seg2 = plat.segment_attach(id).unwrap();
    assert_eq!(seg.id(), seg2.id());
    drop(seg);
    drop(seg2);
    plat.segment_unlink(id).unwrap();

    // Doorbell over a socketpair: signal one side, wait on the other.
    let (a, b) = nix::sys::socket::socketpair(
        nix::sys::socket::AddressFamily::Unix,
        nix::sys::socket::SockType::Stream,
        None,
        nix::sys::socket::SockFlag::empty(),
    )
    .unwrap();
    use std::os::fd::AsRawFd;
    plat.doorbell_signal(a.as_raw_fd()).unwrap();
    plat.doorbell_wait(b.as_raw_fd()).unwrap();

    assert_eq!(plat.death_detection(), shm_core::DeathDetection::LeaseBased);
}
