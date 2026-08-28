//! Integration tests for the `shm-core` ABI and algorithms.

use core::sync::atomic::{AtomicU32, Ordering};

use shm_core::ctrl::ChunkCtrl;
use shm_core::{pack_word, ShmU64, 
    BorrowJournal, ChunkDesc, Error, JournalRecord, PackedRef, Platform, Pool, PoolConfig,
    PosixPlatform, Segment, SharedPod, FREE, LAYOUT_VERSION, LOANED, PUBLISHED, SEGMENT_MAGIC,
};

/// The chunk offsets of the `ChunkPin` records a replay yields (ignores any
/// `ArtifactPin` records), sorted.
fn chunk_offsets(jrn: &BorrowJournal) -> Vec<u32> {
    let mut offs: Vec<u32> = jrn
        .replay()
        .filter_map(|r| match r {
            JournalRecord::ChunkPin(d) => Some(d.offset),
            JournalRecord::ArtifactPin { .. }
            | JournalRecord::WriteLease { .. }
            | JournalRecord::StagedManifest { .. } => None,
        })
        .collect();
    offs.sort_unstable();
    offs
}

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
    // v0.3 (ADR-0003a): PackedRef packs [segment_id:32 | offset:32]; the
    // generation field is dropped. Both halves are lossless over the full u32.
    let r = PackedRef::pack(0xABCD_1234, 0xDEAD_BEEF);
    assert_eq!(r.unpack(), (0xABCD_1234, 0xDEAD_BEEF));
    assert_eq!(r.segment_id(), 0xABCD_1234);
    assert_eq!(r.offset(), 0xDEAD_BEEF);

    // A segment id above the former 2^16 cap now round-trips exactly.
    let r2 = PackedRef::pack(0x0010_0005, 64);
    assert_eq!(r2.segment_id(), 0x0010_0005);
    assert_eq!(r2.offset(), 64);

    let desc = ChunkDesc {
        segment_id: 3,
        generation: 7,
        offset: 128,
        len: 64,
        schema_id: 9,
        _pad: 0,
    };
    let p = PackedRef::from_desc(&desc);
    // `from_desc` carries only `{segment_id, offset}`; `generation` is dropped.
    assert_eq!(p.unpack(), (3, 128));
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
            shm_core::SizeClass {
                chunk_size: 64,
                chunk_count: 4,
            },
            shm_core::SizeClass {
                chunk_size: 128,
                chunk_count: 2,
            },
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
        classes: vec![shm_core::SizeClass {
            chunk_size: 64,
            chunk_count: 8,
        }],
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
    use shm_core::ShmU32;
    let ctrl = ChunkCtrl {
        word: ShmU64::new(pack_word(FREE, 0)),
        owner_actor: ShmU32::new(0),
        generation: ShmU32::new(0),
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
    use shm_core::ShmU32;
    let ctrl = ChunkCtrl {
        word: ShmU64::new(pack_word(FREE, 0)),
        owner_actor: ShmU32::new(0),
        generation: ShmU32::new(5),
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
        classes: vec![shm_core::SizeClass {
            chunk_size: 64,
            chunk_count: 2,
        }],
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
    assert!(matches!(
        ctrl_after.validate(&desc),
        Err(Error::StaleDescriptor)
    ));

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
    assert_eq!(chunk_offsets(&jrn), vec![64, 128, 192]);

    // Release the middle slot, record a new pin (reuses a free slot).
    jrn.release(s1).unwrap();
    assert!(!jrn.is_occupied(s1));
    assert_eq!(jrn.len(), 2);
    let s3 = jrn.record(mk_desc(999)).unwrap();
    assert_eq!(jrn.len(), 3);

    assert_eq!(chunk_offsets(&jrn), vec![64, 192, 999]);
    let _ = s3;

    // Attach a second handle and replay identically (POD in shared memory).
    let jrn2 = BorrowJournal::attach(&seg).unwrap();
    assert_eq!(chunk_offsets(&jrn2), vec![64, 192, 999]);

    seg.unlink().unwrap();
}

// ---------------------------------------------------------------------------
// Borrow journal: tagged entries (item J) — chunk-pin AND artifact-pin kinds
// ---------------------------------------------------------------------------

#[test]
fn journal_tagged_entries_roundtrip_all_kinds() {
    let seg = fresh(64 * 1024);
    let jrn = BorrowJournal::create(&seg, 8).unwrap();

    // Mix all three entry kinds in the same journal (ChunkPin / ArtifactPin /
    // WriteLease — item K adds the third).
    let c0 = jrn.record(mk_desc(64)).unwrap();
    let a0 = jrn
        .record_artifact_pin(/*artifact_id*/ 7, /*incarnation*/ 1, /*version*/ 0x1_0000_0002)
        .unwrap();
    let c1 = jrn.record(mk_desc(128)).unwrap();
    let a1 = jrn.record_artifact_pin(42, 1, 5).unwrap();
    let w0 = jrn
        .record_write_lease(/*artifact_id*/ 9, /*incarnation*/ 1, /*fence*/ 3)
        .unwrap();
    assert_eq!(jrn.len(), 5);
    assert!(
        jrn.is_occupied(c0)
            && jrn.is_occupied(a0)
            && jrn.is_occupied(c1)
            && jrn.is_occupied(a1)
            && jrn.is_occupied(w0)
    );

    // Replay decodes each slot back into its typed record — including a large
    // (> 2^32) version, proving the u64 payload survives the u32-word packing.
    let mut chunks: Vec<u32> = Vec::new();
    let mut arts: Vec<(u32, u32, u64)> = Vec::new();
    let mut leases: Vec<(u32, u32, u32)> = Vec::new();
    for rec in jrn.replay() {
        match rec {
            JournalRecord::ChunkPin(d) => chunks.push(d.offset),
            JournalRecord::ArtifactPin {
                artifact_id,
                incarnation,
                version,
            } => arts.push((artifact_id, incarnation, version)),
            JournalRecord::WriteLease {
                artifact_id,
                incarnation,
                fence,
            } => leases.push((artifact_id, incarnation, fence)),
            JournalRecord::StagedManifest { .. } => {}
        }
    }
    chunks.sort_unstable();
    arts.sort_unstable();
    assert_eq!(chunks, vec![64, 128]);
    assert_eq!(arts, vec![(7, 1, 0x1_0000_0002u64), (42, 1, 5)]);
    assert_eq!(
        leases,
        vec![(9, 1, 3)],
        "the write-lease entry round-trips, incarnation included"
    );

    // Releasing the write-lease slot frees it (proving record/release/replay
    // works for the new kind alongside the others): 5 → 4 entries, and no
    // WriteLease survives the replay.
    jrn.release(w0).unwrap();
    assert_eq!(jrn.len(), 4);
    assert!(!jrn
        .replay()
        .any(|r| matches!(r, JournalRecord::WriteLease { .. })));

    // A ChunkPin round-trips its whole descriptor, not just the offset.
    let full = jrn
        .replay()
        .find_map(|r| match r {
            JournalRecord::ChunkPin(d) if d.offset == 64 => Some(d),
            _ => None,
        })
        .unwrap();
    assert_eq!(full, mk_desc(64));

    // Releasing an artifact-pin slot frees it for reuse by either kind, and the
    // JournalFull backpressure still applies once every slot is taken.
    jrn.release(a0).unwrap();
    assert_eq!(jrn.len(), 3);
    while jrn.record_artifact_pin(1, 1, 1).is_ok() {}
    assert!(matches!(jrn.record(mk_desc(1)), Err(Error::JournalFull)));

    // A second attached handle sees the identical tagged contents (POD in shm).
    let jrn2 = BorrowJournal::attach(&seg).unwrap();
    assert_eq!(jrn2.len(), 8);

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

// ---------------------------------------------------------------------------
// ADR-0011 (Holon P0.4): adopted-fd segments are unnamed
// ---------------------------------------------------------------------------

/// An `SCM_RIGHTS` receiver's [`Segment::from_raw_fd`] handle must be UNNAMED:
/// its `unlink` is a no-op and can never remove the creating process's
/// namespace entry (before ADR-0011 it resolved and unlinked the creator's
/// name). This is also the invariant memfd-backed segments rely on — they have
/// no namespace entry at all.
#[test]
fn adopted_fd_segment_is_unnamed_and_unlink_is_a_noop() {
    use std::os::fd::{BorrowedFd, IntoRawFd};

    let id = uid();
    let _ = Segment::attach(id).and_then(|s| s.unlink());
    let seg = Segment::create(id, 8192).expect("segment create");

    // Duplicate the fd exactly as an SCM_RIGHTS transfer would install it.
    // SAFETY: `seg` keeps the fd open across the borrow.
    let dup = unsafe { BorrowedFd::borrow_raw(seg.as_raw_fd()) }
        .try_clone_to_owned()
        .expect("dup segment fd");
    // SAFETY: `dup` is a live, owned duplicate of a real segment fd whose
    // ownership transfers into the adopted handle exactly once.
    let adopted = unsafe { Segment::from_raw_fd(dup.into_raw_fd(), id) }.expect("adopt fd");
    assert_eq!(adopted.id(), id);

    // The adopted handle's unlink is a no-op...
    adopted.unlink().expect("unnamed unlink must succeed as a no-op");
    // ...so the creator's name must STILL resolve afterwards.
    let reattach = Segment::attach(id)
        .expect("the creator's shm name must survive an adopted handle's unlink");
    drop(reattach);
    drop(adopted);
    seg.unlink().expect("creator unlink");
}

/// ADR-0014 §4: releasing a journal slot is an election — the first caller
/// observes it occupied and wins; a second (a zombie's clean release after the
/// coordinator's replay, or vice versa) observes it already cleared and must
/// not perform the shared-memory release.
fn journal_seg_for_test(capacity: usize) -> Segment {
    let id = 90_000 + (std::process::id() & 0x3ff);
    let _ = Segment::unlink_by_id(id);
    Segment::create(id, 64 * 1024 + capacity * 64).expect("journal seg")
}

#[test]
fn journal_release_is_an_election() {
    let seg = journal_seg_for_test(64);
    let jrn = BorrowJournal::create(&seg, 64).unwrap();
    let slot = jrn.record_artifact_pin(5, 1, 3).unwrap();
    assert!(jrn.release(slot).unwrap(), "first release wins the slot");
    assert!(!jrn.release(slot).unwrap(), "second release loses: bit already clear");
    assert_eq!(jrn.len(), 0);
    // `replay_indexed` yields the slot so a replayer can win it before acting.
    let s2 = jrn.record_artifact_pin(6, 1, 4).unwrap();
    let idx: Vec<usize> = jrn.replay_indexed().into_iter().map(|(i, _)| i).collect();
    assert_eq!(idx, vec![s2]);
    seg.unlink().ok();
}
