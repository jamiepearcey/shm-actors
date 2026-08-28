//! loom model of the **shared-reference release election** the ADR-0013
//! retire cascade rests on.
//!
//! Chained manifests make a manifest chunk multiply-referenced: its own
//! version holds one reference, its successor's `Append` link another. Both
//! are released concurrently in practice — the version by `try_retire_version`
//! (a reader's pin drop, a commit's install-time retire, an `evict_all`), the
//! link by the successor's own cascade or a losing committer's rollback. The
//! cascade (`release_manifest_ref`) must run **exactly once** per manifest, so
//! the whole design rests on one property of the production
//! [`ChunkCtrl::release_shared`]: of two releasers of a `refcount == 2` chunk,
//! **exactly one** observes `true` (the `PUBLISHED → FREE` CAS inside
//! `try_reclaim` elects it) and the chunk ends `FREE` with a bumped generation.
//! If both observed `true` the cascade would run twice (double release of every
//! data chunk and of the next link — a use-after-free down the chain); if
//! neither did, the whole chain would leak.
//!
//! This runs the *production* `release_shared` body over a `ChunkCtrl` built in
//! ordinary memory (its fields are the `ShmU32` substrate loom instruments) and
//! shared across two `loom::thread`s. loom explores every interleaving of the
//! `fetch_sub` / state-owner-refcount loads / CAS / generation bump.
//!
//! Only compiled/run under `--cfg loom`; a no-op otherwise.
#![cfg(loom)]

use loom::sync::Arc;

use shm_core::{pack_word, ChunkCtrl, ShmU32, ShmU64, FREE, OWNER_NONE, PUBLISHED};

/// A published, owner-released chunk carrying `refs` shared references — the
/// state of a manifest chunk held by its version and by one successor link.
fn published_with_refs(refs: u32) -> ChunkCtrl {
    ChunkCtrl {
        word: ShmU64::new(pack_word(PUBLISHED, refs)),
        owner_actor: ShmU32::new(OWNER_NONE),
        generation: ShmU32::new(0),
    }
}

static ITERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[test]
fn loom_ctrl_two_releasers_exactly_one_frees() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ctrl = Arc::new(published_with_refs(2));

        let a = {
            let c = ctrl.clone();
            loom::thread::spawn(move || c.release_shared())
        };
        let b = {
            let c = ctrl.clone();
            loom::thread::spawn(move || c.release_shared())
        };
        let freed_a = a.join().unwrap();
        let freed_b = b.join().unwrap();

        // The election: exactly one releaser is told it freed the chunk — that
        // one (and only that one) runs the cascade.
        assert!(
            freed_a ^ freed_b,
            "release election violated: a={freed_a} b={freed_b} (both or neither observed the free)"
        );
        // And the chunk really is recycled, once: FREE, refcount 0, generation
        // bumped exactly once (so a stale link's `prev_gen` fails validation).
        assert_eq!(ctrl.state(), FREE);
        assert_eq!(ctrl.refcount(), 0);
        assert_eq!(ctrl.generation(), 1);
    });
    eprintln!(
        "loom_ctrl_two_releasers_exactly_one_frees: explored {} interleavings",
        ITERS.swap(0, std::sync::atomic::Ordering::Relaxed)
    );
}

// A three-releaser variant (`refcount == 3`) also holds but takes loom ~10
// minutes to exhaust; the two-thread model is the property the cascade
// depends on (a manifest has at most two concurrent releasers: its version and
// its one successor link), so only that one is kept in the suite.

/// The window `SHMPOOL2` closes: a borrower racing a reclaimer on a published,
/// owner-released, zero-count chunk. Either the borrow wins (chunk stays
/// PUBLISHED with count 1, reclaim reports false) or the reclaim wins (chunk is
/// FREE, borrow errors). Never both — a reference onto a freed chunk — and
/// never neither.
#[test]
fn loom_ctrl_borrow_vs_reclaim_never_both() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ctrl = Arc::new(published_with_refs(0));
        let b = {
            let c = ctrl.clone();
            loom::thread::spawn(move || c.borrow_shared().is_ok())
        };
        let r = {
            let c = ctrl.clone();
            loom::thread::spawn(move || c.try_reclaim())
        };
        let borrowed = b.join().unwrap();
        let reclaimed = r.join().unwrap();
        assert!(
            borrowed ^ reclaimed,
            "borrow={borrowed} reclaim={reclaimed}: a reference onto a freed chunk, or a leak"
        );
        if borrowed {
            assert_eq!(ctrl.state(), PUBLISHED);
            assert_eq!(ctrl.refcount(), 1);
        } else {
            assert_eq!(ctrl.state(), FREE);
            assert_eq!(ctrl.generation(), 1);
        }
    });
    eprintln!(
        "loom_ctrl_borrow_vs_reclaim_never_both: explored {} interleavings",
        ITERS.swap(0, std::sync::atomic::Ordering::Relaxed)
    );
}

/// The last shared releaser racing the owner's release on a count-1 chunk:
/// exactly one of them frees it.
#[test]
fn loom_ctrl_release_vs_owner_release_exactly_one_frees() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ctrl = Arc::new(ChunkCtrl {
            word: ShmU64::new(pack_word(PUBLISHED, 1)),
            owner_actor: ShmU32::new(7),
            generation: ShmU32::new(0),
        });
        let a = {
            let c = ctrl.clone();
            loom::thread::spawn(move || c.release_shared())
        };
        let b = {
            let c = ctrl.clone();
            loom::thread::spawn(move || c.owner_release())
        };
        let fa = a.join().unwrap();
        let fb = b.join().unwrap();
        assert!(fa ^ fb, "release election violated: shared={fa} owner={fb}");
        assert_eq!(ctrl.state(), FREE);
        assert_eq!(ctrl.generation(), 1);
    });
    eprintln!(
        "loom_ctrl_release_vs_owner_release_exactly_one_frees: explored {} interleavings",
        ITERS.swap(0, std::sync::atomic::Ordering::Relaxed)
    );
}
