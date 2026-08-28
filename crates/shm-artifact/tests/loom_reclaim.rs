//! loom model of the **recycle handshake** (ADR-0008 P0.1): every operation
//! *registers* with a `SeqCst` RMW, executes the handshake's `SeqCst` fence,
//! then re-validates the head's incarnation; the sweep *retires* the
//! incarnation (`SeqCst` swap), fences, then scans for registrations with
//! `SeqCst` loads. Either the sweep sees the registration and aborts, or the
//! operation sees the retirement and backs out — never both missing.
//!
//! The explicit fences are load-bearing here exactly as in the pin hazard
//! handshake: loom models `SeqCst` *operations* as acquire/release and only
//! enforces the cross-location total order through `fence(SeqCst)` (see
//! [`shm_core::substrate::fence`]) — dropping any of them makes these models
//! fail.
//!
//! Three models, one per registration point (the three "register" writes the
//! quiescence scan can see), each running the production head methods:
//!
//! - **pin**: [`PinSlot::publish_pin`] → [`PinSlot::accept_pin`] (whose fence
//!   is the pin path's barrier) → [`ArtifactHead::is_incarnation`], vs
//!   [`ArtifactHead::retire`] → [`ArtifactHead::is_quiescent`].
//! - **commit**: [`ArtifactHead::claim_slot`] (install CAS) →
//!   [`ArtifactHead::revalidate_incarnation`] (the `commit_staged_inner`
//!   step-4/4b shape), vs the same sweep.
//! - **lease**: [`ArtifactHead::acquire_write_lease`] →
//!   [`ArtifactHead::revalidate_incarnation`] (the `open_exclusive*` shape),
//!   vs the same sweep.
//!
//! # Asserted safety invariant (the whole point)
//!
//! `!(operation validated && sweep judged quiescent)` — a sweep never reclaims
//! a head region out from under an operation that believes its registration
//! stands. A violation would be a REAL hole in the ADR-0008 handshake (or an
//! unfaithful harness).
//!
//! Only compiled/run under `--cfg loom`; a no-op otherwise.
#![cfg(loom)]

use std::sync::atomic::Ordering::SeqCst;

use loom::sync::Arc;

use shm_artifact::head::{ArtifactHead, NO_VERSION};
use shm_artifact::FIRST_INCARNATION;

/// The sweep half, exactly as `shm-store`'s `entry_is_finished` orders it:
/// retire the incarnation, run the production quiescence scan, re-commission
/// on abort. Returns `true` iff the sweep would reclaim the region.
fn sweep(head: &ArtifactHead) -> bool {
    let held = head.retire();
    let quiescent = head.is_quiescent();
    if !quiescent {
        head.commission(held);
    }
    quiescent
}

static ITERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn report(name: &str) {
    eprintln!(
        "{name}: explored {} interleavings",
        ITERS.swap(0, std::sync::atomic::Ordering::Relaxed)
    );
}

/// Reader pin (publish → accept → validate incarnation) vs sweep.
///
/// The head holds a committed version 1 mid-`evict_all`: the reader raced the
/// eviction and already resolved `current == 1` before the evictor stored
/// `NO_VERSION`, so the model starts from the moment both the reader's
/// `publish_pin` and the sweep can run.
#[test]
fn loom_reclaim_vs_pin() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let head = Arc::new(ArtifactHead::fresh());
        head.commission(FIRST_INCARNATION);
        let idx = head.claim_slot(1, 0x1001).expect("slot for v1");
        // `evict_all` has already flipped `current` to NO_VERSION (the entry is
        // tombstoned); the version slot survives awaiting its retire.
        assert_eq!(head.current.load(SeqCst), NO_VERSION);

        let r = head.clone();
        let reader = loom::thread::spawn(move || {
            let slot = &r.pins[idx];
            slot.publish_pin();
            if !slot.accept_pin(1) {
                slot.unpin();
                return false;
            }
            if !r.is_incarnation(FIRST_INCARNATION) {
                slot.unpin();
                return false;
            }
            true
        });

        let s = head.clone();
        let sweeper = loom::thread::spawn(move || sweep(&s));

        let accepted = reader.join().unwrap();
        let reclaimed = sweeper.join().unwrap();
        assert!(
            !(accepted && reclaimed),
            "sweep reclaimed the region under an accepted, validated pin"
        );
    });
    report("loom_reclaim_vs_pin");
}

/// Committer registration (claim a version slot → validate incarnation) vs
/// sweep — the `commit_staged_inner` step-4/4b shape over an empty (evicted,
/// torn-down) entry, i.e. the straggler-commit-resurrects-a-tombstone race.
#[test]
fn loom_reclaim_vs_claim() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let head = Arc::new(ArtifactHead::fresh());
        head.commission(FIRST_INCARNATION);

        let c = head.clone();
        let committer = loom::thread::spawn(move || {
            let idx = match c.claim_slot(1, 0x1001) {
                Some(i) => i,
                None => return false,
            };
            if !c.revalidate_incarnation(FIRST_INCARNATION) {
                c.pins[idx].store_free();
                return false;
            }
            true // would proceed to the `current` install CAS
        });

        let s = head.clone();
        let sweeper = loom::thread::spawn(move || sweep(&s));

        let installed = committer.join().unwrap();
        let reclaimed = sweeper.join().unwrap();
        assert!(
            !(installed && reclaimed),
            "sweep reclaimed the region under a validated slot claim"
        );
    });
    report("loom_reclaim_vs_claim");
}

/// Writer registration (acquire the write lease → validate incarnation) vs
/// sweep — the `open_exclusive*` shape.
#[test]
fn loom_reclaim_vs_lease() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let head = Arc::new(ArtifactHead::fresh());
        head.commission(FIRST_INCARNATION);

        let w = head.clone();
        let writer = loom::thread::spawn(move || {
            let token = match w.acquire_write_lease(7) {
                Some(t) => t,
                None => return false,
            };
            if !w.revalidate_incarnation(FIRST_INCARNATION) {
                w.release_write_lease(7, token);
                return false;
            }
            true
        });

        let s = head.clone();
        let sweeper = loom::thread::spawn(move || sweep(&s));

        let holding = writer.join().unwrap();
        let reclaimed = sweeper.join().unwrap();
        assert!(
            !(holding && reclaimed),
            "sweep reclaimed the region under a validated write lease"
        );
    });
    report("loom_reclaim_vs_lease");
}

/// **P0.3 (ADR-0010, G12a): the fenced-lease sweep.** `evict_all` now
/// force-releases the write lease (fence bump) in its teardown phase, so the
/// sweep's shape became: *force-release → retire → quiescence scan*. The
/// refined system invariant: a sweep MAY reclaim over a lease it revoked —
/// what must never happen is that the revoked holder still **installs**.
///
/// The writer models the full leased-commit shape: `acquire_write_lease` →
/// fenced `revalidate_incarnation` (the `open_exclusive*` registration) → the
/// step-0 `lease_held_by` gate → `claim_slot` + `revalidate_incarnation` (the
/// step-4/4b commit registration). The step-0 gate is an `Acquire` load and
/// may legally read a *stale* `{owner, token}` (this model found exactly that
/// interleaving when the gate alone was asserted): the gate is best-effort
/// fencing, and the load-bearing guarantee is the **registered** step-4b
/// revalidation — a zombie that slips the gate registers, re-validates, and
/// fails `Stale` against a retired head. Sweep: `force_release_write_lease`
/// → `retire` → `is_quiescent`.
///
/// Asserted: `!(writer would install && sweep reclaimed)`. The Dekker pairing
/// (registration-vs-retire) is untouched: the force-release is a
/// de-registration in the teardown phase, ordered before `retire`, and the
/// fences in `revalidate_incarnation` / `is_quiescent` carry the proof exactly
/// as in `loom_reclaim_vs_lease` / `loom_reclaim_vs_claim`.
#[test]
fn loom_reclaim_vs_fenced_lease() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let head = Arc::new(ArtifactHead::fresh());
        head.commission(FIRST_INCARNATION);

        let w = head.clone();
        let writer = loom::thread::spawn(move || {
            let token = match w.acquire_write_lease(7) {
                Some(t) => t,
                None => return false,
            };
            if !w.revalidate_incarnation(FIRST_INCARNATION) {
                w.release_write_lease(7, token);
                return false;
            }
            // Step 0: the fence gate. A stale read may pass it — that is
            // exactly why the commit registers again below.
            if !w.lease_held_by(7, token) {
                return false; // Error::Fenced: installs nothing
            }
            // Step 4/4b: claim a version slot (the SeqCst registration), then
            // the fenced revalidation. This is what actually stops a zombie
            // from installing onto a reclaimed head.
            let idx = match w.claim_slot(1, 0x1001) {
                Some(i) => i,
                None => return false,
            };
            if !w.revalidate_incarnation(FIRST_INCARNATION) {
                w.pins[idx].store_free();
                return false; // Error::Stale: installs nothing
            }
            true // would proceed to the `current` install CAS
        });

        let s = head.clone();
        let sweeper = loom::thread::spawn(move || {
            // The teardown phase's de-registration (evict_all), then the
            // unchanged retire -> scan shape.
            s.force_release_write_lease();
            sweep(&s)
        });

        let commit_would_install = writer.join().unwrap();
        let reclaimed = sweeper.join().unwrap();
        assert!(
            !(commit_would_install && reclaimed),
            "sweep reclaimed the region under a revoked-lease holder that would \
             still install"
        );
    });
    report("loom_reclaim_vs_fenced_lease");
}
