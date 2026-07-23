//! loom model of the **pin hazard handshake** (ADR-0004 stage L, core 1 — the
//! highest-value model; Fable designed the handshake in ADR-0003a).
//!
//! Runs the *production* [`PinSlot`] handshake methods —
//! [`publish_pin`](PinSlot::publish_pin)/[`accept_pin`](PinSlot::accept_pin)
//! (reader) and [`elect_freeing`](PinSlot::elect_freeing)/
//! [`pin_scan`](PinSlot::pin_scan)/[`store_free`](PinSlot::store_free)/
//! [`revert_live`](PinSlot::revert_live) (reclaimer) — the exact bodies
//! `Artifact::pin` and `try_retire_version` drive, over a `PinSlot` built in
//! ordinary memory and shared across two `loom::thread`s.
//!
//! # Modeled scenario
//!
//! A single `PinSlot` starts `{version: 1, state: SLOT_LIVE, pins: 0}` (i.e. a
//! version that a newer commit has made non-current, so it is now reclaimable).
//!
//! - **Reader thread** performs the pin protocol once: `publish_pin()` (SeqCst
//!   `fetch_add`), then `accept_pin(1)` (revalidate `{version==1, LIVE}`). If
//!   accepted it holds a pin; if rejected it `unpin()`s. Returns whether it
//!   accepted.
//! - **Reclaimer thread** performs one retire attempt: `elect_freeing()` (CAS
//!   `LIVE→FREEING`, publishing the hazard flag); if elected, `pin_scan()`; on
//!   `0` it *frees* (`store_free`), on `>0` it reverts. Returns whether it freed.
//!
//! # Asserted safety invariant (the whole point)
//!
//! `!(reader_accepted && reclaimer_freed)` — the reclaimer NEVER frees the version
//! while the reader holds an *accepted* pin, and an accepted pin therefore always
//! read a LIVE (non-freed) version. This is exactly the Dekker handshake: in the
//! single `SeqCst` total order, either the reader's publish precedes the
//! reclaimer's scan (scan sees `pins ≥ 1`, does not free) or the reclaimer's
//! `FREEING` store precedes the reader's revalidate (revalidate sees `≠ LIVE`,
//! rejects). loom must confirm no interleaving violates it. A violation here would
//! be a REAL bug in the handshake (or an unfaithful harness).
//!
//! Only compiled/run under `--cfg loom`; a no-op otherwise.
#![cfg(loom)]

use loom::sync::Arc;

use shm_artifact::head::{PinSlot, SLOT_LIVE};
use shm_core::{ShmU32, ShmU64};

fn live_slot_v1() -> PinSlot {
    PinSlot {
        version: ShmU64::new(1),
        manifest: ShmU64::new(0),
        pins: ShmU32::new(0),
        state: ShmU32::new(SLOT_LIVE),
    }
}

/// Count of interleavings loom explored in the most recent model run (a genuine
/// `std` counter — `--cfg loom` only swaps the `loom::`-imported atomics, so this
/// persists across iterations). Reported for coverage visibility.
static ITERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[test]
fn loom_pin_hazard_handshake() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let slot = Arc::new(live_slot_v1());

        // Reader: run the pin protocol once, reporting whether the pin was accepted.
        let r = slot.clone();
        let reader = loom::thread::spawn(move || {
            r.publish_pin();
            let accepted = r.accept_pin(1);
            if !accepted {
                r.unpin();
            }
            accepted
        });

        // Reclaimer: one retire attempt, reporting whether it freed the version.
        let c = slot.clone();
        let reclaimer = loom::thread::spawn(move || {
            let mut freed = false;
            if c.elect_freeing() {
                if c.pin_scan() == 0 {
                    c.store_free();
                    freed = true;
                } else {
                    c.revert_live();
                }
            }
            freed
        });

        let accepted = reader.join().unwrap();
        let freed = reclaimer.join().unwrap();

        // The hazard invariant: a freed version and an accepted pin are mutually
        // exclusive. No accepting reader ever dereferences a freed (recycled) chunk.
        assert!(
            !(accepted && freed),
            "hazard handshake violated: reader accepted a pin the reclaimer freed"
        );
    });
    eprintln!(
        "loom_pin_hazard_handshake: explored {} interleavings",
        ITERS.load(std::sync::atomic::Ordering::Relaxed)
    );
}
