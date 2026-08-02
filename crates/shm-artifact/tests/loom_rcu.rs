//! loom model of **RCU version install vs. read** (ADR-0004 stage L, core 4).
//!
//! This overlaps core 1 (the pin harness already exercises the `{version, state}`
//! revalidation), so it is kept minimal and focuses on the *other* half the pin
//! path guards: the two-word `{current, manifest_desc}` install window. It runs a
//! **real [`ArtifactHead`]** (shrunk to a 2-slot pin table under `--cfg loom`)
//! and the production [`claim_slot`](ArtifactHead::claim_slot)/
//! [`find_slot`](ArtifactHead::find_slot) + [`PinSlot`] handshake methods.
//!
//! # Modeled scenario
//!
//! The head starts with version 1 installed (`current = 1`, slot tracks
//! `{v:1, manifest: m1}`, `manifest_desc = m1`).
//!
//! - **Committer thread** installs v2 exactly as `commit_staged_inner` does:
//!   `claim_slot(2, m2)`, then the linearising `current` CAS `1→2` (SeqCst), then
//!   the `Release` store of `manifest_desc = m2`. (The `manifest_desc` store
//!   deliberately *follows* the `current` CAS — the window this test probes.)
//! - **Reader thread** runs the pin path: load `current`, `find_slot`, publish +
//!   revalidate the pin, then load `manifest_desc` and confirm it names the pinned
//!   slot (`slot.manifest == head.manifest_desc`). It returns the accepted
//!   `(version, manifest_desc)` pair, or `None` if it had to retry.
//!
//! # Asserted invariant
//!
//! Whenever the reader accepts, its observed `manifest_desc` is exactly the
//! manifest belonging to the pinned version (`m1` for v1, `m2` for v2) — the
//! reader NEVER accepts a torn `{version=2, manifest_desc=m1}` (or vice-versa)
//! pair. The install window is never observed torn.
//!
//! Only compiled/run under `--cfg loom`; a no-op otherwise.
#![cfg(loom)]

use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};

use loom::sync::Arc;

use shm_artifact::head::ArtifactHead;

/// The manifest sentinel that belongs to version `v` (distinct per version).
fn manifest_for(v: u64) -> u64 {
    0x1000 + v
}

static ITERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[test]
fn loom_rcu_install_vs_read() {
    loom::model(|| {
        ITERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let head = Arc::new(ArtifactHead::fresh());
        // Install version 1 (single-threaded setup).
        head.claim_slot(1, manifest_for(1)).expect("slot for v1");
        head.current
            .compare_exchange(0, 1, SeqCst, SeqCst)
            .expect("install v1");
        head.manifest_desc.store(manifest_for(1), Release);

        // Committer: install version 2 via the production RCU sequence.
        let c = head.clone();
        let committer = loom::thread::spawn(move || {
            c.claim_slot(2, manifest_for(2)).expect("slot for v2");
            // The single linearising CAS, then publish the manifest pointer.
            if c.current.compare_exchange(1, 2, SeqCst, SeqCst).is_ok() {
                c.manifest_desc.store(manifest_for(2), Release);
            }
        });

        // Reader: the pin path's version/manifest consistency check.
        let r = head.clone();
        let reader = loom::thread::spawn(move || -> Option<(u64, u64)> {
            let v = r.current.load(Acquire);
            let idx = r.find_slot(v)?; // None => mid-reclaim; retry in real code
            let slot = &r.pins[idx];
            slot.publish_pin();
            if !slot.accept_pin(v) {
                slot.unpin();
                return None;
            }
            let head_md = r.manifest_desc.load(Acquire);
            if slot.manifest.load(Acquire) != head_md {
                // Install window: current moved but manifest_desc not yet stored,
                // or the transient duplicate slot. Retry in real code.
                slot.unpin();
                return None;
            }
            slot.unpin();
            Some((v, head_md))
        });

        committer.join().unwrap();
        if let Some((v, md)) = reader.join().unwrap() {
            assert_eq!(
                md,
                manifest_for(v),
                "torn RCU read: version {v} observed with a mismatched manifest_desc {md:#x}"
            );
        }
    });
    eprintln!(
        "loom_rcu_install_vs_read: explored {} interleavings",
        ITERS.load(std::sync::atomic::Ordering::Relaxed)
    );
}
