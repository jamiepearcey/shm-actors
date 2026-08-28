#![no_main]
//! Fuzz the ADR-0013 **manifest chain walker** (`shm_artifact::walk_chain_with`)
//! over arbitrary parsed manifests. Contract: for any set of manifests — any
//! links, any depths, any totals, cycles included — the walk returns
//! `Ok`/`Err`, never panics, and never loops (it is bounded by the head's
//! `depth` and by strictly decreasing versions).
//!
//! The input is sliced into fixed-size records, each fed through the exact
//! `parse_manifest_bytes` boundary (so the parser is fuzzed too); every record
//! that parses joins a store keyed by version, the last one is the head, and
//! links resolve against the store — the shape `VersionPin::chain` has, minus
//! the segment.
use libfuzzer_sys::fuzz_target;
use shm_artifact::{parse_manifest_bytes, walk_chain_with, Error, Manifest};

fuzz_target!(|data: &[u8]| {
    let store: Vec<Manifest> = data
        .chunks(96)
        .filter_map(|rec| parse_manifest_bytes(rec).ok())
        .collect();
    let Some(head) = store.last() else {
        return;
    };
    let mut resolved = 0usize;
    let _ = walk_chain_with(head, |link| {
        resolved += 1;
        store
            .iter()
            .find(|m| m.version == link.version)
            .cloned()
            .ok_or(Error::VersionGone)
    });
    // The walk never resolves more links than the head claims as its depth.
    assert!(resolved <= head.depth as usize);
});
