#![no_main]
//! Fuzz the artifact **VersionManifest** parser (`shm_artifact::parse_manifest_bytes`)
//! — the untrusted-input boundary a recycled or corrupt manifest chunk reaches
//! across. Contract: for any input, it returns `Ok`/`Err`, never panics, never
//! reads out of bounds, never UB. This is the exact validation logic
//! `read_manifest` runs after `Segment::resolve` proves the region is mapped.
use libfuzzer_sys::fuzz_target;
use shm_artifact::parse_manifest_bytes;

fuzz_target!(|data: &[u8]| {
    let _ = parse_manifest_bytes(data);
});
