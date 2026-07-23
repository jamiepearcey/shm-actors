#![no_main]
//! Fuzz the Arrow on-chunk **batch layout** parser (`shm_arrow::read_batch_layout`)
//! — the untrusted-input boundary where a corrupt/malicious peer's chunk bytes
//! are turned into a `BatchHeader` + node/buffer tables. Contract: for any input,
//! it returns `Ok`/`Err`, never panics, never reads out of bounds, never UB.
//!
//! `data` is treated as the primary chunk's raw bytes. To let the fuzzer reach
//! the `chunk_count` and per-buffer-extent branches, we synthesize a bounded
//! list of chunk lengths whose count tracks the header's (untrusted) claimed
//! `chunk_count`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // chunk_lens[0] is always the primary (== data.len()). Extra continuation
    // chunk lengths are derived from the input, capped so a malicious count
    // can't drive an unbounded allocation in the harness itself.
    let mut lens: Vec<usize> = vec![data.len()];
    if data.len() >= 32 {
        let cc = u32::from_le_bytes([data[28], data[29], data[30], data[31]]);
        let extra = (cc as usize).min(8).saturating_sub(1);
        for i in 0..extra {
            let b = data.get(i).copied().unwrap_or(0) as usize;
            lens.push(b * 64);
        }
    }
    let _ = shm_arrow::read_batch_layout(data, &lens);
});
