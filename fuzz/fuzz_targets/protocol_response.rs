#![no_main]
//! Fuzz the UDS control-protocol **response** decoder (a compromised or buggy
//! coordinator could hand an actor arbitrary bytes). Contract: never panic /
//! OOB / UB; `response_fd_count` on any decoded value must not panic either
//! (the fd-count logic feeds `recvmsg` and must stay total).
use libfuzzer_sys::fuzz_target;
use shm_runtime::protocol::{response_fd_count, Response};

fuzz_target!(|data: &[u8]| {
    if let Ok(resp) = Response::decode(data) {
        let _ = response_fd_count(&resp);
        let round = Response::decode(&resp.encode()).expect("re-decode of decoded response");
        assert_eq!(resp, round);
    }
});
