#![no_main]
//! Fuzz the UDS control-protocol **request** decoder (untrusted-input boundary:
//! a peer hands arbitrary bytes over the socket). Contract: for any input,
//! `Request::decode` returns `Ok`/`Err` — it never panics, never reads out of
//! bounds, never invokes UB. Anything it decodes must also re-encode+re-decode.
use libfuzzer_sys::fuzz_target;
use shm_runtime::protocol::Request;

fuzz_target!(|data: &[u8]| {
    if let Ok(req) = Request::decode(data) {
        // A decoded value must survive a round trip through the encoder.
        let round = Request::decode(&req.encode()).expect("re-decode of decoded request");
        assert_eq!(req, round);
    }
});
