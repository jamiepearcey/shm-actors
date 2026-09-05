//! `holon-net-demo` — ADR-0017's gate: the SSE / TCP / HTTP fronts end to
//! end, as separate OS processes, with the numbers printed.

#![deny(missing_docs)]

pub mod actors;
pub mod bench;
pub mod roles;

pub use actors::{
    Count, Counter, Echo, Get, Inc, Range, RangeAck, RangeReq, Sink, Work, WorkDone, COUNTER,
    COUNTER_GET, COUNTER_INC, COUNTER_REPLY, DONE_TOPIC, ECHO, ECHO_REQ, RANGE, RANGE_REPLY,
    RANGE_REQ, SINK, WORK_REPLY, WORK_REQ,
};
