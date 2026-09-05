//! The demo actors: the smallest set that exercises every front.
//!
//! - [`Echo`] — ask/reply floor: the reply is the request bytes, returned **by
//!   reference** so it is bounded by the asker's chunk rather than the 192-byte
//!   inline cap.
//! - [`Counter`] — tells mutate, asks read: `INC` (tell) / `GET` (ask).
//! - [`Sink`] — the ArrowRef "signal feed" shape: a unit of work whose
//!   completion is *published to a topic* from inside the handler
//!   (`Cx::publish`), so every SSE/TCP subscriber of `done` sees it without
//!   any gateway code.
//! - [`Range`] — ADR-0006's numeric fast lane as a message design: one
//!   request names a contiguous handle range; completion is either one event
//!   per handle or one range-coalesced event (`compress = 1`).

use holon_actor::{Actor, Cx};
use holon_core::{Envelope, Payload, Reply};
use shm_core::SharedPod;

/// Echo actor name.
pub const ECHO: &str = "echo";
/// Counter actor name.
pub const COUNTER: &str = "counter";
/// Sink actor name.
pub const SINK: &str = "sink";
/// Range actor name.
pub const RANGE: &str = "range";

/// Topic the sink and range actors publish completions to.
pub const DONE_TOPIC: &str = "done";

/// Echo request: any bytes the asker's reply chunk can hold back.
pub const ECHO_REQ: u32 = 2001;
/// Counter increment (tell).
pub const COUNTER_INC: u32 = 2002;
/// Counter read (ask).
pub const COUNTER_GET: u32 = 2003;
/// Counter reply.
pub const COUNTER_REPLY: u32 = 2004;
/// Sink work request.
pub const WORK_REQ: u32 = 2005;
/// Sink work reply.
pub const WORK_REPLY: u32 = 2006;
/// Range request.
pub const RANGE_REQ: u32 = 2007;
/// Range reply.
pub const RANGE_REPLY: u32 = 2008;

/// [`COUNTER_INC`] body.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Inc {
    /// Amount.
    pub by: u64,
}
// SAFETY: repr(C), one scalar.
unsafe impl SharedPod for Inc {}
impl Payload for Inc {
    const SCHEMA_ID: u32 = COUNTER_INC;
}

/// [`COUNTER_GET`] body (empty marker).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Get {
    /// Reserved.
    pub _pad: u64,
}
// SAFETY: as above.
unsafe impl SharedPod for Get {}
impl Payload for Get {
    const SCHEMA_ID: u32 = COUNTER_GET;
}

/// [`COUNTER_REPLY`] body.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Count {
    /// Current count.
    pub count: u64,
}
// SAFETY: as above.
unsafe impl SharedPod for Count {}
impl Payload for Count {
    const SCHEMA_ID: u32 = COUNTER_REPLY;
}

/// [`WORK_REQ`] body.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Work {
    /// Caller's id, echoed in the completion event.
    pub id: u64,
    /// Publisher's clock (nanos), echoed so subscribers can measure latency.
    pub sent_nanos: u64,
}
// SAFETY: as above.
unsafe impl SharedPod for Work {}
impl Payload for Work {
    const SCHEMA_ID: u32 = WORK_REQ;
}

/// [`WORK_REPLY`] body.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct WorkDone {
    /// The work id.
    pub id: u64,
    /// The seq of the completion event on [`DONE_TOPIC`].
    pub seq: u64,
}
// SAFETY: as above.
unsafe impl SharedPod for WorkDone {}
impl Payload for WorkDone {
    const SCHEMA_ID: u32 = WORK_REPLY;
}

/// [`RANGE_REQ`] body.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct RangeReq {
    /// First handle.
    pub first: u64,
    /// Handles in the range.
    pub count: u32,
    /// `1` = one coalesced completion event; `0` = one event per handle.
    pub compress: u32,
}
// SAFETY: as above.
unsafe impl SharedPod for RangeReq {}
impl Payload for RangeReq {
    const SCHEMA_ID: u32 = RANGE_REQ;
}

/// [`RANGE_REPLY`] body.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct RangeAck {
    /// First handle.
    pub first: u64,
    /// Handles completed.
    pub count: u32,
    /// Events published.
    pub events: u32,
}
// SAFETY: as above.
unsafe impl SharedPod for RangeAck {}
impl Payload for RangeAck {
    const SCHEMA_ID: u32 = RANGE_REPLY;
}

/// The echo actor.
#[derive(Default)]
pub struct Echo;

impl Actor for Echo {
    fn accepts() -> &'static [u32] {
        &[ECHO_REQ]
    }

    fn handle(&mut self, _m: &Envelope, body: &[u8], cx: &mut Cx<'_>) -> holon_core::Result<Reply> {
        // Check the budget before building the answer, not after: over-running
        // it fails the task, and a caller deserves to know it asked for more
        // than it made room for.
        if body.len() > cx.max_reply() as usize {
            return Err(holon_core::Error::BodyTooLarge {
                len: body.len(),
                max: cx.max_reply() as usize,
            });
        }
        Reply::bytes(ECHO_REQ, body)
    }
}

/// The counter actor.
#[derive(Default)]
pub struct Counter {
    count: u64,
}

impl Actor for Counter {
    fn accepts() -> &'static [u32] {
        &[COUNTER_INC, COUNTER_GET]
    }

    fn handle(&mut self, m: &Envelope, body: &[u8], _cx: &mut Cx<'_>) -> holon_core::Result<Reply> {
        match m.schema_id {
            COUNTER_INC => {
                let inc = Inc::from_bytes(body)?;
                self.count = self.count.wrapping_add(inc.by);
                Ok(Reply::None)
            }
            _ => Reply::of(&Count { count: self.count }),
        }
    }
}

/// The sink actor: completion → topic.
#[derive(Default)]
pub struct Sink {
    done: u64,
}

impl Actor for Sink {
    fn accepts() -> &'static [u32] {
        &[WORK_REQ]
    }

    fn handle(&mut self, _m: &Envelope, body: &[u8], cx: &mut Cx<'_>) -> holon_core::Result<Reply> {
        let w = Work::from_bytes(body)?;
        self.done += 1;
        // The completion is the event: `<id>:<sent_nanos>` as text.
        let data = format!("{}:{}", w.id, w.sent_nanos);
        let seq = cx.publish(DONE_TOPIC, "done", data.as_bytes())?;
        Reply::of(&WorkDone { id: w.id, seq })
    }
}

/// The range actor: ADR-0006's fast lane as a message.
#[derive(Default)]
pub struct Range;

impl Actor for Range {
    fn accepts() -> &'static [u32] {
        &[RANGE_REQ]
    }

    fn handle(&mut self, _m: &Envelope, body: &[u8], cx: &mut Cx<'_>) -> holon_core::Result<Reply> {
        let r = RangeReq::from_bytes(body)?;
        let mut events = 0u32;
        if r.compress != 0 {
            let data = format!("{}+{}", r.first, r.count);
            cx.publish(DONE_TOPIC, "range", data.as_bytes())?;
            events = 1;
        } else {
            for h in r.first..r.first + u64::from(r.count) {
                let data = h.to_string();
                cx.publish(DONE_TOPIC, "done", data.as_bytes())?;
                events += 1;
            }
        }
        Reply::of(&RangeAck {
            first: r.first,
            count: r.count,
            events,
        })
    }
}
