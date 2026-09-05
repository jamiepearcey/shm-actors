//! The TCP wire: a frame is the 64-byte [`Envelope`] verbatim, then
//! `body_len` bytes (ADR-0017 §3).
//!
//! Nothing is serialized: the envelope on the socket is the envelope in the
//! shm chunk. Location transparency falls out of the ABI. Only the payload
//! resolution differs, and on the wire there is exactly one kind of payload:
//! inline bytes. An envelope carrying `FLAG_LOCAL_REF` is refused at both
//! ends ([`Error::LocalRefOnWire`]).
//!
//! # Control messages
//!
//! Frames addressed to [`ActorId::NONE`] are gateway control, by schema id in
//! the reserved system range (`< 16`):
//!
//! | schema | kind | body | reply |
//! |---|---|---|---|
//! | [`SCHEMA_SUBSCRIBE`] | Ask | [`Subscribe`] | [`SubscribeAck`] |
//! | [`SCHEMA_UNSUBSCRIBE`] | Ask/Tell | [`Unsubscribe`] | empty Reply |
//! | [`SCHEMA_PUBLISH`] | Ask/Tell | [`PublishHeader`] + data | [`PublishAck`] |
//! | [`SCHEMA_PING`] | Ask | empty | [`SCHEMA_PONG`], empty |
//!
//! Gateway → client, unsolicited (`Tell`, `corr = 0`):
//!
//! | schema | body |
//! |---|---|
//! | [`SCHEMA_EVENT`] | [`EventHeader`] + data |
//! | [`SCHEMA_LAGGED`] | [`Lagged`] |
//!
//! Errors are `Err` envelopes with the request's `corr` and an
//! [`ErrorHeader`] + UTF-8 message body ([`SCHEMA_ERROR`]).

use holon_core::{
    ActorId, Envelope, MessageKind, ENVELOPE_SIZE, FLAG_INLINE_PAYLOAD, FLAG_LOCAL_REF,
};
use holon_topic::{MAX_EVENT_NAME, MAX_TOPIC_NAME};
use shm_core::SharedPod;

use crate::error::{Error, Result};

/// Subscribe to a topic from a cursor.
pub const SCHEMA_SUBSCRIBE: u32 = 3;
/// Unsubscribe from a topic.
pub const SCHEMA_UNSUBSCRIBE: u32 = 4;
/// Publish to a topic.
pub const SCHEMA_PUBLISH: u32 = 5;
/// A delivered topic message.
pub const SCHEMA_EVENT: u32 = 6;
/// The subscriber fell out of a topic's window.
pub const SCHEMA_LAGGED: u32 = 7;
/// Ping.
pub const SCHEMA_PING: u32 = 8;
/// Pong.
pub const SCHEMA_PONG: u32 = 9;
/// Error reply body.
pub const SCHEMA_ERROR: u32 = 10;
/// Subscribe acknowledgement.
pub const SCHEMA_SUBSCRIBE_ACK: u32 = 11;
/// Publish acknowledgement.
pub const SCHEMA_PUBLISH_ACK: u32 = 12;

const _: () = assert!(SCHEMA_PUBLISH_ACK < 16);

/// Largest body a frame may carry (`body_len` is `u16`).
pub const MAX_WIRE_BODY: usize = u16::MAX as usize;

/// Largest frame on the wire.
pub const MAX_FRAME: usize = ENVELOPE_SIZE + MAX_WIRE_BODY;

/// Longest content type carried on the wire.
pub const MAX_CONTENT_TYPE: usize = 32;

/// Error codes carried by [`ErrorHeader`].
pub mod code {
    /// Malformed request.
    pub const BAD_REQUEST: u32 = 1;
    /// No actor with that id is hosted (or the ask failed).
    pub const FAILED: u32 = 2;
    /// The ask was cancelled or reaped.
    pub const CANCELLED: u32 = 3;
    /// Too many asks in flight on this connection.
    pub const OVERLOADED: u32 = 4;
    /// Unknown control schema.
    pub const UNKNOWN_SCHEMA: u32 = 5;
    /// Topic or event name rejected.
    pub const BAD_NAME: u32 = 6;
    /// The topic log rejected the publish.
    pub const PUBLISH_FAILED: u32 = 7;
}

/// Body of [`SCHEMA_SUBSCRIBE`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Subscribe {
    /// Deliver messages with `seq > from_seq`; `0` = the whole window,
    /// `u64::MAX` = live only (from the current head).
    pub from_seq: u64,
    /// Topic name bytes (see [`MAX_TOPIC_NAME`]).
    pub topic: [u8; MAX_TOPIC_NAME],
    /// Length of `topic`.
    pub topic_len: u8,
    /// Reserved.
    pub _pad: [u8; 7],
}
// SAFETY: repr(C), all-bytes-valid fields, explicit padding, no pointers.
unsafe impl SharedPod for Subscribe {}
const _: () = assert!(core::mem::size_of::<Subscribe>() == 48);

/// Body of [`SCHEMA_UNSUBSCRIBE`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Unsubscribe {
    /// Topic name bytes.
    pub topic: [u8; MAX_TOPIC_NAME],
    /// Length of `topic`.
    pub topic_len: u8,
    /// Reserved.
    pub _pad: [u8; 7],
}
// SAFETY: as above.
unsafe impl SharedPod for Unsubscribe {}
const _: () = assert!(core::mem::size_of::<Unsubscribe>() == 40);

/// Reply to [`SCHEMA_SUBSCRIBE`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SubscribeAck {
    /// The topic's key id: every [`EventHeader::topic_id`] for this topic.
    pub topic_id: u32,
    /// Reserved.
    pub _pad: u32,
    /// The topic head at subscribe time.
    pub head: u64,
    /// The cursor delivery starts after (clamped into the window).
    pub cursor: u64,
}
// SAFETY: as above.
unsafe impl SharedPod for SubscribeAck {}
const _: () = assert!(core::mem::size_of::<SubscribeAck>() == 24);

/// Body header of [`SCHEMA_PUBLISH`]; the message data follows it.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PublishHeader {
    /// Replay window (messages) if the topic is created by this publish;
    /// `0` = gateway default.
    pub window: u32,
    /// Length of `topic`.
    pub topic_len: u8,
    /// Length of `event`.
    pub event_len: u8,
    /// Length of `content_type`.
    pub ct_len: u8,
    /// Reserved.
    pub _pad: u8,
    /// Topic name bytes.
    pub topic: [u8; MAX_TOPIC_NAME],
    /// Event name bytes.
    pub event: [u8; MAX_EVENT_NAME],
    /// Content type bytes.
    pub content_type: [u8; MAX_CONTENT_TYPE],
}
// SAFETY: as above.
unsafe impl SharedPod for PublishHeader {}
const _: () = assert!(core::mem::size_of::<PublishHeader>() == 88);

/// Reply to [`SCHEMA_PUBLISH`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PublishAck {
    /// The seq assigned to the message.
    pub seq: u64,
}
// SAFETY: as above.
unsafe impl SharedPod for PublishAck {}

/// Body header of [`SCHEMA_EVENT`]; the message data follows it.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct EventHeader {
    /// Message seq.
    pub seq: u64,
    /// Publisher wall clock, ms since the Unix epoch.
    pub ts_ms: u64,
    /// Topic key id (from [`SubscribeAck::topic_id`]).
    pub topic_id: u32,
    /// Length of `event`.
    pub event_len: u8,
    /// Length of `content_type`.
    pub ct_len: u8,
    /// Reserved.
    pub _pad: [u8; 2],
    /// Event name bytes.
    pub event: [u8; MAX_EVENT_NAME],
    /// Content type bytes.
    pub content_type: [u8; MAX_CONTENT_TYPE],
}
// SAFETY: as above.
unsafe impl SharedPod for EventHeader {}
const _: () = assert!(core::mem::size_of::<EventHeader>() == 72);

/// Body of [`SCHEMA_LAGGED`]: messages in `(cursor, resume_seq]` were not
/// delivered; delivery continues from `resume_seq`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Lagged {
    /// Topic key id.
    pub topic_id: u32,
    /// Reserved.
    pub _pad: u32,
    /// The cursor delivery resumed after.
    pub resume_seq: u64,
    /// The topic head when the lag was detected.
    pub head: u64,
}
// SAFETY: as above.
unsafe impl SharedPod for Lagged {}
const _: () = assert!(core::mem::size_of::<Lagged>() == 24);

/// Body header of an `Err` envelope ([`SCHEMA_ERROR`]); a UTF-8 message
/// follows it.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct ErrorHeader {
    /// One of [`code`].
    pub code: u32,
    /// Reserved.
    pub _pad: u32,
}
// SAFETY: as above.
unsafe impl SharedPod for ErrorHeader {}

/// Copy `s` into a fixed name field; returns its length.
pub fn put_name<const N: usize>(dst: &mut [u8; N], s: &str) -> u8 {
    let n = s.len().min(N);
    dst[..n].copy_from_slice(&s.as_bytes()[..n]);
    n as u8
}

/// Read a fixed name field back as `&str` (invalid UTF-8 → `""`).
pub fn get_name<const N: usize>(src: &[u8; N], len: u8) -> &str {
    let n = (len as usize).min(N);
    core::str::from_utf8(&src[..n]).unwrap_or("")
}

impl Subscribe {
    /// Build a subscribe body.
    pub fn new(topic: &str, from_seq: u64) -> Subscribe {
        let mut s = Subscribe {
            from_seq,
            topic: [0; MAX_TOPIC_NAME],
            topic_len: 0,
            _pad: [0; 7],
        };
        s.topic_len = put_name(&mut s.topic, topic);
        s
    }

    /// The topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        get_name(&self.topic, self.topic_len)
    }
}

impl Unsubscribe {
    /// Build an unsubscribe body.
    pub fn new(topic: &str) -> Unsubscribe {
        let mut s = Unsubscribe {
            topic: [0; MAX_TOPIC_NAME],
            topic_len: 0,
            _pad: [0; 7],
        };
        s.topic_len = put_name(&mut s.topic, topic);
        s
    }

    /// The topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        get_name(&self.topic, self.topic_len)
    }
}

impl PublishHeader {
    /// Build a publish header.
    pub fn new(topic: &str, event: &str, content_type: &str, window: u32) -> PublishHeader {
        let mut h = PublishHeader {
            window,
            topic_len: 0,
            event_len: 0,
            ct_len: 0,
            _pad: 0,
            topic: [0; MAX_TOPIC_NAME],
            event: [0; MAX_EVENT_NAME],
            content_type: [0; MAX_CONTENT_TYPE],
        };
        h.topic_len = put_name(&mut h.topic, topic);
        h.event_len = put_name(&mut h.event, event);
        h.ct_len = put_name(&mut h.content_type, content_type);
        h
    }

    /// The topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        get_name(&self.topic, self.topic_len)
    }

    /// The event name.
    #[inline]
    pub fn event(&self) -> &str {
        get_name(&self.event, self.event_len)
    }

    /// The content type.
    #[inline]
    pub fn content_type(&self) -> &str {
        get_name(&self.content_type, self.ct_len)
    }
}

impl EventHeader {
    /// Build an event header.
    pub fn new(
        seq: u64,
        ts_ms: u64,
        topic_id: u32,
        event: &str,
        content_type: &str,
    ) -> EventHeader {
        let mut h = EventHeader {
            seq,
            ts_ms,
            topic_id,
            event_len: 0,
            ct_len: 0,
            _pad: [0; 2],
            event: [0; MAX_EVENT_NAME],
            content_type: [0; MAX_CONTENT_TYPE],
        };
        h.event_len = put_name(&mut h.event, event);
        h.ct_len = put_name(&mut h.content_type, content_type);
        h
    }

    /// The event name.
    #[inline]
    pub fn event(&self) -> &str {
        get_name(&self.event, self.event_len)
    }

    /// The content type.
    #[inline]
    pub fn content_type(&self) -> &str {
        get_name(&self.content_type, self.ct_len)
    }
}

/// Decode a fixed-size POD body prefix; `Err` if too short.
pub fn read_pod<T: bytemuck::Pod>(body: &[u8]) -> Result<T> {
    let n = core::mem::size_of::<T>();
    if body.len() < n {
        return Err(Error::Protocol("body too short for its schema"));
    }
    Ok(bytemuck::pod_read_unaligned(&body[..n]))
}

/// Split a body into a POD header and the trailing bytes.
pub fn read_pod_and_rest<T: bytemuck::Pod>(body: &[u8]) -> Result<(T, &[u8])> {
    let n = core::mem::size_of::<T>();
    let h = read_pod::<T>(body)?;
    Ok((h, &body[n..]))
}

/// Append one frame to `out`. Refuses a `LocalRef` (I2) and forces the
/// inline flag; `env.body_len` is overwritten with `body.len()`.
pub fn encode_frame(out: &mut Vec<u8>, env: &Envelope, body: &[u8]) -> Result<()> {
    if env.flags & FLAG_LOCAL_REF != 0 || env.payload != 0 {
        return Err(Error::LocalRefOnWire);
    }
    if body.len() > MAX_WIRE_BODY {
        return Err(holon_core::Error::BodyTooLarge {
            len: body.len(),
            max: MAX_WIRE_BODY,
        }
        .into());
    }
    let mut e = *env;
    e.flags |= FLAG_INLINE_PAYLOAD;
    e.body_len = body.len() as u16;
    out.extend_from_slice(bytemuck::bytes_of(&e));
    out.extend_from_slice(body);
    Ok(())
}

/// Append one frame with a POD body.
pub fn encode_pod_frame<T: bytemuck::Pod>(
    out: &mut Vec<u8>,
    env: &Envelope,
    body: &T,
) -> Result<()> {
    encode_frame(out, env, bytemuck::bytes_of(body))
}

/// Try to decode one frame from the front of `buf`.
///
/// `Ok(None)` = need more bytes. `Ok(Some((env, body, consumed)))` = one
/// frame; the caller drops `consumed` bytes. `Err` = the peer violated the
/// protocol (bad magic/ABI, a `LocalRef`); the caller closes the connection.
pub fn decode_frame(buf: &[u8]) -> Result<Option<(Envelope, &[u8], usize)>> {
    if buf.len() < ENVELOPE_SIZE {
        return Ok(None);
    }
    let env = Envelope::from_bytes(&buf[..ENVELOPE_SIZE])?;
    if env.flags & FLAG_LOCAL_REF != 0 {
        return Err(Error::LocalRefOnWire);
    }
    let body_len = if env.is_inline() {
        env.body_len as usize
    } else {
        0
    };
    let total = ENVELOPE_SIZE + body_len;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some((env, &buf[ENVELOPE_SIZE..total], total)))
}

/// A `Tell` envelope from `from` to `to`.
pub fn tell(to: ActorId, from: ActorId, schema_id: u32) -> Envelope {
    Envelope::inline(MessageKind::Tell, to, from, 0, schema_id, 0)
}

/// An `Ask` envelope from `from` to `to`.
pub fn ask(to: ActorId, from: ActorId, corr: u64, schema_id: u32) -> Envelope {
    Envelope::inline(MessageKind::Ask, to, from, corr, schema_id, 0)
}

/// Is this a gateway control frame (addressed to nobody)?
#[inline]
pub fn is_control(env: &Envelope) -> bool {
    env.to == ActorId::NONE.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip_and_partial() {
        let mut out = Vec::new();
        let env = ask(ActorId::named("echo"), ActorId::new(0, 9), 42, 1001);
        encode_frame(&mut out, &env, b"hello").unwrap();
        assert_eq!(out.len(), ENVELOPE_SIZE + 5);
        // Partial: not enough bytes yet.
        assert!(decode_frame(&out[..ENVELOPE_SIZE + 2]).unwrap().is_none());
        assert!(decode_frame(&out[..10]).unwrap().is_none());
        let (e, body, n) = decode_frame(&out).unwrap().unwrap();
        assert_eq!(n, out.len());
        assert_eq!(body, b"hello");
        assert_eq!(e.corr, 42);
        assert_eq!(e.schema_id, 1001);
        assert!(e.is_inline());
        assert_eq!(e.body_len, 5);
        // Two frames back to back.
        let mut two = out.clone();
        encode_frame(
            &mut two,
            &tell(ActorId::NONE, ActorId::NONE, SCHEMA_PING),
            &[],
        )
        .unwrap();
        let (_, _, n1) = decode_frame(&two).unwrap().unwrap();
        let (e2, b2, n2) = decode_frame(&two[n1..]).unwrap().unwrap();
        assert_eq!(n1 + n2, two.len());
        assert!(b2.is_empty() && e2.schema_id == SCHEMA_PING);
    }

    #[test]
    fn local_ref_never_crosses_the_wire() {
        let mut out = Vec::new();
        let mut env = ask(ActorId::named("x"), ActorId::NONE, 1, 1);
        env.flags |= FLAG_LOCAL_REF;
        env.payload = 0xdead_beef;
        assert!(matches!(
            encode_frame(&mut out, &env, &[]),
            Err(Error::LocalRefOnWire)
        ));
        // Hand-forged on the wire: refused at ingress too.
        let mut raw = env.to_bytes().to_vec();
        raw.extend_from_slice(&[0u8; 0]);
        assert!(matches!(decode_frame(&raw), Err(Error::LocalRefOnWire)));
    }

    #[test]
    fn bad_magic_is_protocol_error() {
        let mut raw = ask(ActorId::NONE, ActorId::NONE, 1, 1).to_bytes().to_vec();
        raw[52] ^= 0xff;
        assert!(decode_frame(&raw).is_err());
    }

    #[test]
    fn control_bodies_round_trip() {
        let s = Subscribe::new("ticks", 7);
        assert_eq!((s.topic(), s.from_seq), ("ticks", 7));
        let p = PublishHeader::new("ticks", "px", "text/plain", 0);
        assert_eq!(
            (p.topic(), p.event(), p.content_type()),
            ("ticks", "px", "text/plain")
        );
        let mut body = bytemuck::bytes_of(&p).to_vec();
        body.extend_from_slice(b"data!");
        let (h, rest) = read_pod_and_rest::<PublishHeader>(&body).unwrap();
        assert_eq!(h, p);
        assert_eq!(rest, b"data!");
        assert!(read_pod::<PublishHeader>(&body[..10]).is_err());
        let e = EventHeader::new(3, 4, 5, "ev", "ct");
        assert_eq!((e.event(), e.content_type()), ("ev", "ct"));
        // Over-long names are clipped, never overflow.
        let long = "x".repeat(200);
        let s = Subscribe::new(&long, 0);
        assert_eq!(s.topic().len(), MAX_TOPIC_NAME);
    }
}
