//! The UDS control protocol: length-prefixed, hand-rolled binary frames.
//!
//! The coordinator and actors speak a small request/response protocol over a
//! `SOCK_STREAM` Unix domain socket. Frames are tiny (well under a page), so
//! each logical message is sent with one `sendmsg` and read with one `recvmsg`;
//! passed file descriptors ride the same control message (see [`crate::uds`]).
//!
//! # Wire format
//!
//! Every frame is `u32` little-endian *body length* followed by the body. The
//! body's first byte is a [tag](tags); remaining fields are decoded per message.
//! Strings are `u16` length + UTF-8 bytes; a [`ChunkDesc`] is its 24 POD bytes.
//!
//! # Which fds accompany which message
//!
//! - [`Response::Registered`] carries **two** fds: the payload segment then the
//!   actor's borrow-journal segment.
//! - [`Response::Granted`] carries **one** fd: the topic's ring segment.
//! - All other messages carry no fds.

use shm_core::ChunkDesc;

use crate::error::{Error, Result};

/// Message tag bytes (first byte of every frame body).
pub mod tags {
    /// [`Request::Register`].
    pub const REGISTER: u8 = 1;
    /// [`Request::Heartbeat`].
    pub const HEARTBEAT: u8 = 2;
    /// [`Request::CreateTopic`].
    pub const CREATE_TOPIC: u8 = 3;
    /// [`Request::Subscribe`].
    pub const SUBSCRIBE: u8 = 4;
    /// [`Request::Published`].
    pub const PUBLISHED: u8 = 5;
    /// [`Request::Pinned`].
    pub const PINNED: u8 = 6;
    /// [`Request::Bye`].
    pub const BYE: u8 = 7;

    /// [`Response::Registered`].
    pub const REGISTERED: u8 = 128;
    /// [`Response::Granted`].
    pub const GRANTED: u8 = 129;
    /// [`Response::Ack`].
    pub const ACK: u8 = 130;
    /// [`Response::Error`].
    pub const ERROR: u8 = 131;
}

/// A message sent by an actor to the coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Register a new actor by human-readable name. Expects [`Response::Registered`].
    Register {
        /// The actor's descriptive name (e.g. `"producer"`).
        name: String,
    },
    /// Renew the actor's lease. Fire-and-forget (no response).
    Heartbeat,
    /// Ensure a publish topic (and its ring segment) exists and receive its fd.
    /// Expects [`Response::Granted`].
    CreateTopic {
        /// Topic name.
        topic: String,
    },
    /// Subscribe to a topic and receive its ring-segment fd. Expects
    /// [`Response::Granted`].
    Subscribe {
        /// Topic name.
        topic: String,
    },
    /// Notify the coordinator that the actor published `desc`. Fire-and-forget.
    Published {
        /// The published descriptor.
        desc: ChunkDesc,
    },
    /// Notify the coordinator that the actor has taken a shared pin on `desc`
    /// (so the coordinator may release the producer's exclusive ownership
    /// without racing reclamation). Fire-and-forget.
    Pinned {
        /// The pinned descriptor.
        desc: ChunkDesc,
    },
    /// Graceful goodbye; the coordinator drops the actor without reclamation.
    Bye,
}

/// A message sent by the coordinator to an actor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    /// Registration accepted; carries the payload + journal segment fds.
    Registered {
        /// The assigned actor id (>= 1).
        actor_id: u32,
        /// The actor's journal index (equals `actor_id` in v0.1).
        journal_idx: u32,
        /// The payload segment's id (for `Segment::from_raw_fd` validation).
        payload_seg_id: u32,
        /// The actor's journal segment's id.
        journal_seg_id: u32,
    },
    /// A topic ring segment was granted; carries the ring-segment fd.
    Granted {
        /// The ring segment's id.
        ring_seg_id: u32,
        /// The ring region length in bytes (`shm-ring` `required_bytes`).
        region_len: u32,
    },
    /// Generic success for a fire-and-forget-style request that still replies.
    Ack,
    /// The request was rejected; carries a human-readable reason.
    Error {
        /// Why the request failed.
        message: String,
    },
}

/// How many passed fds a decoded [`Response`] requires.
pub fn response_fd_count(resp: &Response) -> usize {
    match resp {
        Response::Registered { .. } => 2,
        Response::Granted { .. } => 1,
        _ => 0,
    }
}

// ---- Encoding helpers ----

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&bytes[..len as usize]);
}

fn put_desc(buf: &mut Vec<u8>, desc: &ChunkDesc) {
    buf.extend_from_slice(&desc_bytes(desc));
}

/// Serialize a `ChunkDesc` to its 24 raw little-endian bytes, in the frozen ABI
/// field order documented on `ChunkDesc` (keeps this crate free of a bytemuck dep).
fn desc_bytes(desc: &ChunkDesc) -> [u8; 24] {
    let mut out = [0u8; 24];
    out[0..4].copy_from_slice(&desc.segment_id.to_le_bytes());
    out[4..8].copy_from_slice(&desc.generation.to_le_bytes());
    out[8..12].copy_from_slice(&desc.offset.to_le_bytes());
    out[12..16].copy_from_slice(&desc.len.to_le_bytes());
    out[16..20].copy_from_slice(&desc.schema_id.to_le_bytes());
    out[20..24].copy_from_slice(&desc._pad.to_le_bytes());
    out
}

/// A minimal cursor over a frame body.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }

    fn u8(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or(Error::Protocol("short read: u8"))?;
        self.pos += 1;
        Ok(b)
    }

    fn u32(&mut self) -> Result<u32> {
        let end = self.pos + 4;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(Error::Protocol("short read: u32"))?;
        self.pos = end;
        Ok(u32::from_le_bytes(slice.try_into().unwrap()))
    }

    fn string(&mut self) -> Result<String> {
        let end = self.pos + 2;
        let lenb = self
            .buf
            .get(self.pos..end)
            .ok_or(Error::Protocol("short read: str len"))?;
        let len = u16::from_le_bytes(lenb.try_into().unwrap()) as usize;
        self.pos = end;
        let s = self
            .buf
            .get(self.pos..self.pos + len)
            .ok_or(Error::Protocol("short read: str body"))?;
        self.pos += len;
        String::from_utf8(s.to_vec()).map_err(|_| Error::Protocol("invalid utf-8 in string"))
    }

    fn desc(&mut self) -> Result<ChunkDesc> {
        let end = self.pos + 24;
        let b = self
            .buf
            .get(self.pos..end)
            .ok_or(Error::Protocol("short read: desc"))?;
        self.pos = end;
        Ok(ChunkDesc {
            segment_id: u32::from_le_bytes(b[0..4].try_into().unwrap()),
            generation: u32::from_le_bytes(b[4..8].try_into().unwrap()),
            offset: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            len: u32::from_le_bytes(b[12..16].try_into().unwrap()),
            schema_id: u32::from_le_bytes(b[16..20].try_into().unwrap()),
            _pad: u32::from_le_bytes(b[20..24].try_into().unwrap()),
        })
    }
}

impl Request {
    /// Encode this request into a frame body (no length prefix — [`crate::uds`]
    /// adds it).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        match self {
            Request::Register { name } => {
                buf.push(tags::REGISTER);
                put_str(&mut buf, name);
            }
            Request::Heartbeat => buf.push(tags::HEARTBEAT),
            Request::CreateTopic { topic } => {
                buf.push(tags::CREATE_TOPIC);
                put_str(&mut buf, topic);
            }
            Request::Subscribe { topic } => {
                buf.push(tags::SUBSCRIBE);
                put_str(&mut buf, topic);
            }
            Request::Published { desc } => {
                buf.push(tags::PUBLISHED);
                put_desc(&mut buf, desc);
            }
            Request::Pinned { desc } => {
                buf.push(tags::PINNED);
                put_desc(&mut buf, desc);
            }
            Request::Bye => buf.push(tags::BYE),
        }
        buf
    }

    /// Decode a request from a frame body.
    pub fn decode(body: &[u8]) -> Result<Request> {
        let mut r = Reader::new(body);
        let tag = r.u8()?;
        Ok(match tag {
            tags::REGISTER => Request::Register { name: r.string()? },
            tags::HEARTBEAT => Request::Heartbeat,
            tags::CREATE_TOPIC => Request::CreateTopic { topic: r.string()? },
            tags::SUBSCRIBE => Request::Subscribe { topic: r.string()? },
            tags::PUBLISHED => Request::Published { desc: r.desc()? },
            tags::PINNED => Request::Pinned { desc: r.desc()? },
            tags::BYE => Request::Bye,
            _ => return Err(Error::Protocol("unknown request tag")),
        })
    }
}

impl Response {
    /// Encode this response into a frame body.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        match self {
            Response::Registered {
                actor_id,
                journal_idx,
                payload_seg_id,
                journal_seg_id,
            } => {
                buf.push(tags::REGISTERED);
                put_u32(&mut buf, *actor_id);
                put_u32(&mut buf, *journal_idx);
                put_u32(&mut buf, *payload_seg_id);
                put_u32(&mut buf, *journal_seg_id);
            }
            Response::Granted {
                ring_seg_id,
                region_len,
            } => {
                buf.push(tags::GRANTED);
                put_u32(&mut buf, *ring_seg_id);
                put_u32(&mut buf, *region_len);
            }
            Response::Ack => buf.push(tags::ACK),
            Response::Error { message } => {
                buf.push(tags::ERROR);
                put_str(&mut buf, message);
            }
        }
        buf
    }

    /// Decode a response from a frame body.
    pub fn decode(body: &[u8]) -> Result<Response> {
        let mut r = Reader::new(body);
        let tag = r.u8()?;
        Ok(match tag {
            tags::REGISTERED => Response::Registered {
                actor_id: r.u32()?,
                journal_idx: r.u32()?,
                payload_seg_id: r.u32()?,
                journal_seg_id: r.u32()?,
            },
            tags::GRANTED => Response::Granted {
                ring_seg_id: r.u32()?,
                region_len: r.u32()?,
            },
            tags::ACK => Response::Ack,
            tags::ERROR => Response::Error { message: r.string()? },
            _ => return Err(Error::Protocol("unknown response tag")),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_desc() -> ChunkDesc {
        ChunkDesc {
            segment_id: 7,
            generation: 3,
            offset: 0x4000,
            len: 65536,
            schema_id: 2,
            _pad: 0,
        }
    }

    #[test]
    fn request_round_trips() {
        let cases = [
            Request::Register {
                name: "producer".into(),
            },
            Request::Heartbeat,
            Request::CreateTopic {
                topic: "demo".into(),
            },
            Request::Subscribe {
                topic: "demo".into(),
            },
            Request::Published { desc: sample_desc() },
            Request::Pinned { desc: sample_desc() },
            Request::Bye,
        ];
        for req in cases {
            let body = req.encode();
            let back = Request::decode(&body).expect("decode");
            assert_eq!(req, back, "round trip failed for {req:?}");
        }
    }

    #[test]
    fn response_round_trips() {
        let cases = [
            Response::Registered {
                actor_id: 1,
                journal_idx: 1,
                payload_seg_id: 100,
                journal_seg_id: 1124,
            },
            Response::Granted {
                ring_seg_id: 101,
                region_len: 33_024,
            },
            Response::Ack,
            Response::Error {
                message: "no such topic".into(),
            },
        ];
        for resp in cases {
            let body = resp.encode();
            let back = Response::decode(&body).expect("decode");
            assert_eq!(resp, back, "round trip failed for {resp:?}");
        }
    }

    #[test]
    fn desc_survives_wire_exactly() {
        let d = sample_desc();
        let body = Request::Published { desc: d }.encode();
        match Request::decode(&body).unwrap() {
            Request::Published { desc } => assert_eq!(desc, d),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn truncated_frame_is_rejected() {
        let body = Request::Register {
            name: "x".into(),
        }
        .encode();
        // Chop the string body off; decode must error, not panic.
        let err = Request::decode(&body[..2]);
        assert!(matches!(err, Err(Error::Protocol(_))));
    }

    #[test]
    fn unknown_tag_is_rejected() {
        assert!(matches!(
            Request::decode(&[200u8]),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            Response::decode(&[201u8]),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn fd_counts_match_messages() {
        assert_eq!(
            response_fd_count(&Response::Registered {
                actor_id: 1,
                journal_idx: 1,
                payload_seg_id: 1,
                journal_seg_id: 2,
            }),
            2
        );
        assert_eq!(
            response_fd_count(&Response::Granted {
                ring_seg_id: 1,
                region_len: 1,
            }),
            1
        );
        assert_eq!(response_fd_count(&Response::Ack), 0);
    }
}
