//! [`Payload`]: `#[repr(C)]` POD messages; [`Reply`]: what a handler returns;
//! [`LocalRef`]: the host-scoped payload descriptor that is deliberately not
//! serialisable.

use shm_core::{PackedRef, SharedPod};

use crate::error::{Error, Result};

/// The largest inline body: a 256-byte store chunk (the pool's smallest class)
/// minus the 64-byte envelope. Bigger replies go by reference ([`Reply::bytes`]).
pub const MAX_INLINE_BODY: usize = 192;

/// The largest **by-reference** reply body, in bytes.
///
/// Bounded by [`Envelope::body_len`](crate::Envelope::body_len) being a `u16`:
/// one envelope describes one body, on the shared-memory path and on the wire
/// alike, so 65 535 is the architectural ceiling for a single-envelope reply.
/// The asker's chunk bounds it further, per ask.
pub const MAX_REPLY_BODY: usize = u16::MAX as usize;

/// A `#[repr(C)]` POD message with a fixed schema id, moved as raw bytes.
///
/// The bound on [`SharedPod`] is what makes `as_bytes` sound across a process
/// boundary: no pointers, no padding, no `Drop`. `from_bytes` checks the exact
/// size and copies out unaligned, so a body read straight from a chunk needs no
/// alignment guarantee.
pub trait Payload: SharedPod {
    /// The schema id stamped into [`Envelope::schema_id`](crate::Envelope::schema_id)
    /// and used as the [`Dispatch`](crate::Dispatch) key. Application-chosen and
    /// stable; distinct per message type.
    const SCHEMA_ID: u32;

    /// The schema id (the const, as a function, for generic call sites).
    #[inline]
    fn schema_id() -> u32 {
        Self::SCHEMA_ID
    }

    /// The message as its wire bytes.
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }

    /// Decode from exactly `size_of::<Self>()` bytes (any alignment).
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let expected = core::mem::size_of::<Self>();
        if bytes.len() != expected {
            return Err(Error::BadPayload {
                expected,
                got: bytes.len(),
            });
        }
        Ok(bytemuck::pod_read_unaligned(bytes))
    }
}

/// A **host-scoped** reference to a payload chunk: a [`PackedRef`]
/// (`segment_id:32 | offset:32`) into a segment mapped on this host.
///
/// This type must never implement `Serialize` (or any wire encoding): a chunk
/// address is meaningless on another host, and the design's location
/// transparency (§4) rests on `LocalRef` and the future `GlobalRef` being
/// *different types* so the compiler keeps a local descriptor off a socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct LocalRef(pub PackedRef);

impl LocalRef {
    /// The segment id.
    #[inline]
    pub fn segment_id(self) -> u32 {
        self.0.segment_id()
    }

    /// The byte offset within the segment.
    #[inline]
    pub fn offset(self) -> u32 {
        self.0.offset()
    }
}

/// A handler's inline reply body: a schema id plus up to [`MAX_INLINE_BODY`]
/// bytes, held on the stack so a reply costs no heap allocation.
#[derive(Clone, Copy)]
pub struct InlineBody {
    schema_id: u32,
    len: u16,
    bytes: [u8; MAX_INLINE_BODY],
}

impl InlineBody {
    /// Build from raw bytes (`len <= MAX_INLINE_BODY`).
    pub fn new(schema_id: u32, body: &[u8]) -> Result<InlineBody> {
        if body.len() > MAX_INLINE_BODY {
            return Err(Error::BodyTooLarge {
                len: body.len(),
                max: MAX_INLINE_BODY,
            });
        }
        let mut bytes = [0u8; MAX_INLINE_BODY];
        bytes[..body.len()].copy_from_slice(body);
        Ok(InlineBody {
            schema_id,
            len: body.len() as u16,
            bytes,
        })
    }

    /// The body's schema id.
    #[inline]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// The body length.
    #[inline]
    pub fn len(&self) -> u16 {
        self.len
    }

    /// Whether the body is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The body bytes.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

impl core::fmt::Debug for InlineBody {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("InlineBody")
            .field("schema_id", &self.schema_id)
            .field("len", &self.len)
            .finish()
    }
}

/// A variable-length reply body: a schema id plus up to [`MAX_REPLY_BODY`]
/// bytes, written into the reply chunk the asker sized, loaned and journaled,
/// and read back **in place** by the asker.
///
/// Owned rather than borrowed so that no lifetime leaks into
/// [`Dispatch`](crate::Dispatch) and every handler signature: a handler that
/// computes its answer into a local buffer must be able to return it.
#[derive(Clone, Debug)]
pub struct ByteBody {
    schema_id: u32,
    bytes: Vec<u8>,
}

impl ByteBody {
    /// Build from owned bytes (`len <= MAX_REPLY_BODY`).
    pub fn new(schema_id: u32, bytes: impl Into<Vec<u8>>) -> Result<ByteBody> {
        let bytes = bytes.into();
        if bytes.len() > MAX_REPLY_BODY {
            return Err(Error::BodyTooLarge {
                len: bytes.len(),
                max: MAX_REPLY_BODY,
            });
        }
        Ok(ByteBody { schema_id, bytes })
    }

    /// The body's schema id.
    #[inline]
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// The body bytes.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The body length.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the body is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// What a handler returns: nothing, a POD reply, or a variable-length body
/// written by reference into the asker's chunk.
#[derive(Clone, Debug)]
pub enum Reply {
    /// No reply (a `tell`, or an ask that completes with an empty result).
    None,
    /// An inline POD reply: the 192-byte fast path, no heap allocation.
    Inline(InlineBody),
    /// A variable-length reply, capped per ask by the asker's chunk
    /// (`Cx::max_reply`).
    Bytes(ByteBody),
}

impl Reply {
    /// Reply with the POD message `p`.
    pub fn of<P: Payload>(p: &P) -> Result<Reply> {
        Ok(Reply::Inline(InlineBody::new(P::SCHEMA_ID, p.as_bytes())?))
    }

    /// Reply with `bytes` under `schema_id`, by reference.
    ///
    /// Fitting the asker's chunk is checked where the chunk is known — a body
    /// past the ask's cap fails the task rather than being truncated into
    /// something that reads as a complete answer.
    pub fn bytes(schema_id: u32, bytes: impl Into<Vec<u8>>) -> Result<Reply> {
        Ok(Reply::Bytes(ByteBody::new(schema_id, bytes)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
    struct Ping {
        a: u64,
        b: f64,
    }
    // SAFETY: repr(C), two 8-byte scalars, no padding, no pointers, no Drop.
    unsafe impl SharedPod for Ping {}
    impl Payload for Ping {
        const SCHEMA_ID: u32 = 77;
    }

    #[test]
    fn payload_round_trip_checks_size() {
        let p = Ping { a: 1, b: 2.5 };
        assert_eq!(Ping::schema_id(), 77);
        assert_eq!(Ping::from_bytes(p.as_bytes()).unwrap(), p);
        assert!(matches!(
            Ping::from_bytes(&p.as_bytes()[..8]),
            Err(Error::BadPayload {
                expected: 16,
                got: 8
            })
        ));
        // Unaligned bytes decode too.
        let mut v = [0u8; 17];
        v[1..].copy_from_slice(p.as_bytes());
        assert_eq!(Ping::from_bytes(&v[1..]).unwrap(), p);
    }

    #[test]
    fn reply_inline_caps_body() {
        let r = Reply::of(&Ping { a: 3, b: 4.0 }).unwrap();
        match r {
            Reply::Inline(b) => {
                assert_eq!(b.schema_id(), 77);
                assert_eq!(b.len(), 16);
                assert!(!b.is_empty());
            }
            Reply::None | Reply::Bytes(_) => panic!("expected inline"),
        }
        assert!(matches!(
            InlineBody::new(1, &[0u8; MAX_INLINE_BODY + 1]),
            Err(Error::BodyTooLarge { .. })
        ));
        assert!(InlineBody::new(1, &[0u8; MAX_INLINE_BODY]).is_ok());
    }

    #[test]
    fn reply_bytes_caps_at_u16() {
        assert_eq!(MAX_REPLY_BODY, u16::MAX as usize);
        let r = Reply::bytes(1234, vec![9u8; 40_000]).unwrap();
        match r {
            Reply::Bytes(b) => {
                assert_eq!(b.schema_id(), 1234);
                assert_eq!(b.len(), 40_000);
                assert!(!b.is_empty());
                assert_eq!(b.bytes()[0], 9);
            }
            Reply::None | Reply::Inline(_) => panic!("expected bytes"),
        }
        assert!(matches!(
            ByteBody::new(1, vec![0u8; MAX_REPLY_BODY + 1]),
            Err(Error::BodyTooLarge { .. })
        ));
        assert!(ByteBody::new(1, vec![0u8; MAX_REPLY_BODY]).is_ok());
    }

    #[test]
    fn local_ref_unpacks() {
        let r = LocalRef(PackedRef::pack(9, 4096));
        assert_eq!(r.segment_id(), 9);
        assert_eq!(r.offset(), 4096);
    }
}
