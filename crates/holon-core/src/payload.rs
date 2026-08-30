//! [`Payload`]: `#[repr(C)]` POD messages; [`Reply`]: what a handler returns;
//! [`LocalRef`]: the host-scoped payload descriptor that is deliberately not
//! serialisable.

use shm_core::{PackedRef, SharedPod};

use crate::error::{Error, Result};

/// The largest inline body: a 256-byte store chunk (the pool's smallest class)
/// minus the 64-byte envelope. Bigger payloads belong in a cell.
pub const MAX_INLINE_BODY: usize = 192;

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

/// What a handler returns: nothing, or a POD reply written to a reply chunk.
#[derive(Clone, Copy, Debug)]
pub enum Reply {
    /// No reply (a `tell`, or an ask that completes with an empty result).
    None,
    /// An inline POD reply.
    Inline(InlineBody),
}

impl Reply {
    /// Reply with the POD message `p`.
    pub fn of<P: Payload>(p: &P) -> Result<Reply> {
        Ok(Reply::Inline(InlineBody::new(P::SCHEMA_ID, p.as_bytes())?))
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
            Reply::None => panic!("expected inline"),
        }
        assert!(matches!(
            InlineBody::new(1, &[0u8; MAX_INLINE_BODY + 1]),
            Err(Error::BodyTooLarge { .. })
        ));
        assert!(InlineBody::new(1, &[0u8; MAX_INLINE_BODY]).is_ok());
    }

    #[test]
    fn local_ref_unpacks() {
        let r = LocalRef(PackedRef::pack(9, 4096));
        assert_eq!(r.segment_id(), 9);
        assert_eq!(r.offset(), 4096);
    }
}
