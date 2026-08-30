//! The 64-byte [`Envelope`] — one cache line, `SharedPod`, the control word of
//! every Holon message (design §4).

use shm_core::{PackedRef, SharedPod};

use crate::error::{Error, Result};
use crate::payload::LocalRef;

/// Envelope magic `0x484f_4c4e` — the ASCII of `"HOLN"` read most-significant
/// byte first (`H`=0x48, `O`=0x4f, `L`=0x4c, `N`=0x4e).
pub const ENVELOPE_MAGIC: u32 = 0x484f_4c4e;

/// The envelope ABI version this build reads and writes.
pub const ENVELOPE_ABI_VERSION: u16 = 1;

/// The wire size of an [`Envelope`], in bytes: exactly one cache line.
pub const ENVELOPE_SIZE: usize = 64;

/// Reserved **system** schema id tagging a chunk that holds an [`Envelope`]
/// (followed by its inline body), so a peer recognises the chunk as a message
/// before reading it — the ADR-0007 §ABI convention (`1..=15` are system ids;
/// `shm_store::SCHEMA_TYPED_REF` is `1`, user schemas start at `16`).
pub const SCHEMA_ENVELOPE: u32 = 2;
const _: () = assert!(SCHEMA_ENVELOPE > 1 && SCHEMA_ENVELOPE < 16);

/// `flags` bit: the body follows the envelope inline in the same chunk;
/// [`Envelope::body_len`] bytes at offset 64.
pub const FLAG_INLINE_PAYLOAD: u16 = 1 << 0;
/// `flags` bit: `payload` is a [`LocalRef`] naming a host-scoped chunk.
pub const FLAG_LOCAL_REF: u16 = 1 << 1;
/// `flags` bit: the sender does not wait for a reply (`tell`); any reply the
/// handler produces is discarded rather than written to a reply chunk.
pub const FLAG_NO_REPLY: u16 = 1 << 2;

/// A global actor id: `host:32 | local:32`.
///
/// `host` is `0` for the local host until `holon-net` assigns real host ids;
/// `local` is either the coordinator-issued actor id of a process or, for a
/// named service, a stable name hash ([`ActorId::named`]) — a placeholder for
/// the Phase 3 registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct ActorId(pub u64);

// SAFETY: transparent over `u64`; pure POD.
unsafe impl SharedPod for ActorId {}

impl ActorId {
    /// The null id (no actor).
    pub const NONE: ActorId = ActorId(0);

    /// Pack `(host, local)`.
    #[inline]
    pub const fn new(host: u32, local: u32) -> ActorId {
        ActorId(((host as u64) << 32) | local as u64)
    }

    /// A local-host id derived from a service name (FNV-1a of the bytes, never
    /// zero). Two processes spawning the same name share the id — a worker
    /// pool over one mailbox, which is exactly the demo's `pricer` shape.
    pub fn named(name: &str) -> ActorId {
        let mut h: u32 = 0x811c_9dc5;
        for b in name.bytes() {
            h ^= b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        ActorId::new(0, h.max(1))
    }

    /// The host half.
    #[inline]
    pub const fn host(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// The local half.
    #[inline]
    pub const fn local(self) -> u32 {
        self.0 as u32
    }
}

/// The message kinds carried in [`Envelope::kind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum MessageKind {
    /// Fire-and-forget; no reply is awaited.
    Tell = 1,
    /// Request; the sender waits for a `Reply` (or `Err`) with the same `corr`.
    Ask = 2,
    /// The reply to an `Ask`.
    Reply = 3,
    /// A failed `Ask`.
    Err = 4,
}

impl MessageKind {
    /// Decode a `kind` discriminant.
    pub fn from_u16(v: u16) -> Result<MessageKind> {
        Ok(match v {
            1 => MessageKind::Tell,
            2 => MessageKind::Ask,
            3 => MessageKind::Reply,
            4 => MessageKind::Err,
            other => return Err(Error::BadKind(other)),
        })
    }

    /// The discriminant.
    #[inline]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// The 64-byte, cache-line-aligned message control word (design §4).
///
/// # Layout (frozen ABI — 64 bytes, `#[repr(C, align(64))]`, no padding)
///
/// | field         | type  | offset | meaning                                              |
/// |---------------|-------|-------:|------------------------------------------------------|
/// | `to`          | `u64` | 0      | destination [`ActorId`]                              |
/// | `from`        | `u64` | 8      | sender [`ActorId`]                                   |
/// | `corr`        | `u64` | 16     | correlation id for ask/reply; `0` = tell             |
/// | `payload`     | `u64` | 24     | [`LocalRef`] bits when `FLAG_LOCAL_REF`; else `0`    |
/// | `schema_id`   | `u32` | 32     | the body's schema ([`Payload::SCHEMA_ID`](crate::Payload::SCHEMA_ID)) |
/// | `version`     | `u32` | 36     | cell version the payload was committed at (`0` inline) |
/// | `kind`        | `u16` | 40     | [`MessageKind`] discriminant                         |
/// | `flags`       | `u16` | 42     | `FLAG_*` bits                                        |
/// | `deadline`    | `u32` | 44     | coarse ms (low 32 bits of the submit deadline in ms) |
/// | `epoch`       | `u32` | 48     | fencing token of the owning memory node (`0` today)  |
/// | `magic`       | `u32` | 52     | [`ENVELOPE_MAGIC`]                                   |
/// | `abi_version` | `u16` | 56     | [`ENVELOPE_ABI_VERSION`]                             |
/// | `body_len`    | `u16` | 58     | inline body length (`FLAG_INLINE_PAYLOAD`)           |
/// | `_reserved`   | `u32` | 60     | must be zero                                         |
///
/// The same struct is meant to be the shm mailbox slot and the TCP frame
/// header; today it rides inside a store-pool chunk (see the crate docs).
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Envelope {
    /// Destination actor.
    pub to: u64,
    /// Sender actor.
    pub from: u64,
    /// Correlation id (`0` for a tell).
    pub corr: u64,
    /// [`LocalRef`] bits when `FLAG_LOCAL_REF` is set; `0` otherwise.
    pub payload: u64,
    /// Schema id of the body.
    pub schema_id: u32,
    /// Cell version the payload was committed at; `0` for an inline body.
    pub version: u32,
    /// [`MessageKind`] discriminant.
    pub kind: u16,
    /// `FLAG_*` bits.
    pub flags: u16,
    /// Coarse deadline: the low 32 bits of the submit deadline in milliseconds.
    pub deadline: u32,
    /// Fencing epoch of the owning memory node (`0` until `holon-mem`).
    pub epoch: u32,
    /// [`ENVELOPE_MAGIC`]; validated on read.
    pub magic: u32,
    /// [`ENVELOPE_ABI_VERSION`]; validated on read.
    pub abi_version: u16,
    /// Inline body length in bytes.
    pub body_len: u16,
    /// Reserved; must be zero.
    pub _reserved: u32,
}

// SAFETY: `#[repr(C)]`, all-POD fields summing to exactly 64 bytes (asserted
// below, so the `align(64)` adds no padding), no pointers, no `Drop`.
unsafe impl SharedPod for Envelope {}

const _: () = assert!(core::mem::size_of::<Envelope>() == ENVELOPE_SIZE);
const _: () = assert!(core::mem::align_of::<Envelope>() == 64);
const _: () = assert!(core::mem::offset_of!(Envelope, to) == 0);
const _: () = assert!(core::mem::offset_of!(Envelope, from) == 8);
const _: () = assert!(core::mem::offset_of!(Envelope, corr) == 16);
const _: () = assert!(core::mem::offset_of!(Envelope, payload) == 24);
const _: () = assert!(core::mem::offset_of!(Envelope, schema_id) == 32);
const _: () = assert!(core::mem::offset_of!(Envelope, version) == 36);
const _: () = assert!(core::mem::offset_of!(Envelope, kind) == 40);
const _: () = assert!(core::mem::offset_of!(Envelope, flags) == 42);
const _: () = assert!(core::mem::offset_of!(Envelope, deadline) == 44);
const _: () = assert!(core::mem::offset_of!(Envelope, epoch) == 48);
const _: () = assert!(core::mem::offset_of!(Envelope, magic) == 52);
const _: () = assert!(core::mem::offset_of!(Envelope, abi_version) == 56);
const _: () = assert!(core::mem::offset_of!(Envelope, body_len) == 58);
const _: () = assert!(core::mem::offset_of!(Envelope, _reserved) == 60);

impl Envelope {
    /// Build an envelope with an **inline** body of `body_len` bytes.
    pub fn inline(
        kind: MessageKind,
        to: ActorId,
        from: ActorId,
        corr: u64,
        schema_id: u32,
        body_len: u16,
    ) -> Envelope {
        let mut flags = FLAG_INLINE_PAYLOAD;
        if kind == MessageKind::Tell {
            flags |= FLAG_NO_REPLY;
        }
        Envelope {
            to: to.0,
            from: from.0,
            corr,
            payload: 0,
            schema_id,
            version: 0,
            kind: kind.as_u16(),
            flags,
            deadline: 0,
            epoch: 0,
            magic: ENVELOPE_MAGIC,
            abi_version: ENVELOPE_ABI_VERSION,
            body_len,
            _reserved: 0,
        }
    }

    /// Build the reply envelope to `ask`: kind `Reply`, `to`/`from` swapped,
    /// the same `corr`, an inline body of `body_len` bytes of `schema_id`.
    pub fn reply_to(ask: &Envelope, schema_id: u32, body_len: u16) -> Envelope {
        let mut e = Envelope::inline(
            MessageKind::Reply,
            ActorId(ask.from),
            ActorId(ask.to),
            ask.corr,
            schema_id,
            body_len,
        );
        e.epoch = ask.epoch;
        e
    }

    /// Build the error reply to `ask`: kind `Err`, `to`/`from` swapped, the
    /// same `corr`, no body.
    pub fn err_to(ask: &Envelope) -> Envelope {
        let mut e = Envelope::reply_to(ask, 0, 0);
        e.kind = MessageKind::Err.as_u16();
        e
    }

    /// Name the asker-owned **reply chunk** the handler must write its reply
    /// into: `payload` carries the [`LocalRef`] and `FLAG_LOCAL_REF` is set.
    /// The reply never rides the task slot's result word, which is reusable
    /// capacity the instant the task completes.
    #[inline]
    pub fn with_reply_ref(mut self, r: LocalRef) -> Envelope {
        self.payload = r.0 .0;
        self.flags |= FLAG_LOCAL_REF;
        self
    }

    /// Stamp the coarse deadline from an absolute nanosecond deadline.
    #[inline]
    pub fn with_deadline_nanos(mut self, deadline_nanos: u64) -> Envelope {
        self.deadline = (deadline_nanos / 1_000_000) as u32;
        self
    }

    /// The decoded [`MessageKind`].
    #[inline]
    pub fn kind(&self) -> Result<MessageKind> {
        MessageKind::from_u16(self.kind)
    }

    /// The destination.
    #[inline]
    pub fn to(&self) -> ActorId {
        ActorId(self.to)
    }

    /// The sender.
    #[inline]
    pub fn from(&self) -> ActorId {
        ActorId(self.from)
    }

    /// Whether the body rides inline after the envelope.
    #[inline]
    pub fn is_inline(&self) -> bool {
        self.flags & FLAG_INLINE_PAYLOAD != 0
    }

    /// Whether the sender awaits no reply.
    #[inline]
    pub fn no_reply(&self) -> bool {
        self.flags & FLAG_NO_REPLY != 0
    }

    /// The [`LocalRef`] the envelope names, if `FLAG_LOCAL_REF` is set.
    #[inline]
    pub fn payload_ref(&self) -> Option<LocalRef> {
        (self.flags & FLAG_LOCAL_REF != 0).then_some(LocalRef(PackedRef(self.payload)))
    }

    /// Validate `magic`, `abi_version` and the reserved word.
    #[inline]
    pub fn validate(&self) -> Result<()> {
        if self.magic != ENVELOPE_MAGIC
            || self.abi_version != ENVELOPE_ABI_VERSION
            || self._reserved != 0
        {
            return Err(Error::BadEnvelope);
        }
        MessageKind::from_u16(self.kind)?;
        Ok(())
    }

    /// The envelope as its 64 wire bytes.
    #[inline]
    pub fn to_bytes(&self) -> [u8; ENVELOPE_SIZE] {
        let mut out = [0u8; ENVELOPE_SIZE];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    /// Decode + validate an envelope from at least 64 bytes (any alignment).
    pub fn from_bytes(bytes: &[u8]) -> Result<Envelope> {
        if bytes.len() < ENVELOPE_SIZE {
            return Err(Error::BadEnvelope);
        }
        let env: Envelope = bytemuck::pod_read_unaligned(&bytes[..ENVELOPE_SIZE]);
        env.validate()?;
        Ok(env)
    }
}

/// Write `env` followed by its inline `body` into `buf` (a chunk's bytes),
/// returning the number of bytes written. `env.body_len` must equal
/// `body.len()`; the caller sizes `buf` to the chunk.
pub fn encode_message(buf: &mut [u8], env: &Envelope, body: &[u8]) -> Result<usize> {
    let total = ENVELOPE_SIZE + body.len();
    if body.len() != env.body_len as usize || total > buf.len() {
        return Err(Error::BodyTooLarge {
            len: body.len(),
            max: buf.len().saturating_sub(ENVELOPE_SIZE),
        });
    }
    buf[..ENVELOPE_SIZE].copy_from_slice(bytemuck::bytes_of(env));
    buf[ENVELOPE_SIZE..total].copy_from_slice(body);
    Ok(total)
}

/// Read + validate an envelope from the front of `buf` and return it with its
/// inline body slice (empty when the body is not inline).
pub fn decode_message(buf: &[u8]) -> Result<(Envelope, &[u8])> {
    let env = Envelope::from_bytes(buf)?;
    if !env.is_inline() {
        return Ok((env, &[]));
    }
    let end = ENVELOPE_SIZE + env.body_len as usize;
    if end > buf.len() {
        return Err(Error::BadEnvelope);
    }
    Ok((env, &buf[ENVELOPE_SIZE..end]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_and_offsets_are_frozen() {
        assert_eq!(core::mem::size_of::<Envelope>(), 64);
        assert_eq!(core::mem::align_of::<Envelope>(), 64);
        assert_eq!(ENVELOPE_MAGIC, 0x484f_4c4e);
    }

    #[test]
    fn round_trip_and_validation() {
        let e = Envelope::inline(
            MessageKind::Ask,
            ActorId::named("pricer"),
            ActorId::new(0, 7),
            42,
            1001,
            24,
        )
        .with_deadline_nanos(5_000_000_000);
        assert!(e.validate().is_ok());
        assert_eq!(e.deadline, 5000);
        assert_eq!(Envelope::from_bytes(&e.to_bytes()).unwrap(), e);

        let mut bad = e;
        bad.magic ^= 1;
        assert!(matches!(
            Envelope::from_bytes(&bad.to_bytes()),
            Err(Error::BadEnvelope)
        ));
        let mut bad = e;
        bad.abi_version = 9;
        assert!(matches!(
            Envelope::from_bytes(&bad.to_bytes()),
            Err(Error::BadEnvelope)
        ));
        let mut bad = e;
        bad.kind = 99;
        assert!(matches!(
            Envelope::from_bytes(&bad.to_bytes()),
            Err(Error::BadKind(99))
        ));
        assert!(matches!(
            Envelope::from_bytes(&[0u8; 32]),
            Err(Error::BadEnvelope)
        ));
        // Unaligned input decodes (the read copies out).
        let mut shifted = [0u8; 65];
        shifted[1..].copy_from_slice(&e.to_bytes());
        assert_eq!(Envelope::from_bytes(&shifted[1..]).unwrap(), e);
    }

    #[test]
    fn reply_swaps_addresses_and_keeps_corr() {
        let ask = Envelope::inline(MessageKind::Ask, ActorId(5), ActorId(9), 77, 1, 0)
            .with_reply_ref(LocalRef(PackedRef::pack(3, 4096)));
        assert_eq!(ask.payload_ref(), Some(LocalRef(PackedRef::pack(3, 4096))));
        let e = Envelope::err_to(&ask);
        assert_eq!(e.kind().unwrap(), MessageKind::Err);
        assert_eq!(e.corr, 77);
        assert!(e.payload_ref().is_none());
        let r = Envelope::reply_to(&ask, 2, 8);
        assert_eq!(r.to, 9);
        assert_eq!(r.from, 5);
        assert_eq!(r.corr, 77);
        assert_eq!(r.kind().unwrap(), MessageKind::Reply);
        assert!(!r.no_reply());
        let t = Envelope::inline(MessageKind::Tell, ActorId(5), ActorId(9), 0, 1, 0);
        assert!(t.no_reply());
    }

    #[test]
    fn message_encode_decode() {
        let body = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let e = Envelope::inline(MessageKind::Ask, ActorId(1), ActorId(2), 3, 4, 8);
        let mut buf = [0u8; 256];
        let n = encode_message(&mut buf, &e, &body).unwrap();
        assert_eq!(n, 72);
        let (got, got_body) = decode_message(&buf).unwrap();
        assert_eq!(got, e);
        assert_eq!(got_body, &body);

        // Body length must match the envelope's claim.
        assert!(matches!(
            encode_message(&mut buf, &e, &body[..4]),
            Err(Error::BodyTooLarge { .. })
        ));
        // A truncated buffer is rejected on decode.
        assert!(matches!(
            decode_message(&buf[..70]),
            Err(Error::BadEnvelope)
        ));
        // A chunk too small for the body is rejected on encode.
        let mut tiny = [0u8; 66];
        assert!(matches!(
            encode_message(&mut tiny, &e, &body),
            Err(Error::BodyTooLarge { .. })
        ));
    }

    #[test]
    fn actor_id_packing() {
        let id = ActorId::new(3, 0xdead);
        assert_eq!(id.host(), 3);
        assert_eq!(id.local(), 0xdead);
        assert_eq!(ActorId::named("pricer"), ActorId::named("pricer"));
        assert_ne!(ActorId::named("pricer"), ActorId::named("client"));
        assert_ne!(ActorId::named("").local(), 0);
    }
}
