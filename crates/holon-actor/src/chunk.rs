//! [`MessagePool`]: message chunks (envelope + inline body) in the keyed
//! store's shared data pool — allocate, write, read, free.
//!
//! The chunk is popped from the pool's free list and never *loaned*
//! (`ChunkCtrl` stays `FREE`), exactly like `shm_store::write_typed_ref`: the
//! chunk's owner is whoever holds its descriptor, and ownership moves with the
//! descriptor through the task queue. See the crate docs for who frees what.

use std::sync::Arc;

use holon_core::{decode_message, encode_message, Envelope, LocalRef, ENVELOPE_SIZE, SCHEMA_ENVELOPE};
use shm_core::{ChunkDesc, Pool, Segment};

/// The size of the **reply chunk** an asker allocates and a handler writes
/// into: the pool's smallest class. The asker allocates it, names it in the
/// request envelope as a [`LocalRef`], and frees it — a handler only ever
/// resolves the ref and writes at most this many bytes.
pub const REPLY_CHUNK_BYTES: u32 = 256;

use crate::error::{Error, Result};

/// A handle onto the store's shared data segment for message chunks.
///
/// Cheap to clone (one `Arc`). Every method attaches a `Pool` view over the
/// segment header on the fly — the same per-call attach the runtime's
/// `write_ref_chunk`/`free_ref_chunk` do; it is a few loads, no syscall.
#[derive(Clone)]
pub struct MessagePool {
    seg: Arc<Segment>,
}

impl MessagePool {
    /// Wrap the store's data segment (see `KeyedStore::data_segment`).
    pub fn new(seg: Arc<Segment>) -> MessagePool {
        MessagePool { seg }
    }

    /// The underlying segment.
    #[inline]
    pub fn segment(&self) -> &Arc<Segment> {
        &self.seg
    }

    /// Allocate a chunk, write `env` + `body` into it, and return its
    /// descriptor tagged [`SCHEMA_ENVELOPE`]. The chunk is sized to the smallest
    /// class that fits `64 + body.len()` bytes.
    pub fn write_message(&self, env: &Envelope, body: &[u8]) -> Result<ChunkDesc> {
        let pool = Pool::attach(&self.seg)?;
        let need = (ENVELOPE_SIZE + body.len()) as u32;
        let mut desc = pool.alloc(need)?;
        desc.schema_id = SCHEMA_ENVELOPE;
        if let Err(e) = self.write_message_into(&desc, env, body) {
            let _ = pool.free(&desc);
            return Err(e);
        }
        Ok(desc)
    }

    /// Allocate an empty **reply chunk** ([`REPLY_CHUNK_BYTES`]) with a zeroed
    /// envelope, so a read before any handler wrote it fails validation
    /// (`BadEnvelope`) rather than decoding garbage.
    pub fn alloc_reply(&self) -> Result<ChunkDesc> {
        let pool = Pool::attach(&self.seg)?;
        let mut desc = pool.alloc(REPLY_CHUNK_BYTES)?;
        desc.schema_id = SCHEMA_ENVELOPE;
        self.with_chunk_mut(&desc, |buf| buf[..ENVELOPE_SIZE].fill(0))?;
        Ok(desc)
    }

    /// The descriptor of a reply chunk named by a [`LocalRef`] (always
    /// [`REPLY_CHUNK_BYTES`] long, tagged [`SCHEMA_ENVELOPE`]).
    #[inline]
    pub fn reply_desc(&self, r: LocalRef) -> ChunkDesc {
        ChunkDesc {
            segment_id: r.segment_id(),
            generation: 0,
            offset: r.offset(),
            len: REPLY_CHUNK_BYTES,
            schema_id: SCHEMA_ENVELOPE,
            _pad: 0,
        }
    }

    /// Write `env` + `body` into an existing chunk the caller owns (or has been
    /// lent by its owner through a [`LocalRef`]).
    pub fn write_message_into(&self, desc: &ChunkDesc, env: &Envelope, body: &[u8]) -> Result<()> {
        self.with_chunk_mut(desc, |buf| encode_message(buf, env, body))??;
        Ok(())
    }

    /// Run `f` over the chunk's bytes, bounds-checked against this segment.
    fn with_chunk_mut<R>(&self, desc: &ChunkDesc, f: impl FnOnce(&mut [u8]) -> R) -> Result<R> {
        if desc.segment_id != self.seg.id()
            || (desc.offset as usize) < shm_core::segment::HEADER_SIZE
            || (desc.offset as usize).saturating_add(desc.len as usize) > self.seg.size()
        {
            return Err(Error::Core(holon_core::Error::BadEnvelope));
        }
        // SAFETY: bounds checked above against the live mapping; the caller owns
        // the chunk (popped it, or holds its descriptor from a claim / a
        // `LocalRef` its owner lent it), so no one else writes it while `f`
        // runs, and the borrow does not escape this call.
        let buf = unsafe {
            core::slice::from_raw_parts_mut(
                self.seg.base_ptr().add(desc.offset as usize),
                desc.len as usize,
            )
        };
        Ok(f(buf))
    }

    /// Read + validate the envelope the chunk `desc` names and borrow its
    /// inline body. The slice is valid until the chunk is freed or reused —
    /// i.e. for as long as the caller owns the descriptor.
    pub fn read_message(&self, desc: &ChunkDesc) -> Result<(Envelope, &[u8])> {
        if desc.schema_id != SCHEMA_ENVELOPE {
            return Err(Error::Core(holon_core::Error::NotEnvelope(desc.schema_id)));
        }
        if desc.segment_id != self.seg.id()
            || (desc.offset as usize).saturating_add(desc.len as usize) > self.seg.size()
        {
            return Err(Error::Core(holon_core::Error::BadEnvelope));
        }
        // SAFETY: bounds checked above against the live mapping; the caller
        // owns the chunk (holds its descriptor from a claim / a completed task),
        // so nothing writes it while the slice is alive.
        let buf = unsafe {
            core::slice::from_raw_parts(
                self.seg.base_ptr().add(desc.offset as usize),
                desc.len as usize,
            )
        };
        Ok(decode_message(buf)?)
    }

    /// Return a message chunk to the pool's free list.
    pub fn free(&self, desc: &ChunkDesc) -> Result<()> {
        let pool = Pool::attach(&self.seg)?;
        pool.free(desc)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holon_core::{ActorId, MessageKind};
    use shm_core::PoolConfig;

    #[test]
    fn write_read_free_round_trip() {
        let id = 90_000 + (std::process::id() & 0x3ff);
        let _ = Segment::unlink_by_id(id);
        let seg = Arc::new(Segment::create(id, 1 << 20).unwrap());
        Pool::create(&seg, &PoolConfig::power_of_two(256, 8192, 8)).unwrap();
        let msgs = MessagePool::new(seg.clone());
        let free0: usize = {
            let p = Pool::attach(&seg).unwrap();
            (0..p.num_classes()).map(|c| p.free_count(c)).sum()
        };

        let body = [7u8; 24];
        let env = Envelope::inline(MessageKind::Ask, ActorId(1), ActorId(2), 9, 1001, 24);
        let desc = msgs.write_message(&env, &body).unwrap();
        assert_eq!(desc.schema_id, SCHEMA_ENVELOPE);
        assert_eq!(desc.len, 256, "smallest class holds envelope + 24 B body");
        let (got, got_body) = msgs.read_message(&desc).unwrap();
        assert_eq!(got, env);
        assert_eq!(got_body, &body);

        // A raw descriptor is not a message.
        let mut raw = desc;
        raw.schema_id = 0;
        assert!(matches!(
            msgs.read_message(&raw),
            Err(Error::Core(holon_core::Error::NotEnvelope(0)))
        ));
        // A descriptor for another segment is rejected before any read.
        let mut other = desc;
        other.segment_id += 1;
        assert!(matches!(
            msgs.read_message(&other),
            Err(Error::Core(holon_core::Error::BadEnvelope))
        ));

        // A reply chunk starts unreadable and becomes readable once written into.
        let reply = msgs.alloc_reply().unwrap();
        assert!(matches!(
            msgs.read_message(&reply),
            Err(Error::Core(holon_core::Error::BadEnvelope))
        ));
        let r = LocalRef(shm_core::PackedRef::from_desc(&reply));
        let rd = msgs.reply_desc(r);
        assert_eq!((rd.segment_id, rd.offset, rd.len), (reply.segment_id, reply.offset, 256));
        let renv = Envelope::reply_to(&env, 1002, 4);
        msgs.write_message_into(&rd, &renv, &[1, 2, 3, 4]).unwrap();
        let (got, got_body) = msgs.read_message(&reply).unwrap();
        assert_eq!(got, renv);
        assert_eq!(got_body, &[1, 2, 3, 4]);
        msgs.free(&reply).unwrap();

        msgs.free(&desc).unwrap();
        let free1: usize = {
            let p = Pool::attach(&seg).unwrap();
            (0..p.num_classes()).map(|c| p.free_count(c)).sum()
        };
        assert_eq!(free0, free1, "write + free is leak-free");

        // A body too big for any class is rejected and leaks nothing.
        let big = vec![0u8; 8192];
        let env = Envelope::inline(MessageKind::Ask, ActorId(1), ActorId(2), 9, 1, 8192);
        assert!(msgs.write_message(&env, &big).is_err());
        let free2: usize = {
            let p = Pool::attach(&seg).unwrap();
            (0..p.num_classes()).map(|c| p.free_count(c)).sum()
        };
        assert_eq!(free0, free2);
        seg.unlink().ok();
    }
}
