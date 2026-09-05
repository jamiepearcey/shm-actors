//! [`MessagePool`]: message chunks (envelope + inline body) in the keyed
//! store's shared data pool — allocate, write, read, free.
//!
//! A **request** chunk is popped from the pool's free list and never *loaned*
//! (`ChunkCtrl` stays `FREE`), exactly like `shm_store::write_typed_ref`: its
//! owner is whoever holds its descriptor, and ownership moves with the
//! descriptor through the task queue.
//!
//! A **reply** chunk is different, and deliberately so: the asker loans it
//! ([`loan_reply`](MessagePool::loan_reply)) and journals it, because the
//! coordinator's crash reclaim only recycles a journaled chunk that is
//! `LOANED` or `PUBLISHED` — a journaled `FREE` chunk is replayed and dropped
//! on the floor. The loan is also what makes the chunk's generation move on a
//! reclaim, which is what a handler checks before writing. See the crate docs
//! for who frees what.

use std::sync::Arc;

use holon_core::{
    decode_message, encode_message, Envelope, LocalRef, ENVELOPE_SIZE, MAX_INLINE_BODY,
    SCHEMA_ENVELOPE,
};
use shm_core::{BorrowJournal, ChunkDesc, Pool, Segment};

/// The default **reply chunk** body cap, in bytes: [`MAX_INLINE_BODY`], so an
/// ask that names no size gets the pool's smallest class exactly as before.
pub const DEFAULT_REPLY_BODY: u32 = MAX_INLINE_BODY as u32;

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

    /// Allocate an empty **reply chunk** holding an envelope plus `max_body`
    /// bytes, with a zeroed envelope so a read before any handler wrote it
    /// fails validation (`BadEnvelope`) rather than decoding garbage.
    ///
    /// The pool rounds up to a size class, so the chunk the handler is lent may
    /// be larger than asked for; [`reply_desc`](Self::reply_desc) reports the
    /// real size and that is the cap the handler is told.
    pub fn alloc_reply(&self, max_body: u32) -> Result<ChunkDesc> {
        let pool = Pool::attach(&self.seg)?;
        let mut desc = pool.alloc(ENVELOPE_SIZE as u32 + max_body)?;
        desc.schema_id = SCHEMA_ENVELOPE;
        self.with_chunk_mut(&desc, |buf| buf[..ENVELOPE_SIZE].fill(0))?;
        Ok(desc)
    }

    /// The **truthful** descriptor of the reply chunk a [`LocalRef`] names: the
    /// pool's own class size and live generation, tagged [`SCHEMA_ENVELOPE`].
    ///
    /// A `LocalRef` carries only `segment_id|offset`, so nothing about the
    /// chunk may be assumed from the sender — the length that bounds the
    /// handler's write and the generation that says whether the asker is still
    /// alive both come from the pool.
    pub fn reply_desc(&self, r: LocalRef) -> Result<ChunkDesc> {
        if r.segment_id() != self.seg.id() {
            return Err(Error::Core(holon_core::Error::BadEnvelope));
        }
        let pool = Pool::attach(&self.seg)?;
        let mut desc = pool.desc_at(r.offset())?;
        desc.schema_id = SCHEMA_ENVELOPE;
        Ok(desc)
    }

    /// Take exclusive ownership of a freshly allocated reply chunk: loan it to
    /// `owner`, then journal it. Returns the journal slot, which **is** the
    /// ownership token (ADR-0014 §4).
    ///
    /// The order is load-bearing and matches `shm-stream`'s staging recipe: a
    /// recorded-but-unloaned chunk is invisible to the coordinator's reclaim,
    /// and a loaned-but-unrecorded chunk leaks if the recorder dies between the
    /// two. On a journal-full backpressure the loan is unwound so a rejected
    /// ask leaks nothing.
    pub fn loan_reply(
        &self,
        desc: &ChunkDesc,
        owner: u32,
        journal_seg: &Arc<Segment>,
    ) -> Result<usize> {
        let pool = Pool::attach(&self.seg)?;
        let ctrl = pool.ctrl(desc)?;
        ctrl.try_loan(owner)?;
        let journal = BorrowJournal::attach(journal_seg)?;
        match journal.record(*desc) {
            Ok(slot) => Ok(slot),
            Err(e) => {
                let _ = ctrl.drop_loan();
                Err(e.into())
            }
        }
    }

    /// Release a loaned + journaled reply chunk, exactly once.
    ///
    /// [`BorrowJournal::release`] is an election: the coordinator's crash
    /// replay clears the same bit before it reclaims, so whoever observes the
    /// bit set performs the shared-memory release and the loser does nothing.
    /// Returns whether this caller won (and therefore freed).
    pub fn release_reply(
        &self,
        desc: &ChunkDesc,
        slot: usize,
        journal_seg: &Arc<Segment>,
    ) -> Result<bool> {
        let journal = BorrowJournal::attach(journal_seg)?;
        if !journal.release(slot)? {
            return Ok(false); // replay already reclaimed it
        }
        let pool = Pool::attach(&self.seg)?;
        if let Ok(ctrl) = pool.ctrl(desc) {
            let _ = ctrl.drop_loan();
        }
        pool.free(desc)?;
        Ok(true)
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

        // A reply chunk starts unreadable and becomes readable once written
        // into. Sized by the asker: envelope + 4 000 bytes lands in the 4 KiB
        // class, and `reply_desc` reports that real size, not an assumed 256.
        let reply = msgs.alloc_reply(4_000).unwrap();
        assert_eq!(reply.len, 4096);
        assert!(matches!(
            msgs.read_message(&reply),
            Err(Error::Core(holon_core::Error::BadEnvelope))
        ));
        let r = LocalRef(shm_core::PackedRef::from_desc(&reply));
        let rd = msgs.reply_desc(r).unwrap();
        assert_eq!(
            (rd.segment_id, rd.offset, rd.len, rd.generation),
            (reply.segment_id, reply.offset, reply.len, reply.generation),
            "the pool, not the sender, says how big the chunk is"
        );
        let big = vec![0xabu8; 4_000];
        let renv = Envelope::reply_to(&env, 1002, big.len() as u16);
        msgs.write_message_into(&rd, &renv, &big).unwrap();
        let (got, got_body) = msgs.read_message(&reply).unwrap();
        assert_eq!(got, renv);
        assert_eq!(got_body, &big[..]);
        msgs.free(&reply).unwrap();

        // A ref into another segment is refused before any pool attach.
        let alien = LocalRef(shm_core::PackedRef::pack(seg.id() + 1, reply.offset));
        assert!(matches!(
            msgs.reply_desc(alien),
            Err(Error::Core(holon_core::Error::BadEnvelope))
        ));

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

    #[test]
    fn loan_journal_release_is_leak_free() {
        let id = 91_000 + (std::process::id() & 0x3ff);
        let _ = Segment::unlink_by_id(id);
        let seg = Arc::new(Segment::create(id, 1 << 20).unwrap());
        Pool::create(&seg, &PoolConfig::power_of_two(256, 8192, 8)).unwrap();
        let jid = 92_000 + (std::process::id() & 0x3ff);
        let _ = Segment::unlink_by_id(jid);
        let jseg = Arc::new(Segment::create(jid, 1 << 16).unwrap());
        shm_core::BorrowJournal::create(&jseg, 8).unwrap();
        let msgs = MessagePool::new(seg.clone());
        let census = || {
            let p = Pool::attach(&seg).unwrap();
            (0..p.num_classes()).map(|c| p.free_count(c)).sum::<usize>()
        };
        let free0 = census();

        let desc = msgs.alloc_reply(192).unwrap();
        let slot = msgs.loan_reply(&desc, 7, &jseg).unwrap();
        let journal = shm_core::BorrowJournal::attach(&jseg).unwrap();
        assert_eq!(journal.len(), 1, "the borrow is on the crash ledger");
        assert_eq!(
            Pool::attach(&seg).unwrap().ctrl(&desc).unwrap().state(),
            shm_core::LOANED,
            "a FREE journaled chunk would be replayed and reclaimed as nothing"
        );
        assert!(msgs.release_reply(&desc, slot, &jseg).unwrap());
        assert!(journal.is_empty());
        assert_eq!(census(), free0, "loan + journal + release is leak-free");

        // Losing the election (replay got there first) releases nothing twice:
        // `Pool::free` validates nothing, so a double push would corrupt the
        // free list rather than be detected.
        let desc = msgs.alloc_reply(192).unwrap();
        let slot = msgs.loan_reply(&desc, 7, &jseg).unwrap();
        assert!(journal.release(slot).unwrap(), "stand in for the replayer");
        assert!(!msgs.release_reply(&desc, slot, &jseg).unwrap());
        assert_eq!(census(), free0 - 1, "the replayer owes the free, not us");

        // Journal full is backpressure, and it unwinds the loan it just took.
        let held: Vec<usize> = (0..8)
            .map(|_| journal.record(desc).expect("fill the journal"))
            .collect();
        let extra = msgs.alloc_reply(192).unwrap();
        assert!(matches!(
            msgs.loan_reply(&extra, 7, &jseg),
            Err(Error::Shm(shm_core::Error::JournalFull))
        ));
        assert_eq!(
            Pool::attach(&seg).unwrap().ctrl(&extra).unwrap().state(),
            shm_core::FREE,
            "a rejected ask leaves no loan behind"
        );
        for s in held {
            journal.release(s).unwrap();
        }
        seg.unlink().ok();
        jseg.unlink().ok();
    }
}
