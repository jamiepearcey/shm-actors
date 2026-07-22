//! The actor-side host: connect to a coordinator, map granted segments, and
//! loan / publish / subscribe over shared memory with no hot-path syscalls.
//!
//! A [`Node`] is an ordinary process's handle onto the substrate. It registers
//! over the control socket, adopts the payload + borrow-journal segment fds the
//! coordinator passes ([`Segment::from_raw_fd`]), and then drives the pub/sub
//! data path directly in shared memory. The only syscalls on the data path are
//! the optional heartbeat (a lease renewal, off the payload path).

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::RecordBatch;
use shm_arrow::{read_batch, write_batch, PinGuard, PoolAllocator, SchemaRegistry};
use shm_core::{BorrowJournal, ChunkDesc, Pool, Segment};
use shm_ring::{Publisher, Ring, Subscriber};

use crate::error::{Error, Result};
use crate::protocol::{Request, Response};
use crate::uds::{recv_frame, send_frame};

/// A mapped topic ring segment plus the ring region length.
struct TopicRing {
    segment: Arc<Segment>,
    region_len: usize,
}

/// An actor's handle onto a running coordinator's substrate.
pub struct Node {
    name: String,
    actor_id: u32,
    payload_seg: Arc<Segment>,
    #[allow(dead_code)]
    journal_seg: Arc<Segment>,
    registry: Arc<SchemaRegistry>,
    topics: HashMap<String, TopicRing>,
    /// Guarded send side (shared with the heartbeat thread).
    send: Arc<Mutex<UnixStream>>,
    /// Read side (responses only; main thread).
    read_stream: UnixStream,
    heartbeat: Option<JoinHandle<()>>,
}

impl Node {
    /// Connect to the coordinator at `uds_path`, register as `name`, and adopt
    /// the payload + journal segments the coordinator passes back.
    ///
    /// `registry` must be seeded identically to every other actor's (the v0.1
    /// in-process schema contract) so interned `schema_id`s agree across the
    /// socket.
    pub fn connect(
        uds_path: impl AsRef<Path>,
        name: &str,
        registry: Arc<SchemaRegistry>,
    ) -> Result<Node> {
        let stream = UnixStream::connect(uds_path)?;
        let send = Arc::new(Mutex::new(stream.try_clone()?));
        let read_stream = stream;

        // --- Register handshake (expects the two segment fds). ---
        {
            let guard = send.lock().expect("send mutex poisoned");
            send_frame(&*guard, &Request::Register { name: name.into() }.encode(), &[])?;
        }
        let frame = recv_frame(&read_stream)?;
        let resp = Response::decode(&frame.body)?;
        let (actor_id, payload_seg_id, journal_seg_id) = match resp {
            Response::Registered {
                actor_id,
                payload_seg_id,
                journal_seg_id,
                ..
            } => (actor_id, payload_seg_id, journal_seg_id),
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected Registered")),
        };
        if frame.fds.len() != 2 {
            return Err(Error::MissingFds {
                expected: 2,
                received: frame.fds.len(),
            });
        }
        // Adopt the passed fds. Order matches the coordinator: payload, journal.
        let mut fds = frame.fds;
        let journal_fd = fds.pop().unwrap();
        let payload_fd = fds.pop().unwrap();
        // SAFETY: these fds were freshly received via SCM_RIGHTS from the
        // coordinator, each naming a live shm object; ownership transfers here.
        let payload_seg = Arc::new(unsafe {
            Segment::from_raw_fd(into_raw(payload_fd), payload_seg_id)?
        });
        let journal_seg = Arc::new(unsafe {
            Segment::from_raw_fd(into_raw(journal_fd), journal_seg_id)?
        });

        Ok(Node {
            name: name.into(),
            actor_id,
            payload_seg,
            journal_seg,
            registry,
            topics: HashMap::new(),
            send,
            read_stream,
            heartbeat: None,
        })
    }

    /// The actor id assigned by the coordinator.
    #[inline]
    pub fn actor_id(&self) -> u32 {
        self.actor_id
    }

    /// The actor's name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A shared handle to the payload segment (e.g. to inspect a `ChunkCtrl`).
    #[inline]
    pub fn payload_segment(&self) -> &Arc<Segment> {
        &self.payload_seg
    }

    /// Send a fire-and-forget request (no response is read).
    fn fire(&self, req: &Request) -> Result<()> {
        let guard = self.send.lock().expect("send mutex poisoned");
        send_frame(&*guard, &req.encode(), &[])
    }

    /// Send a request and read the single response it elicits.
    fn request(&self, req: &Request) -> Result<(Response, Vec<std::os::fd::OwnedFd>)> {
        {
            let guard = self.send.lock().expect("send mutex poisoned");
            send_frame(&*guard, &req.encode(), &[])?;
        }
        let frame = recv_frame(&self.read_stream)?;
        Ok((Response::decode(&frame.body)?, frame.fds))
    }

    /// Ensure `topic` exists (creating its ring), mapping the ring segment.
    fn ensure_topic(&mut self, topic: &str, subscribe: bool) -> Result<()> {
        if self.topics.contains_key(topic) {
            return Ok(());
        }
        let req = if subscribe {
            Request::Subscribe { topic: topic.into() }
        } else {
            Request::CreateTopic { topic: topic.into() }
        };
        let (resp, mut fds) = self.request(&req)?;
        let (ring_seg_id, region_len) = match resp {
            Response::Granted {
                ring_seg_id,
                region_len,
            } => (ring_seg_id, region_len as usize),
            Response::Error { message } => return Err(Error::Rejected(message)),
            _ => return Err(Error::Protocol("expected Granted")),
        };
        if fds.len() != 1 {
            return Err(Error::MissingFds {
                expected: 1,
                received: fds.len(),
            });
        }
        let ring_fd = fds.pop().unwrap();
        // SAFETY: a freshly-received SCM_RIGHTS fd naming the topic's ring shm
        // segment; ownership transfers to the mapped `Segment`.
        let segment = Arc::new(unsafe { Segment::from_raw_fd(into_raw(ring_fd), ring_seg_id)? });
        self.topics.insert(
            topic.into(),
            TopicRing {
                segment,
                region_len,
            },
        );
        Ok(())
    }

    /// Attach a [`Ring`] over a mapped topic's ring region.
    fn ring_for(&self, topic: &str) -> Result<Ring> {
        let t = self
            .topics
            .get(topic)
            .ok_or(Error::NotFound("topic not mapped"))?;
        // The ring lives in the ring segment's payload region.
        let base = t.segment.payload_ptr();
        // SAFETY: `base` points at `region_len` writable bytes the coordinator
        // initialized with `Ring::init`, mapped and kept alive by `t.segment`.
        Ok(unsafe { Ring::attach(base, t.region_len)? })
    }

    /// Loan a chunk, serialize `batch` into it, publish it to `topic`, and
    /// notify the coordinator. Returns the published descriptor.
    ///
    /// This is the full producer data path: allocate from the pool, mark the
    /// chunk `LOANED` (exclusive write), fill it via [`write_batch`], transition
    /// it to `PUBLISHED`, and broadcast the 24-byte descriptor on the ring — the
    /// payload is written exactly once.
    pub fn publish_batch(&mut self, topic: &str, batch: &RecordBatch) -> Result<ChunkDesc> {
        self.ensure_topic(topic, false)?;
        let seg = &self.payload_seg;
        let pool = Pool::attach(seg)?;
        let allocator = PoolAllocator::new(&pool, seg);

        // Serialize into a freshly-popped chunk (ctrl still FREE).
        let desc = write_batch(&allocator, &self.registry, batch)?;

        // FREE -> LOANED (exclusive) -> PUBLISHED (visible to subscribers).
        let ctrl = pool.ctrl(&desc)?;
        ctrl.try_loan(self.actor_id)?;
        ctrl.publish()?;

        // Broadcast the descriptor.
        let ring = self.ring_for(topic)?;
        Publisher::new(ring).publish(desc);

        // Tell the coordinator (control plane) which chunk is now published so it
        // can manage the exclusive-ownership handoff on the pinned ack.
        self.fire(&Request::Published { desc })?;
        Ok(desc)
    }

    /// Subscribe to `topic` from the start of its live ring history.
    ///
    /// Using `from_start` makes the consumer robust to subscribing after the
    /// producer has already published (the message is still live in the ring).
    pub fn subscribe(&mut self, topic: &str) -> Result<Subscriber> {
        self.ensure_topic(topic, true)?;
        let ring = self.ring_for(topic)?;
        Ok(Subscriber::from_start(ring))
    }

    /// Take a shared pin on a received `desc`, record it in the borrow journal,
    /// and reconstruct the `RecordBatch` **zero-copy** over the mapped chunk.
    ///
    /// The returned batch's Arrow buffers point directly into the shared segment
    /// (kept alive by an internal [`PinGuard`]); the pin (refcount + journal
    /// slot) persists in shared memory until [`release_pin`](Self::release_pin)
    /// or — if this actor dies holding it — the coordinator's crash reclaim.
    pub fn pin_and_read(&self, desc: &ChunkDesc) -> Result<Pin> {
        let seg = &self.payload_seg;
        let pool = Pool::attach(seg)?;
        let ctrl = pool.ctrl(desc)?;

        // Take the shared pin FIRST (refcount++), so the chunk cannot be
        // reclaimed out from under the read.
        ctrl.borrow_shared()?;

        // Record the pin in this actor's borrow journal for crash replay.
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        let slot = match journal.record(*desc) {
            Ok(slot) => slot,
            Err(e) => {
                // Undo the refcount bump if we cannot journal the pin.
                ctrl.release_shared();
                return Err(e.into());
            }
        };

        // Reconstruct zero-copy. The PinGuard keeps the mapping alive for every
        // buffer built over it.
        let owner = Arc::new(PinGuard::new(seg.clone()));
        let batch = read_batch(owner, ctrl, desc, &self.registry)?;

        // Notify the coordinator that the chunk is now pinned so it can release
        // the producer's exclusive ownership without racing reclamation.
        self.fire(&Request::Pinned { desc: *desc })?;

        Ok(Pin {
            desc: *desc,
            slot,
            batch,
        })
    }

    /// Release a pin taken by [`pin_and_read`](Self::pin_and_read): drop the
    /// shared refcount and clear the journal slot.
    pub fn release_pin(&self, pin: &Pin) -> Result<()> {
        let pool = Pool::attach(&self.payload_seg)?;
        if let Ok(ctrl) = pool.ctrl(&pin.desc) {
            ctrl.release_shared();
        }
        let journal = BorrowJournal::attach(&self.journal_seg)?;
        journal.release(pin.slot)?;
        Ok(())
    }

    /// Start a background heartbeat thread that renews this actor's lease every
    /// `interval`. Call once, after all request/response handshakes are done.
    pub fn start_heartbeat(&mut self, interval: Duration) {
        let send = self.send.clone();
        let handle = std::thread::spawn(move || {
            let body = Request::Heartbeat.encode();
            loop {
                std::thread::sleep(interval);
                let guard = match send.lock() {
                    Ok(g) => g,
                    Err(_) => break,
                };
                // Once the coordinator/socket is gone, stop heartbeating.
                if send_frame(&*guard, &body, &[]).is_err() {
                    break;
                }
            }
        });
        self.heartbeat = Some(handle);
    }

    /// Send a graceful goodbye so the coordinator drops the actor without
    /// treating it as a crash.
    pub fn say_bye(&self) -> Result<()> {
        self.fire(&Request::Bye)
    }
}

/// A live shared pin plus the zero-copy batch reconstructed over it.
pub struct Pin {
    /// The pinned descriptor.
    pub desc: ChunkDesc,
    /// The borrow-journal slot recording the pin.
    pub slot: usize,
    /// The reconstructed record batch (buffers point into shared memory).
    pub batch: RecordBatch,
}

/// Consume an [`OwnedFd`] into its raw fd, transferring ownership onward.
fn into_raw(fd: std::os::fd::OwnedFd) -> std::os::fd::RawFd {
    // `IntoRawFd` releases the fd from the OwnedFd without closing it; the
    // receiver (`Segment::from_raw_fd`) takes over ownership.
    use std::os::fd::IntoRawFd;
    fd.into_raw_fd()
}
