//! One client connection: a non-blocking socket, a read buffer, a write
//! buffer, the asks it has in flight and the topics it follows.
//!
//! Backpressure is by construction, never by dropping:
//!
//! - **Inbound**: while more than [`MAX_PENDING_ASKS`] asks are in flight, or
//!   the write buffer is above its high-water mark, the reactor stops
//!   reading the socket. The kernel buffer fills; the peer blocks in
//!   `write`. That is TCP doing its job.
//! - **Outbound topics**: a subscription is a cursor. Events are written
//!   only while the write buffer is below the high-water mark; otherwise the
//!   cursor simply stays where it is and delivery resumes when the socket
//!   drains. Falling out of the window is reported (`lagged`), not hidden.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::time::Instant;

use holon_actor::PendingAsk;
use holon_core::Envelope;

use crate::http::HttpState;
use crate::object::BodyStream;

/// Asks in flight per connection before the reactor stops reading it.
pub const MAX_PENDING_ASKS: usize = 4096;

/// Write buffer bytes above which topic delivery and reading pause.
pub const WBUF_HIGH: usize = 1 << 20;

/// Write buffer bytes below which they resume.
pub const WBUF_LOW: usize = 256 << 10;

/// Read buffer cap; a frame or request larger than this is a protocol error.
pub const RBUF_MAX: usize = 1 << 20;

/// Bytes read from the socket per `read` call.
const READ_CHUNK: usize = 64 << 10;

/// Which wire face a connection speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Face {
    /// Envelope frames.
    Tcp,
    /// HTTP/1.1.
    Http,
}

/// Where a reply goes once an ask completes.
#[derive(Debug)]
pub enum AskOrigin {
    /// A TCP frame: reply is `Envelope::reply_to(request)`.
    Frame(Envelope),
    /// An HTTP request: reply is a response.
    Http,
}

/// One ask in flight on a connection.
#[derive(Debug)]
pub struct InFlight {
    /// The substrate side.
    pub pending: PendingAsk,
    /// How to answer.
    pub origin: AskOrigin,
    /// When it was submitted.
    pub started: Instant,
}

/// How a subscription delivers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubMode {
    /// Every message, as long as the connection lives (TCP events, SSE).
    Stream,
    /// One HTTP long-poll: answer with up to `max` messages once any are
    /// available, or with none at `deadline`.
    LongPoll {
        /// Most messages in the answer.
        max: u32,
        /// When to answer empty.
        deadline: Instant,
    },
}

/// A topic subscription: a cursor.
#[derive(Clone, Debug)]
pub struct Subscription {
    /// Topic name.
    pub topic: String,
    /// Topic key id (`EventHeader::topic_id`).
    pub topic_id: u32,
    /// Last seq delivered.
    pub cursor: u64,
    /// Delivery was paused by backpressure with messages possibly waiting,
    /// or has not happened yet.
    pub dirty: bool,
    /// Delivery mode.
    pub mode: SubMode,
}

/// One connection.
pub struct Connection {
    /// Reactor token.
    pub token: usize,
    stream: TcpStream,
    /// Which face.
    pub face: Face,
    rbuf: Vec<u8>,
    wbuf: Vec<u8>,
    wpos: usize,
    /// Asks in flight.
    pub asks: Vec<InFlight>,
    /// Topic subscriptions.
    pub subs: Vec<Subscription>,
    /// HTTP face state.
    pub http: HttpState,
    /// The object body being streamed out, if any: the **only** field that
    /// holds a shared-memory pin across a `poll(2)` park. Dropping the
    /// connection releases it; see [`crate::object`] for every other exit.
    pub body: Option<BodyStream>,
    /// Set when the peer is gone or misbehaved; the reactor drops it.
    pub closed: bool,
    /// Close once the write buffer is flushed (HTTP `Connection: close`).
    pub close_after_flush: bool,
    /// Frames / requests handled.
    pub requests: u64,
}

impl Connection {
    /// Wrap an accepted stream (sets non-blocking + `TCP_NODELAY`).
    pub fn new(token: usize, stream: TcpStream, face: Face) -> io::Result<Connection> {
        stream.set_nonblocking(true)?;
        let _ = stream.set_nodelay(true);
        Ok(Connection {
            token,
            stream,
            face,
            rbuf: Vec::with_capacity(READ_CHUNK),
            wbuf: Vec::with_capacity(READ_CHUNK),
            wpos: 0,
            asks: Vec::new(),
            subs: Vec::new(),
            http: HttpState::default(),
            body: None,
            closed: false,
            close_after_flush: false,
            requests: 0,
        })
    }

    /// The socket fd.
    #[inline]
    pub fn fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    /// Unconsumed inbound bytes.
    #[inline]
    pub fn inbound(&self) -> &[u8] {
        &self.rbuf
    }

    /// Drop `n` consumed inbound bytes.
    pub fn consume(&mut self, n: usize) {
        self.rbuf.drain(..n);
    }

    /// Bytes queued for the socket.
    #[inline]
    pub fn outbound_len(&self) -> usize {
        self.wbuf.len() - self.wpos
    }

    /// Append to the write buffer.
    #[inline]
    pub fn out(&mut self) -> &mut Vec<u8> {
        &mut self.wbuf
    }

    /// Above the high-water mark: stop producing.
    #[inline]
    pub fn throttled(&self) -> bool {
        self.outbound_len() >= WBUF_HIGH
    }

    /// Should the reactor read this socket?
    pub fn wants_read(&self) -> bool {
        !self.closed
            && !self.close_after_flush
            && self.asks.len() < MAX_PENDING_ASKS
            && self.outbound_len() < WBUF_HIGH
            && self.rbuf.len() < RBUF_MAX
            && !self.http.busy
    }

    /// Should the reactor wait for writability?
    #[inline]
    pub fn wants_write(&self) -> bool {
        !self.closed && self.outbound_len() > 0
    }

    /// Read what is available. Returns bytes read (0 = EOF → closed).
    pub fn read(&mut self) -> io::Result<usize> {
        let start = self.rbuf.len();
        self.rbuf.resize(start + READ_CHUNK, 0);
        let mut total = 0;
        loop {
            match self.stream.read(&mut self.rbuf[start + total..]) {
                Ok(0) => {
                    self.closed = true;
                    break;
                }
                Ok(n) => {
                    total += n;
                    if start + total == self.rbuf.len() && self.rbuf.len() < RBUF_MAX {
                        let more = self.rbuf.len();
                        self.rbuf.resize(more + READ_CHUNK, 0);
                        continue;
                    }
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    self.closed = true;
                    self.rbuf.truncate(start + total);
                    return Err(e);
                }
            }
        }
        self.rbuf.truncate(start + total);
        Ok(total)
    }

    /// Write as much of the buffer as the socket takes. Returns `true` when
    /// the buffer is empty afterwards.
    pub fn flush(&mut self) -> io::Result<bool> {
        while self.wpos < self.wbuf.len() {
            match self.stream.write(&self.wbuf[self.wpos..]) {
                Ok(0) => {
                    self.closed = true;
                    return Ok(false);
                }
                Ok(n) => self.wpos += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    self.closed = true;
                    return Err(e);
                }
            }
        }
        if self.wpos == self.wbuf.len() {
            self.wbuf.clear();
            self.wpos = 0;
            if self.close_after_flush {
                self.closed = true;
            }
            return Ok(true);
        }
        // Compact once the consumed prefix dominates.
        if self.wpos > WBUF_LOW && self.wpos * 2 > self.wbuf.len() {
            self.wbuf.drain(..self.wpos);
            self.wpos = 0;
        }
        Ok(false)
    }

    /// Find a subscription by topic id.
    pub fn sub_mut(&mut self, topic_id: u32) -> Option<&mut Subscription> {
        self.subs.iter_mut().find(|s| s.topic_id == topic_id)
    }
}
