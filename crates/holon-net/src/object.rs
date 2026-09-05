//! `GET /objects/{key}` — a **streamed** read of one keyed-store object
//! ([`holon_object`]).
//!
//! Every other HTTP writer in this crate builds a complete body and copies it
//! into the connection's write buffer in one `extend_from_slice`. An object is
//! the one body whose size is not bounded by a frame, an envelope or a poll
//! budget, so it gets the opposite treatment: the response head is written once,
//! and the payload is then pumped out of shared memory in slices that are cut to
//! whatever the write buffer has left below [`WBUF_HIGH`], resuming on the next
//! reactor pass. Peak resident cost of a transfer is therefore one high-water
//! mark, not one object.
//!
//! # The pin is the thing to get right
//!
//! A [`BodyStream`] is the first piece of state in this crate that holds a
//! *shared-memory resource* across a `poll(2)` park: an [`ObjectRead`] carries
//! the journaled [`VersionPin`](shm_artifact::VersionPin) that keeps the bytes
//! it borrows readable while later versions land beside them. Held for the life
//! of the transfer — that is the point, it is what makes the slices safe to read
//! across passes — and released on **every** exit:
//!
//! | exit | site |
//! |---|---|
//! | last byte queued | [`advance`] completes and drops the stream |
//! | client disconnects, write fails, protocol error, HUP | `gateway::close_conn` drops the `Connection`, and with it the stream |
//! | shard shutdown | the same `close_conn` sweep over every live token |
//! | no progress for `object_stall` | [`tick`] |
//!
//! The pin is journaled, so a `kill -9` of the shard is crash-reclaimed by the
//! coordinator. Everything above is what keeps a *clean* exit honest.
//!
//! # Framing
//!
//! `Content-Length` from the pinned length — known exactly at pin time, so
//! there is no need for chunked framing (which the request parser refuses in
//! the other direction anyway) and keep-alive composes unchanged. `ETag`
//! carries the version. While a body is in flight the connection is
//! [`HttpState::busy`](crate::http::HttpState::busy), so the reactor stops
//! reading and a pipelined request cannot interleave itself into a half-written
//! response.

use std::time::{Duration, Instant};

use holon_object::{Error as ObjectError, ObjectRead, Version};

use crate::conn::{Connection, WBUF_HIGH};
use crate::error::{Error, Result};
use crate::gateway::ShardCtx;
use crate::http::Request;

/// One object body in flight on a connection: the pinned payload and how far
/// into it the writer has got.
///
/// Deliberately owns its [`ObjectRead`] rather than borrowing one: an
/// `ObjectRead` has no lifetime parameter (its pin is an `Arc`), which is what
/// lets a transfer outlive the reactor pass that started it.
pub struct BodyStream {
    read: ObjectRead,
    /// Index of the segment the next byte comes from.
    seg: usize,
    /// How far into `seg` the writer has got.
    seg_offset: usize,
    /// Bytes queued across all segments (what `Content-Length` counts down).
    offset: usize,
    last_progress: Instant,
}

impl BodyStream {
    /// The version being served.
    #[inline]
    pub fn version(&self) -> u64 {
        self.read.version()
    }

    /// Total body length.
    #[inline]
    pub fn total(&self) -> usize {
        self.read.len()
    }

    /// Bytes already queued for the socket.
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Bytes still to queue.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.total() - self.offset
    }
}

impl std::fmt::Debug for BodyStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodyStream")
            .field("version", &self.version())
            .field("offset", &self.offset)
            .field("total", &self.total())
            .finish()
    }
}

fn http_err(status: u16, reason: &'static str) -> Error {
    Error::Http { status, reason }
}

/// Bytes to queue this pass: whatever fits under [`WBUF_HIGH`], and never more
/// than one high-water mark in a single append. Zero means "throttled, come
/// back when the socket has drained".
#[inline]
fn slice_len(queued: usize, remaining: usize) -> usize {
    WBUF_HIGH.saturating_sub(queued).min(remaining)
}

/// Serve `GET /objects/{key}[?version=N]`.
///
/// `?version=` is an assert-match against the current version, which is all the
/// substrate offers (see [`holon_object::Version::IfCurrent`]): a version the object
/// has moved past is **410 Gone**, a version it has not reached is 404, an
/// unknown key is 404. On success the head goes out immediately and the body
/// starts pumping in the same pass.
pub fn get(ctx: &mut ShardCtx<'_>, conn: &mut Connection, req: &Request, key: &str) -> Result<()> {
    let want = match req.param("version") {
        None => Version::Latest,
        Some(v) => Version::IfCurrent(
            v.parse::<u64>()
                .map_err(|_| http_err(400, "version must be a u64"))?,
        ),
    };
    let read = match ctx.objects.get(key, want) {
        Ok(r) => r,
        Err(ObjectError::NotFound | ObjectError::BadKey(_)) => {
            return Err(http_err(404, "no such object"))
        }
        // Superseded: the bytes that version named are gone for good, and a
        // caller that re-reads gets different content. That is 410, not 404.
        Err(ObjectError::VersionMismatch { requested, current }) if requested < current => {
            return Err(http_err(410, "version no longer retained"))
        }
        // Asked for a version this object has not reached.
        Err(ObjectError::VersionMismatch { .. }) => return Err(http_err(404, "no such version")),
        Err(ObjectError::BadObject) => return Err(http_err(500, "stored object has a bad shape")),
        Err(_) => return Err(http_err(503, "object read failed")),
    };

    let now = Instant::now();
    // `check_content_type` bounds this to printable ASCII at write time, so it
    // cannot inject a header line here.
    let ct = match read.content_type() {
        "" => "application/octet-stream",
        c => c,
    };
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nETag: \"{}\"\r\n{}\r\n",
        read.len(),
        read.version(),
        if conn.http.close {
            "Connection: close\r\n"
        } else {
            ""
        },
    );
    conn.out().extend_from_slice(head.as_bytes());
    conn.http.last_write = now;
    // Busy until the last byte is queued: no pipelined request may be read into
    // the middle of this response, and no other writer may share the buffer.
    conn.http.busy = true;
    conn.body = Some(BodyStream {
        read,
        seg: 0,
        seg_offset: 0,
        offset: 0,
        last_progress: now,
    });
    // Pump the first slice in this pass so a small object costs no extra
    // round through the reactor.
    advance(conn, now);
    Ok(())
}

/// Queue as much of the in-flight body as fits below [`WBUF_HIGH`], and finish
/// the response once the last byte is queued.
///
/// A no-op for a connection with no body, and a no-op while the write buffer is
/// at or above the high-water mark — which is exactly the backpressure a topic
/// subscription gets, expressed as an offset instead of a cursor.
pub fn advance(conn: &mut Connection, now: Instant) {
    // Taken out so the pinned slice and the write buffer can be borrowed at
    // once; put back below unless the transfer is done.
    let Some(mut b) = conn.body.take() else {
        return;
    };
    // The payload is segments, not one slice, so a pass queues from as many of
    // them as its budget spans — each `extend_from_slice` straight out of shared
    // memory, no intermediate buffer, and the segment boundary is invisible on
    // the wire.
    let mut budget = slice_len(conn.outbound_len(), b.remaining());
    while budget > 0 {
        let Some(seg) = b.read.segment(b.seg) else {
            break;
        };
        // An empty payload is one empty segment; step over it rather than spin.
        let n = (seg.len() - b.seg_offset).min(budget);
        if n > 0 {
            conn.out()
                .extend_from_slice(&seg[b.seg_offset..b.seg_offset + n]);
            b.seg_offset += n;
            b.offset += n;
            budget -= n;
            b.last_progress = now;
            conn.http.last_write = now;
        }
        if b.seg_offset == seg.len() {
            b.seg += 1;
            b.seg_offset = 0;
        } else {
            break;
        }
    }
    if b.offset >= b.total() {
        // Completion: the pin dies here, before the socket has necessarily
        // drained. The bytes it protected are already copied into the write
        // buffer, so there is nothing left to protect.
        drop(b);
        finish(conn);
    } else {
        conn.body = Some(b);
    }
}

/// End a response whose body is fully queued.
fn finish(conn: &mut Connection) {
    conn.body = None;
    conn.http.busy = false;
    if conn.http.close {
        conn.close_after_flush = true;
    }
}

/// Drop the in-flight transfer and release its pin. Returns `true` if there was
/// one.
///
/// The explicit release site. `Connection`'s own drop covers the teardown
/// paths, but a stall has no teardown of its own.
pub fn release(conn: &mut Connection) -> bool {
    conn.body.take().is_some()
}

/// Timer pass: abandon a transfer that has made no progress for `stall`.
///
/// A reader that stops reading would otherwise hold a store version alive for
/// as long as it keeps the socket open. The head is already on the wire, so
/// there is no status code left to send: the connection is closed and the peer
/// sees a short body, which `Content-Length` makes unambiguous.
pub fn tick(conn: &mut Connection, now: Instant, stall: Duration) {
    let stalled = conn
        .body
        .as_ref()
        .is_some_and(|b| now.duration_since(b.last_progress) > stall);
    if stalled {
        release(conn);
        conn.http.busy = false;
        conn.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slice_never_crosses_the_high_water_mark() {
        assert_eq!(slice_len(0, 8 << 20), WBUF_HIGH);
        assert_eq!(slice_len(200, 8 << 20), WBUF_HIGH - 200);
        assert_eq!(slice_len(0, 7), 7);
        // At or over the mark nothing is produced: the transfer parks and the
        // socket's `POLLOUT` restarts it.
        assert_eq!(slice_len(WBUF_HIGH, 8 << 20), 0);
        assert_eq!(slice_len(WBUF_HIGH + 5, 8 << 20), 0);
        // An 8 MiB body is eight passes, and no pass ever holds more than one
        // high-water mark of it.
        let mut left = 8usize << 20;
        let mut passes = 0;
        while left > 0 {
            let n = slice_len(0, left);
            assert!(n <= WBUF_HIGH);
            left -= n;
            passes += 1;
        }
        assert_eq!(passes, 8);
    }
}
