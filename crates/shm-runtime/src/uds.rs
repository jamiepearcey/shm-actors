//! Length-prefixed control frames over a Unix domain socket, with `SCM_RIGHTS`
//! file-descriptor passing.
//!
//! This is the real fd-crossing seam of the runtime: the coordinator maps a
//! segment and hands its descriptor to an actor by attaching the raw fd to a
//! `sendmsg` control message ([`ControlMessage::ScmRights`]); the kernel
//! installs a *duplicate* fd in the receiver, which then adopts it via
//! [`Segment::from_raw_fd`](shm_core::Segment::from_raw_fd). The same path also
//! carries a topic's non-segment doorbell pipe fd (v0.2 stage D) alongside the
//! ring-segment fd in a `Granted` reply.
//!
//! # Framing
//!
//! Each frame is a `u32` little-endian body length followed by the body. Frames
//! are small (control only — never payload), so one message is written with a
//! single `sendmsg` and read with a single `recvmsg` into a page-sized buffer;
//! the accompanying fds ride that same call. This one-send/one-recv assumption
//! holds for the sub-page control messages the runtime exchanges.

use std::io::{IoSlice, IoSliceMut};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use nix::sys::socket::{recvmsg, sendmsg, ControlMessage, ControlMessageOwned, MsgFlags};

use crate::error::{Error, Result};

/// The maximum control-frame body the runtime will send or receive.
///
/// Most messages are a few dozen bytes, but the item-E schema-coordinator frames
/// ([`InternSchema`](crate::protocol::Request::InternSchema) /
/// [`SchemaResolved`](crate::protocol::Response::SchemaResolved)) carry a
/// schema's serialized Arrow-IPC bytes inline, and a **nested** schema (item F:
/// deep `Struct`/`List` trees with metadata) can exceed the old 4 KiB cap. Raised
/// to 64 KiB — comfortably above any realistic single-schema IPC message — while
/// still bounding the per-frame receive buffer and keeping the
/// one-`recvmsg`-per-frame invariant honest. A schema larger than this is
/// rejected rather than silently truncated.
pub const MAX_FRAME: usize = 64 * 1024;

/// Send one framed control message plus optional passed fds over `sock`.
///
/// Writes a `u32` length prefix and the `body` in a single `sendmsg`, attaching
/// `fds` as one `SCM_RIGHTS` control message when non-empty.
pub fn send_frame(sock: &impl AsRawFd, body: &[u8], fds: &[RawFd]) -> Result<()> {
    assert!(body.len() <= MAX_FRAME, "control frame exceeds MAX_FRAME");
    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&(body.len() as u32).to_le_bytes());
    framed.extend_from_slice(body);

    let iov = [IoSlice::new(&framed)];
    let fd = sock.as_raw_fd();
    if fds.is_empty() {
        sendmsg::<()>(fd, &iov, &[], MsgFlags::empty(), None)?;
    } else {
        let cmsgs = [ControlMessage::ScmRights(fds)];
        sendmsg::<()>(fd, &iov, &cmsgs, MsgFlags::empty(), None)?;
    }
    Ok(())
}

/// A control frame received from the socket: the decoded body plus any adopted
/// file descriptors (already wrapped in [`OwnedFd`] so they close on drop).
pub struct Frame {
    /// The frame body (tag + fields); decode with `Request`/`Response`.
    pub body: Vec<u8>,
    /// Descriptors received via `SCM_RIGHTS`, in send order.
    pub fds: Vec<OwnedFd>,
}

/// Receive one framed control message plus any passed fds from `sock`.
///
/// Blocks until a message arrives. Returns [`Error::PeerClosed`] on a clean EOF
/// (zero-length read) so callers can distinguish a graceful disconnect from an
/// error. The whole frame (length prefix + body) is expected in one `recvmsg`.
pub fn recv_frame(sock: &impl AsRawFd) -> Result<Frame> {
    let mut buf = vec![0u8; 4 + MAX_FRAME];
    // Space for a generous SCM_RIGHTS payload (up to 8 fds).
    let mut cmsg_space = nix::cmsg_space!([RawFd; 8]);
    // Filled inside the borrow scope below; outlives it so the `Frame` can own it.
    let mut fds: Vec<OwnedFd> = Vec::new();

    let n = {
        let mut iov = [IoSliceMut::new(&mut buf)];
        let msg = recvmsg::<()>(
            sock.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg_space),
            MsgFlags::empty(),
        )?;

        // Adopt any passed fds before touching the byte payload.
        for cmsg in msg.cmsgs()? {
            if let ControlMessageOwned::ScmRights(raw) = cmsg {
                for r in raw {
                    // SAFETY: `r` is a freshly-installed, owned fd the kernel
                    // placed in this process via SCM_RIGHTS; we take sole
                    // ownership exactly once here.
                    fds.push(unsafe { owned_from_raw(r) });
                }
            }
        }
        msg.bytes
    };

    if n == 0 {
        return Err(Error::PeerClosed);
    }
    if n < 4 {
        return Err(Error::Protocol("frame shorter than length prefix"));
    }
    let body_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if body_len > MAX_FRAME || 4 + body_len > n {
        return Err(Error::Protocol("frame body length exceeds received bytes"));
    }
    let body = buf[4..4 + body_len].to_vec();
    Ok(Frame { body, fds })
}

/// Wrap a raw fd received over `SCM_RIGHTS` in an [`OwnedFd`].
///
/// # Safety
///
/// `raw` must be a valid, freshly-received owned descriptor whose ownership is
/// transferred here exactly once.
unsafe fn owned_from_raw(raw: RawFd) -> OwnedFd {
    use std::os::fd::FromRawFd;
    // SAFETY: forwarded from the caller's contract.
    unsafe { OwnedFd::from_raw_fd(raw) }
}
