//! A `poll(2)` reactor: one syscall-level wait over sockets *and* doorbells.
//!
//! Deliberately the simplest thing that is correct and portable: `poll(2)`
//! is O(fds) per wake, which is fine for the thousands of connections a
//! shard serves and is the only piece to swap for kqueue/epoll later. The
//! shape a reactor must have is decided here — registration by token,
//! interest per fd, readiness fan-out — not the syscall.

use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

/// An opaque registration key chosen by the caller.
pub type Token = usize;

/// Interest bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interest(u8);

impl Interest {
    /// Nothing (parked registration).
    pub const NONE: Interest = Interest(0);
    /// Readable.
    pub const READ: Interest = Interest(1);
    /// Writable.
    pub const WRITE: Interest = Interest(2);
    /// Both.
    pub const READ_WRITE: Interest = Interest(3);

    #[inline]
    fn events(self) -> libc::c_short {
        let mut e = 0;
        if self.0 & 1 != 0 {
            e |= libc::POLLIN;
        }
        if self.0 & 2 != 0 {
            e |= libc::POLLOUT;
        }
        e
    }
}

/// Readiness of one fd after a poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ready {
    /// The registration's token.
    pub token: Token,
    /// Readable (or hung up / errored — the read will report it).
    pub readable: bool,
    /// Writable.
    pub writable: bool,
    /// `POLLHUP` / `POLLERR` / `POLLNVAL`.
    pub hup: bool,
}

/// The reactor.
#[derive(Debug, Default)]
pub struct Reactor {
    fds: Vec<libc::pollfd>,
    tokens: Vec<Token>,
    index: HashMap<Token, usize>,
    ready: Vec<Ready>,
}

impl Reactor {
    /// An empty reactor.
    pub fn new() -> Reactor {
        Reactor::default()
    }

    /// Register `fd` under `token` with `interest`. Replaces an existing
    /// registration of the same token.
    pub fn register(&mut self, fd: RawFd, token: Token, interest: Interest) {
        if let Some(&i) = self.index.get(&token) {
            self.fds[i].fd = fd;
            self.fds[i].events = interest.events();
            return;
        }
        self.index.insert(token, self.fds.len());
        self.fds.push(libc::pollfd {
            fd,
            events: interest.events(),
            revents: 0,
        });
        self.tokens.push(token);
    }

    /// Change the interest of `token`; a no-op for an unknown token.
    pub fn set_interest(&mut self, token: Token, interest: Interest) {
        if let Some(&i) = self.index.get(&token) {
            self.fds[i].events = interest.events();
        }
    }

    /// Remove `token`.
    pub fn deregister(&mut self, token: Token) {
        let Some(i) = self.index.remove(&token) else {
            return;
        };
        let last = self.fds.len() - 1;
        self.fds.swap_remove(i);
        self.tokens.swap_remove(i);
        if i != last {
            let moved = self.tokens[i];
            self.index.insert(moved, i);
        }
    }

    /// Number of registrations.
    #[inline]
    pub fn len(&self) -> usize {
        self.fds.len()
    }

    /// `true` when nothing is registered.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.fds.is_empty()
    }

    /// Wait up to `timeout` for readiness; returns the ready set (empty on
    /// timeout). `EINTR` is reported as an empty set.
    pub fn poll(&mut self, timeout: Duration) -> io::Result<&[Ready]> {
        self.ready.clear();
        if self.fds.is_empty() {
            std::thread::sleep(timeout);
            return Ok(&self.ready);
        }
        let ms = timeout.as_millis().min(i32::MAX as u128) as libc::c_int;
        // SAFETY: `fds` is a live, contiguous `pollfd` array of the given length.
        let n = unsafe { libc::poll(self.fds.as_mut_ptr(), self.fds.len() as libc::nfds_t, ms) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                return Ok(&self.ready);
            }
            return Err(err);
        }
        if n == 0 {
            return Ok(&self.ready);
        }
        for (i, p) in self.fds.iter().enumerate() {
            let r = p.revents;
            if r == 0 {
                continue;
            }
            let hup = r & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0;
            self.ready.push(Ready {
                token: self.tokens[i],
                readable: r & libc::POLLIN != 0 || hup,
                writable: r & libc::POLLOUT != 0,
                hup,
            });
        }
        Ok(&self.ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    #[test]
    fn register_poll_deregister() {
        let (mut a, b) = UnixStream::pair().unwrap();
        let (c, d) = UnixStream::pair().unwrap();
        let mut r = Reactor::new();
        r.register(b.as_raw_fd(), 7, Interest::READ);
        r.register(d.as_raw_fd(), 8, Interest::READ);
        r.register(c.as_raw_fd(), 9, Interest::WRITE);
        assert_eq!(r.len(), 3);
        // Nothing to read yet, but 9 is writable.
        let ready = r.poll(Duration::from_millis(10)).unwrap();
        assert_eq!(ready.len(), 1);
        assert!(ready[0].token == 9 && ready[0].writable);
        r.set_interest(9, Interest::NONE);
        assert!(r.poll(Duration::from_millis(10)).unwrap().is_empty());
        a.write_all(b"x").unwrap();
        let ready = r.poll(Duration::from_millis(100)).unwrap();
        assert_eq!(ready.len(), 1);
        assert!(ready[0].token == 7 && ready[0].readable && !ready[0].hup);
        // Deregister the middle one; the swap keeps the index consistent.
        r.deregister(8);
        assert_eq!(r.len(), 2);
        drop(a);
        let ready = r.poll(Duration::from_millis(100)).unwrap();
        assert!(ready.iter().any(|x| x.token == 7 && x.readable));
        r.deregister(7);
        r.deregister(9);
        r.deregister(9);
        assert!(r.is_empty());
    }
}
