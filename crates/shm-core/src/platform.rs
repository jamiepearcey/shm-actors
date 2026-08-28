//! The platform seam: the OS operations the substrate depends on.
//!
//! Every OS-specific capability is funneled through the [`Platform`] trait so a
//! Linux fast-path implementation (memfd sealing, futex, eventfd, pidfd) can
//! slot in later **without changing semantics**. v0.1 ships [`PosixPlatform`],
//! which assumes only the POSIX baseline: `shm_open` + `ftruncate` + `mmap`, a
//! one-byte UDS write/read doorbell, and lease-based death detection.

#[cfg(not(target_os = "linux"))]
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::fd::{OwnedFd, RawFd};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::segment::Segment;

/// How the substrate detects that an actor has died.
///
/// v0.1 uses coordinator-side leases (not `pidfd`), so this is only a seam
/// describing the policy; the coordinator (in `shm-runtime`) does the timing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeathDetection {
    /// The coordinator expires a lease when an actor stops renewing it.
    LeaseBased,
    /// A Linux fast path (`pidfd`/`eventfd`) — reserved for v0.2.
    KernelNotified,
}

/// OS operations the substrate needs, behind one seam.
///
/// Correctness must never depend on any capability beyond this trait's POSIX
/// baseline contract.
pub trait Platform {
    /// Create a new shared-memory segment of `size` bytes for `id`.
    fn segment_create(&self, id: u32, size: usize) -> Result<Segment>;

    /// Attach to an existing shared-memory segment by `id`.
    fn segment_attach(&self, id: u32) -> Result<Segment>;

    /// Remove a segment's name from the namespace (existing maps stay valid).
    fn segment_unlink(&self, id: u32) -> Result<()>;

    /// Wake a waiter by writing one byte to a doorbell fd (a UDS/pipe fd in
    /// v0.1). Kept deliberately minimal so a futex/eventfd impl can replace it.
    fn doorbell_signal(&self, fd: RawFd) -> Result<()>;

    /// Block until a doorbell byte arrives on `fd`. Returns once one byte is
    /// consumed. (v0.1: a blocking one-byte read on a UDS/pipe fd.)
    fn doorbell_wait(&self, fd: RawFd) -> Result<()>;

    /// The death-detection policy this platform provides.
    fn death_detection(&self) -> DeathDetection;

    /// Create a segment hardened against resizing by fd holders, when the
    /// platform can (Linux: an anonymous sealed `memfd`, ADR-0011). The POSIX
    /// default is a plain named [`segment_create`](Self::segment_create) — per
    /// ADR-0004's item-H ruling, every new `Platform` method carries a
    /// POSIX-fallback default so correctness never depends on a fast path.
    fn segment_create_sealed(&self, id: u32, size: usize) -> Result<Segment> {
        self.segment_create(id, size)
    }

    /// The platform's deadline clock, in nanoseconds. Values are only ever
    /// compared against each other (lease/deadline arithmetic), so the epoch is
    /// platform-defined: the POSIX default is the Unix wall clock; Linux uses
    /// `CLOCK_MONOTONIC` (ADR-0011), immune to wall-clock steps.
    fn now_nanos(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

/// The v0.1 POSIX-baseline platform.
///
/// Works on macOS (dev) and Linux; carries no fast paths.
#[derive(Clone, Copy, Debug, Default)]
pub struct PosixPlatform;

impl PosixPlatform {
    /// Construct a `PosixPlatform`.
    pub const fn new() -> PosixPlatform {
        PosixPlatform
    }
}

impl Platform for PosixPlatform {
    fn segment_create(&self, id: u32, size: usize) -> Result<Segment> {
        Segment::create(id, size)
    }

    fn segment_attach(&self, id: u32) -> Result<Segment> {
        Segment::attach(id)
    }

    fn segment_unlink(&self, id: u32) -> Result<()> {
        Segment::unlink_by_id(id)
    }

    fn doorbell_signal(&self, fd: RawFd) -> Result<()> {
        let byte = [1u8; 1];
        // SAFETY: `write(2)` on a caller-owned fd with a valid buffer/len.
        let n = unsafe { libc::write(fd, byte.as_ptr().cast(), 1) };
        if n < 0 {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    fn doorbell_wait(&self, fd: RawFd) -> Result<()> {
        let mut byte = [0u8; 1];
        loop {
            // SAFETY: `read(2)` on a caller-owned fd into a valid buffer.
            let n = unsafe { libc::read(fd, byte.as_mut_ptr().cast(), 1) };
            if n < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue; // retry on EINTR
                }
                return Err(Error::Io(err));
            }
            return Ok(());
        }
    }

    fn death_detection(&self) -> DeathDetection {
        DeathDetection::LeaseBased
    }
}

// ---- v0.2 broadcast doorbell (ADR-0002 stage D) ----
//
// ADDITIVE: these helpers are the real cross-process wakeup primitive that
// backs `shm-ring`'s `DoorbellNotifier`/`DoorbellParker`. They add no on-shm
// state and change no existing signature; the v0.1 single-reader
// `doorbell_signal`/`doorbell_wait` methods above are left untouched.
//
// The primitive is a per-ring anonymous **pipe** (on Linux, an **eventfd** —
// ADR-0011 — with identical level-triggered semantics). The write-end is handed
// to publishers; the read-end to subscribers. A publish writes one byte; parked
// subscribers block in level-triggered `poll(2)` on the read-end, so a single
// byte wakes *every* subscriber already blocked in `poll` on that fd (the
// kernel wakes the fd's whole poll wait-queue — a true broadcast). On wake each
// subscriber drains the pipe, re-checks the ring `head`, and re-parks if still
// empty. Thundering-herd on wake (all wake, one drains the byte, the rest
// re-park) is accepted for v0.2. A subscriber caught in the gap between its
// emptiness re-check and entering `poll` is a rare miss made harmless by the
// bounded `poll` timeout: it re-checks the ring at worst one timeout later and
// observes the already-advanced `head`, so a wakeup is delayed, never lost.

/// A pipe-backed cross-process wakeup doorbell.
///
/// The coordinator creates one [`DoorbellPair`] per ring and keeps both ends
/// open for the ring's lifetime; it hands a duplicate of [`write`](Self::write)
/// to publishers and of [`read`](Self::read) to subscribers via `SCM_RIGHTS`.
/// Retaining both ends keeps a subscriber's `poll` from ever seeing `POLLHUP`
/// when the last publisher exits — liveness then rests on the bounded timeout.
#[derive(Debug)]
pub struct DoorbellPair {
    /// The read-end subscribers `poll`/drain (non-blocking, close-on-exec).
    pub read: OwnedFd,
    /// The write-end publishers ring (non-blocking, close-on-exec).
    pub write: OwnedFd,
}

/// Create a fresh doorbell with both ends set non-blocking + close-on-exec.
///
/// POSIX backend: an anonymous `pipe(2)` (+ `fcntl(2)`; no `pipe2`, which macOS
/// lacks). Linux backend (ADR-0011): a single **eventfd** whose two "ends" are
/// dups of one object — cheaper than a pipe (a counter, no byte queue) with
/// identical level-triggered broadcast-wake semantics, and because both ends
/// are the same object the `SCM_RIGHTS` granting protocol is unchanged.
///
/// Non-blocking is required on the read-end so [`doorbell_park`] can drain to
/// `EAGAIN`, and desirable on the write-end so [`doorbell_ring`] never blocks a
/// wait-free publish when the doorbell is full/saturated. Close-on-exec keeps
/// the ends from leaking into unrelated child processes (they are distributed
/// explicitly via `SCM_RIGHTS`, not inherited across `exec`).
pub fn doorbell_pair() -> Result<DoorbellPair> {
    #[cfg(target_os = "linux")]
    return crate::platform_linux::eventfd_doorbell_pair();
    #[cfg(not(target_os = "linux"))]
    doorbell_pair_pipe()
}

/// The POSIX pipe backend of [`doorbell_pair`].
#[cfg(not(target_os = "linux"))]
fn doorbell_pair_pipe() -> Result<DoorbellPair> {
    let mut fds = [0 as RawFd; 2];
    // SAFETY: `fds` is a valid writable 2-element array; `pipe` fills both slots.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `pipe` returned two fresh, owned descriptors we adopt exactly once.
    let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: as above for the write-end.
    let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    set_nonblock_cloexec(read.as_raw_fd())?;
    set_nonblock_cloexec(write.as_raw_fd())?;
    Ok(DoorbellPair { read, write })
}

/// Set `O_NONBLOCK` and `FD_CLOEXEC` on `fd`.
#[cfg(not(target_os = "linux"))]
fn set_nonblock_cloexec(fd: RawFd) -> Result<()> {
    // SAFETY: `fd` is a live owned descriptor; these `fcntl` calls only read and
    // rewrite its status/descriptor flags.
    unsafe {
        let fl = libc::fcntl(fd, libc::F_GETFL);
        if fl < 0 || libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK) < 0 {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
        let fd_fl = libc::fcntl(fd, libc::F_GETFD);
        if fd_fl < 0 || libc::fcntl(fd, libc::F_SETFD, fd_fl | libc::FD_CLOEXEC) < 0 {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
    }
    Ok(())
}

/// Ring a doorbell write-end (POSIX: one pipe byte; Linux: an eventfd +1).
///
/// Non-blocking and idempotent for wakeup purposes: a full pipe / saturated
/// eventfd counter (`EAGAIN`) is already readable, so the wakeup is guaranteed
/// regardless; a hung-up pipe reader (`EPIPE`) means no subscriber is
/// listening, so none needs waking. Both are treated as success so a wait-free
/// publish never fails on the doorbell.
pub fn doorbell_ring(fd: RawFd) -> Result<()> {
    #[cfg(target_os = "linux")]
    return crate::platform_linux::eventfd_ring(fd);
    #[cfg(not(target_os = "linux"))]
    doorbell_ring_pipe(fd)
}

/// The POSIX pipe backend of [`doorbell_ring`].
#[cfg(not(target_os = "linux"))]
fn doorbell_ring_pipe(fd: RawFd) -> Result<()> {
    let byte = [1u8; 1];
    loop {
        // SAFETY: writing one byte from a valid buffer to a live owned fd.
        let n = unsafe { libc::write(fd, byte.as_ptr().cast(), 1) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            match err.kind() {
                std::io::ErrorKind::Interrupted => continue,
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::BrokenPipe => return Ok(()),
                _ => return Err(Error::Io(err)),
            }
        }
        return Ok(());
    }
}

/// Block until a doorbell read-end is readable or `timeout` elapses, then drain
/// every pending byte.
///
/// Returns `Ok(true)` if the doorbell was rung (the fd became readable) and
/// `Ok(false)` on a clean timeout (or an `EINTR`, reported as a spurious
/// timeout so the caller re-checks and re-parks). Uses level-triggered
/// `poll(2)`, so every subscriber blocked here when a byte arrives is woken;
/// draining afterwards clears the level so the *next* park blocks again. The
/// caller must always re-check the ring after this returns — a wake is only a
/// hint.
pub fn doorbell_park(fd: RawFd, timeout: Duration) -> Result<bool> {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ms = timeout.as_millis().min(i32::MAX as u128) as libc::c_int;
    // SAFETY: `pfd` is a single valid `pollfd`; `poll` reads/writes just it.
    let n = unsafe { libc::poll(&mut pfd, 1, ms) };
    if n < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::Interrupted {
            return Ok(false); // spurious: caller re-checks the ring and re-parks
        }
        return Err(Error::Io(err));
    }
    let readable = n > 0 && (pfd.revents & (libc::POLLIN | libc::POLLHUP)) != 0;
    if readable {
        drain_doorbell(fd);
    }
    Ok(readable)
}

/// Drain all pending bytes from a non-blocking doorbell read-end.
fn drain_doorbell(fd: RawFd) {
    let mut buf = [0u8; 64];
    loop {
        // SAFETY: reading into a valid local buffer from a live owned fd.
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        // Stop on EAGAIN (n < 0), EOF (n == 0), or a short read that emptied it.
        if n <= 0 || (n as usize) < buf.len() {
            break;
        }
    }
}
