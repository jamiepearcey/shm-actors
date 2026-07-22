//! The platform seam: the OS operations the substrate depends on.
//!
//! Every OS-specific capability is funneled through the [`Platform`] trait so a
//! Linux fast-path implementation (memfd sealing, futex, eventfd, pidfd) can
//! slot in later **without changing semantics**. v0.1 ships [`PosixPlatform`],
//! which assumes only the POSIX baseline: `shm_open` + `ftruncate` + `mmap`, a
//! one-byte UDS write/read doorbell, and lease-based death detection.

use std::os::fd::RawFd;

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
