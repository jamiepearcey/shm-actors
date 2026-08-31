//! Linux fast paths (ADR-0011, Holon P0.4 / ADR-0004 item H).
//!
//! Raw wrappers for the five Linux primitives the substrate accelerates with,
//! plus [`LinuxPlatform`], the Linux implementation of the [`Platform`] seam:
//!
//! - **futex** ([`futex_wait`] / [`futex_wake`]) — the doorbell of record for
//!   in-shm wake words ([`shm-ring`'s reserved `doorbell_seq`]). Deliberately
//!   **without** `FUTEX_PRIVATE_FLAG`: the word lives in `MAP_SHARED` memory
//!   and must wake waiters in *other processes*.
//! - **eventfd** ([`eventfd_doorbell_pair`] / [`eventfd_ring`]) — replaces the
//!   POSIX pipe behind [`doorbell_pair`](crate::doorbell_pair) wherever a
//!   *file descriptor* is required (poll-multiplexed parks, `SCM_RIGHTS`-granted
//!   doorbells). One eventfd object serves as both "ends" of the pair, so the
//!   coordinator's granting protocol is unchanged.
//! - **memfd sealing** ([`memfd_sealed_fd`]) — anonymous, `F_SEAL_SHRINK |
//!   F_SEAL_GROW | F_SEAL_SEAL`-sealed segment backing: a hostile peer holding
//!   the granted fd can no longer `ftruncate` the object out from under mapped
//!   readers (the SIGBUS class of risk R3 in the actor-framework design).
//!   `F_SEAL_WRITE` is **never** added — segments are mutable shared state.
//! - **pidfd** ([`pidfd_open`] / [`socket_peer_pid`]) — near-instant death
//!   *acceleration* for the coordinator's lease monitor. Leases remain the
//!   correctness backstop: a pidfd detects exit, not a wedged-but-alive actor.
//! - **`CLOCK_MONOTONIC`** ([`monotonic_now_nanos`]) — the task-deadline clock.
//!   Safe cross-process because shared memory implies one host and one boot;
//!   immune to NTP steps that could spuriously reap (or immortalize) a lease.
//!
//! This module is compiled **only** on Linux (`#[cfg(target_os = "linux")]` at
//! the `lib.rs` declaration); on macOS none of it is even parsed past
//! cfg-stripping, so the POSIX baseline is untouched by construction.

use core::sync::atomic::AtomicU32;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::platform::{DeathDetection, DoorbellPair, Platform};
use crate::segment::Segment;

// ---------------------------------------------------------------------------
// futex
// ---------------------------------------------------------------------------

/// Block until `word` is woken via [`futex_wake`] or its value no longer equals
/// `expected`, with an optional bounded (relative, `CLOCK_MONOTONIC`) timeout.
///
/// Returns `Ok(true)` when the caller was woken or the value already differed
/// (`EAGAIN` — the wake it was about to miss has effectively arrived), and
/// `Ok(false)` on a clean timeout or `EINTR` (a spurious return the caller
/// handles by re-checking, exactly like the pipe parker's timeout contract).
///
/// The syscall is issued **without** `FUTEX_PRIVATE_FLAG`: the word is expected
/// to live in `MAP_SHARED` memory and wake waiters across processes.
pub fn futex_wait(word: &AtomicU32, expected: u32, timeout: Option<Duration>) -> Result<bool> {
    let ts = timeout.map(|t| libc::timespec {
        tv_sec: t.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
        tv_nsec: t.subsec_nanos() as libc::c_long,
    });
    let ts_ptr = ts
        .as_ref()
        .map_or(core::ptr::null(), |t| t as *const libc::timespec);
    // SAFETY: `word` is a live atomic (its address is valid for the call);
    // FUTEX_WAIT reads the u32 at that address and the timespec (when non-null)
    // for the duration of the syscall only.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32,
            libc::FUTEX_WAIT,
            expected,
            ts_ptr,
            core::ptr::null::<u32>(),
            0u32,
        )
    };
    if rc == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        // Value already != expected: the "missed" wake already happened.
        Some(libc::EAGAIN) => Ok(true),
        // Bounded-timeout liveness fallback; caller re-checks and re-parks.
        Some(libc::ETIMEDOUT) => Ok(false),
        // Spurious return; caller re-checks, same as the pipe parker on EINTR.
        Some(libc::EINTR) => Ok(false),
        _ => Err(Error::Io(err)),
    }
}

/// Wake up to `n` waiters blocked in [`futex_wait`] on `word` (use `i32::MAX`
/// for a broadcast). Returns the number of waiters actually woken.
///
/// Fire-and-forget best-effort in doorbell use: like
/// [`doorbell_ring`](crate::doorbell_ring), a failure here must never fail a
/// wait-free publish (a dropped wake is bounded-recovered by the parker's
/// timeout).
pub fn futex_wake(word: &AtomicU32, n: i32) -> Result<i32> {
    // SAFETY: `word` is a live atomic; FUTEX_WAKE only uses its address as the
    // wait-queue key and reads no memory through it.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32,
            libc::FUTEX_WAKE,
            n,
            core::ptr::null::<libc::timespec>(),
            core::ptr::null::<u32>(),
            0u32,
        )
    };
    if rc < 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(rc as i32)
}

// ---------------------------------------------------------------------------
// eventfd doorbell
// ---------------------------------------------------------------------------

/// Create an eventfd-backed [`DoorbellPair`]: **one** eventfd object
/// (`EFD_CLOEXEC | EFD_NONBLOCK`, non-semaphore) whose read and write "ends"
/// are two dups of the same open file description.
///
/// Both ends being one object is what keeps the coordinator's
/// `SCM_RIGHTS`-granting protocol byte-for-byte unchanged: it still retains and
/// grants a read end to subscribers and a write end to publishers. Semantics
/// match the pipe doorbell: a write makes the fd level-triggered-readable and
/// wakes *every* `poll`er; one drain (a single 8-byte read resets the counter)
/// clears the level.
pub fn eventfd_doorbell_pair() -> Result<DoorbellPair> {
    // SAFETY: plain fd-creating syscall; on success we adopt the fd exactly once.
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if raw < 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `raw` is a fresh, owned descriptor adopted exactly once.
    let read = unsafe { OwnedFd::from_raw_fd(raw) };
    // `dup` shares the open file description, so O_NONBLOCK rides along; the
    // new descriptor gets its own FD_CLOEXEC from `try_clone` (F_DUPFD_CLOEXEC).
    let write = read.try_clone().map_err(Error::Io)?;
    Ok(DoorbellPair { read, write })
}

/// Ring an eventfd doorbell: add 1 to the counter (an 8-byte write).
///
/// Non-blocking and idempotent for wakeup purposes: a saturated counter
/// (`EAGAIN`) is already readable, so the wakeup is guaranteed regardless —
/// treated as success exactly like the pipe doorbell's full-pipe case, so a
/// wait-free publish never fails on the doorbell.
pub fn eventfd_ring(fd: RawFd) -> Result<()> {
    let one: u64 = 1;
    loop {
        // SAFETY: writing 8 bytes from a valid local to a live owned fd.
        let n =
            unsafe { libc::write(fd, (&one as *const u64).cast(), core::mem::size_of::<u64>()) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            match err.kind() {
                std::io::ErrorKind::Interrupted => continue,
                std::io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(Error::Io(err)),
            }
        }
        return Ok(());
    }
}

// ---------------------------------------------------------------------------
// memfd sealing
// ---------------------------------------------------------------------------

/// Create an anonymous `memfd`-backed shm object of exactly `size` bytes,
/// sealed against resizing: `F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL`.
///
/// `SHRINK` kills the hostile-truncate SIGBUS class (a peer holding the granted
/// fd can no longer pull pages out from under mapped readers); `GROW` fixes the
/// size the header claims; `SEAL_SEAL` stops a peer from adding *further* seals
/// (e.g. `F_SEAL_FUTURE_WRITE`) that would wedge legitimate writers.
/// `F_SEAL_WRITE` is deliberately never added — segments are mutable shared
/// state. `debug_id` only names the object in `/proc/.../fd` listings; a memfd
/// has no namespace entry, so distribution is by fd (`SCM_RIGHTS`) only.
pub fn memfd_sealed_fd(debug_id: u32, size: usize) -> Result<OwnedFd> {
    let name = std::ffi::CString::new(format!("shmactr.{debug_id}"))
        .map_err(|_| Error::LayoutOverflow("memfd name contained NUL"))?;
    // SAFETY: `name` is a valid NUL-terminated string for the syscall's duration.
    let raw =
        unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) };
    if raw < 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `raw` is a fresh, owned descriptor adopted exactly once.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    // SAFETY: `ftruncate` on a live owned fd. Must precede the seals: SHRINK
    // and GROW freeze the size they find.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) } != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    let seals = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
    // SAFETY: fcntl on a live owned fd; F_ADD_SEALS takes an int argument.
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, seals) } != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(fd)
}

// ---------------------------------------------------------------------------
// pidfd death detection
// ---------------------------------------------------------------------------

/// Open a pidfd for `pid` (`pidfd_open(2)`, kernel ≥ 5.3). The fd becomes
/// `POLLIN`-readable when the process exits.
///
/// An **accelerator only**: obtaining a pid from `SO_PEERCRED` and then opening
/// it races pid reuse, so the caller must keep leases as the correctness
/// backstop (the coordinator does).
pub fn pidfd_open(pid: libc::pid_t) -> Result<OwnedFd> {
    // SAFETY: plain fd-creating syscall; on success we adopt the fd exactly once.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0u32) };
    if raw < 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `raw` is a fresh, owned descriptor adopted exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(raw as RawFd) })
}

/// The pid of the peer connected on Unix-domain socket `fd` (`SO_PEERCRED`).
pub fn socket_peer_pid(fd: RawFd) -> Result<libc::pid_t> {
    let mut cred: libc::ucred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = core::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `cred`/`len` are valid, correctly-sized out-parameters; the call
    // only writes into them.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(cred.pid)
}

// ---------------------------------------------------------------------------
// CLOCK_MONOTONIC
// ---------------------------------------------------------------------------

/// Nanoseconds on `CLOCK_MONOTONIC` — system-wide consistent across every
/// process on one boot (and shared memory implies one host/one boot), immune to
/// wall-clock steps. The Linux clock domain for `shm-task` deadlines; deadline
/// values are only ever compared against each other, never interpreted as a
/// date, so the epoch (boot) is irrelevant.
pub fn monotonic_now_nanos() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out-parameter; the vDSO call only writes it.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        return 0; // mirrors the wall-clock fallback's unwrap_or(0)
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

// ---------------------------------------------------------------------------
// LinuxPlatform
// ---------------------------------------------------------------------------

/// The Linux fast-path [`Platform`] (ADR-0011): sealed-memfd segment creation,
/// `CLOCK_MONOTONIC` time, and kernel-notified death detection. Named-segment
/// operations stay `shm_open`-backed (the dev/bench attach-by-name path is
/// portable by design).
#[derive(Clone, Copy, Debug, Default)]
pub struct LinuxPlatform;

impl LinuxPlatform {
    /// Construct a `LinuxPlatform`.
    pub const fn new() -> LinuxPlatform {
        LinuxPlatform
    }
}

impl Platform for LinuxPlatform {
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
        // The v0.1 single-reader doorbell fd is a UDS/pipe byte stream on every
        // platform; the eventfd fast path lives in the v0.2 broadcast helpers.
        crate::platform::PosixPlatform::new().doorbell_signal(fd)
    }

    fn doorbell_wait(&self, fd: RawFd) -> Result<()> {
        crate::platform::PosixPlatform::new().doorbell_wait(fd)
    }

    fn death_detection(&self) -> DeathDetection {
        DeathDetection::KernelNotified
    }

    fn segment_create_sealed(&self, id: u32, size: usize) -> Result<Segment> {
        Segment::create_sealed(id, size)
    }

    fn now_nanos(&self) -> u64 {
        monotonic_now_nanos()
    }
}
