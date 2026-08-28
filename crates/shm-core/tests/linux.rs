//! Linux fast-path integration tests (ADR-0011, Holon P0.4).
//!
//! Compiled and run **only on Linux** — on the dev macOS box these execute in
//! a container (`scripts/linux-test.sh`); each test fails if the corresponding
//! syscall contract is misunderstood.
#![cfg(target_os = "linux")]

use core::sync::atomic::{AtomicU32, Ordering};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shm_core::{
    doorbell_pair, doorbell_park, doorbell_ring, futex_wait, futex_wake, monotonic_now_nanos,
    pidfd_open, DeathDetection, LinuxPlatform, NativePlatform, Platform, Segment,
};

/// The eventfd-backed doorbell keeps the pipe doorbell's contract: ring →
/// park observes it (level-triggered), a drain clears the level, and an
/// un-rung park times out `false`.
#[test]
fn eventfd_doorbell_rings_parks_and_drains() {
    let db = doorbell_pair().expect("eventfd doorbell pair");

    // Ring twice on the write end; one park sees it and drains everything.
    doorbell_ring(db.write.as_raw_fd()).expect("ring 1");
    doorbell_ring(db.write.as_raw_fd()).expect("ring 2");
    let woken = doorbell_park(db.read.as_raw_fd(), Duration::from_secs(5)).expect("park");
    assert!(woken, "a rung doorbell must wake the parker");

    // The drain reset the counter: the next park must cleanly time out.
    let start = Instant::now();
    let woken = doorbell_park(db.read.as_raw_fd(), Duration::from_millis(50)).expect("park 2");
    assert!(!woken, "a drained doorbell must not report readable");
    assert!(
        start.elapsed() >= Duration::from_millis(40),
        "an un-rung park must actually block for the bounded timeout"
    );
}

/// `futex_wait` blocks until `futex_wake`; a stale `expected` value returns
/// immediately (the kernel's value check is the lost-wake guard).
#[test]
fn futex_wake_wakes_a_parked_waiter_and_stale_value_returns_immediately() {
    // Stale expected: word == 7, wait expecting 6 → EAGAIN → Ok(true) now.
    let word = AtomicU32::new(7);
    let start = Instant::now();
    let woken = futex_wait(&word, 6, Some(Duration::from_secs(5))).expect("stale wait");
    assert!(woken, "a value mismatch must report as an arrived wake");
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "a value mismatch must not block"
    );

    // Real park: a thread waits on the current value; we bump + wake it.
    let word = Arc::new(AtomicU32::new(0));
    let w = word.clone();
    let waiter = std::thread::spawn(move || {
        let observed = w.load(Ordering::Acquire);
        // Generous bound: the test asserts the wake, the bound only guards CI.
        futex_wait(&w, observed, Some(Duration::from_secs(10))).expect("wait")
    });
    // Give the waiter time to actually enter the syscall.
    std::thread::sleep(Duration::from_millis(100));
    word.fetch_add(1, Ordering::Release);
    let woken_n = futex_wake(&word, i32::MAX).expect("wake");
    let start = Instant::now();
    let woken = waiter.join().expect("join waiter");
    assert!(woken, "the waiter must report woken (or value-mismatch)");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "the wake must be prompt, not the timeout"
    );
    // Either we genuinely woke it (1) or it hadn't entered the wait yet and
    // the value check caught the bump (0); both are within contract.
    assert!(woken_n >= 0);
}

/// A sealed memfd segment rejects `ftruncate` in both directions with `EPERM`
/// while existing mappings stay valid — the hostile-truncate SIGBUS guard.
#[test]
fn sealed_segment_rejects_resize_and_keeps_mappings_valid() {
    let seg = Segment::create_sealed(90_001, 16 * 1024).expect("create sealed");
    assert_eq!(seg.id(), 90_001);
    assert_eq!(seg.size(), 16 * 1024);

    // Write through the mapping first (the payload is ours alone here).
    // SAFETY: payload_ptr covers payload_len writable bytes we exclusively own.
    unsafe { seg.payload_ptr().write(0xAB) };

    // Shrink and grow must both fail with EPERM.
    for new_len in [4096i64, 64 * 1024] {
        // SAFETY: ftruncate on a live owned fd.
        let rc = unsafe { libc::ftruncate(seg.as_raw_fd(), new_len as libc::off_t) };
        assert_eq!(rc, -1, "resize to {new_len} must be refused");
        let err = std::io::Error::last_os_error();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EPERM),
            "a sealed memfd must refuse resize with EPERM, got {err}"
        );
    }

    // The mapping is intact and readable after the refused truncates.
    // SAFETY: same in-bounds payload byte written above.
    assert_eq!(unsafe { seg.payload_ptr().read() }, 0xAB);

    // Unnamed: unlink is a no-op and attach-by-id can never resolve it.
    seg.unlink().expect("sealed segment unlink is a no-op");
    assert!(
        Segment::attach(90_001).is_err(),
        "a memfd segment must have no namespace entry"
    );
}

/// An adopted dup of a sealed segment fd validates its header — the
/// `SCM_RIGHTS` production path for memfd-backed segments.
#[test]
fn sealed_segment_adopts_by_fd() {
    use std::os::fd::{BorrowedFd, IntoRawFd};
    let seg = Segment::create_sealed(90_002, 16 * 1024).expect("create sealed");
    // SAFETY: `seg` keeps the fd open across the borrow.
    let dup = unsafe { BorrowedFd::borrow_raw(seg.as_raw_fd()) }
        .try_clone_to_owned()
        .expect("dup");
    // SAFETY: `dup` is a live, owned duplicate whose ownership transfers once.
    let adopted = unsafe { Segment::from_raw_fd(dup.into_raw_fd(), 90_002) }.expect("adopt");
    assert_eq!(adopted.id(), seg.id());
    assert_eq!(adopted.size(), seg.size());
    assert_eq!(adopted.generation(), seg.generation());
}

/// A pidfd polls readable once (and only once) its process has exited.
#[test]
fn pidfd_reports_child_exit() {
    // A pidfd on a live process (ourselves) must NOT be readable.
    let self_fd = pidfd_open(std::process::id() as libc::pid_t).expect("pidfd self");
    let mut pfd = libc::pollfd {
        fd: self_fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: single valid pollfd.
    let n = unsafe { libc::poll(&mut pfd, 1, 0) };
    assert_eq!(n, 0, "a live process's pidfd must not be readable");

    // A short-lived child's pidfd becomes readable on exit. `pidfd_open`
    // before `wait()` is race-free: an exited-but-unreaped child (zombie) is
    // still openable.
    let mut child = std::process::Command::new("/bin/true")
        .spawn()
        .expect("spawn /bin/true");
    let child_fd = pidfd_open(child.id() as libc::pid_t).expect("pidfd child");
    let mut pfd = libc::pollfd {
        fd: child_fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: single valid pollfd; 10s bound guards CI, the exit is ~instant.
    let n = unsafe { libc::poll(&mut pfd, 1, 10_000) };
    assert_eq!(n, 1, "the exited child's pidfd must become readable");
    assert_ne!(pfd.revents & libc::POLLIN, 0);
    let _ = child.wait();
}

/// `CLOCK_MONOTONIC` nanos are non-zero and non-decreasing, and
/// `LinuxPlatform` is the `NativePlatform` reporting kernel-notified death.
#[test]
fn monotonic_clock_and_native_platform() {
    let a = monotonic_now_nanos();
    let b = monotonic_now_nanos();
    assert!(a > 0, "monotonic clock must be past boot");
    assert!(b >= a, "monotonic clock must not go backwards");

    let plat: NativePlatform = LinuxPlatform::new();
    assert_eq!(plat.death_detection(), DeathDetection::KernelNotified);
    // The platform clock is the monotonic one.
    let c = plat.now_nanos();
    assert!(c >= b);

    // The sealed creation goes through the seam too.
    let seg = plat.segment_create_sealed(90_003, 8192).expect("sealed via seam");
    // SAFETY: ftruncate on a live owned fd (expected to be refused).
    let rc = unsafe { libc::ftruncate(seg.as_raw_fd(), 4096) };
    assert_eq!(rc, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    );
}
