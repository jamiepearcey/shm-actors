//! POSIX shared-memory segments: create, attach, map, unlink.

use core::ffi::c_void;
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use nix::fcntl::OFlag;
use nix::sys::mman::{mmap, munmap, shm_open, shm_unlink, MapFlags, ProtFlags};
use nix::sys::stat::Mode;
use nix::unistd::ftruncate;

use crate::error::{Error, Result};
use crate::pod::SharedPod;

/// Segment magic: the little-endian bytes of `b"SHMACTR1"`.
pub const SEGMENT_MAGIC: u64 = u64::from_le_bytes(*b"SHMACTR1");

/// The only layout version understood by this build.
pub const LAYOUT_VERSION: u32 = 1;

/// Bytes reserved at the base of every segment for the [`SegmentHeader`].
///
/// The real header is 32 bytes; we round the reserved region up to 64 so the
/// payload region that follows starts 64-byte aligned (mmap is page-aligned, so
/// `base + 64` is 64-aligned), matching Arrow buffer alignment.
pub const HEADER_SIZE: usize = 64;

/// Fixed metadata written at the base of every segment.
///
/// # Layout (frozen ABI — 32 bytes)
///
/// | field            | type  |
/// |------------------|-------|
/// | `magic`          | `u64` |
/// | `layout_version` | `u32` |
/// | `segment_id`     | `u32` |
/// | `generation`     | `u64` |
/// | `size`           | `u64` |
#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SegmentHeader {
    /// Must equal [`SEGMENT_MAGIC`].
    pub magic: u64,
    /// Must equal [`LAYOUT_VERSION`].
    pub layout_version: u32,
    /// The segment's id (also encoded in its shm name).
    pub segment_id: u32,
    /// Incarnation generation of the whole segment.
    pub generation: u64,
    /// Total mapped size in bytes (header + payload).
    pub size: u64,
}

// SAFETY: `#[repr(C)]`, no padding (8+4+4+8+8 = 32), no pointers, no Drop.
unsafe impl SharedPod for SegmentHeader {}

const _: () = assert!(core::mem::size_of::<SegmentHeader>() == 32);
const _: () = assert!(core::mem::size_of::<SegmentHeader>() <= HEADER_SIZE);

/// Deterministic shm name for a segment id, e.g. `"/shmactr.7"`.
fn segment_name(id: u32) -> Result<CString> {
    CString::new(format!("/shmactr.{id}"))
        .map_err(|_| Error::LayoutOverflow("segment name contained NUL"))
}

/// An owned, mapped POSIX shared-memory segment.
///
/// Created with `shm_open(O_CREAT|O_EXCL)` + `ftruncate` + `mmap`, or attached
/// to an existing name with `shm_open(O_RDWR)` + `mmap`. Dropping a `Segment`
/// unmaps and closes the fd but does **not** unlink the name — call
/// [`Segment::unlink`] explicitly so passing a segment to another process does
/// not destroy it when one handle drops.
pub struct Segment {
    /// The segment's `shm_open` name, when it has one. `None` for segments
    /// adopted from a raw fd ([`Segment::from_raw_fd`]) and for anonymous
    /// sealed-memfd segments ([`Segment::create_sealed`] on Linux, ADR-0011):
    /// those have no namespace entry, so `unlink` on them is a no-op.
    name: Option<CString>,
    id: u32,
    fd: OwnedFd,
    ptr: NonNull<u8>,
    /// Logical size (header.size); used for all bounds checks.
    size: usize,
    /// Actual mmap length; may exceed `size` when the OS page-rounds the shm
    /// object (e.g. 16 KiB pages on Apple Silicon). Used only for `munmap`.
    map_len: usize,
    generation: u64,
}

// SAFETY: the mapping is `MAP_SHARED`; all cross-process mutation happens through
// atomics placed in it. The `Segment` handle itself only owns an fd + base
// pointer + length, all of which are safe to move between threads.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl Segment {
    /// Create a brand-new segment of `size` bytes for `id`.
    ///
    /// Fails with `EEXIST` (as [`Error::Nix`]) if the name already exists.
    pub fn create(id: u32, size: usize) -> Result<Segment> {
        if size < HEADER_SIZE {
            return Err(Error::LayoutOverflow("segment smaller than header"));
        }
        let name = segment_name(id)?;
        let fd = shm_open(
            name.as_c_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;
        ftruncate(&fd, size as libc::off_t)?;

        let ptr = map_fd(&fd, size)?;
        let generation = 1u64;

        // SAFETY: `ptr` is a fresh mapping of `size >= HEADER_SIZE` bytes,
        // 8-aligned (page aligned), so writing a 32-byte `SegmentHeader` at the
        // base is in-bounds and well aligned.
        unsafe {
            ptr.as_ptr().cast::<SegmentHeader>().write(SegmentHeader {
                magic: SEGMENT_MAGIC,
                layout_version: LAYOUT_VERSION,
                segment_id: id,
                generation,
                size: size as u64,
            });
        }

        Ok(Segment {
            name: Some(name),
            id,
            fd,
            ptr,
            size,
            map_len: size,
            generation,
        })
    }

    /// Create a brand-new **anonymous, sealed** segment of `size` bytes for
    /// `id` (Linux only, ADR-0011): `memfd_create` + `ftruncate` +
    /// `F_ADD_SEALS(F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL)`.
    ///
    /// The object has **no namespace entry** — [`Segment::unlink`] is a no-op
    /// and [`Segment::attach`] can never resolve it — so it can only be
    /// distributed by fd (`SCM_RIGHTS` → [`Segment::from_raw_fd`]), which is
    /// exactly how the runtime distributes every segment. The seals make a
    /// hostile `ftruncate` on any granted fd fail with `EPERM` instead of
    /// SIGBUS-ing mapped readers; writes stay unsealed (segments are mutable
    /// shared state).
    #[cfg(target_os = "linux")]
    pub fn create_sealed(id: u32, size: usize) -> Result<Segment> {
        if size < HEADER_SIZE {
            return Err(Error::LayoutOverflow("segment smaller than header"));
        }
        let fd = crate::platform_linux::memfd_sealed_fd(id, size)?;
        let ptr = map_fd(&fd, size)?;
        let generation = 1u64;

        // SAFETY: `ptr` is a fresh mapping of `size >= HEADER_SIZE` bytes,
        // 8-aligned (page aligned), so writing a 32-byte `SegmentHeader` at the
        // base is in-bounds and well aligned.
        unsafe {
            ptr.as_ptr().cast::<SegmentHeader>().write(SegmentHeader {
                magic: SEGMENT_MAGIC,
                layout_version: LAYOUT_VERSION,
                segment_id: id,
                generation,
                size: size as u64,
            });
        }

        Ok(Segment {
            name: None,
            id,
            fd,
            ptr,
            size,
            map_len: size,
            generation,
        })
    }

    /// Attach to an existing segment by id, validating its header.
    ///
    /// Returns [`Error::LayoutMismatch`] on a magic / version / id / generation
    /// mismatch.
    pub fn attach(id: u32) -> Result<Segment> {
        let name = segment_name(id)?;
        let fd = shm_open(name.as_c_str(), OFlag::O_RDWR, Mode::empty())?;

        // The OS may page-round the object size up (e.g. 16 KiB pages on Apple
        // Silicon), so `fstat` gives the mappable length; the *logical* size is
        // authoritative from the header.
        let map_len = fstat_size(fd.as_raw_fd())?;
        if map_len < HEADER_SIZE {
            return Err(Error::LayoutMismatch);
        }

        let ptr = map_fd(&fd, map_len)?;

        // SAFETY: `ptr` maps at least 32 bytes; reading the header is in-bounds.
        let header = unsafe { ptr.as_ptr().cast::<SegmentHeader>().read() };
        let logical = header.size as usize;
        let bad = header.magic != SEGMENT_MAGIC
            || header.layout_version != LAYOUT_VERSION
            || header.segment_id != id
            || header.generation == 0
            || logical < HEADER_SIZE
            || logical > map_len;
        if bad {
            // SAFETY: `ptr`/`map_len` came from the successful mmap above.
            unsafe { munmap_ignore(ptr, map_len) };
            return Err(Error::LayoutMismatch);
        }

        Ok(Segment {
            name: Some(name),
            id,
            fd,
            ptr,
            size: logical,
            map_len,
            generation: header.generation,
        })
    }

    /// Adopt a shared-memory fd received from another process (e.g. over a Unix
    /// domain socket via `SCM_RIGHTS`) and map it, validating its header.
    ///
    /// This is the by-fd counterpart of [`Segment::attach`]: the coordinator
    /// creates a segment and passes its fd; the receiver never resolves the shm
    /// *name* (it may already be unlinked), so it maps the object directly from
    /// the descriptor. `id` is the expected [`SegmentHeader::segment_id`]; a
    /// mismatch (or a bad magic / version / generation) yields
    /// [`Error::LayoutMismatch`].
    ///
    /// The returned `Segment` takes ownership of `fd` and will close it on drop.
    /// It is created **unnamed** — [`Segment::unlink`] on it is a no-op — so an
    /// adopted handle can never remove the creating process's namespace entry
    /// (and a memfd-backed segment has no entry to remove at all).
    ///
    /// # Safety
    ///
    /// `fd` must be a live, owned file descriptor for a POSIX shared-memory
    /// object (as produced by [`Segment::create`] in the granting process and
    /// transferred intact). Passing an arbitrary or already-closed fd is
    /// undefined. Ownership of `fd` transfers to the returned `Segment`.
    pub unsafe fn from_raw_fd(fd: RawFd, id: u32) -> Result<Segment> {
        // SAFETY: caller guarantees `fd` is a live, owned shm descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };

        // The OS may page-round the object size up; `fstat` gives the mappable
        // length, and the *logical* size is authoritative from the header.
        let map_len = fstat_size(fd.as_raw_fd())?;
        if map_len < HEADER_SIZE {
            return Err(Error::LayoutMismatch);
        }

        let ptr = map_fd(&fd, map_len)?;

        // SAFETY: `ptr` maps at least 32 bytes; reading the header is in-bounds.
        let header = unsafe { ptr.as_ptr().cast::<SegmentHeader>().read() };
        let logical = header.size as usize;
        let bad = header.magic != SEGMENT_MAGIC
            || header.layout_version != LAYOUT_VERSION
            || header.segment_id != id
            || header.generation == 0
            || logical < HEADER_SIZE
            || logical > map_len;
        if bad {
            // SAFETY: `ptr`/`map_len` came from the successful mmap above.
            unsafe { munmap_ignore(ptr, map_len) };
            return Err(Error::LayoutMismatch);
        }

        Ok(Segment {
            name: None,
            id,
            fd,
            ptr,
            size: logical,
            map_len,
            generation: header.generation,
        })
    }

    /// Remove the segment name from the filesystem namespace (`shm_unlink`).
    ///
    /// Existing mappings (including this one) remain valid until unmapped; this
    /// only prevents new [`Segment::attach`] calls from resolving the name. A
    /// no-op for unnamed segments ([`Segment::from_raw_fd`] /
    /// [`Segment::create_sealed`]): they have no namespace entry to remove.
    pub fn unlink(&self) -> Result<()> {
        if let Some(name) = &self.name {
            shm_unlink(name.as_c_str())?;
        }
        Ok(())
    }

    /// Remove a segment name by id without mapping it (`shm_unlink`).
    ///
    /// Useful for the coordinator to reclaim a segment name it never mapped.
    pub fn unlink_by_id(id: u32) -> Result<()> {
        shm_unlink(segment_name(id)?.as_c_str())?;
        Ok(())
    }

    /// The segment id.
    #[inline]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The segment's incarnation generation.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Total mapped size in bytes (header + payload).
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// The raw OS file descriptor, e.g. to `dup` and pass over `SCM_RIGHTS`.
    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// A snapshot copy of the on-segment header.
    #[inline]
    pub fn header(&self) -> SegmentHeader {
        // SAFETY: header lives at the base of a mapping of at least 32 bytes.
        unsafe { self.ptr.as_ptr().cast::<SegmentHeader>().read() }
    }

    /// Base pointer of the whole mapping (header included).
    #[inline]
    pub fn base_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Pointer to the start of the payload region (immediately after the header).
    #[inline]
    pub fn payload_ptr(&self) -> *mut u8 {
        // SAFETY: `HEADER_SIZE < size`, so this stays within the mapping.
        unsafe { self.ptr.as_ptr().add(HEADER_SIZE) }
    }

    /// Length of the payload region in bytes.
    #[inline]
    pub fn payload_len(&self) -> usize {
        self.size - HEADER_SIZE
    }

    /// Resolve a raw region `[offset, offset+len)` measured from the **segment
    /// base**, with a bounds check. Used to dereference a [`ChunkDesc`].
    pub fn resolve(&self, offset: u32, len: u32) -> Result<*mut u8> {
        let offset = offset as usize;
        let len = len as usize;
        let end = offset.checked_add(len).ok_or(Error::OutOfBounds)?;
        if offset < HEADER_SIZE || end > self.size {
            return Err(Error::OutOfBounds);
        }
        // SAFETY: `offset <= size`, so the pointer stays within the mapping.
        Ok(unsafe { self.ptr.as_ptr().add(offset) })
    }

    /// Borrow a `&T` view at `offset` bytes into the **payload** region, with
    /// bounds and alignment checks.
    ///
    /// Returns [`Error::OutOfBounds`] if the region does not fit and
    /// [`Error::Misaligned`] if the address is not aligned for `T`.
    ///
    /// # Safety
    ///
    /// The caller asserts no concurrent writer is mutating these bytes for the
    /// lifetime of the returned reference (the borrow discipline enforced by
    /// [`ChunkCtrl`](crate::ctrl::ChunkCtrl) provides this at the layer above).
    pub unsafe fn view_at<T: SharedPod>(&self, offset: usize) -> Result<&T> {
        let ptr = self.typed_ptr::<T>(offset)?;
        // SAFETY: `typed_ptr` checked bounds + alignment; caller upholds the
        // no-concurrent-writer invariant. `T: SharedPod` is valid for any bytes.
        Ok(unsafe { &*ptr })
    }

    /// Mutable variant of [`Segment::view_at`].
    ///
    /// # Safety
    ///
    /// The caller asserts exclusive access to these bytes (an exclusive `Loan`
    /// at the layer above) for the lifetime of the returned reference.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn view_at_mut<T: SharedPod>(&self, offset: usize) -> Result<&mut T> {
        let ptr = self.typed_ptr::<T>(offset)?;
        // SAFETY: bounds + alignment checked; caller upholds exclusivity.
        Ok(unsafe { &mut *ptr })
    }

    /// Compute + validate a typed pointer at `offset` into the payload region.
    fn typed_ptr<T: SharedPod>(&self, offset: usize) -> Result<*mut T> {
        let size = core::mem::size_of::<T>();
        let end = offset.checked_add(size).ok_or(Error::OutOfBounds)?;
        if end > self.payload_len() {
            return Err(Error::OutOfBounds);
        }
        // SAFETY: `offset <= payload_len`, in-bounds of the mapping.
        let addr = unsafe { self.payload_ptr().add(offset) } as *mut T;
        if !(addr as usize).is_multiple_of(core::mem::align_of::<T>()) {
            return Err(Error::Misaligned);
        }
        Ok(addr)
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`map_len` came from a successful mmap owned by this handle.
        unsafe { munmap_ignore(self.ptr, self.map_len) };
        // `fd` (OwnedFd) closes itself. The name is intentionally left for an
        // explicit `unlink`.
    }
}

/// mmap a full segment `RW`/`SHARED` and return its base as `NonNull<u8>`.
fn map_fd(fd: &OwnedFd, size: usize) -> Result<NonNull<u8>> {
    let len = NonZeroUsize::new(size).ok_or(Error::LayoutOverflow("zero-length mapping"))?;
    // SAFETY: `fd` refers to a shm object truncated to `size`; a fixed shared
    // read/write mapping of `size` bytes at a kernel-chosen address is valid.
    let raw: NonNull<c_void> = unsafe {
        mmap(
            None,
            len,
            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
            MapFlags::MAP_SHARED,
            fd,
            0,
        )?
    };
    Ok(raw.cast::<u8>())
}

/// `munmap`, swallowing errors (only called from `Drop` / error unwinds).
///
/// # Safety
///
/// `ptr`/`size` must come from a live mmap owned by the caller.
unsafe fn munmap_ignore(ptr: NonNull<u8>, size: usize) {
    // SAFETY: forwarded from the caller's contract.
    let _ = unsafe { munmap(ptr.cast::<c_void>(), size) };
}

/// Return the byte size of the object behind `fd` via `fstat`.
fn fstat_size(fd: RawFd) -> Result<usize> {
    // SAFETY: `fd` is a valid open descriptor; `stat` is fully written by fstat.
    let mut st = unsafe { core::mem::zeroed::<libc::stat>() };
    let rc = unsafe { libc::fstat(fd, &mut st) };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(st.st_size as usize)
}
