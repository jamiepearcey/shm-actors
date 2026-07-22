//! Chunk descriptors and the compact 64-bit "manifest pointer" packing.

use crate::pod::SharedPod;

/// The 24-byte handle every primitive passes around instead of copying payload.
///
/// A `ChunkDesc` names a region of a segment together with the generation it was
/// valid at. It is `Copy`, `#[repr(C)]`, and pure POD — it may be written into
/// shared memory, sent over a socket, or stored in a manifest.
///
/// # Layout (frozen ABI — 24 bytes)
///
/// | field        | type  | meaning                                            |
/// |--------------|-------|----------------------------------------------------|
/// | `segment_id` | `u32` | which segment the chunk lives in                   |
/// | `generation` | `u32` | generation the descriptor was minted at            |
/// | `offset`     | `u32` | byte offset of the chunk from the **segment base** |
/// | `len`        | `u32` | usable byte length of the chunk                    |
/// | `schema_id`  | `u32` | interned schema id (0 = untyped bytes)             |
/// | `_pad`       | `u32` | reserved, must be zero                             |
///
/// # Alignment note
///
/// The chunk *payloads* a `ChunkDesc` points at are 64-byte aligned (see
/// [`crate::pool`]) so an Arrow buffer reconstructed over `offset..offset+len`
/// satisfies Arrow's 64-byte buffer-alignment expectation. The descriptor
/// struct itself only needs 4-byte alignment.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, bytemuck::Pod, bytemuck::Zeroable)]
pub struct ChunkDesc {
    /// Segment the chunk lives in.
    pub segment_id: u32,
    /// Generation the descriptor was valid at; compared against the live
    /// [`ChunkCtrl`](crate::ctrl::ChunkCtrl) generation to detect recycling.
    pub generation: u32,
    /// Byte offset of the chunk from the segment base.
    pub offset: u32,
    /// Usable byte length of the chunk.
    pub len: u32,
    /// Interned schema id; `0` means untyped bytes.
    pub schema_id: u32,
    /// Reserved padding, kept zero to keep the struct free of uninit bytes.
    pub _pad: u32,
}

// SAFETY: `#[repr(C)]`, all-`u32` fields (no padding), no pointers, no Drop.
unsafe impl SharedPod for ChunkDesc {}

// The 24-byte size is part of the frozen ABI.
const _: () = assert!(core::mem::size_of::<ChunkDesc>() == 24);
const _: () = assert!(core::mem::align_of::<ChunkDesc>() == 4);

impl ChunkDesc {
    /// A zeroed descriptor (segment 0, generation 0). Used as an empty slot
    /// sentinel in the borrow journal.
    pub const ZERO: ChunkDesc = ChunkDesc {
        segment_id: 0,
        generation: 0,
        offset: 0,
        len: 0,
        schema_id: 0,
        _pad: 0,
    };

    /// Returns `true` if this descriptor is the zero sentinel.
    #[inline]
    pub fn is_zero(&self) -> bool {
        *self == ChunkDesc::ZERO
    }
}

/// A compact 64-bit "manifest pointer": `(segment_id, offset)` squeezed into a
/// single word.
///
/// A full [`ChunkDesc`] is 24 bytes and therefore cannot be packed losslessly
/// into 64 bits, so no `ChunkDesc::pack` is offered. Artifacts instead store a
/// `PackedRef` in a single `AtomicU64` (e.g. `shm-artifact`'s
/// `ArtifactHead.manifest_desc: AtomicU64`) so the version head can be swapped
/// with one atomic store. The `len` and `schema_id` are **not** carried here —
/// they are read from the manifest the packed ref points at.
///
/// # Bit layout (frozen — v0.3, ADR-0003a)
///
/// ```text
///  63                            32 31                            0
/// +--------------------------------+------------------------------+
/// | segment_id                     | offset                       |
/// | 32 bits                        | 32 bits                      |
/// +--------------------------------+------------------------------+
/// ```
///
/// - `segment_id` uses the full 32 bits (matching [`ChunkDesc::segment_id`]);
///   this lifts the former 2^16 artifact-data-segment-id cap to 2^32.
/// - `offset` uses the full 32 bits (segments up to 4 GiB).
///
/// The `generation` field the previous `[seg:16 | gen:16 | off:32]` packing
/// carried is **dropped**: the generation's ABA role for the manifest pointer is
/// subsumed by the manifest's own `{artifact_id, version}` self-validation (a
/// monotonic, never-reissued pair — see `shm-artifact`'s `VersionManifest`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct PackedRef(pub u64);

// SAFETY: transparent over `u64`; pure POD.
unsafe impl SharedPod for PackedRef {}

impl PackedRef {
    const OFFSET_BITS: u32 = 32;
    const SEGMENT_BITS: u32 = 32;

    const OFFSET_MASK: u64 = (1u64 << Self::OFFSET_BITS) - 1;
    const SEGMENT_MASK: u64 = (1u64 << Self::SEGMENT_BITS) - 1;

    const SEGMENT_SHIFT: u32 = Self::OFFSET_BITS;

    /// Packs `(segment_id, offset)` into a single word. Both fields are 32-bit,
    /// so the packing is lossless for any `u32` inputs.
    #[inline]
    pub fn pack(segment_id: u32, offset: u32) -> PackedRef {
        let bits = (u64::from(segment_id) << Self::SEGMENT_SHIFT) | u64::from(offset);
        PackedRef(bits)
    }

    /// Packs the pointer fields of a [`ChunkDesc`]. `generation`/`len`/`schema_id`
    /// are intentionally dropped (the manifest self-validates its identity).
    #[inline]
    pub fn from_desc(desc: &ChunkDesc) -> PackedRef {
        Self::pack(desc.segment_id, desc.offset)
    }

    /// Unpacks to `(segment_id, offset)`.
    #[inline]
    pub fn unpack(self) -> (u32, u32) {
        (self.segment_id(), self.offset())
    }

    /// The segment id (full 32 bits).
    #[inline]
    pub fn segment_id(self) -> u32 {
        ((self.0 >> Self::SEGMENT_SHIFT) & Self::SEGMENT_MASK) as u32
    }

    /// The 32-bit offset from the segment base.
    #[inline]
    pub fn offset(self) -> u32 {
        (self.0 & Self::OFFSET_MASK) as u32
    }

    /// The raw packed word.
    #[inline]
    pub fn to_bits(self) -> u64 {
        self.0
    }
}
