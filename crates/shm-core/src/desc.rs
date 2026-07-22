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

/// A compact 64-bit "manifest pointer": `(segment_id, generation, offset)`
/// squeezed into a single word.
///
/// A full [`ChunkDesc`] is 24 bytes and therefore cannot be packed losslessly
/// into 64 bits, so no `ChunkDesc::pack` is offered. Artifacts instead store a
/// `PackedRef` in a single `AtomicU64` (e.g. `shm-artifact`'s
/// `ArtifactHead.manifest_desc: AtomicU64`) so the version head can be swapped
/// with one atomic store. The `len` and `schema_id` are **not** carried here —
/// they are read from the manifest the packed ref points at.
///
/// # Bit layout (frozen)
///
/// ```text
///  63          48 47          32 31                              0
/// +--------------+--------------+---------------------------------+
/// | segment_id   | generation   | offset                          |
/// | 16 bits      | 16 bits      | 32 bits                         |
/// +--------------+--------------+---------------------------------+
/// ```
///
/// - `segment_id` must be `< 2^16`.
/// - `generation` is stored **mod 2^16** (low 16 bits). Generation compares are
///   still exact within any 65 536-recycle window, which is far beyond the pin
///   lifetime; the authoritative full-width generation always lives in the
///   [`ChunkCtrl`](crate::ctrl::ChunkCtrl).
/// - `offset` uses the full 32 bits (segments up to 4 GiB).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct PackedRef(pub u64);

// SAFETY: transparent over `u64`; pure POD.
unsafe impl SharedPod for PackedRef {}

impl PackedRef {
    const OFFSET_BITS: u32 = 32;
    const GENERATION_BITS: u32 = 16;
    const SEGMENT_BITS: u32 = 16;

    const OFFSET_MASK: u64 = (1u64 << Self::OFFSET_BITS) - 1;
    const GENERATION_MASK: u64 = (1u64 << Self::GENERATION_BITS) - 1;
    const SEGMENT_MASK: u64 = (1u64 << Self::SEGMENT_BITS) - 1;

    const GENERATION_SHIFT: u32 = Self::OFFSET_BITS;
    const SEGMENT_SHIFT: u32 = Self::OFFSET_BITS + Self::GENERATION_BITS;

    /// Packs `(segment_id, generation, offset)` into a single word.
    ///
    /// `segment_id` is masked to 16 bits and `generation` to its low 16 bits;
    /// in debug builds an out-of-range `segment_id` trips an assertion.
    #[inline]
    pub fn pack(segment_id: u32, generation: u32, offset: u32) -> PackedRef {
        debug_assert!(
            u64::from(segment_id) <= Self::SEGMENT_MASK,
            "segment_id does not fit in 16 bits"
        );
        let bits = ((u64::from(segment_id) & Self::SEGMENT_MASK) << Self::SEGMENT_SHIFT)
            | ((u64::from(generation) & Self::GENERATION_MASK) << Self::GENERATION_SHIFT)
            | (u64::from(offset) & Self::OFFSET_MASK);
        PackedRef(bits)
    }

    /// Packs the pointer fields of a [`ChunkDesc`]. `len`/`schema_id` are
    /// intentionally dropped (they come from the manifest).
    #[inline]
    pub fn from_desc(desc: &ChunkDesc) -> PackedRef {
        Self::pack(desc.segment_id, desc.generation, desc.offset)
    }

    /// Unpacks to `(segment_id, generation, offset)`.
    #[inline]
    pub fn unpack(self) -> (u32, u32, u32) {
        (self.segment_id(), self.generation(), self.offset())
    }

    /// The segment id (16 bits).
    #[inline]
    pub fn segment_id(self) -> u32 {
        ((self.0 >> Self::SEGMENT_SHIFT) & Self::SEGMENT_MASK) as u32
    }

    /// The low 16 bits of the generation.
    #[inline]
    pub fn generation(self) -> u32 {
        ((self.0 >> Self::GENERATION_SHIFT) & Self::GENERATION_MASK) as u32
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
