//! Watch / pub-sub: the [`VersionEvent`] emitted on every successful commit.
//!
//! `shm-artifact` does not hard-depend on a running coordinator or a live
//! [`shm-ring`](shm_ring). Instead a commit calls an injected **watch sink** (a
//! closure) with a [`VersionEvent`]; the runtime decides how to route it. For
//! transport over an `shm-ring` — which carries only 24-byte
//! [`ChunkDesc`](shm_core::ChunkDesc)s — this module also provides a lossless
//! [`VersionEvent`] ⇄ [`ChunkDesc`] codec so a caller can publish events on a
//! built-in `__artifacts` topic and decode them on the far side.

use shm_core::ChunkDesc;

/// The conventional topic name a runtime routes [`VersionEvent`]s on.
pub const ARTIFACTS_TOPIC: &str = "__artifacts";

/// Marker written into [`ChunkDesc::schema_id`] by [`VersionEvent::to_desc`] so
/// a subscriber can recognise an event-carrying descriptor: `b"EVNT"` LE.
pub const EVENT_MAGIC: u32 = u32::from_le_bytes(*b"EVNT");

/// The kind of change a commit made, mirrored into [`VersionEvent::kind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CommitKind {
    /// A [`Commit::Replace`](crate::Commit::Replace): the version supersedes its
    /// predecessor wholesale.
    Replace = 0,
    /// A [`Commit::Append`](crate::Commit::Append): the version shares its
    /// predecessor's chunks plus newly staged data.
    Append = 1,
    /// A [`Commit::Patch`](crate::Commit::Patch): a ranged in-place update
    /// (deferred to v0.2).
    Patch = 2,
    /// A [`Commit::Window`](crate::Commit::Window): a fresh root re-listing
    /// the newest batches of its predecessor plus new data (ADR-0016).
    Window = 3,
}

impl CommitKind {
    /// The `u32` wire encoding of this kind.
    #[inline]
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Decode a `u32` wire value back into a [`CommitKind`], or `None` if it is
    /// not a defined kind.
    #[inline]
    pub fn from_u32(v: u32) -> Option<CommitKind> {
        match v {
            0 => Some(CommitKind::Replace),
            1 => Some(CommitKind::Append),
            2 => Some(CommitKind::Patch),
            3 => Some(CommitKind::Window),
            _ => None,
        }
    }
}

/// A commit notification published on the `__artifacts` topic.
///
/// # Layout (frozen ABI — 16 bytes)
///
/// Fields are ordered `version` first so the record packs to 16 bytes with no
/// interior padding.
///
/// | field     | type  | meaning                                       |
/// |-----------|-------|-----------------------------------------------|
/// | `version` | `u64` | the version number just installed             |
/// | `name_id` | `u32` | interned artifact name id                     |
/// | `kind`    | `u32` | [`CommitKind`] discriminant                   |
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VersionEvent {
    /// The newly installed version number.
    pub version: u64,
    /// The interned id of the artifact whose version changed.
    pub name_id: u32,
    /// What kind of commit produced this version.
    pub kind: u32,
}

const _: () = assert!(core::mem::size_of::<VersionEvent>() == 16);

impl VersionEvent {
    /// Build an event for `name_id` reaching `version` via `kind`.
    #[inline]
    pub fn new(name_id: u32, version: u64, kind: CommitKind) -> VersionEvent {
        VersionEvent {
            version,
            name_id,
            kind: kind.as_u32(),
        }
    }

    /// Encode this event losslessly into a [`ChunkDesc`] so it can ride an
    /// [`shm-ring`](shm_ring) broadcast (which carries only descriptors).
    ///
    /// The mapping packs the four fields into the descriptor's `u32` slots and
    /// stamps [`EVENT_MAGIC`] into `schema_id` so [`VersionEvent::from_desc`]
    /// can reject non-event descriptors.
    #[inline]
    pub fn to_desc(self) -> ChunkDesc {
        ChunkDesc {
            segment_id: self.name_id,
            generation: self.kind,
            offset: (self.version & 0xffff_ffff) as u32,
            len: (self.version >> 32) as u32,
            schema_id: EVENT_MAGIC,
            _pad: 0,
        }
    }

    /// Decode a [`ChunkDesc`] produced by [`VersionEvent::to_desc`]. Returns
    /// `None` if the descriptor is not an event (wrong [`EVENT_MAGIC`]).
    #[inline]
    pub fn from_desc(desc: &ChunkDesc) -> Option<VersionEvent> {
        if desc.schema_id != EVENT_MAGIC {
            return None;
        }
        let version = u64::from(desc.offset) | (u64::from(desc.len) << 32);
        Some(VersionEvent {
            version,
            name_id: desc.segment_id,
            kind: desc.generation,
        })
    }
}
