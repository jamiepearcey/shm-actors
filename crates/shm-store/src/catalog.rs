//! The on-shm **catalog**: an append-only array of [`CatalogSlot`]s plus a header,
//! placed by hand into a store-owned management segment.
//!
//! The catalog is the keyed store's shared-memory **fast path**: a worker that has
//! mapped the catalog segment resolves `key_id → CatalogSlot → ArtifactHead` with
//! no UDS round-trip (§ADR-0007 G3). It mirrors how `shm-core` places
//! [`ChunkCtrl`](shm_core::ChunkCtrl) and `shm-ring` places its `RingHeader`:
//! every concurrently-touched word is a [`ShmU32`]/[`ShmU64`] atomic overlaid on
//! shared bytes.
//!
//! # Slot lifecycle (the CAS state machine)
//!
//! ```text
//!   FREE ──alloc+publish──▶ LIVE ──evict──▶ TOMBSTONE ──sweep──▶ RECLAIMING
//!     ▲                                         ▲                    │
//!     └──────── free-list push ─────────────────┴── not quiescent ────┘
//! ```
//!
//! A slot's `head_off` (where its [`ArtifactHead`](shm_artifact::ArtifactHead)
//! lives in the head segment) and `artifact_id` (the id a journal record routes
//! by) are pure functions of the index — `idx * head_stride` and
//! `artifact_id_base + idx`. A creator gets an index it exclusively owns, either
//! by a monotonic `next_slot` bump or by popping the free list, so publishing is
//! race-free across processes.
//!
//! # Reclamation (ADR-0008 P0.1)
//!
//! Slots were **append-only** through v0.5: `evict` tombstoned a slot and never
//! returned it, which made `capacity` a cap on entries created *for the
//! coordinator's lifetime*. Under the entry churn an actor system generates that
//! is an outage waiting on a clock, so `TOMBSTONE → FREE` now exists — with two
//! properties that make it safe:
//!
//! - **Deferred, never immediate.** `Artifact::evict_all` retires versions
//!   *conditionally*: one still pinned by a live reader is left for that reader's
//!   pin-drop. So the evictor cannot know the entry is finished; a **sweep**
//!   decides, under [`SLOT_RECLAIMING`], which no creator allocates from.
//! - **Identified by `(artifact_id, incarnation)`, not by index.** Since the
//!   index is reused, `artifact_id` names a *slot*, not an occupant. Each slot
//!   carries a `gen` — the incarnation stamped into the next occupant's
//!   `ArtifactHead` — so a handle or journal record from a previous occupant is
//!   detected rather than silently retargeted at the current one. A slot whose
//!   `gen` would wrap is retired permanently instead of freed: one slot lost,
//!   and no incarnation is ever reused.

use core::sync::atomic::Ordering::{AcqRel, Acquire, Release};

use shm_core::segment::HEADER_SIZE;
use shm_core::{Segment, ShmU32, ShmU64};

use crate::error::{Error, Result};

/// Catalog header magic: little-endian bytes of `b"SHMSTOR2"`.
///
/// Bumped from `SHMSTOR1` for ADR-0008 P0.1: the header gained a free-list head
/// and each slot gained `gen`/`next_free`, so a v0.5 catalog is rejected rather
/// than misread.
pub const CATALOG_MAGIC: u64 = u64::from_le_bytes(*b"SHMSTOR3");

/// Slot state: unused and claimable (also the zero-initialised value).
pub const SLOT_FREE: u32 = 0;
/// Slot state: tracks a live keyed entry.
pub const SLOT_LIVE: u32 = 1;
/// Slot state: the entry was evicted. The slot is awaiting a sweep, which will
/// return it to [`SLOT_FREE`] once its entry is quiescent.
pub const SLOT_TOMBSTONE: u32 = 2;
/// Slot state: a sweep owns this slot exclusively and is deciding whether its
/// entry is quiescent enough to reclaim (ADR-0008 P0.1). No creator allocates
/// from this state, so the quiescence check cannot race a new occupant.
pub const SLOT_RECLAIMING: u32 = 3;

/// Free-list terminator: "no next slot".
pub const FREE_NIL: u32 = u32::MAX;

/// The first incarnation stamped into a never-yet-recycled slot. Matches
/// [`shm_artifact::FIRST_INCARNATION`]; `0` is reserved for "not in service".
pub const FIRST_GEN: u32 = 1;

/// Pack the free-list head: `{tag:32 | idx:32}`. The tag is bumped on every
/// push and pop, which is what makes the Treiber stack ABA-safe — the same shape
/// [`Pool`](shm_core::Pool) uses for its chunk free lists.
#[inline]
const fn pack_free_head(idx: u32, tag: u32) -> u64 {
    ((tag as u64) << 32) | (idx as u64)
}

/// The kind of referent a [`CatalogSlot`] (and, in the G1 stage, a `TypedRef`)
/// names. The discriminants are the frozen ABI values from ADR-0007.
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefKind {
    /// A raw, untyped chunk (no key; `key_id == 0`).
    RawChunk = 0,
    /// An opaque object (single blob).
    Object = 1,
    /// A versioned artifact.
    Artifact = 2,
    /// A dataset (a table-shaped artifact).
    Dataset = 3,
    /// A task/computation result.
    Result = 4,
}

impl RefKind {
    /// The `u16` ABI discriminant.
    #[inline]
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// Decode a `u16` ABI discriminant, or [`Error::BadKind`] if out of range.
    #[inline]
    pub fn from_u16(v: u16) -> Result<RefKind> {
        Ok(match v {
            0 => RefKind::RawChunk,
            1 => RefKind::Object,
            2 => RefKind::Artifact,
            3 => RefKind::Dataset,
            4 => RefKind::Result,
            other => return Err(Error::BadKind(other)),
        })
    }
}

/// One catalog entry: a keyed pointer to an [`ArtifactHead`](shm_artifact::ArtifactHead).
///
/// # Layout (28 bytes, all fields atomic)
///
/// | field         | type       | meaning                                        |
/// |---------------|------------|------------------------------------------------|
/// | `key_id`      | `AtomicU32`| coordinator-interned key id (0 = none)         |
/// | `artifact_id` | `AtomicU32`| the entry's lineage id (`base + slot index`)   |
/// | `head_off`    | `AtomicU32`| byte offset of the `ArtifactHead` in the head seg |
/// | `kind`        | `AtomicU32`| [`RefKind`] discriminant (a `u16` value widened)|
/// | `state`       | `AtomicU32`| `FREE`/`LIVE`/`TOMBSTONE`/`RECLAIMING` — the CAS gate |
/// | `gen`         | `AtomicU32`| incarnation of the next occupant (ADR-0008)     |
/// | `next_free`   | `AtomicU32`| free-list link (live only while FREE + listed)  |
///
/// ADR-0007 diagrams the field as `kind: u16, _pad: u16`; here the `u16` kind and
/// its pad are folded into one 4-byte atomic word so the whole slot is built from
/// uniform [`ShmU32`] atoms (the `shm-core` placed-by-hand convention).
///
/// ADR-0008 P0.1 grew the slot 20 B → 28 B (`gen` + `next_free`). `next_free`
/// is only meaningful while the slot sits on the free list, so it *could*
/// overlay a word that is dead in that state (`head_off`, say) and keep the
/// slot at 24 B — rejected deliberately: [`find_live_by_key`] reads only
/// `state` and (short-circuited) `key_id` per slot, so with sub-line slots
/// either layout touches essentially the same cache lines and the 4 bytes buy
/// ~14% fewer lines over a full scan of a large catalog — while the overlay
/// couples the free-list handshake to which fields `publish_slot` happens to
/// rewrite. Not worth it at any plausible `store_capacity`.
#[repr(C)]
pub struct CatalogSlot {
    /// Coordinator-interned key id (`0` = none / raw).
    pub key_id: ShmU32,
    /// The entry's lineage id, used to route journaled entry pins on crash.
    pub artifact_id: ShmU32,
    /// Byte offset of this entry's `ArtifactHead` within the head segment.
    pub head_off: ShmU32,
    /// The [`RefKind`] discriminant (a `u16` value stored in a `u32` word).
    pub kind: ShmU32,
    /// **`{gen:32 (hi) | state:32 (lo)}` in ONE word.** `gen` is the
    /// incarnation the next occupant will carry (advanced by each reclaim);
    /// `state` is the lifecycle. Packed so a tombstone is a CAS on the occupant
    /// *and* the state together (ADR-0014): through `SHMSTOR2` an evictor
    /// tombstoned by state alone and re-checked `gen` afterwards, and a full
    /// recycle inside that window — plus a sweep claiming the slot before the
    /// undo — could tear down an innocent new occupant.
    pub word: ShmU64,
    /// Free-list link while this slot is [`SLOT_FREE`] and on the list;
    /// [`FREE_NIL`] terminates. Meaningless in any other state.
    pub next_free: ShmU32,
    /// Pad to 32 bytes. Zero.
    pub _pad: u32,
}

const _: () = assert!(core::mem::size_of::<CatalogSlot>() == 32);
const _: () = assert!(core::mem::align_of::<CatalogSlot>() == 8);

/// Pack `{gen (hi) | state (lo)}`.
#[inline]
pub const fn pack_slot_word(gen: u32, state: u32) -> u64 {
    ((gen as u64) << 32) | (state as u64)
}
#[inline]
const fn slot_word_gen(w: u64) -> u32 {
    (w >> 32) as u32
}
#[inline]
const fn slot_word_state(w: u64) -> u32 {
    w as u32
}

impl CatalogSlot {
    /// The interned key id this slot tracks (`0` if none).
    #[inline]
    pub fn key_id(&self) -> u32 {
        self.key_id.load(Acquire)
    }

    /// The entry's lineage id.
    #[inline]
    pub fn artifact_id(&self) -> u32 {
        self.artifact_id.load(Acquire)
    }

    /// The byte offset of the entry's `ArtifactHead` within the head segment.
    #[inline]
    pub fn head_off(&self) -> u32 {
        self.head_off.load(Acquire)
    }

    /// The entry's [`RefKind`].
    #[inline]
    pub fn kind(&self) -> Result<RefKind> {
        RefKind::from_u16(self.kind.load(Acquire) as u16)
    }

    /// The current lifecycle state ([`SLOT_FREE`]/[`SLOT_LIVE`]/[`SLOT_TOMBSTONE`]).
    #[inline]
    pub fn state(&self) -> u32 {
        slot_word_state(self.word.load(Acquire))
    }

    /// Write the four data fields (before publishing the state). The writer owns
    /// this slot exclusively (a freshly bumped index), so these plain `Release`
    /// stores need no CAS; the subsequent [`publish`](Self::publish) CAS is the
    /// release edge a reader's [`state`](Self::state) `Acquire` load synchronises
    /// with, so a `LIVE` reader always observes the fields written here.
    #[inline]
    fn write_fields(&self, key_id: u32, artifact_id: u32, head_off: u32, kind: RefKind) {
        self.key_id.store(key_id, Release);
        self.artifact_id.store(artifact_id, Release);
        self.head_off.store(head_off, Release);
        self.kind.store(u32::from(kind.as_u16()), Release);
    }

    /// The incarnation the next occupant of this slot will carry.
    #[inline]
    pub fn gen(&self) -> u32 {
        slot_word_gen(self.word.load(Acquire))
    }

    /// CAS the state half from `from` to `to` **at the current gen** — a state
    /// transition that fails if the occupant changed underneath it.
    #[inline]
    fn cas_state(&self, from: u32, to: u32) -> bool {
        let mut cur = self.word.load(Acquire);
        loop {
            if slot_word_state(cur) != from {
                return false;
            }
            match self.word.compare_exchange(
                cur,
                pack_slot_word(slot_word_gen(cur), to),
                AcqRel,
                Acquire,
            ) {
                Ok(_) => return true,
                Err(now) => cur = now,
            }
        }
    }

    /// Publish the slot: `FREE → LIVE`. `true` iff this caller won.
    #[inline]
    pub fn publish(&self) -> bool {
        self.cas_state(SLOT_FREE, SLOT_LIVE)
    }

    /// Tombstone **occupant `gen`**: one CAS `{gen, LIVE} → {gen, TOMBSTONE}`.
    /// A slot recycled since the caller observed `gen` carries a different gen
    /// and the CAS fails — the entry the caller meant is already gone, no other
    /// occupant was touched, and there is nothing to undo. This is the ADR-0008
    /// residual race closed (ADR-0014).
    #[inline]
    pub fn tombstone_gen(&self, gen: u32) -> bool {
        self.word
            .compare_exchange(
                pack_slot_word(gen, SLOT_LIVE),
                pack_slot_word(gen, SLOT_TOMBSTONE),
                AcqRel,
                Acquire,
            )
            .is_ok()
    }

    /// Tombstone whichever occupant is live: `LIVE → TOMBSTONE` at the current
    /// gen (tests and tools; `KeyedStore::evict` uses [`tombstone_gen`]).
    ///
    /// [`tombstone_gen`]: Self::tombstone_gen
    #[inline]
    pub fn tombstone(&self) -> bool {
        self.cas_state(SLOT_LIVE, SLOT_TOMBSTONE)
    }

    /// Take exclusive ownership for a sweep: `TOMBSTONE → RECLAIMING`. `true`
    /// iff this caller won and must now either
    /// [`finish_reclaim`](Self::finish_reclaim) or [`abort_reclaim`](Self::abort_reclaim).
    #[inline]
    fn begin_reclaim(&self) -> bool {
        self.cas_state(SLOT_TOMBSTONE, SLOT_RECLAIMING)
    }

    /// Give the slot back to the tombstone pool: the entry was not quiescent, so
    /// a later sweep must try again. Exclusive (we hold `RECLAIMING`); gen kept.
    #[inline]
    fn abort_reclaim(&self) {
        let cur = self.word.load(Acquire);
        self.word
            .store(pack_slot_word(slot_word_gen(cur), SLOT_TOMBSTONE), Release);
    }

    /// Clear the slot's key, advance `gen` and return the slot to `FREE` in
    /// **one store** (we hold `RECLAIMING` exclusively), returning the
    /// incarnation the next occupant will carry — or `None`, leaving the slot
    /// parked in `RECLAIMING` forever, if `gen` would wrap.
    #[inline]
    fn finish_reclaim(&self) -> Option<u32> {
        let cur = self.word.load(Acquire);
        let next = slot_word_gen(cur).checked_add(1)?;
        self.key_id.store(0, Release);
        self.word.store(pack_slot_word(next, SLOT_FREE), Release);
        Some(next)
    }
}

/// On-segment catalog header (placed at the payload base).
#[repr(C)]
struct CatalogHeader {
    magic: ShmU64,
    capacity: ShmU32,
    next_slot: ShmU32,
    head_stride: ShmU32,
    artifact_id_base: ShmU32,
    /// ABA-safe Treiber head over reclaimed slots: `{tag:32 | idx:32}`,
    /// `idx == FREE_NIL` when empty (ADR-0008 P0.1).
    free_head: ShmU64,
}

/// Byte offset of the slot array within the catalog segment payload.
#[inline]
const fn slots_offset() -> usize {
    // `CatalogSlot` needs 4-byte alignment; the header is 8-aligned and its own
    // size already rounds to that, so this only guards a future field edit.
    let hdr = core::mem::size_of::<CatalogHeader>();
    (hdr + 3) & !3
}

/// A handle to a catalog laid out inside a [`Segment`].
///
/// Like [`Pool`](shm_core::Pool), the handle only caches derived pointers; all
/// state lives in the segment, so many processes may each hold their own
/// `Catalog` over the same shared bytes.
pub struct Catalog<'s> {
    #[allow(dead_code)]
    segment: &'s Segment,
    header: *const CatalogHeader,
    slots: *mut CatalogSlot,
    capacity: usize,
}

impl<'s> Catalog<'s> {
    /// Payload bytes a catalog of `capacity` slots needs.
    #[inline]
    pub const fn required_bytes(capacity: u32) -> usize {
        slots_offset() + (capacity as usize) * core::mem::size_of::<CatalogSlot>()
    }

    /// The total segment size (header + payload) a catalog of `capacity` needs.
    #[inline]
    pub const fn segment_bytes(capacity: u32) -> usize {
        HEADER_SIZE + Self::required_bytes(capacity)
    }

    /// Initialise a fresh catalog into `segment`: write the header and zero every
    /// slot to [`SLOT_FREE`].
    ///
    /// `head_stride` is the byte stride between consecutive entries'
    /// `ArtifactHead`s in the head segment; `artifact_id_base` is the first
    /// lineage id (kept well above the coordinator's per-name artifact id space so
    /// the two never collide in a journal record).
    pub fn init(
        segment: &'s Segment,
        capacity: u32,
        head_stride: u32,
        artifact_id_base: u32,
    ) -> Result<Catalog<'s>> {
        let need = Self::required_bytes(capacity);
        if need > segment.payload_len() {
            return Err(Error::Core(shm_core::Error::LayoutOverflow(
                "catalog does not fit in segment",
            )));
        }
        let base = segment.payload_ptr();
        // SAFETY: every offset below is `< need <= payload_len`, so all writes
        // stay within the mapped payload region; the segment is exclusively owned
        // by the coordinator for this init.
        unsafe {
            base.cast::<CatalogHeader>().write(CatalogHeader {
                magic: ShmU64::new(CATALOG_MAGIC),
                capacity: ShmU32::new(capacity),
                next_slot: ShmU32::new(0),
                head_stride: ShmU32::new(head_stride),
                artifact_id_base: ShmU32::new(artifact_id_base),
                free_head: ShmU64::new(pack_free_head(FREE_NIL, 0)),
            });
            let slots = base.add(slots_offset()).cast::<CatalogSlot>();
            for i in 0..capacity as usize {
                slots.add(i).write(CatalogSlot {
                    key_id: ShmU32::new(0),
                    artifact_id: ShmU32::new(0),
                    head_off: ShmU32::new(0),
                    kind: ShmU32::new(0),
                    word: ShmU64::new(pack_slot_word(FIRST_GEN, SLOT_FREE)),
                    next_free: ShmU32::new(FREE_NIL),
                    _pad: 0,
                });
            }
        }
        Self::attach(segment)
    }

    /// Attach to a catalog previously [`init`](Self::init)ialised in `segment`.
    pub fn attach(segment: &'s Segment) -> Result<Catalog<'s>> {
        let base = segment.payload_ptr();
        // SAFETY: the header lives at the payload base; the store guarantees at
        // least `size_of::<CatalogHeader>()` payload bytes.
        let hdr = unsafe { &*base.cast::<CatalogHeader>() };
        if hdr.magic.load(Acquire) != CATALOG_MAGIC {
            return Err(Error::BadCatalog);
        }
        let capacity = hdr.capacity.load(Acquire) as usize;
        // SAFETY: `slots_offset()` is within the payload for any real catalog.
        let slots = unsafe { base.add(slots_offset()).cast::<CatalogSlot>() };
        Ok(Catalog {
            segment,
            header: base.cast::<CatalogHeader>(),
            slots,
            capacity,
        })
    }

    #[inline]
    fn header(&self) -> &CatalogHeader {
        // SAFETY: header lives at the payload base for the catalog's lifetime.
        unsafe { &*self.header }
    }

    /// The fixed slot capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The byte stride between consecutive entries' `ArtifactHead`s.
    #[inline]
    pub fn head_stride(&self) -> u32 {
        self.header().head_stride.load(Acquire)
    }

    /// The first lineage id (`artifact_id` of slot 0).
    #[inline]
    pub fn artifact_id_base(&self) -> u32 {
        self.header().artifact_id_base.load(Acquire)
    }

    /// The append **high-water mark**: how far `next_slot` ever advanced. Bounds
    /// every scan over the table. Reclaimed slots are recycled below it rather
    /// than growing it, so under steady churn this stops rising.
    #[inline]
    pub fn next_slot(&self) -> u32 {
        self.header().next_slot.load(Acquire)
    }

    /// Borrow slot `idx`. `idx` must be `< capacity`.
    #[inline]
    pub fn slot(&self, idx: u32) -> &CatalogSlot {
        debug_assert!((idx as usize) < self.capacity, "slot index out of range");
        // SAFETY: `idx < capacity`; the slot array has `capacity` entries.
        unsafe { &*self.slots.add(idx as usize) }
    }

    /// Claim a slot index the caller exclusively owns: pop a reclaimed one,
    /// else bump the append high-water mark. [`Error::CatalogFull`] once neither
    /// can supply one.
    ///
    /// The free list is tried **first** so a store under steady create/evict
    /// churn stops growing `next_slot` at all — which is the property that keeps
    /// [`find_live_by_key`](Self::find_live_by_key)'s scan bounded. An un-churned
    /// store never touches the list and behaves exactly as the append-only
    /// version did.
    pub fn alloc_slot(&self) -> Result<u32> {
        if let Some(idx) = self.pop_free() {
            return Ok(idx);
        }
        let idx = self.header().next_slot.fetch_add(1, AcqRel);
        if idx as usize >= self.capacity {
            // Undo the bump so a full catalog does not run `next_slot` away from
            // the high-water mark it is supposed to record.
            self.header().next_slot.fetch_sub(1, AcqRel);
            return Err(Error::CatalogFull(self.capacity as u32));
        }
        Ok(idx)
    }

    /// Pop a reclaimed slot off the free list. Delegates to the loom-checked
    /// [`treiber_pop`](shm_core::pool::treiber_pop) `shm-core` uses for its
    /// chunk free lists (the ABA-safe `{tag:32 | idx:32}` CAS loop is
    /// identical); the intrusive next-link lives in the slot's `next_free`.
    fn pop_free(&self) -> Option<u32> {
        shm_core::pool::treiber_pop(&self.header().free_head, |idx| {
            self.slot(idx).next_free.load(Acquire)
        })
    }

    /// Push a reclaimed slot onto the free list (the loom-checked
    /// [`treiber_push`](shm_core::pool::treiber_push)). The caller must own
    /// `idx` exclusively (it is `RECLAIMING`), so writing its link before the
    /// head CAS publishes it is safe.
    fn push_free(&self, idx: u32) {
        shm_core::pool::treiber_push(&self.header().free_head, idx, |idx, next| {
            self.slot(idx).next_free.store(next, Release);
        });
    }

    /// Try to return one tombstoned slot to the free list.
    ///
    /// `quiescent` is asked **only** while this sweep holds the slot in
    /// [`SLOT_RECLAIMING`], where no creator can allocate it — so a `true`
    /// answer cannot be invalidated by a new occupant appearing underneath. It
    /// receives the slot's `artifact_id`/`head_off` and must report whether that
    /// entry still holds anything (a live version, a pin, a write lease); it may
    /// also *drive* the entry toward quiescence first, as the store's predicate
    /// does by re-running the teardown. The caller supplies it because the
    /// catalog does not own the head segment.
    ///
    /// Returns `true` iff the slot was reclaimed. A slot whose `gen` would wrap
    /// is deliberately **not** reclaimed and never will be: see the module doc.
    pub fn try_reclaim<F>(&self, idx: u32, quiescent: F) -> bool
    where
        F: FnOnce(u32, u32) -> bool,
    {
        let slot = self.slot(idx);
        if !slot.begin_reclaim() {
            return false; // not tombstoned, or another sweep owns it
        }
        if !quiescent(slot.artifact_id(), slot.head_off()) {
            slot.abort_reclaim();
            return false;
        }
        match slot.finish_reclaim() {
            Some(_) => {
                self.push_free(idx);
                true
            }
            None => {
                // `gen` exhausted: leave the slot in RECLAIMING forever rather
                // than hand a future occupant a recycled incarnation.
                false
            }
        }
    }

    /// Sweep every appended slot, reclaiming each tombstone whose entry
    /// `quiescent` reports finished. Returns how many slots were freed.
    ///
    /// Driven from the coordinator's existing lease-monitor tick: no new thread,
    /// no new timer. `evict` attempts its own slot inline first, so this only
    /// ever picks up entries that were still busy at eviction time.
    pub fn reclaim_tombstones<F>(&self, mut quiescent: F) -> usize
    where
        F: FnMut(u32, u32) -> bool,
    {
        let n = self.next_slot().min(self.capacity as u32);
        (0..n)
            .filter(|&idx| {
                self.slot(idx).state() == SLOT_TOMBSTONE
                    && self.try_reclaim(idx, &mut quiescent)
            })
            .count()
    }

    /// The `head_off` for slot `idx` (`idx * head_stride`).
    #[inline]
    pub fn head_off_for(&self, idx: u32) -> u32 {
        idx.wrapping_mul(self.head_stride())
    }

    /// The lineage `artifact_id` for slot `idx` (`artifact_id_base + idx`).
    #[inline]
    pub fn artifact_id_for(&self, idx: u32) -> u32 {
        self.artifact_id_base().wrapping_add(idx)
    }

    /// Populate and publish slot `idx` for `key_id`/`kind`, returning its derived
    /// `(artifact_id, head_off)`. Writes the data fields, then CAS-publishes
    /// `FREE → LIVE`.
    pub fn publish_slot(&self, idx: u32, key_id: u32, kind: RefKind) -> (u32, u32) {
        let artifact_id = self.artifact_id_for(idx);
        let head_off = self.head_off_for(idx);
        let slot = self.slot(idx);
        slot.write_fields(key_id, artifact_id, head_off, kind);
        slot.publish();
        (artifact_id, head_off)
    }

    /// Find the index of the single [`SLOT_LIVE`] slot tracking `key_id`, if any.
    /// Scans the appended slots (`0..next_slot`).
    pub fn find_live_by_key(&self, key_id: u32) -> Option<u32> {
        let n = self.next_slot().min(self.capacity as u32);
        for idx in 0..n {
            let slot = self.slot(idx);
            if slot.state() == SLOT_LIVE && slot.key_id() == key_id {
                return Some(idx);
            }
        }
        None
    }

    /// Resolve a lineage `artifact_id` back to its slot index (for a coordinator
    /// routing a journaled entry pin / write lease on crash). The index is derived
    /// (`artifact_id - base`) and then validated against the stored slot, so a
    /// stale or foreign id yields `None`.
    pub fn slot_for_artifact_id(&self, artifact_id: u32) -> Option<u32> {
        let base = self.artifact_id_base();
        if artifact_id < base {
            return None;
        }
        let idx = artifact_id - base;
        if idx >= self.next_slot().min(self.capacity as u32) {
            return None;
        }
        if self.slot(idx).artifact_id() == artifact_id {
            Some(idx)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_seg(id: u32, capacity: u32) -> Segment {
        let bytes = Catalog::segment_bytes(capacity);
        let _ = Segment::unlink_by_id(id);
        Segment::create(id, bytes).expect("create catalog seg")
    }

    #[test]
    fn ref_kind_round_trips() {
        for k in [
            RefKind::RawChunk,
            RefKind::Object,
            RefKind::Artifact,
            RefKind::Dataset,
            RefKind::Result,
        ] {
            assert_eq!(RefKind::from_u16(k.as_u16()).unwrap(), k);
        }
        assert!(matches!(RefKind::from_u16(5), Err(Error::BadKind(5))));
    }

    #[test]
    fn slot_cas_state_machine() {
        let base = 70_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 8);
        let cat = Catalog::init(&seg, 8, 2048, 1 << 28).expect("init");

        let idx = cat.alloc_slot().expect("alloc");
        let slot = cat.slot(idx);
        assert_eq!(slot.state(), SLOT_FREE, "fresh slot is FREE");

        // FREE -> LIVE (publish).
        let (aid, hoff) = cat.publish_slot(idx, 42, RefKind::Dataset);
        assert_eq!(slot.state(), SLOT_LIVE);
        assert_eq!(slot.key_id(), 42);
        assert_eq!(slot.artifact_id(), aid);
        assert_eq!(slot.head_off(), hoff);
        assert_eq!(slot.kind().unwrap(), RefKind::Dataset);
        // A second publish CAS is a no-op (already LIVE).
        assert!(!slot.publish());

        // LIVE -> TOMBSTONE (evict), and idempotent re-evict.
        assert!(slot.tombstone(), "first evict transitions LIVE->TOMBSTONE");
        assert_eq!(slot.state(), SLOT_TOMBSTONE);
        assert!(!slot.tombstone(), "second evict is a no-op");

        seg.unlink().ok();
    }

    #[test]
    fn find_by_key_and_by_artifact_id() {
        let base = 71_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 16);
        let cat = Catalog::init(&seg, 16, 2048, 1 << 28).expect("init");

        let i0 = cat.alloc_slot().unwrap();
        let (aid0, _) = cat.publish_slot(i0, 100, RefKind::Dataset);
        let i1 = cat.alloc_slot().unwrap();
        let (aid1, _) = cat.publish_slot(i1, 200, RefKind::Result);

        assert_eq!(cat.find_live_by_key(100), Some(i0));
        assert_eq!(cat.find_live_by_key(200), Some(i1));
        assert_eq!(cat.find_live_by_key(999), None, "absent key");

        assert_eq!(cat.slot_for_artifact_id(aid0), Some(i0));
        assert_eq!(cat.slot_for_artifact_id(aid1), Some(i1));
        assert_eq!(cat.slot_for_artifact_id(1), None, "below base");
        assert_eq!(cat.slot_for_artifact_id(aid1 + 1000), None, "unappended");

        // Tombstoning i0 makes its key un-findable but re-create appends a new slot.
        assert!(cat.slot(i0).tombstone());
        assert_eq!(cat.find_live_by_key(100), None, "tombstoned key is gone");
        let i2 = cat.alloc_slot().unwrap();
        let (aid2, _) = cat.publish_slot(i2, 100, RefKind::Dataset);
        assert_eq!(
            cat.find_live_by_key(100),
            Some(i2),
            "re-create finds new slot"
        );
        assert_ne!(aid2, aid0, "reincarnation has a new lineage id");

        seg.unlink().ok();
    }

    #[test]
    fn reclaim_recycles_the_index_and_advances_the_incarnation() {
        let base = 73_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 4);
        let cat = Catalog::init(&seg, 4, 2048, 1 << 28).expect("init");

        let i0 = cat.alloc_slot().unwrap();
        let gen0 = cat.slot(i0).gen();
        cat.publish_slot(i0, 100, RefKind::Dataset);
        assert!(cat.slot(i0).tombstone());

        // A busy entry is refused and left exactly as it was found.
        assert!(!cat.try_reclaim(i0, |_, _| false));
        assert_eq!(cat.slot(i0).state(), SLOT_TOMBSTONE);
        assert_eq!(cat.slot(i0).gen(), gen0, "an aborted sweep advances nothing");

        // A finished one comes back with the next incarnation staged.
        assert!(cat.try_reclaim(i0, |_, _| true));
        assert_eq!(cat.slot(i0).state(), SLOT_FREE);
        assert_eq!(cat.slot(i0).gen(), gen0 + 1);
        assert_eq!(cat.slot(i0).key_id(), 0, "the recycled slot forgets its key");

        // The next allocation reuses the index instead of growing the table.
        let i1 = cat.alloc_slot().unwrap();
        assert_eq!(i1, i0, "the free list is preferred over the high-water bump");
        assert_eq!(cat.next_slot(), 1, "and the high-water mark did not move");

        seg.unlink().ok();
    }

    #[test]
    fn a_slot_whose_incarnation_would_wrap_is_retired_forever() {
        let base = 74_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 2);
        let cat = Catalog::init(&seg, 2, 2048, 1 << 28).expect("init");

        let idx = cat.alloc_slot().unwrap();
        cat.publish_slot(idx, 1, RefKind::Dataset);
        assert!(cat.slot(idx).tombstone());
        // Jump `gen` to the last value it can hold.
        cat.slot(idx).word.store(pack_slot_word(u32::MAX, SLOT_TOMBSTONE), Release);

        assert!(
            !cat.try_reclaim(idx, |_, _| true),
            "a quiescent slot is still refused when its incarnation cannot advance"
        );
        assert_eq!(
            cat.slot(idx).state(),
            SLOT_RECLAIMING,
            "it is parked, not returned: one slot lost beats a reused incarnation"
        );
        assert_eq!(cat.alloc_slot().unwrap(), 1, "the free list stayed empty");

        seg.unlink().ok();
    }

    #[test]
    fn alloc_slot_exhausts_cleanly() {
        let base = 72_000 + (std::process::id() & 0x3ff);
        let seg = catalog_seg(base, 2);
        let cat = Catalog::init(&seg, 2, 2048, 1 << 28).expect("init");
        assert_eq!(cat.alloc_slot().unwrap(), 0);
        assert_eq!(cat.alloc_slot().unwrap(), 1);
        assert!(matches!(cat.alloc_slot(), Err(Error::CatalogFull(2))));
        assert_eq!(
            cat.next_slot(),
            2,
            "a refused allocation leaves the high-water mark at capacity"
        );
        seg.unlink().ok();
    }
}
