//! Single-process proof of the keyed store (ADR-0007 G3), with no coordinator:
//! the test builds the three store segments by hand and injects a trivial
//! in-memory [`KeyResolver`]. It exercises create / open-by-key / open-by-id,
//! commit v1→v3 + pin/read equality, evict (tombstone + full chunk reclaim to a
//! zero-leak census), and clean errors for an absent / tombstoned key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use shm_arrow::SchemaRegistry;
use shm_artifact::{ArtifactHead, Commit};
use shm_core::{segment::HEADER_SIZE, BorrowJournal, Pool, PoolConfig, Segment};
use shm_store::{
    Catalog, Error, KeyResolver, KeyedStore, RefKind, SLOT_FREE, SLOT_LIVE, SLOT_TOMBSTONE,
};

/// A trivial in-memory key interner (the coordinator's job in the real system):
/// same bytes → same monotonic id, different bytes → different id.
#[derive(Default)]
struct MemResolver {
    inner: Mutex<(HashMap<Vec<u8>, u32>, u32)>,
}

impl KeyResolver for MemResolver {
    fn intern_key(&self, key: &[u8]) -> shm_store::Result<u32> {
        let mut g = self.inner.lock().unwrap();
        if let Some(&id) = g.0.get(key) {
            return Ok(id);
        }
        g.1 += 1;
        let id = g.1;
        g.0.insert(key.to_vec(), id);
        Ok(id)
    }
}

/// The store's three owned segments plus the actor journal, kept mapped for the
/// test's duration.
struct Harness {
    catalog: Arc<Segment>,
    head: Arc<Segment>,
    data: Arc<Segment>,
    journal: Arc<Segment>,
    registry: Arc<SchemaRegistry>,
    resolver: MemResolver,
}

impl Harness {
    fn new(base: u32, schema: &SchemaRef) -> Harness {
        let cap = 16u32;
        let head_stride = (ArtifactHead::region_bytes() + 63) & !63;
        for id in base..base + 4 {
            let _ = Segment::unlink_by_id(id);
        }
        let catalog = Segment::create(base, Catalog::segment_bytes(cap)).expect("catalog seg");
        Catalog::init(&catalog, cap, head_stride as u32, 1 << 28).expect("catalog init");
        let head =
            Segment::create(base + 1, HEADER_SIZE + cap as usize * head_stride).expect("head seg");
        let data = Segment::create(base + 2, 1 << 20).expect("data seg");
        Pool::create(&data, &PoolConfig::power_of_two(256, 8192, 32)).expect("pool");
        let journal = Segment::create(base + 3, 64 * 1024).expect("journal seg");
        BorrowJournal::create(&journal, 256).expect("journal");
        Harness {
            catalog: Arc::new(catalog),
            head: Arc::new(head),
            data: Arc::new(data),
            journal: Arc::new(journal),
            registry: Arc::new(SchemaRegistry::with_schemas(std::slice::from_ref(schema))),
            resolver: MemResolver::default(),
        }
    }

    fn store(&self) -> KeyedStore<'_> {
        KeyedStore::new(
            self.catalog.clone(),
            self.head.clone(),
            self.data.clone(),
            self.journal.clone(),
            self.registry.clone(),
            /* owner */ 7,
            &self.resolver,
        )
    }

    /// Total free chunks across the data pool's classes (the leak census).
    fn free_total(&self) -> usize {
        let pool = Pool::attach(&self.data).expect("pool attach");
        (0..pool.num_classes()).map(|c| pool.free_count(c)).sum()
    }

    fn unlink(&self) {
        self.catalog.unlink().ok();
        self.head.unlink().ok();
        self.data.unlink().ok();
        self.journal.unlink().ok();
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch(schema: &SchemaRef, vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vals.to_vec()))],
    )
    .unwrap()
}

#[test]
fn create_commit_v1_to_v3_open_by_key_and_id_read_equal() {
    let sch = schema();
    let h = Harness::new(80_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();

    let entry = store
        .create(b"dataset/X", RefKind::Dataset, &sch)
        .expect("create");
    assert_eq!(entry.kind(), RefKind::Dataset);
    assert_eq!(entry.current_version(), 0, "nothing committed yet");
    let key_id = entry.key_id();

    let b1 = batch(&sch, &[1, 2, 3]);
    let b2 = batch(&sch, &[10, 20, 30]);
    let b3 = batch(&sch, &[100, 200, 300]);
    assert_eq!(entry.commit(Commit::Replace, &b1).expect("v1"), 1);
    assert_eq!(entry.commit(Commit::Replace, &b2).expect("v2"), 2);
    assert_eq!(entry.commit_replace(&b3).expect("v3"), 3);
    assert_eq!(entry.current_version(), 3);

    // Open by key in a *fresh* handle and read the current version zero-copy.
    let reopened = store.open(b"dataset/X").expect("open by key");
    let (pin, got) = reopened.read().expect("read");
    assert_eq!(pin.version(), 3);
    assert_eq!(got, b3, "the pinned current version equals the last commit");
    drop(pin);

    // Open by interned id resolves to the same live entry.
    let by_id = store.open_id(key_id).expect("open_id");
    assert_eq!(by_id.artifact_id(), entry.artifact_id());
    assert_eq!(by_id.current_version(), 3);

    h.unlink();
}

#[test]
fn create_is_idempotent_and_keys_are_distinct() {
    let sch = schema();
    let h = Harness::new(81_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();

    let a = store
        .create(b"a", RefKind::Dataset, &sch)
        .expect("create a");
    let a_again = store
        .create(b"a", RefKind::Dataset, &sch)
        .expect("re-create a");
    assert_eq!(
        a.artifact_id(),
        a_again.artifact_id(),
        "same key returns the same live entry (get-or-create)"
    );

    let b = store.create(b"b", RefKind::Result, &sch).expect("create b");
    assert_ne!(
        a.artifact_id(),
        b.artifact_id(),
        "different keys, different entries"
    );
    assert_ne!(a.key_id(), b.key_id());

    h.unlink();
}

#[test]
fn evict_tombstones_reclaims_to_baseline_and_absent_open_errors() {
    let sch = schema();
    let h = Harness::new(82_000 + (std::process::id() & 0x3ff), &sch);

    // Open of an absent key errors cleanly, before anything exists.
    assert!(matches!(h.store().open(b"dataset/X"), Err(Error::NotFound)));

    let baseline = h.free_total();

    let store = h.store();
    let entry = store
        .create(b"dataset/X", RefKind::Dataset, &sch)
        .expect("create");
    entry.commit_replace(&batch(&sch, &[1, 2, 3])).unwrap();
    entry.commit_replace(&batch(&sch, &[4, 5, 6])).unwrap();
    entry.commit_replace(&batch(&sch, &[7, 8, 9])).unwrap();
    assert!(
        h.free_total() < baseline,
        "committing versions consumed chunks ({} < {})",
        h.free_total(),
        baseline
    );

    // Evict: tombstone + full teardown. The census returns to baseline (zero leak).
    store.evict(b"dataset/X").expect("evict");
    assert_eq!(
        h.free_total(),
        baseline,
        "evict reclaimed every chunk by refcount"
    );

    // Open of a tombstoned key errors cleanly; evict is idempotent.
    assert!(matches!(store.open(b"dataset/X"), Err(Error::NotFound)));
    store.evict(b"dataset/X").expect("second evict is a no-op");

    // A re-create after eviction appends a fresh entry with a new lineage id.
    let e2 = store
        .create(b"dataset/X", RefKind::Dataset, &sch)
        .expect("re-create");
    assert_eq!(e2.current_version(), 0, "fresh reincarnation");

    h.unlink();
}

// ---------------------------------------------------------------------------
// ADR-0008 P0.1 — slot reclamation
//
// Every test below fails on the append-only (pre-P0.1) catalog.
// ---------------------------------------------------------------------------

/// **The blocker.** Create + evict far past `store_capacity`.
///
/// On the append-only catalog this dies at entry 17 of a 16-slot table with
/// `CatalogFull` — `capacity` was a cap on entries created *for the
/// coordinator's lifetime*, not entries retained. With reclamation the same
/// workload runs indefinitely, the high-water mark stops rising, and the chunk
/// pool returns to its baseline.
#[test]
fn create_evict_churn_runs_far_past_capacity() {
    let sch = schema();
    let h = Harness::new(84_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();
    let baseline = h.free_total();

    // 16 slots; 200 entries is >12x the table.
    for i in 0..200i64 {
        let key = format!("churn/{i}");
        let e = store
            .create(key.as_bytes(), RefKind::Dataset, &sch)
            .unwrap_or_else(|err| panic!("create #{i} failed: {err}"));
        e.commit(Commit::Replace, &batch(&sch, &[i])).unwrap();
        store.evict(key.as_bytes()).unwrap();
    }

    let cat = Catalog::attach(&h.catalog).unwrap();
    assert!(
        cat.next_slot() as usize <= 16,
        "the high-water mark stops rising once slots recycle: next_slot={}",
        cat.next_slot()
    );
    assert_eq!(
        h.free_total(),
        baseline,
        "every churned entry's chunks came back (zero leak across 200 recycles)"
    );
    h.unlink();
}

/// A recycled slot hands its next occupant a **fresh incarnation**, and the
/// previous occupant's handle is refused rather than silently retargeted at it.
#[test]
fn reclaimed_slot_gets_a_new_incarnation_and_stale_handles_fail() {
    let sch = schema();
    let h = Harness::new(85_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();

    let stale = store.create(b"k", RefKind::Dataset, &sch).unwrap();
    stale.commit(Commit::Replace, &batch(&sch, &[1])).unwrap();
    let first_incarnation = stale.artifact().incarnation();

    store.evict(b"k").unwrap();

    let cat = Catalog::attach(&h.catalog).unwrap();
    assert_eq!(
        cat.slot(0).state(),
        SLOT_FREE,
        "an unpinned entry is reclaimed inline by evict"
    );

    // The next create reuses slot 0 — with a different incarnation.
    let fresh = store.create(b"k2", RefKind::Dataset, &sch).unwrap();
    assert_eq!(
        cat.next_slot(),
        1,
        "the create recycled slot 0 rather than appending a second slot"
    );
    assert_eq!(cat.slot(0).state(), SLOT_LIVE);
    assert_ne!(
        fresh.artifact().incarnation(),
        first_incarnation,
        "the slot's next occupant never reuses an incarnation"
    );

    // Give the new occupant a live version, so the stale reader below is refused
    // by the *incarnation* check rather than merely finding nothing to read.
    fresh.commit(Commit::Replace, &batch(&sch, &[9])).unwrap();

    // The handle from the previous occupant now names something that is gone.
    assert!(
        matches!(
            stale.commit(Commit::Replace, &batch(&sch, &[2])),
            Err(Error::Artifact(shm_artifact::Error::Stale))
        ),
        "a commit through a stale handle is refused, not applied to the new occupant"
    );
    assert!(
        matches!(
            stale.pin(),
            Err(Error::Artifact(shm_artifact::Error::Stale))
        ),
        "and so is a read, even though the region now holds a perfectly valid \
         version — it belongs to someone else"
    );

    // ...and the new occupant is untouched by any of it.
    let (_pin, got) = fresh.read().unwrap();
    assert_eq!(got, batch(&sch, &[9]));
    h.unlink();
}

/// Eviction is a **level, not an edge**: a straggler handle held across the
/// evict can still commit (its incarnation is still in service while the slot
/// is merely tombstoned), and before the sweep re-ran the teardown that
/// resurrected version made the entry permanently non-quiescent — the slot and
/// its chunks leaked forever. The sweep now tears down again before judging
/// quiescence, so the tombstone converges regardless, and the straggler's next
/// operation after the reclaim fails `Stale`.
#[test]
fn a_straggler_commit_after_evict_cannot_wedge_the_slot() {
    let sch = schema();
    let h = Harness::new(87_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();
    let baseline = h.free_total();

    let straggler = store.create(b"wedge", RefKind::Dataset, &sch).unwrap();
    straggler
        .commit(Commit::Replace, &batch(&sch, &[1]))
        .unwrap();

    // A reader's pin keeps the entry busy across the evict, so the slot stays
    // TOMBSTONE (no inline reclaim) and the straggler's incarnation stays in
    // service.
    let pin = straggler.pin().expect("reader pin");
    store.evict(b"wedge").unwrap();
    let cat = Catalog::attach(&h.catalog).unwrap();
    assert_eq!(cat.slot(0).state(), SLOT_TOMBSTONE);

    // The straggler resurrects a version onto the tombstoned entry. This is
    // allowed — its registration re-validates an incarnation that is still in
    // service — but it must not be able to wedge the slot.
    straggler
        .commit(Commit::Replace, &batch(&sch, &[2]))
        .expect("a straggler commit before the reclaim still succeeds");

    drop(pin);
    assert_eq!(
        store.reclaim_tombstones().unwrap(),
        1,
        "the sweep re-runs the teardown, so the resurrected version cannot \
         keep the slot from reclaiming"
    );
    assert_eq!(cat.slot(0).state(), SLOT_FREE);
    assert_eq!(
        h.free_total(),
        baseline,
        "the resurrected version's chunks came back too"
    );
    assert!(
        matches!(
            straggler.commit(Commit::Replace, &batch(&sch, &[3])),
            Err(Error::Artifact(shm_artifact::Error::Stale))
        ),
        "after the reclaim the straggler is told the entry is gone"
    );
    h.unlink();
}

/// Reclamation is **deferred**: a live reader's pin holds the slot in
/// `TOMBSTONE`, and only the sweep that runs after the pin drops frees it.
#[test]
fn a_pinned_entry_defers_reclaim_until_the_pin_drops() {
    let sch = schema();
    let h = Harness::new(86_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();

    let e = store.create(b"pinned", RefKind::Dataset, &sch).unwrap();
    e.commit(Commit::Replace, &batch(&sch, &[7])).unwrap();
    let pin = e.pin().expect("reader pins the current version");

    store.evict(b"pinned").unwrap();
    let cat = Catalog::attach(&h.catalog).unwrap();
    assert_eq!(
        cat.slot(0).state(),
        SLOT_TOMBSTONE,
        "a pinned entry is NOT reclaimed inline — the reader is still reading"
    );
    assert_eq!(
        store.reclaim_tombstones().unwrap(),
        0,
        "and a sweep while the pin is live frees nothing"
    );
    assert_eq!(cat.slot(0).state(), SLOT_TOMBSTONE, "the abort restores it");

    drop(pin);
    assert_eq!(
        store.reclaim_tombstones().unwrap(),
        1,
        "once the pin drops, the sweep reclaims the slot"
    );
    assert_eq!(cat.slot(0).state(), SLOT_FREE);
    h.unlink();
}

/// P0.3 (ADR-0010, G12a): the write lease dies with the **entry**, not with the
/// actor. A tombstoned entry whose exclusive write lease is held by a
/// live-but-idle committer must still converge to `SLOT_FREE`: `evict` (via
/// `Artifact::evict_all`) force-releases the lease with a fence bump, so the
/// holder's late commit fails `Fenced` *before staging anything* and the slot
/// reclaims as soon as its last pin drops.
///
/// Without the force-release this deadlocks by design: `is_quiescent` requires
/// the lease unowned, nothing ever releases a live actor's lease, and the slot
/// stays `TOMBSTONE` forever.
#[test]
fn a_tombstoned_entry_with_a_held_write_lease_converges() {
    let sch = schema();
    let h = Harness::new(88_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();
    let baseline = h.free_total();

    let entry = store.create(b"leased", RefKind::Dataset, &sch).unwrap();
    entry.commit(Commit::Replace, &batch(&sch, &[1])).unwrap();

    // A reader pin keeps the entry busy across the evict (so the slot stays
    // TOMBSTONE and the incarnation stays in service — the window where the
    // fenced holder's late commit is observable as `Fenced`, not `Stale`).
    let pin = entry.pin().expect("reader pin");

    // A live-but-idle writer holds the fenced exclusive lease across the evict.
    let mut committer = entry
        .artifact()
        .open_exclusive(9)
        .expect("exclusive lease before the evict");

    store.evict(b"leased").unwrap();
    let cat = Catalog::attach(&h.catalog).unwrap();
    assert_eq!(cat.slot(0).state(), SLOT_TOMBSTONE, "pin defers the reclaim");

    // The evict force-released the lease with a fence bump (entry-lifecycle-
    // tied lease): the idle holder's token is stale, so its late commit is
    // rejected before it can resurrect a version onto the tombstoned entry.
    assert!(
        matches!(
            committer.commit(Commit::Replace, &batch(&sch, &[2]), h.registry.as_ref()),
            Err(shm_artifact::Error::Fenced)
        ),
        "the fenced holder's late commit must fail Fenced before staging"
    );

    // With the lease force-released, the entry converges as soon as the pin
    // drops — the lease-holding actor never has to die.
    drop(pin);
    assert_eq!(
        store.reclaim_tombstones().unwrap(),
        1,
        "a tombstoned entry with a (former) live lease holder must reclaim"
    );
    assert_eq!(cat.slot(0).state(), SLOT_FREE);
    assert_eq!(h.free_total(), baseline, "no chunk leaked");
    drop(committer);
    h.unlink();
}

/// P0.3 (ADR-0010, G4): `Entry::evict_current` drops the data but keeps the
/// address — the entry stays `LIVE`, its key still resolves, readers see
/// `VersionGone` (identical to a never-committed entry), and the next commit
/// continues the version sequence. This is the output-side `clear_on_ack`
/// ArrowRef needs (ADR-0005 §4 G4: the spike had to demonstrate clear-on-ack
/// on the *input* because the current version could not be retired).
#[test]
fn entry_evict_current_keeps_the_entry_and_continues_the_sequence() {
    let sch = schema();
    let h = Harness::new(89_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();
    let baseline = h.free_total();

    let entry = store.create(b"output", RefKind::Dataset, &sch).unwrap();
    assert_eq!(entry.commit_replace(&batch(&sch, &[10, 20])).unwrap(), 1);

    let empty = entry.evict_current().unwrap();
    assert_eq!(empty, 2, "the empty version continues the sequence");
    assert_eq!(
        baseline - h.free_total(),
        1,
        "v1's chunks reclaimed; only the empty manifest chunk remains"
    );

    // The key still resolves — the entry was NOT evicted.
    let reopened = store.open(b"output").expect("entry stays LIVE");
    assert_eq!(reopened.current_version(), 2);
    let cat = Catalog::attach(&h.catalog).unwrap();
    assert_eq!(cat.slot(0).state(), SLOT_LIVE);

    // A read of the evicted-current entry reports VersionGone, like a
    // never-committed entry.
    let pin = reopened.pin().expect("empty current is pinnable");
    assert!(matches!(
        pin.as_arrow(&h.registry),
        Err(shm_artifact::Error::VersionGone)
    ));
    drop(pin);

    // The lineage continues.
    assert_eq!(reopened.commit_replace(&batch(&sch, &[30])).unwrap(), 3);
    let (_pin, b) = reopened.read().unwrap();
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values(),
        &[30]
    );
    h.unlink();
}

/// P0.3 (ADR-0010, G12b): `retain_current` takes a guard-less, unjournaled pin
/// that survives the retaining actor (no Drop, no journal entry), holds the
/// entry against reclamation while armed, and is released exactly-once through
/// `release_task_binding` — which drops (rather than misroutes) a binding whose
/// incarnation no longer matches the slot's occupant.
#[test]
fn retained_binding_ties_pin_to_entry_and_task_not_actor() {
    let sch = schema();
    let h = Harness::new(90_000 + (std::process::id() & 0x3ff), &sch);
    let store = h.store();
    let baseline = h.free_total();

    let entry = store.create(b"task-input", RefKind::Dataset, &sch).unwrap();
    entry.commit_replace(&batch(&sch, &[1, 2])).unwrap();

    // Retain v1 for a task: pin count 1 with NO VersionPin guard held.
    let binding = entry.retain_current().expect("retain current");
    // The arm is simulated here; hand the journaled retain off as the real
    // protocol would after a successful `submit_with_binding`.
    entry.binding_armed(&binding).expect("handoff");
    assert_eq!(binding.version, 1);
    assert_eq!(entry.artifact().version_pin_count(1), Some(1));

    // The retained pin freezes v1 across a supersede, like any reader pin.
    entry.commit_replace(&batch(&sch, &[3])).unwrap();
    assert_eq!(
        entry.artifact().version_pin_count(1),
        Some(1),
        "superseded v1 survives while the task binding is armed"
    );

    // A binding forged against the wrong occupant is DROPPED, not misrouted.
    assert!(!shm_store::release_task_binding(
        &h.catalog,
        &h.head,
        &h.data,
        binding.artifact_id,
        binding.incarnation + 1,
        binding.version,
    ));
    assert_eq!(entry.artifact().version_pin_count(1), Some(1), "untouched");

    // The real release retires v1 through the standard pin-drop path.
    assert!(shm_store::release_task_binding(
        &h.catalog,
        &h.head,
        &h.data,
        binding.artifact_id,
        binding.incarnation,
        binding.version,
    ));
    assert_eq!(entry.artifact().version_pin_count(1), None, "v1 retired");
    // A double release finds nothing (the slot no longer tracks v1).
    assert!(!shm_store::release_task_binding(
        &h.catalog,
        &h.head,
        &h.data,
        binding.artifact_id,
        binding.incarnation,
        binding.version,
    ));

    // An armed binding also blocks slot reclamation: retain the current
    // version, evict the entry, and the slot must stay TOMBSTONE until the
    // binding is released — then the sweep frees it and the census balances.
    let held = entry.retain_current().expect("retain v2");
    entry.binding_armed(&held).expect("handoff");
    store.evict(b"task-input").unwrap();
    let cat = Catalog::attach(&h.catalog).unwrap();
    assert_eq!(cat.slot(0).state(), SLOT_TOMBSTONE, "binding defers reclaim");
    assert_eq!(store.reclaim_tombstones().unwrap(), 0);

    assert!(shm_store::release_task_binding(
        &h.catalog,
        &h.head,
        &h.data,
        held.artifact_id,
        held.incarnation,
        held.version,
    ));
    assert_eq!(store.reclaim_tombstones().unwrap(), 1);
    assert_eq!(cat.slot(0).state(), SLOT_FREE);
    assert_eq!(h.free_total(), baseline, "zero leak");
    h.unlink();
}
