//! Runtime configuration: segment geometry, journal capacity, and lease timing.

use std::time::Duration;

use shm_core::PoolConfig;

/// Tunables for a [`Coordinator`](crate::Coordinator) and the actors it hosts.
///
/// The defaults are sized for the v0.1 walking skeleton (a handful of small
/// batches on one topic) and for robust cross-process kill-9 timing in a busy
/// CI sandbox: a 150 ms heartbeat well inside a 500 ms lease deadline, polled
/// every 40 ms.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Base segment id; the coordinator derives every segment id from this so
    /// concurrent test runs (each with a distinct base) never collide on shm
    /// names. Payload = `base`, ring(topic i) = `base + 1 + i`, journal(actor a)
    /// = `base + JOURNAL_ID_OFFSET + a`.
    pub seg_base: u32,

    /// Size in bytes of the shared payload segment (holds the pool + chunks).
    pub payload_size: usize,

    /// Pool geometry carved into the payload segment.
    pub pool: PoolConfig,

    /// Per-topic broadcast ring capacity (slots; must be a power of two).
    pub ring_capacity: u32,

    /// Per-actor borrow-journal capacity (pins).
    pub journal_capacity: usize,

    /// How often an actor sends a heartbeat over its control connection.
    pub heartbeat_interval: Duration,

    /// How long the coordinator waits without a heartbeat before declaring an
    /// actor dead and replaying its borrow journal. Must be comfortably larger
    /// than [`heartbeat_interval`](Self::heartbeat_interval).
    pub lease_deadline: Duration,

    /// How often the lease monitor wakes to check for expired leases.
    pub monitor_tick: Duration,

    /// Slot count of the built-in [`shm_task`] task queue (power of two).
    pub task_capacity: u32,

    /// Reap retry cap for the built-in task queue (see
    /// [`shm_task::TaskQueue::reap`]).
    pub task_max_retries: u32,

    /// **P0.3 (ADR-0010, G12).** Grace period the coordinator's binding reap
    /// ([`shm_task::TaskQueue::reap_bindings`]) adds to a task's submit
    /// deadline before force-releasing the task's retained-ref bindings when
    /// the requester never acked. This bounds how long a terminal task's
    /// retained input/output stays readable without an ack — ArrowRef's
    /// retention/TTL semantics surfacing in the substrate. A multiple of
    /// [`lease_deadline`](Self::lease_deadline) by default (4×), so a
    /// requester always outlives at least a few heartbeat leases' worth of
    /// ack window.
    pub task_binding_grace: Duration,

    /// Pool geometry carved into each artifact's data segment (backs data +
    /// manifest chunks).
    pub artifact_pool: PoolConfig,

    /// Size in bytes of each artifact's data segment.
    pub artifact_data_size: usize,

    /// Slot capacity of the built-in keyed store's catalog (ADR-0007 G3). Each
    /// `create` appends one slot (never reused after eviction), so this bounds the
    /// number of distinct-entry lifetimes.
    pub store_capacity: u32,

    /// Pool geometry carved into the keyed store's shared data segment (backs
    /// every entry's data + manifest chunks).
    pub store_pool: PoolConfig,

    /// Size in bytes of the keyed store's shared data segment.
    pub store_data_size: usize,
}

/// Id offset separating the journal-segment id range from payload/ring ids.
pub const JOURNAL_ID_OFFSET: u32 = 1024;

/// Id offset of the built-in task-queue segment (below the journal range, above
/// the ring range).
pub const TASKQ_ID_OFFSET: u32 = 512;

/// Base id offset of the per-artifact segment pair (head, data). Kept well above
/// the journal range so a busy actor/topic/artifact population never collides.
pub const ARTIFACT_ID_OFFSET: u32 = 4096;

/// Base id offset of the keyed store's three segments (catalog, head, data). Kept
/// well above the per-artifact range so populations never collide.
pub const STORE_ID_OFFSET: u32 = 8192;

/// First lineage `artifact_id` the keyed store issues (ADR-0007 G3). Kept far
/// above the coordinator's per-name artifact id space (which starts at 1) so a
/// journaled entry pin's `artifact_id` unambiguously routes to the store rather
/// than to a normal hosted artifact.
pub const STORE_ARTIFACT_ID_BASE: u32 = 1 << 28;

impl Default for RuntimeConfig {
    fn default() -> RuntimeConfig {
        RuntimeConfig {
            seg_base: 1,
            // 1 MiB payload: comfortably fits the pool below plus headroom.
            payload_size: 1 << 20,
            // A couple of size classes; 64 KiB max chunk holds the demo batch.
            pool: PoolConfig::power_of_two(4096, 65536, 8),
            ring_capacity: 1024,
            journal_capacity: shm_core::DEFAULT_CAPACITY,
            heartbeat_interval: Duration::from_millis(150),
            lease_deadline: Duration::from_millis(500),
            monitor_tick: Duration::from_millis(40),
            task_capacity: 256,
            task_max_retries: shm_task::DEFAULT_MAX_RETRIES,
            // 4 × lease_deadline: a dead requester is declared dead well before
            // its unacked bindings are reaped.
            task_binding_grace: Duration::from_millis(2000),
            // Small classes (256 B..8 KiB) hold the cache-loop demo batch + its
            // manifest chunk comfortably and fit the 1 MiB data segment.
            artifact_pool: PoolConfig::power_of_two(256, 8192, 8),
            artifact_data_size: 1 << 20,
            // The keyed store shares one data pool across all entries, so give it
            // more chunks per class than a single artifact; small classes hold the
            // demo/skeleton batch + its manifest comfortably.
            store_capacity: 256,
            store_pool: PoolConfig::power_of_two(256, 8192, 32),
            store_data_size: 1 << 20,
        }
    }
}

impl RuntimeConfig {
    /// A config rooted at a specific segment-id base (keeps concurrent runs from
    /// colliding on shm names).
    pub fn with_seg_base(seg_base: u32) -> RuntimeConfig {
        RuntimeConfig {
            seg_base,
            ..RuntimeConfig::default()
        }
    }

    /// The payload segment's id.
    #[inline]
    pub fn payload_seg_id(&self) -> u32 {
        self.seg_base
    }

    /// The ring segment id for topic index `i`.
    #[inline]
    pub fn ring_seg_id(&self, topic_index: u32) -> u32 {
        self.seg_base + 1 + topic_index
    }

    /// The journal segment id for actor `actor_id`.
    #[inline]
    pub fn journal_seg_id(&self, actor_id: u32) -> u32 {
        self.seg_base + JOURNAL_ID_OFFSET + actor_id
    }

    /// The built-in task-queue segment id.
    #[inline]
    pub fn task_queue_seg_id(&self) -> u32 {
        self.seg_base + TASKQ_ID_OFFSET
    }

    /// The head-segment id for the artifact at registry index `i`.
    ///
    /// The head segment carries no chunks, so its id is unconstrained and stays
    /// in the `seg_base`-derived space. The **data** segment, by contrast, backs
    /// chunks referenced by manifest [`PackedRef`](shm_core::PackedRef)s. Since
    /// v0.3 (ADR-0003a) a `PackedRef` carries a full 32-bit `segment_id`, so the
    /// former 2^16 cap is lifted; the id is still allocated separately (see the
    /// coordinator's `alloc_artifact_data_id`) for shm-name-space hygiene.
    #[inline]
    pub fn artifact_head_seg_id(&self, i: u32) -> u32 {
        self.seg_base + ARTIFACT_ID_OFFSET + 2 * i
    }

    /// The keyed store's **catalog** segment id.
    #[inline]
    pub fn store_catalog_seg_id(&self) -> u32 {
        self.seg_base + STORE_ID_OFFSET
    }

    /// The keyed store's **head** (management) segment id.
    #[inline]
    pub fn store_head_seg_id(&self) -> u32 {
        self.seg_base + STORE_ID_OFFSET + 1
    }

    /// The keyed store's **data** (shared pool) segment id.
    #[inline]
    pub fn store_data_seg_id(&self) -> u32 {
        self.seg_base + STORE_ID_OFFSET + 2
    }
}
