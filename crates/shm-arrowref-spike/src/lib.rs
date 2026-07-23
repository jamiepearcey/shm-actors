//! # v0.4 stage Q — ArrowRef task-fabric integration SPIKE
//!
//! **This is a SPIKE / example, not a shipped library and not a port.** It maps
//! ONE ArrowRef surface — the *descriptor-only task fabric* — onto shm-actors'
//! published primitives, end to end, to prove the mapping is real and to surface
//! concrete API gaps for a stable ArrowRef rewrite. The findings are written up
//! in `docs/decisions/ADR-0005-arrowref-task-fabric-spike.md`.
//!
//! **Clean-room (ADR-0004 stage Q).** Nothing here depends on the real ArrowRef
//! / query-cache crate. The ArrowRef task contract is a small hand-written mock
//! ([`arrowref_mock`]); everything else is shm-actors' published API.
//!
//! ## What it proves
//!
//! A requester retains an Arrow input **once** in shared memory and submits only
//! its **24-byte descriptor** to a [`TaskQueue`]. A worker (a separate thread,
//! standing in for a separate process — the handles are the same ones a
//! cross-process actor gets) claims it exactly-once, reads the input **zero-copy**
//! straight out of the mapped segment, computes a derived batch, retains the
//! output as a new **[`Artifact`] version**, and completes the task with a 24-byte
//! result descriptor naming that version. The requester waits, reads the retained
//! output **zero-copy**, and then "clears on ack" by releasing the retained input
//! (refcount → 0 → reclaimed).
//!
//! The queue never carries a payload: control messages are 24 bytes each; the
//! Arrow payload is written exactly once and thereafter only *referenced*. That
//! is ArrowRef's core invariant ("descriptors on the wire; payload on the data
//! plane") realised on shm-actors.
//!
//! ## The ArrowRef task-fabric contract this mirrors (file refs)
//!
//! - submit → `query-cache/repo/src/runtime.rs::TaskRuntime::submit`
//! - descriptor-only queue msg → `query-cache/repo/src/queue.rs` +
//!   `model.rs::TaskMessage` (`InputRef` attach, never the payload)
//! - output-as-retained-ref → `runtime.rs` (`output_dataset` / `output_chunks`)
//!   + `task_journal.rs::JournalRecord::Completed`
//! - ack / clear-on-ack → `runtime.rs::TaskRuntime::ack` +
//!   `model.rs::AckPolicy::clear_on_ack`
//! - crash/retry lease → `runtime.rs` lease reap (shm-actors: `TaskQueue::reap`)

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use shm_arrow::{
    read_batch, serialized_len, write_batch, PinGuard, PoolAllocator, SchemaRegistry,
};
use shm_artifact::{Artifact, Commit};
use shm_core::{ChunkDesc, Pool, PoolConfig, Segment};
use shm_task::{now_nanos, TaskQueue, TaskStatus};

pub mod arrowref_mock;

use arrowref_mock::{AckPolicy, OutputPolicy, RetainedInputRef, RetainedOutputRef, TaskRequest, TaskResult};

/// Boxed error alias — the shm-* crates use `thiserror`, so every error already
/// implements [`std::error::Error`].
pub type SpikeError = Box<dyn std::error::Error + Send + Sync>;

const WORKER_ID: u32 = 2;
const REQUESTER_ID: u32 = 1;
const OUT_NAME_ID: u32 = 0xA5A5;
const QUEUE_CAPACITY: u32 = 8;
const ROWS: usize = 4096;
const DATA_SEG_SIZE: usize = 1 << 21;
const HEAD_SEG_SIZE: usize = 1 << 16;
const QUEUE_SEG_SIZE: usize = 1 << 16;

/// Result descriptor `segment_id` sentinel: this "descriptor" does not name a
/// pool chunk — it *encodes a retained artifact version*. That reuse is itself a
/// gap (see ADR-0005 gap G1): shm-task's result payload is a chunk descriptor,
/// not a typed retained-version/dataset ref.
const VERSION_MARKER: u32 = 0xFFFF_FFFF;

/// Everything the spike measured, for the ADR write-up and the assertions.
#[derive(Clone, Debug)]
pub struct SpikeReport {
    /// Serialized size of the Arrow input payload (bytes retained once).
    pub input_payload_bytes: usize,
    /// Bytes of the *control message* that crossed the queue (a [`ChunkDesc`]).
    pub control_msg_bytes: usize,
    /// Full task-queue slot size (state machine + request + result descriptors).
    pub queue_slot_bytes: usize,
    /// How many times larger the retained payload is than the control message.
    pub payload_to_control_ratio: usize,
    /// The retained output ref the worker produced (dataset + version).
    pub output: RetainedOutputRef,
    /// Rows in the retained output.
    pub output_rows: usize,
    /// The input was read straight out of the mapped segment (no copy).
    pub input_read_zero_copy: bool,
    /// The output was read straight out of the mapped segment (no copy).
    pub output_read_zero_copy: bool,
    /// The retained input was reclaimed on "ack" (refcount → 0 → FREE).
    pub cleared_on_ack: bool,
    /// End-to-end submit→retained-output→read latency.
    pub round_trip: Duration,
}

/// Run the full end-to-end spike and return its measurements.
///
/// Panics are avoided; all failure paths return [`SpikeError`]. Any shared-memory
/// segment names are unlinked immediately after creation (every handle is shared
/// as an `Arc<Segment>`, never re-attached by name), so nothing leaks into the
/// shm namespace even on a crash mid-run.
pub fn run_spike() -> Result<SpikeReport, SpikeError> {
    let started = Instant::now();

    // A single shared schema registry stands in for the coordinator's schema
    // catalog (shm-runtime's `intern_schema`/`resolve_schema`): both actors agree
    // on schema ids through it. In-process we share one; cross-process this is a
    // coordinator round-trip.
    let registry = Arc::new(SchemaRegistry::new());
    let schema = input_schema();
    let schema_id = registry.intern(&schema);

    // --- Segments (unlinked immediately; shared purely by Arc). ---
    let payload_seg = fresh_segment(DATA_SEG_SIZE)?;
    let out_head_seg = fresh_segment(HEAD_SEG_SIZE)?;
    let out_data_seg = fresh_segment(DATA_SEG_SIZE)?;
    let queue_seg = fresh_segment(QUEUE_SEG_SIZE)?;

    let pool_config = PoolConfig::power_of_two(1024, 1 << 16, 8);
    Pool::create(&payload_seg, &pool_config)?;

    // The retained OUTPUT store: an Artifact (RCU/MVCC versioned object). This is
    // ArrowRef's `task_results` retained dataset (model.rs DEFAULT_TASK_RESULT_GROUP).
    let out_artifact = Artifact::create(
        OUT_NAME_ID,
        out_head_seg.clone(),
        out_data_seg.clone(),
        &pool_config,
    )?;

    // --- Requester: retain the input ONCE, get its descriptor. ---
    let input_batch = build_input_batch(&schema);
    let input_payload_bytes =
        serialized_len(&input_batch).ok_or("input batch is not serializable")?;
    let input_desc = retain_input_chunk(&payload_seg, &registry, &input_batch, REQUESTER_ID)?;

    // The rich ArrowRef `TaskRequest` (mock). Note: only the 24-byte `input_desc`
    // below actually crosses the shared queue — the descriptor. The rest of this
    // struct (task_id, output policy, deadline) would need a side-channel: gap G2.
    let _task_request = TaskRequest {
        task_id: "spike-task-1".to_string(),
        input: RetainedInputRef {
            dataset: "spike_input".to_string(),
            schema_id: schema_id as u64,
        },
        output: OutputPolicy::Dataset {
            dataset: "task_results".to_string(),
            group: "task_results".to_string(),
            ack: AckPolicy { clear_on_ack: true },
        },
        deadline_nanos: now_nanos().wrapping_add(5_000_000_000),
    };

    // --- Init the task queue and submit the descriptor. ---
    // SAFETY: `queue_seg` is a fresh, 8-aligned mapping of `payload_len` writable
    // bytes, kept mapped by the `Arc<Segment>` for every handle's lifetime, and
    // initialised exactly once here.
    let submit_queue = unsafe {
        TaskQueue::init(queue_seg.payload_ptr(), queue_seg.payload_len(), QUEUE_CAPACITY)?
    };
    let deadline = now_nanos().wrapping_add(5_000_000_000);
    let handle = submit_queue.submit(input_desc, deadline)?;

    // --- Worker thread (stands in for a separate worker process). ---
    let worker = {
        let payload_seg = payload_seg.clone();
        let out_head_seg = out_head_seg.clone();
        let out_data_seg = out_data_seg.clone();
        let queue_seg = queue_seg.clone();
        let registry = registry.clone();
        thread::spawn(move || -> Result<(), String> {
            worker_body(payload_seg, out_head_seg, out_data_seg, queue_seg, registry)
                .map(|_result| ())
                .map_err(|e| e.to_string())
        })
    };

    // --- Requester: wait for the terminal outcome (descriptor-only). ---
    let result_desc = wait_terminal(&submit_queue, handle)?;
    if result_desc.segment_id != VERSION_MARKER {
        return Err("worker result was not a retained-version ref".into());
    }
    let version = decode_version(&result_desc);

    worker
        .join()
        .map_err(|_| "worker thread panicked")?
        .map_err(|e| -> SpikeError { e.into() })?;

    // --- Requester: read the retained OUTPUT zero-copy from the artifact. ---
    let pin = out_artifact.pin()?;
    if pin.version() != version {
        return Err(format!(
            "retained output version mismatch: task said {version}, artifact at {}",
            pin.version()
        )
        .into());
    }
    let output_batch = pin.as_arrow(&registry)?;
    let output_rows = output_batch.num_rows();
    verify_doubled(&input_batch, &output_batch)?;
    let output_read_zero_copy = batch_points_into(&output_batch, &out_data_seg);

    // Independent check that the INPUT is also read zero-copy from the payload
    // segment (what the worker did): re-read it here and confirm the buffer aims
    // into the mapping, not a heap copy.
    let input_read_zero_copy = {
        let pool = Pool::attach(&payload_seg)?;
        let ctrl = pool.ctrl(&input_desc)?;
        let owner = Arc::new(PinGuard::new(payload_seg.clone()));
        let reread = read_batch(owner, ctrl, &input_desc, &registry)?;
        batch_points_into(&reread, &payload_seg)
    };

    let round_trip = started.elapsed();

    // --- "Ack" with clear-on-ack: release the retained input (refcount → 0). ---
    // ArrowRef's `AckPolicy::clear_on_ack` evicts the retained output on ack; the
    // shm-actors analogue of "clear a retained ref" is dropping the last
    // reference so the chunk is reclaimed. We clear the retained INPUT here (the
    // output is still `current` in the artifact and cannot be retired while
    // current — itself gap G4).
    let cleared_on_ack = clear_retained_chunk(&payload_seg, &input_desc)?;

    let output = RetainedOutputRef {
        dataset: "task_results".to_string(),
        version,
    };

    Ok(SpikeReport {
        input_payload_bytes,
        control_msg_bytes: std::mem::size_of::<ChunkDesc>(),
        queue_slot_bytes: std::mem::size_of::<shm_task::TaskSlot>(),
        payload_to_control_ratio: input_payload_bytes / std::mem::size_of::<ChunkDesc>().max(1),
        output,
        output_rows,
        input_read_zero_copy,
        output_read_zero_copy,
        cleared_on_ack,
        round_trip,
    })
}

/// The worker actor: claim exactly-once, read input zero-copy, compute, retain
/// the output as a new artifact version, complete with the version descriptor.
fn worker_body(
    payload_seg: Arc<Segment>,
    out_head_seg: Arc<Segment>,
    out_data_seg: Arc<Segment>,
    queue_seg: Arc<Segment>,
    registry: Arc<SchemaRegistry>,
) -> Result<TaskResult, SpikeError> {
    // SAFETY: `queue_seg` was initialised by `TaskQueue::init` in the requester
    // and stays mapped for this handle's lifetime via the shared `Arc`.
    let queue =
        unsafe { TaskQueue::attach(queue_seg.payload_ptr(), queue_seg.payload_len())? };
    let out_artifact = Artifact::attach(OUT_NAME_ID, out_head_seg, out_data_seg)?;
    let payload_pool = Pool::attach(&payload_seg)?;

    // Claim (with a fresh lease). No coordinator/doorbell here, so spin-claim with
    // a bounded deadline — the same `claim` the runtime's `claim_blocking` wraps.
    let claim_deadline = Instant::now() + Duration::from_secs(5);
    let claimed = loop {
        if let Some(task) = queue.claim_with_lease(WORKER_ID, now_nanos().wrapping_add(2_000_000_000)) {
            break task;
        }
        if Instant::now() >= claim_deadline {
            return Err("worker timed out waiting to claim a task".into());
        }
        thread::yield_now();
    };

    let input_desc = claimed.request();

    // Take a task-duration read LEASE on the retained input (ArrowRef: "chunk
    // leases/ref-counts so referenced chunks stay valid while tasks run") and read
    // it ZERO-COPY straight out of the mapped segment.
    let ctrl = payload_pool.ctrl(&input_desc)?;
    ctrl.borrow_shared()?; // +1 lease for the duration of the task
    let owner = Arc::new(PinGuard::new(payload_seg.clone()));
    let input_batch = read_batch(owner, ctrl, &input_desc, &registry)?;

    // Compute a derived batch (double column 0). This is the only place the
    // payload materialises into a new buffer — the compute output.
    let output_batch = double_first_column(&input_batch)?;
    let rows = output_batch.num_rows();

    // Retain the output as a new artifact VERSION (Append). The bytes are written
    // once into the artifact's data segment; nothing goes through the queue.
    let expect = out_artifact.current_version();
    let version =
        out_artifact.commit_optimistic(WORKER_ID, expect, Commit::Append, &output_batch, &registry)?;

    // Drop the input lease now the compute output is independent of it.
    ctrl.release_shared();

    // Complete the task: the terminal result is a 24-byte descriptor naming the
    // retained output version. (Encoding a version into a `ChunkDesc` is gap G1.)
    claimed.complete(encode_version(version))?;

    Ok(TaskResult {
        output: RetainedOutputRef {
            dataset: "task_results".to_string(),
            version,
        },
        rows,
    })
}

// ---- helpers ----

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn build_input_batch(schema: &SchemaRef) -> RecordBatch {
    let col = Int64Array::from_iter_values(0..ROWS as i64);
    RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]).expect("valid input batch")
}

/// Write `batch` into ONE pool chunk and retain it: loan → publish → +1 shared
/// ref → release owner, leaving it `PUBLISHED` with refcount 1 (survives until an
/// explicit clear). Mirrors `shm-artifact`'s `stage_chunk` retention shape.
fn retain_input_chunk(
    payload_seg: &Arc<Segment>,
    registry: &SchemaRegistry,
    batch: &RecordBatch,
    owner: u32,
) -> Result<ChunkDesc, SpikeError> {
    let pool = Pool::attach(payload_seg)?;
    let alloc = PoolAllocator::new(&pool, payload_seg);
    let desc = write_batch(&alloc, registry, batch)?; // FREE
    let ctrl = pool.ctrl(&desc)?;
    ctrl.try_loan(owner)?; // FREE -> LOANED
    ctrl.publish()?; // LOANED -> PUBLISHED (refcount 0)
    ctrl.borrow_shared()?; // refcount 0 -> 1 : the retained reference
    ctrl.owner_release(); // owner -> NONE; refcount 1 keeps it alive
    Ok(desc)
}

/// Release the retained reference taken by [`retain_input_chunk`]. Returns `true`
/// iff that was the last reference and the chunk was reclaimed (`FREE`) — the
/// shm-actors analogue of ArrowRef's clear-on-ack eviction.
fn clear_retained_chunk(payload_seg: &Arc<Segment>, desc: &ChunkDesc) -> Result<bool, SpikeError> {
    let pool = Pool::attach(payload_seg)?;
    let ctrl = pool.ctrl(desc)?;
    let reclaimed = ctrl.release_shared();
    Ok(reclaimed)
}

fn double_first_column(batch: &RecordBatch) -> Result<RecordBatch, SpikeError> {
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("input column 0 is not Int64")?;
    let doubled = Int64Array::from_iter_values(col.values().iter().map(|v| v * 2));
    Ok(RecordBatch::try_new(batch.schema(), vec![Arc::new(doubled)])?)
}

fn verify_doubled(input: &RecordBatch, output: &RecordBatch) -> Result<(), SpikeError> {
    let a = input
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("input not Int64")?;
    let b = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("output not Int64")?;
    if a.len() != b.len() {
        return Err("row count changed across the task".into());
    }
    for i in 0..a.len() {
        if b.value(i) != a.value(i) * 2 {
            return Err(format!("row {i}: expected {}, got {}", a.value(i) * 2, b.value(i)).into());
        }
    }
    Ok(())
}

/// Poll the queue until the task reaches a terminal state, returning the worker's
/// result descriptor on success. (No doorbell here, so this polls; the runtime's
/// `wait` parks on the done doorbell.)
fn wait_terminal(queue: &TaskQueue, handle: shm_task::TaskHandle) -> Result<ChunkDesc, SpikeError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match queue.poll(handle)? {
            TaskStatus::Done(desc) => return Ok(desc),
            TaskStatus::Failed => return Err("task failed".into()),
            TaskStatus::Cancelled => return Err("task cancelled".into()),
            TaskStatus::Queued | TaskStatus::Claimed => {}
        }
        if Instant::now() >= deadline {
            return Err("requester timed out waiting for the task".into());
        }
        thread::yield_now();
    }
}

/// Encode a retained artifact version into a result [`ChunkDesc`]. The `segment_id`
/// sentinel marks it as a version ref rather than a real chunk pointer; the u64
/// version is split across `offset` (low) and `len` (high). See ADR-0005 gap G1.
fn encode_version(version: u64) -> ChunkDesc {
    ChunkDesc {
        segment_id: VERSION_MARKER,
        generation: 0,
        offset: version as u32,
        len: (version >> 32) as u32,
        schema_id: 0,
        _pad: 0,
    }
}

fn decode_version(desc: &ChunkDesc) -> u64 {
    (desc.offset as u64) | ((desc.len as u64) << 32)
}

/// Whether `batch`'s first column's data buffer points **inside** `seg`'s
/// mapping — i.e. the batch is a zero-copy view over shared memory, not a heap
/// copy.
fn batch_points_into(batch: &RecordBatch, seg: &Segment) -> bool {
    let base = seg.base_ptr() as usize;
    let end = base + DATA_SEG_SIZE;
    let data = batch.column(0).to_data();
    let Some(buffer) = data.buffers().first() else {
        return false;
    };
    let ptr = buffer.as_ptr() as usize;
    ptr >= base && ptr < end
}

/// Create a fresh shared-memory segment and immediately unlink its name. Every
/// consumer shares it by `Arc<Segment>` (never re-attaches by name), so unlinking
/// now guarantees no shm-namespace residue even if the process later crashes.
fn fresh_segment(size: usize) -> Result<Arc<Segment>, SpikeError> {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    static BASE: OnceLock<u32> = OnceLock::new();
    let base = *BASE.get_or_init(|| (std::process::id() & 0x000F_FFFF) << 8);
    for _ in 0..64 {
        let id = base
            .wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed))
            & 0x7FFF_FFFF;
        // Best-effort clear of a stale name from a prior crashed run.
        let _ = Segment::unlink_by_id(id);
        match Segment::create(id, size) {
            Ok(seg) => {
                let _ = seg.unlink();
                return Ok(Arc::new(seg));
            }
            Err(_) => continue,
        }
    }
    Err("could not allocate a fresh shared-memory segment id".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spike_maps_task_fabric_onto_shm_actors() {
        let report = run_spike().expect("spike runs end to end");

        // Descriptor-only control: the queue carried a 24-byte descriptor, not the
        // Arrow payload.
        assert_eq!(report.control_msg_bytes, 24, "control message is a ChunkDesc");
        assert!(
            report.input_payload_bytes > 8 * 1024,
            "input payload is substantial ({} bytes)",
            report.input_payload_bytes
        );
        assert!(
            report.payload_to_control_ratio > 100,
            "payload dwarfs the control message (ratio {})",
            report.payload_to_control_ratio
        );

        // Retained output is a versioned ref, read zero-copy.
        assert_eq!(report.output.version, 1, "first retained output version");
        assert_eq!(report.output_rows, ROWS, "all rows produced");
        assert!(report.input_read_zero_copy, "input read straight from shm");
        assert!(report.output_read_zero_copy, "output read straight from shm");

        // Clear-on-ack reclaimed the retained input.
        assert!(report.cleared_on_ack, "retained input reclaimed on ack");
    }
}
