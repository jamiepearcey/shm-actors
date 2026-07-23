//! A hand-written **MOCK** of ArrowRef's task-fabric contract.
//!
//! This is deliberately *not* the real ArrowRef / query-cache crate (ADR-0004
//! stage Q keeps shm-actors clean-room: no ArrowRef dependency, ever). It is a
//! minimal local stand-in that mirrors the shapes ArrowRef actually uses today,
//! so the spike maps a realistic API surface — not a strawman — onto shm-actors'
//! primitives.
//!
//! Fidelity anchors (as of the read on 2026-07-23), for traceability:
//!
//! - [`TaskRequest`] mirrors `query-cache/repo/src/model.rs::TaskMessage`
//!   (`task_id`, `input: InputRef`, `output: OutputPolicy`, `execution`,
//!   `deadline_ms`).
//! - [`RetainedInputRef`] mirrors `model.rs::InputRef::ArrowRef` /
//!   `DatasetQuery` — a *descriptor* pointing at a retained Arrow payload, never
//!   the payload itself.
//! - [`OutputPolicy`] mirrors `model.rs::OutputPolicy` (`Stream` vs a retained
//!   `Dataset { dataset, group, ack }`).
//! - [`AckPolicy`] mirrors `model.rs::AckPolicy { clear_on_ack }`.
//! - [`TaskResult`] / [`RetainedOutputRef`] mirror the journal's terminal
//!   `Completed { output_dataset, output_chunks, .. }`
//!   (`query-cache/repo/src/task_journal.rs`).
//!
//! Everything here is plain data. The spike ([`crate::run_spike`]) is what
//! carries these shapes across shm-actors' `TaskQueue` + `Artifact`.

/// ArrowRef's task input: a **descriptor** naming a retained Arrow payload.
///
/// Mirrors `InputRef::ArrowRef { dataset, chunk_id, offset, len, schema_id }`.
/// The payload lives once in the retained cache; the task carries only this ref.
#[derive(Clone, Debug)]
pub struct RetainedInputRef {
    /// Logical dataset/artifact name the payload was retained under.
    pub dataset: String,
    /// Interned Arrow schema id shared by producer and consumer.
    pub schema_id: u64,
}

/// ArrowRef's task output policy. Either the result streams back inline
/// (`Stream`) or it is retained as a versioned dataset the requester later
/// references and (optionally) clears on ack.
#[derive(Clone, Debug)]
pub enum OutputPolicy {
    /// Inline streamed result (not exercised by this spike).
    Stream,
    /// Retained dataset output — the descriptor-first path this spike proves.
    Dataset {
        /// The dataset/artifact name the output is retained under.
        dataset: String,
        /// Lifecycle group governing memory/TTL/eviction budgets.
        group: String,
        /// Ack policy (clear-on-ack).
        ack: AckPolicy,
    },
}

/// Mirrors `model.rs::AckPolicy`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AckPolicy {
    /// If set, acking the task clears (evicts) the retained output.
    pub clear_on_ack: bool,
}

/// ArrowRef's submitted task: a `task_id`, a retained **input ref**, an output
/// policy, and a deadline. Mirrors `model.rs::TaskMessage`.
#[derive(Clone, Debug)]
pub struct TaskRequest {
    /// Caller-visible correlation id.
    pub task_id: String,
    /// The descriptor-only input (payload retained elsewhere).
    pub input: RetainedInputRef,
    /// Where/how the output is retained.
    pub output: OutputPolicy,
    /// Absolute deadline in epoch-nanos (mirrors `deadline_ms`, finer-grained).
    pub deadline_nanos: u64,
}

/// ArrowRef's retained task output: a **reference** to a retained versioned
/// dataset, never the bytes. Mirrors the journal `Completed` terminal record's
/// `output_dataset` + `output_chunks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedOutputRef {
    /// The dataset/artifact the output was retained under.
    pub dataset: String,
    /// The retained version the worker produced.
    pub version: u64,
}

/// A worker's terminal result: the retained output ref plus row count (for the
/// spike's assertions). Mirrors the journal `Completed` record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskResult {
    /// The retained output reference.
    pub output: RetainedOutputRef,
    /// Rows in the produced output (spike observability).
    pub rows: usize,
}
