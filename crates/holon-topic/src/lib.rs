//! `holon-topic` — the retained topic log (ADR-0017 §1).
//!
//! A topic is **one** keyed-store entry, `topic/<name>`, whose Arrow schema is
//! a single-row message. Publish is a windowed append (ADR-0016), so the
//! entry's version *is* the message sequence number: monotonic, gap-free,
//! assigned under the entry's exclusive write lease. The window (`K` =
//! [`TopicPolicy::max_messages`], frozen at creation) is both the replay
//! window and the retention policy.
//!
//! Reading is [`TopicLog::read_since`]: a journaled pin plus zero-copy views of
//! every message after a cursor. A reader that fell out of the window is told
//! (`truncated`) — never silently skipped (invariant I6).
//!
//! Notification is descriptor-only: each publish also drops a
//! [`VersionEvent`] `{name_id = key_id, version = seq}` on the
//! [`TOPIC_EVENTS`] ring. Nothing is allocated or pinned for it; a lapped ring
//! loses a wake-up, which the cursor recovers from the log.

#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Array, BinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shm_arrow::SchemaRegistry;
use shm_artifact::{CommitKind, VersionEvent, VersionPin, WindowPolicy};
use shm_runtime::{Node, TopicPublisher};
use shm_store::{Entry, KeyedStore, RefKind};

/// The notification ring every topic publish drops a [`VersionEvent`] on.
pub const TOPIC_EVENTS: &str = "__topic_events";

/// Key prefix of topic entries in the keyed store.
pub const KEY_PREFIX: &str = "topic/";

/// Longest topic name accepted (fits the store key and the TCP control body).
pub const MAX_TOPIC_NAME: usize = 32;

/// Longest event name accepted.
pub const MAX_EVENT_NAME: usize = 16;

/// Retries on a lost write-lease race before giving up.
const PUBLISH_RETRIES: u32 = 64;

/// Errors of the topic log.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Topic or event name is empty, too long, or not ASCII-safe.
    #[error("bad name: {0}")]
    BadName(&'static str),
    /// The store rejected the operation.
    #[error("store: {0}")]
    Store(#[from] shm_store::Error),
    /// The runtime rejected the operation.
    #[error("runtime: {0}")]
    Runtime(#[from] shm_runtime::Error),
    /// The message batch did not have the topic schema.
    #[error("message batch has an unexpected shape")]
    BadMessage,
    /// The write lease stayed contended for every retry.
    #[error("publish lost the write lease {0} times")]
    Contended(u32),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Per-topic retention: the replay window in messages, frozen at creation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicPolicy {
    /// Messages retained after a window re-root (and the maximum extra depth
    /// before the next one), see [`WindowPolicy::new`].
    pub max_messages: u32,
}

impl Default for TopicPolicy {
    fn default() -> TopicPolicy {
        TopicPolicy { max_messages: 1024 }
    }
}

/// Validate a topic name: `1..=MAX_TOPIC_NAME` bytes of `[A-Za-z0-9._-]`.
pub fn check_topic_name(name: &str) -> Result<()> {
    check_name(name, MAX_TOPIC_NAME)
}

/// Validate an event name: `0..=MAX_EVENT_NAME` bytes of `[A-Za-z0-9._-]`.
pub fn check_event_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Ok(());
    }
    check_name(name, MAX_EVENT_NAME)
}

fn check_name(name: &str, max: usize) -> Result<()> {
    if name.is_empty() {
        return Err(Error::BadName("empty"));
    }
    if name.len() > max {
        return Err(Error::BadName("too long"));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        return Err(Error::BadName("only [A-Za-z0-9._-] allowed"));
    }
    Ok(())
}

/// The Arrow schema of one message (one row per commit).
pub fn message_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("ts_ms", DataType::UInt64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("content_type", DataType::Utf8, false),
        Field::new("data", DataType::Binary, false),
    ]))
}

/// Store key of a topic.
pub fn topic_key(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(KEY_PREFIX.len() + topic.len());
    k.extend_from_slice(KEY_PREFIX.as_bytes());
    k.extend_from_slice(topic.as_bytes());
    k
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One message, borrowed zero-copy from a pinned batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageView<'a> {
    /// Sequence number (== the entry version that committed it).
    pub seq: u64,
    /// Publisher wall clock, ms since the Unix epoch.
    pub ts_ms: u64,
    /// Event name (may be empty).
    pub event: &'a str,
    /// MIME type the publisher declared (may be empty).
    pub content_type: &'a str,
    /// Payload bytes.
    pub data: &'a [u8],
}

/// The result of [`TopicLog::read_since`]: the pin holding the delta's
/// buffers alive, plus what the reader needs to know.
pub struct TopicRead {
    _pin: Option<VersionPin>,
    batches: Vec<RecordBatch>,
    /// The version (== seq) the delta starts *after*. Equal to the cursor
    /// passed in when not truncated; otherwise the oldest retained seq − 1.
    pub from_seq: u64,
    /// The newest seq in the log at read time (the next cursor).
    pub head: u64,
    /// `true` when the cursor fell out of the window: messages in
    /// `(cursor, from_seq]` are gone. Report it; never hide it.
    pub truncated: bool,
}

impl TopicRead {
    /// Number of messages in this delta.
    #[inline]
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// `true` when nothing is newer than the cursor.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Iterate the messages oldest-first.
    pub fn messages(&self) -> impl Iterator<Item = Result<MessageView<'_>>> + '_ {
        self.batches.iter().map(decode_message)
    }
}

/// Decode a one-row message batch into a borrowed view.
pub fn decode_message(batch: &RecordBatch) -> Result<MessageView<'_>> {
    if batch.num_columns() != 5 || batch.num_rows() != 1 {
        return Err(Error::BadMessage);
    }
    let seq = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(Error::BadMessage)?
        .value(0);
    let ts_ms = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(Error::BadMessage)?
        .value(0);
    let event = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(Error::BadMessage)?
        .value(0);
    let content_type = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(Error::BadMessage)?
        .value(0);
    let data = batch
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(Error::BadMessage)?
        .value(0);
    Ok(MessageView {
        seq,
        ts_ms,
        event,
        content_type,
        data,
    })
}

/// Build the one-row batch for a message.
pub fn encode_message(
    schema: &SchemaRef,
    seq: u64,
    ts_ms: u64,
    event: &str,
    content_type: &str,
    data: &[u8],
) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![seq])),
            Arc::new(UInt64Array::from(vec![ts_ms])),
            Arc::new(StringArray::from(vec![event])),
            Arc::new(StringArray::from(vec![content_type])),
            Arc::new(BinaryArray::from(vec![data])),
        ],
    )
    .expect("five same-length columns matching the topic schema")
}

/// Everything a topic client needs that does **not** borrow the node: the
/// notification publisher and the interned schema. Build it before
/// `Node::store()`, then pair it with the store in a [`TopicLog`].
pub struct TopicSide {
    events: TopicPublisher,
    schema: SchemaRef,
}

impl TopicSide {
    /// Open the notification ring and intern the message schema.
    pub fn open(node: &mut Node) -> Result<TopicSide> {
        let schema = message_schema();
        node.intern_schema(&schema)?;
        let events = node.topic_publisher(TOPIC_EVENTS)?;
        Ok(TopicSide { events, schema })
    }

    /// The notification publisher.
    #[inline]
    pub fn events(&self) -> &TopicPublisher {
        &self.events
    }
}

/// The topic log over one keyed store.
///
/// Entries are cached per topic name (an `Entry` is a cheap artifact handle;
/// caching skips the catalog probe and the key-intern round trip).
pub struct TopicLog<'a> {
    store: &'a KeyedStore<'a>,
    side: &'a TopicSide,
    registry: Arc<SchemaRegistry>,
    entries: Mutex<HashMap<String, (Arc<Entry>, u32)>>,
}

impl<'a> TopicLog<'a> {
    /// Bind a log to a store and its node-independent side.
    pub fn new(
        store: &'a KeyedStore<'a>,
        side: &'a TopicSide,
        registry: Arc<SchemaRegistry>,
    ) -> TopicLog<'a> {
        registry.intern(&side.schema);
        TopicLog {
            store,
            side,
            registry,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// The store this log reads and writes.
    #[inline]
    pub fn store(&self) -> &'a KeyedStore<'a> {
        self.store
    }

    fn entry(&self, topic: &str) -> Result<(Arc<Entry>, u32)> {
        check_topic_name(topic)?;
        let mut cache = self.entries.lock().expect("entry cache poisoned");
        if let Some((e, kid)) = cache.get(topic) {
            return Ok((e.clone(), *kid));
        }
        // `create` is get-or-create and never appends a catalog slot for an
        // existing key: a topic costs exactly one slot, once.
        let entry = Arc::new(self.store.create(
            &topic_key(topic),
            RefKind::Dataset,
            &self.side.schema,
        )?);
        let kid = entry.key_id();
        cache.insert(topic.to_string(), (entry.clone(), kid));
        Ok((entry, kid))
    }

    /// Get-or-create `topic` and return its key id (the `name_id` carried by
    /// its notification events).
    pub fn key_id(&self, topic: &str) -> Result<u32> {
        Ok(self.entry(topic)?.1)
    }

    /// The newest seq of `topic` (`0` = nothing published yet). Creates the
    /// topic if needed.
    pub fn head(&self, topic: &str) -> Result<u64> {
        Ok(self.entry(topic)?.0.current_version())
    }

    /// Publish one message; returns its seq.
    ///
    /// Concurrent publishers serialize on the entry's write lease; a lost
    /// race is retried, bounded. The seq is assigned by the commit, so it is
    /// exact even under contention.
    pub fn publish(
        &self,
        topic: &str,
        policy: TopicPolicy,
        event: &str,
        content_type: &str,
        data: &[u8],
    ) -> Result<u64> {
        check_event_name(event)?;
        let (entry, kid) = self.entry(topic)?;
        let window = WindowPolicy::new(policy.max_messages.max(1));
        let ts_ms = now_ms();
        let mut tries = 0;
        loop {
            // The seq is the version the commit *will* install; under the
            // write lease that is current + 1. A lost lease race re-reads it.
            let seq = entry.current_version() + 1;
            let batch = encode_message(&self.side.schema, seq, ts_ms, event, content_type, data);
            match entry.append_windowed(&window, &batch) {
                Ok(v) => {
                    debug_assert_eq!(v, seq);
                    let _ = self.side.events.publish_event(VersionEvent::new(
                        kid,
                        v,
                        CommitKind::Append,
                    ));
                    return Ok(v);
                }
                Err(shm_store::Error::Artifact(
                    shm_artifact::Error::WriteLocked | shm_artifact::Error::Conflict { .. },
                )) if tries < PUBLISH_RETRIES => {
                    tries += 1;
                    std::thread::yield_now();
                }
                Err(shm_store::Error::Artifact(
                    shm_artifact::Error::WriteLocked | shm_artifact::Error::Conflict { .. },
                )) => return Err(Error::Contended(tries)),
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Read every message with `seq > cursor`. `cursor = 0` reads the whole
    /// retained window.
    pub fn read_since(&self, topic: &str, cursor: u64) -> Result<TopicRead> {
        let (entry, _) = self.entry(topic)?;
        if entry.current_version() == 0 {
            return Ok(TopicRead {
                _pin: None,
                batches: Vec::new(),
                from_seq: 0,
                head: 0,
                truncated: false,
            });
        }
        let (pin, delta) = entry.read_since(cursor)?;
        let head = pin.version();
        // A truncated delta is the whole retained table from the window root
        // (ADR-0016 §4); its `from_version` names the root, not the seq before
        // the oldest retained message. Every message carries its seq, so the
        // resume point is read off the table itself.
        // The substrate's flag is not the only signal: a `since` at or before
        // a Replace root reads as "the whole table" without `truncated`, and
        // `since = 0` is "from the beginning". The seq column is authoritative:
        // the oldest message present names the resume point, and anything
        // between the cursor and it is gone.
        let first = delta
            .batches
            .first()
            .map(|b| decode_message(b).map(|m| m.seq))
            .transpose()?;
        let (from_seq, truncated) = match first {
            Some(first) if first > cursor + 1 => (first - 1, true),
            Some(first) if first <= cursor => (cursor, false),
            _ => (cursor, false),
        };
        Ok(TopicRead {
            _pin: Some(pin),
            batches: delta.batches,
            from_seq,
            head,
            truncated,
        })
    }

    /// Drop `topic` and release its retained window.
    ///
    /// **The catalog slot is tombstoned permanently** (`shm-store` catalog
    /// slots are append-only, ADR-0007): a node may create at most
    /// `store_capacity` topics over its whole lifetime, deletions included.
    /// Deleting is therefore an operator action, not a per-request one.
    /// Existing subscribers see the topic as empty and resume at seq 0.
    pub fn delete(&self, topic: &str) -> Result<()> {
        check_topic_name(topic)?;
        self.entries
            .lock()
            .expect("entry cache poisoned")
            .remove(topic);
        match self.store.evict(&topic_key(topic)) {
            Ok(()) | Err(shm_store::Error::NotFound) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// The schema registry (readers of raw batches need it).
    #[inline]
    pub fn registry(&self) -> &Arc<SchemaRegistry> {
        &self.registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shm_runtime::{Coordinator, RuntimeConfig};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    static SEG: AtomicU32 = AtomicU32::new(0);

    fn seg_base() -> u32 {
        let pid = std::process::id();
        let n = SEG.fetch_add(1, Ordering::Relaxed);
        1_000_000 + ((pid % 1000) * 100) + n * 20_000
    }

    fn config() -> RuntimeConfig {
        let mut c = RuntimeConfig::with_seg_base(seg_base());
        c.store_pool = shm_core::PoolConfig::power_of_two(256, 8192, 256);
        c.store_data_size = 4 << 20;
        c
    }

    fn connect(uds: &std::path::Path, name: &str) -> Node {
        let mut n = Node::connect(uds, name, Arc::new(SchemaRegistry::new())).expect("connect");
        n.start_heartbeat(Duration::from_millis(150));
        n
    }

    #[test]
    fn names_are_checked() {
        assert!(check_topic_name("ticks.eur-usd_1").is_ok());
        assert!(check_topic_name("").is_err());
        assert!(check_topic_name("a b").is_err());
        assert!(check_topic_name(&"x".repeat(MAX_TOPIC_NAME + 1)).is_err());
        assert!(check_event_name("").is_ok());
        assert!(check_event_name("price").is_ok());
        assert!(check_event_name(&"e".repeat(MAX_EVENT_NAME + 1)).is_err());
    }

    #[test]
    fn message_round_trip() {
        let s = message_schema();
        let b = encode_message(&s, 7, 9, "ev", "text/plain", b"hello");
        let m = decode_message(&b).unwrap();
        assert_eq!(
            m,
            MessageView {
                seq: 7,
                ts_ms: 9,
                event: "ev",
                content_type: "text/plain",
                data: b"hello"
            }
        );
    }

    #[test]
    fn publish_read_since_window_and_events() {
        let dir = tempfile::tempdir().unwrap();
        let uds = dir.path().join("coord.sock");
        let mut coord = Coordinator::bind(&uds, config()).unwrap();
        coord.start().unwrap();
        let baseline = coord.store_data_free_total().unwrap();

        // A watcher on the notification ring, in a second node.
        let mut watcher_node = connect(&uds, "watcher");
        let mut sub = watcher_node.subscribe(TOPIC_EVENTS).unwrap();

        let mut node = connect(&uds, "pub");
        let side = TopicSide::open(&mut node).unwrap();
        let registry = node.registry().clone();
        let store = node.store().unwrap();
        let log = TopicLog::new(&store, &side, registry);
        let policy = TopicPolicy { max_messages: 4 };

        assert_eq!(log.head("t").unwrap(), 0);
        let empty = log.read_since("t", 0).unwrap();
        assert!(empty.is_empty() && empty.head == 0 && !empty.truncated);

        for i in 1..=3u64 {
            let seq = log
                .publish("t", policy, "ev", "text/plain", format!("m{i}").as_bytes())
                .unwrap();
            assert_eq!(seq, i);
        }
        let kid = log.key_id("t").unwrap();
        // Three notification events, in order, carrying key id + seq.
        for i in 1..=3u64 {
            let ev = loop {
                match sub.recv() {
                    shm_ring::Msg::Sample(d) => {
                        if let Some(ev) = VersionEvent::from_desc(&d) {
                            break ev;
                        }
                    }
                    shm_ring::Msg::Lagged(_) => {}
                }
            };
            assert_eq!((ev.name_id, ev.version), (kid, i));
        }

        // Full read from 0.
        let r = log.read_since("t", 0).unwrap();
        assert_eq!((r.len(), r.from_seq, r.head, r.truncated), (3, 0, 3, false));
        let seqs: Vec<u64> = r.messages().map(|m| m.unwrap().seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(r.messages().next().unwrap().unwrap().data, b"m1");

        // Delta from a cursor.
        let r = log.read_since("t", 2).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r.messages().next().unwrap().unwrap().seq, 3);
        let r = log.read_since("t", 3).unwrap();
        assert!(r.is_empty() && r.head == 3);

        // Push far past the window: retention is the window re-root.
        for i in 4..=40u64 {
            assert_eq!(
                log.publish("t", policy, "", "", format!("m{i}").as_bytes())
                    .unwrap(),
                i
            );
        }
        assert_eq!(log.head("t").unwrap(), 40);
        // A reader that kept up sees an exact delta.
        let r = log.read_since("t", 39).unwrap();
        assert_eq!((r.len(), r.truncated), (1, false));
        assert_eq!(r.messages().next().unwrap().unwrap().seq, 40);
        // A reader that fell out of the window is told, and resumes from
        // the window base with contiguous seqs up to head.
        let r = log.read_since("t", 1).unwrap();
        assert!(r.truncated, "cursor 1 is far outside a 4-message window");
        assert!(r.from_seq > 1 && r.from_seq < 40);
        let seqs: Vec<u64> = r.messages().map(|m| m.unwrap().seq).collect();
        let expect: Vec<u64> = (r.from_seq + 1..=40).collect();
        assert_eq!(seqs, expect, "delta is contiguous from the window base");
        assert!(seqs.len() <= 2 * policy.max_messages as usize);

        // Second topic is independent.
        assert_eq!(log.publish("u", policy, "", "", b"x").unwrap(), 1);
        assert_eq!(log.head("t").unwrap(), 40);

        drop(log);
        drop(store);
        drop(node);
        drop(sub);
        drop(watcher_node);
        // Pins are journaled: after the clients are gone and the topics'
        // retained windows are still live, the pool holds only the windows.
        let after = coord.store_data_free_total().unwrap();
        assert!(after < baseline, "retained windows hold chunks");
    }
}
