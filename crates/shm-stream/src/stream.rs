//! The [`StreamWriter`] transaction handle and its coordination modes.

use arrow_array::RecordBatch;
use shm_arrow::{write_batch_chunks, PoolAllocator, SchemaRegistry};
use shm_artifact::{Artifact, Commit, Committer};
use shm_core::{BorrowJournal, ChunkDesc, Pool, Segment};

use crate::error::{Error, Result};

/// How a [`StreamWriter`] coordinates its install against concurrent writers.
///
/// Mirrors `shm-artifact`'s two write disciplines exactly (see
/// [`Artifact::open_exclusive`] vs [`Artifact::commit_optimistic`]).
#[derive(Clone, Copy, Debug)]
pub enum Coordination {
    /// Take the artifact's **fenced** exclusive write lease at
    /// [`StreamWriter::open`]. A second exclusive `open` on the same artifact
    /// fails fast with [`shm_artifact::Error::WriteLocked`]; the lease is held for
    /// the whole transaction and released on commit/abort/drop. The lease is
    /// opened *journalled* (ADR-0003 item K), so a `kill -9`ed exclusive writer is
    /// force-released and fenced by the coordinator rather than wedging the
    /// artifact; a paused writer that was declared dead sees
    /// [`shm_artifact::Error::Fenced`] if it later tries to commit.
    Exclusive,
    /// Take no lease; race on the install CAS at [`commit`](StreamWriter::commit),
    /// expecting the artifact's current version to still be `expect_version`. A
    /// loser observes [`shm_artifact::Error::Conflict`] and its staged chunks are
    /// returned to the pool.
    Optimistic {
        /// The version this transaction expects to supersede (`n` → `n+1`); `0`
        /// ([`shm_artifact` `NO_VERSION`]) for the artifact's first version.
        expect_version: u64,
    },
}

/// One staged (accumulated-but-invisible) chunk: its descriptor plus the borrow
/// journal slot that guarantees a crashed writer's staging is reclaimed.
#[derive(Clone, Copy)]
struct Staged {
    /// The written, `LOANED` chunk (not in any installed manifest → invisible).
    desc: ChunkDesc,
    /// The [`BorrowJournal`] slot recording `desc` for crash reclamation.
    slot: usize,
}

/// The active coordination state a [`StreamWriter`] holds for its lifetime.
enum Coord<'a> {
    /// Holds the exclusive lease (a live [`Committer`]) until commit/abort/drop.
    Exclusive(Committer<'a>),
    /// Records the version the optimistic install will expect at commit time.
    Optimistic { expect: u64 },
}

/// A write transaction against an [`Artifact`]: the WRITE path for artifacts and
/// the 4th `shm-actors` primitive (design doc §5.3, ADR-0002 stage C).
///
/// A stream accumulates Arrow batches as **staged chunks** that are invisible to
/// every reader ([`append_batch`](StreamWriter::append_batch)), then installs
/// them **atomically** as the next artifact version
/// ([`commit`](StreamWriter::commit)). Until commit, a reader pinning the
/// artifact sees only the previously-installed version (or none); on commit they
/// become version `n+1` in one linearising CAS.
///
/// ```no_run
/// # use shm_stream::{StreamWriter, Coordination};
/// # use shm_artifact::{Artifact, Commit};
/// # fn go(artifact: &Artifact, data_seg: &shm_core::Segment,
/// #       journal: &shm_core::BorrowJournal, reg: &shm_arrow::SchemaRegistry,
/// #       a: &arrow_array::RecordBatch, b: &arrow_array::RecordBatch)
/// #       -> shm_stream::Result<()> {
/// let mut stream = StreamWriter::open(
///     artifact, data_seg, journal, reg, 7, Commit::Append, Coordination::Exclusive,
/// )?;
/// stream.append_batch(a)?;      // staged, INVISIBLE to readers
/// stream.append_batch(b)?;      // staged
/// let version = stream.commit()?; // atomic: staged chunks become v(n+1)
/// # let _ = version; Ok(())
/// # }
/// ```
///
/// # Isolation & crash safety
///
/// Each staged chunk is `LOANED` (owned by this writer) and recorded in a
/// [`BorrowJournal`], but never published and never named by an installed
/// manifest — so no reader can reach it. [`abort`](StreamWriter::abort) (or a
/// `Drop` without commit) frees every staged chunk with nothing ever made
/// visible. If the *writer process dies* mid-stage, the coordinator's lease
/// monitor replays this journal and frees the staged chunks (the exact same path
/// exercised by [`crate` tests]); the stream layer adds **no** extra crash
/// machinery. Because staging never touches `current`, no partial version is
/// ever observable. In [`Coordination::Exclusive`] mode the same journal replay
/// also **force-releases and fences** the dead writer's write lease (item K), so
/// a crashed exclusive writer never leaves the artifact permanently un-writable.
pub struct StreamWriter<'a> {
    artifact: &'a Artifact,
    /// The data segment the artifact's chunk pool lives in — the caller must
    /// pass the very segment the `artifact` was created/attached over.
    data_seg: &'a Segment,
    /// This writer's own handle over the artifact's pool (chunk state is all in
    /// shm, so a distinct handle over the same segment is sound and expected).
    pool: Pool<'a>,
    journal: &'a BorrowJournal<'a>,
    registry: &'a SchemaRegistry,
    coord: Coord<'a>,
    owner: u32,
    commit_kind: Commit,
    /// Every staged chunk of every staged batch, **flat** and in order. A
    /// multi-chunk batch (item F) contributes several consecutive entries here;
    /// each is journaled so a crash frees *all* of a batch's chunks.
    staged: Vec<Staged>,
    /// Batch boundaries over `staged`: `batch_spans[b]` consecutive `staged`
    /// chunks form the `b`-th appended batch. `sum(batch_spans) == staged.len()`.
    batch_spans: Vec<u32>,
    /// The interned schema id shared by every staged batch (set by the first
    /// [`append_batch`](StreamWriter::append_batch)).
    schema_id: Option<u32>,
    /// Set once [`commit`](StreamWriter::commit) or [`abort`](StreamWriter::abort)
    /// has run, so `Drop` neither double-frees nor re-commits.
    finished: bool,
}

impl<'a> StreamWriter<'a> {
    /// Open a transaction against `artifact`, allocating staged chunks from the
    /// pool in `data_seg` and recording them in `journal` for crash reclamation.
    ///
    /// - `data_seg` — the segment the `artifact`'s data pool was created/attached
    ///   over (staged chunks and, on commit, the installed version's chunks all
    ///   live here).
    /// - `journal` — this writer's [`BorrowJournal`]; staged chunks are recorded
    ///   so a crash mid-transaction is reclaimed by the coordinator.
    /// - `registry` — the schema registry (seeded identically to any reader's,
    ///   per the v0.1 in-process contract).
    /// - `owner` — this writer's non-zero actor id (the loan/lease owner).
    /// - `commit_kind` — [`Commit::Append`] (extend the table: `v(n+1)`'s
    ///   manifest lists the staged chunks and *links* to `v(n)`'s manifest, so
    ///   the table is the chain — O(new data) per commit, ADR-0013) or
    ///   [`Commit::Replace`] (supersede wholesale: `v(n+1)` = the staged chunks
    ///   only, a new chain root).
    ///   [`Commit::Patch`] is rejected with [`Error::Unsupported`] (deferred to
    ///   v0.3) so no chunk is ever staged for a commit that cannot install.
    /// - `coordination` — [`Coordination::Exclusive`] takes the write lease now
    ///   (a second exclusive open → [`shm_artifact::Error::WriteLocked`]);
    ///   [`Coordination::Optimistic`] races on the install CAS at commit.
    pub fn open(
        artifact: &'a Artifact,
        data_seg: &'a Segment,
        journal: &'a BorrowJournal<'a>,
        registry: &'a SchemaRegistry,
        owner: u32,
        commit_kind: Commit,
        coordination: Coordination,
    ) -> Result<StreamWriter<'a>> {
        if let Commit::Patch(_) = commit_kind {
            return Err(Error::Unsupported("Patch stream deferred to v0.3"));
        }
        let pool = Pool::attach(data_seg)?;
        let coord = match coordination {
            // Open the exclusive lease **journalled** (item K): the lease's
            // WriteLease entry lands in this writer's borrow journal, so a
            // `kill -9` mid-transaction is force-released (and fenced) by the
            // coordinator's lease monitor — not left permanently wedged.
            Coordination::Exclusive => {
                Coord::Exclusive(artifact.open_exclusive_journaled(owner, journal)?)
            }
            Coordination::Optimistic { expect_version } => Coord::Optimistic {
                expect: expect_version,
            },
        };
        Ok(StreamWriter {
            artifact,
            data_seg,
            pool,
            journal,
            registry,
            coord,
            owner,
            commit_kind,
            staged: Vec::new(),
            batch_spans: Vec::new(),
            schema_id: None,
            finished: false,
        })
    }

    /// Number of staged **chunks** (accumulated but not yet committed). A
    /// multi-chunk batch (item F) contributes several; for single-chunk batches
    /// this equals the number of appended batches.
    #[inline]
    pub fn staged_len(&self) -> usize {
        self.staged.len()
    }

    /// The exclusive/optimistic owner (actor id) of this transaction.
    #[inline]
    pub fn owner(&self) -> u32 {
        self.owner
    }

    /// Stage `batch` as the next chunk of the sequence — **invisible** to every
    /// reader until [`commit`](StreamWriter::commit).
    ///
    /// The batch is serialized once into a freshly loaned chunk (`FREE` →
    /// `LOANED`, owned by this writer), recorded in the borrow journal (so a
    /// crash frees it), and pushed onto the private staged list. It is **not**
    /// published and appears in no installed manifest, so no `pin` can reach it.
    ///
    /// Every batch in one transaction must share the same Arrow schema (they are
    /// concatenated on read); a mismatch returns [`Error::SchemaMismatch`] and
    /// leaves the previously staged batches untouched.
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // 1. Serialize the batch into one or more fresh chunks (item F: a large
        //    or nested batch may span several chunks). Each chunk is `FREE`.
        let descs = {
            let alloc = PoolAllocator::new(&self.pool, self.data_seg);
            write_batch_chunks(&alloc, self.registry, batch)?
        };
        let batch_schema_id = descs[0].schema_id;

        // Reject a schema mismatch before taking any loan, so a rejected append
        // costs nothing and leaves the pool exactly as it was.
        if let Some(expected) = self.schema_id {
            if expected != batch_schema_id {
                for d in &descs {
                    let _ = self.pool.free(d);
                }
                return Err(Error::SchemaMismatch {
                    expected,
                    appended: batch_schema_id,
                });
            }
        }

        // 2. Loan + journal EVERY chunk of this batch. On any mid-batch failure,
        //    unwind this batch's chunks (drop loans + release journal slots) and
        //    return every remaining (un-staged) chunk to the pool, so a rejected
        //    append leaks nothing and leaves prior batches untouched.
        let mut batch_staged: Vec<Staged> = Vec::with_capacity(descs.len());
        for (i, desc) in descs.iter().enumerate() {
            let staged = self
                .pool
                .ctrl(desc)
                .map_err(Error::from)
                .and_then(|c| c.try_loan(self.owner).map_err(Error::from))
                .and_then(|()| self.journal.record(*desc).map_err(Error::from));
            match staged {
                Ok(slot) => batch_staged.push(Staged { desc: *desc, slot }),
                Err(e) => {
                    // Undo the chunks already loaned+journaled in THIS batch:
                    // drop the loan, release the journal slot, and return the
                    // chunk to the pool.
                    for s in &batch_staged {
                        if !self.journal.release(s.slot).unwrap_or(false) {
                            continue; // replay already reclaimed it
                        }
                        if let Ok(c) = self.pool.ctrl(&s.desc) {
                            let _ = c.drop_loan();
                        }
                        let _ = self.pool.free(&s.desc);
                    }
                    // Return every remaining (un-staged) chunk of this batch,
                    // including the one whose loan/journal just failed (dropping
                    // its loan first if it is still LOANED).
                    for d in &descs[i..] {
                        if let Ok(c) = self.pool.ctrl(d) {
                            let _ = c.drop_loan();
                        }
                        let _ = self.pool.free(d);
                    }
                    return Err(e);
                }
            }
        }

        self.schema_id.get_or_insert(batch_schema_id);
        self.staged.extend(batch_staged);
        self.batch_spans.push(descs.len() as u32);
        Ok(())
    }

    /// Install every staged chunk **atomically** as the next artifact version,
    /// consuming the transaction; returns the new version number.
    ///
    /// For [`Commit::Append`] the new version extends the prior one: its
    /// manifest lists the staged chunks and carries one reference-counted link
    /// to the prior version's manifest (ADR-0013 chained manifests) — nothing
    /// prior is copied or re-listed, so the commit costs O(staged data)
    /// regardless of the table's history; a reader walks the chain
    /// ([`VersionPin::as_arrow_batches`](shm_artifact::VersionPin::as_arrow_batches)).
    /// For [`Commit::Replace`] the new version's manifest is the staged chunks
    /// alone (a new chain root). The install is a single linearising CAS in
    /// `shm-artifact`; readers only ever see version `n` then version `n+1`,
    /// never a partial batch.
    ///
    /// On success the staged chunks are handed to the new version (reclaimed
    /// thereafter by the artifact's pin/refcount rules) and this writer's journal
    /// slots are released. On failure (e.g. [`shm_artifact::Error::Conflict`] in
    /// optimistic mode) the artifact returns every staged chunk to the pool and
    /// this writer releases its journal slots; the error is propagated. Either
    /// way the transaction is consumed.
    pub fn commit(mut self) -> Result<u64> {
        // An empty stream carries no schema; a zero-chunk version is degenerate
        // but well-formed, so fall back to the untyped id.
        let schema_id = self.schema_id.unwrap_or(shm_arrow::RAW_SCHEMA_ID);
        let descs: Vec<ChunkDesc> = self.staged.iter().map(|s| s.desc).collect();
        let spans = self.batch_spans.clone();
        let kind = self.commit_kind.clone();
        let owner = self.owner;
        let artifact = self.artifact;

        let result = match &mut self.coord {
            Coord::Exclusive(committer) => committer.commit_staged(kind, &descs, &spans, schema_id),
            Coord::Optimistic { expect } => artifact.commit_staged_optimistic_journaled(
                owner,
                *expect,
                kind,
                &descs,
                &spans,
                schema_id,
                self.journal,
            ),
        };

        // Whether the install succeeded (chunks now owned by the version) or
        // failed (chunks freed by the artifact's rollback), the staged chunks are
        // no longer this writer's to reclaim — release the journal slots. Do NOT
        // free the chunks here: the artifact already took over their fate.
        for s in &self.staged {
            let _ = self.journal.release(s.slot);
        }
        self.finished = true;
        result.map_err(Error::from)
    }

    /// Abort the transaction, consuming it: free every staged chunk (`LOANED` →
    /// `FREE`, back to the pool) and release its journal slot. Nothing was ever
    /// visible to any reader, and `current` is untouched — the ISOLATION half of
    /// transactional semantics. Equivalent to dropping without committing.
    pub fn abort(mut self) {
        self.free_staged();
        self.finished = true;
        // Drop of `self` (and its `Coord::Exclusive` committer) releases the
        // exclusive lease, if any.
    }

    /// Free every staged chunk and release its journal slot. Idempotent and
    /// panic-safe: all fallible operations are ignored, and the staged list is
    /// cleared so a second call (or `Drop` after `abort`) does nothing.
    fn free_staged(&mut self) {
        for s in &self.staged {
            // Election first (ADR-0014 §4): if the coordinator's replay already
            // cleared this slot it also dropped the loan and freed the chunk —
            // a second free would corrupt the pool's free list.
            if !self.journal.release(s.slot).unwrap_or(false) {
                continue;
            }
            // LOANED -> FREE (bumps generation), then re-link into the free list.
            if let Ok(ctrl) = self.pool.ctrl(&s.desc) {
                let _ = ctrl.drop_loan();
            }
            let _ = self.pool.free(&s.desc);
        }
        self.staged.clear();
    }
}

impl Drop for StreamWriter<'_> {
    fn drop(&mut self) {
        // A writer dropped without commit/abort aborts: free the staged chunks
        // so nothing leaks and nothing partial is ever visible.
        if !self.finished {
            self.free_staged();
            self.finished = true;
        }
    }
}
