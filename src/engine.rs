use crate::locks::{Mutex, RwLock};
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::VecDeque;
#[cfg(target_arch = "wasm32")]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::mpsc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

#[cfg(not(target_arch = "wasm32"))]
use flate2::Compression;
#[cfg(not(target_arch = "wasm32"))]
use flate2::read::GzDecoder;
#[cfg(not(target_arch = "wasm32"))]
use flate2::write::GzEncoder;
use serde_json::{Value, json};

#[cfg(not(target_arch = "wasm32"))]
use crate::blob::BlobStore;
use crate::btree_collection::BTreeCollection;
use crate::change_stream::{
    ChangeEvent, ChangeStreamBroker, OperationType, ResumeError, SubscriberId, WatchFilter,
    WatchHandle,
};
use crate::collection::{CompactStats, IndexInfo, resolve_field_in_value};
use crate::crypto::EncryptionKey;
use crate::document::DocumentId;
use crate::error::{Error, Result};
#[cfg(not(target_arch = "wasm32"))]
use crate::fts::{self, FtsIndex};
use crate::links::{LinkConfig, LinksTable};
use crate::pipeline::Pipeline;
use crate::query::FindOptions;
use crate::transaction::{ReadRecord, Transaction, TransactionId, WriteOp};
#[cfg(not(target_arch = "wasm32"))]
use crate::tx_log::TxCommitLog;
use crate::value::IndexValue;

/// Callback type for forwarding engine log messages to an external sink.
pub type LogCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Helper: compute index metadata from a `BTreeCollection`.
/// Returns (unique_fields, need_values, has_text_index).
fn index_metadata(col: &BTreeCollection) -> (Vec<String>, bool, bool) {
    let fi = col.field_indexes();
    let ci = col.composite_indexes();
    let vi = col.vector_indexes();
    let unique_fields: Vec<String> = fi
        .values()
        .filter(|idx| idx.unique)
        .map(|idx| idx.field.clone())
        .collect();
    let need = !fi.is_empty() || !ci.is_empty() || col.has_text_index() || !vi.is_empty();
    (unique_fields, need, col.has_text_index())
}

/// Information about a completed backup operation.
#[derive(Debug)]
pub struct BackupInfo {
    pub path: String,
    pub size_bytes: u64,
    pub collections: usize,
}

/// Information about a completed restore operation.
#[derive(Debug)]
pub struct RestoreInfo {
    pub path: String,
    pub collections: usize,
}

/// Information about a completed point-in-time restore (`restore_to_point`).
#[derive(Debug)]
pub struct PitrRestoreInfo {
    pub path: String,
    pub collections: usize,
    /// The GSN the database was restored to (inclusive).
    pub target_gsn: u64,
    /// Archived records applied on top of the base backup.
    pub records_applied: u64,
}

#[cfg(not(target_arch = "wasm32"))]
enum FtsJob {
    Index {
        data: Vec<u8>,
        content_type: String,
        bucket: String,
        key: String,
    },
    Remove {
        bucket: String,
        key: String,
    },
}

/// Round-robin dispatcher that fans FTS jobs out to N worker threads.
/// Each worker has its own bounded channel; when one is full the next
/// send tries the next worker. This parallelizes CPU-bound extract_text
/// across cores.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct FtsDispatcher {
    senders: Vec<mpsc::SyncSender<FtsJob>>,
    counter: AtomicUsize,
    runtime: Arc<FtsRuntime>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FtsDispatcher {
    fn new(senders: Vec<mpsc::SyncSender<FtsJob>>, runtime: Arc<FtsRuntime>) -> Self {
        Self {
            senders,
            counter: AtomicUsize::new(0),
            runtime,
        }
    }

    fn send(&self, job: FtsJob) -> std::result::Result<(), mpsc::SendError<FtsJob>> {
        let n = self.senders.len();
        // Bump the queue depth before the actual handoff. Workers
        // decrement on `recv` so the gauge reflects "jobs accepted but
        // not yet picked up by a worker".
        self.runtime.queue_depth.fetch_add(1, Ordering::Relaxed);
        // Try the next worker round-robin. If its channel is full,
        // fall back to a blocking send to even out load.
        let start = self.counter.fetch_add(1, Ordering::Relaxed) % n;
        let mut current_job = job;
        for i in 0..n {
            let idx = (start + i) % n;
            match self.senders[idx].try_send(current_job) {
                Ok(()) => return Ok(()),
                Err(mpsc::TrySendError::Full(j)) => current_job = j,
                Err(mpsc::TrySendError::Disconnected(j)) => current_job = j,
            }
        }
        // All workers backed up — block on the round-robin pick.
        let res = self.senders[start].send(current_job);
        if res.is_err() {
            // No worker will pick this up — undo the optimistic
            // queue_depth bump so the gauge stays honest.
            self.runtime.queue_depth.fetch_sub(1, Ordering::Relaxed);
        }
        res
    }
}

/// Per-worker observability state for the FTS pipeline. Workers update
/// their own slot before/after each job; readers (e.g. the `fts_status`
/// wire command) take a brief snapshot. Designed so a subscriber-less
/// hot path stays cheap: queue_depth is a single relaxed atomic and the
/// per-worker mutex is only held by that worker plus the snapshot
/// reader.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct FtsRuntime {
    queue_depth: AtomicUsize,
    in_flight: Vec<Mutex<Option<InFlightJob>>>,
    recent: Mutex<VecDeque<RecentJob>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct InFlightJob {
    bucket: String,
    key: String,
    started_at: Instant,
    phase: FtsPhase,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
enum FtsPhase {
    Extracting,
    Indexing,
}

#[cfg(not(target_arch = "wasm32"))]
struct RecentJob {
    bucket: String,
    key: String,
    result: JobOutcome,
    total_ms: u64,
    finished_at: Instant,
}

#[cfg(not(target_arch = "wasm32"))]
enum JobOutcome {
    Indexed,
    Skipped(&'static str),
    Failed(String),
}

#[cfg(not(target_arch = "wasm32"))]
const FTS_RECENT_RING_CAP: usize = 50;

#[cfg(not(target_arch = "wasm32"))]
impl FtsRuntime {
    fn new(n_workers: usize) -> Self {
        let mut slots = Vec::with_capacity(n_workers);
        for _ in 0..n_workers {
            slots.push(Mutex::new(None));
        }
        Self {
            queue_depth: AtomicUsize::new(0),
            in_flight: slots,
            recent: Mutex::new(VecDeque::with_capacity(FTS_RECENT_RING_CAP)),
        }
    }

    fn start(&self, worker_id: usize, bucket: &str, key: &str) {
        // Worker has just received a job — it's no longer queued.
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
        let mut slot = self.in_flight[worker_id].lock();
        *slot = Some(InFlightJob {
            bucket: bucket.to_string(),
            key: key.to_string(),
            started_at: Instant::now(),
            phase: FtsPhase::Extracting,
        });
    }

    fn advance_to_indexing(&self, worker_id: usize) {
        let mut slot = self.in_flight[worker_id].lock();
        if let Some(job) = slot.as_mut() {
            job.phase = FtsPhase::Indexing;
        }
    }

    fn finish(&self, worker_id: usize, outcome: JobOutcome) {
        let mut slot = self.in_flight[worker_id].lock();
        if let Some(job) = slot.take() {
            let total_ms = job.started_at.elapsed().as_millis() as u64;
            let mut ring = self.recent.lock();
            if ring.len() >= FTS_RECENT_RING_CAP {
                ring.pop_front();
            }
            ring.push_back(RecentJob {
                bucket: job.bucket,
                key: job.key,
                result: outcome,
                total_ms,
                finished_at: Instant::now(),
            });
        }
    }

    fn snapshot(&self) -> Value {
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let workers: Vec<Value> = self
            .in_flight
            .iter()
            .enumerate()
            .map(|(id, slot)| {
                let g = slot.lock();
                let current = g.as_ref().map(|j| {
                    let phase = match j.phase {
                        FtsPhase::Extracting => "extracting",
                        FtsPhase::Indexing => "indexing",
                    };
                    json!({
                        "bucket": j.bucket,
                        "key": j.key,
                        "phase": phase,
                        "ms_elapsed": j.started_at.elapsed().as_millis() as u64,
                    })
                });
                json!({ "id": id, "current": current })
            })
            .collect();
        let now = Instant::now();
        let recent: Vec<Value> = self
            .recent
            .lock()
            .iter()
            .map(|r| {
                let result = match &r.result {
                    JobOutcome::Indexed => json!({ "kind": "indexed" }),
                    JobOutcome::Skipped(reason) => {
                        json!({ "kind": "skipped", "reason": reason })
                    }
                    JobOutcome::Failed(error) => {
                        json!({ "kind": "failed", "error": error })
                    }
                };
                json!({
                    "bucket": r.bucket,
                    "key": r.key,
                    "result": result,
                    "total_ms": r.total_ms,
                    "ms_ago": now.duration_since(r.finished_at).as_millis() as u64,
                })
            })
            .collect();
        json!({
            "queue_depth": queue_depth,
            "workers": workers,
            "recent": recent,
        })
    }
}

/// Read OXIDB_FTS_WORKERS / OXIDB_FTS_FLUSH_INTERVAL_MS env vars and
/// spin up the worker pool + periodic flusher thread. Switches the
/// FTS index into batched-persist mode so high-volume ingestion does
/// not rewrite the whole index file on every document.
#[cfg(not(target_arch = "wasm32"))]
fn setup_fts_workers(fts_index: &Arc<RwLock<FtsIndex>>) -> (FtsDispatcher, Arc<FtsRuntime>) {
    let n_workers = std::env::var("OXIDB_FTS_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    let flush_interval_ms = std::env::var("OXIDB_FTS_FLUSH_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1000);

    // Switch into batched mode so individual index_document calls
    // don't each rewrite the whole _fts/index.json file.
    fts_index.write().set_batched(true);

    let runtime = Arc::new(FtsRuntime::new(n_workers));

    let mut senders = Vec::with_capacity(n_workers);
    for worker_id in 0..n_workers {
        let (tx, rx) = mpsc::sync_channel::<FtsJob>(256);
        let fts_worker = Arc::clone(fts_index);
        let runtime_w = Arc::clone(&runtime);
        std::thread::spawn(move || {
            while let Ok(job) = rx.recv() {
                match job {
                    FtsJob::Index {
                        data,
                        content_type,
                        bucket,
                        key,
                    } => {
                        runtime_w.start(worker_id, &bucket, &key);
                        // CPU-bound extraction happens BEFORE the lock so
                        // workers parallelize across cores. catch_unwind:
                        // the extractors (pdf_extract, zip, image decoding)
                        // can panic on malformed input — one bad upload must
                        // not kill the worker thread, which (with the default
                        // single worker) would silently freeze FTS indexing
                        // for the rest of the process lifetime.
                        let extracted =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                fts::extract_text(&data, &content_type)
                            }));
                        match extracted {
                            Ok(Some(text)) => {
                                runtime_w.advance_to_indexing(worker_id);
                                match fts_worker.write().index_document(&bucket, &key, &text) {
                                    Ok(()) => runtime_w.finish(worker_id, JobOutcome::Indexed),
                                    Err(e) => runtime_w
                                        .finish(worker_id, JobOutcome::Failed(e.to_string())),
                                }
                            }
                            Ok(None) => runtime_w.finish(
                                worker_id,
                                JobOutcome::Skipped("no extractor for content type"),
                            ),
                            Err(_) => {
                                eprintln!(
                                    "[fts] text extractor panicked on {bucket}/{key} — skipping"
                                );
                                runtime_w.finish(
                                    worker_id,
                                    JobOutcome::Failed(
                                        "text extractor panicked on malformed input".to_string(),
                                    ),
                                );
                            }
                        }
                    }
                    FtsJob::Remove { bucket, key } => {
                        // Removes are bookkeeping-only — surface them as
                        // a brief in-flight + Indexed entry so the UI sees
                        // something, but the operation is tiny.
                        runtime_w.start(worker_id, &bucket, &key);
                        runtime_w.advance_to_indexing(worker_id);
                        match fts_worker.write().remove_document(&bucket, &key) {
                            Ok(()) => runtime_w.finish(worker_id, JobOutcome::Indexed),
                            Err(e) => {
                                runtime_w.finish(worker_id, JobOutcome::Failed(e.to_string()))
                            }
                        }
                    }
                }
            }
        });
        senders.push(tx);
    }

    // Periodic flusher: writes the FTS index file at most once every
    // flush_interval_ms regardless of how many docs were indexed.
    if flush_interval_ms > 0 {
        let flush_target = Arc::clone(fts_index);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(flush_interval_ms));
                let _ = flush_target.write().flush();
            }
        });
    }

    (FtsDispatcher::new(senders, Arc::clone(&runtime)), runtime)
}

/// The main OxiDB engine. Manages multiple collections.
pub struct OxiDb {
    data_dir: PathBuf,
    collections: RwLock<HashMap<String, Arc<BTreeCollection>>>,
    #[cfg(not(target_arch = "wasm32"))]
    blob_store: BlobStore,
    #[cfg(not(target_arch = "wasm32"))]
    fts_index: Arc<RwLock<FtsIndex>>,
    #[cfg(not(target_arch = "wasm32"))]
    fts_tx: FtsDispatcher,
    #[cfg(not(target_arch = "wasm32"))]
    fts_runtime: Arc<FtsRuntime>,
    #[cfg(not(target_arch = "wasm32"))]
    tx_log: TxCommitLog,
    /// Snapshot of the commit log taken at startup, retained for the life
    /// of the process. Collection opens pass it to WAL replay so entries of
    /// transactions that crashed between WAL fsync and `mark_committed` are
    /// discarded instead of resurrected. Pre-startup state only — new
    /// commits in this run go to `tx_log`, not here.
    #[cfg(not(target_arch = "wasm32"))]
    recovered_tx_ids: std::collections::HashSet<u64>,
    /// PITR archive sequencer — `Some` when `OXIDB_PITR` is enabled. Shared
    /// (`Arc`) by every collection's WAL to stamp records with a global GSN.
    #[cfg(not(target_arch = "wasm32"))]
    archive_sequencer: Option<Arc<crate::pitr::ArchiveSequencer>>,
    next_tx_id: AtomicU64,
    active_transactions: RwLock<HashMap<TransactionId, Mutex<Transaction>>>,
    /// Idle timeout for interactive transactions, in milliseconds
    /// (`OXIDB_TX_MAX_IDLE_SECS`, default 300 s; `0` disables). A
    /// transaction whose owner goes quiet past this is rolled back — its
    /// buffered state dropped, its `find_for_update` locks released —
    /// because a lost client must not park state on the server forever.
    /// The server's disconnect rollback covers a *closed* connection;
    /// this covers the one that stays open (keep-alives, `OXIDB_IDLE_TIMEOUT=0`)
    /// or an embedded caller that leaked a tx id.
    #[cfg(not(target_arch = "wasm32"))]
    tx_max_idle_ms: AtomicU64,
    /// Recently expired transaction ids (bounded to
    /// [`EXPIRED_TX_REMEMBERED`]) so a returning client is told
    /// "expired", not the misleading "not found".
    #[cfg(not(target_arch = "wasm32"))]
    expired_txs: Mutex<std::collections::BTreeSet<TransactionId>>,
    encryption: Option<Arc<EncryptionKey>>,
    verbose: bool,
    #[allow(dead_code)]
    log_callback: Option<LogCallback>,
    change_broker: ChangeStreamBroker,
    #[cfg(not(target_arch = "wasm32"))]
    scheduler_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    lazy_sync: AtomicBool,
    #[cfg(not(target_arch = "wasm32"))]
    sync_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    /// Shutdown channel for the PITR archiver thread (`None` until it is
    /// spawned, which only happens when `OXIDB_PITR` is enabled).
    #[cfg(not(target_arch = "wasm32"))]
    archiver_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    cache_capacity: AtomicUsize,
    /// Max number of *user* collections (names not starting with `_`) this
    /// database may hold. `0` = unlimited (the default). Set per-instance by the
    /// `DatabaseManager` for tenant databases; infrastructure databases stay 0.
    max_collections: AtomicUsize,
    /// Max total number of documents across all *user* collections. `0` =
    /// unlimited (the default). Enforced on the insert path; only tenant
    /// databases arm it, so normal/embedded use pays nothing.
    max_documents: AtomicUsize,
    in_memory: bool,
    /// Serializes the OCC commit critical section (version validation
    /// through apply). Without it, two transactions that both read a
    /// document at version N can both pass validation, both prepare
    /// version N+1, and both apply — the second silently overwriting the
    /// first (a lost update). Holding this lock from validation through
    /// apply guarantees the loser observes the winner's new version and
    /// aborts with a `TransactionConflict`, as OCC requires.
    ///
    /// RwLock, not Mutex: transaction commits take WRITE; direct
    /// (non-transactional) update/delete/find_and_modify take READ. A
    /// direct write that bumped a validated document's version between a
    /// commit's validation and its apply used to be silently overwritten —
    /// OCC never saw the conflict. Direct writes stay concurrent with each
    /// other (shared lock; per-document atomicity is the collection's
    /// job), they only exclude in-flight commits.
    commit_lock: RwLock<()>,
    /// MVCC-lite read snapshots (ADR-0017). Snapshots begin under
    /// `commit_lock.write()`; writers record priors inside their
    /// `commit_lock.read()` scope — that lock is the visibility boundary.
    #[cfg(not(target_arch = "wasm32"))]
    snap_gate: Arc<crate::snapshot::SnapGate>,
    /// Pessimistic per-document locks taken by `tx_find_for_update`.
    /// See `doc_locks.rs` — the hot-document escape hatch from OCC
    /// retry storms. Released on commit (post-apply) and rollback.
    doc_locks: crate::doc_locks::DocLockManager,
    /// Total order of transaction commits. A ticket is assigned under
    /// `commit_lock` once a commit's in-memory apply has succeeded, so
    /// ticket order == the order in which writes became visible.
    commit_ticket: AtomicU64,
    /// Durability poison. Group commit applies a transaction's writes to
    /// memory in phase 1, before the phase-2 WAL fsync. If that fsync
    /// FAILS (disk EIO — "fsyncgate"), the commit returns Err, but the
    /// applied-in-memory state is now untrustworthy: it holds a rejected
    /// transaction's effects. Persisting it would make a "failed" commit
    /// durable. So a fsync failure poisons durability: snapshot persists
    /// and the WAL-truncating final checkpoint are skipped, forcing
    /// recovery to rebuild from the last durable snapshot + WAL replay of
    /// only *marked* (acked) transactions — which excludes the rejected
    /// one. Standard "fsync failure is fatal to in-memory trust" posture.
    durability_poisoned: AtomicBool,
    /// Next ticket allowed to submit its tx_log commit mark (+condvar).
    /// Marks must reach the tx_log committer in ticket order: commit B
    /// may have read commit A's applied-but-not-yet-durable writes, and
    /// submitting marks in order puts A's mark in the same or an earlier
    /// fsync batch — B can never be durable without A. std (not
    /// parking_lot) because Condvar::wait_timeout isn't needed and the
    /// guard must be waitable.
    mark_turn: std::sync::Mutex<u64>,
    mark_cv: std::sync::Condvar,
    /// Per-name serialization of collection opens. Opening a pre-existing
    /// collection replays its WAL, persists a snapshot, and truncates the
    /// WAL; two threads doing that concurrently for the SAME name could
    /// have the loser persist a stale snapshot over the winner's freshly
    /// accepted writes and truncate the WAL that held them. The map is
    /// bounded by the number of distinct collection names ever touched.
    open_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Linked-collection registry (FDW-style remote proxies). Reads
    /// of every collection-targeting command consult this table; a
    /// hit means proxy to the remote OxiDB instead of touching the
    /// local engine. See `src/links.rs`.
    links: Arc<LinksTable>,
    #[cfg(not(target_arch = "wasm32"))]
    ttl_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    #[cfg(not(target_arch = "wasm32"))]
    alert_shutdown: Mutex<Option<mpsc::SyncSender<()>>>,
    #[cfg(feature = "gpu")]
    gpu: Mutex<Option<Arc<crate::gpu::GpuCompute>>>,
}

/// How many expired transaction ids are remembered for accurate error
/// reporting. Ids are monotonic, so eviction drops the oldest.
#[cfg(not(target_arch = "wasm32"))]
const EXPIRED_TX_REMEMBERED: usize = 1024;

/// `OXIDB_TX_MAX_IDLE_SECS` (default 300, `0` = never expire), in ms.
#[cfg(not(target_arch = "wasm32"))]
fn tx_max_idle_ms_from_env() -> u64 {
    std::env::var("OXIDB_TX_MAX_IDLE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300)
        .saturating_mul(1000)
}

impl OxiDb {
    /// Open or create a database at the given directory.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_internal(data_dir, None, false, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_options(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
    ) -> Result<Self> {
        Self::open_internal(data_dir, encryption, false, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_verbose(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
        verbose: bool,
    ) -> Result<Self> {
        Self::open_internal(data_dir, encryption, verbose, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_log(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
        verbose: bool,
        log_callback: LogCallback,
    ) -> Result<Self> {
        Self::open_internal(data_dir, encryption, verbose, Some(log_callback))
    }

    /// Create a pure in-memory database (no disk I/O at all).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_in_memory() -> Result<Self> {
        // Unique per engine instance, not just per process: concurrent
        // in-memory engines (e.g. parallel tests) sharing one directory
        // also shared one _tx_commit_log — their committer threads
        // cross-contaminated each other's committed sets and, with the
        // atomic-replace persist, raced on the same tmp file (ENOENT).
        static MEM_DIR_SEQ: AtomicU64 = AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "oxidb_mem_{}_{}",
            std::process::id(),
            MEM_DIR_SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&tmp)?;

        let blob_store = BlobStore::open_with_encryption(&tmp, None)?;
        let fts_index = Arc::new(RwLock::new(FtsIndex::open(&tmp)?));
        let tx_log = TxCommitLog::open(&tmp)?;

        let (fts_tx, fts_runtime) = setup_fts_workers(&fts_index);

        Ok(Self {
            data_dir: tmp,
            collections: RwLock::new(HashMap::new()),
            blob_store,
            fts_index,
            fts_tx,
            fts_runtime,
            tx_log,
            recovered_tx_ids: std::collections::HashSet::new(),
            archive_sequencer: None,
            next_tx_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            tx_max_idle_ms: AtomicU64::new(tx_max_idle_ms_from_env()),
            expired_txs: Mutex::new(std::collections::BTreeSet::new()),
            encryption: None,
            verbose: false,
            log_callback: None,
            change_broker: ChangeStreamBroker::new(),
            scheduler_shutdown: Mutex::new(None),
            lazy_sync: AtomicBool::new(false),
            sync_shutdown: Mutex::new(None),
            archiver_shutdown: Mutex::new(None),
            cache_capacity: AtomicUsize::new(crate::doc_cache::default_capacity()),
            max_collections: AtomicUsize::new(0),
            max_documents: AtomicUsize::new(0),
            in_memory: true,
            commit_lock: RwLock::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            snap_gate: Arc::new(crate::snapshot::SnapGate::new()),
            doc_locks: crate::doc_locks::DocLockManager::default(),
            commit_ticket: AtomicU64::new(0),
            durability_poisoned: AtomicBool::new(false),
            mark_turn: std::sync::Mutex::new(0),
            mark_cv: std::sync::Condvar::new(),
            open_locks: Mutex::new(HashMap::new()),
            links: LinksTable::in_memory(),
            ttl_shutdown: Mutex::new(None),
            alert_shutdown: Mutex::new(None),
            #[cfg(feature = "gpu")]
            gpu: Mutex::new(None),
        })
    }

    /// Create a pure in-memory database for WebAssembly.
    /// No filesystem, no threads, no blob/FTS subsystems.
    #[cfg(target_arch = "wasm32")]
    pub fn open_in_memory() -> Result<Self> {
        Ok(Self {
            data_dir: PathBuf::new(),
            collections: RwLock::new(HashMap::new()),
            next_tx_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            encryption: None,
            verbose: false,
            log_callback: None,
            change_broker: ChangeStreamBroker::new(),
            lazy_sync: AtomicBool::new(false),
            cache_capacity: AtomicUsize::new(crate::doc_cache::default_capacity()),
            max_collections: AtomicUsize::new(0),
            max_documents: AtomicUsize::new(0),
            in_memory: true,
            commit_lock: RwLock::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            snap_gate: Arc::new(crate::snapshot::SnapGate::new()),
            doc_locks: crate::doc_locks::DocLockManager::default(),
            commit_ticket: AtomicU64::new(0),
            durability_poisoned: AtomicBool::new(false),
            mark_turn: std::sync::Mutex::new(0),
            mark_cv: std::sync::Condvar::new(),
            open_locks: Mutex::new(HashMap::new()),
            links: LinksTable::in_memory(),
        })
    }

    /// Returns true if this database is running in pure in-memory mode.
    pub fn is_in_memory(&self) -> bool {
        self.in_memory
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Start the background TTL eviction thread.
    /// Runs every `interval` and evicts expired documents from all collections.
    pub fn start_ttl_thread(self: &Arc<Self>, interval: std::time::Duration) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("oxidb-ttl".into())
            .spawn(move || {
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    // Time to check for expired documents. The
                    // shared commit lock puts TTL deletes inside the
                    // MVCC-lite snapshot boundary like any writer: a
                    // snapshot either wholly sees a doc TTL kept, or
                    // resolves the recorded prior of one it evicted.
                    let _occ_guard = db.commit_lock.read();
                    // Snapshot before evicting: eviction walks documents and
                    // can write, and holding the registry across the whole
                    // sweep parks every request behind us. See the sync loop.
                    let targets: Vec<_> = {
                        let cols = db.collections.read();
                        cols.values().cloned().collect()
                    };
                    for col_arc in targets {
                        col_arc.evict_expired();
                        col_arc.evict_ttl_indexed();
                    }
                    drop(_occ_guard);
                    #[cfg(not(target_arch = "wasm32"))]
                    db.snap_gate.expire_tick();
                    // Abandoned interactive transactions expire on the
                    // same heartbeat — their client may never send
                    // another request to trigger the lazy check.
                    db.expire_stale_transactions();
                }
            })
            .expect("failed to spawn TTL thread");
        *self.ttl_shutdown.lock() = Some(tx);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_internal(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
        verbose: bool,
        log_callback: Option<LogCallback>,
    ) -> Result<Self> {
        let vlog = |msg: &str| {
            eprintln!("{msg}");
            if let Some(cb) = &log_callback {
                cb(msg);
            }
        };

        if verbose {
            vlog(&format!(
                "[verbose] opening database at {}",
                data_dir.display()
            ));
        }

        std::fs::create_dir_all(data_dir)?;

        #[cfg(not(target_arch = "wasm32"))]
        Self::check_legacy_dat_layout(data_dir)?;

        let blob_store = BlobStore::open_with_encryption(data_dir, encryption.clone())?;

        if verbose {
            vlog("[verbose] blob store opened");
        }

        let fts_index = Arc::new(RwLock::new(FtsIndex::open(data_dir)?));

        if verbose {
            vlog("[verbose] FTS index loaded");
        }

        // Open transaction commit log and read committed tx_ids for recovery
        let tx_log = TxCommitLog::open(data_dir)?;
        let committed_tx_ids = tx_log.read_committed()?;

        if verbose && !committed_tx_ids.is_empty() {
            vlog(&format!(
                "[verbose] tx commit log: {} committed transactions to recover",
                committed_tx_ids.len()
            ));
        }

        let (fts_tx, fts_runtime) = setup_fts_workers(&fts_index);

        if verbose {
            vlog("[verbose] FTS worker threads started");
        }

        // PITR: when OXIDB_PITR is enabled, open the archive sequencer so
        // every collection's WAL stamps its records with a global GSN +
        // wall-clock. Disabled by default — zero cost when off.
        let pitr_enabled = std::env::var("OXIDB_PITR")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
            .unwrap_or(false);
        let archive_sequencer = if pitr_enabled {
            let seq = Arc::new(crate::pitr::ArchiveSequencer::open(data_dir)?);
            if verbose {
                vlog(&format!(
                    "[verbose] PITR enabled — archive sequencer open, next GSN {}",
                    seq.current_gsn()
                ));
            }
            Some(seq)
        } else {
            None
        };

        // Names gathered (and logged) before `Self {}` moves `log_callback`.
        let pending_recovery = Self::collections_with_wal_data(data_dir);
        if verbose && !pending_recovery.is_empty() {
            vlog(&format!(
                "[verbose] recovering {} collection(s) with pending WAL data: {:?}",
                pending_recovery.len(),
                pending_recovery
            ));
        }

        let db = Self {
            data_dir: data_dir.to_path_buf(),
            collections: RwLock::new(HashMap::new()),
            blob_store,
            fts_index,
            fts_tx,
            fts_runtime,
            tx_log,
            recovered_tx_ids: committed_tx_ids.clone(),
            archive_sequencer,
            next_tx_id: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            tx_max_idle_ms: AtomicU64::new(tx_max_idle_ms_from_env()),
            expired_txs: Mutex::new(std::collections::BTreeSet::new()),
            encryption,
            verbose,
            log_callback,
            change_broker: ChangeStreamBroker::new(),
            scheduler_shutdown: Mutex::new(None),
            lazy_sync: AtomicBool::new(false),
            sync_shutdown: Mutex::new(None),
            archiver_shutdown: Mutex::new(None),
            cache_capacity: AtomicUsize::new(crate::doc_cache::default_capacity()),
            max_collections: AtomicUsize::new(0),
            max_documents: AtomicUsize::new(0),
            in_memory: false,
            commit_lock: RwLock::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            snap_gate: Arc::new(crate::snapshot::SnapGate::new()),
            doc_locks: crate::doc_locks::DocLockManager::default(),
            commit_ticket: AtomicU64::new(0),
            durability_poisoned: AtomicBool::new(false),
            mark_turn: std::sync::Mutex::new(0),
            mark_cv: std::sync::Condvar::new(),
            open_locks: Mutex::new(HashMap::new()),
            links: LinksTable::open(data_dir)?,
            ttl_shutdown: Mutex::new(None),
            alert_shutdown: Mutex::new(None),
            #[cfg(feature = "gpu")]
            gpu: Mutex::new(None),
        };

        // Crash recovery: eagerly open every collection that still has WAL
        // data, so its entries are replayed — filtered through the commit
        // log — and checkpointed NOW, while the commit log still records
        // which transactions reached their commit point. If this were left
        // to lazy opens, a collection first touched in a *later* run would
        // see an already-cleared commit log and misclassify committed
        // entries as uncommitted (or vice versa). After a clean shutdown
        // every WAL is empty and this loop opens nothing.
        for name in &pending_recovery {
            db.get_or_create_collection(name)?;
        }

        // All pending WALs are replayed and checkpointed; the commit log
        // entries have served their purpose.
        if !committed_tx_ids.is_empty() {
            db.tx_log.clear()?;
        }

        Ok(db)
    }

    /// Names of collections whose live WAL holds records (length beyond the
    /// 8-byte header) or that have sealed WAL segments (`<name>.wal.<seq>`)
    /// — i.e. collections with state to recover at startup.
    #[cfg(not(target_arch = "wasm32"))]
    fn collections_with_wal_data(data_dir: &Path) -> Vec<String> {
        let mut names = std::collections::BTreeSet::new();
        let Ok(rd) = std::fs::read_dir(data_dir) else {
            return Vec::new();
        };
        for entry in rd.flatten() {
            let fname = entry.file_name();
            let Some(fname) = fname.to_str() else {
                continue;
            };
            if let Some(stem) = fname.strip_suffix(".wal") {
                // Live WAL: 0 bytes after checkpoint, 8 bytes when only the
                // OXWA header was written; any record makes it longer.
                if entry.metadata().map(|m| m.len() > 8).unwrap_or(false) {
                    names.insert(stem.to_string());
                }
            } else if let Some((stem, seq)) = fname.rsplit_once('.') {
                // Sealed segment `<name>.wal.<seq>` (PITR).
                if !seq.is_empty()
                    && seq.bytes().all(|b| b.is_ascii_digit())
                    && stem.ends_with(".wal")
                {
                    names.insert(stem.trim_end_matches(".wal").to_string());
                }
            }
        }
        names.into_iter().collect()
    }

    // -----------------------------------------------------------------------
    // Linked collections (FDW v1)
    // -----------------------------------------------------------------------

    /// Register a linked collection — a local name that, when used in
    /// read commands (find / find_one / count / aggregate), proxies to
    /// a remote OxiDB. Write commands against a linked name are
    /// refused by the handler.
    ///
    /// Replaces any existing link with the same name. URL validation
    /// happens here so a malformed URL is caught at registration,
    /// not on every subsequent query.
    pub fn link_collection(&self, name: &str, url: &str) -> Result<LinkConfig> {
        if name.is_empty() {
            return Err(Error::InvalidQuery("link name must not be empty".into()));
        }
        let cfg = LinkConfig {
            name: name.to_string(),
            url: url.to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        self.links.insert(cfg.clone())?;
        Ok(cfg)
    }

    /// Drop a previously registered link. Returns `true` if the link
    /// existed; `false` for an unknown name (the same behaviour as
    /// `drop_collection` on a missing collection).
    pub fn unlink_collection(&self, name: &str) -> Result<bool> {
        self.links.remove(name)
    }

    /// Snapshot every registered link, ordered by name.
    pub fn list_links(&self) -> Vec<LinkConfig> {
        self.links.list()
    }

    /// Look up the link config for a local collection name, or
    /// `None` if not linked. Used by the handler to route reads to
    /// the proxy and to refuse writes.
    pub fn lookup_link(&self, name: &str) -> Option<LinkConfig> {
        self.links.get(name)
    }

    /// Take (creating if needed) the per-name open lock for `name`. Callers
    /// hold the returned guard's Arc across the open so two threads can never
    /// run `open_resolved` for the same collection concurrently.
    fn open_lock_for(&self, name: &str) -> Arc<Mutex<()>> {
        let mut locks = self.open_locks.lock();
        Arc::clone(
            locks
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    /// Return an Arc to a collection, auto-creating if needed.
    fn get_or_create_collection(&self, name: &str) -> Result<Arc<BTreeCollection>> {
        {
            let cols = self.collections.read();
            if let Some(col) = cols.get(name) {
                return Ok(Arc::clone(col));
            }
        }
        // Serialize the open per name; re-check the map once the lock is
        // held — the previous holder usually inserted the collection.
        let name_lock = self.open_lock_for(name);
        let _open_guard = name_lock.lock();
        {
            let cols = self.collections.read();
            if let Some(col) = cols.get(name) {
                return Ok(Arc::clone(col));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        self.enforce_collection_limit(name)?;
        #[cfg(not(target_arch = "wasm32"))]
        let col = if self.in_memory {
            BTreeCollection::open_in_memory(name)
        } else {
            BTreeCollection::open_recovering(
                name,
                &self.data_dir,
                self.encryption.clone(),
                self.archive_sequencer.clone(),
                None,
                &self.recovered_tx_ids,
            )?
        };
        #[cfg(target_arch = "wasm32")]
        let col = BTreeCollection::open_in_memory(name);
        if self.lazy_sync.load(Ordering::Acquire) {
            col.set_lazy_sync(true);
        }
        col.set_cache_capacity(self.cache_capacity.load(Ordering::Acquire));
        #[cfg(not(target_arch = "wasm32"))]
        col.set_snap_gate(Arc::clone(&self.snap_gate));
        let arc = Arc::new(col);
        let mut cols = self.collections.write();
        if let Some(existing) = cols.get(name) {
            return Ok(Arc::clone(existing));
        }
        cols.insert(name.to_string(), Arc::clone(&arc));
        Ok(arc)
    }

    /// Create a new collection.
    ///
    /// Opens the collection (disk I/O) without holding the global write lock,
    /// then takes the lock only to insert. Mirrors `get_or_create_collection`'s
    /// concurrency pattern so parallel `create_collection` calls don't serialize.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        // Fast path: already exists.
        {
            let cols = self.collections.read();
            if cols.contains_key(name) {
                return Err(Error::CollectionAlreadyExists(name.to_string()));
            }
        }

        // Open without holding the write lock — concurrent creates of distinct
        // collections proceed in parallel here; same-name opens serialize on
        // the per-name lock (a concurrent double-open replays/persists/
        // truncates the same WAL twice and can lose accepted writes).
        let name_lock = self.open_lock_for(name);
        let _open_guard = name_lock.lock();
        {
            let cols = self.collections.read();
            if cols.contains_key(name) {
                return Err(Error::CollectionAlreadyExists(name.to_string()));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        self.enforce_collection_limit(name)?;
        #[cfg(not(target_arch = "wasm32"))]
        let col = if self.in_memory {
            BTreeCollection::open_in_memory(name)
        } else {
            BTreeCollection::open_recovering(
                name,
                &self.data_dir,
                self.encryption.clone(),
                self.archive_sequencer.clone(),
                None,
                &self.recovered_tx_ids,
            )?
        };
        #[cfg(target_arch = "wasm32")]
        let col = BTreeCollection::open_in_memory(name);
        if self.lazy_sync.load(Ordering::Acquire) {
            col.set_lazy_sync(true);
        }
        col.set_cache_capacity(self.cache_capacity.load(Ordering::Acquire));
        #[cfg(not(target_arch = "wasm32"))]
        col.set_snap_gate(Arc::clone(&self.snap_gate));

        // Acquire the write lock only to insert. Re-check existence to
        // handle the race with another writer that won.
        let mut cols = self.collections.write();
        if cols.contains_key(name) {
            return Err(Error::CollectionAlreadyExists(name.to_string()));
        }
        cols.insert(name.to_string(), Arc::new(col));
        Ok(())
    }

    /// Create a new collection with **explicit** per-collection
    /// [`StorageOptions`](crate::btree_storage::StorageOptions) — disk-first vs
    /// in-RAM, `.bdat` compression, and the compaction policy — instead of the
    /// process-wide env defaults. For disk-first collections the options are
    /// persisted (`<name>.bopts`) so reopens are consistent regardless of the
    /// environment. On an in-memory engine the storage shape is forced to
    /// in-RAM and `opts` is ignored.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_collection_with_options(
        &self,
        name: &str,
        opts: crate::btree_storage::StorageOptions,
    ) -> Result<()> {
        {
            let cols = self.collections.read();
            if cols.contains_key(name) {
                return Err(Error::CollectionAlreadyExists(name.to_string()));
            }
        }

        // Per-name open serialization — see `create_collection`.
        let name_lock = self.open_lock_for(name);
        let _open_guard = name_lock.lock();
        {
            let cols = self.collections.read();
            if cols.contains_key(name) {
                return Err(Error::CollectionAlreadyExists(name.to_string()));
            }
        }
        let col = if self.in_memory {
            BTreeCollection::open_in_memory(name)
        } else {
            BTreeCollection::open_recovering(
                name,
                &self.data_dir,
                self.encryption.clone(),
                self.archive_sequencer.clone(),
                Some(opts),
                &self.recovered_tx_ids,
            )?
        };
        if self.lazy_sync.load(Ordering::Acquire) {
            col.set_lazy_sync(true);
        }
        col.set_cache_capacity(self.cache_capacity.load(Ordering::Acquire));
        #[cfg(not(target_arch = "wasm32"))]
        col.set_snap_gate(Arc::clone(&self.snap_gate));

        let mut cols = self.collections.write();
        if cols.contains_key(name) {
            return Err(Error::CollectionAlreadyExists(name.to_string()));
        }
        cols.insert(name.to_string(), Arc::new(col));
        Ok(())
    }

    /// List all collection names (both in-memory and on disk).
    pub fn list_collections(&self) -> Vec<String> {
        let mut names: std::collections::HashSet<String> = {
            let cols = self.collections.read();
            cols.keys().cloned().collect()
        };
        #[cfg(not(target_arch = "wasm32"))]
        if !self.in_memory
            && let Ok(disk_names) = Self::discover_collection_names_on_disk(&self.data_dir)
        {
            for name in disk_names {
                names.insert(name);
            }
        }
        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }

    /// Flush all index data to disk for every loaded collection.
    /// Snapshot every collection now, checkpointing any WAL that has grown
    /// past `OXIDB_WAL_CHECKPOINT_BYTES`. The background sync thread does this
    /// on a timer; this is the same work on demand, for callers that want a
    /// durable point without shutting down.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn sync_all(&self) -> Result<()> {
        // Snapshot the handles and let go of the registry: the flush below is
        // file work, and this lock is on the path of every read and write.
        // See the background sync loop for the full reasoning.
        let targets: Vec<_> = {
            let cols = self.collections.read();
            cols.values().cloned().collect()
        };
        for col in targets {
            col.sync_writes()?;
        }
        self.flush_indexes();
        Ok(())
    }

    pub fn flush_indexes(&self) {
        let targets: Vec<_> = {
            let cols = self.collections.read();
            cols.values().cloned().collect()
        };
        for col_arc in targets {
            col_arc.save_index_data();
        }
    }

    /// Drain background workers and persist everything synchronously.
    ///
    /// Same effect as letting `Drop` run, but callable explicitly so a
    /// signal handler can flush before the process exits (process exit
    /// skips Drop on Arc'd state held by spawned threads). Idempotent:
    /// once shutdown channels are taken they stay None on the second
    /// call, and the per-collection flushes are safe to repeat.
    /// No-op on wasm32: that target spawns no background threads, so there is
    /// nothing to signal or flush. Defined so callers (e.g. dropping a
    /// database) stay target-independent.
    #[cfg(target_arch = "wasm32")]
    pub fn shutdown(&self) {}

    #[cfg(not(target_arch = "wasm32"))]
    pub fn shutdown(&self) {
        // Drop the senders → background sync/scheduler/alert/TTL threads
        // see RecvError, run their own final-flush blocks, exit. The TTL
        // thread holds a strong Arc to the engine, so without this signal a
        // dropped database's engine would never be released.
        let _ = self.scheduler_shutdown.lock().take();
        let _ = self.sync_shutdown.lock().take();
        let _ = self.alert_shutdown.lock().take();
        let _ = self.ttl_shutdown.lock().take();
        #[cfg(not(target_arch = "wasm32"))]
        let _ = self.archiver_shutdown.lock().take();
        // Final checkpoint: persist + truncate WAL. Safe at shutdown
        // because no writers are racing the persist anymore. During
        // normal operation we only persist (sync_writes); the WAL
        // truncate is intentionally deferred to here.
        #[cfg(not(target_arch = "wasm32"))]
        {
            // PITR: before final_checkpoint truncates the live WAL, seal
            // each collection's tail into a sealed segment and run one
            // synchronous archive pass — otherwise the un-sealed tail
            // would never reach the archive. Sealed segments survive the
            // truncate, so even if this races the archiver thread the
            // next startup's reconcile still picks them up.
            if let Some(archive_dir) = self.pitr_archive_dir() {
                {
                    let cols = self.collections.read();
                    for col_arc in cols.values() {
                        let _ = col_arc.seal_wal();
                    }
                }
                if let Err(e) = crate::archive::archive_pass(
                    &self.data_dir,
                    &archive_dir,
                    self.encryption.as_ref(),
                ) {
                    eprintln!("[archiver] shutdown pass failed: {e}");
                }
            }
            // Let any straggler commit finish its durability phase before
            // the final persist (see wait_marks_settled).
            self.wait_marks_settled();
            if self.durability_poisoned.load(Ordering::SeqCst) {
                // A fsync failed earlier: the in-memory state holds a
                // rejected transaction. Do NOT persist it and do NOT
                // truncate any WAL — leave the last durable snapshot and
                // the full WAL intact so recovery replays only marked
                // (acked) transactions, dropping the rejected one.
                self.flush_indexes();
                return;
            }
            let cols = self.collections.read();
            let mut all_checkpointed = true;
            for col_arc in cols.values() {
                if col_arc.final_checkpoint().is_err() {
                    all_checkpointed = false;
                }
            }
            drop(cols);
            // Every WAL is persisted + truncated; the commit log has no
            // entries left to vouch for. Only clear when every checkpoint
            // succeeded — a failed persist means its WAL (and the ids
            // vouching for its tx entries) are still needed for recovery.
            if all_checkpointed {
                let _ = self.tx_log.clear();
            }
        }
        self.flush_indexes();
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Enable lazy sync mode: write operations skip per-operation fsync,
    /// and a background thread flushes all collections every `interval`.
    /// This matches MongoDB's default durability (journal flushed periodically).
    pub fn enable_lazy_sync(self: &Arc<Self>, interval: std::time::Duration) {
        // PITR requires per-op WAL durability — every archived record must
        // already be fsynced. With PITR enabled, lazy-sync downgrades to
        // just the periodic-snapshot thread; the WAL keeps fsyncing per op.
        if self.archive_sequencer.is_some() {
            if self.verbose {
                eprintln!(
                    "[verbose] OXIDB_PITR is on — lazy-sync downgraded to periodic-snapshot-only (WAL fsync stays per-op)"
                );
            }
            self.spawn_periodic_sync(interval);
            return;
        }
        self.lazy_sync.store(true, Ordering::Release);
        {
            let cols = self.collections.read();
            for col_arc in cols.values() {
                col_arc.set_lazy_sync(true);
            }
        }
        self.spawn_periodic_sync(interval);
    }

    /// Start the background snapshot thread without flipping any
    /// collection into lazy-fsync mode. Used in strict ACID-D mode
    /// where every commit already fsyncs the WAL — the background
    /// thread's only job is to flush BTree snapshots periodically so
    /// WAL doesn't grow unbounded and recovery time stays short.
    /// Same shutdown channel and final-flush semantics as
    /// enable_lazy_sync.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn enable_periodic_snapshot(self: &Arc<Self>, interval: std::time::Duration) {
        self.spawn_periodic_sync(interval);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn spawn_periodic_sync(self: &Arc<Self>, interval: std::time::Duration) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("oxidb-sync".into())
            .spawn(move || {
                let mut flush_counter = 0u64;
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    // A prior fsync failure poisoned durability:
                    // never persist the untrusted in-memory state
                    // (it holds a rejected transaction). Let
                    // recovery rebuild from the durable snapshot +
                    // marked-WAL replay instead.
                    if db.durability_poisoned.load(Ordering::SeqCst) {
                        continue;
                    }
                    // Snapshot the commit log BEFORE persisting.
                    // Under the commit lock no transaction sits
                    // between its commit point and apply, so every
                    // id in the snapshot is fully applied in memory;
                    // the persist pass below then makes that data
                    // durable, after which the ids (and their WAL
                    // entries) are redundant and can be pruned. Ids
                    // marked after the snapshot stay — their data may
                    // post-date this persist.
                    let prune: Vec<u64> = {
                        let _commit_guard = db.commit_lock.write();
                        // Group commit applies writes before their
                        // marks are durable; wait for every applied
                        // tx to clear the mark turnstile so the
                        // persist below never snapshots data whose
                        // commit record could still be lost.
                        db.wait_marks_settled();
                        db.tx_log
                            .read_committed()
                            .map(|s| s.into_iter().collect())
                            .unwrap_or_default()
                    };
                    let mut all_persisted = true;
                    // Snapshot the handles, then release the registry before
                    // doing any file work. Holding the read lock across the
                    // whole pass is what wedges the server: sync_writes can
                    // snapshot, checkpoint and even compact a collection —
                    // seconds of I/O on a large one — and this lock is fair,
                    // so a single writer queued behind us (any request that
                    // opens a *new* collection) parks every subsequent reader.
                    // Since every insert and find starts by resolving its
                    // collection through this lock, the whole engine stops
                    // until the pass ends. The Arcs keep these alive; a
                    // collection created meanwhile is picked up next tick.
                    let targets: Vec<_> = {
                        let cols = db.collections.read();
                        cols.values().cloned().collect()
                    };
                    for col_arc in targets {
                        if col_arc.sync_writes().is_err() {
                            all_persisted = false;
                        }
                    }
                    if all_persisted && !prune.is_empty() {
                        let _ = db.tx_log.remove_committed_many(&prune);
                    }
                    flush_counter += 1;
                    // Persist field indexes roughly every 10s
                    // regardless of cadence — they're small and
                    // their write is independent of btree pages.
                    let ticks_per_10s = (10_000 / interval.as_millis().max(1)) as u64;
                    if flush_counter % ticks_per_10s.max(1) == 0 {
                        db.flush_indexes();
                    }
                }
                // Same reasoning as the periodic pass above: snapshot, then
                // release, then do the slow part.
                let targets: Vec<_> = {
                    let cols = db.collections.read();
                    cols.values().cloned().collect()
                };
                for col_arc in targets {
                    let _ = col_arc.sync_writes();
                }
                db.flush_indexes();
            })
            .expect("failed to spawn sync thread");

        *self.sync_shutdown.lock() = Some(tx);

        // When PITR is enabled, run the archiver alongside the periodic
        // snapshot — it copies sealed WAL segments into the archive on
        // its own cadence.
        if self.archive_sequencer.is_some() {
            self.spawn_archiver();
        }
    }

    /// The PITR archive directory, when PITR is enabled — `OXIDB_ARCHIVE_DIR`
    /// if set, else `<data_dir>/_archive`.
    #[cfg(not(target_arch = "wasm32"))]
    fn pitr_archive_dir(&self) -> Option<PathBuf> {
        self.archive_sequencer
            .as_ref()
            .map(|_| crate::archive::archive_dir_for(&self.data_dir))
    }

    /// Spawn the PITR archiver thread: every `OXIDB_ARCHIVE_INTERVAL`
    /// seconds (default 10) it copies any sealed WAL segments into the
    /// archive directory, crash-safely. Best-effort — a failed pass is
    /// logged and retried next tick; it never blocks or fails a
    /// foreground write, and reads only immutable sealed segments.
    #[cfg(not(target_arch = "wasm32"))]
    fn spawn_archiver(self: &Arc<Self>) {
        let archive_dir = match self.pitr_archive_dir() {
            Some(d) => d,
            None => return,
        };
        let interval_secs = std::env::var("OXIDB_ARCHIVE_INTERVAL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(10);
        let interval = std::time::Duration::from_secs(interval_secs);
        // Retention: 0 (default) disables pruning entirely.
        let retention_hours = std::env::var("OXIDB_ARCHIVE_RETENTION_HOURS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("oxidb-archiver".into())
            .spawn(move || {
                // One archiving + retention pass; best-effort, never panics.
                let pass = |label: &str| {
                    if let Err(e) = crate::archive::archive_pass(
                        &db.data_dir,
                        &archive_dir,
                        db.encryption.as_ref(),
                    ) {
                        eprintln!("[archiver] {label} archive pass failed: {e}");
                    }
                    if retention_hours > 0
                        && let Err(e) = crate::archive::prune_archive(
                            &archive_dir,
                            Some(&db.data_dir),
                            retention_hours,
                        )
                    {
                        eprintln!("[archiver] {label} prune failed: {e}");
                    }
                };
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    pass("periodic");
                }
                // Final pass on shutdown so the last sealed segments land.
                pass("final");
            })
            .expect("failed to spawn archiver thread");

        *self.archiver_shutdown.lock() = Some(tx);
    }

    /// Summary of the PITR archive — segment count and GSN / time
    /// coverage. Returns a zeroed summary when PITR has never run.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn archive_status(&self) -> Result<crate::archive::ArchiveStatus> {
        crate::archive::archive_status(&crate::archive::archive_dir_for(&self.data_dir))
    }

    /// The encryption key this database was opened with, if any. Admin
    /// tooling such as `restore_to_point` needs it to read the same
    /// at-rest-encrypted WAL segments.
    pub fn encryption_key(&self) -> Option<Arc<EncryptionKey>> {
        self.encryption.clone()
    }

    /// Returns whether lazy sync mode is enabled.
    pub fn is_lazy_sync(&self) -> bool {
        self.lazy_sync.load(Ordering::Acquire)
    }

    /// Set the per-collection LRU document cache capacity.
    /// Applies immediately to all loaded collections and to future collections.
    pub fn set_cache_capacity(&self, capacity: usize) {
        self.cache_capacity.store(capacity, Ordering::Release);
        let cols = self.collections.read();
        for col_arc in cols.values() {
            col_arc.set_cache_capacity(capacity);
        }
    }

    /// Returns the current per-collection cache capacity.
    pub fn cache_capacity(&self) -> usize {
        self.cache_capacity.load(Ordering::Acquire)
    }

    /// Cap the number of *user* collections (names not starting with `_`) this
    /// database may hold. `0` = unlimited. System collections and loading an
    /// already-existing collection are never blocked.
    pub fn set_max_collections(&self, max: usize) {
        self.max_collections.store(max, Ordering::Release);
    }

    /// Returns the configured user-collection cap (`0` = unlimited).
    pub fn max_collections(&self) -> usize {
        self.max_collections.load(Ordering::Acquire)
    }

    /// Cap the total number of documents across all *user* collections.
    /// `0` = unlimited.
    pub fn set_max_documents(&self, max: usize) {
        self.max_documents.store(max, Ordering::Release);
    }

    /// Returns the configured total-document cap (`0` = unlimited).
    pub fn max_documents(&self) -> usize {
        self.max_documents.load(Ordering::Acquire)
    }

    /// Total documents across all user collections (system `_` collections
    /// excluded). Used to enforce the per-database document quota; bounded work
    /// because the collection cap bounds how many collections exist.
    #[cfg(not(target_arch = "wasm32"))]
    fn total_user_documents(&self) -> usize {
        self.list_collections()
            .iter()
            .filter(|c| !c.starts_with('_'))
            .map(|c| self.count(c, &json!({})).unwrap_or(0))
            .sum()
    }

    /// Reject an insert of `adding` documents into `collection` when it would
    /// push the database past its document cap. No-op when the cap is 0
    /// (unlimited) or the target is a system collection (leading `_`), so engine
    /// bookkeeping is never blocked by a tenant quota.
    #[cfg(not(target_arch = "wasm32"))]
    fn enforce_document_limit(&self, collection: &str, adding: usize) -> Result<()> {
        let max = self.max_documents.load(Ordering::Acquire);
        if max == 0 || collection.starts_with('_') {
            return Ok(());
        }
        if self.total_user_documents() + adding > max {
            return Err(Error::DocumentLimitExceeded(max));
        }
        Ok(())
    }

    /// Reject creating a *new* user collection when the cap is reached. Loading a
    /// collection that already exists (in memory or on disk) and any system
    /// collection (leading `_`) are always allowed, so quotas never lock a
    /// tenant out of existing data or break engine bookkeeping.
    #[cfg(not(target_arch = "wasm32"))]
    fn enforce_collection_limit(&self, name: &str) -> Result<()> {
        let max = self.max_collections.load(Ordering::Acquire);
        if max == 0 || name.starts_with('_') {
            return Ok(());
        }
        let existing = self.list_collections();
        if existing.iter().any(|c| c == name) {
            return Ok(()); // already exists → a load, not a create
        }
        let user_count = existing.iter().filter(|c| !c.starts_with('_')).count();
        if user_count >= max {
            return Err(Error::CollectionLimitExceeded(max));
        }
        Ok(())
    }

    /// Drop a collection and its data.
    ///
    /// On-disk a collection's `.btree` snapshot can be either a file
    /// (single-page collections) or a directory (multi-page); see the
    /// comment on `discover_collection_names_on_disk` and the use of
    /// "files/directories" there. The drop path must handle both —
    /// blindly calling `remove_dir_all` on a file raises
    /// `io error: Not a directory (os error 20)` and aborts the drop
    /// with the collection's bookkeeping already cleared from the
    /// in-memory map (leaving the data files orphaned on disk).
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        // Hold the per-name open lock so a concurrent first-touch open can't
        // be mid-`open_resolved` while files disappear underneath it.
        let name_lock = self.open_lock_for(name);
        let _open_guard = name_lock.lock();
        let mut cols = self.collections.write();
        cols.remove(name);
        #[cfg(not(target_arch = "wasm32"))]
        if !self.in_memory {
            for ext in &[
                "dat", "wal", "idx", "fidx", "cidx", "vidx",
                // Previously leaked, with concrete consequences for a
                // re-created collection of the same name:
                "bdat",  // disk-first data file (old documents)
                "bopts", // persisted storage options (silently inherited)
                "worm",  // WORM locks (applied to reused doc ids)
            ] {
                let path = self.data_dir.join(format!("{}.{}", name, ext));
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
            }
            let btree_path = self.data_dir.join(format!("{}.btree", name));
            if btree_path.is_dir() {
                std::fs::remove_dir_all(&btree_path)?;
            } else if btree_path.exists() {
                std::fs::remove_file(&btree_path)?;
            }
            // Pattern-matched leftovers: sealed WAL segments
            // (`<name>.wal.<seq>` — replayed on the next open of a same-named
            // collection, resurrecting dropped documents) and disk-first
            // field-index files (`<name>.<field>.mfidx`).
            if let Ok(rd) = std::fs::read_dir(&self.data_dir) {
                let wal_prefix = format!("{}.wal.", name);
                let mfidx_prefix = format!("{}.", name);
                for entry in rd.flatten() {
                    let fname = entry.file_name();
                    let Some(fname) = fname.to_str() else {
                        continue;
                    };
                    let is_sealed_segment = fname.strip_prefix(&wal_prefix).is_some_and(|seq| {
                        !seq.is_empty() && seq.bytes().all(|b| b.is_ascii_digit())
                    });
                    let is_mfidx = fname.starts_with(&mfidx_prefix) && fname.ends_with(".mfidx");
                    if is_sealed_segment || is_mfidx {
                        std::fs::remove_file(entry.path())?;
                    }
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Change stream methods
    // -----------------------------------------------------------------------

    /// Subscribe to change events. Returns a `WatchHandle` with the subscriber
    /// ID, event receiver, and backpressure tracking.
    ///
    /// If `resume_after` is `Some(token)`, missed events are replayed from an
    /// internal ring buffer. Returns `Err(ResumeError::TokenTooOld)` if the
    /// token has been evicted.
    pub fn watch(
        &self,
        filter: WatchFilter,
        resume_after: Option<u64>,
    ) -> std::result::Result<WatchHandle, ResumeError> {
        self.change_broker.subscribe(filter, 256, resume_after)
    }

    /// Unsubscribe from change events.
    pub fn unwatch(&self, id: SubscriberId) {
        self.change_broker.unsubscribe(id);
    }

    // -----------------------------------------------------------------------
    // Convenience methods that delegate to collections
    // -----------------------------------------------------------------------

    pub fn insert(&self, collection: &str, doc: Value) -> Result<DocumentId> {
        let col = self.get_or_create_collection(collection)?;
        #[cfg(not(target_arch = "wasm32"))]
        self.enforce_document_limit(collection, 1)?;
        let emit = self.change_broker.has_subscribers();
        let doc_clone = if emit { Some(doc.clone()) } else { None };
        // Shared commit lock (see update()): also the MVCC-lite snapshot
        // boundary — a snapshot begins under the write side, so an insert
        // is either wholly before it (visible) or wholly after (recorded).
        let _occ_guard = self.commit_lock.read();
        let id = col.insert(doc)?;
        if let Some(mut d) = doc_clone {
            if let Some(obj) = d.as_object_mut() {
                obj.insert("_id".to_string(), Value::Number(id.into()));
                obj.insert("_version".to_string(), Value::Number(1.into()));
            }
            self.change_broker.emit(ChangeEvent {
                token: 0,
                operation: OperationType::Insert,
                collection: collection.to_string(),
                doc_id: id,
                document: Some(d),
                tx_id: None,
            });
        }
        Ok(id)
    }

    pub fn insert_many(&self, collection: &str, docs: Vec<Value>) -> Result<Vec<DocumentId>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }

        let col = self.get_or_create_collection(collection)?;
        #[cfg(not(target_arch = "wasm32"))]
        self.enforce_document_limit(collection, docs.len())?;
        let emit = self.change_broker.has_subscribers();
        // Shared commit lock — see insert(); also the snapshot boundary.
        let _occ_guard = self.commit_lock.read();

        // Phase 1: Reserve IDs and check unique constraints
        let (first_id, has_unique_indexes, unique_fields, need_values) = {
            let count = docs.len() as u64;

            let (unique_fields, need_values, _) = index_metadata(&col);

            let first_id = col.reserve_ids(count);

            // Check existing unique constraints only when unique indexes exist
            if !unique_fields.is_empty() {
                for (i, doc) in docs.iter().enumerate() {
                    let id = first_id + i as u64;
                    let mut check_doc = doc.clone();
                    if let Some(obj) = check_doc.as_object_mut() {
                        obj.insert("_id".to_string(), Value::Number(id.into()));
                    }
                    col.check_unique_constraints(&check_doc, None)?;
                }
            }

            (
                first_id,
                !unique_fields.is_empty(),
                unique_fields,
                need_values,
            )
        };

        // Phase 2: Pre-serialize all documents (no lock held — other threads can work)

        // When no indexes need the Value and batch is large, drop it after
        // encoding to reduce allocator churn.
        let keep_values = need_values || emit || docs.len() <= 1000;

        // Assign IDs and prepare docs. The intra-batch uniqueness map lives
        // OUTSIDE the per-document loop — declared inside it (as before) it
        // was recreated empty for every doc, so two documents in the same
        // batch carrying the same unique value both sailed through.
        let mut pending_unique: HashMap<String, HashMap<IndexValue, DocumentId>> = HashMap::new();
        let mut docs_with_ids: Vec<(u64, Value)> = Vec::with_capacity(docs.len());
        for (i, mut data) in docs.into_iter().enumerate() {
            if !data.is_object() {
                return Err(Error::NotAnObject);
            }
            let id = first_id + i as u64;
            let obj = data.as_object_mut().unwrap();
            obj.insert("_id".to_string(), Value::Number(id.into()));
            obj.insert("_version".to_string(), Value::Number(1.into()));

            // Intra-batch uniqueness check
            if has_unique_indexes {
                for field in &unique_fields {
                    if let Some(value) = resolve_field_in_value(&data, field) {
                        let iv = IndexValue::from_json(value);
                        let field_map = pending_unique.entry(field.clone()).or_default();
                        if field_map.contains_key(&iv) {
                            return Err(Error::UniqueViolation {
                                field: field.clone(),
                            });
                        }
                        field_map.insert(iv, id);
                    }
                }
            }
            docs_with_ids.push((id, data));
        }

        // Parallel JSONB encode for large batches
        #[cfg(not(target_arch = "wasm32"))]
        let prepared: Vec<(DocumentId, Value, Vec<u8>)> =
            if docs_with_ids.len() > 500 && !has_unique_indexes {
                use rayon::prelude::*;
                docs_with_ids
                    .into_par_iter()
                    .map(|(id, data)| {
                        // Propagate encode errors — defaulting to empty bytes
                        // persisted an undecodable document (acked, then lost).
                        let bytes = crate::codec::encode_doc(&data)?;
                        if keep_values {
                            Ok((id, data, bytes))
                        } else {
                            Ok((id, Value::Null, bytes))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                let mut prepared = Vec::with_capacity(docs_with_ids.len());
                for (id, data) in docs_with_ids {
                    let bytes = crate::codec::encode_doc(&data)?;
                    if keep_values {
                        prepared.push((id, data, bytes));
                    } else {
                        prepared.push((id, Value::Null, bytes));
                    }
                }
                prepared
            };
        #[cfg(target_arch = "wasm32")]
        let prepared: Vec<(DocumentId, Value, Vec<u8>)> = {
            let mut prepared = Vec::with_capacity(docs_with_ids.len());
            for (id, data) in docs_with_ids {
                let bytes = crate::codec::encode_doc(&data)?;
                if keep_values {
                    prepared.push((id, data, bytes));
                } else {
                    prepared.push((id, Value::Null, bytes));
                }
            }
            prepared
        };

        // Subscribers get the documents in their events (matching single
        // `insert`): snapshot them before `prepared` is consumed. `keep_values`
        // is forced on when `emit` is set, so the Values are real, not Null.
        let emit_docs: Option<Vec<Value>> = if emit {
            Some(prepared.iter().map(|(_, v, _)| v.clone()).collect())
        } else {
            None
        };

        // Phase 3: Insert pre-serialized docs (fast: no serialization)
        let ids = col.insert_many_prepared(prepared)?;

        // Emit change events if needed
        if let Some(docs) = emit_docs {
            for (&id, doc) in ids.iter().zip(docs) {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Insert,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: if doc.is_null() { None } else { Some(doc) },
                    tx_id: None,
                });
            }
        }

        Ok(ids)
    }

    pub fn find(&self, collection: &str, query: &Value) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.find(query)
    }

    pub fn find_with_options(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.find_with_options(query, opts)
    }

    pub fn find_with_options_arcs(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Result<Vec<Arc<Value>>> {
        let col = self.get_or_create_collection(collection)?;
        col.find_with_options_arcs(query, opts)
    }

    /// Explain-and-run a find: query plan (strategy, index, post-filter
    /// operators, sort mode) plus actual examined/returned/duration.
    pub fn explain_find(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Result<Value> {
        let col = self.get_or_create_collection(collection)?;
        col.explain_find(query, opts)
    }

    /// Explain-and-run a count: whether it was served index-only, plus
    /// the count and duration.
    pub fn explain_count(&self, collection: &str, query: &Value) -> Result<Value> {
        let col = self.get_or_create_collection(collection)?;
        col.explain_count(query)
    }

    /// Explain-and-run an aggregation: the stage list, the plan of the
    /// leading `$match` (the stage that decides whether the pipeline
    /// starts from an index or a full scan), and the run's
    /// returned-count/duration.
    pub fn explain_aggregate(&self, collection: &str, pipeline_json: &Value) -> Result<Value> {
        let stages: Vec<String> = pipeline_json
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_object())
                    .filter_map(|o| o.keys().next().cloned())
                    .collect()
            })
            .unwrap_or_default();

        // Plan of the leading $match — everything downstream operates on
        // its output, so it decides index usage for the whole pipeline.
        let first_match: Value = match pipeline_json
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|s| s.get("$match"))
        {
            Some(match_body) => {
                let col = self.get_or_create_collection(collection)?;
                let mut plan = col.explain_find(match_body, &FindOptions::default())?;
                // The inner run's numbers describe only the $match probe;
                // drop them to avoid confusion with the pipeline's run.
                if let Some(obj) = plan.as_object_mut() {
                    obj.remove("returned");
                    obj.remove("duration_ms");
                }
                plan
            }
            None => Value::Null,
        };

        // wasm32 has no usable monotonic clock — `Instant::now()` is not
        // supported there — so the plan reports its own run as 0 ms rather
        // than explain not existing at all on that target.
        #[cfg(not(target_arch = "wasm32"))]
        let start = Instant::now();
        let results = self.aggregate(collection, pipeline_json)?;
        #[cfg(not(target_arch = "wasm32"))]
        let duration_us = start.elapsed().as_micros() as u64;
        #[cfg(target_arch = "wasm32")]
        let duration_us = 0u64;

        Ok(json!({
            "stages": stages,
            "first_match": first_match,
            "returned": results.len(),
            "duration_ms": duration_us as f64 / 1000.0,
        }))
    }

    /// Look up a single doc's pre-encoded OxiWire bytes. Used by the
    /// server's find→wire path to skip per-doc encoding when the bytes
    /// cache (populated at insert time) is warm. Returns `None` if the
    /// collection doesn't exist or the id was never seen.
    pub fn get_oxiwire_bytes(
        &self,
        collection: &str,
        id: crate::document::DocumentId,
    ) -> Option<Arc<[u8]>> {
        let cols = self.collections.read();
        let col = cols.get(collection)?;
        col.load_doc_oxiwire_bytes(id)
    }

    /// Bytes-first find — the fast path that closes the JSONB→Value gap on
    /// the find→wire pipeline. Returns `Some(Ok(_))` when the query can be
    /// fully satisfied by an index (no post-filter, no sort/skip/limit);
    /// otherwise returns `None` so the caller falls back to the
    /// Value-based path. See `BTreeCollection::find_oxiwire_bytes`.
    pub fn find_oxiwire_bytes(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Option<Result<Vec<Arc<[u8]>>>> {
        let col = match self.get_or_create_collection(collection) {
            Ok(c) => c,
            Err(e) => return Some(Err(e)),
        };
        col.find_oxiwire_bytes(query, opts)
    }

    /// Low-memory post-filter find: encodes matching docs straight into one
    /// OxiWire buffer (no `Vec<Arc<Value>>`). See
    /// `BTreeCollection::find_oxiwire_postfilter`. `None` for sort/skip/limit.
    pub fn find_oxiwire_postfilter(
        &self,
        collection: &str,
        query: &Value,
        opts: &FindOptions,
    ) -> Option<Result<(usize, Vec<u8>)>> {
        let col = match self.get_or_create_collection(collection) {
            Ok(c) => c,
            Err(e) => return Some(Err(e)),
        };
        col.find_oxiwire_postfilter(query, opts)
    }

    pub fn find_one(&self, collection: &str, query: &Value) -> Result<Option<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.find_one(query)
    }

    pub fn update(&self, collection: &str, query: &Value, update: &Value) -> Result<u64> {
        self.update_with_upsert(collection, query, update, true, false)
            .map(|(_, n, _)| n)
    }

    /// The document an upsert would insert when nothing matches — the same
    /// synthesis the engine's upsert path runs, exposed so a surface can
    /// adjudicate its CREATE rule against the exact document before asking
    /// for the upsert. No `_id`/`_version`: those exist only once the
    /// insert happens.
    pub fn upsert_preview(query: &Value, update: &Value) -> Result<Value> {
        crate::btree_collection::BTreeCollection::synthesize_upsert_doc(query, update)
    }

    /// `update` with MongoDB-style upsert: when nothing matches and `upsert`
    /// is set, insert a document synthesized from the filter's equality
    /// conditions with the update applied ($setOnInsert included). Returns
    /// (matched count, modified count, upserted id).
    pub fn update_with_upsert(
        &self,
        collection: &str,
        query: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
    ) -> Result<(u64, u64, Option<DocumentId>)> {
        self.update_full(collection, query, update, multi, upsert, None)
    }

    /// `update_with_upsert` plus optional MongoDB `arrayFilters`.
    pub fn update_full(
        &self,
        collection: &str,
        query: &Value,
        update: &Value,
        multi: bool,
        upsert: bool,
        array_filters: Option<&Value>,
    ) -> Result<(u64, u64, Option<DocumentId>)> {
        let col = self.get_or_create_collection(collection)?;
        // Shared commit lock: direct writes run concurrently with each
        // other but never inside a transaction commit's validate→apply
        // window (which holds the write side) — otherwise the version this
        // bumps could be blindly overwritten by the commit's apply.
        let _occ_guard = self.commit_lock.read();
        let limit = if multi { None } else { Some(1) };
        let emit = self.change_broker.has_subscribers();
        let outcome =
            col.update_upsert_filtered_docs(query, update, limit, upsert, array_filters, emit)?;
        if emit {
            for (i, &id) in outcome.updated.iter().enumerate() {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Update,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: outcome.post_images.get(i).map(|a| (**a).clone()),
                    tx_id: None,
                });
            }
            if let Some(id) = outcome.upserted {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Insert,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: outcome.upserted_doc.as_ref().map(|a| (**a).clone()),
                    tx_id: None,
                });
            }
        }
        Ok((
            outcome.matched,
            outcome.updated.len() as u64,
            outcome.upserted,
        ))
    }

    pub fn update_one(&self, collection: &str, query: &Value, update: &Value) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        let _occ_guard = self.commit_lock.read(); // see update()
        let emit = self.change_broker.has_subscribers();
        let outcome = col.update_upsert_filtered_docs(query, update, Some(1), false, None, emit)?;
        if emit {
            for (i, &id) in outcome.updated.iter().enumerate() {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Update,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: outcome.post_images.get(i).map(|a| (**a).clone()),
                    tx_id: None,
                });
            }
        }
        Ok(outcome.updated.len() as u64)
    }

    /// Atomically find one document, apply the update, and return the
    /// modified document. The safe primitive for counters — see
    /// `BTreeCollection::find_and_modify`. Always immediate/atomic; it
    /// does not participate in an open transaction.
    /// WORM phase 2 — engine-level lock. Subsequent update / delete /
    /// find_and_modify on `doc_id` return `Error::DocumentWormLocked`
    /// until `worm_release` clears the lock OR `locked_until_micros`
    /// passes. `crate::worm::INDEFINITE` is the sentinel for
    /// "never time-expire". Idempotent on equal-value locks; refuses
    /// to LOWER an existing lock (operator intent: tighten only).
    pub fn worm_lock(&self, collection: &str, doc_id: u64, locked_until_micros: u64) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.worm_lock(doc_id, locked_until_micros)
    }

    /// WORM phase 2 — clear an engine-level lock. Admin-gated at the
    /// wire layer; this is the storage path.
    pub fn worm_release(&self, collection: &str, doc_id: u64) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.worm_release(doc_id)
    }

    /// `locked_until_micros` for the doc, or 0 if not locked.
    pub fn worm_locked_until(&self, collection: &str, doc_id: u64) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        Ok(col.worm_locked_until(doc_id).unwrap_or(0))
    }

    pub fn find_and_modify(
        &self,
        collection: &str,
        query: &Value,
        update: &Value,
    ) -> Result<Option<Value>> {
        let col = self.get_or_create_collection(collection)?;
        let _occ_guard = self.commit_lock.read(); // see update()
        let result = col.find_and_modify(query, update)?;
        if let Some(ref doc) = result
            && self.change_broker.has_subscribers()
        {
            let doc_id = doc.get("_id").and_then(|v| v.as_u64()).unwrap_or(0);
            self.change_broker.emit(ChangeEvent {
                token: 0,
                operation: OperationType::Update,
                collection: collection.to_string(),
                doc_id,
                document: Some(doc.clone()),
                tx_id: None,
            });
        }
        Ok(result)
    }

    pub fn delete(&self, collection: &str, query: &Value) -> Result<u64> {
        self.delete_limited(collection, query, None)
    }

    pub fn delete_one(&self, collection: &str, query: &Value) -> Result<u64> {
        self.delete_limited(collection, query, Some(1))
    }

    fn delete_limited(&self, collection: &str, query: &Value, limit: Option<usize>) -> Result<u64> {
        let col = self.get_or_create_collection(collection)?;
        let _occ_guard = self.commit_lock.read(); // see update()
        let emit = self.change_broker.has_subscribers();
        let (ids, pre_images) = col.delete_docs(query, limit, emit)?;
        let deleted = ids.len() as u64;
        if emit {
            // Delete events carry the pre-image (the deleted document) —
            // there is no post-image to carry, and a subscriber filtering
            // by document content needs to know what left the result set.
            let mut docs = pre_images.into_iter();
            for &id in &ids {
                self.change_broker.emit(ChangeEvent {
                    token: 0,
                    operation: OperationType::Delete,
                    collection: collection.to_string(),
                    doc_id: id,
                    document: docs.next(),
                    tx_id: None,
                });
            }
        }
        Ok(deleted)
    }

    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_index(field)
    }

    pub fn create_unique_index(&self, collection: &str, field: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_unique_index(field)
    }

    pub fn create_composite_index(&self, collection: &str, fields: Vec<String>) -> Result<String> {
        let col = self.get_or_create_collection(collection)?;
        col.create_composite_index(fields)
    }

    pub fn create_ttl_index(
        &self,
        collection: &str,
        field: &str,
        expire_after_seconds: u64,
    ) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_ttl_index(field, expire_after_seconds)
    }

    pub fn list_indexes(&self, collection: &str) -> Result<Vec<IndexInfo>> {
        let col = self.get_or_create_collection(collection)?;
        Ok(col.list_indexes())
    }

    pub fn drop_index(&self, collection: &str, index_name: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.drop_index(index_name)
    }

    pub fn count(&self, collection: &str, query: &Value) -> Result<usize> {
        let col = self.get_or_create_collection(collection)?;
        if query.as_object().is_some_and(|m| m.is_empty()) {
            Ok(col.count())
        } else {
            col.count_matching(query)
        }
    }

    pub fn compact(&self, collection: &str) -> Result<CompactStats> {
        let col = self.get_or_create_collection(collection)?;
        col.compact()
    }

    pub fn create_text_index(&self, collection: &str, fields: Vec<String>) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_text_index(fields)
    }

    /// Create a geospatial (geohash) index on a point field. Idempotent.
    pub fn create_geo_index(&self, collection: &str, field: &str) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_geo_index(field)
    }

    pub fn text_search(&self, collection: &str, query: &str, limit: usize) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.text_search(query, limit)
    }

    pub fn text_search_highlighted(
        &self,
        collection: &str,
        query: &str,
        limit: usize,
        snippet_chars: usize,
        max_snippets: usize,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.text_search_highlighted(query, limit, snippet_chars, max_snippets)
    }

    /// Set the GPU compute backend for accelerated vector search.
    #[cfg(feature = "gpu")]
    pub fn set_gpu(&self, gpu: Arc<crate::gpu::GpuCompute>) {
        *self.gpu.lock() = Some(gpu);
    }

    pub fn create_vector_index(
        &self,
        collection: &str,
        field: &str,
        dimension: usize,
        metric: crate::vector::DistanceMetric,
    ) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        col.create_vector_index(field, dimension, metric)
    }

    pub fn vector_search(
        &self,
        collection: &str,
        field: &str,
        query_vector: &[f32],
        limit: usize,
        ef_search: Option<usize>,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        col.vector_search(field, query_vector, limit, ef_search)
    }

    /// Aggregate with MVCC-lite snapshot consistency by default (ADR-0017):
    /// the pipeline's answer is the state of one commit instant, never a mix
    /// of documents read at different moments. Optimistic about it — the
    /// existing fast paths run against live state first, and only when a
    /// writer actually raced the pipeline does the snapshot-correct slow
    /// path re-run. Uncontended aggregations pay one atomic load.
    pub fn aggregate(&self, collection: &str, pipeline_json: &Value) -> Result<Vec<Value>> {
        #[cfg(target_arch = "wasm32")]
        {
            return self.aggregate_latest(collection, pipeline_json, None);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let has_out = Pipeline::parse(pipeline_json)?.out_collection().is_some();
            let s = self.begin_snapshot();
            let result = (|| {
                if has_out {
                    // $out WRITES its result; an optimistic run cannot be
                    // retried without writing twice. Straight to the
                    // snapshot-correct path.
                    return self.aggregate_snapshot_slow(collection, pipeline_json, s);
                }
                let touched = std::cell::RefCell::new(std::collections::HashSet::new());
                touched.borrow_mut().insert(collection.to_string());
                let fast = self.aggregate_latest(collection, pipeline_json, Some(&touched))?;
                let raced = touched
                    .borrow()
                    .iter()
                    .any(|c| self.snap_gate.changed_since(c, s));
                if !raced {
                    // Nothing was written to any involved collection while
                    // the pipeline ran: the live state WAS the snapshot.
                    return Ok(fast);
                }
                self.aggregate_snapshot_slow(collection, pipeline_json, s)
            })();
            self.end_snapshot(s);
            result
        }
    }

    fn aggregate_latest(
        &self,
        collection: &str,
        pipeline_json: &Value,
        touched: Option<&std::cell::RefCell<std::collections::HashSet<String>>>,
    ) -> Result<Vec<Value>> {
        let pipeline = Pipeline::parse(pipeline_json)?;
        let (leading_match, start_idx) = pipeline.take_leading_match();
        let out_collection = pipeline.out_collection().map(|s| s.to_string());

        let lookup_fn = |foreign: &str, query: &Value| -> Result<Vec<Value>> {
            if let Some(t) = touched {
                t.borrow_mut().insert(foreign.to_string());
            }
            self.find(foreign, query)
        };

        // Streaming fast path: when pipeline is [$match?] -> $group -> [rest],
        // stream docs through storage sequentially instead of materializing
        // the full Vec<Arc<Value>>. This is 5-10x faster for large collections.
        let result = if let Some((group_key, accumulators, next_idx)) =
            pipeline.try_streaming_group(start_idx)
        {
            let col = self.get_or_create_collection(collection)?;

            // Index-only count: if group key has an index and all accumulators
            // are count-only, read counts directly from the index (zero I/O).
            let fi = col.field_indexes();
            if let Some(index_result) = crate::pipeline::try_index_only_count(
                group_key,
                accumulators,
                &fi,
                col.count(),
                leading_match,
            ) {
                pipeline.execute_from(next_idx, index_result, &lookup_fn)?
            } else if let Some(covered) = {
                // Covered composite-index aggregation: $sum/$avg/$min/$max
                // over fields materialized in a composite index — answered
                // without touching any document (matters most in disk-first,
                // where each doc read is an mmap access + decode). A leading
                // single-equality $match rides the composite prefix; the
                // match field's single-field index cross-checks the count.
                let ci = col.composite_indexes();
                crate::pipeline::try_composite_covered_group(
                    group_key,
                    accumulators,
                    &ci,
                    col.count(),
                    leading_match,
                    Some(&fi),
                )
            } {
                drop(fi);
                pipeline.execute_from(next_idx, covered, &lookup_fn)?
            } else {
                drop(fi);
                let group_result =
                    col.aggregate_streaming(leading_match, group_key, accumulators)?;
                // Continue with remaining pipeline stages on the small group result
                pipeline.execute_from(next_idx, group_result, &lookup_fn)?
            }
        } else {
            let query = match leading_match {
                Some(q) => q.clone(),
                None => json!({}),
            };

            // Standard path: use Arc-based pipeline to avoid cloning all initial docs.
            // This is critical for aggregation over large datasets (200K+ docs).
            let col = self.get_or_create_collection(collection)?;
            let arcs = col.find_arcs(&query)?;
            let doc_lookup = |id: DocumentId| col.load_doc_arc(id);
            let fi = col.field_indexes();
            // `arcs` is the full collection only when there was no leading
            // $match — the flag gates the index-accelerated $group, which
            // reads groups straight from the index.
            pipeline.execute_from_arcs(
                start_idx,
                arcs,
                &lookup_fn,
                Some(&fi),
                Some(&doc_lookup),
                leading_match.is_none(),
            )?
        };

        // Handle $out: write results to the target collection
        if let Some(target) = out_collection {
            let target_col = self.get_or_create_collection(&target)?;
            for doc in &result {
                // Strip _id and _version so the target collection assigns new ones
                let mut clean = doc.clone();
                if let Some(obj) = clean.as_object_mut() {
                    obj.remove("_id");
                    obj.remove("_version");
                }
                target_col.insert(clean)?;
            }
        }

        Ok(result)
    }

    // ── MVCC-lite read snapshots (ADR-0017) ────────────────────────────

    /// Open a read snapshot pinned to the current commit instant. The write
    /// side of `commit_lock` excludes every writer's WAL-append→apply→record
    /// window, so the returned S cleanly splits "visible" from "later".
    /// Snapshots expire after `OXIDB_SNAPSHOT_MAX_SECS` (default 300) —
    /// reads through a dead snapshot fail, writers never wait on one.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn begin_snapshot(&self) -> u64 {
        let _boundary = self.commit_lock.write();
        self.snap_gate.begin()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn end_snapshot(&self, s: u64) {
        self.snap_gate.end(s);
    }

    /// `find` against the state at snapshot `s`. Candidates come from the
    /// live index paths PLUS everything the gate remembered since `s` (a
    /// document that matched at `s` but was updated away since is only
    /// findable through the gate); every candidate is resolved to its state
    /// at `s` and the query re-applied to THAT value.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn snapshot_find(&self, s: u64, collection: &str, query: &Value) -> Result<Vec<Value>> {
        self.snap_gate.check(s)?;
        let col = self.get_or_create_collection(collection)?;
        let parsed = crate::query::parse_query(query)?;

        let mut out: Vec<Value> = Vec::new();
        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let consider = |doc: std::sync::Arc<Value>,
                        out: &mut Vec<Value>,
                        seen: &mut std::collections::HashSet<u64>|
         -> Result<()> {
            let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) else {
                return Ok(());
            };
            if !seen.insert(id) {
                return Ok(());
            }
            let resolved: Option<Value> = match self.snap_gate.resolve(collection, id, s) {
                crate::snapshot::Resolved::Current => Some((*doc).clone()),
                crate::snapshot::Resolved::Prior(Some(bytes)) => {
                    Some(crate::codec::decode_doc(&bytes)?)
                }
                crate::snapshot::Resolved::Prior(None) => None, // born after s
            };
            if let Some(v) = resolved
                && crate::query::matches_value(&parsed, &v)
            {
                out.push(v);
            }
            Ok(())
        };

        for doc in col.find_arcs(query)? {
            consider(doc, &mut out, &mut seen)?;
        }
        for id in self.snap_gate.remembered_ids(collection) {
            if seen.contains(&id) {
                continue;
            }
            if let Some(doc) = col.load_doc_arc(id) {
                consider(doc, &mut out, &mut seen)?;
            } else if let crate::snapshot::Resolved::Prior(Some(bytes)) =
                self.snap_gate.resolve(collection, id, s)
            {
                // Deleted after `s`: resurrect and match.
                seen.insert(id);
                let v = crate::codec::decode_doc(&bytes)?;
                if crate::query::matches_value(&parsed, &v) {
                    out.push(v);
                }
            }
        }
        Ok(out)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn snapshot_count(&self, s: u64, collection: &str, query: &Value) -> Result<u64> {
        Ok(self.snapshot_find(s, collection, query)?.len() as u64)
    }

    /// Aggregate against an explicit snapshot — the always-correct path:
    /// full snapshot doc set, leading $match applied to the RESOLVED state,
    /// `$lookup` reads through the same snapshot.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn snapshot_aggregate(
        &self,
        s: u64,
        collection: &str,
        pipeline_json: &Value,
    ) -> Result<Vec<Value>> {
        self.snap_gate.check(s)?;
        self.aggregate_snapshot_slow(collection, pipeline_json, s)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn aggregate_snapshot_slow(
        &self,
        collection: &str,
        pipeline_json: &Value,
        s: u64,
    ) -> Result<Vec<Value>> {
        let pipeline = Pipeline::parse(pipeline_json)?;
        let (leading_match, start_idx) = pipeline.take_leading_match();
        let out_collection = pipeline.out_collection().map(|t| t.to_string());
        let col = self.get_or_create_collection(collection)?;

        let all = crate::snapshot::snapshot_docs(
            &self.snap_gate,
            collection,
            s,
            col.find_arcs(&json!({}))?,
        )?;
        let docs: Vec<Value> = match leading_match {
            Some(q) => {
                let parsed = crate::query::parse_query(q)?;
                all.into_iter()
                    .filter(|d| crate::query::matches_value(&parsed, d))
                    .map(|a| (*a).clone())
                    .collect()
            }
            None => all.into_iter().map(|a| (*a).clone()).collect(),
        };

        let lookup_fn = |foreign: &str, query: &Value| -> Result<Vec<Value>> {
            self.snapshot_find(s, foreign, query)
        };
        let result = pipeline.execute_from(start_idx, docs, &lookup_fn)?;

        if let Some(target) = out_collection {
            let target_col = self.get_or_create_collection(&target)?;
            for doc in &result {
                let mut clean = doc.clone();
                if let Some(obj) = clean.as_object_mut() {
                    obj.remove("_id");
                    obj.remove("_version");
                }
                target_col.insert(clean)?;
            }
        }
        Ok(result)
    }

    /// Run an aggregation `pipeline` over an explicitly-supplied set of
    /// documents instead of a stored collection.
    ///
    /// This is the merge half of cross-shard scatter-gather aggregation: a
    /// proxy (OxiPool) collects each shard's partial results, concatenates
    /// them, and calls this to run the merge pipeline over the partials using
    /// the real executor — so the merge has identical semantics to a
    /// single-node run, with no logic duplicated in the proxy. `$lookup`
    /// inside the pipeline resolves against this engine's collections.
    pub fn aggregate_docs(&self, pipeline_json: &Value, docs: Vec<Value>) -> Result<Vec<Value>> {
        let pipeline = Pipeline::parse(pipeline_json)?;
        let lookup_fn =
            |foreign: &str, query: &Value| -> Result<Vec<Value>> { self.find(foreign, query) };
        pipeline.execute_from(0, docs, &lookup_fn)
    }

    /// Resident memory breakdown for `collection` (primary store + indexes),
    /// computed from the live structures — independent of process RSS. Returns
    /// `None` if the collection is not currently open. Intended for
    /// introspection / capacity planning.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn memory_report(
        &self,
        collection: &str,
    ) -> Option<crate::btree_collection::CollectionMemory> {
        let cols = self.collections.read();
        cols.get(collection).map(|c| c.memory_report())
    }

    // -----------------------------------------------------------------------
    // Transaction methods
    // -----------------------------------------------------------------------

    /// Begin a new transaction. Returns the transaction ID.
    pub fn begin_transaction(&self) -> TransactionId {
        // Every begin doubles as a sweep, so abandoned transactions die
        // even in embedded use where no TTL thread runs.
        #[cfg(not(target_arch = "wasm32"))]
        self.expire_stale_transactions();
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let tx = Transaction::new(tx_id);
        self.active_transactions
            .write()
            .insert(tx_id, Mutex::new(tx));
        tx_id
    }

    /// Set the interactive-transaction idle timeout in milliseconds
    /// (`0` = never expire). Programmatic override of
    /// `OXIDB_TX_MAX_IDLE_SECS` for embedded callers and tests.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_tx_max_idle_ms(&self, ms: u64) {
        self.tx_max_idle_ms.store(ms, Ordering::Relaxed);
    }

    /// Look up an active transaction and run `f` on it, enforcing the
    /// idle timeout: an expired transaction is rolled back right here
    /// (state removed, `find_for_update` locks released) and the caller
    /// gets [`Error::TransactionExpired`] — never a silent continuation
    /// on state the sweeper is about to drop. A live transaction gets its
    /// idle clock reset.
    fn with_tx<T>(&self, tx_id: TransactionId, f: impl FnOnce(&mut Transaction) -> T) -> Result<T> {
        let txs = self.active_transactions.read();
        let Some(tx_mutex) = txs.get(&tx_id) else {
            drop(txs);
            return Err(self.tx_gone(tx_id));
        };
        let mut tx = tx_mutex.lock();
        #[cfg(not(target_arch = "wasm32"))]
        {
            if self.tx_idle_expired(&tx) {
                drop(tx);
                drop(txs);
                self.expire_transaction(tx_id);
                return Err(Error::TransactionExpired(tx_id));
            }
            tx.last_active = std::time::Instant::now();
        }
        Ok(f(&mut tx))
    }

    /// The error for a transaction id that is not in the active set:
    /// `TransactionExpired` if the engine timed it out, else the generic
    /// `TransactionNotFound`.
    #[cfg(not(target_arch = "wasm32"))]
    fn tx_gone(&self, tx_id: TransactionId) -> Error {
        if self.expired_txs.lock().contains(&tx_id) {
            Error::TransactionExpired(tx_id)
        } else {
            Error::TransactionNotFound(tx_id)
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn tx_gone(&self, tx_id: TransactionId) -> Error {
        Error::TransactionNotFound(tx_id)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn tx_idle_expired(&self, tx: &Transaction) -> bool {
        let ms = self.tx_max_idle_ms.load(Ordering::Relaxed);
        ms > 0 && tx.last_active.elapsed() >= std::time::Duration::from_millis(ms)
    }

    /// Remember an expired id (bounded) for accurate late-caller errors.
    #[cfg(not(target_arch = "wasm32"))]
    fn remember_expired(&self, tx_id: TransactionId) {
        let mut expired = self.expired_txs.lock();
        expired.insert(tx_id);
        while expired.len() > EXPIRED_TX_REMEMBERED {
            let oldest = *expired.iter().next().unwrap();
            expired.remove(&oldest);
        }
    }

    /// Roll back an expired transaction: drop its buffered state and
    /// release its pessimistic doc locks.
    #[cfg(not(target_arch = "wasm32"))]
    fn expire_transaction(&self, tx_id: TransactionId) {
        if self.active_transactions.write().remove(&tx_id).is_none() {
            return; // raced a commit, rollback, or another expirer
        }
        self.doc_locks.release_all(tx_id);
        self.remember_expired(tx_id);
        if self.verbose {
            eprintln!(
                "transaction {tx_id} expired (idle past OXIDB_TX_MAX_IDLE_SECS); rolled back"
            );
        }
    }

    /// Expire every transaction past the idle timeout. Called on each
    /// `begin_transaction` and periodically by the TTL thread, so an
    /// abandoned transaction dies — and frees its `find_for_update`
    /// locks — even if its client never sends another request.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn expire_stale_transactions(&self) {
        if self.tx_max_idle_ms.load(Ordering::Relaxed) == 0 {
            return;
        }
        let stale: Vec<TransactionId> = {
            let txs = self.active_transactions.read();
            if txs.is_empty() {
                return;
            }
            txs.iter()
                .filter_map(|(id, m)| {
                    // try_lock: a held mutex means an operation is running
                    // on the transaction right now — the opposite of idle.
                    let tx = m.try_lock()?;
                    self.tx_idle_expired(&tx).then_some(*id)
                })
                .collect()
        };
        for id in stale {
            self.expire_transaction(id);
        }
    }

    /// Extract buffered write ops from a transaction (for Raft replication).
    /// Removes the transaction from the active set.
    pub fn extract_transaction_writes(&self, tx_id: TransactionId) -> Result<Vec<WriteOp>> {
        let mut txs = self.active_transactions.write();
        let tx_mutex = txs.remove(&tx_id).ok_or_else(|| self.tx_gone(tx_id))?;
        drop(txs);
        let tx = tx_mutex.into_inner();
        #[cfg(not(target_arch = "wasm32"))]
        if self.tx_idle_expired(&tx) {
            self.doc_locks.release_all(tx_id);
            self.remember_expired(tx_id);
            return Err(Error::TransactionExpired(tx_id));
        }
        Ok(tx.write_ops)
    }

    /// Buffer an insert within a transaction. Returns the document id
    /// that will be assigned at commit time. Pre-allocating the id
    /// here (rather than at commit) lets the caller weave the id into
    /// sibling writes inside the same tx — e.g. inserting a parent doc
    /// and a version row referencing it. If the tx rolls back the id
    /// becomes a gap; that's harmless since ids don't need to be
    /// contiguous.
    pub fn tx_insert(
        &self,
        tx_id: TransactionId,
        collection: &str,
        doc: Value,
    ) -> Result<DocumentId> {
        // Reserve an id from the target collection's monotonic counter.
        // get_or_create_collection is the same path the commit path
        // uses, so we don't open a second collection later under a
        // different name.
        let col = self.get_or_create_collection(collection)?;
        let id = col.reserve_ids(1);

        self.with_tx(tx_id, |tx| {
            tx.collections_involved.insert(collection.to_string());
            tx.write_ops.push(WriteOp::Insert {
                collection: collection.to_string(),
                data: doc,
                id: Some(id),
            });
        })?;
        Ok(id)
    }

    /// Execute a read within a transaction, recording versions for OCC.
    pub fn tx_find(
        &self,
        tx_id: TransactionId,
        collection: &str,
        query: &Value,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;
        let results = col.find(query)?;

        // Record read versions
        self.with_tx(tx_id, |tx| {
            tx.collections_involved.insert(collection.to_string());
            for doc in &results {
                if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let version = col.get_version(doc_id);
                    tx.read_set.push(ReadRecord {
                        collection: collection.to_string(),
                        doc_id,
                        version,
                    });
                }
            }
        })?;

        Ok(results)
    }

    /// `tx_find` with pessimistic per-document write locks — the engine's
    /// `SELECT ... FOR UPDATE`. Locks every matched document (in doc-id
    /// order within this call) before reading it, so between this read
    /// and the commit no other `for_update` transaction can slip a write
    /// in — on hot documents this replaces the OCC retry storm with
    /// orderly queueing. Locks are released at commit (as soon as the
    /// writes are applied) or rollback; waiting for a busy document gives
    /// up with [`Error::LockTimeout`] after `lock_timeout`.
    ///
    /// The locks only exclude other `for_update` callers; plain writers
    /// bypass them, and OCC validation at commit remains the correctness
    /// backstop. Callers locking documents across several calls should
    /// order those calls consistently to avoid deadlock-by-timeout.
    pub fn tx_find_for_update(
        &self,
        tx_id: TransactionId,
        collection: &str,
        query: &Value,
        lock_timeout: std::time::Duration,
    ) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection(collection)?;

        // Match first (unlocked), then lock in sorted-id order.
        let matching = col.find(query)?;
        let mut ids: Vec<u64> = matching
            .iter()
            .filter_map(|d| d.get("_id").and_then(|v| v.as_u64()))
            .collect();
        ids.sort_unstable();
        for id in &ids {
            self.doc_locks.lock(collection, *id, tx_id, lock_timeout)?;
        }

        // Re-read AFTER acquiring the locks: the matched docs may have
        // changed while we waited, and the version recorded in the read
        // set must be the locked one for OCC validation to pass. Direct
        // by-id lookup (cache-backed) — a `find` on `_id` would be a
        // full collection scan per document.
        let mut results = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(doc) = col.load_doc_arc(*id) {
                results.push((*doc).clone());
            }
        }

        // On any failure (expired or unknown tx) the per-document locks
        // taken above must not stay parked under a dead transaction id.
        if let Err(e) = self.with_tx(tx_id, |tx| {
            tx.collections_involved.insert(collection.to_string());
            for doc in &results {
                if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let version = col.get_version(doc_id);
                    tx.read_set.push(ReadRecord {
                        collection: collection.to_string(),
                        doc_id,
                        version,
                    });
                }
            }
        }) {
            self.doc_locks.release_all(tx_id);
            return Err(e);
        }

        Ok(results)
    }

    /// Buffer an update within a transaction, recording read versions.
    pub fn tx_update(
        &self,
        tx_id: TransactionId,
        collection: &str,
        query: &Value,
        update: &Value,
    ) -> Result<()> {
        // Read to find matching docs and record their versions
        let col = self.get_or_create_collection(collection)?;
        let matching = col.find(query)?;

        self.with_tx(tx_id, |tx| {
            tx.collections_involved.insert(collection.to_string());
            for doc in &matching {
                if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let version = col.get_version(doc_id);
                    tx.read_set.push(ReadRecord {
                        collection: collection.to_string(),
                        doc_id,
                        version,
                    });
                }
            }
            tx.write_ops.push(WriteOp::Update {
                collection: collection.to_string(),
                query: query.clone(),
                update: update.clone(),
            });
        })
    }

    /// Buffer a delete within a transaction, recording read versions.
    pub fn tx_delete(&self, tx_id: TransactionId, collection: &str, query: &Value) -> Result<()> {
        let col = self.get_or_create_collection(collection)?;
        let matching = col.find(query)?;

        self.with_tx(tx_id, |tx| {
            tx.collections_involved.insert(collection.to_string());
            for doc in &matching {
                if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let version = col.get_version(doc_id);
                    tx.read_set.push(ReadRecord {
                        collection: collection.to_string(),
                        doc_id,
                        version,
                    });
                }
            }
            tx.write_ops.push(WriteOp::Delete {
                collection: collection.to_string(),
                query: query.clone(),
            });
        })
    }

    /// Commit a transaction using OCC validation.
    ///
    /// Two-phase group commit. Phase 1 (validate → WAL append → apply)
    /// runs under `commit_lock`; phase 2 (WAL fsync + commit-log mark)
    /// runs OUTSIDE it, so concurrent committers overlap their phase 1
    /// with an in-flight fsync and share the next one — throughput is no
    /// longer one-fsync-per-commit. The ack still comes only after both
    /// fsyncs: durability semantics are unchanged. The one observable
    /// trade: between apply and ack, other connections can already read
    /// this transaction's writes; a crash inside that window rolls them
    /// back on recovery (the tx was never acked).
    pub fn commit_transaction(&self, tx_id: TransactionId) -> Result<()> {
        let res = self.commit_transaction_inner(tx_id);
        // Every exit — success, conflict, IO error — must release the
        // pessimistic doc locks. The happy path already released them
        // right after apply; this is the backstop for error returns.
        self.doc_locks.release_all(tx_id);
        res
    }

    fn commit_transaction_inner(&self, tx_id: TransactionId) -> Result<()> {
        // 1. Remove transaction from active set
        let tx = {
            let mut txs = self.active_transactions.write();
            txs.remove(&tx_id).ok_or_else(|| self.tx_gone(tx_id))?
        };
        let tx = tx.into_inner();

        // An expired transaction must not commit: its owner was told (or
        // would have been told) it was rolled back, and honoring a commit
        // that raced the sweeper would make "expired" mean "maybe".
        #[cfg(not(target_arch = "wasm32"))]
        if self.tx_idle_expired(&tx) {
            self.remember_expired(tx_id);
            return Err(Error::TransactionExpired(tx_id));
        }

        // 2. Resolve all involved collections
        let mut locked_collections: Vec<(String, Arc<BTreeCollection>)> = Vec::new();
        for col_name in &tx.collections_involved {
            let col = self.get_or_create_collection(col_name)?;
            locked_collections.push((col_name.clone(), col));
        }

        // ---- PHASE 1: validate → WAL append (no fsync) → apply, under the
        // commit lock. Serialized against every other committer AND against
        // direct (non-transactional) writes, which hold the read side —
        // version validation and the corresponding apply are atomic with
        // respect to every other writer. Everything in here is memory plus
        // buffered file appends; the fsyncs happen in phase 2 OUTSIDE the
        // lock, so other commits run their phase 1 while we flush and the
        // next flush covers them all (group commit).
        let (my_ticket, wal_cols, pending_events, apply_result, deferred) = {
            let _commit_guard = self.commit_lock.write();

            // 3. OCC validation: verify all recorded versions match current versions
            for record in &tx.read_set {
                if let Some((_, col)) = locked_collections
                    .iter()
                    .find(|(n, _)| n == &record.collection)
                {
                    let current_version = col.get_version(record.doc_id);
                    if current_version != record.version {
                        return Err(Error::TransactionConflict {
                            collection: record.collection.clone(),
                            doc_id: record.doc_id,
                            expected_version: record.version,
                            actual_version: current_version,
                        });
                    }
                }
            }

            // Build a name→Arc map for quick lookup
            let col_map: HashMap<String, Arc<BTreeCollection>> = locked_collections
                .iter()
                .map(|(n, c)| (n.clone(), Arc::clone(c)))
                .collect();

            // 4. Prepare: execute each WriteOp against the collection
            //    Collect WAL entries and mutations per collection.
            //
            //    `staged` carries each collection's in-transaction doc
            //    state so successive ops on the same document compose
            //    (read-your-own-writes). Without it, two updates to one
            //    doc in a transaction each recomputed from the committed
            //    base and the last write silently clobbered the first —
            //    e.g. `$inc -2` then `$inc +1` yielded +1, not -1,
            //    creating money in a ledger. (The benches never hit this
            //    because they force distinct from/to accounts.)
            let mut all_mutations: HashMap<String, Vec<crate::collection::PreparedMutation>> =
                HashMap::new();
            let mut staged: HashMap<String, HashMap<DocumentId, Value>> = HashMap::new();

            for op in tx.write_ops {
                match op {
                    WriteOp::Insert {
                        collection,
                        data,
                        id,
                    } => {
                        let col = col_map.get(&collection).unwrap();
                        let mutation = col.prepare_tx_insert(data, tx_id, id)?;
                        staged
                            .entry(collection.clone())
                            .or_default()
                            .insert(mutation.doc_id, mutation.new_data.clone());
                        all_mutations.entry(collection).or_default().push(mutation);
                    }
                    WriteOp::Update {
                        collection,
                        query,
                        update,
                    } => {
                        let col = col_map.get(&collection).unwrap();
                        let stage = staged.entry(collection.clone()).or_default();
                        let mutations = col.prepare_tx_update(&query, &update, tx_id, stage)?;
                        all_mutations
                            .entry(collection)
                            .or_default()
                            .extend(mutations);
                    }
                    WriteOp::Delete { collection, query } => {
                        let col = col_map.get(&collection).unwrap();
                        let mutations = col.prepare_tx_delete(&query, tx_id)?;
                        // Deleted docs leave the staged view so a later
                        // update in the same tx can't resurrect them.
                        if let Some(stage) = staged.get_mut(&collection) {
                            for m in &mutations {
                                stage.remove(&m.doc_id);
                            }
                        }
                        all_mutations
                            .entry(collection)
                            .or_default()
                            .extend(mutations);
                    }
                }
            }

            // 5. WAL append — NO fsync here. Failing mid-way is safe: the
            // appended entries belong to a tx that will never be marked
            // committed, so replay discards them.
            for (col_name, mutations) in &all_mutations {
                let col = col_map.get(col_name).unwrap();
                let entries: Vec<crate::wal::WalEntry> = mutations
                    .iter()
                    .map(|m| match &m.wal_entry {
                        crate::wal::WalEntry::Insert {
                            doc_id,
                            doc_bytes,
                            tx_id,
                        } => crate::wal::WalEntry::Insert {
                            doc_id: *doc_id,
                            doc_bytes: doc_bytes.clone(),
                            tx_id: *tx_id,
                        },
                        crate::wal::WalEntry::Update {
                            doc_id,
                            doc_bytes,
                            tx_id,
                        } => crate::wal::WalEntry::Update {
                            doc_id: *doc_id,
                            doc_bytes: doc_bytes.clone(),
                            tx_id: *tx_id,
                        },
                        crate::wal::WalEntry::Delete { doc_id, tx_id } => {
                            crate::wal::WalEntry::Delete {
                                doc_id: *doc_id,
                                tx_id: *tx_id,
                            }
                        }
                    })
                    .collect();
                col.log_wal_batch_no_sync(&entries)?;
            }

            // The collections whose WALs phase 2 must make durable.
            let wal_cols: Vec<Arc<BTreeCollection>> = all_mutations
                .keys()
                .map(|n| Arc::clone(col_map.get(n).unwrap()))
                .collect();

            // 6. Collect event data before consuming mutations
            let emit = self.change_broker.has_subscribers();
            let pending_events: Vec<ChangeEvent> = if emit {
                all_mutations
                    .iter()
                    .flat_map(|(col_name, mutations)| {
                        mutations.iter().map(move |m| {
                            if m.is_delete {
                                ChangeEvent {
                                    token: 0,
                                    operation: OperationType::Delete,
                                    collection: col_name.clone(),
                                    doc_id: m.doc_id,
                                    document: m.old_data.clone(),
                                    tx_id: Some(tx_id),
                                }
                            } else if m.old_data.is_some() || m.old_loc.is_some() {
                                // A mutation of an existing document is an
                                // update. `old_loc` is always None in B-tree
                                // mode, so keying on it alone (as before
                                // 0.42.9) reported every tx update as an
                                // Insert event.
                                ChangeEvent {
                                    token: 0,
                                    operation: OperationType::Update,
                                    collection: col_name.clone(),
                                    doc_id: m.doc_id,
                                    document: Some(m.new_data.clone()),
                                    tx_id: Some(tx_id),
                                }
                            } else {
                                ChangeEvent {
                                    token: 0,
                                    operation: OperationType::Insert,
                                    collection: col_name.clone(),
                                    doc_id: m.doc_id,
                                    document: Some(m.new_data.clone()),
                                    tx_id: Some(tx_id),
                                }
                            }
                        })
                    })
                    .collect()
            } else {
                Vec::new()
            };

            // 7. Apply: for each collection, apply mutations to storage.
            // On error we still run phase 2: the WAL entries are complete,
            // so marking the tx committed lets replay-on-restart heal the
            // in-memory state — the same semantics as the old ordering,
            // where an apply error happened after the commit point.
            let mut apply_result: Result<()> = Ok(());
            // What each collection deferred until this transaction's commit
            // point: records written pending, records it displaced. Settled in
            // phase 2 once the mark is durable — see `apply_prepared_pending`.
            #[cfg(not(target_arch = "wasm32"))]
            let mut deferred: Vec<(
                Arc<BTreeCollection>,
                crate::btree_storage::PendingOps,
            )> = Vec::with_capacity(all_mutations.len());
            'apply: for (col_name, mut mutations) in all_mutations {
                let col = col_map.get(&col_name).unwrap();
                #[cfg(not(target_arch = "wasm32"))]
                let mut ops = crate::btree_storage::PendingOps::default();
                #[cfg(not(target_arch = "wasm32"))]
                let applied = col.apply_prepared_pending(&mut mutations, &mut ops);
                #[cfg(target_arch = "wasm32")]
                let applied = col.apply_prepared(&mut mutations);
                // Always carried, even when empty: the count it settles is
                // what keeps a snapshot from capturing this transaction before
                // it commits, which is the in-RAM mode's version of the same
                // problem.
                #[cfg(not(target_arch = "wasm32"))]
                deferred.push((Arc::clone(col), ops));
                if let Err(e) = applied {
                    apply_result = Err(e);
                    break 'apply;
                }
            }

            // 8. Mark collections dirty for the background persister.
            for (col_name, _) in &locked_collections {
                let col = col_map.get(col_name).unwrap();
                col.checkpoint_wal()?;
            }

            // 9. Ticket — our writes are now visible, so take our slot in
            // the durability order. From this point the mark turnstile in
            // phase 2 MUST be traversed on every path, or later commits
            // wait on our turn forever.
            let my_ticket = self.commit_ticket.fetch_add(1, Ordering::SeqCst);

            #[cfg(not(target_arch = "wasm32"))]
            let carried = deferred;
            #[cfg(target_arch = "wasm32")]
            let carried: Vec<()> = Vec::new();
            (my_ticket, wal_cols, pending_events, apply_result, carried)
        };

        // Writes are applied and versioned — the pessimistic doc locks have
        // done their job. Release BEFORE the fsync wait so the next
        // transaction on the same hot documents runs its phase 1 while we
        // flush; that overlap is exactly what batches hot-doc commits.
        self.doc_locks.release_all(tx_id);

        // ---- PHASE 2: durability, outside the lock.

        // a) Group-fsync each written collection's WAL. Concurrent
        // committers share the physical fsync (Wal::sync_shared).
        let mut wal_result: Result<()> = Ok(());
        for col in &wal_cols {
            if let Err(e) = col.sync_wal_shared() {
                wal_result = Err(e);
                break;
            }
        }

        // b) Submit the commit mark in ticket order. Ordered submission
        // guarantees a commit can only become durable together with (or
        // after) every commit whose applied writes it may have read — the
        // tx_log committer fsyncs batches in submission order. The
        // turnstile must advance even when the WAL fsync failed (in which
        // case we DON'T mark: an unmarked tx is discarded on replay).
        #[cfg(not(target_arch = "wasm32"))]
        let mark_rx = {
            let mut turn = self.mark_turn.lock().unwrap_or_else(|e| e.into_inner());
            while *turn != my_ticket {
                turn = self.mark_cv.wait(turn).unwrap_or_else(|e| e.into_inner());
            }
            let rx = if wal_result.is_ok() {
                Some(self.tx_log.mark_committed_async(tx_id))
            } else {
                None
            };
            *turn += 1;
            drop(turn);
            self.mark_cv.notify_all();
            rx
        };
        #[cfg(target_arch = "wasm32")]
        let _ = my_ticket;

        if let Err(e) = wal_result {
            // The WAL fsync failed: this transaction's phase-1 in-memory
            // apply is a rejected write that must never become durable.
            // Poison durability so no checkpoint persists the untrusted
            // state; recovery rebuilds from the last durable snapshot +
            // marked-WAL replay (this tx was never marked). See the field.
            //
            // Its file work is abandoned rather than performed: the records it
            // wrote stay pending, so no rebuild will see them.
            #[cfg(not(target_arch = "wasm32"))]
            for (col, ops) in &deferred {
                col.discard_pending(ops);
            }
            self.durability_poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }

        // c) COMMIT POINT: wait for the tx_log batch fsync that covers our
        // mark. Many commits wait here concurrently on the same batch.
        #[cfg(not(target_arch = "wasm32"))]
        {
            let rx = match mark_rx.expect("mark submitted when WAL sync succeeded") {
                Ok(rx) => rx,
                Err(e) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    for (col, ops) in &deferred {
                        col.discard_pending(ops);
                    }
                    return Err(e);
                }
            };
            match rx.recv() {
                Ok(Ok(())) => {}
                // Never reached its commit point: leave the file as if it never
                // happened.
                Ok(Err(e)) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    for (col, ops) in &deferred {
                        col.discard_pending(ops);
                    }
                    return Err(e);
                }
                Err(_) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    for (col, ops) in &deferred {
                        col.discard_pending(ops);
                    }
                    return Err(Error::Io(std::io::Error::other(
                        "tx commit-log writer thread is gone",
                    )));
                }
            }
        }

        // d) The transaction is durable now, so its deferred file work can be
        // done: publish the records it wrote, retire the ones it displaced.
        // Before the commit point either act would have survived a crash that
        // this transaction did not, which is how a transaction spanning two
        // collections came back half-applied.
        //
        // Not fsynced, and not fatal if it does not happen: a lost flip is
        // rebuilt by WAL replay, which is gated on the commit log this
        // transaction is now in.
        #[cfg(not(target_arch = "wasm32"))]
        for (col, ops) in &deferred {
            col.settle_pending(ops);
        }

        // The tx_id intentionally STAYS in the commit log here. WAL
        // replay skips transactional entries whose id is absent from the
        // log, and this transaction's WAL entries outlive this function
        // (the WAL is only truncated at checkpoint). Removing the id now
        // would make a crash-before-snapshot-persist silently drop the
        // committed writes on recovery. The background sync thread prunes
        // the log after each snapshot persist; shutdown clears it.

        // Surface a deferred apply error only after durability settled —
        // the on-disk state is committed either way (see step 7).
        apply_result?;

        // Emit change events after the commit is durable.
        for event in pending_events {
            self.change_broker.emit(event);
        }

        Ok(())
    }

    /// Rollback a transaction, discarding all buffered operations.
    pub fn rollback_transaction(&self, tx_id: TransactionId) -> Result<()> {
        let mut txs = self.active_transactions.write();
        txs.remove(&tx_id);
        drop(txs);
        self.doc_locks.release_all(tx_id);
        Ok(())
    }

    /// Wait until every assigned commit ticket has passed the mark
    /// turnstile in `commit_transaction` phase 2. Callers hold
    /// `commit_lock` (or run at shutdown with writers quiesced), so no
    /// new tickets appear while waiting. Afterwards `tx_log`'s committed
    /// set — whose reads are answered post-fsync — reflects every
    /// applied transaction. Required before persisting snapshots: a
    /// snapshot may contain applied writes, and their commit marks must
    /// be durable first, or a crash could leave a multi-collection
    /// transaction half-persisted with no commit-log record to finish
    /// or discard it.
    #[cfg(not(target_arch = "wasm32"))]
    fn wait_marks_settled(&self) {
        let target = self.commit_ticket.load(Ordering::SeqCst);
        let mut turn = self.mark_turn.lock().unwrap_or_else(|e| e.into_inner());
        while *turn < target {
            turn = self.mark_cv.wait(turn).unwrap_or_else(|e| e.into_inner());
        }
    }

    // -----------------------------------------------------------------------
    // Blob storage methods (native only)
    // -----------------------------------------------------------------------

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_bucket(&self, name: &str) -> Result<()> {
        self.blob_store.create_bucket(name)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn list_buckets(&self) -> Vec<String> {
        self.blob_store.list_buckets()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        self.blob_store.delete_bucket(name)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Snapshot of the FTS pipeline: queue depth, per-worker in-flight
    /// jobs, and a ring of recently completed/failed jobs. Cheap to call
    /// — locks are held only while assembling the JSON value.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fts_status(&self) -> Value {
        self.fts_runtime.snapshot()
    }

    /// Bytes of indexed text attributable to a single blob bucket. Used
    /// by per-tenant FTS quota accounting; the caller maps `bucket` to
    /// a tenant id (e.g. DMS uses `t_<tid>`). Locks the index for read,
    /// so it competes with search but never with writes.
    ///
    /// Native-only — the WASM build of OxiDb compiles without an
    /// `fts_index` field, so this method doesn't exist there.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bucket_fts_size(&self, bucket: &str) -> u64 {
        self.fts_index.read().bucket_text_size(bucket)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: HashMap<String, String>,
    ) -> Result<Value> {
        let meta = self
            .blob_store
            .put_object(bucket, key, data, content_type, metadata)?;

        if let Err(e) = self.fts_tx.send(FtsJob::Index {
            data: data.to_vec(),
            content_type: content_type.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
        }) {
            eprintln!("[fts] failed to queue index job: {e}");
        }

        Ok(serde_json::to_value(&meta)?)
    }

    /// `put_object` with a caller-supplied ETag — see
    /// [`BlobStore::put_object_with_etag`]. Multipart is the only caller.
    /// Native-only, like every blob method: wasm32 has no `blob_store` (the
    /// break that made v0.35.0 ship binary-only was exactly a forgotten cfg).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn put_object_with_etag(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: HashMap<String, String>,
        etag: Option<String>,
    ) -> Result<Value> {
        let meta = self.blob_store.put_object_with_etag(
            bucket,
            key,
            data,
            content_type,
            metadata,
            etag,
        )?;

        if let Err(e) = self.fts_tx.send(FtsJob::Index {
            data: data.to_vec(),
            content_type: content_type.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
        }) {
            eprintln!("[fts] failed to queue index job: {e}");
        }

        Ok(serde_json::to_value(&meta)?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(Vec<u8>, Value)> {
        let (data, meta) = self.blob_store.get_object(bucket, key)?;
        Ok((data, serde_json::to_value(&meta)?))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn head_object(&self, bucket: &str, key: &str) -> Result<Value> {
        let meta = self.blob_store.head_object(bucket, key)?;
        Ok(serde_json::to_value(&meta)?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.blob_store.delete_object(bucket, key)?;

        if let Err(e) = self.fts_tx.send(FtsJob::Remove {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }) {
            eprintln!("[fts] failed to queue remove job: {e}");
        }

        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        self.list_objects_from(bucket, prefix, None, limit)
    }

    /// Objects after a given key — keyset paging, stable across writes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn list_objects_from(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let metas = self
            .blob_store
            .list_objects_from(bucket, prefix, after, limit)?;
        metas
            .into_iter()
            .map(|m| serde_json::to_value(&m).map_err(Error::from))
            .collect()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn search(&self, bucket: Option<&str>, query: &str, limit: usize) -> Result<Vec<Value>> {
        let results = self.fts_index.read().search(bucket, query, limit);
        Ok(results
            .into_iter()
            .map(|r| {
                json!({
                    "bucket": r.bucket,
                    "key": r.key,
                    "score": r.score,
                })
            })
            .collect())
    }

    /// Blob FTS search with `<mark>...</mark>` highlighted snippets.
    ///
    /// For each hit, fetches the blob, re-extracts text via
    /// `fts::extract_text()`, and produces snippets through `fts::highlight()`.
    /// This is more expensive than `search()` because it re-extracts
    /// each matched document — pass it only when the caller actually
    /// wants snippets, and keep `limit` modest.
    ///
    /// Output shape per hit:
    ///   { "bucket": "...", "key": "...", "score": ...,
    ///     "highlights": ["...<mark>term</mark>...", ...] }
    /// `highlights` is omitted when extraction fails or there are no matches.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn search_highlighted(
        &self,
        bucket: Option<&str>,
        query: &str,
        limit: usize,
        snippet_chars: usize,
        max_snippets: usize,
    ) -> Result<Vec<Value>> {
        let results = self.fts_index.read().search(bucket, query, limit);
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            let mut entry = json!({
                "bucket": r.bucket,
                "key": r.key,
                "score": r.score,
            });
            if max_snippets > 0
                && snippet_chars > 0
                && let Ok((data, meta)) = self.blob_store.get_object(&r.bucket, &r.key)
                && let Some(text) = crate::fts::extract_text(&data, &meta.content_type)
            {
                let snippets = crate::fts::highlight(&text, query, snippet_chars, max_snippets);
                if !snippets.is_empty() {
                    let arr: Vec<Value> = snippets
                        .into_iter()
                        .map(|s| Value::String(s.text))
                        .collect();
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert("highlights".to_string(), Value::Array(arr));
                    }
                }
            }
            out.push(entry);
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Stored procedures
    // -----------------------------------------------------------------------

    /// Create or replace a stored procedure.
    pub fn create_procedure(&self, name: &str, body: Value) -> Result<()> {
        // Validate the procedure definition
        crate::procedure::parse_procedure(&body)?;

        let col = self.get_or_create_collection("_procedures")?;

        // Ensure unique index on name
        let _ = col.create_unique_index("name");

        // Delete existing procedure with same name (upsert semantics)
        let _ = col.delete(&json!({"name": name}), None);

        // Store the full definition with the name
        let mut doc = body;
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("name".to_string(), Value::String(name.to_string()));
        }
        col.insert(doc)?;
        Ok(())
    }

    /// Execute a stored procedure by name with the given parameters.
    pub fn call_procedure(&self, name: &str, params: Value) -> Result<Value> {
        let proc_def = {
            let col = self.get_or_create_collection("_procedures")?;
            col.find_one(&json!({"name": name}))?
                .ok_or_else(|| Error::ProcedureNotFound(name.to_string()))?
        };
        crate::procedure::execute_procedure(self, &proc_def, &params)
    }

    /// List all stored procedure names.
    pub fn list_procedures(&self) -> Result<Vec<String>> {
        let col = self.get_or_create_collection("_procedures")?;
        let docs = col.find(&json!({}))?;
        Ok(docs
            .iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str()).map(String::from))
            .collect())
    }

    /// Get a stored procedure definition by name.
    pub fn get_procedure(&self, name: &str) -> Result<Value> {
        let col = self.get_or_create_collection("_procedures")?;
        col.find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::ProcedureNotFound(name.to_string()))
    }

    /// Delete a stored procedure by name.
    pub fn delete_procedure(&self, name: &str) -> Result<()> {
        let col = self.get_or_create_collection("_procedures")?;
        let deleted = col.delete(&json!({"name": name}), None)?;
        if deleted.is_empty() {
            return Err(Error::ProcedureNotFound(name.to_string()));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cron Scheduler
    // -----------------------------------------------------------------------

    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_scheduler(self: &Arc<Self>) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::spawn(move || {
            crate::scheduler::scheduler_loop(db, rx);
        });
        *self.scheduler_shutdown.lock() = Some(tx);
    }

    /// Create or replace a named schedule.
    pub fn create_schedule(&self, name: &str, mut def: Value) -> Result<()> {
        // Validate: must have either "cron" or "every"
        let has_cron = def.get("cron").and_then(|v| v.as_str()).is_some();
        let has_every = def.get("every").and_then(|v| v.as_str()).is_some();
        if !has_cron && !has_every {
            return Err(Error::ScheduleError(
                "schedule must have 'cron' or 'every' field".into(),
            ));
        }
        if has_cron && has_every {
            return Err(Error::ScheduleError(
                "schedule cannot have both 'cron' and 'every'".into(),
            ));
        }

        // Validate cron expression if present
        if let Some(cron_str) = def.get("cron").and_then(|v| v.as_str()) {
            crate::scheduler::parse_cron(cron_str)?;
        }
        // Validate interval if present
        if let Some(every_str) = def.get("every").and_then(|v| v.as_str()) {
            crate::scheduler::parse_interval(every_str)?;
        }

        // Validate procedure exists
        let procedure = def
            .get("procedure")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ScheduleError("missing 'procedure' field".into()))?
            .to_string();
        // Check procedure exists
        self.get_procedure(&procedure)?;

        // Ensure required fields
        if let Some(obj) = def.as_object_mut() {
            obj.insert("name".to_string(), Value::String(name.to_string()));
            obj.entry("enabled".to_string())
                .or_insert(Value::Bool(true));
            obj.entry("last_run".to_string()).or_insert(Value::Null);
            obj.entry("last_run_epoch".to_string()).or_insert(json!(0));
            obj.entry("last_status".to_string()).or_insert(Value::Null);
            obj.entry("last_error".to_string()).or_insert(Value::Null);
            obj.entry("run_count".to_string()).or_insert(json!(0));
        }

        let col = self.get_or_create_collection("_schedules")?;
        let _ = col.create_unique_index("name");
        // Upsert: delete existing, then insert
        let _ = col.delete(&json!({"name": name}), None);
        col.insert(def)?;
        Ok(())
    }

    /// List all schedules.
    pub fn list_schedules(&self) -> Result<Vec<Value>> {
        let col = self.get_or_create_collection("_schedules")?;
        col.find(&json!({}))
    }

    /// Get a schedule by name.
    pub fn get_schedule(&self, name: &str) -> Result<Value> {
        let col = self.get_or_create_collection("_schedules")?;
        col.find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::ScheduleError(format!("schedule not found: {name}")))
    }

    /// Delete a schedule by name.
    pub fn delete_schedule(&self, name: &str) -> Result<()> {
        let col = self.get_or_create_collection("_schedules")?;
        let deleted = col.delete(&json!({"name": name}), None)?;
        if deleted.is_empty() {
            return Err(Error::ScheduleError(format!("schedule not found: {name}")));
        }
        Ok(())
    }

    /// Enable a schedule.
    pub fn enable_schedule(&self, name: &str) -> Result<()> {
        // Verify it exists
        self.get_schedule(name)?;
        self.update(
            "_schedules",
            &json!({"name": name}),
            &json!({"$set": {"enabled": true}}),
        )?;
        Ok(())
    }

    /// Disable (pause) a schedule.
    pub fn disable_schedule(&self, name: &str) -> Result<()> {
        // Verify it exists
        self.get_schedule(name)?;
        self.update(
            "_schedules",
            &json!({"name": name}),
            &json!({"$set": {"enabled": false}}),
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Retention Policies
    // -----------------------------------------------------------------------

    /// Set a retention policy on a collection.
    /// Documents older than `retain_days` (based on `_ts` field) are automatically deleted.
    /// Internally creates a TTL index on `_ts` — the existing TTL eviction thread handles cleanup.
    pub fn set_retention(&self, collection: &str, retain_days: u64) -> Result<()> {
        if retain_days == 0 {
            return Err(Error::InvalidQuery("retain_days must be > 0".to_string()));
        }
        let expire_after_seconds = retain_days * 86400;

        // Create TTL index on _ts (idempotent — skips if already exists)
        self.create_ttl_index(collection, "_ts", expire_after_seconds)?;

        // Upsert policy record in _retention_policies
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        let _ = policy_col.create_index("collection");
        let _ = policy_col.delete(&json!({"collection": collection}), None);
        policy_col.insert(json!({
            "collection": collection,
            "retain_days": retain_days,
            "expire_after_seconds": expire_after_seconds,
            "_ts": chrono::Utc::now().to_rfc3339(),
        }))?;

        Ok(())
    }

    /// Get the retention policy for a collection.
    pub fn get_retention(&self, collection: &str) -> Result<Value> {
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        policy_col
            .find_one(&json!({"collection": collection}))?
            .ok_or_else(|| Error::InvalidQuery(format!("no retention policy for '{collection}'")))
    }

    /// Delete the retention policy for a collection.
    /// Removes the TTL config so documents are no longer auto-evicted.
    pub fn delete_retention(&self, collection: &str) -> Result<()> {
        // Remove TTL config from the target collection
        let col = self.get_or_create_collection(collection)?;
        col.remove_ttl_config("_ts");

        // Remove the policy record
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        let deleted = policy_col.delete(&json!({"collection": collection}), None)?;
        if deleted.is_empty() {
            return Err(Error::InvalidQuery(format!(
                "no retention policy for '{collection}'"
            )));
        }
        Ok(())
    }

    /// List all retention policies.
    pub fn list_retentions(&self) -> Result<Vec<Value>> {
        let policy_col = self.get_or_create_collection("_retention_policies")?;
        policy_col.find(&json!({}))
    }

    // -----------------------------------------------------------------------
    // Alerting
    // -----------------------------------------------------------------------

    /// Start the background alert evaluator thread.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_alert_evaluator(self: &Arc<Self>, interval: std::time::Duration) {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name("oxidb-alerts".into())
            .spawn(move || {
                crate::alerting::alert_loop(db, rx, interval);
            })
            .expect("failed to spawn alert evaluator thread");
        *self.alert_shutdown.lock() = Some(tx);
    }

    /// Create a new alert rule.
    pub fn create_alert(&self, name: &str, mut def: Value) -> Result<()> {
        // Validate required fields
        let obj = def.as_object_mut().ok_or_else(|| {
            Error::InvalidQuery("alert definition must be a JSON object".to_string())
        })?;

        if !obj.contains_key("collection") {
            return Err(Error::InvalidQuery("missing 'collection'".to_string()));
        }
        if !obj.contains_key("condition") {
            return Err(Error::InvalidQuery("missing 'condition'".to_string()));
        }
        if !obj.contains_key("actions") {
            return Err(Error::InvalidQuery("missing 'actions'".to_string()));
        }

        // Validate condition has required fields
        if let Some(cond) = obj.get("condition").and_then(|v| v.as_object()) {
            if !cond.contains_key("type") {
                return Err(Error::InvalidQuery("condition missing 'type'".to_string()));
            }
            if !cond.contains_key("threshold") {
                return Err(Error::InvalidQuery(
                    "condition missing 'threshold'".to_string(),
                ));
            }
        }

        // Set defaults
        obj.entry("name").or_insert(json!(name));
        obj.entry("enabled").or_insert(json!(true));
        obj.entry("cooldown_seconds").or_insert(json!(300));
        obj.entry("last_fired").or_insert(json!(null));
        obj.entry("last_fired_epoch").or_insert(json!(0));
        obj.entry("fire_count").or_insert(json!(0));
        obj.entry("_ts")
            .or_insert(json!(chrono::Utc::now().to_rfc3339()));

        // Check for duplicate
        let alert_col = self.get_or_create_collection("_alerts")?;
        let _ = alert_col.create_index("name");
        if alert_col.find_one(&json!({"name": name}))?.is_some() {
            // Update existing alert
            let _ = alert_col.delete(&json!({"name": name}), None);
        }

        alert_col.insert(Value::Object(obj.clone()))?;
        Ok(())
    }

    /// Delete an alert by name.
    pub fn delete_alert(&self, name: &str) -> Result<()> {
        let alert_col = self.get_or_create_collection("_alerts")?;
        let deleted = alert_col.delete(&json!({"name": name}), None)?;
        if deleted.is_empty() {
            return Err(Error::InvalidQuery(format!("alert not found: {name}")));
        }
        Ok(())
    }

    /// List all alerts.
    pub fn list_alerts(&self) -> Result<Vec<Value>> {
        let alert_col = self.get_or_create_collection("_alerts")?;
        alert_col.find(&json!({}))
    }

    /// Get a single alert by name.
    pub fn get_alert(&self, name: &str) -> Result<Value> {
        let alert_col = self.get_or_create_collection("_alerts")?;
        alert_col
            .find_one(&json!({"name": name}))?
            .ok_or_else(|| Error::InvalidQuery(format!("alert not found: {name}")))
    }

    /// Test an alert by evaluating its condition immediately (without cooldown or actions).
    pub fn test_alert(&self, name: &str) -> Result<Value> {
        let alert = self.get_alert(name)?;
        let now = crate::scheduler::epoch_now();

        let collection = alert
            .get("collection")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::InvalidQuery("alert missing 'collection'".to_string()))?;
        let condition = alert
            .get("condition")
            .ok_or_else(|| Error::InvalidQuery("alert missing 'condition'".to_string()))?;
        let cond_type = condition.get("type").and_then(|v| v.as_str()).unwrap_or("");

        let result = match cond_type {
            "count_threshold" => {
                let query = crate::alerting::build_windowed_query_pub(condition, now);
                let count = match query {
                    Some(q) => self.count(collection, &q)? as i64,
                    None => 0,
                };
                let threshold = condition
                    .get("threshold")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let operator = condition
                    .get("operator")
                    .and_then(|v| v.as_str())
                    .unwrap_or("gte");
                json!({
                    "alert": name,
                    "type": "count_threshold",
                    "current_value": count,
                    "threshold": threshold,
                    "operator": operator,
                    "would_fire": crate::alerting::compare_pub(count, threshold, operator),
                })
            }
            _ => {
                json!({"alert": name, "error": format!("unsupported condition type: {cond_type}")})
            }
        };

        Ok(result)
    }

    /// List alert history.
    pub fn list_alert_history(&self) -> Result<Vec<Value>> {
        let hist_col = self.get_or_create_collection("_alert_history")?;
        hist_col.find(&json!({}))
    }

    // -----------------------------------------------------------------------
    // Backup & Restore
    // -----------------------------------------------------------------------

    /// Create a compressed tar.gz backup of the entire data directory.
    ///
    /// The backup flushes all indexes and WAL checkpoints before archiving,
    /// then holds read locks on all collections to ensure a consistent snapshot.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn backup(&self, output_path: &Path) -> Result<BackupInfo> {
        // 1. Validate output path doesn't already exist
        if output_path.exists() {
            return Err(Error::Backup(format!(
                "output path already exists: {}",
                output_path.display()
            )));
        }

        // Ensure parent directory exists
        if let Some(parent) = output_path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }

        // 2. Discover any collections on disk that haven't been loaded yet
        let disk_names = Self::discover_collection_names_on_disk(&self.data_dir)?;

        // 3. Ensure all collections are loaded
        for name in &disk_names {
            let _ = self.get_or_create_collection(name)?;
        }

        // 4. Flush indexes and checkpoint WALs for each collection
        let num_collections = {
            let cols = self.collections.read();
            for col_arc in cols.values() {
                col_arc.save_index_data();
                let _ = col_arc.checkpoint_wal();
            }
            cols.len()
        };

        // 4b. PITR: capture the GSN watermark, then barrier every WAL so
        //     the tar is guaranteed to see every record below it. Order
        //     matters — reading the counter first, then barriering, waits
        //     out any in-flight write that allocated a GSN below the
        //     watermark (GSNs are allocated under the WAL lock). The
        //     watermark is written into the data dir so it travels inside
        //     the tarball; the PITR replay tool reads it to learn where
        //     to resume from the archive.
        if let Some(seq) = &self.archive_sequencer {
            let base_gsn = seq.current_gsn();
            {
                let cols = self.collections.read();
                for col_arc in cols.values() {
                    col_arc.wal_barrier();
                }
            }
            crate::pitr::BaseMeta::new(base_gsn).write_to(&self.data_dir)?;
        }

        // 5. Hold collections map read lock for consistent snapshot
        let _cols = self.collections.read();

        // 6. Create tar.gz archive
        let file = std::fs::File::create(output_path)?;
        let enc = GzEncoder::new(file, Compression::default());
        let mut archive = tar::Builder::new(enc);

        Self::add_dir_to_tar(&mut archive, &self.data_dir, &self.data_dir)?;

        let enc = archive
            .into_inner()
            .map_err(|e| Error::Backup(e.to_string()))?;
        enc.finish().map_err(|e| Error::Backup(e.to_string()))?;

        // 7. Return info
        let metadata = std::fs::metadata(output_path)?;
        Ok(BackupInfo {
            path: output_path.to_string_lossy().into_owned(),
            size_bytes: metadata.len(),
            collections: num_collections,
        })
    }

    /// Validate and extract a backup tarball into `target_dir`. Shared by
    /// `restore` and `restore_to_point`.
    #[cfg(not(target_arch = "wasm32"))]
    fn extract_backup(archive_path: &Path, target_dir: &Path) -> Result<()> {
        if !archive_path.exists() {
            return Err(Error::Backup(format!(
                "archive not found: {}",
                archive_path.display()
            )));
        }
        if target_dir.exists() {
            if std::fs::read_dir(target_dir)?.next().is_some() {
                return Err(Error::Backup(format!(
                    "target directory is not empty: {}",
                    target_dir.display()
                )));
            }
        } else {
            std::fs::create_dir_all(target_dir)?;
        }
        let file = std::fs::File::open(archive_path)?;
        let dec = GzDecoder::new(file);
        let mut archive = tar::Archive::new(dec);
        archive.unpack(target_dir)?;
        Ok(())
    }

    /// Restore a tar.gz backup archive to a target directory.
    ///
    /// This is a static method — the caller should open a new `OxiDb` instance
    /// on the target directory after restoration.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn restore(archive_path: &Path, target_dir: &Path) -> Result<RestoreInfo> {
        Self::extract_backup(archive_path, target_dir)?;
        let collections = Self::discover_collection_names_on_disk(target_dir)?;
        Ok(RestoreInfo {
            path: target_dir.to_string_lossy().into_owned(),
            collections: collections.len(),
        })
    }

    /// Restore the database to a point in time: extract `base_backup`,
    /// then replay the archive in `archive_dir` on top of it up to
    /// `target`.
    ///
    /// Static method — open a fresh `OxiDb` on `target_dir` afterward to
    /// get the point-in-time state. The base backup must have been taken
    /// with PITR enabled (it carries a `base.meta` watermark); a base
    /// without one degrades to a plain restore. `archive_dir` is the live
    /// database's archive directory (`OXIDB_ARCHIVE_DIR`, default
    /// `<data_dir>/_archive`); `encryption` must be the same key the
    /// source database used, if any.
    ///
    /// v1 limitations: blob objects are restored only to the base-backup
    /// point (the document set is restored to `target`); the FTS index is
    /// dropped and must be rebuilt; create/drop-index DDL between the base
    /// and `target` is not replayed — indexes rebuild against the
    /// base-time schema.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn restore_to_point(
        base_backup: &Path,
        archive_dir: &Path,
        target_dir: &Path,
        target: crate::pitr::PitrTarget,
        encryption: Option<Arc<EncryptionKey>>,
    ) -> Result<PitrRestoreInfo> {
        Self::extract_backup(base_backup, target_dir)?;
        let outcome =
            crate::archive::replay_into(target_dir, archive_dir, target, encryption.as_ref())?;
        Ok(PitrRestoreInfo {
            path: target_dir.to_string_lossy().into_owned(),
            collections: outcome.collections,
            target_gsn: outcome.target_gsn,
            records_applied: outcome.records_applied,
        })
    }

    /// Refuse to open a data directory whose collections are still in
    /// the legacy append-only `.dat` format. The engine is BTree-only
    /// since v0.25.x — loading that layout would silently shadow the
    /// old data with empty `.btree` files, which is exactly the silent
    /// data loss we want to prevent.
    ///
    /// A `.dat` file with a sibling `.btree` directory is treated as
    /// already-migrated debris and ignored (the user can delete the
    /// `.dat` at their leisure). Set `OXIDB_ALLOW_LEGACY_DAT=1` to
    /// bypass — only safe if you know the legacy data is disposable.
    #[cfg(not(target_arch = "wasm32"))]
    fn check_legacy_dat_layout(dir: &Path) -> Result<()> {
        if std::env::var("OXIDB_ALLOW_LEGACY_DAT").as_deref() == Ok("1") {
            return Ok(());
        }
        if !dir.exists() {
            return Ok(());
        }
        let mut orphans: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("dat") {
                continue;
            }
            // Skip empty .dat files — a previous run may have created
            // them as zero-byte placeholders alongside a real .btree.
            let meta = match std::fs::metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.len() == 0 {
                continue;
            }
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            let btree = dir.join(format!("{stem}.btree"));
            if !btree.exists() {
                orphans.push(stem.to_string());
            }
        }
        if orphans.is_empty() {
            return Ok(());
        }
        Err(Error::Io(std::io::Error::other(format!(
            "legacy .dat collection files detected without matching .btree: [{}]. \
The current engine is BTree-only and cannot read this layout — opening the database \
would silently shadow the old data with empty collections. Migrate or delete the \
.dat files. Set OXIDB_ALLOW_LEGACY_DAT=1 to bypass (acknowledges data loss).",
            orphans.join(", ")
        ))))
    }

    /// Scan a directory for `*.dat` files and `*.btree` files/directories and return collection names.
    #[cfg(not(target_arch = "wasm32"))]
    fn discover_collection_names_on_disk(dir: &Path) -> Result<Vec<String>> {
        let mut names = std::collections::HashSet::new();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            // A collection on disk shows up as a `.btree` snapshot, a
            // `.bdat` disk-first data file, a `.wal` (its data may live only in
            // the WAL — e.g. a database backed up before any snapshot persist
            // ran), or a legacy `.dat` file. A single collection can have
            // several of these, so the HashSet dedups them. Sealed WAL segments
            // `<name>.wal.<seq>` have a numeric extension and are skipped.
            if matches!(
                ext,
                Some("btree") | Some("bdat") | Some("wal") | Some("dat")
            ) && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            {
                names.insert(stem.to_string());
            }
        }
        Ok(names.into_iter().collect())
    }

    /// Recursively add directory contents to a tar archive, skipping `.tmp` files.
    #[cfg(not(target_arch = "wasm32"))]
    fn add_dir_to_tar<W: std::io::Write>(
        archive: &mut tar::Builder<W>,
        dir: &std::path::Path,
        base: &std::path::Path,
    ) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            // Skip .tmp files
            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                continue;
            }

            let rel = path.strip_prefix(base).unwrap_or(&path);

            if path.is_dir() {
                Self::add_dir_to_tar(archive, &path, base)?;
            } else if path.is_file() {
                archive
                    .append_path_with_name(&path, rel)
                    .map_err(|e| Error::Backup(e.to_string()))?;
            }
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OxiDb {
    fn drop(&mut self) {
        // Re-use the explicit-shutdown path so Drop and the signal
        // handler converge on the same semantics.
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn temp_db() -> OxiDb {
        let dir = tempdir().unwrap();
        OxiDb::open(dir.path()).unwrap()
    }

    #[test]
    fn max_collections_caps_new_user_collections() {
        let db = temp_db();
        db.set_max_collections(2);
        // Two user collections are fine.
        db.insert("a", json!({"x": 1})).unwrap();
        db.insert("b", json!({"x": 1})).unwrap();
        // A third is rejected.
        let e = db.insert("c", json!({"x": 1})).unwrap_err();
        assert!(matches!(e, Error::CollectionLimitExceeded(2)));
        // Existing collections still accept writes (a load, not a create).
        db.insert("a", json!({"x": 2})).unwrap();
        // System collections (leading `_`) are exempt from the cap.
        db.insert("_meta", json!({"x": 1})).unwrap();
        // Raising the cap lets a new one through.
        db.set_max_collections(3);
        db.insert("c", json!({"x": 1})).unwrap();
    }

    #[test]
    fn max_documents_caps_total_inserts() {
        let db = temp_db();
        db.set_max_documents(3);
        db.insert("a", json!({"x": 1})).unwrap();
        db.insert("b", json!({"x": 2})).unwrap(); // different collection, same quota
        db.insert("a", json!({"x": 3})).unwrap();
        // 4th document (across any collection) is rejected.
        let e = db.insert("a", json!({"x": 4})).unwrap_err();
        assert!(matches!(e, Error::DocumentLimitExceeded(3)));
        // insert_many respects the remaining headroom too.
        db.set_max_documents(5);
        let e2 = db
            .insert_many("c", vec![json!({}), json!({}), json!({})])
            .unwrap_err();
        assert!(matches!(e2, Error::DocumentLimitExceeded(5))); // 3 existing + 3 > 5
        db.insert_many("c", vec![json!({}), json!({})]).unwrap(); // 3 + 2 = 5 ok
        // System collections don't count against the quota.
        db.insert("_meta", json!({"x": 1})).unwrap();
    }

    #[test]
    fn max_collections_zero_is_unlimited() {
        let db = temp_db(); // default cap is 0 = unlimited
        for i in 0..12 {
            db.insert(&format!("c{i}"), json!({"x": i})).unwrap();
        }
        assert_eq!(db.list_collections().len(), 12);
    }

    #[test]
    fn tx_multiple_updates_same_doc_compose() {
        // Regression: two $inc updates to the SAME doc in one transaction
        // must compose (read-your-own-writes), not clobber. 100 -2 +1 = 99.
        let db = temp_db();
        db.insert("acc", json!({"id": "x", "bal": 100})).unwrap();

        let tx = db.begin_transaction();
        db.tx_update(
            tx,
            "acc",
            &json!({"id": "x"}),
            &json!({"$inc": {"bal": -2}}),
        )
        .unwrap();
        db.tx_update(tx, "acc", &json!({"id": "x"}), &json!({"$inc": {"bal": 1}}))
            .unwrap();
        db.commit_transaction(tx).unwrap();

        let doc = db.find_one("acc", &json!({"id": "x"})).unwrap().unwrap();
        assert_eq!(doc["bal"], 99, "deltas must compose, not last-write-wins");
    }

    #[test]
    fn tx_insert_then_update_same_doc() {
        // Insert then update the inserted doc in the same tx: the update
        // must see the inserted value.
        let db = temp_db();
        let tx = db.begin_transaction();
        db.tx_insert(tx, "acc", json!({"id": "y", "bal": 10}))
            .unwrap();
        db.tx_update(tx, "acc", &json!({"id": "y"}), &json!({"$inc": {"bal": 5}}))
            .unwrap();
        db.commit_transaction(tx).unwrap();
        let doc = db.find_one("acc", &json!({"id": "y"})).unwrap().unwrap();
        assert_eq!(doc["bal"], 15);
    }

    #[test]
    fn tx_money_conserved_with_self_transfer() {
        // A transfer where from == to (two updates hit the same doc) plus
        // a fee leg must conserve total balance. This is the exact shape
        // that created money before the staging fix.
        let db = temp_db();
        db.insert("acc", json!({"id": "a", "bal": 1000})).unwrap();
        db.insert("acc", json!({"id": "fee", "bal": 0})).unwrap();
        let total_before = 1000;

        // from==to==a: debit amount+fee (2), credit amount (1), fee +1.
        let tx = db.begin_transaction();
        db.tx_update(
            tx,
            "acc",
            &json!({"id": "a"}),
            &json!({"$inc": {"bal": -2}}),
        )
        .unwrap();
        db.tx_update(tx, "acc", &json!({"id": "a"}), &json!({"$inc": {"bal": 1}}))
            .unwrap();
        db.tx_update(
            tx,
            "acc",
            &json!({"id": "fee"}),
            &json!({"$inc": {"bal": 1}}),
        )
        .unwrap();
        db.commit_transaction(tx).unwrap();

        let total: i64 = db
            .find("acc", &json!({}))
            .unwrap()
            .iter()
            .map(|d| d["bal"].as_i64().unwrap())
            .sum();
        assert_eq!(total, total_before, "money must be conserved");
        let a = db.find_one("acc", &json!({"id": "a"})).unwrap().unwrap();
        assert_eq!(a["bal"], 999); // 1000 - 2 + 1
    }

    #[test]
    fn explain_reports_collscan_vs_index() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        for i in 0..50 {
            db.insert(
                "orders",
                json!({"status": if i % 2 == 0 { "open" } else { "done" }, "n": i}),
            )
            .unwrap();
        }

        // No index: full scan, post-filter.
        let plan = db
            .explain_find(
                "orders",
                &json!({"status": "open"}),
                &FindOptions::default(),
            )
            .unwrap();
        assert_eq!(plan["strategy"], "COLLSCAN");
        assert_eq!(plan["examined"], 50);
        assert_eq!(plan["returned"], 25);
        assert_eq!(plan["index_used"], Value::Null);

        // With an index: index scan, candidates narrowed to the match.
        db.create_index("orders", "status").unwrap();
        let plan = db
            .explain_find(
                "orders",
                &json!({"status": "open"}),
                &FindOptions::default(),
            )
            .unwrap();
        assert_eq!(plan["strategy"], "INDEX_SCAN");
        assert_eq!(plan["candidates"], 25);
        assert_eq!(plan["examined"], 25);
        assert_eq!(plan["returned"], 25);
        assert_eq!(plan["post_filter"], false);

        // Post-filter-only operator: index can't serve $mod, and the
        // plan says so.
        let plan = db
            .explain_find(
                "orders",
                &json!({"n": {"$mod": [7, 0]}}),
                &FindOptions::default(),
            )
            .unwrap();
        assert_eq!(plan["strategy"], "COLLSCAN");
        assert_eq!(plan["post_filter"], true);
        assert_eq!(plan["post_filter_ops"], json!(["$mod"]));

        // Index-only count.
        let plan = db
            .explain_count("orders", &json!({"status": "open"}))
            .unwrap();
        assert_eq!(plan["strategy"], "INDEX_ONLY_COUNT");
        assert_eq!(plan["count"], 25);

        // Aggregate: stage list + leading-$match plan.
        let plan = db
            .explain_aggregate(
                "orders",
                &json!([
                    {"$match": {"status": "open"}},
                    {"$group": {"_id": null, "total": {"$sum": "$n"}}}
                ]),
            )
            .unwrap();
        assert_eq!(plan["stages"], json!(["$match", "$group"]));
        assert_eq!(plan["first_match"]["strategy"], "INDEX_SCAN");
        assert_eq!(plan["returned"], 1);
    }

    #[test]
    fn explain_reports_index_backed_sort() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        for i in 0..10 {
            db.insert("t", json!({"ts": i})).unwrap();
        }
        db.create_index("t", "ts").unwrap();
        let opts = FindOptions {
            sort: Some(vec![("ts".to_string(), crate::query::SortOrder::Asc)]),
            limit: Some(3),
            ..Default::default()
        };
        let plan = db.explain_find("t", &json!({}), &opts).unwrap();
        assert_eq!(plan["strategy"], "INDEX_SORT");
        assert_eq!(plan["sort"], "index-backed");
        assert_eq!(plan["index_used"], "ts");
        assert_eq!(plan["returned"], 3);
    }

    #[test]
    fn tx_insert_commit() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"}))
            .unwrap();
        db.commit_transaction(tx_id).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Alice");
    }

    #[test]
    fn tx_insert_rollback() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"}))
            .unwrap();
        db.rollback_transaction(tx_id).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn tx_multi_collection_commit() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"}))
            .unwrap();
        db.tx_insert(tx_id, "orders", json!({"item": "Widget"}))
            .unwrap();
        db.commit_transaction(tx_id).unwrap();

        let users = db.find("users", &json!({})).unwrap();
        let orders = db.find("orders", &json!({})).unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn tx_multi_collection_rollback() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"}))
            .unwrap();
        db.tx_insert(tx_id, "orders", json!({"item": "Widget"}))
            .unwrap();
        db.rollback_transaction(tx_id).unwrap();

        let users = db.find("users", &json!({})).unwrap();
        let orders = db.find("orders", &json!({})).unwrap();
        assert_eq!(users.len(), 0);
        assert_eq!(orders.len(), 0);
    }

    #[test]
    fn tx_occ_conflict() {
        let db = temp_db();
        // Insert a doc outside of a transaction
        db.insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();

        // TX1 reads the doc
        let tx1 = db.begin_transaction();
        let docs = db.tx_find(tx1, "users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(docs.len(), 1);

        // TX2 updates the doc and commits
        let tx2 = db.begin_transaction();
        db.tx_update(
            tx2,
            "users",
            &json!({"name": "Alice"}),
            &json!({"$set": {"age": 31}}),
        )
        .unwrap();
        db.commit_transaction(tx2).unwrap();

        // TX1 tries to update -- should get a conflict since the version changed
        db.tx_update(
            tx1,
            "users",
            &json!({"name": "Alice"}),
            &json!({"$set": {"age": 32}}),
        )
        .unwrap();
        let result = db.commit_transaction(tx1);
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::TransactionConflict { .. } => {}
            other => panic!("Expected TransactionConflict, got: {other}"),
        }
    }

    #[test]
    fn concurrent_no_conflict() {
        let db = temp_db();
        db.insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25}))
            .unwrap();

        // TX1 reads and updates Alice
        let tx1 = db.begin_transaction();
        db.tx_find(tx1, "users", &json!({"name": "Alice"})).unwrap();
        db.tx_update(
            tx1,
            "users",
            &json!({"name": "Alice"}),
            &json!({"$set": {"age": 31}}),
        )
        .unwrap();

        // TX2 reads and updates Bob (different doc)
        let tx2 = db.begin_transaction();
        db.tx_find(tx2, "users", &json!({"name": "Bob"})).unwrap();
        db.tx_update(
            tx2,
            "users",
            &json!({"name": "Bob"}),
            &json!({"$set": {"age": 26}}),
        )
        .unwrap();

        // Both should succeed
        db.commit_transaction(tx1).unwrap();
        db.commit_transaction(tx2).unwrap();

        let alice = db
            .find_one("users", &json!({"name": "Alice"}))
            .unwrap()
            .unwrap();
        let bob = db
            .find_one("users", &json!({"name": "Bob"}))
            .unwrap()
            .unwrap();
        assert_eq!(alice["age"], 31);
        assert_eq!(bob["age"], 26);
    }

    #[test]
    fn auto_rollback_on_drop() {
        let db = temp_db();
        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Ghost"}))
            .unwrap();
        // Simulate disconnect: just rollback without commit
        db.rollback_transaction(tx_id).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn backup_creates_archive() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("orders", json!({"item": "Widget"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        let info = db.backup(&backup_path).unwrap();

        assert!(backup_path.exists());
        assert!(info.size_bytes > 0);
        assert_eq!(info.collections, 2);
    }

    #[test]
    fn backup_fails_if_output_exists() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        std::fs::write(&backup_path, b"existing").unwrap();

        let result = db.backup(&backup_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn restore_from_backup() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("users", json!({"name": "Bob"})).unwrap();
        db.insert("orders", json!({"item": "Widget"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        db.backup(&backup_path).unwrap();
        drop(db);

        let restore_dir = dir.path().join("restored");
        let info = OxiDb::restore(&backup_path, &restore_dir).unwrap();
        assert_eq!(info.collections, 2);

        let db2 = OxiDb::open(&restore_dir).unwrap();
        let users = db2.find("users", &json!({})).unwrap();
        assert_eq!(users.len(), 2);
        let orders = db2.find("orders", &json!({})).unwrap();
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn restore_fails_if_target_not_empty() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();

        let backup_path = dir.path().join("backup.tar.gz");
        db.backup(&backup_path).unwrap();

        let restore_dir = dir.path().join("notempty");
        std::fs::create_dir_all(&restore_dir).unwrap();
        std::fs::write(restore_dir.join("file.txt"), b"data").unwrap();

        let result = OxiDb::restore(&backup_path, &restore_dir);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not empty"));
    }

    // -----------------------------------------------------------------------
    // Change stream tests
    // -----------------------------------------------------------------------

    #[test]
    fn watch_insert_emits_event() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::All, None).unwrap();

        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.operation, OperationType::Insert);
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, id);
        assert!(event.document.is_some());
        assert!(event.token > 0);
        let doc = event.document.unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn watch_update_emits_event() {
        let db = temp_db();
        let id = db
            .insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        db.update(
            "users",
            &json!({"name": "Alice"}),
            &json!({"$set": {"age": 31}}),
        )
        .unwrap();

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.operation, OperationType::Update);
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, id);
        // Update events carry the post-image.
        let doc = event
            .document
            .expect("update event must carry the post-image");
        assert_eq!(doc["age"], 31);
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["_version"], 2);
    }

    #[test]
    fn watch_delete_emits_event() {
        let db = temp_db();
        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        db.delete("users", &json!({"name": "Alice"})).unwrap();

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.operation, OperationType::Delete);
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, id);
        // Delete events carry the pre-image (the deleted document).
        let doc = event
            .document
            .expect("delete event must carry the pre-image");
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn watch_upsert_insert_carries_document() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::All, None).unwrap();

        // Nothing matches — the upsert inserts. The Insert event's contract
        // says the document is always present; before 0.42.9 this path sent
        // None, a silent null-deref source for subscribers trusting it.
        let (matched, _, upserted) = db
            .update_with_upsert(
                "drivers",
                &json!({"driver": "u42"}),
                &json!({"$set": {"lat": 41.0, "lon": 29.0}}),
                true,
                true,
            )
            .unwrap();
        assert_eq!(matched, 0);
        let id = upserted.expect("upsert must insert");

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.operation, OperationType::Insert);
        assert_eq!(event.doc_id, id);
        let doc = event
            .document
            .expect("upsert Insert event must carry the inserted document");
        assert_eq!(doc["driver"], "u42");
        assert_eq!(doc["lat"], 41.0);
        assert_eq!(doc["_id"], id);
    }

    #[test]
    fn watch_find_and_modify_carries_post_image() {
        let db = temp_db();
        let id = db
            .insert("counters", json!({"name": "uidnext", "n": 7}))
            .unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        db.find_and_modify(
            "counters",
            &json!({"name": "uidnext"}),
            &json!({"$inc": {"n": 1}}),
        )
        .unwrap()
        .expect("must match");

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.operation, OperationType::Update);
        assert_eq!(event.doc_id, id);
        let doc = event
            .document
            .expect("find_and_modify event must carry the post-image");
        assert_eq!(doc["n"], 8);
    }

    #[test]
    fn watch_update_one_carries_post_image() {
        let db = temp_db();
        db.insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        db.update_one(
            "users",
            &json!({"name": "Alice"}),
            &json!({"$set": {"age": 44}}),
        )
        .unwrap();

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let doc = event
            .document
            .expect("update_one event must carry the post-image");
        assert_eq!(doc["age"], 44);
    }

    #[test]
    fn watch_bulk_update_carries_post_images() {
        // The bulk path (>64 matched docs) invalidates the doc cache
        // instead of warming it — the post-image must still ride the event.
        let db = temp_db();
        for i in 0..80 {
            db.insert("fleet", json!({"kind": "truck", "i": i}))
                .unwrap();
        }

        let handle = db.watch(WatchFilter::All, None).unwrap();
        db.update(
            "fleet",
            &json!({"kind": "truck"}),
            &json!({"$set": {"seen": true}}),
        )
        .unwrap();

        for _ in 0..80 {
            let event = handle
                .rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap();
            assert_eq!(event.operation, OperationType::Update);
            let doc = event
                .document
                .expect("bulk update event must carry the post-image");
            assert_eq!(doc["seen"], true);
            assert_eq!(doc["kind"], "truck");
        }
    }

    #[test]
    fn watch_tx_update_carries_post_image() {
        let db = temp_db();
        let id = db
            .insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        let tx_id = db.begin_transaction();
        db.tx_update(
            tx_id,
            "users",
            &json!({"name": "Alice"}),
            &json!({"$set": {"age": 31}}),
        )
        .unwrap();
        db.commit_transaction(tx_id).unwrap();

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.operation, OperationType::Update);
        assert_eq!(event.doc_id, id);
        assert_eq!(event.tx_id, Some(tx_id));
        let doc = event
            .document
            .expect("tx update event must carry the post-image");
        assert_eq!(doc["age"], 31);
    }

    #[test]
    fn watch_tx_delete_carries_pre_image() {
        let db = temp_db();
        let id = db.insert("users", json!({"name": "Alice"})).unwrap();

        let handle = db.watch(WatchFilter::All, None).unwrap();
        let tx_id = db.begin_transaction();
        db.tx_delete(tx_id, "users", &json!({"name": "Alice"}))
            .unwrap();
        db.commit_transaction(tx_id).unwrap();

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.operation, OperationType::Delete);
        assert_eq!(event.doc_id, id);
        let doc = event
            .document
            .expect("tx delete event must carry the pre-image");
        assert_eq!(doc["name"], "Alice");
    }

    #[test]
    fn watch_tx_commit_emits_events() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::All, None).unwrap();

        let tx_id = db.begin_transaction();
        db.tx_insert(tx_id, "users", json!({"name": "Alice"}))
            .unwrap();
        db.tx_insert(tx_id, "users", json!({"name": "Bob"}))
            .unwrap();
        db.commit_transaction(tx_id).unwrap();

        let e1 = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let e2 = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(e1.operation, OperationType::Insert);
        assert_eq!(e2.operation, OperationType::Insert);
        assert!(e1.tx_id.is_some());
        assert!(e2.tx_id.is_some());
    }

    #[test]
    fn unwatch_stops_events() {
        let db = temp_db();
        let handle = db.watch(WatchFilter::All, None).unwrap();

        db.unwatch(handle.id);
        db.insert("users", json!({"name": "Alice"})).unwrap();

        assert!(
            handle
                .rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err()
        );
    }

    #[test]
    fn watch_filters_by_collection() {
        let db = temp_db();
        let handle = db
            .watch(WatchFilter::Collection("orders".to_string()), None)
            .unwrap();

        db.insert("users", json!({"name": "Alice"})).unwrap();
        let order_id = db.insert("orders", json!({"item": "Widget"})).unwrap();

        let event = handle
            .rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(event.collection, "orders");
        assert_eq!(event.doc_id, order_id);

        // No more events (the users insert was filtered out)
        assert!(
            handle
                .rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err()
        );
    }

    #[test]
    fn test_sp_demo() {
        let db = temp_db();

        // ── Seed data ──────────────────────────────────────────────
        db.insert(
            "accounts",
            json!({
                "account_id": "ACC001", "owner": "Alice", "balance": 500
            }),
        )
        .unwrap();
        db.insert(
            "accounts",
            json!({
                "account_id": "ACC002", "owner": "Bob", "balance": 200
            }),
        )
        .unwrap();

        // ── 1. Create a stored procedure ───────────────────────────
        db.create_procedure(
            "transfer_funds",
            json!({
                "name": "transfer_funds",
                "params": ["from_account", "to_account", "amount"],
                "steps": [
                    {
                        "step": "find_one",
                        "collection": "accounts",
                        "query": { "account_id": "$param.from_account" },
                        "as": "sender"
                    },
                    {
                        "step": "find_one",
                        "collection": "accounts",
                        "query": { "account_id": "$param.to_account" },
                        "as": "receiver"
                    },
                    {
                        "step": "if",
                        "condition": { "$expr": { "$lt": ["$sender.balance", "$param.amount"] } },
                        "then": [
                            { "step": "abort", "message": "insufficient funds" }
                        ]
                    },
                    {
                        "step": "update",
                        "collection": "accounts",
                        "query": { "account_id": "$param.from_account" },
                        "update": { "$inc": { "balance": -150 } }
                    },
                    {
                        "step": "update",
                        "collection": "accounts",
                        "query": { "account_id": "$param.to_account" },
                        "update": { "$inc": { "balance": 150 } }
                    },
                    {
                        "step": "return",
                        "value": {
                            "status": "ok",
                            "from": "$param.from_account",
                            "to": "$param.to_account",
                            "amount": "$param.amount",
                            "sender_old_balance": "$sender.balance",
                            "receiver_old_balance": "$receiver.balance"
                        }
                    }
                ]
            }),
        )
        .unwrap();

        // ── 2. List procedures ─────────────────────────────────────
        let procs = db.list_procedures().unwrap();
        println!("\n=== Stored procedures: {:?}", procs);
        assert_eq!(procs, vec!["transfer_funds"]);

        // ── 3. Get procedure definition ────────────────────────────
        let def = db.get_procedure("transfer_funds").unwrap();
        println!(
            "\n=== Procedure definition:\n{}",
            serde_json::to_string_pretty(&def).unwrap()
        );

        // ── 4. Call the procedure (success) ────────────────────────
        let result = db
            .call_procedure(
                "transfer_funds",
                json!({
                    "from_account": "ACC001",
                    "to_account": "ACC002",
                    "amount": 150
                }),
            )
            .unwrap();
        println!(
            "\n=== Transfer result:\n{}",
            serde_json::to_string_pretty(&result).unwrap()
        );
        assert_eq!(result["status"], "ok");

        // Verify balances after transfer
        let alice = db
            .find_one("accounts", &json!({"account_id": "ACC001"}))
            .unwrap()
            .unwrap();
        let bob = db
            .find_one("accounts", &json!({"account_id": "ACC002"}))
            .unwrap()
            .unwrap();
        println!("\n=== After transfer:");
        println!("  Alice: {}", alice["balance"]);
        println!("  Bob:   {}", bob["balance"]);
        assert_eq!(alice["balance"], 350);
        assert_eq!(bob["balance"], 350);

        // ── 5. Call the procedure (insufficient funds → abort) ─────
        let err = db.call_procedure(
            "transfer_funds",
            json!({
                "from_account": "ACC001",
                "to_account": "ACC002",
                "amount": 9999
            }),
        );
        println!("\n=== Insufficient funds error: {}", err.unwrap_err());

        // Verify balances unchanged after abort
        let alice = db
            .find_one("accounts", &json!({"account_id": "ACC001"}))
            .unwrap()
            .unwrap();
        let bob = db
            .find_one("accounts", &json!({"account_id": "ACC002"}))
            .unwrap()
            .unwrap();
        println!("  Alice still: {}", alice["balance"]);
        println!("  Bob still:   {}", bob["balance"]);
        assert_eq!(alice["balance"], 350);
        assert_eq!(bob["balance"], 350);

        // ── 6. Delete the procedure ────────────────────────────────
        db.delete_procedure("transfer_funds").unwrap();
        let procs = db.list_procedures().unwrap();
        println!("\n=== After delete, procedures: {:?}", procs);
        assert!(procs.is_empty());
    }

    #[test]
    fn test_sp_nested_if() {
        let db = temp_db();

        // Seed: users with different ages and membership tiers
        db.insert(
            "users",
            json!({
                "name": "Alice", "age": 25, "tier": "gold", "balance": 1000
            }),
        )
        .unwrap();
        db.insert(
            "users",
            json!({
                "name": "Bob", "age": 16, "tier": "silver", "balance": 500
            }),
        )
        .unwrap();
        db.insert(
            "users",
            json!({
                "name": "Charlie", "age": 30, "tier": "bronze", "balance": 50
            }),
        )
        .unwrap();

        // A procedure that classifies a user's discount based on nested rules:
        //   - if age < 18 → "minor_not_eligible"
        //   - else →
        //       - if tier == "gold" →
        //           - if balance >= 500 → 30% discount
        //           - else → 20% discount
        //       - else →
        //           - if balance >= 200 → 10% discount
        //           - else → 5% discount
        db.create_procedure("calc_discount", json!({
            "name": "calc_discount",
            "params": ["username"],
            "steps": [
                {
                    "step": "find_one",
                    "collection": "users",
                    "query": { "name": "$param.username" },
                    "as": "user"
                },
                {
                    "step": "if",
                    "condition": { "$expr": { "$lt": ["$user.age", 18] } },
                    "then": [
                        { "step": "return", "value": {
                            "user": "$param.username",
                            "discount": 0,
                            "reason": "minor_not_eligible"
                        }}
                    ],
                    "else": [
                        {
                            "step": "if",
                            "condition": { "$expr": { "$eq": ["$user.tier", "gold"] } },
                            "then": [
                                {
                                    "step": "if",
                                    "condition": { "$expr": { "$gte": ["$user.balance", 500] } },
                                    "then": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 30,
                                            "reason": "gold_high_balance"
                                        }}
                                    ],
                                    "else": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 20,
                                            "reason": "gold_low_balance"
                                        }}
                                    ]
                                }
                            ],
                            "else": [
                                {
                                    "step": "if",
                                    "condition": { "$expr": { "$gte": ["$user.balance", 200] } },
                                    "then": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 10,
                                            "reason": "standard_high_balance"
                                        }}
                                    ],
                                    "else": [
                                        { "step": "return", "value": {
                                            "user": "$param.username",
                                            "discount": 5,
                                            "reason": "standard_low_balance"
                                        }}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        })).unwrap();

        // Alice: age 25, gold, balance 1000 → gold_high_balance (30%)
        let r = db
            .call_procedure("calc_discount", json!({"username": "Alice"}))
            .unwrap();
        println!("\nAlice: {}", serde_json::to_string_pretty(&r).unwrap());
        assert_eq!(r["discount"], 30);
        assert_eq!(r["reason"], "gold_high_balance");

        // Bob: age 16 → minor_not_eligible (0%)
        let r = db
            .call_procedure("calc_discount", json!({"username": "Bob"}))
            .unwrap();
        println!("\nBob: {}", serde_json::to_string_pretty(&r).unwrap());
        assert_eq!(r["discount"], 0);
        assert_eq!(r["reason"], "minor_not_eligible");

        // Charlie: age 30, bronze, balance 50 → standard_low_balance (5%)
        let r = db
            .call_procedure("calc_discount", json!({"username": "Charlie"}))
            .unwrap();
        println!("\nCharlie: {}", serde_json::to_string_pretty(&r).unwrap());
        assert_eq!(r["discount"], 5);
        assert_eq!(r["reason"], "standard_low_balance");
    }

    // -----------------------------------------------------------------------
    // In-memory engine tests
    // -----------------------------------------------------------------------

    fn mem_db() -> OxiDb {
        OxiDb::open_in_memory().unwrap()
    }

    #[test]
    fn in_memory_insert_find() {
        let db = mem_db();
        db.insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25}))
            .unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);

        let alice = db.find("users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0]["age"], 30);
    }

    #[test]
    fn in_memory_update_delete() {
        let db = mem_db();
        db.insert("users", json!({"name": "Alice", "age": 30}))
            .unwrap();

        db.update(
            "users",
            &json!({"name": "Alice"}),
            &json!({"$set": {"age": 31}}),
        )
        .unwrap();
        let docs = db.find("users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(docs[0]["age"], 31);

        db.delete("users", &json!({"name": "Alice"})).unwrap();
        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn in_memory_collections() {
        let db = mem_db();
        db.insert("users", json!({"name": "Alice"})).unwrap();
        db.insert("orders", json!({"item": "Widget"})).unwrap();

        let cols = db.list_collections();
        assert!(cols.contains(&"users".to_string()));
        assert!(cols.contains(&"orders".to_string()));

        db.drop_collection("orders").unwrap();
        let cols = db.list_collections();
        assert!(!cols.contains(&"orders".to_string()));
    }

    #[test]
    fn in_memory_indexes() {
        let db = mem_db();
        db.create_index("users", "email").unwrap();
        db.insert("users", json!({"name": "Alice", "email": "alice@test.com"}))
            .unwrap();
        db.insert("users", json!({"name": "Bob", "email": "bob@test.com"}))
            .unwrap();

        let docs = db
            .find("users", &json!({"email": "alice@test.com"}))
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Alice");
    }

    #[test]
    fn in_memory_transactions() {
        let db = mem_db();
        let tx = db.begin_transaction();
        db.tx_insert(tx, "users", json!({"name": "Alice"})).unwrap();
        db.tx_insert(tx, "users", json!({"name": "Bob"})).unwrap();
        db.commit_transaction(tx).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn in_memory_transaction_rollback() {
        let db = mem_db();
        db.insert("users", json!({"name": "Existing"})).unwrap();

        let tx = db.begin_transaction();
        db.tx_insert(tx, "users", json!({"name": "Temporary"}))
            .unwrap();
        db.rollback_transaction(tx).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Existing");
    }

    #[test]
    fn in_memory_is_in_memory() {
        let db = mem_db();
        assert!(db.is_in_memory());

        let disk_db = temp_db();
        assert!(!disk_db.is_in_memory());
    }

    #[test]
    fn in_memory_ttl_eviction() {
        let db = mem_db();
        // Insert a doc with 0-second TTL (expires immediately)
        db.insert("cache", json!({"key": "session", "_ttl": 0}))
            .unwrap();
        // Insert a doc without TTL
        db.insert("cache", json!({"key": "permanent"})).unwrap();

        // Before eviction, both docs exist
        let docs = db.find("cache", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);

        // Wait briefly then evict
        std::thread::sleep(std::time::Duration::from_millis(10));
        {
            let col = db.get_or_create_collection("cache").unwrap();
            let evicted = col.evict_expired();
            assert_eq!(evicted, 1);
        }

        let docs = db.find("cache", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["key"], "permanent");
    }

    #[test]
    fn ttl_index_create_and_list() {
        let db = mem_db();
        db.create_ttl_index("sessions", "created_at", 3600).unwrap();

        let indexes = db.list_indexes("sessions").unwrap();
        // Should have both a field index on "created_at" and a TTL index
        let ttl_idx = indexes.iter().find(|i| i.index_type == "ttl").unwrap();
        assert_eq!(ttl_idx.name, "created_at_ttl");
        assert_eq!(ttl_idx.fields, vec!["created_at".to_string()]);
        assert_eq!(ttl_idx.expire_after_seconds, Some(3600));

        let field_idx = indexes.iter().find(|i| i.index_type == "field").unwrap();
        assert_eq!(field_idx.name, "created_at");
    }

    #[test]
    fn ttl_index_evicts_expired_docs() {
        let db = mem_db();
        // TTL of 1 second
        db.create_ttl_index("sessions", "created_at", 1).unwrap();

        // Insert a doc with a timestamp 5 seconds in the past (should be expired)
        let past = chrono::Utc::now() - chrono::Duration::seconds(5);
        let past_str = past.to_rfc3339();
        db.insert(
            "sessions",
            json!({"user": "expired", "created_at": past_str}),
        )
        .unwrap();

        // Insert a doc with current timestamp (should survive)
        let now_str = chrono::Utc::now().to_rfc3339();
        db.insert("sessions", json!({"user": "active", "created_at": now_str}))
            .unwrap();

        // Both exist before eviction
        let docs = db.find("sessions", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);

        // Run eviction
        {
            let col = db.get_or_create_collection("sessions").unwrap();
            let evicted = col.evict_ttl_indexed();
            assert_eq!(evicted, 1);
        }

        // Only the active doc remains
        let docs = db.find("sessions", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["user"], "active");
    }

    #[test]
    fn ttl_index_no_eviction_before_expiry() {
        let db = mem_db();
        // TTL of 1 hour
        db.create_ttl_index("cache", "ts", 3600).unwrap();

        // Insert a doc with current timestamp — should NOT be evicted
        let now_str = chrono::Utc::now().to_rfc3339();
        db.insert("cache", json!({"key": "fresh", "ts": now_str}))
            .unwrap();

        {
            let col = db.get_or_create_collection("cache").unwrap();
            let evicted = col.evict_ttl_indexed();
            assert_eq!(evicted, 0);
        }

        let docs = db.find("cache", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn ttl_index_idempotent_create() {
        let db = mem_db();
        db.create_ttl_index("logs", "timestamp", 86400).unwrap();
        db.create_ttl_index("logs", "timestamp", 86400).unwrap(); // should not error

        let indexes = db.list_indexes("logs").unwrap();
        let ttl_count = indexes.iter().filter(|i| i.index_type == "ttl").count();
        assert_eq!(ttl_count, 1);
    }

    #[test]
    fn ttl_index_drop() {
        let db = mem_db();
        db.create_ttl_index("sessions", "created_at", 3600).unwrap();

        // Verify it exists
        let indexes = db.list_indexes("sessions").unwrap();
        assert!(indexes.iter().any(|i| i.name == "created_at_ttl"));

        // Drop it
        db.drop_index("sessions", "created_at_ttl").unwrap();

        // Verify it's gone
        let indexes = db.list_indexes("sessions").unwrap();
        assert!(!indexes.iter().any(|i| i.name == "created_at_ttl"));
    }

    #[test]
    fn ttl_index_persists_across_restart() {
        let dir = tempdir().unwrap();

        // Phase 1: create TTL index and insert docs
        {
            let db = OxiDb::open(dir.path()).unwrap();
            db.create_ttl_index("sessions", "created_at", 2).unwrap();

            // Insert an already-expired doc (5s ago, TTL=2s)
            let past = chrono::Utc::now() - chrono::Duration::seconds(5);
            db.insert(
                "sessions",
                json!({"user": "old", "created_at": past.to_rfc3339()}),
            )
            .unwrap();

            // Insert a fresh doc
            db.insert(
                "sessions",
                json!({"user": "new", "created_at": chrono::Utc::now().to_rfc3339()}),
            )
            .unwrap();

            // Flush to disk
            let col = db.get_or_create_collection("sessions").unwrap();
            let _ = col.sync_writes();
        }
        // db dropped — simulates shutdown

        // Phase 2: reopen and verify TTL index was restored
        {
            let db = OxiDb::open(dir.path()).unwrap();

            // TTL index should be in list_indexes
            let indexes = db.list_indexes("sessions").unwrap();
            let ttl_idx = indexes.iter().find(|i| i.index_type == "ttl");
            assert!(ttl_idx.is_some(), "TTL index not restored after restart");
            let ttl_idx = ttl_idx.unwrap();
            assert_eq!(ttl_idx.name, "created_at_ttl");
            assert_eq!(ttl_idx.expire_after_seconds, Some(2));

            // Evict — the old doc should be removed
            let col = db.get_or_create_collection("sessions").unwrap();
            let evicted = col.evict_ttl_indexed();
            assert_eq!(evicted, 1);

            // Only the fresh doc remains
            let docs = db.find("sessions", &json!({})).unwrap();
            assert_eq!(docs.len(), 1);
            assert_eq!(docs[0]["user"], "new");
        }
    }

    #[test]
    fn ttl_index_evicts_on_create() {
        let db = mem_db();
        // Insert already-expired docs BEFORE creating the TTL index
        let past = chrono::Utc::now() - chrono::Duration::seconds(10);
        db.insert("events", json!({"type": "old", "at": past.to_rfc3339()}))
            .unwrap();
        db.insert(
            "events",
            json!({"type": "current", "at": chrono::Utc::now().to_rfc3339()}),
        )
        .unwrap();

        // Creating the TTL index should immediately evict the old doc
        db.create_ttl_index("events", "at", 5).unwrap();

        let docs = db.find("events", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["type"], "current");
    }

    /// Regression: dropping a collection whose `.btree` snapshot is a
    /// file (not a directory) used to bail out with
    /// `io error: Not a directory (os error 20)` because the drop path
    /// unconditionally called `remove_dir_all`. drop_collection must
    /// handle BOTH the file shape (small / single-page collection) and
    /// the directory shape (multi-page snapshot).
    #[test]
    fn drop_collection_handles_btree_file_layout() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();

        // Insert a doc so the collection materialises a .btree on disk.
        db.insert("orphans", json!({"x": 1})).unwrap();

        // Make .btree a FILE (the legacy single-page layout). We do
        // this by removing whatever the engine wrote, then touching an
        // empty file with the same name — drop_collection should still
        // succeed without raising "Not a directory".
        let btree = dir.path().join("orphans.btree");
        if btree.is_dir() {
            std::fs::remove_dir_all(&btree).unwrap();
        } else if btree.exists() {
            std::fs::remove_file(&btree).unwrap();
        }
        std::fs::write(&btree, b"").unwrap();
        assert!(
            btree.exists() && !btree.is_dir(),
            "preflight: .btree should be a file"
        );

        db.drop_collection("orphans")
            .expect("drop_collection on a .btree file shape should succeed");
        assert!(
            !btree.exists(),
            ".btree file should be removed by drop_collection"
        );
    }

    /// Companion: the directory-shape `.btree` (multi-page snapshot)
    /// keeps working too — the same drop path must handle both.
    #[test]
    fn drop_collection_handles_btree_directory_layout() {
        let dir = tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();

        db.insert("dirty", json!({"x": 1})).unwrap();

        let btree = dir.path().join("dirty.btree");
        // Force the directory layout: remove whatever the engine wrote,
        // then mkdir + drop a sentinel file inside.
        if btree.is_dir() {
            std::fs::remove_dir_all(&btree).unwrap();
        } else if btree.exists() {
            std::fs::remove_file(&btree).unwrap();
        }
        std::fs::create_dir_all(&btree).unwrap();
        std::fs::write(btree.join("page-0000"), b"").unwrap();
        assert!(btree.is_dir(), "preflight: .btree should be a directory");

        db.drop_collection("dirty")
            .expect("drop_collection on a .btree dir shape should succeed");
        assert!(
            !btree.exists(),
            ".btree dir should be removed by drop_collection"
        );
    }
}
