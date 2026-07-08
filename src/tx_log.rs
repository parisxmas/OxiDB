use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::Mutex;

use crate::error::{Error, Result};

pub type TransactionId = u64;

// ── On-disk header (Phase 1b of ADR-0003 / docs/format/tx-commit-log.md) ──
//
// Layout (8 bytes, little-endian):
//   [b"OXTX" (4)][version u16][flags u16]
//
// version=1 is the current format. The reader accepts both v1 and a
// legacy header-less form (detected by absence of `OXTX` at offset 0);
// the next `persist` rewrites legacy files with a v1 header. Reading a
// version we don't recognise is a hard error rather than silent
// misinterpretation.

const TX_MAGIC: &[u8; 4] = b"OXTX";
const TX_VERSION: u16 = 1;
const TX_HEADER_SIZE: usize = 8;

/// Upper bound on how many submissions ride a single fsync. A pending
/// queue larger than this gets sliced into batches of this size; the
/// next iteration picks up the rest. Higher = more amortisation under
/// concurrency, lower = bounded p99 wait. 512 was the sweet spot in
/// load tests — above that the marginal fsync savings stop tracking
/// the extra latency budget.
const MAX_BATCH: usize = 512;

/// Window the committer waits for the *first* command of a new batch
/// before deciding the queue is genuinely idle. Once a first command
/// arrives we drain non-blockingly; this timeout only matters for the
/// teardown path where the channel has gone quiet and the thread
/// should poll the senders-dropped signal periodically.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Global transaction commit log with group-commit semantics.
///
/// Submissions (`mark_committed`, `remove_committed`, `clear`) cross a
/// channel to a dedicated committer thread. The committer drains the
/// queue, applies every mutation against an in-memory `HashSet`, then
/// performs ONE `sync_data()` per batch — so N concurrent commits
/// share one fsync instead of paying N. On startup, the file is parsed
/// once into the in-memory set and that becomes the source of truth;
/// the file is rewritten in full at each batch boundary.
///
/// File format: `[magic "OXTX"][version u16 LE][flags u16 LE]` header
/// followed by a sequence of `[tx_id: u64 LE]` entries. Files written
/// by versions predating the header are accepted as legacy: detected
/// by the absence of `OXTX` magic at offset 0; the entries stream
/// starts at offset 0 instead of offset 8. The first `persist` rewrites
/// any legacy file with the v1 header. See
/// [`docs/format/tx-commit-log.md`](../../docs/format/tx-commit-log.md)
/// for the byte-level spec.
///
/// Readers (e.g. recovery via `read_committed`) see only durable
/// state — a Read in the same batch as a Mark is deferred until after
/// the batch's fsync, so callers never observe an entry that hasn't
/// been persisted.
pub struct TxCommitLog {
    submit: Sender<Cmd>,
    /// Joined on Drop so callers see committer errors cleanly during
    /// shutdown. Wrapped in Mutex<Option<_>> so Drop can `.take()`.
    committer: Mutex<Option<JoinHandle<()>>>,
    #[allow(dead_code)]
    path: PathBuf,
}

enum Cmd {
    Mark {
        tx_id: TransactionId,
        done: SyncSender<Result<()>>,
    },
    Remove {
        tx_id: TransactionId,
        done: SyncSender<Result<()>>,
    },
    RemoveMany {
        tx_ids: Vec<TransactionId>,
        done: SyncSender<Result<()>>,
    },
    Clear {
        done: SyncSender<Result<()>>,
    },
    Read {
        done: SyncSender<HashSet<TransactionId>>,
    },
    Shutdown {
        done: SyncSender<()>,
    },
}

impl TxCommitLog {
    /// Open or create the commit log file at `<data_dir>/_tx_commit_log`.
    /// Parses any pre-existing entries into the in-memory set and spawns
    /// the committer thread.
    pub fn open(data_dir: &Path) -> Result<Self> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join("_tx_commit_log");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;

        // A stale sibling tmp file means a crash landed inside a persist's
        // write-to-tmp step (before the atomic rename). The live file is
        // intact — the tmp is garbage; remove it so nothing ever reads it.
        let _ = fs::remove_file(path.with_extension("tmp"));

        let committed = parse_log(&mut file)?;

        let (tx, rx) = mpsc::channel::<Cmd>();
        let path_for_thread = path.clone();
        let handle = thread::Builder::new()
            .name("oxidb-tx-commit".into())
            .spawn(move || committer_loop(rx, file, committed, path_for_thread))
            .map_err(|e| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("spawn tx_log committer: {e}"),
                ))
            })?;

        Ok(Self {
            submit: tx,
            committer: Mutex::new(Some(handle)),
            path,
        })
    }

    /// Mark a transaction as committed. Blocks until the enclosing
    /// batch has been fsync'd — durability semantics identical to the
    /// pre-group-commit version; only the underlying fsync is shared.
    pub fn mark_committed(&self, tx_id: TransactionId) -> Result<()> {
        let done_rx = self.mark_committed_async(tx_id)?;
        match done_rx.recv() {
            Ok(r) => r,
            Err(_) => Err(committer_gone()),
        }
    }

    /// Submit a commit mark WITHOUT waiting for the batch fsync; the
    /// returned receiver fires once the enclosing batch is durable.
    /// This is the group-commit split point: the engine submits marks in
    /// commit order (cheap) and lets many commits wait on the same batch
    /// fsync concurrently, instead of serializing one fsync per commit.
    pub fn mark_committed_async(&self, tx_id: TransactionId) -> Result<mpsc::Receiver<Result<()>>> {
        let (done_tx, done_rx) = mpsc::sync_channel::<Result<()>>(1);
        self.submit
            .send(Cmd::Mark {
                tx_id,
                done: done_tx,
            })
            .map_err(|_| committer_gone())?;
        Ok(done_rx)
    }

    /// Remove a tx_id from the commit log. Same fsync-coupled semantics
    /// as `mark_committed`. Idempotent — removing an absent id is a
    /// no-op (still rides the batch and gets ack'd after fsync).
    pub fn remove_committed(&self, tx_id: TransactionId) -> Result<()> {
        let (done_tx, done_rx) = mpsc::sync_channel::<Result<()>>(1);
        self.submit
            .send(Cmd::Remove {
                tx_id,
                done: done_tx,
            })
            .map_err(|_| committer_gone())?;
        match done_rx.recv() {
            Ok(r) => r,
            Err(_) => Err(committer_gone()),
        }
    }

    /// Remove a batch of tx_ids in one submission — one channel round-trip
    /// and at most one fsync for the whole batch, unlike a loop over
    /// `remove_committed` which pays a batch wait per id. Used by the
    /// background sync thread to prune ids whose data has been persisted.
    pub fn remove_committed_many(&self, tx_ids: &[TransactionId]) -> Result<()> {
        if tx_ids.is_empty() {
            return Ok(());
        }
        let (done_tx, done_rx) = mpsc::sync_channel::<Result<()>>(1);
        self.submit
            .send(Cmd::RemoveMany {
                tx_ids: tx_ids.to_vec(),
                done: done_tx,
            })
            .map_err(|_| committer_gone())?;
        match done_rx.recv() {
            Ok(r) => r,
            Err(_) => Err(committer_gone()),
        }
    }

    /// Read all committed transaction IDs from the log. Returns
    /// durable state only: a Read sitting in the same batch as
    /// in-flight Marks waits behind their fsync before snapshotting.
    pub fn read_committed(&self) -> Result<HashSet<TransactionId>> {
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        self.submit
            .send(Cmd::Read { done: done_tx })
            .map_err(|_| committer_gone())?;
        match done_rx.recv() {
            Ok(set) => Ok(set),
            Err(_) => Err(committer_gone()),
        }
    }

    /// Clear the commit log (drops every tx_id). Called after full
    /// recovery once the engine has reapplied every committed tx.
    pub fn clear(&self) -> Result<()> {
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        self.submit
            .send(Cmd::Clear { done: done_tx })
            .map_err(|_| committer_gone())?;
        match done_rx.recv() {
            Ok(r) => r,
            Err(_) => Err(committer_gone()),
        }
    }

    /// Explicit shutdown: flush any pending batch and join the
    /// committer thread. Idempotent; `drop` calls this for you.
    pub fn close(&self) -> Result<()> {
        let handle = self.committer.lock().take();
        if handle.is_none() {
            return Ok(());
        }
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        // If send fails the thread is already gone; treat as success.
        let _ = self.submit.send(Cmd::Shutdown { done: done_tx });
        let _ = done_rx.recv_timeout(Duration::from_secs(5));
        if let Some(h) = handle {
            let _ = h.join();
        }
        Ok(())
    }
}

impl Drop for TxCommitLog {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn committer_gone() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::Other,
        "tx_log committer thread is gone",
    ))
}

fn parse_log(file: &mut File) -> std::io::Result<HashSet<TransactionId>> {
    file.seek(SeekFrom::Start(0))?;
    let file_len = file.metadata()?.len();
    let mut set = HashSet::new();
    if file_len == 0 {
        return Ok(set);
    }

    // Try the new-style header. A file written by a current engine starts with
    // `OXTX` magic; anything else is treated as legacy (header-less) — those
    // 8 bytes are the first tx_id and we rewind to read it.
    let mut entries_start: u64 = 0;
    if file_len >= TX_HEADER_SIZE as u64 {
        let mut header = [0u8; TX_HEADER_SIZE];
        file.read_exact(&mut header)?;
        if &header[0..4] == TX_MAGIC {
            let version = u16::from_le_bytes([header[4], header[5]]);
            if version != TX_VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "unsupported _tx_commit_log format version {version}; \
                         this binary understands version {TX_VERSION}"
                    ),
                ));
            }
            // header[6..8] = flags; reserved at 0 for now — ignored by readers
            // until a bit is documented + assigned.
            entries_start = TX_HEADER_SIZE as u64;
        }
    }
    if entries_start == 0 {
        file.seek(SeekFrom::Start(0))?;
    }

    let entries_len = file_len.saturating_sub(entries_start);
    let entry_count = entries_len / 8;
    for _ in 0..entry_count {
        let mut buf = [0u8; 8];
        if file.read_exact(&mut buf).is_err() {
            break;
        }
        set.insert(u64::from_le_bytes(buf));
    }
    Ok(set)
}

/// Rewrite the log with the current set + fsync, via atomic replace.
/// Sorted for reproducible on-disk content — keeps tests deterministic
/// and makes hex-diffing post-recovery state across runs cheap.
///
/// Atomic replace (tmp + fsync + rename + dir fsync) is load-bearing:
/// the previous implementation truncated and rewrote the live file in
/// place, so a crash (SIGKILL, power loss) between the truncate and the
/// completed write left the commit log empty or torn — and recovery
/// then discarded EVERY transactional WAL entry not yet persisted to a
/// snapshot: acked commits vanished wholesale. Found by
/// `tests/jepsen_bank_crash.rs` (round 3). `rename(2)` is atomic on
/// POSIX: a crash now leaves either the complete old set or the
/// complete new set, never a torn file.
fn persist(path: &Path, set: &HashSet<TransactionId>) -> std::io::Result<()> {
    let mut ids: Vec<TransactionId> = set.iter().copied().collect();
    ids.sort_unstable();
    let mut buf: Vec<u8> = Vec::with_capacity(TX_HEADER_SIZE + ids.len() * 8);
    // Header: magic + version + flags. Phase 1b of ADR-0003; every persist
    // rewrites the whole file, so a legacy header-less file is migrated to the
    // v1 header on the first batch after upgrade.
    buf.extend_from_slice(TX_MAGIC);
    buf.extend_from_slice(&TX_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags, reserved
    for id in &ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_data()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

fn committer_loop(
    rx: Receiver<Cmd>,
    _file: File,
    mut committed: HashSet<TransactionId>,
    path: PathBuf,
) {
    loop {
        // Block waiting for the first command of a new batch. A short
        // timeout lets us re-poll if all senders have been dropped
        // (recv_timeout returns Disconnected in that case) — strictly
        // speaking redundant because `rx.recv()` would also see it,
        // but the timeout protects against any future change where we
        // want to coalesce around an idle tick.
        let first = match rx.recv_timeout(POLL_INTERVAL) {
            Ok(c) => c,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        };

        let mut batch: Vec<Cmd> = Vec::with_capacity(MAX_BATCH);
        batch.push(first);
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(c) => batch.push(c),
                Err(_) => break,
            }
        }

        let mut mutated = false;
        let mut mutating_replies: Vec<SyncSender<Result<()>>> = Vec::new();
        // Reads are answered AFTER fsync so callers never see a
        // mid-batch in-memory state that hasn't been persisted.
        let mut deferred_reads: Vec<SyncSender<HashSet<TransactionId>>> = Vec::new();
        let mut shutdown_reply: Option<SyncSender<()>> = None;

        for cmd in batch {
            match cmd {
                Cmd::Mark { tx_id, done } => {
                    committed.insert(tx_id);
                    mutating_replies.push(done);
                    mutated = true;
                }
                Cmd::Remove { tx_id, done } => {
                    committed.remove(&tx_id);
                    mutating_replies.push(done);
                    mutated = true;
                }
                Cmd::RemoveMany { tx_ids, done } => {
                    for id in tx_ids {
                        committed.remove(&id);
                    }
                    mutating_replies.push(done);
                    mutated = true;
                }
                Cmd::Clear { done } => {
                    committed.clear();
                    mutating_replies.push(done);
                    mutated = true;
                }
                Cmd::Read { done } => {
                    deferred_reads.push(done);
                }
                Cmd::Shutdown { done } => {
                    shutdown_reply = Some(done);
                    // Stop pulling new commands; the senders side has
                    // been dropped (or is about to be), so flushing
                    // what we have is correct and finite.
                    break;
                }
            }
        }

        let flush_result: std::io::Result<()> = if mutated {
            persist(&path, &committed)
        } else {
            Ok(())
        };

        match &flush_result {
            Ok(()) => {
                for tx in mutating_replies.drain(..) {
                    let _ = tx.send(Ok(()));
                }
            }
            Err(e) => {
                // io::Error isn't Clone — synthesise per waiter with
                // matching kind and the formatted message. The kind
                // preservation lets callers `errors::Io(kind=...)`
                // match if they want.
                let kind = e.kind();
                let msg = format!("tx_log persist: {e}");
                for tx in mutating_replies.drain(..) {
                    let _ = tx.send(Err(Error::Io(std::io::Error::new(kind, msg.clone()))));
                }
            }
        }

        // Reads always see post-fsync state. If the flush itself
        // failed we still hand back the in-memory snapshot — the
        // mutations are already in `committed`, just not durable yet,
        // and the caller (recovery) doesn't care about durability for
        // a read.
        for tx in deferred_reads.drain(..) {
            let _ = tx.send(committed.clone());
        }

        if let Some(done) = shutdown_reply {
            let _ = done.send(());
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn empty_log_has_no_committed() {
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();
        let committed = log.read_committed().unwrap();
        assert!(committed.is_empty());
    }

    #[test]
    fn mark_and_read_committed() {
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();

        log.mark_committed(1).unwrap();
        log.mark_committed(2).unwrap();
        log.mark_committed(3).unwrap();

        let committed = log.read_committed().unwrap();
        assert_eq!(committed.len(), 3);
        assert!(committed.contains(&1));
        assert!(committed.contains(&2));
        assert!(committed.contains(&3));
    }

    #[test]
    fn remove_committed_entry() {
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();

        log.mark_committed(10).unwrap();
        log.mark_committed(20).unwrap();
        log.mark_committed(30).unwrap();

        log.remove_committed(20).unwrap();

        let committed = log.read_committed().unwrap();
        assert_eq!(committed.len(), 2);
        assert!(committed.contains(&10));
        assert!(!committed.contains(&20));
        assert!(committed.contains(&30));
    }

    #[test]
    fn clear_empties_log() {
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();

        log.mark_committed(1).unwrap();
        log.mark_committed(2).unwrap();
        assert_eq!(log.read_committed().unwrap().len(), 2);

        log.clear().unwrap();
        assert!(log.read_committed().unwrap().is_empty());
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let log = TxCommitLog::open(dir.path()).unwrap();
            log.mark_committed(42).unwrap();
            log.mark_committed(99).unwrap();
            // Explicit close — Drop would do it too, but spelling it
            // out makes the test's invariant ("Drop drains pending
            // batches") obvious.
            log.close().unwrap();
        }
        let log = TxCommitLog::open(dir.path()).unwrap();
        let committed = log.read_committed().unwrap();
        assert!(committed.contains(&42));
        assert!(committed.contains(&99));
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();

        log.mark_committed(1).unwrap();
        log.remove_committed(999).unwrap();

        let committed = log.read_committed().unwrap();
        assert_eq!(committed.len(), 1);
        assert!(committed.contains(&1));
    }

    #[test]
    fn duplicate_mark_committed() {
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();

        log.mark_committed(5).unwrap();
        log.mark_committed(5).unwrap();

        let committed = log.read_committed().unwrap();
        assert!(committed.contains(&5));
    }

    #[test]
    fn concurrent_marks_all_durable() {
        // 64 threads × 32 commits each = 2048 commits, all expected
        // to land. If group-commit batching dropped any of them we'd
        // see a count mismatch after reopen.
        let dir = TempDir::new().unwrap();
        let log = Arc::new(TxCommitLog::open(dir.path()).unwrap());
        let mut handles = vec![];
        for t in 0..64u64 {
            let log = Arc::clone(&log);
            handles.push(thread::spawn(move || {
                for i in 0..32u64 {
                    log.mark_committed(t * 1000 + i).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(log.read_committed().unwrap().len(), 64 * 32);

        // Reopen and re-verify durability survived shutdown.
        log.close().unwrap();
        drop(log);
        let log = TxCommitLog::open(dir.path()).unwrap();
        assert_eq!(log.read_committed().unwrap().len(), 64 * 32);
    }

    #[test]
    fn read_sees_post_fsync_state() {
        // Mark then Read in flight together — Read should reflect the
        // Mark (deferred-until-after-fsync semantics).
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();
        let _ = log.mark_committed(7);
        let s = log.read_committed().unwrap();
        assert!(s.contains(&7));
    }

    // ── Phase 1b header (docs/format/tx-commit-log.md) ───────────────────

    /// Persist writes the OXTX magic + version 1 + flags 0 header before
    /// the entries stream — verified by reading the raw file bytes.
    #[test]
    fn persist_writes_oxtx_header() {
        let dir = TempDir::new().unwrap();
        let log = TxCommitLog::open(dir.path()).unwrap();
        log.mark_committed(0x0102030405060708).unwrap();
        log.close().unwrap();

        let raw = fs::read(dir.path().join("_tx_commit_log")).unwrap();
        assert!(raw.len() >= TX_HEADER_SIZE + 8, "header + 1 entry expected");
        assert_eq!(&raw[0..4], TX_MAGIC, "OXTX magic at offset 0");
        assert_eq!(u16::from_le_bytes([raw[4], raw[5]]), TX_VERSION);
        assert_eq!(u16::from_le_bytes([raw[6], raw[7]]), 0, "flags reserved 0");
        // Entry sits immediately after the 8-byte header.
        assert_eq!(
            u64::from_le_bytes(raw[8..16].try_into().unwrap()),
            0x0102030405060708,
        );
    }

    /// A legacy (header-less) file written by a pre-Phase-1b engine is
    /// still readable — the absence of OXTX at offset 0 is detected and
    /// the file is parsed as a flat u64 stream from offset 0.
    #[test]
    fn reads_legacy_header_less_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("_tx_commit_log");

        // Hand-write a pre-Phase-1b file: just three sorted u64s, no header.
        let mut bytes = Vec::new();
        for id in [10u64, 20, 30] {
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        fs::write(&path, &bytes).unwrap();

        let log = TxCommitLog::open(dir.path()).unwrap();
        let committed = log.read_committed().unwrap();
        assert_eq!(committed.len(), 3);
        assert!(committed.contains(&10));
        assert!(committed.contains(&20));
        assert!(committed.contains(&30));

        // After a single mutation, the file is rewritten with the new header.
        log.mark_committed(40).unwrap();
        log.close().unwrap();
        let raw = fs::read(&path).unwrap();
        assert_eq!(&raw[0..4], TX_MAGIC, "legacy file migrated on next persist");
    }

    /// A file with a newer format version we don't recognise must be
    /// refused rather than silently misinterpreted. The error surfaces
    /// up to the caller (TxCommitLog::open) — engine startup fails fast.
    #[test]
    fn refuses_newer_format_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("_tx_commit_log");

        // Hand-write a v2 file: OXTX + version=2 + flags=0 + one entry.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(TX_MAGIC);
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&42u64.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let err = TxCommitLog::open(dir.path())
            .err()
            .expect("open should fail on newer format version");
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported _tx_commit_log format version 2"),
            "error should mention the version: {msg}"
        );
    }
}
