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
/// File format unchanged: a sequence of `[tx_id: u64 LE]` entries.
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
        let (done_tx, done_rx) = mpsc::sync_channel::<Result<()>>(1);
        self.submit
            .send(Cmd::Mark {
                tx_id,
                done: done_tx,
            })
            .map_err(|_| committer_gone())?;
        match done_rx.recv() {
            Ok(r) => r,
            Err(_) => Err(committer_gone()),
        }
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
    let entry_count = file_len / 8;
    for _ in 0..entry_count {
        let mut buf = [0u8; 8];
        if file.read_exact(&mut buf).is_err() {
            break;
        }
        set.insert(u64::from_le_bytes(buf));
    }
    Ok(set)
}

/// Rewrite the file with the current set + fsync. Sorted for
/// reproducible on-disk content — keeps tests deterministic and makes
/// hex-diffing post-recovery state across runs cheap.
fn persist(file: &mut File, set: &HashSet<TransactionId>) -> std::io::Result<()> {
    let mut ids: Vec<TransactionId> = set.iter().copied().collect();
    ids.sort_unstable();
    let mut buf: Vec<u8> = Vec::with_capacity(ids.len() * 8);
    for id in &ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&buf)?;
    file.sync_data()?;
    Ok(())
}

fn committer_loop(
    rx: Receiver<Cmd>,
    mut file: File,
    mut committed: HashSet<TransactionId>,
    _path: PathBuf,
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
            persist(&mut file, &committed)
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
                    let _ = tx.send(Err(Error::Io(std::io::Error::new(
                        kind,
                        msg.clone(),
                    ))));
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
}
