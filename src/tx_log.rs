use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::Result;

pub type TransactionId = u64;

/// Global transaction commit log.
///
/// Format: sequence of `[tx_id: u64 LE]` entries, append-only.
/// A tx_id present in this log means the transaction is committed.
pub struct TxCommitLog {
    inner: Mutex<File>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl TxCommitLog {
    /// Open or create the commit log file at `<data_dir>/_tx_commit_log`.
    pub fn open(data_dir: &Path) -> Result<Self> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join("_tx_commit_log");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;

        Ok(Self {
            inner: Mutex::new(file),
            path,
        })
    }

    /// Mark a transaction as committed by appending its tx_id and fsyncing.
    /// This is THE COMMIT POINT for the transaction.
    pub fn mark_committed(&self, tx_id: TransactionId) -> Result<()> {
        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::End(0))?;
        file.write_all(&tx_id.to_le_bytes())?;
        file.sync_data()?;
        Ok(())
    }

    /// Read all committed transaction IDs from the log.
    pub fn read_committed(&self) -> Result<HashSet<TransactionId>> {
        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len();
        let mut set = HashSet::new();

        let entry_count = file_len / 8;
        for _ in 0..entry_count {
            let mut buf = [0u8; 8];
            if file.read_exact(&mut buf).is_err() {
                break;
            }
            let tx_id = u64::from_le_bytes(buf);
            set.insert(tx_id);
        }

        Ok(set)
    }

    /// Remove a specific tx_id from the commit log by rewriting without it.
    pub fn remove_committed(&self, tx_id: TransactionId) -> Result<()> {
        let mut file = self.inner.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len();

        // Read all entries
        let entry_count = file_len / 8;
        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let mut buf = [0u8; 8];
            if file.read_exact(&mut buf).is_err() {
                break;
            }
            let id = u64::from_le_bytes(buf);
            if id != tx_id {
                entries.push(id);
            }
        }

        // Rewrite the file
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        for id in entries {
            file.write_all(&id.to_le_bytes())?;
        }
        file.sync_data()?;
        Ok(())
    }

    /// Clear the commit log (truncate to 0). Called after full recovery.
    pub fn clear(&self) -> Result<()> {
        let file = self.inner.lock().unwrap();
        file.set_len(0)?;
        file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        log.remove_committed(999).unwrap(); // Not in log

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

        // HashSet deduplicates, so read_committed returns 1 entry
        let committed = log.read_committed().unwrap();
        assert!(committed.contains(&5));
    }
}
