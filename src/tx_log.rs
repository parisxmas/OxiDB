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
