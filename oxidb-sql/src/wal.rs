//! The SQL engine's own write-ahead log.
//!
//! Fully independent of the document engine's WAL: its own file, its own
//! framing, its own recovery. Nothing here reconciles with the document log,
//! because no transaction spans both engines (ADR-0010, §5).
//!
//! File layout: an 8-byte header followed by a sequence of records.
//!
//! ```text
//! header:  [b"OXSW" (4)][version: u16 LE][flags: u16 LE]
//! record:  [crc32: u32 LE][len: u32 LE][seq: u64 LE][payload: len bytes]
//! ```
//!
//! `crc32` covers `seq` bytes followed by the JSON `payload`. Recovery reads
//! records until it hits a torn tail (a short read, i.e. a write interrupted by
//! a crash) or a CRC mismatch, at which point the valid prefix of the log is
//! everything read so far. Each record carries a monotonically increasing `seq`.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::catalog::{IndexDef, Table};
use crate::error::{Result, SqlError};
use crate::types::Value;

const WAL_MAGIC: &[u8; 4] = b"OXSW";
const WAL_VERSION: u16 = 1;
const HEADER_LEN: u64 = 8;

/// A single logical mutation recorded before it is applied.
///
/// All variants are **idempotent by identity** (table name + `row_id`) so that
/// replaying the WAL over a checkpoint snapshot converges to the same state even
/// if some records were already materialized before a crash.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalRecord {
    CreateTable(Table),
    DropTable(String),
    CreateIndex(IndexDef),
    DropIndex(String),
    CreateView {
        name: String,
        sql: String,
    },
    DropView(String),
    Insert {
        table: String,
        row_id: u64,
        cells: Vec<Value>,
    },
    Delete {
        table: String,
        row_id: u64,
    },
    /// An atomic group of records committed together by a transaction. On
    /// replay it is applied whole; a torn/corrupt batch is discarded whole
    /// (the CRC covers the entire batch), giving all-or-nothing durability.
    Batch(Vec<WalRecord>),
}

/// How WAL appends are made durable.
///
/// `Full` (default) is a true storage flush (`File::sync_all` — on macOS an
/// `F_FULLFSYNC`, surviving power loss). `Data` uses `File::sync_data`
/// (`fdatasync` on Linux, a barrier fsync on macOS) — the same durability
/// class as PostgreSQL's default `wal_sync_method`, several times faster per
/// commit but not power-loss-proof on hardware with volatile write caches.
/// Selected via `OXIDB_SQL_SYNC` = `full` (default) | `data`.
#[derive(Clone, Copy, PartialEq)]
enum SyncMode {
    Full,
    Data,
}

fn sync_mode_from_env() -> SyncMode {
    match std::env::var("OXIDB_SQL_SYNC")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "data" | "datasync" | "barrier" => SyncMode::Data,
        _ => SyncMode::Full,
    }
}

/// The `Data` sync: `File::sync_data` everywhere except macOS, where Rust's
/// `sync_data` still issues `F_FULLFSYNC` (Apple's `fdatasync` does not flush
/// the drive cache, so std plays safe). Here `Data` explicitly means the
/// PostgreSQL-default durability class — an OS-cache-level `fsync(2)` — so on
/// macOS we call it directly.
fn sync_data_fast(file: &File) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_data()
    }
}

/// Append-only WAL writer bound to `sql/wal/live.wal`.
pub struct Wal {
    file: File,
    next_seq: u64,
    sync: SyncMode,
    /// Current on-disk size (header + valid records) — drives auto-checkpoint.
    bytes: u64,
}

impl Wal {
    /// Open (creating if needed) the WAL under `dir` (`dir` is the SQL root),
    /// replaying existing records. Returns the writer and the replayed records
    /// in log order.
    pub fn open(dir: &Path) -> Result<(Wal, Vec<WalRecord>)> {
        let wal_dir = dir.join("wal");
        fs::create_dir_all(&wal_dir)?;
        let path = wal_dir.join("live.wal");

        let (records, valid_end, max_seq) = Self::replay(&path)?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let bytes = if file.metadata()?.len() == 0 {
            Self::write_header(&mut file)?;
            HEADER_LEN
        } else {
            // Discard any torn tail so the next append starts from clean data.
            file.set_len(valid_end)?;
            valid_end
        };
        file.seek(SeekFrom::End(0))?;

        let wal = Wal {
            file,
            next_seq: max_seq + 1,
            sync: sync_mode_from_env(),
            bytes,
        };
        Ok((wal, records))
    }

    /// Current on-disk size of the live WAL in bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    fn write_header(file: &mut File) -> Result<()> {
        let mut hdr = Vec::with_capacity(HEADER_LEN as usize);
        hdr.extend_from_slice(WAL_MAGIC);
        hdr.extend_from_slice(&WAL_VERSION.to_le_bytes());
        hdr.extend_from_slice(&0u16.to_le_bytes()); // flags
        file.write_all(&hdr)?;
        file.sync_all()?;
        Ok(())
    }

    /// Read all valid records. Returns `(records, valid_end_offset, max_seq)`.
    /// `valid_end_offset` is the byte offset just past the last intact record,
    /// used to trim a torn tail.
    fn replay(path: &Path) -> Result<(Vec<WalRecord>, u64, u64)> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0, 0)),
            Err(e) => return Err(e.into()),
        };
        let len = file.metadata()?.len();
        if len < HEADER_LEN {
            return Ok((Vec::new(), 0, 0));
        }
        let mut reader = BufReader::new(file);

        let mut header = [0u8; HEADER_LEN as usize];
        reader.read_exact(&mut header)?;
        if &header[0..4] != WAL_MAGIC {
            return Err(SqlError::Corrupt("bad WAL magic".into()));
        }
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != WAL_VERSION {
            return Err(SqlError::Corrupt(format!("unknown WAL version {version}")));
        }

        let mut records = Vec::new();
        let mut offset = HEADER_LEN;
        let mut max_seq = 0u64;

        loop {
            // Read the fixed frame prefix: crc(4) + len(4) + seq(8).
            let mut prefix = [0u8; 16];
            match read_full(&mut reader, &mut prefix) {
                ReadOutcome::Full => {}
                ReadOutcome::Eof => break,  // clean end
                ReadOutcome::Torn => break, // partial frame prefix -> torn tail
            }
            let crc = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
            let plen = u32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]) as usize;
            let seq = u64::from_le_bytes(prefix[8..16].try_into().unwrap());

            let mut payload = vec![0u8; plen];
            match read_full(&mut reader, &mut payload) {
                ReadOutcome::Full => {}
                ReadOutcome::Eof | ReadOutcome::Torn => break, // torn payload
            }

            // Verify CRC over seq bytes + payload.
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&prefix[8..16]);
            hasher.update(&payload);
            if hasher.finalize() != crc {
                break; // corrupt record -> treat as end of valid log
            }

            let rec: WalRecord = serde_json::from_slice(&payload)?;
            records.push(rec);
            max_seq = max_seq.max(seq);
            offset += 16 + plen as u64;
        }

        Ok((records, offset, max_seq))
    }

    /// Append a record and fsync. Returns the sequence number assigned.
    pub fn append(&mut self, rec: &WalRecord) -> Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let payload = serde_json::to_vec(rec)?;
        let mut hasher = crc32fast::Hasher::new();
        let seq_bytes = seq.to_le_bytes();
        hasher.update(&seq_bytes);
        hasher.update(&payload);
        let crc = hasher.finalize();

        let mut frame = Vec::with_capacity(16 + payload.len());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&seq_bytes);
        frame.extend_from_slice(&payload);

        self.file.write_all(&frame)?;
        match self.sync {
            SyncMode::Full => self.file.sync_all()?,
            SyncMode::Data => sync_data_fast(&self.file)?,
        }
        self.bytes += frame.len() as u64;
        Ok(seq)
    }

    /// Reset the WAL to an empty (header-only) state after a checkpoint has
    /// durably captured all prior records into the `.rdat` snapshots.
    pub fn truncate(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Self::write_header(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;
        self.bytes = HEADER_LEN;
        Ok(())
    }
}

enum ReadOutcome {
    Full,
    Eof,
    Torn,
}

/// Fill `buf` completely; distinguish a clean EOF (nothing read) from a torn
/// read (some but not all bytes available).
fn read_full<R: Read>(reader: &mut R, buf: &mut [u8]) -> ReadOutcome {
    let mut read = 0;
    while read < buf.len() {
        match reader.read(&mut buf[read..]) {
            Ok(0) => {
                return if read == 0 {
                    ReadOutcome::Eof
                } else {
                    ReadOutcome::Torn
                };
            }
            Ok(n) => read += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return ReadOutcome::Torn,
        }
    }
    ReadOutcome::Full
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, Table};
    use crate::types::SqlType;

    fn rec_create() -> WalRecord {
        WalRecord::CreateTable(Table::new(
            "t",
            vec![Column::new("id", SqlType::Int).primary_key()],
        ))
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, replayed) = Wal::open(dir.path()).unwrap();
            assert!(replayed.is_empty());
            wal.append(&rec_create()).unwrap();
            wal.append(&WalRecord::Insert {
                table: "t".into(),
                row_id: 1,
                cells: vec![Value::Int(7)],
            })
            .unwrap();
        }
        let (_wal, replayed) = Wal::open(dir.path()).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0], rec_create());
    }

    #[test]
    fn torn_tail_is_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path()).unwrap();
            wal.append(&rec_create()).unwrap();
        }
        // Simulate a crash mid-write: append garbage bytes (a partial frame).
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(dir.path().join("wal").join("live.wal"))
                .unwrap();
            f.write_all(&[0xAB, 0xCD, 0xEF]).unwrap(); // 3 bytes: shorter than a frame prefix
            f.sync_all().unwrap();
        }
        let (_wal, replayed) = Wal::open(dir.path()).unwrap();
        assert_eq!(replayed.len(), 1, "torn tail must be ignored");
    }

    #[test]
    fn truncate_clears_log() {
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, _) = Wal::open(dir.path()).unwrap();
        wal.append(&rec_create()).unwrap();
        wal.truncate().unwrap();
        drop(wal);
        let (_wal, replayed) = Wal::open(dir.path()).unwrap();
        assert!(replayed.is_empty());
    }
}
