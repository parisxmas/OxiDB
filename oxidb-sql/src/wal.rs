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

/// A WAL replay result: the `(seq, record)` pairs, the byte offset past the
/// last intact record (for trimming a torn tail), and the highest seq seen.
type Replayed = (Vec<(u64, WalRecord)>, u64, u64);

/// A single logical mutation recorded before it is applied.
///
/// All variants are **idempotent by identity** (table name + `row_id`) so that
/// replaying the WAL over a checkpoint snapshot converges to the same state even
/// if some records were already materialized before a crash.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalRecord {
    /// `ALTER TABLE` (ADR-0013 Phase D). Replayed in log order, so records
    /// before it carry the old shape and records after it the new one.
    AlterTable {
        table: String,
        op: crate::ast::AlterOp,
    },
    CreateTable(Table),
    DropTable(String),
    CreateIndex(IndexDef),
    DropIndex(String),
    CreateView {
        name: String,
        sql: String,
    },
    DropView(String),
    CreateProcedure {
        name: String,
        def: crate::catalog::ProcedureDef,
    },
    DropProcedure(String),
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

/// Opt-in WAL pre-allocation chunk, in bytes (0 = disabled). Set
/// `OXIDB_SQL_WAL_PREALLOC=<MiB>` to enable: the WAL is then grown to a
/// pre-sized, block-allocated file so an append writes *within* it and an
/// `fdatasync` flushes only data, not the inode's size — the size-metadata
/// write that makes a growing WAL's sync ~2x slower than PostgreSQL's
/// (which recycles pre-sized 16 MiB segments). Off by default: the file then
/// tracks the logical size exactly (leaner for many small multi-db WALs).
fn prealloc_chunk_from_env() -> u64 {
    std::env::var("OXIDB_SQL_WAL_PREALLOC")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&mib| mib > 0)
        .map(|mib| mib * 1024 * 1024)
        .unwrap_or(0)
}

/// Grow the file's on-disk size to `cap`, allocating real blocks where the OS
/// supports it (Linux `posix_fallocate`), so a later write inside `[0, cap)`
/// touches no size metadata. Existing bytes are untouched; the newly-allocated
/// tail reads as zeros — which recovery treats as end-of-log (a zero frame
/// fails the CRC check). Falls back to a plain `set_len` (sparse) off Linux:
/// correct everywhere, fast where WAL latency actually matters.
#[cfg(all(unix, target_os = "linux"))]
fn preallocate_to(file: &File, cap: u64) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    let rc = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, cap as libc::off_t) };
    if rc != 0 {
        return Err(std::io::Error::from_raw_os_error(rc).into());
    }
    Ok(())
}
#[cfg(not(all(unix, target_os = "linux")))]
fn preallocate_to(file: &File, cap: u64) -> Result<()> {
    file.set_len(cap)?;
    Ok(())
}

/// Append-only WAL writer bound to `sql/wal/live.wal`.
pub struct Wal {
    file: File,
    next_seq: u64,
    sync: SyncMode,
    /// Logical size — header + valid records (the write position). Drives
    /// auto-checkpoint; equals the file size unless pre-allocation is on.
    bytes: u64,
    /// Pre-allocation chunk in bytes (0 = disabled). See
    /// [`prealloc_chunk_from_env`].
    chunk: u64,
    /// Pre-allocated on-disk size when `chunk > 0` (else unused). Appends stay
    /// within it; it grows by `chunk` when the next frame would cross it.
    capacity: u64,
}

impl Wal {
    /// Open the WAL, returning only the records whose sequence is strictly
    /// greater than `min_seq` — the checkpoint watermark from the manifest.
    /// Records at or below it are already folded into the loaded snapshots, so
    /// replaying them would double-apply (and is unnecessary). The writer's
    /// next sequence still continues past the highest seq *in the file*, so
    /// sequences stay monotonic even across an untruncated WAL.
    pub fn open_since(dir: &Path, min_seq: u64) -> Result<(Wal, Vec<WalRecord>)> {
        let wal_dir = dir.join("wal");
        fs::create_dir_all(&wal_dir)?;
        let path = wal_dir.join("live.wal");

        let (all, valid_end, max_seq) = Self::replay(&path)?;
        let records: Vec<WalRecord> = all
            .into_iter()
            .filter(|(seq, _)| *seq > min_seq)
            .map(|(_, rec)| rec)
            .collect();

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
            // Discard any torn tail (or a prior pre-allocated zero region) so
            // the pre-allocation below overlays clean zeros right after the
            // last intact record.
            file.set_len(valid_end)?;
            valid_end
        };
        // With pre-allocation on, size the file ahead and position the writer
        // at the *logical* end (not the file end — now the pre-allocated size).
        let chunk = prealloc_chunk_from_env();
        let capacity = if chunk > 0 {
            let cap = ((bytes / chunk) + 1) * chunk;
            preallocate_to(&file, cap)?;
            file.seek(SeekFrom::Start(bytes))?;
            cap
        } else {
            file.seek(SeekFrom::End(0))?;
            0
        };

        let wal = Wal {
            file,
            next_seq: max_seq + 1,
            sync: sync_mode_from_env(),
            bytes,
            chunk,
            capacity,
        };
        Ok((wal, records))
    }

    /// Current on-disk size of the live WAL in bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The highest sequence appended so far (0 if none). A checkpoint records
    /// this as the manifest watermark: its snapshots reflect every record up to
    /// and including it.
    pub fn last_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
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

    /// Read all valid records as `(seq, record)`. Returns `(records,
    /// valid_end_offset, max_seq)`. `valid_end_offset` is the byte offset just
    /// past the last intact record, used to trim a torn tail.
    fn replay(path: &Path) -> Result<Replayed> {
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
            records.push((seq, rec));
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

        // Grow the pre-allocation if this frame would cross it, then write
        // inside the pre-sized file at the logical write position — so the sync
        // flushes data only, not the inode size.
        let frame_len = frame.len() as u64;
        if self.chunk > 0 {
            if self.bytes + frame_len > self.capacity {
                self.capacity = (((self.bytes + frame_len) / self.chunk) + 1) * self.chunk;
                preallocate_to(&self.file, self.capacity)?;
            }
            self.file.seek(SeekFrom::Start(self.bytes))?;
        }
        self.file.write_all(&frame)?;
        match self.sync {
            SyncMode::Full => self.file.sync_all()?,
            SyncMode::Data => sync_data_fast(&self.file)?,
        }
        self.bytes += frame_len;
        Ok(seq)
    }

    /// Reset the WAL to an empty (header-only) state after a checkpoint has
    /// durably captured all prior records into the `.rdat` snapshots.
    pub fn truncate(&mut self) -> Result<()> {
        // Truncate to nothing (dropping old records), rewrite the header, then
        // re-establish the pre-allocated zero tail so appends stay in-place.
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Self::write_header(&mut self.file)?;
        self.bytes = HEADER_LEN;
        if self.chunk > 0 {
            self.capacity = self.chunk;
            preallocate_to(&self.file, self.capacity)?;
        }
        self.file.seek(SeekFrom::Start(self.bytes))?;
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
            let (mut wal, replayed) = Wal::open_since(dir.path(), 0).unwrap();
            assert!(replayed.is_empty());
            wal.append(&rec_create()).unwrap();
            wal.append(&WalRecord::Insert {
                table: "t".into(),
                row_id: 1,
                cells: vec![Value::Int(7)],
            })
            .unwrap();
        }
        let (_wal, replayed) = Wal::open_since(dir.path(), 0).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0], rec_create());
    }

    #[test]
    fn prealloc_writes_within_and_recovers() {
        // SAFETY: sibling WAL tests are size-agnostic (they assert record
        // counts, not file sizes), so a transient env here is benign under
        // parallel execution.
        unsafe { std::env::set_var("OXIDB_SQL_WAL_PREALLOC", "1") };
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal").join("live.wal");
        {
            let (mut wal, replayed) = Wal::open_since(dir.path(), 0).unwrap();
            assert!(replayed.is_empty());
            for _ in 0..5 {
                wal.append(&rec_create()).unwrap();
            }
        }
        // The file is pre-allocated far past the five tiny records.
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() >= 1024 * 1024,
            "WAL pre-allocated to >= 1 MiB"
        );
        // Reopen: the zero tail after the records must not read back as records,
        // and an append after recovery must still land correctly.
        {
            let (mut wal, replayed) = Wal::open_since(dir.path(), 0).unwrap();
            assert_eq!(
                replayed.len(),
                5,
                "records survive; pre-allocated zeros ignored"
            );
            wal.append(&rec_create()).unwrap();
        }
        let (_wal, replayed) = Wal::open_since(dir.path(), 0).unwrap();
        assert_eq!(replayed.len(), 6);
        unsafe { std::env::set_var("OXIDB_SQL_WAL_PREALLOC", "") };
    }

    #[test]
    fn torn_tail_is_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open_since(dir.path(), 0).unwrap();
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
        let (_wal, replayed) = Wal::open_since(dir.path(), 0).unwrap();
        assert_eq!(replayed.len(), 1, "torn tail must be ignored");
    }

    #[test]
    fn truncate_clears_log() {
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, _) = Wal::open_since(dir.path(), 0).unwrap();
        wal.append(&rec_create()).unwrap();
        wal.truncate().unwrap();
        drop(wal);
        let (_wal, replayed) = Wal::open_since(dir.path(), 0).unwrap();
        assert!(replayed.is_empty());
    }
}
