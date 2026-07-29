//! Row-oriented `.rdat` snapshot files.
//!
//! Each table's live rows are materialized into `sql/<table>.rdat` at checkpoint
//! time. A snapshot is written whole (temp file + fsync + rename), so it is
//! always a consistent point-in-time image; incremental durability between
//! checkpoints is provided by the WAL, not these files.
//!
//! ```text
//! header:  [b"OXSR" (4)][version: u16 LE][flags: u16 LE]
//! record:  [row_id: u64 LE][len: u32 LE][crc32: u32 LE][payload: len bytes]
//! ```
//!
//! `payload` is the typed-row encoding from [`crate::types::encode_row`]; `crc32`
//! covers the payload only.

use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Result, SqlError};
use crate::types::{Value, decode_row, decode_row_into, encode_row};

const RDAT_MAGIC: &[u8; 4] = b"OXSR";
const RDAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 8;

/// Path of a table's snapshot file within the SQL root `dir`.
pub fn rdat_path(dir: &Path, table: &str) -> PathBuf {
    dir.join(format!("{table}.rdat"))
}

/// Atomically write a table snapshot containing `rows` (each `(row_id, cells)`).
pub fn write_snapshot<I, C>(dir: &Path, table: &str, rows: I) -> Result<()>
where
    I: IntoIterator<Item = (u64, C)>,
    C: std::borrow::Borrow<[Value]>,
{
    let path = rdat_path(dir, table);
    let tmp = path.with_extension("rdat.tmp");

    // Streamed through a BufWriter rather than assembled in one `Vec`. Building
    // the whole file in memory first cost a second copy of the table at every
    // checkpoint — invisible on an unconstrained machine, fatal in a cgroup: a
    // 1.2M-row database could not open inside a 256 MB limit that it otherwise
    // runs in comfortably, because opening replays the WAL tail and checkpoints.
    {
        let f = File::create(&tmp)?;
        let mut w = std::io::BufWriter::with_capacity(1 << 16, f);
        w.write_all(RDAT_MAGIC)?;
        w.write_all(&RDAT_VERSION.to_le_bytes())?;
        w.write_all(&0u16.to_le_bytes())?; // flags

        for (row_id, cells) in rows {
            let payload = encode_row(cells.borrow());
            let crc = crc32fast::hash(&payload);
            w.write_all(&row_id.to_le_bytes())?;
            w.write_all(&(payload.len() as u32).to_le_bytes())?;
            w.write_all(&crc.to_le_bytes())?;
            w.write_all(&payload)?;
        }
        let f = w.into_inner().map_err(|e| e.into_error())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    Ok(())
}

/// Read a table snapshot. `ncols` is the table arity (from the catalog), needed
/// to decode each fixed-schema row. Returns rows in file order.
///
/// A missing file yields an empty vector (the table has never been checkpointed).
pub fn read_snapshot(dir: &Path, table: &str, ncols: usize) -> Result<Vec<(u64, Vec<Value>)>> {
    let path = rdat_path(dir, table);
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut reader = BufReader::new(file);

    let mut header = [0u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    if &header[0..4] != RDAT_MAGIC {
        return Err(SqlError::Corrupt(format!("bad .rdat magic for {table:?}")));
    }
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != RDAT_VERSION {
        return Err(SqlError::Corrupt(format!(
            "unknown .rdat version {version} for {table:?}"
        )));
    }

    let mut rows = Vec::new();
    loop {
        let mut prefix = [0u8; 16]; // row_id(8) + len(4) + crc(4)
        match reader.read_exact(&mut prefix) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let row_id = u64::from_le_bytes(prefix[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(prefix[8..12].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(prefix[12..16].try_into().unwrap());

        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;
        if crc32fast::hash(&payload) != crc {
            return Err(SqlError::Corrupt(format!(
                "crc mismatch in {table:?} snapshot (row {row_id})"
            )));
        }
        let cells = decode_row(&payload, ncols)?;
        rows.push((row_id, cells));
    }
    Ok(rows)
}

/// A table snapshot mapped read-only into memory (disk-first mode).
///
/// Record CRCs are verified once at open; reads afterwards decode straight
/// from the mapping without re-hashing. The mapping pins the file's *inode*,
/// so a later checkpoint atomically renaming a new snapshot over the path
/// does not invalidate an existing `MappedSnapshot`.
pub struct MappedSnapshot {
    mmap: memmap2::Mmap,
    /// `(row_id, payload offset, payload len)`, ascending by `row_id`.
    index: Vec<(u64, u64, u32)>,
    arity: usize,
}

impl MappedSnapshot {
    /// Map a table's snapshot. `Ok(None)` when the table has never been
    /// checkpointed (no file).
    pub fn open(dir: &Path, table: &str, arity: usize) -> Result<Option<MappedSnapshot>> {
        let path = rdat_path(dir, table);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Safety: the file is only ever replaced via rename (never written in
        // place), so the mapped inode's contents are immutable.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        let bytes: &[u8] = &mmap;
        if bytes.len() < HEADER_LEN || &bytes[0..4] != RDAT_MAGIC {
            return Err(SqlError::Corrupt(format!("bad .rdat magic for {table:?}")));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != RDAT_VERSION {
            return Err(SqlError::Corrupt(format!(
                "unknown .rdat version {version} for {table:?}"
            )));
        }

        let mut index = Vec::new();
        let mut pos = HEADER_LEN;
        while pos < bytes.len() {
            let prefix = bytes.get(pos..pos + 16).ok_or_else(|| {
                SqlError::Corrupt(format!("truncated record header in {table:?} snapshot"))
            })?;
            let row_id = u64::from_le_bytes(prefix[0..8].try_into().unwrap());
            let len = u32::from_le_bytes(prefix[8..12].try_into().unwrap());
            let crc = u32::from_le_bytes(prefix[12..16].try_into().unwrap());
            let start = pos + 16;
            let end = start + len as usize;
            let payload = bytes.get(start..end).ok_or_else(|| {
                SqlError::Corrupt(format!("truncated payload in {table:?} snapshot"))
            })?;
            if crc32fast::hash(payload) != crc {
                return Err(SqlError::Corrupt(format!(
                    "crc mismatch in {table:?} snapshot (row {row_id})"
                )));
            }
            index.push((row_id, start as u64, len));
            pos = end;
        }
        // `write_snapshot` emits rows in ascending row_id order, but sort
        // defensively — the binary searches below depend on it.
        index.sort_unstable_by_key(|&(id, _, _)| id);

        Ok(Some(MappedSnapshot { mmap, index, arity }))
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn contains(&self, row_id: u64) -> bool {
        self.index
            .binary_search_by_key(&row_id, |&(id, _, _)| id)
            .is_ok()
    }

    /// Decode one row by id.
    pub fn get(&self, row_id: u64) -> Option<Vec<Value>> {
        let i = self
            .index
            .binary_search_by_key(&row_id, |&(id, _, _)| id)
            .ok()?;
        Some(self.decode_at(i))
    }

    /// All rows in ascending `row_id` order, decoded on the fly.
    pub fn entries(&self) -> impl Iterator<Item = (u64, Vec<Value>)> + '_ {
        (0..self.index.len()).map(|i| (self.index[i].0, self.decode_at(i)))
    }

    /// The `row_id` at position `i` of the (sorted) index.
    pub fn row_id_at(&self, i: usize) -> u64 {
        self.index[i].0
    }

    /// Decode the row at position `i` of the index. CRC was verified at open,
    /// so a decode failure here is snapshot corruption after the fact.
    pub fn decode_at(&self, i: usize) -> Vec<Value> {
        let mut buf = Vec::new();
        self.decode_at_into(i, &mut buf);
        buf
    }

    /// [`decode_at`](MappedSnapshot::decode_at) into a reused buffer, so a scan
    /// does not allocate a row at a time.
    pub fn decode_at_into(&self, i: usize, buf: &mut Vec<Value>) {
        let (row_id, off, len) = self.index[i];
        let payload = &self.mmap[off as usize..off as usize + len as usize];
        decode_row_into(payload, self.arity, buf)
            .unwrap_or_else(|e| panic!("snapshot row {row_id} failed to decode: {e}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let r1 = vec![Value::Int(1), Value::Text("a".into())];
        let r2 = vec![Value::Int(2), Value::Null];
        let rows: Vec<(u64, &[Value])> = vec![(10, &r1), (11, &r2)];
        write_snapshot(dir.path(), "t", rows).unwrap();

        let back = read_snapshot(dir.path(), "t", 2).unwrap();
        assert_eq!(back, vec![(10, r1), (11, r2)]);
    }

    #[test]
    fn missing_snapshot_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_snapshot(dir.path(), "nope", 1).unwrap().is_empty());
    }
}
