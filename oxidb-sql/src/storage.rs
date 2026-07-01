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
use crate::types::{Value, decode_row, encode_row};

const RDAT_MAGIC: &[u8; 4] = b"OXSR";
const RDAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 8;

/// Path of a table's snapshot file within the SQL root `dir`.
pub fn rdat_path(dir: &Path, table: &str) -> PathBuf {
    dir.join(format!("{table}.rdat"))
}

/// Atomically write a table snapshot containing `rows` (each `(row_id, cells)`).
pub fn write_snapshot<'a, I>(dir: &Path, table: &str, rows: I) -> Result<()>
where
    I: IntoIterator<Item = (u64, &'a [Value])>,
{
    let path = rdat_path(dir, table);
    let tmp = path.with_extension("rdat.tmp");

    let mut buf = Vec::with_capacity(HEADER_LEN + 64);
    buf.extend_from_slice(RDAT_MAGIC);
    buf.extend_from_slice(&RDAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags

    for (row_id, cells) in rows {
        let payload = encode_row(cells);
        let crc = crc32fast::hash(&payload);
        buf.extend_from_slice(&row_id.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&payload);
    }

    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
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

/// Delete a table's snapshot file if present (used when a table is dropped).
pub fn remove_snapshot(dir: &Path, table: &str) -> Result<()> {
    let path = rdat_path(dir, table);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
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
