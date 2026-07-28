//! Disk-backed secondary indexes: sorted `.sidx` files, mmap'd and searched in
//! place.
//!
//! An in-memory `BTreeMap<KeyTuple, RowIds>` costs about 106 bytes per row
//! entry and never shrinks, which made indexes the largest resident structure
//! in the engine. PostgreSQL spends about 20 bytes per entry and spends it *on
//! disk*, in pages its buffer pool may evict (`src/include/access/itup.h`,
//! `src/backend/storage/buffer/README`). This module is the same trade at the
//! granularity OxiDB already uses for rows: the bulk lives in a file the OS can
//! page out, and only changes since the last checkpoint are resident.
//!
//! The layout is deliberately the one the document engine's `.mcidx` proved —
//! a **sorted entry table**, not a B-tree. Every writer here rewrites the file
//! whole at checkpoint time, so there is nothing to split, no free-space map,
//! and no page-level concurrency to get wrong; a lookup is a binary search over
//! a fixed-stride table, and a range scan is a lower bound plus a walk.
//!
//! ```text
//! header:      [b"OXSX"(4)][version u16][slots u16][entries u64][keys_len u64]
//! entry table: entries × [key_off u32][key_len u32][ids_at u32][ids_len u32]
//!              `key_off` is a byte offset into the key blob; `ids_at` and
//!              `ids_len` count row ids, not bytes
//!              sorted ascending by decoded key tuple
//! key blob:    encode_row(key tuple) per entry
//! ids blob:    row ids, u64 LE, ascending within an entry
//! ```
//!
//! Keys reuse [`crate::types::encode_row`], so the file inherits the row
//! encoding's type handling rather than inventing a second one. That encoding
//! is *not* order-preserving, so the binary search decodes a key per probe —
//! about twenty decodes for a million entries, against a lookup that then has
//! to read rows anyway.
//!
//! ## The base is a hint, not the truth
//!
//! A `.sidx` describes the rows as they were at the last checkpoint. Rows
//! written, changed or deleted since then are not in it. Rather than maintain
//! tombstones, callers **verify** each candidate against the live row — a
//! deleted row is gone from the store, and a row whose indexed columns changed
//! no longer matches the key. Verification costs one comparison on a row the
//! caller was about to read regardless, and it makes a stale base self-
//! correcting instead of wrong.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Result, SqlError};
use crate::types::{IndexKey, KeyTuple, Value, decode_row, encode_row};

const SIDX_MAGIC: &[u8; 4] = b"OXSX";
const SIDX_VERSION: u16 = 1;
const HEADER_LEN: usize = 24;
/// key_off + key_len + ids_off + ids_len.
const ENTRY_SIZE: usize = 16;

/// Path of one index's file within a generation directory.
pub fn sidx_path(dir: &Path, table: &str, index: &str) -> PathBuf {
    dir.join(format!("{table}.{index}.sidx"))
}

/// Write `entries` — which **must** be sorted ascending by key — as a `.sidx`.
///
/// Atomic: built in memory, written to a temp file, fsynced, then renamed, so a
/// crash mid-checkpoint leaves the previous generation's file untouched.
pub fn write_index<'a, I>(
    dir: &Path,
    table: &str,
    index: &str,
    slots: usize,
    entries: I,
) -> Result<()>
where
    I: IntoIterator<Item = (&'a KeyTuple, &'a [u64])>,
{
    let mut table_buf: Vec<u8> = Vec::new();
    let mut keys: Vec<u8> = Vec::new();
    let mut ids: Vec<u8> = Vec::new();
    let mut count: u64 = 0;

    for (key, row_ids) in entries {
        // `IndexKey` is a transparent newtype over `Value`, but the codec takes
        // values, so unwrap the tuple for encoding.
        let cells: Vec<Value> = key.iter().map(|k| k.0.clone()).collect();
        let encoded = encode_row(&cells);
        let key_off = keys.len() as u32;
        let key_len = encoded.len() as u32;
        keys.extend_from_slice(&encoded);

        // Element index, not a byte offset — the reader scales by 8.
        let ids_off = (ids.len() / 8) as u32;
        for id in row_ids {
            ids.extend_from_slice(&id.to_le_bytes());
        }
        let ids_len = row_ids.len() as u32;

        table_buf.extend_from_slice(&key_off.to_le_bytes());
        table_buf.extend_from_slice(&key_len.to_le_bytes());
        table_buf.extend_from_slice(&ids_off.to_le_bytes());
        table_buf.extend_from_slice(&ids_len.to_le_bytes());
        count += 1;
    }

    let mut buf = Vec::with_capacity(HEADER_LEN + table_buf.len() + keys.len() + ids.len());
    buf.extend_from_slice(SIDX_MAGIC);
    buf.extend_from_slice(&SIDX_VERSION.to_le_bytes());
    buf.extend_from_slice(&(slots as u16).to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&(keys.len() as u64).to_le_bytes());
    buf.extend_from_slice(&table_buf);
    buf.extend_from_slice(&keys);
    buf.extend_from_slice(&ids);

    let path = sidx_path(dir, table, index);
    let tmp = path.with_extension("sidx.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    Ok(())
}

/// A `.sidx` mapped into the address space.
///
/// Holds no per-entry Rust structure — the entry table is read straight out of
/// the mapping — so an index of any size costs one `Mmap` plus the pages the OS
/// decides to keep, which it may drop under pressure.
pub struct MappedIndex {
    mmap: memmap2::Mmap,
    entries: usize,
    slots: usize,
    table_off: usize,
    keys_off: usize,
    ids_off: usize,
}

impl MappedIndex {
    /// Map an index file. `Ok(None)` when it has never been checkpointed.
    pub fn open(dir: &Path, table: &str, index: &str) -> Result<Option<MappedIndex>> {
        let path = sidx_path(dir, table, index);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Safety: `.sidx` files are only ever replaced by rename, never written
        // in place, so a live mapping's bytes are immutable — the same argument
        // `MappedSnapshot` relies on.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let bytes: &[u8] = &mmap;
        if bytes.len() < HEADER_LEN || &bytes[0..4] != SIDX_MAGIC {
            return Err(SqlError::Corrupt(format!(
                "bad .sidx magic for {table:?}.{index:?}"
            )));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != SIDX_VERSION {
            return Err(SqlError::Corrupt(format!(
                "unknown .sidx version {version} for {table:?}.{index:?}"
            )));
        }
        let slots = u16::from_le_bytes([bytes[6], bytes[7]]) as usize;
        let entries = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let keys_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let table_off = HEADER_LEN;
        let keys_off = table_off + entries * ENTRY_SIZE;
        let ids_off = keys_off + keys_len;
        if ids_off > bytes.len() {
            return Err(SqlError::Corrupt(format!(
                "truncated .sidx entry table for {table:?}.{index:?}"
            )));
        }
        Ok(Some(MappedIndex {
            mmap,
            entries,
            slots,
            table_off,
            keys_off,
            ids_off,
        }))
    }

    /// Entry count — distinct keys, not row ids. Used by the format's own
    /// tests; the engine only ever asks for a key.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.entries
    }

    fn slot(&self, i: usize) -> (u32, u32, u32, u32) {
        let b = &self.mmap[self.table_off + i * ENTRY_SIZE..];
        (
            u32::from_le_bytes(b[0..4].try_into().unwrap()),
            u32::from_le_bytes(b[4..8].try_into().unwrap()),
            u32::from_le_bytes(b[8..12].try_into().unwrap()),
            u32::from_le_bytes(b[12..16].try_into().unwrap()),
        )
    }

    /// The decoded key of entry `i`.
    fn key_at(&self, i: usize) -> Result<KeyTuple> {
        let (key_off, key_len, _, _) = self.slot(i);
        let start = self.keys_off + key_off as usize;
        let bytes = self
            .mmap
            .get(start..start + key_len as usize)
            .ok_or_else(|| SqlError::Corrupt("truncated .sidx key".into()))?;
        Ok(decode_row(bytes, self.slots)?
            .into_iter()
            .map(IndexKey)
            .collect())
    }

    /// The row ids of entry `i`, read straight from the mapping.
    fn ids_at(&self, i: usize) -> impl Iterator<Item = u64> + '_ {
        let (_, _, ids_off, ids_len) = self.slot(i);
        let start = self.ids_off + ids_off as usize * 8;
        (0..ids_len as usize).map(move |k| {
            let o = start + k * 8;
            u64::from_le_bytes(self.mmap[o..o + 8].try_into().unwrap())
        })
    }

    /// Index of the first entry whose key is `>= key`.
    fn lower_bound(&self, key: &[IndexKey]) -> Result<usize> {
        let (mut lo, mut hi) = (0usize, self.entries);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.key_at(mid)?.as_slice() < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Row ids recorded for exactly `key` at the last checkpoint. Callers must
    /// verify each against the live row — see the module note.
    pub fn get(&self, key: &[IndexKey]) -> Result<Vec<u64>> {
        let at = self.lower_bound(key)?;
        if at >= self.entries || self.key_at(at)?.as_slice() != key {
            return Ok(Vec::new());
        }
        Ok(self.ids_at(at).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(vals: &[Value]) -> KeyTuple {
        vals.iter().cloned().map(IndexKey).collect()
    }

    fn roundtrip(dir: &Path, entries: &[(KeyTuple, Vec<u64>)], slots: usize) -> MappedIndex {
        let refs: Vec<(&KeyTuple, &[u64])> =
            entries.iter().map(|(k, v)| (k, v.as_slice())).collect();
        write_index(dir, "t", "i", slots, refs).unwrap();
        MappedIndex::open(dir, "t", "i").unwrap().unwrap()
    }

    #[test]
    fn finds_every_key_it_was_given() {
        let dir = tempfile::tempdir().unwrap();
        let mut entries: Vec<(KeyTuple, Vec<u64>)> = (0..500)
            .map(|i| (key(&[Value::Int(i)]), vec![i as u64 * 2, i as u64 * 2 + 1]))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let idx = roundtrip(dir.path(), &entries, 1);

        assert_eq!(idx.len(), 500);
        for (k, ids) in &entries {
            assert_eq!(&idx.get(k).unwrap(), ids, "wrong ids for {k:?}");
        }
        // A key that was never written reads as empty, not as a neighbour's ids.
        assert!(idx.get(&key(&[Value::Int(9999)])).unwrap().is_empty());
        assert!(idx.get(&key(&[Value::Int(-1)])).unwrap().is_empty());
    }

    #[test]
    fn handles_text_and_composite_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut entries: Vec<(KeyTuple, Vec<u64>)> = (0..200)
            .map(|i| {
                (
                    key(&[Value::Text(format!("k{i:04}").into()), Value::Int(i % 7)]),
                    vec![i as u64],
                )
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let idx = roundtrip(dir.path(), &entries, 2);
        for (k, ids) in &entries {
            assert_eq!(&idx.get(k).unwrap(), ids, "wrong ids for {k:?}");
        }
        // Same first slot, absent second slot.
        assert!(
            idx.get(&key(&[Value::Text("k0001".into()), Value::Int(99)]))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn an_empty_index_is_valid() {
        let dir = tempfile::tempdir().unwrap();
        let idx = roundtrip(dir.path(), &[], 1);
        assert_eq!(idx.len(), 0);
        assert!(idx.get(&key(&[Value::Int(1)])).unwrap().is_empty());
    }

    #[test]
    fn a_missing_file_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            MappedIndex::open(dir.path(), "nope", "nope")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn nulls_and_mixed_types_survive() {
        let dir = tempfile::tempdir().unwrap();
        let mut entries: Vec<(KeyTuple, Vec<u64>)> = vec![
            (key(&[Value::Null]), vec![1]),
            (key(&[Value::Bool(true)]), vec![2]),
            (key(&[Value::Int(5)]), vec![3, 4]),
            (key(&[Value::Double(1.5)]), vec![5]),
            (key(&[Value::Text("z".into())]), vec![6]),
        ];
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let idx = roundtrip(dir.path(), &entries, 1);
        for (k, ids) in &entries {
            assert_eq!(&idx.get(k).unwrap(), ids, "wrong ids for {k:?}");
        }
    }
}
