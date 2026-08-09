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

/// Entries buffered before a sort-and-spill. At ~40 bytes an entry this is a
/// megabyte and a half — small enough that a checkpoint's peak stops tracking
/// the table, large enough that an ordinary table produces one run and never
/// touches the merge path.
///
/// Sized by measurement, and the measurement changed when a checkpoint began
/// building a table's indexes from a single row walk: several builders are now
/// alive at once, so the buffer is paid per index rather than once. Over a
/// 1.2M-row, 5-table database, dropping this from 131_072 took the open-time
/// peak from 84 MB to 63 MB for 10% more time. Below 32_768 the peak stops
/// falling — the buffers are no longer what is largest — and the extra spill
/// runs keep costing merge IO, so this is the knee and not a floor.
const SPILL_ENTRIES: usize = 32_768;

/// Builds a `.sidx` from an unordered stream of `(key, row_id)` pairs in
/// **bounded memory**.
///
/// The obvious implementation — collect everything into a `BTreeMap`, sort by
/// construction, write it out — is what made a checkpoint's peak proportional
/// to the table. That was invisible until the memory-pressure test: a 1.2M-row
/// database peaked at 415 MB opening, against the 94 MB it then ran in, so a
/// server sized for its steady state could not restart after a bulk load.
///
/// This is an external sort. Pairs accumulate until `SPILL_ENTRIES`, then get
/// sorted and written to a run file and the buffer is cleared; at the end the
/// runs and the final buffer are merged k-way, coalescing equal keys, straight
/// into the output. Memory is the buffer plus one small read block per run.
///
/// The output is byte-identical to what the in-memory version produced — the
/// reader is untouched.
pub struct IndexBuilder {
    dir: PathBuf,
    table: String,
    index: String,
    slots: usize,
    buf: Vec<(KeyTuple, u64)>,
    runs: Vec<PathBuf>,
}

/// `[key_len u32][key bytes][id_count u32][ids u64…]`, entries ascending by key.
/// A private spill format — never read after the merge, never left behind.
fn write_run(path: &Path, entries: &[(KeyTuple, Vec<u64>)]) -> Result<()> {
    let f = File::create(path)?;
    let mut w = std::io::BufWriter::with_capacity(1 << 16, f);
    for (key, ids) in entries {
        let cells: Vec<Value> = key.iter().map(|k| k.0.clone()).collect();
        let encoded = encode_row(&cells);
        w.write_all(&(encoded.len() as u32).to_le_bytes())?;
        w.write_all(&encoded)?;
        w.write_all(&(ids.len() as u32).to_le_bytes())?;
        for id in ids {
            w.write_all(&id.to_le_bytes())?;
        }
    }
    w.flush()?;
    Ok(())
}

/// One spill file, read back an entry at a time.
struct RunReader {
    r: std::io::BufReader<File>,
    slots: usize,
    head: Option<(KeyTuple, Vec<u64>)>,
}

impl RunReader {
    fn open(path: &Path, slots: usize) -> Result<RunReader> {
        let mut rr = RunReader {
            r: std::io::BufReader::with_capacity(1 << 16, File::open(path)?),
            slots,
            head: None,
        };
        rr.advance()?;
        Ok(rr)
    }

    fn advance(&mut self) -> Result<()> {
        use std::io::Read;
        let mut u32b = [0u8; 4];
        if self.r.read_exact(&mut u32b).is_err() {
            self.head = None;
            return Ok(());
        }
        let klen = u32::from_le_bytes(u32b) as usize;
        let mut kbuf = vec![0u8; klen];
        self.r.read_exact(&mut kbuf)?;
        let key: KeyTuple = decode_row(&kbuf, self.slots)?
            .into_iter()
            .map(IndexKey)
            .collect();
        self.r.read_exact(&mut u32b)?;
        let n = u32::from_le_bytes(u32b) as usize;
        let mut ids = Vec::with_capacity(n);
        let mut idb = [0u8; 8];
        for _ in 0..n {
            self.r.read_exact(&mut idb)?;
            ids.push(u64::from_le_bytes(idb));
        }
        self.head = Some((key, ids));
        Ok(())
    }
}

impl IndexBuilder {
    pub fn new(dir: &Path, table: &str, index: &str, slots: usize) -> IndexBuilder {
        IndexBuilder {
            dir: dir.to_path_buf(),
            table: table.to_string(),
            index: index.to_string(),
            slots,
            buf: Vec::new(),
            runs: Vec::new(),
        }
    }

    pub fn push(&mut self, key: KeyTuple, row_id: u64) -> Result<()> {
        self.buf.push((key, row_id));
        if self.buf.len() >= SPILL_ENTRIES {
            self.spill()?;
        }
        Ok(())
    }

    /// Sort the buffer, coalesce equal keys, and write it out as a run.
    fn spill(&mut self) -> Result<()> {
        let grouped = Self::drain_sorted(&mut self.buf);
        let path = self.dir.join(format!(
            "{}.{}.run{}",
            self.table,
            self.index,
            self.runs.len()
        ));
        write_run(&path, &grouped)?;
        self.runs.push(path);
        Ok(())
    }

    /// `(key, row_id)` pairs -> ascending, coalesced `(key, ids)` groups.
    fn drain_sorted(buf: &mut Vec<(KeyTuple, u64)>) -> Vec<(KeyTuple, Vec<u64>)> {
        buf.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut out: Vec<(KeyTuple, Vec<u64>)> = Vec::new();
        for (key, rid) in buf.drain(..) {
            match out.last_mut() {
                Some((k, ids)) if *k == key => ids.push(rid),
                _ => out.push((key, vec![rid])),
            }
        }
        out
    }

    /// Merge every run and the remaining buffer into the final `.sidx`.
    pub fn finish(mut self) -> Result<()> {
        let tail = Self::drain_sorted(&mut self.buf);

        // Sections are written to separate temp files because the entry table
        // has to precede the blobs it points into, while its offsets are only
        // known as the blobs are laid out. Concatenating three streams keeps
        // the whole thing O(1) in memory; holding them was the second half of
        // the peak this class exists to remove.
        let base = self.dir.join(format!("{}.{}", self.table, self.index));
        let (tp, kp, ip) = (
            base.with_extension("tbl.tmp"),
            base.with_extension("key.tmp"),
            base.with_extension("ids.tmp"),
        );
        let mut tw = std::io::BufWriter::with_capacity(1 << 16, File::create(&tp)?);
        let mut kw = std::io::BufWriter::with_capacity(1 << 16, File::create(&kp)?);
        let mut iw = std::io::BufWriter::with_capacity(1 << 16, File::create(&ip)?);

        let mut readers: Vec<RunReader> = Vec::new();
        for r in &self.runs {
            readers.push(RunReader::open(r, self.slots)?);
        }
        let mut tail_iter = tail.into_iter().peekable();

        let (mut count, mut keys_len, mut ids_at) = (0u64, 0u64, 0u32);
        loop {
            // Smallest head across the runs and the in-memory tail.
            let mut best: Option<(KeyTuple, Option<usize>)> = None;
            for (i, rr) in readers.iter().enumerate() {
                if let Some((k, _)) = &rr.head
                    && best.as_ref().is_none_or(|(b, _)| k < b)
                {
                    best = Some((k.clone(), Some(i)));
                }
            }
            if let Some((k, _)) = tail_iter.peek()
                && best.as_ref().is_none_or(|(b, _)| k < b)
            {
                best = Some((k.clone(), None));
            }
            let Some((key, _)) = best else { break };

            // Every source holding that key contributes its ids.
            let mut ids: Vec<u64> = Vec::new();
            for rr in readers.iter_mut() {
                while rr.head.as_ref().is_some_and(|(k, _)| *k == key) {
                    ids.extend(rr.head.take().expect("checked").1);
                    rr.advance()?;
                }
            }
            while tail_iter.peek().is_some_and(|(k, _)| *k == key) {
                ids.extend(tail_iter.next().expect("checked").1);
            }
            ids.sort_unstable();
            ids.dedup();

            let cells: Vec<Value> = key.iter().map(|k| k.0.clone()).collect();
            let encoded = encode_row(&cells);
            tw.write_all(&(keys_len as u32).to_le_bytes())?;
            tw.write_all(&(encoded.len() as u32).to_le_bytes())?;
            tw.write_all(&ids_at.to_le_bytes())?;
            tw.write_all(&(ids.len() as u32).to_le_bytes())?;
            kw.write_all(&encoded)?;
            keys_len += encoded.len() as u64;
            for id in &ids {
                iw.write_all(&id.to_le_bytes())?;
            }
            ids_at += ids.len() as u32;
            count += 1;
        }
        tw.flush()?;
        kw.flush()?;
        iw.flush()?;
        drop((tw, kw, iw));

        let path = sidx_path(&self.dir, &self.table, &self.index);
        let tmp = path.with_extension("sidx.tmp");
        {
            let f = File::create(&tmp)?;
            let mut w = std::io::BufWriter::with_capacity(1 << 16, f);
            w.write_all(SIDX_MAGIC)?;
            w.write_all(&SIDX_VERSION.to_le_bytes())?;
            w.write_all(&(self.slots as u16).to_le_bytes())?;
            w.write_all(&count.to_le_bytes())?;
            w.write_all(&keys_len.to_le_bytes())?;
            for part in [&tp, &kp, &ip] {
                let mut r = std::io::BufReader::with_capacity(1 << 16, File::open(part)?);
                std::io::copy(&mut r, &mut w)?;
            }
            let f = w.into_inner().map_err(|e| e.into_error())?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;

        for junk in [tp, kp, ip].iter().chain(self.runs.iter()) {
            let _ = fs::remove_file(junk);
        }
        Ok(())
    }
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

    /// Row ids whose **first** key column falls inside `[lo, hi]`, as of the
    /// last checkpoint. Callers must verify each against the live row.
    ///
    /// The file is sorted by decoded key tuple, so this is a `lower_bound` seek
    /// followed by a forward walk that stops at the first key past `hi` — the
    /// cost is the matching run, not the index. Note the seek is by the
    /// one-element tuple `[lo]`, which sorts at or below every tuple beginning
    /// with `lo`; an exclusive low bound is filtered per entry rather than
    /// seeked past, because `[lo]` sorts *below* `[lo, x]` and skipping it would
    /// skip composite keys that belong in the answer.
    pub fn range_first_col(
        &self,
        lo: &crate::store::RangeBound,
        hi: &crate::store::RangeBound,
    ) -> Result<Vec<u64>> {
        let at = match lo.value() {
            None => 0,
            Some(v) => self.lower_bound(&[IndexKey(v.clone())])?,
        };
        let mut out = Vec::new();
        for i in at..self.entries {
            let key = self.key_at(i)?;
            let Some(first) = key.first() else { continue };
            if !hi.allows_high(&first.0) {
                break; // sorted by key — no later entry can qualify
            }
            if lo.allows_low(&first.0) {
                out.extend(self.ids_at(i));
            }
        }
        Ok(out)
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

    /// Feed entries through the builder in a deliberately *unsorted* order —
    /// the builder is responsible for ordering, so handing it sorted input
    /// would test less than it looks like it does.
    fn roundtrip(dir: &Path, entries: &[(KeyTuple, Vec<u64>)], slots: usize) -> MappedIndex {
        let mut b = IndexBuilder::new(dir, "t", "i", slots);
        let mut flat: Vec<(KeyTuple, u64)> = entries
            .iter()
            .flat_map(|(k, ids)| ids.iter().map(move |id| (k.clone(), *id)))
            .collect();
        flat.reverse();
        for (k, id) in flat {
            b.push(k, id).unwrap();
        }
        b.finish().unwrap();
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

    /// Everything above stays inside one buffer, so it never reaches the merge.
    /// This crosses `SPILL_ENTRIES` several times, with keys shuffled so runs
    /// genuinely overlap and duplicate keys land in different runs — the case
    /// where a merge that failed to coalesce would show up.
    #[test]
    fn spilled_runs_merge_back_into_one_sorted_index() {
        let dir = tempfile::tempdir().unwrap();
        let n: i64 = (SPILL_ENTRIES as i64) * 3 + 777;
        let mut b = IndexBuilder::new(dir.path(), "t", "i", 1);
        // Two row ids per key, pushed far apart so they fall in different runs,
        // and an order that is neither ascending nor descending.
        for pass in 0..2 {
            for i in 0..n {
                let k = (i * 7919) % n; // coprime stride: a full, scrambled cycle
                b.push(key(&[Value::Int(k)]), (k as u64) * 2 + pass)
                    .unwrap();
            }
        }
        b.finish().unwrap();

        let idx = MappedIndex::open(dir.path(), "t", "i").unwrap().unwrap();
        assert_eq!(idx.len(), n as usize, "one entry per distinct key");
        for k in [0i64, 1, n / 3, n / 2, n - 1] {
            assert_eq!(
                idx.get(&key(&[Value::Int(k)])).unwrap(),
                vec![k as u64 * 2, k as u64 * 2 + 1],
                "key {k} lost an id across the merge"
            );
        }
        assert!(idx.get(&key(&[Value::Int(n)])).unwrap().is_empty());

        // No spill files left behind.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|f| !f.ends_with(".sidx"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
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
