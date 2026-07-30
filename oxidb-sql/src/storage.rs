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
use crate::types::{
    Value, ValueRef, decode_row, decode_row_into, decode_row_masked, decode_row_refs,
    encode_row_into,
};

const RDAT_MAGIC: &[u8; 4] = b"OXSR";
const RDAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 8;

/// Path of a table's snapshot file within the SQL root `dir`.
pub fn rdat_path(dir: &Path, table: &str) -> PathBuf {
    dir.join(format!("{table}.rdat"))
}

/// A snapshot being written, one row at a time.
///
/// The whole-iterator [`write_snapshot`] cannot share a walk with anything else,
/// and a checkpoint has several things to do per row: write it here, and derive
/// the primary-key, UNIQUE and secondary-index entries for it. Walking the table
/// once per consumer meant decoding every row once per consumer — the dominant
/// cost of a checkpoint, and paid again on each of them. This exposes the writer
/// as a sink so one walk can feed them all.
///
/// Nothing is visible until [`finish`](Self::finish): rows go to a temporary
/// file that is fsynced and then renamed over the target.
pub struct SnapshotWriter {
    w: std::io::BufWriter<File>,
    tmp: PathBuf,
    path: PathBuf,
    /// Reused encode buffer — one per snapshot rather than one per row.
    buf: Vec<u8>,
}

impl SnapshotWriter {
    pub fn create(dir: &Path, table: &str) -> Result<SnapshotWriter> {
        let path = rdat_path(dir, table);
        let tmp = path.with_extension("rdat.tmp");
        // Streamed through a BufWriter rather than assembled in one `Vec`.
        // Building the whole file in memory first cost a second copy of the
        // table at every checkpoint — invisible on an unconstrained machine,
        // fatal in a cgroup: a 1.2M-row database could not open inside a 256 MB
        // limit that it otherwise runs in comfortably, because opening replays
        // the WAL tail and checkpoints.
        let mut w = std::io::BufWriter::with_capacity(1 << 16, File::create(&tmp)?);
        w.write_all(RDAT_MAGIC)?;
        w.write_all(&RDAT_VERSION.to_le_bytes())?;
        w.write_all(&0u16.to_le_bytes())?; // flags
        Ok(SnapshotWriter {
            w,
            tmp,
            path,
            buf: Vec::new(),
        })
    }

    pub fn push(&mut self, row_id: u64, cells: &[Value]) -> Result<()> {
        self.buf.clear();
        encode_row_into(cells, &mut self.buf);
        let crc = crc32fast::hash(&self.buf);
        self.w.write_all(&row_id.to_le_bytes())?;
        self.w.write_all(&(self.buf.len() as u32).to_le_bytes())?;
        self.w.write_all(&crc.to_le_bytes())?;
        self.w.write_all(&self.buf)?;
        Ok(())
    }

    /// fsync the rows written so far, then atomically publish them.
    pub fn finish(self) -> Result<()> {
        let f = self.w.into_inner().map_err(|e| e.into_error())?;
        f.sync_all()?;
        fs::rename(&self.tmp, &self.path)?;
        Ok(())
    }
}

/// Atomically write a table snapshot containing `rows` (each `(row_id, cells)`).
///
/// The whole-table convenience form over [`SnapshotWriter`]. The checkpoint path
/// drives the writer directly so it can share its row walk with the index
/// builders, which leaves this used only by the tests that pin the file format.
#[cfg_attr(not(test), allow(dead_code))]
pub fn write_snapshot<I, C>(dir: &Path, table: &str, rows: I) -> Result<()>
where
    I: IntoIterator<Item = (u64, C)>,
    C: std::borrow::Borrow<[Value]>,
{
    let mut w = SnapshotWriter::create(dir, table)?;
    for (row_id, cells) in rows {
        w.push(row_id, cells.borrow())?;
    }
    w.finish()
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

/// Records described by one sparse-index entry.
///
/// Sets the only real trade here: memory against how far a lookup by `row_id`
/// walks after the binary search. At 16 bytes an entry this is 0.69 bytes per row
/// in practice, against the 24 bytes per row a dense index cost.
///
/// Measured over 9.6M rows, the walk barely registers — and deliberately *not*
/// pushed further because of what the measurement cannot see:
///
/// ```text
///   block   resident   full scan   point lookup by PK
///      16    12.5 MB      133 ms       7.1 µs
///      32     6.3 MB      136 ms       7.3 µs
///      64     3.2 MB      133 ms       7.3 µs
///     128     1.6 MB      134 ms       7.4 µs
/// ```
///
/// Larger blocks look free there, but that run has the whole file in page cache.
/// A walk skips over payloads, so a block of 128 records spans ~14 KB — four
/// pages that a cold lookup would fault in to read one row, against one page at
/// 32. Since every size above already reduces this structure from gigabytes to
/// megabytes, the remaining megabytes are not worth buying with page faults that
/// this benchmark is not equipped to charge for.
const BLOCK_RECORDS: usize = 32;

/// Where a block of records starts, and the `row_id` it starts with.
///
/// The `row_id` is here so a lookup can binary-search blocks without touching
/// the mapping at all, and only reads records once it knows which block can
/// hold the one it wants.
#[derive(Clone, Copy)]
struct Block {
    first_row_id: u64,
    off: u64,
}

/// A table snapshot mapped read-only into memory (disk-first mode).
///
/// Record CRCs are verified once at open; reads afterwards decode straight
/// from the mapping without re-hashing. The mapping pins the file's *inode*,
/// so a later checkpoint atomically renaming a new snapshot over the path
/// does not invalidate an existing `MappedSnapshot`.
///
/// ## Why the index is sparse
///
/// This held `(row_id, offset, len)` per row — 24 bytes — which was the last
/// per-row structure in a mode whose whole point is that per-row cost lives on
/// disk. It made the engine's resident memory grow at ~33 bytes a row with
/// nothing to cap it: fine at 1M rows (28 MB), decisive at 100M (2.4 GB), and
/// unlike a cache it could not be evicted and re-read, because it *is* the map
/// to the file. PostgreSQL has no equivalent — a heap tuple is found through a
/// page number and a line pointer *in* the page.
///
/// Records are fixed-header and laid out in ascending `row_id` order, so the
/// file is its own index: one entry per [`BLOCK_RECORDS`] records is enough, and
/// getting from there to a specific record is a walk of 16-byte headers. That is
/// half a byte per row instead of 24.
///
/// Sequential access must not pay for that walk — the scan path reads records in
/// order, and re-walking a block per row would make a scan quadratic in the
/// block size. So sequential readers use [`SnapshotCursor`], which carries its
/// own offset and advances in constant time; only lookup by `row_id` walks.
pub struct MappedSnapshot {
    mmap: memmap2::Mmap,
    /// One entry per [`BLOCK_RECORDS`] records, ascending by `first_row_id`.
    blocks: Vec<Block>,
    /// Records in the file.
    count: usize,
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

        let mut blocks = Vec::new();
        let mut count = 0usize;
        let mut last_row_id: Option<u64> = None;
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
            // A sparse index cannot re-sort what it does not hold, and every
            // writer emits ascending `row_id`s (the checkpoint walks base and
            // overlay merged in id order), so out-of-order records mean a
            // corrupt file rather than a case to tolerate. The dense index this
            // replaced sorted defensively, which would have quietly masked it.
            if let Some(prev) = last_row_id
                && row_id <= prev
            {
                return Err(SqlError::Corrupt(format!(
                    "{table:?} snapshot is not in ascending row_id order \
                     (row {row_id} follows {prev})"
                )));
            }
            last_row_id = Some(row_id);
            if count % BLOCK_RECORDS == 0 {
                blocks.push(Block {
                    first_row_id: row_id,
                    off: pos as u64,
                });
            }
            count += 1;
            pos = end;
        }

        Ok(Some(MappedSnapshot {
            mmap,
            blocks,
            count,
            arity,
        }))
    }

    pub fn len(&self) -> usize {
        self.count
    }

    /// `(row_id, payload range)` of the record whose header starts at `off`.
    fn record_at(&self, off: usize) -> (u64, usize, usize) {
        let h = &self.mmap[off..off + 16];
        let row_id = u64::from_le_bytes(h[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(h[8..12].try_into().unwrap()) as usize;
        (row_id, off + 16, len)
    }

    /// Byte offset of the record holding `row_id`, or `None`.
    ///
    /// Binary-searches the block whose range can contain the id, then walks that
    /// block's records. The walk is bounded by [`BLOCK_RECORDS`] and stops early
    /// once the ids pass the target, which they must, being ascending.
    fn offset_of(&self, row_id: u64) -> Option<usize> {
        // The last block whose first id is <= the target: any earlier block ends
        // before it, any later one begins after it.
        let b = self
            .blocks
            .partition_point(|b| b.first_row_id <= row_id)
            .checked_sub(1)?;
        let mut off = self.blocks[b].off as usize;
        let end = match self.blocks.get(b + 1) {
            Some(next) => next.off as usize,
            None => self.mmap.len(),
        };
        while off < end {
            let (id, payload, len) = self.record_at(off);
            if id == row_id {
                return Some(off);
            }
            if id > row_id {
                return None;
            }
            off = payload + len;
        }
        None
    }

    pub fn contains(&self, row_id: u64) -> bool {
        self.offset_of(row_id).is_some()
    }

    /// Decode one row by id.
    pub fn get(&self, row_id: u64) -> Option<Vec<Value>> {
        let off = self.offset_of(row_id)?;
        let mut buf = Vec::new();
        self.decode_record(off, &mut buf);
        Some(buf)
    }

    /// Decode the record whose header starts at `off`. CRC was verified at open,
    /// so a decode failure here is snapshot corruption after the fact.
    fn decode_record(&self, off: usize, buf: &mut Vec<Value>) {
        let (row_id, payload, len) = self.record_at(off);
        decode_row_into(&self.mmap[payload..payload + len], self.arity, buf)
            .unwrap_or_else(|e| panic!("snapshot row {row_id} failed to decode: {e}"));
    }

    /// [`decode_record`](Self::decode_record) for only the columns in `want`,
    /// leaving the rest as `Value::Null` at their own positions. See
    /// [`crate::types::decode_row_masked`] for why the shape is preserved.
    fn decode_record_masked(&self, off: usize, want: &[bool], buf: &mut Vec<Value>) {
        let (row_id, payload, len) = self.record_at(off);
        decode_row_masked(&self.mmap[payload..payload + len], self.arity, want, buf)
            .unwrap_or_else(|e| panic!("snapshot row {row_id} failed to decode: {e}"));
    }

    /// A reader positioned at the first record, for reading rows **in order**.
    pub fn cursor(&self) -> SnapshotCursor<'_> {
        SnapshotCursor {
            snap: self,
            off: HEADER_LEN,
            left: self.count,
        }
    }

    /// All rows in ascending `row_id` order, decoded on the fly.
    pub fn entries(&self) -> impl Iterator<Item = (u64, Vec<Value>)> + '_ {
        let mut cur = self.cursor();
        std::iter::from_fn(move || {
            let id = cur.row_id()?;
            let mut buf = Vec::new();
            cur.decode_into(&mut buf);
            cur.advance();
            Some((id, buf))
        })
    }
}

/// A sequential reader over a snapshot's records.
///
/// Holds its own byte offset, so stepping to the next record is reading one
/// length and adding it — the sparse index is not consulted at all. That is what
/// keeps a full scan linear: looking each row up by position would re-walk its
/// block every time.
pub struct SnapshotCursor<'a> {
    snap: &'a MappedSnapshot,
    /// Header offset of the current record; meaningless once `left` is 0.
    off: usize,
    /// Records remaining, including the current one.
    left: usize,
}

impl<'a> SnapshotCursor<'a> {
    /// The current record's `row_id`, or `None` at the end.
    pub fn row_id(&self) -> Option<u64> {
        match self.left {
            0 => None,
            _ => Some(self.snap.record_at(self.off).0),
        }
    }

    /// Decode the current record into a reused buffer. Panics past the end,
    /// which callers avoid by checking [`row_id`](Self::row_id) first.
    pub fn decode_into(&self, buf: &mut Vec<Value>) {
        assert!(self.left > 0, "cursor read past the last record");
        self.snap.decode_record(self.off, buf);
    }

    /// [`decode_into`](Self::decode_into) for only the columns in `want`.
    pub fn decode_into_masked(&self, want: &[bool], buf: &mut Vec<Value>) {
        assert!(self.left > 0, "cursor read past the last record");
        self.snap.decode_record_masked(self.off, want, buf);
    }

    /// The current record's wanted cells **borrowed from the mapping** — no copy
    /// for text or bytes. The borrow is of the snapshot, which outlives the
    /// cursor, so the cells stay valid for as long as the caller holds them.
    pub fn decode_refs_into(&self, want: &[bool], buf: &mut Vec<ValueRef<'a>>) {
        assert!(self.left > 0, "cursor read past the last record");
        let (row_id, payload, len) = self.snap.record_at(self.off);
        // Borrowed from the *snapshot*, not from this cursor: the mapping outlives
        // the walk, so the cells stay valid after the cursor advances. Tying them
        // to `&self` instead made every decoded row keep the cursor frozen, which
        // is both wrong in intent and rejected outright.
        let bytes: &'a [u8] = &self.snap.mmap;
        decode_row_refs(&bytes[payload..payload + len], self.snap.arity, want, buf)
            .unwrap_or_else(|e| panic!("snapshot row {row_id} failed to decode: {e}"));
    }

    /// Decode the current record into a fresh vector.
    pub fn decode(&self) -> Vec<Value> {
        let mut buf = Vec::new();
        self.decode_into(&mut buf);
        buf
    }

    /// Step to the next record. A no-op at the end.
    pub fn advance(&mut self) {
        if self.left == 0 {
            return;
        }
        let (_, payload, len) = self.snap.record_at(self.off);
        self.off = payload + len;
        self.left -= 1;
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

#[cfg(test)]
mod sparse_index_tests {
    //! The snapshot's in-memory index holds one entry per [`BLOCK_RECORDS`]
    //! records rather than one per row, and reaches a specific record by walking
    //! headers from the block that can hold it.
    //!
    //! Two things have to be true for that to be a win rather than a bug: a
    //! lookup must still find exactly the rows that are there (the walk has to
    //! stop in the right places, including in the gaps that deletes leave), and
    //! sequential reads must not walk at all, or a scan becomes quadratic in the
    //! block size.
    use super::*;

    /// `n` rows with ids `1, 3, 5, …` — deliberately gappy, because row ids are
    /// dense only until something is deleted, and a walk that assumes otherwise
    /// would still pass on contiguous ids.
    fn snapshot_of(dir: &Path, n: u64) -> MappedSnapshot {
        let rows: Vec<(u64, Vec<Value>)> = (0..n)
            .map(|i| {
                let id = i * 2 + 1;
                (
                    id,
                    vec![Value::Int(id as i64), Value::Text(format!("r{id}").into())],
                )
            })
            .collect();
        write_snapshot(dir, "t", rows.iter().map(|(id, c)| (*id, c.as_slice()))).unwrap();
        MappedSnapshot::open(dir, "t", 2)
            .unwrap()
            .expect("just written")
    }

    /// The point of the whole change: the index does not scale with rows. Without
    /// this, a dense index would pass every other test in this module.
    #[test]
    fn the_index_holds_one_entry_per_block() {
        let dir = tempfile::tempdir().unwrap();
        let n = BLOCK_RECORDS as u64 * 5 + 7;
        let snap = snapshot_of(dir.path(), n);
        assert_eq!(snap.len(), n as usize);
        assert_eq!(
            snap.blocks.len(),
            (n as usize).div_ceil(BLOCK_RECORDS),
            "the index should hold one entry per block, not per row"
        );
    }

    /// Every id present is found, and the row decoded is that id's row — across
    /// several blocks, so block selection is exercised rather than assumed.
    #[test]
    fn every_row_is_found_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let n = BLOCK_RECORDS as u64 * 4 + 3;
        let snap = snapshot_of(dir.path(), n);
        for i in 0..n {
            let id = i * 2 + 1;
            let row = snap.get(id).unwrap_or_else(|| panic!("row {id} not found"));
            assert_eq!(row[0], Value::Int(id as i64));
            assert_eq!(row[1], Value::Text(format!("r{id}").into()));
            assert!(snap.contains(id));
        }
    }

    /// The ids that are *not* there. The gaps matter most: an id between two
    /// present rows sits inside a block, so the walk has to stop on "passed it"
    /// rather than on running out of records.
    #[test]
    fn absent_ids_are_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let n = BLOCK_RECORDS as u64 * 3;
        let snap = snapshot_of(dir.path(), n);
        let last = (n - 1) * 2 + 1;

        assert!(!snap.contains(0), "below the first id");
        assert!(snap.get(0).is_none());
        assert!(!snap.contains(last + 1), "above the last id");
        assert!(!snap.contains(u64::MAX));
        // Every even id is a gap, including ones that land on block boundaries.
        for i in 0..n {
            let gap = i * 2;
            assert!(!snap.contains(gap), "id {gap} is a gap but was found");
        }
    }

    /// A block's first and last record are the two the walk is most likely to
    /// get wrong (off-by-one at either end of the range it searches).
    #[test]
    fn block_boundary_rows_are_found() {
        let dir = tempfile::tempdir().unwrap();
        let n = BLOCK_RECORDS as u64 * 3 + 1;
        let snap = snapshot_of(dir.path(), n);
        for b in 0..snap.blocks.len() {
            let first = b * BLOCK_RECORDS;
            let last = ((b + 1) * BLOCK_RECORDS - 1).min(n as usize - 1);
            for pos in [first, last] {
                let id = pos as u64 * 2 + 1;
                assert!(snap.contains(id), "boundary row {id} (block {b}) not found");
            }
        }
    }

    /// The cursor visits every record once, in order, and then stops.
    #[test]
    fn the_cursor_walks_every_record_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let n = BLOCK_RECORDS as u64 * 2 + 5;
        let snap = snapshot_of(dir.path(), n);

        let mut seen = Vec::new();
        let mut cur = snap.cursor();
        while let Some(id) = cur.row_id() {
            let row = cur.decode();
            assert_eq!(row[0], Value::Int(id as i64));
            seen.push(id);
            cur.advance();
        }
        assert_eq!(seen, (0..n).map(|i| i * 2 + 1).collect::<Vec<_>>());
        // Past the end it stays past the end.
        cur.advance();
        assert!(cur.row_id().is_none());
        assert_eq!(snap.entries().count(), n as usize);
    }

    #[test]
    fn an_empty_snapshot_answers_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let empty: Vec<(u64, &[Value])> = Vec::new();
        write_snapshot(dir.path(), "t", empty).unwrap();
        let snap = MappedSnapshot::open(dir.path(), "t", 2).unwrap().unwrap();

        assert_eq!(snap.len(), 0);
        assert!(snap.blocks.is_empty());
        assert!(!snap.contains(1));
        assert!(snap.get(1).is_none());
        assert!(snap.cursor().row_id().is_none());
        assert_eq!(snap.entries().count(), 0);
    }

    /// Ascending order is now an invariant rather than something to repair: the
    /// dense index this replaced sorted itself defensively, and a sparse one
    /// cannot — it does not hold the rows. Every writer emits ascending ids, so a
    /// file that does not is corrupt and should say so instead of answering
    /// lookups wrongly for the rest of its life.
    #[test]
    fn out_of_order_records_are_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let r = vec![Value::Int(1), Value::Null];
        let rows: Vec<(u64, &[Value])> = vec![(5, &r), (2, &r)];
        write_snapshot(dir.path(), "t", rows).unwrap();

        match MappedSnapshot::open(dir.path(), "t", 2) {
            Err(SqlError::Corrupt(msg)) => {
                assert!(msg.contains("ascending"), "unhelpful message: {msg}")
            }
            Err(e) => panic!("expected a corruption error, got {e:?}"),
            Ok(_) => panic!("an out-of-order snapshot opened without complaint"),
        }
    }

    /// A duplicate id is the same problem — ids come from a monotonic counter and
    /// a snapshot holds each row once.
    #[test]
    fn duplicate_ids_are_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let r = vec![Value::Int(1), Value::Null];
        let rows: Vec<(u64, &[Value])> = vec![(7, &r), (7, &r)];
        write_snapshot(dir.path(), "t", rows).unwrap();
        assert!(
            matches!(
                MappedSnapshot::open(dir.path(), "t", 2),
                Err(SqlError::Corrupt(_))
            ),
            "a snapshot with a duplicate row id opened without complaint"
        );
    }
}
