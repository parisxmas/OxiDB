//! Disk-backed composite index: an mmap'd `.mcidx` file + in-memory overlay.
//!
//! Phase 1 of disk-backed composites (the RSS story's last structural gap:
//! three in-RAM composites cost ~300 MB on the 1M-doc bench). This module is
//! the format and the standalone index type, mirroring `mmap_field_index`'s
//! proven design one-to-one — mmap for the bulk, a small `BTreeMap` overlay
//! for writes since the last persist, per-doc tombstones applied by probing
//! each entry's OWN ids (never by scanning the whole tombstone map — see the
//! O(entries × tombstones) lesson in that module), a live `total_ids`
//! counter, and skip-clean `persist()`. Collection integration (create /
//! open / sync-tick persist / covered-aggregation walk) is Phase 2.
//!
//! ## `.mcidx` v1 layout
//! ```text
//! [b"OXCX"(4)][version u32(4)][slot_count u16(2)]
//! per slot: [name_len u16][name bytes]
//! [entry_count u64][total_ids u64]
//! entry table: per entry, per slot [vtype u8][value_i64 le][value_len u32]
//!              then [docid_offset u64][docid_count u32]
//! string table (referenced by value_i64=offset, value_len=len)
//! docid section (u64 le each)
//! ```
//! Entries are sorted by their full slot tuple (`CompositeKey` order), so a
//! prefix scan is a binary-searched lower bound plus a linear walk.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use serde_json::Value;

use crate::document::DocumentId;
use crate::index::{CompositeKey, DocIdSet};
use crate::value::IndexValue;

const MAGIC: &[u8; 4] = b"OXCX";
const FORMAT_VERSION: u32 = 1;
const SLOT_SIZE: usize = 13; // vtype(1) + value_i64(8) + value_len(4)

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_DATETIME: u8 = 4;
const TAG_STRING: u8 = 5;

fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}
fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}
fn read_i64(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn resolve_value_field<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = data;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

#[derive(Debug, Clone)]
struct McidxLayout {
    entry_table_offset: usize,
    string_table_offset: usize,
    docid_section_offset: usize,
    entry_count: u64,
}

/// A composite index backed by an mmap'd `.mcidx` file plus a write overlay.
#[derive(Debug)]
pub struct MmapCompositeIndex {
    mmap: Option<Mmap>,
    layout: Option<McidxLayout>,
    path: PathBuf,
    pub fields: Vec<String>,
    /// Writes since the last persist.
    overlay: BTreeMap<CompositeKey, DocIdSet>,
    /// Tombstones: doc_id → keys removed from the mmap layer since persist.
    removed: HashMap<DocumentId, Vec<CompositeKey>>,
    /// Live (key, id) pair total — mmap minus tombstoned-in-mmap plus
    /// overlay. Kept O(1) for the covered-aggregation guards.
    total_ids: usize,
}

impl MmapCompositeIndex {
    pub fn new(fields: Vec<String>) -> Self {
        Self {
            mmap: None,
            layout: None,
            path: PathBuf::new(),
            fields,
            overlay: BTreeMap::new(),
            removed: HashMap::new(),
            total_ids: 0,
        }
    }

    pub fn set_path(&mut self, path: PathBuf) {
        self.path = path;
    }

    pub fn name(&self) -> String {
        self.fields.join("_")
    }

    pub fn count_all(&self) -> usize {
        self.total_ids
    }

    /// Resident heap: overlay + tombstones only — the mmap'd bulk is
    /// reclaimable page cache, which is the point of this type.
    pub fn memory_bytes(&self) -> usize {
        let mut total = 0;
        for (key, ids) in &self.overlay {
            total += key.0.capacity() * std::mem::size_of::<IndexValue>() + 48;
            for iv in &key.0 {
                total += iv.heap_bytes();
            }
            total += ids.heap_bytes();
        }
        total += self.removed.len() * 64;
        total
    }

    /// Open an existing `.mcidx` via mmap. Instant — no deserialization; the
    /// same header scan that locates the string table seeds `total_ids`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, format!("mcidx: {m}"));
        let file = fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let buf = &mmap[..];
        if buf.len() < 10 || &buf[0..4] != MAGIC {
            return Err(bad("bad magic or truncated header"));
        }
        if read_u32(buf, 4) != FORMAT_VERSION {
            return Err(bad("unsupported version"));
        }
        let slot_count = read_u16(buf, 8) as usize;
        if slot_count == 0 {
            return Err(bad("zero slots"));
        }
        let mut off = 10;
        let mut fields = Vec::with_capacity(slot_count);
        for _ in 0..slot_count {
            if off + 2 > buf.len() {
                return Err(bad("truncated field names"));
            }
            let len = read_u16(buf, off) as usize;
            off += 2;
            if off + len > buf.len() {
                return Err(bad("truncated field name"));
            }
            fields.push(
                String::from_utf8(buf[off..off + len].to_vec()).map_err(|_| bad("bad utf8"))?,
            );
            off += len;
        }
        if off + 16 > buf.len() {
            return Err(bad("truncated counts"));
        }
        let entry_count = read_u64(buf, off);
        off += 16; // entry_count + (redundant) total_ids slot
        let entry_size = slot_count * SLOT_SIZE + 12;
        let entry_table_offset = off;
        let entries_end = entry_table_offset
            .checked_add((entry_count as usize).checked_mul(entry_size).ok_or_else(|| bad("entry count overflow"))?)
            .filter(|&e| e <= buf.len())
            .ok_or_else(|| bad("truncated entry table"))?;

        // One pass over entry headers: find the string table's extent and
        // sum the doc-id counts (free total_ids seed, like the field index).
        let mut string_table_end = 0usize;
        let mut total_ids = 0usize;
        for i in 0..entry_count as usize {
            let eoff = entry_table_offset + i * entry_size;
            for s in 0..slot_count {
                let soff = eoff + s * SLOT_SIZE;
                if buf[soff] == TAG_STRING {
                    let end = (read_i64(buf, soff + 1) as usize)
                        .checked_add(read_u32(buf, soff + 9) as usize)
                        .ok_or_else(|| bad("string range overflow"))?;
                    string_table_end = string_table_end.max(end);
                }
            }
            total_ids = total_ids.saturating_add(read_u32(buf, eoff + slot_count * SLOT_SIZE + 8) as usize);
        }
        let string_table_offset = entries_end;
        let docid_section_offset = string_table_offset
            .checked_add(string_table_end)
            .filter(|&o| o <= buf.len())
            .ok_or_else(|| bad("truncated string table"))?;

        Ok(Self {
            layout: Some(McidxLayout {
                entry_table_offset,
                string_table_offset,
                docid_section_offset,
                entry_count,
            }),
            mmap: Some(mmap),
            path: path.to_path_buf(),
            fields,
            overlay: BTreeMap::new(),
            removed: HashMap::new(),
            total_ids,
        })
    }

    fn entry_size(&self) -> usize {
        self.fields.len() * SLOT_SIZE + 12
    }

    /// Decode entry `idx`'s full key from the mmap. None when out of range
    /// or on a corrupt slot.
    fn mmap_entry_key(&self, idx: usize) -> Option<CompositeKey> {
        let (buf, layout) = (self.mmap.as_deref()?, self.layout.as_ref()?);
        if idx >= layout.entry_count as usize {
            return None;
        }
        let eoff = layout.entry_table_offset + idx * self.entry_size();
        let mut values = Vec::with_capacity(self.fields.len());
        for s in 0..self.fields.len() {
            let soff = eoff + s * SLOT_SIZE;
            let v_i64 = read_i64(buf, soff + 1);
            let v_len = read_u32(buf, soff + 9) as usize;
            values.push(match buf[soff] {
                TAG_NULL => IndexValue::Null,
                TAG_BOOL => IndexValue::Boolean(v_i64 != 0),
                TAG_INT => IndexValue::Integer(v_i64),
                TAG_FLOAT => IndexValue::Float(f64::from_bits(v_i64 as u64)),
                TAG_DATETIME => IndexValue::DateTime(v_i64),
                TAG_STRING => {
                    let start = layout.string_table_offset + v_i64 as usize;
                    let end = start.checked_add(v_len).filter(|&e| e <= buf.len())?;
                    IndexValue::String(std::str::from_utf8(&buf[start..end]).ok()?.to_string())
                }
                _ => return None,
            });
        }
        Some(CompositeKey(values))
    }

    /// Read entry `idx`'s doc ids from the mmap (no tombstone filtering).
    fn mmap_entry_docids(&self, idx: usize) -> DocIdSet {
        let (Some(buf), Some(layout)) = (self.mmap.as_deref(), self.layout.as_ref()) else {
            return DocIdSet::Empty;
        };
        if idx >= layout.entry_count as usize {
            return DocIdSet::Empty;
        }
        let eoff = layout.entry_table_offset + idx * self.entry_size();
        let base = eoff + self.fields.len() * SLOT_SIZE;
        let off = layout.docid_section_offset + read_u64(buf, base) as usize;
        let count = read_u32(buf, base + 8) as usize;
        if off + count * 8 > buf.len() || count == 0 {
            return DocIdSet::Empty;
        }
        if count == 1 {
            return DocIdSet::One(read_u64(buf, off));
        }
        let mut set = std::collections::BTreeSet::new();
        for j in 0..count {
            set.insert(read_u64(buf, off + j * 8));
        }
        DocIdSet::Set(set)
    }

    /// Same inverted tombstone application as the field index: probe THIS
    /// entry's ids against the tombstone map — O(|ids|) hash lookups.
    fn apply_tombstones(&self, key: &CompositeKey, ids: &mut DocIdSet) {
        if self.removed.is_empty() {
            return;
        }
        let doomed: Vec<DocumentId> = ids
            .iter()
            .filter(|id| {
                self.removed
                    .get(id)
                    .is_some_and(|keys| keys.iter().any(|k| k == key))
            })
            .copied()
            .collect();
        for id in doomed {
            ids.remove(&id);
        }
    }

    /// First mmap entry whose key is >= `prefix` (compared on the prefix's
    /// length only when `prefix_only`, else on the full tuple).
    fn mmap_lower_bound(&self, target: &[IndexValue]) -> usize {
        let Some(layout) = self.layout.as_ref() else {
            return 0;
        };
        let (mut lo, mut hi) = (0usize, layout.entry_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let Some(key) = self.mmap_entry_key(mid) else {
                break;
            };
            let probe = &key.0[..target.len().min(key.0.len())];
            if probe < target { lo = mid + 1 } else { hi = mid }
        }
        lo
    }

    fn extract_key(&self, data: &Value) -> CompositeKey {
        CompositeKey(
            self.fields
                .iter()
                .map(|f| {
                    resolve_value_field(data, f)
                        .map(IndexValue::from_json)
                        .unwrap_or(IndexValue::Null)
                })
                .collect(),
        )
    }

    pub fn insert_value(&mut self, id: DocumentId, data: &Value) {
        let key = self.extract_key(data);
        self.insert_key(id, key);
    }

    /// Insert a pre-extracted key (callers that already resolved the slots).
    pub fn insert_key(&mut self, id: DocumentId, key: CompositeKey) {
        if self.overlay.entry(key).or_default().insert(id) {
            self.total_ids += 1;
        }
    }

    pub fn remove_value(&mut self, id: DocumentId, data: &Value) {
        let key = self.extract_key(data);
        self.remove_key(id, key);
    }

    /// Remove a pre-extracted key: overlay first, else tombstone the mmap
    /// layer (once per pair; `total_ids` drops only when the pair really
    /// exists somewhere).
    pub fn remove_key(&mut self, id: DocumentId, key: CompositeKey) {
        if let Some(set) = self.overlay.get_mut(&key) {
            if set.remove(&id) {
                if set.is_empty() {
                    self.overlay.remove(&key);
                }
                self.total_ids = self.total_ids.saturating_sub(1);
                return;
            }
        }
        let already = self
            .removed
            .get(&id)
            .is_some_and(|keys| keys.iter().any(|k| k == &key));
        if !already {
            let idx = self.mmap_lower_bound(&key.0);
            let in_mmap = self
                .mmap_entry_key(idx)
                .is_some_and(|k| k == key && self.mmap_entry_docids(idx).contains(&id));
            if in_mmap {
                self.total_ids = self.total_ids.saturating_sub(1);
            }
            self.removed.entry(id).or_default().push(key);
        }
    }

    pub fn clear(&mut self) {
        self.overlay.clear();
        self.removed.clear();
        self.mmap = None;
        self.layout = None;
        self.total_ids = 0;
    }

    /// Merged (mmap ⊎ overlay, tombstones applied) iteration in ascending
    /// key order, lazily — one mmap entry materialized per step. `prefix`
    /// empty ⇒ every entry. The callback returns `false` to stop.
    pub fn for_each_prefix_entries<F: FnMut(&[IndexValue], &DocIdSet) -> bool>(
        &self,
        prefix: &[IndexValue],
        mut f: F,
    ) {
        let mmap_count = self.layout.as_ref().map_or(0, |l| l.entry_count as usize);
        let mut mpos = if prefix.is_empty() { 0 } else { self.mmap_lower_bound(prefix) };
        let mut opos = self
            .overlay
            .range(CompositeKey(prefix.to_vec())..)
            .map(|(k, v)| (k, v));
        let mut onext: Option<(&CompositeKey, &DocIdSet)> = opos.next();
        let in_prefix =
            |k: &CompositeKey| k.0.len() >= prefix.len() && k.0[..prefix.len()] == *prefix;

        loop {
            // Next in-prefix mmap key, skipping corrupt entries.
            let mkey = loop {
                if mpos >= mmap_count {
                    break None;
                }
                match self.mmap_entry_key(mpos) {
                    Some(k) if in_prefix(&k) => break Some(k),
                    Some(_) => break None, // sorted: left the prefix range
                    None => mpos += 1,
                }
            };
            let otake = onext.filter(|(k, _)| in_prefix(k));

            let (key, ids) = match (&mkey, otake) {
                (None, None) => return,
                (Some(mk), o) => {
                    let take_overlay = o.is_some_and(|(ok, _)| ok.0 < mk.0);
                    if take_overlay {
                        let (ok, ov) = o.expect("checked");
                        onext = opos.next();
                        (ok.clone(), ov.clone())
                    } else {
                        let mut ids = self.mmap_entry_docids(mpos);
                        self.apply_tombstones(mk, &mut ids);
                        mpos += 1;
                        if let Some((ok, ov)) = o {
                            if ok.0 == mk.0 {
                                for &id in ov.iter() {
                                    ids.insert(id);
                                }
                                onext = opos.next();
                            }
                        }
                        (mk.clone(), ids)
                    }
                }
                (None, Some((ok, ov))) => {
                    onext = opos.next();
                    (ok.clone(), ov.clone())
                }
            };
            if !ids.is_empty() && !f(&key.0, &ids) {
                return;
            }
        }
    }

    /// Persist mmap + overlay merged (tombstones folded away) and reopen.
    /// Clean (no overlay, no tombstones, file present) ⇒ no-op, so the
    /// periodic sync tick never rewrites an unchanged index.
    pub fn persist(&mut self) -> io::Result<()> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }
        if self.mmap.is_some()
            && self.overlay.is_empty()
            && self.removed.is_empty()
            && self.path.exists()
        {
            return Ok(());
        }

        let mut entries: Vec<(CompositeKey, DocIdSet)> = Vec::new();
        self.for_each_prefix_entries(&[], |slots, ids| {
            entries.push((CompositeKey(slots.to_vec()), ids.clone()));
            true
        });
        self.total_ids = entries.iter().map(|(_, ids)| ids.len()).sum();

        // Serialize: header + names + counts + entry table + strings + ids.
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC);
        body.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        body.extend_from_slice(&(self.fields.len() as u16).to_le_bytes());
        for f in &self.fields {
            body.extend_from_slice(&(f.len() as u16).to_le_bytes());
            body.extend_from_slice(f.as_bytes());
        }
        body.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        body.extend_from_slice(&(self.total_ids as u64).to_le_bytes());

        let mut strings: Vec<u8> = Vec::new();
        let mut ids_bytes: Vec<u8> = Vec::new();
        for (key, ids) in &entries {
            for iv in &key.0 {
                let (tag, v_i64, v_len) = match iv {
                    IndexValue::Null => (TAG_NULL, 0i64, 0u32),
                    IndexValue::Boolean(b) => (TAG_BOOL, *b as i64, 0),
                    IndexValue::Integer(i) => (TAG_INT, *i, 0),
                    IndexValue::Float(fl) => (TAG_FLOAT, fl.to_bits() as i64, 0),
                    IndexValue::DateTime(ms) => (TAG_DATETIME, *ms, 0),
                    IndexValue::String(s) => {
                        let off = strings.len() as i64;
                        strings.extend_from_slice(s.as_bytes());
                        (TAG_STRING, off, s.len() as u32)
                    }
                };
                body.push(tag);
                body.extend_from_slice(&v_i64.to_le_bytes());
                body.extend_from_slice(&v_len.to_le_bytes());
            }
            body.extend_from_slice(&((ids_bytes.len()) as u64).to_le_bytes());
            body.extend_from_slice(&(ids.len() as u32).to_le_bytes());
            for &id in ids.iter() {
                ids_bytes.extend_from_slice(&id.to_le_bytes());
            }
        }
        body.extend_from_slice(&strings);
        body.extend_from_slice(&ids_bytes);

        let tmp = self.path.with_extension("mcidx.tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&body)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;

        self.overlay.clear();
        self.removed.clear();
        let reopened = Self::open(&self.path)?;
        self.mmap = reopened.mmap;
        self.layout = reopened.layout;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::CompositeIndex;
    use serde_json::json;
    use tempfile::tempdir;

    fn doc(region: &str, dept: &str, salary: i64) -> Value {
        json!({"region": region, "dept": dept, "salary": salary})
    }

    /// Collect merged entries through the public walk.
    fn dump(idx: &MmapCompositeIndex, prefix: &[IndexValue]) -> Vec<(Vec<IndexValue>, Vec<u64>)> {
        let mut out = Vec::new();
        idx.for_each_prefix_entries(prefix, |slots, ids| {
            out.push((slots.to_vec(), ids.iter().copied().collect()));
            true
        });
        out
    }

    /// Reference: the in-RAM CompositeIndex fed the same operations.
    fn dump_ram(idx: &CompositeIndex, prefix: &[IndexValue]) -> Vec<(Vec<IndexValue>, Vec<u64>)> {
        let mut out = Vec::new();
        if prefix.is_empty() {
            idx.for_each_entry(|slots, ids| {
                out.push((slots.to_vec(), ids.iter().copied().collect()));
                true
            });
        } else {
            idx.for_each_prefix_entries(prefix, |slots, ids| {
                out.push((slots.to_vec(), ids.iter().copied().collect()));
                true
            });
        }
        out
    }

    #[test]
    fn matches_in_ram_composite_through_persist_reopen_and_mutation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("c.mcidx");
        let fields = vec!["region".to_string(), "dept".to_string(), "salary".to_string()];

        let mut disk = MmapCompositeIndex::new(fields.clone());
        disk.set_path(path.clone());
        let mut ram = CompositeIndex::new(fields);

        let docs: Vec<(u64, Value)> = vec![
            (1, doc("EU", "eng", 100)),
            (2, doc("EU", "eng", 100)), // same key as 1
            (3, doc("EU", "ops", 50)),
            (4, doc("US", "eng", 70)),
            (5, json!({"region": "EU", "salary": 10})), // dept missing → Null slot
        ];
        for (id, d) in &docs {
            disk.insert_value(*id, d);
            ram.insert_value(*id, d);
        }
        assert_eq!(disk.count_all(), 5);
        assert_eq!(dump(&disk, &[]), dump_ram(&ram, &[]));

        // Persist → mmap-backed; overlay empty; identical view.
        disk.persist().unwrap();
        assert_eq!(disk.count_all(), 5);
        assert_eq!(dump(&disk, &[]), dump_ram(&ram, &[]));

        // Reopen from disk: same view, total seeded from headers.
        let mut disk = MmapCompositeIndex::open(&path).unwrap();
        disk.set_path(path.clone());
        assert_eq!(disk.count_all(), 5);
        assert_eq!(dump(&disk, &[]), dump_ram(&ram, &[]));

        // Mutate on top of the mmap: overlay insert (key collision with an
        // mmap entry), a brand-new key, a tombstone, a dup remove, and a
        // remove of an overlay-resident pair.
        let extra = [
            (6, doc("EU", "eng", 100)),  // collides with mmap key
            (7, doc("AP", "hr", 5)),     // new key, sorts first
        ];
        for (id, d) in &extra {
            disk.insert_value(*id, d);
            ram.insert_value(*id, d);
        }
        disk.remove_value(3, &doc("EU", "ops", 50)); // tombstone mmap pair
        ram.remove_value(3, &doc("EU", "ops", 50));
        disk.remove_value(3, &doc("EU", "ops", 50)); // dup: no double count
        disk.remove_value(7, &doc("AP", "hr", 5)); // overlay remove
        ram.remove_value(7, &doc("AP", "hr", 5));
        assert_eq!(disk.count_all(), 5);
        assert_eq!(dump(&disk, &[]), dump_ram(&ram, &[]));

        // Prefix walks agree (both the pinned-region shape and a full one).
        let eu = [IndexValue::String("EU".into())];
        assert_eq!(dump(&disk, &eu), dump_ram(&ram, &eu));
        let eu_eng = [IndexValue::String("EU".into()), IndexValue::String("eng".into())];
        assert_eq!(dump(&disk, &eu_eng), dump_ram(&ram, &eu_eng));

        // Persist folds tombstones + overlay; reopen and re-check.
        disk.persist().unwrap();
        let disk = MmapCompositeIndex::open(&path).unwrap();
        assert_eq!(disk.count_all(), 5);
        assert_eq!(dump(&disk, &[]), dump_ram(&ram, &[]));
        assert_eq!(dump(&disk, &eu), dump_ram(&ram, &eu));
    }

    #[test]
    fn remove_of_key_past_every_mmap_entry_is_safe() {
        // lower_bound lands one past the last entry — the decode helpers
        // must treat that as absent, not read into the string table.
        let dir = tempdir().unwrap();
        let path = dir.path().join("c.mcidx");
        let mut idx = MmapCompositeIndex::new(vec!["a".into(), "b".into()]);
        idx.set_path(path.clone());
        idx.insert_value(1, &json!({"a": 1, "b": 1}));
        idx.persist().unwrap();
        let mut idx = MmapCompositeIndex::open(&path).unwrap();
        idx.set_path(path);
        idx.remove_value(99, &json!({"a": 9, "b": 9})); // sorts after everything
        assert_eq!(idx.count_all(), 1);
        assert_eq!(dump(&idx, &[]).len(), 1);
    }

    #[test]
    fn skip_clean_persist_and_early_stop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("c.mcidx");
        let mut idx = MmapCompositeIndex::new(vec!["a".into(), "b".into()]);
        idx.set_path(path.clone());
        for i in 0..10u64 {
            idx.insert_value(i, &json!({"a": (i % 3) as i64, "b": i as i64}));
        }
        idx.persist().unwrap();
        let mtime = fs::metadata(&path).unwrap().modified().unwrap();
        idx.persist().unwrap(); // clean → must not rewrite
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), mtime);

        // Early termination: callback false stops the walk.
        let mut seen = 0;
        idx.for_each_prefix_entries(&[], |_, _| {
            seen += 1;
            seen < 3
        });
        assert_eq!(seen, 3);
    }
}
