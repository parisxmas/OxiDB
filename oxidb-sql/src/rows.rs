//! The per-table row store, in one of two modes:
//!
//! - **Resident** (default): every live row is a `Vec<Value>` in a `BTreeMap`
//!   — today's behavior; fastest, but the dataset must fit in RAM.
//! - **Disk-first** (`OXIDB_SQL_DISK_FIRST`): the bulk of the data stays on
//!   disk in the last checkpoint's mmap'd `.rdat` snapshot; RAM holds only a
//!   small overlay of changes made since that checkpoint (`Some(cells)` =
//!   upsert, `None` = delete). Auto-checkpointing bounds the overlay: each
//!   checkpoint folds it into a new snapshot and clears it.
//!
//! Both modes share the same on-disk format (`.rdat` + WAL), so a database
//! can be reopened in either mode.

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::storage::MappedSnapshot;
use crate::types::Value;

enum RowMode {
    Resident(BTreeMap<u64, Vec<Value>>),
    DiskFirst(DiskRows),
}

/// A per-table row store plus a monotonic `gen` bumped on every mutation. All
/// row writes funnel through insert/remove/rewrite_all/attach_base — a single
/// choke point — so a contiguous scan cache keyed on `gen` can never miss a
/// write and serve stale rows.
pub(crate) struct RowStore {
    mode: RowMode,
    wgen: u64,
}

pub(crate) struct DiskRows {
    /// The last checkpoint's snapshot, if the table has ever been checkpointed.
    base: Option<MappedSnapshot>,
    /// Changes since the checkpoint: `Some` upserts, `None` deletes a base row.
    overlay: BTreeMap<u64, Option<Vec<Value>>>,
    /// Live row count across base + overlay.
    live: usize,
}

impl RowStore {
    pub fn new(disk_first: bool) -> RowStore {
        let mode = if disk_first {
            RowMode::DiskFirst(DiskRows {
                base: None,
                overlay: BTreeMap::new(),
                live: 0,
            })
        } else {
            RowMode::Resident(BTreeMap::new())
        };
        RowStore { mode, wgen: 0 }
    }

    /// The mutation generation — bumped on every write. A scan cache built at
    /// this value stays valid until it changes.
    pub fn generation(&self) -> u64 {
        self.wgen
    }

    /// True for the RAM-resident mode (the only mode a contiguous scan cache
    /// materializes for; disk-first stays lazy over its mmap).
    pub fn is_resident(&self) -> bool {
        matches!(self.mode, RowMode::Resident(_))
    }

    /// Adopt `snap` as the new base (after open or a checkpoint). The overlay
    /// is cleared: the snapshot it was folded into now carries those changes.
    pub fn attach_base(&mut self, snap: MappedSnapshot) {
        self.wgen = self.wgen.wrapping_add(1);
        if let RowMode::DiskFirst(d) = &mut self.mode {
            d.live = snap.len();
            d.base = Some(snap);
            d.overlay.clear();
        }
    }

    pub fn len(&self) -> usize {
        match &self.mode {
            RowMode::Resident(m) => m.len(),
            RowMode::DiskFirst(d) => d.live,
        }
    }

    pub fn contains(&self, row_id: u64) -> bool {
        match &self.mode {
            RowMode::Resident(m) => m.contains_key(&row_id),
            RowMode::DiskFirst(d) => match d.overlay.get(&row_id) {
                Some(Some(_)) => true,
                Some(None) => false,
                None => d.base.as_ref().is_some_and(|b| b.contains(row_id)),
            },
        }
    }

    /// The row's cells, decoded/cloned into an owned vector.
    pub fn get(&self, row_id: u64) -> Option<Vec<Value>> {
        match &self.mode {
            RowMode::Resident(m) => m.get(&row_id).cloned(),
            RowMode::DiskFirst(d) => match d.overlay.get(&row_id) {
                Some(Some(cells)) => Some(cells.clone()),
                Some(None) => None,
                None => d.base.as_ref().and_then(|b| b.get(row_id)),
            },
        }
    }

    pub fn insert(&mut self, row_id: u64, cells: Vec<Value>) {
        self.wgen = self.wgen.wrapping_add(1);
        match &mut self.mode {
            RowMode::Resident(m) => {
                m.insert(row_id, cells);
            }
            RowMode::DiskFirst(d) => {
                let existed = match d.overlay.get(&row_id) {
                    Some(Some(_)) => true,
                    Some(None) => false,
                    None => d.base.as_ref().is_some_and(|b| b.contains(row_id)),
                };
                d.overlay.insert(row_id, Some(cells));
                if !existed {
                    d.live += 1;
                }
            }
        }
    }

    /// Remove a row, returning its previous cells (needed for index upkeep).
    pub fn remove(&mut self, row_id: u64) -> Option<Vec<Value>> {
        self.wgen = self.wgen.wrapping_add(1);
        match &mut self.mode {
            RowMode::Resident(m) => m.remove(&row_id),
            RowMode::DiskFirst(d) => {
                let in_base = d.base.as_ref().is_some_and(|b| b.contains(row_id));
                match d.overlay.get(&row_id) {
                    Some(None) => None, // already deleted
                    Some(Some(_)) => {
                        // Tombstone only if a base row would otherwise resurface.
                        let old = if in_base {
                            d.overlay.insert(row_id, None)
                        } else {
                            d.overlay.remove(&row_id)
                        };
                        d.live -= 1;
                        old.flatten()
                    }
                    None => {
                        if !in_base {
                            return None;
                        }
                        let old = d.base.as_ref().and_then(|b| b.get(row_id));
                        d.overlay.insert(row_id, None);
                        d.live -= 1;
                        old
                    }
                }
            }
        }
    }

    /// Rewrite every live row in place (`ALTER TABLE` shape changes). In
    /// disk-first mode the mmap'd base is immutable, so its rows materialize
    /// into the overlay with the new shape (the caller checkpoints right
    /// after, folding everything into a fresh snapshot).
    pub fn rewrite_all(&mut self, f: impl Fn(&mut Vec<Value>)) {
        self.wgen = self.wgen.wrapping_add(1);
        match &mut self.mode {
            RowMode::Resident(m) => {
                for cells in m.values_mut() {
                    f(cells);
                }
            }
            RowMode::DiskFirst(d) => {
                if let Some(base) = d.base.take() {
                    for (id, cells) in base.entries() {
                        d.overlay.entry(id).or_insert(Some(cells));
                    }
                }
                d.overlay.retain(|_, change| change.is_some());
                for cells in d.overlay.values_mut().flatten() {
                    f(cells);
                }
                d.live = d.overlay.len();
            }
        }
    }

    /// All live rows in ascending `row_id` order. Resident rows are borrowed;
    /// disk-first base rows are decoded on the fly.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (u64, Cow<'_, [Value]>)> + '_> {
        match &self.mode {
            RowMode::Resident(m) => {
                Box::new(m.iter().map(|(id, c)| (*id, Cow::Borrowed(c.as_slice()))))
            }
            RowMode::DiskFirst(d) => Box::new(MergeIter {
                base: d.base.as_ref(),
                base_pos: 0,
                overlay: d.overlay.iter().peekable(),
            }),
        }
    }
}

/// Ordered merge of the mmap'd base snapshot with the RAM overlay. On an id
/// collision the overlay wins (upsert shadows, tombstone hides).
struct MergeIter<'a> {
    base: Option<&'a MappedSnapshot>,
    base_pos: usize,
    overlay: std::iter::Peekable<std::collections::btree_map::Iter<'a, u64, Option<Vec<Value>>>>,
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = (u64, Cow<'a, [Value]>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let base_id = self
                .base
                .filter(|b| self.base_pos < b.len())
                .map(|b| b.row_id_at(self.base_pos));
            let over_id = self.overlay.peek().map(|(id, _)| **id);

            match (base_id, over_id) {
                (None, None) => return None,
                (Some(bid), None) => {
                    let b = self.base.unwrap();
                    let i = self.base_pos;
                    self.base_pos += 1;
                    return Some((bid, Cow::Owned(b.decode_at(i))));
                }
                (None, Some(_)) => {
                    let (id, change) = self.overlay.next().unwrap();
                    match change {
                        Some(cells) => return Some((*id, Cow::Borrowed(cells.as_slice()))),
                        None => continue, // tombstone for a base row we've passed
                    }
                }
                (Some(bid), Some(oid)) => {
                    if bid < oid {
                        let b = self.base.unwrap();
                        let i = self.base_pos;
                        self.base_pos += 1;
                        return Some((bid, Cow::Owned(b.decode_at(i))));
                    }
                    if bid == oid {
                        self.base_pos += 1; // overlay shadows the base row
                    }
                    let (id, change) = self.overlay.next().unwrap();
                    match change {
                        Some(cells) => return Some((*id, Cow::Borrowed(cells.as_slice()))),
                        None => continue, // deleted base row
                    }
                }
            }
        }
    }
}
