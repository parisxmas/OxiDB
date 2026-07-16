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
/// row writes funnel through insert/remove/attach_base/set_layout — a single
/// choke point — so a contiguous scan cache keyed on `gen` can never miss a
/// write and serve stale rows.
pub(crate) struct RowStore {
    mode: RowMode,
    wgen: u64,
    /// Per-**physical**-slot default template, at the table's full physical
    /// arity (including tombstoned columns). A stored row physically narrower
    /// than this — an existing row after a metadata-only `ADD COLUMN`, which
    /// does not rewrite stored rows — is padded up to this width on read, its
    /// missing trailing cells filled from here. Empty until
    /// [`set_layout`](RowStore::set_layout) is called.
    fill: Vec<Value>,
    /// Physical slot indices of the live columns, in logical order — the read
    /// projection from a stored (physical) row to the query-visible (logical)
    /// row. The identity `0..fill.len()` until a `DROP COLUMN` tombstones a
    /// slot; [`projected`](RowStore::projected) caches whether it still is.
    live: Vec<usize>,
    /// `true` once `live` is no longer the contiguous identity — i.e. a column
    /// has been dropped, so logical reads must gather cells by `live` rather
    /// than serve the stored row (padded) as-is. Keeps the no-drop path a plain
    /// length compare with no per-row projection.
    projected: bool,
}

/// Widen `row` to full physical width, filling any missing trailing cells from
/// `fill`. A row already at (or past) full width is returned untouched — still
/// borrowed in the resident scan path, so the common case never clones.
fn pad_cow<'a>(fill: &[Value], row: Cow<'a, [Value]>) -> Cow<'a, [Value]> {
    if row.len() < fill.len() {
        let mut v = row.into_owned();
        v.extend_from_slice(&fill[v.len()..]);
        Cow::Owned(v)
    } else {
        row
    }
}

/// Project a stored (physical) row down to the live columns named by `live`,
/// taking each slot's value or its `fill` default when the row is too short to
/// hold it (a narrow row from an earlier lazy `ADD COLUMN`).
fn project_row(fill: &[Value], live: &[usize], row: &[Value]) -> Vec<Value> {
    live.iter()
        .map(|&s| row.get(s).cloned().unwrap_or_else(|| fill[s].clone()))
        .collect()
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
        RowStore {
            mode,
            wgen: 0,
            fill: Vec::new(),
            live: Vec::new(),
            projected: false,
        }
    }

    /// The mutation generation — bumped on every write. A scan cache built at
    /// this value stays valid until it changes.
    pub fn generation(&self) -> u64 {
        self.wgen
    }

    /// Set the read layout: the per-physical-slot default template (`fill`) and
    /// the live-slot projection (`live`). Bumps the generation so any scan
    /// cache built at the old schema is discarded. Called whenever the table's
    /// column set changes (`ALTER TABLE`) and once at open.
    pub fn set_layout(&mut self, live: Vec<usize>, fill: Vec<Value>) {
        self.wgen = self.wgen.wrapping_add(1);
        self.projected = live.iter().copied().ne(0..fill.len());
        self.live = live;
        self.fill = fill;
    }

    /// The query-visible (logical) width — the number of live columns.
    pub fn logical_width(&self) -> usize {
        self.live.len()
    }

    /// Physically drop the tombstoned columns: rewrite every stored row down to
    /// its live columns and return to an identity layout, so the space a lazy
    /// `DROP COLUMN` left occupied is reclaimed. The O(n) rewrite the instant
    /// DROP deferred happens here (called at checkpoint). In disk-first mode the
    /// immutable mmap'd base rows materialize (projected) into the overlay; the
    /// caller writes a fresh snapshot and re-attaches right after. A no-op when
    /// nothing is dropped.
    pub fn compact(&mut self) {
        if !self.projected {
            return;
        }
        self.wgen = self.wgen.wrapping_add(1);
        let old_live = std::mem::take(&mut self.live);
        let old_fill = std::mem::take(&mut self.fill);
        let project = |row: &[Value]| project_row(&old_fill, &old_live, row);
        match &mut self.mode {
            RowMode::Resident(m) => {
                for cells in m.values_mut() {
                    *cells = project(cells);
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
                    *cells = project(cells);
                }
                d.live = d.overlay.len();
            }
        }
        // Identity layout at the new (live) width; the caller re-derives the
        // same layout authoritatively from the compacted schema via set_layout.
        let new_fill: Vec<Value> = old_live.iter().map(|&s| old_fill[s].clone()).collect();
        self.live = (0..new_fill.len()).collect();
        self.fill = new_fill;
        self.projected = false;
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

    /// The stored (physical) cells for `row_id`, exactly as written — no
    /// padding, no projection.
    fn raw(&self, row_id: u64) -> Option<Vec<Value>> {
        match &self.mode {
            RowMode::Resident(m) => m.get(&row_id).cloned(),
            RowMode::DiskFirst(d) => match d.overlay.get(&row_id) {
                Some(Some(cells)) => Some(cells.clone()),
                Some(None) => None,
                None => d.base.as_ref().and_then(|b| b.get(row_id)),
            },
        }
    }

    /// The row's **query-visible** (logical) cells: the stored physical row
    /// projected down to the live columns, tombstoned slots removed and any
    /// narrow row padded from `fill`. This is what the executor consumes.
    pub fn get(&self, row_id: u64) -> Option<Vec<Value>> {
        let mut cells = self.raw(row_id)?;
        if self.projected {
            return Some(project_row(&self.fill, &self.live, &cells));
        }
        if cells.len() < self.fill.len() {
            cells.extend_from_slice(&self.fill[cells.len()..]);
        }
        Some(cells)
    }

    /// The row's full **physical** cells, padded to the physical arity — used
    /// for index/constraint maintenance, which addresses cells by physical
    /// slot (and must see tombstoned slots too).
    pub fn get_physical(&self, row_id: u64) -> Option<Vec<Value>> {
        let mut cells = self.raw(row_id)?;
        if cells.len() < self.fill.len() {
            cells.extend_from_slice(&self.fill[cells.len()..]);
        }
        Some(cells)
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

    /// Remove a row, returning its previous **physical** cells padded to the
    /// physical arity — index/constraint upkeep addresses them by physical
    /// slot, so a narrow row must be widened the same way it was when indexed.
    pub fn remove(&mut self, row_id: u64) -> Option<Vec<Value>> {
        let mut old = self.remove_raw(row_id)?;
        if old.len() < self.fill.len() {
            old.extend_from_slice(&self.fill[old.len()..]);
        }
        Some(old)
    }

    fn remove_raw(&mut self, row_id: u64) -> Option<Vec<Value>> {
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

    /// Raw stored (physical) rows in ascending `row_id` order — resident rows
    /// borrowed, disk-first base rows decoded on the fly. No padding, no
    /// projection.
    fn iter_raw(&self) -> Box<dyn Iterator<Item = (u64, Cow<'_, [Value]>)> + '_> {
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

    /// All live rows in ascending `row_id` order, projected to the
    /// **query-visible** (logical) schema — tombstoned columns removed, narrow
    /// rows padded from `fill`. Resident full-width rows with nothing dropped
    /// stay borrowed (the common case); dropped-column tables and narrow rows
    /// materialize owned projected rows.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (u64, Cow<'_, [Value]>)> + '_> {
        let fill = self.fill.as_slice();
        if self.projected {
            let live = self.live.as_slice();
            return Box::new(
                self.iter_raw()
                    .map(move |(id, c)| (id, Cow::Owned(project_row(fill, live, &c)))),
            );
        }
        Box::new(self.iter_raw().map(move |(id, c)| (id, pad_cow(fill, c))))
    }

    /// All live rows in ascending `row_id` order, in full **physical** layout
    /// (tombstoned slots included, narrow rows padded to physical arity). Used
    /// for index rebuilds and snapshot writes, which work in physical space.
    pub fn iter_physical(&self) -> Box<dyn Iterator<Item = (u64, Cow<'_, [Value]>)> + '_> {
        let fill = self.fill.as_slice();
        Box::new(self.iter_raw().map(move |(id, c)| (id, pad_cow(fill, c))))
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
