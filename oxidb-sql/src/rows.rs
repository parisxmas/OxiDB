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

use crate::storage::{MappedSnapshot, SnapshotCursor};
use crate::types::{Value, ValueRef};

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

    /// Rewrite one physical slot's cell in every stored row (`ALTER COLUMN
    /// TYPE`). Rows too narrow to reach the slot are left as-is — their
    /// logical value comes from the `fill` template, which the caller
    /// regenerates from the column's (also-cast) default. In disk-first mode
    /// the immutable mmap'd base materializes into the overlay; the caller
    /// checkpoints right after, folding it into a fresh snapshot.
    pub fn rewrite_slot(&mut self, slot: usize, f: &dyn Fn(&Value) -> Value) {
        self.wgen = self.wgen.wrapping_add(1);
        match &mut self.mode {
            RowMode::Resident(m) => {
                for cells in m.values_mut() {
                    if let Some(c) = cells.get_mut(slot) {
                        *c = f(c);
                    }
                }
            }
            RowMode::DiskFirst(d) => {
                if let Some(base) = d.base.take() {
                    for (id, cells) in base.entries() {
                        d.overlay.entry(id).or_insert(Some(cells));
                    }
                }
                // Deletes of base rows are meaningless once the base is gone.
                d.overlay.retain(|_, change| change.is_some());
                for cells in d.overlay.values_mut().flatten() {
                    if let Some(c) = cells.get_mut(slot) {
                        *c = f(c);
                    }
                }
                d.live = d.overlay.len();
            }
        }
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

    /// `true` when the stored rows are *exactly* the attached base snapshot —
    /// disk-first, a base present, and nothing changed since it was attached.
    ///
    /// A checkpoint uses this to reuse a table's existing files instead of
    /// rewriting them. It is derived from the store rather than tracked as a
    /// dirty flag on purpose: every mutation path already funnels through
    /// insert/remove/compact/rewrite_slot, each of which leaves an overlay
    /// entry, so a write cannot fail to be noticed here. A flag would have to be
    /// set in each of those places and would silently publish a stale snapshot
    /// the day one was missed.
    pub fn is_base_only(&self) -> bool {
        match &self.mode {
            RowMode::Resident(_) => false,
            RowMode::DiskFirst(d) => d.base.is_some() && d.overlay.is_empty(),
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
    /// The stored cells of one row, **borrowed where they already exist**.
    ///
    /// [`raw`](RowStore::raw) clones unconditionally, which is right when the
    /// caller keeps the row and wrong when it only reads it. An index lookup
    /// reads: it verifies the key and hands the row to a visitor. Cloning there
    /// copied every column of every candidate — in resident mode, out of a
    /// `Vec` sitting in memory.
    ///
    /// Only the disk-first base has to materialize, because its rows live
    /// encoded in the mmap.
    fn raw_ref(&self, row_id: u64) -> Option<Cow<'_, [Value]>> {
        match &self.mode {
            RowMode::Resident(m) => m.get(&row_id).map(|c| Cow::Borrowed(c.as_slice())),
            RowMode::DiskFirst(d) => match d.overlay.get(&row_id) {
                Some(Some(cells)) => Some(Cow::Borrowed(cells.as_slice())),
                Some(None) => None,
                None => d.base.as_ref().and_then(|b| b.get(row_id)).map(Cow::Owned),
            },
        }
    }

    /// [`get_physical`](RowStore::get_physical) without the copy: the row is
    /// padded to physical arity only when it is actually narrow, which is the
    /// only case that needs to own.
    pub fn physical_ref(&self, row_id: u64) -> Option<Cow<'_, [Value]>> {
        let cells = self.raw_ref(row_id)?;
        Some(pad_cow(&self.fill, cells))
    }

    /// The query-visible view of a row the caller already holds physically.
    /// Borrowed unless a `DROP COLUMN` means the two differ.
    pub fn logical_ref<'a>(&'a self, phys: Cow<'a, [Value]>) -> Cow<'a, [Value]> {
        match self.projected {
            true => Cow::Owned(project_row(&self.fill, &self.live, &phys)),
            false => phys,
        }
    }

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
                base: d.base.as_ref().map(|b| b.cursor()),
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
    /// Walk every live row in `row_id` order, handing each to `f` as the
    /// **query-visible** (logical) cells, reusing one buffer for rows that must
    /// be decoded.
    ///
    /// The iterator form has to yield an owned `Vec` for a disk-first base row,
    /// because a borrow of its own scratch cannot outlive `next`. That made a
    /// scan of a million-row snapshot allocate a million vectors. A push loop
    /// can hold the scratch itself, so it allocates one.
    ///
    /// Resident rows and overlay rows are still handed over borrowed.
    ///
    /// `want` names the columns to decode; `None` decodes everything. A base
    /// row's unwanted cells are
    /// skipped rather than materialized and arrive as `Value::Null` at their own
    /// positions — a scan reading a few of a wide table's columns stops paying
    /// for the rest, which in disk-first mode is an allocation and a copy for
    /// every text cell. Rows that are already materialized (resident mode, or the
    /// overlay) are handed over whole: they cost nothing to pass and masking them
    /// would only hide values the caller may legitimately read.
    ///
    /// Masking is **declined when the table has a dropped column**: `want` is in
    /// query-visible column positions, while the skip happens in stored
    /// (physical) ones, and after a `DROP COLUMN` those differ. Translating the
    /// mask is possible but it is the one case where getting it wrong returns
    /// wrong data, so this takes the full decode instead.
    pub fn visit_rows_masked(
        &self,
        want: Option<&[bool]>,
        f: &mut dyn FnMut(u64, &[Value]) -> crate::error::Result<bool>,
    ) -> crate::error::Result<()> {
        let fill = self.fill.as_slice();
        // Padding a narrow row (one written before an `ADD COLUMN`) needs a
        // buffer of its own; the common case never touches it.
        let mut pad: Vec<Value> = Vec::new();
        let projected = self.projected;
        let live = self.live.as_slice();
        let want = match projected {
            true => None, // see the note above: physical positions differ
            false => want,
        };
        self.walk_raw(want, &mut |id, cells| {
            // Pad a row written before an `ADD COLUMN`, then project away any
            // slot a `DROP COLUMN` tombstoned. Neither happens on the common
            // path, so neither buffer is touched there.
            let padded: &[Value] = if cells.len() < fill.len() {
                pad.clear();
                pad.extend_from_slice(cells);
                pad.extend_from_slice(&fill[cells.len()..]);
                &pad
            } else {
                cells
            };
            if !projected {
                return f(id, padded);
            }
            let logical: Vec<Value> = live
                .iter()
                .map(|&s| padded.get(s).cloned().unwrap_or_else(|| fill[s].clone()))
                .collect();
            f(id, &logical)
        })
    }

    /// Walk every live row in `row_id` order, handing each to `f` in full
    /// **physical** layout — tombstoned slots included, narrow rows padded to
    /// physical arity. The push counterpart of [`Self::iter_physical`], and for
    /// the same reason: a checkpoint walks a table once per index plus once for
    /// the snapshot, and the iterator form allocated an owned `Vec` per row on
    /// every one of those walks.
    pub fn visit_physical(
        &self,
        f: &mut dyn FnMut(u64, &[Value]) -> crate::error::Result<bool>,
    ) -> crate::error::Result<()> {
        let fill = self.fill.as_slice();
        let mut pad: Vec<Value> = Vec::new();
        self.walk_raw(None, &mut |id, cells| {
            let padded: &[Value] = if cells.len() < fill.len() {
                pad.clear();
                pad.extend_from_slice(cells);
                pad.extend_from_slice(&fill[cells.len()..]);
                &pad
            } else {
                cells
            };
            f(id, padded)
        })
    }

    /// Walk every live row in `row_id` order, handing each to `f` as **borrowed**
    /// query-visible cells: a base row's text and bytes point into the mapping
    /// rather than being copied out of it.
    ///
    /// Only the columns in `want` are decoded; the rest are `Null` placeholders,
    /// as in [`Self::visit_rows_masked`]. Overlay and resident rows are already
    /// materialized, so their cells are borrowed from the store itself
    /// ([`Value::as_ref`]) — same shape, nothing copied either way.
    ///
    /// Declined, by handing back `None`, when the table has a dropped column: the
    /// mask is in query-visible positions and the skip happens in stored ones, and
    /// after a `DROP COLUMN` those differ. The caller then takes the owned path.
    /// A decimal cell cannot be borrowed (`Value::as_ref` cannot represent one),
    /// so a caller that may meet decimals must compare through the fallback in
    /// `eq_value_ref` — which it does.
    pub fn visit_rows_refs<'a>(
        &'a self,
        want: &[bool],
        f: &mut dyn FnMut(u64, &[ValueRef<'a>]) -> crate::error::Result<bool>,
    ) -> Option<crate::error::Result<()>> {
        if self.projected {
            return None;
        }
        Some(self.walk_refs(want, self.fill.as_slice(), f))
    }

    fn walk_refs<'a>(
        &'a self,
        want: &[bool],
        fill: &'a [Value],
        f: &mut dyn FnMut(u64, &[ValueRef<'a>]) -> crate::error::Result<bool>,
    ) -> crate::error::Result<()> {
        // One buffer for the borrowed cells, reused per row, plus the padding
        // template converted once — a row written before an `ADD COLUMN` is
        // shorter than the layout and its tail comes from `fill`.
        let mut cells: Vec<ValueRef<'a>> = Vec::new();
        // A row written before an `ADD COLUMN` is short; its tail reads from the
        // layout's default template, exactly as the owned walk pads it. Padding
        // with `Null` instead would answer a query about the new column with NULL
        // where its default is something else.
        let pad_from = |cells: &mut Vec<ValueRef<'a>>| {
            while cells.len() < fill.len() {
                cells.push(fill[cells.len()].as_ref());
            }
        };
        match &self.mode {
            RowMode::Resident(m) => {
                for (id, row) in m {
                    cells.clear();
                    cells.extend(row.iter().map(|v| v.as_ref()));
                    pad_from(&mut cells);
                    if !f(*id, &cells)? {
                        break;
                    }
                }
            }
            RowMode::DiskFirst(d) => {
                let mut base = d.base.as_ref().map(|b| b.cursor());
                let mut overlay = d.overlay.iter().peekable();
                loop {
                    let base_id = base.as_ref().and_then(|c| c.row_id());
                    let over_id = overlay.peek().map(|(id, _)| **id);
                    let take_base = match (base_id, over_id) {
                        (None, None) => break,
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        (Some(b), Some(o)) => b < o,
                    };
                    if take_base {
                        let cur = base.as_ref().expect("a base id implies a cursor");
                        cur.decode_refs_into(want, &mut cells);
                        pad_from(&mut cells);
                        let id = base_id.expect("checked above");
                        let go = f(id, &cells)?;
                        base.as_mut().expect("a base id implies a cursor").advance();
                        if !go {
                            break;
                        }
                    } else {
                        let (id, change) = overlay.next().expect("peeked");
                        if base_id == Some(*id) {
                            base.as_mut().expect("a base id implies a cursor").advance();
                        }
                        match change {
                            Some(row) => {
                                cells.clear();
                                cells.extend(row.iter().map(|v| v.as_ref()));
                                pad_from(&mut cells);
                                if !f(*id, &cells)? {
                                    break;
                                }
                            }
                            None => continue,
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The shared walk under [`Self::visit_rows`] and [`Self::visit_physical`]:
    /// every live row in `row_id` order, raw (unpadded, unprojected), decoded
    /// into one reused buffer. `f` returns `false` to stop early.
    fn walk_raw(
        &self,
        want: Option<&[bool]>,
        f: &mut dyn FnMut(u64, &[Value]) -> crate::error::Result<bool>,
    ) -> crate::error::Result<()> {
        match &self.mode {
            RowMode::Resident(m) => {
                for (id, cells) in m {
                    if !f(*id, cells)? {
                        break;
                    }
                }
            }
            RowMode::DiskFirst(d) => {
                let mut buf: Vec<Value> = Vec::new();
                // A cursor rather than a position: the base is read strictly in
                // order here, and its index is sparse, so asking for record `i`
                // would re-walk the block holding it on every row.
                let mut base = d.base.as_ref().map(|b| b.cursor());
                let mut overlay = d.overlay.iter().peekable();
                loop {
                    let base_id = base.as_ref().and_then(|c| c.row_id());
                    let over_id = overlay.peek().map(|(id, _)| **id);
                    let take_base = match (base_id, over_id) {
                        (None, None) => break,
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        // The overlay shadows the base at the same id.
                        (Some(b), Some(o)) => b < o,
                    };
                    if take_base {
                        let cur = base.as_mut().expect("a base id implies a cursor");
                        match want {
                            Some(w) => cur.decode_into_masked(w, &mut buf),
                            None => cur.decode_into(&mut buf),
                        }
                        let id = base_id.expect("checked above");
                        cur.advance();
                        if !f(id, &buf)? {
                            break;
                        }
                    } else {
                        let (id, change) = overlay.next().expect("peeked");
                        // A tombstone, or an upsert shadowing the base row of
                        // the same id — skip the base copy either way.
                        if base_id == Some(*id) {
                            base.as_mut().expect("a base id implies a cursor").advance();
                        }
                        match change {
                            Some(cells) => {
                                if !f(*id, cells)? {
                                    break;
                                }
                            }
                            None => continue,
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn iter_physical(&self) -> Box<dyn Iterator<Item = (u64, Cow<'_, [Value]>)> + '_> {
        let fill = self.fill.as_slice();
        Box::new(self.iter_raw().map(move |(id, c)| (id, pad_cow(fill, c))))
    }
}

/// Ordered merge of the mmap'd base snapshot with the RAM overlay. On an id
/// collision the overlay wins (upsert shadows, tombstone hides).
///
/// The base side is a [`SnapshotCursor`] for the same reason the push walk uses
/// one: it reads records in order, and the snapshot's index is sparse.
struct MergeIter<'a> {
    base: Option<SnapshotCursor<'a>>,
    overlay: std::iter::Peekable<std::collections::btree_map::Iter<'a, u64, Option<Vec<Value>>>>,
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = (u64, Cow<'a, [Value]>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let base_id = self.base.as_ref().and_then(|c| c.row_id());
            let over_id = self.overlay.peek().map(|(id, _)| **id);

            match (base_id, over_id) {
                (None, None) => return None,
                (Some(bid), None) => {
                    let cur = self.base.as_mut().expect("a base id implies a cursor");
                    let row = cur.decode();
                    cur.advance();
                    return Some((bid, Cow::Owned(row)));
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
                        let cur = self.base.as_mut().expect("a base id implies a cursor");
                        let row = cur.decode();
                        cur.advance();
                        return Some((bid, Cow::Owned(row)));
                    }
                    if bid == oid {
                        // The overlay shadows the base row.
                        self.base
                            .as_mut()
                            .expect("a base id implies a cursor")
                            .advance();
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
