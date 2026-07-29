//! # oxidb-sql
//!
//! A standalone SQL engine mounted alongside the OxiDB document engine
//! (see ADR-0010). It owns its own files, catalog, storage format, WAL, and
//! recovery, and shares no state with the document engine.
//!
//! The engine exposes both a programmatic row API (`create_table`, `insert`,
//! `scan`, …) and a SQL surface via [`SqlEngine::execute`] /
//! [`SqlEngine::execute_params`]: DDL, DML, single-table and inner-join SELECT
//! with aggregation, secondary indexes, parameterized queries, and per-engine
//! transactions (`BEGIN`/`COMMIT`/`ROLLBACK`).

mod ast;
mod catalog;
mod cobra;
mod decimal;
mod error;
mod executor;
mod index_file;
pub mod json;
mod manifest;
mod parser;
mod row_locks;
mod rows;
mod storage;
mod store;
mod transaction;
mod types;
mod wal;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub use ast::QueryResult;
pub use catalog::{Column, FkAction, ForeignKey, IndexDef, Table};
pub use decimal::Decimal;
pub use error::{Result, SqlError};
pub use parser::{
    DatabaseStatement, UserStatement, parse_database_statement, parse_user_statement,
};
use types::KeyTuple;
pub use types::{SqlType, Value};

use catalog::Catalog;
use rows::RowStore;
use store::Store;
use transaction::Transaction;
use types::IndexKey;
use wal::{Wal, WalRecord};

/// Engine-level options, normally read from the environment by
/// [`SqlEngine::open`]. Both row-store modes share the same on-disk format,
/// so a database can be reopened in either mode.
#[derive(Debug, Clone)]
pub struct SqlOptions {
    /// Keep the bulk of each table on disk (mmap'd last-checkpoint snapshot)
    /// with only post-checkpoint changes in RAM, instead of holding every row
    /// resident. Env: `OXIDB_SQL_DISK_FIRST`.
    pub disk_first: bool,
    /// Auto-checkpoint when the live WAL exceeds this many bytes (folds the
    /// WAL into `.rdat` snapshots and truncates it; also bounds the RAM
    /// overlay in disk-first mode). `0` disables auto-checkpointing.
    /// Env: `OXIDB_SQL_CHECKPOINT_BYTES`.
    pub checkpoint_bytes: u64,
    /// How long `SELECT ... FOR UPDATE` / UPDATE / DELETE wait on a row lock
    /// held by another transaction before failing with a lock-timeout error
    /// (also how a deadlock resolves). Env: `OXIDB_SQL_LOCK_TIMEOUT_MS`.
    pub lock_timeout_ms: u64,
    /// Row operations replayed between folds when opening a disk-first
    /// database. Bounds how much of a WAL tail is ever materialized at once.
    ///
    /// Small enough that a large tail cannot dominate the process, large enough
    /// that an ordinary restart folds once or not at all — a fold rewrites
    /// every table, so a tiny value turns recovery into repeated full rewrites.
    /// Exposed mainly so tests can reach the mid-replay path without writing
    /// hundreds of thousands of rows.
    pub replay_fold_ops: usize,
}

impl Default for SqlOptions {
    fn default() -> Self {
        SqlOptions {
            disk_first: false,
            checkpoint_bytes: 64 << 20, // 64 MiB
            lock_timeout_ms: 5_000,
            replay_fold_ops: REPLAY_FOLD_OPS,
        }
    }
}

impl SqlOptions {
    pub fn from_env() -> Self {
        let mut opts = SqlOptions::default();
        if let Ok(v) = std::env::var("OXIDB_SQL_DISK_FIRST") {
            opts.disk_first =
                matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on");
        }
        if let Ok(v) = std::env::var("OXIDB_SQL_CHECKPOINT_BYTES")
            && let Ok(n) = v.trim().parse::<u64>()
        {
            opts.checkpoint_bytes = n;
        }
        if let Ok(v) = std::env::var("OXIDB_SQL_LOCK_TIMEOUT_MS")
            && let Ok(n) = v.trim().parse::<u64>()
        {
            opts.lock_timeout_ms = n;
        }
        opts
    }
}

/// An in-memory secondary index over one or more columns:
/// key tuple -> set of row ids.
/// The row ids one index key points at, kept sorted and ascending.
///
/// A `BTreeSet<u64>` was the obvious choice and the expensive one: an empty set
/// is 24 bytes, but the moment it holds a single id it allocates a whole leaf
/// node — sized for eleven keys whether it has one or eleven — so a key
/// matching one row cost about 150 bytes to say so. Index keys are mostly
/// selective, so that was the common case, not the corner.
///
/// A sorted inline vector answers the same three questions (insert, remove,
/// iterate in order) with zero allocations for a unique key and one small one
/// beyond that. Insert and remove are a binary search plus a memmove, which for
/// the posting-list sizes indexes actually produce beats pointer-chasing a
/// tree.
#[derive(Default, Debug, Clone)]
struct RowIds(smallvec::SmallVec<[u64; 1]>);

impl RowIds {
    fn insert(&mut self, id: u64) {
        if let Err(at) = self.0.binary_search(&id) {
            self.0.insert(at, id);
        }
    }

    fn remove(&mut self, id: u64) {
        if let Ok(at) = self.0.binary_search(&id) {
            self.0.remove(at);
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = &u64> {
        self.0.iter()
    }
}

/// One column-level `UNIQUE` constraint: the value -> row id mapping that
/// enforces it, split the same way the primary key is.
///
/// `map` holds only what has been written since the last checkpoint; `base` is
/// that checkpoint's file, mapped. Both are hints and both are verified against
/// the live row — a `UNIQUE` column is a constraint, so the cost of trusting a
/// stale one is a wrong answer, not a slow one.
struct UniqueCol {
    /// Physical column position.
    pos: usize,
    map: BTreeMap<IndexKey, u64>,
    base: Option<index_file::MappedIndex>,
}

/// File name a table's `UNIQUE` column is materialized under. Prefixed like
/// `$pk` so it cannot collide with a user index, and suffixed by *position*
/// rather than name so a `RENAME COLUMN` cannot silently point at a stale file.
fn unique_index_name(pos: usize) -> String {
    format!("$uq{pos}")
}

/// File name a table's primary key is materialized under, alongside its
/// secondary indexes. `$` is not legal in an identifier, so this can never
/// collide with a user-created index on the same table.
const PK_INDEX_NAME: &str = "$pk";

/// PRIMARY KEY tuple -> `row_id`, in whichever representation the key shape
/// allows.
///
/// The general form has to be `BTreeMap<KeyTuple, u64>`, because a key may be
/// composite and may hold any value. But the overwhelmingly common primary key
/// is a single integer column, and paying the general price for it is what made
/// this the engine's single largest resident structure: measured at **103 bytes
/// per row** for an `INT PRIMARY KEY`, against the 16 the key and row id
/// actually occupy. The rest is a 24-byte `Value` discriminant, the `SmallVec`
/// length word, and `BTreeMap` node slack.
///
/// So a single-column integer key is stored as the bare `i64`. Everything else
/// — composite, text, anything — keeps the general map. The representation is
/// chosen from the declared column type, and *upgrades itself* if a value that
/// does not fit ever arrives (an old WAL record written before an
/// `ALTER COLUMN TYPE`, say), so the specialization can never silently lose or
/// conflate a key.
enum PkMap {
    Int(BTreeMap<i64, u64>),
    Tuple(BTreeMap<KeyTuple, u64>),
}

impl PkMap {
    /// `Int` when the key is exactly one column of an integer type.
    fn for_key(def: &Table, pk_cols: &[usize]) -> PkMap {
        match pk_cols {
            [p] if matches!(def.columns[*p].ty, SqlType::Int) => PkMap::Int(BTreeMap::new()),
            _ => PkMap::Tuple(BTreeMap::new()),
        }
    }

    /// The `i64` a key reduces to, when it reduces to one.
    fn as_int(key: &[IndexKey]) -> Option<i64> {
        match key {
            [IndexKey(Value::Int(n))] => Some(*n),
            _ => None,
        }
    }

    fn clear(&mut self) {
        match self {
            PkMap::Int(m) => m.clear(),
            PkMap::Tuple(m) => m.clear(),
        }
    }

    fn get(&self, key: &[IndexKey]) -> Option<u64> {
        match self {
            PkMap::Int(m) => Self::as_int(key).and_then(|k| m.get(&k)).copied(),
            PkMap::Tuple(m) => m.get(key).copied(),
        }
    }

    fn remove(&mut self, key: &[IndexKey]) {
        match self {
            PkMap::Int(m) => {
                if let Some(k) = Self::as_int(key) {
                    m.remove(&k);
                }
            }
            PkMap::Tuple(m) => {
                m.remove(key);
            }
        }
    }

    fn insert(&mut self, key: KeyTuple, row_id: u64) {
        if let PkMap::Int(m) = self {
            match Self::as_int(&key) {
                Some(k) => {
                    m.insert(k, row_id);
                    return;
                }
                // A key the compact form cannot hold. Rather than drop it —
                // which would lose a uniqueness constraint silently — widen the
                // whole map and carry on in the general representation.
                None => self.widen(),
            }
        }
        let PkMap::Tuple(m) = self else {
            unreachable!()
        };
        m.insert(key, row_id);
    }

    fn widen(&mut self) {
        if let PkMap::Int(m) = self {
            let general = std::mem::take(m)
                .into_iter()
                .map(|(k, rid)| (KeyTuple::from_elem(IndexKey(Value::Int(k)), 1), rid))
                .collect();
            *self = PkMap::Tuple(general);
        }
    }
}

/// A secondary index, which may not be **populated** yet.
///
/// An index that exists in the catalog costs nothing until something reads it.
/// At open the engine used to rebuild every index into RAM before answering a
/// single query — the single largest thing a restart pays for, and pure waste
/// for any index the workload never touches. Measured at 1M rows, three
/// indexes cost 318 MB to reconstruct that way.
///
/// PostgreSQL never does this: its indexes live on disk and pages enter the
/// buffer pool only when a scan needs them. `populated: false` is the same
/// bargain at the granularity available here — pay for an index when a query
/// actually uses it, not because it was declared.
///
/// An unpopulated index also needs **no maintenance**: writes skip it, because
/// it is built from the rows as they stand whenever it is finally wanted. That
/// is what keeps this from being a trade of memory for correctness.
struct SecondaryIndex {
    col_pos: Vec<usize>,
    map: BTreeMap<KeyTuple, RowIds>,
    populated: bool,
    /// The last checkpoint's `.sidx`, mapped. When present the index is served
    /// from it plus `map` as an overlay of changes since, and `populated` stays
    /// false — there is nothing to populate, which is the point.
    base: Option<index_file::MappedIndex>,
}

impl SecondaryIndex {
    fn key_of(&self, cells: &[Value]) -> KeyTuple {
        self.col_pos
            .iter()
            .map(|&p| IndexKey(cells[p].clone()))
            .collect()
    }

    /// Row ids that *might* match `key`: the mapped base plus the overlay.
    ///
    /// Candidates only — the base describes the rows as they were at the last
    /// checkpoint, so callers must check each against the live row. See
    /// [`index_file`] for why that is cheaper than maintaining tombstones.
    fn candidates(&self, key: &[IndexKey]) -> Result<Vec<u64>> {
        let mut ids = match &self.base {
            Some(base) => base.get(key)?,
            None => Vec::new(),
        };
        if let Some(extra) = self.map.get(key) {
            ids.extend(extra.iter().copied());
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    /// Whether this index can answer a lookup at all: either it has a mapped
    /// base, or it has been populated in memory.
    fn usable(&self) -> bool {
        self.base.is_some() || self.populated
    }
}

/// Runtime state for one table: its definition, its live rows keyed by a dense
/// engine-assigned `row_id`, and any secondary indexes.
struct TableState {
    def: Table,
    rows: RowStore,
    next_row_id: u64,
    /// Position of the AUTO_INCREMENT column (if any) and the next value its
    /// counter would assign. Seeded from existing data at open (max + 1), so
    /// restart semantics match SQLite's default rowid behavior.
    auto_pos: Option<usize>,
    next_auto: i64,
    /// Secondary indexes keyed by index name.
    indexes: BTreeMap<String, SecondaryIndex>,
    /// PRIMARY KEY column positions (empty when the table has none) and the
    /// key tuple -> row_id map used to enforce uniqueness on writes. A
    /// single-column key is simply a one-element tuple, so composite and
    /// simple primary keys take one code path.
    pk_cols: Vec<usize>,
    /// PRIMARY KEY -> row id for rows written since the last checkpoint. With
    /// `pk_base` present this is an *overlay*, not the whole key set.
    pk_map: PkMap,
    /// The last checkpoint's primary-key file, mapped. Same hint contract as a
    /// secondary index's base: it is the key set as of that checkpoint, and
    /// every hit is verified against the live row.
    ///
    /// This is what stops a primary key costing 34 bytes of RAM per row for the
    /// life of the process. PostgreSQL does not hold one either — a unique
    /// insert there descends the on-disk index and binary-searches a locked
    /// leaf page (`_bt_check_unique`, `src/backend/access/nbtree/nbtinsert.c`).
    pk_base: Option<index_file::MappedIndex>,
    /// Column-level `UNIQUE` constraints. NULLs are exempt (per SQL).
    uniques: Vec<UniqueCol>,
    /// Cached `def.has_dropped()` — true once a lazy `DROP COLUMN` tombstoned
    /// a column, so writes must expand logical rows to physical layout. False
    /// keeps the write path a no-op fast path.
    has_dropped: bool,
    /// Contiguous row-major scan cache (resident mode only): `(generation,
    /// width, flat cells)`. A full-table scan streams this instead of chasing
    /// the BTreeMap's scattered per-row Vecs; rebuilt lazily when the store's
    /// generation moves (any write). Repeated scans of an unchanged table
    /// (the common analytic read loop) then run over contiguous memory.
    scan_cache: Option<(u64, usize, Vec<Value>)>,
    /// The store generation seen at the last uncached scan. The cache is built
    /// only on the SECOND scan at the same generation, so a table scanned once
    /// or written-then-scanned never pays the build/memory cost — only a
    /// genuinely repeatedly-scanned (read-mostly) table caches.
    scan_seen_gen: Option<u64>,
}

impl TableState {
    fn empty(def: Table, disk_first: bool) -> Self {
        let pk_cols = def.pk_cols();
        let pk_map = PkMap::for_key(&def, &pk_cols);
        let auto_pos = def.columns.iter().position(|c| c.auto_increment);
        let uniques = def
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique && !c.primary_key)
            .map(|(i, _)| UniqueCol {
                pos: i,
                map: BTreeMap::new(),
                base: None,
            })
            .collect();
        let mut state = TableState {
            def,
            rows: RowStore::new(disk_first),
            next_row_id: 1,
            auto_pos,
            next_auto: 1,
            indexes: BTreeMap::new(),
            pk_cols,
            pk_map,
            pk_base: None,
            uniques,
            has_dropped: false,
            scan_cache: None,
            scan_seen_gen: None,
        };
        state.sync_layout();
        state
    }

    /// Refresh the row store's read layout (live-slot projection + per-slot
    /// default template) and the cached `has_dropped` flag from the current
    /// physical schema. A narrow row from a lazy `ADD COLUMN` then reads back
    /// its new column's default, and a tombstoned column from a lazy `DROP
    /// COLUMN` is projected out. Called at open and after every `ALTER TABLE`.
    fn sync_layout(&mut self) {
        self.has_dropped = self.def.has_dropped();
        let live = self.def.live_slots();
        let fill: Vec<Value> = self
            .def
            .columns
            .iter()
            .map(|c| c.default_value.clone().unwrap_or(Value::Null))
            .collect();
        self.rows.set_layout(live, fill);
    }

    /// Expand a **logical** row (live columns, as the executor builds it) into
    /// the full **physical** layout stored on disk: each live value goes to its
    /// physical slot, tombstoned slots get a `NULL` placeholder. Identity (no
    /// clone) when nothing is dropped — the common case.
    fn to_physical(&self, logical: Vec<Value>) -> Vec<Value> {
        if !self.has_dropped {
            return logical;
        }
        let mut phys = vec![Value::Null; self.def.columns.len()];
        for (val, &slot) in logical.into_iter().zip(self.def.live_slots().iter()) {
            phys[slot] = val;
        }
        phys
    }

    /// Coerce and validate a logical row (as the executor built it) against the
    /// query-visible schema, then expand it to the physical layout that gets
    /// logged and stored. The caller still runs the PRIMARY KEY / UNIQUE checks
    /// (they key on physical slots, so run them on the returned row).
    fn prepare_write(&self, mut cells: Vec<Value>) -> Result<Vec<Value>> {
        let ldef = self.def.logical();
        ldef.coerce_row(&mut cells);
        ldef.validate_row(&cells)?;
        Ok(self.to_physical(cells))
    }

    /// This row's PRIMARY KEY tuple, or `None` when the table has no primary
    /// key. `cells` is a **physical** row (`pk_cols` are physical positions).
    fn pk_key(&self, cells: &[Value]) -> Option<KeyTuple> {
        if self.pk_cols.is_empty() {
            return None;
        }
        Some(
            self.pk_cols
                .iter()
                .map(|&p| IndexKey(cells[p].clone()))
                .collect(),
        )
    }

    /// Recompute the schema-derived positions and reseed every constraint
    /// map from the current rows (used after `ALTER TABLE`).
    fn rebuild_meta(&mut self) {
        self.pk_cols = self.def.pk_cols();
        self.auto_pos = self.def.columns.iter().position(|c| c.auto_increment);
        self.uniques = self
            .def
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique && !c.primary_key)
            .map(|(i, _)| UniqueCol {
                pos: i,
                map: BTreeMap::new(),
                base: None,
            })
            .collect();
        self.pk_map.clear();
        // The mapped base was built at the *old* physical layout, so after an
        // ALTER shifts positions its keys describe different columns. Drop it
        // and rebuild the whole key set in memory below; the next checkpoint
        // writes a fresh file at the new layout. Keeping it would be worse than
        // slow — a lookup would miss a genuine duplicate.
        self.pk_base = None;
        self.next_auto = 1;
        // Constraint maps key on physical cell positions, so seed from the
        // physical rows (tombstoned slots present, narrow rows padded).
        let mut seeds: Vec<(u64, Vec<Value>)> = Vec::new();
        for (rid, cells) in self.rows.iter_physical() {
            seeds.push((rid, cells.into_owned()));
        }
        for (rid, cells) in seeds {
            if let Some(key) = self.pk_key(&cells) {
                self.pk_map.insert(key, rid);
            }
            for u in self.uniques.iter_mut() {
                if !matches!(cells[u.pos], Value::Null) {
                    u.map.insert(IndexKey(cells[u.pos].clone()), rid);
                }
            }
            self.observe_auto(&cells);
        }
    }

    fn observe_row_id(&mut self, row_id: u64) {
        if row_id >= self.next_row_id {
            self.next_row_id = row_id + 1;
        }
    }

    /// Keep the auto-increment counter ahead of every value that reaches the
    /// table — explicit inserts, WAL replay, snapshot load.
    fn observe_auto(&mut self, cells: &[Value]) {
        if let Some(p) = self.auto_pos
            && let Some(Value::Int(v)) = cells.get(p)
            && *v >= self.next_auto
        {
            self.next_auto = v + 1;
        }
    }

    /// (Re)build a secondary index over `columns` from the current rows.
    fn build_index(&mut self, index_name: &str, columns: &[String]) -> Result<()> {
        let col_pos: Vec<usize> = columns
            .iter()
            .map(|column| {
                self.def
                    .columns
                    .iter()
                    .position(|c| &c.name == column)
                    .ok_or_else(|| SqlError::NoSuchColumn(column.to_string()))
            })
            .collect::<Result<_>>()?;
        let mut idx = SecondaryIndex {
            col_pos,
            map: BTreeMap::new(),
            populated: true,
            base: None,
        };
        // `col_pos` are physical positions, so index the physical rows.
        for (rid, cells) in self.rows.iter_physical() {
            let key = idx.key_of(&cells);
            idx.map.entry(key).or_default().insert(rid);
        }
        self.indexes.insert(index_name.to_string(), idx);
        Ok(())
    }

    /// Register an index without populating it — the open path. It costs a
    /// `col_pos` vector until a query wants it.
    fn declare_index(&mut self, index_name: &str, columns: &[String]) -> Result<()> {
        let col_pos: Vec<usize> = columns
            .iter()
            .map(|column| {
                self.def
                    .columns
                    .iter()
                    .position(|c| &c.name == column)
                    .ok_or_else(|| SqlError::NoSuchColumn(column.to_string()))
            })
            .collect::<Result<_>>()?;
        self.indexes.insert(
            index_name.to_string(),
            SecondaryIndex {
                col_pos,
                map: BTreeMap::new(),
                populated: false,
                base: None,
            },
        );
        Ok(())
    }

    /// Fill an index declared but never built. Reads the rows as they stand, so
    /// the result is identical to having maintained it since open.
    fn populate_index(&mut self, index_name: &str) {
        let Some(idx) = self.indexes.get(index_name) else {
            return;
        };
        // A mapped base already answers lookups; building the map as well would
        // put the whole index back in RAM, which is what the base exists to
        // avoid.
        if idx.populated || idx.base.is_some() {
            return;
        }
        let col_pos = idx.col_pos.clone();
        let mut map: BTreeMap<KeyTuple, RowIds> = BTreeMap::new();
        for (rid, cells) in self.rows.iter_physical() {
            let key: KeyTuple = col_pos
                .iter()
                .map(|&p| IndexKey(cells[p].clone()))
                .collect();
            map.entry(key).or_default().insert(rid);
        }
        if let Some(idx) = self.indexes.get_mut(index_name) {
            idx.map = map;
            idx.populated = true;
        }
    }

    fn index_insert(&mut self, row_id: u64, cells: &[Value]) {
        for idx in self.indexes.values_mut() {
            // An index with a mapped base is live: its `map` is the overlay of
            // changes since the checkpoint the base describes. Only a declared-
            // but-never-built index is skipped.
            if !idx.usable() {
                continue;
            }
            let key = idx.key_of(cells);
            idx.map.entry(key).or_default().insert(row_id);
        }
        if let Some(key) = self.pk_key(cells) {
            self.pk_map.insert(key, row_id);
        }
        for u in self.uniques.iter_mut() {
            if !matches!(cells[u.pos], Value::Null) {
                u.map.insert(IndexKey(cells[u.pos].clone()), row_id);
            }
        }
    }

    fn index_remove(&mut self, row_id: u64, cells: &[Value]) {
        for idx in self.indexes.values_mut() {
            if !idx.usable() {
                continue;
            }
            let key = idx.key_of(cells);
            if let Some(set) = idx.map.get_mut(&key) {
                set.remove(row_id);
                if set.is_empty() {
                    idx.map.remove(&key);
                }
            }
        }
        if let Some(key) = self.pk_key(cells) {
            // Only remove the mapping if it still points at this row (an
            // idempotent WAL replay can re-insert before the old delete).
            if self.pk_owner_of(&key) == Some(row_id) {
                self.pk_map.remove(&key);
            }
        }
        for u in self.uniques.iter_mut() {
            let key = IndexKey(cells[u.pos].clone());
            if u.map.get(&key) == Some(&row_id) {
                u.map.remove(&key);
            }
        }
    }

    /// The live row owning PRIMARY KEY `key`, consulting the overlay first and
    /// then the mapped base.
    ///
    /// Both are hints and both are verified: the overlay can name a row a later
    /// statement deleted, and the base can name one deleted or re-keyed since
    /// the checkpoint. A stale hit that were *not* rejected would be a phantom
    /// duplicate-key error on a key that is actually free, so this check is the
    /// difference between a correct constraint and a broken one.
    fn pk_owner_of(&self, key: &[IndexKey]) -> Option<u64> {
        let verify = |rid: u64| -> Option<u64> {
            let phys = self.rows.get_physical(rid)?;
            (self.pk_key(&phys)?.as_slice() == key).then_some(rid)
        };
        if let Some(rid) = self.pk_map.get(key)
            && let Some(rid) = verify(rid)
        {
            return Some(rid);
        }
        let base = self.pk_base.as_ref()?;
        base.get(key).ok()?.into_iter().find_map(verify)
    }

    /// The live row owning `key` in `UNIQUE` column `u` — overlay, then mapped
    /// base, each hit verified against the live physical row for the same
    /// reason the primary key is.
    fn unique_owner_of(&self, u: &UniqueCol, key: &IndexKey) -> Option<u64> {
        let verify = |rid: u64| -> Option<u64> {
            let phys = self.rows.get_physical(rid)?;
            (phys.get(u.pos) == Some(&key.0)).then_some(rid)
        };
        if let Some(&rid) = u.map.get(key)
            && let Some(rid) = verify(rid)
        {
            return Some(rid);
        }
        let base = u.base.as_ref()?;
        base.get(std::slice::from_ref(key))
            .ok()?
            .into_iter()
            .find_map(verify)
    }

    /// Error if `cells`' PRIMARY KEY value already belongs to a row other
    /// than `exclude_row`. A composite key collides only when *every* member
    /// matches.
    fn check_pk(&self, cells: &[Value], exclude_row: Option<u64>) -> Result<()> {
        if let Some(key) = self.pk_key(cells)
            && let Some(existing) = self.pk_owner_of(&key)
            && Some(existing) != exclude_row
        {
            return Err(SqlError::DuplicateKey(format!(
                "PRIMARY KEY value {} already exists in {:?}",
                types::render_key(&self.pk_cols, cells),
                self.def.name
            )));
        }
        for u in &self.uniques {
            if matches!(cells[u.pos], Value::Null) {
                continue; // SQL: NULLs never collide under UNIQUE
            }
            let key = IndexKey(cells[u.pos].clone());
            if let Some(existing) = self.unique_owner_of(u, &key)
                && Some(existing) != exclude_row
            {
                return Err(SqlError::DuplicateKey(format!(
                    "UNIQUE value {:?} already exists in {:?}.{:?}",
                    cells[u.pos], self.def.name, self.def.columns[u.pos].name
                )));
            }
        }
        Ok(())
    }
}

/// One table's view of a committing batch, for the commit-time uniqueness
/// re-check (`SqlEngine::validate_batch`). Holds what the batch has done so
/// far, which is what the committed constraint maps have to be read against.
struct BatchSim<'a> {
    state: &'a TableState,
    keys: Vec<BatchKey>,
    /// Rows the batch has rewritten or deleted: what the committed maps say
    /// they own no longer stands.
    touched: BTreeSet<u64>,
}

/// One uniqueness constraint, as the commit-time re-check tracks it.
struct BatchKey {
    /// The cells that form the key.
    cols: Vec<usize>,
    /// Where the committed owner lives: `None` = the PRIMARY KEY map,
    /// `Some(i)` = `TableState::uniques[i]`.
    unique_idx: Option<usize>,
    /// Keys this batch has claimed so far, and the row that claimed each.
    claimed: BTreeMap<KeyTuple, u64>,
}

impl<'a> BatchSim<'a> {
    fn new(state: &'a TableState) -> Self {
        let mut keys = Vec::new();
        if !state.pk_cols.is_empty() {
            keys.push(BatchKey {
                cols: state.pk_cols.clone(),
                unique_idx: None,
                claimed: BTreeMap::new(),
            });
        }
        for (i, u) in state.uniques.iter().enumerate() {
            keys.push(BatchKey {
                cols: vec![u.pos],
                unique_idx: Some(i),
                claimed: BTreeMap::new(),
            });
        }
        BatchSim {
            state,
            keys,
            touched: BTreeSet::new(),
        }
    }

    /// Error if writing `cells` as `row_id` would take a key already owned by
    /// a committed row the batch hasn't touched, or by another row within it.
    fn check(&self, row_id: u64, cells: &[Value]) -> Result<()> {
        for k in &self.keys {
            // SQL: NULLs never collide under UNIQUE. PRIMARY KEY columns are
            // NOT NULL, so its map is consulted unconditionally (matching
            // `TableState::check_pk`).
            if k.unique_idx.is_some() && matches!(cells[k.cols[0]], Value::Null) {
                continue;
            }
            let key: KeyTuple = k.cols.iter().map(|&p| IndexKey(cells[p].clone())).collect();
            let committed = match k.unique_idx {
                None => self.state.pk_owner_of(&key),
                Some(i) => {
                    let u = &self.state.uniques[i];
                    self.state.unique_owner_of(u, &key[0])
                }
            };
            let taken = committed
                .filter(|rid| *rid != row_id && !self.touched.contains(rid))
                .or_else(|| k.claimed.get(&key).copied().filter(|rid| *rid != row_id));
            if taken.is_some() {
                return Err(SqlError::DuplicateKey(format!(
                    "{} value {} already exists in {:?} (committed by another writer \
                     while this transaction was open)",
                    if k.unique_idx.is_none() {
                        "PRIMARY KEY"
                    } else {
                        "UNIQUE"
                    },
                    types::render_key(&k.cols, cells),
                    self.state.def.name
                )));
            }
        }
        Ok(())
    }

    /// Record the batch's effect on `row_id`: `Some(cells)` claims its keys,
    /// `None` (a delete) just releases the old ones.
    fn claim(&mut self, row_id: u64, cells: Option<&[Value]>) {
        self.touched.insert(row_id);
        for k in &mut self.keys {
            k.claimed.retain(|_, rid| *rid != row_id);
            let Some(cells) = cells else { continue };
            if k.unique_idx.is_some() && matches!(cells[k.cols[0]], Value::Null) {
                continue;
            }
            let key: KeyTuple = k.cols.iter().map(|&p| IndexKey(cells[p].clone())).collect();
            k.claimed.insert(key, row_id);
        }
    }
}

struct Inner {
    dir: PathBuf,
    /// The committed generation whose `gen.<N>/` holds the live catalog +
    /// snapshots. `0` means the legacy flat layout (catalog + snapshots at the
    /// root, no MANIFEST yet); the first checkpoint migrates it to `gen.1`.
    generation: u64,
    catalog: Catalog,
    tables: BTreeMap<String, TableState>,
    wal: Wal,
    disk_first: bool,
    /// Auto-checkpoint threshold in WAL bytes (0 = manual only).
    checkpoint_bytes: u64,
    /// The WAL watermark committed with the current `generation` — the highest
    /// seq folded into its snapshots. A low-lock backup records it in the
    /// archive's synthesized `MANIFEST`.
    committed_wal_seq: u64,
    /// Generations pinned by in-progress low-lock backups, refcounted (two
    /// backups can pin the same generation). GC never removes a pinned
    /// generation, and while any pin is held the WAL is not truncated — so a
    /// backup can archive a committed generation and a stable WAL prefix
    /// without holding the engine lock across the (slow) compression.
    pinned_gens: std::collections::BTreeMap<u64, usize>,
}

/// The public SQL engine handle. Cheap to share behind an `Arc`.
pub struct SqlEngine {
    inner: Mutex<Inner>,
    /// Interactive (session) transactions parked between calls, keyed by id
    /// (ADR-0013 Phase B). A transaction lives here while its connection is
    /// between requests; execution takes it out and puts it back.
    session_txns: Mutex<std::collections::HashMap<u64, transaction::TxnState>>,
    next_session_txn: std::sync::atomic::AtomicU64,
    /// Parsed-statement cache: SQL text -> AST. Applications loop over a
    /// small set of parameterized texts, and parsing costs more than an AST
    /// clone; execution works on a clone, so the cached AST is never touched.
    /// Text -> AST is pure, making invalidation a non-issue; the map is
    /// cleared wholesale if it ever grows past a cap.
    stmt_cache: Mutex<std::collections::HashMap<String, std::sync::Arc<Vec<ast::Statement>>>>,
    /// Pessimistic row locks (`SELECT ... FOR UPDATE`, writer-writer
    /// exclusion). Waited on ONLY while `inner` is not held.
    row_locks: row_locks::RowLocks,
    /// See [`SqlOptions::lock_timeout_ms`].
    lock_timeout: std::time::Duration,
    /// Max number of tables this engine may hold. `0` = unlimited (the default).
    /// Set per-instance by the data plane for OxiBase tenant databases.
    max_tables: std::sync::atomic::AtomicUsize,
    /// Group commit. Deliberately **outside** `inner`, because its whole
    /// purpose is to be reachable while another writer holds the engine lock.
    commit: CommitGate,
}

/// Shared durability point for writes: many appends, one fsync.
///
/// A write appends to the WAL under the engine lock and applies its effect,
/// then flushes here *after* releasing it. Concurrent writers therefore
/// overlap — while one flushes, the others append — and a single physical
/// fsync makes all of their records durable.
///
/// Without this the engine paid one fsync per statement with the lock held, so
/// concurrent writers could not batch at all: throughput stayed flat at
/// ~1/fsync no matter how many connections were writing, and latency grew
/// linearly with them.
///
/// The acknowledgement rule is unchanged: a write returns to its caller only
/// after a flush that covers its own sequence. What *is* newly observable is
/// that another connection can read a write between its apply and its flush;
/// a crash in that window loses it, and the writer never got an ack. This is
/// the same window PostgreSQL has, and the same one the document engine's
/// group commit already accepts.
struct CommitGate {
    /// Elects one flusher at a time; the rest wait and are covered by it.
    leader: Mutex<()>,
    /// Highest WAL sequence known to be on disk.
    synced_seq: std::sync::atomic::AtomicU64,
    /// Highest WAL sequence written (not necessarily flushed). Published under
    /// the engine lock, read by a flusher to learn what its fsync will cover.
    appended_seq: std::sync::atomic::AtomicU64,
    /// A duplicate of the WAL's descriptor, taken once at open. This is what
    /// lets a writer flush after releasing the engine lock: the `Wal` itself
    /// lives behind that lock, but an fsync on a duplicate flushes the same
    /// file. Only ever fsynced here — never read, written or seeked — so
    /// sharing the file offset with the writer side is immaterial.
    file: std::fs::File,
    mode: wal::SyncMode,
}

impl CommitGate {
    /// Make everything up to `target` durable, sharing the flush with any
    /// concurrent caller.
    fn sync_upto(&self, target: u64) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self.synced_seq.load(Ordering::SeqCst) >= target {
            // A concurrent flush already covered us — the group-commit win.
            return Ok(());
        }
        let _lead = self.leader.lock().unwrap();
        if self.synced_seq.load(Ordering::SeqCst) >= target {
            // Covered while we waited for the leader slot.
            return Ok(());
        }
        // Read what is written *before* flushing: appends that land during the
        // flush may or may not be included, so they are not claimed.
        let covered = self.appended_seq.load(Ordering::SeqCst);
        wal::sync_file(&self.file, self.mode)?;
        self.synced_seq.fetch_max(covered, Ordering::SeqCst);
        Ok(())
    }

    /// Declare everything up to `seq` durable without flushing — used after a
    /// checkpoint, which has already made the same data durable by fsyncing
    /// the snapshot it folded the records into.
    fn mark_durable(&self, seq: u64) {
        use std::sync::atomic::Ordering;
        self.synced_seq.fetch_max(seq, Ordering::SeqCst);
    }
}

thread_local! {
    /// Lock owner of the autocommit statement currently executing on this
    /// thread (0 = none). Set around each autocommit statement in
    /// `run_session_batch`; `<SqlEngine as Store>::lock_rows` reads it. A
    /// thread executes one statement at a time, so this is exact. Statements
    /// inside a transaction don't use it — the `Transaction` store carries
    /// its own owner.
    static STMT_LOCK_OWNER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Statement-cache entry cap: past this the whole map is dropped (workloads
/// have few distinct texts; unbounded growth would only come from literals
/// inlined into SQL, which shouldn't be cached anyway).
const STMT_CACHE_CAP: usize = 512;

/// Row operations replayed between folds when opening a disk-first database.
///
/// Bounds how much of a WAL tail is ever materialized at once. Small enough
/// that a large tail cannot dominate the process, large enough that an ordinary
/// restart folds once or not at all — a fold rewrites every table, so making
/// this small would turn recovery into repeated full rewrites.
const REPLAY_FOLD_OPS: usize = 200_000;

/// Longest statement text worth caching.
///
/// The cache exists so a repeated statement parses once, which pays off for the
/// parameterized text an application sends over and over — and that text is
/// short. A statement long enough to exceed this is one with its values inlined,
/// which is unique by construction: caching it stores a large key and a much
/// larger AST that can never be hit.
///
/// It is not free to get this wrong. A bulk load of 500-row `INSERT`s filled
/// the cache with 512 such entries — tens of megabytes of text and AST for a
/// zero percent hit rate — and that showed up as resident memory for as long as
/// the process lived. Above the limit the statement is still parsed, just not
/// kept.
const STMT_CACHE_MAX_TEXT: usize = 4096;

impl SqlEngine {
    /// Open (creating if needed) a SQL engine rooted at `dir` (e.g.
    /// `oxidb_data/sql`), with options from the environment. Loads the catalog
    /// and `.rdat` snapshots, replays the WAL tail, then (re)builds secondary
    /// indexes.
    pub fn open(dir: impl AsRef<Path>) -> Result<SqlEngine> {
        Self::open_with_options(dir, SqlOptions::from_env())
    }

    /// [`open`](SqlEngine::open) with explicit options.
    pub fn open_with_options(dir: impl AsRef<Path>, opts: SqlOptions) -> Result<SqlEngine> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // 1. Find the committed generation. The MANIFEST is the atomic pointer;
        //    its absence means a fresh or legacy database (catalog + snapshots
        //    still at the root), loaded as generation 0.
        let manifest = manifest::Manifest::load(&dir)?;
        let (generation, load_dir, watermark) = match &manifest {
            Some(m) => (
                m.generation,
                manifest::gen_dir(&dir, m.generation),
                m.wal_seq,
            ),
            None => (0, dir.clone(), 0),
        };

        // Sweep orphan generation dirs — a crashed checkpoint or a failed GC —
        // so only the committed generation survives on disk.
        Self::sweep_orphan_generations(&dir, generation);

        // 2. Durable schema snapshot for that generation. Sequences persist
        //    separately (see `sequences.json`), so overlay them last.
        let mut catalog = Catalog::load(&load_dir)?;
        if let Some(seqs) = catalog::load_sequences(&dir)? {
            catalog.sequences = seqs;
        }

        // 3. Load each table's row snapshot. Resident mode materializes every
        //    row; disk-first maps the snapshot and makes one decoding pass to
        //    seed `next_row_id` and the PK map without retaining the rows.
        let mut tables: BTreeMap<String, TableState> = BTreeMap::new();
        for (name, def) in &catalog.tables {
            let mut state = TableState::empty(def.clone(), opts.disk_first);
            if opts.disk_first {
                // A materialized primary key means the map does not have to be
                // rebuilt at all — the pass below still runs, because
                // `next_row_id` and AUTO_INCREMENT have to be recovered from
                // the rows, but it stops retaining a key per row.
                state.pk_base = index_file::MappedIndex::open(&load_dir, name, PK_INDEX_NAME)?;
                let seed_pk = state.pk_base.is_none();
                for u in state.uniques.iter_mut() {
                    u.base =
                        index_file::MappedIndex::open(&load_dir, name, &unique_index_name(u.pos))?;
                }
                let seed_unique: std::collections::BTreeSet<usize> = state
                    .uniques
                    .iter()
                    .filter(|u| u.base.is_none())
                    .map(|u| u.pos)
                    .collect();
                if let Some(snap) = storage::MappedSnapshot::open(&load_dir, name, def.arity())? {
                    for (row_id, cells) in snap.entries() {
                        state.observe_row_id(row_id);
                        state.observe_auto(&cells);
                        if seed_pk && let Some(key) = state.pk_key(&cells) {
                            state.pk_map.insert(key, row_id);
                        }
                        for u in state.uniques.iter_mut() {
                            if seed_unique.contains(&u.pos) && !matches!(cells[u.pos], Value::Null)
                            {
                                u.map.insert(IndexKey(cells[u.pos].clone()), row_id);
                            }
                        }
                    }
                    state.rows.attach_base(snap);
                }
            } else {
                for (row_id, cells) in storage::read_snapshot(&load_dir, name, def.arity())? {
                    state.observe_row_id(row_id);
                    state.observe_auto(&cells);
                    if let Some(key) = state.pk_key(&cells) {
                        state.pk_map.insert(key, row_id);
                    }
                    for u in state.uniques.iter_mut() {
                        if !matches!(cells[u.pos], Value::Null) {
                            u.map.insert(IndexKey(cells[u.pos].clone()), row_id);
                        }
                    }
                    state.rows.insert(row_id, cells);
                }
            }
            tables.insert(name.clone(), state);
        }

        // 4. Register the catalog's indexes **before** replaying, and map each
        //    one's `.sidx` where the generation has it.
        //
        //    Order matters. A mapped base describes the rows as of the
        //    checkpoint; the records replayed below are exactly the ones it
        //    does not know about. Registering first means those records go
        //    through the normal write path and land in the overlay. Doing it
        //    afterwards — as this once did — left the base current only to the
        //    checkpoint and the overlay empty, so every row written since was
        //    invisible to an indexed lookup.
        //
        //    An index with no `.sidx` (never checkpointed, or created after the
        //    last one) stays declared-but-unbuilt and is built on first use.
        let defs: Vec<IndexDef> = catalog.indexes.values().cloned().collect();
        for def in defs {
            let Some(state) = tables.get_mut(&def.table) else {
                continue;
            };
            if state.indexes.contains_key(&def.name) {
                continue;
            }
            let _ = state.declare_index(&def.name, &def.columns);
            if opts.disk_first
                && let Ok(Some(base)) =
                    index_file::MappedIndex::open(&load_dir, &def.table, &def.name)
                && let Some(idx) = state.indexes.get_mut(&def.name)
            {
                idx.base = Some(base);
            }
        }

        // 5. Replay the WAL past the manifest watermark (idempotent). Records at
        //    or below it are already folded into the snapshots above.
        //
        //    The engine is constructed *before* the replay rather than after,
        //    so the replay can checkpoint as it goes. A replayed record becomes
        //    an overlay row in disk-first mode, and the overlay is only folded
        //    back into the mmap'd snapshot by a checkpoint — so replaying a
        //    large tail in one pass materializes all of it at once. That was
        //    the bulk of a measured 415 MB peak opening a database that then
        //    ran in 94 MB.
        let (wal, records) = Wal::open_since(&dir, watermark)?;

        // Everything already in the WAL at open is on disk (it was read from
        // there), so the gate starts with that sequence durable.
        let commit = CommitGate {
            leader: Mutex::new(()),
            synced_seq: std::sync::atomic::AtomicU64::new(wal.last_seq()),
            appended_seq: std::sync::atomic::AtomicU64::new(wal.last_seq()),
            file: wal.dup_handle()?,
            mode: wal.sync_mode(),
        };

        let tail = wal.bytes();
        let engine = SqlEngine {
            session_txns: Mutex::new(std::collections::HashMap::new()),
            next_session_txn: std::sync::atomic::AtomicU64::new(1),
            stmt_cache: Mutex::new(std::collections::HashMap::new()),
            row_locks: row_locks::RowLocks::default(),
            lock_timeout: std::time::Duration::from_millis(opts.lock_timeout_ms),
            max_tables: std::sync::atomic::AtomicUsize::new(0),
            commit,
            inner: Mutex::new(Inner {
                dir,
                generation,
                catalog,
                tables,
                wal,
                disk_first: opts.disk_first,
                checkpoint_bytes: opts.checkpoint_bytes,
                committed_wal_seq: watermark,
                pinned_gens: std::collections::BTreeMap::new(),
            }),
        };

        // Fold periodically while replaying, so the overlay never holds the
        // whole tail. Each fold records the last *applied* sequence as its
        // watermark and leaves the log intact — see `checkpoint_upto`, where
        // getting either wrong would lose the records not yet replayed.
        //
        // Only in disk-first mode: resident mode keeps every row in RAM
        // whatever happens, so a fold would cost IO and save nothing. Records
        // are consumed rather than borrowed, freeing each as it is applied.
        let fold_during_replay = opts.disk_first && opts.checkpoint_bytes > 0;
        {
            let mut inner = engine.inner.lock().unwrap();
            let mut since_fold = 0usize;
            for item in records {
                let (seq, rec) = item?;
                // A `Batch` is one record carrying many row operations, so
                // counting records alone would let one batch blow the bound.
                since_fold += match &rec {
                    WalRecord::Batch(ops) => ops.len(),
                    _ => 1,
                };
                let Inner {
                    catalog, tables, ..
                } = &mut *inner;
                Self::apply_live(catalog, tables, &rec, opts.disk_first);
                if fold_during_replay && since_fold >= opts.replay_fold_ops {
                    // Best-effort: a fold that fails leaves the overlay larger
                    // than intended, which is the old behaviour, not a
                    // correctness problem.
                    let _ = Self::checkpoint_upto(&mut inner, &engine.commit, Some(seq));
                    since_fold = 0;
                }
            }
        }

        // Whatever is left after the last periodic fold. The threshold is a
        // fraction of the auto-checkpoint size rather than the size itself,
        // because the trade differs at open: a tail costs RAM for the whole
        // life of the process, not just until the next write.
        // `checkpoint_bytes == 0` means checkpoints are manual, and that is
        // respected. This one covers the whole log, so it may truncate.
        if fold_during_replay && tail > opts.checkpoint_bytes / 8 {
            let _ = engine.checkpoint();
        }
        Ok(engine)
    }

    /// Delete every `gen.<N>/` directory except the committed generation —
    /// leftovers from a checkpoint that crashed before committing its MANIFEST,
    /// or a GC that never ran. Best-effort: a failure only wastes disk.
    fn sweep_orphan_generations(root: &Path, keep: u64) {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        for entry in entries.flatten() {
            if let Some(rest) = entry.file_name().to_string_lossy().strip_prefix("gen.")
                && rest.parse::<u64>().ok() != Some(keep)
            {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    /// Append `rec` to the WAL **without flushing** and apply it to live
    /// state, returning what the caller must flush once it drops the engine
    /// lock. Every write path goes through here, so the append/flush split —
    /// and therefore group commit — is uniform.
    ///
    /// Call [`SqlEngine::commit_pending`] after releasing the lock; the write
    /// is not acknowledged until that returns.
    fn log_and_apply(inner: &mut Inner, gate: &CommitGate, rec: &WalRecord) -> Result<u64> {
        let seq = inner.wal.append_no_sync(rec)?;
        gate.appended_seq
            .fetch_max(inner.wal.last_seq(), std::sync::atomic::Ordering::SeqCst);
        Self::apply_live(&mut inner.catalog, &mut inner.tables, rec, inner.disk_first);
        Ok(seq)
    }

    /// Complete the durability a logged write owes: flush until `seq` is on
    /// disk. Must be called with the engine lock **released** — holding it here
    /// would serialize the flushes and give back everything group commit buys.
    fn commit_pending(&self, seq: u64) -> Result<()> {
        self.commit.sync_upto(seq)
    }

    /// Apply a WAL record to the in-memory catalog + tables, maintaining
    /// secondary indexes and `next_row_id`. Idempotent by (table, row_id) so
    /// replay over a snapshot always converges. `Batch` is applied whole.
    fn apply_live(
        catalog: &mut Catalog,
        tables: &mut BTreeMap<String, TableState>,
        rec: &WalRecord,
        disk_first: bool,
    ) {
        match rec {
            WalRecord::AlterTable { table, op } => {
                let Some(def) = catalog.tables.get_mut(table) else {
                    return;
                };
                use ast::AlterOp;
                // A column is only ever appended, so ADD COLUMN shifts no
                // existing position: constraint maps and secondary indexes stay
                // valid, and the stored rows need no rewrite (they read back
                // padded with the new column's default). That makes it O(1) —
                // skip the full-table row rewrite and index rebuild below.
                // ADD appends a slot; DROP tombstones one in place. Neither
                // shifts an existing physical position, so constraint maps and
                // secondary indexes stay valid and the stored rows need no
                // rewrite — old rows read back padded (ADD) or projected (DROP).
                // Both are O(1); RENAME and ALTER TYPE fall through to the
                // rebuild below (TYPE also rewrites the stored cells).
                let metadata_only = matches!(op, AlterOp::AddColumn(_) | AlterOp::DropColumn(_));
                match op {
                    AlterOp::AddColumn(col) => {
                        def.columns.push(col.clone());
                        if let Some(state) = tables.get_mut(table) {
                            state.def = def.clone();
                        }
                    }
                    AlterOp::DropColumn(name) => {
                        // Tombstone the live column of this name (a re-added
                        // column can share a dropped one's name; target the
                        // live one so replay stays deterministic).
                        let Some(pos) = def
                            .columns
                            .iter()
                            .position(|c| &c.name == name && !c.dropped)
                        else {
                            return;
                        };
                        def.columns[pos].dropped = true;
                        if let Some(state) = tables.get_mut(table) {
                            state.def = def.clone();
                        }
                    }
                    AlterOp::RenameColumn { old, new } => {
                        if let Some(c) = def
                            .columns
                            .iter_mut()
                            .find(|c| &c.name == old && !c.dropped)
                        {
                            c.name = new.clone();
                        }
                        for d in catalog.indexes.values_mut() {
                            if &d.table == table {
                                for c in d.columns.iter_mut() {
                                    if c == old {
                                        *c = new.clone();
                                    }
                                }
                            }
                        }
                        if let Some(state) = tables.get_mut(table) {
                            state.def = def.clone();
                        }
                    }
                    AlterOp::AlterColumnType {
                        column,
                        ty,
                        max_len,
                        int_width,
                    } => {
                        let Some(pos) = def
                            .columns
                            .iter()
                            .position(|c| &c.name == column && !c.dropped)
                        else {
                            return;
                        };
                        // The cast was validated before the WAL append, so it
                        // succeeds here (and on deterministic replay over the
                        // same data). Keep the original on the impossible
                        // failure rather than losing the cell.
                        let cast = |v: &Value| {
                            executor::cast_value(v.clone(), *ty).unwrap_or_else(|_| v.clone())
                        };
                        def.columns[pos].ty = *ty;
                        def.columns[pos].max_len = *max_len;
                        def.columns[pos].int_width = *int_width;
                        if let Some(dv) = def.columns[pos].default_value.take() {
                            def.columns[pos].default_value = Some(cast(&dv));
                        }
                        if let Some(state) = tables.get_mut(table) {
                            state.def = def.clone();
                            // Narrow rows (pre-ADD-COLUMN) read from the fill
                            // template, regenerated from the cast default by
                            // sync_layout below.
                            state.rows.rewrite_slot(pos, &cast);
                        }
                    }
                }
                // Keep the row store's read layout (projection + pad template)
                // and the cached has_dropped flag in step with the new schema
                // (also invalidates any stale scan cache). Cheap for every op.
                if let Some(state) = tables.get_mut(table) {
                    state.sync_layout();
                }
                // RENAME leaves index/constraint column names stale in the
                // maps; rebuild them. ADD/DROP shift no positions, so they skip
                // this O(n) pass.
                if !metadata_only {
                    let defs: Vec<IndexDef> = catalog
                        .indexes
                        .values()
                        .filter(|d| &d.table == table)
                        .cloned()
                        .collect();
                    if let Some(state) = tables.get_mut(table) {
                        state.rebuild_meta();
                        state.indexes.clear();
                        for d in defs {
                            let _ = state.build_index(&d.name, &d.columns);
                        }
                    }
                }
            }
            WalRecord::CreateTable(def) => {
                catalog.tables.insert(def.name.clone(), def.clone());
                tables
                    .entry(def.name.clone())
                    .or_insert_with(|| TableState::empty(def.clone(), disk_first));
            }
            WalRecord::DropTable(name) => {
                catalog.tables.remove(name);
                tables.remove(name);
                // Drop any indexes that belonged to the table.
                catalog.indexes.retain(|_, d| &d.table != name);
            }
            WalRecord::CreateIndex(def) => {
                catalog.indexes.insert(def.name.clone(), def.clone());
                if let Some(state) = tables.get_mut(&def.table) {
                    let _ = state.declare_index(&def.name, &def.columns);
                }
            }
            WalRecord::DropIndex(name) => {
                if let Some(def) = catalog.indexes.remove(name)
                    && let Some(state) = tables.get_mut(&def.table)
                {
                    state.indexes.remove(name);
                }
            }
            WalRecord::CreateView { name, sql } => {
                catalog.views.insert(name.clone(), sql.clone());
            }
            WalRecord::DropView(name) => {
                catalog.views.remove(name);
            }
            WalRecord::CreateProcedure { name, def } => {
                catalog.procedures.insert(name.clone(), def.clone());
            }
            WalRecord::DropProcedure(name) => {
                catalog.procedures.remove(name);
            }
            WalRecord::Insert {
                table,
                row_id,
                cells,
            } => {
                if let Some(state) = tables.get_mut(table) {
                    // `cells` are physical (expanded before logging); index and
                    // constraint maps address physical slots, so the prior row
                    // must be read physical too.
                    if let Some(old) = state.rows.get_physical(*row_id) {
                        state.index_remove(*row_id, &old);
                    }
                    state.observe_row_id(*row_id);
                    state.observe_auto(cells);
                    state.rows.insert(*row_id, cells.clone());
                    state.index_insert(*row_id, cells);
                }
            }
            WalRecord::Delete { table, row_id } => {
                if let Some(state) = tables.get_mut(table)
                    && let Some(old) = state.rows.remove(*row_id)
                {
                    state.index_remove(*row_id, &old);
                }
            }
            WalRecord::Batch(ops) => {
                for op in ops {
                    Self::apply_live(catalog, tables, op, disk_first);
                }
            }
        }
    }

    /// Create a new table. Errors if a table or view of the same name exists.
    /// Cap the number of tables this engine may hold (`0` = unlimited). Set by
    /// the data plane from a tenant project's OxiBase quota; refreshed per
    /// request so plan changes take effect without a restart.
    pub fn set_max_tables(&self, max: usize) {
        self.max_tables
            .store(max, std::sync::atomic::Ordering::Release);
    }

    pub fn create_table(&self, def: Table) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.contains(&def.name) || inner.catalog.views.contains_key(&def.name) {
            return Err(SqlError::TableExists(def.name));
        }
        // Enforce the per-instance table cap (OxiBase tenant quota). Existing
        // tables are unaffected; only creating a *new* one past the cap fails.
        let max = self.max_tables.load(std::sync::atomic::Ordering::Acquire);
        if max > 0 && inner.catalog.tables.len() >= max {
            return Err(SqlError::TableLimitExceeded(max));
        }
        let rec = WalRecord::CreateTable(def);
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Drop a table and its rows. Errors if it does not exist.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.contains(name) {
            return Err(SqlError::NoSuchTable(name.to_string()));
        }
        let rec = WalRecord::DropTable(name.to_string());
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Create a secondary index over one or more columns. Errors if the index
    /// name is taken or the table / any column does not exist.
    pub fn create_index(&self, name: &str, table: &str, columns: &[String]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.indexes.contains_key(name) {
            return Err(SqlError::IndexExists(name.to_string()));
        }
        if columns.is_empty() {
            return Err(SqlError::Unsupported("index without columns".into()));
        }
        let def = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?
            .def
            .clone();
        for column in columns {
            if !def.columns.iter().any(|c| &c.name == column) {
                return Err(SqlError::NoSuchColumn(column.to_string()));
            }
        }
        let rec = WalRecord::CreateIndex(IndexDef {
            name: name.to_string(),
            table: table.to_string(),
            columns: columns.to_vec(),
        });
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        // Applying the record only *declares* the index — that is what makes a
        // WAL replay at open cheap. But an explicit `CREATE INDEX` is work the
        // caller asked for, so build it here rather than surprising whoever
        // runs the next query with the scan.
        if let Some(state) = inner.tables.get_mut(table) {
            state.populate_index(name);
        }
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Create (or with `or_replace`, overwrite) a view. The body must be a
    /// single SELECT; it is trial-executed so errors surface at creation.
    pub fn create_view(&self, name: &str, query_sql: &str, or_replace: bool) -> Result<()> {
        // Validate before taking the engine lock: must parse to exactly one
        // SELECT, and a trial run proves the referenced tables/columns exist.
        let stmts = parser::parse(query_sql)?;
        let is_single_select =
            stmts.len() == 1 && matches!(stmts.first(), Some(ast::Statement::Select(_)));
        if !is_single_select {
            return Err(SqlError::Unsupported(
                "a view body must be a single SELECT".into(),
            ));
        }
        executor::execute(self, stmts.into_iter().next().expect("checked"), &[])?;

        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.contains(name) {
            return Err(SqlError::TableExists(name.to_string()));
        }
        if inner.catalog.views.contains_key(name) && !or_replace {
            return Err(SqlError::TableExists(format!("{name} (view)")));
        }
        let rec = WalRecord::CreateView {
            name: name.to_string(),
            sql: query_sql.to_string(),
        };
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Drop a view by name. Errors if it does not exist.
    pub fn drop_view(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.views.contains_key(name) {
            return Err(SqlError::NoSuchView(name.to_string()));
        }
        let rec = WalRecord::DropView(name.to_string());
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// The stored SQL text of a view, if it exists.
    pub fn view_sql(&self, name: &str) -> Option<String> {
        self.inner.lock().unwrap().catalog.views.get(name).cloned()
    }

    /// Create (or with `or_alter`, overwrite) a stored procedure. A SQL body
    /// was validated to shape (DML/SELECT only, params rewritten to `$N`) at
    /// parse; here it is trial-parsed so unsupported constructs surface at
    /// creation, not first call. A COBRA body was already decoded and
    /// validated by the executor (its `body` is a display placeholder, not
    /// SQL).
    pub fn create_procedure(
        &self,
        name: &str,
        def: catalog::ProcedureDef,
        or_alter: bool,
    ) -> Result<()> {
        if def.language == catalog::ProcLanguage::Sql {
            parser::parse(&def.body)?;
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.catalog.procedures.contains_key(name) && !or_alter {
            return Err(SqlError::TableExists(format!("{name} (procedure)")));
        }
        let rec = WalRecord::CreateProcedure {
            name: name.to_string(),
            def,
        };
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Drop a stored procedure by name. Errors if it does not exist.
    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.procedures.contains_key(name) {
            return Err(SqlError::NoSuchProcedure(name.to_string()));
        }
        let rec = WalRecord::DropProcedure(name.to_string());
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// A stored procedure's definition, if it exists.
    pub fn procedure_def(&self, name: &str) -> Option<catalog::ProcedureDef> {
        self.inner
            .lock()
            .unwrap()
            .catalog
            .procedures
            .get(name)
            .cloned()
    }

    /// All stored procedures as `(name, def)` pairs, sorted by name.
    pub fn list_procedures(&self) -> Vec<(String, catalog::ProcedureDef)> {
        let inner = self.inner.lock().unwrap();
        inner
            .catalog
            .procedures
            .iter()
            .map(|(n, d)| (n.clone(), d.clone()))
            .collect()
    }

    /// Drop a secondary index by name. Errors if it does not exist.
    pub fn drop_index(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.indexes.contains_key(name) {
            return Err(SqlError::NoSuchIndex(name.to_string()));
        }
        let rec = WalRecord::DropIndex(name.to_string());
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Insert a row, returning its assigned `row_id`. Validated against the
    /// schema (arity, types, nullability, PRIMARY KEY uniqueness) before it
    /// is logged; integer values widen into DOUBLE/TIMESTAMP columns.
    pub fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64> {
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let cells = state.prepare_write(cells)?;
        state.check_pk(&cells, None)?;

        let row_id = inner.tables.get(table).expect("present").next_row_id;
        let rec = WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells,
        };
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(row_id)
    }

    /// Insert many rows in one durable step: all rows are validated (schema +
    /// PRIMARY KEY uniqueness, including duplicates *within* the batch),
    /// logged as a single WAL `Batch` record (one fsync), then applied.
    /// Returns the number of rows inserted.
    pub fn insert_many(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let mut batch_keys: BTreeSet<KeyTuple> = BTreeSet::new();
        let mut phys_rows: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for cells in rows {
            let cells = state.prepare_write(cells)?;
            state.check_pk(&cells, None)?;
            if let Some(key) = state.pk_key(&cells)
                && !batch_keys.insert(key)
            {
                return Err(SqlError::DuplicateKey(format!(
                    "PRIMARY KEY value {} appears twice in the same INSERT",
                    types::render_key(&state.pk_cols, &cells)
                )));
            }
            phys_rows.push(cells);
        }

        let first_id = inner.tables.get(table).expect("present").next_row_id;
        let n = phys_rows.len() as u64;
        let ops: Vec<WalRecord> = phys_rows
            .into_iter()
            .enumerate()
            .map(|(i, cells)| WalRecord::Insert {
                table: table.to_string(),
                row_id: first_id + i as u64,
                cells,
            })
            .collect();
        let rec = WalRecord::Batch(ops);
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(n)
    }

    /// Overwrite the cells of an existing row, keeping its `row_id`. Logged as
    /// an idempotent `Insert` record for `row_id`.
    pub fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let cells = state.prepare_write(cells)?;
        state.check_pk(&cells, Some(row_id))?;
        if !inner
            .tables
            .get(table)
            .expect("present")
            .rows
            .contains(row_id)
        {
            return Err(SqlError::SchemaMismatch(format!(
                "row {row_id} does not exist in {table:?}"
            )));
        }
        let rec = WalRecord::Insert {
            table: table.to_string(),
            row_id,
            cells,
        };
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Delete a row by `row_id`. Returns whether a row was removed.
    pub fn delete(&self, table: &str, row_id: u64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.contains(table) {
            return Err(SqlError::NoSuchTable(table.to_string()));
        }
        let present = inner
            .tables
            .get(table)
            .map(|s| s.rows.contains(row_id))
            .unwrap_or(false);
        if !present {
            return Ok(false);
        }
        let rec = WalRecord::Delete {
            table: table.to_string(),
            row_id,
        };
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(true)
    }

    /// Delete many rows of one table as a single durable unit — one
    /// `WalRecord::Batch` (one fsync), applied all-or-nothing. This is what
    /// makes `ON DELETE CASCADE` cost one fsync for the whole child set instead
    /// of one per child. Missing ids are skipped (delete is idempotent).
    pub fn delete_many(&self, table: &str, row_ids: &[u64]) -> Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.catalog.contains(table) {
            return Err(SqlError::NoSuchTable(table.to_string()));
        }
        let present: Vec<u64> = {
            let state = inner.tables.get(table);
            row_ids
                .iter()
                .copied()
                .filter(|&id| state.map(|s| s.rows.contains(id)).unwrap_or(false))
                .collect()
        };
        if present.is_empty() {
            return Ok(0);
        }
        let ops: Vec<WalRecord> = present
            .iter()
            .map(|&row_id| WalRecord::Delete {
                table: table.to_string(),
                row_id,
            })
            .collect();
        let n = ops.len();
        let rec = WalRecord::Batch(ops);
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(n)
    }

    /// Delete `(table, row_id)` pairs that may span several tables as one
    /// durable `WalRecord::Batch` (one fsync), applied all-or-nothing. Used by
    /// DELETE to commit a row and its whole ON DELETE CASCADE closure — parent
    /// and children across tables — in a single fsync. Missing pairs are
    /// skipped and duplicates (a shared cascade child) collapse to one delete.
    pub fn delete_multi(&self, items: &[(String, u64)]) -> Result<usize> {
        let mut inner = self.inner.lock().unwrap();
        let mut seen: std::collections::HashSet<(&str, u64)> = std::collections::HashSet::new();
        let mut ops: Vec<WalRecord> = Vec::with_capacity(items.len());
        for (table, id) in items {
            if !inner.catalog.contains(table) {
                return Err(SqlError::NoSuchTable(table.clone()));
            }
            let present = inner
                .tables
                .get(table.as_str())
                .map(|s| s.rows.contains(*id))
                .unwrap_or(false);
            if present && seen.insert((table.as_str(), *id)) {
                ops.push(WalRecord::Delete {
                    table: table.clone(),
                    row_id: *id,
                });
            }
        }
        if ops.is_empty() {
            return Ok(0);
        }
        let n = ops.len();
        let rec = WalRecord::Batch(ops);
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(n)
    }

    /// Return all live rows of a table as `(row_id, cells)`, ordered by `row_id`.
    pub fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>> {
        let inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        Ok(state
            .rows
            .iter()
            .map(|(id, c)| (id, c.into_owned()))
            .collect())
    }

    /// The committed row currently owning `key` in a table's UNIQUE map at
    /// column position `pos` (`None` for unknown tables — e.g. ones created
    /// inside an open transaction). Transactions probe this for uniqueness
    /// checks instead of seeding a snapshot of the whole table.
    pub(crate) fn unique_owner(&self, table: &str, pos: usize, key: &IndexKey) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        let state = inner.tables.get(table)?;
        let u = state.uniques.iter().find(|u| u.pos == pos)?;
        state.unique_owner_of(u, key)
    }

    /// The committed row currently owning PRIMARY KEY tuple `key` — the
    /// composite-aware sibling of [`unique_owner`](Self::unique_owner). `key`
    /// must be built from the table's `pk_cols` in order.
    pub(crate) fn pk_owner(&self, table: &str, key: &[IndexKey]) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner.tables.get(table)?.pk_owner_of(key)
    }

    /// Look up rows using a secondary index whose columns are all present in
    /// the `column = value` pairs `eqs`. `Ok(None)` when no index qualifies.
    /// Collecting form of [`index_visit_eq`](Self::index_visit_eq).
    fn index_lookup_eq(&self, table: &str, eqs: &[(String, Value)]) -> Result<Option<store::Rows>> {
        let mut rows: store::Rows = Vec::new();
        let found = self.index_visit_eq_inner(table, eqs, &mut |id, cells| {
            rows.push((id, cells.to_vec()));
            Ok(true)
        })?;
        Ok(found.map(|()| rows))
    }

    /// Walk the rows an index selects for `eqs`, handing each to `visit`
    /// without building a result.
    ///
    /// `Ok(None)` when no index qualifies — the caller then scans. `visit`
    /// returns `false` to stop early.
    ///
    /// The point is what does *not* happen: a predicate matching 20,000 rows
    /// used to cost a 20,000-element vector of cloned rows before the caller
    /// could look at any of them, even to answer `count(*)`.
    fn index_visit_eq_inner(
        &self,
        table: &str,
        eqs: &[(String, Value)],
        visit: &mut dyn FnMut(u64, &[Value]) -> Result<bool>,
    ) -> Result<Option<()>> {
        // `mut` because a qualifying index may not be populated yet — see
        // `SecondaryIndex::populated`. Reads still take the same single lock.
        let mut inner = self.inner.lock().unwrap();
        let Some(state) = inner.tables.get(table) else {
            return Err(SqlError::NoSuchTable(table.to_string()));
        };
        // PRIMARY KEY: a unique equality lookup is the most selective index
        // there is, and the pk_map already maps the key tuple -> row_id. Use it
        // before considering (redundant) secondary indexes. A composite key
        // qualifies only when *every* member column has an equality pair —
        // a partial key isn't unique, so it would miss rows.
        if !state.pk_cols.is_empty() {
            let key: Option<KeyTuple> = state
                .pk_cols
                .iter()
                .map(|&p| {
                    eqs.iter()
                        .find(|(col, _)| *col == state.def.columns[p].name)
                        .map(|(_, v)| IndexKey(v.clone()))
                })
                .collect();
            if let Some(key) = key {
                if let Some(id) = state.pk_owner_of(&key)
                    && let Some(cells) = state.rows.get(id)
                {
                    visit(id, &cells)?;
                }
                return Ok(Some(()));
            }
        }
        // Find an index all of whose columns have an equality pair. Prefer
        // wider indexes (more matched columns = more selective).
        let mut best: Option<&IndexDef> = None;
        for def in inner.catalog.indexes.values() {
            if def.table == table
                && def
                    .columns
                    .iter()
                    .all(|c| eqs.iter().any(|(col, _)| col == c))
                && best.is_none_or(|b| def.columns.len() > b.columns.len())
            {
                best = Some(def);
            }
        }
        let Some(def) = best else {
            return Ok(None);
        };
        if !state.indexes.contains_key(&def.name) {
            return Ok(None);
        }
        // First real use of an index that has no mapped base: build it now.
        // `state` is reborrowed mutably for exactly this, then dropped back to a
        // shared borrow for the lookup itself.
        let name = def.name.clone();
        let key_cols: Vec<String> = def.columns.clone();
        let state = inner
            .tables
            .get_mut(table)
            .expect("looked up immediately above");
        state.populate_index(&name);
        let state = &*state;
        let idx = state.indexes.get(&name).expect("checked above");
        let key: KeyTuple = key_cols
            .iter()
            .map(|c| {
                let (_, v) = eqs.iter().find(|(col, _)| col == c).expect("checked");
                IndexKey(v.clone())
            })
            .collect();
        // Candidates from the mapped base and the overlay, each **verified**
        // against the live row. The base is the last checkpoint's view, so it
        // can name a row that has since been deleted (gone from the store) or
        // whose indexed columns have changed (no longer matches the key).
        // Checking costs one comparison on a row we are fetching anyway.
        // `col_pos` are PHYSICAL positions, so the key is recomputed from the
        // physical row — a table with a dropped column has a shorter logical
        // row, and checking against that would read the wrong cells.
        // One materialization per candidate, not two: the physical row is what
        // verification needs, and unless a column has been dropped it *is* the
        // logical row. Fetching both cost a second full row build for every
        // candidate — 20,000 of them on a low-selectivity predicate.
        for id in idx.candidates(&key)? {
            let Some(phys) = state.rows.get_physical(id) else {
                continue;
            };
            if idx.key_of(&phys) != key {
                continue;
            }
            if !visit(id, &state.rows.to_logical(phys))? {
                break;
            }
        }
        Ok(Some(()))
    }

    /// Number of live rows in a table.
    pub fn row_count(&self, table: &str) -> Result<usize> {
        let inner = self.inner.lock().unwrap();
        inner
            .tables
            .get(table)
            .map(|s| s.rows.len())
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))
    }

    /// The **query-visible** table definition, if it exists: the physical
    /// schema with columns tombstoned by a lazy `DROP COLUMN` filtered out. The
    /// executor only ever sees this logical view, so it addresses live columns
    /// by contiguous position and never has to know a dropped column exists.
    pub fn table_def(&self, name: &str) -> Option<Table> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.get(name).map(|t| t.logical().into_owned())
    }

    /// The physical storage layout of `table`: `(live_slots, physical_arity)`.
    /// `live_slots[k]` is the physical cell position of the k-th live column;
    /// it equals `k` (and its length equals the arity) until a column is
    /// dropped. Transactions use it to map their logical rows/positions to the
    /// physical layout the engine's WAL and constraint maps speak.
    pub(crate) fn table_layout(&self, table: &str) -> Option<(Vec<usize>, usize)> {
        let inner = self.inner.lock().unwrap();
        inner
            .catalog
            .tables
            .get(table)
            .map(|t| (t.live_slots(), t.columns.len()))
    }

    /// All table names, sorted.
    pub fn table_names(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.tables.keys().cloned().collect()
    }

    /// All table definitions, sorted by name.
    pub fn list_tables(&self) -> Vec<Table> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.tables.values().cloned().collect()
    }

    /// All views as `(name, body SQL)` pairs, sorted by name.
    pub fn list_views(&self) -> Vec<(String, String)> {
        let inner = self.inner.lock().unwrap();
        inner
            .catalog
            .views
            .iter()
            .map(|(n, s)| (n.clone(), s.clone()))
            .collect()
    }

    /// All secondary index definitions, sorted by index name.
    pub fn list_indexes(&self) -> Vec<IndexDef> {
        let inner = self.inner.lock().unwrap();
        inner.catalog.indexes.values().cloned().collect()
    }

    /// Atomically reserve `n` auto-increment values for `table`, returning
    /// the first. Values are handed out even if the insert later fails —
    /// gaps are normal auto-increment behavior.
    pub(crate) fn next_auto_block(&self, table: &str, n: i64) -> Result<i64> {
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get_mut(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let start = state.next_auto;
        state.next_auto += n;
        Ok(start)
    }

    /// Atomically reserve `n` consecutive `row_id`s for `table`, returning the
    /// first (`None` when the table isn't in the engine — created inside the
    /// caller's transaction, or dropped underneath it).
    ///
    /// A buffered transaction must take its ids from here rather than reading
    /// the counter and allocating locally: the counter advances under the
    /// engine lock, so no other writer — another transaction, or a plain
    /// autocommit INSERT — can be handed the same id while the transaction is
    /// still uncommitted. Ids belonging to a transaction that never commits
    /// are simply never used (a gap, as with auto-increment).
    pub(crate) fn reserve_row_ids(&self, table: &str, n: u64) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        let state = inner.tables.get_mut(table)?;
        let start = state.next_row_id;
        state.next_row_id += n;
        Some(start)
    }

    /// Atomically apply a group of records produced by a committing transaction:
    /// one `Batch` WAL record (a single fsync), then applied to live state.
    ///
    /// **Applies without re-checking** — this is the path a replicated commit
    /// takes (`apply_replicated_txn_ops`), where the decision to commit was
    /// already made and every node must apply exactly the same ops or diverge.
    /// A locally-committing transaction goes through
    /// [`commit_batch_checked`](Self::commit_batch_checked) instead.
    pub(crate) fn commit_batch(&self, ops: Vec<WalRecord>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().unwrap();
        let rec = WalRecord::Batch(ops);
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Commit a local transaction's ops: re-check their uniqueness constraints
    /// against the committed state **as of now**, then log and apply the batch
    /// — all under one lock, so nothing can slip in between the check and the
    /// write.
    ///
    /// The transaction checked each write when it buffered it, but another
    /// writer may have committed a colliding key since. Without this the batch
    /// would apply anyway and leave two rows sharing one key.
    pub(crate) fn commit_batch_checked(&self, ops: Vec<WalRecord>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().unwrap();
        Self::validate_batch(&inner.tables, &ops)?;
        let rec = WalRecord::Batch(ops);
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        Ok(())
    }

    /// Re-check a committing batch's uniqueness constraints against committed
    /// state, simulating the ops in order so the batch's own effects count: a
    /// row it deletes or rewrites no longer owns its old keys, and a key it
    /// claims is owned by the row that claimed it.
    ///
    /// Tables the batch creates or drops are skipped — they have no committed
    /// state to check against, and the transaction already checked them
    /// against its own overlay. A row whose arity no longer matches its table
    /// (a concurrent `ALTER`) is skipped rather than indexed out of bounds.
    fn validate_batch(tables: &BTreeMap<String, TableState>, ops: &[WalRecord]) -> Result<()> {
        let mut sims: BTreeMap<&str, BatchSim<'_>> = BTreeMap::new();
        let mut skip: BTreeSet<&str> = BTreeSet::new();
        for op in ops {
            match op {
                WalRecord::CreateTable(t) => {
                    skip.insert(t.name.as_str());
                }
                WalRecord::DropTable(name) => {
                    skip.insert(name.as_str());
                    sims.remove(name.as_str());
                }
                WalRecord::Insert {
                    table,
                    row_id,
                    cells,
                } => {
                    if skip.contains(table.as_str()) {
                        continue;
                    }
                    let Some(state) = tables.get(table.as_str()) else {
                        continue;
                    };
                    if cells.len() != state.def.columns.len() {
                        continue;
                    }
                    let sim = sims
                        .entry(table.as_str())
                        .or_insert_with(|| BatchSim::new(state));
                    sim.check(*row_id, cells)?;
                    sim.claim(*row_id, Some(cells));
                }
                WalRecord::Delete { table, row_id } => {
                    if skip.contains(table.as_str()) {
                        continue;
                    }
                    let Some(state) = tables.get(table.as_str()) else {
                        continue;
                    };
                    sims.entry(table.as_str())
                        .or_insert_with(|| BatchSim::new(state))
                        .claim(*row_id, None);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Durably capture current state into a fresh generation (`gen.<N+1>/`
    /// catalog + snapshots), atomically promote it by committing the MANIFEST,
    /// then re-attach disk-first bases, GC the old generation, and truncate the
    /// WAL. The MANIFEST rename is the single commit point: a crash anywhere
    /// before it leaves the previous generation whole and in force.
    pub fn checkpoint(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        Self::checkpoint_locked(&mut inner, &self.commit)
    }

    /// `gate` is told what the checkpoint made durable: the snapshot it wrote
    /// (and fsynced, and committed by MANIFEST rename) covers every record
    /// applied so far, including any whose WAL append has not been flushed. A
    /// writer still waiting to flush is then already satisfied — and must be,
    /// since the WAL it would have flushed may have just been truncated.
    fn checkpoint_locked(inner: &mut Inner, gate: &CommitGate) -> Result<()> {
        Self::checkpoint_upto(inner, gate, None)
    }

    /// Checkpoint, recording `applied_upto` as the manifest watermark instead
    /// of the WAL's last sequence.
    ///
    /// `None` means "everything in the WAL has been applied", which is true of
    /// every checkpoint taken by a running engine and lets the WAL be
    /// truncated. `Some(seq)` is for a checkpoint taken **part-way through a
    /// replay**, where the log still holds records this snapshot does not
    /// contain: the watermark must name what was actually applied, and the WAL
    /// must survive, or recovery would skip those records and they would be
    /// deleted with them. That is the difference between bounding replay memory
    /// and losing committed writes.
    fn checkpoint_upto(
        inner: &mut Inner,
        gate: &CommitGate,
        applied_upto: Option<u64>,
    ) -> Result<()> {
        let Inner {
            dir,
            generation,
            catalog,
            tables,
            wal,
            disk_first,
            committed_wal_seq,
            pinned_gens,
            ..
        } = inner;

        // Compaction: physically reclaim the slots left by lazy `DROP COLUMN`s
        // before persisting, returning each affected table to a plain identity
        // layout. This is the O(n) rewrite the instant DROP deferred — done
        // here, off the critical path, so its cost is folded into the
        // checkpoint that was going to rewrite every row anyway. After it, the
        // snapshot and catalog carry no tombstones; the space is freed.
        let compacting: Vec<String> = catalog
            .tables
            .iter()
            .filter(|(_, t)| t.has_dropped())
            .map(|(name, _)| name.clone())
            .collect();
        for name in &compacting {
            if let Some(state) = tables.get_mut(name) {
                state.rows.compact(); // rewrite rows physical -> live-only
            }
            if let Some(t) = catalog.tables.get_mut(name) {
                t.columns.retain(|c| !c.dropped); // drop the tombstones
            }
            // Positions shifted: rebuild constraint maps and secondary indexes
            // (which reference surviving columns by name) at the new layout.
            let index_defs: Vec<IndexDef> = catalog
                .indexes
                .values()
                .filter(|d| &d.table == name)
                .cloned()
                .collect();
            if let Some(state) = tables.get_mut(name) {
                state.def = catalog.tables[name].clone();
                state.sync_layout();
                state.rebuild_meta();
                state.indexes.clear();
                for d in index_defs {
                    let _ = state.build_index(&d.name, &d.columns);
                }
            }
        }

        // Write the whole new generation into its own directory — nothing here
        // touches the currently-committed generation, so a crash mid-write is
        // harmless (the MANIFEST still names the old one).
        let old_gen = *generation;
        let new_gen = old_gen + 1;
        let new_dir = manifest::gen_dir(dir, new_gen);
        std::fs::create_dir_all(&new_dir)?;
        for (name, state) in tables.iter() {
            storage::write_snapshot(&new_dir, name, state.rows.iter_physical())?;
        }
        // Primary keys, UNIQUE columns and secondary indexes are all
        // materialized into the generation, whether or not they are currently
        // in memory — that is what lets the next open serve them from a mapping
        // instead of rebuilding them into RAM.
        //
        // Each streams through an `IndexBuilder`, which sorts in bounded chunks
        // and spills. Collecting the entries into a map first was simpler and
        // made a checkpoint's peak proportional to the table: a 1.2M-row
        // database peaked at 415 MB opening against the 94 MB it then ran in,
        // so it could not restart inside a limit it ran fine in.
        for (name, state) in tables.iter() {
            if !state.pk_cols.is_empty() {
                let mut b = index_file::IndexBuilder::new(
                    &new_dir,
                    name,
                    PK_INDEX_NAME,
                    state.pk_cols.len(),
                );
                for (rid, cells) in state.rows.iter_physical() {
                    if let Some(key) = state.pk_key(&cells) {
                        b.push(key, rid)?;
                    }
                }
                b.finish()?;
            }

            // NULLs are exempt from UNIQUE under SQL, so they are simply not
            // written — a key absent from the file is a key nothing owns.
            for u in &state.uniques {
                let mut b =
                    index_file::IndexBuilder::new(&new_dir, name, &unique_index_name(u.pos), 1);
                for (rid, cells) in state.rows.iter_physical() {
                    if !matches!(cells[u.pos], Value::Null) {
                        b.push(
                            std::iter::once(IndexKey(cells[u.pos].clone())).collect(),
                            rid,
                        )?;
                    }
                }
                b.finish()?;
            }
        }

        for def in catalog.indexes.values() {
            let Some(state) = tables.get(&def.table) else {
                continue;
            };
            let Some(idx) = state.indexes.get(&def.name) else {
                continue;
            };
            let mut b =
                index_file::IndexBuilder::new(&new_dir, &def.table, &def.name, idx.col_pos.len());
            if idx.populated {
                // Already the whole truth, in key order — feed it straight
                // through; the builder will not spill on an ordered stream any
                // sooner than on an unordered one, and this avoids re-reading
                // every row.
                for (key, ids) in idx.map.iter() {
                    for id in ids.iter() {
                        b.push(key.clone(), *id)?;
                    }
                }
            } else {
                // A mapped base plus an overlay, or an index never built: the
                // rows are the only source that reflects both.
                for (rid, cells) in state.rows.iter_physical() {
                    b.push(idx.key_of(&cells), rid)?;
                }
            }
            b.finish()?;
        }
        catalog.save(&new_dir)?;

        // Commit: this rename atomically switches the live generation. The
        // watermark is the highest WAL seq folded into these snapshots, so a
        // not-yet-truncated WAL replays only what came after.
        let watermark = applied_upto.unwrap_or_else(|| wal.last_seq());
        manifest::Manifest::commit(dir, new_gen, watermark)?;
        *generation = new_gen;
        *committed_wal_seq = watermark;

        // Disk-first: adopt the new generation's snapshots as the bases and drop
        // the RAM overlays they absorbed.
        if *disk_first {
            for (name, state) in tables.iter_mut() {
                if let Some(snap) =
                    storage::MappedSnapshot::open(&new_dir, name, state.def.arity())?
                {
                    state.rows.attach_base(snap);
                }
                if !state.pk_cols.is_empty()
                    && let Some(base) =
                        index_file::MappedIndex::open(&new_dir, name, PK_INDEX_NAME)?
                {
                    state.pk_base = Some(base);
                    state.pk_map.clear();
                }
                for i in 0..state.uniques.len() {
                    let pos = state.uniques[i].pos;
                    if let Some(base) =
                        index_file::MappedIndex::open(&new_dir, name, &unique_index_name(pos))?
                    {
                        state.uniques[i].base = Some(base);
                        state.uniques[i].map = BTreeMap::new();
                    }
                }
                // Same for the indexes: adopt the files just written and drop
                // the in-RAM maps they now contain. The overlay restarts empty
                // because the base is current as of this instant.
                let index_names: Vec<String> = state.indexes.keys().cloned().collect();
                for iname in index_names {
                    if let Some(base) = index_file::MappedIndex::open(&new_dir, name, &iname)? {
                        let idx = state.indexes.get_mut(&iname).expect("just listed");
                        idx.base = Some(base);
                        idx.map = BTreeMap::new();
                        idx.populated = false;
                    }
                }
            }
        }

        // Reclaim the superseded generation (unless a backup pinned it) and the
        // WAL prefix now captured in the snapshot. Both are best-effort: the
        // committed MANIFEST already makes recovery correct, so a failure here
        // only leaves reclaimable disk behind. While a backup is pinning any
        // generation, skip the WAL truncation too — the backup is archiving a
        // stable prefix of it.
        if !pinned_gens.contains_key(&old_gen) {
            Self::gc_superseded(dir, old_gen);
        }
        // Only when this snapshot covers the whole log. Mid-replay it does not,
        // and truncating would destroy the records still to be applied.
        if pinned_gens.is_empty() && applied_upto.is_none() {
            let _ = wal.truncate();
        }
        gate.mark_durable(wal.last_seq());
        Ok(())
    }

    /// Remove the storage the just-committed checkpoint superseded: the old
    /// `gen.<old>/` directory, or — on the first checkpoint of a legacy
    /// database (`old_gen == 0`) — the flat root-level `catalog.json` and
    /// `<table>.rdat` files it migrated from.
    fn gc_superseded(root: &Path, old_gen: u64) {
        if old_gen >= 1 {
            let _ = std::fs::remove_dir_all(manifest::gen_dir(root, old_gen));
            return;
        }
        let _ = std::fs::remove_file(root.join("catalog.json"));
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().ends_with(".rdat") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }

    /// Checkpoint if the live WAL has grown past the configured threshold.
    /// Best-effort: the data an auto-checkpoint would compact is already
    /// durable in the WAL, so a failure here only defers compaction.
    fn maybe_checkpoint(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.checkpoint_bytes > 0
            && inner.wal.bytes() >= inner.checkpoint_bytes
            && let Err(e) = Self::checkpoint_locked(&mut inner, &self.commit)
        {
            eprintln!("[oxidb-sql] auto-checkpoint failed (will retry): {e}");
        }
    }

    /// Parse and execute a SQL string, returning one [`QueryResult`] per
    /// statement.
    pub fn execute(&self, sql: &str) -> Result<Vec<QueryResult>> {
        self.execute_params(sql, &[])
    }

    /// Like [`execute`](Self::execute) but binds `?`/`$N` placeholders to
    /// `params` (left-to-right for `?`, `N-1` for `$N`).
    ///
    /// `BEGIN`/`COMMIT`/`ROLLBACK` within the string open and close a
    /// transaction whose writes are buffered and, on `COMMIT`, flushed
    /// atomically as one WAL batch. An unmatched `BEGIN` at end of string is
    /// rolled back (its buffered writes are discarded).
    pub fn execute_params(&self, sql: &str, params: &[Value]) -> Result<Vec<QueryResult>> {
        // Batch-scoped semantics: a transaction left open at the end of the
        // batch is discarded (auto-rollback), exactly as before interactive
        // transactions existed.
        let mut session_tx = None;
        let result = self.execute_params_in_session(sql, params, &mut session_tx);
        if let Some(id) = session_tx {
            self.rollback_session_txn(id);
        }
        result
    }

    /// Like [`execute_params`](Self::execute_params), but `BEGIN`/`COMMIT`
    /// may span calls: a transaction left open at the end of the batch is
    /// parked in the engine and `*session_tx` carries its id for the next
    /// call (ADR-0013 Phase B — interactive transactions). `SAVEPOINT name`,
    /// `ROLLBACK TO SAVEPOINT name`, and `RELEASE SAVEPOINT name` operate on
    /// the open transaction. A statement error rolls the open transaction
    /// back and clears `*session_tx`.
    pub fn execute_params_in_session(
        &self,
        sql: &str,
        params: &[Value],
        session_tx: &mut Option<u64>,
    ) -> Result<Vec<QueryResult>> {
        // Resume a parked transaction, if the session has one.
        let mut txn: Option<Transaction<'_>> = match *session_tx {
            Some(id) => {
                let state = self
                    .session_txns
                    .lock()
                    .unwrap()
                    .remove(&id)
                    .ok_or_else(|| {
                        SqlError::Unsupported(format!(
                            "no such transaction: {id} (rolled back, committed, or busy)"
                        ))
                    })?;
                Some(Transaction::from_state(self, state))
            }
            None => None,
        };

        let result = self.run_session_batch(sql, params, &mut txn);

        match result {
            Ok(results) => {
                // Park a still-open transaction for the next call.
                match txn {
                    Some(t) => {
                        let id = session_tx.unwrap_or_else(|| {
                            self.next_session_txn
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        });
                        self.session_txns.lock().unwrap().insert(id, t.into_state());
                        *session_tx = Some(id);
                    }
                    None => *session_tx = None,
                }
                self.maybe_checkpoint();
                Ok(results)
            }
            Err(e) => {
                // A failed statement aborts the transaction (rolled back,
                // row locks released); the session starts clean.
                if let Some(t) = txn.take() {
                    t.rollback();
                }
                *session_tx = None;
                Err(e)
            }
        }
    }

    /// Execute one statement batch against an optional open transaction.
    /// Parse `sql`, serving repeats from the statement cache (text -> AST is
    /// pure; execution clones the statements, so cached ASTs are immutable).
    /// Only successful parses are cached.
    fn cached_parse(&self, sql: &str) -> Result<std::sync::Arc<Vec<ast::Statement>>> {
        if let Some(hit) = self.stmt_cache.lock().unwrap().get(sql) {
            return Ok(hit.clone());
        }
        let parsed = std::sync::Arc::new(parser::parse(sql)?);
        if sql.len() > STMT_CACHE_MAX_TEXT {
            return Ok(parsed);
        }
        let mut cache = self.stmt_cache.lock().unwrap();
        if cache.len() >= STMT_CACHE_CAP {
            cache.clear();
        }
        cache.insert(sql.to_string(), parsed.clone());
        Ok(parsed)
    }

    /// Classify each statement in `sql` — one [`CommandKind`] per statement, in
    /// order, aligned with the `QueryResult`s `execute` returns.
    ///
    /// Served from the same statement cache execution uses, so a caller that
    /// classifies and then executes the same text parses it once.
    pub fn command_kinds(&self, sql: &str) -> Result<Vec<CommandKind>> {
        Ok(self.cached_parse(sql)?.iter().map(command_kind).collect())
    }

    /// EF Core's HiLo value generation emits `CREATE SEQUENCE`, `DROP
    /// SEQUENCE`, and `SELECT NEXT VALUE FOR seq` — none of which sqlparser's
    /// GenericDialect parses. Handle those three shapes directly (EF sends one
    /// per command) before the SQL reaches the parser; returns None for
    /// anything else so the normal path runs. Sequences persist in the catalog.
    fn try_sequence_stmt(&self, sql: &str) -> Result<Option<QueryResult>> {
        let t = sql.trim().trim_end_matches(';').trim();
        let up = t.to_ascii_uppercase();
        if !(up.starts_with("CREATE SEQUENCE")
            || up.starts_with("DROP SEQUENCE")
            || up.starts_with("SELECT NEXT VALUE FOR"))
        {
            return Ok(None);
        }
        let strip = |s: &str| {
            s.trim_matches(|c| matches!(c, '"' | '[' | ']' | '`'))
                .to_string()
        };
        let toks: Vec<&str> = t.split_whitespace().collect();
        let num = |s: &str| -> i64 {
            s.trim_matches(|c: char| !c.is_ascii_digit() && c != '-')
                .parse()
                .unwrap_or(1)
        };

        if up.starts_with("CREATE SEQUENCE") {
            // CREATE SEQUENCE <name> [AS type] [START WITH n] [INCREMENT BY n] ...
            let name = strip(
                toks.get(2)
                    .ok_or_else(|| SqlError::Parse("CREATE SEQUENCE: missing name".into()))?,
            );
            let (mut start, mut inc) = (1i64, 1i64);
            for i in 0..toks.len() {
                let w = toks[i].to_ascii_uppercase();
                if w == "START"
                    && toks
                        .get(i + 1)
                        .is_some_and(|s| s.eq_ignore_ascii_case("WITH"))
                    && let Some(v) = toks.get(i + 2)
                {
                    start = num(v);
                }
                if w == "INCREMENT"
                    && toks
                        .get(i + 1)
                        .is_some_and(|s| s.eq_ignore_ascii_case("BY"))
                    && let Some(v) = toks.get(i + 2)
                {
                    inc = num(v);
                }
            }
            let mut inner = self.inner.lock().unwrap();
            inner.catalog.sequences.insert(
                name,
                catalog::SequenceDef {
                    next: start,
                    increment: inc,
                },
            );
            let dir = inner.dir.clone();
            catalog::save_sequences(&dir, &inner.catalog.sequences)?;
            return Ok(Some(QueryResult::Ddl));
        }

        if up.starts_with("DROP SEQUENCE") {
            let name = strip(
                toks.last()
                    .ok_or_else(|| SqlError::Parse("DROP SEQUENCE: missing name".into()))?,
            );
            let mut inner = self.inner.lock().unwrap();
            let existed = inner.catalog.sequences.remove(&name).is_some();
            if existed {
                let dir = inner.dir.clone();
                catalog::save_sequences(&dir, &inner.catalog.sequences)?;
            } else if !up.contains("IF EXISTS") {
                return Err(SqlError::Unsupported(format!("no such sequence: {name}")));
            }
            return Ok(Some(QueryResult::Ddl));
        }

        // SELECT NEXT VALUE FOR <name>
        let name = strip(
            toks.get(4)
                .ok_or_else(|| SqlError::Parse("NEXT VALUE FOR: missing sequence".into()))?,
        );
        let mut inner = self.inner.lock().unwrap();
        let seq = inner
            .catalog
            .sequences
            .get_mut(&name)
            .ok_or_else(|| SqlError::Unsupported(format!("no such sequence: {name}")))?;
        let v = seq.next;
        seq.next = seq.next.saturating_add(seq.increment);
        let dir = inner.dir.clone();
        catalog::save_sequences(&dir, &inner.catalog.sequences)?;
        Ok(Some(QueryResult::Select {
            columns: vec![String::new()],
            types: vec![Some(types::SqlType::Int)],
            rows: vec![vec![Value::Int(v)]],
        }))
    }

    fn run_session_batch<'a>(
        &'a self,
        sql: &str,
        params: &[Value],
        txn: &mut Option<Transaction<'a>>,
    ) -> Result<Vec<QueryResult>> {
        use ast::Statement;
        // HiLo sequence SQL that the parser can't handle, dispatched before it.
        // Sequences are non-transactional (a value handed out is not rolled
        // back), so this runs whether or not a transaction is open — EF wraps
        // EnsureCreated's CREATE SEQUENCE commands in one.
        if let Some(r) = self.try_sequence_stmt(sql)? {
            return Ok(vec![r]);
        }
        let statements = self.cached_parse(sql)?;
        let mut results = Vec::with_capacity(statements.len());

        for stmt in statements.iter().cloned() {
            match stmt {
                Statement::Begin => {
                    if txn.is_some() {
                        return Err(SqlError::Unsupported("nested transaction".into()));
                    }
                    *txn = Some(Transaction::new(self));
                    results.push(QueryResult::Transaction);
                }
                Statement::Commit => {
                    let t = txn
                        .take()
                        .ok_or_else(|| SqlError::Unsupported("COMMIT without BEGIN".into()))?;
                    t.commit()?;
                    results.push(QueryResult::Transaction);
                }
                Statement::Rollback => {
                    txn.take()
                        .ok_or_else(|| SqlError::Unsupported("ROLLBACK without BEGIN".into()))?
                        .rollback();
                    results.push(QueryResult::Transaction);
                }
                Statement::Savepoint(name) => {
                    txn.as_ref()
                        .ok_or_else(|| {
                            SqlError::Unsupported("SAVEPOINT outside a transaction".into())
                        })?
                        .savepoint(&name);
                    results.push(QueryResult::Transaction);
                }
                Statement::RollbackToSavepoint(name) => {
                    txn.as_ref()
                        .ok_or_else(|| {
                            SqlError::Unsupported(
                                "ROLLBACK TO SAVEPOINT outside a transaction".into(),
                            )
                        })?
                        .rollback_to_savepoint(&name)?;
                    results.push(QueryResult::Transaction);
                }
                Statement::ReleaseSavepoint(name) => {
                    txn.as_ref()
                        .ok_or_else(|| {
                            SqlError::Unsupported("RELEASE SAVEPOINT outside a transaction".into())
                        })?
                        .release_savepoint(&name)?;
                    results.push(QueryResult::Transaction);
                }
                Statement::Call { name, args } => {
                    // A CALL is atomic: inside an open transaction it simply
                    // joins it; at top level it runs in an implicit one.
                    let r = match &txn {
                        Some(t) => executor::exec_call(t, &name, &args, params)?,
                        None => {
                            let t = Transaction::new(self);
                            let r = executor::exec_call(&t, &name, &args, params)?;
                            t.commit()?;
                            r
                        }
                    };
                    results.push(r);
                }
                other => {
                    let r = match &txn {
                        Some(t) => executor::execute(t, other, params)?,
                        None => {
                            // Autocommit: row locks taken by this statement
                            // (FOR UPDATE, UPDATE, DELETE) live exactly as
                            // long as the statement.
                            let owner = self.alloc_lock_owner();
                            STMT_LOCK_OWNER.set(owner);
                            let r = executor::execute(self, other, params);
                            STMT_LOCK_OWNER.set(0);
                            self.row_locks.release_all(owner);
                            r?
                        }
                    };
                    results.push(r);
                }
            }
        }
        Ok(results)
    }

    /// `ALTER TABLE` — validate, log, apply, and checkpoint (the checkpoint
    /// rewrites `.rdat` snapshots at the new arity, which disk-first mode's
    /// mmap'd bases depend on).
    pub fn alter_table(&self, table: &str, op: &ast::AlterOp) -> Result<()> {
        use ast::AlterOp;
        let mut inner = self.inner.lock().unwrap();
        let Some(def) = inner.catalog.tables.get(table) else {
            return Err(SqlError::NoSuchTable(table.to_string()));
        };
        match op {
            AlterOp::AddColumn(col) => {
                // A tombstoned column's name is free to reuse — only a live
                // column of the same name collides.
                if def.columns.iter().any(|c| c.name == col.name && !c.dropped) {
                    return Err(SqlError::SchemaMismatch(format!(
                        "column {:?} already exists in {table:?}",
                        col.name
                    )));
                }
                if col.primary_key || col.auto_increment {
                    return Err(SqlError::Unsupported(
                        "adding a PRIMARY KEY / AUTO_INCREMENT column".into(),
                    ));
                }
                let has_rows = inner
                    .tables
                    .get(table)
                    .map(|s| s.rows.len() > 0)
                    .unwrap_or(false);
                if !col.nullable && col.default_value.is_none() && has_rows {
                    return Err(SqlError::SchemaMismatch(format!(
                        "cannot add NOT NULL column {:?} without a DEFAULT to a non-empty table",
                        col.name
                    )));
                }
            }
            AlterOp::DropColumn(name) => {
                let Some(pos) = def
                    .columns
                    .iter()
                    .position(|c| &c.name == name && !c.dropped)
                else {
                    return Err(SqlError::NoSuchColumn(name.clone()));
                };
                if def.columns[pos].primary_key {
                    return Err(SqlError::Unsupported(
                        "dropping the PRIMARY KEY column".into(),
                    ));
                }
                if let Some(d) = inner
                    .catalog
                    .indexes
                    .values()
                    .find(|d| d.table == table && d.columns.contains(name))
                {
                    return Err(SqlError::SchemaMismatch(format!(
                        "index {:?} depends on column {:?} (drop the index first)",
                        d.name, name
                    )));
                }
            }
            AlterOp::RenameColumn { old, new } => {
                if !def.columns.iter().any(|c| &c.name == old && !c.dropped) {
                    return Err(SqlError::NoSuchColumn(old.clone()));
                }
                if def.columns.iter().any(|c| &c.name == new && !c.dropped) {
                    return Err(SqlError::SchemaMismatch(format!(
                        "column {new:?} already exists in {table:?}"
                    )));
                }
            }
            AlterOp::AlterColumnType {
                column,
                ty,
                max_len,
                int_width,
            } => {
                let Some(pos) = def
                    .columns
                    .iter()
                    .position(|c| &c.name == column && !c.dropped)
                else {
                    return Err(SqlError::NoSuchColumn(column.clone()));
                };
                let col = &def.columns[pos];
                // Keys keep their identity semantics: a cast can collide two
                // previously-distinct values, silently breaking uniqueness.
                if col.primary_key || col.auto_increment || col.unique {
                    return Err(SqlError::Unsupported(
                        "changing the type of a PRIMARY KEY / AUTO_INCREMENT / UNIQUE column"
                            .into(),
                    ));
                }
                // FK join columns must stay type-compatible on both sides.
                let fk_bound = def.foreign_keys.iter().any(|fk| &fk.column == column)
                    || inner.catalog.tables.values().any(|t| {
                        t.foreign_keys
                            .iter()
                            .any(|fk| fk.parent_table == table && &fk.parent_column == column)
                    });
                if fk_bound {
                    return Err(SqlError::Unsupported(
                        "changing the type of a FOREIGN KEY column".into(),
                    ));
                }
                // Dry-run: every stored value (and the default) must cast, fit
                // a shrunk VARCHAR(n), and fit a narrowed integer width, before
                // anything is written.
                let mut probe = Column::new(column.clone(), *ty);
                probe.max_len = *max_len;
                probe.int_width = *int_width;
                let int_range = probe.int_range();
                let type_name = probe.type_name();
                let check_len = |v: &Value| -> Result<()> {
                    if let (Some(max), Value::Text(s)) = (max_len, v) {
                        let got = s.chars().count();
                        if got as u64 > u64::from(*max) {
                            return Err(SqlError::ValueTooLong {
                                column: column.clone(),
                                max: *max,
                                got,
                            });
                        }
                    }
                    if let (Some((min, max)), Value::Int(n)) = (int_range, v)
                        && (*n < min || *n > max)
                    {
                        return Err(SqlError::IntegerOutOfRange {
                            column: column.clone(),
                            type_name,
                            value: *n,
                            min,
                            max,
                        });
                    }
                    Ok(())
                };
                if let Some(dv) = &col.default_value
                    && !matches!(dv, Value::Null)
                {
                    check_len(&executor::cast_value(dv.clone(), *ty)?)?;
                }
                if let Some(state) = inner.tables.get(table) {
                    for (_id, cells) in state.rows.iter_physical() {
                        let v = &cells[pos];
                        if !matches!(v, Value::Null) {
                            check_len(&executor::cast_value(v.clone(), *ty)?)?;
                        }
                    }
                }
            }
        }
        let rec = WalRecord::AlterTable {
            table: table.to_string(),
            op: op.clone(),
        };
        let seq = Self::log_and_apply(&mut inner, &self.commit, &rec)?;
        drop(inner);
        self.commit_pending(seq)?;
        // Metadata-only ADD/DROP COLUMN are O(1): the WAL record is durable on
        // its own and the stored rows are untouched (read back padded for ADD,
        // projected for DROP), so skip the checkpoint — which would write the
        // whole table's snapshot and defeat the point. A later auto/manual
        // checkpoint folds it in lazily. RENAME still checkpoints eagerly.
        if matches!(op, AlterOp::AddColumn(_) | AlterOp::DropColumn(_)) {
            return Ok(());
        }
        // RENAME / TYPE rewrite the stored rows, so they checkpoint eagerly —
        // which needs the lock back, now that the record is durable.
        let mut inner = self.inner.lock().unwrap();
        Self::checkpoint_locked(&mut inner, &self.commit)
    }

    /// Take a parked session transaction's buffered operations as JSON,
    /// removing the transaction — for a host that replicates the commit
    /// (cluster mode) instead of applying it locally. Applying the returned
    /// ops via [`apply_replicated_txn_ops`](Self::apply_replicated_txn_ops)
    /// on every node (including this one) completes the commit.
    pub fn take_session_txn_ops(&self, id: u64) -> Result<serde_json::Value> {
        let state = self
            .session_txns
            .lock()
            .unwrap()
            .remove(&id)
            .ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "no such transaction: {id} (rolled back, committed, or busy)"
                ))
            })?;
        Ok(serde_json::to_value(state.take_ops())?)
    }

    /// Apply a replicated buffered commit (the ops from
    /// [`take_session_txn_ops`](Self::take_session_txn_ops)) as one atomic
    /// WAL batch. Deterministic: the ops carry final row ids and cells.
    pub fn apply_replicated_txn_ops(&self, ops: &serde_json::Value) -> Result<()> {
        let ops: Vec<WalRecord> = serde_json::from_value(ops.clone())?;
        self.commit_batch(ops)
    }

    /// Roll back (discard) a parked session transaction. Safe to call for
    /// ids that no longer exist.
    /// Allocate a unique lock-owner id (shared counter with session-txn ids;
    /// 0 is reserved for "none").
    pub(crate) fn alloc_lock_owner(&self) -> u64 {
        self.next_session_txn
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// Acquire row locks for `owner` (blocking, engine lock-timeout). The
    /// `row_locks` field is module-private; these two helpers are the
    /// cross-module surface `transaction.rs` uses.
    pub(crate) fn row_locks_lock(&self, table: &str, row_ids: &[u64], owner: u64) -> Result<()> {
        self.row_locks
            .lock_many(table, row_ids, owner, self.lock_timeout)
    }

    pub(crate) fn row_locks_release(&self, owner: u64) {
        self.row_locks.release_all(owner);
    }

    pub fn rollback_session_txn(&self, id: u64) {
        // Dropping the state discards the buffered writes; its row locks go
        // with it.
        if let Some(state) = self.session_txns.lock().unwrap().remove(&id) {
            self.row_locks.release_all(state.lock_owner());
        }
    }

    /// Path to the SQL root directory.
    pub fn dir(&self) -> PathBuf {
        self.inner.lock().unwrap().dir.clone()
    }

    /// Write a consistent, compressed (`.tar.gz`) backup of this engine's data
    /// to `out`. **Low-lock**: the engine lock is held only for two O(1)
    /// phases — pinning a committed generation and reading the WAL length up
    /// front, then unpinning at the end — while the slow compression runs with
    /// the lock released, so concurrent queries and writes are not blocked by
    /// the archive. The archived image is consistent as of the moment the
    /// generation was pinned (a crash-consistent point): pinning keeps that
    /// generation from being GC'd and freezes the WAL from truncation, so the
    /// snapshot + a stable WAL prefix restore cleanly. Returns the archive size.
    pub fn backup(&self, out: &Path) -> Result<u64> {
        if out.exists() {
            return Err(SqlError::Unsupported(format!(
                "backup target already exists: {}",
                out.display()
            )));
        }
        if let Some(parent) = out.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        // Phase 1 (brief lock): choose a committed generation, pin it, and read
        // its watermark + the current WAL length. A never-checkpointed database
        // (generation 0) is checkpointed once here to materialize `gen.1`.
        let (generation, watermark, wal_len, dir) = {
            let mut inner = self.inner.lock().unwrap();
            if inner.generation == 0 {
                Self::checkpoint_locked(&mut inner, &self.commit)?;
            }
            let generation = inner.generation;
            *inner.pinned_gens.entry(generation).or_insert(0) += 1;
            (
                generation,
                inner.committed_wal_seq,
                inner.wal.bytes(),
                inner.dir.clone(),
            )
        };

        // Phase 2 (no lock): compress. While pinned, `gen.<generation>` is safe
        // from GC and the WAL prefix `[0, wal_len)` is frozen (no truncation),
        // so this reads a stable, consistent image even as writes continue.
        let result = Self::write_backup_archive(&dir, generation, watermark, wal_len, out);

        // Phase 3 (brief lock): drop the pin, and reclaim the generation if a
        // checkpoint superseded it during the backup (GC skipped it while
        // pinned).
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(count) = inner.pinned_gens.get_mut(&generation) {
                *count -= 1;
                if *count == 0 {
                    inner.pinned_gens.remove(&generation);
                }
            }
            if generation != inner.generation && !inner.pinned_gens.contains_key(&generation) {
                let _ = std::fs::remove_dir_all(manifest::gen_dir(&inner.dir, generation));
            }
        }

        result?;
        Ok(std::fs::metadata(out)?.len())
    }

    /// Assemble a backup archive (no lock held): a synthesized `MANIFEST`
    /// pointing at the pinned `generation`, that generation's directory, a
    /// stable prefix `[0, wal_len)` of the live WAL, and `sequences.json`.
    fn write_backup_archive(
        root: &Path,
        generation: u64,
        watermark: u64,
        wal_len: u64,
        out: &Path,
    ) -> Result<()> {
        use std::io::Read;
        let map_tar =
            |e: std::io::Error| SqlError::Unsupported(format!("backup archive failed: {e}"));

        let file = std::fs::File::create(out)?;
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut ar = tar::Builder::new(enc);

        // Synthesized MANIFEST — points at the pinned generation, not the live
        // one (which a concurrent checkpoint may have advanced past).
        let manifest_bytes = manifest::Manifest::to_bytes(generation, watermark)?;
        let mut hdr = tar::Header::new_gnu();
        hdr.set_size(manifest_bytes.len() as u64);
        hdr.set_mode(0o644);
        hdr.set_cksum();
        ar.append_data(&mut hdr, "MANIFEST", &manifest_bytes[..])
            .map_err(map_tar)?;

        // The pinned generation directory (immutable once committed).
        ar.append_dir_all(
            format!("gen.{generation}"),
            manifest::gen_dir(root, generation),
        )
        .map_err(map_tar)?;

        // A stable prefix of the WAL. Truncation is frozen while pinned, so the
        // file is at least `wal_len` bytes and its prefix never changes.
        let wal_path = root.join("wal").join("live.wal");
        if wal_len > 0 && wal_path.exists() {
            let f = std::fs::File::open(&wal_path)?;
            let mut hdr = tar::Header::new_gnu();
            hdr.set_size(wal_len);
            hdr.set_mode(0o644);
            hdr.set_cksum();
            ar.append_data(&mut hdr, "wal/live.wal", f.take(wal_len))
                .map_err(map_tar)?;
        }

        // Sequences persist outside the generation.
        let seq_path = root.join("sequences.json");
        if seq_path.exists() {
            ar.append_path_with_name(&seq_path, "sequences.json")
                .map_err(map_tar)?;
        }

        ar.into_inner()
            .map_err(map_tar)?
            .finish()
            .map_err(map_tar)?;
        Ok(())
    }

    /// Extract a `.tar.gz` backup produced by [`backup`](SqlEngine::backup) into
    /// `target` (which must be empty or absent). Static: open a fresh
    /// `SqlEngine` on `target` afterward to use the restored database.
    pub fn restore(archive: &Path, target: &Path) -> Result<()> {
        if !archive.exists() {
            return Err(SqlError::Unsupported(format!(
                "backup archive not found: {}",
                archive.display()
            )));
        }
        if target.exists() {
            if std::fs::read_dir(target)?.next().is_some() {
                return Err(SqlError::Unsupported(format!(
                    "restore target is not empty: {}",
                    target.display()
                )));
            }
        } else {
            std::fs::create_dir_all(target)?;
        }
        let file = std::fs::File::open(archive)?;
        let dec = flate2::read::GzDecoder::new(file);
        tar::Archive::new(dec)
            .unpack(target)
            .map_err(|e| SqlError::Unsupported(format!("restore extract failed: {e}")))?;
        Ok(())
    }
}

/// Whether every statement in `sql` is a read (a SELECT — possibly with set
/// operations — or a SHOW/DESCRIBE introspection statement). Callers that
/// gate write access per statement (e.g. a read-only server role) check this
/// before executing.
/// Whether executing `sql` would leave a transaction open at the end of the
/// batch (a `BEGIN` without a matching `COMMIT`/`ROLLBACK`). Used by the
/// cluster session layer, which cannot replicate open-ended transactions.
/// Whether `sql` is exactly one `BEGIN` statement (how a cluster session
/// starts an interactive transaction).
pub fn is_lone_begin(sql: &str) -> bool {
    matches!(parser::parse(sql).as_deref(), Ok([ast::Statement::Begin]))
}

/// Whether `sql` is exactly one `COMMIT` statement (a cluster session's
/// buffered commit is intercepted and replicated).
pub fn is_lone_commit(sql: &str) -> bool {
    matches!(parser::parse(sql).as_deref(), Ok([ast::Statement::Commit]))
}

pub fn leaves_transaction_open(sql: &str) -> Result<bool> {
    let mut open = false;
    for stmt in parser::parse(sql)? {
        match stmt {
            ast::Statement::Begin => open = true,
            ast::Statement::Commit | ast::Statement::Rollback => open = false,
            _ => {}
        }
    }
    Ok(open)
}

/// Every base table a statement reads or writes, in no particular order.
///
/// Used by the server to decide whether an untrusted caller may run a
/// statement at all: a security rule is per table, and a query that joins
/// three of them has to satisfy the rule of each. Derived tables and CTEs
/// contribute their own inner references, not their alias.
pub fn referenced_tables(sql: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for stmt in parser::parse(sql)? {
        collect_statement_tables(&stmt, &mut out);
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn push_table(name: &str, out: &mut Vec<String>) {
    if !name.is_empty() {
        out.push(name.to_string());
    }
}

fn collect_statement_tables(stmt: &ast::Statement, out: &mut Vec<String>) {
    use ast::Statement as S;
    match stmt {
        S::Select(q) => collect_query_tables(q, out),
        S::Insert { table, .. } => push_table(table, out),
        S::Update { table, .. } => push_table(table, out),
        S::Delete { table, .. } => push_table(table, out),
        S::CreateTable { table, .. } => push_table(&table.name, out),
        S::DropTable { name, .. } => push_table(name, out),
        S::AlterTable { table, .. } => push_table(table, out),
        S::CreateIndex { table, .. } => push_table(table, out),
        // Anything else (transactions, SHOW, sequences, procedures) names no
        // base table a rule could apply to.
        _ => {}
    }
}

fn collect_query_tables(q: &ast::SelectQuery, out: &mut Vec<String>) {
    for cte in &q.ctes {
        // A recursive CTE reads its anchor and its step; the CTE's own name is
        // not a base table.
        collect_body_tables(&cte.anchor, out);
        collect_body_tables(&cte.step, out);
    }
    collect_body_tables(&q.body, out);
}

fn collect_body_tables(body: &ast::QueryBody, out: &mut Vec<String>) {
    match body {
        ast::QueryBody::Select(sel) => {
            if let Some(from) = &sel.from {
                collect_table_ref(from, out);
            }
            for join in &sel.joins {
                collect_table_ref(&join.table, out);
            }
        }
        ast::QueryBody::SetOp { left, right, .. } => {
            collect_body_tables(left, out);
            collect_body_tables(right, out);
        }
        ast::QueryBody::Values(_) => {}
    }
}

fn collect_table_ref(t: &ast::TableRef, out: &mut Vec<String>) {
    match &t.subquery {
        // A derived table reads whatever is inside it; its alias is not a table.
        Some(inner) => collect_query_tables(inner, out),
        None => push_table(&t.name, out),
    }
}

/// What kind of statement produced a result — the PostgreSQL wire protocol's
/// `CommandComplete` has to name it (`INSERT 0 3`, `UPDATE 2`, `CREATE TABLE`),
/// and [`QueryResult`] deliberately does not: `Mutation` and `Ddl` say what
/// happened, not which verb asked for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    Select,
    Insert,
    Update,
    Delete,
    Begin,
    Commit,
    Rollback,
    Savepoint,
    Release,
    Call,
    Show,
    /// DDL, carrying the tag PostgreSQL would use (`CREATE TABLE`, `DROP VIEW`).
    Ddl(&'static str),
}

impl CommandKind {
    /// The `CommandComplete` tag, given the row count the statement produced.
    /// `INSERT` alone carries an OID field (always 0 since PostgreSQL 12).
    pub fn tag(self, rows: usize) -> String {
        match self {
            CommandKind::Select => format!("SELECT {rows}"),
            CommandKind::Insert => format!("INSERT 0 {rows}"),
            CommandKind::Update => format!("UPDATE {rows}"),
            CommandKind::Delete => format!("DELETE {rows}"),
            CommandKind::Begin => "BEGIN".into(),
            CommandKind::Commit => "COMMIT".into(),
            CommandKind::Rollback => "ROLLBACK".into(),
            CommandKind::Savepoint => "SAVEPOINT".into(),
            CommandKind::Release => "RELEASE".into(),
            CommandKind::Call => "CALL".into(),
            CommandKind::Show => format!("SELECT {rows}"),
            CommandKind::Ddl(tag) => tag.into(),
        }
    }
}

fn command_kind(s: &ast::Statement) -> CommandKind {
    use ast::Statement as S;
    match s {
        S::Select(_) => CommandKind::Select,
        S::Insert { .. } => CommandKind::Insert,
        S::Update { .. } => CommandKind::Update,
        S::Delete { .. } => CommandKind::Delete,
        S::Begin => CommandKind::Begin,
        S::Commit => CommandKind::Commit,
        S::Rollback => CommandKind::Rollback,
        S::Savepoint(_) | S::RollbackToSavepoint(_) => CommandKind::Savepoint,
        S::ReleaseSavepoint(_) => CommandKind::Release,
        S::Call { .. } => CommandKind::Call,
        S::Show(_) => CommandKind::Show,
        S::CreateTable { .. } => CommandKind::Ddl("CREATE TABLE"),
        S::DropTable { .. } => CommandKind::Ddl("DROP TABLE"),
        S::AlterTable { .. } => CommandKind::Ddl("ALTER TABLE"),
        S::CreateIndex { .. } => CommandKind::Ddl("CREATE INDEX"),
        S::DropIndex { .. } => CommandKind::Ddl("DROP INDEX"),
        S::CreateView { .. } => CommandKind::Ddl("CREATE VIEW"),
        S::DropView { .. } => CommandKind::Ddl("DROP VIEW"),
        S::CreateProcedure { .. } => CommandKind::Ddl("CREATE PROCEDURE"),
        S::DropProcedure { .. } => CommandKind::Ddl("DROP PROCEDURE"),
    }
}

pub fn is_read_only(sql: &str) -> Result<bool> {
    Ok(parser::parse(sql)?.iter().all(|s| match s {
        // FOR UPDATE takes row locks: routing it to a replica would return
        // rows with no lock behind them, so it must ride the write path.
        ast::Statement::Select(q) => !q.for_update,
        ast::Statement::Show(_) => true,
        _ => false,
    }))
}

/// Autocommit `Store`: every operation is applied and logged immediately.
impl Store for SqlEngine {
    fn table_def(&self, name: &str) -> Option<Table> {
        SqlEngine::table_def(self, name)
    }
    fn lock_rows(&self, table: &str, row_ids: &[u64]) -> Result<()> {
        let owner = STMT_LOCK_OWNER.get();
        if owner == 0 {
            // Every public execution path scopes an owner around the
            // statement; reaching this means a new call path skipped it.
            return Err(SqlError::Unsupported(
                "row locking outside a statement scope".into(),
            ));
        }
        // Blocks on contention — `inner` is NOT held here, so the holder can
        // commit and release while we wait.
        self.row_locks
            .lock_many(table, row_ids, owner, self.lock_timeout)
    }
    fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>> {
        SqlEngine::scan(self, table)
    }
    fn scan_pruned(&self, table: &str, keep: &[usize]) -> Result<store::Chunk> {
        let inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        let n = state.rows.len();
        let mut cells = Vec::with_capacity(n * keep.len());
        for (_, row) in state.rows.iter() {
            for &k in keep {
                cells.push(row[k].clone());
            }
        }
        Ok(store::Chunk {
            width: keep.len(),
            n,
            cells,
        })
    }
    fn scan_visit(
        &self,
        table: &str,
        visit: &mut dyn FnMut(&[Value]) -> Result<bool>,
    ) -> Result<()> {
        // Rows are handed to the visitor borrowed, under the table lock: a
        // streamed scan allocates nothing per row. The executor guarantees
        // the visitor never re-enters this engine (the lock is not reentrant).
        let mut inner = self.inner.lock().unwrap();
        let state = inner
            .tables
            .get_mut(table)
            .ok_or_else(|| SqlError::NoSuchTable(table.to_string()))?;
        // Contiguous scan cache: for a resident table big enough to matter,
        // stream a flat row-major buffer instead of chasing the BTreeMap's
        // scattered per-row Vecs (the dominant cost of a large scan). The
        // buffer is keyed on the store's mutation generation, so any write
        // rebuilds it; repeated scans of an unchanged table reuse it.
        // `scan`/`iter` yield the logical (projected) row, so the cache width
        // is the logical column count — not the physical arity.
        let width = state.rows.logical_width();
        if width > 0 && state.rows.is_resident() && state.rows.len() >= 1024 {
            let generation = state.rows.generation();
            let hit = matches!(&state.scan_cache, Some((g, _, _)) if *g == generation);
            if !hit && state.scan_seen_gen == Some(generation) {
                // Second scan at an unchanged generation: worth caching now.
                let mut flat = Vec::with_capacity(state.rows.len() * width);
                for (_, cells) in state.rows.iter() {
                    flat.extend_from_slice(&cells);
                }
                state.scan_cache = Some((generation, width, flat));
            }
            if let Some((g, w, flat)) = &state.scan_cache
                && *g == generation
            {
                for chunk in flat.chunks_exact(*w) {
                    if !visit(chunk)? {
                        break;
                    }
                }
                return Ok(());
            }
            // First sight of this generation (or just recorded): iterate direct.
            state.scan_seen_gen = Some(generation);
        }
        for (_, row) in state.rows.iter() {
            if !visit(row.as_ref())? {
                break;
            }
        }
        Ok(())
    }
    fn insert(&self, table: &str, cells: Vec<Value>) -> Result<u64> {
        SqlEngine::insert(self, table, cells)
    }
    fn insert_many(&self, table: &str, rows: Vec<Vec<Value>>) -> Result<u64> {
        SqlEngine::insert_many(self, table, rows)
    }
    fn update_row(&self, table: &str, row_id: u64, cells: Vec<Value>) -> Result<()> {
        SqlEngine::update_row(self, table, row_id, cells)
    }
    fn delete(&self, table: &str, row_id: u64) -> Result<bool> {
        SqlEngine::delete(self, table, row_id)
    }
    fn delete_many(&self, table: &str, row_ids: &[u64]) -> Result<usize> {
        SqlEngine::delete_many(self, table, row_ids)
    }
    fn delete_multi(&self, items: &[(String, u64)]) -> Result<usize> {
        SqlEngine::delete_multi(self, items)
    }
    fn create_table(&self, table: Table) -> Result<()> {
        SqlEngine::create_table(self, table)
    }
    fn drop_table(&self, name: &str) -> Result<()> {
        SqlEngine::drop_table(self, name)
    }
    fn create_index(&self, name: &str, table: &str, columns: &[String]) -> Result<()> {
        SqlEngine::create_index(self, name, table, columns)
    }
    fn drop_index(&self, name: &str) -> Result<()> {
        SqlEngine::drop_index(self, name)
    }
    fn create_view(&self, name: &str, query_sql: &str, or_replace: bool) -> Result<()> {
        SqlEngine::create_view(self, name, query_sql, or_replace)
    }
    fn drop_view(&self, name: &str) -> Result<()> {
        SqlEngine::drop_view(self, name)
    }
    fn view_sql(&self, name: &str) -> Option<String> {
        SqlEngine::view_sql(self, name)
    }
    fn create_procedure(
        &self,
        name: &str,
        def: catalog::ProcedureDef,
        or_alter: bool,
    ) -> Result<()> {
        SqlEngine::create_procedure(self, name, def, or_alter)
    }
    fn drop_procedure(&self, name: &str) -> Result<()> {
        SqlEngine::drop_procedure(self, name)
    }
    fn procedure_def(&self, name: &str) -> Option<catalog::ProcedureDef> {
        SqlEngine::procedure_def(self, name)
    }
    fn list_procedures(&self) -> Vec<(String, catalog::ProcedureDef)> {
        SqlEngine::list_procedures(self)
    }
    fn next_auto_block(&self, table: &str, n: i64) -> Result<i64> {
        SqlEngine::next_auto_block(self, table, n)
    }
    fn alter_table(&self, table: &str, op: &ast::AlterOp) -> Result<()> {
        SqlEngine::alter_table(self, table, op)
    }
    fn list_tables(&self) -> Vec<Table> {
        SqlEngine::list_tables(self)
    }
    fn list_views(&self) -> Vec<(String, String)> {
        SqlEngine::list_views(self)
    }
    fn list_indexes(&self) -> Vec<IndexDef> {
        SqlEngine::list_indexes(self)
    }
    fn row_count_hint(&self, table: &str) -> Option<usize> {
        SqlEngine::row_count(self, table).ok()
    }
    fn index_lookup_eq(&self, table: &str, eqs: &[(String, Value)]) -> Result<Option<store::Rows>> {
        SqlEngine::index_lookup_eq(self, table, eqs)
    }
    fn index_visit_eq(
        &self,
        table: &str,
        eqs: &[(String, Value)],
        visit: &mut dyn FnMut(&[Value]) -> Result<bool>,
    ) -> Result<Option<()>> {
        SqlEngine::index_visit_eq_inner(self, table, eqs, &mut |_, cells| visit(cells))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users() -> Table {
        Table::new(
            "users",
            vec![
                Column::new("id", SqlType::Int).primary_key(),
                Column::new("name", SqlType::Text).not_null(),
            ],
        )
    }

    #[test]
    fn basic_crud() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();

        let a = db
            .insert("users", vec![Value::Int(1), Value::Text("ada".into())])
            .unwrap();
        let b = db
            .insert("users", vec![Value::Int(2), Value::Text("bob".into())])
            .unwrap();
        assert_ne!(a, b);
        assert_eq!(db.row_count("users").unwrap(), 2);

        assert!(db.delete("users", a).unwrap());
        assert!(!db.delete("users", a).unwrap());
        assert_eq!(db.row_count("users").unwrap(), 1);
    }

    fn select_rows(db: &SqlEngine, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop().unwrap() {
            QueryResult::Select { rows, .. } => rows,
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn index_nested_loop_join_matches_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE small (id INT PRIMARY KEY, tag TEXT)")
            .unwrap();
        db.execute("CREATE TABLE big (id INT PRIMARY KEY, sid INT, v INT)")
            .unwrap();
        db.execute("CREATE INDEX idx_big_sid ON big(sid)").unwrap();
        db.execute("INSERT INTO small VALUES (1,'a'),(2,'b'),(3,'c')")
            .unwrap();
        // big has rows for sid 1 and 2 (multiple), none for 3; plus an orphan sid 9.
        db.execute("INSERT INTO big VALUES (10,1,100),(11,1,101),(12,2,200),(13,9,900)")
            .unwrap();

        // INNER join: small ⋈ big on sid — the index-nested-loop path prunes
        // big to only sid ∈ {1,2,3}. Must equal the set-based result.
        let inner = select_rows(
            &db,
            "SELECT s.id, s.tag, b.v FROM small s JOIN big b ON b.sid = s.id \
             ORDER BY s.id, b.v",
        );
        assert_eq!(
            inner,
            vec![
                vec![Value::Int(1), Value::Text("a".into()), Value::Int(100)],
                vec![Value::Int(1), Value::Text("a".into()), Value::Int(101)],
                vec![Value::Int(2), Value::Text("b".into()), Value::Int(200)],
            ]
        );

        // LEFT join: small=3 has no big row → one NULL-extended row. The INL
        // prune must not drop it.
        let left = select_rows(
            &db,
            "SELECT s.id, b.v FROM small s LEFT JOIN big b ON b.sid = s.id \
             ORDER BY s.id, b.v",
        );
        assert_eq!(
            left,
            vec![
                vec![Value::Int(1), Value::Int(100)],
                vec![Value::Int(1), Value::Int(101)],
                vec![Value::Int(2), Value::Int(200)],
                vec![Value::Int(3), Value::Null],
            ]
        );

        // Aggregate over the joined result (the fraud-scan shape).
        let agg = select_rows(
            &db,
            "SELECT s.id, COUNT(b.id) AS n, SUM(b.v) AS tot \
             FROM small s JOIN big b ON b.sid = s.id GROUP BY s.id ORDER BY s.id",
        );
        assert_eq!(
            agg,
            vec![
                vec![Value::Int(1), Value::Int(2), Value::Int(201)],
                vec![Value::Int(2), Value::Int(1), Value::Int(200)],
            ]
        );
    }

    #[test]
    fn indexed_lookup_inside_transaction_sees_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v TEXT)")
            .unwrap();
        db.execute("CREATE INDEX idx_t_k ON t(k)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10,'a'),(2,10,'b'),(3,20,'c'),(4,30,'d')")
            .unwrap();

        // Baseline (no transaction): index lookup returns the two k=10 rows.
        let base = select_rows(&db, "SELECT id FROM t WHERE k = 10 ORDER BY id");
        assert_eq!(base, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

        // Inside a transaction the same indexed lookup must reflect the overlay:
        // an insert into the group, an update moving a row in, and a delete.
        // A transaction spans calls, so drive it through the session API.
        let mut sess: Option<u64> = None;
        db.execute_params_in_session("BEGIN", &[], &mut sess)
            .unwrap();
        db.execute_params_in_session("INSERT INTO t VALUES (5,10,'e')", &[], &mut sess)
            .unwrap();
        db.execute_params_in_session("UPDATE t SET k = 10 WHERE id = 3", &[], &mut sess)
            .unwrap();
        db.execute_params_in_session("DELETE FROM t WHERE id = 1", &[], &mut sess)
            .unwrap();
        let inside = match db
            .execute_params_in_session("SELECT id FROM t WHERE k = 10 ORDER BY id", &[], &mut sess)
            .unwrap()
            .pop()
            .unwrap()
        {
            QueryResult::Select { rows, .. } => rows,
            other => panic!("expected Select, got {other:?}"),
        };
        db.execute_params_in_session("ROLLBACK", &[], &mut sess)
            .unwrap();
        // Expect {2, 3, 5}: original 2, updated-in 3, inserted 5; 1 deleted.
        assert_eq!(
            inside,
            vec![
                vec![Value::Int(2)],
                vec![Value::Int(3)],
                vec![Value::Int(5)]
            ]
        );

        // After rollback the base is unchanged.
        let after = select_rows(&db, "SELECT id FROM t WHERE k = 10 ORDER BY id");
        assert_eq!(after, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    }

    #[test]
    fn least_greatest_ignore_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,3,9,5),(2,7,NULL,2)")
            .unwrap();
        let rows = select_rows(
            &db,
            "SELECT LEAST(a,b,c) AS lo, GREATEST(a,b,c) AS hi FROM t ORDER BY id",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(3), Value::Int(9)],
                // row 2: b is NULL and is ignored (not treated as smallest).
                vec![Value::Int(2), Value::Int(7)],
            ]
        );
        // All-NULL → NULL.
        let r = select_rows(&db, "SELECT LEAST(NULL, NULL) AS x FROM t WHERE id=1");
        assert_eq!(r, vec![vec![Value::Null]]);
    }

    #[test]
    fn cte_basic_and_chained() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
            .unwrap();

        // Single CTE.
        let r = select_rows(
            &db,
            "WITH big AS (SELECT id, v FROM t WHERE v >= 30) \
                                  SELECT id FROM big ORDER BY id",
        );
        assert_eq!(r, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);

        // Chained CTE (b references a) + aggregate in the body.
        let r = select_rows(
            &db,
            "WITH a AS (SELECT id, v FROM t WHERE v >= 20), \
                  b AS (SELECT v FROM a WHERE v <= 30) \
             SELECT SUM(v) AS s FROM b",
        );
        assert_eq!(r, vec![vec![Value::Int(50)]]); // 20 + 30
    }

    #[test]
    fn cte_referenced_twice_and_joined() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
            .unwrap();
        // `nums` used twice: once for the avg, once joined — the classic
        // "compare each row to the group average" shape.
        let r = select_rows(
            &db,
            "WITH nums AS (SELECT id, v FROM t) \
             SELECT n.id, n.v FROM nums n \
             JOIN (SELECT AVG(v) AS av FROM nums) g ON 1=1 \
             WHERE n.v > g.av ORDER BY n.id",
        );
        assert_eq!(r, vec![vec![Value::Int(3), Value::Int(30)]]); // avg=20, only 30 > 20
    }

    #[test]
    fn distinct_on_keeps_first_per_group() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE sales (id INT PRIMARY KEY, cust INT, amount INT)")
            .unwrap();
        db.execute(
            "INSERT INTO sales VALUES (1,10,5),(2,10,9),(3,10,3),(4,20,7),(5,20,7),(6,30,1)",
        )
        .unwrap();
        // Highest-amount row per customer (argmax) — DISTINCT ON + ORDER BY.
        let rows = select_rows(
            &db,
            "SELECT DISTINCT ON (cust) cust, amount FROM sales \
             ORDER BY cust, amount DESC",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(10), Value::Int(9)],
                vec![Value::Int(20), Value::Int(7)],
                vec![Value::Int(30), Value::Int(1)],
            ]
        );
    }

    #[test]
    fn distinct_on_over_group_by_is_argmax() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE s (id INT PRIMARY KEY, cust INT, cat TEXT, spend INT)")
            .unwrap();
        // cust 1: books=10, toys=25 → toys.  cust 2: books=8, toys=3 → books.
        db.execute(
            "INSERT INTO s VALUES \
             (1,1,'books',6),(2,1,'books',4),(3,1,'toys',25),\
             (4,2,'books',8),(5,2,'toys',3)",
        )
        .unwrap();
        // Dominant category per customer: GROUP BY (cust,cat) sums, then
        // DISTINCT ON (cust) picks the top by summed spend.
        let rows = select_rows(
            &db,
            "SELECT DISTINCT ON (cust) cust, cat, SUM(spend) AS s \
             FROM s GROUP BY cust, cat ORDER BY cust, SUM(spend) DESC",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1), Value::Text("toys".into()), Value::Int(25)],
                vec![Value::Int(2), Value::Text("books".into()), Value::Int(8)],
            ]
        );
    }

    #[test]
    fn mode_within_group_returns_most_frequent() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, c TEXT)")
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES \
             (1,1,'a'),(2,1,'a'),(3,1,'b'),(4,2,'x'),(5,2,'y'),(6,2,'x')",
        )
        .unwrap();
        let rows = select_rows(
            &db,
            "SELECT g, mode() WITHIN GROUP (ORDER BY c) FROM t GROUP BY g ORDER BY g",
        );
        assert_eq!(
            rows,
            vec![
                vec![Value::Int(1), Value::Text("a".into())],
                vec![Value::Int(2), Value::Text("x".into())],
            ]
        );
    }

    #[test]
    fn mode_ties_break_to_smallest() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        // 5 and 9 each appear twice; the tie breaks to the smaller value.
        db.execute("INSERT INTO t VALUES (1,9),(2,5),(3,9),(4,5),(5,7)")
            .unwrap();
        let rows = select_rows(&db, "SELECT mode() WITHIN GROUP (ORDER BY v) FROM t");
        assert_eq!(rows, vec![vec![Value::Int(5)]]);
    }

    #[test]
    fn create_table_rejects_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        assert!(matches!(
            db.create_table(users()),
            Err(SqlError::TableExists(_))
        ));
    }

    #[test]
    fn insert_validates_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        assert!(
            db.insert("users", vec![Value::Int(1), Value::Null])
                .is_err()
        );
        assert!(db.insert("users", vec![Value::Int(1)]).is_err());
    }
}

#[cfg(test)]
mod stmt_cache_tests {
    use super::*;

    fn open() -> (tempfile::TempDir, SqlEngine) {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE t (a INT, b TEXT)").unwrap();
        (dir, db)
    }

    fn cached(db: &SqlEngine) -> usize {
        db.stmt_cache.lock().unwrap().len()
    }

    /// A short, repeatable statement is what the cache is for.
    #[test]
    fn short_statements_are_cached() {
        let (_d, db) = open();
        let before = cached(&db);
        db.execute("SELECT a FROM t WHERE a = 1").unwrap();
        assert!(cached(&db) > before, "a short SELECT should be cached");
    }

    /// A statement big enough to be carrying its values inline is unique by
    /// construction, so caching it costs a large key and a much larger AST for
    /// a hit that never comes. A bulk load of these held ~250 MB resident.
    #[test]
    fn statements_with_inlined_values_are_not_cached() {
        let (_d, db) = open();
        let rows: Vec<String> = (0..600).map(|i| format!("({i},'v{i}')")).collect();
        let sql = format!("INSERT INTO t (a,b) VALUES {}", rows.join(","));
        assert!(
            sql.len() > STMT_CACHE_MAX_TEXT,
            "the test's statement must exceed the limit to test anything"
        );
        let before = cached(&db);
        db.execute(&sql).unwrap();
        assert_eq!(cached(&db), before, "a bulk INSERT must not be cached");
        // ...and it still ran.
        assert_eq!(
            db.row_count("t").unwrap(),
            600,
            "skipping the cache must not skip the work"
        );
    }
}

#[cfg(test)]
mod replay_fold_tests {
    //! Folding part-way through a WAL replay bounds how much of the tail is
    //! materialized at once — but a checkpoint normally records the log's last
    //! sequence as its watermark and then truncates the log. Doing that
    //! mid-replay would declare records folded that were never applied and
    //! delete them in the same breath. These tests are about not losing data.
    use super::*;

    fn opts(fold_every: usize) -> SqlOptions {
        SqlOptions {
            disk_first: true,
            // Small enough that a handful of rows crosses it.
            replay_fold_ops: fold_every,
            ..SqlOptions::default()
        }
    }

    fn rows_of(db: &SqlEngine, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop() {
            Some(QueryResult::Select { rows, .. }) => rows,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    /// Write a tail far longer than the fold threshold, then reopen. Every row
    /// must survive, and the constraints must still hold.
    #[test]
    fn every_row_survives_a_folded_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = SqlEngine::open_with_options(dir.path(), opts(usize::MAX)).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v TEXT UNIQUE)")
                .unwrap();
            db.execute("CREATE INDEX ti ON t (k)").unwrap();
            db.checkpoint().unwrap();
            // Everything from here on is WAL tail.
            for i in 1..=200 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, {}, 'v{i}')", i % 5))
                    .unwrap();
            }
            db.execute("DELETE FROM t WHERE id = 7").unwrap();
            db.execute("UPDATE t SET k = 99 WHERE id = 8").unwrap();
        }

        // Reopen with a threshold that forces many folds through that tail.
        let db = SqlEngine::open_with_options(dir.path(), opts(10)).unwrap();
        assert_eq!(
            db.row_count("t").unwrap(),
            199,
            "a replayed row went missing"
        );
        assert_eq!(
            rows_of(&db, "SELECT k FROM t WHERE id = 8"),
            vec![vec![Value::Int(99)]]
        );
        assert!(
            rows_of(&db, "SELECT id FROM t WHERE id = 7").is_empty(),
            "a replayed delete was undone"
        );
        // Indexes and constraints rebuilt from the folded state.
        assert_eq!(rows_of(&db, "SELECT id FROM t WHERE k = 1").len(), 40);
        assert!(
            db.execute("INSERT INTO t VALUES (1, 0, 'dup')").is_err(),
            "PRIMARY KEY not enforced after a folded replay"
        );
        assert!(
            db.execute("INSERT INTO t VALUES (999, 0, 'v1')").is_err(),
            "UNIQUE not enforced after a folded replay"
        );
        // The freed key is reusable.
        db.execute("INSERT INTO t VALUES (7, 2, 'again')").unwrap();
    }

    /// The dangerous case, made explicit: after a mid-replay fold the manifest
    /// watermark must name what was *applied*, and the log must still hold
    /// everything past it. Opening again — as a crash mid-replay would — must
    /// therefore find the same database, not a truncated one.
    #[test]
    fn a_crash_between_folds_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = SqlEngine::open_with_options(dir.path(), opts(usize::MAX)).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
            db.checkpoint().unwrap();
            for i in 1..=100 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
                    .unwrap();
            }
        }
        // Open and drop repeatedly. Each open folds several times part-way
        // through the tail; if a fold ever claimed more than it applied, the
        // next open would come back short.
        for round in 0..4 {
            let db = SqlEngine::open_with_options(dir.path(), opts(7)).unwrap();
            assert_eq!(
                db.row_count("t").unwrap(),
                100,
                "rows lost after {round} reopen(s)"
            );
        }
    }

    /// Resident mode has no overlay to bound, so it must not pay for folds it
    /// cannot benefit from — and must still replay correctly.
    #[test]
    fn resident_mode_replays_without_folding() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = SqlEngine::open(dir.path()).unwrap();
            db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
            for i in 1..=50 {
                db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
            }
        }
        let db = SqlEngine::open_with_options(
            dir.path(),
            SqlOptions {
                disk_first: false,
                replay_fold_ops: 5,
                ..SqlOptions::default()
            },
        )
        .unwrap();
        assert_eq!(db.row_count("t").unwrap(), 50);
    }
}

#[cfg(test)]
mod disk_index_tests {
    //! The `.sidx` base is a *hint*: it describes the rows as they were at the
    //! last checkpoint, and every candidate is verified against the live row.
    //! These pin the ways that could go wrong.
    use super::*;

    fn open(dir: &std::path::Path) -> SqlEngine {
        SqlEngine::open_with_options(
            dir,
            SqlOptions {
                disk_first: true,
                ..SqlOptions::default()
            },
        )
        .unwrap()
    }

    fn q(db: &SqlEngine, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop() {
            Some(QueryResult::Select { rows, .. }) => rows,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
        let mut v: Vec<i64> = rows
            .iter()
            .map(|r| match r[0] {
                Value::Int(n) => n,
                _ => panic!("expected int"),
            })
            .collect();
        v.sort();
        v
    }

    fn has_base(db: &SqlEngine, table: &str, index: &str) -> bool {
        db.inner.lock().unwrap().tables[table].indexes[index]
            .base
            .is_some()
    }

    /// Seed 40 rows, checkpoint so a `.sidx` exists, and reopen.
    fn seeded() -> (tempfile::TempDir, SqlEngine) {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v TEXT)")
                .unwrap();
            db.execute("CREATE INDEX ti ON t (k)").unwrap();
            for i in 1..=40 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, {}, 'v{i}')", i % 4))
                    .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let db = open(dir.path());
        assert!(
            has_base(&db, "t", "ti"),
            "the checkpoint must write a .sidx"
        );
        (dir, db)
    }

    #[test]
    fn a_checkpointed_index_is_served_from_disk() {
        let (_d, db) = seeded();
        assert_eq!(
            ids(&q(&db, "SELECT id FROM t WHERE k = 2")),
            vec![2, 6, 10, 14, 18, 22, 26, 30, 34, 38]
        );
        // Serving it must not have pulled the whole index back into RAM.
        assert!(
            db.inner.lock().unwrap().tables["t"].indexes["ti"]
                .map
                .is_empty(),
            "a lookup must not populate the in-memory map"
        );
    }

    /// A row deleted after the checkpoint is still named by the base. It must
    /// not come back — the row is gone from the store, so verification drops it.
    #[test]
    fn deleted_rows_do_not_come_back_from_the_base() {
        let (_d, db) = seeded();
        db.execute("DELETE FROM t WHERE id = 6").unwrap();
        assert_eq!(
            ids(&q(&db, "SELECT id FROM t WHERE k = 2")),
            vec![2, 10, 14, 18, 22, 26, 30, 34, 38]
        );
    }

    /// A row whose indexed column changed is named by the base under its OLD
    /// key. It must not answer for the old key, and must answer for the new one.
    #[test]
    fn updated_rows_move_keys() {
        let (_d, db) = seeded();
        db.execute("UPDATE t SET k = 3 WHERE id = 2").unwrap();
        assert!(
            !ids(&q(&db, "SELECT id FROM t WHERE k = 2")).contains(&2),
            "the old key must not still answer with the moved row"
        );
        assert!(
            ids(&q(&db, "SELECT id FROM t WHERE k = 3")).contains(&2),
            "the new key must find the moved row"
        );
    }

    /// Rows written after the checkpoint live only in the overlay.
    #[test]
    fn rows_written_after_the_checkpoint_are_found() {
        let (_d, db) = seeded();
        db.execute("INSERT INTO t VALUES (99, 2, 'new')").unwrap();
        assert!(ids(&q(&db, "SELECT id FROM t WHERE k = 2")).contains(&99));
    }

    /// The same, across a restart: the WAL tail replays into the overlay, which
    /// only works because indexes are registered before the replay.
    #[test]
    fn rows_in_the_replayed_wal_tail_are_found() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT)")
                .unwrap();
            db.execute("CREATE INDEX ti ON t (k)").unwrap();
            for i in 1..=10 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 3))
                    .unwrap();
            }
            db.checkpoint().unwrap();
            // Written after the checkpoint: in the WAL, not in the .sidx.
            db.execute("INSERT INTO t VALUES (11, 2)").unwrap();
            db.execute("DELETE FROM t WHERE id = 2").unwrap();
        }
        let db = open(dir.path());
        assert!(has_base(&db, "t", "ti"));
        let found = ids(&q(&db, "SELECT id FROM t WHERE k = 2"));
        assert!(found.contains(&11), "WAL-tail insert missing: {found:?}");
        assert!(!found.contains(&2), "WAL-tail delete ignored: {found:?}");
    }

    /// A checkpoint taken while the overlay is non-empty must fold it in, so
    /// the next base is complete on its own.
    #[test]
    fn a_second_checkpoint_folds_the_overlay() {
        let (dir, db) = seeded();
        db.execute("INSERT INTO t VALUES (99, 2, 'new')").unwrap();
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        db.checkpoint().unwrap();
        drop(db);

        let db = open(dir.path());
        let found = ids(&q(&db, "SELECT id FROM t WHERE k = 2"));
        assert!(
            found.contains(&99),
            "refolded base lost an insert: {found:?}"
        );
        assert!(
            !found.contains(&2),
            "refolded base kept a delete: {found:?}"
        );
    }
}

#[cfg(test)]
mod disk_pk_tests {
    //! The primary key served from a mapped file. Uniqueness is a *constraint*,
    //! so a stale hint here is worse than a slow lookup: accepting a duplicate
    //! corrupts the table, and rejecting a free key fails a valid write. Every
    //! way the base can go stale gets a test.
    use super::*;

    fn open(dir: &std::path::Path) -> SqlEngine {
        SqlEngine::open_with_options(
            dir,
            SqlOptions {
                disk_first: true,
                ..SqlOptions::default()
            },
        )
        .unwrap()
    }

    fn has_pk_base(db: &SqlEngine, table: &str) -> bool {
        db.inner.lock().unwrap().tables[table].pk_base.is_some()
    }

    /// 20 rows, checkpointed so a `$pk` file exists, then reopened.
    fn seeded() -> (tempfile::TempDir, SqlEngine) {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
            for i in 1..=20 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
                    .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let db = open(dir.path());
        assert!(
            has_pk_base(&db, "t"),
            "the checkpoint must write a $pk file"
        );
        (dir, db)
    }

    #[test]
    fn the_key_set_is_not_rebuilt_in_memory() {
        let (_d, db) = seeded();
        let resident = match &db.inner.lock().unwrap().tables["t"].pk_map {
            PkMap::Int(m) => m.len(),
            PkMap::Tuple(m) => m.len(),
        };
        assert_eq!(resident, 0, "opening must not rebuild the key map");
        // ...and the key still resolves.
        assert_eq!(db.pk_owner("t", &[IndexKey(Value::Int(7))]), Some(7));
    }

    /// The constraint still holds for a key that exists only in the file.
    #[test]
    fn a_duplicate_of_a_checkpointed_key_is_refused() {
        let (_d, db) = seeded();
        let err = db.execute("INSERT INTO t VALUES (7, 'dup')").unwrap_err();
        assert!(
            matches!(err, SqlError::DuplicateKey(_)),
            "expected a duplicate-key error, got {err:?}"
        );
        // The failed insert changed nothing.
        assert_eq!(db.row_count("t").unwrap(), 20);
    }

    /// A deleted row leaves its key in the base. The key must be reusable —
    /// rejecting it would fail a perfectly valid insert.
    #[test]
    fn a_deleted_keys_slot_can_be_reused() {
        let (_d, db) = seeded();
        db.execute("DELETE FROM t WHERE id = 7").unwrap();
        db.execute("INSERT INTO t VALUES (7, 'again')").unwrap();
        assert_eq!(db.row_count("t").unwrap(), 20);
        // And it is now a duplicate again.
        assert!(db.execute("INSERT INTO t VALUES (7, 'no')").is_err());
    }

    /// Moving a row's key frees the old one and claims the new one, while the
    /// base still says otherwise about both.
    #[test]
    fn a_moved_key_frees_the_old_and_claims_the_new() {
        let (_d, db) = seeded();
        db.execute("UPDATE t SET id = 100 WHERE id = 7").unwrap();
        // Old key is free.
        db.execute("INSERT INTO t VALUES (7, 'reused')").unwrap();
        // New key is taken.
        assert!(db.execute("INSERT INTO t VALUES (100, 'no')").is_err());
        assert_eq!(db.row_count("t").unwrap(), 21);
    }

    /// Rows written after the checkpoint are enforced from the overlay, and
    /// survive a restart through the WAL replay.
    #[test]
    fn keys_written_after_the_checkpoint_are_enforced_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
            db.checkpoint().unwrap();
            db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
            assert!(db.execute("INSERT INTO t VALUES (2, 'dup')").is_err());
        }
        let db = open(dir.path());
        assert!(has_pk_base(&db, "t"));
        // Both the checkpointed key and the WAL-tail key are still taken.
        assert!(db.execute("INSERT INTO t VALUES (1, 'dup')").is_err());
        assert!(db.execute("INSERT INTO t VALUES (2, 'dup')").is_err());
        db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        assert_eq!(db.row_count("t").unwrap(), 3);
    }

    /// Composite keys take the same path.
    #[test]
    fn composite_keys_work_from_the_base() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (a INT NOT NULL, b TEXT NOT NULL, v TEXT, CONSTRAINT pk PRIMARY KEY (a, b))")
                .unwrap();
            for i in 1..=10 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, 'w{}', 'v')", i % 3))
                    .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let db = open(dir.path());
        assert!(has_pk_base(&db, "t"));
        assert!(
            db.execute("INSERT INTO t VALUES (1, 'w1', 'dup')").is_err(),
            "a checkpointed composite key must still collide"
        );
        // Same first member, different second: not a collision.
        db.execute("INSERT INTO t VALUES (1, 'other', 'ok')")
            .unwrap();
        assert_eq!(db.row_count("t").unwrap(), 11);
    }

    /// `ALTER` shifts physical positions, so the base's keys stop describing
    /// the key columns. It must be dropped, not consulted.
    #[test]
    fn an_alter_that_shifts_positions_drops_the_base() {
        let (_d, db) = seeded();
        db.execute("ALTER TABLE t RENAME COLUMN v TO w").unwrap();
        // Still enforced, from whatever representation the rebuild produced.
        assert!(db.execute("INSERT INTO t VALUES (7, 'dup')").is_err());
        db.execute("INSERT INTO t VALUES (21, 'ok')").unwrap();
        assert_eq!(db.row_count("t").unwrap(), 21);
    }
}

#[cfg(test)]
mod disk_unique_tests {
    //! Column-level `UNIQUE` served from a mapped file. Same constraint risk as
    //! the primary key, plus one rule of its own: NULLs never collide, so they
    //! are not written to the file at all.
    use super::*;

    fn open(dir: &std::path::Path) -> SqlEngine {
        SqlEngine::open_with_options(
            dir,
            SqlOptions {
                disk_first: true,
                ..SqlOptions::default()
            },
        )
        .unwrap()
    }

    fn has_base(db: &SqlEngine, table: &str) -> bool {
        db.inner.lock().unwrap().tables[table]
            .uniques
            .iter()
            .all(|u| u.base.is_some())
    }

    fn resident(db: &SqlEngine, table: &str) -> usize {
        db.inner.lock().unwrap().tables[table]
            .uniques
            .iter()
            .map(|u| u.map.len())
            .sum()
    }

    fn seeded() -> (tempfile::TempDir, SqlEngine) {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE, v TEXT)")
                .unwrap();
            for i in 1..=20 {
                db.execute(&format!("INSERT INTO t VALUES ({i}, 'u{i}@x', 'v')"))
                    .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let db = open(dir.path());
        assert!(has_base(&db, "t"), "the checkpoint must write a $uq file");
        (dir, db)
    }

    #[test]
    fn the_value_set_is_not_rebuilt_in_memory() {
        let (_d, db) = seeded();
        assert_eq!(resident(&db, "t"), 0, "opening must not rebuild the map");
    }

    #[test]
    fn a_duplicate_of_a_checkpointed_value_is_refused() {
        let (_d, db) = seeded();
        let err = db
            .execute("INSERT INTO t VALUES (99, 'u7@x', 'dup')")
            .unwrap_err();
        assert!(
            matches!(err, SqlError::DuplicateKey(_)),
            "expected a duplicate-key error, got {err:?}"
        );
        assert_eq!(db.row_count("t").unwrap(), 20);
    }

    #[test]
    fn a_deleted_rows_value_can_be_reused() {
        let (_d, db) = seeded();
        db.execute("DELETE FROM t WHERE id = 7").unwrap();
        db.execute("INSERT INTO t VALUES (99, 'u7@x', 'again')")
            .unwrap();
        assert!(
            db.execute("INSERT INTO t VALUES (100, 'u7@x', 'no')")
                .is_err(),
            "the reused value must now collide"
        );
    }

    #[test]
    fn a_changed_value_frees_the_old_one() {
        let (_d, db) = seeded();
        db.execute("UPDATE t SET email = 'moved@x' WHERE id = 7")
            .unwrap();
        db.execute("INSERT INTO t VALUES (99, 'u7@x', 'reused')")
            .unwrap();
        assert!(
            db.execute("INSERT INTO t VALUES (100, 'moved@x', 'no')")
                .is_err(),
            "the new value must be taken"
        );
    }

    /// SQL exempts NULL from `UNIQUE`, so many rows may hold it. The file must
    /// not record them — writing one would make the second NULL a duplicate.
    #[test]
    fn nulls_never_collide_across_a_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
            db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
            db.checkpoint().unwrap();
        }
        let db = open(dir.path());
        db.execute("INSERT INTO t VALUES (3, NULL)").unwrap();
        assert_eq!(db.row_count("t").unwrap(), 3, "NULLs must not collide");
    }

    #[test]
    fn values_written_after_the_checkpoint_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = open(dir.path());
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'a@x')").unwrap();
            db.checkpoint().unwrap();
            db.execute("INSERT INTO t VALUES (2, 'b@x')").unwrap();
        }
        let db = open(dir.path());
        assert!(db.execute("INSERT INTO t VALUES (3, 'a@x')").is_err());
        assert!(db.execute("INSERT INTO t VALUES (4, 'b@x')").is_err());
        db.execute("INSERT INTO t VALUES (5, 'c@x')").unwrap();
    }
}

#[cfg(test)]
mod lazy_index_tests {
    use super::*;

    fn open_at(dir: &std::path::Path) -> SqlEngine {
        SqlEngine::open_with_options(
            dir,
            SqlOptions {
                disk_first: false,
                ..SqlOptions::default()
            },
        )
        .unwrap()
    }

    /// The last statement's SELECT rows.
    fn q(db: &SqlEngine, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop() {
            Some(QueryResult::Select { rows, .. }) => rows,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    fn populated(db: &SqlEngine, table: &str, index: &str) -> bool {
        db.inner.lock().unwrap().tables[table].indexes[index].populated
    }

    fn seed(dir: &std::path::Path) {
        let db = open_at(dir);
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v TEXT)")
            .unwrap();
        db.execute("CREATE INDEX ti ON t (k)").unwrap();
        for i in 1..=20 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, {}, 'v{i}')", i % 4))
                .unwrap();
        }
    }

    /// An index is not rebuilt at open, and building it on first use gives the
    /// same answer it would have given had it been maintained all along.
    #[test]
    fn an_index_is_built_on_first_use_not_at_open() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path());

        let db = open_at(dir.path());
        assert!(
            !populated(&db, "t", "ti"),
            "reopening must not rebuild the index"
        );

        // An equality lookup on the indexed column is what triggers the build.
        let rows = q(&db, "SELECT id FROM t WHERE k = 2");
        assert_eq!(rows.len(), 5, "wrong rows through the index: {rows:?}");
        assert!(populated(&db, "t", "ti"), "the lookup must have built it");
    }

    /// The dangerous case: rows written while the index was unpopulated are
    /// skipped by maintenance on purpose, so the build has to see them. If it
    /// did not, an index would silently miss every row written since open.
    #[test]
    fn rows_written_before_the_build_are_in_the_index() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path());

        let db = open_at(dir.path());
        assert!(!populated(&db, "t", "ti"));
        // Writes land while the index is still unpopulated — including a delete
        // of a row that was there at open.
        db.execute("INSERT INTO t VALUES (21, 2, 'new')").unwrap();
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        assert!(!populated(&db, "t", "ti"), "a write must not build it");

        let rows = q(&db, "SELECT id FROM t WHERE k = 2");
        assert!(populated(&db, "t", "ti"));
        let mut ids: Vec<i64> = rows
            .iter()
            .map(|r| match r[0] {
                Value::Int(n) => n,
                _ => panic!("expected int"),
            })
            .collect();
        ids.sort();
        // id=2 was deleted; id=21 was inserted; 6,10,14,18 were already there.
        assert_eq!(ids, vec![6, 10, 14, 18, 21], "the build missed a write");
    }

    /// Once populated, an index is maintained normally again.
    #[test]
    fn writes_after_the_build_are_indexed() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path());
        let db = open_at(dir.path());
        q(&db, "SELECT id FROM t WHERE k = 1");
        assert!(populated(&db, "t", "ti"));

        db.execute("INSERT INTO t VALUES (99, 1, 'after')").unwrap();
        let rows = q(&db, "SELECT id FROM t WHERE k = 1");
        assert_eq!(rows.len(), 6, "a post-build write is missing: {rows:?}");
    }

    /// `CREATE INDEX` on a live table still builds immediately — it is DDL the
    /// caller asked for, not an artefact of opening a database.
    #[test]
    fn create_index_still_builds_eagerly() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_at(dir.path());
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 7)").unwrap();
        db.execute("CREATE INDEX ti ON t (k)").unwrap();
        assert!(populated(&db, "t", "ti"));
    }
}

#[cfg(test)]
mod commit_gate_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// The gate may never claim durability for a sequence that has not been
    /// written: a flush covers what is *on disk when it starts*, nothing after.
    ///
    /// This is the invariant that catches an over-claiming watermark. Getting
    /// it wrong by one — publishing the sequence the next append *will* use
    /// rather than the last one written — makes every second write skip its
    /// fsync, and no functional test notices, because a clean shutdown flushes
    /// the OS cache anyway. Only power loss would tell, so the arithmetic is
    /// pinned here instead.
    #[test]
    fn a_flush_never_claims_a_record_that_does_not_exist_yet() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(Table::new(
            "t",
            vec![Column::new("id", SqlType::Int).primary_key()],
        ))
        .unwrap();

        for id in 1..=8 {
            db.insert("t", vec![Value::Int(id)]).unwrap();
            let last = db.inner.lock().unwrap().wal.last_seq();
            let synced = db.commit.synced_seq.load(Ordering::SeqCst);
            assert!(
                synced <= last,
                "gate claims seq {synced} durable but only {last} has been written"
            );
            // And the write that just returned really is covered.
            assert_eq!(synced, last, "an acknowledged write was left unflushed");
        }
    }

    /// A checkpoint publishes durability it actually established — again, never
    /// past the end of the log.
    #[test]
    fn a_checkpoint_publishes_exactly_what_it_captured() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(Table::new(
            "t",
            vec![Column::new("id", SqlType::Int).primary_key()],
        ))
        .unwrap();
        db.insert("t", vec![Value::Int(1)]).unwrap();
        db.checkpoint().unwrap();

        let last = db.inner.lock().unwrap().wal.last_seq();
        assert_eq!(db.commit.synced_seq.load(Ordering::SeqCst), last);
    }
}
