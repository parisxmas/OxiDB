//! B-tree page-based storage engine for OxiDB.
//!
//! Stores documents as key-value pairs (doc_id → JSONB bytes) in an `scc::HashMap`
//! for lock-free concurrent access. Methods take `&self` instead of `&mut self`
//! because `scc::HashMap` provides interior mutability with per-bucket locking.
//!
//! Page layout (on-disk persistence format):
//!   - 4 bytes: page type (0=free, 1=internal, 2=leaf)
//!   - 4 bytes: number of entries
//!   - 4 bytes: next_page (leaf only, 0 = none)
//!   - 4 bytes: prev_page (leaf only, 0 = none)
//!   - remaining: entries
//!
//! Leaf entry:
//!   [key: u64 LE][value_len: u32 LE][value bytes]
//!
//! Internal entry:
//!   [child_page: u32 LE][key: u64 LE]
//!   ... last child_page follows after last entry

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// On native targets the storage map is `scc::HashMap` (lock-free, per-bucket
// locking). On `wasm32-unknown-unknown` scc is unusable: its first bucket-array
// resize (load factor 13/16 of the 64-slot minimum, i.e. ~52 entries) hands the
// retired array to `sdd`'s epoch-based reclamation, whose reclamation path traps
// (`RuntimeError: unreachable`) under wasm — aborting every insert past ~52 docs
// per collection. wasm is single-threaded, so a plain `BTreeMap` behind a
// `Mutex` is correct, simpler, and never enters that path. See `wasm_map`.
#[cfg(not(target_arch = "wasm32"))]
use scc::HashMap as SccMap;
#[cfg(target_arch = "wasm32")]
use wasm_map::WasmMap as SccMap;

use crate::error::{Error, Result};

/// Single-threaded stand-in for `scc::HashMap` on `wasm32-unknown-unknown`.
///
/// Implements only the `*_sync` surface `BTreeStorage` uses, mirroring scc's
/// signatures so the call sites are identical across targets. Backed by a
/// `Mutex<BTreeMap>` — `Send + Sync` with no `unsafe`, and iteration is in key
/// order (the cursors sort anyway, so behaviour is unchanged).
#[cfg(target_arch = "wasm32")]
mod wasm_map {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Debug)]
    pub struct WasmMap<K, V> {
        inner: Mutex<BTreeMap<K, V>>,
    }

    impl<K: Ord + Clone, V: Clone> WasmMap<K, V> {
        pub fn new() -> Self {
            Self {
                inner: Mutex::new(BTreeMap::new()),
            }
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<K, V>> {
            // Single-threaded: the lock is uncontended and can't be poisoned in
            // practice; recover the guard if it ever is rather than panicking.
            self.inner.lock().unwrap_or_else(|e| e.into_inner())
        }

        /// Insert or replace, returning the previous value (scc `upsert_sync`).
        pub fn upsert_sync(&self, key: K, val: V) -> Option<V> {
            self.lock().insert(key, val)
        }

        /// Insert only if absent; on conflict return the rejected `(key, val)`
        /// (scc `insert_sync`).
        pub fn insert_sync(&self, key: K, val: V) -> Result<(), (K, V)> {
            let mut map = self.lock();
            if map.contains_key(&key) {
                return Err((key, val));
            }
            map.insert(key, val);
            Ok(())
        }

        /// Read a present entry through `reader` (scc `read_sync`).
        pub fn read_sync<R, F: FnOnce(&K, &V) -> R>(&self, key: &K, reader: F) -> Option<R> {
            self.lock().get_key_value(key).map(|(k, v)| reader(k, v))
        }

        /// Remove and return the entry (scc `remove_sync`).
        pub fn remove_sync(&self, key: &K) -> Option<(K, V)> {
            self.lock().remove_entry(key)
        }

        pub fn contains_sync(&self, key: &K) -> bool {
            self.lock().contains_key(key)
        }

        /// Visit every entry until `f` returns `false`; returns `false` if it
        /// stopped early, `true` if it ran to completion (scc `iter_sync`).
        pub fn iter_sync<F: FnMut(&K, &V) -> bool>(&self, mut f: F) -> bool {
            let map = self.lock();
            for (k, v) in map.iter() {
                if !f(k, v) {
                    return false;
                }
            }
            true
        }

        pub fn clear_sync(&self) {
            self.lock().clear();
        }

        pub fn len(&self) -> usize {
            self.lock().len()
        }
    }
}

/// Disk-first value store (opt-in via `OXIDB_DISK_FIRST`). Instead of holding
/// every document's bytes resident in RAM, the in-memory map keeps only a
/// compact `doc_id → DocLocation{offset,len}` index (~24 B/doc vs the full
/// ~hundreds of B/doc payload), and the document bytes live in an mmap'd
/// append-only data file read on demand. This reuses the hardened append-only
/// [`crate::storage::Storage`] backend (mmap reads, soft-delete, CRC,
/// encryption, torn-tail recovery).
///
/// Trade-off: point reads cost an mmap lookup (cheap; reclaimable under memory
/// pressure) instead of a RAM clone, and resident memory no longer scales with
/// the dataset. Updates/deletes leave dead space in the data file reclaimed by
/// compaction.
#[cfg(not(target_arch = "wasm32"))]
struct DiskBackend {
    /// doc_id → location of the live record in `data`.
    index: SccMap<u64, crate::storage::DocLocation>,
    /// The mmap'd append-only data file holding document bytes. Behind an
    /// `RwLock` so `compact()` can atomically swap in a fresh, dead-space-free
    /// file: every normal op takes the read lock spanning its index lookup +
    /// data read (so a `DocLocation` is never used against a swapped file);
    /// compaction takes the write lock, excluding all readers/writers while it
    /// rebuilds the file and remaps the index.
    data: crate::locks::RwLock<Arc<crate::storage::Storage>>,
}

// ── File header (Phase 1b of ADR-0003 / docs/format/btree.md) ──
//
// Layout (8 bytes, little-endian) sitting at the start of the plaintext
// (i.e. inside the encrypted blob when encryption is on):
//   [b"OXBT" (4)][version u16][flags u16]
//
// Files written by versions predating the header are accepted as
// legacy: detected by the absence of `OXBT` magic at the start of the
// (post-decrypt) plaintext. The next `persist()` rewrites them with a
// v1 header automatically — every persist serializes the whole tree in
// full, so there's no migration window beyond "first persist after
// upgrade". Recognising a version we don't know is fatal — `open()`
// surfaces it as `Error::Io(InvalidData)` rather than silently loading
// from an empty state and letting WAL replay paper over a data loss.
const BTREE_MAGIC: &[u8; 4] = b"OXBT";
const BTREE_VERSION: u16 = 1;
const BTREE_HEADER_SIZE: usize = 8;

/// Concurrent B-tree storage using an `scc::HashMap<u64, Vec<u8>>`.
///
/// All mutating methods take `&self` — `scc::HashMap` handles internal locking
/// at bucket granularity, so multiple threads can insert/remove concurrently
/// without an external `RwLock`.
///
/// Advantages over the append-only StorageBackend adapter:
/// - O(1) amortized point lookups by doc_id (hash map)
/// - Sequential cursor scan via sorted snapshot (no per-doc pread)
/// - No DocLocation indirection — doc_id IS the key
/// - No soft-delete / compaction needed — updates are in-place
/// - Lock-free concurrent reads and writes via scc bucket-level locking
pub struct BTreeStorage {
    /// The concurrent map: doc_id → JSONB-encoded document bytes.
    tree: SccMap<u64, Vec<u8>>,
    /// Total bytes stored (for stats). Updated atomically.
    total_bytes: AtomicU64,
    /// Data directory for persistence (empty for in-memory).
    data_dir: PathBuf,
    /// Collection name (for file naming).
    name: String,
    /// Optional encryption key for at-rest encryption of the .btree file.
    encryption: Option<Arc<crate::EncryptionKey>>,
    /// Serializes concurrent persist() calls. Without this, two
    /// concurrent commits both write to the same `{name}.btree.tmp`
    /// file, mangle each other's bytes, and the surviving rename
    /// produces a corrupt on-disk image — the source of the
    /// "io error: No such file or directory" / truncated-file errors
    /// observed under load. Mutex is per-collection so writes to
    /// different collections still proceed in parallel.
    persist_mu: crate::locks::Mutex<()>,
    /// When `Some`, this collection runs in disk-first mode: `tree` is unused
    /// and document bytes live in `disk.data`, with only the offset index
    /// resident. `None` = the default in-RAM mode (values live in `tree`).
    #[cfg(not(target_arch = "wasm32"))]
    disk: Option<DiskBackend>,
    /// MVCC-lite (ADR-0017): the engine's snapshot gate, attached once after
    /// open (never during recovery replay). Every logical mutation reports
    /// the displaced value here; free when no snapshot is open.
    #[cfg(not(target_arch = "wasm32"))]
    snap_gate: std::sync::OnceLock<Arc<crate::snapshot::SnapGate>>,
    /// Resolved per-collection storage options (compression + compaction
    /// policy). `disk_first` here mirrors `disk.is_some()`.
    opts: StorageOptions,
}

/// Whether disk-first storage is enabled. **ON by default** since 0.38:
/// resident memory should not scale with the dataset out of the box. Set
/// `OXIDB_DISK_FIRST=0` for the old always-resident mode (fastest point
/// reads, RAM proportional to data). Read once. Existing collections keep
/// whichever format they were created with — the on-disk format (and
/// `.bopts`) is authoritative over this flag, so flipping it never
/// reinterprets existing data.
#[cfg(not(target_arch = "wasm32"))]
fn disk_first_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("OXIDB_DISK_FIRST")
            .map(|v| v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
            .unwrap_or(false)
    })
}

/// Whether disk-first stores write the `.bdat` **uncompressed** (env
/// `OXIDB_DISK_UNCOMPRESSED` truthy). Read once. Off by default. Trades disk
/// space for CPU: new records skip the per-record compress on write and the
/// decompress on read, and (when unencrypted) become zero-copy mmap reads —
/// faster bulk insert and faster full-collection scans/aggregations. Only
/// meaningful with `OXIDB_DISK_FIRST`. Reads stay adaptive (records decoded by
/// magic bytes), so toggling it needs no migration; compaction rewrites records
/// in whichever mode is active.
#[cfg(not(target_arch = "wasm32"))]
fn disk_compress_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("OXIDB_DISK_UNCOMPRESSED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
            .unwrap_or(false)
    })
}

/// Auto-compaction tuning (read once). The periodic sync path calls
/// [`BTreeStorage::should_compact`] and compacts when the disk-first data file
/// is at least `OXIDB_COMPACT_MIN_BYTES` (default 4 MiB) AND at least
/// `OXIDB_COMPACT_DEAD_RATIO` (default 0.5) dead. Disable with
/// `OXIDB_AUTO_COMPACT=0`.
#[cfg(not(target_arch = "wasm32"))]
fn auto_compact_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("OXIDB_AUTO_COMPACT")
            .map(|v| {
                !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
            })
            .unwrap_or(true)
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn compact_min_bytes() -> u64 {
    use std::sync::OnceLock;
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("OXIDB_COMPACT_MIN_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4 * 1024 * 1024)
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn compact_dead_ratio() -> f64 {
    use std::sync::OnceLock;
    static V: OnceLock<f64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("OXIDB_COMPACT_DEAD_RATIO")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|r: &f64| *r > 0.0 && *r < 1.0)
            .unwrap_or(0.5)
    })
}

/// Per-collection storage options.
///
/// These were previously process-wide `OXIDB_*` env vars; they are now
/// per-collection. A collection created via `create_collection_with_options`
/// carries its own settings; collections created implicitly (or without
/// options) fall back to [`StorageOptions::from_env`], so the env-var workflow
/// and all existing behavior are preserved as the *default*.
///
/// For a disk-first collection the resolved options are persisted next to its
/// data as `<name>.bopts`, so a reopen uses the same settings regardless of the
/// current environment — flipping `OXIDB_DISK_FIRST` between runs can no longer
/// mismatch an existing collection's on-disk format.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct StorageOptions {
    /// Disk-first mode: only the offset index is resident and document bytes
    /// live in an mmap'd `.bdat`. `false` = the default in-RAM `.btree` store.
    pub disk_first: bool,
    /// Compress `.bdat` records with zstd. Ignored in in-RAM mode. Disable to
    /// trade disk for CPU on scan/insert-heavy workloads (reads stay adaptive).
    pub compress: bool,
    /// Run compaction automatically from the periodic sync path.
    pub auto_compact: bool,
    /// Compaction floor: never compact a `.bdat` smaller than this.
    pub compact_min_bytes: u64,
    /// Compaction trigger: dead-space fraction (0..1) at/above which to compact.
    pub compact_dead_ratio: f64,
}

impl Default for StorageOptions {
    /// Disk-first, compressed, auto-compaction on with the standard
    /// thresholds — matches the engine's defaults when no env vars are set.
    /// (In-memory-mode and wasm collections go through `new_in_memory`,
    /// which does not consult these options.)
    fn default() -> Self {
        Self {
            disk_first: true,
            compress: true,
            auto_compact: true,
            compact_min_bytes: 4 * 1024 * 1024,
            compact_dead_ratio: 0.5,
        }
    }
}

impl StorageOptions {
    /// Options sourced from the `OXIDB_*` env vars — the default for collections
    /// created without explicit options.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_env() -> Self {
        Self {
            disk_first: disk_first_enabled(),
            compress: disk_compress_enabled(),
            auto_compact: auto_compact_enabled(),
            compact_min_bytes: compact_min_bytes(),
            compact_dead_ratio: compact_dead_ratio(),
        }
    }

    /// A disk-first preset starting from the env defaults.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn disk_first() -> Self {
        Self {
            disk_first: true,
            ..Self::from_env()
        }
    }
}

/// A cursor for iterating through the storage in ascending key order.
/// Wraps a collected snapshot of entries for stable iteration.
pub struct Cursor {
    entries: Vec<(u64, Vec<u8>)>,
    pos: usize,
}

impl Cursor {
    /// Create a cursor positioned at the first entry (sorted by key).
    pub fn seek_first(tree: &SccMap<u64, Vec<u8>>) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();
        tree.iter_sync(|k, v| {
            entries.push((*k, v.clone()));
            true
        });
        entries.sort_unstable_by_key(|(k, _)| *k);
        Ok(Self { entries, pos: 0 })
    }

    /// Create a cursor positioned at a specific key (or the next key >= it).
    pub fn seek(tree: &SccMap<u64, Vec<u8>>, key: u64) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();
        tree.iter_sync(|k, v| {
            if *k >= key {
                entries.push((*k, v.clone()));
            }
            true
        });
        entries.sort_unstable_by_key(|(k, _)| *k);
        Ok(Self { entries, pos: 0 })
    }

    /// Advance to the next entry. Returns None when exhausted.
    pub fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        if self.pos >= self.entries.len() {
            return Ok(None);
        }
        let entry = self.entries[self.pos].clone();
        self.pos += 1;
        Ok(Some(entry))
    }

    /// Peek at the current entry without advancing.
    pub fn peek(&self) -> Option<&(u64, Vec<u8>)> {
        self.entries.get(self.pos)
    }

    /// Number of remaining entries.
    pub fn remaining(&self) -> usize {
        self.entries.len().saturating_sub(self.pos)
    }
}

/// A reverse cursor for iterating through the storage in descending key order.
pub struct ReverseCursor {
    entries: Vec<(u64, Vec<u8>)>,
    pos: usize,
}

impl ReverseCursor {
    /// Create a cursor positioned at the last entry (descending key order).
    pub fn seek_last(tree: &SccMap<u64, Vec<u8>>) -> Result<Self> {
        let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();
        tree.iter_sync(|k, v| {
            entries.push((*k, v.clone()));
            true
        });
        entries.sort_unstable_by(|(a, _), (b, _)| b.cmp(a)); // descending
        Ok(Self { entries, pos: 0 })
    }

    /// Advance to the previous (next lower) entry.
    pub fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        if self.pos >= self.entries.len() {
            return Ok(None);
        }
        let entry = self.entries[self.pos].clone();
        self.pos += 1;
        Ok(Some(entry))
    }
}

impl BTreeStorage {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(name: &str, data_dir: &Path, encryption: Option<Arc<crate::EncryptionKey>>) -> Self {
        Self {
            tree: SccMap::new(),
            total_bytes: AtomicU64::new(0),
            data_dir: data_dir.to_path_buf(),
            name: name.to_string(),
            encryption,
            persist_mu: crate::locks::Mutex::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            disk: None,
            #[cfg(not(target_arch = "wasm32"))]
            snap_gate: std::sync::OnceLock::new(),
            opts: StorageOptions {
                disk_first: false,
                ..StorageOptions::default()
            },
        }
    }

    /// Create a new in-memory B-tree storage (no persistence). Always in-RAM
    /// mode — disk-first requires a data directory.
    pub fn new_in_memory(name: &str) -> Self {
        Self {
            tree: SccMap::new(),
            total_bytes: AtomicU64::new(0),
            data_dir: PathBuf::new(),
            name: name.to_string(),
            encryption: None,
            persist_mu: crate::locks::Mutex::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            disk: None,
            #[cfg(not(target_arch = "wasm32"))]
            snap_gate: std::sync::OnceLock::new(),
            opts: StorageOptions {
                disk_first: false,
                ..StorageOptions::default()
            },
        }
    }

    /// Open or load a B-tree from disk. Falls back to empty if no file
    /// exists. Tolerates truncation: if the on-disk image got partially
    /// written before we landed `persist()`'s atomic-rename fix (or in
    /// any future bug), load every fully-decoded record and let the WAL
    /// replay catch up with the rest. Without this fallback the engine
    /// refuses to boot — bricked database for one bad shutdown.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(
        name: &str,
        data_dir: &Path,
        encryption: Option<Arc<crate::EncryptionKey>>,
    ) -> Result<Self> {
        let opts = Self::resolve_opts(name, data_dir);
        Self::open_with_options(name, data_dir, encryption, opts)
    }

    /// Resolve the storage options for opening `name`. Precedence:
    /// 1. A persisted `<name>.bopts` (written when a disk-first collection was
    ///    created) — authoritative across reopens regardless of the env.
    /// 2. Otherwise the env defaults ([`StorageOptions::from_env`]), with
    ///    `disk_first` overridden by **detecting the on-disk format**: a
    ///    `.bdat` forces disk-first, a `.btree` forces in-RAM. This keeps an
    ///    existing collection readable even if `OXIDB_DISK_FIRST` flipped
    ///    between runs.
    #[cfg(not(target_arch = "wasm32"))]
    fn resolve_opts(name: &str, data_dir: &Path) -> StorageOptions {
        let bopts_path = data_dir.join(format!("{}.bopts", name));
        if let Ok(bytes) = std::fs::read(&bopts_path) {
            if let Ok(o) = serde_json::from_slice::<StorageOptions>(&bytes) {
                return o;
            }
        }
        let mut o = StorageOptions::from_env();
        if data_dir.join(format!("{}.bdat", name)).exists() {
            o.disk_first = true;
        } else if data_dir.join(format!("{}.btree", name)).exists() {
            o.disk_first = false;
        }
        o
    }

    /// Open or load a B-tree with explicit per-collection [`StorageOptions`].
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_options(
        name: &str,
        data_dir: &Path,
        encryption: Option<Arc<crate::EncryptionKey>>,
        opts: StorageOptions,
    ) -> Result<Self> {
        if opts.disk_first {
            return Self::open_disk(name, data_dir, encryption, opts);
        }
        let path = data_dir.join(format!("{}.btree", name));
        let tmp = data_dir.join(format!("{}.btree.tmp", name));
        let mut storage = Self::new(name, data_dir, encryption);
        storage.opts = opts;

        // Stale tmp from a crash mid-persist: drop it. The real file
        // (if present) is either intact (rename completed first) or
        // partially the old version (rename never ran) — both are
        // valid starting points; the leftover tmp is just garbage.
        if tmp.exists() {
            let _ = std::fs::remove_file(&tmp);
        }

        if path.exists() {
            let mut data = std::fs::read(&path)?;
            if let Some(ref key) = storage.encryption {
                // Decryption failure on the whole file is fatal — we
                // can't partially-decrypt to recover. WAL replay alone
                // (without the BTree base) would still rebuild state
                // for whatever WAL has retained, so we soldier on.
                match key.decrypt(&data) {
                    Ok(d) => data = d,
                    Err(e) => {
                        eprintln!(
                            "[btree] {}.btree decryption failed: {} — \
                             starting from empty store, WAL replay will rebuild",
                            name, e
                        );
                        return Ok(storage);
                    }
                }
            }

            // Phase 1b version check. If the plaintext starts with `OXBT`,
            // validate the version and strip the header before parsing;
            // otherwise it's a legacy (pre-Phase-1b) file and we parse
            // records from offset 0. An unknown version is fatal — we must
            // not silently load from empty and let WAL replay claim it
            // reconciled an image we never read.
            let records: &[u8] = if data.len() >= BTREE_HEADER_SIZE && &data[0..4] == BTREE_MAGIC {
                let version = u16::from_le_bytes([data[4], data[5]]);
                if version != BTREE_VERSION {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "unsupported .btree format version {} at {}; \
                                 this binary understands version {}",
                            version,
                            path.display(),
                            BTREE_VERSION
                        ),
                    )));
                }
                // data[6..8] = flags; reserved at 0 — ignored until a
                // bit is documented + assigned.
                &data[BTREE_HEADER_SIZE..]
            } else {
                &data[..]
            };

            if let Err(e) = storage.load_from_bytes(records) {
                eprintln!(
                    "[btree] {}.btree partially loaded ({} records, {} bytes total) \
                     before error: {} — WAL replay will reconcile",
                    name,
                    storage.tree.len(),
                    data.len(),
                    e
                );
                // Fall through with whatever was loaded — load_from_bytes
                // upserts records as it parses them, so any prefix that
                // decoded cleanly is already in `tree`.
            }
        }

        Ok(storage)
    }

    /// Open in disk-first mode: open the append-only data file and rebuild the
    /// in-memory `doc_id → DocLocation` index by scanning its live records
    /// (each document carries its `_id`). Document bytes stay on disk.
    #[cfg(not(target_arch = "wasm32"))]
    fn open_disk(
        name: &str,
        data_dir: &Path,
        encryption: Option<Arc<crate::EncryptionKey>>,
        opts: StorageOptions,
    ) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        // Persist the resolved options next to the data so a reopen is
        // consistent regardless of the env. Written once (don't clobber an
        // existing file — that's the authoritative record for reopens).
        let bopts_path = data_dir.join(format!("{}.bopts", name));
        if !bopts_path.exists() {
            if let Ok(json) = serde_json::to_vec_pretty(&opts) {
                let _ = std::fs::write(&bopts_path, json);
            }
        }
        // `.bdat` (BTree disk-first data), deliberately NOT `.dat` — the engine
        // treats bare `.dat` as the legacy append-only `Collection` format and
        // refuses to open it.
        let data_path = data_dir.join(format!("{}.bdat", name));
        let data = crate::storage::Storage::open_with_options(
            &data_path,
            encryption.clone(),
            opts.compress,
        )?;

        let index: SccMap<u64, crate::storage::DocLocation> = SccMap::new();
        let mut total: u64 = 0;
        // Rebuild the index from the data file's live records. File order is
        // append order, so for a doc updated in place (old record soft-deleted,
        // new appended) only the live record is visited; if any duplicate live
        // record slipped through a crash, the later (higher-offset) append wins.
        data.for_each_active(|loc, bytes| {
            if let Ok(doc) = crate::codec::decode_doc(&bytes) {
                if let Some(id) = doc.get("_id").and_then(|v| v.as_u64()) {
                    let _ = index.upsert_sync(id, loc);
                    total += loc.length as u64;
                }
            }
            Ok(())
        })?;

        Ok(Self {
            tree: SccMap::new(), // unused in disk-first mode
            total_bytes: AtomicU64::new(total),
            data_dir: data_dir.to_path_buf(),
            name: name.to_string(),
            encryption,
            persist_mu: crate::locks::Mutex::new(()),
            disk: Some(DiskBackend {
                index,
                data: crate::locks::RwLock::new(Arc::new(data)),
            }),
            snap_gate: std::sync::OnceLock::new(),
            opts,
        })
    }

    /// Deserialize the B-tree from a binary dump (after any decryption
    /// and after `open()` has stripped the OXBT file header, if present).
    /// Body format: repeated `[key: u64 LE][len: u32 LE][value bytes]`.
    /// See [`docs/format/btree.md`](../../docs/format/btree.md) for the full
    /// on-disk layout including the v1 file header.
    fn load_from_bytes(&self, data: &[u8]) -> Result<()> {
        let mut pos = 0;
        let mut total = 0u64;
        while pos + 12 <= data.len() {
            let key = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + len > data.len() {
                return Err(Error::Codec("truncated btree file".into()));
            }
            let value = data[pos..pos + len].to_vec();
            total += len as u64;
            // upsert_sync behaves like DashMap::insert — inserts or replaces
            let _ = self.tree.upsert_sync(key, value);
            pos += len;
        }
        self.total_bytes.store(total, Ordering::Release);
        Ok(())
    }

    /// Persist the B-tree to disk atomically.
    ///
    /// Durability protocol: write to a sibling tmp path, fsync the file
    /// data, atomically rename onto the target, then fsync the parent
    /// directory so the rename itself is durable across power loss.
    ///
    /// Why each step matters:
    ///   - tmp + rename: a process crash mid-write leaves the original
    ///     `{name}.btree` untouched. Without this, a partial overwrite
    ///     produces the "truncated btree file" startup error we hit.
    ///   - fsync(tmp): ensures the tmp file's bytes hit disk before we
    ///     rename. Skipping it lets rename succeed while data is still
    ///     in OS page cache; a power loss between rename and natural
    ///     flush re-creates the truncation problem on a "valid-named"
    ///     file.
    ///   - fsync(dir): the rename itself is a directory metadata
    ///     change. POSIX requires fsync on the directory to make the
    ///     entry durable.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn persist(&self) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            // Document bytes are already appended to the data file; the index
            // is rebuilt from it on open, so there's no separate snapshot to
            // write — just fsync the data file.
            let _guard = self.persist_mu.lock();
            return d.data.read().sync();
        }
        if self.data_dir.as_os_str().is_empty() {
            return Ok(()); // in-memory mode
        }
        // Serialize concurrent persists for THIS collection. Two
        // concurrent commits that both reach checkpoint_wal would
        // otherwise both open `{name}.btree.tmp` with truncate(true),
        // mangle each other's bytes, and the surviving rename leaves
        // the on-disk image either truncated or unrelated to either
        // committer's view of state. We hold the lock for the entire
        // write+rename+fsync sequence so the on-disk image is always
        // a snapshot of the tree at some single point in time.
        //
        // This serializes commits that target the same collection
        // but doesn't block commits on other collections (each has
        // its own BTreeStorage with its own mutex).
        let _persist_guard = self.persist_mu.lock();

        let path = self.data_dir.join(format!("{}.btree", self.name));
        let tmp = self.data_dir.join(format!("{}.btree.tmp", self.name));

        // Phase 1b: plaintext = [OXBT header][record stream]. The header
        // sits INSIDE the encrypted blob when encryption is on, so the
        // reader detects + validates it after decryption.
        let body_cap = self.total_bytes.load(Ordering::Acquire) as usize + self.tree.len() * 12;
        let mut buf = Vec::with_capacity(BTREE_HEADER_SIZE + body_cap);
        buf.extend_from_slice(BTREE_MAGIC);
        buf.extend_from_slice(&BTREE_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags reserved
        self.tree.iter_sync(|key, value| {
            buf.extend_from_slice(&key.to_le_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
            true
        });
        let write_buf = if let Some(ref key) = self.encryption {
            key.encrypt(&buf)?
        } else {
            buf
        };

        // 1) Write + fsync the tmp file.
        {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            std::io::Write::write_all(&mut f, &write_buf)?;
            f.sync_data()?;
        }

        // 2) Atomic rename. POSIX guarantees this is observed by other
        // processes as either the old file or the new one — never a
        // mix.
        std::fs::rename(&tmp, &path)?;

        // 3) Fsync the parent directory so the rename hits disk too.
        // On macOS APFS this is a no-op as far as durability goes (FS
        // crash-consistency is stronger), but it costs nothing and is
        // required on ext4 / xfs / etc.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_data();
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Insert a key-value pair. Returns the previous value if the key existed.
    ///
    /// Uses `scc::HashMap::upsert_sync` which returns the old value on replace
    /// (same semantics as the former `DashMap::insert`).
    ///
    /// Uses atomic `fetch_add` / `fetch_sub` for `total_bytes` to avoid
    /// load+store races under concurrent inserts.
    pub fn insert(&self, key: u64, value: Vec<u8>) -> Option<Vec<u8>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            // Read guard: shared with other readers/writers, excluded only by
            // compaction's write lock. `_no_sync`: durability for this write
            // comes from the WAL (fsynced per commit). The data file is flushed
            // in `persist()` at checkpoint — fsyncing every single append would
            // serialize writes on the disk and is what the in-RAM path (a plain
            // RAM store) doesn't pay either.
            let data = d.data.read();
            // MVCC-lite: with a snapshot open, the displaced value must be
            // remembered BEFORE its location is retired — read it out of the
            // mmap now (compaction may purge the bytes later). One index
            // probe + one read, paid only while a snapshot is active.
            let prior_for_gate = self.recording_gate().map(|g| {
                let prior = d
                    .index
                    .read_sync(&key, |_, loc| *loc)
                    .and_then(|loc| data.read_lockfree(loc).ok());
                (Arc::clone(g), prior)
            });
            match data.append_no_sync(&value) {
                Ok(new_loc) => {
                    let old_loc = d.index.upsert_sync(key, new_loc);
                    if let Some((gate, prior)) = prior_for_gate {
                        gate.record(&self.name, key, prior.as_deref());
                    }
                    if let Some(old) = old_loc {
                        let _ = data.mark_deleted_no_sync(old);
                        self.total_bytes
                            .fetch_sub(old.length as u64, Ordering::AcqRel);
                    }
                    // Account in on-disk (compressed) units — `open_disk` and
                    // `compact` rebuild this counter from `loc.length`, and
                    // `should_compact` compares it against the physical file
                    // size. Mixing in the uncompressed `value.len()` here
                    // overstated live bytes and could pin the dead ratio at
                    // ≤ 0, permanently suppressing auto-compaction.
                    self.total_bytes
                        .fetch_add(new_loc.length as u64, Ordering::AcqRel);
                }
                Err(e) => {
                    // The WAL still holds this write; recovery will reconcile.
                    eprintln!(
                        "[btree-disk] {} append failed: {} — WAL replay will reconcile",
                        self.name, e
                    );
                }
            }
            return None; // displaced value is not read back in disk mode
        }
        let new_len = value.len() as u64;
        let old = self.tree.upsert_sync(key, value);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(g) = self.recording_gate() {
            g.record(&self.name, key, old.as_deref());
        }
        if let Some(ref old_val) = old {
            // Replace: subtract old size, add new size (two atomic ops, no race).
            let old_len = old_val.len() as u64;
            self.total_bytes.fetch_sub(old_len, Ordering::AcqRel);
            self.total_bytes.fetch_add(new_len, Ordering::AcqRel);
        } else {
            self.total_bytes.fetch_add(new_len, Ordering::AcqRel);
        }
        old
    }

    /// Get a value by key. Returns a cloned `Vec<u8>` because `scc::HashMap`
    /// entry guards cannot outlive the bucket lock scope.
    pub fn get(&self, key: u64) -> Option<Vec<u8>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            let data = d.data.read();
            let loc = d.index.read_sync(&key, |_, loc| *loc)?;
            return data.read_lockfree(loc).ok();
        }
        self.tree.read_sync(&key, |_, v| v.clone())
    }

    /// Remove a key-value pair. Returns the removed value.
    pub fn remove(&self, key: u64) -> Option<Vec<u8>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            let data = d.data.read();
            if let Some((_, old)) = d.index.remove_sync(&key) {
                // MVCC-lite: remember the deleted value while a snapshot is
                // open — a snapshot older than this delete must still see it.
                if let Some(g) = self.recording_gate() {
                    let prior = data.read_lockfree(old).ok();
                    g.record(&self.name, key, prior.as_deref());
                }
                let _ = data.mark_deleted_no_sync(old);
                self.total_bytes
                    .fetch_sub(old.length as u64, Ordering::AcqRel);
            }
            return None;
        }
        let old = self.tree.remove_sync(&key).map(|(_, v)| v);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(g) = self.recording_gate() {
            if old.is_some() {
                g.record(&self.name, key, old.as_deref());
            }
        }
        if let Some(ref val) = old {
            let len = val.len() as u64;
            self.total_bytes.fetch_sub(len, Ordering::AcqRel);
        }
        old
    }

    /// Attach the engine's snapshot gate (ADR-0017). Called once, after
    /// open — never during recovery replay, so replayed records are not
    /// mistaken for live changes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_snap_gate(&self, gate: Arc<crate::snapshot::SnapGate>) {
        let _ = self.snap_gate.set(gate);
    }

    /// The gate, only when at least one snapshot is open — the single
    /// relaxed load that keeps the no-snapshot write path free.
    #[cfg(not(target_arch = "wasm32"))]
    #[inline]
    fn recording_gate(&self) -> Option<&Arc<crate::snapshot::SnapGate>> {
        self.snap_gate.get().filter(|g| g.is_active())
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: u64) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            return d.index.contains_sync(&key);
        }
        self.tree.contains_sync(&key)
    }

    /// Number of entries in the storage.
    pub fn count(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            return d.index.len();
        }
        self.tree.len()
    }

    /// True when this storage is running in disk-first mode.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn is_disk_first(&self) -> bool {
        self.disk.is_some()
    }

    /// Reclaim dead space left by updates/deletes. Returns
    /// `(old_file_bytes, new_file_bytes)`.
    ///
    /// In-RAM mode has no dead space (updates are in place) — this just
    /// persists the snapshot. Disk-first mode rewrites the append-only data
    /// file keeping only the live records (those still referenced by the
    /// index), then atomically swaps it in and remaps the index. The write
    /// lock makes this exclusive w.r.t. all readers/writers, so a
    /// `DocLocation` is never used against the swapped file.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn compact(&self) -> Result<(u64, u64)> {
        let d = match &self.disk {
            Some(d) => d,
            None => {
                self.persist()?;
                let sz = self.total_bytes();
                return Ok((sz, sz));
            }
        };

        let mut data_guard = d.data.write(); // exclusive: no concurrent ops
        let old_size = data_guard.file_size();

        // Snapshot the live (doc_id, location) set.
        let mut entries: Vec<(u64, crate::storage::DocLocation)> = Vec::new();
        d.index.iter_sync(|k, loc| {
            entries.push((*k, *loc));
            true
        });

        // Copy live records into a fresh data file.
        let tmp = self.data_dir.join(format!("{}.bdat.compact", self.name));
        let _ = std::fs::remove_file(&tmp);
        let fresh = crate::storage::Storage::open_with_options(
            &tmp,
            self.encryption.clone(),
            self.opts.compress,
        )?;
        let mut new_entries: Vec<(u64, crate::storage::DocLocation)> =
            Vec::with_capacity(entries.len());
        let mut total = 0u64;
        for (id, old_loc) in &entries {
            // Compaction must be all-or-nothing: a read error here is a LIVE
            // document we cannot copy. Silently skipping it would make the
            // swap below destroy that document permanently — and this runs
            // unattended from the auto-compaction path. Abort and leave the
            // original file untouched instead.
            let bytes = match data_guard.read_lockfree(*old_loc) {
                Ok(b) => b,
                Err(e) => {
                    drop(fresh);
                    let _ = std::fs::remove_file(&tmp);
                    return Err(e);
                }
            };
            let new_loc = fresh.append_no_sync(&bytes)?;
            new_entries.push((*id, new_loc));
            total += new_loc.length as u64;
        }
        fresh.sync()?;
        let new_size = fresh.file_size();

        // Atomic swap: replace the live file, reopen, swap the handle, remap.
        let live = self.data_dir.join(format!("{}.bdat", self.name));
        std::fs::rename(&tmp, &live)?;
        // Make the rename durable: without the parent-dir fsync a power loss
        // can revive the pre-compaction file after the in-memory index has
        // been rebuilt for the new one (same protocol as `persist()`).
        if let Ok(dirf) = std::fs::File::open(&self.data_dir) {
            let _ = dirf.sync_all();
        }
        let reopened = crate::storage::Storage::open_with_options(
            &live,
            self.encryption.clone(),
            self.opts.compress,
        )?;
        *data_guard = Arc::new(reopened); // old Storage drops → old fd/mmap freed
        d.index.clear_sync();
        for (id, loc) in new_entries {
            let _ = d.index.upsert_sync(id, loc);
        }
        self.total_bytes.store(total, Ordering::Release);
        Ok((old_size, new_size))
    }

    /// Whether the disk-first data file has accumulated enough dead space to be
    /// worth compacting. Cheap to call (one file-size read + one atomic load);
    /// the periodic sync path checks it and compacts when true. Always `false`
    /// in in-RAM mode or when auto-compaction is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn should_compact(&self) -> bool {
        if !self.opts.auto_compact {
            return false;
        }
        let d = match &self.disk {
            Some(d) => d,
            None => return false,
        };
        let file = d.data.read().file_size();
        if file < self.opts.compact_min_bytes {
            return false;
        }
        // Fraction of the file that is dead (overwritten/deleted records plus
        // framing). `total_bytes` tracks the live payload.
        let live = self.total_bytes();
        let dead_ratio = 1.0 - (live as f64 / file as f64);
        dead_ratio >= self.opts.compact_dead_ratio
    }
    #[cfg(target_arch = "wasm32")]
    pub fn is_disk_first(&self) -> bool {
        false
    }

    /// Total bytes stored.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }

    // -----------------------------------------------------------------------
    // Cursor-based iteration
    // -----------------------------------------------------------------------

    /// Collect cursor entries from disk: sorted (key, value) pairs, reading
    /// each value from the data file. Like the in-RAM cursor, this materializes
    /// the snapshot (a transient per-query cost); the steady-state memory win
    /// is that the resident index holds only locations.
    #[cfg(not(target_arch = "wasm32"))]
    fn disk_cursor_entries(
        &self,
        d: &DiskBackend,
        min_key: Option<u64>,
        descending: bool,
    ) -> Vec<(u64, Vec<u8>)> {
        let mut items: Vec<(u64, crate::storage::DocLocation)> = Vec::new();
        d.index.iter_sync(|k, loc| {
            if min_key.is_none_or(|m| *k >= m) {
                items.push((*k, *loc));
            }
            true
        });
        if descending {
            items.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        } else {
            items.sort_unstable_by_key(|(k, _)| *k);
        }
        let data = d.data.read();
        items
            .into_iter()
            .filter_map(|(k, loc)| data.read_lockfree(loc).ok().map(|v| (k, v)))
            .collect()
    }

    /// Create a forward cursor starting at the first entry.
    pub fn cursor_first(&self) -> Result<Cursor> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            return Ok(Cursor {
                entries: self.disk_cursor_entries(d, None, false),
                pos: 0,
            });
        }
        Cursor::seek_first(&self.tree)
    }

    /// Create a forward cursor starting at the given key.
    pub fn cursor_seek(&self, key: u64) -> Result<Cursor> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            return Ok(Cursor {
                entries: self.disk_cursor_entries(d, Some(key), false),
                pos: 0,
            });
        }
        Cursor::seek(&self.tree, key)
    }

    /// Create a reverse cursor starting at the last entry.
    pub fn cursor_last(&self) -> Result<ReverseCursor> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            return Ok(ReverseCursor {
                entries: self.disk_cursor_entries(d, None, true),
                pos: 0,
            });
        }
        ReverseCursor::seek_last(&self.tree)
    }

    /// Iterate all entries calling `f` for each (sorted by key).
    /// Stops early if `f` returns false.
    /// Collects keys first, then looks up each value, so concurrent
    /// mutations between collect and lookup are safe (missing keys skipped).
    pub fn scan_all_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, &[u8]) -> Result<bool>,
    {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            let data = d.data.read();
            let mut items: Vec<(u64, crate::storage::DocLocation)> = Vec::new();
            d.index.iter_sync(|k, loc| {
                items.push((*k, *loc));
                true
            });
            items.sort_unstable_by_key(|(k, _)| *k);
            for (key, loc) in items {
                if let Ok(value) = data.read_lockfree(loc) {
                    if !f(key, &value)? {
                        break;
                    }
                }
            }
            return Ok(());
        }
        let mut keys: Vec<u64> = Vec::new();
        self.tree.iter_sync(|k, _| {
            keys.push(*k);
            true
        });
        keys.sort_unstable();
        for key in keys {
            if let Some(value) = self.tree.read_sync(&key, |_, v| v.clone()) {
                if !f(key, &value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Iterate all entries calling `f` for each (bytes only, sorted by key).
    /// Collects keys first, then looks up each value.
    pub fn scan_bytes_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            let data = d.data.read();
            let mut items: Vec<(u64, crate::storage::DocLocation)> = Vec::new();
            d.index.iter_sync(|k, loc| {
                items.push((*k, *loc));
                true
            });
            items.sort_unstable_by_key(|(k, _)| *k);
            for (_key, loc) in items {
                if let Ok(value) = data.read_lockfree(loc) {
                    if !f(&value)? {
                        break;
                    }
                }
            }
            return Ok(());
        }
        let mut keys: Vec<u64> = Vec::new();
        self.tree.iter_sync(|k, _| {
            keys.push(*k);
            true
        });
        keys.sort_unstable();
        for key in keys {
            if let Some(value) = self.tree.read_sync(&key, |_, v| v.clone()) {
                if !f(&value)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Clear all entries.
    pub fn clear(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            d.index.clear_sync();
            self.total_bytes.store(0, Ordering::Release);
            // Data-file space is reclaimed by compaction / when the collection
            // is dropped; the orphaned records are now unreferenced.
            return;
        }
        self.tree.clear_sync();
        self.total_bytes.store(0, Ordering::Release);
    }

    /// No-op. `scc::HashMap` manages its own capacity internally.
    /// Kept for API compatibility.
    pub fn reserve(&self, _additional: usize) {
        // scc::HashMap handles its own capacity.
    }

    /// Batch insert multiple entries. Uses `upsert_sync` for each entry.
    /// Uses atomic `fetch_add` for the total byte count.
    ///
    /// IMPORTANT: Only use for new keys (no replacements). If keys might exist,
    /// use the regular `insert()` loop instead.
    pub fn insert_batch(&self, entries: Vec<(u64, Vec<u8>)>) {
        // MVCC-lite: batch inserts are new keys by contract, so the prior is
        // None for each — but the insert EVENT must still be remembered, or
        // a snapshot could see documents born after it.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(g) = self.recording_gate() {
            for (key, _) in &entries {
                g.record(&self.name, *key, None);
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            // New keys only (per contract). One batched, un-synced append for
            // the whole batch (durability via WAL; flushed in persist()), then
            // record each returned location. The `_buffered` variant compresses
            // the batch in parallel and writes it with a single lock + seek +
            // `write_all`, instead of a per-record lock/seek/write — the win on
            // the bulk-insert path.
            let slices: Vec<&[u8]> = entries.iter().map(|(_, v)| v.as_slice()).collect();
            let data = d.data.read();
            match data.append_batch_no_sync_buffered(&slices) {
                Ok(locs) => {
                    let mut added = 0u64;
                    for ((key, _value), loc) in entries.iter().zip(locs.iter()) {
                        let _ = d.index.upsert_sync(*key, *loc);
                        // On-disk units, like `insert()` — see comment there.
                        added += loc.length as u64;
                    }
                    self.total_bytes.fetch_add(added, Ordering::AcqRel);
                }
                Err(e) => eprintln!(
                    "[btree-disk] {} batch append failed: {} — WAL replay will reconcile",
                    self.name, e
                ),
            }
            return;
        }
        let total_new: u64 = entries.iter().map(|(_, v)| v.len() as u64).sum();
        #[cfg(not(target_arch = "wasm32"))]
        if entries.len() > 1000 {
            use rayon::prelude::*;
            entries.into_par_iter().for_each(|(key, value)| {
                let _ = self.tree.insert_sync(key, value);
            });
        } else {
            for (key, value) in entries {
                let _ = self.tree.insert_sync(key, value);
            }
        }
        #[cfg(target_arch = "wasm32")]
        for (key, value) in entries {
            let _ = self.tree.insert_sync(key, value);
        }
        self.total_bytes.fetch_add(total_new, Ordering::AcqRel);
    }

    /// Return all values as owned `Vec<u8>` copies for parallel processing.
    /// Values are cloned because `scc::HashMap` bucket guards cannot outlive iteration.
    pub fn values_as_slices(&self) -> Vec<Vec<u8>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            let data = d.data.read();
            let mut locs: Vec<crate::storage::DocLocation> = Vec::new();
            d.index.iter_sync(|_, loc| {
                locs.push(*loc);
                true
            });
            return locs
                .into_iter()
                .filter_map(|loc| data.read_lockfree(loc).ok())
                .collect();
        }
        let mut values = Vec::new();
        self.tree.iter_sync(|_, v| {
            values.push(v.clone());
            true
        });
        values
    }

    /// Collect all keys (lightweight — no value cloning).
    pub fn scan_keys<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            d.index.iter_sync(|key, _| {
                f(*key);
                true
            });
            return;
        }
        self.tree.iter_sync(|key, _| {
            f(*key);
            true
        });
    }

    /// Iterate all entries in arbitrary order (no key sort). Faster for full scans
    /// like aggregation where key order doesn't matter.
    pub fn for_each_value<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, &[u8]) -> Result<bool>,
    {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(d) = &self.disk {
            let data = d.data.read();
            // Collect (key, loc) first — we can't read `data` while holding an
            // index bucket guard.
            let mut items: Vec<(u64, crate::storage::DocLocation)> = Vec::new();
            d.index.iter_sync(|k, loc| {
                items.push((*k, *loc));
                true
            });
            // Read in data-file (offset) order so the scan is a sequential,
            // readahead-friendly sweep rather than random index-order page
            // faults. `for_each_payload` borrows directly from the mmap for
            // records needing no decode (zero-copy) and locks the mmap once.
            items.sort_unstable_by_key(|(_, loc)| loc.offset);
            let locs: Vec<crate::storage::DocLocation> =
                items.iter().map(|(_, loc)| *loc).collect();
            let mut cb_err: Option<Error> = None;
            data.for_each_payload(&locs, |i, bytes| match f(items[i].0, bytes) {
                Ok(true) => Ok(true),
                Ok(false) => Ok(false),
                Err(e) => {
                    cb_err = Some(e);
                    Ok(false)
                }
            })?;
            if let Some(e) = cb_err {
                return Err(e);
            }
            return Ok(());
        }
        let mut err: Option<Error> = None;
        self.tree.iter_sync(|key, value| match f(*key, value) {
            Ok(true) => true,
            Ok(false) => false,
            Err(e) => {
                err = Some(e);
                false
            }
        });
        if let Some(e) = err {
            return Err(e);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_crud() {
        let storage = BTreeStorage::new_in_memory("test");
        assert_eq!(storage.count(), 0);

        storage.insert(1, b"hello".to_vec());
        storage.insert(2, b"world".to_vec());
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.get(1).unwrap().as_slice(), b"hello");
        assert_eq!(storage.get(2).unwrap().as_slice(), b"world");

        storage.insert(1, b"updated".to_vec());
        assert_eq!(storage.count(), 2);
        assert_eq!(storage.get(1).unwrap().as_slice(), b"updated");

        let removed = storage.remove(2);
        assert_eq!(removed.unwrap(), b"world");
        assert_eq!(storage.count(), 1);
        assert!(storage.get(2).is_none());
    }

    #[test]
    fn cursor_forward() {
        let storage = BTreeStorage::new_in_memory("test");
        for i in 1..=5 {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut cursor = storage.cursor_first().unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = cursor.next().unwrap() {
            keys.push(key);
        }
        assert_eq!(keys, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn cursor_seek() {
        let storage = BTreeStorage::new_in_memory("test");
        for i in [1, 3, 5, 7, 9] {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut cursor = storage.cursor_seek(4).unwrap();
        let (key, _) = cursor.next().unwrap().unwrap();
        assert_eq!(key, 5); // next key >= 4
    }

    #[test]
    fn cursor_reverse() {
        let storage = BTreeStorage::new_in_memory("test");
        for i in 1..=5 {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut cursor = storage.cursor_last().unwrap();
        let mut keys = Vec::new();
        while let Some((key, _value)) = cursor.next().unwrap() {
            keys.push(key);
        }
        assert_eq!(keys, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn scan_all() {
        let storage = BTreeStorage::new_in_memory("test");
        for i in 1..=10 {
            storage.insert(i, format!("doc_{}", i).into_bytes());
        }

        let mut count = 0;
        storage
            .scan_all_while(|_key, _value| {
                count += 1;
                Ok(count < 5)
            })
            .unwrap();
        assert_eq!(count, 5); // stopped after 5
    }

    #[test]
    fn persistence_roundtrip() {
        if disk_first_enabled() {
            return;
        } // tests the in-RAM .btree format
        let dir = tempfile::tempdir().unwrap();
        {
            let storage = BTreeStorage::new("test_persist", dir.path(), None);
            storage.insert(1, b"alpha".to_vec());
            storage.insert(2, b"beta".to_vec());
            storage.insert(3, b"gamma".to_vec());
            storage.persist().unwrap();
        }
        {
            let storage = BTreeStorage::open("test_persist", dir.path(), None).unwrap();
            assert_eq!(storage.count(), 3);
            assert_eq!(storage.get(1).unwrap().as_slice(), b"alpha");
            assert_eq!(storage.get(2).unwrap().as_slice(), b"beta");
            assert_eq!(storage.get(3).unwrap().as_slice(), b"gamma");
        }
    }

    // ── Phase 1b header (docs/format/btree.md) ────────────────────────

    /// `persist()` prepends the OXBT / version=1 / flags=0 header to the
    /// plaintext, just before the record stream. Verified by reading raw
    /// bytes on the unencrypted path.
    #[test]
    fn persist_writes_oxbt_header_unencrypted() {
        let dir = tempfile::tempdir().unwrap();
        let storage = BTreeStorage::new("hdr", dir.path(), None);
        storage.insert(0x1122334455667788, b"X".to_vec());
        storage.persist().unwrap();

        let raw = std::fs::read(dir.path().join("hdr.btree")).unwrap();
        assert!(
            raw.len() >= BTREE_HEADER_SIZE + 8 + 4 + 1,
            "header + 1 record minimum"
        );
        assert_eq!(&raw[0..4], BTREE_MAGIC, "OXBT magic at offset 0");
        assert_eq!(u16::from_le_bytes([raw[4], raw[5]]), BTREE_VERSION);
        assert_eq!(u16::from_le_bytes([raw[6], raw[7]]), 0, "flags reserved 0");
        // First record's key sits immediately after the header.
        assert_eq!(
            u64::from_le_bytes(raw[8..16].try_into().unwrap()),
            0x1122334455667788
        );
    }

    /// A pre-Phase-1b file (no OXBT magic at offset 0 of the plaintext)
    /// reads correctly. After one `persist()` the file is rewritten with
    /// the v1 header — every persist serialises the whole tree, so the
    /// migration window is exactly "first persist after upgrade".
    #[test]
    fn reads_legacy_header_less_btree() {
        if disk_first_enabled() {
            return;
        } // tests the in-RAM .btree format
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.btree");

        // Hand-craft a legacy file: just one [key u64][len u32][value] record,
        // no header. This is byte-identical to what a pre-Phase-1b engine
        // would have written for an unencrypted .btree.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&42u64.to_le_bytes());
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.extend_from_slice(b"hello");
        std::fs::write(&path, &bytes).unwrap();

        // Open + read: legacy bytes parse correctly.
        let storage = BTreeStorage::open("legacy", dir.path(), None).unwrap();
        assert_eq!(storage.count(), 1);
        assert_eq!(storage.get(42).unwrap().as_slice(), b"hello");

        // The next persist rewrites with a v1 header.
        storage.persist().unwrap();
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(
            &raw[0..4],
            BTREE_MAGIC,
            "legacy file migrated on next persist"
        );

        // ...and still reads back correctly on reopen.
        let reopened = BTreeStorage::open("legacy", dir.path(), None).unwrap();
        assert_eq!(reopened.count(), 1);
        assert_eq!(reopened.get(42).unwrap().as_slice(), b"hello");
    }

    /// A `.btree` file with a newer format version we don't recognise must
    /// be refused at `open()` rather than silently loaded as empty (which
    /// would let WAL replay paper over a real data loss).
    #[test]
    fn refuses_newer_btree_version() {
        if disk_first_enabled() {
            return;
        } // tests the in-RAM .btree format
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.btree");

        // Hand-craft a v2 file: OXBT + version=2 + flags=0 + one record.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BTREE_MAGIC);
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&1u64.to_le_bytes()); // a record we should never reach
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(b'A');
        std::fs::write(&path, &bytes).unwrap();

        let err = BTreeStorage::open("future", dir.path(), None)
            .err()
            .expect("open should reject a newer btree format version");
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported .btree format version 2"),
            "error should name the unsupported version: {msg}"
        );
    }
}
