use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{Entry, LogId, SnapshotMeta, StorageError, StoredMembership, Vote};
use serde::{Deserialize, Serialize};
use serde_json::json;

use oxidb::OxiDb;

use super::types::{OxiDbRequest, OxiDbResponse, TransactionWriteOp, TypeConfig};

/// Snapshot metadata stored alongside the state machine.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, openraft::BasicNode>,
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

/// Shared inner state behind `Arc<RwLock<...>>` so that log readers and
/// snapshot builders returned by `get_log_reader` / `get_snapshot_builder`
/// always see the latest data.
struct Inner {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged_log_id: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    sm_data: StateMachineData,
    current_snapshot: Option<StoredSnapshot>,
}

/// Persisted Raft metadata — the small, frequently-updated bits.
/// Excludes the log (which lives in a separate append-only file) and the
/// in-memory snapshot blob (which Raft can rebuild from the log).
#[derive(Default, Serialize, Deserialize)]
struct PersistedMeta {
    last_purged_log_id: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    sm_data: StateMachineData,
}

impl Inner {
    fn to_meta(&self) -> PersistedMeta {
        PersistedMeta {
            last_purged_log_id: self.last_purged_log_id,
            vote: self.vote,
            committed: self.committed,
            sm_data: self.sm_data.clone(),
        }
    }
}

/// Persistence layout (under `<data_dir>/`):
///   raft_meta.json   — small file: vote, committed, last_purged_log_id, sm_data.
///                      Rewritten on metadata changes (write+rename, ~constant size).
///   raft_log.jsonl   — append-only log: one Entry per line.
///                      Truncated only on `delete_conflict_logs_since` /
///                      `purge_logs_upto` (rare events).
///
/// Combined log store + state machine implementing the v1 `RaftStorage` trait.
/// Wrapped by `Adaptor` for use with `Raft::new`.
///
/// All mutable state lives behind `Arc<RwLock<Inner>>` so that the handles
/// returned by `get_log_reader()` and `get_snapshot_builder()` share the
/// same underlying data as the main store.
///
/// When `paths` is `Some(...)`, every mutation is mirrored to disk in O(1)
/// per entry. On `OxiDbStore::open(data_dir)`, both files are loaded so the
/// node rejoins its Raft cluster after a restart instead of coming back as
/// a `Learner` with `term=0`.
pub struct OxiDbStore {
    inner: Arc<RwLock<Inner>>,
    db: Arc<OxiDb>,
    /// Database registry for multi-database requests (`Scoped`,
    /// `CreateDatabase`, `DropDatabase`). `None` = default database only.
    db_manager: Option<Arc<oxidb::DatabaseManager>>,
    paths: Option<RaftPaths>,
    /// Lock that serializes mutations to the on-disk log file.
    /// (in-memory operations are already serialized by the RwLock on `inner`.)
    log_writer: Arc<std::sync::Mutex<()>>,
}

#[derive(Clone)]
struct RaftPaths {
    meta: PathBuf,
    log: PathBuf,
    meta_tmp: PathBuf,
    log_tmp: PathBuf,
}

impl Clone for OxiDbStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            db: Arc::clone(&self.db),
            db_manager: self.db_manager.clone(),
            paths: self.paths.clone(),
            log_writer: Arc::clone(&self.log_writer),
        }
    }
}

impl OxiDbStore {
    /// In-memory only — no persistence. Use `open` for production deployments.
    pub fn new(db: Arc<OxiDb>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                log: BTreeMap::new(),
                last_purged_log_id: None,
                vote: None,
                committed: None,
                sm_data: StateMachineData::default(),
                current_snapshot: None,
            })),
            db,
            db_manager: None,
            paths: None,
            log_writer: Arc::new(std::sync::Mutex::new(())),
        }
    }

    /// Attach the database registry so multi-database requests (`Scoped`,
    /// `CreateDatabase`, `DropDatabase`) can be applied on this node.
    pub fn with_manager(mut self, mgr: Arc<oxidb::DatabaseManager>) -> Self {
        self.db_manager = Some(mgr);
        self
    }

    /// Persistent variant: loads existing Raft state from `<data_dir>/raft_meta.json`
    /// + `<data_dir>/raft_log.jsonl`. Every subsequent mutation appends to the log
    /// or rewrites the (small) meta file in O(1).
    pub fn open(db: Arc<OxiDb>, data_dir: &std::path::Path) -> Self {
        if let Err(e) = fs::create_dir_all(data_dir) {
            eprintln!("raft: failed to create data dir {data_dir:?}: {e}");
        }
        let paths = RaftPaths {
            meta: data_dir.join("raft_meta.json"),
            log: data_dir.join("raft_log.jsonl"),
            meta_tmp: data_dir.join("raft_meta.json.tmp"),
            log_tmp: data_dir.join("raft_log.jsonl.tmp"),
        };

        // Migrate old single-file format if present.
        let legacy = data_dir.join("raft_state.json");
        if legacy.exists() && !paths.meta.exists() {
            eprintln!("raft: migrating legacy raft_state.json → split files");
            if let Ok(bytes) = fs::read(&legacy) {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    let mut meta = PersistedMeta::default();
                    meta.last_purged_log_id = v
                        .get("last_purged_log_id")
                        .and_then(|x| serde_json::from_value(x.clone()).ok());
                    meta.vote = v
                        .get("vote")
                        .and_then(|x| serde_json::from_value(x.clone()).ok());
                    meta.committed = v
                        .get("committed")
                        .and_then(|x| serde_json::from_value(x.clone()).ok());
                    if let Some(sm) = v.get("sm_data") {
                        if let Ok(s) = serde_json::from_value::<StateMachineData>(sm.clone()) {
                            meta.sm_data = s;
                        }
                    }
                    let _ = fs::write(&paths.meta, serde_json::to_vec(&meta).unwrap_or_default());
                    if let Some(arr) = v.get("log").and_then(|x| x.as_object()) {
                        let mut sorted: Vec<(u64, &serde_json::Value)> = arr
                            .iter()
                            .filter_map(|(k, v)| k.parse::<u64>().ok().map(|i| (i, v)))
                            .collect();
                        sorted.sort_by_key(|(i, _)| *i);
                        let mut log_buf = String::new();
                        for (_, entry) in sorted {
                            log_buf.push_str(&entry.to_string());
                            log_buf.push('\n');
                        }
                        let _ = fs::write(&paths.log, log_buf);
                    }
                    let _ = fs::remove_file(&legacy);
                }
            }
        }

        // Load metadata.
        let meta = if paths.meta.exists() {
            match fs::read(&paths.meta) {
                Ok(b) => serde_json::from_slice::<PersistedMeta>(&b).unwrap_or_else(|e| {
                    eprintln!("raft: corrupt {:?}: {e}; starting fresh", paths.meta);
                    PersistedMeta::default()
                }),
                Err(e) => {
                    eprintln!("raft: read {:?}: {e}", paths.meta);
                    PersistedMeta::default()
                }
            }
        } else {
            PersistedMeta::default()
        };

        // Load log entries (line-by-line).
        let mut log = BTreeMap::new();
        if paths.log.exists() {
            match fs::read_to_string(&paths.log) {
                Ok(s) => {
                    for line in s.lines() {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<Entry<TypeConfig>>(line) {
                            Ok(entry) => {
                                log.insert(entry.log_id.index, entry);
                            }
                            Err(e) => {
                                eprintln!("raft: skip corrupt log line: {e}");
                            }
                        }
                    }
                }
                Err(e) => eprintln!("raft: read {:?}: {e}", paths.log),
            }
        }

        eprintln!(
            "raft: loaded state from {:?} ({} log entries, vote={:?}, committed={:?})",
            data_dir,
            log.len(),
            meta.vote,
            meta.committed
        );

        Self {
            inner: Arc::new(RwLock::new(Inner {
                log,
                last_purged_log_id: meta.last_purged_log_id,
                vote: meta.vote,
                committed: meta.committed,
                sm_data: meta.sm_data,
                current_snapshot: None,
            })),
            db,
            db_manager: None,
            paths: Some(paths),
            log_writer: Arc::new(std::sync::Mutex::new(())),
        }
    }

    /// Rewrite the small metadata file (vote + committed + last_purged + sm_data).
    /// O(1) — file is tiny regardless of cluster age.
    fn persist_meta(&self) {
        let paths = match &self.paths {
            Some(p) => p,
            None => return,
        };
        let meta = self.inner.read().unwrap().to_meta();
        let bytes = match serde_json::to_vec(&meta) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("raft persist_meta: serialize: {e}");
                return;
            }
        };
        if let Err(e) = fs::write(&paths.meta_tmp, &bytes) {
            eprintln!("raft persist_meta: write tmp: {e}");
            return;
        }
        if let Err(e) = fs::rename(&paths.meta_tmp, &paths.meta) {
            eprintln!("raft persist_meta: rename: {e}");
        }
    }

    /// Append a single log entry to the on-disk log file. O(1) per entry.
    fn append_log_entry(&self, entry: &Entry<TypeConfig>) {
        let paths = match &self.paths {
            Some(p) => p,
            None => return,
        };
        let line = match serde_json::to_string(entry) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("raft append_log: serialize: {e}");
                return;
            }
        };
        let _guard = self.log_writer.lock().unwrap();
        let res = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&paths.log)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(line.as_bytes())?;
                f.write_all(b"\n")
            });
        if let Err(e) = res {
            eprintln!("raft append_log: write: {e}");
        }
    }

    /// Rewrite the log file from current in-memory state. Called from
    /// `delete_conflict_logs_since` and `purge_logs_upto` — rare in steady state.
    fn rewrite_log(&self) {
        let paths = match &self.paths {
            Some(p) => p,
            None => return,
        };
        let buf = {
            let inner = self.inner.read().unwrap();
            let mut s = String::with_capacity(inner.log.len() * 64);
            for entry in inner.log.values() {
                if let Ok(line) = serde_json::to_string(entry) {
                    s.push_str(&line);
                    s.push('\n');
                }
            }
            s
        };
        let _guard = self.log_writer.lock().unwrap();
        if let Err(e) = fs::write(&paths.log_tmp, buf.as_bytes()) {
            eprintln!("raft rewrite_log: write tmp: {e}");
            return;
        }
        if let Err(e) = fs::rename(&paths.log_tmp, &paths.log) {
            eprintln!("raft rewrite_log: rename: {e}");
        }
    }
}

/// Apply a single `OxiDbRequest` against the database engine.
fn apply_request(
    db: &OxiDb,
    mgr: Option<&oxidb::DatabaseManager>,
    req: OxiDbRequest,
) -> OxiDbResponse {
    apply_request_in(db, mgr, oxidb::database_manager::DEFAULT_DATABASE, req)
}

/// Apply one replicated request against `db` (the engine of database
/// `db_name`). `Scoped` re-targets both before recursing.
fn apply_request_in(
    db: &OxiDb,
    mgr: Option<&oxidb::DatabaseManager>,
    db_name: &str,
    req: OxiDbRequest,
) -> OxiDbResponse {
    use std::collections::HashMap;

    match req {
        OxiDbRequest::Scoped { db: name, inner } => {
            let Some(mgr) = mgr else {
                return OxiDbResponse::Error {
                    message: "multi-database requests need a database registry on this node"
                        .to_string(),
                };
            };
            match mgr.get_database(&name) {
                Ok(target) => apply_request_in(&target, Some(mgr), &name, *inner),
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::CreateDatabase {
            name,
            if_not_exists,
        } => {
            let Some(mgr) = mgr else {
                return OxiDbResponse::Error {
                    message: "multi-database requests need a database registry on this node"
                        .to_string(),
                };
            };
            match mgr.create_database(&name) {
                Ok(()) => OxiDbResponse::Ok {
                    data: json!(format!("database '{name}' created")),
                },
                Err(oxidb::Error::DatabaseAlreadyExists(_)) if if_not_exists => OxiDbResponse::Ok {
                    data: json!(format!("database '{name}' created")),
                },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::DropDatabase { name, if_exists } => {
            let Some(mgr) = mgr else {
                return OxiDbResponse::Error {
                    message: "multi-database requests need a database registry on this node"
                        .to_string(),
                };
            };
            match mgr.drop_database(&name) {
                Ok(()) => {
                    crate::sql_bridge::forget_database(&name);
                    OxiDbResponse::Ok {
                        data: json!(format!("database '{name}' dropped")),
                    }
                }
                Err(oxidb::Error::DatabaseNotFound(_)) if if_exists => OxiDbResponse::Ok {
                    data: json!(format!("database '{name}' dropped")),
                },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::Insert {
            collection,
            document,
        } => match db.insert(&collection, document) {
            Ok(id) => OxiDbResponse::Ok {
                data: json!({ "id": id }),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::InsertMany {
            collection,
            documents,
        } => match db.insert_many(&collection, documents) {
            Ok(ids) => OxiDbResponse::Ok { data: json!(ids) },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::Update {
            collection,
            query,
            update,
        } => match db.update(&collection, &query, &update) {
            Ok(count) => OxiDbResponse::Ok {
                data: json!({ "modified": count }),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::UpdateOne {
            collection,
            query,
            update,
        } => match db.update_one(&collection, &query, &update) {
            Ok(count) => OxiDbResponse::Ok {
                data: json!({ "modified": count }),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::Delete { collection, query } => match db.delete(&collection, &query) {
            Ok(count) => OxiDbResponse::Ok {
                data: json!({ "deleted": count }),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::DeleteOne { collection, query } => match db.delete_one(&collection, &query) {
            Ok(count) => OxiDbResponse::Ok {
                data: json!({ "deleted": count }),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::CreateCollection { name } => match db.create_collection(&name) {
            Ok(()) => OxiDbResponse::Ok {
                data: json!("collection created"),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::CreateCollectionWithOptions { name, options } => {
            match db.create_collection_with_options(&name, options) {
                Ok(()) => OxiDbResponse::Ok {
                    data: json!("collection created"),
                },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::Sql { sql, params } => {
            // Re-execute the replicated SQL on this node's SQL engine. The
            // engine must be enabled (`OXIDB_SQL=1`) on every cluster node —
            // a node with it disabled reports an error here but the log entry
            // is already committed (operational requirement, documented).
            let params = if params.is_null() {
                None
            } else {
                Some(&params)
            };
            match crate::sql_bridge::execute_json_in(db_name, &sql, params, false) {
                Ok(results) => OxiDbResponse::Ok { data: results },
                Err(message) => OxiDbResponse::Error { message },
            }
        }
        OxiDbRequest::DropCollection { name } => match db.drop_collection(&name) {
            Ok(()) => OxiDbResponse::Ok {
                data: json!("collection dropped"),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::Compact { collection } => match db.compact(&collection) {
            Ok(stats) => OxiDbResponse::Ok {
                data: json!({ "old_size": stats.old_size, "new_size": stats.new_size, "docs_kept": stats.docs_kept }),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::CreateIndex { collection, field } => {
            match db.create_index(&collection, &field) {
                Ok(()) => OxiDbResponse::Ok {
                    data: json!("index created"),
                },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::CreateUniqueIndex { collection, field } => {
            match db.create_unique_index(&collection, &field) {
                Ok(()) => OxiDbResponse::Ok {
                    data: json!("unique index created"),
                },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::CreateCompositeIndex { collection, fields } => {
            match db.create_composite_index(&collection, fields) {
                Ok(name) => OxiDbResponse::Ok {
                    data: json!({ "index": name }),
                },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::CreateTextIndex { collection, fields } => {
            match db.create_text_index(&collection, fields) {
                Ok(()) => OxiDbResponse::Ok {
                    data: json!("text index created"),
                },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::DropIndex { collection, index } => match db.drop_index(&collection, &index) {
            Ok(()) => OxiDbResponse::Ok {
                data: json!("index dropped"),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::CreateBucket { bucket } => match db.create_bucket(&bucket) {
            Ok(()) => OxiDbResponse::Ok {
                data: json!("bucket created"),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::DeleteBucket { bucket } => match db.delete_bucket(&bucket) {
            Ok(()) => OxiDbResponse::Ok {
                data: json!("bucket deleted"),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::PutObject {
            bucket,
            key,
            data_b64,
            content_type,
            metadata,
        } => {
            let data =
                match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &data_b64)
                {
                    Ok(d) => d,
                    Err(e) => {
                        return OxiDbResponse::Error {
                            message: format!("invalid base64: {e}"),
                        };
                    }
                };
            let meta_map: HashMap<String, String> = metadata
                .as_object()
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            match db.put_object(&bucket, &key, &data, &content_type, meta_map) {
                Ok(meta) => OxiDbResponse::Ok { data: meta },
                Err(e) => OxiDbResponse::Error {
                    message: e.to_string(),
                },
            }
        }
        OxiDbRequest::DeleteObject { bucket, key } => match db.delete_object(&bucket, &key) {
            Ok(()) => OxiDbResponse::Ok {
                data: json!("object deleted"),
            },
            Err(e) => OxiDbResponse::Error {
                message: e.to_string(),
            },
        },
        OxiDbRequest::CommitTransaction { write_ops } => {
            // Apply all buffered transaction writes atomically
            let mut errors = Vec::new();
            for op in write_ops {
                let result = match op {
                    TransactionWriteOp::Insert {
                        collection,
                        document,
                    } => db.insert(&collection, document).map(|_| ()),
                    TransactionWriteOp::Update {
                        collection,
                        query,
                        update,
                    } => db.update(&collection, &query, &update).map(|_| ()),
                    TransactionWriteOp::Delete { collection, query } => {
                        db.delete(&collection, &query).map(|_| ())
                    }
                };
                if let Err(e) = result {
                    errors.push(e.to_string());
                }
            }
            if errors.is_empty() {
                OxiDbResponse::Ok {
                    data: json!("transaction committed"),
                }
            } else {
                OxiDbResponse::Error {
                    message: format!("partial commit errors: {}", errors.join("; ")),
                }
            }
        }
    }
}

impl RaftLogReader<TypeConfig> for OxiDbStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.read().unwrap();
        let entries: Vec<_> = inner.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for OxiDbStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut inner = self.inner.write().unwrap();
        let data = inner.sm_data.clone();
        let snap_data = serde_json::to_vec(&data).unwrap_or_default();

        let snapshot_id = format!(
            "{}-{}",
            data.last_applied_log
                .map(|l| l.index.to_string())
                .unwrap_or_default(),
            chrono::Utc::now().timestamp_millis()
        );

        let meta = SnapshotMeta {
            last_log_id: data.last_applied_log,
            last_membership: data.last_membership,
            snapshot_id,
        };

        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: snap_data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snap_data)),
        })
    }
}

impl RaftStorage<TypeConfig> for OxiDbStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.write().unwrap().vote = Some(*vote);
        self.persist_meta();
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.read().unwrap().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.inner.write().unwrap().committed = committed;
        self.persist_meta();
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.read().unwrap().committed)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.read().unwrap();
        let last_log_id = inner.log.last_key_value().map(|(_, e)| e.log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: last_log_id.or(inner.last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        // Collect the entries first so we can both insert them in-memory AND
        // append them to disk without holding the write lock during file I/O.
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        {
            let mut inner = self.inner.write().unwrap();
            for entry in &entries {
                inner.log.insert(entry.log_id.index, entry.clone());
            }
        }
        // O(1) per entry: append-only write to raft_log.jsonl
        for entry in &entries {
            self.append_log_entry(entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        {
            let mut inner = self.inner.write().unwrap();
            let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
            for k in keys {
                inner.log.remove(&k);
            }
        }
        // Rare event: rewrite the log file from current in-memory state.
        self.rewrite_log();
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut inner = self.inner.write().unwrap();
            inner.last_purged_log_id = Some(log_id);
            let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
            for k in keys {
                inner.log.remove(&k);
            }
        }
        // last_purged_log_id changed → meta needs persisting; log was truncated.
        self.persist_meta();
        self.rewrite_log();
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let inner = self.inner.read().unwrap();
        Ok((
            inner.sm_data.last_applied_log,
            inner.sm_data.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<OxiDbResponse>, StorageError<u64>> {
        let results = {
            let mut inner = self.inner.write().unwrap();
            let mut out = Vec::new();
            for entry in entries {
                inner.sm_data.last_applied_log = Some(entry.log_id);
                match &entry.payload {
                    openraft::EntryPayload::Blank => {
                        out.push(OxiDbResponse::Ok { data: json!(null) });
                    }
                    openraft::EntryPayload::Normal(req) => {
                        let resp = apply_request(&self.db, self.db_manager.as_deref(), req.clone());
                        out.push(resp);
                    }
                    openraft::EntryPayload::Membership(mem) => {
                        inner.sm_data.last_membership =
                            StoredMembership::new(Some(entry.log_id), mem.clone());
                        out.push(OxiDbResponse::Ok {
                            data: json!("membership updated"),
                        });
                    }
                }
            }
            out
        };
        // sm_data (last_applied + membership) changed → small file rewrite.
        self.persist_meta();
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        {
            let mut inner = self.inner.write().unwrap();
            inner.sm_data.last_applied_log = meta.last_log_id;
            inner.sm_data.last_membership = meta.last_membership.clone();
            inner.current_snapshot = Some(StoredSnapshot {
                meta: meta.clone(),
                data: snapshot.into_inner(),
            });
        }
        self.persist_meta();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.read().unwrap();
        match &inner.current_snapshot {
            Some(snap) => Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(snap.data.clone())),
            })),
            None => Ok(None),
        }
    }
}
