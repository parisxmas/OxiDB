use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::{Arc, RwLock};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{Entry, LogId, SnapshotMeta, StorageError, StoredMembership, Vote};
use serde::{Deserialize, Serialize};
use serde_json::json;

use oxidb::OxiDb;

use super::types::{OxiDbRequest, OxiDbResponse, TypeConfig};

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

/// Combined log store + state machine implementing the v1 `RaftStorage` trait.
/// Wrapped by `Adaptor` for use with `Raft::new`.
///
/// All mutable state lives behind `Arc<RwLock<Inner>>` so that the handles
/// returned by `get_log_reader()` and `get_snapshot_builder()` share the
/// same underlying data as the main store.
pub struct OxiDbStore {
    inner: Arc<RwLock<Inner>>,
    db: Arc<OxiDb>,
}

impl Clone for OxiDbStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            db: Arc::clone(&self.db),
        }
    }
}

impl OxiDbStore {
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
        }
    }
}

/// Apply a single `OxiDbRequest` against the database engine.
fn apply_request(db: &OxiDb, req: OxiDbRequest) -> OxiDbResponse {
    use std::collections::HashMap;

    match req {
        OxiDbRequest::Insert { collection, document } => match db.insert(&collection, document) {
            Ok(id) => OxiDbResponse::Ok { data: json!({ "id": id }) },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::InsertMany { collection, documents } => match db.insert_many(&collection, documents) {
            Ok(ids) => OxiDbResponse::Ok { data: json!(ids) },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::Update { collection, query, update } => match db.update(&collection, &query, &update) {
            Ok(count) => OxiDbResponse::Ok { data: json!({ "modified": count }) },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::UpdateOne { collection, query, update } => match db.update_one(&collection, &query, &update) {
            Ok(count) => OxiDbResponse::Ok { data: json!({ "modified": count }) },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::Delete { collection, query } => match db.delete(&collection, &query) {
            Ok(count) => OxiDbResponse::Ok { data: json!({ "deleted": count }) },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::DeleteOne { collection, query } => match db.delete_one(&collection, &query) {
            Ok(count) => OxiDbResponse::Ok { data: json!({ "deleted": count }) },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::CreateCollection { name } => match db.create_collection(&name) {
            Ok(()) => OxiDbResponse::Ok { data: json!("collection created") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::DropCollection { name } => match db.drop_collection(&name) {
            Ok(()) => OxiDbResponse::Ok { data: json!("collection dropped") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::Compact { collection } => match db.compact(&collection) {
            Ok(stats) => OxiDbResponse::Ok {
                data: json!({ "old_size": stats.old_size, "new_size": stats.new_size, "docs_kept": stats.docs_kept }),
            },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::CreateIndex { collection, field } => match db.create_index(&collection, &field) {
            Ok(()) => OxiDbResponse::Ok { data: json!("index created") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::CreateUniqueIndex { collection, field } => match db.create_unique_index(&collection, &field) {
            Ok(()) => OxiDbResponse::Ok { data: json!("unique index created") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::CreateCompositeIndex { collection, fields } => match db.create_composite_index(&collection, fields) {
            Ok(name) => OxiDbResponse::Ok { data: json!({ "index": name }) },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::CreateTextIndex { collection, fields } => match db.create_text_index(&collection, fields) {
            Ok(()) => OxiDbResponse::Ok { data: json!("text index created") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::DropIndex { collection, index } => match db.drop_index(&collection, &index) {
            Ok(()) => OxiDbResponse::Ok { data: json!("index dropped") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::CreateBucket { bucket } => match db.create_bucket(&bucket) {
            Ok(()) => OxiDbResponse::Ok { data: json!("bucket created") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::DeleteBucket { bucket } => match db.delete_bucket(&bucket) {
            Ok(()) => OxiDbResponse::Ok { data: json!("bucket deleted") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
        OxiDbRequest::PutObject { bucket, key, data_b64, content_type, metadata } => {
            let data = match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &data_b64) {
                Ok(d) => d,
                Err(e) => return OxiDbResponse::Error { message: format!("invalid base64: {e}") },
            };
            let meta_map: HashMap<String, String> = metadata
                .as_object()
                .map(|obj| obj.iter().filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string()))).collect())
                .unwrap_or_default();
            match db.put_object(&bucket, &key, &data, &content_type, meta_map) {
                Ok(meta) => OxiDbResponse::Ok { data: meta },
                Err(e) => OxiDbResponse::Error { message: e.to_string() },
            }
        }
        OxiDbRequest::DeleteObject { bucket, key } => match db.delete_object(&bucket, &key) {
            Ok(()) => OxiDbResponse::Ok { data: json!("object deleted") },
            Err(e) => OxiDbResponse::Error { message: e.to_string() },
        },
    }
}

impl RaftLogReader<TypeConfig> for OxiDbStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
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
            data.last_applied_log.map(|l| l.index.to_string()).unwrap_or_default(),
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
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.read().unwrap().vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.inner.write().unwrap().committed = committed;
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
        let mut inner = self.inner.write().unwrap();
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().unwrap();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().unwrap();
        inner.last_purged_log_id = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, openraft::BasicNode>), StorageError<u64>> {
        let inner = self.inner.read().unwrap();
        Ok((inner.sm_data.last_applied_log, inner.sm_data.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<OxiDbResponse>, StorageError<u64>> {
        let mut inner = self.inner.write().unwrap();
        let mut results = Vec::new();

        for entry in entries {
            inner.sm_data.last_applied_log = Some(entry.log_id);

            match &entry.payload {
                openraft::EntryPayload::Blank => {
                    results.push(OxiDbResponse::Ok { data: json!(null) });
                }
                openraft::EntryPayload::Normal(req) => {
                    let resp = apply_request(&self.db, req.clone());
                    results.push(resp);
                }
                openraft::EntryPayload::Membership(mem) => {
                    inner.sm_data.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());
                    results.push(OxiDbResponse::Ok { data: json!("membership updated") });
                }
            }
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().unwrap();
        inner.sm_data.last_applied_log = meta.last_log_id;
        inner.sm_data.last_membership = meta.last_membership.clone();

        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        });

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
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
