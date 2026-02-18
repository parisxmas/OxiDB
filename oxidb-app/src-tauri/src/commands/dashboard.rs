use std::sync::Mutex;

use serde::Serialize;
use serde_json::json;
use tauri::State;

use crate::state::DbBackend;

#[derive(Serialize)]
pub struct DashboardStats {
    pub collections: Vec<CollectionStat>,
    pub total_docs: usize,
    pub total_storage_bytes: u64,
}

#[derive(Serialize)]
pub struct CollectionStat {
    pub name: String,
    pub doc_count: usize,
    pub storage_bytes: u64,
}

#[tauri::command]
pub fn get_dashboard_stats(
    state: State<'_, Mutex<DbBackend>>,
) -> Result<DashboardStats, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, data_path, .. } => {
            let names = db.list_collections();
            let mut collections = Vec::new();
            let mut total_docs = 0usize;
            let mut total_storage = 0u64;

            for name in &names {
                let count = db
                    .count(name, &json!({}))
                    .unwrap_or(0);
                let dat_path = std::path::Path::new(data_path).join(format!("{name}.dat"));
                let size = std::fs::metadata(&dat_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                total_docs += count;
                total_storage += size;
                collections.push(CollectionStat {
                    name: name.clone(),
                    doc_count: count,
                    storage_bytes: size,
                });
            }

            Ok(DashboardStats {
                collections,
                total_docs,
                total_storage_bytes: total_storage,
            })
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "list_collections"});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            let names: Vec<String> = resp
                .get("data")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();

            let mut collections = Vec::new();
            let mut total_docs = 0usize;

            for name in &names {
                let count_req = json!({"cmd": "count", "collection": name, "query": {}});
                let count_resp = DbBackend::send_or_reconnect(stream, host, *port, &count_req)?;
                let count = count_resp
                    .pointer("/data/count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as usize;
                total_docs += count;
                collections.push(CollectionStat {
                    name: name.clone(),
                    doc_count: count,
                    storage_bytes: 0,
                });
            }

            Ok(DashboardStats {
                collections,
                total_docs,
                total_storage_bytes: 0,
            })
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}
