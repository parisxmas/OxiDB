use std::sync::Mutex;

use serde::Deserialize;
use serde_json::{Value, json};
use tauri::State;

use crate::state::DbBackend;

#[derive(Deserialize)]
pub struct FindParams {
    pub collection: String,
    pub query: Option<Value>,
    pub sort: Option<Value>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

#[tauri::command]
pub fn find_documents(
    params: FindParams,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Vec<Value>, String> {
    let mut backend = state.lock().unwrap();
    let query = params.query.unwrap_or(json!({}));

    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let opts = oxidb::query::FindOptions {
                sort: params.sort.as_ref().and_then(|s| {
                    s.as_object().map(|obj| {
                        obj.iter()
                            .map(|(k, v)| {
                                let order = if v.as_i64().unwrap_or(1) == -1 {
                                    oxidb::query::SortOrder::Desc
                                } else {
                                    oxidb::query::SortOrder::Asc
                                };
                                (k.clone(), order)
                            })
                            .collect()
                    })
                }),
                skip: params.skip,
                limit: params.limit,
            };
            db.find_with_options(&params.collection, &query, &opts)
                .map_err(|e| e.to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let mut req = json!({
                "cmd": "find",
                "collection": params.collection,
                "query": query,
            });
            if let Some(sort) = params.sort {
                req["sort"] = sort;
            }
            if let Some(skip) = params.skip {
                req["skip"] = json!(skip);
            }
            if let Some(limit) = params.limit {
                req["limit"] = json!(limit);
            }
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok(resp
                    .get("data")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default())
            } else {
                Err(resp
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error")
                    .to_string())
            }
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

#[tauri::command]
pub fn insert_document(
    collection: String,
    doc: Value,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let id = db.insert(&collection, doc).map_err(|e| e.to_string())?;
            Ok(json!({"id": id}))
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "insert", "collection": collection, "doc": doc});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok(resp.get("data").cloned().unwrap_or(json!(null)))
            } else {
                Err(resp
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error")
                    .to_string())
            }
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

#[tauri::command]
pub fn update_documents(
    collection: String,
    query: Value,
    update: Value,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let count = db
                .update(&collection, &query, &update)
                .map_err(|e| e.to_string())?;
            Ok(json!({"modified": count}))
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({
                "cmd": "update",
                "collection": collection,
                "query": query,
                "update": update,
            });
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok(resp.get("data").cloned().unwrap_or(json!(null)))
            } else {
                Err(resp
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error")
                    .to_string())
            }
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

#[tauri::command]
pub fn delete_documents(
    collection: String,
    query: Value,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let count = db
                .delete(&collection, &query)
                .map_err(|e| e.to_string())?;
            Ok(json!({"deleted": count}))
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({
                "cmd": "delete",
                "collection": collection,
                "query": query,
            });
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok(resp.get("data").cloned().unwrap_or(json!(null)))
            } else {
                Err(resp
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error")
                    .to_string())
            }
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

#[tauri::command]
pub fn count_documents(
    collection: String,
    query: Option<Value>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<usize, String> {
    let mut backend = state.lock().unwrap();
    let query = query.unwrap_or(json!({}));
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.count(&collection, &query).map_err(|e| e.to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "count", "collection": collection, "query": query});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            resp.pointer("/data/count")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize)
                .ok_or_else(|| "invalid response".to_string())
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}
