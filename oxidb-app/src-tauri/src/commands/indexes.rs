use std::sync::Mutex;

use serde_json::{Value, json};
use tauri::State;

use crate::state::DbBackend;

#[tauri::command]
pub fn list_indexes(
    collection: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let indexes = db.list_indexes(&collection).map_err(|e| e.to_string())?;
            Ok(json!(indexes))
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "list_indexes", "collection": collection});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok(resp.get("data").cloned().unwrap_or(json!([])))
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
pub fn create_index(
    collection: String,
    field: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.create_index(&collection, &field)
                .map_err(|e| e.to_string())?;
            Ok("index created".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "create_index", "collection": collection, "field": field});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("index created".to_string())
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
pub fn create_unique_index(
    collection: String,
    field: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.create_unique_index(&collection, &field)
                .map_err(|e| e.to_string())?;
            Ok("unique index created".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "create_unique_index", "collection": collection, "field": field});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("unique index created".to_string())
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
pub fn create_composite_index(
    collection: String,
    fields: Vec<String>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let name = db
                .create_composite_index(&collection, fields)
                .map_err(|e| e.to_string())?;
            Ok(name)
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "create_composite_index", "collection": collection, "fields": fields});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok(resp
                    .pointer("/data/index")
                    .and_then(|v| v.as_str())
                    .unwrap_or("composite index created")
                    .to_string())
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
pub fn create_text_index(
    collection: String,
    fields: Vec<String>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.create_text_index(&collection, fields)
                .map_err(|e| e.to_string())?;
            Ok("text index created".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "create_text_index", "collection": collection, "fields": fields});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("text index created".to_string())
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
pub fn drop_index(
    collection: String,
    index: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.drop_index(&collection, &index)
                .map_err(|e| e.to_string())?;
            Ok("index dropped".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "drop_index", "collection": collection, "index": index});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("index dropped".to_string())
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
