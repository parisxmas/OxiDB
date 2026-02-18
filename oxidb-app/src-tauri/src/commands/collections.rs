use std::sync::Mutex;

use serde_json::json;
use tauri::State;

use crate::state::DbBackend;

#[tauri::command]
pub fn list_collections(state: State<'_, Mutex<DbBackend>>) -> Result<Vec<String>, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => Ok(db.list_collections()),
        DbBackend::Client { stream, host, port } => {
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &json!({"cmd": "list_collections"}))?;
            resp.get("data")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .ok_or_else(|| "invalid response".to_string())
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

#[tauri::command]
pub fn create_collection(
    name: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.create_collection(&name)
                .map_err(|e| e.to_string())?;
            Ok("collection created".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let resp = DbBackend::send_or_reconnect(
                stream,
                host,
                *port,
                &json!({"cmd": "create_collection", "collection": name}),
            )?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("collection created".to_string())
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
pub fn drop_collection(
    name: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.drop_collection(&name).map_err(|e| e.to_string())?;
            Ok("collection dropped".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let resp = DbBackend::send_or_reconnect(
                stream,
                host,
                *port,
                &json!({"cmd": "drop_collection", "collection": name}),
            )?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("collection dropped".to_string())
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
pub fn compact_collection(
    name: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<serde_json::Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let stats = db.compact(&name).map_err(|e| e.to_string())?;
            Ok(json!({
                "old_size": stats.old_size,
                "new_size": stats.new_size,
                "docs_kept": stats.docs_kept
            }))
        }
        DbBackend::Client { stream, host, port } => {
            let resp = DbBackend::send_or_reconnect(
                stream,
                host,
                *port,
                &json!({"cmd": "compact", "collection": name}),
            )?;
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
