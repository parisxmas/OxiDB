use std::collections::HashMap;
use std::sync::Mutex;

use base64::Engine;
use serde_json::{Value, json};
use tauri::State;

use crate::state::DbBackend;

#[tauri::command]
pub fn list_buckets(state: State<'_, Mutex<DbBackend>>) -> Result<Vec<String>, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => Ok(db.list_buckets()),
        DbBackend::Client { stream, host, port } => {
            let resp =
                DbBackend::send_or_reconnect(stream, host, *port, &json!({"cmd": "list_buckets"}))?;
            resp.get("data")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .ok_or_else(|| "invalid response".to_string())
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

#[tauri::command]
pub fn create_bucket(
    name: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.create_bucket(&name).map_err(|e| e.to_string())?;
            Ok("bucket created".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let resp = DbBackend::send_or_reconnect(
                stream,
                host,
                *port,
                &json!({"cmd": "create_bucket", "bucket": name}),
            )?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("bucket created".to_string())
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
pub fn delete_bucket(
    name: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.delete_bucket(&name).map_err(|e| e.to_string())?;
            Ok("bucket deleted".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let resp = DbBackend::send_or_reconnect(
                stream,
                host,
                *port,
                &json!({"cmd": "delete_bucket", "bucket": name}),
            )?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("bucket deleted".to_string())
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
pub fn list_objects(
    bucket: String,
    prefix: Option<String>,
    limit: Option<u64>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Vec<Value>, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => db
            .list_objects(&bucket, prefix.as_deref(), limit.map(|n| n as usize))
            .map_err(|e| e.to_string()),
        DbBackend::Client { stream, host, port } => {
            let mut req = json!({"cmd": "list_objects", "bucket": bucket});
            if let Some(p) = prefix {
                req["prefix"] = json!(p);
            }
            if let Some(l) = limit {
                req["limit"] = json!(l);
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
pub fn put_object(
    bucket: String,
    key: String,
    data_b64: String,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    let ct = content_type.as_deref().unwrap_or("application/octet-stream");
    let meta = metadata.unwrap_or_default();

    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let data = base64::engine::general_purpose::STANDARD
                .decode(&data_b64)
                .map_err(|e| format!("invalid base64: {e}"))?;
            db.put_object(&bucket, &key, &data, ct, meta)
                .map_err(|e| e.to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({
                "cmd": "put_object",
                "bucket": bucket,
                "key": key,
                "data": data_b64,
                "content_type": ct,
                "metadata": meta,
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
pub fn get_object(
    bucket: String,
    key: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            let (data, meta) = db.get_object(&bucket, &key).map_err(|e| e.to_string())?;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
            Ok(json!({"content": encoded, "metadata": meta}))
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "get_object", "bucket": bucket, "key": key});
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
pub fn delete_object(
    bucket: String,
    key: String,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => {
            db.delete_object(&bucket, &key)
                .map_err(|e| e.to_string())?;
            Ok("object deleted".to_string())
        }
        DbBackend::Client { stream, host, port } => {
            let req = json!({"cmd": "delete_object", "bucket": bucket, "key": key});
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &req)?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("object deleted".to_string())
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
pub fn search_objects(
    query: String,
    bucket: Option<String>,
    limit: Option<u64>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Vec<Value>, String> {
    let mut backend = state.lock().unwrap();
    let lim = limit.unwrap_or(10) as usize;
    match &mut *backend {
        DbBackend::Embedded { db, .. } => db
            .search(bucket.as_deref(), &query, lim)
            .map_err(|e| e.to_string()),
        DbBackend::Client { stream, host, port } => {
            let mut req = json!({"cmd": "search", "query": query, "limit": lim});
            if let Some(b) = bucket {
                req["bucket"] = json!(b);
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
