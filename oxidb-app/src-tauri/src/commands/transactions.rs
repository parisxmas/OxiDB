use std::sync::Mutex;

use serde_json::{Value, json};
use tauri::State;

use crate::state::DbBackend;

#[tauri::command]
pub fn begin_transaction(
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, active_tx, .. } => {
            if active_tx.is_some() {
                return Err("transaction already active".to_string());
            }
            let tx_id = db.begin_transaction();
            *active_tx = Some(tx_id);
            Ok(json!({"tx_id": tx_id}))
        }
        DbBackend::Client { stream, host, port } => {
            let resp =
                DbBackend::send_or_reconnect(stream, host, *port, &json!({"cmd": "begin_tx"}))?;
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
pub fn commit_transaction(
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, active_tx, .. } => match active_tx.take() {
            Some(tx_id) => {
                db.commit_transaction(tx_id)
                    .map_err(|e| e.to_string())?;
                Ok("committed".to_string())
            }
            None => Err("no active transaction".to_string()),
        },
        DbBackend::Client { stream, host, port } => {
            let resp =
                DbBackend::send_or_reconnect(stream, host, *port, &json!({"cmd": "commit_tx"}))?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("committed".to_string())
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
pub fn rollback_transaction(
    state: State<'_, Mutex<DbBackend>>,
) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, active_tx, .. } => match active_tx.take() {
            Some(tx_id) => {
                let _ = db.rollback_transaction(tx_id);
                Ok("rolled back".to_string())
            }
            None => Err("no active transaction".to_string()),
        },
        DbBackend::Client { stream, host, port } => {
            let resp =
                DbBackend::send_or_reconnect(stream, host, *port, &json!({"cmd": "rollback_tx"}))?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok("rolled back".to_string())
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
