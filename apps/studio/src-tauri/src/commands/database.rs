//! Multiple databases (ADR-0012). The server hosts many isolated databases;
//! these commands list/create/drop them over the wire. Embedded mode is a
//! single database, so it reports just the default and rejects create/drop.

use std::sync::Mutex;

use serde_json::json;
use tauri::State;

use crate::state::DbBackend;

#[tauri::command]
pub fn list_databases(state: State<'_, Mutex<DbBackend>>) -> Result<Vec<String>, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { .. } => Ok(vec!["oxidb".to_string()]),
        DbBackend::Client { stream, host, port, user, password } => {
            let resp = DbBackend::send_or_reconnect(
                stream, host, *port, user.as_deref(), password.as_deref(),
                &json!({"cmd": "list_databases"}),
            )?;
            resp.get("data")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .ok_or_else(|| "invalid response".to_string())
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

fn db_admin(cmd: &str, name: String, state: &State<'_, Mutex<DbBackend>>) -> Result<String, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { .. } => {
            Err("multiple databases require a Remote Server connection".to_string())
        }
        DbBackend::Client { stream, host, port, user, password } => {
            let resp = DbBackend::send_or_reconnect(
                stream, host, *port, user.as_deref(), password.as_deref(),
                &json!({"cmd": cmd, "name": name}),
            )?;
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                Ok(resp.get("data").and_then(|v| v.as_str()).unwrap_or("ok").to_string())
            } else {
                Err(resp.get("error").and_then(|v| v.as_str()).unwrap_or("unknown error").to_string())
            }
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}

#[tauri::command]
pub fn create_database(name: String, state: State<'_, Mutex<DbBackend>>) -> Result<String, String> {
    db_admin("create_database", name, &state)
}

#[tauri::command]
pub fn drop_database(name: String, state: State<'_, Mutex<DbBackend>>) -> Result<String, String> {
    db_admin("drop_database", name, &state)
}
