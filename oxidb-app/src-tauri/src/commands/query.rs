use std::sync::Mutex;

use serde_json::{Value, json};
use tauri::State;

use crate::state::DbBackend;

#[tauri::command]
pub fn execute_raw_command(
    command: Value,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, active_tx, .. } => {
            let cmd = match command.get("cmd").and_then(|v| v.as_str()) {
                Some(c) => c.to_string(),
                None => return Err("missing 'cmd' field".to_string()),
            };

            // Re-use oxidb-server handler logic inline for the common commands
            let collection = command
                .get("collection")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            match cmd.as_str() {
                "ping" => Ok(json!({"ok": true, "data": "pong"})),
                "find" => {
                    let col = collection.as_deref().ok_or("missing 'collection'")?;
                    let empty = json!({});
                    let query = command.get("query").unwrap_or(&empty);
                    let opts = oxidb::query::parse_find_options(&command)
                        .map_err(|e| e.to_string())?;
                    let docs = db
                        .find_with_options(col, query, &opts)
                        .map_err(|e| e.to_string())?;
                    Ok(json!({"ok": true, "data": docs}))
                }
                "find_one" => {
                    let col = collection.as_deref().ok_or("missing 'collection'")?;
                    let empty = json!({});
                    let query = command.get("query").unwrap_or(&empty);
                    let doc = db.find_one(col, query).map_err(|e| e.to_string())?;
                    Ok(json!({"ok": true, "data": doc}))
                }
                "insert" => {
                    let col = collection.as_deref().ok_or("missing 'collection'")?;
                    let doc = command.get("doc").cloned().ok_or("missing 'doc'")?;
                    if let Some(tx_id) = *active_tx {
                        db.tx_insert(tx_id, col, doc).map_err(|e| e.to_string())?;
                        Ok(json!({"ok": true, "data": "buffered"}))
                    } else {
                        let id = db.insert(col, doc).map_err(|e| e.to_string())?;
                        Ok(json!({"ok": true, "data": {"id": id}}))
                    }
                }
                "update" => {
                    let col = collection.as_deref().ok_or("missing 'collection'")?;
                    let query = command.get("query").ok_or("missing 'query'")?;
                    let update = command.get("update").ok_or("missing 'update'")?;
                    let count = db.update(col, query, update).map_err(|e| e.to_string())?;
                    Ok(json!({"ok": true, "data": {"modified": count}}))
                }
                "delete" => {
                    let col = collection.as_deref().ok_or("missing 'collection'")?;
                    let query = command.get("query").ok_or("missing 'query'")?;
                    let count = db.delete(col, query).map_err(|e| e.to_string())?;
                    Ok(json!({"ok": true, "data": {"deleted": count}}))
                }
                "count" => {
                    let col = collection.as_deref().ok_or("missing 'collection'")?;
                    let empty = json!({});
                    let query = command.get("query").unwrap_or(&empty);
                    let n = db.count(col, query).map_err(|e| e.to_string())?;
                    Ok(json!({"ok": true, "data": {"count": n}}))
                }
                "aggregate" => {
                    let col = collection.as_deref().ok_or("missing 'collection'")?;
                    let pipeline = command.get("pipeline").ok_or("missing 'pipeline'")?;
                    let docs = db.aggregate(col, pipeline).map_err(|e| e.to_string())?;
                    Ok(json!({"ok": true, "data": docs}))
                }
                "list_collections" => {
                    let names = db.list_collections();
                    Ok(json!({"ok": true, "data": names}))
                }
                _ => Err(format!("unknown command: {cmd}")),
            }
        }
        DbBackend::Client { stream, host, port } => {
            let resp = DbBackend::send_or_reconnect(stream, host, *port, &command)?;
            Ok(resp)
        }
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}
