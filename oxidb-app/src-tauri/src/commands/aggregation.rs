use std::sync::Mutex;

use serde_json::{Value, json};
use tauri::State;

use crate::state::DbBackend;

#[tauri::command]
pub fn run_aggregation(
    collection: String,
    pipeline: Value,
    db: Option<String>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Vec<Value>, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Embedded { db, .. } => db
            .aggregate(&collection, &pipeline)
            .map_err(|e| e.to_string()),
        DbBackend::Client { stream, host, port, user, password } => {
            let mut req = json!({
                "cmd": "aggregate",
                "collection": collection,
                "pipeline": pipeline,
            });
            crate::state::inject_db(&mut req, &db);
            let resp = DbBackend::send_or_reconnect(stream, host, *port, user.as_deref(), password.as_deref(), &req)?;
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
