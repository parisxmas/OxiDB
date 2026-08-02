use std::sync::Mutex;

use serde_json::{Value, json};
use tauri::State;

use crate::state::DbBackend;

/// Run a SQL statement against the connected server's SQL engine (ADR-0010).
///
/// The SQL engine is a server-side crate (`oxidb-sql`) reached over the wire
/// with `engine: "sql"`. It is not part of the embedded document engine this
/// app links, so an embedded connection reports that clearly rather than
/// pretending to run SQL.
///
/// Returns the raw server envelope: `{"ok": true, "data": [ <per-statement> ]}`
/// where each entry is `{columns, types, rows}` (SELECT), `{affected, ...}`
/// (DML), `{ddl: true}`, or `{transaction: true}`.
#[tauri::command]
pub fn run_sql(
    sql: String,
    params: Option<Value>,
    db: Option<String>,
    state: State<'_, Mutex<DbBackend>>,
) -> Result<Value, String> {
    let mut backend = state.lock().unwrap();
    match &mut *backend {
        DbBackend::Client { stream, host, port, user, password } => {
            let mut request = json!({
                "engine": "sql",
                "cmd": "sql",
                "sql": sql,
            });
            if let Some(p) = params {
                request["params"] = p;
            }
            if let Some(name) = db {
                if !name.is_empty() {
                    request["db"] = json!(name);
                }
            }
            DbBackend::send_or_reconnect(stream, host, *port, user.as_deref(), password.as_deref(), &request)
        }
        DbBackend::Embedded { .. } => Err(
            "SQL runs on a server's SQL engine — connect to a Remote Server \
             (with OXIDB_SQL enabled) to use the SQL editor."
                .to_string(),
        ),
        DbBackend::Disconnected => Err("not connected".to_string()),
    }
}
