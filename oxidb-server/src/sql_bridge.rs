//! Bridge between the wire protocol and the standalone SQL engine (ADR-0010).
//!
//! The SQL engine is a *second* engine mounted in the same server process. It
//! owns entirely separate files (`${OXIDB_DATA}/sql` by default) and shares no
//! state with the document engine. It is **off by default** and constructed
//! lazily on first use only when `OXIDB_SQL` is truthy, so a server that never
//! uses SQL pays nothing.
//!
//! Routing (in [`crate::handler::handle_request`]): a request whose `engine`
//! field is `"sql"` — or which uses the reserved `sql` command — is served
//! here. Requests without an `engine` field default to `"doc"` and keep the
//! document path byte-for-byte, so every existing client is unaffected.
//!
//! Access control: the `sql` command is gated at the `ReadWrite` role by
//! [`crate::rbac`] (checked before this handler runs), so the SQL engine
//! requires at least write privileges — there is no read-only SQL role in v1.

use std::sync::{Arc, OnceLock};

use oxidb_sql::SqlEngine;
use serde_json::Value;

use crate::handler::{err_bytes, ok_bytes};

static ENGINE: OnceLock<Option<Arc<SqlEngine>>> = OnceLock::new();

/// Whether an env var is set to a truthy value (`1`/`true`/`yes`/`on`).
fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The lazily-opened SQL engine, or `None` when disabled / failed to open.
fn engine() -> Option<Arc<SqlEngine>> {
    ENGINE
        .get_or_init(|| {
            if !env_truthy("OXIDB_SQL") {
                return None;
            }
            let dir = std::env::var("OXIDB_SQL_DATA").unwrap_or_else(|_| {
                let data =
                    std::env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".to_string());
                format!("{data}/sql")
            });
            match SqlEngine::open(&dir) {
                Ok(e) => {
                    eprintln!("[oxidb] SQL engine enabled at {dir}");
                    Some(Arc::new(e))
                }
                Err(err) => {
                    eprintln!("[oxidb] failed to open SQL engine at {dir}: {err}");
                    None
                }
            }
        })
        .clone()
}

/// Handle a SQL-engine request. `cmd` must be the reserved `"sql"` command.
///
/// Request shape:
/// ```json
/// { "engine": "sql", "cmd": "sql", "sql": "SELECT ...", "params": [ ... ] }
/// ```
/// `params` is optional and binds `?` / `$N` placeholders left-to-right.
/// `readonly` is decided server-side from the session's effective role
/// (Read = SELECT-only); it never comes from the request.
pub fn handle_sql(cmd: &str, request: &Value, readonly: bool) -> Vec<u8> {
    if cmd != "sql" {
        return err_bytes("SQL engine requests must use cmd \"sql\"");
    }
    let Some(sql) = request.get("sql").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'sql' field");
    };
    match execute_json(sql, request.get("params"), readonly) {
        Ok(results) => ok_bytes(results),
        Err(msg) => err_bytes(&msg),
    }
}

/// Execute a SQL string with optional JSON `params` and return the results as
/// JSON (one entry per statement). Shared by the TCP wire handler and the
/// REST `POST /api/sql` endpoint. With `readonly`, only SELECT statements are
/// permitted. (JSON bridging lives in `oxidb_sql::json`, shared with the
/// embedded FFI.)
pub fn execute_json(sql: &str, params: Option<&Value>, readonly: bool) -> Result<Value, String> {
    let Some(engine) = engine() else {
        return Err("SQL engine is not enabled (set OXIDB_SQL=1)".to_string());
    };
    oxidb_sql::json::execute_json(&engine, sql, params, readonly)
}
