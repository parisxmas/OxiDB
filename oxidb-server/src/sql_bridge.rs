//! Bridge between the wire protocol and the standalone SQL engine (ADR-0010).
//!
//! The SQL engine is a *second* engine mounted in the same server process. It
//! owns entirely separate files and shares no state with the document engine.
//! It is **off by default** and constructed lazily on first use only when
//! `OXIDB_SQL` is truthy, so a server that never uses SQL pays nothing.
//!
//! Routing (in [`crate::handler::handle_request_in_db`]): a request whose
//! `engine` field is `"sql"` — or which uses the reserved `sql` command — is
//! served here. Requests without an `engine` field default to `"doc"` and keep
//! the document path byte-for-byte, so every existing client is unaffected.
//!
//! **Databases** (ADR-0012): each database gets its own lazily-opened SQL
//! engine, mirroring the document engine's `DatabaseManager` layout:
//!
//! ```text
//! ${OXIDB_SQL_DATA:-$OXIDB_DATA/sql}   # default database ("oxidb"/"postgres")
//! ${OXIDB_DATA}/<name>/sql             # every other database
//! ```
//!
//! The default database keeps its historical directory so existing SQL data
//! is untouched by the multi-database support.
//!
//! Access control: the `sql` command is gated at the `ReadWrite` role by
//! [`crate::rbac`] (checked before this handler runs), so the SQL engine
//! requires at least write privileges — there is no read-only SQL role in v1.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};

use oxidb::database_manager::DEFAULT_DATABASE;
use oxidb_sql::SqlEngine;
use serde_json::Value;

use crate::handler::{err_bytes, ok_bytes};

/// Per-database SQL engines, keyed by (normalized) database name.
struct Registry {
    /// The document engine's data root (`OXIDB_DATA`) — named databases live
    /// in subdirectories of it.
    root: PathBuf,
    /// The default database's SQL directory (`OXIDB_SQL_DATA`, historically
    /// `${OXIDB_DATA}/sql`).
    default_dir: PathBuf,
    engines: RwLock<HashMap<String, Arc<SqlEngine>>>,
}

static REGISTRY: OnceLock<Option<Registry>> = OnceLock::new();

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

fn registry() -> Option<&'static Registry> {
    REGISTRY
        .get_or_init(|| {
            if !env_truthy("OXIDB_SQL") {
                return None;
            }
            let root = PathBuf::from(
                std::env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".into()),
            );
            let default_dir = std::env::var("OXIDB_SQL_DATA")
                .map(PathBuf::from)
                .unwrap_or_else(|_| root.join("sql"));
            eprintln!(
                "[oxidb] SQL engine enabled (default db at {})",
                default_dir.display()
            );
            Some(Registry {
                root,
                default_dir,
                engines: RwLock::new(HashMap::new()),
            })
        })
        .as_ref()
}

/// `postgres` is an alias for the default database; an empty name means the
/// default too (matches `DatabaseManager::get_database`).
fn normalize(db_name: &str) -> &str {
    if db_name.is_empty() || db_name == "postgres" {
        DEFAULT_DATABASE
    } else {
        db_name
    }
}

/// The lazily-opened SQL engine for one database.
fn engine_for(db_name: &str) -> Result<Arc<SqlEngine>, String> {
    let Some(reg) = registry() else {
        return Err("SQL engine is not enabled (set OXIDB_SQL=1)".to_string());
    };
    let name = normalize(db_name);

    if let Some(e) = reg.engines.read().unwrap().get(name) {
        return Ok(Arc::clone(e));
    }

    let dir = if name == DEFAULT_DATABASE {
        reg.default_dir.clone()
    } else {
        // The database itself must exist (created via `create_database`);
        // its SQL directory is then created on first use.
        let db_dir = reg.root.join(name);
        if !db_dir.is_dir() {
            return Err(format!("database not found: {name}"));
        }
        db_dir.join("sql")
    };

    let engine = SqlEngine::open(&dir)
        .map_err(|e| format!("failed to open SQL engine for database {name:?}: {e}"))?;
    let mut engines = reg.engines.write().unwrap();
    if let Some(existing) = engines.get(name) {
        return Ok(Arc::clone(existing));
    }
    let arc = Arc::new(engine);
    engines.insert(name.to_string(), Arc::clone(&arc));
    Ok(arc)
}

/// Drop a database's SQL engine from the registry (its files go away with the
/// database directory). Called by `drop_database`.
pub fn forget_database(db_name: &str) {
    if let Some(reg) = registry() {
        reg.engines.write().unwrap().remove(normalize(db_name));
    }
}

/// Handle a SQL-engine request against one database. `cmd` must be the
/// reserved `"sql"` command.
///
/// Request shape:
/// ```json
/// { "engine": "sql", "cmd": "sql", "sql": "SELECT ...", "params": [ ... ] }
/// ```
/// `params` is optional and binds `?` / `$N` placeholders left-to-right.
/// `readonly` is decided server-side from the session's effective role
/// (Read = SELECT-only); it never comes from the request. `db_name` was
/// resolved by the session layer (request `db` field, else the session's
/// current database).
pub fn handle_sql(cmd: &str, request: &Value, readonly: bool, db_name: &str) -> Vec<u8> {
    if cmd != "sql" {
        return err_bytes("SQL engine requests must use cmd \"sql\"");
    }
    let Some(sql) = request.get("sql").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'sql' field");
    };
    match execute_json_in(db_name, sql, request.get("params"), readonly) {
        Ok(results) => ok_bytes(results),
        Err(msg) => err_bytes(&msg),
    }
}

/// Execute a SQL string against the default database. Kept for callers that
/// predate multi-database SQL (REST `POST /api/sql`).
pub fn execute_json(sql: &str, params: Option<&Value>, readonly: bool) -> Result<Value, String> {
    execute_json_in(DEFAULT_DATABASE, sql, params, readonly)
}

/// Execute a SQL string with optional JSON `params` against one database's
/// SQL engine and return the results as JSON (one entry per statement).
/// With `readonly`, only SELECT statements are permitted. (JSON bridging
/// lives in `oxidb_sql::json`, shared with the embedded FFI.)
pub fn execute_json_in(
    db_name: &str,
    sql: &str,
    params: Option<&Value>,
    readonly: bool,
) -> Result<Value, String> {
    let engine = engine_for(db_name)?;
    oxidb_sql::json::execute_json(&engine, sql, params, readonly)
}
