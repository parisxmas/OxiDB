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
use serde_json::{Value, json};

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

/// Arm a database's SQL engine with a table cap (OxiBase per-project quota,
/// `0` = unlimited). No-op when the SQL engine is disabled or the database has
/// no engine yet (it will be created on first use, defaulting to unlimited until
/// the next request re-applies the cap). Cheap enough to call per request.
pub fn set_table_limit(db_name: &str, max: usize) {
    if let Ok(engine) = engine_for(db_name) {
        engine.set_max_tables(max);
    }
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
pub fn handle_sql(
    cmd: &str,
    request: &Value,
    readonly: bool,
    db_name: &str,
    session_tx: &mut Option<u64>,
) -> Vec<u8> {
    // Engine-aware backup/restore. `cmd` reaches here already gated: "backup"
    // and "restore" are admin-only in the RBAC table (like the document
    // engine's), so no extra role check is needed.
    match cmd {
        "backup" => return sql_backup(request, db_name),
        "restore" => return sql_restore(request),
        "sql" => {}
        _ => return err_bytes("SQL engine requests must use cmd \"sql\""),
    }
    let Some(sql) = request.get("sql").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'sql' field");
    };
    let engine = match engine_for(db_name) {
        Ok(e) => e,
        Err(msg) => return err_bytes(&msg),
    };
    match oxidb_sql::json::execute_json_in_session(
        &engine,
        sql,
        request.get("params"),
        readonly,
        session_tx,
    ) {
        Ok(results) => ok_bytes(results),
        Err(msg) => err_bytes(&msg),
    }
}

/// `{ "engine": "sql", "cmd": "backup", "path": "..." }` — a consistent
/// `.tar.gz` of the SQL database `db_name`'s data directory.
fn sql_backup(request: &Value, db_name: &str) -> Vec<u8> {
    let Some(path) = request.get("path").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'path'");
    };
    let engine = match engine_for(db_name) {
        Ok(e) => e,
        Err(msg) => return err_bytes(&msg),
    };
    match engine.backup(std::path::Path::new(path)) {
        Ok(size_bytes) => ok_bytes(json!({ "path": path, "size_bytes": size_bytes })),
        Err(e) => err_bytes(&e.to_string()),
    }
}

/// `{ "engine": "sql", "cmd": "restore", "archive": "...", "target": "..." }` —
/// extract a SQL backup into an empty target directory. Static: point a fresh
/// engine (or restart the server) at `target` to use the restored database.
fn sql_restore(request: &Value) -> Vec<u8> {
    let Some(archive) = request.get("archive").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'archive'");
    };
    let Some(target) = request.get("target").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'target'");
    };
    match oxidb_sql::SqlEngine::restore(std::path::Path::new(archive), std::path::Path::new(target))
    {
        Ok(()) => ok_bytes(json!({
            "path": target,
            "message": "SQL restore complete; open a fresh SQL engine on this directory to use",
        })),
        Err(e) => err_bytes(&e.to_string()),
    }
}

/// Take a parked interactive transaction's buffered ops (cluster commit
/// path — the ops replicate through Raft and apply on every node).
pub fn take_session_ops(db_name: &str, txn_id: u64) -> Result<serde_json::Value, String> {
    let engine = engine_for(db_name)?;
    engine
        .take_session_txn_ops(txn_id)
        .map_err(|e| format!("sql error: {e}"))
}

/// Apply a replicated buffered commit on this node.
pub fn apply_replicated_sql_ops(db_name: &str, ops: &serde_json::Value) -> Result<(), String> {
    let engine = engine_for(db_name)?;
    engine
        .apply_replicated_txn_ops(ops)
        .map_err(|e| format!("sql error: {e}"))
}

/// Roll back a parked interactive SQL transaction (disconnect cleanup, or
/// entry points that don't carry session state). Safe on stale ids.
pub fn rollback_session_tx(db_name: &str, txn_id: u64) {
    if let Ok(engine) = engine_for(db_name) {
        engine.rollback_session_txn(txn_id);
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

/// True when the SQL engine is enabled and hosts a table named `table` in
/// database `db_name`. Used by the PostgREST surface to decide whether
/// `/rest/v1/{table}` targets a SQL table or a document collection. Returns
/// `false` (never opens anything spuriously fatal) when SQL is disabled or the
/// database has no SQL engine yet.
pub fn sql_table_exists(db_name: &str, table: &str) -> bool {
    engine_for(db_name)
        .map(|e| e.table_def(table).is_some())
        .unwrap_or(false)
}

/// The single-column foreign keys declared on `table` as
/// `(local_column, parent_table, parent_column)`, for PostgREST resource
/// embedding over the SQL engine. Empty when SQL is off or the table has none.
pub fn sql_foreign_keys(db_name: &str, table: &str) -> Vec<(String, String, String)> {
    let Ok(engine) = engine_for(db_name) else {
        return Vec::new();
    };
    match engine.table_def(table) {
        Some(t) => t
            .foreign_keys
            .into_iter()
            .map(|fk| (fk.column, fk.parent_table, fk.parent_column))
            .collect(),
        None => Vec::new(),
    }
}
