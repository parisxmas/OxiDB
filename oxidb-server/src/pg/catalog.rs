//! Statements PostgreSQL clients send that are *not* queries against the
//! user's data: `SET`, `SHOW`, `version()`, and the `pg_catalog` lookups psql
//! runs behind `\l` and `\dt`.
//!
//! None of these reach the SQL engine. `SET extra_float_digits = 3` is sent by
//! every client before it will talk at all, and the engine's parser rejects
//! `SET` outright — without this module no client completes a connection.
//!
//! The rule for everything else is the one `amqp.rs` follows: answer what is
//! implemented, and **refuse the rest by name**. A `pg_catalog` query answered
//! with a plausible-looking empty result is worse than an error, because the
//! client believes the answer.

use std::collections::BTreeMap;

use oxidb_sql::{CommandKind, SqlType, Value};

use super::errors::{PgError, SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_UNDEFINED_OBJECT};
use super::session::{describe_columns, PgSession, Reply};
use crate::auth::Role;

/// Lowercased, whitespace-collapsed, trailing-semicolon-free — the form every
/// match in this module is written against.
fn normalize(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut last_space = true;
    for ch in sql.trim().chars() {
        if ch.is_whitespace() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.extend(ch.to_lowercase());
            last_space = false;
        }
    }
    out.trim_end_matches([' ', ';']).to_string()
}

/// Build a single-column-per-entry canned result.
fn rows_reply(columns: &[&str], rows: Vec<Vec<Value>>) -> Reply {
    let names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
    let types = vec![Some(SqlType::Text); columns.len()];
    let fields = describe_columns(&names, &types, &rows, &[]);
    let tag = Some(CommandKind::Select.tag(rows.len()));
    Reply::Rows { fields, rows, tag }
}

fn text(s: impl Into<String>) -> Value {
    Value::Text(s.into().into())
}

/// The server version this listener reports. Clients gate features on it, so
/// it names a real PostgreSQL release *and* says what is actually answering.
pub fn server_version() -> String {
    format!("16.0 (OxiDB {})", env!("CARGO_PKG_VERSION"))
}

/// The settings a client may `SHOW`, and which seed `ParameterStatus` at
/// startup. Values a client `SET` later shadow these.
pub fn defaults(session_user: &str, readonly: bool, is_superuser: bool) -> Vec<(String, String)> {
    vec![
        ("server_version".into(), server_version()),
        ("server_encoding".into(), "UTF8".into()),
        ("client_encoding".into(), "UTF8".into()),
        ("DateStyle".into(), "ISO, MDY".into()),
        ("IntervalStyle".into(), "postgres".into()),
        ("TimeZone".into(), "UTC".into()),
        ("integer_datetimes".into(), "on".into()),
        ("standard_conforming_strings".into(), "on".into()),
        ("session_authorization".into(), session_user.into()),
        (
            "is_superuser".into(),
            if is_superuser { "on" } else { "off" }.into(),
        ),
        ("search_path".into(), "public".into()),
        ("transaction_isolation".into(), "read committed".into()),
        (
            "default_transaction_isolation".into(),
            "read committed".into(),
        ),
        (
            "transaction_read_only".into(),
            if readonly { "on" } else { "off" }.into(),
        ),
        (
            "default_transaction_read_only".into(),
            if readonly { "on" } else { "off" }.into(),
        ),
    ]
}

/// The subset of [`defaults`] worth sending as `ParameterStatus` at startup —
/// the ones clients read out of the handshake rather than asking for.
pub const STARTUP_PARAMETERS: &[&str] = &[
    "server_version",
    "server_encoding",
    "client_encoding",
    "DateStyle",
    "IntervalStyle",
    "TimeZone",
    "integer_datetimes",
    "standard_conforming_strings",
    "session_authorization",
    "is_superuser",
];

fn setting(session: &PgSession, name: &str) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    if let Some(v) = session.settings.get(&lower) {
        return Some(v.clone());
    }
    defaults(
        &session.user,
        session.readonly,
        session.role == Role::Admin,
    )
    .into_iter()
    .find(|(k, _)| k.to_ascii_lowercase() == lower)
    .map(|(_, v)| v)
}

/// Handle `sql` here, or hand it on to the engine.
///
/// `Ok(None)` = not ours. `Ok(Some(..))` = fully answered. `Err` = ours, and
/// refused.
pub fn intercept(session: &mut PgSession, sql: &str) -> Result<Option<Vec<Reply>>, PgError> {
    let norm = normalize(sql);
    if norm.is_empty() {
        return Ok(None);
    }

    // A failed transaction accepts exactly two statements, and answers both
    // with ROLLBACK — everything else is refused by the session's guard.
    if session.failed_tx {
        if matches!(norm.as_str(), "commit" | "rollback" | "end" | "abort")
            || norm.starts_with("rollback ")
            || norm.starts_with("commit ")
        {
            session.failed_tx = false;
            if let Some(id) = session.sql_tx.take() {
                session.engine.rollback_session_txn(id);
            }
            return Ok(Some(vec![Reply::Tag("ROLLBACK".into())]));
        }
        return Ok(None); // the guard reports 25P02
    }

    // Several settings in one round trip (pgjdbc's opening volley). Only split
    // when *every* part is interceptable: a semicolon inside a string literal
    // then yields parts that match nothing, and the whole text falls through
    // to the engine untouched.
    if norm.contains(';') {
        let parts: Vec<&str> = norm.split(';').map(str::trim).collect();
        if parts.len() > 1 && parts.iter().all(|p| p.is_empty() || is_session_command(p)) {
            let mut out = Vec::new();
            for p in parts.iter().filter(|p| !p.is_empty()) {
                out.extend(session_command(session, p)?);
            }
            return Ok(Some(out));
        }
        return Ok(None);
    }

    if is_session_command(&norm) {
        return Ok(Some(session_command(session, &norm)?));
    }

    if let Some(replies) = catalog_query(session, &norm)? {
        return Ok(Some(replies));
    }

    Ok(None)
}

/// `SET`/`RESET`/`SHOW`/`DISCARD` — statements about the session, not the data.
fn is_session_command(norm: &str) -> bool {
    norm.starts_with("set ")
        || norm.starts_with("reset ")
        || norm.starts_with("discard ")
        || (norm.starts_with("show ") && !is_engine_show(norm))
}

/// `SHOW TABLES` and friends belong to the engine, not to the GUC table.
fn is_engine_show(norm: &str) -> bool {
    let rest = norm.trim_start_matches("show ").trim();
    rest.starts_with("tables")
        || rest.starts_with("views")
        || rest.starts_with("indexes")
        || rest.starts_with("index")
        || rest.starts_with("procedures")
        || rest.starts_with("columns")
        || rest.starts_with("databases")
        || rest.starts_with("create ")
}

fn session_command(session: &mut PgSession, norm: &str) -> Result<Vec<Reply>, PgError> {
    if let Some(rest) = norm.strip_prefix("set ") {
        // `SET name = value` / `SET name TO value` / `SET TIME ZONE x`.
        // Accepted and remembered, never rejected: a client that cannot set a
        // parameter usually refuses to proceed at all, and none of these
        // change how the engine executes anything.
        let rest = rest.trim_start_matches("session ").trim_start_matches("local ");
        if let Some((name, value)) = rest.split_once('=').or_else(|| rest.split_once(" to ")) {
            let name = name.trim().trim_matches('"').to_ascii_lowercase();
            let value = value.trim().trim_matches('\'').trim_matches('"').to_string();
            session.settings.insert(name, value);
        }
        return Ok(vec![Reply::Tag("SET".into())]);
    }
    if let Some(rest) = norm.strip_prefix("reset ") {
        let name = rest.trim().to_ascii_lowercase();
        if name == "all" {
            session.settings.clear();
        } else {
            session.settings.remove(&name);
        }
        return Ok(vec![Reply::Tag("RESET".into())]);
    }
    if norm.starts_with("discard ") {
        // Connection poolers send DISCARD ALL when handing a connection back.
        session.settings.clear();
        session.portals.clear();
        session.prepared.clear();
        return Ok(vec![Reply::Tag("DISCARD ALL".into())]);
    }
    if let Some(rest) = norm.strip_prefix("show ") {
        let name = rest.trim().trim_matches('"');
        if name == "all" {
            let rows = defaults(&session.user, session.readonly, session.role == Role::Admin)
                .into_iter()
                .map(|(k, v)| {
                    let v = session
                        .settings
                        .get(&k.to_ascii_lowercase())
                        .cloned()
                        .unwrap_or(v);
                    vec![text(k), text(v), text("")]
                })
                .collect();
            return Ok(vec![rows_reply(&["name", "setting", "description"], rows)]);
        }
        return match setting(session, name) {
            Some(v) => Ok(vec![rows_reply(&[name], vec![vec![text(v)]])]),
            None => Err(PgError::new(
                SQLSTATE_UNDEFINED_OBJECT,
                format!("unrecognized configuration parameter \"{name}\""),
            )),
        };
    }
    unreachable!("is_session_command gated this")
}

/// Catalog and system-function queries.
fn catalog_query(session: &PgSession, norm: &str) -> Result<Option<Vec<Reply>>, PgError> {
    // The one-liners clients and REPLs open with.
    let scalar: BTreeMap<&str, String> = BTreeMap::from([
        ("select version()", server_version()),
        ("select pg_catalog.version()", server_version()),
        ("select current_schema()", "public".to_string()),
        ("select current_schema", "public".to_string()),
        ("select current_database()", session.database.clone()),
        ("select current_user", session.user.clone()),
        ("select user", session.user.clone()),
        ("select session_user", session.user.clone()),
        ("select current_catalog", session.database.clone()),
    ]);
    if let Some(v) = scalar.get(norm) {
        let col = norm.trim_start_matches("select ").replace("pg_catalog.", "");
        return Ok(Some(vec![rows_reply(
            &[col.trim_end_matches("()")],
            vec![vec![text(v.clone())]],
        )]));
    }
    if norm == "select pg_backend_pid()" {
        let rows = vec![vec![Value::Int(std::process::id() as i64)]];
        let names = vec!["pg_backend_pid".to_string()];
        let fields = describe_columns(&names, &[Some(SqlType::Int)], &rows, &[]);
        let tag = Some(CommandKind::Select.tag(rows.len()));
        return Ok(Some(vec![Reply::Rows { fields, rows, tag }]));
    }

    if !norm.contains("pg_catalog.") && !mentions_catalog_table(norm) {
        return Ok(None);
    }

    // psql's \l — the SQL engine's databases are opened on demand and not
    // enumerable, so this reports the one this connection is attached to
    // rather than inventing a list.
    if norm.contains("pg_database") {
        let rows = vec![vec![
            text(session.database.clone()),
            text(session.user.clone()),
            text("UTF8"),
            text("C"),
            text("C"),
            text(""),
        ]];
        return Ok(Some(vec![rows_reply(
            &[
                "Name",
                "Owner",
                "Encoding",
                "Collate",
                "Ctype",
                "Access privileges",
            ],
            rows,
        )]));
    }

    // psql's `\d <table>` opens by selecting the relation's oid, and then uses
    // that oid in follow-up queries this server has no answer for. Refusing the
    // first one tells the user why; answering it with the table list would make
    // psql fail on a column it expected to be there ("column number 4 is out of
    // range" — how this was found).
    if norm.contains("pg_class") && norm.contains("c.oid") {
        return Err(PgError::new(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "psql's \\d needs the PostgreSQL system catalogs, which OxiDB does not \
             implement — use DESCRIBE <table> (or \\dt for the table list)",
        ));
    }

    // psql's \dt — answered from the engine's own catalog.
    if norm.contains("pg_class") {
        let tables = session
            .engine
            .execute("SHOW TABLES")
            .map_err(PgError::from)?;
        let mut rows = Vec::new();
        if let Some(oxidb_sql::QueryResult::Select { rows: trows, .. }) = tables.first() {
            for r in trows {
                let name = r.first().cloned().unwrap_or(Value::Null);
                rows.push(vec![
                    text("public"),
                    name,
                    text("table"),
                    text(session.user.clone()),
                ]);
            }
        }
        return Ok(Some(vec![rows_reply(
            &["Schema", "Name", "Type", "Owner"],
            rows,
        )]));
    }

    // Anything else in the catalog: say so, and say what does work. An empty
    // result would be taken as truth.
    Err(PgError::new(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "this query reads the PostgreSQL system catalogs, which OxiDB does not implement — \
         use SHOW TABLES, SHOW INDEXES or DESCRIBE <table> instead",
    ))
}

fn mentions_catalog_table(norm: &str) -> bool {
    [
        "pg_class",
        "pg_attribute",
        "pg_namespace",
        "pg_type",
        "pg_database",
        "pg_index",
        "pg_proc",
        "pg_roles",
        "pg_tablespace",
        "information_schema.",
    ]
    .iter()
    .any(|t| norm.contains(t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_collapses_and_strips() {
        assert_eq!(normalize("  SET   x = 1 ;  "), "set x = 1");
        assert_eq!(normalize("SELECT\n\t1"), "select 1");
    }

    #[test]
    fn session_commands_are_recognized_but_engine_shows_are_not() {
        assert!(is_session_command("set extra_float_digits = 3"));
        assert!(is_session_command("reset all"));
        assert!(is_session_command("show transaction_isolation"));
        assert!(is_session_command("discard all"));
        // These belong to the engine.
        assert!(!is_session_command("show tables"));
        assert!(!is_session_command("show indexes from t"));
        assert!(!is_session_command("select 1"));
    }

    #[test]
    fn a_semicolon_inside_a_literal_does_not_split_a_statement() {
        // Both halves would have to look like session commands to split; an
        // INSERT does not, so the whole text goes to the engine.
        let norm = normalize("insert into t values ('a;b')");
        assert!(norm.contains(';'));
        let parts: Vec<&str> = norm.split(';').map(str::trim).collect();
        assert!(!parts.iter().all(|p| p.is_empty() || is_session_command(p)));
    }

    #[test]
    fn catalog_tables_are_detected() {
        assert!(mentions_catalog_table("select * from pg_class"));
        assert!(mentions_catalog_table("select * from information_schema.tables"));
        assert!(!mentions_catalog_table("select * from users"));
    }
}
