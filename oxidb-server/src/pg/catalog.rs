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

use oxidb_sql::{CommandKind, FkAction, SqlType, Value};

use super::errors::{PgError, SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_UNDEFINED_OBJECT};
use super::session::{describe_columns, PgSession, Reply};
use super::types;
use super::wire::FieldDesc;
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

/// Build a canned all-text result.
fn rows_reply(columns: &[&str], rows: Vec<Vec<Value>>) -> Reply {
    let names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
    let types = vec![Some(SqlType::Text); columns.len()];
    let fields = describe_columns(&names, &types, &rows, &[]);
    let tag = Some(CommandKind::Select.tag(rows.len()));
    Reply::Rows { fields, rows, tag }
}

/// Build a canned result whose columns carry **specific** type OIDs.
///
/// The catalog answers need this: a driver reading `pg_type.oid` expects the
/// `oid` type and will refuse a plain `int8`, so these results have to be
/// described with the same OIDs real PostgreSQL would use.
fn typed_rows(columns: &[(&str, i32)], rows: Vec<Vec<Value>>) -> Reply {
    let fields = columns
        .iter()
        .map(|(name, oid)| FieldDesc {
            name: (*name).to_string(),
            type_oid: *oid,
            type_len: types::type_len(*oid),
            format: types::FORMAT_TEXT,
        })
        .collect();
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

    if is_session_command(&norm) {
        return Ok(Some(session_command(session, &norm)?));
    }

    if let Some(replies) = catalog_query(session, &norm, sql)? {
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

/// Catalog and system-function queries. `norm` is the normalized form every
/// match is written against; `sql` is the original, needed where a *value*
/// matters (normalization lowercases string literals along with everything
/// else, and a table name is case-sensitive).
fn catalog_query(
    session: &PgSession,
    norm: &str,
    sql: &str,
) -> Result<Option<Vec<Reply>>, PgError> {
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

    if let Some(reply) = type_catalog(norm) {
        return Ok(Some(vec![reply]));
    }

    // JDBC's DatabaseMetaData. Matched before the per-relation refusal below,
    // which they would otherwise trip: all of them join on `c.oid`.
    //
    // Each match is keyed on an alias unique to that call, not on the tables it
    // reads. Matching loosely here is how `getIndexInfo` once came back holding
    // the *table list*: a wrong answer a caller cannot tell from a right one.
    if norm.contains("self_referencing_col_name") {
        return Ok(Some(vec![jdbc_tables(session, norm, sql)]));
    }
    if norm.contains("attidentity") || norm.contains("partition by a.attrelid") {
        return Ok(Some(vec![jdbc_columns(session, sql)]));
    }
    // getImportedKeys/getExportedKeys also select `key_seq` and `pk_name`, so
    // the foreign-key columns are what tell them apart — without this check,
    // asking for a table's foreign keys came back holding its primary key.
    if norm.contains("fkcolumn_name") || norm.contains("fk_name") {
        return Ok(Some(vec![jdbc_foreign_keys(session, norm, sql)]));
    }
    if norm.contains("key_seq") && norm.contains("pk_name") {
        return Ok(Some(vec![jdbc_primary_keys(session, sql)]));
    }
    if norm.contains("is_array") && norm.contains("typname") {
        return Ok(Some(vec![jdbc_type_info()]));
    }
    // pgjdbc's type cache, loaded before getTypeInfo's own query: name and oid
    // for every type, keyed on the `typrelid = 0` predicate that asks for
    // non-composite ones.
    if norm.contains("typrelid = 0") {
        let rows = SUPPORTED_TYPES
            .iter()
            .map(|(oid, name)| vec![text(*name), Value::Int(*oid as i64)])
            .collect();
        return Ok(Some(vec![typed_rows(
            &[("typname", types::OID_TEXT), ("oid", types::OID_OID)],
            rows,
        )]));
    }
    if norm.contains("index_qualifier") {
        return Ok(Some(vec![jdbc_index_info(session, sql)]));
    }
    if norm.contains("table_catalog") && norm.contains("pg_namespace") {
        // getSchemas: this server has exactly one, and it is not configurable.
        return Ok(Some(vec![typed_rows(
            &[
                ("table_schem", types::OID_TEXT),
                ("table_catalog", types::OID_TEXT),
            ],
            vec![vec![text("public"), Value::Null]],
        )]));
    }
    if norm.contains("pg_settings") && norm.contains("max_index_keys") {
        // Asked before the metadata calls above; the answer is PostgreSQL's
        // own compiled-in default and nothing here depends on it.
        return Ok(Some(vec![typed_rows(
            &[("setting", types::OID_TEXT)],
            vec![vec![text("32")]],
        )]));
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

    // Per-relation introspection — psql's `\d <table>`, and JDBC's
    // `DatabaseMetaData.getTables`/`getColumns` — selects the relation's oid
    // and joins onward from it. Refusing tells the caller why; answering with
    // the table list instead made psql fail on a column it expected to be
    // there ("column number 4 is out of range" — how this was found).
    // (A query that also reads `pg_type` is asking about types, not relations,
    // and gets the general message below instead of this one.)
    if norm.contains("pg_class") && norm.contains("c.oid") && !norm.contains("pg_type") {
        return Err(PgError::new(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "per-table introspection reads the PostgreSQL system catalogs, which OxiDB \
             does not implement — use DESCRIBE <table>, SHOW TABLES or SHOW INDEXES \
             (this is what psql's \\d and JDBC's DatabaseMetaData need)",
        ));
    }

    // psql's \dt — answered from the engine's own catalog. Guarded by what it
    // must *not* mention, so a catalog query about indexes, constraints or
    // settings falls through to the refusal instead of being handed a list of
    // tables under someone else's column names.
    const NOT_A_TABLE_LIST: &[&str] = &[
        "pg_index",
        "pg_attribute",
        "pg_constraint",
        "pg_settings",
        "pg_proc",
        "pg_get_indexdef",
        "indisprimary",
        "information_schema.",
    ];
    if norm.contains("pg_class")
        && norm.contains("relkind")
        && !NOT_A_TABLE_LIST.iter().any(|t| norm.contains(t))
    {
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

/// The three queries a driver runs to learn the server's **type** system.
///
/// Npgsql sends all three in its opening batch and refuses to connect without
/// them (its alternative is `Server Compatibility Mode=NoTypeLoading` in the
/// connection string, which users should not have to know about).
///
/// Unlike the table catalog, the answer here is essentially static: it is the
/// list of types this server has, which does not change. So it is answered
/// from a constant rather than from the engine — and the two follow-up queries
/// (composite fields, enum labels) are answered empty, which is *true*: this
/// server has no composite types and no enums.
fn type_catalog(norm: &str) -> Option<Reply> {
    // Enum labels: none exist.
    if norm.contains("pg_enum") {
        return Some(typed_rows(
            &[("oid", types::OID_OID), ("enumlabel", types::OID_TEXT)],
            Vec::new(),
        ));
    }
    // Fields of free-standing composite types: none exist. (Told apart from
    // pgjdbc's getColumns, which also joins pg_attribute, by the composite
    // predicate `typtype = 'c'`.)
    if norm.contains("pg_attribute") && norm.contains("typtype = 'c'") {
        return Some(typed_rows(
            &[
                ("oid", types::OID_OID),
                ("attname", types::OID_TEXT),
                ("atttypid", types::OID_OID),
            ],
            Vec::new(),
        ));
    }
    // The type list itself. Keyed on `elemtypoid` — the alias this specific
    // query gives its element-type column — not on "mentions pg_type", which
    // also matches JDBC's getTypeInfo and would answer it with the wrong
    // columns instead of refusing.
    if !norm.contains("elemtypoid") {
        return None;
    }
    let rows = SUPPORTED_TYPES
        .iter()
        .map(|(oid, name)| {
            vec![
                text("pg_catalog"),
                Value::Int(*oid as i64),
                text(*name),
                // Every type this server has is a base type: not a range,
                // enum, domain, composite or array.
                text("b"),
                Value::Bool(false),
                // No element type — nothing here is an array or range.
                Value::Null,
            ]
        })
        .collect();
    Some(typed_rows(
        &[
            ("nspname", types::OID_TEXT),
            ("oid", types::OID_OID),
            ("typname", types::OID_TEXT),
            ("typtype", types::OID_CHAR),
            ("typnotnull", types::OID_BOOL),
            ("elemtypoid", types::OID_OID),
        ],
        rows,
    ))
}

/// `DatabaseMetaData.getTables` — a pass-through query, so the result *is* the
/// JDBC row shape and the column names have to be the ones JDBC reads.
///
/// The requested relation kinds are read back out of the predicate pgjdbc
/// builds (`c.relkind = 'r'` for tables, `'v'` for views), so asking for only
/// views does not get tables.
fn jdbc_tables(session: &PgSession, norm: &str, sql: &str) -> Reply {
    let pattern = like_pattern(sql, "relname").unwrap_or_else(|| "%".to_string());
    let want_tables = norm.contains("relkind = 'r'");
    let want_views = norm.contains("relkind = 'v'");

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let row = |name: &str, kind: &str| {
        vec![
            Value::Null, // table_cat — this server has no catalogs
            text("public"),
            text(name),
            text(kind),
            Value::Null, // remarks
            text(""),
            text(""),
            text(""),
            text(""),
            text(""),
        ]
    };
    if want_tables {
        for t in session.engine.list_tables() {
            if like_match(&pattern, &t.name) {
                rows.push(row(&t.name, "TABLE"));
            }
        }
    }
    if want_views {
        for (name, _) in session.engine.list_views() {
            if like_match(&pattern, &name) {
                rows.push(row(&name, "VIEW"));
            }
        }
    }
    rows.sort_by(|a, b| format!("{:?}", a[2]).cmp(&format!("{:?}", b[2])));

    typed_rows(
        &[
            ("table_cat", types::OID_TEXT),
            ("table_schem", types::OID_TEXT),
            ("table_name", types::OID_TEXT),
            ("table_type", types::OID_TEXT),
            ("remarks", types::OID_TEXT),
            ("type_cat", types::OID_TEXT),
            ("type_schem", types::OID_TEXT),
            ("type_name", types::OID_TEXT),
            ("self_referencing_col_name", types::OID_TEXT),
            ("ref_generation", types::OID_TEXT),
        ],
        rows,
    )
}

/// `DatabaseMetaData.getColumns` — unlike getTables this is pgjdbc's *internal*
/// query, whose rows it reshapes itself, reading each field **by name**. So the
/// names below are the contract, and the values are the engine's own schema.
///
/// Views are not included: a view's column types are only knowable by running
/// it, and reporting a guess would be worse than reporting nothing.
fn jdbc_columns(session: &PgSession, sql: &str) -> Reply {
    let table_pattern = like_pattern(sql, "relname").unwrap_or_else(|| "%".to_string());
    let column_pattern = like_pattern(sql, "attname").unwrap_or_else(|| "%".to_string());

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut tables = session.engine.list_tables();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    for t in tables {
        if !like_match(&table_pattern, &t.name) {
            continue;
        }
        for (i, col) in t.columns.iter().filter(|c| !c.dropped).enumerate() {
            if !like_match(&column_pattern, &col.name) {
                continue;
            }
            // A declared length makes it `varchar(n)` rather than unbounded
            // `text` — a schema tool shows the length, and a code generator
            // emits the right column type.
            let oid = match col.max_len {
                Some(_) => types::OID_VARCHAR,
                None => types::oid_of(Some(col.ty)),
            };
            rows.push(vec![
                text("public"),
                text(&t.name),
                text(&col.name),
                Value::Int(oid as i64),
                Value::Bool(!col.nullable),
                // atttypmod carries a declared length: PostgreSQL stores
                // VARCHAR(n) as n + 4 (the varlena header), and -1 for
                // everything unbounded.
                Value::Int(col.max_len.map_or(-1, |n| i64::from(n) + 4)),
                Value::Int(i64::from(types::type_len(oid))),
                Value::Int(-1), // typtypmod
                Value::Int(i as i64 + 1),
                Value::Null, // attidentity — no identity columns
                Value::Null, // attgenerated — no generated columns
                Value::Null, // adsrc — defaults are not rendered as SQL text
                Value::Null, // description — no column comments
                Value::Int(0), // typbasetype — nothing is a domain
                text("b"),     // typtype — every type here is a base type
            ]);
        }
    }

    typed_rows(
        &[
            ("nspname", types::OID_TEXT),
            ("relname", types::OID_TEXT),
            ("attname", types::OID_TEXT),
            ("atttypid", types::OID_OID),
            ("attnotnull", types::OID_BOOL),
            ("atttypmod", types::OID_INT4),
            ("attlen", types::OID_INT2),
            ("typtypmod", types::OID_INT4),
            ("attnum", types::OID_INT4),
            ("attidentity", types::OID_CHAR),
            ("attgenerated", types::OID_CHAR),
            ("adsrc", types::OID_TEXT),
            ("description", types::OID_TEXT),
            ("typbasetype", types::OID_OID),
            ("typtype", types::OID_CHAR),
        ],
        rows,
    )
}

/// `DatabaseMetaData.getPrimaryKeys` — one row per key column, `key_seq`
/// counting from 1, so a composite key reports all of its parts in order.
fn jdbc_primary_keys(session: &PgSession, sql: &str) -> Reply {
    let pattern = like_pattern(sql, "relname")
        .or_else(|| equals_literal(sql, "ct.relname"))
        .unwrap_or_else(|| "%".to_string());

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut tables = session.engine.list_tables();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    for t in tables {
        if !like_match(&pattern, &t.name) {
            continue;
        }
        for (seq, pos) in t.pk_cols().into_iter().enumerate() {
            rows.push(vec![
                Value::Null,
                text("public"),
                text(&t.name),
                text(&t.columns[pos].name),
                Value::Int(seq as i64 + 1),
                text(format!("{}_pkey", t.name)),
            ]);
        }
    }
    typed_rows(
        &[
            ("table_cat", types::OID_TEXT),
            ("table_schem", types::OID_TEXT),
            ("table_name", types::OID_TEXT),
            ("column_name", types::OID_TEXT),
            ("key_seq", types::OID_INT2),
            ("pk_name", types::OID_TEXT),
        ],
        rows,
    )
}

/// `DatabaseMetaData.getIndexInfo` — the table's secondary indexes, plus the
/// primary key, which PostgreSQL also reports as a unique index and a schema
/// tool expects to see. Column-level `UNIQUE` constraints have no named index
/// in this engine and are not reported.
fn jdbc_index_info(session: &PgSession, sql: &str) -> Reply {
    let pattern = like_pattern(sql, "relname")
        .or_else(|| equals_literal(sql, "ct.relname"))
        .unwrap_or_else(|| "%".to_string());

    // JDBC's tableIndexOther.
    const INDEX_TYPE: i64 = 3;
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let indexes = session.engine.list_indexes();
    let mut tables = session.engine.list_tables();
    tables.sort_by(|a, b| a.name.cmp(&b.name));

    for t in tables {
        if !like_match(&pattern, &t.name) {
            continue;
        }
        let mut row = |index: &str, unique: bool, ordinal: usize, column: &str| {
            rows.push(vec![
                Value::Null,
                text("public"),
                text(&t.name),
                Value::Bool(!unique),
                Value::Null, // index_qualifier
                text(index),
                Value::Int(INDEX_TYPE),
                Value::Int(ordinal as i64),
                text(column),
                text("A"),   // asc_or_desc — indexes are ascending here
                Value::Null, // cardinality: not tracked
                Value::Null, // pages: not applicable
                Value::Null, // filter_condition: no partial indexes
            ]);
        };
        for (i, pos) in t.pk_cols().into_iter().enumerate() {
            let name = format!("{}_pkey", t.name);
            row(&name, true, i + 1, &t.columns[pos].name);
        }
        for def in indexes.iter().filter(|d| d.table == t.name) {
            for (i, col) in def.columns.iter().enumerate() {
                row(&def.name, false, i + 1, col);
            }
        }
    }
    typed_rows(
        &[
            ("table_cat", types::OID_TEXT),
            ("table_schem", types::OID_TEXT),
            ("table_name", types::OID_TEXT),
            ("non_unique", types::OID_BOOL),
            ("index_qualifier", types::OID_TEXT),
            ("index_name", types::OID_TEXT),
            ("type", types::OID_INT2),
            ("ordinal_position", types::OID_INT2),
            ("column_name", types::OID_TEXT),
            ("asc_or_desc", types::OID_TEXT),
            ("cardinality", types::OID_INT8),
            ("pages", types::OID_INT8),
            ("filter_condition", types::OID_TEXT),
        ],
        rows,
    )
}

/// `DatabaseMetaData.getImportedKeys` / `getExportedKeys` — the engine's
/// single-column foreign keys. `getExportedKeys` asks the same question from
/// the other side (which children point at *this* table), so the direction is
/// read from which side the query pins down.
fn jdbc_foreign_keys(session: &PgSession, norm: &str, sql: &str) -> Reply {
    // pgjdbc names the two sides `fkt`/`pkt` in its predicate; whichever is
    // constrained to a literal is the table being asked about.
    let child = equals_literal(sql, "fkt.relname").or_else(|| like_pattern(sql, "fkt.relname"));
    let parent = equals_literal(sql, "pkt.relname").or_else(|| like_pattern(sql, "pkt.relname"));
    let _ = norm;

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut tables = session.engine.list_tables();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    for t in &tables {
        if let Some(c) = &child
            && !like_match(c, &t.name)
        {
            continue;
        }
        for (i, fk) in t.foreign_keys.iter().enumerate() {
            if let Some(p) = &parent
                && !like_match(p, &fk.parent_table)
            {
                continue;
            }
            // An unnamed reference resolves to the parent's primary key.
            let parent_column = if fk.parent_column.is_empty() {
                session
                    .engine
                    .table_def(&fk.parent_table)
                    .and_then(|d| d.pk_cols().first().map(|p| d.columns[*p].name.clone()))
                    .unwrap_or_default()
            } else {
                fk.parent_column.clone()
            };
            rows.push(vec![
                Value::Null,
                text("public"),
                text(&fk.parent_table),
                text(parent_column),
                Value::Null,
                text("public"),
                text(&t.name),
                text(&fk.column),
                Value::Int(i as i64 + 1),
                Value::Int(fk_rule(fk.on_update)),
                Value::Int(fk_rule(fk.on_delete)),
                text(format!("{}_{}_fkey", t.name, fk.column)),
                text(format!("{}_pkey", fk.parent_table)),
                // importedKeyNotDeferrable
                Value::Int(7),
            ]);
        }
    }
    typed_rows(
        &[
            ("pktable_cat", types::OID_TEXT),
            ("pktable_schem", types::OID_TEXT),
            ("pktable_name", types::OID_TEXT),
            ("pkcolumn_name", types::OID_TEXT),
            ("fktable_cat", types::OID_TEXT),
            ("fktable_schem", types::OID_TEXT),
            ("fktable_name", types::OID_TEXT),
            ("fkcolumn_name", types::OID_TEXT),
            ("key_seq", types::OID_INT2),
            ("update_rule", types::OID_INT2),
            ("delete_rule", types::OID_INT2),
            ("fk_name", types::OID_TEXT),
            ("pk_name", types::OID_TEXT),
            ("deferrability", types::OID_INT2),
        ],
        rows,
    )
}

/// JDBC's referential-action codes.
fn fk_rule(action: FkAction) -> i64 {
    match action {
        FkAction::Cascade => 0,  // importedKeyCascade
        FkAction::SetNull => 2,  // importedKeySetNull
        FkAction::NoAction => 3, // importedKeyNoAction
    }
}

/// `DatabaseMetaData.getTypeInfo` — the same type list the connect-time
/// catalog reports, in the shape pgjdbc's own reader expects.
fn jdbc_type_info() -> Reply {
    let rows = SUPPORTED_TYPES
        .iter()
        .map(|(oid, name)| {
            vec![
                // Nothing here is an array type.
                Value::Bool(false),
                text("b"),
                text(*name),
                Value::Int(*oid as i64),
            ]
        })
        .collect();
    typed_rows(
        &[
            ("is_array", types::OID_BOOL),
            ("typtype", types::OID_CHAR),
            ("typname", types::OID_TEXT),
            ("oid", types::OID_OID),
        ],
        rows,
    )
}

/// Pull the value out of `<field> = '<value>'` — the form the metadata queries
/// use where they match a table exactly rather than by pattern.
fn equals_literal(sql: &str, field: &str) -> Option<String> {
    let hay = sql.to_ascii_lowercase();
    let at = hay.find(&format!("{field} = "))?;
    let rest = sql[at + field.len() + 3..].trim_start();
    let body = rest.strip_prefix('\'')?;
    let end = body.find('\'')?;
    Some(body[..end].to_string())
}

/// Pull the pattern out of `<field> LIKE '<pattern>'`, from the **original**
/// SQL so the literal keeps its case.
fn like_pattern(sql: &str, field: &str) -> Option<String> {
    let hay = sql.to_ascii_lowercase();
    let needle = format!("{field} like ");
    let mut from = 0;
    loop {
        let at = hay[from..].find(&needle)? + from;
        let rest = &sql[at + needle.len()..];
        let rest = rest.trim_start();
        if let Some(body) = rest.strip_prefix('\'') {
            let end = body.find('\'')?;
            return Some(body[..end].to_string());
        }
        // Not a literal (a bind parameter, say) — keep looking.
        from = at + needle.len();
    }
}

/// SQL `LIKE`: `%` matches any run, `_` any single character. Backtracking is
/// bounded by the pattern length, which comes from a driver, not a user.
fn like_match(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    let (mut pi, mut vi) = (0, 0);
    let (mut star, mut mark) = (usize::MAX, 0);
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = pi;
            mark = vi;
            pi += 1;
        } else if star != usize::MAX {
            pi = star + 1;
            mark += 1;
            vi = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

/// The types this server can produce, by their real PostgreSQL OIDs — so a
/// driver's existing handler for each one applies unchanged.
const SUPPORTED_TYPES: &[(i32, &str)] = &[
    (types::OID_BOOL, "bool"),
    (types::OID_BYTEA, "bytea"),
    (types::OID_CHAR, "char"),
    (types::OID_INT8, "int8"),
    (types::OID_INT2, "int2"),
    (types::OID_INT4, "int4"),
    (types::OID_TEXT, "text"),
    (types::OID_OID, "oid"),
    (types::OID_FLOAT4, "float4"),
    (types::OID_FLOAT8, "float8"),
    (types::OID_VARCHAR, "varchar"),
    (types::OID_TIMESTAMP, "timestamp"),
    (types::OID_TIMESTAMPTZ, "timestamptz"),
    (types::OID_NUMERIC, "numeric"),
];

fn mentions_catalog_table(norm: &str) -> bool {
    [
        "pg_class",
        "pg_attribute",
        "pg_attrdef",
        "pg_constraint",
        "pg_description",
        "pg_enum",
        "pg_namespace",
        "pg_type",
        "pg_range",
        "pg_database",
        "pg_index",
        "pg_proc",
        "pg_roles",
        "pg_settings",
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
