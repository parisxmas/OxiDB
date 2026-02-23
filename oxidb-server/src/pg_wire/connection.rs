use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::SystemTime;

use oxidb::{DatabaseManager, OxiDb};

use super::codec::{self, ColumnDef, FrontendMessage};
use super::types;

/// State for a prepared statement (Extended Query Protocol).
struct PreparedStatement {
    sql: String,
}

/// State for a bound portal (Extended Query Protocol).
struct Portal {
    sql: String,
    params: Vec<String>,
}

/// Convert raw bind parameter bytes to strings.
/// Handles both text-format (UTF-8 digits) and binary-format (big-endian int32/int64) parameters.
fn params_to_strings(params: &[Option<Vec<u8>>]) -> Vec<String> {
    params.iter().map(|p| {
        match p.as_ref() {
            None => String::new(),
            Some(b) => {
                // Try text format first (valid UTF-8 string).
                if let Ok(s) = std::str::from_utf8(b) {
                    if !s.is_empty() && (s.chars().all(|c| c.is_ascii_graphic() || c == ' ')) {
                        return s.to_string();
                    }
                }
                // Binary int32 (4 bytes big-endian) — common for OIDs.
                if b.len() == 4 {
                    let val = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
                    return val.to_string();
                }
                // Binary int64 (8 bytes big-endian).
                if b.len() == 8 {
                    let val = i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
                    return val.to_string();
                }
                // Fallback: UTF-8 lossy.
                String::from_utf8_lossy(b).to_string()
            }
        }
    }).collect()
}

/// Handle a PostgreSQL wire protocol connection from startup to termination.
pub fn handle_pg_connection(stream: &TcpStream, db_manager: &Arc<DatabaseManager>) {
    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);

    if let Err(e) = run_connection(&mut reader, &mut writer, db_manager)
        && e.kind() != io::ErrorKind::UnexpectedEof
        && e.kind() != io::ErrorKind::ConnectionReset
        && e.kind() != io::ErrorKind::BrokenPipe
    {
        eprintln!("[pg_wire] connection error: {e}");
    }
    let _ = writer.flush();
}

fn run_connection<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    db_manager: &Arc<DatabaseManager>,
) -> io::Result<()> {
    let db_name = startup_handshake(reader, writer, db_manager)?;
    let db = db_manager.get_database(&db_name).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, e.to_string())
    })?;

    let mut statements: HashMap<String, PreparedStatement> = HashMap::new();
    let mut portals: HashMap<String, Portal> = HashMap::new();

    loop {
        let msg = codec::read_message(reader);
        match msg {
            // ── Simple Query Protocol ────────────────────────────
            // Simple Query sends RowDescription + DataRows + CommandComplete together.
            Ok(FrontendMessage::Query(sql)) => {
                let statements = split_statements(&sql);
                for stmt in &statements {
                    let stmt = stmt.trim();
                    if stmt.is_empty() {
                        continue;
                    }
                    dispatch_query(writer, &db, db_manager, &db_name, stmt, true, &[])?;
                }
                codec::write_ready_for_query(writer, b'I')?;
                writer.flush()?;
            }

            // ── Extended Query Protocol ──────────────────────────
            Ok(FrontendMessage::Parse {
                name,
                sql,
                param_types: _,
            }) => {
                statements.insert(name.clone(), PreparedStatement { sql });
                codec::write_parse_complete(writer)?;
            }
            Ok(FrontendMessage::Bind {
                portal,
                statement,
                param_values,
            }) => {
                let sql = statements
                    .get(&statement)
                    .map(|s| s.sql.clone())
                    .unwrap_or_default();
                portals.insert(portal, Portal { sql, params: params_to_strings(&param_values) });
                codec::write_bind_complete(writer)?;
            }
            // Describe sends RowDescription (or NoData). No data rows.
            Ok(FrontendMessage::Describe { kind, name }) => {
                if kind == b'S' {
                    let param_count = statements
                        .get(&name)
                        .map(|s| codec::count_parameters(&s.sql))
                        .unwrap_or(0);
                    codec::write_parameter_description(writer, param_count)?;
                    if let Some(stmt) = statements.get(&name) {
                        describe_query(writer, &db, &stmt.sql)?;
                    } else {
                        codec::write_no_data(writer)?;
                    }
                } else if let Some(portal) = portals.get(&name) {
                    describe_query(writer, &db, &portal.sql)?;
                } else {
                    codec::write_no_data(writer)?;
                }
            }
            // Execute sends DataRows + CommandComplete. NO RowDescription.
            Ok(FrontendMessage::Execute {
                portal,
                max_rows: _,
            }) => {
                if let Some(p) = portals.get(&portal) {
                    let sql = p.sql.clone();
                    let params = p.params.clone();
                    dispatch_query(writer, &db, db_manager, &db_name, &sql, false, &params)?;
                } else {
                    codec::write_command_complete(writer, "SELECT 0")?;
                }
            }
            Ok(FrontendMessage::Close { kind, name }) => {
                if kind == b'S' {
                    statements.remove(&name);
                } else {
                    portals.remove(&name);
                }
                codec::write_close_complete(writer)?;
            }
            Ok(FrontendMessage::Sync) => {
                codec::write_ready_for_query(writer, b'I')?;
                writer.flush()?;
            }
            Ok(FrontendMessage::Flush) => {
                writer.flush()?;
            }

            // ── Lifecycle ────────────────────────────────────────
            Ok(FrontendMessage::Terminate) => break,
            Ok(_) => {}
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                if e.kind() == io::ErrorKind::Other {
                    continue;
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Perform the PostgreSQL startup handshake.
/// Returns the database name extracted from the startup message.
fn startup_handshake<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    db_manager: &DatabaseManager,
) -> io::Result<String> {
    let mut database = "oxidb".to_string();

    loop {
        match codec::read_startup(reader)? {
            FrontendMessage::SslRequest => {
                writer.write_all(b"N")?;
                writer.flush()?;
            }
            FrontendMessage::Startup(msg) => {
                // Extract database name from startup parameters.
                for (key, value) in &msg.params {
                    if key == "database" && !value.is_empty() {
                        database = value.clone();
                    }
                }
                break;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected message during startup",
                ));
            }
        }
    }

    // Validate the database exists.
    if !db_manager.database_exists(&database) {
        let msg = format!("database \"{}\" does not exist", database);
        codec::write_error_response(writer, "FATAL", "3D000", &msg)?;
        writer.flush()?;
        return Err(io::Error::new(io::ErrorKind::Other, msg));
    }

    codec::write_auth_ok(writer)?;

    codec::write_parameter_status(writer, "server_version", "15.0")?;
    codec::write_parameter_status(writer, "server_encoding", "UTF8")?;
    codec::write_parameter_status(writer, "client_encoding", "UTF8")?;
    codec::write_parameter_status(writer, "DateStyle", "ISO, MDY")?;
    codec::write_parameter_status(writer, "integer_datetimes", "on")?;
    codec::write_parameter_status(writer, "standard_conforming_strings", "on")?;
    codec::write_parameter_status(writer, "TimeZone", "UTC")?;
    codec::write_parameter_status(writer, "is_superuser", "on")?;
    codec::write_parameter_status(writer, "session_authorization", "oxidb")?;

    let pid = std::process::id() as i32;
    codec::write_backend_key_data(writer, pid, 0)?;

    codec::write_ready_for_query(writer, b'I')?;
    writer.flush()?;

    Ok(database)
}

// ── Query dispatch ───────────────────────────────────────────────────
//
// `include_row_desc`:
//   - true  → Simple Query: send RowDescription + DataRows + CommandComplete
//   - false → Extended Query Execute: send DataRows + CommandComplete only

/// Dispatch a query, optionally including RowDescription.
fn dispatch_query<W: Write>(
    writer: &mut W,
    db: &Arc<OxiDb>,
    db_manager: &Arc<DatabaseManager>,
    db_name: &str,
    raw_sql: &str,
    include_row_desc: bool,
    params: &[String],
) -> io::Result<()> {
    let sql = raw_sql.trim().trim_end_matches(';').trim();

    if sql.is_empty() {
        if include_row_desc {
            codec::write_empty_query_response(writer)?;
        } else {
            codec::write_command_complete(writer, "SELECT 0")?;
        }
        return Ok(());
    }

    // Try intercepted queries first.
    if let Some(()) = handle_intercepted_query(writer, db, db_manager, db_name, sql, include_row_desc, params)? {
        return Ok(());
    }

    // Normalize SQL for OxiDB engine: strip schema prefixes, table aliases,
    // and qualified wildcards that PostgreSQL clients send.
    let sql = &normalize_pg_sql(sql, params);

    // Route to OxiDB SQL engine.
    match oxidb::execute_sql(db, sql) {
        Ok(result) => match result {
            oxidb::SqlResult::ShowDatabases(names) => {
                let docs: Vec<serde_json::Value> = names
                    .into_iter()
                    .map(|n| serde_json::json!({"database_name": n}))
                    .collect();
                write_sql_result(writer, oxidb::SqlResult::Select(docs), include_row_desc)
            }
            oxidb::SqlResult::UseDatabase(_name) => {
                // In PG wire mode, USE is not standard. Just acknowledge.
                codec::write_command_complete(writer, "SET")?;
                Ok(())
            }
            other => write_sql_result(writer, other, include_row_desc),
        },
        Err(e) => {
            let (code, severity) = error_to_sqlstate(&e);
            codec::write_error_response(writer, severity, code, &e.to_string())?;
            Ok(())
        }
    }
}

/// Describe a query — send RowDescription or NoData. No data rows.
fn describe_query<W: Write>(writer: &mut W, db: &Arc<OxiDb>, raw_sql: &str) -> io::Result<()> {
    let sql = raw_sql.trim().trim_end_matches(';').trim();

    if sql.is_empty() {
        codec::write_no_data(writer)?;
        return Ok(());
    }

    let upper = normalize_whitespace(&sql.to_uppercase());
    let upper = upper.trim();

    // Non-SELECT commands → NoData
    if upper.starts_with("SET ")
        || upper.starts_with("RESET ")
        || upper.starts_with("BEGIN")
        || upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK")
        || upper.starts_with("DEALLOCATE")
        || upper == "DISCARD ALL"
        || upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
        || upper.starts_with("DROP")
    {
        codec::write_no_data(writer)?;
        return Ok(());
    }

    // For intercepted SELECT/catalog queries, return proper column descriptions.
    if let Some(cols) = describe_intercepted_columns(sql) {
        codec::write_row_description(writer, &cols)?;
        return Ok(());
    }

    // For real SELECT queries, try to infer from execution.
    // Normalize PG-flavored SQL (schema prefixes, aliases) for the OxiDB engine.
    // Use LIMIT 1 for inference to avoid loading massive datasets (e.g. embedding vectors).
    if upper.starts_with("SELECT") || upper.starts_with("SHOW") {
        let normalized = normalize_pg_sql(sql, &[]);
        let infer_sql = limit_for_inference(&normalized);
        match oxidb::execute_sql(db, &infer_sql) {
            Ok(oxidb::SqlResult::Select(rows)) => {
                let columns = types::infer_columns(&rows);
                codec::write_row_description(writer, &columns)?;
            }
            _ => {
                codec::write_no_data(writer)?;
            }
        }
    } else {
        codec::write_no_data(writer)?;
    }
    Ok(())
}

// ── Intercepted query handling ───────────────────────────────────────

/// Check if a query should be intercepted.
/// `include_row_desc`: whether to include RowDescription (Simple Query) or not (Execute).
fn handle_intercepted_query<W: Write>(
    writer: &mut W,
    db: &Arc<OxiDb>,
    db_manager: &Arc<DatabaseManager>,
    db_name: &str,
    sql: &str,
    include_row_desc: bool,
    params: &[String],
) -> io::Result<Option<()>> {
    let upper = normalize_whitespace(&sql.to_uppercase());
    let upper = upper.trim();

    // SET / RESET commands
    if upper.starts_with("SET ") {
        codec::write_command_complete(writer, "SET")?;
        return Ok(Some(()));
    }
    if upper.starts_with("RESET ") {
        codec::write_command_complete(writer, "RESET")?;
        return Ok(Some(()));
    }
    if upper == "DISCARD ALL" {
        codec::write_command_complete(writer, "DISCARD ALL")?;
        return Ok(Some(()));
    }

    // Transaction stubs
    if upper == "BEGIN"
        || upper.starts_with("BEGIN ")
        || upper.starts_with("START TRANSACTION")
    {
        codec::write_command_complete(writer, "BEGIN")?;
        return Ok(Some(()));
    }
    if upper == "COMMIT" || upper.starts_with("COMMIT ") || upper.starts_with("END") {
        codec::write_command_complete(writer, "COMMIT")?;
        return Ok(Some(()));
    }
    if upper == "ROLLBACK" || upper.starts_with("ROLLBACK ") || upper.starts_with("ABORT") {
        codec::write_command_complete(writer, "ROLLBACK")?;
        return Ok(Some(()));
    }

    // DEALLOCATE / LISTEN / UNLISTEN / CLOSE
    if upper.starts_with("DEALLOCATE ") {
        codec::write_command_complete(writer, "DEALLOCATE")?;
        return Ok(Some(()));
    }
    if upper.starts_with("LISTEN ") || upper.starts_with("UNLISTEN ") {
        codec::write_command_complete(writer, "LISTEN")?;
        return Ok(Some(()));
    }
    if upper.starts_with("CLOSE ") {
        codec::write_command_complete(writer, "CLOSE")?;
        return Ok(Some(()));
    }

    // txid_current() — return a fake transaction ID as a numeric value.
    if upper.contains("TXID_CURRENT") {
        let epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let txid = epoch % 4_294_967_296; // match the modulo DataGrip expects
        let col_name = if let Some(idx) = upper.rfind(" AS ") {
            sql[idx + 4..].trim().trim_matches('"').to_string()
        } else {
            "txid_current".to_string()
        };
        send_row(writer, &col_name, &txid.to_string(), include_row_desc)?;
        return Ok(Some(()));
    }

    // pg_postmaster_start_time() — return server epoch as a numeric value.
    if upper.contains("PG_POSTMASTER_START_TIME") {
        let epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let col_name = if let Some(idx) = upper.rfind(" AS ") {
            sql[idx + 4..].trim().trim_matches('"').to_string()
        } else {
            "startup_time".to_string()
        };
        send_row(writer, &col_name, &epoch.to_string(), include_row_desc)?;
        return Ok(Some(()));
    }

    // SELECT without a real FROM table clause — covers:
    // - Simple literals: SELECT 1, SELECT 'hello'
    // - Single/multi-column function calls: SELECT version(), SELECT current_database(), current_schema()
    // - Mixed expressions: SELECT current_database() AS a, current_schemas(false) AS b
    if upper.starts_with("SELECT ") && !has_from_table_clause(upper) {
        // Try simple literal first (SELECT 1, SELECT 'hello')
        if let Some(result) = try_select_literal(sql) {
            send_row(writer, &result.0, &result.1, include_row_desc)?;
            return Ok(Some(()));
        }
        // Try multi-column expression evaluation
        if let Some(()) = handle_select_expressions(writer, db_name, sql, include_row_desc)? {
            return Ok(Some(()));
        }
    }

    // SHOW server_version
    if upper == "SHOW SERVER_VERSION" {
        send_row(writer, "server_version", "15.0", include_row_desc)?;
        return Ok(Some(()));
    }

    // SHOW commands
    if upper.starts_with("SHOW ") {
        let param = sql[5..].trim().trim_end_matches(';').trim();
        let value = match param.to_lowercase().as_str() {
            "transaction_isolation" | "transaction isolation level" => "read committed",
            "standard_conforming_strings" => "on",
            "client_encoding" => "UTF8",
            "server_encoding" => "UTF8",
            "lc_collate" | "lc_ctype" => "en_US.UTF-8",
            "is_superuser" => "on",
            "session_authorization" => "oxidb",
            "datestyle" => "ISO, MDY",
            "intervalstyle" => "postgres",
            "timezone" => "UTC",
            "integer_datetimes" => "on",
            "max_identifier_length" => "63",
            "search_path" => "\"$user\", public",
            _ => "",
        };
        send_row(writer, param, value, include_row_desc)?;
        return Ok(Some(()));
    }

    // pg_catalog.set_config(...)
    if upper.contains("SET_CONFIG") || upper.contains("PG_CATALOG.SET_CONFIG") {
        send_row(writer, "set_config", "", include_row_desc)?;
        return Ok(Some(()));
    }

    // SHOW DATABASES
    if upper == "SHOW DATABASES" {
        let names = db_manager.list_databases();
        let cols = vec![text_col("database_name")];
        if include_row_desc {
            codec::write_row_description(writer, &cols)?;
        }
        for name in &names {
            codec::write_data_row(writer, &[Some(name.clone())])?;
        }
        codec::write_command_complete(writer, &format!("SELECT {}", names.len()))?;
        return Ok(Some(()));
    }

    // Catalog queries
    if let Some(()) = handle_catalog_query(writer, db, db_manager, db_name, sql, &upper, include_row_desc, params)? {
        return Ok(Some(()));
    }

    Ok(None)
}

/// Handle catalog/system queries (pg_catalog, information_schema, etc.).
fn handle_catalog_query<W: Write>(
    writer: &mut W,
    db: &Arc<OxiDb>,
    db_manager: &Arc<DatabaseManager>,
    db_name: &str,
    _sql: &str,
    upper: &str,
    desc: bool,
    params: &[String],
) -> io::Result<Option<()>> {
    // ── Schema probe queries (WHERE 1<>1 or WHERE 0=1) — return 0 rows ──
    // Clients send these to discover column structure without fetching data.
    // Parse the actual SQL columns so clients (DataGrip, DBeaver) get correct metadata.
    if upper.contains("WHERE 1<>1") || upper.contains("WHERE 0=1") {
        let cols = extract_select_columns(_sql);
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── string_agg / pg_get_keywords() — return empty ──
    if upper.contains("PG_GET_KEYWORDS") || upper.contains("STRING_AGG") {
        send_row(writer, "string_agg", "", desc)?;
        return Ok(Some(()));
    }

    // ── format_type function — return empty text ──
    // Only match standalone format_type queries (no real FROM table clause),
    // not queries that use format_type() as a column expression (e.g., pg_sequence).
    if upper.contains("FORMAT_TYPE") && !upper.contains("PG_TYPE")
        && !has_from_table_clause(upper)
    {
        send_row(writer, "format_type", "", desc)?;
        return Ok(Some(()));
    }

    // ── ACL union queries (MUST be first — they reference multiple catalogs) ──
    if upper.contains("SPCACL") || upper.contains("DATACL") || upper.contains("NSPACL")
        || upper.contains("RELACL") || upper.contains("PROACL")
    {
        let cols = vec![int_col("object_id"), text_col("acl")];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_locks ────────────────────────────────────────
    if upper.contains("PG_LOCKS") {
        let cols = vec![int_col("transaction_id")];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_database ──────────────────────────────────────
    if upper.contains("PG_DATABASE") {
        // Simple encoding-only query (no multi-column introspection).
        if upper.contains("PG_ENCODING_TO_CHAR")
            && !upper.contains("DATISTEMPLATE")
            && !upper.contains("DATALLOWCONN")
        {
            send_row(writer, "pg_encoding_to_char", "UTF8", desc)?;
            return Ok(Some(()));
        }

        // Joined with pg_shdescription — return empty for the join part.
        // Also catch any other joined catalog query that references pg_database.
        if upper.contains("PG_SHDESCRIPTION") || upper.contains("PG_DESCRIPTION") {
            // DataGrip query: SELECT N.oid::bigint as id, datname as name,
            //   D.description, datistemplate as is_template, datallowconn as allow_connections,
            //   pg_catalog.pg_get_userbyid(N.datdba) as "owner"
            //   FROM pg_catalog.pg_database N LEFT JOIN pg_catalog.pg_shdescription D ...
            let cols = vec![
                int_col("id"),
                text_col("name"),
                text_col("description"),
                bool_col("is_template"),
                bool_col("allow_connections"),
                text_col("owner"),
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            let mut databases = db_manager.list_databases();
            // Sort: connected database first (matches DataGrip's ORDER BY current_database()).
            databases.sort_by(|a, b| {
                if a == db_name { std::cmp::Ordering::Less }
                else if b == db_name { std::cmp::Ordering::Greater }
                else { a.cmp(b) }
            });
            let count = databases.len();
            for (i, dbn) in databases.iter().enumerate() {
                let oid = (16384 + i).to_string();
                codec::write_data_row(writer, &[
                    Some(oid),                    // id
                    Some(dbn.clone()),            // name
                    None,                         // description (no pg_shdescription)
                    Some("f".to_string()),        // is_template
                    Some("t".to_string()),        // allow_connections
                    Some("oxidb".to_string()),    // owner
                ])?;
            }
            codec::write_command_complete(writer, &format!("SELECT {count}"))?;
            return Ok(Some(()));
        }

        // Standard pg_database query (JDBC getCatalogs, psql \l, etc.)
        // DBeaver sends `db.oid,db.*` — in PG 15 oid is a regular column so
        // db.* already includes it.  We prepend an extra oid column when the
        // query explicitly selects `DB.OID` before `DB.*`.
        let has_explicit_oid = upper.contains("DB.OID") && upper.contains("DB.*");
        let mut cols = Vec::new();
        if has_explicit_oid {
            cols.push(int_col("oid")); // explicit db.oid
        }
        cols.extend_from_slice(&[
            int_col("oid"),
            text_col("datname"),
            int_col("datdba"),
            int_col("encoding"),
            text_col("datlocprovider"),
            bool_col("datistemplate"),
            bool_col("datallowconn"),
            int_col("datconnlimit"),
            int_col("datfrozenxid"),
            int_col("datminmxid"),
            int_col("dattablespace"),
            text_col("datcollate"),
            text_col("datctype"),
            text_col("daticulocale"),
            text_col("daticurules"),
            text_col("datcollversion"),
            text_col("datacl"),
        ]);
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        // When query has $1 parameter (DBeaver's single-database lookup),
        // return only the connected database. Otherwise return all.
        let mut databases = if upper.contains("$1") {
            vec![db_name.to_string()]
        } else {
            db_manager.list_databases()
        };
        // Sort: connected database first.
        databases.sort_by(|a, b| {
            if a == db_name { std::cmp::Ordering::Less }
            else if b == db_name { std::cmp::Ordering::Greater }
            else { a.cmp(b) }
        });
        let count = databases.len();
        for (i, name) in databases.iter().enumerate() {
            let oid = (16384 + i).to_string();
            let mut row = Vec::new();
            if has_explicit_oid {
                row.push(Some(oid.clone())); // explicit db.oid
            }
            row.extend_from_slice(&[
                Some(oid),                    // oid
                Some(name.clone()),           // datname
                Some("10".to_string()),       // datdba (owner OID)
                Some("6".to_string()),        // encoding (6 = UTF8)
                Some("c".to_string()),        // datlocprovider
                Some("f".to_string()),        // datistemplate
                Some("t".to_string()),        // datallowconn
                Some("-1".to_string()),       // datconnlimit
                Some("722".to_string()),      // datfrozenxid
                Some("1".to_string()),        // datminmxid
                Some("1663".to_string()),     // dattablespace (pg_default)
                Some("en_US.UTF-8".to_string()), // datcollate
                Some("en_US.UTF-8".to_string()), // datctype
                None,                         // daticulocale
                None,                         // daticurules
                None,                         // datcollversion
                None,                         // datacl
            ]);
            codec::write_data_row(writer, &row)?;
        }
        codec::write_command_complete(writer, &format!("SELECT {count}"))?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_settings ──────────────────────────────────────
    if upper.contains("PG_SETTINGS") {
        if upper.contains("MAX_INDEX_KEYS") {
            send_row(writer, "setting", "32", desc)?;
            return Ok(Some(()));
        }
        // Return real pg_settings rows when queried by name ($1 param or WHERE name=).
        // DBeaver/DataGrip JDBC drivers query standard_conforming_strings this way.
        let settings_map: &[(&str, &str)] = &[
            ("standard_conforming_strings", "on"),
            ("search_path", "\"$user\", public"),
            ("client_encoding", "UTF8"),
            ("server_encoding", "UTF8"),
            ("server_version", "15.0"),
            ("integer_datetimes", "on"),
            ("DateStyle", "ISO, MDY"),
            ("IntervalStyle", "postgres"),
            ("TimeZone", "UTC"),
            ("lc_collate", "en_US.UTF-8"),
            ("lc_ctype", "en_US.UTF-8"),
            ("is_superuser", "on"),
            ("session_authorization", "oxidb"),
            ("transaction_isolation", "read committed"),
        ];
        // Check if a specific setting is requested via $1 parameter.
        let wanted = params.first().map(|s| s.to_lowercase());
        let matching: Vec<_> = settings_map.iter()
            .filter(|(name, _)| wanted.as_ref().is_none() || wanted.as_deref() == Some(&name.to_lowercase()))
            .collect();
        // pg_settings has many columns; return the essential ones that clients use.
        let cols = vec![
            text_col("name"), text_col("setting"), text_col("unit"),
            text_col("category"), text_col("short_desc"), text_col("extra_desc"),
            text_col("context"), text_col("vartype"), text_col("source"),
            text_col("min_val"), text_col("max_val"), text_col("enumvals"),
            text_col("boot_val"), text_col("reset_val"),
            text_col("sourcefile"), int_col("sourceline"),
            bool_col("pending_restart"),
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        let count = matching.len();
        for (name, value) in &matching {
            codec::write_data_row(writer, &[
                Some(name.to_string()),   // name
                Some(value.to_string()),  // setting
                None,                     // unit
                Some("Preset Options".to_string()), // category
                None,                     // short_desc
                None,                     // extra_desc
                Some("internal".to_string()), // context
                Some("string".to_string()),   // vartype
                Some("default".to_string()),  // source
                None,                     // min_val
                None,                     // max_val
                None,                     // enumvals
                Some(value.to_string()),  // boot_val
                Some(value.to_string()),  // reset_val
                None,                     // sourcefile
                None,                     // sourceline
                Some("f".to_string()),    // pending_restart
            ])?;
        }
        codec::write_command_complete(writer, &format!("SELECT {count}"))?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_type ──────────────────────────────────────────
    // Exclude PG_CAST: cast queries join pg_type but should not return type data.
    if upper.contains("PG_TYPE") && !upper.contains("PG_TYPEOF") && !upper.contains("PG_CAST") {
        // DBeaver's type query: uses `t.oid,t.*` with format_type() and joins
        // pg_class, pg_description. Return type rows with native PG columns.
        // Only match T.* (DBeaver pattern); FORMAT_TYPE alone is too broad
        // (DataGrip uses format_type() but expects its own 12-column layout).
        if upper.contains("T.*") {
            let cols = vec![
                int_col("oid"),
                int_col("oid"),       // from t.*
                text_col("typname"),
                int_col("typnamespace"),
                int_col("typowner"),
                int_col("typlen"),
                bool_col("typbyval"),
                text_col("typtype"),
                text_col("typcategory"),
                bool_col("typispreferred"),
                bool_col("typisdefined"),
                text_col("typdelim"),
                int_col("typrelid"),
                int_col("typsubscript"),
                int_col("typelem"),
                int_col("typarray"),
                text_col("typinput"),
                text_col("typoutput"),
                text_col("typreceive"),
                text_col("typsend"),
                text_col("typmodin"),
                text_col("typmodout"),
                text_col("typanalyze"),
                text_col("typalign"),
                text_col("typstorage"),
                bool_col("typnotnull"),
                int_col("typbasetype"),
                int_col("typtypmod"),
                int_col("typndims"),
                int_col("typcollation"),
                text_col("typdefaultbin"),
                text_col("typdefault"),
                text_col("typacl"),
                text_col("relkind"),          // from c.relkind
                text_col("base_type_name"),   // from format_type()
                text_col("description"),      // from d.description
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            // Return core PostgreSQL types
            let type_rows: &[(&str, &str, &str, &str, &str)] = &[
                ("16",   "bool",        "b", "B", "1"),
                ("17",   "bytea",       "b", "U", "-1"),
                ("20",   "int8",        "b", "N", "8"),
                ("21",   "int2",        "b", "N", "2"),
                ("23",   "int4",        "b", "N", "4"),
                ("25",   "text",        "b", "S", "-1"),
                ("26",   "oid",         "b", "N", "4"),
                ("114",  "json",        "b", "U", "-1"),
                ("142",  "xml",         "b", "U", "-1"),
                ("700",  "float4",      "b", "N", "4"),
                ("701",  "float8",      "b", "N", "8"),
                ("1042", "bpchar",      "b", "S", "-1"),
                ("1043", "varchar",     "b", "S", "-1"),
                ("1082", "date",        "b", "D", "4"),
                ("1083", "time",        "b", "D", "8"),
                ("1114", "timestamp",   "b", "D", "8"),
                ("1184", "timestamptz", "b", "D", "8"),
                ("1700", "numeric",     "b", "N", "-1"),
                ("2950", "uuid",        "b", "U", "16"),
                ("3802", "jsonb",       "b", "U", "-1"),
            ];
            for (oid, name, typtype, typcategory, typlen) in type_rows {
                let row = vec![
                    Some(oid.to_string()),     // explicit t.oid
                    Some(oid.to_string()),     // oid from t.*
                    Some(name.to_string()),    // typname
                    Some("11".to_string()),    // typnamespace (pg_catalog)
                    Some("10".to_string()),    // typowner
                    Some(typlen.to_string()),  // typlen
                    Some("t".to_string()),     // typbyval
                    Some(typtype.to_string()), // typtype
                    Some(typcategory.to_string()), // typcategory
                    Some("f".to_string()),     // typispreferred
                    Some("t".to_string()),     // typisdefined
                    Some(",".to_string()),     // typdelim
                    Some("0".to_string()),     // typrelid
                    Some("0".to_string()),     // typsubscript
                    Some("0".to_string()),     // typelem
                    Some("0".to_string()),     // typarray
                    Some(format!("{name}in")), // typinput
                    Some(format!("{name}out")),// typoutput
                    Some(format!("{name}recv")),// typreceive
                    Some(format!("{name}send")),// typsend
                    Some("-".to_string()),     // typmodin
                    Some("-".to_string()),     // typmodout
                    Some("-".to_string()),     // typanalyze
                    Some("c".to_string()),     // typalign
                    Some("p".to_string()),     // typstorage
                    Some("f".to_string()),     // typnotnull
                    Some("0".to_string()),     // typbasetype
                    Some("-1".to_string()),    // typtypmod
                    Some("0".to_string()),     // typndims
                    Some("0".to_string()),     // typcollation
                    None,                      // typdefaultbin
                    None,                      // typdefault
                    None,                      // typacl
                    None,                      // relkind (from c)
                    None,                      // base_type_name (format_type)
                    None,                      // description (from d)
                ];
                codec::write_data_row(writer, &row)?;
            }
            let count = type_rows.len();
            codec::write_command_complete(writer, &format!("SELECT {count}"))?;
            return Ok(Some(()));
        }

        // DataGrip's complex type query — return empty to avoid auto-generated casts
        // Use precise match: "TYPE_NAME" as alias (not "BASE_TYPE_NAME")
        let has_type_name_alias = upper.contains(" AS TYPE_NAME")
            || upper.contains(" AS \"TYPE_NAME\"")
            || (upper.contains("TYPE_NAME") && !upper.contains("BASE_TYPE_NAME"));
        if has_type_name_alias || upper.contains("TYPCATEGORY")
            || (upper.contains("TYPTYPE") && upper.contains("PG_CLASS"))
        {
            let cols = vec![
                int_col("type_id"), int_col("type_state_number"),
                text_col("type_name"), text_col("type_sub_kind"),
                text_col("type_category"), int_col("class_id"),
                int_col("base_type_id"), text_col("type_def"),
                int_col("dimensions_number"), text_col("default_expression"),
                bool_col("mandatory"), text_col("owner"),
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            codec::write_command_complete(writer, "SELECT 0")?;
            return Ok(Some(()));
        }
        // TYPNAME as a column reference (not just part of TYPNAMESPACE)
        let has_typname_col = upper.replace("TYPNAMESPACE", "").contains("TYPNAME");
        if has_typname_col && upper.contains("OID") {
            let cols = vec![
                int_col("oid"),
                text_col("typname"),
                int_col("typnamespace"),
                text_col("typtype"),
                int_col("typbasetype"),
                int_col("typrelid"),
                int_col("typtypmod"),
                text_col("typnotnull"),
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            let type_rows: &[(&str, &str)] = &[
                ("16", "bool"),
                ("17", "bytea"),
                ("20", "int8"),
                ("21", "int2"),
                ("23", "int4"),
                ("25", "text"),
                ("26", "oid"),
                ("114", "json"),
                ("142", "xml"),
                ("700", "float4"),
                ("701", "float8"),
                ("1042", "bpchar"),
                ("1043", "varchar"),
                ("1082", "date"),
                ("1083", "time"),
                ("1114", "timestamp"),
                ("1184", "timestamptz"),
                ("1700", "numeric"),
                ("2950", "uuid"),
                ("3802", "jsonb"),
            ];
            for (oid, name) in type_rows {
                codec::write_data_row(writer, &[
                    Some(oid.to_string()),
                    Some(name.to_string()),
                    Some("11".to_string()),
                    Some("b".to_string()),
                    Some("0".to_string()),
                    Some("0".to_string()),
                    Some("-1".to_string()),
                    Some("f".to_string()),
                ])?;
            }
            let count = type_rows.len();
            codec::write_command_complete(writer, &format!("SELECT {count}"))?;
            return Ok(Some(()));
        }
        // Generic pg_type fallback — use DataGrip column names
        let cols = vec![
            int_col("type_id"), int_col("type_state_number"),
            text_col("type_name"), text_col("type_sub_kind"),
            text_col("type_category"), int_col("class_id"),
            int_col("base_type_id"), text_col("type_def"),
            int_col("dimensions_number"), text_col("default_expression"),
            bool_col("mandatory"), text_col("owner"),
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_attribute — expose document fields as columns ──
    if upper.contains("PG_ATTRIBUTE") && upper.contains("C.OID=$1") {
        // DBeaver column query: gets attributes for a specific table OID ($1).
        // Introspect collection documents and generate columns from JSON keys.
        let table_oid: u64 = params.first().and_then(|p| p.parse().ok()).unwrap_or(0);
        let collections = db.list_collections();
        let table_idx = if table_oid >= 16384 { (table_oid - 16384) as usize } else { usize::MAX };
        let table_name = collections.get(table_idx).cloned();

        // DBeaver expects: c.relname, a.* (all pg_attribute cols), def_value, description, dep.objid
        let cols = vec![
            text_col("relname"),       // c.relname
            int_col("attrelid"),       // from a.*
            text_col("attname"),
            int_col("atttypid"),
            int_col("attlen"),
            int_col("attnum"),
            int_col("atttypmod"),
            bool_col("attnotnull"),
            bool_col("atthasdef"),
            bool_col("attisdropped"),
            text_col("attidentity"),
            text_col("attgenerated"),
            int_col("attinhcount"),
            int_col("attcollation"),
            text_col("attacl"),
            text_col("attoptions"),
            text_col("attfdwoptions"),
            text_col("attmissingval"),
            int_col("attstattarget"),
            text_col("attstorage"),
            text_col("attcompression"),
            text_col("def_value"),     // pg_get_expr()
            text_col("description"),   // dsc.description
            int_col("objid"),          // dep.objid
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }

        if let Some(tname) = table_name {
            // Introspect: find all unique top-level keys from the first few documents.
            let sample = oxidb::execute_sql(db, &format!(
                "SELECT * FROM \"{}\" LIMIT 20", tname
            ));
            let mut key_set = std::collections::BTreeMap::<String, i32>::new();
            // Always include _id as first column.
            key_set.insert("_id".to_string(), 23); // int4
            if let Ok(oxidb::SqlResult::Select(rows)) = sample {
                for row in &rows {
                    if let Some(obj) = row.as_object() {
                        for (key, val) in obj {
                            key_set.entry(key.clone()).or_insert_with(|| {
                                match val {
                                    serde_json::Value::Bool(_) => 16,      // bool
                                    serde_json::Value::Number(n) => {
                                        if n.is_f64() { 701 } else { 20 } // float8 or int8
                                    }
                                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => 3802, // jsonb
                                    _ => 25, // text
                                }
                            });
                        }
                    }
                }
            }

            for (attnum, (key, type_oid)) in key_set.iter().enumerate() {
                let attnum_val = (attnum as i32 + 1).to_string();
                let type_len: i16 = match *type_oid {
                    16 => 1,
                    20 => 8,
                    23 => 4,
                    701 => 8,
                    _ => -1,
                };
                codec::write_data_row(writer, &[
                    Some(tname.clone()),            // relname
                    Some(table_oid.to_string()),    // attrelid
                    Some(key.clone()),              // attname
                    Some(type_oid.to_string()),     // atttypid
                    Some(type_len.to_string()),     // attlen
                    Some(attnum_val),               // attnum
                    Some("-1".to_string()),         // atttypmod
                    Some(if key == "_id" { "t" } else { "f" }.to_string()), // attnotnull
                    Some("f".to_string()),         // atthasdef
                    Some("f".to_string()),         // attisdropped
                    Some("".to_string()),          // attidentity
                    Some("".to_string()),          // attgenerated
                    Some("0".to_string()),         // attinhcount
                    Some("0".to_string()),         // attcollation
                    None,                          // attacl
                    None,                          // attoptions
                    None,                          // attfdwoptions
                    None,                          // attmissingval
                    Some("-1".to_string()),        // attstattarget
                    Some("x".to_string()),         // attstorage
                    Some("".to_string()),          // attcompression
                    None,                          // def_value
                    None,                          // description
                    None,                          // objid
                ])?;
            }
            codec::write_command_complete(writer, &format!("SELECT {}", key_set.len()))?;
        } else {
            codec::write_command_complete(writer, "SELECT 0")?;
        }
        return Ok(Some(()));
    }

    // ── pg_total_relation_size / pg_relation_size ─────────────────────
    if upper.contains("PG_TOTAL_RELATION_SIZE") || upper.contains("PG_RELATION_SIZE") {
        // DBeaver queries table sizes. Return 0 for all.
        let cols = vec![int_col("oid"), int_col("total_rel_size"), int_col("rel_size")];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── Catalog queries about specific pg_catalog tables ─────────────────
    // MUST be checked BEFORE pg_namespace/pg_class to prevent false routing.
    // Queries about these tables often JOIN pg_class/pg_namespace; without this
    // guard they'd hit the broad pg_namespace or pg_class handlers and return
    // wrong columns (causing DataGrip "@NotNull name" errors).
    //
    // IMPORTANT: Skip this guard when the query's PRIMARY table is pg_class or
    // pg_sequence — these queries JOIN pg_am, pg_namespace, pg_description, etc.
    // and must reach the pg_class/pg_sequence handler.
    let is_primarily_pg_class = upper.contains("FROM PG_CATALOG.PG_CLASS")
        || upper.contains("FROM PG_CLASS");
    let is_primarily_pg_sequence = upper.contains("FROM PG_CATALOG.PG_SEQUENCE")
        || upper.contains("FROM PG_SEQUENCE");
    // Allow PG_ATTRIBUTE to override is_primarily_pg_class — column introspection
    // queries join pg_class (often via CTE) but should NOT go through the pg_class handler.
    let pg_class_override = is_primarily_pg_class && (upper.contains("PG_ATTRIBUTE") || upper.contains("PG_ATTRDEF"));
    if (!is_primarily_pg_class || pg_class_override) && !is_primarily_pg_sequence && (upper.contains("PG_ATTRIBUTE")
        || upper.contains("PG_CAST")
        || upper.contains("PG_INDEX")
        || upper.contains("PG_CONSTRAINT")
        || upper.contains("PG_TRIGGER")
        || upper.contains("PG_REWRITE")
        || upper.contains("PG_POLICY")
        || upper.contains("PG_DEPEND")
        || upper.contains("PG_EXTENSION")
        || upper.contains("PG_FOREIGN")
        || upper.contains("PG_EVENT_TRIGGER")
        || upper.contains("PG_AGGREGATE")
        || upper.contains("PG_OPERATOR")
        || upper.contains("PG_OPCLASS")
        || upper.contains("PG_OPFAMILY")
        || upper.contains("PG_AM")
        || upper.contains("PG_COLLATION")
        || upper.contains("PG_PROC")
        || upper.contains("PG_ENUM")
        || upper.contains("PG_RANGE")
        || upper.contains("PG_INHERITS")
        || upper.contains("PG_LANGUAGE")
        || upper.contains("PG_ATTRDEF")
        || upper.contains("PG_STAT_")
        || upper.contains("PG_MATVIEWS")
        || upper.contains("PG_PUBLICATION")
        || upper.contains("PG_SUBSCRIPTION")
        || upper.contains("PG_USER_MAPPING")
        || upper.contains("PG_SHDESCRIPTION")
        || upper.contains("PG_GET_VIEWDEF")
        || upper.contains("PG_BACKEND_PID")
        || (upper.contains("PG_DESCRIPTION") && !upper.contains("PG_DATABASE") && !upper.contains("PG_NAMESPACE") && !upper.contains("PG_TYPE") && !upper.contains("PG_CLASS"))
        || upper.contains("INFORMATION_SCHEMA"))
    {
        // Parse column names from the SQL SELECT clause so clients (DataGrip)
        // get the correct column names even when we return 0 rows.
        let cols = extract_select_columns(_sql);
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_namespace ─────────────────────────────────────
    // Skip when pg_class or pg_sequence is the primary table (they JOIN pg_namespace).
    if upper.contains("PG_NAMESPACE") && !is_primarily_pg_class && !is_primarily_pg_sequence {
        // DataGrip query: uses aliased columns like `nspname as name`
        let is_datagrip_ns = upper.contains("AS ID") || upper.contains("AS NAME")
            || upper.contains("AS STATE_NUMBER");

        if is_datagrip_ns && (upper.contains("PG_DESCRIPTION") || upper.contains("NSPOWNER")) {
            let cols = vec![
                int_col("id"),
                int_col("state_number"),
                text_col("name"),
                text_col("description"),
                text_col("owner"),
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            let schemas: &[(&str, &str)] = &[
                ("2200", "public"),
                ("11", "pg_catalog"),
                ("2201", "information_schema"),
            ];
            for (oid, name) in schemas {
                codec::write_data_row(writer, &[
                    Some(oid.to_string()),         // id
                    Some("1".to_string()),         // state_number (xmin)
                    Some(name.to_string()),        // name
                    None,                          // description
                    Some("oxidb".to_string()),     // owner
                ])?;
            }
            codec::write_command_complete(writer, "SELECT 3")?;
            return Ok(Some(()));
        }

        // DBeaver / JDBC query: `SELECT n.oid,n.*,d.description FROM pg_namespace n
        //   LEFT OUTER JOIN pg_description d ...`
        // Uses native column names. n.* includes oid, nspname, nspowner, nspacl.
        let has_n_star = upper.contains("N.*");
        if has_n_star && upper.contains("PG_DESCRIPTION") {
            // n.oid (explicit) + n.* (oid, nspname, nspowner, nspacl) + d.description
            let cols = vec![
                int_col("oid"),       // explicit n.oid
                int_col("oid"),       // from n.*
                text_col("nspname"),  // from n.*
                int_col("nspowner"),  // from n.*
                text_col("nspacl"),   // from n.*
                text_col("description"), // from d
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            let all_schemas: &[(&str, &str, &str)] = &[
                ("11", "pg_catalog", "10"),
                ("2200", "public", "10"),
                ("2201", "information_schema", "10"),
            ];
            // Filter by $1 if present (DBeaver: WHERE nspname=$1)
            let filter_name = params.first().map(|s| s.as_str());
            let schemas: Vec<_> = all_schemas.iter()
                .filter(|(_, name, _)| filter_name.is_none() || filter_name == Some(*name))
                .collect();
            let count = schemas.len();
            for (oid, name, owner) in &schemas {
                codec::write_data_row(writer, &[
                    Some(oid.to_string()),
                    Some(oid.to_string()),
                    Some(name.to_string()),
                    Some(owner.to_string()),
                    None, // nspacl
                    None, // description
                ])?;
            }
            codec::write_command_complete(writer, &format!("SELECT {count}"))?;
            return Ok(Some(()));
        }

        // Standard pg_namespace query
        let cols = vec![int_col("oid"), text_col("nspname"), int_col("nspowner")];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_data_row(writer, &[
            Some("2200".to_string()),
            Some("public".to_string()),
            Some("10".to_string()),
        ])?;
        codec::write_data_row(writer, &[
            Some("11".to_string()),
            Some("pg_catalog".to_string()),
            Some("10".to_string()),
        ])?;
        codec::write_data_row(writer, &[
            Some("2201".to_string()),
            Some("information_schema".to_string()),
            Some("10".to_string()),
        ])?;
        codec::write_command_complete(writer, "SELECT 3")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_sequence ────────────────────────────────────────
    // Must be checked before pg_class since sequence queries join pg_class.
    if upper.contains("PG_SEQUENCE") {
        let cols = vec![
            int_col("sequence_state_number"), int_col("sequence_id"),
            text_col("sequence_name"), text_col("data_type"),
            int_col("start_value"), int_col("inc_value"),
            int_col("min_value"), int_col("max_value"),
            int_col("cache_size"), bool_col("cycle_option"),
            text_col("owner"),
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_class — expose OxiDB collections as tables ─────
    if upper.contains("PG_CLASS") {
        // DBeaver pattern: `c.oid,c.*,d.description,pg_get_expr(...),pg_get_partkeydef(...)`
        let is_dbeaver_class = upper.contains("C.*") && upper.contains("PG_DESCRIPTION");
        if is_dbeaver_class {
            // c.oid (explicit) + c.* (all pg_class columns) + d.description + partition_expr + partition_key
            let cols = vec![
                int_col("oid"),                // explicit c.oid
                int_col("oid"),                // from c.*
                text_col("relname"),
                int_col("relnamespace"),
                int_col("reltype"),
                int_col("reloftype"),
                int_col("relowner"),
                int_col("relam"),
                int_col("relfilenode"),
                int_col("reltablespace"),
                int_col("relpages"),
                int_col("reltuples"),
                int_col("relallvisible"),
                int_col("reltoastrelid"),
                bool_col("relhasindex"),
                bool_col("relisshared"),
                text_col("relpersistence"),
                text_col("relkind"),
                int_col("relnatts"),
                int_col("relchecks"),
                bool_col("relhasrules"),
                bool_col("relhastriggers"),
                bool_col("relhassubclass"),
                bool_col("relrowsecurity"),
                bool_col("relforcerowsecurity"),
                bool_col("relispopulated"),
                text_col("relreplident"),
                bool_col("relispartition"),
                int_col("relrewrite"),
                int_col("relfrozenxid"),
                int_col("relminmxid"),
                text_col("relacl"),
                text_col("reloptions"),
                text_col("relpartbound"),
                text_col("description"),       // d.description
                text_col("partition_expr"),     // pg_get_expr()
                text_col("partition_key"),      // pg_get_partkeydef()
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            // Only return collections when querying the public schema (OID 2200).
            // DBeaver sends relnamespace=$1 with the schema OID as bind parameter.
            let is_public_schema = params.first().is_some_and(|p| p == "2200")
                || !upper.contains("RELNAMESPACE");
            let collections = if is_public_schema {
                db.list_collections()
            } else {
                vec![]
            };
            for (i, name) in collections.iter().enumerate() {
                let oid = (16384 + i).to_string();
                codec::write_data_row(writer, &[
                    Some(oid.clone()),             // explicit c.oid
                    Some(oid.clone()),             // oid from c.*
                    Some(name.clone()),            // relname
                    Some("2200".to_string()),      // relnamespace (public)
                    Some("0".to_string()),         // reltype
                    Some("0".to_string()),         // reloftype
                    Some("10".to_string()),        // relowner
                    Some("2".to_string()),         // relam (heap)
                    Some(oid.clone()),             // relfilenode
                    Some("0".to_string()),         // reltablespace
                    Some("0".to_string()),         // relpages
                    Some("-1".to_string()),        // reltuples
                    Some("0".to_string()),         // relallvisible
                    Some("0".to_string()),         // reltoastrelid
                    Some("f".to_string()),         // relhasindex
                    Some("f".to_string()),         // relisshared
                    Some("p".to_string()),         // relpersistence (permanent)
                    Some("r".to_string()),         // relkind (ordinary table)
                    Some("0".to_string()),         // relnatts
                    Some("0".to_string()),         // relchecks
                    Some("f".to_string()),         // relhasrules
                    Some("f".to_string()),         // relhastriggers
                    Some("f".to_string()),         // relhassubclass
                    Some("f".to_string()),         // relrowsecurity
                    Some("f".to_string()),         // relforcerowsecurity
                    Some("t".to_string()),         // relispopulated
                    Some("d".to_string()),         // relreplident (default)
                    Some("f".to_string()),         // relispartition
                    Some("0".to_string()),         // relrewrite
                    Some("722".to_string()),       // relfrozenxid
                    Some("1".to_string()),         // relminmxid
                    None,                          // relacl
                    None,                          // reloptions
                    None,                          // relpartbound
                    None,                          // description
                    None,                          // partition_expr
                    None,                          // partition_key
                ])?;
            }
            codec::write_command_complete(writer, &format!("SELECT {}", collections.len()))?;
            return Ok(Some(()));
        }

        // DataGrip pattern: uses aliased columns like table_kind, table_name, etc.
        let is_datagrip = upper.contains("RELNAME AS") || upper.contains("RELKIND AS")
            || upper.contains("TABLE_KIND");

        if is_datagrip {
            let cols = vec![
                text_col("table_kind"), text_col("table_name"),
                int_col("table_id"), int_col("table_state_number"),
                bool_col("table_with_oids"), int_col("tablespace_id"),
                text_col("options"), text_col("persistence"),
                text_col("ancestors"), text_col("successors"),
                bool_col("is_partition"), text_col("partition_key"),
                text_col("partition_expression"), int_col("am_id"),
                text_col("owner"),
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            // DataGrip uses parameterized queries — relkind values ('r','p') are
            // in bind parameters ($1,$2), not in the SQL text. Always return tables
            // when DataGrip pattern is detected AND query references RELKIND.
            let wants_tables = upper.contains("'R'") || upper.contains("'P'")
                || upper.contains("RELKIND");
            if wants_tables {
                let collections = db.list_collections();
                for (i, name) in collections.iter().enumerate() {
                    let oid = (16384 + i).to_string();
                    codec::write_data_row(writer, &[
                        Some("r".to_string()),          // table_kind
                        Some(name.clone()),             // table_name
                        Some(oid),                      // table_id
                        Some("1".to_string()),          // table_state_number (xmin)
                        Some("f".to_string()),          // table_with_oids
                        Some("0".to_string()),          // tablespace_id
                        None,                           // options (reloptions)
                        Some("p".to_string()),          // persistence (permanent)
                        None,                           // ancestors
                        None,                           // successors
                        Some("f".to_string()),          // is_partition
                        None,                           // partition_key
                        None,                           // partition_expression
                        Some("0".to_string()),          // am_id
                        Some("oxidb".to_string()),      // owner
                    ])?;
                }
                codec::write_command_complete(writer, &format!("SELECT {}", collections.len()))?;
            } else {
                codec::write_command_complete(writer, "SELECT 0")?;
            }
            return Ok(Some(()));
        }

        // Only return collections for table queries (relkind 'r' or 'p').
        let wants_views = upper.contains("'V'") || upper.contains("'M'");
        if wants_views && !upper.contains("'R'") {
            let cols = vec![
                int_col("oid"), text_col("relname"), int_col("relnamespace"),
                text_col("relkind"), int_col("relowner"), int_col("reltablespace"),
                int_col("reltuples"), int_col("relpages"), text_col("relhasindex"),
                text_col("relhasrules"), text_col("relhastriggers"), text_col("relrowsecurity"),
            ];
            if desc {
                codec::write_row_description(writer, &cols)?;
            }
            codec::write_command_complete(writer, "SELECT 0")?;
            return Ok(Some(()));
        }

        // Standard pg_class query (psql, JDBC getCatalogs, etc.)
        let collections = db.list_collections();
        let cols = vec![
            int_col("oid"),
            text_col("relname"),
            int_col("relnamespace"),
            text_col("relkind"),
            int_col("relowner"),
            int_col("reltablespace"),
            int_col("reltuples"),
            int_col("relpages"),
            text_col("relhasindex"),
            text_col("relhasrules"),
            text_col("relhastriggers"),
            text_col("relrowsecurity"),
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        for (i, name) in collections.iter().enumerate() {
            let oid = (16384 + i).to_string();
            codec::write_data_row(writer, &[
                Some(oid),                    // oid
                Some(name.clone()),           // relname
                Some("2200".to_string()),     // relnamespace (public)
                Some("r".to_string()),        // relkind (ordinary table)
                Some("10".to_string()),       // relowner
                Some("0".to_string()),        // reltablespace
                Some("-1".to_string()),       // reltuples
                Some("0".to_string()),        // relpages
                Some("f".to_string()),        // relhasindex
                Some("f".to_string()),        // relhasrules
                Some("f".to_string()),        // relhastriggers
                Some("f".to_string()),        // relrowsecurity
            ])?;
        }
        codec::write_command_complete(writer, &format!("SELECT {}", collections.len()))?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_tablespace ──────────────────────────────────────
    if upper.contains("PG_TABLESPACE") && !upper.contains("SPCACL") {
        let cols = vec![
            int_col("id"), text_col("name"), int_col("state_number"),
            text_col("owner"), text_col("location"), text_col("options"),
            text_col("comment"),
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── pg_user ───────────────────────────────────────────────────────
    if upper.contains("PG_USER") && !upper.contains("PG_USER_MAPPING")
        && !upper.contains("PG_AUTHID") && !upper.contains("PG_ROLES")
    {
        if upper.contains("USESUPER") {
            send_row(writer, "usesuper", "t", desc)?;
        } else {
            send_empty(writer, desc)?;
        }
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_roles ─────────────────────────────────────────
    if upper.contains("PG_ROLES") {
        let cols = vec![
            int_col("role_id"), text_col("role_name"),
            bool_col("is_super"), bool_col("is_inherit"),
            bool_col("can_createrole"), bool_col("can_createdb"),
            bool_col("can_login"), bool_col("is_replication"),
            int_col("conn_limit"), text_col("valid_until"),
            bool_col("bypass_rls"), text_col("config"),
            text_col("description"),
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_data_row(writer, &[
            Some("10".to_string()),         // role_id
            Some("oxidb".to_string()),      // role_name
            Some("t".to_string()),          // is_super
            Some("t".to_string()),          // is_inherit
            Some("t".to_string()),          // can_createrole
            Some("t".to_string()),          // can_createdb
            Some("t".to_string()),          // can_login
            Some("f".to_string()),          // is_replication
            Some("-1".to_string()),         // conn_limit
            None,                           // valid_until
            Some("f".to_string()),          // bypass_rls
            None,                           // config
            None,                           // description
        ])?;
        codec::write_command_complete(writer, "SELECT 1")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_auth_members ──────────────────────────────────
    if upper.contains("PG_AUTH_MEMBERS") {
        let cols = vec![
            int_col("id"), int_col("role_id"), bool_col("admin_option"),
        ];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── pg_catalog.pg_timezone ──────────────────────────────────────
    if upper.contains("PG_TIMEZONE") {
        let cols = vec![text_col("name"), bool_col("is_dst")];
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── Catch-all for other pg_catalog / information_schema tables ───
    if upper.contains("PG_ATTRIBUTE")
        || upper.contains("PG_DESCRIPTION")
        || upper.contains("PG_INDEX")
        || upper.contains("PG_PROC")
        || upper.contains("PG_USER")
        || upper.contains("PG_AUTHID")
        || upper.contains("PG_STAT_")
        || upper.contains("PG_EXTENSION")
        || upper.contains("PG_AVAILABLE_EXTENSIONS")
        || upper.contains("PG_CONSTRAINT")
        || upper.contains("PG_DEPEND")
        || upper.contains("PG_INHERITS")
        || upper.contains("PG_ENUM")
        || upper.contains("PG_RANGE")
        || upper.contains("PG_COLLATION")
        || upper.contains("PG_AM")
        || upper.contains("PG_TRIGGER")
        || upper.contains("PG_REWRITE")
        || upper.contains("PG_SHDESCRIPTION")
        || upper.contains("PG_MATVIEWS")
        || upper.contains("PG_FOREIGN")
        || upper.contains("PG_POLICY")
        || upper.contains("PG_PUBLICATION")
        || upper.contains("PG_SUBSCRIPTION")
        || upper.contains("PG_BACKEND_PID")
        || upper.contains("INFORMATION_SCHEMA")
        || upper.contains("PG_CATALOG")
    {
        let cols = extract_select_columns(_sql);
        if desc {
            codec::write_row_description(writer, &cols)?;
        }
        codec::write_command_complete(writer, "SELECT 0")?;
        return Ok(Some(()));
    }

    // ── Functions the JDBC driver calls ─────────────────────────────
    if upper.contains("PG_TYPEOF") {
        send_row(writer, "pg_typeof", "text", desc)?;
        return Ok(Some(()));
    }
    if upper.contains("PG_IS_IN_RECOVERY") {
        send_row(writer, "pg_is_in_recovery", "f", desc)?;
        return Ok(Some(()));
    }

    Ok(None)
}

/// Return column definitions for a known intercepted query (used by Describe).
fn describe_intercepted_columns(sql: &str) -> Option<Vec<ColumnDef>> {
    let upper = normalize_whitespace(&sql.to_uppercase());
    let upper = upper.trim();

    if upper == "SHOW DATABASES" {
        return Some(vec![text_col("database_name")]);
    }

    if upper == "SELECT VERSION()" {
        return Some(vec![text_col("version")]);
    }
    if upper == "SELECT CURRENT_DATABASE()" {
        return Some(vec![text_col("current_database")]);
    }
    if upper == "SELECT CURRENT_USER"
        || upper == "SELECT CURRENT_SCHEMA"
        || upper == "SELECT CURRENT_SCHEMA()"
    {
        return Some(vec![text_col("current_user")]);
    }
    if upper.starts_with("SELECT CURRENT_SETTING(") {
        return Some(vec![text_col("current_setting")]);
    }
    if upper.contains("TXID_CURRENT") {
        let col_name = if let Some(idx) = upper.rfind(" AS ") {
            sql[idx + 4..].trim().trim_matches('"').to_string()
        } else {
            "txid_current".to_string()
        };
        return Some(vec![int_col(&col_name)]);
    }
    if upper.contains("PG_POSTMASTER_START_TIME") {
        let col_name = if let Some(idx) = upper.rfind(" AS ") {
            sql[idx + 4..].trim().trim_matches('"').to_string()
        } else {
            "startup_time".to_string()
        };
        return Some(vec![int_col(&col_name)]);
    }
    if upper.contains("SET_CONFIG") || upper.contains("PG_CATALOG.SET_CONFIG") {
        return Some(vec![text_col("set_config")]);
    }

    // SELECT <expression> without a real FROM table clause
    if upper.starts_with("SELECT ") && !has_from_table_clause(upper) {
        if let Some(result) = try_select_literal(sql) {
            return Some(vec![text_col(&result.0)]);
        }
        // Multi-column expressions: try to describe all columns
        let rest = &sql[7..];
        let exprs = split_top_level_commas(rest);
        if !exprs.is_empty() {
            let mut cols = Vec::new();
            for expr in &exprs {
                let (_, alias) = extract_alias(expr.trim());
                cols.push(text_col(&alias));
            }
            return Some(cols);
        }
    }

    if upper.starts_with("SHOW ") {
        let param = sql[5..].trim().trim_end_matches(';').trim();
        return Some(vec![text_col(param)]);
    }

    // Schema probe queries (WHERE 1<>1 or WHERE 0=1) — return proper columns
    if upper.contains("WHERE 1<>1") || upper.contains("WHERE 0=1") {
        return Some(extract_select_columns(sql));
    }

    // string_agg / pg_get_keywords
    if upper.contains("PG_GET_KEYWORDS") || upper.contains("STRING_AGG") {
        return Some(vec![text_col("string_agg")]);
    }

    // pg_attribute — DBeaver column introspection (C.OID=$1)
    if upper.contains("PG_ATTRIBUTE") && upper.contains("C.OID=$1") {
        return Some(vec![
            text_col("relname"), int_col("attrelid"), text_col("attname"),
            int_col("atttypid"), int_col("attlen"), int_col("attnum"),
            int_col("atttypmod"), bool_col("attnotnull"), bool_col("atthasdef"),
            bool_col("attisdropped"), text_col("attidentity"), text_col("attgenerated"),
            int_col("attinhcount"), int_col("attcollation"), text_col("attacl"),
            text_col("attoptions"), text_col("attfdwoptions"), text_col("attmissingval"),
            int_col("attstattarget"), text_col("attstorage"), text_col("attcompression"),
            text_col("def_value"), text_col("description"), int_col("objid"),
        ]);
    }

    // pg_total_relation_size / pg_relation_size
    if upper.contains("PG_TOTAL_RELATION_SIZE") || upper.contains("PG_RELATION_SIZE") {
        return Some(vec![int_col("oid"), int_col("total_rel_size"), int_col("rel_size")]);
    }

    // ACL union queries (MUST be before pg_database — ACL queries reference multiple catalogs)
    if upper.contains("SPCACL") || upper.contains("DATACL") || upper.contains("NSPACL")
        || upper.contains("RELACL") || upper.contains("PROACL")
    {
        return Some(vec![int_col("object_id"), text_col("acl")]);
    }

    // pg_locks
    if upper.contains("PG_LOCKS") {
        return Some(vec![int_col("transaction_id")]);
    }

    // pg_database
    if upper.contains("PG_DATABASE") {
        if upper.contains("PG_ENCODING_TO_CHAR")
            && !upper.contains("DATISTEMPLATE")
            && !upper.contains("DATALLOWCONN")
        {
            return Some(vec![text_col("pg_encoding_to_char")]);
        }
        if upper.contains("PG_SHDESCRIPTION") || upper.contains("PG_DESCRIPTION") {
            return Some(vec![
                int_col("id"),
                text_col("name"),
                text_col("description"),
                bool_col("is_template"),
                bool_col("allow_connections"),
                text_col("owner"),
            ]);
        }
        // DBeaver: db.oid, db.* — prepend explicit oid if present
        let has_explicit_oid = upper.contains("DB.OID") && upper.contains("DB.*");
        let mut cols = Vec::new();
        if has_explicit_oid {
            cols.push(int_col("oid"));
        }
        cols.extend_from_slice(&[
            int_col("oid"),
            text_col("datname"),
            int_col("datdba"),
            int_col("encoding"),
            text_col("datlocprovider"),
            bool_col("datistemplate"),
            bool_col("datallowconn"),
            int_col("datconnlimit"),
            int_col("datfrozenxid"),
            int_col("datminmxid"),
            int_col("dattablespace"),
            text_col("datcollate"),
            text_col("datctype"),
            text_col("daticulocale"),
            text_col("daticurules"),
            text_col("datcollversion"),
            text_col("datacl"),
        ]);
        return Some(cols);
    }

    // pg_settings — return full column structure matching the handler
    if upper.contains("PG_SETTINGS") {
        if upper.contains("MAX_INDEX_KEYS") {
            return Some(vec![text_col("setting")]);
        }
        return Some(vec![
            text_col("name"), text_col("setting"), text_col("unit"),
            text_col("category"), text_col("short_desc"), text_col("extra_desc"),
            text_col("context"), text_col("vartype"), text_col("source"),
            text_col("min_val"), text_col("max_val"), text_col("enumvals"),
            text_col("boot_val"), text_col("reset_val"),
            text_col("sourcefile"), int_col("sourceline"),
            bool_col("pending_restart"),
        ]);
    }

    // pg_type — exclude PG_CAST (cast queries join pg_type but need different handling)
    if upper.contains("PG_TYPE") && !upper.contains("PG_TYPEOF") && !upper.contains("PG_CAST") {
        // DBeaver: t.oid,t.* with format_type() — full PG type columns
        // Only match T.* (DBeaver pattern); FORMAT_TYPE alone is too broad.
        if upper.contains("T.*") {
            return Some(vec![
                int_col("oid"), int_col("oid"),
                text_col("typname"), int_col("typnamespace"),
                int_col("typowner"), int_col("typlen"),
                bool_col("typbyval"), text_col("typtype"),
                text_col("typcategory"), bool_col("typispreferred"),
                bool_col("typisdefined"), text_col("typdelim"),
                int_col("typrelid"), int_col("typsubscript"),
                int_col("typelem"), int_col("typarray"),
                text_col("typinput"), text_col("typoutput"),
                text_col("typreceive"), text_col("typsend"),
                text_col("typmodin"), text_col("typmodout"),
                text_col("typanalyze"), text_col("typalign"),
                text_col("typstorage"), bool_col("typnotnull"),
                int_col("typbasetype"), int_col("typtypmod"),
                int_col("typndims"), int_col("typcollation"),
                text_col("typdefaultbin"), text_col("typdefault"),
                text_col("typacl"), text_col("relkind"),
                text_col("base_type_name"), text_col("description"),
            ]);
        }
        // DataGrip's complex type query — return proper column definitions
        let has_type_name_alias = upper.contains(" AS TYPE_NAME")
            || upper.contains(" AS \"TYPE_NAME\"")
            || (upper.contains("TYPE_NAME") && !upper.contains("BASE_TYPE_NAME"));
        if has_type_name_alias || upper.contains("TYPCATEGORY")
            || (upper.contains("TYPTYPE") && upper.contains("PG_CLASS"))
        {
            return Some(vec![
                int_col("type_id"), int_col("type_state_number"),
                text_col("type_name"), text_col("type_sub_kind"),
                text_col("type_category"), int_col("class_id"),
                int_col("base_type_id"), text_col("type_def"),
                int_col("dimensions_number"), text_col("default_expression"),
                bool_col("mandatory"), text_col("owner"),
            ]);
        }
        // TYPNAME as a column reference (not just part of TYPNAMESPACE)
        let has_typname_col = upper.replace("TYPNAMESPACE", "").contains("TYPNAME");
        if has_typname_col && upper.contains("OID") {
            return Some(vec![
                int_col("oid"),
                text_col("typname"),
                int_col("typnamespace"),
                text_col("typtype"),
                int_col("typbasetype"),
                int_col("typrelid"),
                int_col("typtypmod"),
                text_col("typnotnull"),
            ]);
        }
        return Some(vec![
            int_col("type_id"), int_col("type_state_number"),
            text_col("type_name"), text_col("type_sub_kind"),
            text_col("type_category"), int_col("class_id"),
            int_col("base_type_id"), text_col("type_def"),
            int_col("dimensions_number"), text_col("default_expression"),
            bool_col("mandatory"), text_col("owner"),
        ]);
    }

    // Catalog queries about specific pg_catalog tables — must match handle_catalog_query.
    // Skip when pg_class or pg_sequence is the primary table (they JOIN these catalogs).
    let is_primarily_pg_class = upper.contains("FROM PG_CATALOG.PG_CLASS")
        || upper.contains("FROM PG_CLASS");
    let is_primarily_pg_sequence = upper.contains("FROM PG_CATALOG.PG_SEQUENCE")
        || upper.contains("FROM PG_SEQUENCE");
    // Allow PG_ATTRIBUTE to override is_primarily_pg_class — column introspection
    // queries join pg_class (often via CTE) but should NOT go through the pg_class handler.
    let pg_class_override = is_primarily_pg_class && (upper.contains("PG_ATTRIBUTE") || upper.contains("PG_ATTRDEF"));
    if (!is_primarily_pg_class || pg_class_override) && !is_primarily_pg_sequence && (upper.contains("PG_ATTRIBUTE")
        || upper.contains("PG_CAST")
        || upper.contains("PG_INDEX")
        || upper.contains("PG_CONSTRAINT")
        || upper.contains("PG_TRIGGER")
        || upper.contains("PG_REWRITE")
        || upper.contains("PG_POLICY")
        || upper.contains("PG_DEPEND")
        || upper.contains("PG_EXTENSION")
        || upper.contains("PG_FOREIGN")
        || upper.contains("PG_EVENT_TRIGGER")
        || upper.contains("PG_AGGREGATE")
        || upper.contains("PG_OPERATOR")
        || upper.contains("PG_OPCLASS")
        || upper.contains("PG_OPFAMILY")
        || upper.contains("PG_AM")
        || upper.contains("PG_COLLATION")
        || upper.contains("PG_PROC")
        || upper.contains("PG_ENUM")
        || upper.contains("PG_RANGE")
        || upper.contains("PG_INHERITS")
        || upper.contains("PG_LANGUAGE")
        || upper.contains("PG_ATTRDEF")
        || upper.contains("PG_STAT_")
        || upper.contains("PG_MATVIEWS")
        || upper.contains("PG_PUBLICATION")
        || upper.contains("PG_SUBSCRIPTION")
        || upper.contains("PG_USER_MAPPING")
        || upper.contains("PG_SHDESCRIPTION")
        || upper.contains("PG_GET_VIEWDEF")
        || upper.contains("PG_BACKEND_PID")
        || (upper.contains("PG_DESCRIPTION") && !upper.contains("PG_DATABASE") && !upper.contains("PG_NAMESPACE") && !upper.contains("PG_TYPE") && !upper.contains("PG_CLASS"))
        || upper.contains("INFORMATION_SCHEMA"))
    {
        return Some(extract_select_columns(sql));
    }

    // pg_namespace — skip when pg_class or pg_sequence is primary (they JOIN pg_namespace)
    if upper.contains("PG_NAMESPACE") && !is_primarily_pg_class && !is_primarily_pg_sequence {
        let is_datagrip_ns = upper.contains("AS ID") || upper.contains("AS NAME")
            || upper.contains("AS STATE_NUMBER");
        if is_datagrip_ns && (upper.contains("PG_DESCRIPTION") || upper.contains("NSPOWNER")) {
            return Some(vec![
                int_col("id"),
                int_col("state_number"),
                text_col("name"),
                text_col("description"),
                text_col("owner"),
            ]);
        }
        // DBeaver: n.oid,n.*,d.description
        if upper.contains("N.*") && upper.contains("PG_DESCRIPTION") {
            return Some(vec![
                int_col("oid"), int_col("oid"),
                text_col("nspname"), int_col("nspowner"),
                text_col("nspacl"), text_col("description"),
            ]);
        }
        return Some(vec![
            int_col("oid"),
            text_col("nspname"),
            int_col("nspowner"),
        ]);
    }

    // pg_sequence — must be before pg_class
    if upper.contains("PG_SEQUENCE") {
        return Some(vec![
            int_col("sequence_state_number"), int_col("sequence_id"),
            text_col("sequence_name"), text_col("data_type"),
            int_col("start_value"), int_col("inc_value"),
            int_col("min_value"), int_col("max_value"),
            int_col("cache_size"), bool_col("cycle_option"),
            text_col("owner"),
        ]);
    }

    // pg_class — must match handle_catalog_query columns
    if upper.contains("PG_CLASS") {
        // DBeaver: c.oid,c.*,d.description,pg_get_expr(),pg_get_partkeydef()
        if upper.contains("C.*") && upper.contains("PG_DESCRIPTION") {
            return Some(vec![
                int_col("oid"), int_col("oid"),
                text_col("relname"), int_col("relnamespace"),
                int_col("reltype"), int_col("reloftype"),
                int_col("relowner"), int_col("relam"),
                int_col("relfilenode"), int_col("reltablespace"),
                int_col("relpages"), int_col("reltuples"),
                int_col("relallvisible"), int_col("reltoastrelid"),
                bool_col("relhasindex"), bool_col("relisshared"),
                text_col("relpersistence"), text_col("relkind"),
                int_col("relnatts"), int_col("relchecks"),
                bool_col("relhasrules"), bool_col("relhastriggers"),
                bool_col("relhassubclass"), bool_col("relrowsecurity"),
                bool_col("relforcerowsecurity"), bool_col("relispopulated"),
                text_col("relreplident"), bool_col("relispartition"),
                int_col("relrewrite"), int_col("relfrozenxid"),
                int_col("relminmxid"), text_col("relacl"),
                text_col("reloptions"), text_col("relpartbound"),
                text_col("description"), text_col("partition_expr"),
                text_col("partition_key"),
            ]);
        }
        // DataGrip query uses aliased columns like table_kind, table_name, etc.
        if upper.contains("RELNAME AS") || upper.contains("RELKIND AS") || upper.contains("TABLE_KIND") {
            return Some(vec![
                text_col("table_kind"), text_col("table_name"),
                int_col("table_id"), int_col("table_state_number"),
                bool_col("table_with_oids"), int_col("tablespace_id"),
                text_col("options"), text_col("persistence"),
                text_col("ancestors"), text_col("successors"),
                bool_col("is_partition"), text_col("partition_key"),
                text_col("partition_expression"), int_col("am_id"),
                text_col("owner"),
            ]);
        }
        return Some(vec![
            int_col("oid"),
            text_col("relname"),
            int_col("relnamespace"),
            text_col("relkind"),
            int_col("relowner"),
            int_col("reltablespace"),
            int_col("reltuples"),
            int_col("relpages"),
            text_col("relhasindex"),
            text_col("relhasrules"),
            text_col("relhastriggers"),
            text_col("relrowsecurity"),
        ]);
    }

    // pg_tablespace
    if upper.contains("PG_TABLESPACE") {
        return Some(vec![
            int_col("id"), text_col("name"), int_col("state_number"),
            text_col("owner"), text_col("location"), text_col("options"),
            text_col("comment"),
        ]);
    }

    // pg_roles (specific handler with proper DataGrip columns)
    if upper.contains("PG_ROLES") {
        return Some(vec![
            int_col("role_id"), text_col("role_name"),
            bool_col("is_super"), bool_col("is_inherit"),
            bool_col("can_createrole"), bool_col("can_createdb"),
            bool_col("can_login"), bool_col("is_replication"),
            int_col("conn_limit"), text_col("valid_until"),
            bool_col("bypass_rls"), text_col("config"),
            text_col("description"),
        ]);
    }

    // pg_auth_members
    if upper.contains("PG_AUTH_MEMBERS") {
        return Some(vec![
            int_col("id"), int_col("role_id"), bool_col("admin_option"),
        ]);
    }

    // pg_user
    if upper.contains("PG_USER") && !upper.contains("PG_USER_MAPPING") {
        if upper.contains("USESUPER") {
            return Some(vec![bool_col("usesuper")]);
        }
        return Some(vec![text_col("usename"), bool_col("usesuper")]);
    }

    // pg_timezone
    if upper.contains("PG_TIMEZONE") {
        return Some(vec![text_col("name"), bool_col("is_dst")]);
    }

    // Other pg_catalog / information_schema — parse columns from SQL
    if upper.contains("PG_CATALOG")
        || upper.contains("PG_ATTRIBUTE")
        || upper.contains("PG_DESCRIPTION")
        || upper.contains("INFORMATION_SCHEMA")
        || upper.contains("PG_INDEX")
        || upper.contains("PG_PROC")
        || upper.contains("PG_STAT_")
        || upper.contains("PG_CONSTRAINT")
        || upper.contains("PG_DEPEND")
        || upper.contains("PG_INHERITS")
        || upper.contains("PG_ENUM")
        || upper.contains("PG_RANGE")
        || upper.contains("PG_COLLATION")
        || upper.contains("PG_AM")
        || upper.contains("PG_TRIGGER")
        || upper.contains("PG_BACKEND_PID")
    {
        return Some(extract_select_columns(sql));
    }

    if upper.contains("PG_TYPEOF") {
        return Some(vec![text_col("pg_typeof")]);
    }
    if upper.contains("PG_IS_IN_RECOVERY") {
        return Some(vec![text_col("pg_is_in_recovery")]);
    }

    None
}

// ── SqlResult conversion ─────────────────────────────────────────────

fn error_to_sqlstate(err: &oxidb::Error) -> (&'static str, &'static str) {
    match err {
        oxidb::Error::InvalidQuery(_) | oxidb::Error::InvalidPipeline(_) => {
            ("42601", "ERROR")
        }
        oxidb::Error::CollectionNotFound(_) => ("42P01", "ERROR"),
        oxidb::Error::CollectionAlreadyExists(_) => ("42P07", "ERROR"),
        oxidb::Error::UniqueViolation { .. } => ("23505", "ERROR"),
        oxidb::Error::TransactionConflict { .. } => ("40001", "ERROR"),
        _ => ("XX000", "ERROR"),
    }
}

fn write_sql_result<W: Write>(
    writer: &mut W,
    result: oxidb::SqlResult,
    include_row_desc: bool,
) -> io::Result<()> {
    match result {
        oxidb::SqlResult::Select(rows) => {
            let columns = types::infer_columns(&rows);
            if include_row_desc {
                codec::write_row_description(writer, &columns)?;
            }
            let count = rows.len();
            for row in &rows {
                let values = types::row_values(row, &columns);
                codec::write_data_row(writer, &values)?;
            }
            codec::write_command_complete(writer, &format!("SELECT {count}"))?;
        }
        oxidb::SqlResult::Insert(ids) => {
            let count = ids.len();
            codec::write_command_complete(writer, &format!("INSERT 0 {count}"))?;
        }
        oxidb::SqlResult::Update(count) => {
            codec::write_command_complete(writer, &format!("UPDATE {count}"))?;
        }
        oxidb::SqlResult::Delete(count) => {
            codec::write_command_complete(writer, &format!("DELETE {count}"))?;
        }
        oxidb::SqlResult::Ddl(msg) => {
            let tag = ddl_command_tag(&msg);
            codec::write_command_complete(writer, tag)?;
        }
        oxidb::SqlResult::UseDatabase(_) => {
            codec::write_command_complete(writer, "SET")?;
        }
        oxidb::SqlResult::ShowDatabases(names) => {
            let cols = vec![text_col("database_name")];
            if include_row_desc {
                codec::write_row_description(writer, &cols)?;
            }
            let count = names.len();
            for name in &names {
                codec::write_data_row(writer, &[Some(name.clone())])?;
            }
            codec::write_command_complete(writer, &format!("SELECT {count}"))?;
        }
    }
    Ok(())
}

fn ddl_command_tag(msg: &str) -> &str {
    let upper = msg.to_uppercase();
    if upper.contains("CREATE") && upper.contains("DATABASE") {
        "CREATE DATABASE"
    } else if upper.contains("DROP") && upper.contains("DATABASE") {
        "DROP DATABASE"
    } else if upper.contains("CREATE") && upper.contains("INDEX") {
        "CREATE INDEX"
    } else if upper.contains("DROP") && upper.contains("TABLE") {
        "DROP TABLE"
    } else if upper.contains("CREATE") && upper.contains("TABLE") {
        "CREATE TABLE"
    } else if upper.contains("DROP") && upper.contains("INDEX") {
        "DROP INDEX"
    } else {
        "OK"
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Normalize PostgreSQL-flavored SQL for the OxiDB engine.
/// - Strip schema prefixes: `public.orders` → `orders`
/// - Strip table aliases: `FROM orders AS o` → `FROM orders`
/// - Replace qualified wildcards: `o.*` → `*`
/// - Substitute bind parameters: `$1` → literal values from params
fn normalize_pg_sql(sql: &str, params: &[String]) -> String {
    // Normalize whitespace first: collapse newlines, tabs, and multiple spaces
    // into single spaces. DataGrip sends multi-line SQL with \n that breaks
    // pattern matching for " FROM ", " LIMIT ", etc.
    let mut s = sql.split_whitespace().collect::<Vec<_>>().join(" ");

    // Substitute $N parameters with their values (for simple cases).
    for (i, val) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        if !val.is_empty() {
            // Wrap non-numeric values in quotes.
            let replacement = if val.chars().all(|c| c.is_ascii_digit() || c == '-' || c == '.') {
                val.clone()
            } else {
                format!("'{}'", val.replace('\'', "''"))
            };
            s = s.replace(&placeholder, &replacement);
        }
    }

    // Remove schema prefix: `public.tablename` → `tablename`
    // Only strip `public.` when it appears as a schema qualifier outside of quoted strings/identifiers.
    {
        let mut result = String::with_capacity(s.len());
        let bytes = s.as_bytes();
        let len = bytes.len();
        let mut i = 0;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        while i < len {
            // Track single-quoted strings (string literals)
            if bytes[i] == b'\'' && !in_double_quote {
                if !in_single_quote {
                    in_single_quote = true;
                } else {
                    // Check for escaped quote ('')
                    if i + 1 < len && bytes[i + 1] == b'\'' {
                        result.push('\'');
                        result.push('\'');
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                result.push('\'');
                i += 1;
                continue;
            }
            // Track double-quoted identifiers
            if bytes[i] == b'"' && !in_single_quote {
                if !in_double_quote {
                    in_double_quote = true;
                } else {
                    // Check for escaped double-quote ("")
                    if i + 1 < len && bytes[i + 1] == b'"' {
                        result.push('"');
                        result.push('"');
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                result.push('"');
                i += 1;
                continue;
            }
            // Inside any quoted context, pass through as-is
            if in_single_quote || in_double_quote {
                result.push(bytes[i] as char);
                i += 1;
                continue;
            }
            // Outside quotes: check for `public.` (case-insensitive)
            if i + 7 <= len {
                let candidate = &s[i..i + 7];
                if candidate.eq_ignore_ascii_case("public.") {
                    // Only strip if preceded by a non-identifier char (or start of string)
                    let preceded_ok = if i == 0 {
                        true
                    } else {
                        let prev = bytes[i - 1];
                        !prev.is_ascii_alphanumeric() && prev != b'_'
                    };
                    if preceded_ok {
                        i += 7; // skip `public.`
                        continue;
                    }
                }
            }
            result.push(bytes[i] as char);
            i += 1;
        }
        s = result;
    }

    // Replace qualified wildcards: `alias.*` → `*`
    // Match patterns like `o.*`, `t.*`, `orders.*` etc.
    let re_qualified_star = |input: &str| -> String {
        let mut result = String::with_capacity(input.len());
        let chars: Vec<char> = input.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '.' && i + 1 < chars.len() && chars[i + 1] == '*' {
                // Check if this is `identifier.*` — walk back to find the identifier start.
                if i > 0 && (chars[i - 1].is_alphanumeric() || chars[i - 1] == '_' || chars[i - 1] == '"') {
                    // Remove the identifier before `.*`
                    // Walk back to the start of the identifier.
                    let mut start = result.len();
                    while start > 0 {
                        let prev = result.as_bytes()[start - 1];
                        if prev.is_ascii_alphanumeric() || prev == b'_' || prev == b'"' {
                            start -= 1;
                        } else {
                            break;
                        }
                    }
                    result.truncate(start);
                    result.push('*');
                    i += 2; // skip `.*`
                    continue;
                }
            }
            result.push(chars[i]);
            i += 1;
        }
        result
    };
    s = re_qualified_star(&s);

    // Strip PostgreSQL system columns (CTID, OID, XMIN, etc.) from SELECT list.
    // These don't exist in OxiDB. Remove `, CTID` or `CTID,` patterns.
    {
        let up = s.to_uppercase();
        // Find SELECT ... FROM boundaries
        if let Some(sel_idx) = up.find("SELECT ") {
            if let Some(from_idx) = up.find(" FROM ") {
                let projection = &s[sel_idx + 7..from_idx];
                let sys_cols = ["CTID", "OID", "XMIN", "XMAX", "CMIN", "CMAX", "TABLEOID"];
                let parts: Vec<&str> = projection.split(',').collect();
                let filtered: Vec<&str> = parts.iter()
                    .map(|p| p.trim())
                    .filter(|p| {
                        let pu = p.to_uppercase();
                        !sys_cols.iter().any(|sc| pu == *sc)
                    })
                    .collect();
                if filtered.len() < parts.len() {
                    let new_projection = filtered.join(", ");
                    s = format!("{}{new_projection}{}", &s[..sel_idx + 7], &s[from_idx..]);
                }
            }
        }
    }

    // Remove table aliases in FROM clause:
    //   `FROM tablename AS alias` → `FROM tablename`
    //   `FROM tablename alias`    → `FROM tablename`  (implicit alias)
    let upper = s.to_uppercase();
    if let Some(from_idx) = upper.find(" FROM ") {
        let after_from = &s[from_idx + 6..];
        let trimmed = after_from.trim_start();
        let table_end = trimmed.find(|c: char| c.is_ascii_whitespace()).unwrap_or(trimmed.len());
        let rest = trimmed[table_end..].trim_start();
        let rest_upper = rest.to_uppercase();
        if rest_upper.starts_with("AS ") {
            // Explicit alias: `FROM tablename AS alias`
            let alias_rest = &rest[3..];
            let alias_end = alias_rest.find(|c: char| c.is_ascii_whitespace() || c == ';' || c == ')').unwrap_or(alias_rest.len());
            let before_from = &s[..from_idx + 6];
            let table_name = &trimmed[..table_end];
            let after_alias = &alias_rest[alias_end..];
            s = format!("{before_from}{table_name}{after_alias}");
        } else if !rest_upper.is_empty()
            && !rest_upper.starts_with("WHERE ")
            && !rest_upper.starts_with("ORDER ")
            && !rest_upper.starts_with("LIMIT ")
            && !rest_upper.starts_with("GROUP ")
            && !rest_upper.starts_with("HAVING ")
            && !rest_upper.starts_with("JOIN ")
            && !rest_upper.starts_with("LEFT ")
            && !rest_upper.starts_with("RIGHT ")
            && !rest_upper.starts_with("INNER ")
            && !rest_upper.starts_with("OUTER ")
            && !rest_upper.starts_with("CROSS ")
            && !rest_upper.starts_with("ON ")
            && !rest_upper.starts_with("UNION ")
            && !rest_upper.starts_with("INTERSECT ")
            && !rest_upper.starts_with("EXCEPT ")
        {
            // Implicit alias: `FROM tablename alias ...`
            // The next word is the alias — skip it.
            let alias_end = rest.find(|c: char| c.is_ascii_whitespace() || c == ';' || c == ')').unwrap_or(rest.len());
            let before_from = &s[..from_idx + 6];
            let table_name = &trimmed[..table_end];
            let after_alias = &rest[alias_end..];
            s = format!("{before_from}{table_name}{after_alias}");
        }
    }

    s
}

/// Extract column names/aliases from a SELECT query's projection clause.
/// Parses `SELECT expr AS alias, expr2 alias2, ... FROM ...` and returns
/// ColumnDef entries with the correct names. Falls back to `text_col("result")`
/// if parsing fails.
fn extract_select_columns(sql: &str) -> Vec<ColumnDef> {
    let norm = normalize_whitespace(sql);
    let upper = norm.to_uppercase();
    // Find the SELECT ... FROM boundaries.
    let select_start = if let Some(idx) = upper.find("SELECT ") {
        idx + 7
    } else {
        return vec![text_col("result")];
    };
    // Find " FROM " at top level (not inside parentheses).
    let projection = {
        let search = &norm[select_start..];
        let search_upper = &upper[select_start..];
        let mut depth = 0i32;
        let mut from_pos = None;
        let mut i = 0;
        let bytes = search_upper.as_bytes();
        while i < bytes.len() {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b' ' if depth == 0 && i + 6 <= bytes.len() => {
                    if &search_upper[i..i + 6] == " FROM " {
                        from_pos = Some(i);
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        match from_pos {
            Some(pos) => &search[..pos],
            None => search, // no FROM — use everything after SELECT
        }
    };

    let exprs = split_top_level_commas(projection);
    if exprs.is_empty() {
        return vec![text_col("result")];
    }

    let mut cols = Vec::with_capacity(exprs.len());
    for expr in &exprs {
        let expr = expr.trim();
        if expr.is_empty() {
            continue;
        }
        let upper_expr = expr.to_uppercase();

        // Extract alias: look for " AS " (case insensitive), or take last token.
        let alias = if let Some(as_idx) = find_top_level_as(&upper_expr) {
            expr[as_idx + 4..].trim().trim_matches('"').trim_matches('\'')
        } else {
            // No AS — use last identifier token (after last dot, removing casts/parens).
            extract_last_identifier(expr)
        };

        // Skip CASE/subquery expressions that didn't get an alias
        if alias.is_empty() || alias == "(" || alias == ")" {
            cols.push(text_col("?column?"));
            continue;
        }

        cols.push(text_col(alias));
    }

    if cols.is_empty() {
        vec![text_col("result")]
    } else {
        cols
    }
}

/// Find " AS " at the top level (not inside parentheses) in an upper-case string.
/// Returns the byte offset of the space before AS.
fn find_top_level_as(upper: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    // Search from the END to find the last top-level AS (handles nested subqueries).
    let mut last_as = None;
    while i + 4 <= bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b' ' if depth == 0 && i + 4 <= bytes.len() => {
                if &upper[i..i + 4] == " AS " {
                    last_as = Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    last_as
}

/// Extract the last identifier from an expression.
/// Handles: `table.column` → `column`, `func(...)` → `func`, bare `name` → `name`.
fn extract_last_identifier(expr: &str) -> &str {
    let trimmed = expr.trim();
    // If it ends with ), find the function name before the parens
    if trimmed.ends_with(')') {
        // Find matching open paren
        let mut depth = 0;
        for (i, c) in trimmed.char_indices().rev() {
            match c {
                ')' => depth += 1,
                '(' => {
                    depth -= 1;
                    if depth == 0 {
                        let before = trimmed[..i].trim();
                        // Get last token before (
                        return before.rsplit(|c: char| c == '.' || c == ' ').next().unwrap_or(before);
                    }
                }
                _ => {}
            }
        }
        return trimmed;
    }
    // Handle `::type` casts — strip them
    let without_cast = if let Some(idx) = trimmed.find("::") {
        &trimmed[..idx]
    } else {
        trimmed
    };
    // Handle `/* comment */` — strip them
    let clean = if let Some(idx) = without_cast.find("/*") {
        without_cast[..idx].trim()
    } else {
        without_cast.trim()
    };
    // Take last segment after dot or space
    clean.rsplit(|c: char| c == '.' || c == ' ').next().unwrap_or(clean)
}

/// Collapse all whitespace (newlines, tabs, multiple spaces) into single spaces.
fn normalize_whitespace(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut prev_ws = false;
    for c in s.chars() {
        if c.is_ascii_whitespace() {
            if !prev_ws {
                result.push(' ');
            }
            prev_ws = true;
        } else {
            result.push(c);
            prev_ws = false;
        }
    }
    result
}

/// Check if a SELECT query has a real FROM <table> clause (not just "from" inside a function).
/// E.g. `SELECT * FROM users` → true, `SELECT extract(epoch from now())` → false.
fn has_from_table_clause(upper_sql: &str) -> bool {
    // Find " FROM " and check if it's followed by a table-like identifier
    // (not a keyword or closing paren that would indicate it's inside a function).
    let search = upper_sql;
    let mut pos = 0;
    while let Some(idx) = search[pos..].find(" FROM ") {
        let abs = pos + idx + 6; // position after " FROM "
        if abs >= search.len() {
            return false;
        }
        let after = search[abs..].trim_start();
        // If what follows FROM is a parenthesis or quote or keyword like "pg_",
        // it could still be a function arg. Check if it looks like a table name.
        if let Some(first_char) = after.chars().next() {
            // Table names start with a letter or underscore or double-quote
            // BUT skip if the FROM is inside parentheses (function call context).
            // Simple heuristic: count open/close parens before this FROM.
            let before = &search[..pos + idx];
            let open = before.chars().filter(|&c| c == '(').count();
            let close = before.chars().filter(|&c| c == ')').count();
            if open == close && (first_char.is_ascii_alphabetic() || first_char == '_' || first_char == '"') {
                return true;
            }
        }
        pos = abs;
    }
    false
}

/// Try to parse a simple `SELECT <literal>` without FROM as a literal result.
/// Handles: `SELECT 1`, `SELECT 'hello'`, `SELECT 1 AS col_name`, etc.
/// Rejects complex expressions (CASE, subqueries, function calls, casts).
/// Returns `Some((column_name, value))` if parseable.
fn try_select_literal(sql: &str) -> Option<(String, String)> {
    // Strip "SELECT " prefix (case-insensitive)
    let rest = if sql.len() > 7 && sql[..7].eq_ignore_ascii_case("SELECT ") {
        &sql[7..]
    } else {
        return None;
    };
    let rest = rest.trim();

    // Split on AS to get value and optional alias
    let (value_part, alias) = if let Some(idx) = rest.to_uppercase().find(" AS ") {
        let val = rest[..idx].trim();
        let alias = rest[idx + 4..].trim().trim_matches('"');
        (val, alias.to_string())
    } else {
        (rest, "?column?".to_string())
    };

    // Only accept simple literals:
    // - Numeric: 1, 42, 3.14
    // - String: 'hello', "hello"
    // - Simple identifiers without dots/parens: true, false
    // Reject anything with parentheses, CASE, ::, operators, etc.
    let upper_val = value_part.to_uppercase();
    if value_part.contains('(')
        || value_part.contains(')')
        || value_part.contains("::")
        || upper_val.starts_with("CASE ")
        || upper_val.contains(" WHEN ")
        || value_part.contains('.')
    {
        return None;
    }

    // Strip quotes from string literals
    let value = value_part
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    Some((alias, value))
}

/// Handle SELECT queries with one or more expressions (no FROM clause).
/// Parses comma-separated expressions, evaluates known functions, and returns a multi-column row.
fn handle_select_expressions<W: Write>(
    writer: &mut W,
    db_name: &str,
    sql: &str,
    include_row_desc: bool,
) -> io::Result<Option<()>> {
    let rest = if sql.len() > 7 && sql[..7].eq_ignore_ascii_case("SELECT ") {
        &sql[7..]
    } else {
        return Ok(None);
    };

    // Split on top-level commas (not inside parentheses).
    let exprs = split_top_level_commas(rest);
    if exprs.is_empty() {
        return Ok(None);
    }

    let mut columns = Vec::with_capacity(exprs.len());
    let mut values = Vec::with_capacity(exprs.len());

    for expr in &exprs {
        let expr = expr.trim();
        let (val_part, alias) = extract_alias(expr);
        let upper_val = val_part.to_uppercase();
        let upper_val = upper_val.trim();

        let value = if upper_val == "CURRENT_DATABASE()" {
            db_name.to_string()
        } else if upper_val == "CURRENT_SCHEMA()" || upper_val == "CURRENT_SCHEMA" {
            "public".to_string()
        } else if upper_val == "CURRENT_USER" || upper_val == "SESSION_USER" {
            "oxidb".to_string()
        } else if upper_val == "VERSION()" {
            "OxiDB 0.17.0 (PostgreSQL compatible)".to_string()
        } else if upper_val.starts_with("CURRENT_SETTING(") {
            if upper_val.contains("SERVER_VERSION") {
                "15.0".to_string()
            } else if upper_val.contains("MAX_INDEX_KEYS") {
                "32".to_string()
            } else {
                String::new()
            }
        } else if upper_val == "CURRENT_SCHEMAS(FALSE)" || upper_val == "CURRENT_SCHEMAS(TRUE)" {
            "{public}".to_string()
        } else if upper_val == "INET_SERVER_PORT()" {
            "5433".to_string()
        } else if upper_val == "PG_BACKEND_PID()" {
            std::process::id().to_string()
        } else if upper_val.starts_with("PG_CATALOG.") {
            // pg_catalog.function() — strip prefix and recurse
            let inner = &val_part[11..]; // skip "pg_catalog."
            let inner_upper = inner.to_uppercase();
            let inner_upper = inner_upper.trim();
            if inner_upper == "CURRENT_DATABASE()" {
                db_name.to_string()
            } else if inner_upper == "CURRENT_SCHEMA()" {
                "public".to_string()
            } else if inner_upper.starts_with("SET_CONFIG(") {
                String::new()
            } else {
                // Unknown pg_catalog function — return empty string
                String::new()
            }
        } else if upper_val.starts_with("HAS_SCHEMA_PRIVILEGE(")
            || upper_val.starts_with("HAS_DATABASE_PRIVILEGE(")
            || upper_val.starts_with("HAS_TABLE_PRIVILEGE(")
        {
            "true".to_string()
        } else if upper_val.starts_with("OBJ_DESCRIPTION(")
            || upper_val.starts_with("SHOBJ_DESCRIPTION(")
            || upper_val.starts_with("COL_DESCRIPTION(")
        {
            String::new()
        } else if upper_val.starts_with("PG_GET_USERBYID(") {
            "oxidb".to_string()
        } else if upper_val.starts_with("ARRAY_TO_STRING(") {
            String::new()
        } else if upper_val.starts_with("QUOTE_IDENT(") {
            // Return the first argument value as-is
            let inner = &val_part[12..];
            inner
                .trim()
                .trim_end_matches(')')
                .split(',')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('\'')
                .to_string()
        } else if upper_val == "PG_IS_IN_RECOVERY()" {
            "f".to_string()
        } else if upper_val.starts_with("PG_ENCODING_TO_CHAR(") {
            "UTF8".to_string()
        } else if upper_val.starts_with("FORMAT_TYPE(") {
            String::new()
        } else if upper_val.starts_with("PG_TOTAL_RELATION_SIZE(")
            || upper_val.starts_with("PG_RELATION_SIZE(")
            || upper_val.starts_with("PG_TABLE_SIZE(")
        {
            "0".to_string()
        } else if upper_val.starts_with("PG_GET_EXPR(") {
            String::new()
        } else if upper_val.starts_with("PG_GET_CONSTRAINTDEF(") {
            String::new()
        } else if upper_val.starts_with("PG_GET_INDEXDEF(") {
            String::new()
        } else if upper_val.starts_with("PG_GET_PARTKEYDEF(") {
            String::new()
        } else if upper_val.starts_with("PG_STAT_GET_") {
            "0".to_string()
        } else if upper_val.starts_with("HAS_FUNCTION_PRIVILEGE(")
            || upper_val.starts_with("HAS_SEQUENCE_PRIVILEGE(")
            || upper_val.starts_with("HAS_TYPE_PRIVILEGE(")
            || upper_val.starts_with("HAS_SERVER_PRIVILEGE(")
            || upper_val.starts_with("HAS_FOREIGN_DATA_WRAPPER_PRIVILEGE(")
            || upper_val.starts_with("HAS_LANGUAGE_PRIVILEGE(")
            || upper_val.starts_with("HAS_TABLESPACE_PRIVILEGE(")
            || upper_val.starts_with("HAS_COLUMN_PRIVILEGE(")
            || upper_val.starts_with("PG_HAS_ROLE(")
        {
            "true".to_string()
        } else if upper_val.starts_with("ARRAY_AGG(")
            || upper_val.starts_with("ARRAY_UPPER(")
            || upper_val.starts_with("ARRAY_LOWER(")
        {
            String::new()
        } else if upper_val.starts_with("GENERATE_SERIES(") {
            String::new()
        } else {
            // Unknown expression — return empty string to stay consistent
            // with describe_intercepted_columns which always returns columns.
            String::new()
        };

        columns.push(text_col(&alias));
        values.push(Some(value));
    }

    if include_row_desc {
        codec::write_row_description(writer, &columns)?;
    }
    codec::write_data_row(writer, &values)?;
    codec::write_command_complete(writer, "SELECT 1")?;
    Ok(Some(()))
}

/// Split a string on commas that are not inside parentheses or quoted strings.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    for (i, c) in s.char_indices() {
        match c {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote => depth -= 1,
            ',' if depth == 0 && !in_single_quote && !in_double_quote => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

/// Rewrite a SELECT query to use LIMIT 1 for column type inference.
/// This avoids loading massive datasets (e.g. collections with embedding vectors)
/// just to determine column names during the Describe phase.
fn limit_for_inference(sql: &str) -> String {
    let upper = sql.to_uppercase();
    if !upper.starts_with("SELECT") {
        return sql.to_string();
    }
    // If already has LIMIT, replace it with LIMIT 1
    if let Some(idx) = upper.rfind(" LIMIT ") {
        let after = &sql[idx + 7..].trim_start();
        // Find end of the LIMIT value
        let end = after.find(|c: char| !c.is_ascii_digit()).unwrap_or(after.len());
        let before = &sql[..idx];
        let rest = &after[end..];
        return format!("{before} LIMIT 1{rest}");
    }
    // No LIMIT clause: append one
    let trimmed = sql.trim_end_matches(';').trim();
    format!("{trimmed} LIMIT 1")
}

/// Split a SQL string on semicolons, respecting single-quoted and double-quoted strings.
fn split_statements(sql: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    for (i, c) in sql.char_indices() {
        match c {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            ';' if !in_single_quote && !in_double_quote => {
                let stmt = sql[start..i].trim();
                if !stmt.is_empty() {
                    result.push(stmt);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = sql[start..].trim();
    if !last.is_empty() {
        result.push(last);
    }
    result
}

/// Extract alias from an expression like `expr AS alias` or `expr alias`.
fn extract_alias(expr: &str) -> (&str, String) {
    let upper = expr.to_uppercase();
    if let Some(idx) = upper.rfind(" AS ") {
        let val = expr[..idx].trim();
        let alias = expr[idx + 4..].trim().trim_matches('"');
        (val, alias.to_string())
    } else {
        // Use the expression itself as alias, cleaned up
        let alias = expr
            .trim()
            .trim_end_matches(')')
            .rsplit('(')
            .last()
            .unwrap_or(expr)
            .trim();
        let alias = if alias.contains('.') {
            alias.rsplit('.').next().unwrap_or(alias)
        } else {
            alias
        };
        (expr.trim(), alias.to_string())
    }
}

fn text_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        type_oid: types::OID_TEXT,
        type_len: -1,
        type_mod: -1,
    }
}

fn int_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        type_oid: types::OID_INT8,
        type_len: 8,
        type_mod: -1,
    }
}

fn bool_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        type_oid: types::OID_BOOL,
        type_len: 1,
        type_mod: -1,
    }
}

/// Send a single-row, single-column result.
/// If `include_row_desc` is true, include RowDescription (Simple Query).
/// If false, only DataRow + CommandComplete (Extended Query Execute).
fn send_row<W: Write>(
    writer: &mut W,
    col_name: &str,
    value: &str,
    include_row_desc: bool,
) -> io::Result<()> {
    if include_row_desc {
        let columns = vec![text_col(col_name)];
        codec::write_row_description(writer, &columns)?;
    }
    codec::write_data_row(writer, &[Some(value.to_string())])?;
    codec::write_command_complete(writer, "SELECT 1")?;
    Ok(())
}

/// Send an empty result set with a dummy column (avoids 0-column RowDescription).
fn send_empty<W: Write>(writer: &mut W, include_row_desc: bool) -> io::Result<()> {
    if include_row_desc {
        codec::write_row_description(writer, &[text_col("result")])?;
    }
    codec::write_command_complete(writer, "SELECT 0")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Arc<OxiDb> {
        let dir = tempfile::tempdir().unwrap();
        Arc::new(OxiDb::open(dir.path()).unwrap())
    }

    fn test_db_manager() -> Arc<DatabaseManager> {
        let dir = tempfile::tempdir().unwrap();
        Arc::new(DatabaseManager::open(dir.path(), None, false, None).unwrap())
    }

    #[test]
    fn test_error_to_sqlstate() {
        assert_eq!(error_to_sqlstate(&oxidb::Error::InvalidQuery("x".into())).0, "42601");
        assert_eq!(error_to_sqlstate(&oxidb::Error::CollectionNotFound("x".into())).0, "42P01");
        assert_eq!(
            error_to_sqlstate(&oxidb::Error::UniqueViolation { field: "e".into() }).0,
            "23505"
        );
        assert_eq!(
            error_to_sqlstate(&oxidb::Error::TransactionConflict {
                collection: "c".into(),
                doc_id: 1,
                expected_version: 1,
                actual_version: 2,
            })
            .0,
            "40001"
        );
        assert_eq!(error_to_sqlstate(&oxidb::Error::NotAnObject).0, "XX000");
    }

    #[test]
    fn test_ddl_command_tag() {
        assert_eq!(ddl_command_tag("table 'users' created"), "CREATE TABLE");
        assert_eq!(ddl_command_tag("table 'users' dropped"), "DROP TABLE");
        assert_eq!(ddl_command_tag("index created on field 'name'"), "CREATE INDEX");
        assert_eq!(ddl_command_tag("something else"), "OK");
    }

    // Simple Query mode tests (include_row_desc = true)
    #[test]
    fn test_intercepted_set() {
        let mut buf = Vec::new();
        let result = handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","SET client_encoding TO 'UTF8'", true, &[]);
        assert!(result.unwrap().is_some());
        assert_eq!(buf[0], b'C');
    }

    #[test]
    fn test_intercepted_select_version() {
        let mut buf = Vec::new();
        let result = handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","SELECT version()", true, &[]);
        assert!(result.unwrap().is_some());
        assert_eq!(buf[0], b'T'); // RowDescription present
    }

    #[test]
    fn test_extended_query_no_row_desc() {
        // In extended query mode (include_row_desc=false), Execute should NOT send RowDescription
        let mut buf = Vec::new();
        let result = handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","SELECT version()", false, &[]);
        assert!(result.unwrap().is_some());
        assert_eq!(buf[0], b'D'); // DataRow, NOT RowDescription
    }

    #[test]
    fn test_intercepted_pg_catalog() {
        let mut buf = Vec::new();
        let result =
            handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","SELECT * FROM pg_catalog.pg_type", true, &[]);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_not_intercepted() {
        let mut buf = Vec::new();
        let result = handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","SELECT * FROM users", true, &[]);
        assert!(result.unwrap().is_none());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_show_intercepted() {
        let mut buf = Vec::new();
        let result = handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","SHOW transaction_isolation", true, &[]);
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_begin_commit_rollback() {
        let mut buf = Vec::new();
        assert!(handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","BEGIN", true, &[]).unwrap().is_some());
        buf.clear();
        assert!(handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","COMMIT", true, &[]).unwrap().is_some());
        buf.clear();
        assert!(handle_intercepted_query(&mut buf, &test_db(), &test_db_manager(), "oxidb","ROLLBACK", true, &[]).unwrap().is_some());
    }

    #[test]
    fn test_pg_database_encoding() {
        let mut buf = Vec::new();
        let db = test_db();
        let mgr = test_db_manager();
        let result = handle_intercepted_query(
            &mut buf, &db, &mgr, "oxidb",
            "SELECT pg_encoding_to_char(encoding) FROM pg_catalog.pg_database WHERE datname = current_database()",
            true, &[],
        );
        assert!(result.unwrap().is_some());
        assert_eq!(buf[0], b'T');
    }

    #[test]
    fn test_pg_namespace() {
        let mut buf = Vec::new();
        let db = test_db();
        let mgr = test_db_manager();
        let result = handle_intercepted_query(
            &mut buf, &db, &mgr, "oxidb",
            "SELECT oid, nspname FROM pg_catalog.pg_namespace",
            true, &[],
        );
        assert!(result.unwrap().is_some());
        assert_eq!(buf[0], b'T');
    }

    #[test]
    fn test_set_config() {
        let mut buf = Vec::new();
        let db = test_db();
        let mgr = test_db_manager();
        let result = handle_intercepted_query(
            &mut buf, &db, &mgr, "oxidb",
            "select pg_catalog.set_config('search_path', '', false)",
            true, &[],
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_describe_select_version() {
        let cols = describe_intercepted_columns("SELECT version()");
        assert!(cols.is_some());
        let cols = cols.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "version");
    }

    #[test]
    fn test_describe_pg_database() {
        let cols = describe_intercepted_columns(
            "SELECT pg_encoding_to_char(encoding) FROM pg_catalog.pg_database WHERE datname = current_database()",
        );
        assert!(cols.is_some());
        assert_eq!(cols.unwrap().len(), 1);
    }

    #[test]
    fn test_write_select_result() {
        use serde_json::json;
        let result = oxidb::SqlResult::Select(vec![
            json!({"_id": 1, "name": "Alice"}),
            json!({"_id": 2, "name": "Bob"}),
        ]);
        let mut buf = Vec::new();
        write_sql_result(&mut buf, result, true).unwrap();
        assert_eq!(buf[0], b'T');
    }

    #[test]
    fn test_write_select_result_no_desc() {
        use serde_json::json;
        let result = oxidb::SqlResult::Select(vec![json!({"_id": 1, "name": "Alice"})]);
        let mut buf = Vec::new();
        write_sql_result(&mut buf, result, false).unwrap();
        assert_eq!(buf[0], b'D'); // DataRow first, no RowDescription
    }

    #[test]
    fn test_write_insert_result() {
        let result = oxidb::SqlResult::Insert(vec![1, 2, 3]);
        let mut buf = Vec::new();
        write_sql_result(&mut buf, result, true).unwrap();
        let payload = String::from_utf8_lossy(&buf[5..]);
        assert!(payload.contains("INSERT 0 3"));
    }

    #[test]
    fn test_write_update_result() {
        let result = oxidb::SqlResult::Update(5);
        let mut buf = Vec::new();
        write_sql_result(&mut buf, result, true).unwrap();
        let payload = String::from_utf8_lossy(&buf[5..]);
        assert!(payload.contains("UPDATE 5"));
    }

    #[test]
    fn test_write_delete_result() {
        let result = oxidb::SqlResult::Delete(3);
        let mut buf = Vec::new();
        write_sql_result(&mut buf, result, true).unwrap();
        let payload = String::from_utf8_lossy(&buf[5..]);
        assert!(payload.contains("DELETE 3"));
    }
}
