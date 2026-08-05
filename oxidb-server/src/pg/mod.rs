//! PostgreSQL v3 wire protocol listener.
//!
//! A tenth protocol on its own port (`OXIDB_PG_PORT`, off by default), serving
//! the SQL engine to unmodified PostgreSQL clients — `psql`, `psycopg`, and
//! anything else that speaks the frontend/backend protocol. **Nothing in the
//! OxiWire path is touched**: this module reads the same per-database engines
//! through `sql_bridge`, the same user store through `auth`, and the same SCRAM
//! verifiers through `scram`, but it is a separate listener end to end.
//!
//! Layout: `wire` is the codec, `auth` the handshake, `session` the state and
//! the one call into the engine, `catalog` the statements that never reach it,
//! `types` the value mapping, `errors` the SQLSTATEs. This file is the loop
//! that reads a message, decides who handles it, and writes the replies.

pub mod auth;
pub mod catalog;
pub mod errors;
pub mod pgcatalog;
pub mod session;
pub mod types;
pub mod wire;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use errors::{PgError, SQLSTATE_UNDEFINED_DATABASE};
use session::{PgSession, Reply};
use wire::{Conn, Msg, Reader, Startup};

use crate::auth::UserStore;
use oxidb::database_manager::DEFAULT_DATABASE;

/// What the listener needs from the server, passed in rather than reached for:
/// the server's own state struct differs between the standalone and cluster
/// builds, and this module should not care which one it was compiled with.
#[derive(Clone)]
pub struct PgConfig {
    pub user_store: Option<Arc<Mutex<UserStore>>>,
    pub auth_enabled: bool,
    /// Raft is active. Writes are node-local on this port, so they are refused
    /// rather than allowed to diverge a replica.
    pub cluster: bool,
    /// The server's TLS config when `OXIDB_TLS_CERT`/`_KEY` are set. A
    /// PostgreSQL client asks for TLS *before* the startup packet, so the
    /// answer has to be decided in this module rather than by the listener.
    pub tls: Option<Arc<rustls::ServerConfig>>,
}

/// Serve one client. Owns the socket for the connection's lifetime.
pub fn handle_client(stream: TcpStream, cfg: PgConfig) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    let mut conn = Conn::new(stream);

    // SSL/GSSAPI negotiation comes before the startup packet, and a client may
    // ask for each in turn before settling.
    let startup = loop {
        match conn.read_startup() {
            Ok(Startup::Ssl) => {
                let Some(tls_cfg) = cfg.tls.clone() else {
                    // 'N' = no TLS here; a client with sslmode=prefer carries
                    // on in plaintext, one with sslmode=require disconnects.
                    if conn.w().write_all(b"N").is_err() || conn.flush().is_err() {
                        return;
                    }
                    continue;
                };
                if conn.w().write_all(b"S").is_err() || conn.flush().is_err() {
                    return;
                }
                let sock = conn.into_inner();
                match rustls::ServerConnection::new(tls_cfg) {
                    Ok(tls_conn) => {
                        let stream = rustls::StreamOwned::new(tls_conn, sock);
                        return serve(Conn::new(stream), cfg, &peer);
                    }
                    Err(e) => {
                        eprintln!("[pg] TLS setup failed for {peer}: {e}");
                        return;
                    }
                }
            }
            Ok(Startup::GssEnc) => {
                if conn.w().write_all(b"N").is_err() || conn.flush().is_err() {
                    return;
                }
            }
            // Query cancellation needs a registry of running backends to be
            // worth anything; there is nothing to interrupt here, and the
            // protocol says the server may ignore it. Silently, per spec — the
            // connection carries no reply.
            Ok(Startup::Cancel(..)) => return,
            Ok(Startup::Params(p)) => break p,
            Err(e) => {
                eprintln!("[pg] {peer}: {e}");
                return;
            }
        }
    };
    serve_after_startup(conn, cfg, &peer, startup);
}

/// TLS path: the startup packet has not been read yet on the encrypted stream.
fn serve<S: Read + Write>(mut conn: Conn<S>, cfg: PgConfig, peer: &str) {
    match conn.read_startup() {
        Ok(Startup::Params(p)) => serve_after_startup(conn, cfg, peer, p),
        Ok(_) => {
            let _ = wire::fatal_response(
                conn.w(),
                errors::SQLSTATE_PROTOCOL_VIOLATION,
                "expected a startup packet on the encrypted connection",
            );
            let _ = conn.flush();
        }
        Err(e) => eprintln!("[pg] {peer}: {e}"),
    }
}

fn serve_after_startup<S: Read + Write>(
    mut conn: Conn<S>,
    cfg: PgConfig,
    peer: &str,
    params: Vec<(String, String)>,
) {
    let get = |key: &str| {
        params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.clone())
    };
    let user = get("user").unwrap_or_default();
    if user.is_empty() {
        fatal(
            &mut conn,
            errors::SQLSTATE_INVALID_AUTHORIZATION,
            "no user name was supplied",
        );
        return;
    }
    // PostgreSQL defaults the database to the user name; OxiDB's default
    // database is a better guess for a client that omitted it.
    let database = get("database").unwrap_or_else(|| DEFAULT_DATABASE.to_string());

    let engine = match crate::sql_bridge::engine(&database) {
        Ok(e) => e,
        Err(msg) => {
            fatal(&mut conn, SQLSTATE_UNDEFINED_DATABASE, &msg);
            return;
        }
    };

    let role = match auth::authenticate(
        &mut conn,
        &user,
        &database,
        cfg.auth_enabled,
        cfg.user_store.as_deref(),
    ) {
        Ok(r) => r,
        Err(e) => {
            fatal(&mut conn, e.code, &e.message);
            return;
        }
    };

    let mut sess = PgSession::new(user.clone(), database.clone(), role, cfg.cluster, engine);
    if let Some(app) = get("application_name") {
        sess.settings.insert("application_name".into(), app);
    }

    // ParameterStatus, then the key a CancelRequest would carry, then "your
    // turn". Clients read their capabilities out of these.
    let defaults = catalog::defaults(&user, sess.readonly, role == crate::auth::Role::Admin);
    for name in catalog::STARTUP_PARAMETERS {
        if let Some((_, v)) = defaults.iter().find(|(k, _)| k == name)
            && wire::parameter_status(conn.w(), name, v).is_err()
        {
            return;
        }
    }
    if wire::backend_key_data(conn.w(), std::process::id() as i32, 0).is_err()
        || wire::ready_for_query(conn.w(), sess.tx_status()).is_err()
        || conn.flush().is_err()
    {
        return;
    }

    eprintln!(
        "[pg] {peer} connected as {user:?} to {database:?} (role {})",
        role.as_str()
    );
    let result = message_loop(&mut conn, &mut sess);
    sess.close();
    if let Err(e) = result
        && e.kind() != std::io::ErrorKind::UnexpectedEof
    {
        eprintln!("[pg] {peer}: {e}");
    }
}

fn fatal<S: Read + Write>(conn: &mut Conn<S>, code: &str, message: &str) {
    let _ = wire::fatal_response(conn.w(), code, message);
    let _ = conn.flush();
}

/// The main loop: one frontend message at a time until Terminate or EOF.
fn message_loop<S: Read + Write>(conn: &mut Conn<S>, sess: &mut PgSession) -> std::io::Result<()> {
    // The extended protocol's error rule: after an error the server discards
    // every message until the next Sync, so a client that pipelined a whole
    // batch is not answered statement by statement for work it abandoned.
    let mut skip_until_sync = false;

    loop {
        let msg = conn.read()?;
        match msg.tag {
            wire::F_TERMINATE => return Ok(()),
            wire::F_SYNC => {
                skip_until_sync = false;
                wire::ready_for_query(conn.w(), sess.tx_status())?;
                conn.flush()?;
            }
            wire::F_FLUSH => conn.flush()?,
            _ if skip_until_sync => {}
            wire::F_QUERY => {
                simple_query(conn, sess, &msg)?;
            }
            wire::F_PARSE => {
                if let Err(e) = parse(conn, sess, &msg) {
                    error_response(conn, &e)?;
                    skip_until_sync = true;
                }
            }
            wire::F_BIND => {
                if let Err(e) = bind(conn, sess, &msg) {
                    error_response(conn, &e)?;
                    skip_until_sync = true;
                }
            }
            wire::F_DESCRIBE => {
                if let Err(e) = describe(conn, sess, &msg) {
                    error_response(conn, &e)?;
                    skip_until_sync = true;
                }
            }
            wire::F_EXECUTE => {
                if let Err(e) = execute(conn, sess, &msg) {
                    error_response(conn, &e)?;
                    skip_until_sync = true;
                }
            }
            wire::F_CLOSE => {
                if let Err(e) = close(conn, sess, &msg) {
                    error_response(conn, &e)?;
                    skip_until_sync = true;
                }
            }
            other => {
                error_response(
                    conn,
                    &PgError::protocol(format!("unsupported message type '{}'", other as char)),
                )?;
                skip_until_sync = true;
            }
        }
    }
}

fn error_response<S: Read + Write>(conn: &mut Conn<S>, e: &PgError) -> std::io::Result<()> {
    wire::error_response(conn.w(), e.code, &e.message, None)?;
    conn.flush()
}

// ── simple query ────────────────────────────────────────────────────────────

fn simple_query<S: Read + Write>(
    conn: &mut Conn<S>,
    sess: &mut PgSession,
    msg: &Msg,
) -> std::io::Result<()> {
    let sql = Reader::new(&msg.body).cstring()?;
    if sql.trim().trim_end_matches(';').is_empty() {
        wire::empty_query_response(conn.w())?;
        wire::ready_for_query(conn.w(), sess.tx_status())?;
        return conn.flush();
    }
    match sess.execute(&sql, &[]) {
        Ok(replies) => {
            for reply in replies {
                write_reply(conn, reply, true)?;
            }
        }
        Err(e) => wire::error_response(conn.w(), e.code, &e.message, None)?,
    }
    wire::ready_for_query(conn.w(), sess.tx_status())?;
    conn.flush()
}

/// Write one reply. `describe_rows` is false when the client already received
/// a `RowDescription` from a `Describe` on the portal — sending a second one
/// would be a protocol violation.
fn write_reply<S: Read + Write>(
    conn: &mut Conn<S>,
    reply: Reply,
    describe_rows: bool,
) -> std::io::Result<()> {
    match reply {
        Reply::Notice(text) => wire::notice_response(conn.w(), &text),
        Reply::Tag(tag) => wire::command_complete(conn.w(), &tag),
        Reply::Suspended => wire::portal_suspended(conn.w()),
        Reply::Rows { fields, rows, tag } => {
            if describe_rows {
                wire::row_description(conn.w(), &fields)?;
            }
            for row in &rows {
                let cells = encode_row(row, &fields)?;
                wire::data_row(conn.w(), &cells)?;
            }
            match tag {
                Some(t) => wire::command_complete(conn.w(), &t),
                // Suspended part-way: the statement has not finished, so no
                // completion goes out — a `Suspended` reply follows instead.
                None => Ok(()),
            }
        }
    }
}

/// Encode one row in the formats its columns were described with.
fn encode_row(
    row: &[oxidb_sql::Value],
    fields: &[wire::FieldDesc],
) -> std::io::Result<Vec<Option<Vec<u8>>>> {
    row.iter()
        .enumerate()
        .map(|(i, v)| {
            let f = fields.get(i);
            let format = f.map_or(types::FORMAT_TEXT, |f| f.format);
            let oid = f.map_or(types::OID_TEXT, |f| f.type_oid);
            Ok(if format == types::FORMAT_BINARY {
                types::to_binary(v, oid)
            } else {
                types::to_text(v)
            })
        })
        .collect()
}

// ── extended query ──────────────────────────────────────────────────────────

fn parse<S: Read + Write>(
    conn: &mut Conn<S>,
    sess: &mut PgSession,
    msg: &Msg,
) -> Result<(), PgError> {
    let mut r = Reader::new(&msg.body);
    let name = r.cstring().map_err(protocol)?;
    let sql = r.cstring().map_err(protocol)?;
    let count = r.i16().map_err(protocol)?;
    let mut param_oids = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count.max(0) {
        param_oids.push(r.i32().map_err(protocol)?);
    }
    // PostgreSQL's own rule, and one this server depends on: a portal holds at
    // most one result set, so a prepared statement holds one command. (A text
    // the engine cannot parse is left alone — it may be a `SET` the catalog
    // layer handles, and its syntax error belongs to Execute.)
    if let Ok(kinds) = sess.engine.command_kinds(&sql)
        && kinds.len() > 1
    {
        return Err(PgError::syntax(
            "cannot insert multiple commands into a prepared statement",
        ));
    }
    sess.prepared
        .insert(name, session::Prepared { sql, param_oids });
    wire::parse_complete(conn.w()).map_err(protocol)
}

fn bind<S: Read + Write>(
    conn: &mut Conn<S>,
    sess: &mut PgSession,
    msg: &Msg,
) -> Result<(), PgError> {
    let mut r = Reader::new(&msg.body);
    let portal_name = r.cstring().map_err(protocol)?;
    let stmt_name = r.cstring().map_err(protocol)?;

    let stmt = sess.prepared.get(&stmt_name).cloned().ok_or_else(|| {
        PgError::protocol(format!("prepared statement {stmt_name:?} does not exist"))
    })?;

    // Parameter formats, then the parameters themselves, then result formats.
    let fmt_count = r.i16().map_err(protocol)?;
    let mut param_formats = Vec::with_capacity(fmt_count.max(0) as usize);
    for _ in 0..fmt_count.max(0) {
        param_formats.push(r.i16().map_err(protocol)?);
    }
    let value_count = r.i16().map_err(protocol)?;
    let mut params = Vec::with_capacity(value_count.max(0) as usize);
    for i in 0..value_count.max(0) as usize {
        let bytes = r.nullable_bytes().map_err(protocol)?;
        let oid = stmt
            .param_oids
            .get(i)
            .copied()
            .unwrap_or(types::OID_UNSPECIFIED);
        let format = session::format_for(&param_formats, i);
        params.push(types::decode_param(bytes, oid, format).map_err(PgError::from)?);
    }
    let res_count = r.i16().map_err(protocol)?;
    let mut result_formats = Vec::with_capacity(res_count.max(0) as usize);
    for _ in 0..res_count.max(0) {
        result_formats.push(r.i16().map_err(protocol)?);
    }

    sess.portals.insert(
        portal_name,
        session::Portal {
            sql: stmt.sql,
            params,
            result_formats,
            executed: None,
            described: false,
        },
    );
    wire::bind_complete(conn.w()).map_err(protocol)
}

fn describe<S: Read + Write>(
    conn: &mut Conn<S>,
    sess: &mut PgSession,
    msg: &Msg,
) -> Result<(), PgError> {
    let mut r = Reader::new(&msg.body);
    let what = r.u8().map_err(protocol)?;
    let name = r.cstring().map_err(protocol)?;
    match what {
        b'S' => {
            let stmt = sess.prepared.get(&name).ok_or_else(|| {
                PgError::protocol(format!("prepared statement {name:?} does not exist"))
            })?;
            // The engine infers no parameter types, so a client that asked the
            // server to decide (OID 0) is told `text` and its own coercion
            // takes over on the way in.
            let oids: Vec<i32> = stmt
                .param_oids
                .iter()
                .map(|o| {
                    if *o == types::OID_UNSPECIFIED {
                        types::OID_TEXT
                    } else {
                        *o
                    }
                })
                .collect();
            wire::parameter_description(conn.w(), &oids).map_err(protocol)?;
            // Column types are only knowable by running the statement, which
            // Describe must not do. NoData is the protocol's answer for that;
            // the RowDescription follows at Execute.
            wire::no_data(conn.w()).map_err(protocol)
        }
        b'P' => match sess.portal_fields(&name)? {
            Some(fields) => wire::row_description(conn.w(), &fields).map_err(protocol),
            None => wire::no_data(conn.w()).map_err(protocol),
        },
        other => Err(PgError::protocol(format!(
            "Describe target '{}' is neither a statement nor a portal",
            other as char
        ))),
    }
}

fn execute<S: Read + Write>(
    conn: &mut Conn<S>,
    sess: &mut PgSession,
    msg: &Msg,
) -> Result<(), PgError> {
    let mut r = Reader::new(&msg.body);
    let name = r.cstring().map_err(protocol)?;
    let max_rows = r.i32().map_err(protocol)?.max(0) as usize;

    // The RowDescription goes out once per portal, whether it was a `Describe`
    // or the first `Execute` that sent it — a second one is a protocol
    // violation, and a resumed portal must not repeat it.
    let mut described = sess.portals.get(&name).is_some_and(|p| p.described);
    let replies = sess.execute_portal(&name, max_rows)?;
    for reply in replies {
        let is_rows = matches!(reply, Reply::Rows { .. });
        let describe_rows = is_rows && !described;
        if is_rows {
            described = true;
        }
        write_reply(conn, reply, describe_rows).map_err(protocol)?;
    }
    if described && let Some(p) = sess.portals.get_mut(&name) {
        p.described = true;
    }
    Ok(())
}

fn close<S: Read + Write>(
    conn: &mut Conn<S>,
    sess: &mut PgSession,
    msg: &Msg,
) -> Result<(), PgError> {
    let mut r = Reader::new(&msg.body);
    let what = r.u8().map_err(protocol)?;
    let name = r.cstring().map_err(protocol)?;
    match what {
        b'S' => {
            sess.prepared.remove(&name);
        }
        b'P' => {
            sess.portals.remove(&name);
        }
        other => {
            return Err(PgError::protocol(format!(
                "Close target '{}' is neither a statement nor a portal",
                other as char
            )));
        }
    }
    wire::close_complete(conn.w()).map_err(protocol)
}

fn protocol(e: std::io::Error) -> PgError {
    PgError::protocol(e.to_string())
}
