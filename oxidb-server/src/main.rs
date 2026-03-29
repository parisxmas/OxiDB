#[cfg(not(target_os = "windows"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configure jemalloc to aggressively return freed memory to the OS.
#[cfg(not(target_os = "windows"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:100,muzzy_decay_ms:100\0";

use oxidb_server::audit::{self, AuditEvent, AuditLog};
use oxidb_server::auth::UserStore;
use oxidb_server::gelf::{GelfLevel, GelfLogger};
use oxidb_server::handler;
use oxidb_server::protocol;
use oxidb_server::rbac;
use oxidb_server::oximem;
use oxidb_server::scram::ScramState;
use oxidb_server::session::Session;
use oxidb_server::tls;

#[cfg(feature = "cluster")]
use openraft::storage::Adaptor;
#[cfg(feature = "cluster")]
use oxidb_server::async_server::{self, ServerState as AsyncServerState};
#[cfg(feature = "cluster")]
use oxidb_server::raft::config::RaftConfig;
#[cfg(feature = "cluster")]
use oxidb_server::raft::log_store::OxiDbStore;
#[cfg(feature = "cluster")]
use oxidb_server::raft::network::{self, OxiDbNetworkFactory};

use std::env;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxidb::{DatabaseManager, OxiDb};

/// Log to stderr and optionally to a GELF endpoint.
///
/// Usage:
/// - `server_log!(state, GelfLevel::Error, "something failed: {e}");`
/// - `server_log!(state, GelfLevel::Info, "msg", extra: "peer" => &peer);`
macro_rules! server_log {
    // With extra GELF fields
    ($state:expr, $level:expr, $fmt:expr, extra: $( $key:expr => $val:expr ),+ $(,)?) => {{
        let msg = $fmt.to_string();
        eprintln!("{msg}");
        if let Some(gelf) = &$state.gelf {
            gelf.send($level, &msg, &[ $( ($key, $val) ),+ ]);
        }
    }};
    // Without extra fields
    ($state:expr, $level:expr, $fmt:expr) => {{
        let msg = $fmt.to_string();
        eprintln!("{msg}");
        if let Some(gelf) = &$state.gelf {
            gelf.send($level, &msg, &[]);
        }
    }};
}

fn configure_stream(stream: &TcpStream, idle_timeout: Duration) {
    let _ = stream.set_read_timeout(Some(idle_timeout));
    let _ = stream.set_nodelay(true);
}

/// Shared server state passed to each connection handler.
struct ServerState {
    db: Arc<OxiDb>,
    db_manager: Arc<DatabaseManager>,
    user_store: Option<Arc<Mutex<UserStore>>>,
    audit_log: Option<Arc<AuditLog>>,
    gelf: Option<Arc<GelfLogger>>,
    auth_enabled: bool,
}

/// Dispatch a single request through auth -> RBAC -> handler pipeline.
fn dispatch_request(
    request: &serde_json::Value,
    state: &ServerState,
    session: &mut Session,
    active_tx: &mut Option<u64>,
    _peer: &str,
) -> Vec<u8> {
    let cmd = request
        .get("cmd")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let collection = request
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // ---------------------------------------------------------------
    // Authentication flow (SCRAM-SHA-256)
    // ---------------------------------------------------------------
    if state.auth_enabled && !session.is_authenticated() {
        return match cmd.as_str() {
            "ping" => handler::ok_bytes(serde_json::json!("pong")),

            "authenticate" => {
                let client_first = request
                    .get("payload")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let user_store_guard = state.user_store.as_ref().unwrap().lock().unwrap();
                match ScramState::process_client_first(client_first, &user_store_guard) {
                    Ok((server_first, scram_state)) => {
                        drop(user_store_guard);
                        session.scram_state = Some(scram_state);
                        handler::ok_bytes(serde_json::json!({
                            "payload": server_first,
                            "done": false,
                        }))
                    }
                    Err(e) => handler::err_bytes(&e),
                }
            }

            "authenticate_continue" => {
                let client_final = request
                    .get("payload")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if let Some(scram_state) = session.scram_state.take() {
                    let user_store_guard =
                        state.user_store.as_ref().unwrap().lock().unwrap();
                    match scram_state.process_client_final(client_final, &user_store_guard) {
                        Ok((server_final, role)) => {
                            let username = scram_state.username().to_string();
                            drop(user_store_guard);
                            session.set_authenticated(username, role);
                            handler::ok_bytes(serde_json::json!({
                                "payload": server_final,
                                "done": true,
                            }))
                        }
                        Err(e) => handler::err_bytes(&e),
                    }
                } else {
                    handler::err_bytes("no SCRAM state; send 'authenticate' first")
                }
            }

            "auth_simple" => {
                let username = request
                    .get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let password = request
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let user_store_guard = state.user_store.as_ref().unwrap().lock().unwrap();
                match user_store_guard.authenticate(username, password) {
                    Some(role) => {
                        drop(user_store_guard);
                        session.set_authenticated(username.to_string(), role);
                        handler::ok_bytes(serde_json::json!({
                            "role": role.as_str(),
                            "done": true,
                        }))
                    }
                    None => handler::err_bytes("authentication failed"),
                }
            }

            _ => handler::err_bytes("authentication required"),
        };
    }

    // ---------------------------------------------------------------
    // RBAC check
    // ---------------------------------------------------------------
    if state.auth_enabled {
        if let Some(role) = session.role() {
            let is_user_cmd = matches!(
                cmd.as_str(),
                "create_user" | "drop_user" | "update_user" | "list_users"
                    | "grant_db_role" | "revoke_db_role"
            );

            // Resolve the effective role for the target database.
            // For user management and database-level commands, use the global role.
            // For all other commands, use the per-database effective role.
            let effective_role = if is_user_cmd
                || matches!(cmd.as_str(), "create_database" | "drop_database" | "list_databases" | "use_db")
            {
                role
            } else if let Some(ref user_store) = state.user_store {
                let target_db = request
                    .get("db")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&session.current_database);
                let store = user_store.lock().unwrap();
                store
                    .effective_role(session.username_str(), target_db)
                    .unwrap_or(role)
            } else {
                role
            };

            let permitted = if is_user_cmd {
                role == oxidb_server::auth::Role::Admin
            } else {
                rbac::is_permitted(effective_role, &cmd)
            };

            if !permitted {
                log_audit(state, session, &cmd, collection.as_deref(), "denied", "");
                return handler::err_bytes(&format!(
                    "permission denied: role '{}' cannot execute '{}'",
                    role.as_str(),
                    cmd
                ));
            }
        }
    }

    // ---------------------------------------------------------------
    // Database management commands
    // ---------------------------------------------------------------
    match cmd.as_str() {
        "create_database" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return handler::err_bytes("missing 'name'"),
            };
            match state.db_manager.create_database(name) {
                Ok(()) => {
                    log_audit(state, session, &cmd, None, "ok", name);
                    return handler::ok_bytes(serde_json::json!(format!("database '{name}' created")));
                }
                Err(e) => return handler::err_bytes(&e.to_string()),
            }
        }
        "drop_database" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return handler::err_bytes("missing 'name'"),
            };
            match state.db_manager.drop_database(name) {
                Ok(()) => {
                    log_audit(state, session, &cmd, None, "ok", name);
                    return handler::ok_bytes(serde_json::json!(format!("database '{name}' dropped")));
                }
                Err(e) => return handler::err_bytes(&e.to_string()),
            }
        }
        "list_databases" => {
            let names = state.db_manager.list_databases();
            log_audit(state, session, &cmd, None, "ok", "");
            return handler::ok_bytes(serde_json::json!(names));
        }
        "use_db" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return handler::err_bytes("missing 'name'"),
            };
            if !state.db_manager.database_exists(name) {
                return handler::err_bytes(&format!("database not found: {name}"));
            }
            session.set_database(name.to_string());
            log_audit(state, session, &cmd, None, "ok", name);
            return handler::ok_bytes(serde_json::json!(format!("switched to database '{name}'")));
        }
        "set_dialect" => {
            let dialect_str = match request.get("dialect").and_then(|v| v.as_str()) {
                Some(d) => d,
                None => return handler::err_bytes("missing 'dialect' (mysql, postgresql, mssql, generic)"),
            };
            let dialect = oxidb::SqlDialect::from_str(dialect_str);
            session.set_sql_dialect(dialect);
            log_audit(state, session, &cmd, None, "ok", dialect_str);
            return handler::ok_bytes(serde_json::json!(format!("SQL dialect set to {dialect:?}")));
        }
        _ => {}
    }

    // ---------------------------------------------------------------
    // Handle user management commands
    // ---------------------------------------------------------------
    if let Some(user_store) = &state.user_store {
        if let Some(resp_bytes) =
            handler::handle_user_command(&cmd, request, user_store)
        {
            log_audit(state, session, &cmd, None, "ok", "");
            return resp_bytes;
        }
    }

    // ---------------------------------------------------------------
    // Resolve the target database for this request
    // ---------------------------------------------------------------
    let target_db_name = request
        .get("db")
        .and_then(|v| v.as_str())
        .unwrap_or(&session.current_database);
    let target_db = match state.db_manager.get_database(target_db_name) {
        Ok(db) => db,
        Err(e) => return handler::err_bytes(&e.to_string()),
    };

    // ---------------------------------------------------------------
    // Standard command dispatch
    // ---------------------------------------------------------------
    let resp_bytes = if cmd == "sql" {
        // SQL commands get access to the DatabaseManager for CREATE/DROP DATABASE.
        let query_str = match request.get("query").and_then(|v| v.as_str()) {
            Some(q) => q,
            None => return handler::err_bytes("missing 'query' string"),
        };
        let dialect = request
            .get("dialect")
            .and_then(|v| v.as_str())
            .map(oxidb::SqlDialect::from_str)
            .unwrap_or(session.sql_dialect);
        match oxidb::sql::execute_sql_with_db_manager(&target_db, query_str, Some(&state.db_manager), dialect) {
            Ok(result) => match result {
                oxidb::SqlResult::Select(docs) => handler::ok_bytes(serde_json::json!(docs)),
                oxidb::SqlResult::Insert(ids) => handler::ok_bytes(serde_json::json!({ "ids": ids })),
                oxidb::SqlResult::Update(count) => handler::ok_bytes(serde_json::json!({ "modified": count })),
                oxidb::SqlResult::Delete(count) => handler::ok_bytes(serde_json::json!({ "deleted": count })),
                oxidb::SqlResult::Ddl(msg) => handler::ok_bytes(serde_json::json!(msg)),
                oxidb::SqlResult::UseDatabase(name) => {
                    if !state.db_manager.database_exists(&name) {
                        handler::err_bytes(&format!("database not found: {name}"))
                    } else {
                        session.set_database(name.clone());
                        handler::ok_bytes(serde_json::json!(format!("switched to database '{name}'")))
                    }
                }
                oxidb::SqlResult::ShowDatabases(names) => {
                    let docs: Vec<serde_json::Value> = names
                        .into_iter()
                        .map(|n| serde_json::json!({"database_name": n}))
                        .collect();
                    handler::ok_bytes(serde_json::json!(docs))
                }
            },
            Err(e) => handler::err_bytes(&e.to_string()),
        }
    } else {
        handler::handle_request(&target_db, request.clone(), active_tx)
    };

    log_audit(state, session, &cmd, collection.as_deref(), "ok", "");

    resp_bytes
}

/// Parsed watch request parameters.
struct WatchRequest {
    filter: oxidb::WatchFilter,
    resume_after: Option<u64>,
}

/// Check if an incoming request is a `watch` command.
/// Returns `Ok(Some(params))` if authorized, `Err(msg)` if watch but unauthorized,
/// `Ok(None)` if not a watch command.
fn try_watch_request(
    request: &serde_json::Value,
    state: &ServerState,
    session: &Session,
) -> std::result::Result<Option<WatchRequest>, &'static str> {
    let cmd = match request.get("cmd").and_then(|v| v.as_str()) {
        Some(c) => c,
        None => return Ok(None),
    };
    if cmd != "watch" {
        return Ok(None);
    }
    // Require authentication with Admin role
    if state.auth_enabled {
        if !session.is_authenticated() {
            return Err("authentication required");
        }
        if session.role() != Some(oxidb_server::auth::Role::Admin) {
            return Err("permission denied: watch requires Admin role");
        }
    }
    let filter = match request.get("collection").and_then(|v| v.as_str()) {
        Some(col) => oxidb::WatchFilter::Collection(col.to_string()),
        None => oxidb::WatchFilter::All,
    };
    let resume_after = request.get("resume_after").and_then(|v| v.as_u64());
    Ok(Some(WatchRequest { filter, resume_after }))
}

/// Watch mode loop: push change events to the client and listen for `unwatch`.
///
/// Uses a reader thread to avoid blocking on `read_message` while pushing events.
/// The reader thread sends parsed messages through a channel. The main loop polls
/// both the event receiver and the reader channel.
///
/// Before each event, checks `handle.take_dropped()` and sends an overflow message
/// if any events were dropped due to backpressure.
fn handle_watch_mode<R: Read + Send + 'static, W: Write>(
    reader: R,
    writer: &mut W,
    state: &ServerState,
    handle: oxidb::WatchHandle,
    peer: &str,
) {
    let sub_id = handle.id;

    // Spawn a reader thread that forwards raw messages to a channel.
    let (msg_tx, msg_rx) = mpsc::channel::<Option<Vec<u8>>>();
    let _reader_handle = std::thread::spawn({
        let msg_tx = msg_tx;
        let mut reader = reader;
        move || {
            loop {
                match protocol::read_message(&mut reader) {
                    Ok(data) => {
                        if msg_tx.send(Some(data)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut
                        {
                            // Idle timeout — signal disconnect
                        }
                        let _ = msg_tx.send(None);
                        break;
                    }
                }
            }
        }
    });

    loop {
        // Check for change events (non-blocking with short timeout)
        match handle.rx.recv_timeout(Duration::from_millis(50)) {
            Ok(event) => {
                // Check for backpressure drops before sending
                let dropped = handle.take_dropped();
                if dropped > 0 {
                    let overflow_msg = serde_json::json!({
                        "event": "overflow",
                        "data": { "dropped": dropped },
                    });
                    let overflow_bytes = overflow_msg.to_string().into_bytes();
                    if let Err(e) = protocol::write_message(writer, &overflow_bytes) {
                        server_log!(state, GelfLevel::Error, format!("watch write error to {peer}: {e}"), extra: "peer" => peer);
                        break;
                    }
                }

                let msg = serde_json::json!({
                    "event": "change",
                    "data": event,
                });
                let msg_bytes = msg.to_string().into_bytes();
                if let Err(e) = protocol::write_message(writer, &msg_bytes) {
                    server_log!(state, GelfLevel::Error, format!("watch write error to {peer}: {e}"), extra: "peer" => peer);
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }

        // Check for client commands (non-blocking)
        match msg_rx.try_recv() {
            Ok(Some(data)) => {
                if let Ok(request) = serde_json::from_slice::<serde_json::Value>(&data) {
                    let cmd = request.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
                    if cmd == "unwatch" {
                        state.db.unwatch(sub_id);
                        let ack = handler::ok_bytes(serde_json::json!("unwatched"));
                        let _ = protocol::write_message(writer, &ack);
                        return; // Return to normal connection loop
                    }
                }
            }
            Ok(None) => {
                // Reader disconnected
                break;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        }
    }

    state.db.unwatch(sub_id);
}

/// Generic message loop for split reader/writer (plain TCP).
fn handle_connection(
    stream: &TcpStream,
    state: &ServerState,
    peer: &str,
) {
    let mut active_tx: Option<u64> = None;
    let mut session = Session::new();

    if !state.auth_enabled {
        session.set_authenticated("anonymous".to_string(), oxidb_server::auth::Role::Admin);
    }

    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);

    loop {
        let msg = match protocol::read_message(&mut reader) {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    server_log!(state, GelfLevel::Warning, format!("idle timeout for {peer}, disconnecting"), extra: "peer" => peer);
                } else if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    server_log!(state, GelfLevel::Error, format!("read error from {peer}: {e}"), extra: "peer" => peer);
                }
                break;
            }
        };

        let (request, wire_fmt) = match protocol::deserialize_message(&msg) {
            Ok(v) => v,
            Err(e) => {
                let resp =
                    serde_json::json!({"ok": false, "error": e});
                let resp_bytes = protocol::serialize_response(&resp, protocol::WireFormat::Json);
                let _ = protocol::write_message(&mut writer, &resp_bytes);
                continue;
            }
        };
        protocol::set_wire_format(wire_fmt);

        // Check for watch command
        match try_watch_request(&request, state, &session) {
            Err(msg) => {
                let resp = handler::err_bytes(msg);
                let _ = protocol::write_message(&mut writer, &resp);
                continue;
            }
            Ok(Some(watch_req)) => {
                let handle = match state.db.watch(watch_req.filter, watch_req.resume_after) {
                    Ok(h) => h,
                    Err(oxidb::ResumeError::TokenTooOld) => {
                        let resp = handler::err_bytes("resume token too old");
                        let _ = protocol::write_message(&mut writer, &resp);
                        continue;
                    }
                };
                let sub_id = handle.id;
                let ack = handler::ok_bytes(serde_json::json!("watching"));
                if let Err(e) = protocol::write_message(&mut writer, &ack) {
                    server_log!(state, GelfLevel::Error, format!("write error to {peer}: {e}"), extra: "peer" => peer);
                    state.db.unwatch(sub_id);
                    break;
                }
                // Enter watch mode — clone the TcpStream for the reader thread.
                let reader_stream = match stream.try_clone() {
                    Ok(s) => BufReader::new(s),
                    Err(e) => {
                        server_log!(state, GelfLevel::Error, format!("stream clone error for {peer}: {e}"), extra: "peer" => peer);
                        state.db.unwatch(sub_id);
                        break;
                    }
                };
                handle_watch_mode(reader_stream, &mut writer, state, handle, peer);
                // The reader thread may still be blocked on the cloned TcpStream.
                // Break out to avoid two concurrent readers on the same socket.
                break;
            }
            Ok(None) => {}
        }

        // Pipeline command: process multiple commands in one roundtrip
        let resp_bytes = if request.get("cmd").and_then(|v| v.as_str()) == Some("pipeline") {
            handle_pipeline(&request, state, &mut session, &mut active_tx, peer)
        } else {
            dispatch_request(&request, state, &mut session, &mut active_tx, peer)
        };

        if let Err(e) = protocol::write_message(&mut writer, &resp_bytes) {
            server_log!(state, GelfLevel::Error, format!("write error to {peer}: {e}"), extra: "peer" => peer);
            break;
        }
    }

    if let Some(tx_id) = active_tx {
        let _ = state.db.rollback_transaction(tx_id);
    }
}

/// Process a pipeline command: execute multiple commands in one roundtrip.
/// Request: {"cmd":"pipeline","commands":[{...},{...},...]}
/// Response: {"ok":true,"data":[<result1>,<result2>,...]}
/// Each result is the raw bytes of the individual command response.
fn handle_pipeline(
    request: &serde_json::Value,
    state: &ServerState,
    session: &mut Session,
    active_tx: &mut Option<u64>,
    peer: &str,
) -> Vec<u8> {
    let commands = match request.get("commands").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return handler::err_bytes("pipeline: missing 'commands' array"),
    };

    match protocol::wire_format() {
        protocol::WireFormat::Json => {
            let mut buf = Vec::with_capacity(commands.len() * 128 + 64);
            buf.extend_from_slice(b"{\"ok\":true,\"data\":[");
            for (i, cmd) in commands.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                let resp = dispatch_request(cmd, state, session, active_tx, peer);
                buf.extend_from_slice(&resp);
            }
            buf.extend_from_slice(b"]}");
            buf
        }
        protocol::WireFormat::MsgPack => {
            let mut buf = Vec::with_capacity(commands.len() * 128 + 64);
            rmp::encode::write_map_len(&mut buf, 2).unwrap();
            rmp::encode::write_str(&mut buf, "ok").unwrap();
            rmp::encode::write_bool(&mut buf, true).unwrap();
            rmp::encode::write_str(&mut buf, "data").unwrap();
            rmp::encode::write_array_len(&mut buf, commands.len() as u32).unwrap();
            for cmd in commands {
                let resp = dispatch_request(cmd, state, session, active_tx, peer);
                buf.extend_from_slice(&resp);
            }
            buf
        }
        protocol::WireFormat::OxiWire => {
            // OxiWire pipeline: each sub-response is an OxiWire envelope [0xDB][status][value].
            // We need to wrap them as maps {ok: bool, data/error: ...} inside the outer array.
            let mut sub_values = Vec::with_capacity(commands.len());
            for cmd in commands {
                let resp = dispatch_request(cmd, state, session, active_tx, peer);
                // resp is OxiWire: [0xDB][status][value]
                // Decode it to reconstruct as a map value
                if resp.len() >= 2 && resp[0] == oxidb_server::oxiwire::MAGIC {
                    let status = resp[1];
                    let mut pos = 2;
                    let val = oxidb_server::oxiwire::decode_value(&resp, &mut pos)
                        .unwrap_or(serde_json::Value::Null);
                    if status == 0 {
                        sub_values.push(serde_json::json!({"ok": true, "data": val}));
                    } else {
                        sub_values.push(serde_json::json!({"ok": false, "error": val}));
                    }
                } else {
                    sub_values.push(serde_json::Value::Null);
                }
            }
            handler::ok_bytes(serde_json::json!(sub_values))
        }
    }
}

/// Variant for streams that are a single Read+Write object (e.g. TLS).
fn handle_connection_single<S: Read + Write>(
    stream: &mut S,
    state: &ServerState,
    peer: &str,
) {
    let mut active_tx: Option<u64> = None;
    let mut session = Session::new();

    if !state.auth_enabled {
        session.set_authenticated("anonymous".to_string(), oxidb_server::auth::Role::Admin);
    }

    loop {
        let msg = match protocol::read_message(stream) {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    server_log!(state, GelfLevel::Warning, format!("idle timeout for {peer}, disconnecting"), extra: "peer" => peer);
                } else if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    server_log!(state, GelfLevel::Error, format!("read error from {peer}: {e}"), extra: "peer" => peer);
                }
                break;
            }
        };

        let (request, wire_fmt) = match protocol::deserialize_message(&msg) {
            Ok(v) => v,
            Err(e) => {
                let resp =
                    serde_json::json!({"ok": false, "error": e});
                let resp_bytes = protocol::serialize_response(&resp, protocol::WireFormat::Json);
                let _ = protocol::write_message(stream, &resp_bytes);
                continue;
            }
        };
        protocol::set_wire_format(wire_fmt);

        // Intercept watch/unwatch — not supported over TLS (can't split the stream)
        if request.get("cmd").and_then(|v| v.as_str()) == Some("watch") {
            let resp = handler::err_bytes("watch is not supported over TLS connections");
            let _ = protocol::write_message(stream, &resp);
            continue;
        }

        let resp_bytes = if request.get("cmd").and_then(|v| v.as_str()) == Some("pipeline") {
            handle_pipeline(&request, state, &mut session, &mut active_tx, peer)
        } else {
            dispatch_request(
                &request,
                state,
                &mut session,
                &mut active_tx,
                peer,
            )
        };

        if let Err(e) = protocol::write_message(stream, &resp_bytes) {
            server_log!(state, GelfLevel::Error, format!("write error to {peer}: {e}"), extra: "peer" => peer);
            break;
        }
    }

    if let Some(tx_id) = active_tx {
        let _ = state.db.rollback_transaction(tx_id);
    }
}

fn log_audit(
    state: &ServerState,
    session: &Session,
    cmd: &str,
    collection: Option<&str>,
    result: &str,
    detail: &str,
) {
    if let Some(audit) = &state.audit_log {
        audit.log(&AuditEvent {
            ts: audit::now_rfc3339(),
            user: session.username_str(),
            cmd,
            collection,
            result,
            detail,
        });
    }
}

fn handle_client(stream: TcpStream, state: &Arc<ServerState>, idle_timeout: Duration, tls_config: Option<&Arc<rustls::ServerConfig>>) {
    configure_stream(&stream, idle_timeout);

    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    server_log!(state, GelfLevel::Informational, format!("client connected: {peer}"), extra: "peer" => &peer);

    if let Some(tls_cfg) = tls_config {
        // TLS connection
        let conn = match rustls::ServerConnection::new(Arc::clone(tls_cfg)) {
            Ok(c) => c,
            Err(e) => {
                server_log!(state, GelfLevel::Error, format!("TLS handshake error for {peer}: {e}"), extra: "peer" => &peer);
                return;
            }
        };
        let mut tls_stream = rustls::StreamOwned::new(conn, stream);
        // StreamOwned<ServerConnection, TcpStream> implements both Read + Write.
        // handle_connection_single takes a single &mut impl Read + Write.
        handle_connection_single(&mut tls_stream, state, &peer);
    } else {
        // Plain TCP connection
        handle_connection(&stream, state, &peer);
    }

    server_log!(state, GelfLevel::Informational, format!("client disconnected: {peer}"), extra: "peer" => &peer);
}

/// Log a RESP response value to stderr.
fn log_resp_value(value: &oxidb_server::resp::RespValue) {
    use oxidb_server::resp::RespValue;
    match value {
        RespValue::SimpleString(s) => eprintln!("[oximem] >> +{s}"),
        RespValue::Error(s) => eprintln!("[oximem] >> -{s}"),
        RespValue::Integer(n) => eprintln!("[oximem] >> :{n}"),
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(b);
            if s.len() > 100 {
                eprintln!("[oximem] >> ${}...({} bytes)", &s[..100], b.len());
            } else {
                eprintln!("[oximem] >> ${s}");
            }
        }
        RespValue::Array(items) => eprintln!("[oximem] >> *{} items", items.len()),
        RespValue::Null => eprintln!("[oximem] >> (nil)"),
    }
}

/// Handle a single OxiMem RESP client connection.
fn handle_oximem_client(stream: TcpStream, store: &oximem::OxiMemStore, log: bool) {
    use oxidb_server::resp;

    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    if log {
        eprintln!("[oximem] connected peer={peer}");
    }

    loop {
        // Read first command (blocking)
        let value = match resp::read_value(&mut reader) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        };

        // Check for SUBSCRIBE before normal dispatch
        if let resp::RespValue::Array(ref items) = value {
            if let Some(cmd) = items.first().and_then(|a| a.as_str()) {
                if cmd.eq_ignore_ascii_case("SUBSCRIBE") {
                    if log {
                        let channels: Vec<&str> = items[1..].iter().filter_map(|a| a.as_str()).collect();
                        eprintln!("[oximem] << SUBSCRIBE {}", channels.join(" "));
                    }
                    handle_oximem_subscribe(&stream, store, &mut reader, &mut writer, &items[1..]);
                    return;
                }
            }
        }

        // Fast path: no pipeline data buffered — execute directly, no Vec
        if reader.buffer().is_empty() {
            let args = match value {
                resp::RespValue::Array(ref items) => items.as_slice(),
                _ => {
                    let _ = resp::write_value(&mut writer, &resp::err("expected array"));
                    let _ = writer.flush();
                    continue;
                }
            };
            if log {
                let cmd_str: Vec<&str> = args.iter().filter_map(|a| a.as_str()).collect();
                eprintln!("[oximem] << {}", cmd_str.join(" "));
            }
            let response = oximem::execute(store, args);
            if log {
                log_resp_value(&response);
            }
            if resp::write_value(&mut writer, &response).is_err() {
                break;
            }
        } else {
            // Pipeline: collect all buffered commands
            let mut batch = vec![value];
            while !reader.buffer().is_empty() {
                match resp::read_value(&mut reader) {
                    Ok(v) => batch.push(v),
                    Err(_) => break,
                }
            }

            if !store.sql_enabled() {
                // Lock-coalesced pipeline (fast in-memory mode)
                let responses = oximem::execute_pipeline(store, &batch);
                for response in &responses {
                    if resp::write_value(&mut writer, response).is_err() {
                        return;
                    }
                }
            } else {
                // SQL mode — execute one by one
                for cmd in &batch {
                    let args = match cmd {
                        resp::RespValue::Array(items) => items.as_slice(),
                        _ => {
                            let _ = resp::write_value(&mut writer, &resp::err("expected array"));
                            continue;
                        }
                    };
                    let response = oximem::execute(store, args);
                    if resp::write_value(&mut writer, &response).is_err() {
                        return;
                    }
                }
            }
        }

        // Single flush for entire batch — key pipeline optimization
        if writer.flush().is_err() {
            break;
        }
    }
}

/// Subscription mode: receive published messages and handle SUBSCRIBE/UNSUBSCRIBE.
fn handle_oximem_subscribe(
    stream: &TcpStream,
    store: &oximem::OxiMemStore,
    reader: &mut BufReader<&TcpStream>,
    writer: &mut BufWriter<&TcpStream>,
    initial_channels: &[oxidb_server::resp::RespValue],
) {
    use oxidb_server::resp;

    let mut receivers: Vec<(String, std::sync::mpsc::Receiver<(String, String)>)> = Vec::new();

    // Subscribe to initial channels
    for arg in initial_channels {
        if let Some(ch) = arg.as_str() {
            let rx = store.subscribe(ch);
            receivers.push((ch.to_string(), rx));
            // Send subscribe confirmation
            let msg = resp::array(vec![
                resp::bulk_string("subscribe"),
                resp::bulk_string(ch),
                resp::integer(receivers.len() as i64),
            ]);
            let _ = resp::write_value(writer, &msg);
        }
    }
    let _ = writer.flush();

    // Set read timeout so we can alternate between checking messages and reading commands
    let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));

    loop {
        // Deliver any pending messages
        let mut wrote = false;
        for (ch_name, rx) in &receivers {
            while let Ok((channel, message)) = rx.try_recv() {
                let msg = resp::array(vec![
                    resp::bulk_string("message"),
                    resp::bulk_string(&channel),
                    resp::bulk_string(&message),
                ]);
                if resp::write_value(writer, &msg).is_err() {
                    return;
                }
                wrote = true;
                let _ = ch_name; // used for tracking
            }
        }
        if wrote {
            if writer.flush().is_err() {
                return;
            }
        }

        // Try to read a command (non-blocking due to timeout)
        match resp::read_value(reader) {
            Ok(value) => {
                if let resp::RespValue::Array(ref items) = value {
                    if let Some(cmd) = items.first().and_then(|a| a.as_str()) {
                        if cmd.eq_ignore_ascii_case("SUBSCRIBE") {
                            for arg in &items[1..] {
                                if let Some(ch) = arg.as_str() {
                                    let rx = store.subscribe(ch);
                                    receivers.push((ch.to_string(), rx));
                                    let msg = resp::array(vec![
                                        resp::bulk_string("subscribe"),
                                        resp::bulk_string(ch),
                                        resp::integer(receivers.len() as i64),
                                    ]);
                                    let _ = resp::write_value(writer, &msg);
                                }
                            }
                            let _ = writer.flush();
                        } else if cmd.eq_ignore_ascii_case("UNSUBSCRIBE") {
                            if items.len() > 1 {
                                for arg in &items[1..] {
                                    if let Some(ch) = arg.as_str() {
                                        receivers.retain(|(name, _)| name != ch);
                                        store.unsubscribe(ch);
                                        let msg = resp::array(vec![
                                            resp::bulk_string("unsubscribe"),
                                            resp::bulk_string(ch),
                                            resp::integer(receivers.len() as i64),
                                        ]);
                                        let _ = resp::write_value(writer, &msg);
                                    }
                                }
                            } else {
                                // Unsubscribe from all
                                for (name, _) in &receivers {
                                    store.unsubscribe(name);
                                }
                                receivers.clear();
                                let msg = resp::array(vec![
                                    resp::bulk_string("unsubscribe"),
                                    resp::null(),
                                    resp::integer(0),
                                ]);
                                let _ = resp::write_value(writer, &msg);
                            }
                            let _ = writer.flush();
                            if receivers.is_empty() {
                                return; // Exit subscription mode
                            }
                        } else if cmd.eq_ignore_ascii_case("PING") {
                            let _ = resp::write_value(writer, &resp::array(vec![
                                resp::bulk_string("pong"),
                                resp::bulk_string(""),
                            ]));
                            let _ = writer.flush();
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => break,
        }
    }
}

fn main() {
    // If OXIDB_NODE_ID is set and cluster feature is enabled, run in cluster mode.
    #[cfg(feature = "cluster")]
    if env::var("OXIDB_NODE_ID").is_ok() {
        run_cluster_mode();
        return;
    }

    let addr = env::var("OXIDB_ADDR").unwrap_or_else(|_| "127.0.0.1:4444".to_string());
    let data_dir = env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".to_string());
    let pool_size: usize = env::var("OXIDB_POOL_SIZE")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .expect("OXIDB_POOL_SIZE must be a valid usize");

    let idle_timeout_secs: u64 = env::var("OXIDB_IDLE_TIMEOUT")
        .unwrap_or_else(|_| "30".to_string())
        .parse()
        .expect("OXIDB_IDLE_TIMEOUT must be a valid u64 (seconds)");
    let idle_timeout = Duration::from_secs(idle_timeout_secs);

    // Verbose mode: --verbose flag or OXIDB_VERBOSE=true env var
    let verbose = env::args().any(|a| a == "--verbose")
        || env::var("OXIDB_VERBOSE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

    // GELF UDP logging (e.g. OXIDB_GELF_ADDR=192.0.2.100:12201)
    let gelf = match env::var("OXIDB_GELF_ADDR") {
        Ok(gelf_addr) => {
            let logger = GelfLogger::new(&gelf_addr).expect("failed to create GELF logger");
            eprintln!("GELF logging: enabled ({gelf_addr})");
            let logger = Arc::new(logger);
            logger.send(GelfLevel::Informational, &format!("GELF logging: enabled ({gelf_addr})"), &[]);
            Some(logger)
        }
        Err(_) => None,
    };

    if verbose {
        eprintln!("verbose: enabled");
    }

    // Encryption at rest
    let encryption_key = match env::var("OXIDB_ENCRYPTION_KEY") {
        Ok(path) => {
            let key = oxidb::EncryptionKey::load_from_file(Path::new(&path))
                .expect("failed to load encryption key");
            eprintln!("encryption-at-rest: enabled");
            if let Some(g) = &gelf {
                g.send(GelfLevel::Informational, "encryption-at-rest: enabled", &[]);
            }
            Some(key)
        }
        Err(_) => None,
    };

    let open_start = std::time::Instant::now();
    let log_callback: Option<oxidb::LogCallback> = if let Some(ref g) = gelf {
        let gelf_cb = Arc::clone(g);
        Some(Arc::new(move |msg: &str| {
            gelf_cb.send(GelfLevel::Informational, msg, &[]);
        }))
    } else {
        None
    };
    // In-memory mode: OXIDB_MODE=memory runs everything in RAM (no disk I/O).
    // All OxiDB features (SQL, indexes, transactions, aggregation, TTL) work unchanged.
    let oxidb_mode = env::var("OXIDB_MODE").unwrap_or_default();
    let in_memory_mode = oxidb_mode == "memory" || oxidb_mode == "in-memory";
    let mqtt_only_mode = oxidb_mode == "mqtt";

    let db_manager = if in_memory_mode {
        eprintln!("mode: in-memory (all data in RAM, no persistence)");
        if let Some(g) = &gelf {
            g.send(GelfLevel::Informational, "mode: in-memory", &[]);
        }
        DatabaseManager::open_in_memory()
            .expect("failed to open in-memory database manager")
    } else {
        DatabaseManager::open(
            Path::new(&data_dir),
            encryption_key,
            verbose,
            log_callback,
        )
        .expect("failed to open database manager")
    };
    let db = db_manager
        .get_default_database()
        .expect("failed to open default database");
    if verbose {
        eprintln!(
            "[verbose] database opened in {:.2}s",
            open_start.elapsed().as_secs_f64()
        );
    }
    let db_manager = Arc::new(db_manager);
    db_manager.start_scheduler().expect("failed to start scheduler");

    // Lazy sync mode: defer per-write fsync to a background thread (default: enabled).
    // Matches MongoDB's default durability semantics (journal flushed every ~10ms).
    // Disable with OXIDB_LAZY_SYNC=false for strict per-write durability.
    let lazy_sync = env::var("OXIDB_LAZY_SYNC")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);
    if lazy_sync && !in_memory_mode {
        let sync_interval_ms: u64 = env::var("OXIDB_SYNC_INTERVAL_MS")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .expect("OXIDB_SYNC_INTERVAL_MS must be a valid u64");
        db.enable_lazy_sync(Duration::from_millis(sync_interval_ms));
        eprintln!("lazy sync: enabled (interval={}ms)", sync_interval_ms);
    }

    // TTL eviction thread: automatically remove expired documents.
    // Runs every 100ms in memory mode, every 1s in file mode.
    let ttl_interval = if in_memory_mode {
        Duration::from_millis(100)
    } else {
        Duration::from_secs(1)
    };
    db.start_ttl_thread(ttl_interval);
    eprintln!("TTL eviction: enabled (interval={}ms)", ttl_interval.as_millis());

    // Document cache capacity per collection (default: 100,000).
    if let Ok(cap_str) = env::var("OXIDB_CACHE_SIZE") {
        let cap: usize = cap_str.parse().expect("OXIDB_CACHE_SIZE must be a valid usize");
        db.set_cache_capacity(cap);
        eprintln!("doc cache capacity: {cap} per collection");
    }

    // TLS
    let tls_config = match (env::var("OXIDB_TLS_CERT"), env::var("OXIDB_TLS_KEY")) {
        (Ok(cert), Ok(key)) => {
            let config = tls::load_tls_config(Path::new(&cert), Path::new(&key))
                .expect("failed to load TLS config");
            eprintln!("TLS: enabled");
            if let Some(g) = &gelf {
                g.send(GelfLevel::Informational, "TLS: enabled", &[]);
            }
            Some(config)
        }
        _ => None,
    };

    // Authentication
    let auth_enabled = env::var("OXIDB_AUTH")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let user_store = if auth_enabled {
        let store =
            UserStore::open(Path::new(&data_dir)).expect("failed to open user store");
        eprintln!("authentication: enabled");
        if let Some(g) = &gelf {
            g.send(GelfLevel::Informational, "authentication: enabled", &[]);
        }
        Some(Arc::new(Mutex::new(store)))
    } else {
        None
    };

    // Audit logging
    let audit_enabled = env::var("OXIDB_AUDIT")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let audit_log = if audit_enabled {
        let log =
            AuditLog::open(Path::new(&data_dir)).expect("failed to open audit log");
        eprintln!("audit logging: enabled");
        if let Some(g) = &gelf {
            g.send(GelfLevel::Informational, "audit logging: enabled", &[]);
        }
        Some(Arc::new(log))
    } else {
        None
    };

    let state = Arc::new(ServerState {
        db,
        db_manager,
        user_store,
        audit_log,
        gelf,
        auth_enabled,
    });

    let listener = TcpListener::bind(&addr).expect("failed to bind TCP listener");
    server_log!(state, GelfLevel::Notice, format!("oxidb-server listening on {addr} (pool_size={pool_size}, data_dir={data_dir}, idle_timeout={idle_timeout_secs}s)"));

    // PostgreSQL wire protocol listener (optional, enabled via OXIDB_PG_PORT)
    let pg_port: u16 = env::var("OXIDB_PG_PORT")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("OXIDB_PG_PORT must be a valid u16");

    if pg_port > 0 {
        let pg_addr = format!("0.0.0.0:{pg_port}");
        let pg_listener =
            TcpListener::bind(&pg_addr).expect("failed to bind PostgreSQL wire protocol listener");
        server_log!(state, GelfLevel::Notice, format!("PostgreSQL wire protocol listening on {pg_addr}"));

        // Accept PG connections in a background thread, spawning a thread per connection.
        let state_pg = Arc::clone(&state);
        std::thread::spawn(move || {
            for stream in pg_listener.incoming() {
                match stream {
                    Ok(s) => {
                        let _ = s.set_nodelay(true);
                        let db_mgr = Arc::clone(&state_pg.db_manager);
                        std::thread::spawn(move || {
                            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                oxidb_server::pg_wire::handle_pg_connection(&s, &db_mgr);
                            }));
                            if let Err(e) = result {
                                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                                    s.to_string()
                                } else if let Some(s) = e.downcast_ref::<String>() {
                                    s.clone()
                                } else {
                                    "unknown panic".to_string()
                                };
                                eprintln!("[pg_wire] connection handler panicked: {msg}");
                            }
                        });
                    }
                    Err(e) => {
                        server_log!(state_pg, GelfLevel::Error, format!("[pg_wire] accept error: {e}"));
                    }
                }
            }
        });
    }

    // Command logging: OXIDB_LOG_COMMANDS=true logs all OxiMem/MQTT commands and responses
    let log_commands = env::var("OXIDB_LOG_COMMANDS")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // Shared store for OxiMem + MQTT pub/sub interop
    let oximem_sql = env::var("OXIDB_OXIMEM_SQL")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let shared_store = if oximem_sql {
        Arc::new(oxidb_server::oximem::OxiMemStore::new_with_sql(Arc::clone(&state.db)))
    } else {
        Arc::new(oxidb_server::oximem::OxiMemStore::new())
    };

    // OxiMem RESP listener (optional, enabled via OXIDB_OXIMEM_PORT)
    let oximem_port: u16 = env::var("OXIDB_OXIMEM_PORT")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("OXIDB_OXIMEM_PORT must be a valid u16");

    if oximem_port > 0 {
        let oximem_addr = format!("0.0.0.0:{oximem_port}");
        let oximem_listener =
            TcpListener::bind(&oximem_addr).expect("failed to bind OxiMem RESP listener");

        let mode_label = if oximem_sql { "SQL mirroring" } else { "fast mode" };
        server_log!(state, GelfLevel::Notice, format!("OxiMem RESP listening on {oximem_addr} ({mode_label})"));

        let state_oximem = Arc::clone(&state);
        let oximem_store = Arc::clone(&shared_store);
        let oximem_log = log_commands;
        std::thread::spawn(move || {
            for stream in oximem_listener.incoming() {
                match stream {
                    Ok(s) => {
                        let _ = s.set_nodelay(true);
                        let store = Arc::clone(&oximem_store);
                        std::thread::spawn(move || {
                            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                handle_oximem_client(s, &store, oximem_log);
                            }));
                            if let Err(e) = result {
                                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                                    s.to_string()
                                } else if let Some(s) = e.downcast_ref::<String>() {
                                    s.clone()
                                } else {
                                    "unknown panic".to_string()
                                };
                                eprintln!("[oximem] connection handler panicked: {msg}");
                            }
                        });
                    }
                    Err(e) => {
                        server_log!(state_oximem, GelfLevel::Error, format!("[oximem] accept error: {e}"));
                    }
                }
            }
        });
    }

    // MQTT listener (optional, enabled via OXIDB_MQTT_PORT)
    let mqtt_port: u16 = env::var("OXIDB_MQTT_PORT")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("OXIDB_MQTT_PORT must be a valid u16");

    if mqtt_port > 0 {
        let mqtt_addr = format!("0.0.0.0:{mqtt_port}");
        let mqtt_listener =
            TcpListener::bind(&mqtt_addr).expect("failed to bind MQTT listener");
        server_log!(state, GelfLevel::Notice, format!("MQTT listening on {mqtt_addr}"));

        let state_mqtt = Arc::clone(&state);
        let mqtt_store = Arc::clone(&shared_store);
        let mqtt_log = log_commands;
        std::thread::spawn(move || {
            for stream in mqtt_listener.incoming() {
                match stream {
                    Ok(s) => {
                        let _ = s.set_nodelay(true);
                        let store = Arc::clone(&mqtt_store);
                        std::thread::spawn(move || {
                            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                oxidb_server::mqtt::handle_client(s, &store, mqtt_log);
                            }));
                            if let Err(e) = result {
                                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                                    s.to_string()
                                } else if let Some(s) = e.downcast_ref::<String>() {
                                    s.clone()
                                } else {
                                    "unknown panic".to_string()
                                };
                                eprintln!("[mqtt] connection handler panicked: {msg}");
                            }
                        });
                    }
                    Err(e) => {
                        server_log!(state_mqtt, GelfLevel::Error, format!("[mqtt] accept error: {e}"));
                    }
                }
            }
        });
    }

    // S3-compatible HTTP API (optional, enabled via OXIDB_S3_PORT)
    #[cfg(feature = "s3")]
    {
        let s3_port: u16 = env::var("OXIDB_S3_PORT")
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .expect("OXIDB_S3_PORT must be a valid u16");

        if s3_port > 0 {
            let s3_addr = format!("0.0.0.0:{s3_port}");
            let s3_db = Arc::clone(&state.db);
            oxidb_server::s3::start_s3_listener(&s3_addr, s3_db);
            server_log!(state, GelfLevel::Notice,
                format!("S3-compatible API listening on {s3_addr}"));
        }
    }

    // UDP log ingestion listener (optional, enabled via OXIDB_UDP_PORT)
    let udp_port: u16 = env::var("OXIDB_UDP_PORT")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("OXIDB_UDP_PORT must be a valid u16");

    if udp_port > 0 {
        let udp_addr = format!("0.0.0.0:{udp_port}");
        let udp_collection = env::var("OXIDB_UDP_COLLECTION")
            .unwrap_or_else(|_| "_udp_logs".to_string());
        let udp_db = Arc::clone(&state.db);
        oxidb_server::udp_ingest::start_udp_listener(&udp_addr, udp_db, udp_collection.clone());
        server_log!(state, GelfLevel::Notice,
            format!("UDP log ingestion listening on {udp_addr} → collection '{udp_collection}'"));
    }

    // MQTT-only mode: skip the main OxiDB TCP listener
    if mqtt_only_mode {
        server_log!(state, GelfLevel::Notice, "MQTT-only mode — OxiDB TCP listener disabled".to_string());
        // Block forever (MQTT listener runs in its own thread)
        loop {
            std::thread::park();
        }
    }

    let (tx, rx) = mpsc::channel::<TcpStream>();
    let rx = Arc::new(Mutex::new(rx));

    for _ in 0..pool_size {
        let rx = Arc::clone(&rx);
        let state = Arc::clone(&state);
        let tls_config = tls_config.clone();
        std::thread::spawn(move || loop {
            let stream = rx.lock().unwrap().recv();
            match stream {
                Ok(stream) => handle_client(stream, &state, idle_timeout, tls_config.as_ref()),
                Err(_) => break,
            }
        });
    }

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if let Err(e) = tx.send(s) {
                    server_log!(state, GelfLevel::Error, format!("failed to dispatch connection: {e}"));
                }
            }
            Err(e) => {
                server_log!(state, GelfLevel::Error, format!("accept error: {e}"));
            }
        }
    }
}

/// Run the server in cluster mode with Raft consensus.
///
/// Activated when `--features cluster` is enabled and `OXIDB_NODE_ID` is set.
/// Parses all standard env vars (OXIDB_ADDR, OXIDB_DATA, etc.) plus Raft-specific
/// ones (OXIDB_NODE_ID, OXIDB_RAFT_ADDR, OXIDB_RAFT_PEERS), then starts an async
/// tokio runtime with the Raft node and async client listener.
#[cfg(feature = "cluster")]
fn run_cluster_mode() {
    let addr = env::var("OXIDB_ADDR").unwrap_or_else(|_| "127.0.0.1:4444".to_string());
    let data_dir = env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".to_string());
    let pool_size: usize = env::var("OXIDB_POOL_SIZE")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .expect("OXIDB_POOL_SIZE must be a valid usize");

    let idle_timeout_secs: u64 = env::var("OXIDB_IDLE_TIMEOUT")
        .unwrap_or_else(|_| "30".to_string())
        .parse()
        .expect("OXIDB_IDLE_TIMEOUT must be a valid u64 (seconds)");
    let idle_timeout = Duration::from_secs(idle_timeout_secs);

    let verbose = env::args().any(|a| a == "--verbose")
        || env::var("OXIDB_VERBOSE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

    let gelf: Option<Arc<GelfLogger>> = match env::var("OXIDB_GELF_ADDR") {
        Ok(gelf_addr) => {
            let logger = GelfLogger::new(&gelf_addr).expect("failed to create GELF logger");
            eprintln!("GELF logging: enabled ({gelf_addr})");
            let logger = Arc::new(logger);
            logger.send(
                GelfLevel::Informational,
                &format!("GELF logging: enabled ({gelf_addr})"),
                &[],
            );
            Some(logger)
        }
        Err(_) => None,
    };

    if verbose {
        eprintln!("verbose: enabled");
    }

    let encryption_key = match env::var("OXIDB_ENCRYPTION_KEY") {
        Ok(path) => {
            let key = oxidb::EncryptionKey::load_from_file(Path::new(&path))
                .expect("failed to load encryption key");
            eprintln!("encryption-at-rest: enabled");
            if let Some(g) = &gelf {
                g.send(GelfLevel::Informational, "encryption-at-rest: enabled", &[]);
            }
            Some(key)
        }
        Err(_) => None,
    };

    // Parse Raft config
    let raft_config =
        RaftConfig::from_env().expect("OXIDB_NODE_ID is set but Raft config is invalid");

    // Open database
    let open_start = std::time::Instant::now();
    let db = if let Some(ref g) = gelf {
        let gelf_cb = Arc::clone(g);
        OxiDb::open_with_log(
            Path::new(&data_dir),
            encryption_key,
            verbose,
            Arc::new(move |msg: &str| {
                gelf_cb.send(GelfLevel::Informational, msg, &[]);
            }),
        )
    } else {
        OxiDb::open_verbose(Path::new(&data_dir), encryption_key, verbose)
    }
    .expect("failed to open database");
    if verbose {
        eprintln!(
            "[verbose] database opened in {:.2}s",
            open_start.elapsed().as_secs_f64()
        );
    }
    let db = Arc::new(db);

    // Authentication
    let auth_enabled = env::var("OXIDB_AUTH")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let user_store = if auth_enabled {
        let store = UserStore::open(Path::new(&data_dir)).expect("failed to open user store");
        eprintln!("authentication: enabled");
        if let Some(g) = &gelf {
            g.send(GelfLevel::Informational, "authentication: enabled", &[]);
        }
        Some(Arc::new(Mutex::new(store)))
    } else {
        None
    };

    // Audit logging
    let audit_enabled = env::var("OXIDB_AUDIT")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let audit_log = if audit_enabled {
        let log = AuditLog::open(Path::new(&data_dir)).expect("failed to open audit log");
        eprintln!("audit logging: enabled");
        if let Some(g) = &gelf {
            g.send(GelfLevel::Informational, "audit logging: enabled", &[]);
        }
        Some(Arc::new(log))
    } else {
        None
    };

    // TLS
    let tls_acceptor: Option<tokio_rustls::TlsAcceptor> =
        match (env::var("OXIDB_TLS_CERT"), env::var("OXIDB_TLS_KEY")) {
            (Ok(cert), Ok(key)) => {
                let config = tls::load_tls_config(Path::new(&cert), Path::new(&key))
                    .expect("failed to load TLS config");
                eprintln!("TLS: enabled");
                if let Some(g) = &gelf {
                    g.send(GelfLevel::Informational, "TLS: enabled", &[]);
                }
                Some(tokio_rustls::TlsAcceptor::from(config))
            }
            _ => None,
        };

    // Build tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(pool_size)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    runtime.block_on(async move {
        // Create Raft node
        let node_id = raft_config.node_id;
        let raft_addr = raft_config.raft_addr.clone();
        let openraft_config = RaftConfig::openraft_config();

        let store = OxiDbStore::new(Arc::clone(&db));
        let (log_store, sm) = Adaptor::new(store);

        let raft = openraft::Raft::new(
            node_id,
            openraft_config,
            OxiDbNetworkFactory,
            log_store,
            sm,
        )
        .await
        .expect("failed to create Raft node");
        let raft = Arc::new(raft);

        // Build async server state
        let state = Arc::new(AsyncServerState {
            db,
            user_store,
            audit_log,
            auth_enabled,
            raft: Some(Arc::clone(&raft)),
        });

        // Spawn Raft RPC listener
        let raft_listener = tokio::net::TcpListener::bind(&raft_addr)
            .await
            .expect("failed to bind Raft RPC listener");
        eprintln!("raft RPC listening on {raft_addr} (node_id={node_id})");
        if let Some(g) = &gelf {
            g.send(
                GelfLevel::Informational,
                &format!("raft RPC listening on {raft_addr} (node_id={node_id})"),
                &[],
            );
        }

        let raft_for_rpc = Arc::clone(&raft);
        tokio::spawn(async move {
            loop {
                match raft_listener.accept().await {
                    Ok((stream, _)) => {
                        let raft_ref = Arc::clone(&raft_for_rpc);
                        tokio::spawn(async move {
                            network::handle_raft_rpc(stream, &raft_ref).await;
                        });
                    }
                    Err(e) => {
                        eprintln!("raft accept error: {e}");
                    }
                }
            }
        });

        // Spawn client listener
        let client_listener = tokio::net::TcpListener::bind(&addr)
            .await
            .expect("failed to bind client listener");

        eprintln!(
            "oxidb-server (cluster) listening on {addr} (node_id={node_id}, pool_size={pool_size}, data_dir={data_dir}, idle_timeout={idle_timeout_secs}s)"
        );
        if let Some(g) = &gelf {
            g.send(
                GelfLevel::Notice,
                &format!(
                    "oxidb-server (cluster) listening on {addr} (node_id={node_id}, pool_size={pool_size}, data_dir={data_dir}, idle_timeout={idle_timeout_secs}s)"
                ),
                &[],
            );
        }

        loop {
            match client_listener.accept().await {
                Ok((stream, _)) => {
                    let state = Arc::clone(&state);
                    let tls = tls_acceptor.clone();
                    tokio::spawn(async move {
                        if let Some(acceptor) = tls {
                            async_server::handle_tls_connection(
                                stream, state, acceptor, idle_timeout,
                            )
                            .await;
                        } else {
                            async_server::handle_connection(stream, state, idle_timeout).await;
                        }
                    });
                }
                Err(e) => {
                    eprintln!("accept error: {e}");
                }
            }
        }
    });
}
