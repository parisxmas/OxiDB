#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use oxidb_server::audit::{self, AuditEvent, AuditLog};
use oxidb_server::auth::UserStore;
use oxidb_server::gelf::{GelfLevel, GelfLogger};
use oxidb_server::handler;
use oxidb_server::protocol;
use oxidb_server::rbac;
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

use oxidb::OxiDb;

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
            );
            let permitted = if is_user_cmd {
                role == oxidb_server::auth::Role::Admin
            } else {
                rbac::is_permitted(role, &cmd)
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
    // Standard command dispatch
    // ---------------------------------------------------------------
    let resp_bytes = handler::handle_request(&state.db, request.clone(), active_tx);

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

        let request: serde_json::Value = match serde_json::from_slice(&msg) {
            Ok(v) => v,
            Err(e) => {
                let resp =
                    serde_json::json!({"ok": false, "error": format!("invalid JSON: {e}")});
                let _ = protocol::write_message(&mut writer, resp.to_string().as_bytes());
                continue;
            }
        };

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

        let resp_bytes = dispatch_request(&request, state, &mut session, &mut active_tx, peer);

        if let Err(e) = protocol::write_message(&mut writer, &resp_bytes) {
            server_log!(state, GelfLevel::Error, format!("write error to {peer}: {e}"), extra: "peer" => peer);
            break;
        }
    }

    if let Some(tx_id) = active_tx {
        let _ = state.db.rollback_transaction(tx_id);
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

        let request: serde_json::Value = match serde_json::from_slice(&msg) {
            Ok(v) => v,
            Err(e) => {
                let resp =
                    serde_json::json!({"ok": false, "error": format!("invalid JSON: {e}")});
                let _ = protocol::write_message(stream, resp.to_string().as_bytes());
                continue;
            }
        };

        // Intercept watch/unwatch — not supported over TLS (can't split the stream)
        if request.get("cmd").and_then(|v| v.as_str()) == Some("watch") {
            let resp = handler::err_bytes("watch is not supported over TLS connections");
            let _ = protocol::write_message(stream, &resp);
            continue;
        }

        let resp_bytes = dispatch_request(
            &request,
            state,
            &mut session,
            &mut active_tx,
            peer,
        );

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

    // GELF UDP logging (e.g. OXIDB_GELF_ADDR=172.17.0.1:12201)
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
        user_store,
        audit_log,
        gelf,
        auth_enabled,
    });

    let listener = TcpListener::bind(&addr).expect("failed to bind TCP listener");
    server_log!(state, GelfLevel::Notice, format!("oxidb-server listening on {addr} (pool_size={pool_size}, data_dir={data_dir}, idle_timeout={idle_timeout_secs}s)"));

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
