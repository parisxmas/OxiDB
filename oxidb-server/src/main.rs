#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use oxidb_server::audit::{self, AuditEvent, AuditLog};
use oxidb_server::auth::UserStore;
use oxidb_server::handler;
use oxidb_server::protocol;
use oxidb_server::rbac;
use oxidb_server::scram::ScramState;
use oxidb_server::session::Session;
use oxidb_server::tls;

use std::env;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxidb::OxiDb;

fn configure_stream(stream: &TcpStream, idle_timeout: Duration) {
    let _ = stream.set_read_timeout(Some(idle_timeout));
    let _ = stream.set_nodelay(true);
}

/// Shared server state passed to each connection handler.
struct ServerState {
    db: Arc<OxiDb>,
    user_store: Option<Arc<Mutex<UserStore>>>,
    audit_log: Option<Arc<AuditLog>>,
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

/// Generic message loop for split reader/writer (plain TCP).
fn handle_connection<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    state: &ServerState,
    peer: &str,
) {
    let mut active_tx: Option<u64> = None;
    let mut session = Session::new();

    if !state.auth_enabled {
        session.set_authenticated("anonymous".to_string(), oxidb_server::auth::Role::Admin);
    }

    loop {
        let msg = match protocol::read_message(reader) {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    eprintln!("idle timeout for {peer}, disconnecting");
                } else if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    eprintln!("read error from {peer}: {e}");
                }
                break;
            }
        };

        let request: serde_json::Value = match serde_json::from_slice(&msg) {
            Ok(v) => v,
            Err(e) => {
                let resp =
                    serde_json::json!({"ok": false, "error": format!("invalid JSON: {e}")});
                let _ = protocol::write_message(writer, resp.to_string().as_bytes());
                continue;
            }
        };

        let resp_bytes = dispatch_request(&request, state, &mut session, &mut active_tx, peer);

        if let Err(e) = protocol::write_message(writer, &resp_bytes) {
            eprintln!("write error to {peer}: {e}");
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
                    eprintln!("idle timeout for {peer}, disconnecting");
                } else if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    eprintln!("read error from {peer}: {e}");
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

        let resp_bytes = dispatch_request(
            &request,
            state,
            &mut session,
            &mut active_tx,
            peer,
        );

        if let Err(e) = protocol::write_message(stream, &resp_bytes) {
            eprintln!("write error to {peer}: {e}");
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
    eprintln!("client connected: {peer}");

    if let Some(tls_cfg) = tls_config {
        // TLS connection
        let conn = match rustls::ServerConnection::new(Arc::clone(tls_cfg)) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("TLS handshake error for {peer}: {e}");
                return;
            }
        };
        let mut tls_stream = rustls::StreamOwned::new(conn, stream);
        // StreamOwned<ServerConnection, TcpStream> implements both Read + Write.
        // handle_connection_single takes a single &mut impl Read + Write.
        handle_connection_single(&mut tls_stream, state, &peer);
    } else {
        // Plain TCP connection: split into separate reader/writer for better buffering
        let mut reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        handle_connection(&mut reader, &mut writer, state, &peer);
    }

    eprintln!("client disconnected: {peer}");
}

fn main() {
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

    // Encryption at rest
    let encryption_key = match env::var("OXIDB_ENCRYPTION_KEY") {
        Ok(path) => {
            let key = oxidb::EncryptionKey::load_from_file(Path::new(&path))
                .expect("failed to load encryption key");
            eprintln!("encryption-at-rest: enabled");
            Some(key)
        }
        Err(_) => None,
    };

    let db = OxiDb::open_with_options(Path::new(&data_dir), encryption_key)
        .expect("failed to open database");
    let db = Arc::new(db);

    // TLS
    let tls_config = match (env::var("OXIDB_TLS_CERT"), env::var("OXIDB_TLS_KEY")) {
        (Ok(cert), Ok(key)) => {
            let config = tls::load_tls_config(Path::new(&cert), Path::new(&key))
                .expect("failed to load TLS config");
            eprintln!("TLS: enabled");
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
        Some(Arc::new(log))
    } else {
        None
    };

    let state = Arc::new(ServerState {
        db,
        user_store,
        audit_log,
        auth_enabled,
    });

    let listener = TcpListener::bind(&addr).expect("failed to bind TCP listener");
    eprintln!("oxidb-server listening on {addr} (pool_size={pool_size}, data_dir={data_dir}, idle_timeout={idle_timeout_secs}s)");

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
                    eprintln!("failed to dispatch connection: {e}");
                }
            }
            Err(e) => {
                eprintln!("accept error: {e}");
            }
        }
    }
}
