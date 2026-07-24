//! WebSocket server for OxiDB — real-time subscriptions and document operations.
//!
//! Enabled via `OXIDB_WS_PORT`. Clients connect over WebSocket and send JSON
//! commands as text frames. Subscriptions push change events in real-time.
//!
//! # Protocol
//!
//! Client → Server (JSON text frames):
//! ```json
//! {"cmd": "subscribe", "id": "sub1", "collection": "users", "query": {"status": "online"}}
//! {"cmd": "unsubscribe", "id": "sub1"}
//! {"cmd": "insert", "collection": "users", "doc": {"name": "Alice"}}
//! {"cmd": "find", "collection": "users", "query": {}}
//! {"cmd": "update", "collection": "users", "query": {}, "update": {"$set": {"age": 31}}}
//! {"cmd": "delete", "collection": "users", "query": {}}
//! {"cmd": "sql", "query": "SELECT * FROM users"}
//! ```
//!
//! Server → Client:
//! ```json
//! {"ok": true, "data": ...}
//! {"event": "change", "subscription": "sub1", "op": "insert", "collection": "users", "doc": {...}}
//! ```

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use serde_json::{Value, json};
use sha1::Digest;

use crate::auth;
use crate::jwt;
use crate::rules::{self, AuthContext, Operation, ReadAccess};
use oxidb::OxiDb;
use oxidb::change_stream::{WatchFilter, WatchHandle};

const POOL_SIZE: usize = 64;
const MAX_QUEUED: usize = 512;
// RFC 6455 §1.3 handshake GUID. This was a scrambled constant until 0.39.12 —
// spec-compliant clients (browsers, `ws`, undici) verify Sec-WebSocket-Accept
// and refused the connection; only non-validating clients ever connected.
const WS_MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

struct WsState {
    db: Arc<OxiDb>,
    active: AtomicUsize,
    jwt_secret: Option<String>,
    /// Database registry (ADR-0012). A message's `"db"` field targets a
    /// named database; absent (or `None` here) = the default database.
    db_manager: Option<Arc<oxidb::DatabaseManager>>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn start_ws_listener(
    addr: &str,
    db: Arc<OxiDb>,
    jwt_secret: Option<String>,
) -> std::thread::JoinHandle<()> {
    start_ws_listener_with_manager(addr, db, jwt_secret, None)
}

/// [`start_ws_listener`] with a database registry, enabling per-message
/// `"db"` targeting (ADR-0012).
pub fn start_ws_listener_with_manager(
    addr: &str,
    db: Arc<OxiDb>,
    jwt_secret: Option<String>,
    db_manager: Option<Arc<oxidb::DatabaseManager>>,
) -> std::thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).expect("failed to bind WebSocket listener");

    if jwt_secret.is_some() {
        eprintln!(
            "[ws] JWT authentication enabled (send {{\"cmd\":\"auth\",\"token\":\"...\"}} first)"
        );
    }

    let state = Arc::new(WsState {
        db,
        active: AtomicUsize::new(0),
        jwt_secret,
        db_manager,
    });

    let (conn_tx, conn_rx) = std::sync::mpsc::sync_channel::<TcpStream>(MAX_QUEUED);
    let conn_rx = Arc::new(Mutex::new(conn_rx));

    for i in 0..POOL_SIZE {
        let rx = Arc::clone(&conn_rx);
        let state = Arc::clone(&state);
        std::thread::Builder::new()
            .name(format!("ws-worker-{i}"))
            .spawn(move || {
                loop {
                    let stream = match rx.lock().unwrap().recv() {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    state.active.fetch_add(1, Ordering::Relaxed);
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handle_connection(stream, &state);
                    }));
                    state.active.fetch_sub(1, Ordering::Relaxed);
                    if let Err(e) = result {
                        let msg = if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        eprintln!("[ws] handler panicked: {msg}");
                    }
                }
            })
            .expect("failed to spawn ws worker");
    }
    eprintln!("[ws] thread pool: {POOL_SIZE} workers, queue depth {MAX_QUEUED}");

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    if conn_tx.try_send(s).is_err() {
                        eprintln!("[ws] connection rejected: queue full");
                    }
                }
                Err(e) => eprintln!("[ws] accept error: {e}"),
            }
        }
    })
}

// ---------------------------------------------------------------------------
// WebSocket handshake
// ---------------------------------------------------------------------------

fn do_handshake(stream: &mut TcpStream) -> bool {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut headers = HashMap::new();

    // Read HTTP request line
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return false;
    }

    // Read headers
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    // Verify upgrade request
    let upgrade = headers.get("upgrade").map(|v| v.to_lowercase());
    if upgrade.as_deref() != Some("websocket") {
        let resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(resp.as_bytes());
        return false;
    }

    let ws_key = match headers.get("sec-websocket-key") {
        Some(k) => k.clone(),
        None => return false,
    };

    // Compute accept key: SHA-1(key + magic) → base64
    let mut hasher = sha1::Sha1::new();
    hasher.update(ws_key.as_bytes());
    hasher.update(WS_MAGIC.as_bytes());
    let hash = hasher.finalize();
    let accept = base64::engine::general_purpose::STANDARD.encode(hash);

    // Phase 2 (ADR-0003): negotiate Sec-WebSocket-Protocol. Server advertises
    // `oxidb.v1` as the 1.0 stable subprotocol. Clients that don't offer it
    // still connect (no subprotocol header in the response — backward compat).
    let chosen_subprotocol = headers.get("sec-websocket-protocol").and_then(|hdr| {
        hdr.split(',')
            .map(|s| s.trim())
            .find(|p| SUPPORTED_WS_SUBPROTOCOLS.contains(p))
            .map(|s| s.to_string())
    });

    let subprotocol_line = match &chosen_subprotocol {
        Some(p) => format!("Sec-WebSocket-Protocol: {p}\r\n"),
        None => String::new(),
    };

    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n{subprotocol_line}\r\n"
    );
    stream.write_all(response.as_bytes()).is_ok()
}

/// WebSocket subprotocol identifiers this server supports. `oxidb.v1` is the
/// 1.0 stable subprotocol per ADR-0003 Phase 2. Future major versions add
/// new identifiers here (e.g. `oxidb.v2`); the v1 identifier keeps working
/// for the lifetime of the v1 stable surface.
pub const SUPPORTED_WS_SUBPROTOCOLS: &[&str] = &["oxidb.v1"];

// ---------------------------------------------------------------------------
// WebSocket frame reading/writing
// ---------------------------------------------------------------------------

/// Read a single WebSocket frame. Returns (opcode, payload).
/// Returns None on connection close or error.
fn read_frame(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut header = [0u8; 2];
    read_exact(stream, &mut header)?;

    let opcode = header[0] & 0x0F;
    let masked = header[1] & 0x80 != 0;
    let mut payload_len = (header[1] & 0x7F) as u64;

    if payload_len == 126 {
        let mut buf = [0u8; 2];
        read_exact(stream, &mut buf)?;
        payload_len = u16::from_be_bytes(buf) as u64;
    } else if payload_len == 127 {
        let mut buf = [0u8; 8];
        read_exact(stream, &mut buf)?;
        payload_len = u64::from_be_bytes(buf);
    }

    if payload_len > 16 * 1024 * 1024 {
        return None; // reject frames > 16 MiB
    }

    let mask_key = if masked {
        let mut key = [0u8; 4];
        read_exact(stream, &mut key)?;
        Some(key)
    } else {
        None
    };

    let mut payload = vec![0u8; payload_len as usize];
    read_exact(stream, &mut payload)?;

    if let Some(key) = mask_key {
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte ^= key[i % 4];
        }
    }

    Some((opcode, payload))
}

fn read_exact(stream: &mut TcpStream, buf: &mut [u8]) -> Option<()> {
    let mut offset = 0;
    while offset < buf.len() {
        match stream.read(&mut buf[offset..]) {
            Ok(0) => return None,
            Ok(n) => offset += n,
            Err(_) => return None,
        }
    }
    Some(())
}

/// Write a WebSocket text frame.
fn write_text_frame(stream: &mut TcpStream, data: &[u8]) -> bool {
    write_frame(stream, 0x01, data)
}

/// Write a WebSocket close frame.
fn write_close_frame(stream: &mut TcpStream) -> bool {
    write_frame(stream, 0x08, &[])
}

fn write_frame(stream: &mut TcpStream, opcode: u8, data: &[u8]) -> bool {
    let mut frame = Vec::with_capacity(10 + data.len());
    frame.push(0x80 | opcode); // FIN + opcode

    if data.len() < 126 {
        frame.push(data.len() as u8);
    } else if data.len() <= 65535 {
        frame.push(126);
        frame.extend_from_slice(&(data.len() as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(data.len() as u64).to_be_bytes());
    }

    frame.extend_from_slice(data);
    stream.write_all(&frame).is_ok() && stream.flush().is_ok()
}

fn send_json(stream: &mut TcpStream, val: &Value) -> bool {
    let bytes = serde_json::to_vec(val).unwrap_or_default();
    write_text_frame(stream, &bytes)
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

struct Subscription {
    handle: WatchHandle,
    #[allow(dead_code)]
    collection: Option<String>,
    query: Option<Value>,
    /// Per-row read rule (RLS): an event's document must pass this expression
    /// for the subscriber's auth context, or the event is not delivered.
    /// Doc-less events (deletes) are dropped under a filter — delivering even
    /// the id would leak row existence to a caller who can't read the row.
    read_filter: Option<String>,
    /// The engine this watch was registered on (its database).
    db: Arc<OxiDb>,
}

/// Per-connection auth + database-pinning state.
///
/// A connection authenticated with an OxiBase **project key** (`auth` with a
/// `"db"` field, verified against the project's ES256 public key) is pinned to
/// that project's database: every later command runs there and may not target
/// another `db`. Global-secret connections behave as before.
struct ConnState {
    auth_ctx: AuthContext,
    enforced_role: Option<auth::Role>,
    /// `(name, engine)` — set when the connection is bound to one database.
    pinned: Option<(String, Arc<OxiDb>)>,
}

impl ConnState {
    fn open() -> Self {
        ConnState {
            auth_ctx: AuthContext::anonymous(),
            enforced_role: None,
            pinned: None,
        }
    }
}

/// Handle an `auth` command: a global-secret token, or — with `"db"` — an
/// OxiBase project key/user token verified with that project's ES256 public
/// key. Returns the new connection state, or an error message.
fn try_auth(cmd: &Value, state: &WsState) -> Result<ConnState, String> {
    let token = cmd["token"].as_str().ok_or("missing 'token'")?;

    if let Some(db_name) = cmd.get("db").and_then(|v| v.as_str()) {
        // Project auth (ADR-0020): resolve ref-or-slug, verify with the
        // project's public key alone, pin the connection to that database.
        let mgr = state
            .db_manager
            .as_ref()
            .ok_or("database targeting is not available")?;
        if !crate::tenant_auth::enabled() {
            return Err("project authentication is not available".to_string());
        }
        let db_ref = crate::tenant_auth::resolve_tenant(mgr, db_name)
            .unwrap_or_else(|| db_name.to_string());
        let pubkey = crate::tenant_auth::project_pubkey(mgr, &db_ref)
            .ok_or_else(|| format!("unknown project: {db_name}"))?;
        let claims = jwt::verify_es256(token, &pubkey).map_err(|e| e.to_string())?;
        let db = mgr.get_database(&db_ref).map_err(|e| e.to_string())?;
        Ok(ConnState {
            enforced_role: Some(auth::Role::from_str(&claims.role).unwrap_or(auth::Role::Read)),
            auth_ctx: AuthContext::from_claims(&claims.sub, &claims.role),
            pinned: Some((db_ref, db)),
        })
    } else {
        let secret = state
            .jwt_secret
            .as_ref()
            .ok_or("auth is not enabled on this server")?;
        let claims = jwt::verify(token, secret).map_err(|e| e.to_string())?;
        Ok(ConnState {
            enforced_role: Some(auth::Role::from_str(&claims.role).unwrap_or(auth::Role::Read)),
            auth_ctx: AuthContext::from_claims(&claims.sub, &claims.role),
            pinned: None,
        })
    }
}

fn handle_connection(mut stream: TcpStream, state: &WsState) {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));

    if !do_handshake(&mut stream) {
        return;
    }

    // JWT auth: if enabled, require {"cmd": "auth", "token": "...", ["db": "..."]}
    // as the first message. When no global secret is set the connection starts
    // open, but an `auth` command is still accepted at any time (OxiBase
    // project keys authenticate per-database, independent of the global secret).
    let mut conn = ConnState::open();
    if state.jwt_secret.is_some() {
        let mut authenticated = false;
        let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
        if let Some((0x01, payload)) = read_frame(&mut stream)
            && let Ok(text) = std::str::from_utf8(&payload)
            && let Ok(cmd) = serde_json::from_str::<Value>(text)
            && cmd["cmd"].as_str() == Some("auth")
        {
            match try_auth(&cmd, state) {
                Ok(c) => {
                    conn = c;
                    authenticated = true;
                    let _ = send_json(&mut stream, &json!({"ok": true, "data": "authenticated"}));
                }
                Err(e) => {
                    let _ = send_json(&mut stream, &json!({"ok": false, "error": e}));
                    return;
                }
            }
        }
        if !authenticated {
            let _ = send_json(
                &mut stream,
                &json!({"ok": false, "error": "auth required: send {\"cmd\":\"auth\",\"token\":\"...\"}"}),
            );
            return;
        }
        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
    }

    let mut subscriptions: HashMap<String, Subscription> = HashMap::new();

    loop {
        // Check for incoming frames (non-blocking due to short read timeout)
        match read_frame(&mut stream) {
            Some((0x01, payload)) => {
                // Text frame — parse JSON command
                if let Ok(text) = std::str::from_utf8(&payload) {
                    if let Ok(cmd) = serde_json::from_str::<Value>(text) {
                        let resp = handle_command(&cmd, state, &mut subscriptions, &mut conn);
                        if !send_json(&mut stream, &resp) {
                            break;
                        }
                    }
                }
            }
            Some((0x08, _)) => {
                // Close frame
                let _ = write_close_frame(&mut stream);
                break;
            }
            Some((0x09, payload)) => {
                // Ping → Pong
                let _ = write_frame(&mut stream, 0x0A, &payload);
            }
            Some(_) => {} // ignore other opcodes
            None => {
                // read_timeout or connection closed — check if socket is dead
                // by attempting a zero-length peek
                let mut peek = [0u8; 1];
                match stream.peek(&mut peek) {
                    Err(ref e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(_) => break, // connection closed
                    Ok(0) => break,  // EOF
                    Ok(_) => {}      // data available, will read next iteration
                }
            }
        }

        // Push subscription events to client
        let mut dead_subs = Vec::new();
        for (sub_id, sub) in subscriptions.iter() {
            while let Ok(event) = sub.handle.rx.try_recv() {
                // Per-row read rule (RLS): only doc-bearing events the caller
                // may read are delivered; doc-less events are dropped entirely.
                if let Some(rule) = &sub.read_filter {
                    match &event.document {
                        Some(doc) => {
                            if !rules::row_visible(rule, &conn.auth_ctx, doc) {
                                continue;
                            }
                        }
                        None => continue,
                    }
                }
                // If subscription has a query filter, check if the doc matches
                if let Some(ref query) = sub.query {
                    if !query.as_object().map_or(true, |q| q.is_empty()) {
                        // Only filter insert events that have a document
                        if let Some(ref doc) = event.document {
                            if !doc_matches_query(doc, query) {
                                continue;
                            }
                        }
                    }
                }

                let event_json = json!({
                    "event": "change",
                    "subscription": sub_id,
                    "op": format!("{:?}", event.operation).to_lowercase(),
                    "collection": event.collection,
                    "doc_id": event.doc_id,
                    "doc": event.document,
                    "token": event.token,
                });
                if !send_json(&mut stream, &event_json) {
                    dead_subs.push(sub_id.clone());
                    break;
                }
            }
        }
        for id in dead_subs {
            if let Some(sub) = subscriptions.remove(&id) {
                sub.db.unwatch(sub.handle.id);
            }
        }
    }

    // Clean up all subscriptions
    for (_, sub) in subscriptions {
        sub.db.unwatch(sub.handle.id);
    }
}

/// Simple top-level field matching for subscription query filters.
fn doc_matches_query(doc: &Value, query: &Value) -> bool {
    let q = match query.as_object() {
        Some(o) => o,
        None => return true,
    };
    let d = match doc.as_object() {
        Some(o) => o,
        None => return false,
    };
    for (key, expected) in q {
        match d.get(key) {
            Some(actual) if actual == expected => {}
            _ => return false,
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Command handler
// ---------------------------------------------------------------------------

fn handle_command(
    cmd: &Value,
    state: &WsState,
    subs: &mut HashMap<String, Subscription>,
    conn: &mut ConnState,
) -> Value {
    let cmd_name = cmd["cmd"].as_str().unwrap_or("");

    // (Re-)authentication at any time — this is how OxiBase project keys and
    // end-user tokens authenticate on servers without a global secret.
    if cmd_name == "auth" {
        return match try_auth(cmd, state) {
            Ok(c) => {
                if !subs.is_empty() {
                    // Existing subscriptions were authorized under the old
                    // identity; drop them rather than leak under the new one.
                    for (_, sub) in subs.drain() {
                        sub.db.unwatch(sub.handle.id);
                    }
                }
                *conn = c;
                json!({"ok": true, "data": "authenticated"})
            }
            Err(e) => json!({"ok": false, "error": e}),
        };
    }

    // Authorization gate: when auth is enabled, mutating commands require a
    // ReadWrite (or Admin) role. Read-only commands are allowed for any role.
    if let Some(role) = conn.enforced_role {
        let is_mutating = matches!(cmd_name, "insert" | "update" | "delete");
        if is_mutating && role == auth::Role::Read {
            return json!({"ok": false, "error": "insufficient privileges: write operations require readWrite or admin"});
        }
    }

    // ── Database targeting (ADR-0012): a message's `"db"` field selects
    // the database; absent = the default database (backward compatible).
    // A project-authenticated connection is pinned: commands run on the
    // project's database and may not target any other.
    let db: Arc<OxiDb> = match (&conn.pinned, cmd.get("db").and_then(|v| v.as_str())) {
        (Some((_, db)), None) => Arc::clone(db),
        (Some((name, db)), Some(requested)) => {
            if requested == name {
                Arc::clone(db)
            } else {
                return json!({"ok": false, "error": format!("connection is bound to database {name:?}")});
            }
        }
        (None, None) => Arc::clone(&state.db),
        (None, Some(name)) => match &state.db_manager {
            None => {
                return json!({"ok": false, "error": "database targeting is not available"});
            }
            Some(mgr) => match mgr.get_database(name) {
                Ok(db) => db,
                Err(e) => return json!({"ok": false, "error": e.to_string()}),
            },
        },
    };
    let auth_ctx = &conn.auth_ctx;

    match cmd_name {
        "ping" => json!({"ok": true, "data": "pong"}),

        "subscribe" => {
            let sub_id = cmd["id"].as_str().unwrap_or("default").to_string();
            let collection = cmd
                .get("collection")
                .and_then(|v| v.as_str())
                .map(String::from);
            let query = cmd.get("query").cloned();

            // Security rules (RLS): resolve the caller's read access for the
            // collection once, at subscribe time. A non-admin token may not
            // open an all-collections firehose — per-collection rules could
            // not be applied to it.
            let read_filter = match &collection {
                Some(col) => match rules::read_access(&db, col, auth_ctx) {
                    ReadAccess::All => None,
                    ReadAccess::Filter(expr) => Some(expr),
                    ReadAccess::None => {
                        return json!({"ok": false, "error": format!("access denied: read on '{col}'")});
                    }
                },
                None => {
                    let privileged = matches!(
                        conn.enforced_role,
                        None | Some(auth::Role::Admin) | Some(auth::Role::ReadWrite)
                    );
                    if !privileged {
                        return json!({"ok": false, "error": "subscribing without a 'collection' requires readWrite or admin"});
                    }
                    None
                }
            };

            let filter = match &collection {
                Some(col) => WatchFilter::Collection(col.clone()),
                None => WatchFilter::All,
            };

            match db.watch(filter, None) {
                Ok(handle) => {
                    let wh_id = handle.id;
                    subs.insert(
                        sub_id.clone(),
                        Subscription {
                            handle,
                            collection,
                            query,
                            read_filter,
                            db: Arc::clone(&db),
                        },
                    );
                    json!({"ok": true, "data": {"subscribed": sub_id, "watch_id": wh_id}})
                }
                Err(e) => json!({"ok": false, "error": format!("{e}")}),
            }
        }

        "unsubscribe" => {
            let sub_id = cmd["id"].as_str().unwrap_or("default");
            if let Some(sub) = subs.remove(sub_id) {
                sub.db.unwatch(sub.handle.id);
                json!({"ok": true, "data": "unsubscribed"})
            } else {
                json!({"ok": false, "error": "subscription not found"})
            }
        }

        "insert" => {
            let col = match cmd["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection'"}),
            };
            if let Some(doc) = cmd.get("doc") {
                if let Err(e) =
                    rules::check_access(&db, col, Operation::Create, auth_ctx, None, Some(doc))
                {
                    return json!({"ok": false, "error": e});
                }
                match db.insert(col, doc.clone()) {
                    Ok(id) => json!({"ok": true, "data": {"id": id}}),
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                }
            } else if let Some(docs) = cmd.get("docs").and_then(|v| v.as_array()) {
                if let Err(e) = rules::check_access(
                    &db,
                    col,
                    Operation::Create,
                    auth_ctx,
                    None,
                    docs.first(),
                ) {
                    return json!({"ok": false, "error": e});
                }
                match db.insert_many(col, docs.clone()) {
                    Ok(ids) => json!({"ok": true, "data": {"ids": ids}}),
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                }
            } else {
                json!({"ok": false, "error": "missing 'doc' or 'docs'"})
            }
        }

        "find" => {
            let col = match cmd["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection'"}),
            };
            let empty = json!({});
            let query = cmd.get("query").unwrap_or(&empty);
            match db.find(col, query) {
                Ok(docs) => match rules::read_access(&db, col, auth_ctx) {
                    ReadAccess::All => json!({"ok": true, "data": docs}),
                    ReadAccess::None => {
                        json!({"ok": false, "error": format!("access denied: read on '{col}'")})
                    }
                    ReadAccess::Filter(expr) => {
                        let visible: Vec<Value> = docs
                            .into_iter()
                            .filter(|d| rules::row_visible(&expr, auth_ctx, d))
                            .collect();
                        json!({"ok": true, "data": visible})
                    }
                },
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "find_one" => {
            let col = match cmd["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection'"}),
            };
            let empty = json!({});
            let query = cmd.get("query").unwrap_or(&empty);
            match db.find_one(col, query) {
                Ok(doc) => match rules::read_access(&db, col, auth_ctx) {
                    ReadAccess::All => json!({"ok": true, "data": doc}),
                    ReadAccess::None => {
                        json!({"ok": false, "error": format!("access denied: read on '{col}'")})
                    }
                    ReadAccess::Filter(expr) => {
                        let visible =
                            doc.filter(|d| rules::row_visible(&expr, auth_ctx, d));
                        json!({"ok": true, "data": visible})
                    }
                },
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "update" => {
            let col = match cmd["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection'"}),
            };
            let query = match cmd.get("query") {
                Some(q) => q,
                None => return json!({"ok": false, "error": "missing 'query'"}),
            };
            let update = match cmd.get("update") {
                Some(u) => u,
                None => return json!({"ok": false, "error": "missing 'update'"}),
            };
            // Per-row rule check over the match set (REST parity).
            if let Err(e) = check_write_rules(&db, col, Operation::Update, auth_ctx, query) {
                return json!({"ok": false, "error": e});
            }
            match db.update(col, query, update) {
                Ok(n) => json!({"ok": true, "data": {"modified": n}}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "delete" => {
            let col = match cmd["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection'"}),
            };
            let query = match cmd.get("query") {
                Some(q) => q,
                None => return json!({"ok": false, "error": "missing 'query'"}),
            };
            if let Err(e) = check_write_rules(&db, col, Operation::Delete, auth_ctx, query) {
                return json!({"ok": false, "error": e});
            }
            match db.delete(col, query) {
                Ok(n) => json!({"ok": true, "data": {"deleted": n}}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "count" => {
            let col = match cmd["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection'"}),
            };
            let empty = json!({});
            let query = cmd.get("query").unwrap_or(&empty);
            match rules::read_access(&db, col, auth_ctx) {
                ReadAccess::None => {
                    json!({"ok": false, "error": format!("access denied: read on '{col}'")})
                }
                ReadAccess::All => match db.count(col, query) {
                    Ok(n) => json!({"ok": true, "data": {"count": n}}),
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                },
                // A per-row rule means the caller's count is the count of rows
                // it can see, not the collection's.
                ReadAccess::Filter(expr) => match db.find(col, query) {
                    Ok(docs) => {
                        let n = docs
                            .iter()
                            .filter(|d| rules::row_visible(&expr, auth_ctx, d))
                            .count();
                        json!({"ok": true, "data": {"count": n}})
                    }
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                },
            }
        }

        // (`sql` cmd removed alongside the SQL surface.)
        _ => json!({"ok": false, "error": format!("unknown command: {cmd_name}")}),
    }
}

/// Enforce a write-class rule (`update`/`delete`) over every document the
/// query matches — the same per-row semantics the REST surface applies. The
/// first denied row rejects the whole command.
fn check_write_rules(
    db: &OxiDb,
    col: &str,
    op: Operation,
    auth_ctx: &AuthContext,
    query: &Value,
) -> Result<(), String> {
    let docs = db.find(col, query).map_err(|e| e.to_string())?;
    // Zero matches = zero writes; nothing to authorize (REST parity).
    for doc in &docs {
        rules::check_access(db, col, op, auth_ctx, Some(doc), None)?;
    }
    Ok(())
}
