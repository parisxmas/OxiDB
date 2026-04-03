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
use std::io::{BufReader, BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use sha1::Digest;
use serde_json::{json, Value};

use oxidb::OxiDb;
use oxidb::change_stream::{WatchFilter, WatchHandle, SubscriberId};
use crate::jwt;

const POOL_SIZE: usize = 64;
const MAX_QUEUED: usize = 512;
const WS_MAGIC: &str = "258EAFA5-E914-47DA-95CA-5AB5DC11D685";

struct WsState {
    db: Arc<OxiDb>,
    active: AtomicUsize,
    jwt_secret: Option<String>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn start_ws_listener(addr: &str, db: Arc<OxiDb>, jwt_secret: Option<String>) -> std::thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).expect("failed to bind WebSocket listener");

    if jwt_secret.is_some() {
        eprintln!("[ws] JWT authentication enabled (send {{\"cmd\":\"auth\",\"token\":\"...\"}} first)");
    }

    let state = Arc::new(WsState {
        db,
        active: AtomicUsize::new(0),
        jwt_secret,
    });

    let (conn_tx, conn_rx) = std::sync::mpsc::sync_channel::<TcpStream>(MAX_QUEUED);
    let conn_rx = Arc::new(Mutex::new(conn_rx));

    for i in 0..POOL_SIZE {
        let rx = Arc::clone(&conn_rx);
        let state = Arc::clone(&state);
        std::thread::Builder::new()
            .name(format!("ws-worker-{i}"))
            .spawn(move || loop {
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

    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes()).is_ok()
}

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
    collection: Option<String>,
    query: Option<Value>,
}

fn handle_connection(mut stream: TcpStream, state: &WsState) {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));

    if !do_handshake(&mut stream) {
        return;
    }

    // JWT auth: if enabled, require {"cmd": "auth", "token": "..."} as first message
    let mut authenticated = state.jwt_secret.is_none(); // open if no secret
    if !authenticated {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
        match read_frame(&mut stream) {
            Some((0x01, payload)) => {
                if let Ok(text) = std::str::from_utf8(&payload) {
                    if let Ok(cmd) = serde_json::from_str::<Value>(text) {
                        if cmd["cmd"].as_str() == Some("auth") {
                            if let Some(token) = cmd["token"].as_str() {
                                if let Some(ref secret) = state.jwt_secret {
                                    match jwt::verify(token, secret) {
                                        Ok(_claims) => {
                                            authenticated = true;
                                            let _ = send_json(&mut stream, &json!({"ok": true, "data": "authenticated"}));
                                        }
                                        Err(e) => {
                                            let _ = send_json(&mut stream, &json!({"ok": false, "error": e}));
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        if !authenticated {
            let _ = send_json(&mut stream, &json!({"ok": false, "error": "auth required: send {\"cmd\":\"auth\",\"token\":\"...\"}"}));
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
                        let resp = handle_command(&cmd, state, &mut subscriptions);
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
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
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
                state.db.unwatch(sub.handle.id);
            }
        }
    }

    // Clean up all subscriptions
    for (_, sub) in subscriptions {
        state.db.unwatch(sub.handle.id);
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
) -> Value {
    let cmd_name = cmd["cmd"].as_str().unwrap_or("");

    match cmd_name {
        "ping" => json!({"ok": true, "data": "pong"}),

        "subscribe" => {
            let sub_id = cmd["id"].as_str().unwrap_or("default").to_string();
            let collection = cmd.get("collection").and_then(|v| v.as_str()).map(String::from);
            let query = cmd.get("query").cloned();

            let filter = match &collection {
                Some(col) => WatchFilter::Collection(col.clone()),
                None => WatchFilter::All,
            };

            match state.db.watch(filter, None) {
                Ok(handle) => {
                    let wh_id = handle.id;
                    subs.insert(sub_id.clone(), Subscription {
                        handle,
                        collection,
                        query,
                    });
                    json!({"ok": true, "data": {"subscribed": sub_id, "watch_id": wh_id}})
                }
                Err(e) => json!({"ok": false, "error": format!("{e}")}),
            }
        }

        "unsubscribe" => {
            let sub_id = cmd["id"].as_str().unwrap_or("default");
            if let Some(sub) = subs.remove(sub_id) {
                state.db.unwatch(sub.handle.id);
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
                match state.db.insert(col, doc.clone()) {
                    Ok(id) => json!({"ok": true, "data": {"id": id}}),
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                }
            } else if let Some(docs) = cmd.get("docs").and_then(|v| v.as_array()) {
                match state.db.insert_many(col, docs.clone()) {
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
            match state.db.find(col, query) {
                Ok(docs) => json!({"ok": true, "data": docs}),
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
            match state.db.find_one(col, query) {
                Ok(doc) => json!({"ok": true, "data": doc}),
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
            match state.db.update(col, query, update) {
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
            match state.db.delete(col, query) {
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
            match state.db.count(col, query) {
                Ok(n) => json!({"ok": true, "data": {"count": n}}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "sql" => {
            let query = match cmd["query"].as_str() {
                Some(q) => q,
                None => return json!({"ok": false, "error": "missing 'query'"}),
            };
            match oxidb::sql::execute_sql(&state.db, query) {
                Ok(result) => {
                    let data = match result {
                        oxidb::sql::SqlResult::Select(rows) => json!(rows),
                        oxidb::sql::SqlResult::Insert(ids) => json!({"inserted": ids}),
                        oxidb::sql::SqlResult::Update(n) => json!({"modified": n}),
                        oxidb::sql::SqlResult::Delete(n) => json!({"deleted": n}),
                        oxidb::sql::SqlResult::Ddl(msg) => json!({"result": msg}),
                        oxidb::sql::SqlResult::UseDatabase(name) => json!({"database": name}),
                        oxidb::sql::SqlResult::ShowDatabases(names) => json!({"databases": names}),
                    };
                    json!({"ok": true, "data": data})
                }
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        _ => json!({"ok": false, "error": format!("unknown command: {cmd_name}")}),
    }
}
