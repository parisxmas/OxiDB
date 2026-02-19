//! Integration tests for server handler commands that are not covered by acid_test.rs.
//!
//! Tests: insert_many, update_one, delete_one, create_collection, compact,
//! create_index, create_unique_index, create_composite_index, create_text_index,
//! list_indexes, drop_index, text_search, aggregate, blob commands,
//! user management, auth/RBAC, and crash recovery.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};

use serde_json::{Value, json};
use tempfile::TempDir;

use oxidb::OxiDb;
use oxidb_server::protocol::{read_message, write_message};

// ---------------------------------------------------------------------------
// Test infrastructure (mirrors acid_test.rs)
// ---------------------------------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    _dir: Option<TempDir>,
    data_dir: std::path::PathBuf,
}

impl TestServer {
    fn start() -> Self {
        let dir = TempDir::new().expect("failed to create temp dir");
        let data_dir = dir.path().to_path_buf();
        let addr = Self::start_at_path(&data_dir);
        Self {
            addr,
            _dir: Some(dir),
            data_dir,
        }
    }

    fn start_at_path(data_dir: &Path) -> SocketAddr {
        let db = OxiDb::open(data_dir).expect("failed to open database");
        let db = Arc::new(db);

        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel::<TcpStream>();
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..4 {
            let rx = Arc::clone(&rx);
            let db = Arc::clone(&db);
            std::thread::spawn(move || loop {
                let stream = rx.lock().unwrap().recv();
                match stream {
                    Ok(stream) => handle_client(stream, &db),
                    Err(_) => break,
                }
            });
        }

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(s) => {
                        if tx.send(s).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        addr
    }

    fn reopen(&self) -> TestServer {
        let addr = Self::start_at_path(&self.data_dir);
        TestServer {
            addr,
            _dir: None,
            data_dir: self.data_dir.clone(),
        }
    }
}

fn handle_client(mut stream: TcpStream, db: &Arc<OxiDb>) {
    let mut active_tx: Option<u64> = None;

    loop {
        let msg = match read_message(&mut stream) {
            Ok(m) => m,
            Err(_) => break,
        };

        let request: Value = match serde_json::from_slice(&msg) {
            Ok(v) => v,
            Err(e) => {
                let resp = json!({"ok": false, "error": format!("invalid JSON: {e}")});
                let _ = write_message(&mut stream, resp.to_string().as_bytes());
                continue;
            }
        };

        let resp_bytes = oxidb_server::handler::handle_request(db, request, &mut active_tx);

        if write_message(&mut stream, &resp_bytes).is_err() {
            break;
        }
    }

    if let Some(tx_id) = active_tx {
        let _ = db.rollback_transaction(tx_id);
    }
}

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).expect("failed to connect");
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        Self { stream }
    }

    fn send(&mut self, request: &Value) -> Value {
        let bytes = request.to_string().into_bytes();
        write_message(&mut self.stream, &bytes).expect("send failed");
        let resp = read_message(&mut self.stream).expect("recv failed");
        serde_json::from_slice(&resp).expect("invalid JSON response")
    }
}

fn assert_ok(resp: &Value) {
    assert_eq!(resp["ok"], true, "expected ok response, got: {resp}");
}

fn assert_err(resp: &Value) {
    assert_eq!(resp["ok"], false, "expected error response, got: {resp}");
}

// ===========================================================================
// CRUD: insert_many
// ===========================================================================

#[test]
fn test_insert_many() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    let resp = c.send(&json!({
        "cmd": "insert_many",
        "collection": "items",
        "docs": [
            {"name": "a", "val": 1},
            {"name": "b", "val": 2},
            {"name": "c", "val": 3},
        ]
    }));
    assert_ok(&resp);

    // Verify count
    let resp = c.send(&json!({"cmd": "count", "collection": "items"}));
    assert_ok(&resp);
    assert_eq!(resp["data"]["count"], 3);
}

// ===========================================================================
// CRUD: update_one / delete_one
// ===========================================================================

#[test]
fn test_update_one() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Insert 3 docs with same status
    for i in 0..3 {
        c.send(&json!({
            "cmd": "insert", "collection": "docs",
            "doc": {"status": "pending", "idx": i}
        }));
    }

    // update_one should only modify 1
    let resp = c.send(&json!({
        "cmd": "update_one",
        "collection": "docs",
        "query": {"status": "pending"},
        "update": {"$set": {"status": "done"}}
    }));
    assert_ok(&resp);
    assert_eq!(resp["data"]["modified"], 1);

    // Verify: 2 still pending
    let resp = c.send(&json!({
        "cmd": "find", "collection": "docs",
        "query": {"status": "pending"}
    }));
    assert_ok(&resp);
    assert_eq!(resp["data"].as_array().unwrap().len(), 2);
}

#[test]
fn test_delete_one() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    for i in 0..3 {
        c.send(&json!({
            "cmd": "insert", "collection": "docs",
            "doc": {"tag": "remove", "idx": i}
        }));
    }

    let resp = c.send(&json!({
        "cmd": "delete_one",
        "collection": "docs",
        "query": {"tag": "remove"}
    }));
    assert_ok(&resp);
    assert_eq!(resp["data"]["deleted"], 1);

    let resp = c.send(&json!({"cmd": "count", "collection": "docs"}));
    assert_eq!(resp["data"]["count"], 2);
}

// ===========================================================================
// Collection management
// ===========================================================================

#[test]
fn test_create_collection() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    let resp = c.send(&json!({"cmd": "create_collection", "collection": "empty_col"}));
    assert_ok(&resp);

    let resp = c.send(&json!({"cmd": "list_collections"}));
    assert_ok(&resp);
    let cols = resp["data"].as_array().unwrap();
    assert!(cols.iter().any(|v| v == "empty_col"));
}

#[test]
fn test_drop_collection() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    c.send(&json!({"cmd": "insert", "collection": "temp", "doc": {"x": 1}}));
    let resp = c.send(&json!({"cmd": "drop_collection", "collection": "temp"}));
    assert_ok(&resp);

    let resp = c.send(&json!({"cmd": "list_collections"}));
    let cols = resp["data"].as_array().unwrap();
    assert!(!cols.iter().any(|v| v == "temp"));
}

// ===========================================================================
// Compact
// ===========================================================================

#[test]
fn test_compact() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Insert and delete to create garbage
    for i in 0..10 {
        c.send(&json!({
            "cmd": "insert", "collection": "garbage",
            "doc": {"idx": i}
        }));
    }
    c.send(&json!({
        "cmd": "delete", "collection": "garbage",
        "query": {"idx": {"$lt": 5}}
    }));

    let resp = c.send(&json!({"cmd": "compact", "collection": "garbage"}));
    assert_ok(&resp);
    assert!(resp["data"]["old_size"].as_u64().unwrap() > 0);
    assert!(resp["data"]["new_size"].as_u64().unwrap() > 0);
    assert_eq!(resp["data"]["docs_kept"], 5);

    // Verify data integrity after compaction
    let resp = c.send(&json!({"cmd": "count", "collection": "garbage"}));
    assert_eq!(resp["data"]["count"], 5);
}

// ===========================================================================
// Index management
// ===========================================================================

#[test]
fn test_create_and_list_indexes() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Insert some data first
    c.send(&json!({"cmd": "insert", "collection": "idx_test", "doc": {"name": "a", "age": 1}}));

    let resp = c.send(&json!({
        "cmd": "create_index", "collection": "idx_test", "field": "name"
    }));
    assert_ok(&resp);

    let resp = c.send(&json!({
        "cmd": "list_indexes", "collection": "idx_test"
    }));
    assert_ok(&resp);
    let indexes = resp["data"].as_array().unwrap();
    assert!(indexes.iter().any(|v| v.as_str() == Some("name") || v.to_string().contains("name")));
}

#[test]
fn test_create_unique_index() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    c.send(&json!({"cmd": "insert", "collection": "uniq", "doc": {"email": "a@b.c"}}));

    let resp = c.send(&json!({
        "cmd": "create_unique_index", "collection": "uniq", "field": "email"
    }));
    assert_ok(&resp);

    // Insert duplicate should fail
    let resp = c.send(&json!({
        "cmd": "insert", "collection": "uniq", "doc": {"email": "a@b.c"}
    }));
    assert_err(&resp);
}

#[test]
fn test_create_composite_index() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    c.send(&json!({"cmd": "insert", "collection": "comp", "doc": {"a": 1, "b": 2}}));

    let resp = c.send(&json!({
        "cmd": "create_composite_index", "collection": "comp", "fields": ["a", "b"]
    }));
    assert_ok(&resp);
    assert!(resp["data"]["index"].is_string());
}

#[test]
fn test_drop_index() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    c.send(&json!({"cmd": "insert", "collection": "drop_idx", "doc": {"x": 1}}));
    c.send(&json!({"cmd": "create_index", "collection": "drop_idx", "field": "x"}));

    let resp = c.send(&json!({
        "cmd": "drop_index", "collection": "drop_idx", "index": "x"
    }));
    assert_ok(&resp);
}

#[test]
fn test_create_text_index() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    c.send(&json!({"cmd": "insert", "collection": "text", "doc": {"title": "hello", "body": "world"}}));

    let resp = c.send(&json!({
        "cmd": "create_text_index", "collection": "text", "fields": ["title", "body"]
    }));
    assert_ok(&resp);
}

// ===========================================================================
// Text search
// ===========================================================================

#[test]
fn test_text_search() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Insert docs and create text index
    c.send(&json!({
        "cmd": "insert", "collection": "articles",
        "doc": {"title": "Rust programming", "body": "Systems language"}
    }));
    c.send(&json!({
        "cmd": "insert", "collection": "articles",
        "doc": {"title": "Python scripting", "body": "Dynamic language"}
    }));
    c.send(&json!({
        "cmd": "create_text_index", "collection": "articles", "fields": ["title", "body"]
    }));

    // Allow time for background FTS indexing
    std::thread::sleep(std::time::Duration::from_millis(500));

    let resp = c.send(&json!({
        "cmd": "text_search", "collection": "articles", "query": "Rust", "limit": 10
    }));
    assert_ok(&resp);
}

// ===========================================================================
// Aggregate
// ===========================================================================

#[test]
fn test_aggregate() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    c.send(&json!({"cmd": "insert", "collection": "sales", "doc": {"product": "A", "amount": 10}}));
    c.send(&json!({"cmd": "insert", "collection": "sales", "doc": {"product": "B", "amount": 20}}));
    c.send(&json!({"cmd": "insert", "collection": "sales", "doc": {"product": "A", "amount": 30}}));

    let resp = c.send(&json!({
        "cmd": "aggregate",
        "collection": "sales",
        "pipeline": [
            {"$group": {"_id": "$product", "total": {"$sum": "$amount"}}},
            {"$sort": {"_id": 1}}
        ]
    }));
    assert_ok(&resp);
    let data = resp["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
}

// ===========================================================================
// Blob storage
// ===========================================================================

#[test]
fn test_blob_crud() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Create bucket
    let resp = c.send(&json!({"cmd": "create_bucket", "bucket": "files"}));
    assert_ok(&resp);

    // List buckets
    let resp = c.send(&json!({"cmd": "list_buckets"}));
    assert_ok(&resp);
    let buckets = resp["data"].as_array().unwrap();
    assert!(buckets.iter().any(|v| v == "files"));

    // Put object (base64 of "hello world")
    let data_b64 = base64_encode(b"hello world");
    let resp = c.send(&json!({
        "cmd": "put_object",
        "bucket": "files",
        "key": "greeting.txt",
        "data": data_b64,
        "content_type": "text/plain",
        "metadata": {"author": "test"}
    }));
    assert_ok(&resp);

    // Get object
    let resp = c.send(&json!({
        "cmd": "get_object", "bucket": "files", "key": "greeting.txt"
    }));
    assert_ok(&resp);
    assert!(resp["data"]["content"].is_string());

    // Head object
    let resp = c.send(&json!({
        "cmd": "head_object", "bucket": "files", "key": "greeting.txt"
    }));
    assert_ok(&resp);

    // List objects
    let resp = c.send(&json!({
        "cmd": "list_objects", "bucket": "files"
    }));
    assert_ok(&resp);

    // Delete object
    let resp = c.send(&json!({
        "cmd": "delete_object", "bucket": "files", "key": "greeting.txt"
    }));
    assert_ok(&resp);

    // Delete bucket
    let resp = c.send(&json!({"cmd": "delete_bucket", "bucket": "files"}));
    assert_ok(&resp);
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

// ===========================================================================
// FTS (blob-level search)
// ===========================================================================

#[test]
fn test_search_command() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // search on empty DB should succeed
    let resp = c.send(&json!({
        "cmd": "search", "query": "test", "limit": 5
    }));
    assert_ok(&resp);
}

// ===========================================================================
// Error handling
// ===========================================================================

#[test]
fn test_unknown_command() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    let resp = c.send(&json!({"cmd": "nonexistent_cmd"}));
    assert_err(&resp);
    assert!(resp["error"].as_str().unwrap().contains("unknown command"));
}

#[test]
fn test_missing_cmd_field() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    let resp = c.send(&json!({"not_cmd": "insert"}));
    assert_err(&resp);
}

#[test]
fn test_missing_collection() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    let resp = c.send(&json!({"cmd": "insert", "doc": {"x": 1}}));
    assert_err(&resp);
}

#[test]
fn test_missing_doc() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    let resp = c.send(&json!({"cmd": "insert", "collection": "test"}));
    assert_err(&resp);
}

#[test]
fn test_find_with_sort_skip_limit() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    for i in 0..10 {
        c.send(&json!({
            "cmd": "insert", "collection": "sorted",
            "doc": {"idx": i}
        }));
    }

    let resp = c.send(&json!({
        "cmd": "find", "collection": "sorted",
        "query": {},
        "sort": {"idx": -1},
        "skip": 2,
        "limit": 3
    }));
    assert_ok(&resp);
    let data = resp["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
}

// ===========================================================================
// Crash recovery: commit survives restart
// ===========================================================================

#[test]
fn test_crash_recovery_committed_data() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Insert data outside transaction
    for i in 0..5 {
        let resp = c.send(&json!({
            "cmd": "insert", "collection": "persist",
            "doc": {"val": i}
        }));
        assert_ok(&resp);
    }

    // Insert via committed transaction
    c.send(&json!({"cmd": "begin_tx"}));
    c.send(&json!({
        "cmd": "insert", "collection": "persist",
        "doc": {"val": 100}
    }));
    let resp = c.send(&json!({"cmd": "commit_tx"}));
    assert_ok(&resp);

    // Disconnect
    drop(c);

    // Reopen
    let server2 = server.reopen();
    let mut c2 = Client::connect(server2.addr);

    let resp = c2.send(&json!({"cmd": "count", "collection": "persist"}));
    assert_ok(&resp);
    assert_eq!(resp["data"]["count"], 6);
}

#[test]
fn test_crash_recovery_uncommitted_discarded() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Insert baseline
    c.send(&json!({
        "cmd": "insert", "collection": "recover",
        "doc": {"committed": true}
    }));

    // Start transaction but don't commit
    c.send(&json!({"cmd": "begin_tx"}));
    c.send(&json!({
        "cmd": "insert", "collection": "recover",
        "doc": {"committed": false}
    }));

    // Drop connection (simulates crash)
    drop(c);

    // Reopen
    let server2 = server.reopen();
    let mut c2 = Client::connect(server2.addr);

    let resp = c2.send(&json!({"cmd": "count", "collection": "recover"}));
    assert_ok(&resp);
    // Only the committed insert should survive
    assert_eq!(resp["data"]["count"], 1);
}

// ===========================================================================
// User management (handler-level)
// ===========================================================================

#[test]
fn test_user_management_commands() {
    use oxidb_server::auth::UserStore;
    use oxidb_server::handler::handle_user_command;

    let dir = TempDir::new().unwrap();
    let user_store = UserStore::open(dir.path()).unwrap();
    let user_store = Arc::new(Mutex::new(user_store));

    // Create user
    let req = json!({"cmd": "create_user", "username": "alice", "password": "pass123", "role": "readWrite"});
    let resp_bytes = handle_user_command("create_user", &req, &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], true);

    // Duplicate user fails
    let resp_bytes = handle_user_command("create_user", &req, &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], false);

    // List users
    let resp_bytes = handle_user_command("list_users", &json!({}), &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], true);
    let users = resp["data"].as_array().unwrap();
    assert!(users.iter().any(|u| u["username"] == "alice"));

    // Update user role
    let req = json!({"cmd": "update_user", "username": "alice", "role": "admin"});
    let resp_bytes = handle_user_command("update_user", &req, &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], true);

    // Update user password
    let req = json!({"cmd": "update_user", "username": "alice", "password": "newpass"});
    let resp_bytes = handle_user_command("update_user", &req, &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], true);

    // Update with nothing fails
    let req = json!({"cmd": "update_user", "username": "alice"});
    let resp_bytes = handle_user_command("update_user", &req, &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], false);

    // Drop user
    let req = json!({"cmd": "drop_user", "username": "alice"});
    let resp_bytes = handle_user_command("drop_user", &req, &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], true);

    // Drop nonexistent user fails
    let resp_bytes = handle_user_command("drop_user", &req, &user_store).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], false);

    // Unknown command returns None
    assert!(handle_user_command("unknown", &json!({}), &user_store).is_none());
}

// ===========================================================================
// Auth: UserStore
// ===========================================================================

#[test]
fn test_user_store_authenticate() {
    use oxidb_server::auth::{Role, UserStore};

    let dir = TempDir::new().unwrap();
    let mut store = UserStore::open(dir.path()).unwrap();

    store.create_user("bob", "secret", Role::Read).unwrap();

    // Correct password
    let role = store.authenticate("bob", "secret");
    assert_eq!(role, Some(Role::Read));

    // Wrong password
    let role = store.authenticate("bob", "wrong");
    assert_eq!(role, None);

    // Nonexistent user
    let role = store.authenticate("nobody", "secret");
    assert_eq!(role, None);
}

#[test]
fn test_user_store_persistence() {
    use oxidb_server::auth::{Role, UserStore};

    let dir = TempDir::new().unwrap();

    {
        let mut store = UserStore::open(dir.path()).unwrap();
        store.create_user("persist_user", "pass", Role::ReadWrite).unwrap();
    }

    // Reopen
    let store = UserStore::open(dir.path()).unwrap();
    let role = store.authenticate("persist_user", "pass");
    assert_eq!(role, Some(Role::ReadWrite));
}

#[test]
fn test_user_store_update() {
    use oxidb_server::auth::{Role, UserStore};

    let dir = TempDir::new().unwrap();
    let mut store = UserStore::open(dir.path()).unwrap();

    store.create_user("updatable", "old", Role::Read).unwrap();

    // Update password
    store.update_user("updatable", Some("new"), None).unwrap();
    assert_eq!(store.authenticate("updatable", "new"), Some(Role::Read));
    assert_eq!(store.authenticate("updatable", "old"), None);

    // Update role
    store.update_user("updatable", None, Some(Role::Admin)).unwrap();
    assert_eq!(store.authenticate("updatable", "new"), Some(Role::Admin));

    // Update nonexistent
    assert!(store.update_user("ghost", Some("x"), None).is_err());
}

// ===========================================================================
// RBAC: permission checks
// ===========================================================================

#[test]
fn test_rbac_admin_all_permitted() {
    use oxidb_server::auth::Role;
    use oxidb_server::rbac::is_permitted;

    let cmds = [
        "ping", "insert", "find", "update", "delete", "count",
        "create_index", "create_user", "drop_user", "drop_collection",
    ];
    for cmd in cmds {
        assert!(is_permitted(Role::Admin, cmd), "Admin should be permitted: {cmd}");
    }
}

#[test]
fn test_rbac_readwrite_permissions() {
    use oxidb_server::auth::Role;
    use oxidb_server::rbac::is_permitted;

    // Allowed
    let allowed = [
        "ping", "insert", "insert_many", "find", "find_one", "update",
        "delete", "count", "create_index", "aggregate", "begin_tx",
        "commit_tx", "rollback_tx", "create_bucket", "put_object",
    ];
    for cmd in allowed {
        assert!(is_permitted(Role::ReadWrite, cmd), "ReadWrite should permit: {cmd}");
    }

    // Denied
    let denied = ["create_user", "drop_user", "update_user", "list_users", "drop_collection"];
    for cmd in denied {
        assert!(!is_permitted(Role::ReadWrite, cmd), "ReadWrite should deny: {cmd}");
    }
}

#[test]
fn test_rbac_read_permissions() {
    use oxidb_server::auth::Role;
    use oxidb_server::rbac::is_permitted;

    // Allowed
    let allowed = [
        "ping", "find", "find_one", "count", "aggregate",
        "list_collections", "list_buckets", "get_object", "head_object", "search",
    ];
    for cmd in allowed {
        assert!(is_permitted(Role::Read, cmd), "Read should permit: {cmd}");
    }

    // Denied
    let denied = [
        "insert", "update", "delete", "create_index",
        "create_user", "drop_collection", "put_object",
    ];
    for cmd in denied {
        assert!(!is_permitted(Role::Read, cmd), "Read should deny: {cmd}");
    }
}

// ===========================================================================
// Session
// ===========================================================================

#[test]
fn test_session_lifecycle() {
    use oxidb_server::auth::Role;
    use oxidb_server::session::Session;

    let mut session = Session::new();
    assert!(!session.is_authenticated());
    assert_eq!(session.role(), None);
    assert_eq!(session.username_str(), "anonymous");

    session.set_authenticated("admin".into(), Role::Admin);
    assert!(session.is_authenticated());
    assert_eq!(session.role(), Some(Role::Admin));
    assert_eq!(session.username_str(), "admin");
}

// ===========================================================================
// Audit
// ===========================================================================

#[test]
fn test_audit_log() {
    use oxidb_server::audit::{AuditEvent, AuditLog, now_rfc3339};

    let dir = TempDir::new().unwrap();
    let audit = AuditLog::open(dir.path()).unwrap();

    let event = AuditEvent {
        ts: now_rfc3339(),
        user: "test_user",
        cmd: "insert",
        collection: Some("test_col"),
        result: "ok",
        detail: "",
    };
    audit.log(&event);

    // Verify audit log file was created and written to
    let log_path = dir.path().join("_audit").join("audit.log");
    assert!(log_path.exists());
    let content = std::fs::read_to_string(&log_path).unwrap();
    assert!(content.contains("test_user"));
    assert!(content.contains("insert"));
}

#[test]
fn test_now_rfc3339_format() {
    use oxidb_server::audit::now_rfc3339;

    let ts = now_rfc3339();
    // Should be formatted like "2024-01-15T10:30:00Z"
    assert!(ts.ends_with('Z'));
    assert!(ts.contains('T'));
    assert_eq!(ts.len(), 20);
}

// ===========================================================================
// Role parsing
// ===========================================================================

#[test]
fn test_role_from_str() {
    use oxidb_server::auth::Role;

    assert_eq!(Role::from_str("admin"), Some(Role::Admin));
    assert_eq!(Role::from_str("readWrite"), Some(Role::ReadWrite));
    assert_eq!(Role::from_str("readwrite"), Some(Role::ReadWrite));
    assert_eq!(Role::from_str("read"), Some(Role::Read));
    assert_eq!(Role::from_str("invalid"), None);
}

#[test]
fn test_role_as_str() {
    use oxidb_server::auth::Role;

    assert_eq!(Role::Admin.as_str(), "admin");
    assert_eq!(Role::ReadWrite.as_str(), "readWrite");
    assert_eq!(Role::Read.as_str(), "read");
}
