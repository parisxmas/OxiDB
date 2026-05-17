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

// ===========================================================================
// Linked collections (FDW v1)
// ===========================================================================

/// link_collection / unlink_collection / list_links roundtrip on one
/// instance — the registry surface works independently of any actual
/// proxy traffic.
#[test]
fn test_link_collection_registry_roundtrip() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    // Register a link.
    let resp = c.send(&json!({
        "cmd": "link_collection",
        "collection": "remote_users",
        "url": "oxidb://central.example.com:4444/users",
    }));
    assert_ok(&resp);
    assert_eq!(resp["data"]["name"], "remote_users");
    assert_eq!(resp["data"]["url"], "oxidb://central.example.com:4444/users");

    // list_links sees it.
    let resp = c.send(&json!({"cmd": "list_links"}));
    assert_ok(&resp);
    let list = resp["data"].as_array().expect("list_links data is an array");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["name"], "remote_users");

    // Bad URL is rejected at registration time, not on query.
    let resp = c.send(&json!({
        "cmd": "link_collection",
        "collection": "bogus",
        "url": "not-a-url",
    }));
    assert_eq!(resp["ok"], false);
    assert!(resp["error"].as_str().unwrap().contains("oxidb://"));

    // Unlink.
    let resp = c.send(&json!({"cmd": "unlink_collection", "collection": "remote_users"}));
    assert_ok(&resp);
    assert_eq!(resp["data"]["unlinked"], "remote_users");
    let resp = c.send(&json!({"cmd": "list_links"}));
    assert_eq!(resp["data"].as_array().unwrap().len(), 0);
}

/// Two OxiDB servers on different ports: instance A links a remote
/// collection on instance B. A `find` against the link on A actually
/// hits B and returns B's documents. Read commands proxied, writes
/// refused.
#[test]
fn test_linked_collection_proxies_reads() {
    // B is the "remote" — seed it with two docs in a `users` collection.
    let remote = TestServer::start();
    let mut bclient = Client::connect(remote.addr);
    bclient.send(&json!({"cmd": "insert", "collection": "users",
        "doc": {"name": "Alice", "age": 30}}));
    bclient.send(&json!({"cmd": "insert", "collection": "users",
        "doc": {"name": "Bob", "age": 25}}));

    // A is the "local" — register a link `remote_users` → B/users.
    let local = TestServer::start();
    let mut aclient = Client::connect(local.addr);
    let url = format!("oxidb://127.0.0.1:{}/users", remote.addr.port());
    let resp = aclient.send(&json!({
        "cmd": "link_collection",
        "collection": "remote_users",
        "url": url,
    }));
    assert_ok(&resp);

    // A `find` against the LOCAL link name returns B's docs.
    let resp = aclient.send(&json!({"cmd": "find", "collection": "remote_users", "query": {}}));
    assert_ok(&resp);
    let docs = resp["data"].as_array().expect("find data is an array");
    assert_eq!(docs.len(), 2);

    // Filtered find — predicate pushed through.
    let resp = aclient.send(&json!({
        "cmd": "find",
        "collection": "remote_users",
        "query": {"age": {"$gt": 27}},
    }));
    assert_ok(&resp);
    let docs = resp["data"].as_array().unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "Alice");

    // count works too — the count endpoint returns {"count": N}.
    let resp = aclient.send(&json!({"cmd": "count", "collection": "remote_users", "query": {}}));
    assert_ok(&resp);
    assert_eq!(resp["data"]["count"], 2);

    // v2c: a write through the link IS now proxied — the remote
    // gets a new doc. The doc shows up both via the link's count and
    // via the remote's own client, proving it was the remote that
    // actually wrote (not a local-only ghost).
    let resp = aclient.send(&json!({
        "cmd": "insert",
        "collection": "remote_users",
        "doc": {"name": "Carol", "age": 22},
    }));
    assert_ok(&resp);
    let resp = aclient.send(&json!({"cmd": "count", "collection": "remote_users", "query": {}}));
    assert_eq!(resp["data"]["count"], 3, "remote count incremented via the link");
    let resp = bclient.send(&json!({"cmd": "find", "collection": "users",
        "query": {"name": "Carol"}}));
    assert_eq!(resp["data"].as_array().unwrap().len(), 1,
        "Carol exists on the remote when read directly — proves the proxy WROTE");

    // Non-CRUD commands (schema / transactional / admin) MUST still
    // refuse through a link — those would either mutate remote schema
    // silently or break pool reuse. The refusal message must mention
    // "CRUD" so callers can distinguish from a transient remote error.
    let resp = aclient.send(&json!({
        "cmd": "create_index",
        "collection": "remote_users",
        "field": "name",
    }));
    assert_eq!(resp["ok"], false);
    assert!(
        resp["error"].as_str().unwrap().contains("only CRUD"),
        "schema-cmd refusal should say 'only CRUD': {:?}",
        resp["error"]
    );
}

/// A find against a missing remote (port not listening) returns an
/// error, not a panic — and the error names both the link and the
/// underlying transport reason.
#[test]
fn test_linked_collection_unreachable_remote_errors_cleanly() {
    let local = TestServer::start();
    let mut c = Client::connect(local.addr);

    // Link to a deliberately-closed port.
    c.send(&json!({
        "cmd": "link_collection",
        "collection": "unreachable",
        "url": "oxidb://127.0.0.1:1/dummy", // port 1 — reserved, nothing listens
    }));
    let resp = c.send(&json!({"cmd": "find", "collection": "unreachable", "query": {}}));
    assert_eq!(resp["ok"], false);
    let err = resp["error"].as_str().unwrap();
    assert!(err.contains("unreachable"), "error mentions link name: {}", err);
    assert!(
        err.contains("connect") || err.contains("refused") || err.contains("127.0.0.1:1"),
        "error mentions transport: {}",
        err
    );
}

// ===========================================================================
// Linked collections — write proxy (FDW v2c)
// ===========================================================================

/// Full CRUD round-trip through a link: insert_many, update_one,
/// delete_one. Every mutation goes through the link from instance A,
/// every result is verified by reading instance B directly — so a
/// passing test means the proxy actually wrote to the remote (not
/// to a local shadow or to nowhere at all).
#[test]
fn test_linked_collection_write_proxy_full_crud() {
    let remote = TestServer::start();
    let mut bclient = Client::connect(remote.addr);

    let local = TestServer::start();
    let mut aclient = Client::connect(local.addr);
    let url = format!("oxidb://127.0.0.1:{}/items", remote.addr.port());
    assert_ok(&aclient.send(&json!({
        "cmd": "link_collection",
        "collection": "remote_items",
        "url": url,
    })));

    // insert_many through the link → remote should have all 3.
    assert_ok(&aclient.send(&json!({
        "cmd": "insert_many",
        "collection": "remote_items",
        "docs": [
            {"sku": "A", "qty": 1},
            {"sku": "B", "qty": 2},
            {"sku": "C", "qty": 3},
        ],
    })));
    let resp = bclient.send(&json!({"cmd": "count", "collection": "items", "query": {}}));
    assert_eq!(resp["data"]["count"], 3, "insert_many landed on the remote");

    // update_one through the link → remote's B doc has qty=20.
    let resp = aclient.send(&json!({
        "cmd": "update_one",
        "collection": "remote_items",
        "query": {"sku": "B"},
        "update": {"$set": {"qty": 20}},
    }));
    assert_ok(&resp);
    let resp = bclient.send(&json!({"cmd": "find_one", "collection": "items",
        "query": {"sku": "B"}}));
    assert_ok(&resp);
    assert_eq!(resp["data"]["qty"], 20, "update reflected on the remote");

    // delete_one through the link → remote loses C; count drops to 2.
    assert_ok(&aclient.send(&json!({
        "cmd": "delete_one",
        "collection": "remote_items",
        "query": {"sku": "C"},
    })));
    let resp = bclient.send(&json!({"cmd": "count", "collection": "items", "query": {}}));
    assert_eq!(resp["data"]["count"], 2, "delete reflected on the remote");

    // Sanity: a find through the link sees the post-mutation state
    // (so the linked-collection read path agrees with the direct
    // read on B). Two docs left: A (qty=1), B (qty=20).
    let resp = aclient.send(&json!({"cmd": "find", "collection": "remote_items", "query": {}}));
    assert_ok(&resp);
    let docs = resp["data"].as_array().unwrap();
    assert_eq!(docs.len(), 2);
    let qtys: Vec<i64> = docs.iter().map(|d| d["qty"].as_i64().unwrap()).collect();
    assert!(qtys.contains(&1) && qtys.contains(&20),
        "post-mutation state visible through the link: {:?}", qtys);
}

/// Write proxy reuses the same pool as the read proxy — interleaved
/// reads + writes against one link don't accumulate idle conns above
/// 1. This is the v2c invariant the v2a pool gave us; this test
/// pins it so a future refactor that accidentally side-pools writes
/// trips immediately.
#[test]
fn test_linked_collection_write_proxy_shares_pool() {
    use oxidb_server::remote_client;

    let remote = TestServer::start();
    let local = TestServer::start();
    let mut aclient = Client::connect(local.addr);
    let url = format!("oxidb://127.0.0.1:{}/things", remote.addr.port());
    assert_ok(&aclient.send(&json!({
        "cmd": "link_collection",
        "collection": "remote_things",
        "url": url,
    })));

    // Interleave: insert, find, update, find, delete, find. Each
    // call returns the pool conn before the next takes it; idle
    // count should stay at 1.
    for i in 0..3 {
        assert_ok(&aclient.send(&json!({
            "cmd": "insert", "collection": "remote_things",
            "doc": {"i": i},
        })));
        assert_ok(&aclient.send(&json!({
            "cmd": "find", "collection": "remote_things", "query": {},
        })));
    }
    assert_ok(&aclient.send(&json!({
        "cmd": "update_one",
        "collection": "remote_things",
        "query": {"i": 0},
        "update": {"$set": {"i": 99}},
    })));
    assert_ok(&aclient.send(&json!({
        "cmd": "delete_one",
        "collection": "remote_things",
        "query": {"i": 99},
    })));

    let port = remote.addr.port();
    assert_eq!(
        remote_client::pool().idle_count("127.0.0.1", port, None),
        1,
        "mixed reads + writes through one link reuse the same pooled conn"
    );
}

// ===========================================================================
// Linked collections — CSV adapter (FDW v3a)
// ===========================================================================

/// End-to-end CSV adapter test: link a local CSV file via `csv://`,
/// then exercise the full CRUD cycle through the public wire — same
/// surface as the OxiDB-to-OxiDB tests above. Proves the dispatcher
/// in `fdw::adapter_for` actually wires `handle_linked_command` to
/// `CsvAdapter` based on URL scheme.
#[test]
fn test_linked_collection_csv_adapter_full_crud() {
    let dir = TempDir::new().unwrap();
    let csv_path = dir.path().join("people.csv");
    std::fs::write(&csv_path, "name,age\nalice,30\nbob,25\n").unwrap();

    let local = TestServer::start();
    let mut c = Client::connect(local.addr);

    // Link `people` → csv://<path>. Note the URL is a local-file
    // path; nothing crosses the network here.
    let url = format!("csv://{}", csv_path.to_str().unwrap());
    assert_ok(&c.send(&json!({
        "cmd": "link_collection",
        "collection": "people",
        "url": url,
    })));

    // find: empty query returns both seed rows.
    let resp = c.send(&json!({"cmd": "find", "collection": "people", "query": {}}));
    assert_ok(&resp);
    let rows = resp["data"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], "alice");

    // count agrees.
    let resp = c.send(&json!({"cmd": "count", "collection": "people", "query": {}}));
    assert_eq!(resp["data"]["count"], 2);

    // find with predicate.
    let resp = c.send(&json!({"cmd": "find", "collection": "people",
        "query": {"age": "25"}}));
    assert_eq!(resp["data"].as_array().unwrap().len(), 1);
    assert_eq!(resp["data"][0]["name"], "bob");

    // insert appends — count goes to 3, file on disk has the new row.
    assert_ok(&c.send(&json!({
        "cmd": "insert",
        "collection": "people",
        "doc": {"name": "carol", "age": "22"},
    })));
    let resp = c.send(&json!({"cmd": "count", "collection": "people", "query": {}}));
    assert_eq!(resp["data"]["count"], 3);
    let raw = std::fs::read_to_string(&csv_path).unwrap();
    assert!(raw.contains("carol,22"), "insert landed in the CSV: {:?}", raw);

    // update_one with $set.
    assert_ok(&c.send(&json!({
        "cmd": "update_one",
        "collection": "people",
        "query": {"name": "alice"},
        "update": {"$set": {"age": "31"}},
    })));
    let resp = c.send(&json!({"cmd": "find_one", "collection": "people",
        "query": {"name": "alice"}}));
    assert_eq!(resp["data"]["age"], "31");

    // delete_one drops bob.
    assert_ok(&c.send(&json!({
        "cmd": "delete_one",
        "collection": "people",
        "query": {"name": "bob"},
    })));
    let resp = c.send(&json!({"cmd": "count", "collection": "people", "query": {}}));
    assert_eq!(resp["data"]["count"], 2, "bob removed; alice + carol remain");

    // Schema commands must still be refused on a linked collection
    // regardless of the underlying adapter — the policy lives in the
    // handler, not in the adapter.
    let resp = c.send(&json!({
        "cmd": "create_index",
        "collection": "people",
        "field": "name",
    }));
    assert_eq!(resp["ok"], false);
    assert!(resp["error"].as_str().unwrap().contains("only CRUD"));
}

/// A CSV link to a file:// URL with the wrong extension MUST be
/// refused at link-time (well — at first-query time, since
/// link_collection doesn't pre-validate the URL). The error must
/// name .csv specifically so an operator can fix the typo.
#[test]
fn test_linked_collection_csv_rejects_non_csv_file_url() {
    let local = TestServer::start();
    let mut c = Client::connect(local.addr);
    assert_ok(&c.send(&json!({
        "cmd": "link_collection",
        "collection": "broken",
        "url": "file:///tmp/data.parquet",
    })));
    let resp = c.send(&json!({"cmd": "find", "collection": "broken", "query": {}}));
    assert_eq!(resp["ok"], false);
    assert!(
        resp["error"].as_str().unwrap().contains(".csv"),
        "{}",
        resp["error"]
    );
}

// ===========================================================================
// Linked collections — connection pool (FDW v2a)
// ===========================================================================

/// Repeated proxy calls reuse a pooled connection: after N calls the
/// pool holds 1 idle conn for that remote (not N). The first call
/// dials; the rest return-then-take the same conn.
#[test]
fn test_linked_collection_pool_reuses_connection() {
    use oxidb_server::remote_client;

    let remote = TestServer::start();
    let mut bclient = Client::connect(remote.addr);
    bclient.send(&json!({"cmd": "insert", "collection": "users", "doc": {"x": 1}}));

    let local = TestServer::start();
    let mut aclient = Client::connect(local.addr);
    let url = format!("oxidb://127.0.0.1:{}/users", remote.addr.port());
    aclient.send(&json!({
        "cmd": "link_collection",
        "collection": "remote_users",
        "url": url,
    }));

    // Fire 5 finds against the link.
    for _ in 0..5 {
        let resp = aclient.send(&json!({"cmd": "find", "collection": "remote_users", "query": {}}));
        assert_ok(&resp);
    }

    // Pool should hold exactly 1 idle conn for the remote — every
    // call returned it before the next took it.
    let p = remote_client::pool();
    let port = remote.addr.port();
    let idle = p.idle_count("127.0.0.1", port, None);
    assert_eq!(idle, 1, "pool idle_count after 5 sequential calls = {}, want 1", idle);
}

/// When a pooled connection has been killed by the remote (e.g.
/// process restart or LB tear-down), the next proxy call must NOT
/// surface that as an error — it has to drop the dead conn and
/// retry with a fresh dial transparently.
#[test]
fn test_linked_collection_pool_retries_when_pooled_conn_is_dead() {
    use oxidb_server::remote_client;

    let remote = TestServer::start();
    let mut bclient = Client::connect(remote.addr);
    bclient.send(&json!({"cmd": "insert", "collection": "users", "doc": {"x": 1}}));

    let local = TestServer::start();
    let mut aclient = Client::connect(local.addr);
    let url = format!("oxidb://127.0.0.1:{}/users", remote.addr.port());
    aclient.send(&json!({
        "cmd": "link_collection",
        "collection": "remote_dead",
        "url": url,
    }));

    // Prime the pool with one good conn.
    assert_ok(&aclient.send(&json!({"cmd": "find", "collection": "remote_dead", "query": {}})));
    let port = remote.addr.port();
    assert_eq!(remote_client::pool().idle_count("127.0.0.1", port, None), 1);

    // Forcibly close the pooled conn by taking it out and dropping it.
    // (Simulates the remote killing it while we held it idle.)
    let dead = remote_client::pool().take("127.0.0.1", port, None).expect("had idle conn");
    // Half-close so the next write/read on it errors out. shutdown()
    // is the most realistic stand-in for a server-side close.
    let _ = dead.shutdown(std::net::Shutdown::Both);
    remote_client::pool().give_back("127.0.0.1", port, None, dead);
    assert_eq!(remote_client::pool().idle_count("127.0.0.1", port, None), 1);

    // Next call must still succeed — the pool returns the dead conn,
    // round_trip fails, the code path falls through to a fresh dial.
    let resp = aclient.send(&json!({"cmd": "find", "collection": "remote_dead", "query": {}}));
    assert_ok(&resp);
    let docs = resp["data"].as_array().unwrap();
    assert_eq!(docs.len(), 1);

    // After the retry, exactly 1 idle conn (the fresh one) is back
    // in the pool.
    assert_eq!(remote_client::pool().idle_count("127.0.0.1", port, None), 1);
}

// ===========================================================================
// SCRAM-SHA-256 (RFC 7677) — server-side refactor end-to-end test
// ===========================================================================

/// Full RFC 7677 SCRAM-SHA-256 exchange between a stub client (this
/// test, doing the math by hand) and the server-side ScramState
/// machine. Exercises:
///
///   1. Server returns the user's STORED salt + iter_count in
///      server-first (was per-call random before the refactor).
///   2. Client derives proof from PLAINTEXT password using that salt
///      — the whole point of real SCRAM.
///   3. Server verifies the proof against the stored_key on the user
///      record; returns server-final v=<server_signature>.
///   4. Wrong password produces "authentication failed", not a panic.
#[test]
fn test_scram_rfc7677_roundtrip_against_stored_verifier() {
    use hmac::{Hmac, Mac};
    use oxidb_server::auth::{Role, UserStore, SCRAM_ITER_COUNT};
    use oxidb_server::scram::{
        base64_decode_simple_pub as b64dec, base64_encode_simple_pub as b64enc,
        hmac_sha256_pub as hmac256, pbkdf2_sha256_pub as pbkdf2, sha256_hash_pub as sha256,
        ScramState,
    };
    type HmacSha256 = Hmac<sha2::Sha256>;

    // Boot a fresh UserStore + create a user — this exercises
    // create_user_internal which now derives the SCRAM verifier from
    // the plaintext password at creation time.
    let dir = tempfile::tempdir().unwrap();
    let mut store = UserStore::open(dir.path()).unwrap();
    store.create_user("scramuser", "correct-horse-battery", Role::Admin).unwrap();

    // Sanity: the on-record user has the four scram_* fields set.
    let user = store.get_user("scramuser").expect("user exists");
    assert!(user.scram_salt.is_some(), "create_user must populate scram_salt");
    assert!(user.scram_iter_count.is_some());
    assert!(user.scram_stored_key.is_some());
    assert!(user.scram_server_key.is_some());

    // --- CLIENT: build client-first-message-bare ---
    let client_nonce = "fyko+d2lbbFgONRv9qkxdawL"; // RFC 7677 example nonce
    let client_first_bare = format!("n=scramuser,r={}", client_nonce);
    let client_first_full = format!("n,,{}", client_first_bare);

    // --- SERVER: process_client_first ---
    let (server_first, scram_state) =
        ScramState::process_client_first(&client_first_full, &store).expect("server-first");

    // Parse server-first to harvest salt + iter_count + combined nonce.
    let mut combined_nonce = String::new();
    let mut server_salt_b64 = String::new();
    let mut iter_count: u32 = 0;
    for part in server_first.split(',') {
        if let Some(r) = part.strip_prefix("r=") { combined_nonce = r.to_string(); }
        if let Some(s) = part.strip_prefix("s=") { server_salt_b64 = s.to_string(); }
        if let Some(i) = part.strip_prefix("i=") { iter_count = i.parse().unwrap(); }
    }
    assert!(combined_nonce.starts_with(client_nonce),
        "combined nonce must extend client_nonce: {}", combined_nonce);
    // Salt + iter_count match what the server stored, not a fresh
    // per-call random — this is the whole point of the refactor.
    assert_eq!(iter_count, SCRAM_ITER_COUNT);
    assert_eq!(server_salt_b64, user.scram_salt.clone().unwrap());

    // --- CLIENT: derive proof from PLAINTEXT password + server salt ---
    let salt = b64dec(&server_salt_b64).unwrap();
    let salted = pbkdf2(b"correct-horse-battery", &salt, iter_count);
    let client_key = hmac256(&salted, b"Client Key");
    let stored_key_client = sha256(&client_key);

    let channel_binding_b64 = "biws"; // base64 of "n,,"
    let client_final_no_proof = format!("c={},r={}", channel_binding_b64, combined_nonce);
    let auth_message = format!("{},{},{}", client_first_bare, server_first, client_final_no_proof);
    let client_signature = hmac256(&stored_key_client, auth_message.as_bytes());
    let client_proof: Vec<u8> = client_key.iter().zip(client_signature.iter())
        .map(|(a, b)| a ^ b).collect();
    let client_final = format!("{},p={}", client_final_no_proof, b64enc(&client_proof));

    // --- SERVER: process_client_final ---
    let (server_final, role) = scram_state
        .process_client_final(&client_final, &store)
        .expect("server-final (correct password)");
    assert_eq!(role, Role::Admin);

    // Verify server-final's v=<server_signature> matches what the
    // client computes — closes the loop.
    let v_b64 = server_final.strip_prefix("v=").unwrap();
    let server_sig_from_server = b64dec(v_b64).unwrap();
    // The client knows server_key from PBKDF2(plaintext, salt, iters)
    // -> HMAC(salted, "Server Key").
    let server_key_client = {
        let mut mac = HmacSha256::new_from_slice(&salted).unwrap();
        mac.update(b"Server Key");
        mac.finalize().into_bytes().to_vec()
    };
    let server_sig_from_client = hmac256(&server_key_client, auth_message.as_bytes());
    assert_eq!(server_sig_from_server, server_sig_from_client,
        "server-final signature must verify against client-derived server_key");
}

/// Wrong-password attempt produces "authentication failed", not a
/// successful exchange or a panic.
#[test]
fn test_scram_rfc7677_wrong_password_is_rejected() {
    use oxidb_server::auth::{Role, UserStore};
    use oxidb_server::scram::{
        base64_decode_simple_pub as b64dec, base64_encode_simple_pub as b64enc,
        hmac_sha256_pub as hmac256, pbkdf2_sha256_pub as pbkdf2, sha256_hash_pub as sha256,
        ScramState,
    };

    let dir = tempfile::tempdir().unwrap();
    let mut store = UserStore::open(dir.path()).unwrap();
    store.create_user("baduser", "right-password", Role::Read).unwrap();

    // Client-first
    let client_nonce = "abcdefghijklmnop";
    let client_first_bare = format!("n=baduser,r={}", client_nonce);
    let client_first_full = format!("n,,{}", client_first_bare);
    let (server_first, scram_state) =
        ScramState::process_client_first(&client_first_full, &store).unwrap();

    // Parse out salt + iter_count + nonce
    let mut combined_nonce = String::new();
    let mut salt_b64 = String::new();
    let mut iter_count: u32 = 0;
    for part in server_first.split(',') {
        if let Some(r) = part.strip_prefix("r=") { combined_nonce = r.to_string(); }
        if let Some(s) = part.strip_prefix("s=") { salt_b64 = s.to_string(); }
        if let Some(i) = part.strip_prefix("i=") { iter_count = i.parse().unwrap(); }
    }
    let salt = b64dec(&salt_b64).unwrap();

    // Client derives proof from WRONG password.
    let salted = pbkdf2(b"WRONG-password", &salt, iter_count);
    let client_key = hmac256(&salted, b"Client Key");
    let stored_key_c = sha256(&client_key);
    let client_final_no_proof = format!("c=biws,r={}", combined_nonce);
    let auth_message = format!("{},{},{}", client_first_bare, server_first, client_final_no_proof);
    let client_signature = hmac256(&stored_key_c, auth_message.as_bytes());
    let proof: Vec<u8> = client_key.iter().zip(client_signature.iter()).map(|(a,b)| a^b).collect();
    let client_final = format!("{},p={}", client_final_no_proof, b64enc(&proof));

    let err = scram_state.process_client_final(&client_final, &store).expect_err("must fail");
    assert!(
        err.contains("authentication failed"),
        "wrong-password error must say 'authentication failed': {}",
        err
    );
}

/// A user record without scram_* fields (i.e. created before this
/// refactor, loaded from a legacy users.json) cannot SCRAM-auth —
/// the server refuses with an actionable error pointing the operator
/// at the password update path.
#[test]
fn test_scram_rfc7677_legacy_user_without_verifier_is_refused() {
    use oxidb_server::auth::{Role, UserRecord, UserStore};
    use oxidb_server::scram::ScramState;
    use std::collections::HashMap;
    use std::fs;

    let dir = tempfile::tempdir().unwrap();
    let auth_dir = dir.path().join("_auth");
    fs::create_dir_all(&auth_dir).unwrap();
    // Hand-craft a users.json the way pre-refactor OxiDB would have
    // written it: password_hash + role only, no scram_* fields.
    let legacy = vec![UserRecord {
        username: "legacy".into(),
        password_hash: "$argon2id$v=19$m=19456,t=2,p=1$abcdefghij$_FAKE_HASH".into(),
        role: Role::Read,
        db_roles: HashMap::new(),
        scram_salt: None,
        scram_iter_count: None,
        scram_stored_key: None,
        scram_server_key: None,
    }];
    fs::write(
        auth_dir.join("users.json"),
        serde_json::to_string_pretty(&legacy).unwrap(),
    )
    .unwrap();
    let store = UserStore::open(dir.path()).unwrap();
    assert!(store.get_user("legacy").is_some());

    let err = match ScramState::process_client_first("n,,n=legacy,r=xyz", &store) {
        Ok(_) => panic!("process_client_first must fail for a legacy user"),
        Err(e) => e,
    };
    assert!(
        err.contains("no SCRAM verifier") && err.contains("account passwd"),
        "legacy-user error must mention the verifier + the recovery command: {}",
        err
    );
}

// ===========================================================================
// FDW SCRAM auth passthrough (v2b PR2)
// ===========================================================================
//
// `proxy_command` now performs a SCRAM exchange against the remote
// when the link URL carries userinfo, then forwards the actual
// command over the now-authenticated socket. These tests use a tiny
// auth-aware mock server that:
//   1. Refuses every non-auth command on an unauthenticated session.
//   2. Implements `authenticate` + `authenticate_continue` via
//      ScramState against a real UserStore.
//   3. After successful SCRAM, forwards subsequent commands to
//      handler::handle_request — which is enough to test that the
//      authed socket actually services real reads.
//
// We deliberately don't reuse the OXIDB_AUTH-aware async_server here:
// the rest of this test file is sync-handle_request-based, and a
// purpose-built mock keeps these tests independent of any tokio
// runtime spin-up.

/// Spawn a mock auth-required OxiDB server. Returns (addr, kept-alive
/// data dir). Keeps the temp dir alive via the returned struct so
/// dropping it on test exit cleans up the on-disk user store.
struct AuthTestServer {
    addr: SocketAddr,
    _dir: TempDir,
}

impl AuthTestServer {
    fn start(users: Vec<(&'static str, &'static str, oxidb_server::auth::Role)>) -> Self {
        use oxidb_server::auth::UserStore;

        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let db = Arc::new(OxiDb::open(&data_dir).unwrap());

        // Provision the SCRAM user store under data_dir/auth/. Bumps the
        // verifier at create-user time per the PR #15 refactor.
        let auth_dir = data_dir.join("auth");
        std::fs::create_dir_all(&auth_dir).unwrap();
        let mut store = UserStore::open(&auth_dir).unwrap();
        for (u, p, r) in users {
            store.create_user(u, p, r).unwrap();
        }
        let user_store = Arc::new(Mutex::new(store));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if let Ok(stream) = stream {
                    let db = Arc::clone(&db);
                    let user_store = Arc::clone(&user_store);
                    std::thread::spawn(move || {
                        Self::serve(stream, db, user_store);
                    });
                }
            }
        });

        Self {
            addr,
            _dir: dir,
        }
    }

    fn serve(
        mut stream: TcpStream,
        db: Arc<OxiDb>,
        user_store: Arc<Mutex<oxidb_server::auth::UserStore>>,
    ) {
        use oxidb_server::scram::ScramState;

        // Per-connection session state. Authed sessions get a username
        // (currently only used to gate command dispatch — handle_request
        // doesn't check RBAC in this mock, matching the rest of the
        // tests that bypass RBAC).
        let mut authed: Option<String> = None;
        let mut scram_state: Option<ScramState> = None;
        let mut active_tx: Option<u64> = None;

        loop {
            let msg = match read_message(&mut stream) {
                Ok(m) => m,
                Err(_) => break,
            };
            let request: Value = match serde_json::from_slice(&msg) {
                Ok(v) => v,
                Err(_) => break,
            };
            let cmd = request.get("command")
                .or_else(|| request.get("cmd"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let resp = match cmd {
                "authenticate" => {
                    let payload = request.get("payload").and_then(|v| v.as_str()).unwrap_or("");
                    let store = user_store.lock().unwrap();
                    match ScramState::process_client_first(payload, &store) {
                        Ok((server_first, state)) => {
                            scram_state = Some(state);
                            json!({"ok": true, "data": {"payload": server_first, "done": false}})
                        }
                        Err(e) => json!({"ok": false, "error": e}),
                    }
                }
                "authenticate_continue" => {
                    let payload = request.get("payload").and_then(|v| v.as_str()).unwrap_or("");
                    match scram_state.take() {
                        Some(state) => {
                            let store = user_store.lock().unwrap();
                            match state.process_client_final(payload, &store) {
                                Ok((server_final, _role)) => {
                                    authed = Some(state.username().to_string());
                                    json!({"ok": true, "data": {"payload": server_final, "done": true}})
                                }
                                Err(e) => json!({"ok": false, "error": e}),
                            }
                        }
                        None => json!({"ok": false, "error": "no SCRAM state"}),
                    }
                }
                _ => {
                    // Every other command requires the session to have
                    // completed SCRAM first. This is exactly the
                    // wire-level gate `authenticate()` must defeat.
                    if authed.is_none() {
                        json!({"ok": false, "error": "authentication required"})
                    } else {
                        let bytes = oxidb_server::handler::handle_request(&db, request, &mut active_tx);
                        serde_json::from_slice::<Value>(&bytes).unwrap()
                    }
                }
            };

            let bytes = resp.to_string().into_bytes();
            if write_message(&mut stream, &bytes).is_err() {
                break;
            }
        }
    }
}

#[test]
fn test_fdw_authenticate_helper_unlocks_a_remote_session() {
    use oxidb::links::parse_remote;
    use oxidb_server::remote_client;
    use std::net::TcpStream as RawStream;

    // Set up an auth-required remote with one user, then seed a doc
    // through a privileged side channel (the remote DB is shared with
    // the mock listener; we open it directly to insert without going
    // through the wire).
    let remote = AuthTestServer::start(vec![
        ("fdw_user", "s3cret-pass", oxidb_server::auth::Role::Admin),
    ]);
    {
        let db = OxiDb::open(remote._dir.path()).unwrap();
        db.insert("people", json!({"name": "alice", "age": 30})).unwrap();
        db.insert("people", json!({"name": "bob", "age": 25})).unwrap();
    }

    // Confirm the gate: an unauthenticated socket gets refused.
    let stream = RawStream::connect(remote.addr).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    let req = json!({"cmd": "find", "collection": "people", "query": {}});
    write_message(&mut (&stream), &req.to_string().into_bytes()).unwrap();
    let resp_bytes = read_message(&mut (&stream)).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], false, "must refuse unauthed find: {resp}");
    assert!(resp["error"].as_str().unwrap().contains("authentication required"));
    drop(stream);

    // Now: a freshly-dialed socket, after authenticate(), services the
    // same find — proves the helper actually moved the session state
    // from "anonymous" to "authed".
    let stream = RawStream::connect(remote.addr).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    remote_client::authenticate(&stream, "fdw_user", "s3cret-pass")
        .expect("authenticate must succeed against a real remote");

    let req = json!({"cmd": "find", "collection": "people", "query": {}});
    write_message(&mut (&stream), &req.to_string().into_bytes()).unwrap();
    let resp_bytes = read_message(&mut (&stream)).unwrap();
    let resp: Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], true, "post-auth find must succeed: {resp}");
    let docs = resp["data"].as_array().expect("data is array");
    assert_eq!(docs.len(), 2, "must see both seeded docs");

    // And: a wrong password makes the helper return the SCRAM-level
    // error verbatim — useful for ops debugging.
    let stream2 = RawStream::connect(remote.addr).unwrap();
    stream2.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    let err = remote_client::authenticate(&stream2, "fdw_user", "WRONG").unwrap_err();
    assert!(
        err.contains("authentication failed"),
        "wrong-password error must surface the server's message: {}",
        err
    );

    // Sanity: parse_remote on the URL form we expect operators to use
    // sets user + password correctly so proxy_command will call into
    // authenticate() on its own.
    let parsed = parse_remote(&format!(
        "oxidb://fdw_user:s3cret-pass@127.0.0.1:{}/people",
        remote.addr.port()
    )).unwrap();
    assert_eq!(parsed.user.as_deref(), Some("fdw_user"));
    assert_eq!(parsed.password.as_deref(), Some("s3cret-pass"));
}

#[test]
fn test_fdw_proxy_command_authenticates_and_pools_per_user() {
    use oxidb::links::parse_remote;
    use oxidb_server::remote_client;

    let remote = AuthTestServer::start(vec![
        ("p_user", "pw-A", oxidb_server::auth::Role::Admin),
    ]);
    {
        let db = OxiDb::open(remote._dir.path()).unwrap();
        db.insert("items", json!({"sku": "x"})).unwrap();
    }
    let port = remote.addr.port();
    let url = format!("oxidb://p_user:pw-A@127.0.0.1:{}/items", port);
    let parsed = parse_remote(&url).unwrap();

    // First call: cold pool → dial + authenticate + find.
    let cmd = json!({"cmd": "find", "collection": "items", "query": {}});
    let resp = remote_client::proxy_command(&parsed, &cmd)
        .expect("proxy_command with valid creds must succeed");
    assert_eq!(resp["ok"], true, "{resp}");
    assert_eq!(resp["data"].as_array().unwrap().len(), 1);

    // The conn should now live under the user-keyed bucket — NOT
    // under the anonymous bucket, which would let an unauthed link
    // accidentally reuse our authed socket.
    let pool = remote_client::pool();
    assert_eq!(
        pool.idle_count("127.0.0.1", port, Some("p_user")),
        1,
        "authed conn must land in the per-user bucket"
    );
    assert_eq!(
        pool.idle_count("127.0.0.1", port, None),
        0,
        "anonymous bucket must remain empty for an authed remote"
    );

    // Second call: reuses the pooled authed conn. If the helper had
    // re-authenticated it would still pass, but observably idle_count
    // would dip to 0 mid-call — what we really want to assert is that
    // the call works without a fresh handshake. The simplest proxy
    // for "no fresh handshake" is "the wrong-password variant of the
    // helper, if it had run, would have errored". So instead we just
    // make the call and assert success: combined with the bucket
    // assertion above, that's enough.
    let resp2 = remote_client::proxy_command(&parsed, &cmd).unwrap();
    assert_eq!(resp2["ok"], true, "{resp2}");
    assert_eq!(
        pool.idle_count("127.0.0.1", port, Some("p_user")),
        1,
        "pool stays at 1 after reuse"
    );

    // Wrong password → proxy_command surfaces the failure cleanly
    // and does NOT pool the half-authed conn. Use a SECOND server +
    // a username that has never had a successful auth in this process,
    // so the pool can't sneak us a previously-authed reuse. (Once a
    // user has authed, the pool happily reuses that session — the
    // pool key is `(host, port, user)`, deliberately not including
    // the password.)
    let remote2 = AuthTestServer::start(vec![
        ("bad_pw_user", "the-real-pw", oxidb_server::auth::Role::Admin),
    ]);
    let bad_url = format!(
        "oxidb://bad_pw_user:WRONG@127.0.0.1:{}/items",
        remote2.addr.port()
    );
    let bad_parsed = parse_remote(&bad_url).unwrap();
    let err = remote_client::proxy_command(&bad_parsed, &cmd).unwrap_err();
    assert!(
        err.contains("authentication failed"),
        "wrong-pw error must surface verbatim: {}",
        err
    );
    // Failed auth must NOT pool the half-authed conn.
    assert_eq!(
        pool.idle_count("127.0.0.1", remote2.addr.port(), Some("bad_pw_user")),
        0,
        "failed-auth conn must NOT pool"
    );
}
