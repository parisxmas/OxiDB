//! End-to-end ACID compliance integration tests.
//!
//! Each test starts a real TCP server on a random port, connects via the
//! length-prefixed protocol, and exercises transactional guarantees through
//! the full server stack.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tempfile::TempDir;

use oxidb::OxiDb;
use oxidb_server::protocol::{read_message, write_message};

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    _dir: Option<TempDir>,
    data_dir: PathBuf,
}

impl TestServer {
    /// Start a server on a random port with a fresh temp data directory.
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

    /// Start a server on a random port using the given data directory.
    /// The caller is responsible for keeping the directory alive.
    fn start_at_path(data_dir: &Path) -> SocketAddr {
        let db = OxiDb::open(data_dir).expect("failed to open database");
        let db = Arc::new(db);

        let listener =
            TcpListener::bind("127.0.0.1:0").expect("failed to bind");
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = mpsc::channel::<TcpStream>();
        let rx = Arc::new(Mutex::new(rx));

        // Spawn 4 worker threads (some tests use 3+ concurrent clients).
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

        // Accept loop in background.
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

    /// Reopen the same data directory with a fresh server instance.
    fn reopen(&self) -> TestServer {
        let addr = Self::start_at_path(&self.data_dir);
        TestServer {
            addr,
            _dir: None, // original TempDir still owned by first instance
            data_dir: self.data_dir.clone(),
        }
    }
}

/// Minimal per-connection handler — mirrors oxidb-server/src/main.rs exactly.
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

        let response =
            oxidb_server::handler::handle_request(db, &request, &mut active_tx);
        let resp_bytes = response.to_string().into_bytes();

        if write_message(&mut stream, &resp_bytes).is_err() {
            break;
        }
    }

    // Auto-rollback on disconnect.
    if let Some(tx_id) = active_tx {
        let _ = db.rollback_transaction(tx_id);
    }
}

// -- Client helper ----------------------------------------------------------

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

    fn ping(&mut self) -> Value {
        self.send(&json!({"cmd": "ping"}))
    }

    fn insert(&mut self, col: &str, doc: Value) -> Value {
        self.send(&json!({"cmd": "insert", "collection": col, "doc": doc}))
    }

    fn find(&mut self, col: &str, query: Value) -> Value {
        self.send(&json!({"cmd": "find", "collection": col, "query": query}))
    }

    fn update(&mut self, col: &str, query: Value, update: Value) -> Value {
        self.send(&json!({
            "cmd": "update",
            "collection": col,
            "query": query,
            "update": update,
        }))
    }

    fn delete(&mut self, col: &str, query: Value) -> Value {
        self.send(&json!({"cmd": "delete", "collection": col, "query": query}))
    }

    fn begin_tx(&mut self) -> Value {
        self.send(&json!({"cmd": "begin_tx"}))
    }

    fn commit_tx(&mut self) -> Value {
        self.send(&json!({"cmd": "commit_tx"}))
    }

    fn rollback_tx(&mut self) -> Value {
        self.send(&json!({"cmd": "rollback_tx"}))
    }
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

fn assert_ok(resp: &Value) {
    assert_eq!(
        resp["ok"], true,
        "expected ok response, got: {resp}"
    );
}

fn assert_err(resp: &Value) {
    assert_eq!(
        resp["ok"], false,
        "expected error response, got: {resp}"
    );
}

fn find_docs(client: &mut Client, col: &str, query: Value) -> Vec<Value> {
    let resp = client.find(col, query);
    assert_ok(&resp);
    resp["data"]
        .as_array()
        .expect("data should be array")
        .clone()
}

// ===========================================================================
// Test cases
// ===========================================================================

// 1. Atomicity — commit
#[test]
fn test_atomicity_commit() {
    let server = TestServer::start();
    let mut c1 = Client::connect(server.addr);

    // Sanity check
    assert_ok(&c1.ping());

    // Begin tx, insert into two collections, commit
    assert_ok(&c1.begin_tx());
    assert_ok(&c1.insert("users", json!({"name": "Alice"})));
    assert_ok(&c1.insert("orders", json!({"item": "Widget", "user": "Alice"})));
    assert_ok(&c1.commit_tx());

    // Verify both via a separate client (no tx)
    let mut c2 = Client::connect(server.addr);
    let users = find_docs(&mut c2, "users", json!({}));
    let orders = find_docs(&mut c2, "orders", json!({}));
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"], "Alice");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0]["item"], "Widget");
}

// 2. Atomicity — rollback
#[test]
fn test_atomicity_rollback() {
    let server = TestServer::start();
    let mut c1 = Client::connect(server.addr);

    assert_ok(&c1.begin_tx());
    assert_ok(&c1.insert("users", json!({"name": "Bob"})));
    assert_ok(&c1.insert("orders", json!({"item": "Gadget", "user": "Bob"})));
    assert_ok(&c1.rollback_tx());

    // Neither doc should exist.
    let mut c2 = Client::connect(server.addr);
    let users = find_docs(&mut c2, "users", json!({}));
    let orders = find_docs(&mut c2, "orders", json!({}));
    assert_eq!(users.len(), 0);
    assert_eq!(orders.len(), 0);
}

// 3. Atomicity — disconnect auto-rollback
#[test]
fn test_atomicity_disconnect_auto_rollback() {
    let server = TestServer::start();

    // Client 1: begin tx, insert, then disconnect without commit/rollback
    {
        let mut c1 = Client::connect(server.addr);
        assert_ok(&c1.begin_tx());
        assert_ok(&c1.insert("users", json!({"name": "Ghost"})));
        // c1 drops here — TCP close triggers auto-rollback
    }

    // Give the server a moment to process the disconnect
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Client 2: doc should not exist
    let mut c2 = Client::connect(server.addr);
    let users = find_docs(&mut c2, "users", json!({}));
    assert_eq!(users.len(), 0);
}

// 4. Consistency — OCC version conflict
#[test]
fn test_consistency_version_conflict() {
    let server = TestServer::start();

    // Seed a doc outside any transaction.
    let mut setup = Client::connect(server.addr);
    assert_ok(&setup.insert("accounts", json!({"owner": "Alice", "balance": 100})));

    // Client 1: begin tx, read the doc (records version)
    let mut c1 = Client::connect(server.addr);
    assert_ok(&c1.begin_tx());
    let resp = c1.find("accounts", json!({"owner": "Alice"}));
    assert_ok(&resp);

    // Client 2 (no tx): update the same doc — bumps its version
    let mut c2 = Client::connect(server.addr);
    let resp = c2.update(
        "accounts",
        json!({"owner": "Alice"}),
        json!({"$set": {"balance": 200}}),
    );
    assert_ok(&resp);

    // Client 1: update same doc within tx, then try to commit
    assert_ok(&c1.update(
        "accounts",
        json!({"owner": "Alice"}),
        json!({"$set": {"balance": 150}}),
    ));
    let commit_resp = c1.commit_tx();
    assert_err(&commit_resp);
    assert!(
        commit_resp["error"]
            .as_str()
            .unwrap()
            .contains("conflict"),
        "expected TransactionConflict error, got: {}",
        commit_resp["error"]
    );
}

// 5. Isolation — uncommitted writes not visible
#[test]
fn test_isolation_uncommitted_not_visible() {
    let server = TestServer::start();

    // Client 1: begin tx, insert (buffered, not committed)
    let mut c1 = Client::connect(server.addr);
    assert_ok(&c1.begin_tx());
    assert_ok(&c1.insert("items", json!({"name": "Secret"})));

    // Client 2 (no tx): should NOT see the doc
    let mut c2 = Client::connect(server.addr);
    let docs = find_docs(&mut c2, "items", json!({}));
    assert_eq!(docs.len(), 0, "uncommitted doc should not be visible");

    // Commit
    assert_ok(&c1.commit_tx());

    // Now client 2 should see it.
    let docs = find_docs(&mut c2, "items", json!({}));
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "Secret");
}

// 6. Isolation — concurrent tx on separate collections, no conflict
#[test]
fn test_isolation_concurrent_tx_no_conflict() {
    let server = TestServer::start();

    let mut c1 = Client::connect(server.addr);
    let mut c2 = Client::connect(server.addr);

    // Both begin tx
    assert_ok(&c1.begin_tx());
    assert_ok(&c2.begin_tx());

    // Insert into different collections
    assert_ok(&c1.insert("col_a", json!({"x": 1})));
    assert_ok(&c2.insert("col_b", json!({"y": 2})));

    // Both commit — no conflict because no shared data
    assert_ok(&c1.commit_tx());
    assert_ok(&c2.commit_tx());

    // Verify
    let mut reader = Client::connect(server.addr);
    let a = find_docs(&mut reader, "col_a", json!({}));
    let b = find_docs(&mut reader, "col_b", json!({}));
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
}

// 7. Durability — committed data survives server restart
#[test]
fn test_durability_committed_survives_reopen() {
    let server = TestServer::start();

    // Insert doc (non-tx)
    let mut c = Client::connect(server.addr);
    assert_ok(&c.insert("persist", json!({"key": "durable"})));
    let docs = find_docs(&mut c, "persist", json!({}));
    assert_eq!(docs.len(), 1);

    // Drop client, "stop" server (let background threads die), reopen
    drop(c);
    let server2 = server.reopen();

    let mut c2 = Client::connect(server2.addr);
    let docs = find_docs(&mut c2, "persist", json!({}));
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["key"], "durable");
}

// 8. Durability — tx-committed data survives restart
#[test]
fn test_durability_tx_committed_survives_reopen() {
    let server = TestServer::start();

    let mut c = Client::connect(server.addr);
    assert_ok(&c.begin_tx());
    assert_ok(&c.insert("tx_persist_a", json!({"a": 1})));
    assert_ok(&c.insert("tx_persist_b", json!({"b": 2})));
    assert_ok(&c.commit_tx());

    drop(c);
    let server2 = server.reopen();

    let mut c2 = Client::connect(server2.addr);
    let a = find_docs(&mut c2, "tx_persist_a", json!({}));
    let b = find_docs(&mut c2, "tx_persist_b", json!({}));
    assert_eq!(a.len(), 1);
    assert_eq!(a[0]["a"], 1);
    assert_eq!(b.len(), 1);
    assert_eq!(b[0]["b"], 2);
}

// 9. Multi-op tx — insert, update, delete in one transaction
#[test]
fn test_tx_insert_update_delete_commit() {
    let server = TestServer::start();
    let mut setup = Client::connect(server.addr);

    // Seed two docs outside tx
    assert_ok(&setup.insert("multi", json!({"name": "Alpha", "v": 1})));
    assert_ok(&setup.insert("multi", json!({"name": "Beta", "v": 1})));

    // Begin tx: insert new, update Alpha, delete Beta
    let mut c = Client::connect(server.addr);
    assert_ok(&c.begin_tx());
    assert_ok(&c.insert("multi", json!({"name": "Gamma", "v": 1})));
    assert_ok(&c.update(
        "multi",
        json!({"name": "Alpha"}),
        json!({"$set": {"v": 2}}),
    ));
    assert_ok(&c.delete("multi", json!({"name": "Beta"})));
    assert_ok(&c.commit_tx());

    // Verify: Alpha (v=2), Gamma (v=1), no Beta
    let mut reader = Client::connect(server.addr);
    let docs = find_docs(&mut reader, "multi", json!({}));
    assert_eq!(docs.len(), 2, "expected Alpha + Gamma, got: {docs:?}");

    let alpha = docs.iter().find(|d| d["name"] == "Alpha").expect("Alpha missing");
    assert_eq!(alpha["v"], 2);

    let gamma = docs.iter().find(|d| d["name"] == "Gamma").expect("Gamma missing");
    assert_eq!(gamma["v"], 1);

    assert!(
        docs.iter().all(|d| d["name"] != "Beta"),
        "Beta should have been deleted"
    );
}

// 10. Error handling — double begin rejected
#[test]
fn test_double_begin_rejected() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    assert_ok(&c.begin_tx());
    let resp = c.begin_tx();
    assert_err(&resp);
    assert!(
        resp["error"]
            .as_str()
            .unwrap()
            .contains("already active"),
        "expected 'already active' error, got: {}",
        resp["error"]
    );
}

// 11. Error handling — commit without begin rejected
#[test]
fn test_commit_without_begin_rejected() {
    let server = TestServer::start();
    let mut c = Client::connect(server.addr);

    let resp = c.commit_tx();
    assert_err(&resp);
    assert!(
        resp["error"]
            .as_str()
            .unwrap()
            .contains("no active transaction"),
        "expected 'no active transaction' error, got: {}",
        resp["error"]
    );
}
