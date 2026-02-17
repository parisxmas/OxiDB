use std::collections::HashMap;

use oxidb::{EncryptionKey, OxiDb};
use serde_json::json;

/// Test encryption-at-rest: data written with key is readable with same key.
#[test]
fn encryption_at_rest_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("test.key");
    std::fs::write(&key_path, &[0x42u8; 32]).unwrap();

    let key = EncryptionKey::load_from_file(&key_path).unwrap();

    // Write data with encryption
    {
        let db = OxiDb::open_with_options(dir.path(), Some(key.clone())).unwrap();
        db.insert("users", json!({"name": "Alice", "age": 30})).unwrap();
        db.insert("users", json!({"name": "Bob", "age": 25})).unwrap();
    }

    // Reopen with same key — data should be accessible
    {
        let db = OxiDb::open_with_options(dir.path(), Some(key.clone())).unwrap();
        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 2);
        let names: Vec<&str> = docs.iter().map(|d| d["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }
}

/// Test that data files are not readable as plain JSON when encrypted.
#[test]
fn encrypted_data_not_plain_text() {
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("test.key");
    std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
    let key = EncryptionKey::load_from_file(&key_path).unwrap();

    let db = OxiDb::open_with_options(dir.path(), Some(key)).unwrap();
    db.insert("secrets", json!({"password": "super_secret_123"})).unwrap();
    drop(db);

    // Read the raw .dat file — it should NOT contain the plaintext
    let dat = std::fs::read(dir.path().join("secrets.dat")).unwrap();
    let dat_str = String::from_utf8_lossy(&dat);
    assert!(
        !dat_str.contains("super_secret_123"),
        "plaintext found in encrypted data file"
    );
}

/// Test that data is accessible without encryption key when not encrypted.
#[test]
fn no_encryption_is_backward_compatible() {
    let dir = tempfile::tempdir().unwrap();

    let db = OxiDb::open(dir.path()).unwrap();
    db.insert("test", json!({"value": 42})).unwrap();
    drop(db);

    let db = OxiDb::open(dir.path()).unwrap();
    let docs = db.find("test", &json!({})).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["value"], 42);
}

/// Test encryption with blob store.
#[test]
fn encrypted_blob_store() {
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("test.key");
    std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
    let key = EncryptionKey::load_from_file(&key_path).unwrap();

    let db = OxiDb::open_with_options(dir.path(), Some(key.clone())).unwrap();
    db.put_object("docs", "hello.txt", b"Hello World", "text/plain", HashMap::new())
        .unwrap();

    let (data, _meta) = db.get_object("docs", "hello.txt").unwrap();
    assert_eq!(data, b"Hello World");

    // Verify raw .data file is encrypted
    let data_file = dir.path().join("_blobs/docs/0.data");
    let raw = std::fs::read(data_file).unwrap();
    assert_ne!(raw, b"Hello World", "blob data should be encrypted on disk");
}

/// Test encryption with compaction.
#[test]
fn encrypted_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("test.key");
    std::fs::write(&key_path, &[0xABu8; 32]).unwrap();
    let key = EncryptionKey::load_from_file(&key_path).unwrap();

    let db = OxiDb::open_with_options(dir.path(), Some(key)).unwrap();
    for i in 0..10 {
        db.insert("coll", json!({"n": i, "pad": "x".repeat(100)})).unwrap();
    }
    db.delete("coll", &json!({"n": {"$lt": 7}})).unwrap();

    let stats = db.compact("coll").unwrap();
    assert_eq!(stats.docs_kept, 3);

    let docs = db.find("coll", &json!({})).unwrap();
    assert_eq!(docs.len(), 3);
}

/// Test user store creation and authentication.
#[test]
fn user_store_basic() {
    use oxidb_server::auth::{Role, UserStore};

    let dir = tempfile::tempdir().unwrap();
    let mut store = UserStore::open(dir.path()).unwrap();

    // Default admin should exist
    let users = store.list_users();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["username"], "admin");

    // Create a new user
    store.create_user("alice", "pass123", Role::ReadWrite).unwrap();
    assert!(store.authenticate("alice", "pass123").is_some());
    assert!(store.authenticate("alice", "wrongpass").is_none());

    // Update user role
    store.update_user("alice", None, Some(Role::Read)).unwrap();
    assert_eq!(store.authenticate("alice", "pass123"), Some(Role::Read));

    // Drop user
    store.drop_user("alice").unwrap();
    assert!(store.authenticate("alice", "pass123").is_none());
}

/// Test RBAC permissions.
#[test]
fn rbac_permissions() {
    use oxidb_server::auth::Role;
    use oxidb_server::rbac;

    // Admin can do everything
    assert!(rbac::is_permitted(Role::Admin, "insert"));
    assert!(rbac::is_permitted(Role::Admin, "create_user"));
    assert!(rbac::is_permitted(Role::Admin, "drop_collection"));

    // ReadWrite can do CRUD but not user management
    assert!(rbac::is_permitted(Role::ReadWrite, "insert"));
    assert!(rbac::is_permitted(Role::ReadWrite, "find"));
    assert!(rbac::is_permitted(Role::ReadWrite, "update"));
    assert!(rbac::is_permitted(Role::ReadWrite, "delete"));
    assert!(rbac::is_permitted(Role::ReadWrite, "begin_tx"));
    assert!(!rbac::is_permitted(Role::ReadWrite, "drop_collection"));

    // Read can only read
    assert!(rbac::is_permitted(Role::Read, "find"));
    assert!(rbac::is_permitted(Role::Read, "find_one"));
    assert!(rbac::is_permitted(Role::Read, "count"));
    assert!(rbac::is_permitted(Role::Read, "aggregate"));
    assert!(!rbac::is_permitted(Role::Read, "insert"));
    assert!(!rbac::is_permitted(Role::Read, "update"));
    assert!(!rbac::is_permitted(Role::Read, "delete"));
    assert!(!rbac::is_permitted(Role::Read, "drop_collection"));
}

/// Test audit log writes entries.
#[test]
fn audit_log_writes_entries() {
    use oxidb_server::audit::{AuditEvent, AuditLog};

    let dir = tempfile::tempdir().unwrap();
    let log = AuditLog::open(dir.path()).unwrap();

    log.log(&AuditEvent {
        ts: "2024-01-01T00:00:00Z".to_string(),
        user: "admin",
        cmd: "insert",
        collection: Some("users"),
        result: "ok",
        detail: "",
    });

    log.log(&AuditEvent {
        ts: "2024-01-01T00:00:01Z".to_string(),
        user: "alice",
        cmd: "find",
        collection: Some("users"),
        result: "ok",
        detail: "",
    });

    // Read the audit log and verify
    let content = std::fs::read_to_string(dir.path().join("_audit/audit.log")).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2);

    let entry1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(entry1["user"], "admin");
    assert_eq!(entry1["cmd"], "insert");

    let entry2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(entry2["user"], "alice");
    assert_eq!(entry2["cmd"], "find");
}

/// Test encryption with transactions.
#[test]
fn encrypted_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("test.key");
    std::fs::write(&key_path, &[0x77u8; 32]).unwrap();
    let key = EncryptionKey::load_from_file(&key_path).unwrap();

    let db = OxiDb::open_with_options(dir.path(), Some(key)).unwrap();

    let tx_id = db.begin_transaction();
    db.tx_insert(tx_id, "test", json!({"name": "TxDoc"})).unwrap();
    db.commit_transaction(tx_id).unwrap();

    let docs = db.find("test", &json!({})).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "TxDoc");
}

/// Test session state tracking.
#[test]
fn session_tracking() {
    use oxidb_server::auth::Role;
    use oxidb_server::session::Session;

    let mut session = Session::new();
    assert!(!session.is_authenticated());
    assert_eq!(session.username_str(), "anonymous");

    session.set_authenticated("alice".to_string(), Role::ReadWrite);
    assert!(session.is_authenticated());
    assert_eq!(session.username_str(), "alice");
    assert_eq!(session.role(), Some(Role::ReadWrite));
}

/// Test user store persistence across restarts.
#[test]
fn user_store_persistence() {
    use oxidb_server::auth::{Role, UserStore};

    let dir = tempfile::tempdir().unwrap();

    // First open — creates default admin
    {
        let mut store = UserStore::open(dir.path()).unwrap();
        store.create_user("bob", "bobpass", Role::Read).unwrap();
    }

    // Second open — should still have both users
    {
        let store = UserStore::open(dir.path()).unwrap();
        let users = store.list_users();
        assert_eq!(users.len(), 2);
        assert!(store.authenticate("bob", "bobpass").is_some());
    }
}
