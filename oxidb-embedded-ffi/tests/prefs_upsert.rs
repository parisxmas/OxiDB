//! The mobile Preferences contract, exercised through the C API exactly as
//! the Swift/Android wrappers drive it: `update`/`update_one` with
//! `"upsert": true` (insert-or-replace in one call), upsert refused by name
//! inside a transaction, and the raw-bytes encrypted open
//! (`oxidb_open_encrypted_bytes`) — the Keystore-friendly variant — proven
//! by a write/reopen/read roundtrip.

use std::ffi::{CStr, CString, c_char};
use std::path::Path;

use oxidb_embedded_ffi::{
    oxidb_close, oxidb_execute, oxidb_free_string, oxidb_open, oxidb_open_encrypted_bytes,
};

fn open(dir: &Path) -> *mut std::ffi::c_void {
    let c = CString::new(dir.to_str().unwrap()).unwrap();
    let h = unsafe { oxidb_open(c.as_ptr()) };
    assert!(!h.is_null(), "open failed");
    h
}

fn open_encrypted(dir: &Path, key: &[u8; 32]) -> *mut std::ffi::c_void {
    let c = CString::new(dir.to_str().unwrap()).unwrap();
    unsafe { oxidb_open_encrypted_bytes(c.as_ptr(), key.as_ptr(), key.len()) }
}

/// Execute a JSON command, return the raw response string.
fn exec(h: *mut std::ffi::c_void, req: serde_json::Value) -> String {
    let c = CString::new(req.to_string()).unwrap();
    let ptr: *mut c_char = unsafe { oxidb_execute(h, c.as_ptr()) };
    assert!(!ptr.is_null(), "execute returned null");
    let out = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string();
    unsafe { oxidb_free_string(ptr) };
    out
}

fn exec_ok(h: *mut std::ffi::c_void, req: serde_json::Value) -> serde_json::Value {
    let out = exec(h, req);
    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["ok"], true, "command failed: {out}");
    v
}

/// One preferences write, as the wrappers spell it.
fn put(h: *mut std::ffi::c_void, key: &str, value: serde_json::Value) -> serde_json::Value {
    exec_ok(
        h,
        serde_json::json!({"cmd": "update_one", "collection": "_prefs",
            "query": {"k": key}, "update": {"$set": {"v": value}}, "upsert": true}),
    )
}

fn get(h: *mut std::ffi::c_void, key: &str) -> serde_json::Value {
    let v = exec_ok(
        h,
        serde_json::json!({"cmd": "find_one", "collection": "_prefs", "query": {"k": key}}),
    );
    v["data"]["v"].clone()
}

#[test]
fn upsert_inserts_then_replaces_and_reports_the_id() {
    let dir = tempfile::tempdir().unwrap();
    let h = open(dir.path());
    exec_ok(
        h,
        serde_json::json!({"cmd": "create_unique_index", "collection": "_prefs", "field": "k"}),
    );

    // First put: nothing matches — the response must carry the upserted id.
    let first = put(h, "theme", serde_json::json!("dark"));
    assert!(
        first["data"]["upserted"].is_u64() || first["upserted"].is_u64(),
        "upsert-insert must report the id: {first}"
    );
    assert_eq!(get(h, "theme"), serde_json::json!("dark"));

    // Second put: replaces in place — still exactly one document.
    let second = put(h, "theme", serde_json::json!("light"));
    assert!(
        second["data"]["upserted"].is_null() && second["upserted"].is_null(),
        "an upsert that updated must not claim an insert: {second}"
    );
    assert_eq!(get(h, "theme"), serde_json::json!("light"));
    let count = exec_ok(
        h,
        serde_json::json!({"cmd": "count", "collection": "_prefs", "query": {"k": "theme"}}),
    );
    assert_eq!(count["data"]["count"].as_u64().or(count["count"].as_u64()), Some(1));

    // Non-string values survive the roundtrip.
    put(h, "volume", serde_json::json!(0.75));
    put(h, "onboarded", serde_json::json!(true));
    assert_eq!(get(h, "volume"), serde_json::json!(0.75));
    assert_eq!(get(h, "onboarded"), serde_json::json!(true));

    unsafe { oxidb_close(h) };
}

#[test]
fn upsert_inside_a_transaction_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let h = open(dir.path());
    exec_ok(h, serde_json::json!({"cmd": "begin_tx"}));
    let out = exec(
        h,
        serde_json::json!({"cmd": "update", "collection": "c",
            "query": {"a": 1}, "update": {"$set": {"b": 2}}, "upsert": true}),
    );
    assert!(
        out.contains("not supported inside a transaction"),
        "must refuse, not silently drop the upsert flag: {out}"
    );
    unsafe { oxidb_close(h) };
}

#[test]
fn encrypted_bytes_open_roundtrips_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let key = [42u8; 32];

    let h = open_encrypted(dir.path(), &key);
    assert!(!h.is_null(), "encrypted open failed");
    put(h, "secret", serde_json::json!("s3cr3t"));
    unsafe { oxidb_close(h) };

    // Same key: data readable.
    let h = open_encrypted(dir.path(), &key);
    assert!(!h.is_null());
    assert_eq!(get(h, "secret"), serde_json::json!("s3cr3t"));
    unsafe { oxidb_close(h) };

    // The stored bytes must not contain the plaintext anywhere on disk.
    let mut found = false;
    for entry in walk(dir.path()) {
        if let Ok(bytes) = std::fs::read(&entry)
            && bytes.windows(6).any(|w| w == b"s3cr3t")
        {
            found = true;
            eprintln!("plaintext found in {entry:?}");
        }
    }
    assert!(!found, "plaintext leaked to disk despite encryption");

    // A wrong key must not open a readable database.
    let wrong = [7u8; 32];
    let h = open_encrypted(dir.path(), &wrong);
    if !h.is_null() {
        let out = exec(
            h,
            serde_json::json!({"cmd": "find_one", "collection": "_prefs", "query": {"k": "secret"}}),
        );
        let v: serde_json::Value = serde_json::from_str(&out).unwrap_or_default();
        assert_ne!(
            v["data"]["v"],
            serde_json::json!("s3cr3t"),
            "wrong key must never decrypt"
        );
        unsafe { oxidb_close(h) };
    }
}

/// Zero-length key or wrong length: refused (NULL), never a silent
/// truncation to some other key.
#[test]
fn wrong_key_length_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let c = CString::new(dir.path().to_str().unwrap()).unwrap();
    let short = [1u8; 16];
    let h = unsafe { oxidb_open_encrypted_bytes(c.as_ptr(), short.as_ptr(), short.len()) };
    assert!(h.is_null());
    let h = unsafe { oxidb_open_encrypted_bytes(c.as_ptr(), std::ptr::null(), 32) };
    assert!(h.is_null());
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}
