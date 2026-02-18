use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_void};
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};

use base64::Engine;
use oxidb::OxiDb;
use oxidb::query::parse_find_options;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Per-handle state
// ---------------------------------------------------------------------------

struct OxiDbHandle {
    db: Arc<OxiDb>,
    active_tx: Mutex<Option<u64>>,
}

type Handle = c_void;

// ---------------------------------------------------------------------------
// JSON response helpers (same format as the server)
// ---------------------------------------------------------------------------

fn ok_bytes(data: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({ "ok": true, "data": data })).unwrap()
}

fn err_bytes(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "ok": false, "error": msg })).unwrap()
}

/// Serialize find results directly from Arc references — zero Value::clone.
fn ok_docs_bytes(docs: &[Arc<Value>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(docs.len() * 200 + 64);
    buf.extend_from_slice(b"{\"ok\":true,\"data\":[");
    for (i, doc) in docs.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        serde_json::to_writer(&mut buf, doc.as_ref()).unwrap();
    }
    buf.extend_from_slice(b"]}");
    buf
}

// ---------------------------------------------------------------------------
// Command dispatcher (mirrors oxidb-server/src/handler.rs handle_request)
// ---------------------------------------------------------------------------

fn handle_request(db: &Arc<OxiDb>, request: Value, active_tx: &mut Option<u64>) -> Vec<u8> {
    let cmd = match request.get("cmd").and_then(|v| v.as_str().map(|s| s.to_string())) {
        Some(c) => c,
        None => return err_bytes("missing or invalid 'cmd' field"),
    };

    let collection: Option<String> =
        request.get("collection").and_then(|v| v.as_str().map(|s| s.to_string()));

    let mut request = request;

    match cmd.as_str() {
        "ping" => ok_bytes(json!("pong")),

        // --- Transactions ---

        "begin_tx" => {
            if active_tx.is_some() {
                return err_bytes("transaction already active");
            }
            let tx_id = db.begin_transaction();
            *active_tx = Some(tx_id);
            ok_bytes(json!({ "tx_id": tx_id }))
        }

        "commit_tx" => match active_tx.take() {
            Some(tx_id) => match db.commit_transaction(tx_id) {
                Ok(()) => ok_bytes(json!("committed")),
                Err(e) => err_bytes(&e.to_string()),
            },
            None => err_bytes("no active transaction"),
        },

        "rollback_tx" => match active_tx.take() {
            Some(tx_id) => {
                let _ = db.rollback_transaction(tx_id);
                ok_bytes(json!("rolled back"))
            }
            None => err_bytes("no active transaction"),
        },

        // --- CRUD ---

        "insert" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let doc = match request.get_mut("doc").map(Value::take) {
                Some(d) if !d.is_null() => d,
                _ => return err_bytes("missing 'doc'"),
            };
            if let Some(tx_id) = *active_tx {
                match db.tx_insert(tx_id, col, doc) {
                    Ok(()) => ok_bytes(json!("buffered")),
                    Err(e) => err_bytes(&e.to_string()),
                }
            } else {
                match db.insert(col, doc) {
                    Ok(id) => ok_bytes(json!({ "id": id })),
                    Err(e) => err_bytes(&e.to_string()),
                }
            }
        }

        "insert_many" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let docs = match request.get_mut("docs").map(Value::take) {
                Some(Value::Array(arr)) => arr,
                _ => return err_bytes("missing or invalid 'docs' array"),
            };
            if let Some(tx_id) = *active_tx {
                for doc in docs {
                    if let Err(e) = db.tx_insert(tx_id, col, doc) {
                        return err_bytes(&e.to_string());
                    }
                }
                ok_bytes(json!("buffered"))
            } else {
                match db.insert_many(col, docs) {
                    Ok(ids) => ok_bytes(json!(ids)),
                    Err(e) => err_bytes(&e.to_string()),
                }
            }
        }

        "find" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let empty = json!({});
            let query = request.get("query").unwrap_or(&empty);
            if let Some(tx_id) = *active_tx {
                match db.tx_find(tx_id, col, query) {
                    Ok(docs) => ok_bytes(json!(docs)),
                    Err(e) => err_bytes(&e.to_string()),
                }
            } else {
                let opts = match parse_find_options(&request) {
                    Ok(o) => o,
                    Err(e) => return err_bytes(&e.to_string()),
                };
                match db.find_with_options_arcs(col, query, &opts) {
                    Ok(arcs) => ok_docs_bytes(&arcs),
                    Err(e) => err_bytes(&e.to_string()),
                }
            }
        }

        "find_one" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let empty = json!({});
            let query = request.get("query").unwrap_or(&empty);
            match db.find_one(col, query) {
                Ok(doc) => ok_bytes(json!(doc)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "update" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let query = match request.get("query") {
                Some(q) => q,
                None => return err_bytes("missing 'query'"),
            };
            let update = match request.get("update") {
                Some(u) => u,
                None => return err_bytes("missing 'update'"),
            };
            if let Some(tx_id) = *active_tx {
                match db.tx_update(tx_id, col, query, update) {
                    Ok(()) => ok_bytes(json!("buffered")),
                    Err(e) => err_bytes(&e.to_string()),
                }
            } else {
                match db.update(col, query, update) {
                    Ok(count) => ok_bytes(json!({ "modified": count })),
                    Err(e) => err_bytes(&e.to_string()),
                }
            }
        }

        "update_one" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let query = match request.get("query") {
                Some(q) => q,
                None => return err_bytes("missing 'query'"),
            };
            let update = match request.get("update") {
                Some(u) => u,
                None => return err_bytes("missing 'update'"),
            };
            match db.update_one(col, query, update) {
                Ok(count) => ok_bytes(json!({ "modified": count })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "delete" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let query = match request.get("query") {
                Some(q) => q,
                None => return err_bytes("missing 'query'"),
            };
            if let Some(tx_id) = *active_tx {
                match db.tx_delete(tx_id, col, query) {
                    Ok(()) => ok_bytes(json!("buffered")),
                    Err(e) => err_bytes(&e.to_string()),
                }
            } else {
                match db.delete(col, query) {
                    Ok(count) => ok_bytes(json!({ "deleted": count })),
                    Err(e) => err_bytes(&e.to_string()),
                }
            }
        }

        "delete_one" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let query = match request.get("query") {
                Some(q) => q,
                None => return err_bytes("missing 'query'"),
            };
            match db.delete_one(col, query) {
                Ok(count) => ok_bytes(json!({ "deleted": count })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "count" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let empty = json!({});
            let query = request.get("query").unwrap_or(&empty);
            match db.count(col, query) {
                Ok(n) => ok_bytes(json!({ "count": n })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // --- Indexes ---

        "create_index" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let field = match request.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return err_bytes("missing 'field'"),
            };
            match db.create_index(col, field) {
                Ok(()) => ok_bytes(json!("index created")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "create_unique_index" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let field = match request.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return err_bytes("missing 'field'"),
            };
            match db.create_unique_index(col, field) {
                Ok(()) => ok_bytes(json!("unique index created")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "create_composite_index" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let fields = match request.get("fields").and_then(|v| v.as_array()) {
                Some(arr) => {
                    let strs: Option<Vec<String>> =
                        arr.iter().map(|v| v.as_str().map(String::from)).collect();
                    match strs {
                        Some(s) => s,
                        None => return err_bytes("'fields' must be an array of strings"),
                    }
                }
                None => return err_bytes("missing 'fields' array"),
            };
            match db.create_composite_index(col, fields) {
                Ok(name) => ok_bytes(json!({ "index": name })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "create_text_index" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let fields = match request.get("fields").and_then(|v| v.as_array()) {
                Some(arr) => {
                    let strs: Option<Vec<String>> =
                        arr.iter().map(|v| v.as_str().map(String::from)).collect();
                    match strs {
                        Some(s) => s,
                        None => return err_bytes("'fields' must be an array of strings"),
                    }
                }
                None => return err_bytes("missing 'fields' array"),
            };
            match db.create_text_index(col, fields) {
                Ok(()) => ok_bytes(json!("text index created")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_indexes" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            match db.list_indexes(col) {
                Ok(indexes) => ok_bytes(json!(indexes)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "drop_index" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let index = match request.get("index").and_then(|v| v.as_str()) {
                Some(i) => i,
                None => return err_bytes("missing 'index'"),
            };
            match db.drop_index(col, index) {
                Ok(()) => ok_bytes(json!("index dropped")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "text_search" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let query = match request.get("query").and_then(|v| v.as_str()) {
                Some(q) => q,
                None => return err_bytes("missing 'query' string"),
            };
            let limit = request
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db.text_search(col, query, limit) {
                Ok(results) => ok_bytes(json!(results)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // --- Collections ---

        "create_collection" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            match db.create_collection(col) {
                Ok(()) => ok_bytes(json!("collection created")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_collections" => {
            let names = db.list_collections();
            ok_bytes(json!(names))
        }

        "drop_collection" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            match db.drop_collection(col) {
                Ok(()) => ok_bytes(json!("collection dropped")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "compact" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            match db.compact(col) {
                Ok(stats) => ok_bytes(json!({
                    "old_size": stats.old_size,
                    "new_size": stats.new_size,
                    "docs_kept": stats.docs_kept
                })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "aggregate" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let pipeline = match request.get("pipeline") {
                Some(p) => p,
                None => return err_bytes("missing 'pipeline'"),
            };
            match db.aggregate(col, pipeline) {
                Ok(docs) => ok_bytes(json!(docs)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // --- Blob storage + FTS ---

        "create_bucket" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            match db.create_bucket(bucket) {
                Ok(()) => ok_bytes(json!("bucket created")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_buckets" => {
            let buckets = db.list_buckets();
            ok_bytes(json!(buckets))
        }

        "delete_bucket" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            match db.delete_bucket(bucket) {
                Ok(()) => ok_bytes(json!("bucket deleted")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "put_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_bytes("missing 'key'"),
            };
            let data_b64 = match request.get("data").and_then(|v| v.as_str()) {
                Some(d) => d,
                None => return err_bytes("missing 'data' (base64)"),
            };
            let data = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
                Ok(d) => d,
                Err(e) => return err_bytes(&format!("invalid base64: {e}")),
            };
            let content_type = request
                .get("content_type")
                .and_then(|v| v.as_str())
                .unwrap_or("application/octet-stream");
            let metadata: HashMap<String, String> = request
                .get("metadata")
                .and_then(|v| v.as_object())
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            match db.put_object(bucket, key, &data, content_type, metadata) {
                Ok(meta) => ok_bytes(meta),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "get_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_bytes("missing 'key'"),
            };
            match db.get_object(bucket, key) {
                Ok((data, meta)) => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
                    ok_bytes(json!({
                        "content": encoded,
                        "metadata": meta,
                    }))
                }
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "head_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_bytes("missing 'key'"),
            };
            match db.head_object(bucket, key) {
                Ok(meta) => ok_bytes(meta),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "delete_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_bytes("missing 'key'"),
            };
            match db.delete_object(bucket, key) {
                Ok(()) => ok_bytes(json!("object deleted")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_objects" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            let prefix = request.get("prefix").and_then(|v| v.as_str());
            let limit = request
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize);
            match db.list_objects(bucket, prefix, limit) {
                Ok(list) => ok_bytes(json!(list)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "search" => {
            let query = match request.get("query").and_then(|v| v.as_str()) {
                Some(q) => q,
                None => return err_bytes("missing 'query'"),
            };
            let bucket = request.get("bucket").and_then(|v| v.as_str());
            let limit = request
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db.search(bucket, query, limit) {
                Ok(results) => ok_bytes(json!(results)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        _ => err_bytes(&format!("unknown command: {cmd}")),
    }
}

// ---------------------------------------------------------------------------
// C FFI entry points
// ---------------------------------------------------------------------------

unsafe fn cstr_to_str<'a>(s: *const c_char) -> Option<&'a str> {
    if s.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(s) }.to_str().ok()
}

fn result_to_cstring(bytes: Vec<u8>) -> *mut c_char {
    match String::from_utf8(bytes) {
        Ok(s) => match CString::new(s) {
            Ok(cs) => cs.into_raw(),
            Err(_) => ptr::null_mut(),
        },
        Err(_) => ptr::null_mut(),
    }
}

/// Open a database at the given directory path. Returns an opaque handle, or NULL on error.
///
/// # Safety
/// `path` must be a valid null-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_open(path: *const c_char) -> *mut Handle {
    let path_str = match unsafe { cstr_to_str(path) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    match OxiDb::open(Path::new(path_str)) {
        Ok(db) => {
            let handle = Box::new(OxiDbHandle {
                db: Arc::new(db),
                active_tx: Mutex::new(None),
            });
            Box::into_raw(handle) as *mut Handle
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Open a database with AES-GCM encryption. `key_path` points to a 32-byte key file.
/// Returns an opaque handle, or NULL on error.
///
/// # Safety
/// `path` and `key_path` must be valid null-terminated C strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_open_encrypted(
    path: *const c_char,
    key_path: *const c_char,
) -> *mut Handle {
    let path_str = match unsafe { cstr_to_str(path) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let key_path_str = match unsafe { cstr_to_str(key_path) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let encryption_key = match oxidb::EncryptionKey::load_from_file(Path::new(key_path_str)) {
        Ok(k) => k,
        Err(_) => return ptr::null_mut(),
    };
    match OxiDb::open_with_options(Path::new(path_str), Some(encryption_key)) {
        Ok(db) => {
            let handle = Box::new(OxiDbHandle {
                db: Arc::new(db),
                active_tx: Mutex::new(None),
            });
            Box::into_raw(handle) as *mut Handle
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Close the database and free the handle. Safe to call with NULL.
///
/// # Safety
/// `handle` must be a pointer returned by `oxidb_open` / `oxidb_open_encrypted`, or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_close(handle: *mut Handle) {
    if !handle.is_null() {
        let _ = unsafe { Box::from_raw(handle as *mut OxiDbHandle) };
    }
}

/// Execute a JSON command against the database. Returns a JSON response string
/// (caller must free with `oxidb_free_string`). Returns NULL only on invalid input.
///
/// The JSON command format is identical to the OxiDB server protocol.
///
/// # Safety
/// `handle` must be a valid handle from `oxidb_open`. `cmd_json` must be a valid
/// null-terminated C string containing JSON.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_execute(
    handle: *mut Handle,
    cmd_json: *const c_char,
) -> *mut c_char {
    if handle.is_null() {
        return result_to_cstring(err_bytes("null handle"));
    }
    let h = unsafe { &*(handle as *mut OxiDbHandle) };

    let cmd_str = match unsafe { cstr_to_str(cmd_json) } {
        Some(s) => s,
        None => return result_to_cstring(err_bytes("invalid command string")),
    };

    let request: Value = match serde_json::from_str(cmd_str) {
        Ok(v) => v,
        Err(e) => return result_to_cstring(err_bytes(&format!("invalid JSON: {e}"))),
    };

    let mut active_tx = h.active_tx.lock().unwrap();
    let response = handle_request(&h.db, request, &mut active_tx);
    result_to_cstring(response)
}

/// Free a string returned by `oxidb_execute`. Safe to call with NULL.
///
/// # Safety
/// `ptr` must be a pointer returned by `oxidb_execute`, or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = unsafe { CString::from_raw(ptr) };
    }
}
