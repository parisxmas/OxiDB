mod connection;

use connection::OxiDbConnection;
use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;

type OxiDbConn = c_void;

/// Helper: send a JSON request, return the response as a C string (or NULL on error).
unsafe fn send_request(conn: *mut OxiDbConn, json: &serde_json::Value) -> *mut c_char {
    if conn.is_null() {
        return ptr::null_mut();
    }
    let conn = unsafe { &mut *(conn as *mut OxiDbConnection) };
    let payload = json.to_string();

    match conn.request(payload.as_bytes()) {
        Ok(resp) => match CString::new(resp) {
            Ok(cs) => cs.into_raw(),
            Err(e) => {
                conn.set_last_error(format!("response contains null byte: {e}"));
                ptr::null_mut()
            }
        },
        Err(e) => {
            conn.set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

/// Helper: convert a C string pointer to a &str, returning None if null or invalid UTF-8.
unsafe fn cstr_to_str<'a>(s: *const c_char) -> Option<&'a str> {
    if s.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(s) }.to_str().ok()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_connect(host: *const c_char, port: u16) -> *mut OxiDbConn {
    let host_str = match unsafe { cstr_to_str(host) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };

    match OxiDbConnection::connect(host_str, port) {
        Ok(conn) => Box::into_raw(Box::new(conn)) as *mut OxiDbConn,
        Err(_) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_disconnect(conn: *mut OxiDbConn) {
    if !conn.is_null() {
        let _ = unsafe { Box::from_raw(conn as *mut OxiDbConnection) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_ping(conn: *mut OxiDbConn) -> *mut c_char {
    let req = serde_json::json!({"cmd": "ping"});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_insert(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    doc_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let doc_str = match unsafe { cstr_to_str(doc_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let doc: serde_json::Value = match serde_json::from_str(doc_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "insert", "collection": col, "doc": doc});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_insert_many(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    docs_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let docs_str = match unsafe { cstr_to_str(docs_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let docs: serde_json::Value = match serde_json::from_str(docs_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "insert_many", "collection": col, "docs": docs});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_find(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    query_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query_str = match unsafe { cstr_to_str(query_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query: serde_json::Value = match serde_json::from_str(query_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "find", "collection": col, "query": query});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_find_one(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    query_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query_str = match unsafe { cstr_to_str(query_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query: serde_json::Value = match serde_json::from_str(query_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "find_one", "collection": col, "query": query});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_update(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    query_json: *const c_char,
    update_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query_str = match unsafe { cstr_to_str(query_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let update_str = match unsafe { cstr_to_str(update_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query: serde_json::Value = match serde_json::from_str(query_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let update: serde_json::Value = match serde_json::from_str(update_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req =
        serde_json::json!({"cmd": "update", "collection": col, "query": query, "update": update});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_update_one(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    query_json: *const c_char,
    update_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query_str = match unsafe { cstr_to_str(query_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let update_str = match unsafe { cstr_to_str(update_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query: serde_json::Value = match serde_json::from_str(query_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let update: serde_json::Value = match serde_json::from_str(update_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req =
        serde_json::json!({"cmd": "update_one", "collection": col, "query": query, "update": update});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_delete(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    query_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query_str = match unsafe { cstr_to_str(query_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query: serde_json::Value = match serde_json::from_str(query_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "delete", "collection": col, "query": query});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_delete_one(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    query_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query_str = match unsafe { cstr_to_str(query_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let query: serde_json::Value = match serde_json::from_str(query_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "delete_one", "collection": col, "query": query});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_count(
    conn: *mut OxiDbConn,
    collection: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "count", "collection": col});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_compact(
    conn: *mut OxiDbConn,
    collection: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "compact", "collection": col});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_index(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    field: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fld = match unsafe { cstr_to_str(field) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "create_index", "collection": col, "field": fld});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_unique_index(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    field: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fld = match unsafe { cstr_to_str(field) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "create_unique_index", "collection": col, "field": fld});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_composite_index(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    fields_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fields_str = match unsafe { cstr_to_str(fields_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fields: serde_json::Value = match serde_json::from_str(fields_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req =
        serde_json::json!({"cmd": "create_composite_index", "collection": col, "fields": fields});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_text_index(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    fields_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fields_str = match unsafe { cstr_to_str(fields_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fields: serde_json::Value = match serde_json::from_str(fields_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req =
        serde_json::json!({"cmd": "create_text_index", "collection": col, "fields": fields});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_list_indexes(
    conn: *mut OxiDbConn,
    collection: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "list_indexes", "collection": col});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_drop_index(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    index: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let idx = match unsafe { cstr_to_str(index) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "drop_index", "collection": col, "index": idx});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_text_search(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    query: *const c_char,
    limit: i32,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let q = match unsafe { cstr_to_str(query) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let mut req = serde_json::json!({"cmd": "text_search", "collection": col, "query": q});
    if limit > 0 {
        req["limit"] = serde_json::json!(limit);
    }
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_list_collections(conn: *mut OxiDbConn) -> *mut c_char {
    let req = serde_json::json!({"cmd": "list_collections"});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_collection(
    conn: *mut OxiDbConn,
    collection: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "create_collection", "collection": col});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_drop_collection(
    conn: *mut OxiDbConn,
    collection: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "drop_collection", "collection": col});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_aggregate(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    pipeline_json: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let pipeline_str = match unsafe { cstr_to_str(pipeline_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let pipeline: serde_json::Value = match serde_json::from_str(pipeline_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let req =
        serde_json::json!({"cmd": "aggregate", "collection": col, "pipeline": pipeline});
    unsafe { send_request(conn, &req) }
}

// ---------------------------------------------------------------------------
// Blob storage + FTS
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_bucket(
    conn: *mut OxiDbConn,
    bucket: *const c_char,
) -> *mut c_char {
    let b = match unsafe { cstr_to_str(bucket) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "create_bucket", "bucket": b});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_list_buckets(conn: *mut OxiDbConn) -> *mut c_char {
    let req = serde_json::json!({"cmd": "list_buckets"});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_delete_bucket(
    conn: *mut OxiDbConn,
    bucket: *const c_char,
) -> *mut c_char {
    let b = match unsafe { cstr_to_str(bucket) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "delete_bucket", "bucket": b});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_put_object(
    conn: *mut OxiDbConn,
    bucket: *const c_char,
    key: *const c_char,
    data_b64: *const c_char,
    content_type: *const c_char,
    metadata_json: *const c_char,
) -> *mut c_char {
    let b = match unsafe { cstr_to_str(bucket) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let k = match unsafe { cstr_to_str(key) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let d = match unsafe { cstr_to_str(data_b64) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let ct = unsafe { cstr_to_str(content_type) }.unwrap_or("application/octet-stream");
    let meta: serde_json::Value = unsafe { cstr_to_str(metadata_json) }
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(serde_json::json!({}));

    let req = serde_json::json!({
        "cmd": "put_object",
        "bucket": b,
        "key": k,
        "data": d,
        "content_type": ct,
        "metadata": meta,
    });
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_get_object(
    conn: *mut OxiDbConn,
    bucket: *const c_char,
    key: *const c_char,
) -> *mut c_char {
    let b = match unsafe { cstr_to_str(bucket) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let k = match unsafe { cstr_to_str(key) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "get_object", "bucket": b, "key": k});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_head_object(
    conn: *mut OxiDbConn,
    bucket: *const c_char,
    key: *const c_char,
) -> *mut c_char {
    let b = match unsafe { cstr_to_str(bucket) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let k = match unsafe { cstr_to_str(key) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "head_object", "bucket": b, "key": k});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_delete_object(
    conn: *mut OxiDbConn,
    bucket: *const c_char,
    key: *const c_char,
) -> *mut c_char {
    let b = match unsafe { cstr_to_str(bucket) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let k = match unsafe { cstr_to_str(key) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "delete_object", "bucket": b, "key": k});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_list_objects(
    conn: *mut OxiDbConn,
    bucket: *const c_char,
    prefix: *const c_char,
    limit: i32,
) -> *mut c_char {
    let b = match unsafe { cstr_to_str(bucket) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let mut req = serde_json::json!({"cmd": "list_objects", "bucket": b});
    if let Some(p) = unsafe { cstr_to_str(prefix) } {
        req["prefix"] = serde_json::json!(p);
    }
    if limit > 0 {
        req["limit"] = serde_json::json!(limit);
    }
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_search(
    conn: *mut OxiDbConn,
    query: *const c_char,
    bucket: *const c_char,
    limit: i32,
) -> *mut c_char {
    let q = match unsafe { cstr_to_str(query) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let mut req = serde_json::json!({"cmd": "search", "query": q});
    if let Some(b) = unsafe { cstr_to_str(bucket) } {
        req["bucket"] = serde_json::json!(b);
    }
    if limit > 0 {
        req["limit"] = serde_json::json!(limit);
    }
    unsafe { send_request(conn, &req) }
}

// ---------------------------------------------------------------------------
// Transaction commands
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_begin_tx(conn: *mut OxiDbConn) -> *mut c_char {
    let req = serde_json::json!({"cmd": "begin_tx"});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_commit_tx(conn: *mut OxiDbConn) -> *mut c_char {
    let req = serde_json::json!({"cmd": "commit_tx"});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_rollback_tx(conn: *mut OxiDbConn) -> *mut c_char {
    let req = serde_json::json!({"cmd": "rollback_tx"});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_sql(
    conn: *mut OxiDbConn,
    query: *const c_char,
) -> *mut c_char {
    let q = match unsafe { cstr_to_str(query) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "sql", "query": q});
    unsafe { send_request(conn, &req) }
}

// ---------------------------------------------------------------------------
// Cron scheduler
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_schedule(
    conn: *mut OxiDbConn,
    schedule_json: *const c_char,
) -> *mut c_char {
    let json_str = match unsafe { cstr_to_str(schedule_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let mut def: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    def["cmd"] = serde_json::json!("create_schedule");
    unsafe { send_request(conn, &def) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_list_schedules(conn: *mut OxiDbConn) -> *mut c_char {
    let req = serde_json::json!({"cmd": "list_schedules"});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_get_schedule(
    conn: *mut OxiDbConn,
    name: *const c_char,
) -> *mut c_char {
    let n = match unsafe { cstr_to_str(name) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "get_schedule", "name": n});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_delete_schedule(
    conn: *mut OxiDbConn,
    name: *const c_char,
) -> *mut c_char {
    let n = match unsafe { cstr_to_str(name) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "delete_schedule", "name": n});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_enable_schedule(
    conn: *mut OxiDbConn,
    name: *const c_char,
) -> *mut c_char {
    let n = match unsafe { cstr_to_str(name) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "enable_schedule", "name": n});
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_disable_schedule(
    conn: *mut OxiDbConn,
    name: *const c_char,
) -> *mut c_char {
    let n = match unsafe { cstr_to_str(name) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let req = serde_json::json!({"cmd": "disable_schedule", "name": n});
    unsafe { send_request(conn, &req) }
}

// ---------------------------------------------------------------------------
// Vector index
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_create_vector_index(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    field: *const c_char,
    dimension: i32,
    metric: *const c_char,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fld = match unsafe { cstr_to_str(field) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let m = unsafe { cstr_to_str(metric) }.unwrap_or("cosine");
    let req = serde_json::json!({
        "cmd": "create_vector_index",
        "collection": col,
        "field": fld,
        "dimension": dimension,
        "metric": m,
    });
    unsafe { send_request(conn, &req) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_vector_search(
    conn: *mut OxiDbConn,
    collection: *const c_char,
    field: *const c_char,
    vector_json: *const c_char,
    limit: i32,
) -> *mut c_char {
    let col = match unsafe { cstr_to_str(collection) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let fld = match unsafe { cstr_to_str(field) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let vec_str = match unsafe { cstr_to_str(vector_json) } {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let vector: serde_json::Value = match serde_json::from_str(vec_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let mut req = serde_json::json!({
        "cmd": "vector_search",
        "collection": col,
        "field": fld,
        "vector": vector,
    });
    if limit > 0 {
        req["limit"] = serde_json::json!(limit);
    }
    unsafe { send_request(conn, &req) }
}

/// Free a string returned by any `oxidb_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn oxidb_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = unsafe { CString::from_raw(ptr) };
    }
}
