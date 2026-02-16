use base64::Engine;
use oxidb::OxiDb;
use oxidb::query::parse_find_options;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

fn ok_response(data: Value) -> Value {
    json!({ "ok": true, "data": data })
}

fn err_response(msg: &str) -> Value {
    json!({ "ok": false, "error": msg })
}

/// Handle a single JSON request and return a JSON response.
pub fn handle_request(db: &Arc<OxiDb>, request: &Value, active_tx: &mut Option<u64>) -> Value {
    let cmd = match request.get("cmd").and_then(|v| v.as_str()) {
        Some(c) => c,
        None => return err_response("missing or invalid 'cmd' field"),
    };

    let collection = request.get("collection").and_then(|v| v.as_str());

    match cmd {
        "ping" => ok_response(json!("pong")),

        // -------------------------------------------------------------------
        // Transaction commands
        // -------------------------------------------------------------------

        "begin_tx" => {
            if active_tx.is_some() {
                return err_response("transaction already active");
            }
            let tx_id = db.begin_transaction();
            *active_tx = Some(tx_id);
            ok_response(json!({ "tx_id": tx_id }))
        }

        "commit_tx" => {
            match active_tx.take() {
                Some(tx_id) => match db.commit_transaction(tx_id) {
                    Ok(()) => ok_response(json!("committed")),
                    Err(e) => err_response(&e.to_string()),
                },
                None => err_response("no active transaction"),
            }
        }

        "rollback_tx" => {
            match active_tx.take() {
                Some(tx_id) => {
                    let _ = db.rollback_transaction(tx_id);
                    ok_response(json!("rolled back"))
                }
                None => err_response("no active transaction"),
            }
        }

        // -------------------------------------------------------------------
        // CRUD commands (tx-aware)
        // -------------------------------------------------------------------

        "insert" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let doc = match request.get("doc") {
                Some(d) => d.clone(),
                None => return err_response("missing 'doc'"),
            };
            if let Some(tx_id) = *active_tx {
                match db.tx_insert(tx_id, col, doc) {
                    Ok(()) => ok_response(json!("buffered")),
                    Err(e) => err_response(&e.to_string()),
                }
            } else {
                match db.insert(col, doc) {
                    Ok(id) => ok_response(json!({ "id": id })),
                    Err(e) => err_response(&e.to_string()),
                }
            }
        }

        "insert_many" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let docs = match request.get("docs").and_then(|v| v.as_array()) {
                Some(arr) => arr.clone(),
                None => return err_response("missing or invalid 'docs' array"),
            };
            if let Some(tx_id) = *active_tx {
                for doc in docs {
                    if let Err(e) = db.tx_insert(tx_id, col, doc) {
                        return err_response(&e.to_string());
                    }
                }
                ok_response(json!("buffered"))
            } else {
                match db.insert_many(col, docs) {
                    Ok(ids) => ok_response(json!(ids)),
                    Err(e) => err_response(&e.to_string()),
                }
            }
        }

        "find" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let empty = json!({});
            let query = request.get("query").unwrap_or(&empty);
            if let Some(tx_id) = *active_tx {
                match db.tx_find(tx_id, col, query) {
                    Ok(docs) => ok_response(json!(docs)),
                    Err(e) => err_response(&e.to_string()),
                }
            } else {
                let opts = match parse_find_options(request) {
                    Ok(o) => o,
                    Err(e) => return err_response(&e.to_string()),
                };
                match db.find_with_options(col, query, &opts) {
                    Ok(docs) => ok_response(json!(docs)),
                    Err(e) => err_response(&e.to_string()),
                }
            }
        }

        "find_one" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let empty = json!({});
            let query = request.get("query").unwrap_or(&empty);
            match db.find_one(col, query) {
                Ok(doc) => ok_response(json!(doc)),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "update" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let query = match request.get("query") {
                Some(q) => q,
                None => return err_response("missing 'query'"),
            };
            let update = match request.get("update") {
                Some(u) => u,
                None => return err_response("missing 'update'"),
            };
            if let Some(tx_id) = *active_tx {
                match db.tx_update(tx_id, col, query, update) {
                    Ok(()) => ok_response(json!("buffered")),
                    Err(e) => err_response(&e.to_string()),
                }
            } else {
                match db.update(col, query, update) {
                    Ok(count) => ok_response(json!({ "modified": count })),
                    Err(e) => err_response(&e.to_string()),
                }
            }
        }

        "delete" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let query = match request.get("query") {
                Some(q) => q,
                None => return err_response("missing 'query'"),
            };
            if let Some(tx_id) = *active_tx {
                match db.tx_delete(tx_id, col, query) {
                    Ok(()) => ok_response(json!("buffered")),
                    Err(e) => err_response(&e.to_string()),
                }
            } else {
                match db.delete(col, query) {
                    Ok(count) => ok_response(json!({ "deleted": count })),
                    Err(e) => err_response(&e.to_string()),
                }
            }
        }

        "count" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let empty = json!({});
            let query = request.get("query").unwrap_or(&empty);
            match db.count(col, query) {
                Ok(n) => ok_response(json!({ "count": n })),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "create_index" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let field = match request.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return err_response("missing 'field'"),
            };
            match db.create_index(col, field) {
                Ok(()) => ok_response(json!("index created")),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "create_unique_index" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let field = match request.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return err_response("missing 'field'"),
            };
            match db.create_unique_index(col, field) {
                Ok(()) => ok_response(json!("unique index created")),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "create_composite_index" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let fields = match request.get("fields").and_then(|v| v.as_array()) {
                Some(arr) => {
                    let strs: Option<Vec<String>> =
                        arr.iter().map(|v| v.as_str().map(String::from)).collect();
                    match strs {
                        Some(s) => s,
                        None => return err_response("'fields' must be an array of strings"),
                    }
                }
                None => return err_response("missing 'fields' array"),
            };
            match db.create_composite_index(col, fields) {
                Ok(name) => ok_response(json!({ "index": name })),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "create_collection" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            match db.create_collection(col) {
                Ok(()) => ok_response(json!("collection created")),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "list_collections" => {
            let names = db.list_collections();
            ok_response(json!(names))
        }

        "drop_collection" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            match db.drop_collection(col) {
                Ok(()) => ok_response(json!("collection dropped")),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "compact" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            match db.compact(col) {
                Ok(stats) => ok_response(json!({
                    "old_size": stats.old_size,
                    "new_size": stats.new_size,
                    "docs_kept": stats.docs_kept
                })),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "aggregate" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let pipeline = match request.get("pipeline") {
                Some(p) => p,
                None => return err_response("missing 'pipeline'"),
            };
            match db.aggregate(col, pipeline) {
                Ok(docs) => ok_response(json!(docs)),
                Err(e) => err_response(&e.to_string()),
            }
        }

        // -------------------------------------------------------------------
        // Blob storage + FTS commands
        // -------------------------------------------------------------------

        "create_bucket" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_response("missing 'bucket'"),
            };
            match db.create_bucket(bucket) {
                Ok(()) => ok_response(json!("bucket created")),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "list_buckets" => {
            let buckets = db.list_buckets();
            ok_response(json!(buckets))
        }

        "delete_bucket" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_response("missing 'bucket'"),
            };
            match db.delete_bucket(bucket) {
                Ok(()) => ok_response(json!("bucket deleted")),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "put_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_response("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_response("missing 'key'"),
            };
            let data_b64 = match request.get("data").and_then(|v| v.as_str()) {
                Some(d) => d,
                None => return err_response("missing 'data' (base64)"),
            };
            let data = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
                Ok(d) => d,
                Err(e) => return err_response(&format!("invalid base64: {e}")),
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
                Ok(meta) => ok_response(meta),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "get_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_response("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_response("missing 'key'"),
            };
            match db.get_object(bucket, key) {
                Ok((data, meta)) => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
                    ok_response(json!({
                        "content": encoded,
                        "metadata": meta,
                    }))
                }
                Err(e) => err_response(&e.to_string()),
            }
        }

        "head_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_response("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_response("missing 'key'"),
            };
            match db.head_object(bucket, key) {
                Ok(meta) => ok_response(meta),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "delete_object" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_response("missing 'bucket'"),
            };
            let key = match request.get("key").and_then(|v| v.as_str()) {
                Some(k) => k,
                None => return err_response("missing 'key'"),
            };
            match db.delete_object(bucket, key) {
                Ok(()) => ok_response(json!("object deleted")),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "list_objects" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_response("missing 'bucket'"),
            };
            let prefix = request.get("prefix").and_then(|v| v.as_str());
            let limit = request
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize);
            match db.list_objects(bucket, prefix, limit) {
                Ok(list) => ok_response(json!(list)),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "search" => {
            let query = match request.get("query").and_then(|v| v.as_str()) {
                Some(q) => q,
                None => return err_response("missing 'query'"),
            };
            let bucket = request.get("bucket").and_then(|v| v.as_str());
            let limit = request
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            match db.search(bucket, query, limit) {
                Ok(results) => ok_response(json!(results)),
                Err(e) => err_response(&e.to_string()),
            }
        }

        _ => err_response(&format!("unknown command: {cmd}")),
    }
}
