use oxidb::OxiDb;
use oxidb::query::parse_find_options;
use serde_json::{Value, json};
use std::sync::Arc;

fn ok_response(data: Value) -> Value {
    json!({ "ok": true, "data": data })
}

fn err_response(msg: &str) -> Value {
    json!({ "ok": false, "error": msg })
}

/// Handle a single JSON request and return a JSON response.
pub fn handle_request(db: &Arc<OxiDb>, request: &Value) -> Value {
    let cmd = match request.get("cmd").and_then(|v| v.as_str()) {
        Some(c) => c,
        None => return err_response("missing or invalid 'cmd' field"),
    };

    let collection = request.get("collection").and_then(|v| v.as_str());

    match cmd {
        "ping" => ok_response(json!("pong")),

        "insert" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let doc = match request.get("doc") {
                Some(d) => d.clone(),
                None => return err_response("missing 'doc'"),
            };
            match db.insert(col, doc) {
                Ok(id) => ok_response(json!({ "id": id })),
                Err(e) => err_response(&e.to_string()),
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
            match db.insert_many(col, docs) {
                Ok(ids) => ok_response(json!(ids)),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "find" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            let empty = json!({});
            let query = request.get("query").unwrap_or(&empty);
            let opts = match parse_find_options(request) {
                Ok(o) => o,
                Err(e) => return err_response(&e.to_string()),
            };
            match db.find_with_options(col, query, &opts) {
                Ok(docs) => ok_response(json!(docs)),
                Err(e) => err_response(&e.to_string()),
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
            match db.update(col, query, update) {
                Ok(count) => ok_response(json!({ "modified": count })),
                Err(e) => err_response(&e.to_string()),
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
            match db.delete(col, query) {
                Ok(count) => ok_response(json!({ "deleted": count })),
                Err(e) => err_response(&e.to_string()),
            }
        }

        "count" => {
            let col = match collection {
                Some(c) => c,
                None => return err_response("missing 'collection'"),
            };
            match db.find(col, &json!({})) {
                Ok(docs) => ok_response(json!({ "count": docs.len() })),
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

        _ => err_response(&format!("unknown command: {cmd}")),
    }
}
