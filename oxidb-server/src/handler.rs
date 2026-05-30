use base64::Engine;
use oxidb::OxiDb;
use oxidb::query::parse_find_options;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::auth::{Role, UserStore};
use crate::protocol::{self, WireFormat, value_to_msgpack};

/// Serialize a single document to MsgPack.
fn doc_to_msgpack(doc: &Arc<Value>, buf: &mut Vec<u8>) {
    value_to_msgpack(doc.as_ref(), buf);
}

/// Serialize a single document to JSON.
fn doc_to_json(doc: &Arc<Value>, buf: &mut Vec<u8>) {
    serde_json::to_writer(&mut *buf, doc.as_ref()).unwrap();
}

pub fn ok_bytes(data: Value) -> Vec<u8> {
    match protocol::wire_format() {
        WireFormat::Json => serde_json::to_vec(&json!({ "ok": true, "data": data })).unwrap(),
        WireFormat::MsgPack => {
            let mut buf = Vec::with_capacity(256);
            rmp::encode::write_map_len(&mut buf, 2).unwrap();
            rmp::encode::write_str(&mut buf, "ok").unwrap();
            rmp::encode::write_bool(&mut buf, true).unwrap();
            rmp::encode::write_str(&mut buf, "data").unwrap();
            value_to_msgpack(&data, &mut buf);
            buf
        }
        WireFormat::OxiWire => crate::oxiwire::ok_response(&data),
    }
}

pub fn err_bytes(msg: &str) -> Vec<u8> {
    match protocol::wire_format() {
        WireFormat::Json => serde_json::to_vec(&json!({ "ok": false, "error": msg })).unwrap(),
        WireFormat::MsgPack => {
            let mut buf = Vec::with_capacity(64 + msg.len());
            rmp::encode::write_map_len(&mut buf, 2).unwrap();
            rmp::encode::write_str(&mut buf, "ok").unwrap();
            rmp::encode::write_bool(&mut buf, false).unwrap();
            rmp::encode::write_str(&mut buf, "error").unwrap();
            rmp::encode::write_str(&mut buf, msg).unwrap();
            buf
        }
        WireFormat::OxiWire => crate::oxiwire::err_response(msg),
    }
}

/// Serialize find results directly from Arc references — zero Value::clone.
/// Uses a per-thread wire cache so hot documents are never re-serialized.
/// On first query, each doc is serialized once; on subsequent queries the
/// pre-serialized bytes are memcpy'd directly into the response buffer.
fn ok_docs_bytes(docs: &[Arc<Value>]) -> Vec<u8> {
    match protocol::wire_format() {
        WireFormat::Json => {
            let mut buf = Vec::with_capacity(docs.len() * 200 + 64);
            buf.extend_from_slice(b"{\"ok\":true,\"data\":[");
            for (i, doc) in docs.iter().enumerate() {
                if i > 0 { buf.push(b','); }
                doc_to_json(doc, &mut buf);
            }
            buf.extend_from_slice(b"]}");
            buf
        }
        WireFormat::MsgPack => {
            let mut buf = Vec::with_capacity(docs.len() * 150 + 64);
            rmp::encode::write_map_len(&mut buf, 2).unwrap();
            rmp::encode::write_str(&mut buf, "ok").unwrap();
            rmp::encode::write_bool(&mut buf, true).unwrap();
            rmp::encode::write_str(&mut buf, "data").unwrap();
            rmp::encode::write_array_len(&mut buf, docs.len() as u32).unwrap();
            for doc in docs {
                doc_to_msgpack(doc, &mut buf);
            }
            buf
        }
        WireFormat::OxiWire => crate::oxiwire::ok_docs_response_fast(docs),
    }
}

/// Handle a single JSON request and return pre-serialized JSON response bytes.
pub fn handle_request(db: &Arc<OxiDb>, request: Value, active_tx: &mut Option<u64>) -> Vec<u8> {
    let cmd = match request.get("cmd").and_then(|v| v.as_str().map(|s| s.to_string())) {
        Some(c) => c,
        None => return err_bytes("missing or invalid 'cmd' field"),
    };

    let collection: Option<String> = request.get("collection").and_then(|v| v.as_str().map(|s| s.to_string()));

    // Take ownership of mutable request for extracting fields without cloning
    let mut request = request;

    // FDW v1: if the targeted collection is registered as a linked
    // collection (see oxidb::links), route to the remote OxiDB
    // instead of the local engine. Read commands are proxied; write
    // commands are refused with an explicit "read-only" error so the
    // caller knows the failure is policy, not a transient error.
    //
    // The link-management commands themselves (link_collection /
    // unlink_collection / list_links) are handled below in their own
    // match arms — they DO carry a `collection` field but it names
    // the link being registered, not a collection to query. We skip
    // the proxy check for them.
    if let Some(ref col) = collection {
        if !is_link_management_cmd(&cmd) {
            if let Some(link) = db.lookup_link(col) {
                return handle_linked_command(&cmd, &link, request);
            }
        }
    }

    match cmd.as_str() {
        "ping" => ok_bytes(json!("pong")),

        // -------------------------------------------------------------------
        // Linked collections (FDW v1)
        // -------------------------------------------------------------------

        "link_collection" => {
            let name = match collection.as_deref() {
                Some(c) if !c.is_empty() => c,
                _ => return err_bytes("missing 'collection' (the local link name)"),
            };
            let url = match request.get("url").and_then(|v| v.as_str()) {
                Some(u) if !u.is_empty() => u,
                _ => return err_bytes("missing 'url'"),
            };
            match db.link_collection(name, url) {
                Ok(cfg) => ok_bytes(json!(cfg)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }
        "unlink_collection" => {
            let name = match collection.as_deref() {
                Some(c) if !c.is_empty() => c,
                _ => return err_bytes("missing 'collection'"),
            };
            match db.unlink_collection(name) {
                Ok(true) => ok_bytes(json!({"unlinked": name})),
                Ok(false) => err_bytes(&format!("no such linked collection {:?}", name)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }
        "list_links" => ok_bytes(json!(db.list_links())),

        // -------------------------------------------------------------------
        // Transaction commands
        // -------------------------------------------------------------------

        "begin_tx" => {
            if active_tx.is_some() {
                return err_bytes("transaction already active");
            }
            let tx_id = db.begin_transaction();
            *active_tx = Some(tx_id);
            ok_bytes(json!({ "tx_id": tx_id }))
        }

        "commit_tx" => {
            match active_tx.take() {
                Some(tx_id) => match db.commit_transaction(tx_id) {
                    Ok(()) => ok_bytes(json!("committed")),
                    Err(e) => err_bytes(&e.to_string()),
                },
                None => err_bytes("no active transaction"),
            }
        }

        "rollback_tx" => {
            match active_tx.take() {
                Some(tx_id) => {
                    let _ = db.rollback_transaction(tx_id);
                    ok_bytes(json!("rolled back"))
                }
                None => err_bytes("no active transaction"),
            }
        }

        // -------------------------------------------------------------------
        // CRUD commands (tx-aware)
        // -------------------------------------------------------------------

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
                    Ok(id) => ok_bytes(json!({ "id": id })),
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
                let mut ids = Vec::with_capacity(docs.len());
                for doc in docs {
                    match db.tx_insert(tx_id, col, doc) {
                        Ok(id) => ids.push(id),
                        Err(e) => return err_bytes(&e.to_string()),
                    }
                }
                ok_bytes(json!(ids))
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
                // Bytes-first fast path: when the wire format is OxiWire AND
                // the query is fully index-satisfiable, skip the Value
                // materialization entirely. The engine streams pre-encoded
                // OxiWire bytes via the doc_bytes_cache + jsonb_oxiwire
                // converter (no JSONB→Value→encode round-trip). Falls back
                // to the Value path for any query the fast path can't
                // handle (post-filters, sorts, projections).
                if protocol::wire_format() == WireFormat::OxiWire {
                    if let Some(result) = db.find_oxiwire_bytes(col, query, &opts) {
                        match result {
                            Ok(byte_arcs) => return crate::oxiwire::ok_docs_bytes_response(&byte_arcs),
                            Err(e) => return err_bytes(&e.to_string()),
                        }
                    }
                    // Not index-satisfiable: byte-level post-filter encoding,
                    // avoiding a Vec<Arc<Value>> for large unindexed results.
                    if let Some(result) = db.find_oxiwire_postfilter(col, query, &opts) {
                        match result {
                            Ok((count, doc_bytes)) => {
                                return crate::oxiwire::ok_docs_concat_response(count, &doc_bytes)
                            }
                            Err(e) => return err_bytes(&e.to_string()),
                        }
                    }
                }
                // Fallback: existing zero-copy Arc<Value> path.
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
            // Inside a tx, route through tx_find so the read version
            // is recorded for OCC validation. Without this, a
            // read-then-write pattern (e.g. quota reserve: read row,
            // check limit, increment) inside a tx would skip the
            // read-set check at commit, opening a lost-update race.
            if let Some(tx_id) = *active_tx {
                match db.tx_find(tx_id, col, query) {
                    Ok(mut docs) => {
                        let first = if docs.is_empty() {
                            Value::Null
                        } else {
                            docs.remove(0)
                        };
                        ok_bytes(first)
                    }
                    Err(e) => err_bytes(&e.to_string()),
                }
            } else {
                match db.find_one(col, query) {
                    Ok(doc) => ok_bytes(json!(doc)),
                    Err(e) => err_bytes(&e.to_string()),
                }
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
            if let Some(tx_id) = *active_tx {
                match db.tx_update(tx_id, col, query, update) {
                    Ok(()) => ok_bytes(json!("buffered")),
                    Err(e) => err_bytes(&e.to_string()),
                }
            } else {
                match db.update_one(col, query, update) {
                    Ok(count) => ok_bytes(json!({ "modified": count })),
                    Err(e) => err_bytes(&e.to_string()),
                }
            }
        }

        "worm_lock" => {
            // WORM phase 2 admin op — engine-level immutability.
            // RBAC is enforced by the caller (handler dispatch
            // gates admin commands separately). This branch is the
            // pure storage path: locks `doc_id` on `collection`
            // until `locked_until_micros` (u64::MAX = indefinite).
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let doc_id = match request.get("doc_id").and_then(|v| v.as_u64()) {
                Some(v) => v,
                None => return err_bytes("missing or invalid 'doc_id' (u64)"),
            };
            let until = match request.get("locked_until_micros").and_then(|v| v.as_u64()) {
                Some(v) => v,
                None => return err_bytes("missing or invalid 'locked_until_micros' (u64)"),
            };
            match db.worm_lock(col, doc_id, until) {
                Ok(()) => ok_bytes(json!({"locked": true, "doc_id": doc_id, "locked_until_micros": until})),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "worm_release" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let doc_id = match request.get("doc_id").and_then(|v| v.as_u64()) {
                Some(v) => v,
                None => return err_bytes("missing or invalid 'doc_id' (u64)"),
            };
            match db.worm_release(col, doc_id) {
                Ok(()) => ok_bytes(json!({"released": true, "doc_id": doc_id})),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "worm_status" => {
            // Read-only: surface `locked_until_micros` for a doc.
            // 0 means "not locked". Operators use this from the
            // admin UI / status endpoint without needing to attempt
            // a doomed write.
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let doc_id = match request.get("doc_id").and_then(|v| v.as_u64()) {
                Some(v) => v,
                None => return err_bytes("missing or invalid 'doc_id' (u64)"),
            };
            match db.worm_locked_until(col, doc_id) {
                Ok(until) => ok_bytes(json!({"doc_id": doc_id, "locked_until_micros": until})),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "find_and_modify" => {
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
            // Always an immediate atomic op — never buffered into an open
            // transaction (it is the alternative to transactions for
            // contended counters).
            match db.find_and_modify(col, query, update) {
                Ok(Some(doc)) => ok_bytes(doc),
                Ok(None) => ok_bytes(json!(null)),
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
            if let Some(tx_id) = *active_tx {
                match db.tx_delete(tx_id, col, query) {
                    Ok(()) => ok_bytes(json!("buffered")),
                    Err(e) => err_bytes(&e.to_string()),
                }
            } else {
                match db.delete_one(col, query) {
                    Ok(count) => ok_bytes(json!({ "deleted": count })),
                    Err(e) => err_bytes(&e.to_string()),
                }
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

        "create_ttl_index" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let field = match request.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return err_bytes("missing 'field'"),
            };
            let expire_after = match request.get("expireAfterSeconds").and_then(|v| v.as_u64()) {
                Some(s) => s,
                None => return err_bytes("missing 'expireAfterSeconds'"),
            };
            match db.create_ttl_index(col, field, expire_after) {
                Ok(()) => ok_bytes(json!("ttl index created")),
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

            // Optional highlight: client passes
            //   { "highlight": true } for defaults, or
            //   { "highlight": { "snippet_chars": 80, "max_snippets": 3 } }
            let highlight_cfg = request.get("highlight").and_then(|h| {
                if h.as_bool() == Some(true) {
                    Some((80usize, 3usize))
                } else if let Some(obj) = h.as_object() {
                    let snippet_chars =
                        obj.get("snippet_chars").and_then(|v| v.as_u64()).unwrap_or(80) as usize;
                    let max_snippets =
                        obj.get("max_snippets").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
                    Some((snippet_chars, max_snippets))
                } else {
                    None
                }
            });

            let result = match highlight_cfg {
                Some((sc, ms)) => db.text_search_highlighted(col, query, limit, sc, ms),
                None => db.text_search(col, query, limit),
            };
            match result {
                Ok(results) => ok_bytes(json!(results)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

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

        "create_collection_with_options" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            // `options` is a JSON object; missing fields fall back to the
            // `StorageOptions` defaults (in-RAM, compressed, auto-compaction).
            // e.g. {"disk_first":true,"compress":false}.
            let opts = match request.get("options") {
                Some(v) => match serde_json::from_value::<oxidb::StorageOptions>(v.clone()) {
                    Ok(o) => o,
                    Err(e) => return err_bytes(&format!("invalid 'options': {e}")),
                },
                None => oxidb::StorageOptions::default(),
            };
            match db.create_collection_with_options(col, opts) {
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

        "aggregate_docs" => {
            // Run a pipeline over a supplied document array (not a stored
            // collection). Used by the OxiPool sharding proxy to run the merge
            // half of a cross-shard aggregation over the shards' concatenated
            // partial results, reusing the real executor.
            let pipeline = match request.get("pipeline") {
                Some(p) => p,
                None => return err_bytes("missing 'pipeline'"),
            };
            let docs: Vec<Value> = match request.get("docs").and_then(|v| v.as_array()) {
                Some(arr) => arr.clone(),
                None => return err_bytes("missing 'docs' array"),
            };
            match db.aggregate_docs(pipeline, docs) {
                Ok(out) => ok_bytes(json!(out)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // -------------------------------------------------------------------
        // Blob storage + FTS commands
        // -------------------------------------------------------------------

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

        "extract_text" => {
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
                    let content_type = meta
                        .get("content_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("application/octet-stream");
                    match oxidb::fts::extract_text(&data, content_type) {
                        Some(text) => ok_bytes(json!({ "text": text })),
                        None => err_bytes("could not extract text from this file type"),
                    }
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
                None => return err_bytes("missing 'collection'"),
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

            // Optional highlight: `{ "highlight": true }` for defaults, or
            // `{ "highlight": { "snippet_chars": N, "max_snippets": M } }`.
            // Note: extracting text from PDFs/DOCX is expensive, so the
            // caller pays only when they ask for highlights.
            let highlight_cfg = request.get("highlight").and_then(|h| {
                if h.as_bool() == Some(true) {
                    Some((80usize, 3usize))
                } else if let Some(obj) = h.as_object() {
                    let snippet_chars =
                        obj.get("snippet_chars").and_then(|v| v.as_u64()).unwrap_or(80) as usize;
                    let max_snippets =
                        obj.get("max_snippets").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
                    Some((snippet_chars, max_snippets))
                } else {
                    None
                }
            });

            let result = match highlight_cfg {
                Some((sc, ms)) => db.search_highlighted(bucket, query, limit, sc, ms),
                None => db.search(bucket, query, limit),
            };
            match result {
                Ok(results) => ok_bytes(json!(results)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "fts_status" => ok_bytes(db.fts_status()),

        "bucket_fts_size" => {
            let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                Some(b) => b,
                None => return err_bytes("missing 'bucket'"),
            };
            ok_bytes(json!({ "bucket": bucket, "bytes": db.bucket_fts_size(bucket) }))
        }

        "proc_status" => ok_bytes(crate::proc_stats::PROC_STATS.snapshot()),

        // -------------------------------------------------------------------
        // Backup & Restore (admin-only via RBAC)
        // -------------------------------------------------------------------

        "backup" => {
            let path = match request.get("path").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => return err_bytes("missing 'path'"),
            };
            match db.backup(std::path::Path::new(path)) {
                Ok(info) => ok_bytes(json!({
                    "path": info.path,
                    "size_bytes": info.size_bytes,
                    "collections": info.collections,
                })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "restore" => {
            let archive = match request.get("archive").and_then(|v| v.as_str()) {
                Some(a) => a,
                None => return err_bytes("missing 'archive'"),
            };
            let target = match request.get("target").and_then(|v| v.as_str()) {
                Some(t) => t,
                None => return err_bytes("missing 'target'"),
            };
            match oxidb::OxiDb::restore(
                std::path::Path::new(archive),
                std::path::Path::new(target),
            ) {
                Ok(info) => ok_bytes(json!({
                    "path": info.path,
                    "collections": info.collections,
                    "message": "restore complete; restart server with this data directory to use",
                })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "restore_to_point" => {
            let base_backup = match request.get("base_backup").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => return err_bytes("missing 'base_backup'"),
            };
            let archive = match request.get("archive").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => return err_bytes("missing 'archive'"),
            };
            let target = match request.get("target").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => return err_bytes("missing 'target'"),
            };
            // Target point: an explicit `gsn`, an explicit `at_micros`
            // (wall-clock, micros since epoch), or — by default — the
            // latest record in the archive.
            let point = if let Some(g) = request.get("gsn").and_then(|v| v.as_u64()) {
                oxidb::PitrTarget::Gsn(g)
            } else if let Some(t) = request.get("at_micros").and_then(|v| v.as_u64()) {
                oxidb::PitrTarget::Timestamp(t)
            } else {
                oxidb::PitrTarget::Latest
            };
            match oxidb::OxiDb::restore_to_point(
                std::path::Path::new(base_backup),
                std::path::Path::new(archive),
                std::path::Path::new(target),
                point,
                db.encryption_key(),
            ) {
                Ok(info) => ok_bytes(json!({
                    "path": info.path,
                    "collections": info.collections,
                    "target_gsn": info.target_gsn,
                    "records_applied": info.records_applied,
                    "message": "point-in-time restore complete; restart server with this data directory to use",
                })),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "archive_status" => match db.archive_status() {
            Ok(s) => ok_bytes(json!({
                "segment_count": s.segment_count,
                "total_records": s.total_records,
                "min_gsn": s.min_gsn,
                "max_gsn": s.max_gsn,
                "min_wall_clock": s.min_wall_clock,
                "max_wall_clock": s.max_wall_clock,
            })),
            Err(e) => err_bytes(&e.to_string()),
        },

        // (`sql` cmd removed alongside the SQL surface. OxiDB is a
        //  document database — use document CRUD + aggregation pipeline.)

        // -------------------------------------------------------------------
        // Stored procedures
        // -------------------------------------------------------------------

        "create_procedure" => {
            // Check if this is an OxiScript source (has "script" field)
            if let Some(script) = request.get("script").and_then(|v| v.as_str()) {
                match oxidb::oxiscript::compile(script) {
                    Ok(compiled) => {
                        let name = compiled["name"].as_str().unwrap_or("").to_string();
                        match db.create_procedure(&name, compiled) {
                            Ok(()) => ok_bytes(json!("procedure created")),
                            Err(e) => err_bytes(&e.to_string()),
                        }
                    }
                    Err(e) => err_bytes(&format!("oxiscript compile error: {}", e)),
                }
            } else {
                let name = match request.get("name").and_then(|v| v.as_str()) {
                    Some(n) => n.to_string(),
                    None => return err_bytes("missing 'name'"),
                };
                match db.create_procedure(&name, request) {
                    Ok(()) => ok_bytes(json!("procedure created")),
                    Err(e) => err_bytes(&e.to_string()),
                }
            }
        }

        "compile_oxiscript" => {
            let script = match request.get("script").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => return err_bytes("missing 'script'"),
            };
            match oxidb::oxiscript::compile(script) {
                Ok(compiled) => ok_bytes(compiled),
                Err(e) => err_bytes(&format!("oxiscript compile error: {}", e)),
            }
        }

        "call_procedure" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            let params = request.get("params").cloned().unwrap_or(json!({}));
            match db.call_procedure(name, params) {
                Ok(val) => ok_bytes(val),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_procedures" => match db.list_procedures() {
            Ok(names) => ok_bytes(json!(names)),
            Err(e) => err_bytes(&e.to_string()),
        },

        "get_procedure" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.get_procedure(name) {
                Ok(def) => ok_bytes(def),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "delete_procedure" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.delete_procedure(name) {
                Ok(()) => ok_bytes(json!("procedure deleted")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // -------------------------------------------------------------------
        // Cron schedules
        // -------------------------------------------------------------------

        "create_schedule" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => return err_bytes("missing 'name'"),
            };
            match db.create_schedule(&name, request) {
                Ok(()) => ok_bytes(json!("schedule created")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_schedules" => match db.list_schedules() {
            Ok(schedules) => ok_bytes(json!(schedules)),
            Err(e) => err_bytes(&e.to_string()),
        },

        "get_schedule" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.get_schedule(name) {
                Ok(sched) => ok_bytes(sched),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "delete_schedule" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.delete_schedule(name) {
                Ok(()) => ok_bytes(json!("schedule deleted")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "enable_schedule" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.enable_schedule(name) {
                Ok(()) => ok_bytes(json!("schedule enabled")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "disable_schedule" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.disable_schedule(name) {
                Ok(()) => ok_bytes(json!("schedule disabled")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // -------------------------------------------------------------------
        // Retention policies
        // -------------------------------------------------------------------

        "set_retention" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let days = match request.get("days").and_then(|v| v.as_u64()) {
                Some(d) => d,
                None => return err_bytes("missing 'days'"),
            };
            match db.set_retention(col, days) {
                Ok(()) => ok_bytes(json!(format!("retention set: {col} ({days} days)"))),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "get_retention" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            match db.get_retention(col) {
                Ok(policy) => ok_bytes(policy),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "delete_retention" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            match db.delete_retention(col) {
                Ok(()) => ok_bytes(json!(format!("retention deleted: {col}"))),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_retentions" => {
            match db.list_retentions() {
                Ok(policies) => ok_bytes(json!(policies)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // -------------------------------------------------------------------
        // Alerting
        // -------------------------------------------------------------------

        "create_alert" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            let mut def = request.clone();
            match db.create_alert(name, def) {
                Ok(()) => ok_bytes(json!(format!("alert created: {name}"))),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "delete_alert" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.delete_alert(name) {
                Ok(()) => ok_bytes(json!(format!("alert deleted: {name}"))),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_alerts" => {
            match db.list_alerts() {
                Ok(alerts) => ok_bytes(json!(alerts)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "get_alert" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.get_alert(name) {
                Ok(alert) => ok_bytes(alert),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "test_alert" => {
            let name = match request.get("name").and_then(|v| v.as_str()) {
                Some(n) => n,
                None => return err_bytes("missing 'name'"),
            };
            match db.test_alert(name) {
                Ok(result) => ok_bytes(result),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "list_alert_history" => {
            match db.list_alert_history() {
                Ok(history) => ok_bytes(json!(history)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        // -------------------------------------------------------------------
        // Vector index commands
        // -------------------------------------------------------------------

        "create_vector_index" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let field = match request.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return err_bytes("missing 'field'"),
            };
            let dimension = match request.get("dimension").and_then(|v| v.as_u64()) {
                Some(d) => d as usize,
                None => return err_bytes("missing 'dimension'"),
            };
            let metric_str = request
                .get("metric")
                .and_then(|v| v.as_str())
                .unwrap_or("cosine");
            let metric = oxidb::vector::VectorIndex::parse_metric(metric_str);
            match db.create_vector_index(col, field, dimension, metric) {
                Ok(()) => ok_bytes(json!("vector index created")),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        "vector_search" => {
            let col = match collection.as_deref() {
                Some(c) => c,
                None => return err_bytes("missing 'collection'"),
            };
            let field = match request.get("field").and_then(|v| v.as_str()) {
                Some(f) => f,
                None => return err_bytes("missing 'field'"),
            };
            let vector = match request.get("vector").and_then(|v| v.as_array()) {
                Some(arr) => {
                    let floats: Option<Vec<f32>> = arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect();
                    match floats {
                        Some(f) => f,
                        None => return err_bytes("'vector' must be an array of numbers"),
                    }
                }
                None => return err_bytes("missing 'vector' array"),
            };
            let limit = request
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            let ef_search = request
                .get("ef_search")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);
            match db.vector_search(col, field, &vector, limit, ef_search) {
                Ok(results) => ok_bytes(json!(results)),
                Err(e) => err_bytes(&e.to_string()),
            }
        }

        _ => err_bytes(&format!("unknown command: {cmd}")),
    }
}

/// is_link_management_cmd returns true for commands that take a
/// `collection` field as a LINK NAME (not a query target). Used by
/// the FDW pre-dispatch hook to skip the proxy lookup for these
/// commands so registering / removing a link doesn't try to proxy
/// itself.
fn is_link_management_cmd(cmd: &str) -> bool {
    matches!(cmd, "link_collection" | "unlink_collection")
}

/// handle_linked_command routes a query against a linked collection.
/// Read AND write commands are proxied to the remote OxiDB after
/// rewriting the `collection` field from the local link name to the
/// remote collection name (v2c). Schema, transactional, and admin
/// commands are still refused — those operate on whole-DB state and
/// should be issued directly against the remote, not laundered
/// through a link.
///
/// Returns the pre-serialized response bytes; the caller (the main
/// dispatch in handle_request) just returns this directly.
fn handle_linked_command(cmd: &str, link: &oxidb::links::LinkConfig, request: Value) -> Vec<u8> {
    use crate::fdw;

    // Allow-list: every CRUD command that operates on a single
    // collection's documents. Schema / transaction / management
    // commands stay refused — proxying them through a link would
    // either silently mutate the remote's schema or break pool reuse
    // (transactions need a sticky conn).
    match cmd {
        // Reads.
        "find" | "find_one" | "count" | "aggregate" | "text_search"
        // Writes (v2c).
        | "insert" | "insert_many"
        | "update" | "update_one" | "find_and_modify"
        | "delete" | "delete_one" => {}
        // Everything else against a linked collection is rejected with
        // a clear, actionable message.
        _ => {
            return err_bytes(&format!(
                "command {:?} is not allowed on linked collection {:?} \
                — only CRUD commands are proxied through a link; schema, \
                index, transaction, and admin commands must be issued \
                directly against the remote",
                cmd, link.name
            ));
        }
    }

    // Pick the right adapter from the link URL scheme (v3a). Bad URLs
    // surface here as a proxy error rather than a panic — same shape
    // the v1/v2 parse_remote error path had.
    let adapter = match fdw::adapter_for(&link.url) {
        Ok(a) => a,
        Err(e) => return err_bytes(&format!("linked {}: {}", link.name, e)),
    };

    match adapter.execute(cmd, &request) {
        Ok(resp) => {
            // Adapters return a fully-formed `{ok, ...}` envelope.
            // Pass through verbatim — the local server is just a proxy.
            serde_json::to_vec(&resp).unwrap_or_else(|_| err_bytes("proxy: encode response"))
        }
        Err(e) => err_bytes(&format!("linked {} → {}: {}", link.name, link.url, e)),
    }
}

/// Handle user management commands (requires admin role).
pub fn handle_user_command(
    cmd: &str,
    request: &Value,
    user_store: &Arc<Mutex<UserStore>>,
) -> Option<Vec<u8>> {
    match cmd {
        "create_user" => {
            let username = match request.get("username").and_then(|v| v.as_str()) {
                Some(u) => u,
                None => return Some(err_bytes("missing 'username'")),
            };
            let password = match request.get("password").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => return Some(err_bytes("missing 'password'")),
            };
            let role_str = request.get("role").and_then(|v| v.as_str()).unwrap_or("read");
            let role = match Role::from_str(role_str) {
                Some(r) => r,
                None => return Some(err_bytes(&format!("invalid role: {role_str}"))),
            };
            let mut store = user_store.lock().unwrap();
            match store.create_user(username, password, role) {
                Ok(()) => Some(ok_bytes(json!("user created"))),
                Err(e) => Some(err_bytes(&e)),
            }
        }
        "drop_user" => {
            let username = match request.get("username").and_then(|v| v.as_str()) {
                Some(u) => u,
                None => return Some(err_bytes("missing 'username'")),
            };
            let mut store = user_store.lock().unwrap();
            match store.drop_user(username) {
                Ok(()) => Some(ok_bytes(json!("user dropped"))),
                Err(e) => Some(err_bytes(&e)),
            }
        }
        "update_user" => {
            let username = match request.get("username").and_then(|v| v.as_str()) {
                Some(u) => u,
                None => return Some(err_bytes("missing 'username'")),
            };
            let password = request.get("password").and_then(|v| v.as_str());
            let role = request.get("role").and_then(|v| v.as_str()).and_then(Role::from_str);
            if password.is_none() && role.is_none() {
                return Some(err_bytes("must specify 'password' or 'role' to update"));
            }
            let mut store = user_store.lock().unwrap();
            match store.update_user(username, password, role) {
                Ok(()) => Some(ok_bytes(json!("user updated"))),
                Err(e) => Some(err_bytes(&e)),
            }
        }
        "list_users" => {
            let store = user_store.lock().unwrap();
            let users = store.list_users();
            Some(ok_bytes(json!(users)))
        }
        "grant_db_role" => {
            let username = match request.get("username").and_then(|v| v.as_str()) {
                Some(u) => u,
                None => return Some(err_bytes("missing 'username'")),
            };
            let database = match request.get("database").and_then(|v| v.as_str()) {
                Some(d) => d,
                None => return Some(err_bytes("missing 'database'")),
            };
            let role_str = match request.get("role").and_then(|v| v.as_str()) {
                Some(r) => r,
                None => return Some(err_bytes("missing 'role'")),
            };
            let role = match Role::from_str(role_str) {
                Some(r) => r,
                None => return Some(err_bytes(&format!("invalid role: {role_str}"))),
            };
            let mut store = user_store.lock().unwrap();
            match store.grant_db_role(username, database, role) {
                Ok(()) => Some(ok_bytes(json!(format!(
                    "granted role '{}' on database '{}' to user '{}'",
                    role.as_str(), database, username
                )))),
                Err(e) => Some(err_bytes(&e)),
            }
        }
        "revoke_db_role" => {
            let username = match request.get("username").and_then(|v| v.as_str()) {
                Some(u) => u,
                None => return Some(err_bytes("missing 'username'")),
            };
            let database = match request.get("database").and_then(|v| v.as_str()) {
                Some(d) => d,
                None => return Some(err_bytes("missing 'database'")),
            };
            let mut store = user_store.lock().unwrap();
            match store.revoke_db_role(username, database) {
                Ok(()) => Some(ok_bytes(json!(format!(
                    "revoked database role on '{}' from user '{}'",
                    database, username
                )))),
                Err(e) => Some(err_bytes(&e)),
            }
        }
        _ => None,
    }
}
