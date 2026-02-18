use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

use base64::Engine;
use oxidb::OxiDb;
use oxidb::query::parse_find_options;
use serde_json::{Value, json};

pub trait CommandExecutor {
    fn execute(&mut self, cmd: Value) -> Result<Value, String>;
}

// ---------------------------------------------------------------------------
// Embedded executor — opens database files directly
// ---------------------------------------------------------------------------

pub struct EmbeddedExecutor {
    db: Arc<OxiDb>,
    active_tx: Option<u64>,
}

impl EmbeddedExecutor {
    pub fn open(data_dir: &Path, encryption_key: Option<Arc<oxidb::EncryptionKey>>) -> Result<Self, String> {
        let db = OxiDb::open_with_options(data_dir, encryption_key)
            .map_err(|e| format!("failed to open database: {e}"))?;
        Ok(Self {
            db: Arc::new(db),
            active_tx: None,
        })
    }
}

fn ok_val(data: Value) -> Result<Value, String> {
    Ok(json!({"ok": true, "data": data}))
}

fn err_val(msg: &str) -> Result<Value, String> {
    Ok(json!({"ok": false, "error": msg}))
}

impl CommandExecutor for EmbeddedExecutor {
    fn execute(&mut self, mut request: Value) -> Result<Value, String> {
        let cmd = match request.get("cmd").and_then(|v| v.as_str()) {
            Some(c) => c.to_string(),
            None => return err_val("missing or invalid 'cmd' field"),
        };

        let collection: Option<String> =
            request.get("collection").and_then(|v| v.as_str().map(|s| s.to_string()));

        match cmd.as_str() {
            "ping" => ok_val(json!("pong")),

            // --- Transactions ---
            "begin_tx" => {
                if self.active_tx.is_some() {
                    return err_val("transaction already active");
                }
                let tx_id = self.db.begin_transaction();
                self.active_tx = Some(tx_id);
                ok_val(json!({"tx_id": tx_id}))
            }
            "commit_tx" => match self.active_tx.take() {
                Some(tx_id) => match self.db.commit_transaction(tx_id) {
                    Ok(()) => ok_val(json!("committed")),
                    Err(e) => err_val(&e.to_string()),
                },
                None => err_val("no active transaction"),
            },
            "rollback_tx" => match self.active_tx.take() {
                Some(tx_id) => {
                    let _ = self.db.rollback_transaction(tx_id);
                    ok_val(json!("rolled back"))
                }
                None => err_val("no active transaction"),
            },

            // --- CRUD ---
            "insert" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let doc = match request.get_mut("doc").map(Value::take) {
                    Some(d) if !d.is_null() => d,
                    _ => return err_val("missing 'doc'"),
                };
                if let Some(tx_id) = self.active_tx {
                    match self.db.tx_insert(tx_id, col, doc) {
                        Ok(()) => ok_val(json!("buffered")),
                        Err(e) => err_val(&e.to_string()),
                    }
                } else {
                    match self.db.insert(col, doc) {
                        Ok(id) => ok_val(json!({"id": id})),
                        Err(e) => err_val(&e.to_string()),
                    }
                }
            }
            "insert_many" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let docs = match request.get_mut("docs").map(Value::take) {
                    Some(Value::Array(arr)) => arr,
                    _ => return err_val("missing or invalid 'docs' array"),
                };
                if let Some(tx_id) = self.active_tx {
                    for doc in docs {
                        if let Err(e) = self.db.tx_insert(tx_id, col, doc) {
                            return err_val(&e.to_string());
                        }
                    }
                    ok_val(json!("buffered"))
                } else {
                    match self.db.insert_many(col, docs) {
                        Ok(ids) => ok_val(json!(ids)),
                        Err(e) => err_val(&e.to_string()),
                    }
                }
            }
            "find" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let empty = json!({});
                let query = request.get("query").unwrap_or(&empty);
                if let Some(tx_id) = self.active_tx {
                    match self.db.tx_find(tx_id, col, query) {
                        Ok(docs) => ok_val(json!(docs)),
                        Err(e) => err_val(&e.to_string()),
                    }
                } else {
                    let opts = match parse_find_options(&request) {
                        Ok(o) => o,
                        Err(e) => return err_val(&e.to_string()),
                    };
                    match self.db.find_with_options(col, query, &opts) {
                        Ok(docs) => ok_val(json!(docs)),
                        Err(e) => err_val(&e.to_string()),
                    }
                }
            }
            "find_one" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let empty = json!({});
                let query = request.get("query").unwrap_or(&empty);
                match self.db.find_one(col, query) {
                    Ok(doc) => ok_val(json!(doc)),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "update" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let query = match request.get("query") {
                    Some(q) => q,
                    None => return err_val("missing 'query'"),
                };
                let update = match request.get("update") {
                    Some(u) => u,
                    None => return err_val("missing 'update'"),
                };
                if let Some(tx_id) = self.active_tx {
                    match self.db.tx_update(tx_id, col, query, update) {
                        Ok(()) => ok_val(json!("buffered")),
                        Err(e) => err_val(&e.to_string()),
                    }
                } else {
                    match self.db.update(col, query, update) {
                        Ok(count) => ok_val(json!({"modified": count})),
                        Err(e) => err_val(&e.to_string()),
                    }
                }
            }
            "update_one" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let query = match request.get("query") {
                    Some(q) => q,
                    None => return err_val("missing 'query'"),
                };
                let update = match request.get("update") {
                    Some(u) => u,
                    None => return err_val("missing 'update'"),
                };
                match self.db.update_one(col, query, update) {
                    Ok(count) => ok_val(json!({"modified": count})),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "delete" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let query = match request.get("query") {
                    Some(q) => q,
                    None => return err_val("missing 'query'"),
                };
                if let Some(tx_id) = self.active_tx {
                    match self.db.tx_delete(tx_id, col, query) {
                        Ok(()) => ok_val(json!("buffered")),
                        Err(e) => err_val(&e.to_string()),
                    }
                } else {
                    match self.db.delete(col, query) {
                        Ok(count) => ok_val(json!({"deleted": count})),
                        Err(e) => err_val(&e.to_string()),
                    }
                }
            }
            "delete_one" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let query = match request.get("query") {
                    Some(q) => q,
                    None => return err_val("missing 'query'"),
                };
                match self.db.delete_one(col, query) {
                    Ok(count) => ok_val(json!({"deleted": count})),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "count" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let empty = json!({});
                let query = request.get("query").unwrap_or(&empty);
                match self.db.count(col, query) {
                    Ok(n) => ok_val(json!({"count": n})),
                    Err(e) => err_val(&e.to_string()),
                }
            }

            // --- Indexes ---
            "create_index" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let field = match request.get("field").and_then(|v| v.as_str()) {
                    Some(f) => f,
                    None => return err_val("missing 'field'"),
                };
                match self.db.create_index(col, field) {
                    Ok(()) => ok_val(json!("index created")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "create_unique_index" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let field = match request.get("field").and_then(|v| v.as_str()) {
                    Some(f) => f,
                    None => return err_val("missing 'field'"),
                };
                match self.db.create_unique_index(col, field) {
                    Ok(()) => ok_val(json!("unique index created")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "create_composite_index" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let fields = match request.get("fields").and_then(|v| v.as_array()) {
                    Some(arr) => {
                        let strs: Option<Vec<String>> =
                            arr.iter().map(|v| v.as_str().map(String::from)).collect();
                        match strs {
                            Some(s) => s,
                            None => return err_val("'fields' must be an array of strings"),
                        }
                    }
                    None => return err_val("missing 'fields' array"),
                };
                match self.db.create_composite_index(col, fields) {
                    Ok(name) => ok_val(json!({"index": name})),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "create_text_index" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let fields = match request.get("fields").and_then(|v| v.as_array()) {
                    Some(arr) => {
                        let strs: Option<Vec<String>> =
                            arr.iter().map(|v| v.as_str().map(String::from)).collect();
                        match strs {
                            Some(s) => s,
                            None => return err_val("'fields' must be an array of strings"),
                        }
                    }
                    None => return err_val("missing 'fields' array"),
                };
                match self.db.create_text_index(col, fields) {
                    Ok(()) => ok_val(json!("text index created")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "text_search" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let query = match request.get("query").and_then(|v| v.as_str()) {
                    Some(q) => q,
                    None => return err_val("missing 'query' string"),
                };
                let limit = request
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize;
                match self.db.text_search(col, query, limit) {
                    Ok(results) => ok_val(json!(results)),
                    Err(e) => err_val(&e.to_string()),
                }
            }

            // --- Collections ---
            "create_collection" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                match self.db.create_collection(col) {
                    Ok(()) => ok_val(json!("collection created")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "list_collections" => {
                let names = self.db.list_collections();
                ok_val(json!(names))
            }
            "drop_collection" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                match self.db.drop_collection(col) {
                    Ok(()) => ok_val(json!("collection dropped")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "compact" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                match self.db.compact(col) {
                    Ok(stats) => ok_val(json!({
                        "old_size": stats.old_size,
                        "new_size": stats.new_size,
                        "docs_kept": stats.docs_kept
                    })),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "aggregate" => {
                let col = match collection.as_deref() {
                    Some(c) => c,
                    None => return err_val("missing 'collection'"),
                };
                let pipeline = match request.get("pipeline") {
                    Some(p) => p,
                    None => return err_val("missing 'pipeline'"),
                };
                match self.db.aggregate(col, pipeline) {
                    Ok(docs) => ok_val(json!(docs)),
                    Err(e) => err_val(&e.to_string()),
                }
            }

            // --- Blob storage ---
            "create_bucket" => {
                let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => return err_val("missing 'bucket'"),
                };
                match self.db.create_bucket(bucket) {
                    Ok(()) => ok_val(json!("bucket created")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "list_buckets" => {
                let buckets = self.db.list_buckets();
                ok_val(json!(buckets))
            }
            "delete_bucket" => {
                let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => return err_val("missing 'bucket'"),
                };
                match self.db.delete_bucket(bucket) {
                    Ok(()) => ok_val(json!("bucket deleted")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "put_object" => {
                let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => return err_val("missing 'bucket'"),
                };
                let key = match request.get("key").and_then(|v| v.as_str()) {
                    Some(k) => k,
                    None => return err_val("missing 'key'"),
                };
                let data_b64 = match request.get("data").and_then(|v| v.as_str()) {
                    Some(d) => d,
                    None => return err_val("missing 'data' (base64)"),
                };
                let data = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
                    Ok(d) => d,
                    Err(e) => return err_val(&format!("invalid base64: {e}")),
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
                match self.db.put_object(bucket, key, &data, content_type, metadata) {
                    Ok(meta) => ok_val(meta),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "get_object" => {
                let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => return err_val("missing 'bucket'"),
                };
                let key = match request.get("key").and_then(|v| v.as_str()) {
                    Some(k) => k,
                    None => return err_val("missing 'key'"),
                };
                match self.db.get_object(bucket, key) {
                    Ok((data, meta)) => {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
                        ok_val(json!({"content": encoded, "metadata": meta}))
                    }
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "head_object" => {
                let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => return err_val("missing 'bucket'"),
                };
                let key = match request.get("key").and_then(|v| v.as_str()) {
                    Some(k) => k,
                    None => return err_val("missing 'key'"),
                };
                match self.db.head_object(bucket, key) {
                    Ok(meta) => ok_val(meta),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "delete_object" => {
                let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => return err_val("missing 'bucket'"),
                };
                let key = match request.get("key").and_then(|v| v.as_str()) {
                    Some(k) => k,
                    None => return err_val("missing 'key'"),
                };
                match self.db.delete_object(bucket, key) {
                    Ok(()) => ok_val(json!("object deleted")),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "list_objects" => {
                let bucket = match request.get("bucket").and_then(|v| v.as_str()) {
                    Some(b) => b,
                    None => return err_val("missing 'bucket'"),
                };
                let prefix = request.get("prefix").and_then(|v| v.as_str());
                let limit = request
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize);
                match self.db.list_objects(bucket, prefix, limit) {
                    Ok(list) => ok_val(json!(list)),
                    Err(e) => err_val(&e.to_string()),
                }
            }
            "search" => {
                let query = match request.get("query").and_then(|v| v.as_str()) {
                    Some(q) => q,
                    None => return err_val("missing 'query'"),
                };
                let bucket = request.get("bucket").and_then(|v| v.as_str());
                let limit = request
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize;
                match self.db.search(bucket, query, limit) {
                    Ok(results) => ok_val(json!(results)),
                    Err(e) => err_val(&e.to_string()),
                }
            }

            _ => err_val(&format!("unknown command: {cmd}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Client executor — connects to a running OxiDB server via TCP
// ---------------------------------------------------------------------------

pub struct ClientExecutor {
    stream: TcpStream,
}

impl ClientExecutor {
    pub fn connect(host: &str, port: u16) -> Result<Self, String> {
        let stream = TcpStream::connect((host, port))
            .map_err(|e| format!("failed to connect to {host}:{port}: {e}"))?;
        Ok(Self { stream })
    }
}

impl CommandExecutor for ClientExecutor {
    fn execute(&mut self, cmd: Value) -> Result<Value, String> {
        let payload = cmd.to_string();
        let payload_bytes = payload.as_bytes();

        // Write: [u32 LE length][json]
        let len = (payload_bytes.len() as u32).to_le_bytes();
        self.stream
            .write_all(&len)
            .map_err(|e| format!("write error: {e}"))?;
        self.stream
            .write_all(payload_bytes)
            .map_err(|e| format!("write error: {e}"))?;
        self.stream
            .flush()
            .map_err(|e| format!("flush error: {e}"))?;

        // Read: [u32 LE length][json]
        let mut len_buf = [0u8; 4];
        self.stream
            .read_exact(&mut len_buf)
            .map_err(|e| format!("read error: {e}"))?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; resp_len];
        self.stream
            .read_exact(&mut buf)
            .map_err(|e| format!("read error: {e}"))?;

        serde_json::from_slice(&buf).map_err(|e| format!("invalid response JSON: {e}"))
    }
}
