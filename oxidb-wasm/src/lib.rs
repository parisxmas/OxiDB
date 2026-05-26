use wasm_bindgen::prelude::*;
use serde_json::{json, Value};
use std::sync::Arc;

use oxidb::OxiDb;
use oxidb::locks::RwLock;

static DB: RwLock<Option<Arc<OxiDb>>> = RwLock::new(None);

fn with_db<F, R>(f: F) -> Result<R, JsValue>
where
    F: FnOnce(&OxiDb) -> oxidb::Result<R>,
{
    let guard = DB.read();
    let db = guard
        .as_ref()
        .ok_or_else(|| JsValue::from_str("database not initialized — call init() first"))?;
    f(db).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Initialize the in-memory database. Must be called before any other operation.
#[wasm_bindgen]
pub fn init() {
    let db = OxiDb::open_in_memory().expect("failed to create in-memory database");
    *DB.write() = Some(Arc::new(db));
}

/// Insert a document into a collection. Returns the assigned document ID.
#[wasm_bindgen]
pub fn insert(collection: &str, json_doc: &str) -> Result<String, JsValue> {
    let doc: Value =
        serde_json::from_str(json_doc).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let doc_id = with_db(|db| db.insert(collection, doc))?;
    Ok(doc_id.to_string())
}

/// Insert multiple documents. Expects a JSON array string. Returns JSON array of IDs.
#[wasm_bindgen]
pub fn insert_many(collection: &str, json_docs: &str) -> Result<String, JsValue> {
    let docs: Vec<Value> =
        serde_json::from_str(json_docs).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let ids = with_db(|db| db.insert_many(collection, docs))?;
    serde_json::to_string(&ids).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Find documents matching a query. Returns JSON array string.
#[wasm_bindgen]
pub fn find(collection: &str, query: &str) -> Result<String, JsValue> {
    let q: Value =
        serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let results = with_db(|db| db.find(collection, &q))?;
    serde_json::to_string(&results).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Find a single document matching a query. Returns JSON string or "null".
#[wasm_bindgen]
pub fn find_one(collection: &str, query: &str) -> Result<String, JsValue> {
    let q: Value =
        serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let result = with_db(|db| db.find_one(collection, &q))?;
    serde_json::to_string(&result).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Update documents matching a query. Returns number of modified documents.
#[wasm_bindgen]
pub fn update(collection: &str, query: &str, update_doc: &str) -> Result<u32, JsValue> {
    let q: Value =
        serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let u: Value =
        serde_json::from_str(update_doc).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let count = with_db(|db| db.update(collection, &q, &u))?;
    Ok(count as u32)
}

/// Delete documents matching a query. Returns number of deleted documents.
#[wasm_bindgen]
pub fn delete(collection: &str, query: &str) -> Result<u32, JsValue> {
    let q: Value =
        serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let count = with_db(|db| db.delete(collection, &q))?;
    Ok(count as u32)
}

/// Count documents matching a query.
#[wasm_bindgen]
pub fn count(collection: &str, query: &str) -> Result<u32, JsValue> {
    let q: Value =
        serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let n = with_db(|db| db.count(collection, &q))?;
    Ok(n as u32)
}

/// Create an index on a field.
#[wasm_bindgen]
pub fn create_index(collection: &str, field: &str) -> Result<(), JsValue> {
    with_db(|db| db.create_index(collection, field))
}

/// List all collection names. Returns JSON array string.
#[wasm_bindgen]
pub fn list_collections() -> Result<String, JsValue> {
    let guard = DB.read();
    let db = guard
        .as_ref()
        .ok_or_else(|| JsValue::from_str("database not initialized"))?;
    let names = db.list_collections();
    serde_json::to_string(&names).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Drop a collection.
#[wasm_bindgen]
pub fn drop_collection(name: &str) -> Result<(), JsValue> {
    with_db(|db| db.drop_collection(name))
}

/// Run an aggregation pipeline. Returns JSON array string.
#[wasm_bindgen]
pub fn aggregate(collection: &str, pipeline: &str) -> Result<String, JsValue> {
    let stages: Value =
        serde_json::from_str(pipeline).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let results = with_db(|db| db.aggregate(collection, &stages))?;
    serde_json::to_string(&results).map_err(|e| JsValue::from_str(&e.to_string()))
}
