use serde_json::{Value, json};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

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
    let q: Value = serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let results = with_db(|db| db.find(collection, &q))?;
    serde_json::to_string(&results).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Find a single document matching a query. Returns JSON string or "null".
#[wasm_bindgen]
pub fn find_one(collection: &str, query: &str) -> Result<String, JsValue> {
    let q: Value = serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let result = with_db(|db| db.find_one(collection, &q))?;
    serde_json::to_string(&result).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Update documents matching a query. Returns number of modified documents.
#[wasm_bindgen]
pub fn update(collection: &str, query: &str, update_doc: &str) -> Result<u32, JsValue> {
    let q: Value = serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let u: Value =
        serde_json::from_str(update_doc).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let count = with_db(|db| db.update(collection, &q, &u))?;
    Ok(count as u32)
}

/// Delete documents matching a query. Returns number of deleted documents.
#[wasm_bindgen]
pub fn delete(collection: &str, query: &str) -> Result<u32, JsValue> {
    let q: Value = serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let count = with_db(|db| db.delete(collection, &q))?;
    Ok(count as u32)
}

/// Count documents matching a query.
#[wasm_bindgen]
pub fn count(collection: &str, query: &str) -> Result<u32, JsValue> {
    let q: Value = serde_json::from_str(query).map_err(|e| JsValue::from_str(&e.to_string()))?;
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

/// Serialize the entire database to a portable JSON image:
/// `{ "version": 1, "collections": { name: [docs...] } }`.
///
/// Intended for durable persistence in environments without a filesystem
/// (e.g. the browser): dump the image, store it (OPFS, etc.), and reload it
/// with [`restore`] on the next start.
#[wasm_bindgen]
pub fn dump() -> Result<String, JsValue> {
    let guard = DB.read();
    let db = guard
        .as_ref()
        .ok_or_else(|| JsValue::from_str("database not initialized — call init() first"))?;
    let mut collections = serde_json::Map::new();
    for name in db.list_collections() {
        let docs = db
            .find(&name, &json!({}))
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        collections.insert(name, Value::Array(docs));
    }
    let image = json!({ "version": 1, "collections": Value::Object(collections) });
    serde_json::to_string(&image).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Load a database image produced by [`dump`] into the current (freshly
/// initialized) database. The engine-managed `_id` / `_version` fields are
/// stripped so documents are re-inserted with fresh identifiers.
#[wasm_bindgen]
pub fn restore(image: &str) -> Result<(), JsValue> {
    let parsed: Value =
        serde_json::from_str(image).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let collections = parsed
        .get("collections")
        .and_then(Value::as_object)
        .ok_or_else(|| JsValue::from_str("invalid image: missing 'collections' object"))?;

    let guard = DB.read();
    let db = guard
        .as_ref()
        .ok_or_else(|| JsValue::from_str("database not initialized — call init() first"))?;

    for (name, docs_val) in collections {
        let Some(arr) = docs_val.as_array() else { continue };
        let docs: Vec<Value> = arr
            .iter()
            .map(|doc| {
                let mut doc = doc.clone();
                if let Some(obj) = doc.as_object_mut() {
                    obj.remove("_id");
                    obj.remove("_version");
                }
                doc
            })
            .collect();
        if !docs.is_empty() {
            db.insert_many(name, docs)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
        }
    }
    Ok(())
}
