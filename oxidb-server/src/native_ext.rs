//! COBRA extension methods for stored procedures (ADR-0025 Phase 4): `db.rec_*`
//! and `db.vector_*` inside a `LANGUAGE COBRA` procedure.
//!
//! The SQL crate cannot depend on the server, so it exposes a
//! [`oxidb_sql::NativeExt`] hook and the server installs an implementation per
//! engine (from `sql_bridge`, right after opening it). Dispatch reuses the
//! engines' existing entry points — `rec_bridge::native_call` is the same
//! function the wire goes through, so a procedure and a client validate and
//! answer identically.
//!
//! Call convention (COBRA side): one dict argument mirroring the wire request,
//! e.g. `db.rec_related({"model": "purchase", "item": "kahve"})` or
//! `db.vector_search({"collection": "docs", "field": "emb",
//! "vector": [..], "limit": 5})`.
//!
//! `vector_*` needs the document engine, which lives in `ServerState`, not a
//! global — `set_doc_resolver` is installed at server startup; before that (or
//! with `OXIDB_DOC=0`) the methods refuse by name.

use std::sync::{Arc, OnceLock};

use oxidb::OxiDb;
use serde_json::Value;

/// Resolves a database name to its document engine. Installed once at startup.
type DocResolver = Box<dyn Fn(&str) -> Option<Arc<OxiDb>> + Send + Sync>;

static DOC_RESOLVER: OnceLock<DocResolver> = OnceLock::new();

pub fn set_doc_resolver(f: impl Fn(&str) -> Option<Arc<OxiDb>> + Send + Sync + 'static) {
    let _ = DOC_RESOLVER.set(Box::new(f));
}

struct ServerExt {
    db_name: String,
}

impl oxidb_sql::NativeExt for ServerExt {
    fn call(&self, method: &str, args: &Value) -> Result<Value, String> {
        // One dict argument, mirroring the wire request body.
        let request = match args.as_array().map(Vec::as_slice) {
            Some([Value::Object(o)]) => Value::Object(o.clone()),
            _ => {
                return Err(format!(
                    "db.{method} takes exactly one dict argument mirroring the wire request"
                ));
            }
        };
        if let Some(op) = method.strip_prefix("rec_") {
            // A procedure runs with the engine's authority; the wire's Read
            // gate applies to the CALLer of the procedure, which RBAC already
            // gated at `call_procedure`.
            return crate::rec_bridge::native_call(op, &request, false, &self.db_name);
        }
        if method == "vector_search" {
            let Some(resolver) = DOC_RESOLVER.get() else {
                return Err("vector_search is unavailable: document engine not wired".into());
            };
            let Some(db) = resolver(&self.db_name) else {
                return Err(format!("database not found: {}", self.db_name));
            };
            let collection = request
                .get("collection")
                .and_then(|v| v.as_str())
                .ok_or("missing 'collection'")?;
            let field = request
                .get("field")
                .and_then(|v| v.as_str())
                .ok_or("missing 'field'")?;
            let vector: Vec<f32> = request
                .get("vector")
                .and_then(|v| v.as_array())
                .ok_or("missing 'vector'")?
                .iter()
                .map(|v| v.as_f64().map(|f| f as f32).ok_or("vector must be numbers"))
                .collect::<Result<_, _>>()?;
            let limit = request.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
            let ef = request
                .get("ef_search")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);
            let hits = db
                .vector_search(collection, field, &vector, limit, ef)
                .map_err(|e| e.to_string())?;
            return Ok(Value::Array(hits));
        }
        Err(format!("unknown extension method: db.{method}"))
    }
}

/// Install the server's extension methods on a freshly-opened SQL engine.
pub fn install(engine: &oxidb_sql::SqlEngine, db_name: &str) {
    engine.set_native_ext(Arc::new(ServerExt {
        db_name: db_name.to_string(),
    }));
}
