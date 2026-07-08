//! Slow-query profiler — records wire operations that exceed a latency
//! threshold into the `_profile` collection, MongoDB-`system.profile`
//! style.
//!
//! Opt-in and zero-cost when off: set `OXIDB_SLOW_QUERY_MS=<n>` to
//! record every wire command slower than `n` ms. Each record carries
//! the timestamp, database, command, collection, the request *shape*
//! (query/pipeline/sql — enough to re-run it under `explain`), the
//! measured duration, and the active threshold. `_profile` gets a TTL
//! index on `ts` (default 24 h, `OXIDB_PROFILE_TTL_SECS` overrides) so
//! it can't grow unbounded. `oxidb_slow_queries_total` counts the hits
//! for Prometheus, so an alert can page before anyone reads the log.
//!
//! Scope: everything routed through the session handler — the sync and
//! cluster TCP paths, including SQL. REST handlers don't pass through
//! it and are not profiled.

use std::collections::HashSet;
use std::sync::LazyLock;
use std::sync::Mutex;

use oxidb::OxiDb;
use serde_json::{Value, json};

/// Threshold in ms; `None` = profiler off (the default).
pub static SLOW_MS: LazyLock<Option<u64>> = LazyLock::new(|| {
    std::env::var("OXIDB_SLOW_QUERY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
});

static PROFILE_TTL_SECS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("OXIDB_PROFILE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(86_400)
});

/// Databases whose `_profile` TTL index we've already ensured this
/// process lifetime. `create_ttl_index` is cheap but not free; once per
/// database is enough.
static TTL_ENSURED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Mutex::default);

/// The part of a request worth keeping in a profile record: enough to
/// understand and re-run the operation, without hauling megabyte
/// payloads (docs being inserted are deliberately NOT captured).
pub fn request_shape(request: &Value) -> Value {
    let mut shape = serde_json::Map::new();
    for key in [
        "query", "pipeline", "sql", "update", "options", "sort", "limit", "skip", "inner",
    ] {
        if let Some(v) = request.get(key) {
            shape.insert(key.to_string(), v.clone());
        }
    }
    Value::Object(shape)
}

/// Record one slow operation. Writes straight to the engine (not back
/// through the request handler), so profiling can never recurse.
pub fn record_slow(
    db: &OxiDb,
    db_name: &str,
    cmd: &str,
    collection: Option<&str>,
    shape: Value,
    duration_ms: f64,
    threshold_ms: u64,
) {
    crate::metrics::METRICS
        .slow_queries
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    {
        let mut ensured = TTL_ENSURED.lock().unwrap_or_else(|e| e.into_inner());
        if ensured.insert(db_name.to_string()) {
            let _ = db.create_ttl_index("_profile", "ts", *PROFILE_TTL_SECS);
        }
    }

    let _ = db.insert(
        "_profile",
        json!({
            "ts": chrono::Utc::now().to_rfc3339(),
            "db": db_name,
            "cmd": cmd,
            "collection": collection,
            "duration_ms": duration_ms,
            "threshold_ms": threshold_ms,
            "request": shape,
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shape_keeps_query_drops_docs() {
        let req = json!({
            "cmd": "find", "collection": "t",
            "query": {"a": 1}, "limit": 5,
            "doc": {"huge": "payload"}, "docs": [1, 2, 3]
        });
        let shape = request_shape(&req);
        assert_eq!(shape["query"], json!({"a": 1}));
        assert_eq!(shape["limit"], 5);
        assert!(shape.get("doc").is_none());
        assert!(shape.get("docs").is_none());
    }

    #[test]
    fn record_slow_writes_profile_doc() {
        let db = OxiDb::open_in_memory().unwrap();
        record_slow(
            &db,
            "oxidb",
            "find",
            Some("orders"),
            json!({"query": {"status": "open"}}),
            123.4,
            100,
        );
        let docs = db.find("_profile", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["cmd"], "find");
        assert_eq!(docs[0]["collection"], "orders");
        assert_eq!(docs[0]["duration_ms"], 123.4);
        assert_eq!(docs[0]["request"]["query"]["status"], "open");
    }
}
