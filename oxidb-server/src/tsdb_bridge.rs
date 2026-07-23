//! Bridge between the wire protocol and the standalone time-series engine
//! (`oxidb-tsdb`).
//!
//! A *third* engine mounted in the same server, in the spirit of the SQL
//! engine (`sql_bridge`): entirely separate storage, **off by default**, built
//! lazily only when `OXIDB_TSDB` is truthy. Routing (in
//! [`crate::handler`]): a request whose `engine` field is `"tsdb"` — or the
//! reserved `tsdb` command — is served here.
//!
//! Per-database, mirroring the SQL engine. **MVP: in-memory** (data is not yet
//! persisted across restarts); on-disk blocks + WAL are the next step.
//!
//! Wire (all under `cmd: "tsdb"`, an `op` selects the action):
//! ```json
//! { "engine":"tsdb", "cmd":"tsdb", "op":"write",
//!   "points":[ {"measurement":"cpu","tags":{"host":"a"},"fields":{"usage":0.9},"ts":1700000000000} ] }
//!
//! { "engine":"tsdb", "cmd":"tsdb", "op":"query", "measurement":"cpu", "field":"usage",
//!   "tags":{"host":"a"}, "start":.., "end":.., "group_by":["region"], "interval":60000, "agg":"mean" }
//!
//! { "op":"stats" }                    -> { series, points, bytes }
//! { "op":"retention", "cutoff":.. }   -> { removed }
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};

use oxidb::database_manager::DEFAULT_DATABASE;
use oxidb_tsdb::{Agg, Point, QuerySpec, RollupSpec, TagPredicate, Tsdb};
use serde_json::{Value, json};

use crate::handler::{err_bytes, ok_bytes};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parse an aggregation name (with `p95` / `percentile`+`p` shorthands).
fn parse_agg(s: &str, p: Option<f64>) -> Result<Agg, String> {
    Ok(match s {
        "mean" | "avg" => Agg::Mean,
        "sum" => Agg::Sum,
        "min" => Agg::Min,
        "max" => Agg::Max,
        "count" => Agg::Count,
        "distinct" => Agg::Distinct,
        "first" => Agg::First,
        "last" => Agg::Last,
        "rate" => Agg::Rate,
        "percentile" | "quantile" => Agg::Percentile(p.ok_or("percentile requires 'p'")?),
        s if s.starts_with('p') && s[1..].parse::<f64>().is_ok() => {
            Agg::Percentile(s[1..].parse().unwrap())
        }
        other => return Err(format!("unknown agg: {other:?}")),
    })
}

/// Define a rollup rule: `{measurement, label, interval, aggs:[...]}`.
/// `{ "engine": "tsdb", "cmd": "backup", "path": "..." }` — a consistent
/// `.tar.gz` of the TSDB database `db_name`'s data directory. Holds the engine
/// write lock across the checkpoint + archive so nothing writes mid-tar.
fn tsdb_backup(request: &Value, db_name: &str) -> Vec<u8> {
    let Some(path) = request.get("path").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'path'");
    };
    let engine = match engine_for(db_name) {
        Ok(e) => e,
        Err(msg) => return err_bytes(&msg),
    };
    // Low-lock: pin under the write lock (phase 1), compress with the lock
    // released (phase 2), then unpin under the write lock (phase 3). Writes and
    // checkpoints run concurrently with the compression.
    let plan = match engine.write().unwrap().backup_begin() {
        Ok(p) => p,
        Err(e) => return err_bytes(&e.to_string()),
    };
    let result = Tsdb::backup_write(&plan, std::path::Path::new(path));
    engine.write().unwrap().backup_end(&plan);
    match result {
        Ok(size_bytes) => ok_bytes(json!({ "path": path, "size_bytes": size_bytes })),
        Err(e) => err_bytes(&e.to_string()),
    }
}

/// `{ "engine": "tsdb", "cmd": "restore", "archive": "...", "target": "..." }` —
/// extract a TSDB backup into an empty target directory.
fn tsdb_restore(request: &Value) -> Vec<u8> {
    let Some(archive) = request.get("archive").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'archive'");
    };
    let Some(target) = request.get("target").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'target'");
    };
    match Tsdb::restore(std::path::Path::new(archive), std::path::Path::new(target)) {
        Ok(()) => ok_bytes(json!({
            "path": target,
            "message": "TSDB restore complete; open a fresh TSDB engine on this directory to use",
        })),
        Err(e) => err_bytes(&e.to_string()),
    }
}

fn rollup_add(engine: &Arc<RwLock<Tsdb>>, request: &Value) -> Vec<u8> {
    let Some(measurement) = request.get("measurement").and_then(|v| v.as_str()) else {
        return err_bytes("rollup_add requires a 'measurement'");
    };
    let Some(interval) = request.get("interval").and_then(|v| v.as_i64()) else {
        return err_bytes("rollup_add requires an integer 'interval' (ms)");
    };
    if interval <= 0 {
        return err_bytes("interval must be > 0");
    }
    let label = request
        .get("label")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| format!("{interval}ms"));
    let agg_names: Vec<&str> = request
        .get("aggs")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_else(|| vec!["mean", "min", "max", "count"]);
    let mut aggs = Vec::new();
    for name in agg_names {
        match parse_agg(name, None) {
            Ok(a) => aggs.push(a),
            Err(e) => return err_bytes(&e),
        }
    }
    engine.write().unwrap().add_rollup(RollupSpec {
        measurement: measurement.to_string(),
        label,
        interval,
        aggs,
    });
    ok_bytes(json!({ "ok": true }))
}

/// Per-database time-series engines, persisted on disk.
struct Registry {
    root: PathBuf,
    default_dir: PathBuf,
    engines: RwLock<HashMap<String, Arc<RwLock<Tsdb>>>>,
}

static REGISTRY: OnceLock<Option<Registry>> = OnceLock::new();

fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn registry() -> Option<&'static Registry> {
    REGISTRY
        .get_or_init(|| {
            if !env_truthy("OXIDB_TSDB") {
                return None;
            }
            let root = PathBuf::from(
                std::env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".into()),
            );
            let default_dir = std::env::var("OXIDB_TSDB_DATA")
                .map(PathBuf::from)
                .unwrap_or_else(|_| root.join("tsdb"));
            eprintln!(
                "[oxidb] TSDB engine enabled (default db at {})",
                default_dir.display()
            );
            Some(Registry {
                root,
                default_dir,
                engines: RwLock::new(HashMap::new()),
            })
        })
        .as_ref()
}

fn normalize(db_name: &str) -> &str {
    if db_name.is_empty() || db_name == "postgres" {
        DEFAULT_DATABASE
    } else {
        db_name
    }
}

fn engine_for(db_name: &str) -> Result<Arc<RwLock<Tsdb>>, String> {
    let Some(reg) = registry() else {
        return Err("TSDB engine is not enabled (set OXIDB_TSDB=1)".to_string());
    };
    let name = normalize(db_name);
    if let Some(e) = reg.engines.read().unwrap().get(name) {
        return Ok(Arc::clone(e));
    }
    let dir = if name == DEFAULT_DATABASE {
        reg.default_dir.clone()
    } else {
        let db_dir = reg.root.join(name);
        if !db_dir.is_dir() {
            return Err(format!("database not found: {name}"));
        }
        db_dir.join("tsdb")
    };
    let engine =
        Tsdb::open(&dir).map_err(|e| format!("failed to open TSDB for database {name:?}: {e}"))?;
    let mut engines = reg.engines.write().unwrap();
    if let Some(existing) = engines.get(name) {
        return Ok(Arc::clone(existing));
    }
    let arc = Arc::new(RwLock::new(engine));
    engines.insert(name.to_string(), Arc::clone(&arc));
    Ok(arc)
}

/// Drop a database's TSDB engine (called by `drop_database`).
pub fn forget_database(db_name: &str) {
    if let Some(reg) = registry() {
        reg.engines.write().unwrap().remove(normalize(db_name));
    }
}

pub fn handle_tsdb(cmd: &str, request: &Value, readonly: bool, db_name: &str) -> Vec<u8> {
    // Engine-aware backup/restore. Like the document and SQL engines, "backup"
    // and "restore" are admin-only in the RBAC table, so they arrive pre-gated.
    match cmd {
        "backup" => return tsdb_backup(request, db_name),
        "restore" => return tsdb_restore(request),
        "tsdb" => {}
        _ => return err_bytes("TSDB engine requests must use cmd \"tsdb\""),
    }
    let op = request.get("op").and_then(|v| v.as_str()).unwrap_or("");
    let engine = match engine_for(db_name) {
        Ok(e) => e,
        Err(msg) => return err_bytes(&msg),
    };
    match op {
        "write" => {
            if readonly {
                return err_bytes("permission denied: read-only role cannot write time-series");
            }
            write_points(&engine, request)
        }
        "write_lp" => {
            if readonly {
                return err_bytes("permission denied: read-only role cannot write time-series");
            }
            write_line_protocol(&engine, request)
        }
        "query" => query(&engine, request),
        "stats" => {
            let db = engine.read().unwrap();
            ok_bytes(json!({
                "series": db.series_count(),
                "points": db.point_count(),
                "bytes": db.compressed_bytes(),
            }))
        }
        "retention" => {
            if readonly {
                return err_bytes("permission denied: read-only role cannot enforce retention");
            }
            let Some(cutoff) = request.get("cutoff").and_then(|v| v.as_i64()) else {
                return err_bytes("retention requires an integer 'cutoff' (epoch ms)");
            };
            let removed = engine.write().unwrap().enforce_retention(cutoff);
            ok_bytes(json!({ "removed": removed }))
        }
        "checkpoint" => {
            if readonly {
                return err_bytes("permission denied: read-only role cannot checkpoint");
            }
            match engine.write().unwrap().checkpoint() {
                Ok(()) => ok_bytes(json!({ "ok": true })),
                Err(e) => err_bytes(&format!("checkpoint failed: {e}")),
            }
        }
        "rollup_add" => {
            if readonly {
                return err_bytes("permission denied: read-only role cannot define rollups");
            }
            rollup_add(&engine, request)
        }
        "rollup_refresh" => {
            if readonly {
                return err_bytes("permission denied: read-only role cannot refresh rollups");
            }
            let now = request
                .get("now")
                .and_then(|v| v.as_i64())
                .unwrap_or_else(now_ms);
            let n = engine.write().unwrap().refresh_rollups(now);
            ok_bytes(json!({ "written": n }))
        }
        "rollups" => {
            let db = engine.read().unwrap();
            let list: Vec<Value> = db
                .rollups()
                .iter()
                .map(|r| json!({ "measurement": r.measurement, "label": r.label, "interval": r.interval }))
                .collect();
            ok_bytes(json!(list))
        }
        "" => err_bytes(
            "missing 'op' (write | write_lp | query | stats | retention | checkpoint | rollup_add | rollup_refresh | rollups)",
        ),
        other => err_bytes(&format!("unknown tsdb op: {other:?}")),
    }
}

fn write_points(engine: &Arc<RwLock<Tsdb>>, request: &Value) -> Vec<u8> {
    match write_points_core(engine, request) {
        Ok(written) => ok_bytes(json!({ "written": written })),
        Err(e) => err_bytes(&e),
    }
}

/// Write the `points` array; returns the count. Shared by the wire handler and
/// the PostgREST bridge ([`tsdb_write_json`]).
fn write_points_core(engine: &Arc<RwLock<Tsdb>>, request: &Value) -> Result<usize, String> {
    let Some(points) = request.get("points").and_then(|v| v.as_array()) else {
        return Err("write requires a 'points' array".to_string());
    };
    let mut db = engine.write().unwrap();
    let mut written = 0usize;
    for p in points {
        let Some(measurement) = p.get("measurement").and_then(|v| v.as_str()) else {
            return Err("each point needs a 'measurement'".to_string());
        };
        let Some(ts) = p.get("ts").and_then(|v| v.as_i64()) else {
            return Err("each point needs an integer 'ts' (epoch ms)".to_string());
        };
        let mut point = Point::new(measurement, ts);
        if let Some(tags) = p.get("tags").and_then(|v| v.as_object()) {
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    point = point.tag(k, s);
                }
            }
        }
        let fields = p.get("fields").and_then(|v| v.as_object());
        let Some(fields) = fields else {
            return Err("each point needs a 'fields' object".to_string());
        };
        for (k, v) in fields {
            // JSON bool → boolean, integer → integer, string → text, else float.
            if let Some(b) = v.as_bool() {
                point = point.field_bool(k, b);
            } else if v.is_i64() {
                point = point.field_int(k, v.as_i64().unwrap());
            } else if let Some(s) = v.as_str() {
                point = point.field_str(k, s);
            } else if let Some(f) = v.as_f64() {
                point = point.field(k, f);
            }
        }
        db.write(&point);
        written += 1;
    }
    Ok(written)
}

/// Ingest InfluxDB line protocol from the `lp` field. Lines without a
/// timestamp use the server's current time (epoch ms).
fn write_line_protocol(engine: &Arc<RwLock<Tsdb>>, request: &Value) -> Vec<u8> {
    let Some(lp) = request.get("lp").and_then(|v| v.as_str()) else {
        return err_bytes("write_lp requires an 'lp' string");
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let points = match oxidb_tsdb::parse_line_protocol(lp, now_ms) {
        Ok(p) => p,
        Err(e) => return err_bytes(&format!("line protocol: {e}")),
    };
    let mut db = engine.write().unwrap();
    for p in &points {
        db.write(p);
    }
    ok_bytes(json!({ "written": points.len() }))
}

fn query(engine: &Arc<RwLock<Tsdb>>, request: &Value) -> Vec<u8> {
    match query_core(engine, request) {
        Ok(v) => ok_bytes(v),
        Err(e) => err_bytes(&e),
    }
}

/// Run a query and return the series array. Shared by the wire handler and the
/// PostgREST bridge ([`tsdb_query_json`]).
fn query_core(engine: &Arc<RwLock<Tsdb>>, request: &Value) -> Result<Value, String> {
    let Some(measurement) = request.get("measurement").and_then(|v| v.as_str()) else {
        return Err("query requires a 'measurement'".to_string());
    };
    let Some(field) = request.get("field").and_then(|v| v.as_str()) else {
        return Err("query requires a 'field'".to_string());
    };
    let tag_filters = request
        .get("tags")
        .and_then(|v| v.as_object())
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| {
                    v.as_str().map(|s| TagPredicate {
                        key: k.clone(),
                        value: s.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let group_tags = request
        .get("group_by")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let agg_str = request
        .get("agg")
        .and_then(|v| v.as_str())
        .unwrap_or("mean");
    let agg = match agg_str {
        "mean" | "avg" => Agg::Mean,
        "sum" => Agg::Sum,
        "min" => Agg::Min,
        "max" => Agg::Max,
        "count" => Agg::Count,
        "distinct" => Agg::Distinct,
        "first" => Agg::First,
        "last" => Agg::Last,
        "rate" => Agg::Rate,
        "percentile" | "quantile" => {
            let Some(p) = request.get("p").and_then(|v| v.as_f64()) else {
                return Err("percentile requires a 'p' (0..=100)".to_string());
            };
            Agg::Percentile(p)
        }
        // Shorthand p50 / p90 / p95 / p99 / p99.9 …
        s if s.starts_with('p') && s[1..].parse::<f64>().is_ok() => {
            Agg::Percentile(s[1..].parse::<f64>().unwrap())
        }
        other => return Err(format!("unknown agg: {other:?}")),
    };
    let spec = QuerySpec {
        measurement: measurement.to_string(),
        field: field.to_string(),
        tag_filters,
        start: request
            .get("start")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MIN / 2),
        end: request
            .get("end")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX / 2),
        group_tags,
        interval: request
            .get("interval")
            .and_then(|v| v.as_i64())
            .filter(|&i| i > 0),
        agg,
    };
    let db = engine.read().unwrap();
    // A text field goes through the string query path (first/last/count/distinct).
    if db.is_string_field(measurement, field) {
        let out: Vec<Value> = db
            .query_str(&spec)
            .into_iter()
            .map(|r| {
                let tags: serde_json::Map<String, Value> = r
                    .tags
                    .into_iter()
                    .map(|(k, v)| (k, Value::String(v)))
                    .collect();
                json!({
                    "tags": tags,
                    "type": "string",
                    "points": r.points.into_iter().map(|p| {
                        let value = match p.value {
                            oxidb_tsdb::StrValue::Text(t) => Value::String(t),
                            oxidb_tsdb::StrValue::Num(n) => json!(n),
                        };
                        json!({ "ts": p.ts, "value": value })
                    }).collect::<Vec<_>>(),
                })
            })
            .collect();
        return Ok(json!(out));
    }
    let out: Vec<Value> = db
        .query(&spec)
        .into_iter()
        .map(|r| {
            let tags: serde_json::Map<String, Value> = r
                .tags
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();
            json!({
                "tags": tags,
                "type": r.ftype.as_str(),
                "points": r.points.into_iter()
                    .map(|p| json!({ "ts": p.ts, "value": p.value }))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(json!(out))
}

// ---------------------------------------------------------------------------
// PostgREST bridge helpers (ADR-0019) — clean Result-returning entry points
// ---------------------------------------------------------------------------

/// Run a TSDB query for the PostgREST surface, returning the series array
/// (`[{tags, type, points:[{ts,value}]}, …]`) or an error message. `request`
/// is the same shape the `query` op accepts (measurement/field/tags/start/end/
/// agg/interval/group_by).
pub fn tsdb_query_json(db_name: &str, request: &Value) -> Result<Value, String> {
    let engine = engine_for(db_name)?;
    query_core(&engine, request)
}

/// Write points for the PostgREST surface, returning the count. `request` is
/// the same shape the `write` op accepts (a `points` array).
pub fn tsdb_write_json(db_name: &str, request: &Value) -> Result<usize, String> {
    let engine = engine_for(db_name)?;
    write_points_core(&engine, request)
}
