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
use oxidb_tsdb::{Agg, Point, QuerySpec, TagPredicate, Tsdb};
use serde_json::{Value, json};

use crate::handler::{err_bytes, ok_bytes};

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
            let root =
                PathBuf::from(std::env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".into()));
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
    if cmd != "tsdb" {
        return err_bytes("TSDB engine requests must use cmd \"tsdb\"");
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
        "" => err_bytes("missing 'op' (write | query | stats | retention | checkpoint)"),
        other => err_bytes(&format!("unknown tsdb op: {other:?}")),
    }
}

fn write_points(engine: &Arc<RwLock<Tsdb>>, request: &Value) -> Vec<u8> {
    let Some(points) = request.get("points").and_then(|v| v.as_array()) else {
        return err_bytes("write requires a 'points' array");
    };
    let mut db = engine.write().unwrap();
    let mut written = 0usize;
    for p in points {
        let Some(measurement) = p.get("measurement").and_then(|v| v.as_str()) else {
            return err_bytes("each point needs a 'measurement'");
        };
        let Some(ts) = p.get("ts").and_then(|v| v.as_i64()) else {
            return err_bytes("each point needs an integer 'ts' (epoch ms)");
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
            return err_bytes("each point needs a 'fields' object");
        };
        for (k, v) in fields {
            // JSON bool → boolean field, integer → integer field, else float.
            if let Some(b) = v.as_bool() {
                point = point.field_bool(k, b);
            } else if v.is_i64() {
                point = point.field_int(k, v.as_i64().unwrap());
            } else if let Some(f) = v.as_f64() {
                point = point.field(k, f);
            }
        }
        db.write(&point);
        written += 1;
    }
    ok_bytes(json!({ "written": written }))
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
    let Some(measurement) = request.get("measurement").and_then(|v| v.as_str()) else {
        return err_bytes("query requires a 'measurement'");
    };
    let Some(field) = request.get("field").and_then(|v| v.as_str()) else {
        return err_bytes("query requires a 'field'");
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
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let agg_str = request.get("agg").and_then(|v| v.as_str()).unwrap_or("mean");
    let agg = match agg_str {
        "mean" | "avg" => Agg::Mean,
        "sum" => Agg::Sum,
        "min" => Agg::Min,
        "max" => Agg::Max,
        "count" => Agg::Count,
        "first" => Agg::First,
        "last" => Agg::Last,
        "rate" => Agg::Rate,
        "percentile" | "quantile" => {
            let Some(p) = request.get("p").and_then(|v| v.as_f64()) else {
                return err_bytes("percentile requires a 'p' (0..=100)");
            };
            Agg::Percentile(p)
        }
        // Shorthand p50 / p90 / p95 / p99 / p99.9 …
        s if s.starts_with('p') && s[1..].parse::<f64>().is_ok() => {
            Agg::Percentile(s[1..].parse::<f64>().unwrap())
        }
        other => return err_bytes(&format!("unknown agg: {other:?}")),
    };
    let spec = QuerySpec {
        measurement: measurement.to_string(),
        field: field.to_string(),
        tag_filters,
        start: request.get("start").and_then(|v| v.as_i64()).unwrap_or(i64::MIN / 2),
        end: request.get("end").and_then(|v| v.as_i64()).unwrap_or(i64::MAX / 2),
        group_tags,
        interval: request.get("interval").and_then(|v| v.as_i64()).filter(|&i| i > 0),
        agg,
    };
    let results = engine.read().unwrap().query(&spec);
    let out: Vec<Value> = results
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
    ok_bytes(json!(out))
}
