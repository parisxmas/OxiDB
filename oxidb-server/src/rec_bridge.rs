//! Bridge between the wire protocol and the recommendation engine
//! (`oxidb-rec`, ADR-0025) — the fourth engine, mounted exactly as the third
//! was: entirely separate storage, off by default, built lazily only when
//! `OXIDB_REC` is truthy. Routing (in [`crate::handler`]): a request whose
//! `engine` field is `"rec"` — or the reserved `rec` command — is served
//! here. Per-database, like SQL and TSDB.
//!
//! Wire (all under `cmd: "rec"`, an `op` selects the action):
//! ```json
//! { "engine":"rec", "cmd":"rec", "op":"track",
//!   "model":"purchase", "basket_id":9812, "items":["kahve","süt"], "ts":1700000000 }
//!
//! { "op":"related", "model":"purchase", "item":"kahve",
//!   "scoring":"llr", "half_life":2.0, "min_support":1.0, "limit":10 }
//!
//! { "op":"for_basket", "model":"purchase", "items":["kahve","süt"],
//!   "exclude":["poşet"], "limit":10 }
//!
//! { "op":"stats" }
//! { "op":"checkpoint" }
//! ```
//!
//! `ts` is epoch **seconds**, optional — the wall clock when absent. The
//! engine itself holds no clock (its tests demand exactness); the bridge is
//! where "now" enters.
//!
//! RBAC: `related` / `for_basket` / `stats` sit in the Read tier; `track` and
//! `checkpoint` need write (the `readonly` flag arrives from the session
//! layer, as with TSDB). Not replicated in v1 — ADR-0025 §8 records the gap
//! it shares with TSDB, and why the eventual fix is cheap (idempotent track,
//! commutative counters).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use oxidb::database_manager::DEFAULT_DATABASE;
use oxidb_rec::{Query, Rec, RecConfig, Scoring};
use serde_json::{Value, json};

use crate::handler::{err_bytes, ok_bytes};

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Per-database recommendation engines. A plain `Mutex`, not `RwLock`:
/// `track` is the hot path and takes `&mut` either way, and queries are
/// single-row lookups — contention shapes like TSDB's.
struct Registry {
    root: PathBuf,
    default_dir: PathBuf,
    engines: Mutex<HashMap<String, Arc<Mutex<Rec>>>>,
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
            if !env_truthy("OXIDB_REC") {
                return None;
            }
            let root = PathBuf::from(
                std::env::var("OXIDB_DATA").unwrap_or_else(|_| "./oxidb_data".into()),
            );
            let default_dir = std::env::var("OXIDB_REC_DATA")
                .map(PathBuf::from)
                .unwrap_or_else(|_| root.join("rec"));
            eprintln!(
                "[oxidb] recommendation engine enabled (default db at {})",
                default_dir.display()
            );
            Some(Registry {
                root,
                default_dir,
                engines: Mutex::new(HashMap::new()),
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

fn config_from_env() -> RecConfig {
    let mut c = RecConfig::default();
    if let Ok(v) = std::env::var("OXIDB_REC_BUCKET_SECS")
        && let Ok(n) = v.parse::<u64>()
        && n > 0
    {
        c.bucket_secs = n;
    }
    if let Ok(v) = std::env::var("OXIDB_REC_MAX_BASKET")
        && let Ok(n) = v.parse::<usize>()
        && n > 0
    {
        c.max_basket = n;
    }
    if let Ok(v) = std::env::var("OXIDB_REC_CHECKPOINT_BYTES")
        && let Ok(n) = v.parse::<u64>()
    {
        c.checkpoint_bytes = n;
    }
    c
}

fn engine_for(db_name: &str) -> Result<Arc<Mutex<Rec>>, String> {
    let Some(reg) = registry() else {
        return Err("recommendation engine is not enabled (set OXIDB_REC=1)".to_string());
    };
    let name = normalize(db_name);
    let mut engines = reg.engines.lock().unwrap();
    if let Some(e) = engines.get(name) {
        return Ok(Arc::clone(e));
    }
    let dir = if name == DEFAULT_DATABASE {
        reg.default_dir.clone()
    } else {
        let db_dir = reg.root.join(name);
        if !db_dir.is_dir() {
            return Err(format!("database not found: {name}"));
        }
        db_dir.join("rec")
    };
    let engine = Rec::open(&dir, config_from_env())
        .map_err(|e| format!("failed to open rec engine for database {name:?}: {e}"))?;
    let arc = Arc::new(Mutex::new(engine));
    engines.insert(name.to_string(), Arc::clone(&arc));
    Ok(arc)
}

/// Drop a database's rec engine (called by `drop_database`).
pub fn forget_database(db_name: &str) {
    if let Some(reg) = registry() {
        reg.engines.lock().unwrap().remove(normalize(db_name));
    }
}

fn parse_query(request: &Value) -> Result<Query, String> {
    let mut q = Query::default();
    if let Some(s) = request.get("scoring").and_then(|v| v.as_str()) {
        q.scoring = s.parse::<Scoring>()?;
    }
    if let Some(h) = request.get("half_life").and_then(|v| v.as_f64()) {
        q.half_life = h;
    }
    if let Some(m) = request.get("min_support").and_then(|v| v.as_f64()) {
        q.min_support = m;
    }
    if let Some(l) = request.get("limit").and_then(|v| v.as_u64()) {
        q.limit = l as usize;
    }
    Ok(q)
}

fn str_list<'a>(request: &'a Value, key: &str) -> Result<Vec<&'a str>, String> {
    match request.get(key) {
        None => Ok(Vec::new()),
        Some(Value::Array(a)) => a
            .iter()
            .map(|v| v.as_str().ok_or_else(|| format!("'{key}' must be strings")))
            .collect(),
        Some(_) => Err(format!("'{key}' must be an array of strings")),
    }
}

pub fn handle_rec(cmd: &str, request: &Value, readonly: bool, db_name: &str) -> Vec<u8> {
    if cmd != "rec" {
        return err_bytes(&format!("unknown rec command: {cmd:?}"));
    }
    let Some(op) = request.get("op").and_then(|v| v.as_str()) else {
        return err_bytes("missing 'op' (track, related, for_basket, stats, checkpoint)");
    };
    match dispatch(op, request, readonly, db_name) {
        Ok(v) => ok_bytes(v),
        Err(m) => err_bytes(&m),
    }
}

/// The op dispatch behind both surfaces: the wire (`handle_rec`) and the
/// COBRA extension methods (`db.rec_*`, ADR-0025 Phase 4) — one
/// implementation, one validation.
pub fn native_call(
    op: &str,
    request: &Value,
    readonly: bool,
    db_name: &str,
) -> Result<Value, String> {
    dispatch(op, request, readonly, db_name)
}

fn dispatch(op: &str, request: &Value, readonly: bool, db_name: &str) -> Result<Value, String> {
    let engine = engine_for(db_name)?;
    let ts = request
        .get("ts")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_secs);
    let model = request.get("model").and_then(|v| v.as_str());

    match op {
        "track" => {
            if readonly {
                return Err("permission denied: track requires write access".into());
            }
            let model = model.ok_or("missing 'model'")?;
            let basket_id = request
                .get("basket_id")
                .and_then(|v| v.as_u64())
                .ok_or("missing 'basket_id'")?;
            let items = str_list(request, "items")?;
            if items.is_empty() {
                return Err("missing 'items'".into());
            }
            let counted = engine.lock().unwrap().track(model, basket_id, &items, ts);
            Ok(json!({ "counted": counted }))
        }
        "related" => {
            let model = model.ok_or("missing 'model'")?;
            let item = request
                .get("item")
                .and_then(|v| v.as_str())
                .ok_or("missing 'item'")?;
            let q = parse_query(request)?;
            let recs = engine
                .lock()
                .unwrap()
                .related(model, item, ts, &q)
                .map_err(|e| e.to_string())?;
            Ok(json!({ "recommendations": recs }))
        }
        "for_basket" => {
            let model = model.ok_or("missing 'model'")?;
            let items = str_list(request, "items")?;
            if items.is_empty() {
                return Err("missing 'items'".into());
            }
            let exclude = str_list(request, "exclude")?;
            let q = parse_query(request)?;
            let recs = engine
                .lock()
                .unwrap()
                .for_basket(model, &items, &exclude, ts, &q)
                .map_err(|e| e.to_string())?;
            Ok(json!({ "recommendations": recs }))
        }
        "stats" => Ok(engine.lock().unwrap().stats()),
        "checkpoint" => {
            if readonly {
                return Err("permission denied: checkpoint requires write access".into());
            }
            engine
                .lock()
                .unwrap()
                .checkpoint(ts)
                .map_err(|e| e.to_string())?;
            Ok(json!({ "ok": true }))
        }
        other => Err(format!(
            "unknown rec op: {other:?} (track, related, for_basket, stats, checkpoint)"
        )),
    }
}
