//! PostgREST-compatible surface over the **TSDB engine** (ADR-0019, Phase 2d).
//!
//! The time-series engine is aggregation-shaped (measurement × tag-set × field,
//! queried by field + tag filters + time range + aggregate), not row-shaped, so
//! the mapping is a deliberate, honest fit rather than a 1:1 translation.
//!
//! **Engine selection** uses PostgREST's own schema mechanism: a request with
//! `Accept-Profile: tsdb` (reads) / `Content-Profile: tsdb` (writes) — what the
//! `postgrest-js` client emits for `.schema('tsdb')` — is routed here. This
//! also sidesteps the "measurement only exists after the first write" problem
//! that existence-based routing (used for SQL) would hit.
//!
//! **Read** — `GET /rest/v1/{measurement}` with `Accept-Profile: tsdb`:
//! - `select=<field>` names the field to query (required — TSDB aggregates one
//!   field at a time).
//! - `tag=eq.value` filters on a tag (equality only — that is all a tag
//!   predicate is).
//! - `ts=gte.<ms>` / `ts=lt.<ms>` (or `time`) set the `[start, end)` range.
//! - extension params `agg` (default `mean`; `p` for percentile), `interval`
//!   (GROUP BY time, ms) and `group_by` (tags) shape the aggregation.
//! - `order=ts.desc` and `limit` apply to the flattened output.
//! The series result is **flattened to rows** — one `{ts, value, <tags…>}` per
//! point — so it looks like every other PostgREST response.
//!
//! **Write** — `POST /rest/v1/{measurement}` with `Content-Profile: tsdb`:
//! a flat object `{ts, host:"web1", usage:0.5}` maps `ts`/`time` to the
//! timestamp (default now), string values to **tags**, numeric/bool values to
//! **fields**; a nested `{ts, tags:{…}, fields:{…}}` is honored as-is.
//!
//! **PATCH/DELETE** are `405` — the series store is append-only; expire data
//! with retention, not per-point deletes. Authorization is RBAC-only (the
//! parent `rest_permitted` gate), like the wire `tsdb` command.

use serde_json::{Value, json};

use super::postgrest::{max_rows, split_pairs};
use crate::s3::http::{HttpRequest, HttpResponse};

type PgResult<T> = Result<T, (u16, String)>;

/// `GET /rest/v1/{measurement}` with `Accept-Profile: tsdb`.
pub(super) fn handle_get(db: &str, measurement: &str, req: &HttpRequest) -> HttpResponse {
    let parsed = match build_query(measurement, &req.query) {
        Ok(p) => p,
        Err((s, m)) => return err(s, &m),
    };
    let series = match crate::tsdb_bridge::tsdb_query_json(db, &parsed.request) {
        Ok(v) => v,
        Err(e) => return err(400, &e),
    };
    let mut rows = flatten(&series);
    if let Some(desc) = parsed.order_desc {
        rows.sort_by(|a, b| {
            let ta = a.get("ts").and_then(|v| v.as_i64()).unwrap_or(0);
            let tb = b.get("ts").and_then(|v| v.as_i64()).unwrap_or(0);
            if desc { tb.cmp(&ta) } else { ta.cmp(&tb) }
        });
    }
    let cap = max_rows();
    let limit = parsed.limit.map_or(cap, |l| l.min(cap)) as usize;
    rows.truncate(limit);

    let n = rows.len();
    let range = if n == 0 {
        "*/*".to_string()
    } else {
        format!("0-{}/*", n - 1)
    };
    super::json_response(200, "OK", Value::Array(rows)).with_header("Content-Range", &range)
}

/// `POST /rest/v1/{measurement}` with `Content-Profile: tsdb`.
pub(super) fn handle_post(db: &str, measurement: &str, req: &HttpRequest) -> HttpResponse {
    let body = match serde_json::from_slice::<Value>(&req.body) {
        Ok(v) => v,
        Err(_) => return err(400, "invalid JSON body"),
    };
    let objs: Vec<Value> = match body {
        Value::Array(a) => a,
        obj @ Value::Object(_) => vec![obj],
        _ => return err(400, "body must be an object or array of objects"),
    };
    if objs.is_empty() {
        return err(400, "empty write");
    }
    let mut points = Vec::with_capacity(objs.len());
    for obj in &objs {
        match to_point(measurement, obj) {
            Ok(p) => points.push(p),
            Err((s, m)) => return err(s, &m),
        }
    }
    let request = json!({ "points": points });
    match crate::tsdb_bridge::tsdb_write_json(db, &request) {
        Ok(_) => super::json_response(201, "Created", json!([])),
        Err(e) => err(400, &e),
    }
}

/// The series store is append-only — no per-point update/delete.
pub(super) fn handle_unsupported() -> HttpResponse {
    err(
        405,
        "the time-series engine is append-only; expire data with retention, not per-point PATCH/DELETE",
    )
}

// ---------------------------------------------------------------------------
// URL grammar → TSDB query request
// ---------------------------------------------------------------------------

struct ParsedQuery {
    request: Value,
    order_desc: Option<bool>,
    limit: Option<u64>,
}

/// Translate the query string into a `query` request Value plus the row-level
/// order/limit modifiers applied after flattening.
fn build_query(measurement: &str, query: &str) -> PgResult<ParsedQuery> {
    let mut field: Option<String> = None;
    let mut tags = serde_json::Map::new();
    let mut start: Option<i64> = None;
    let mut end: Option<i64> = None;
    let mut agg = "mean".to_string();
    let mut p: Option<f64> = None;
    let mut interval: Option<i64> = None;
    let mut group_by: Vec<Value> = Vec::new();
    let mut order_desc: Option<bool> = None;
    let mut limit: Option<u64> = None;

    for (key, val) in split_pairs(query) {
        match key.as_str() {
            "db" => {}
            "select" => field = Some(val),
            "agg" => agg = val,
            "p" => p = val.parse().ok(),
            "interval" => {
                interval = Some(
                    val.parse()
                        .map_err(|_| (400, "invalid 'interval' (ms)".to_string()))?,
                )
            }
            "group_by" => {
                group_by = val
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| Value::String(s.to_string()))
                    .collect()
            }
            "limit" => {
                limit = Some(
                    val.parse()
                        .map_err(|_| (400, "invalid 'limit'".to_string()))?,
                )
            }
            "order" => order_desc = Some(parse_order(&val)?),
            "ts" | "time" => {
                let (op, arg) = val
                    .split_once('.')
                    .ok_or((400, format!("time filter must be 'op.value', got '{val}'")))?;
                let ms: i64 = arg
                    .parse()
                    .map_err(|_| (400, format!("time value must be epoch ms, got '{arg}'")))?;
                match op {
                    "gte" | "gt" => start = Some(ms),
                    "lte" | "lt" => end = Some(ms),
                    other => {
                        return Err((400, format!("time supports gt/gte/lt/lte, got '{other}'")));
                    }
                }
            }
            // Any other column is a tag equality filter.
            tag => {
                let (op, arg) = val
                    .split_once('.')
                    .ok_or((400, format!("tag filter must be 'eq.value', got '{val}'")))?;
                if op != "eq" {
                    return Err((400, format!("tags support only 'eq', got '{op}'")));
                }
                tags.insert(tag.to_string(), Value::String(arg.to_string()));
            }
        }
    }

    let field = field.ok_or((
        400,
        "a tsdb read requires ?select=<field> (the field to aggregate)".to_string(),
    ))?;

    let mut request = serde_json::Map::new();
    request.insert("measurement".into(), json!(measurement));
    request.insert("field".into(), json!(field));
    request.insert("tags".into(), Value::Object(tags));
    request.insert("agg".into(), json!(agg));
    if let Some(s) = start {
        request.insert("start".into(), json!(s));
    }
    if let Some(e) = end {
        request.insert("end".into(), json!(e));
    }
    if let Some(i) = interval {
        request.insert("interval".into(), json!(i));
    }
    if let Some(p) = p {
        request.insert("p".into(), json!(p));
    }
    if !group_by.is_empty() {
        request.insert("group_by".into(), Value::Array(group_by));
    }

    Ok(ParsedQuery {
        request: Value::Object(request),
        order_desc,
        limit,
    })
}

/// `order=ts.desc` → `Some(true)`; `ts`/`ts.asc` → `Some(false)`. Only `ts`
/// (the time axis) is orderable.
fn parse_order(spec: &str) -> PgResult<bool> {
    let mut it = spec.split('.');
    let col = it.next().unwrap_or("");
    if col != "ts" && col != "time" {
        return Err((
            400,
            format!("a tsdb read can only order by 'ts', got '{col}'"),
        ));
    }
    match it.next() {
        None | Some("asc") => Ok(false),
        Some("desc") => Ok(true),
        Some(other) => Err((400, format!("bad order direction '{other}'"))),
    }
}

/// Flatten the series result (`[{tags, points:[{ts,value}]}, …]`) into one row
/// object per point, merging the series tags in.
fn flatten(series: &Value) -> Vec<Value> {
    let mut rows = Vec::new();
    let Some(arr) = series.as_array() else {
        return rows;
    };
    for s in arr {
        let tags = s.get("tags").and_then(|v| v.as_object());
        let Some(points) = s.get("points").and_then(|v| v.as_array()) else {
            continue;
        };
        for pt in points {
            let mut row = serde_json::Map::new();
            if let Some(tags) = tags {
                for (k, v) in tags {
                    row.insert(k.clone(), v.clone());
                }
            }
            if let Some(ts) = pt.get("ts") {
                row.insert("ts".into(), ts.clone());
            }
            if let Some(val) = pt.get("value") {
                row.insert("value".into(), val.clone());
            }
            rows.push(Value::Object(row));
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Write body → point
// ---------------------------------------------------------------------------

/// Map one request object to a TSDB point Value (`{measurement, ts, tags,
/// fields}`). Honors an explicit nested `tags`/`fields`, else splits a flat
/// object by value type (string → tag, numeric/bool → field).
fn to_point(measurement: &str, obj: &Value) -> PgResult<Value> {
    let Value::Object(map) = obj else {
        return Err((400, "each point must be a JSON object".to_string()));
    };
    let ts = map
        .get("ts")
        .or_else(|| map.get("time"))
        .and_then(|v| v.as_i64())
        .unwrap_or_else(now_ms);

    let (tags, fields) = if map.contains_key("tags") || map.contains_key("fields") {
        let tags = map.get("tags").cloned().unwrap_or_else(|| json!({}));
        let fields = map.get("fields").cloned().unwrap_or_else(|| json!({}));
        (tags, fields)
    } else {
        let mut tags = serde_json::Map::new();
        let mut fields = serde_json::Map::new();
        for (k, v) in map {
            if k == "ts" || k == "time" {
                continue;
            }
            if v.is_string() {
                tags.insert(k.clone(), v.clone());
            } else if v.is_number() || v.is_boolean() {
                fields.insert(k.clone(), v.clone());
            }
        }
        (Value::Object(tags), Value::Object(fields))
    };

    if fields.as_object().is_none_or(|f| f.is_empty()) {
        return Err((
            400,
            "a point needs at least one numeric/boolean field".to_string(),
        ));
    }

    Ok(json!({
        "measurement": measurement,
        "ts": ts,
        "tags": tags,
        "fields": fields,
    }))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn err(status: u16, message: &str) -> HttpResponse {
    let text = match status {
        400 => "Bad Request",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    super::json_response(status, text, json!({ "message": message }))
}

// ---------------------------------------------------------------------------
// Tests — grammar → request (no engine required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_query_basic() {
        let p = build_query("cpu", "select=usage&host=eq.web1&ts=gte.1000&ts=lt.5000").unwrap();
        assert_eq!(p.request["measurement"], json!("cpu"));
        assert_eq!(p.request["field"], json!("usage"));
        assert_eq!(p.request["tags"], json!({ "host": "web1" }));
        assert_eq!(p.request["start"], json!(1000));
        assert_eq!(p.request["end"], json!(5000));
        assert_eq!(p.request["agg"], json!("mean"));
    }

    #[test]
    fn build_query_agg_interval_group() {
        let p = build_query(
            "cpu",
            "select=usage&agg=max&interval=60000&group_by=host,region",
        )
        .unwrap();
        assert_eq!(p.request["agg"], json!("max"));
        assert_eq!(p.request["interval"], json!(60000));
        assert_eq!(p.request["group_by"], json!(["host", "region"]));
    }

    #[test]
    fn field_is_required() {
        assert!(build_query("cpu", "host=eq.web1").is_err());
    }

    #[test]
    fn tags_only_support_eq() {
        assert!(build_query("cpu", "select=usage&host=gt.web1").is_err());
    }

    #[test]
    fn order_and_limit() {
        let p = build_query("cpu", "select=usage&order=ts.desc&limit=10").unwrap();
        assert_eq!(p.order_desc, Some(true));
        assert_eq!(p.limit, Some(10));
    }

    #[test]
    fn order_only_by_ts() {
        assert!(build_query("cpu", "select=usage&order=value.desc").is_err());
    }

    #[test]
    fn flatten_merges_tags_into_rows() {
        let series = json!([
            { "tags": { "host": "web1" }, "type": "float",
              "points": [{ "ts": 1000, "value": 0.5 }, { "ts": 2000, "value": 0.7 }] }
        ]);
        let rows = flatten(&series);
        assert_eq!(
            rows,
            vec![
                json!({ "host": "web1", "ts": 1000, "value": 0.5 }),
                json!({ "host": "web1", "ts": 2000, "value": 0.7 }),
            ]
        );
    }

    #[test]
    fn to_point_flat_splits_by_type() {
        let pt = to_point("cpu", &json!({ "ts": 1000, "host": "web1", "usage": 0.5 })).unwrap();
        assert_eq!(pt["measurement"], json!("cpu"));
        assert_eq!(pt["ts"], json!(1000));
        assert_eq!(pt["tags"], json!({ "host": "web1" }));
        assert_eq!(pt["fields"], json!({ "usage": 0.5 }));
    }

    #[test]
    fn to_point_nested_form() {
        let pt = to_point(
            "cpu",
            &json!({ "ts": 5, "tags": { "host": "a" }, "fields": { "x": 1 } }),
        )
        .unwrap();
        assert_eq!(pt["tags"], json!({ "host": "a" }));
        assert_eq!(pt["fields"], json!({ "x": 1 }));
    }

    #[test]
    fn to_point_requires_a_field() {
        // All-string flat object → only tags, no field → rejected.
        assert!(to_point("cpu", &json!({ "ts": 1, "host": "a" })).is_err());
    }
}
