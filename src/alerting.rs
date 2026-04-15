//! Background alert evaluation engine for OxiDB.
//!
//! Periodically evaluates alert rules stored in the `_alerts` collection.
//! When a condition fires, executes configured actions (webhook, stderr, log to history).

use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::engine::OxiDb;
use crate::scheduler::{epoch_now, epoch_to_iso, parse_interval};

const ALERTS_COLLECTION: &str = "_alerts";
const HISTORY_COLLECTION: &str = "_alert_history";

/// Background alert evaluator loop.
/// Wakes every `interval`, loads enabled alerts, evaluates conditions, fires actions.
pub fn alert_loop(db: Arc<OxiDb>, rx: mpsc::Receiver<()>, interval: Duration) {
    loop {
        match rx.recv_timeout(interval) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        evaluate_alerts(&db);
    }
}

/// Evaluate all enabled alerts.
fn evaluate_alerts(db: &OxiDb) {
    let alerts = match db.find(ALERTS_COLLECTION, &json!({"enabled": true})) {
        Ok(a) => a,
        Err(_) => return,
    };

    let now = epoch_now();

    for alert in &alerts {
        if !should_evaluate(alert, now) {
            continue;
        }
        if let Some(result_value) = check_condition(db, alert, now) {
            execute_actions(db, alert, result_value, now);
            update_alert_state(db, alert, now);
        }
    }
}

/// Check if an alert should be evaluated (respects cooldown).
fn should_evaluate(alert: &Value, now: i64) -> bool {
    let last_fired = alert
        .get("last_fired_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cooldown = alert
        .get("cooldown_seconds")
        .and_then(|v| v.as_i64())
        .unwrap_or(300);

    now >= last_fired + cooldown
}

/// Evaluate the alert condition. Returns the result value if the condition fires.
fn check_condition(db: &OxiDb, alert: &Value, now: i64) -> Option<i64> {
    let collection = alert.get("collection")?.as_str()?;
    let condition = alert.get("condition")?;
    let cond_type = condition.get("type")?.as_str()?;

    match cond_type {
        "count_threshold" => check_count_threshold(db, collection, condition, now),
        "aggregation_threshold" => check_aggregation_threshold(db, collection, condition, now),
        _ => None,
    }
}

/// Count-based threshold: count docs matching query within window, compare to threshold.
fn check_count_threshold(
    db: &OxiDb,
    collection: &str,
    condition: &Value,
    now: i64,
) -> Option<i64> {
    let query = build_windowed_query(condition, now)?;
    let threshold = condition.get("threshold")?.as_i64()?;
    let operator = condition.get("operator").and_then(|v| v.as_str()).unwrap_or("gte");

    let count = db.count(collection, &query).ok()? as i64;

    if compare(count, threshold, operator) {
        Some(count)
    } else {
        None
    }
}

/// Aggregation-based threshold: run pipeline, extract result, compare to threshold.
fn check_aggregation_threshold(
    db: &OxiDb,
    collection: &str,
    condition: &Value,
    now: i64,
) -> Option<i64> {
    let pipeline = condition.get("pipeline")?.as_array()?;
    let threshold = condition.get("threshold")?.as_i64()?;
    let operator = condition.get("operator").and_then(|v| v.as_str()).unwrap_or("gte");
    let result_field = condition
        .get("result_field")
        .and_then(|v| v.as_str())
        .unwrap_or("count");

    // Prepend a $match stage with the time window
    let window_secs = parse_window(condition);
    let cutoff = epoch_to_iso(now - window_secs);
    let mut full_pipeline = vec![json!({"$match": {"_ts": {"$gte": cutoff}}})];
    for stage in pipeline {
        full_pipeline.push(stage.clone());
    }

    let pipeline_value = Value::Array(full_pipeline);
    let results = db.aggregate(collection, &pipeline_value).ok()?;
    let first = results.first()?;
    let value = first.get(result_field)?.as_i64()?;

    if compare(value, threshold, operator) {
        Some(value)
    } else {
        None
    }
}

/// Build a query with _ts window filter merged into the alert's query.
fn build_windowed_query(condition: &Value, now: i64) -> Option<Value> {
    let base_query = condition.get("query").cloned().unwrap_or(json!({}));
    let window_secs = parse_window(condition);
    let cutoff = epoch_to_iso(now - window_secs);

    let mut query = base_query.as_object()?.clone();
    query.insert("_ts".to_string(), json!({"$gte": cutoff}));
    Some(Value::Object(query))
}

/// Parse the window field (e.g., "5m", "1h", "30s") into seconds.
fn parse_window(condition: &Value) -> i64 {
    condition
        .get("window")
        .and_then(|v| v.as_str())
        .and_then(|s| parse_interval(s).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(300) // default 5 minutes
}

/// Compare value against threshold using the given operator.
fn compare(value: i64, threshold: i64, operator: &str) -> bool {
    match operator {
        "gt" => value > threshold,
        "gte" => value >= threshold,
        "lt" => value < threshold,
        "lte" => value <= threshold,
        "eq" => value == threshold,
        "ne" => value != threshold,
        _ => value >= threshold,
    }
}

/// Execute all actions for a fired alert.
fn execute_actions(db: &OxiDb, alert: &Value, result_value: i64, now: i64) {
    let name = alert.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
    let collection = alert.get("collection").and_then(|v| v.as_str()).unwrap_or("unknown");
    let condition = alert.get("condition").cloned().unwrap_or(json!({}));
    let threshold = condition.get("threshold").and_then(|v| v.as_i64()).unwrap_or(0);
    let operator = condition
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("gte");

    let actions = match alert.get("actions").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return,
    };

    let history_doc = json!({
        "alert_name": name,
        "collection": collection,
        "fired_at": epoch_to_iso(now),
        "fired_at_epoch": now,
        "condition_result": result_value,
        "threshold": threshold,
        "operator": operator,
    });

    let mut executed_actions = Vec::new();

    for action in actions {
        let action_type = match action.get("type").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => continue,
        };

        match action_type {
            "webhook" => {
                if let Some(url) = action.get("url").and_then(|v| v.as_str()) {
                    let payload = history_doc.clone();
                    let url = url.to_string();
                    // Fire-and-forget thread to avoid blocking the evaluator
                    std::thread::spawn(move || {
                        let _ = webhook_post(&url, &payload);
                    });
                    executed_actions.push("webhook");
                }
            }
            "stderr" => {
                eprintln!(
                    "[ALERT] {name}: {collection} — {result_value} {operator} {threshold}"
                );
                executed_actions.push("stderr");
            }
            "log" => {
                executed_actions.push("log");
            }
            _ => {}
        }
    }

    // Always write to _alert_history
    let mut history = history_doc;
    if let Some(obj) = history.as_object_mut() {
        obj.insert(
            "actions_executed".to_string(),
            json!(executed_actions),
        );
        obj.insert(
            "_ts".to_string(),
            Value::String(epoch_to_iso(now)),
        );
    }
    let _ = db.insert(HISTORY_COLLECTION, history);
}

/// Update the alert document after it fires.
fn update_alert_state(db: &OxiDb, alert: &Value, now: i64) {
    if let Some(name) = alert.get("name").and_then(|v| v.as_str()) {
        let _ = db.update(
            ALERTS_COLLECTION,
            &json!({"name": name}),
            &json!({"$set": {
                "last_fired": epoch_to_iso(now),
                "last_fired_epoch": now,
            }, "$inc": {"fire_count": 1}}),
        );
    }
}

/// Send a fire-and-forget HTTP POST to a webhook URL.
/// Supports `http://host:port/path` format. No TLS.
fn webhook_post(url: &str, payload: &Value) -> std::io::Result<()> {
    let (host, port, path) = parse_http_url(url)?;
    let body = serde_json::to_vec(payload).unwrap_or_default();

    let mut stream = TcpStream::connect_timeout(
        &format!("{host}:{port}").parse().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}"))
        })?,
        Duration::from_secs(5),
    )?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    write!(
        stream,
        "POST {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    Ok(())
}

/// Parse a simple HTTP URL into (host, port, path).
fn parse_http_url(url: &str) -> std::io::Result<(String, u16, String)> {
    let url = url
        .strip_prefix("http://")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "only http:// URLs supported"))?;

    let (host_port, path) = match url.find('/') {
        Some(i) => (&url[..i], &url[i..]),
        None => (url, "/"),
    };

    let (host, port) = match host_port.rfind(':') {
        Some(i) => (
            &host_port[..i],
            host_port[i + 1..]
                .parse::<u16>()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}")))?,
        ),
        None => (host_port, 80),
    };

    Ok((host.to_string(), port, path.to_string()))
}

/// Public wrapper for `build_windowed_query` (used by engine::test_alert).
pub fn build_windowed_query_pub(condition: &Value, now: i64) -> Option<Value> {
    build_windowed_query(condition, now)
}

/// Public wrapper for `compare` (used by engine::test_alert).
pub fn compare_pub(value: i64, threshold: i64, operator: &str) -> bool {
    compare(value, threshold, operator)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare() {
        assert!(compare(100, 50, "gt"));
        assert!(compare(100, 100, "gte"));
        assert!(!compare(100, 100, "gt"));
        assert!(compare(50, 100, "lt"));
        assert!(compare(100, 100, "eq"));
        assert!(compare(99, 100, "ne"));
    }

    #[test]
    fn test_parse_window() {
        let c = json!({"window": "5m"});
        assert_eq!(parse_window(&c), 300);

        let c = json!({"window": "1h"});
        assert_eq!(parse_window(&c), 3600);

        let c = json!({"window": "30s"});
        assert_eq!(parse_window(&c), 30);

        // Default
        let c = json!({});
        assert_eq!(parse_window(&c), 300);
    }

    #[test]
    fn test_should_evaluate_respects_cooldown() {
        let now = 1000;
        let alert = json!({"last_fired_epoch": 800, "cooldown_seconds": 300});
        assert!(!should_evaluate(&alert, now)); // 1000 < 800+300

        let alert = json!({"last_fired_epoch": 600, "cooldown_seconds": 300});
        assert!(should_evaluate(&alert, now)); // 1000 >= 600+300
    }

    #[test]
    fn test_should_evaluate_never_fired() {
        let alert = json!({"enabled": true});
        assert!(should_evaluate(&alert, 1000));
    }

    #[test]
    fn test_parse_http_url() {
        let (h, p, path) = parse_http_url("http://localhost:8080/webhook").unwrap();
        assert_eq!(h, "localhost");
        assert_eq!(p, 8080);
        assert_eq!(path, "/webhook");

        let (h, p, path) = parse_http_url("http://example.com/api/alerts").unwrap();
        assert_eq!(h, "example.com");
        assert_eq!(p, 80);
        assert_eq!(path, "/api/alerts");

        assert!(parse_http_url("https://secure.com/path").is_err());
    }

    #[test]
    fn test_build_windowed_query() {
        let condition = json!({
            "query": {"level": {"$lte": 3}},
            "window": "5m"
        });
        let now = epoch_now();
        let q = build_windowed_query(&condition, now).unwrap();
        assert!(q.get("level").is_some());
        assert!(q.get("_ts").is_some());
    }
}
