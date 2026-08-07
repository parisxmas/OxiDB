//! MQTT → collection bridge: a publish on a mapped topic becomes a
//! queryable document, declaratively.
//!
//! `OXIDB_MQTT_INGEST` is a comma-separated list of `topic-filter:collection`
//! routes, MQTT wildcards included:
//!
//! ```text
//! OXIDB_MQTT_INGEST="fleet/+/pos:pings,sensors/#:sensor_readings"
//! ```
//!
//! A JSON-object payload becomes the document itself (so `{"lat": ..}` is
//! queryable as `lat`, not `payload.lat`); any other payload is wrapped as
//! `{"payload": ...}`. Either way the document gains `_topic` (the concrete
//! topic published to — with a wildcard route that is how rows are told
//! apart) and `_received_at` (epoch ms). The insert happens before the
//! QoS ack in the broker, so an acknowledged publish is durably in the
//! collection too — the same write-before-ack contract the broker itself
//! honours (ADR-0015). Unconfigured cost: one atomic load per publish.
//!
//! A malformed route spec refuses to start the server — a typo must not
//! become a bridge that silently never fires.

use std::sync::{Arc, OnceLock};

use oxidb::OxiDb;
use serde_json::{Value, json};

/// MQTT topic-filter matching: `/`-separated levels, `+` matches exactly
/// one level, a trailing `#` matches the rest (including nothing).
pub fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    fn rec(f: &[&str], t: &[&str]) -> bool {
        match (f.first(), t.first()) {
            (None, None) => true,
            (Some(&"#"), _) => true,
            (Some(&"+"), Some(_)) => rec(&f[1..], &t[1..]),
            (Some(&w), Some(&tw)) if w == tw => rec(&f[1..], &t[1..]),
            _ => false,
        }
    }
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    rec(&f, &t)
}

pub struct MqttIngest {
    routes: Vec<(String, String)>,
    db: Arc<OxiDb>,
}

impl MqttIngest {
    /// Parse `filter:collection[,filter:collection...]`. The split is on the
    /// LAST `:` of each entry — an MQTT topic level may itself contain one.
    pub fn parse(spec: &str, db: Arc<OxiDb>) -> Result<MqttIngest, String> {
        let mut routes = Vec::new();
        for entry in spec.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let (filter, collection) = entry.rsplit_once(':').ok_or_else(|| {
                format!("OXIDB_MQTT_INGEST entry `{entry}` is not `topic-filter:collection`")
            })?;
            let (filter, collection) = (filter.trim(), collection.trim());
            if filter.is_empty() || collection.is_empty() {
                return Err(format!(
                    "OXIDB_MQTT_INGEST entry `{entry}` has an empty topic filter or collection"
                ));
            }
            // `#` only as the last level — mid-filter it silently matches
            // nothing per spec, which is exactly the typo this refuses.
            let levels: Vec<&str> = filter.split('/').collect();
            if levels[..levels.len() - 1].contains(&"#") {
                return Err(format!(
                    "OXIDB_MQTT_INGEST filter `{filter}`: `#` is only valid as the last level"
                ));
            }
            routes.push((filter.to_string(), collection.to_string()));
        }
        if routes.is_empty() {
            return Err("OXIDB_MQTT_INGEST is set but contains no routes".to_string());
        }
        Ok(MqttIngest { db, routes })
    }

    /// Route one published message into every matching collection.
    pub fn ingest(&self, topic: &str, message: &str) {
        for (filter, collection) in &self.routes {
            if !mqtt_topic_matches(filter, topic) {
                continue;
            }
            let mut doc = match serde_json::from_str::<Value>(message) {
                Ok(Value::Object(map)) => Value::Object(map),
                Ok(other) => json!({ "payload": other }),
                Err(_) => json!({ "payload": message }),
            };
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_topic".to_string(), json!(topic));
                obj.insert("_received_at".to_string(), json!(now_ms()));
            }
            if let Err(e) = self.db.insert(collection, doc) {
                eprintln!("[mqtt] ingest into '{collection}' failed: {e}");
            }
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

static GLOBAL: OnceLock<MqttIngest> = OnceLock::new();

/// Install the bridge (once, at startup). Errors are the caller's to
/// surface — the server refuses to start on a malformed spec.
pub fn init(spec: &str, db: Arc<OxiDb>) -> Result<usize, String> {
    let ingest = MqttIngest::parse(spec, db)?;
    let n = ingest.routes.len();
    GLOBAL
        .set(ingest)
        .map_err(|_| "MQTT ingest bridge initialized twice".to_string())?;
    Ok(n)
}

/// The broker's hook: one atomic load when unconfigured.
pub fn ingest(topic: &str, message: &str) {
    if let Some(bridge) = GLOBAL.get() {
        bridge.ingest(topic, message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_filters_match_like_mqtt() {
        assert!(mqtt_topic_matches("fleet/+/pos", "fleet/u42/pos"));
        assert!(!mqtt_topic_matches("fleet/+/pos", "fleet/u42/speed"));
        assert!(!mqtt_topic_matches("fleet/+/pos", "fleet/u42/a/pos"));
        assert!(mqtt_topic_matches("sensors/#", "sensors/room1/temp"));
        assert!(mqtt_topic_matches("sensors/#", "sensors"));
        assert!(mqtt_topic_matches("#", "anything/at/all"));
        assert!(mqtt_topic_matches("exact/topic", "exact/topic"));
        assert!(!mqtt_topic_matches("exact/topic", "exact/other"));
    }

    #[test]
    fn malformed_specs_are_refused_by_name() {
        let db = Arc::new(OxiDb::open_in_memory().unwrap());
        assert!(MqttIngest::parse("no-colon-here", Arc::clone(&db)).is_err());
        assert!(MqttIngest::parse("a/b:", Arc::clone(&db)).is_err());
        assert!(MqttIngest::parse(":pings", Arc::clone(&db)).is_err());
        assert!(MqttIngest::parse("", Arc::clone(&db)).is_err());
        // `#` mid-filter matches nothing per spec — a typo, refused.
        assert!(MqttIngest::parse("a/#/b:pings", Arc::clone(&db)).is_err());
        assert!(MqttIngest::parse("fleet/+/pos:pings", db).is_ok());
    }

    #[test]
    fn published_messages_become_queryable_documents() {
        let db = Arc::new(OxiDb::open_in_memory().unwrap());
        let bridge =
            MqttIngest::parse("fleet/+/pos:pings,sensors/#:readings", Arc::clone(&db)).unwrap();

        // JSON object payload: fields land at the top level.
        bridge.ingest("fleet/u42/pos", r#"{"lat": 41.0, "lon": 29.0}"#);
        // Non-object payload: wrapped.
        bridge.ingest("sensors/room1/temp", "23.5");
        // Unmapped topic: nothing written anywhere.
        bridge.ingest("chat/general", r#"{"msg": "hi"}"#);

        let pings = db.find("pings", &json!({})).unwrap();
        assert_eq!(pings.len(), 1);
        assert_eq!(pings[0]["lat"], 41.0);
        assert_eq!(pings[0]["_topic"], "fleet/u42/pos");
        assert!(pings[0]["_received_at"].as_u64().unwrap() > 0);

        let readings = db.find("readings", &json!({})).unwrap();
        assert_eq!(readings.len(), 1);
        assert_eq!(readings[0]["payload"], 23.5);

        assert_eq!(db.find("chat", &json!({})).unwrap().len(), 0);
        // The queryability point: a real filter over ingested fields.
        let hits = db.find("pings", &json!({"lat": {"$gte": 40.0}})).unwrap();
        assert_eq!(hits.len(), 1);
    }
}
