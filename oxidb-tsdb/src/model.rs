//! Time-series data model (InfluxDB-shaped): a *point* is a measurement, a set
//! of string **tags** (the series identity), one or more numeric **fields**,
//! and a millisecond timestamp. Internally each (measurement, tag-set, field)
//! is its own compressed stream — a **series**.

use serde::{Deserialize, Serialize};

/// Identity of one stored stream: measurement + sorted tags + field name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SeriesKey {
    pub measurement: String,
    /// Tag pairs, kept sorted by key so equal tag-sets hash equal.
    pub tags: Vec<(String, String)>,
    pub field: String,
}

impl SeriesKey {
    pub fn new(measurement: &str, mut tags: Vec<(String, String)>, field: &str) -> Self {
        tags.sort();
        SeriesKey {
            measurement: measurement.to_string(),
            tags,
            field: field.to_string(),
        }
    }

    pub fn tag(&self, key: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// A point to ingest: one timestamp, shared tags, several fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    pub measurement: String,
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, f64)>,
    pub ts: i64, // epoch millis
}

impl Point {
    pub fn new(measurement: &str, ts: i64) -> Self {
        Point {
            measurement: measurement.to_string(),
            tags: Vec::new(),
            fields: Vec::new(),
            ts,
        }
    }
    pub fn tag(mut self, k: &str, v: &str) -> Self {
        self.tags.push((k.to_string(), v.to_string()));
        self
    }
    pub fn field(mut self, k: &str, v: f64) -> Self {
        self.fields.push((k.to_string(), v));
        self
    }
}
