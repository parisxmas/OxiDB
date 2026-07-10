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

/// The logical type of a field. Values are stored as `f64` internally (ints are
/// exact to 2^53, bools are 0/1); the type is remembered per series so queries
/// can report and render values correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FieldType {
    #[default]
    Float,
    Int,
    Bool,
}

impl FieldType {
    pub fn as_str(self) -> &'static str {
        match self {
            FieldType::Float => "float",
            FieldType::Int => "integer",
            FieldType::Bool => "boolean",
        }
    }
    pub fn to_u8(self) -> u8 {
        match self {
            FieldType::Float => 0,
            FieldType::Int => 1,
            FieldType::Bool => 2,
        }
    }
    pub fn from_u8(b: u8) -> FieldType {
        match b {
            1 => FieldType::Int,
            2 => FieldType::Bool,
            _ => FieldType::Float,
        }
    }
}

/// A typed field value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Int(i64),
    Bool(bool),
}

impl FieldValue {
    pub fn as_f64(self) -> f64 {
        match self {
            FieldValue::Float(f) => f,
            FieldValue::Int(i) => i as f64,
            FieldValue::Bool(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
        }
    }
    pub fn ftype(self) -> FieldType {
        match self {
            FieldValue::Float(_) => FieldType::Float,
            FieldValue::Int(_) => FieldType::Int,
            FieldValue::Bool(_) => FieldType::Bool,
        }
    }
}

/// A point to ingest: one timestamp, shared tags, several fields.
#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, FieldValue)>,
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
    /// Add a float field.
    pub fn field(mut self, k: &str, v: f64) -> Self {
        self.fields.push((k.to_string(), FieldValue::Float(v)));
        self
    }
    pub fn field_int(mut self, k: &str, v: i64) -> Self {
        self.fields.push((k.to_string(), FieldValue::Int(v)));
        self
    }
    pub fn field_bool(mut self, k: &str, v: bool) -> Self {
        self.fields.push((k.to_string(), FieldValue::Bool(v)));
        self
    }
}
