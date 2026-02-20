use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Write};

use serde_json::Value as JsonValue;

/// Index value with type-aware ordering.
/// Dates are stored as i64 millisecond timestamps for fast comparison —
/// this is the core advantage over PostgreSQL JSONB which stores dates as text.
#[derive(Debug, Clone)]
pub enum IndexValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    DateTime(i64), // millis since epoch — fast integer comparison
    String(String),
}

impl Eq for IndexValue {}

impl Hash for IndexValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            IndexValue::Null => {}
            IndexValue::Boolean(b) => b.hash(state),
            IndexValue::Integer(i) => i.hash(state),
            IndexValue::Float(f) => f.to_bits().hash(state),
            IndexValue::DateTime(ms) => ms.hash(state),
            IndexValue::String(s) => s.hash(state),
        }
    }
}

impl PartialEq for IndexValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for IndexValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use IndexValue::*;
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,

            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Boolean(_), _) => Ordering::Less,
            (_, Boolean(_)) => Ordering::Greater,

            (Integer(a), Integer(b)) => a.cmp(b),
            (Integer(a), Float(b)) => (*a as f64).total_cmp(b),
            (Float(a), Integer(b)) => a.total_cmp(&(*b as f64)),
            (Float(a), Float(b)) => a.total_cmp(b),
            (Integer(_) | Float(_), _) => Ordering::Less,
            (_, Integer(_) | Float(_)) => Ordering::Greater,

            (DateTime(a), DateTime(b)) => a.cmp(b),
            (DateTime(_), _) => Ordering::Less,
            (_, DateTime(_)) => Ordering::Greater,

            (String(a), String(b)) => a.cmp(b),
        }
    }
}

impl IndexValue {
    /// Convert a JSON value to an IndexValue.
    /// String values are automatically checked for date formats and stored
    /// as DateTime(millis) for fast numeric comparison.
    pub fn from_json(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => IndexValue::Null,
            JsonValue::Bool(b) => IndexValue::Boolean(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    IndexValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    IndexValue::Float(f)
                } else {
                    IndexValue::Null
                }
            }
            JsonValue::String(s) => Self::parse_string(s),
            // Arrays/objects: serialize to string for indexing
            other => IndexValue::String(other.to_string()),
        }
    }

    pub fn parse_string(s: &str) -> Self {
        // Fast path: skip date parsing for strings that don't look like dates.
        // Valid date strings start with YYYY-MM (4 digits + '-' + 2 digits).
        let b = s.as_bytes();
        if b.len() < 10
            || !b[0].is_ascii_digit()
            || !b[1].is_ascii_digit()
            || !b[2].is_ascii_digit()
            || !b[3].is_ascii_digit()
            || b[4] != b'-'
            || !b[5].is_ascii_digit()
            || !b[6].is_ascii_digit()
        {
            return IndexValue::String(s.to_string());
        }

        // Try RFC 3339 / ISO 8601 with timezone: "2024-01-15T10:30:00Z"
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return IndexValue::DateTime(dt.timestamp_millis());
        }
        // Try ISO 8601 without timezone: "2024-01-15T10:30:00"
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            return IndexValue::DateTime(dt.and_utc().timestamp_millis());
        }
        // Try space-separated datetime: "2024-01-15 10:30:00"
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return IndexValue::DateTime(dt.and_utc().timestamp_millis());
        }
        // Try date only: "2024-01-15"
        if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                return IndexValue::DateTime(dt.and_utc().timestamp_millis());
            }
        }
        IndexValue::String(s.to_string())
    }

    /// Convert this IndexValue back to a JSON value.
    pub fn to_json(&self) -> JsonValue {
        match self {
            IndexValue::Null => JsonValue::Null,
            IndexValue::Boolean(b) => JsonValue::Bool(*b),
            IndexValue::Integer(i) => JsonValue::Number((*i).into()),
            IndexValue::Float(f) => {
                serde_json::Number::from_f64(*f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
            IndexValue::DateTime(ms) => {
                // Convert back to ISO 8601 string
                let secs = ms / 1000;
                let nsecs = ((ms % 1000).abs() as u32) * 1_000_000;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                    JsonValue::String(dt.to_rfc3339())
                } else {
                    JsonValue::Number((*ms).into())
                }
            }
            IndexValue::String(s) => JsonValue::String(s.clone()),
        }
    }

    /// Check if this value matches a JSON value for query comparison.
    /// Handles cross-type matching (e.g., DateTime index vs string query).
    pub fn matches_json(&self, json: &JsonValue) -> bool {
        let other = IndexValue::from_json(json);
        self == &other
    }

    // -- Binary serialization -------------------------------------------------

    const TAG_NULL: u8 = 0;
    const TAG_BOOL: u8 = 1;
    const TAG_INT: u8 = 2;
    const TAG_FLOAT: u8 = 3;
    const TAG_DATETIME: u8 = 4;
    const TAG_STRING: u8 = 5;

    /// Serialize this value to a binary writer.
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            IndexValue::Null => w.write_all(&[Self::TAG_NULL]),
            IndexValue::Boolean(b) => {
                w.write_all(&[Self::TAG_BOOL])?;
                w.write_all(&[*b as u8])
            }
            IndexValue::Integer(i) => {
                w.write_all(&[Self::TAG_INT])?;
                w.write_all(&i.to_le_bytes())
            }
            IndexValue::Float(f) => {
                w.write_all(&[Self::TAG_FLOAT])?;
                w.write_all(&f.to_le_bytes())
            }
            IndexValue::DateTime(ms) => {
                w.write_all(&[Self::TAG_DATETIME])?;
                w.write_all(&ms.to_le_bytes())
            }
            IndexValue::String(s) => {
                w.write_all(&[Self::TAG_STRING])?;
                let bytes = s.as_bytes();
                w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                w.write_all(bytes)
            }
        }
    }

    /// Deserialize a value from a binary reader.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        match tag[0] {
            Self::TAG_NULL => Ok(IndexValue::Null),
            Self::TAG_BOOL => {
                let mut b = [0u8; 1];
                r.read_exact(&mut b)?;
                Ok(IndexValue::Boolean(b[0] != 0))
            }
            Self::TAG_INT => {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                Ok(IndexValue::Integer(i64::from_le_bytes(buf)))
            }
            Self::TAG_FLOAT => {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                Ok(IndexValue::Float(f64::from_le_bytes(buf)))
            }
            Self::TAG_DATETIME => {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                Ok(IndexValue::DateTime(i64::from_le_bytes(buf)))
            }
            Self::TAG_STRING => {
                let mut len_buf = [0u8; 4];
                r.read_exact(&mut len_buf)?;
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut str_buf = vec![0u8; len];
                r.read_exact(&mut str_buf)?;
                let s = String::from_utf8(str_buf).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, e)
                })?;
                Ok(IndexValue::String(s))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown IndexValue tag: {}", tag[0]),
            )),
        }
    }

    /// Returns the immediate successor in the ordering, if computable.
    /// Used by composite index range queries for efficient reverse iteration.
    pub fn try_successor(&self) -> Option<IndexValue> {
        match self {
            IndexValue::Null => Some(IndexValue::Boolean(false)),
            IndexValue::Boolean(false) => Some(IndexValue::Boolean(true)),
            IndexValue::Boolean(true) => Some(IndexValue::Integer(i64::MIN)),
            IndexValue::Integer(n) if *n < i64::MAX => Some(IndexValue::Integer(n + 1)),
            IndexValue::DateTime(n) if *n < i64::MAX => Some(IndexValue::DateTime(n + 1)),
            IndexValue::DateTime(_) => Some(IndexValue::String(String::new())),
            IndexValue::String(s) => {
                // '\0' is the minimum byte, so s + "\0" is the immediate successor of s.
                let mut next = s.clone();
                next.push('\0');
                Some(IndexValue::String(next))
            }
            _ => None, // Float and i64::MAX Integer: complex edge cases
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_parsing() {
        let v = IndexValue::from_json(&JsonValue::String("2024-01-15T10:30:00Z".into()));
        assert!(matches!(v, IndexValue::DateTime(_)));
    }

    #[test]
    fn date_ordering() {
        let a = IndexValue::from_json(&JsonValue::String("2024-01-01".into()));
        let b = IndexValue::from_json(&JsonValue::String("2024-06-15".into()));
        assert!(a < b);
    }

    #[test]
    fn type_ordering() {
        let null = IndexValue::Null;
        let boolean = IndexValue::Boolean(true);
        let integer = IndexValue::Integer(42);
        let date = IndexValue::DateTime(1000);
        let string = IndexValue::String("hello".into());
        assert!(null < boolean);
        assert!(boolean < integer);
        assert!(integer < date);
        assert!(date < string);
    }

    #[test]
    fn date_only_parsing() {
        let v = IndexValue::from_json(&JsonValue::String("2024-01-15".into()));
        assert!(matches!(v, IndexValue::DateTime(_)));
    }

    #[test]
    fn datetime_without_tz() {
        let v = IndexValue::from_json(&JsonValue::String("2024-01-15T10:30:00".into()));
        assert!(matches!(v, IndexValue::DateTime(_)));
    }

    #[test]
    fn datetime_space_separated() {
        let v = IndexValue::from_json(&JsonValue::String("2024-01-15 10:30:00".into()));
        assert!(matches!(v, IndexValue::DateTime(_)));
    }

    #[test]
    fn non_date_string_stays_string() {
        let v = IndexValue::from_json(&JsonValue::String("hello world".into()));
        assert!(matches!(v, IndexValue::String(_)));
    }

    #[test]
    fn short_string_not_date() {
        let v = IndexValue::from_json(&JsonValue::String("hi".into()));
        assert!(matches!(v, IndexValue::String(_)));
    }

    #[test]
    fn integer_from_json() {
        let v = IndexValue::from_json(&serde_json::json!(42));
        assert_eq!(v, IndexValue::Integer(42));
    }

    #[test]
    fn float_from_json() {
        let v = IndexValue::from_json(&serde_json::json!(3.14));
        assert!(matches!(v, IndexValue::Float(_)));
    }

    #[test]
    fn null_from_json() {
        let v = IndexValue::from_json(&JsonValue::Null);
        assert_eq!(v, IndexValue::Null);
    }

    #[test]
    fn bool_from_json() {
        let v = IndexValue::from_json(&serde_json::json!(true));
        assert_eq!(v, IndexValue::Boolean(true));
    }

    #[test]
    fn integer_float_cross_type_comparison() {
        let i = IndexValue::Integer(42);
        let f = IndexValue::Float(42.0);
        assert_eq!(i, f);
    }

    #[test]
    fn integer_float_ordering() {
        let i = IndexValue::Integer(5);
        let f = IndexValue::Float(5.5);
        assert!(i < f);
    }

    #[test]
    fn boolean_ordering() {
        let f = IndexValue::Boolean(false);
        let t = IndexValue::Boolean(true);
        assert!(f < t);
    }

    #[test]
    fn string_lexicographic_ordering() {
        let a = IndexValue::String("apple".into());
        let b = IndexValue::String("banana".into());
        assert!(a < b);
    }

    #[test]
    fn matches_json_date_string() {
        let dt = IndexValue::from_json(&JsonValue::String("2024-06-15".into()));
        assert!(dt.matches_json(&JsonValue::String("2024-06-15".into())));
    }

    #[test]
    fn array_serialized_to_string() {
        let v = IndexValue::from_json(&serde_json::json!([1, 2, 3]));
        assert!(matches!(v, IndexValue::String(_)));
    }

    #[test]
    fn negative_integer() {
        let v = IndexValue::from_json(&serde_json::json!(-10));
        assert_eq!(v, IndexValue::Integer(-10));
        assert!(v < IndexValue::Integer(0));
    }

    #[test]
    fn binary_roundtrip_all_variants() {
        let values = vec![
            IndexValue::Null,
            IndexValue::Boolean(false),
            IndexValue::Boolean(true),
            IndexValue::Integer(0),
            IndexValue::Integer(-42),
            IndexValue::Integer(i64::MAX),
            IndexValue::Integer(i64::MIN),
            IndexValue::Float(3.14),
            IndexValue::Float(f64::NEG_INFINITY),
            IndexValue::DateTime(1_700_000_000_000),
            IndexValue::DateTime(-1_000),
            IndexValue::String(String::new()),
            IndexValue::String("hello world".into()),
            IndexValue::String("日本語テスト".into()),
        ];
        for val in &values {
            let mut buf = Vec::new();
            val.write_to(&mut buf).unwrap();
            let decoded = IndexValue::read_from(&mut &buf[..]).unwrap();
            assert_eq!(val, &decoded, "roundtrip failed for {:?}", val);
        }
    }
}
