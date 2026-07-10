//! SQL type system and the typed-row binary codec.
//!
//! This is deliberately independent of the document engine's dynamic
//! `serde_json::Value` model: a SQL table has a fixed schema, so rows are
//! vectors of typed [`Value`] cells whose layout is known from the catalog.
//! The binary codec here is what the row-oriented `.rdat` storage writes.

use serde::{Deserialize, Serialize};

use crate::decimal::Decimal;
use crate::error::{Result, SqlError};

/// The static type of a column.
///
/// `Timestamp` is stored as epoch milliseconds (`i64`), mirroring the document
/// engine's date handling so the two engines agree on the wire representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SqlType {
    Int,
    Double,
    Text,
    Bool,
    Timestamp,
    /// Binary data (`BLOB`/`BYTEA`/`BINARY`). JSON wire form is base64.
    Blob,
    /// Exact base-10 fixed-point (`DECIMAL`/`NUMERIC`). Backed by [`Decimal`].
    Decimal,
}

/// A single typed cell value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "lowercase")]
pub enum Value {
    Null,
    Bytes(Vec<u8>),
    Int(i64),
    Double(f64),
    Text(String),
    Bool(bool),
    /// Epoch milliseconds.
    Timestamp(i64),
    /// Exact base-10 fixed-point value.
    Decimal(Decimal),
}

impl Value {
    /// True when this value is compatible with `ty` (NULL is compatible with
    /// every type; nullability is enforced separately by the catalog).
    pub fn matches_type(&self, ty: SqlType) -> bool {
        matches!(
            (self, ty),
            (Value::Null, _)
                | (Value::Int(_), SqlType::Int)
                | (Value::Double(_), SqlType::Double)
                | (Value::Text(_), SqlType::Text)
                | (Value::Bool(_), SqlType::Bool)
                | (Value::Timestamp(_), SqlType::Timestamp)
                | (Value::Bytes(_), SqlType::Blob)
                | (Value::Decimal(_), SqlType::Decimal)
        )
    }

    /// Numeric view of Int/Double/Timestamp/Decimal, for cross-numeric
    /// comparison. Decimal is viewed lossily as `f64` here; exact Decimal
    /// comparisons go through [`Value::total_order`] / the executor's
    /// `cmp_values`.
    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(n) => Some(*n as f64),
            Value::Double(f) => Some(*f),
            Value::Timestamp(t) => Some(*t as f64),
            Value::Decimal(d) => Some(d.to_f64()),
            _ => None,
        }
    }

    /// A **total** order over values, used by ORDER BY and by index keys.
    ///
    /// Establishes a cross-type ranking (Null < Bool < numeric < Text), orders
    /// within a kind, and treats the numeric kinds (Int/Double/Timestamp) as one
    /// comparable class. NaN doubles are treated as equal to avoid a partial
    /// order (they should not occur in stored data).
    pub fn total_order(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Int(_) | Value::Double(_) | Value::Timestamp(_) | Value::Decimal(_) => 2,
                Value::Text(_) => 3,
                Value::Bytes(_) => 4,
            }
        }
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            (Value::Text(x), Value::Text(y)) => x.cmp(y),
            (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y),
            // Exact ordering among Decimal / Int so equal values (e.g. 2 and
            // 2.00) sort together and index keys stay precise.
            (Value::Decimal(x), Value::Decimal(y)) => x.cmp(y),
            (Value::Decimal(x), Value::Int(y)) => x.cmp(&Decimal::from_i64(*y)),
            (Value::Int(x), Value::Decimal(y)) => Decimal::from_i64(*x).cmp(y),
            _ if rank(a) == 2 && rank(b) == 2 => a
                .as_f64()
                .unwrap()
                .partial_cmp(&b.as_f64().unwrap())
                .unwrap_or(Ordering::Equal),
            _ => rank(a).cmp(&rank(b)),
        }
    }
}

/// A wrapper giving [`Value`] a total `Ord`, so values can be used as keys in a
/// `BTreeMap` (secondary indexes). Ordering is [`Value::total_order`].
#[derive(Debug, Clone, PartialEq)]
pub struct IndexKey(pub Value);

impl Eq for IndexKey {}
impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        Value::total_order(&self.0, &other.0)
    }
}

// Cell tags. NULL has its own tag so a nullable column round-trips exactly.
const TAG_NULL: u8 = 0;
const TAG_INT: u8 = 1;
const TAG_DOUBLE: u8 = 2;
const TAG_TEXT: u8 = 3;
const TAG_BOOL: u8 = 4;
const TAG_TIMESTAMP: u8 = 5;
const TAG_BYTES: u8 = 6;
const TAG_DECIMAL: u8 = 7;

/// Append the binary encoding of a single cell to `buf`.
///
/// Layout: `[tag:u8]` followed by the payload:
/// - Null: nothing
/// - Int / Timestamp: `i64` little-endian (8 bytes)
/// - Double: `f64` bits little-endian (8 bytes)
/// - Bool: one byte (0/1)
/// - Text: `[len:u32 LE][utf8 bytes]`
pub fn encode_cell(v: &Value, buf: &mut Vec<u8>) {
    match v {
        Value::Null => buf.push(TAG_NULL),
        Value::Int(n) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Double(f) => {
            buf.push(TAG_DOUBLE);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(*b as u8);
        }
        Value::Text(s) => {
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Timestamp(n) => {
            buf.push(TAG_TIMESTAMP);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Bytes(b) => {
            buf.push(TAG_BYTES);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        // Mantissa (i128 LE, 16 bytes) + scale (u32 LE, 4 bytes).
        Value::Decimal(d) => {
            buf.push(TAG_DECIMAL);
            buf.extend_from_slice(&d.mantissa().to_le_bytes());
            buf.extend_from_slice(&d.scale().to_le_bytes());
        }
    }
}

/// Decode one cell starting at `*pos`, advancing `*pos` past it.
fn decode_cell(bytes: &[u8], pos: &mut usize) -> Result<Value> {
    let tag = *bytes
        .get(*pos)
        .ok_or_else(|| SqlError::Corrupt("truncated cell tag".into()))?;
    *pos += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_INT => Ok(Value::Int(read_i64(bytes, pos)?)),
        TAG_TIMESTAMP => Ok(Value::Timestamp(read_i64(bytes, pos)?)),
        TAG_DOUBLE => Ok(Value::Double(f64::from_le_bytes(read_8(bytes, pos)?))),
        TAG_BOOL => {
            let b = *bytes
                .get(*pos)
                .ok_or_else(|| SqlError::Corrupt("truncated bool".into()))?;
            *pos += 1;
            Ok(Value::Bool(b != 0))
        }
        TAG_BYTES => {
            let len = u32::from_le_bytes(read_4(bytes, pos)?) as usize;
            let end = *pos + len;
            let raw = bytes
                .get(*pos..end)
                .ok_or_else(|| SqlError::Corrupt("truncated bytes".into()))?
                .to_vec();
            *pos = end;
            Ok(Value::Bytes(raw))
        }
        TAG_DECIMAL => {
            let mantissa = i128::from_le_bytes(read_16(bytes, pos)?);
            let scale = u32::from_le_bytes(read_4(bytes, pos)?);
            Ok(Value::Decimal(Decimal::new(mantissa, scale)))
        }
        TAG_TEXT => {
            let len = u32::from_le_bytes(read_4(bytes, pos)?) as usize;
            let end = *pos + len;
            let raw = bytes
                .get(*pos..end)
                .ok_or_else(|| SqlError::Corrupt("truncated text".into()))?;
            let s = std::str::from_utf8(raw)
                .map_err(|_| SqlError::Corrupt("invalid utf8 in text cell".into()))?
                .to_string();
            *pos = end;
            Ok(Value::Text(s))
        }
        other => Err(SqlError::Corrupt(format!("unknown cell tag {other}"))),
    }
}

/// Encode a full row (one cell per column, in column order) to bytes.
pub fn encode_row(cells: &[Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(cells.len() * 9);
    for c in cells {
        encode_cell(c, &mut buf);
    }
    buf
}

/// Decode a row of exactly `ncols` cells from `bytes`.
pub fn decode_row(bytes: &[u8], ncols: usize) -> Result<Vec<Value>> {
    let mut pos = 0;
    let mut cells = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        cells.push(decode_cell(bytes, &mut pos)?);
    }
    if pos != bytes.len() {
        return Err(SqlError::Corrupt(format!(
            "row had trailing bytes: consumed {pos} of {}",
            bytes.len()
        )));
    }
    Ok(cells)
}

fn read_8(bytes: &[u8], pos: &mut usize) -> Result<[u8; 8]> {
    let end = *pos + 8;
    let slice = bytes
        .get(*pos..end)
        .ok_or_else(|| SqlError::Corrupt("truncated 8-byte field".into()))?;
    *pos = end;
    Ok(slice.try_into().unwrap())
}

fn read_16(bytes: &[u8], pos: &mut usize) -> Result<[u8; 16]> {
    let end = *pos + 16;
    let slice = bytes
        .get(*pos..end)
        .ok_or_else(|| SqlError::Corrupt("truncated 16-byte field".into()))?;
    *pos = end;
    Ok(slice.try_into().unwrap())
}

fn read_4(bytes: &[u8], pos: &mut usize) -> Result<[u8; 4]> {
    let end = *pos + 4;
    let slice = bytes
        .get(*pos..end)
        .ok_or_else(|| SqlError::Corrupt("truncated 4-byte field".into()))?;
    *pos = end;
    Ok(slice.try_into().unwrap())
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_8(bytes, pos)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_roundtrip_all_types() {
        let cells = vec![
            Value::Null,
            Value::Int(-42),
            Value::Double(3.5),
            Value::Bool(true),
            Value::Text("héllo".into()),
            Value::Timestamp(1_700_000_000_000),
            Value::Decimal(Decimal::parse("-19.90").unwrap()),
        ];
        let bytes = encode_row(&cells);
        let back = decode_row(&bytes, cells.len()).unwrap();
        assert_eq!(cells, back);
    }

    #[test]
    fn truncated_row_is_corrupt() {
        let bytes = encode_row(&[Value::Text("abc".into())]);
        // Drop the last byte of the utf8 payload.
        let err = decode_row(&bytes[..bytes.len() - 1], 1).unwrap_err();
        assert!(matches!(err, SqlError::Corrupt(_)));
    }

    #[test]
    fn type_checking() {
        assert!(Value::Int(1).matches_type(SqlType::Int));
        assert!(Value::Null.matches_type(SqlType::Text));
        assert!(!Value::Int(1).matches_type(SqlType::Text));
    }
}
