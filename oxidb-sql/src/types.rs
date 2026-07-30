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
    // Text/Bytes are boxed slices, not String/Vec: they drop the unused
    // capacity word, keeping `Value` at 24 bytes (the hot per-row cell type).
    // They are immutable once built (no push/append anywhere), so a boxed
    // slice loses nothing.
    Bytes(Box<[u8]>),
    Int(i64),
    Double(f64),
    Text(Box<str>),
    Bool(bool),
    /// Epoch milliseconds.
    Timestamp(i64),
    /// Exact base-10 fixed-point value. Boxed because `Decimal` holds an
    /// `i128` (32-byte struct); inlining it would make every `Value` 48 bytes
    /// even for the common Int/Double/Text cases. Boxing keeps `Value` small
    /// (the hot per-row cell type), at the cost of one allocation per Decimal
    /// value — which is rare relative to how often Values are cloned/scanned.
    Decimal(Box<Decimal>),
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

/// A cell **borrowed** from the bytes it was stored in.
///
/// Decoding a row out of a disk-first mapping costs about 2 ns for a fixed-width
/// cell and about 20 ns for a text one, and two thirds of that difference is the
/// `Box<str>` the text cell is copied into (`examples/decode_bench.rs`). A scan
/// that only compares a text cell — grouping by it, say — pays that copy once per
/// row and then drops it.
///
/// This is the same cell without the copy: a `&str` pointing straight into the
/// mapping. It is deliberately **not** what `Value` becomes: giving `Value` a
/// lifetime would spread through the catalog, the WAL records and their serde
/// derives, for no benefit anywhere but the scan path. Instead the scan path uses
/// this and converts to `Value` at the moment a value is actually kept — for a
/// group key, once per group rather than once per row.
///
/// Sized to match `Value` at 24 bytes: `Decimal` is a 32-byte struct, so it stays
/// borrowed as its encoded form and materializes on demand (decimals do not
/// appear as group keys — `total_order` compares them across numeric types in a
/// way no hash reproduces, so they are excluded from the borrowed key path).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    Int(i64),
    Double(f64),
    Bool(bool),
    Timestamp(i64),
    Text(&'a str),
    Bytes(&'a [u8]),
    /// The cell's encoded payload: `[i128 mantissa][u32 scale]`, little-endian.
    Decimal(&'a [u8]),
}

impl<'a> ValueRef<'a> {
    /// Materialize into an owned [`Value`] — the copy this type exists to defer.
    pub fn to_value(&self) -> Value {
        match self {
            ValueRef::Null => Value::Null,
            ValueRef::Int(n) => Value::Int(*n),
            ValueRef::Double(f) => Value::Double(*f),
            ValueRef::Bool(b) => Value::Bool(*b),
            ValueRef::Timestamp(t) => Value::Timestamp(*t),
            ValueRef::Text(s) => Value::Text((*s).into()),
            ValueRef::Bytes(b) => Value::Bytes((*b).into()),
            ValueRef::Decimal(raw) => {
                let mantissa = i128::from_le_bytes(raw[0..16].try_into().expect("16 bytes"));
                let scale = u32::from_le_bytes(raw[16..20].try_into().expect("4 bytes"));
                Value::Decimal(Box::new(Decimal::new(mantissa, scale)))
            }
        }
    }
}

impl Value {
    /// Borrow this value as a [`ValueRef`], so owned and borrowed rows can go
    /// through one comparison path.
    pub fn as_ref(&self) -> ValueRef<'_> {
        match self {
            Value::Null => ValueRef::Null,
            Value::Int(n) => ValueRef::Int(*n),
            Value::Double(f) => ValueRef::Double(*f),
            Value::Bool(b) => ValueRef::Bool(*b),
            Value::Timestamp(t) => ValueRef::Timestamp(*t),
            Value::Text(s) => ValueRef::Text(s),
            Value::Bytes(b) => ValueRef::Bytes(b),
            // The one variant that cannot be borrowed as-is: `Decimal` holds a
            // decoded struct, not its encoding. Callers that meet a decimal fall
            // back to owned comparison (see `eq_value_ref`).
            Value::Decimal(_) => ValueRef::Null,
        }
    }
}

/// Whether a stored value and a borrowed cell are the same value.
///
/// Used to match a row against an existing group key without materializing the
/// row's cell. Only the variants that can *be* a borrowed group key are compared
/// directly — integer, text, boolean and timestamp, which is exactly the set the
/// grouping fast path admits — and anything else materializes and compares as
/// `Value`, so an unexpected pairing is slow rather than wrong.
///
/// `Decimal` must take that fallback: [`Value::as_ref`] cannot represent it.
pub fn eq_value_ref(v: &Value, r: &ValueRef<'_>) -> bool {
    match (v, r) {
        (Value::Int(a), ValueRef::Int(b)) => a == b,
        (Value::Text(a), ValueRef::Text(b)) => a.as_ref() == *b,
        (Value::Bool(a), ValueRef::Bool(b)) => a == b,
        (Value::Timestamp(a), ValueRef::Timestamp(b)) => a == b,
        (Value::Null, ValueRef::Null) => true,
        (Value::Bytes(a), ValueRef::Bytes(b)) => a.as_ref() == *b,
        (Value::Double(a), ValueRef::Double(b)) => a == b,
        // Mismatched variants are unequal — but only after the fallback, because
        // a decimal on either side reaches here for representational reasons
        // rather than because the values differ.
        _ => *v == r.to_value(),
    }
}

/// [`decode_row_masked`] into borrowed cells.
///
/// Wanted cells point into `bytes`; the rest are `Null` placeholders holding
/// their positions, exactly as the owned form does. Nothing is allocated —
/// including for text and bytes, which is the point.
pub fn decode_row_refs<'a>(
    bytes: &'a [u8],
    ncols: usize,
    want: &[bool],
    cells: &mut Vec<ValueRef<'a>>,
) -> Result<()> {
    cells.clear();
    cells.reserve(ncols);
    let mut pos = 0;
    for i in 0..ncols {
        match want.get(i).copied().unwrap_or(false) {
            true => cells.push(decode_cell_ref(bytes, &mut pos)?),
            false => {
                skip_cell(bytes, &mut pos)?;
                cells.push(ValueRef::Null);
            }
        }
    }
    if pos != bytes.len() {
        return Err(SqlError::Corrupt(format!(
            "row had trailing bytes: consumed {pos} of {}",
            bytes.len()
        )));
    }
    Ok(())
}

/// [`decode_cell_ref`] for tests in other modules that need to build a borrowed
/// cell the way a base row produces one (rather than via `Value::as_ref`).
#[cfg(test)]
pub fn decode_cell_ref_for_test<'a>(bytes: &'a [u8], pos: &mut usize) -> Result<ValueRef<'a>> {
    decode_cell_ref(bytes, pos)
}

/// One cell, borrowed. Mirrors [`decode_cell`] tag for tag; the two are checked
/// against each other in `decode_cell_ref_matches_decode_cell`.
fn decode_cell_ref<'a>(bytes: &'a [u8], pos: &mut usize) -> Result<ValueRef<'a>> {
    let tag = *bytes
        .get(*pos)
        .ok_or_else(|| SqlError::Corrupt("truncated cell tag".into()))?;
    *pos += 1;
    match tag {
        TAG_NULL => Ok(ValueRef::Null),
        TAG_INT => Ok(ValueRef::Int(read_i64(bytes, pos)?)),
        TAG_TIMESTAMP => Ok(ValueRef::Timestamp(read_i64(bytes, pos)?)),
        TAG_DOUBLE => Ok(ValueRef::Double(f64::from_le_bytes(read_8(bytes, pos)?))),
        TAG_BOOL => {
            let b = *bytes
                .get(*pos)
                .ok_or_else(|| SqlError::Corrupt("truncated bool".into()))?;
            *pos += 1;
            Ok(ValueRef::Bool(b != 0))
        }
        TAG_BYTES => {
            let len = u32::from_le_bytes(read_4(bytes, pos)?) as usize;
            let end = *pos + len;
            let raw = bytes
                .get(*pos..end)
                .ok_or_else(|| SqlError::Corrupt("truncated bytes".into()))?;
            *pos = end;
            Ok(ValueRef::Bytes(raw))
        }
        TAG_DECIMAL => {
            let end = *pos + 20; // i128 mantissa + u32 scale
            let raw = bytes
                .get(*pos..end)
                .ok_or_else(|| SqlError::Corrupt("truncated decimal".into()))?;
            *pos = end;
            Ok(ValueRef::Decimal(raw))
        }
        TAG_TEXT => {
            let len = u32::from_le_bytes(read_4(bytes, pos)?) as usize;
            let end = *pos + len;
            let raw = bytes
                .get(*pos..end)
                .ok_or_else(|| SqlError::Corrupt("truncated text".into()))?;
            // Still validated: the copy is what is being avoided, not the check
            // that the bytes are text at all. It costs about 3 ns of the 20 a
            // materialized text cell costs.
            let s = std::str::from_utf8(raw)
                .map_err(|_| SqlError::Corrupt("invalid utf8 in text cell".into()))?;
            *pos = end;
            Ok(ValueRef::Text(s))
        }
        other => Err(SqlError::Corrupt(format!("unknown cell tag {other}"))),
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

/// One index or primary key, as a tuple of column values.
///
/// Composite and single-column keys take one code path, so this is a sequence
/// even when it holds one element — and one element is the overwhelmingly
/// common case, which is why it inlines. A `Vec` here would put a separate heap
/// allocation behind every key in every index and primary-key map: 24 bytes of
/// header plus a rounded-up 32-byte allocation to hold a 24-byte value. At a
/// million rows that is the difference between tens and hundreds of megabytes.
pub type KeyTuple = smallvec::SmallVec<[IndexKey; 1]>;

/// How a key reads in a constraint-violation message: the bare value for a
/// single column (`Int(1)`), a tuple for a composite one (`(Int(1), Text("a"))`).
pub(crate) fn render_key(cols: &[usize], cells: &[Value]) -> String {
    match cols {
        [p] => format!("{:?}", cells[*p]),
        _ => {
            let parts: Vec<String> = cols.iter().map(|&p| format!("{:?}", cells[p])).collect();
            format!("({})", parts.join(", "))
        }
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
            Ok(Value::Bytes((raw).into()))
        }
        TAG_DECIMAL => {
            let mantissa = i128::from_le_bytes(read_16(bytes, pos)?);
            let scale = u32::from_le_bytes(read_4(bytes, pos)?);
            Ok(Value::Decimal(Box::new(Decimal::new(mantissa, scale))))
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
            Ok(Value::Text((s).into()))
        }
        other => Err(SqlError::Corrupt(format!("unknown cell tag {other}"))),
    }
}

/// Encode a full row (one cell per column, in column order) to bytes.
pub fn encode_row(cells: &[Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(cells.len() * 9);
    encode_row_into(cells, &mut buf);
    buf
}

/// [`encode_row`] into a buffer the caller owns and reuses — the counterpart of
/// [`decode_row_into`], for writing a whole table without an allocation per row.
/// The buffer is appended to, so callers clear it between rows.
pub fn encode_row_into(cells: &[Value], buf: &mut Vec<u8>) {
    for c in cells {
        encode_cell(c, buf);
    }
}

/// Decode a row of exactly `ncols` cells from `bytes`.
pub fn decode_row(bytes: &[u8], ncols: usize) -> Result<Vec<Value>> {
    let mut cells = Vec::with_capacity(ncols);
    decode_row_into(bytes, ncols, &mut cells)?;
    Ok(cells)
}

/// [`decode_row`] into a buffer the caller owns and reuses.
///
/// A disk-first scan decodes every row out of the mmap, and allocating a fresh
/// `Vec` for each one was a per-row allocation on the hottest path in that
/// mode. The buffer is cleared, not reallocated, so a scan of a million rows
/// makes one.
pub fn decode_row_into(bytes: &[u8], ncols: usize, cells: &mut Vec<Value>) -> Result<()> {
    cells.clear();
    cells.reserve(ncols);
    let mut pos = 0;
    for _ in 0..ncols {
        cells.push(decode_cell(bytes, &mut pos)?);
    }
    if pos != bytes.len() {
        return Err(SqlError::Corrupt(format!(
            "row had trailing bytes: consumed {pos} of {}",
            bytes.len()
        )));
    }
    Ok(())
}

/// [`decode_row_into`], but only the cells whose position is `true` in `want`.
///
/// A scan usually reads a few of a table's columns — `sum(total)` reads one of
/// five — but a disk-first scan decoded the whole row out of the mapping, and
/// every text or bytes cell allocates and copies to be decoded. Skipping a cell
/// costs reading its length and advancing.
///
/// Unwanted cells are pushed as `Value::Null` so **positions are preserved**: the
/// row handed to a visitor has the table's arity and its columns where the
/// executor expects them, which is what lets this be a drop-in for the full
/// decode with no index remapping anywhere. The cost of that choice is that a
/// caller reading a column it did not ask for sees `Null` rather than the stored
/// value, so `want` must cover every column the query can read — callers derive
/// it from the same `collect_needed` walk that decides which columns to project,
/// which covers the projection, filter, joins, GROUP BY, HAVING and ORDER BY.
///
/// `want` shorter than `ncols` treats the missing tail as unwanted.
pub fn decode_row_masked(
    bytes: &[u8],
    ncols: usize,
    want: &[bool],
    cells: &mut Vec<Value>,
) -> Result<()> {
    cells.clear();
    cells.reserve(ncols);
    let mut pos = 0;
    for i in 0..ncols {
        match want.get(i).copied().unwrap_or(false) {
            true => cells.push(decode_cell(bytes, &mut pos)?),
            false => {
                skip_cell(bytes, &mut pos)?;
                cells.push(Value::Null);
            }
        }
    }
    if pos != bytes.len() {
        return Err(SqlError::Corrupt(format!(
            "row had trailing bytes: consumed {pos} of {}",
            bytes.len()
        )));
    }
    Ok(())
}

/// Advance `pos` past one encoded cell without materializing it.
///
/// Must consume exactly what [`decode_cell`] would, or every later cell in the
/// row is read from the wrong offset — so the two are deliberately written to
/// the same shape, tag for tag, and `skip_cell_matches_decode_cell` checks them
/// against each other over every variant.
fn skip_cell(bytes: &[u8], pos: &mut usize) -> Result<()> {
    let tag = *bytes
        .get(*pos)
        .ok_or_else(|| SqlError::Corrupt("truncated cell tag".into()))?;
    *pos += 1;
    let width = match tag {
        TAG_NULL => 0,
        TAG_BOOL => 1,
        TAG_INT | TAG_TIMESTAMP | TAG_DOUBLE => 8,
        TAG_DECIMAL => 20, // i128 mantissa + u32 scale
        // `read_4` has already consumed the length prefix itself.
        TAG_BYTES | TAG_TEXT => u32::from_le_bytes(read_4(bytes, pos)?) as usize,
        other => return Err(SqlError::Corrupt(format!("unknown cell tag {other}"))),
    };
    let end = *pos + width;
    if end > bytes.len() {
        return Err(SqlError::Corrupt("truncated cell".into()));
    }
    *pos = end;
    Ok(())
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
            Value::Decimal(Box::new(Decimal::parse("-19.90").unwrap())),
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

#[cfg(test)]
mod masked_decode_tests {
    //! [`decode_row_masked`] skips cells instead of materializing them, which is
    //! only safe if skipping advances the read position by exactly what decoding
    //! would. If the two ever disagree for one variant, every cell after it in
    //! the row is read from the wrong offset — usually a corruption error, but
    //! potentially a wrong value silently.
    use super::*;
    use crate::decimal::Decimal;

    /// One of every variant, including the empty and multi-byte string cases.
    fn every_variant() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(i64::MIN),
            Value::Int(0),
            Value::Int(i64::MAX),
            Value::Timestamp(1_704_067_200_000),
            Value::Double(-2.5),
            Value::Double(f64::INFINITY),
            Value::Text("".into()),
            Value::Text("ascii".into()),
            Value::Text("çok baytlı — multi-byte".into()),
            Value::Bytes(Vec::new().into()),
            Value::Bytes(vec![0u8, 255, 7].into()),
            Value::Decimal(Box::new(Decimal::new(-12_345, 3))),
            Value::Decimal(Box::new(Decimal::new(i128::MAX, 0))),
        ]
    }

    #[test]
    fn skip_cell_matches_decode_cell() {
        for v in every_variant() {
            let mut buf = Vec::new();
            encode_cell(&v, &mut buf);

            let mut decode_pos = 0;
            let decoded = decode_cell(&buf, &mut decode_pos).expect("decode");
            let mut skip_pos = 0;
            skip_cell(&buf, &mut skip_pos).expect("skip");

            assert_eq!(decoded, v, "round trip broke for {v:?}");
            assert_eq!(
                skip_pos, decode_pos,
                "skip and decode disagree on the width of {v:?}"
            );
            assert_eq!(skip_pos, buf.len(), "skip did not consume all of {v:?}");
        }
    }

    /// The wanted cells come back exactly as a full decode gives them, and the
    /// rest are `Null` placeholders at their own positions.
    #[test]
    fn masking_preserves_positions_and_values() {
        let row = every_variant();
        let encoded = encode_row(&row);
        let n = row.len();

        let mut full = Vec::new();
        decode_row_into(&encoded, n, &mut full).expect("full decode");
        assert_eq!(full, row);

        // Every single-column mask: the one wanted cell must match the full
        // decode, and every other position must be Null.
        for i in 0..n {
            let mut want = vec![false; n];
            want[i] = true;
            let mut masked = Vec::new();
            decode_row_masked(&encoded, n, &want, &mut masked).expect("masked decode");
            assert_eq!(masked.len(), n, "arity changed at column {i}");
            assert_eq!(masked[i], row[i], "wanted column {i} decoded wrongly");
            for (j, cell) in masked.iter().enumerate() {
                if j != i {
                    assert_eq!(*cell, Value::Null, "unwanted column {j} was materialized");
                }
            }
        }
    }

    #[test]
    fn masking_everything_equals_a_full_decode() {
        let row = every_variant();
        let encoded = encode_row(&row);
        let mut masked = Vec::new();
        decode_row_masked(&encoded, row.len(), &vec![true; row.len()], &mut masked).unwrap();
        assert_eq!(masked, row);
    }

    #[test]
    fn masking_nothing_still_walks_the_whole_row() {
        let row = every_variant();
        let encoded = encode_row(&row);
        let mut masked = Vec::new();
        // All-false must still consume every byte — the trailing-bytes check is
        // what would catch a skip that stopped short.
        decode_row_masked(&encoded, row.len(), &[], &mut masked).expect("all-skipped decode");
        assert_eq!(masked, vec![Value::Null; row.len()]);
    }

    /// A truncated row must be rejected while skipping, not read past its end.
    #[test]
    fn a_truncated_row_is_rejected_while_skipping() {
        let row = vec![Value::Text("abcdef".into()), Value::Int(9)];
        let encoded = encode_row(&row);
        let cut = &encoded[..encoded.len() - 5];
        let mut masked = Vec::new();
        assert!(
            decode_row_masked(cut, 2, &[false, true], &mut masked).is_err(),
            "a truncated row decoded without complaint"
        );
    }
}

#[cfg(test)]
mod value_ref_tests {
    //! [`ValueRef`] exists to let a scan compare a cell without copying it, which
    //! means there are now two ways to decode a row and two ways to compare a
    //! value. Both are places where a second implementation can quietly drift
    //! from the first — earlier in this engine a fast grouping path compared with
    //! `==` while the general path used `total_order`, and the two grouped
    //! differently. These tests exist to make drift fail.
    use super::*;
    use crate::decimal::Decimal;

    fn every_variant() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(i64::MIN),
            Value::Int(0),
            Value::Int(i64::MAX),
            Value::Timestamp(1_704_067_200_000),
            Value::Double(-2.5),
            Value::Text("".into()),
            Value::Text("ascii".into()),
            Value::Text("çok baytlı — multi-byte".into()),
            Value::Bytes(Vec::new().into()),
            Value::Bytes(vec![0u8, 255, 7].into()),
            Value::Decimal(Box::new(Decimal::new(-12_345, 3))),
        ]
    }

    /// The borrowed decoder must read exactly what the owned one reads, cell for
    /// cell — including consuming the same bytes, or later cells shift.
    #[test]
    fn decode_cell_ref_matches_decode_cell() {
        for v in every_variant() {
            let mut buf = Vec::new();
            encode_cell(&v, &mut buf);

            let mut owned_pos = 0;
            let owned = decode_cell(&buf, &mut owned_pos).expect("owned decode");
            let mut ref_pos = 0;
            let borrowed = decode_cell_ref(&buf, &mut ref_pos).expect("borrowed decode");

            assert_eq!(ref_pos, owned_pos, "widths disagree for {v:?}");
            assert_eq!(
                borrowed.to_value(),
                owned,
                "borrowed and owned decode disagree for {v:?}"
            );
            assert_eq!(owned, v, "round trip broke for {v:?}");
        }
    }

    /// A whole row, both ways, with a mask — the shape the scan path uses.
    #[test]
    fn a_masked_row_decodes_the_same_borrowed_or_owned() {
        let row = every_variant();
        let encoded = encode_row(&row);
        let n = row.len();

        for i in 0..n {
            let mut want = vec![false; n];
            want[i] = true;
            let mut owned = Vec::new();
            decode_row_masked(&encoded, n, &want, &mut owned).expect("owned");
            let mut borrowed = Vec::new();
            decode_row_refs(&encoded, n, &want, &mut borrowed).expect("borrowed");

            assert_eq!(borrowed.len(), owned.len());
            let materialized: Vec<Value> = borrowed.iter().map(|c| c.to_value()).collect();
            assert_eq!(materialized, owned, "row disagreed with column {i} wanted");
        }
    }

    /// `eq_value_ref` must agree with `Value`'s own equality on every pair, which
    /// is what makes it safe to match a row against a stored group key.
    #[test]
    fn borrowed_equality_agrees_with_owned_equality() {
        let vals = every_variant();
        for a in &vals {
            for b in &vals {
                // Compare `a` (stored key) against `b` borrowed. Decimal cannot
                // be borrowed from an owned Value, so build that side from its
                // encoding, which is how a scan would meet it.
                let mut buf = Vec::new();
                encode_cell(b, &mut buf);
                let mut pos = 0;
                let b_ref = decode_cell_ref(&buf, &mut pos).expect("decode");

                assert_eq!(
                    eq_value_ref(a, &b_ref),
                    a == b,
                    "eq_value_ref disagrees with == for {a:?} vs {b:?}"
                );
            }
        }
    }

    /// `Value::as_ref` round-trips every variant it can represent, and is honest
    /// about the one it cannot.
    #[test]
    fn as_ref_round_trips_except_decimal() {
        for v in every_variant() {
            let back = v.as_ref().to_value();
            match v {
                Value::Decimal(_) => assert_eq!(
                    back,
                    Value::Null,
                    "as_ref must not claim to represent a decimal"
                ),
                _ => assert_eq!(back, v, "as_ref lost {v:?}"),
            }
        }
    }

    /// `ValueRef` is meant to be the same size as `Value` — it is stored one per
    /// cell in a scan buffer, so a wider one would cost memory on the hot path.
    #[test]
    fn value_ref_is_no_wider_than_value() {
        assert_eq!(
            std::mem::size_of::<ValueRef<'_>>(),
            std::mem::size_of::<Value>(),
            "ValueRef grew past Value"
        );
    }
}
