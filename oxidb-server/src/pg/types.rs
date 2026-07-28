//! Type mapping between the SQL engine and the PostgreSQL wire.
//!
//! Two directions, and they are not symmetric:
//!
//!   engine -> wire   a column's [`SqlType`] picks a type OID, and each
//!                    [`Value`] is rendered in the format the client asked for
//!   wire -> engine   a bound parameter arrives with the OID the *client*
//!                    declared (or 0 for "you decide"), and is decoded to a
//!                    [`Value`] accordingly
//!
//! Integers map to `int8`, not `int4`: the engine's `Int` is an i64, and
//! declaring it int4 would truncate every value past 2^31 in binary format and
//! mislead every client's type inference in text format.

use oxidb_sql::{Decimal, SqlType, Value};

// Type OIDs, from PostgreSQL's pg_type. These numbers are permanent.
pub const OID_BOOL: i32 = 16;
pub const OID_BYTEA: i32 = 17;
/// Single byte. Only the catalog answers use it (`pg_type.typtype`), where a
/// driver reads it as a char and would reject `text`.
pub const OID_CHAR: i32 = 18;
pub const OID_INT8: i32 = 20;
pub const OID_INT2: i32 = 21;
pub const OID_INT4: i32 = 23;
pub const OID_TEXT: i32 = 25;
/// Object identifier. Catalog answers only — a driver reading `pg_type.oid`
/// expects this type and refuses a plain `int8`.
pub const OID_OID: i32 = 26;
pub const OID_FLOAT4: i32 = 700;
pub const OID_FLOAT8: i32 = 701;
pub const OID_VARCHAR: i32 = 1043;
pub const OID_TIMESTAMP: i32 = 1114;
pub const OID_TIMESTAMPTZ: i32 = 1184;
pub const OID_NUMERIC: i32 = 1700;
/// The catch-all a client sends when it wants the server to infer a type.
pub const OID_UNSPECIFIED: i32 = 0;

/// Wire format codes.
pub const FORMAT_TEXT: i16 = 0;
pub const FORMAT_BINARY: i16 = 1;

/// The OID a column of this type is advertised as. An unknown type (the engine
/// reports `None` when it cannot infer one statically) is advertised as `text`,
/// which every client can render.
pub fn oid_of(ty: Option<SqlType>) -> i32 {
    match ty {
        Some(SqlType::Int) => OID_INT8,
        Some(SqlType::Double) => OID_FLOAT8,
        Some(SqlType::Text) | None => OID_TEXT,
        Some(SqlType::Bool) => OID_BOOL,
        Some(SqlType::Timestamp) => OID_TIMESTAMP,
        Some(SqlType::Blob) => OID_BYTEA,
        Some(SqlType::Decimal) => OID_NUMERIC,
    }
}

/// The OID a *declared column* is reported as, which is narrower than what
/// [`oid_of`] can say from the type alone: a `VARCHAR(n)` is `varchar` rather
/// than unbounded `text`, and a `SMALLINT`/`INT` is `int2`/`int4` rather than
/// the `int8` every integer is stored as.
///
/// Safe because those declarations are *enforced* — a value outside the
/// declared range is refused on write — so a client that generates a 16- or
/// 32-bit field from this metadata cannot be handed something too big for it.
///
/// Note the asymmetry with query results: a `RowDescription` is built from the
/// engine's inferred column types, which carry no declared width, so a SELECT
/// over a `SMALLINT` column still reports `int8`. Both are true; this one is
/// more specific.
pub fn oid_of_column(col: &oxidb_sql::Column) -> i32 {
    if col.max_len.is_some() {
        return OID_VARCHAR;
    }
    match col.int_range().map(|_| col.int_width) {
        Some(Some(1 | 2)) => OID_INT2,
        Some(Some(4)) => OID_INT4,
        _ => oid_of(Some(col.ty)),
    }
}

/// The fixed width of a type, or `-1` for variable-length ones.
pub fn type_len(oid: i32) -> i16 {
    match oid {
        OID_BOOL | OID_CHAR => 1,
        OID_INT2 => 2,
        OID_INT4 | OID_FLOAT4 | OID_OID => 4,
        OID_INT8 | OID_FLOAT8 | OID_TIMESTAMP | OID_TIMESTAMPTZ => 8,
        _ => -1,
    }
}

/// The OID a *value* should be described as when the column type is unknown —
/// used for result sets the engine could not type statically, where guessing
/// from the first row beats calling everything text.
pub fn oid_of_value(v: &Value) -> i32 {
    match v {
        Value::Null => OID_TEXT,
        Value::Int(_) => OID_INT8,
        Value::Double(_) => OID_FLOAT8,
        Value::Text(_) => OID_TEXT,
        Value::Bool(_) => OID_BOOL,
        Value::Timestamp(_) => OID_TIMESTAMP,
        Value::Bytes(_) => OID_BYTEA,
        Value::Decimal(_) => OID_NUMERIC,
    }
}

/// Render a value in text format. `None` is SQL NULL (a `-1` length on the
/// wire, which is not the same as an empty string).
pub fn to_text(v: &Value) -> Option<Vec<u8>> {
    let s = match v {
        Value::Null => return None,
        Value::Int(i) => i.to_string(),
        Value::Double(f) => {
            // PostgreSQL spells the non-finite floats out; `{}` would give
            // "inf"/"NaN", which clients do not parse back.
            if f.is_nan() {
                "NaN".to_string()
            } else if f.is_infinite() {
                if *f > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
            } else {
                // Rust's Display for f64 is shortest-round-trip.
                f.to_string()
            }
        }
        Value::Text(s) => s.to_string(),
        Value::Bool(b) => (if *b { "t" } else { "f" }).to_string(),
        Value::Timestamp(ms) => timestamp_to_text(*ms),
        Value::Bytes(b) => bytea_to_text(b),
        Value::Decimal(d) => d.to_string(),
    };
    Some(s.into_bytes())
}

/// Epoch milliseconds as PostgreSQL's ISO `DateStyle` renders a `timestamp`.
pub fn timestamp_to_text(ms: i64) -> String {
    use chrono::{DateTime, Utc};
    match DateTime::<Utc>::from_timestamp_millis(ms) {
        // Microsecond precision, trailing zeros trimmed — PostgreSQL's own
        // rendering, and what every client's parser expects.
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        None => ms.to_string(),
    }
}

/// PostgreSQL's hex `bytea` output format (`standard_conforming_strings` on).
fn bytea_to_text(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("\\x");
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Render a value in binary format for a column advertised as `oid`.
///
/// Only the types whose binary layout is unambiguous and cheap are supported;
/// `numeric` (base-10000 digit groups) and the date/time types are refused by
/// [`can_binary`] before we get here, so a client asking for them gets a clear
/// error rather than mis-decoded data.
pub fn to_binary(v: &Value, oid: i32) -> Option<Vec<u8>> {
    let b = match v {
        Value::Null => return None,
        Value::Int(i) => match oid {
            OID_INT2 => (*i as i16).to_be_bytes().to_vec(),
            OID_INT4 => (*i as i32).to_be_bytes().to_vec(),
            _ => i.to_be_bytes().to_vec(),
        },
        Value::Double(f) => match oid {
            OID_FLOAT4 => (*f as f32).to_be_bytes().to_vec(),
            _ => f.to_be_bytes().to_vec(),
        },
        Value::Bool(b) => vec![u8::from(*b)],
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Bytes(b) => b.to_vec(),
        // Refused by can_binary; rendering the text form is the least-wrong
        // fallback if one ever slips through.
        Value::Timestamp(ms) => timestamp_to_text(*ms).into_bytes(),
        Value::Decimal(d) => d.to_string().into_bytes(),
    };
    Some(b)
}

/// Whether this server can encode `oid` in binary format.
pub fn can_binary(oid: i32) -> bool {
    matches!(
        oid,
        OID_BOOL | OID_BYTEA | OID_INT2 | OID_INT4 | OID_INT8 | OID_FLOAT4 | OID_FLOAT8 | OID_TEXT
            | OID_VARCHAR
    )
}

/// The name a type OID is known by, for error messages.
pub fn oid_name(oid: i32) -> &'static str {
    match oid {
        OID_BOOL => "bool",
        OID_BYTEA => "bytea",
        OID_CHAR => "char",
        OID_OID => "oid",
        OID_INT2 => "int2",
        OID_INT4 => "int4",
        OID_INT8 => "int8",
        OID_TEXT => "text",
        OID_FLOAT4 => "float4",
        OID_FLOAT8 => "float8",
        OID_VARCHAR => "varchar",
        OID_TIMESTAMP => "timestamp",
        OID_TIMESTAMPTZ => "timestamptz",
        OID_NUMERIC => "numeric",
        _ => "unknown",
    }
}

/// Decode a bound parameter into an engine value.
///
/// `oid` is what the *client* declared at Parse time (0 = "server decides"),
/// and `format` what it used at Bind time. An unspecified OID is taken as text,
/// which the engine coerces per target column on write.
pub fn decode_param(bytes: Option<&[u8]>, oid: i32, format: i16) -> Result<Value, String> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };
    if format == FORMAT_BINARY {
        return decode_binary_param(bytes, oid);
    }
    let s = std::str::from_utf8(bytes).map_err(|_| "parameter is not valid UTF-8".to_string())?;
    Ok(match oid {
        OID_INT2 | OID_INT4 | OID_INT8 => Value::Int(
            s.parse::<i64>()
                .map_err(|_| format!("invalid integer parameter {s:?}"))?,
        ),
        OID_FLOAT4 | OID_FLOAT8 => Value::Double(
            s.parse::<f64>()
                .map_err(|_| format!("invalid float parameter {s:?}"))?,
        ),
        OID_BOOL => Value::Bool(match s {
            "t" | "true" | "TRUE" | "1" | "y" | "yes" | "on" => true,
            "f" | "false" | "FALSE" | "0" | "n" | "no" | "off" => false,
            other => return Err(format!("invalid boolean parameter {other:?}")),
        }),
        OID_NUMERIC => Value::Decimal(Box::new(
            Decimal::parse(s).ok_or_else(|| format!("invalid numeric parameter {s:?}"))?,
        )),
        OID_BYTEA => Value::Bytes(parse_bytea(s)?.into()),
        OID_TIMESTAMP | OID_TIMESTAMPTZ => Value::Timestamp(parse_timestamp(s)?),
        // text, varchar, unspecified, and anything exotic: hand the engine the
        // string and let its per-column coercion decide.
        _ => Value::Text(s.into()),
    })
}

fn decode_binary_param(bytes: &[u8], oid: i32) -> Result<Value, String> {
    let int = |n: usize| -> Result<i64, String> {
        if bytes.len() != n {
            return Err(format!(
                "binary {} parameter is {} bytes, expected {n}",
                oid_name(oid),
                bytes.len()
            ));
        }
        Ok(match n {
            2 => i16::from_be_bytes([bytes[0], bytes[1]]) as i64,
            4 => i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64,
            _ => i64::from_be_bytes(bytes.try_into().expect("checked")),
        })
    };
    Ok(match oid {
        OID_INT2 => Value::Int(int(2)?),
        OID_INT4 => Value::Int(int(4)?),
        OID_INT8 => Value::Int(int(8)?),
        OID_FLOAT4 => {
            if bytes.len() != 4 {
                return Err("binary float4 parameter is not 4 bytes".into());
            }
            Value::Double(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64)
        }
        OID_FLOAT8 => {
            if bytes.len() != 8 {
                return Err("binary float8 parameter is not 8 bytes".into());
            }
            Value::Double(f64::from_be_bytes(bytes.try_into().expect("checked")))
        }
        OID_BOOL => Value::Bool(bytes.first().is_some_and(|b| *b != 0)),
        OID_BYTEA => Value::Bytes(bytes.to_vec().into()),
        OID_TEXT | OID_VARCHAR | OID_UNSPECIFIED => Value::Text(
            std::str::from_utf8(bytes)
                .map_err(|_| "binary text parameter is not valid UTF-8".to_string())?
                .into(),
        ),
        other => {
            return Err(format!(
                "binary parameter format for type {} is not supported — send it as text",
                oid_name(other)
            ));
        }
    })
}

/// PostgreSQL's `\x...` hex form, and the legacy escape form's plain bytes.
fn parse_bytea(s: &str) -> Result<Vec<u8>, String> {
    let Some(hex) = s.strip_prefix("\\x") else {
        return Ok(s.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 {
        return Err("bytea hex string has an odd length".into());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| "invalid bytea hex".to_string()))
        .collect()
}

/// Parse the timestamp spellings clients send into epoch milliseconds, which
/// is how the engine stores them.
pub fn parse_timestamp(s: &str) -> Result<i64, String> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(dt.and_utc().timestamp_millis());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp_millis());
    }
    Err(format!("invalid timestamp parameter {s:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_is_int8_so_large_values_survive() {
        assert_eq!(oid_of(Some(SqlType::Int)), OID_INT8);
        let big = Value::Int(i64::MAX);
        assert_eq!(to_text(&big).unwrap(), i64::MAX.to_string().into_bytes());
        assert_eq!(to_binary(&big, OID_INT8).unwrap(), i64::MAX.to_be_bytes());
    }

    #[test]
    fn null_is_none_not_empty() {
        assert!(to_text(&Value::Null).is_none());
        assert!(to_binary(&Value::Null, OID_TEXT).is_none());
        // An empty string is a value, not a NULL.
        assert_eq!(to_text(&Value::Text("".into())), Some(Vec::new()));
    }

    #[test]
    fn booleans_are_t_and_f() {
        assert_eq!(to_text(&Value::Bool(true)).unwrap(), b"t");
        assert_eq!(to_text(&Value::Bool(false)).unwrap(), b"f");
    }

    #[test]
    fn non_finite_floats_use_postgres_spelling() {
        assert_eq!(to_text(&Value::Double(f64::NAN)).unwrap(), b"NaN");
        assert_eq!(to_text(&Value::Double(f64::INFINITY)).unwrap(), b"Infinity");
        assert_eq!(
            to_text(&Value::Double(f64::NEG_INFINITY)).unwrap(),
            b"-Infinity"
        );
    }

    #[test]
    fn bytea_uses_the_hex_form() {
        let v = Value::Bytes(vec![0x00, 0xff, 0x10].into());
        assert_eq!(to_text(&v).unwrap(), b"\\x00ff10");
    }

    #[test]
    fn timestamps_render_and_parse_back() {
        let text = timestamp_to_text(0);
        assert_eq!(text, "1970-01-01 00:00:00");
        assert_eq!(parse_timestamp(&text).unwrap(), 0);
        assert_eq!(
            parse_timestamp("2026-07-28T12:34:56.789Z").unwrap(),
            parse_timestamp("2026-07-28 12:34:56.789").unwrap()
        );
    }

    #[test]
    fn text_parameters_decode_by_declared_oid() {
        assert_eq!(
            decode_param(Some(b"42"), OID_INT8, FORMAT_TEXT).unwrap(),
            Value::Int(42)
        );
        assert_eq!(
            decode_param(Some(b"42"), OID_UNSPECIFIED, FORMAT_TEXT).unwrap(),
            Value::Text("42".into())
        );
        assert_eq!(
            decode_param(Some(b"true"), OID_BOOL, FORMAT_TEXT).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(decode_param(None, OID_INT8, FORMAT_TEXT).unwrap(), Value::Null);
        assert!(decode_param(Some(b"nope"), OID_INT8, FORMAT_TEXT).is_err());
    }

    #[test]
    fn binary_parameters_decode_by_width() {
        assert_eq!(
            decode_param(Some(&7i32.to_be_bytes()), OID_INT4, FORMAT_BINARY).unwrap(),
            Value::Int(7)
        );
        assert_eq!(
            decode_param(Some(&1.5f64.to_be_bytes()), OID_FLOAT8, FORMAT_BINARY).unwrap(),
            Value::Double(1.5)
        );
        // A width that doesn't match the declared type is refused, not
        // silently reinterpreted.
        assert!(decode_param(Some(b"xx"), OID_INT8, FORMAT_BINARY).is_err());
        // And a type we can't decode binary says so by name.
        let e = decode_param(Some(b"\x00"), OID_NUMERIC, FORMAT_BINARY).unwrap_err();
        assert!(e.contains("numeric"), "{e}");
    }

    #[test]
    fn bytea_parameters_accept_the_hex_form() {
        assert_eq!(
            decode_param(Some(b"\\x00ff"), OID_BYTEA, FORMAT_TEXT).unwrap(),
            Value::Bytes(vec![0x00, 0xff].into())
        );
    }
}
