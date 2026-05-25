//! Engine-local OxiWire encoder.
//!
//! Duplicated from `oxidb-server/src/oxiwire.rs` so the engine's
//! `DocBytesCache` can pre-encode documents at insert time without taking
//! a dependency on the server crate. The two implementations must stay
//! byte-compatible; a fuzz target / round-trip test pinning them together
//! is a Phase 2 follow-up.
//!
//! See `oxidb-server/src/oxiwire.rs` for the type-tag map.

use serde_json::Value;

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_INT64: u8 = 0x03;
const TAG_UINT64: u8 = 0x04;
const TAG_FLOAT: u8 = 0x05;
const TAG_STRING: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_MAP: u8 = 0x08;

#[inline]
pub fn encode_value(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::Bool(false) => buf.push(TAG_FALSE),
        Value::Bool(true) => buf.push(TAG_TRUE),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= 0 {
                    buf.push(TAG_UINT64);
                    buf.extend_from_slice(&(i as u64).to_le_bytes());
                } else {
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&i.to_le_bytes());
                }
            } else if let Some(u) = n.as_u64() {
                buf.push(TAG_UINT64);
                buf.extend_from_slice(&u.to_le_bytes());
            } else if let Some(f) = n.as_f64() {
                buf.push(TAG_FLOAT);
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::String(s) => {
            buf.push(TAG_STRING);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Array(arr) => {
            buf.push(TAG_ARRAY);
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
            for item in arr {
                encode_value(item, buf);
            }
        }
        Value::Object(map) => {
            buf.push(TAG_MAP);
            buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
            for (key, val) in map {
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key.as_bytes());
                encode_value(val, buf);
            }
        }
    }
}

/// Encode a Value into a freshly-allocated bytes buffer. Convenience for
/// cache populators that want an owned `Vec<u8>`.
pub fn encode_value_owned(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    encode_value(value, &mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_simple_object() {
        let v = json!({"name": "alpha", "n": 42});
        let bytes = encode_value_owned(&v);
        // Just sanity-check the encoding starts with the map tag.
        assert_eq!(bytes[0], TAG_MAP);
        assert!(bytes.len() > 10);
    }

    #[test]
    fn null_is_one_byte() {
        let bytes = encode_value_owned(&Value::Null);
        assert_eq!(bytes, vec![TAG_NULL]);
    }
}
