//! Direct JSONB → OxiWire byte conversion, bypassing `serde_json::Value`.
//!
//! The standard read path goes JSONB bytes → `Value` tree (HashMap-backed,
//! ~3.5 KB per doc) → walk the tree → emit OxiWire bytes. The intermediate
//! tree is the dominant cost for the find-then-ship-to-wire pipeline that
//! big result sets hit; benchmarks show ~20 µs/doc spent there.
//!
//! This module reads JSONB once and emits OxiWire bytes in-place, never
//! constructing a `Value`. Implementation: a custom serde `Visitor` that
//! appends tags and payloads to a `Vec<u8>` as the JSONB deserializer walks
//! the doc.
//!
//! `size_hint()` on JSONB's seq/map deserializers returns the exact count
//! (verified against jsonb 0.5.6's source) — required because OxiWire
//! containers carry a 4-byte length prefix before the elements.

use std::fmt;

use jsonb::{from_raw_jsonb, RawJsonb};
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;

// OxiWire tag constants — keep in lock-step with `wire_oxiwire.rs`.
const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_INT64: u8 = 0x03;
const TAG_UINT64: u8 = 0x04;
const TAG_FLOAT: u8 = 0x05;
const TAG_STRING: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_MAP: u8 = 0x08;

/// Convert JSONB bytes to OxiWire bytes, appending to `out`.
pub fn jsonb_to_oxiwire(jsonb_bytes: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
    let raw = RawJsonb::new(jsonb_bytes);
    let owned: OxiWireOutput =
        from_raw_jsonb(&raw).map_err(|e| format!("jsonb→oxiwire: {e}"))?;
    out.extend_from_slice(&owned.0);
    Ok(())
}

/// Convenience that allocates a fresh `Vec<u8>` for the OxiWire output.
pub fn jsonb_to_oxiwire_owned(jsonb_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let raw = RawJsonb::new(jsonb_bytes);
    let owned: OxiWireOutput =
        from_raw_jsonb(&raw).map_err(|e| format!("jsonb→oxiwire: {e}"))?;
    Ok(owned.0)
}

/// Newtype whose `Deserialize` impl produces OxiWire bytes instead of a
/// `Value` tree. Used via `from_raw_jsonb::<OxiWireOutput>(...)`.
struct OxiWireOutput(Vec<u8>);

impl<'de> Deserialize<'de> for OxiWireOutput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut buf = Vec::with_capacity(256);
        deserializer.deserialize_any(Vis { buf: &mut buf })?;
        Ok(OxiWireOutput(buf))
    }
}

/// `DeserializeSeed` carrying a borrowed buffer — lets seq/map element
/// deserialization append into the same buffer without per-element allocs.
struct Seed<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'de, 'a> DeserializeSeed<'de> for Seed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(Vis { buf: self.buf })
    }
}

struct Vis<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'de, 'a> Visitor<'de> for Vis<'a> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.buf.push(TAG_NULL);
        Ok(())
    }

    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        self.buf.push(TAG_NULL);
        Ok(())
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<(), E> {
        self.buf.push(if v { TAG_TRUE } else { TAG_FALSE });
        Ok(())
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<(), E> {
        if v >= 0 {
            self.buf.push(TAG_UINT64);
            self.buf.extend_from_slice(&(v as u64).to_le_bytes());
        } else {
            self.buf.push(TAG_INT64);
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
        Ok(())
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<(), E> {
        self.buf.push(TAG_UINT64);
        self.buf.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<(), E> {
        self.buf.push(TAG_FLOAT);
        self.buf.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<(), E> {
        self.buf.push(TAG_STRING);
        self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(v.as_bytes());
        Ok(())
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<(), E> {
        self.visit_str(&v)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        let len = seq.size_hint().ok_or_else(|| {
            de::Error::custom("jsonb seq deserializer must provide exact size_hint")
        })?;
        self.buf.push(TAG_ARRAY);
        self.buf.extend_from_slice(&(len as u32).to_le_bytes());
        while seq.next_element_seed(Seed { buf: self.buf })?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let len = map.size_hint().ok_or_else(|| {
            de::Error::custom("jsonb map deserializer must provide exact size_hint")
        })?;
        self.buf.push(TAG_MAP);
        self.buf.extend_from_slice(&(len as u32).to_le_bytes());
        while let Some(key) = map.next_key::<String>()? {
            // Map keys are bare length-prefixed strings — no type tag.
            self.buf
                .extend_from_slice(&(key.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(key.as_bytes());
            map.next_value_seed(Seed { buf: self.buf })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_doc;
    use serde_json::{json, Value};
    use std::io::Read;

    fn jsonb_from(v: &Value) -> Vec<u8> {
        encode_doc(v).unwrap()
    }

    /// Decode an OxiWire-encoded scalar/container into a `serde_json::Value`.
    /// Used by tests so we can compare logical values instead of bytes —
    /// the JSONB→OxiWire converter walks JSONB keys in stored (sorted)
    /// order while `encode_value` walks `serde_json::Value::Object` in
    /// insertion order, so byte-equality fails on map orderings but
    /// logical equality holds.
    fn decode_oxiwire(buf: &[u8]) -> (Value, usize) {
        let mut cur = std::io::Cursor::new(buf);
        let v = read_value(&mut cur);
        (v, cur.position() as usize)
    }

    fn read_value(cur: &mut std::io::Cursor<&[u8]>) -> Value {
        let mut tag = [0u8; 1];
        cur.read_exact(&mut tag).unwrap();
        match tag[0] {
            TAG_NULL => Value::Null,
            TAG_FALSE => Value::Bool(false),
            TAG_TRUE => Value::Bool(true),
            TAG_INT64 => {
                let mut b = [0u8; 8];
                cur.read_exact(&mut b).unwrap();
                Value::Number(i64::from_le_bytes(b).into())
            }
            TAG_UINT64 => {
                let mut b = [0u8; 8];
                cur.read_exact(&mut b).unwrap();
                Value::Number(u64::from_le_bytes(b).into())
            }
            TAG_FLOAT => {
                let mut b = [0u8; 8];
                cur.read_exact(&mut b).unwrap();
                Value::Number(
                    serde_json::Number::from_f64(f64::from_le_bytes(b)).unwrap(),
                )
            }
            TAG_STRING => {
                let mut len_buf = [0u8; 4];
                cur.read_exact(&mut len_buf).unwrap();
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut s = vec![0u8; len];
                cur.read_exact(&mut s).unwrap();
                Value::String(String::from_utf8(s).unwrap())
            }
            TAG_ARRAY => {
                let mut len_buf = [0u8; 4];
                cur.read_exact(&mut len_buf).unwrap();
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut arr = Vec::with_capacity(len);
                for _ in 0..len {
                    arr.push(read_value(cur));
                }
                Value::Array(arr)
            }
            TAG_MAP => {
                let mut len_buf = [0u8; 4];
                cur.read_exact(&mut len_buf).unwrap();
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut map = serde_json::Map::with_capacity(len);
                for _ in 0..len {
                    let mut klen = [0u8; 4];
                    cur.read_exact(&mut klen).unwrap();
                    let klen = u32::from_le_bytes(klen) as usize;
                    let mut k = vec![0u8; klen];
                    cur.read_exact(&mut k).unwrap();
                    let key = String::from_utf8(k).unwrap();
                    let val = read_value(cur);
                    map.insert(key, val);
                }
                Value::Object(map)
            }
            other => panic!("unexpected OxiWire tag: {other}"),
        }
    }

    fn assert_logical_equal(v: Value) {
        let jsonb = jsonb_from(&v);
        let bytes = jsonb_to_oxiwire_owned(&jsonb).expect("convert");
        let (decoded, consumed) = decode_oxiwire(&bytes);
        assert_eq!(consumed, bytes.len(), "trailing bytes after decode");
        assert_eq!(decoded, v, "roundtrip mismatch");
    }

    #[test]
    fn simple_object_roundtrips() {
        assert_logical_equal(json!({"name": "alpha", "n": 1, "ok": true}));
    }

    #[test]
    fn nested_object_roundtrips() {
        assert_logical_equal(json!({"x": 3.5, "nested": {"y": "deep", "z": null}}));
    }

    #[test]
    fn arrays_roundtrip() {
        assert_logical_equal(json!({"tags": ["a", "b", "c"], "n": [1, 2, 3]}));
    }

    #[test]
    fn null_and_bools_roundtrip() {
        assert_logical_equal(json!({"x": null, "y": false, "z": true}));
    }

    #[test]
    fn realistic_employee_roundtrips() {
        assert_logical_equal(json!({
            "name": "Alice",
            "age": 30,
            "salary": 99999.99,
            "active": true,
            "tags": ["eng", "sf"],
            "address": {"street": "1 Main", "zip": "94100"}
        }));
    }

    #[test]
    fn deeply_nested_array_of_objects_roundtrips() {
        assert_logical_equal(json!({
            "items": [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
                {"id": 3, "name": "c"}
            ]
        }));
    }

    #[test]
    fn empty_array_and_object_roundtrip() {
        assert_logical_equal(json!({"empty_arr": [], "empty_obj": {}}));
    }
}
