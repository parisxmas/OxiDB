//! OxiWire — OxiDB's custom binary wire protocol for maximum decode speed.
//!
//! All lengths are fixed-size (4 bytes LE), all numbers are fixed-size (8 bytes LE).
//! This eliminates variable-length integer decoding overhead on the client side.
//!
//! Type tags:
//!   0x00 = Null
//!   0x01 = False
//!   0x02 = True
//!   0x03 = Int64  (8 bytes LE)
//!   0x04 = Uint64 (8 bytes LE)
//!   0x05 = Float64 (8 bytes LE)
//!   0x06 = String (4-byte LE length + raw bytes)
//!   0x07 = Array  (4-byte LE count + values)
//!   0x08 = Map    (4-byte LE count + (string-key, value) pairs)
//!
//! Response envelope: [0xDB] [status: 0=ok, 1=error] [value]

use serde_json::Value;
use std::sync::Arc;

pub const MAGIC: u8 = 0xDB;

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_INT64: u8 = 0x03;
const TAG_UINT64: u8 = 0x04;
const TAG_FLOAT: u8 = 0x05;
const TAG_STRING: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_MAP: u8 = 0x08;

/// Encode a serde_json::Value into OxiWire binary format.
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
                // Map keys are bare strings (no type tag — always string)
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key.as_bytes());
                encode_value(val, buf);
            }
        }
    }
}

/// Encode a successful response with the given data value.
pub fn ok_response(data: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.push(MAGIC);
    buf.push(0x00); // ok
    encode_value(data, &mut buf);
    buf
}

/// Encode an error response.
pub fn err_response(msg: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(msg.len() + 8);
    buf.push(MAGIC);
    buf.push(0x01); // error
    buf.push(TAG_STRING);
    buf.extend_from_slice(&(msg.len() as u32).to_le_bytes());
    buf.extend_from_slice(msg.as_bytes());
    buf
}

/// Encode a document array response — the hot path for find queries.
/// Serializes directly from Arc references without cloning.
pub fn ok_docs_response(docs: &[Arc<Value>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(docs.len() * 150 + 16);
    buf.push(MAGIC);
    buf.push(0x00); // ok
    buf.push(TAG_ARRAY);
    buf.extend_from_slice(&(docs.len() as u32).to_le_bytes());
    for doc in docs {
        encode_value(doc.as_ref(), &mut buf);
    }
    buf
}

/// Threshold for switching to parallel serialization.
const PARALLEL_THRESHOLD: usize = 5_000;

/// Maximum number of threads for parallel serialization.
const MAX_PARALLEL_THREADS: usize = 8;

/// Encode a document array response with OxiWire binary format.
/// For large result sets (>= 5000 docs), serialization is parallelized across
/// multiple threads using `std::thread::scope`.
pub fn ok_docs_response_fast(docs: &[Arc<Value>]) -> Vec<u8> {
    if docs.len() >= PARALLEL_THRESHOLD {
        return ok_docs_response_parallel(docs);
    }

    ok_docs_response(docs)
}

/// Build an OxiWire response from pre-encoded doc bytes. Used by the
/// bytes-first find path: each `Arc<[u8]>` already contains a valid
/// OxiWire-encoded document body, so we just frame them in the response
/// array. No per-doc encode runs.
pub fn ok_docs_bytes_response(docs: &[Arc<[u8]>]) -> Vec<u8> {
    let total: usize = docs.iter().map(|b| b.len()).sum();
    let mut buf = Vec::with_capacity(total + 16);
    buf.push(MAGIC);
    buf.push(0x00);
    buf.push(TAG_ARRAY);
    buf.extend_from_slice(&(docs.len() as u32).to_le_bytes());
    for d in docs {
        buf.extend_from_slice(d);
    }
    buf
}

/// Frame `count` already-concatenated OxiWire document encodings into an array
/// response. Pairs with `OxiDb::find_oxiwire_postfilter`, which encodes all
/// matches into one buffer — framed with a single header, no per-doc `Arc`.
pub fn ok_docs_concat_response(count: usize, doc_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(doc_bytes.len() + 8);
    buf.push(MAGIC);
    buf.push(0x00);
    buf.push(TAG_ARRAY);
    buf.extend_from_slice(&(count as u32).to_le_bytes());
    buf.extend_from_slice(doc_bytes);
    buf
}

/// Parallel serialization path for large result sets.
/// Splits doc slice into chunks, each thread serializes into one contiguous buffer.
fn ok_docs_response_parallel(docs: &[Arc<Value>]) -> Vec<u8> {
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(MAX_PARALLEL_THREADS);

    if num_cpus <= 1 {
        return ok_docs_response(docs);
    }

    let chunk_size = docs.len().div_ceil(num_cpus);

    let chunk_bufs: Vec<Vec<u8>> = std::thread::scope(|s| {
        let handles: Vec<_> = docs
            .chunks(chunk_size)
            .map(|chunk| {
                s.spawn(move || {
                    let mut buf = Vec::with_capacity(chunk.len() * 150);
                    for doc in chunk {
                        encode_value(doc.as_ref(), &mut buf);
                    }
                    buf
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("parallel serialization thread panicked"))
            .collect()
    });

    let total: usize = chunk_bufs.iter().map(|b| b.len()).sum();
    let mut buf = Vec::with_capacity(7 + total);
    buf.push(MAGIC);
    buf.push(0x00);
    buf.push(TAG_ARRAY);
    buf.extend_from_slice(&(docs.len() as u32).to_le_bytes());
    for chunk_buf in &chunk_bufs {
        buf.extend_from_slice(chunk_buf);
    }

    buf
}

/// Decode OxiWire binary format back into serde_json::Value.
/// Used for decoding client requests sent in OxiWire format.
pub fn decode_value(buf: &[u8], pos: &mut usize) -> Result<Value, String> {
    if *pos >= buf.len() {
        return Err("unexpected end of input".to_string());
    }
    let tag = buf[*pos];
    *pos += 1;

    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_FALSE => Ok(Value::Bool(false)),
        TAG_TRUE => Ok(Value::Bool(true)),
        TAG_INT64 => {
            if *pos + 8 > buf.len() {
                return Err("truncated int64".to_string());
            }
            let v = i64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(Value::Number(serde_json::Number::from(v)))
        }
        TAG_UINT64 => {
            if *pos + 8 > buf.len() {
                return Err("truncated uint64".to_string());
            }
            let v = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(Value::Number(serde_json::Number::from(v)))
        }
        TAG_FLOAT => {
            if *pos + 8 > buf.len() {
                return Err("truncated float64".to_string());
            }
            let bits = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            let f = f64::from_bits(bits);
            match serde_json::Number::from_f64(f) {
                Some(n) => Ok(Value::Number(n)),
                None => Ok(Value::Null), // NaN/Inf
            }
        }
        TAG_STRING => {
            if *pos + 4 > buf.len() {
                return Err("truncated string length".to_string());
            }
            let len = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            if *pos + len > buf.len() {
                return Err("truncated string data".to_string());
            }
            let s = std::str::from_utf8(&buf[*pos..*pos + len])
                .map_err(|e| format!("invalid utf8: {e}"))?;
            *pos += len;
            Ok(Value::String(s.to_string()))
        }
        TAG_ARRAY => {
            if *pos + 4 > buf.len() {
                return Err("truncated array length".to_string());
            }
            let count = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            // ALLOC GUARD (fuzz finding, post-PR #45): the wire-provided
            // count is attacker-controlled. Pre-allocating with that
            // capacity is an alloc bomb — a 7-byte payload claiming
            // 0xFFFFFFFF elements asked for ~32 GiB of Vec headroom and
            // OOM'd the process. Cap at remaining bytes; the loop body
            // would fail on the very first truncation anyway, so this
            // changes nothing for honest inputs.
            let cap = count.min(buf.len().saturating_sub(*pos));
            let mut arr = Vec::with_capacity(cap);
            for _ in 0..count {
                arr.push(decode_value(buf, pos)?);
            }
            Ok(Value::Array(arr))
        }
        TAG_MAP => {
            if *pos + 4 > buf.len() {
                return Err("truncated map length".to_string());
            }
            let count = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            // Same alloc-guard rationale as TAG_ARRAY above. A map
            // entry needs at least 4 bytes (key-len u32), so the
            // remaining-bytes / 4 ceiling is a tighter cap.
            let cap = count.min(buf.len().saturating_sub(*pos) / 4);
            let mut map = serde_json::Map::with_capacity(cap);
            for _ in 0..count {
                // Keys are bare strings (length + bytes, no type tag)
                if *pos + 4 > buf.len() {
                    return Err("truncated map key length".to_string());
                }
                let key_len = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
                *pos += 4;
                if *pos + key_len > buf.len() {
                    return Err("truncated map key data".to_string());
                }
                let key = std::str::from_utf8(&buf[*pos..*pos + key_len])
                    .map_err(|e| format!("invalid utf8 key: {e}"))?
                    .to_string();
                *pos += key_len;
                let val = decode_value(buf, pos)?;
                map.insert(key, val);
            }
            Ok(Value::Object(map))
        }
        _ => Err(format!("unknown tag 0x{tag:02x} at position {}", *pos - 1)),
    }
}

/// Decode an OxiWire request (0xDB prefix + value).
pub fn decode_request(msg: &[u8]) -> Result<Value, String> {
    if msg.is_empty() || msg[0] != MAGIC {
        return Err("not an OxiWire message".to_string());
    }
    let mut pos = 1; // skip magic byte
    decode_value(msg, &mut pos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_primitives() {
        let mut buf = Vec::new();
        encode_value(&json!(null), &mut buf);
        assert_eq!(buf, vec![TAG_NULL]);

        buf.clear();
        encode_value(&json!(true), &mut buf);
        assert_eq!(buf, vec![TAG_TRUE]);

        buf.clear();
        encode_value(&json!(false), &mut buf);
        assert_eq!(buf, vec![TAG_FALSE]);
    }

    #[test]
    fn encode_string() {
        let mut buf = Vec::new();
        encode_value(&json!("hello"), &mut buf);
        assert_eq!(buf[0], TAG_STRING);
        assert_eq!(u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]), 5);
        assert_eq!(&buf[5..], b"hello");
    }

    #[test]
    fn encode_int() {
        let mut buf = Vec::new();
        encode_value(&json!(42), &mut buf);
        assert_eq!(buf[0], TAG_UINT64);
        let v = u64::from_le_bytes(buf[1..9].try_into().unwrap());
        assert_eq!(v, 42);
    }

    #[test]
    fn encode_map() {
        let mut buf = Vec::new();
        encode_value(&json!({"a": 1}), &mut buf);
        assert_eq!(buf[0], TAG_MAP);
        // count = 1
        assert_eq!(u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]), 1);
    }

    #[test]
    fn ok_response_magic() {
        let resp = ok_response(&json!("test"));
        assert_eq!(resp[0], MAGIC);
        assert_eq!(resp[1], 0x00);
    }

    #[test]
    fn err_response_magic() {
        let resp = err_response("oops");
        assert_eq!(resp[0], MAGIC);
        assert_eq!(resp[1], 0x01);
    }

    #[test]
    fn decode_roundtrip_map() {
        let original = json!({"cmd": "find", "collection": "users", "query": {"active": true}});
        let mut buf = vec![MAGIC];
        encode_value(&original, &mut buf);
        let decoded = decode_request(&buf).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_roundtrip_array() {
        let original = json!([1, "hello", null, false, 2.75]);
        let mut buf = Vec::new();
        encode_value(&original, &mut buf);
        let mut pos = 0;
        let decoded = decode_value(&buf, &mut pos).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_roundtrip_nested() {
        let original = json!({
            "cmd": "insert_many",
            "collection": "test",
            "docs": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": -5, "tags": ["a", "b"]}
            ]
        });
        let mut buf = vec![MAGIC];
        encode_value(&original, &mut buf);
        let decoded = decode_request(&buf).unwrap();
        assert_eq!(decoded, original);
    }

    /// Regression for the wire_oxiwire / wire_deserialize fuzz findings
    /// (post-PR #45). A 7-byte payload claiming TAG_ARRAY with
    /// `0xFFFFFFFF` element count used to call `Vec::with_capacity(4B)`
    /// and OOM the process before reaching the truncation check in the
    /// loop body. Must now return an `Err` cleanly.
    #[test]
    fn fuzz_regression_array_count_does_not_alloc_bomb() {
        // wire_deserialize OOM input: [0xDB, 0x07, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        // 0xDB = MAGIC, 0x07 = TAG_ARRAY, then 4 bytes of u32 count =
        // 0xFFFFFFFF = ~4 billion elements claimed, only 1 byte of
        // payload after. Must NOT pre-allocate 4B-sized Vec.
        let input = [0xDBu8, TAG_ARRAY, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8];
        let res = decode_request(&input);
        assert!(res.is_err(), "expected Err, got {res:?}");
    }

    #[test]
    fn fuzz_regression_map_count_does_not_alloc_bomb() {
        // wire_oxiwire OOM input: [0xDB, 0x08, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xDB]
        // 0xDB = MAGIC, 0x08 = TAG_MAP, then 4 bytes of u32 count.
        let input = [
            0xDBu8, TAG_MAP, 0x00u8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xDBu8,
        ];
        let res = decode_request(&input);
        assert!(res.is_err(), "expected Err, got {res:?}");
    }

    #[test]
    fn fuzz_regression_array_with_honest_count_still_works() {
        // Sanity: the alloc-guard must NOT break the happy path. An
        // array of 3 nulls is 1 (MAGIC) + 1 (TAG_ARRAY) + 4 (count=3) + 3 (NULLs)
        let input = [
            0xDBu8, TAG_ARRAY, 3u8, 0u8, 0u8, 0u8, TAG_NULL, TAG_NULL, TAG_NULL,
        ];
        let decoded = decode_request(&input).expect("honest 3-element array");
        match decoded {
            Value::Array(a) => {
                assert_eq!(a.len(), 3);
                assert!(a.iter().all(|v| v.is_null()));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
