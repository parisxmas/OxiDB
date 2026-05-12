use serde_json::Value;

use crate::error::{Error, Result};

/// Encode a `serde_json::Value` into JSONB binary format.
///
/// Routes Value → JSON text → `jsonb::parse_owned_jsonb_standard_mode`
/// instead of the direct serde Serialize → JSONB path. The serde
/// encoder in jsonb 0.5 allocates a fresh `Serializer` (with its own
/// `Vec<u8>`) for every map value, materializes each child as an
/// intermediate `OwnedJsonb`, then concatenates them at the end —
/// the dominant cost in the encode profile (3-4× slower than the
/// text-parse path) and the cause of an extra `SCALAR_CONTAINER_TAG`
/// (4 bytes) wrapped around every scalar inside a container (which
/// inflates the on-disk image by ~30-50% for realistic docs).
///
/// The text-parse path produces output bytes that are still a valid
/// `OwnedJsonb` and decode through the same `jsonb::from_raw_jsonb` /
/// `RawJsonb::get_by_*` paths — same crate, same struct, same format
/// surface. Legacy fat-format images from older writers continue to
/// decode unchanged.
///
/// Standard mode is used (rather than extended) because `serde_json`
/// always emits strict JSON — there are no leading plus signs, NaN
/// literals, or empty array elements to accommodate.
pub fn encode_doc(value: &Value) -> Result<Vec<u8>> {
    let mut text = Vec::with_capacity(64);
    serde_json::to_writer(&mut text, value)
        .map_err(|e| Error::Codec(e.to_string()))?;
    let owned = jsonb::parse_owned_jsonb_standard_mode(&text)
        .map_err(|e| Error::Codec(e.to_string()))?;
    Ok(owned.to_vec())
}

/// Extract a single top-level field from JSONB bytes without full decode.
/// Returns None if the field doesn't exist or bytes are not JSONB.
/// Much faster than decode_doc() for large nested documents.
pub fn extract_field(bytes: &[u8], field: &str) -> Option<Value> {
    if bytes.is_empty() {
        return None;
    }
    match bytes[0] {
        b'{' | b'[' => {
            // Legacy JSON — must full decode (no partial extraction)
            let doc: Value = serde_json::from_slice(bytes).ok()?;
            doc.get(field).cloned()
        }
        _ => {
            // JSONB binary — partial extraction via get_by_name
            let raw = jsonb::RawJsonb::new(bytes);
            let owned = raw.get_by_name(field, false).ok()??;
            jsonb::from_raw_jsonb(&owned.as_raw()).ok()
        }
    }
}

/// Extract multiple top-level fields from JSONB bytes without full decode.
pub fn extract_fields(bytes: &[u8], fields: &[&str]) -> Vec<(String, Value)> {
    if bytes.is_empty() || fields.is_empty() {
        return Vec::new();
    }
    match bytes[0] {
        b'{' | b'[' => {
            if let Ok(doc) = serde_json::from_slice::<Value>(bytes) {
                fields.iter()
                    .filter_map(|f| doc.get(*f).map(|v| (f.to_string(), v.clone())))
                    .collect()
            } else {
                Vec::new()
            }
        }
        _ => {
            let raw = jsonb::RawJsonb::new(bytes);
            fields.iter()
                .filter_map(|f| {
                    let owned = raw.get_by_name(f, false).ok()??;
                    let val = jsonb::from_raw_jsonb(&owned.as_raw()).ok()?;
                    Some((f.to_string(), val))
                })
                .collect()
        }
    }
}

/// Decode bytes into a `serde_json::Value`.
///
/// Auto-detects the format: if the first byte is `{` (0x7B) or `[` (0x5B),
/// the payload is treated as JSON text; otherwise it is decoded as JSONB binary.
/// This allows transparent reading of legacy JSON `.dat` files alongside new
/// JSONB records without requiring a migration step.
pub fn decode_doc(bytes: &[u8]) -> Result<Value> {
    if bytes.is_empty() {
        return Err(Error::Codec("empty payload".into()));
    }

    match bytes[0] {
        b'{' | b'[' => {
            // Legacy JSON text
            serde_json::from_slice(bytes).map_err(|e| Error::Codec(e.to_string()))
        }
        _ => {
            // JSONB binary
            let raw = jsonb::RawJsonb::new(bytes);
            jsonb::from_raw_jsonb(&raw).map_err(|e| Error::Codec(e.to_string()))
        }
    }
}

/// Decode bytes directly to JSON text, skipping the `serde_json::Value`
/// intermediate.
///
/// `RawJsonb::to_string` walks the JSONB tree once and writes JSON
/// straight to a `String` (via `serde_json::ser::CompactFormatter`)
/// — the wire format the server actually wants. Going through
/// `decode_doc` + `serde_json::to_vec` does the same work plus an
/// allocated `Value` tree, so for find/get wire responses this path
/// is ~5× faster on realistic documents (LARGE: ~30 µs → ~5 µs).
///
/// For legacy `{`/`[`-prefixed JSON text the bytes already _are_ the
/// answer; we just copy them out.
pub fn decode_doc_to_text(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() {
        return Err(Error::Codec("empty payload".into()));
    }
    match bytes[0] {
        b'{' | b'[' => {
            // Legacy JSON text — the bytes are already the answer.
            String::from_utf8(bytes.to_vec())
                .map_err(|e| Error::Codec(e.to_string()))
        }
        _ => {
            // JSONB binary — walk once, emit text.
            Ok(jsonb::RawJsonb::new(bytes).to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_object() {
        let val = json!({"_id": 1, "name": "Alice", "age": 30});
        let encoded = encode_doc(&val).unwrap();
        // JSONB binary should NOT start with '{'
        assert_ne!(encoded[0], b'{');
        let decoded = decode_doc(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn roundtrip_nested() {
        let val = json!({"user": {"name": "Bob", "tags": [1, 2, 3]}, "active": true});
        let encoded = encode_doc(&val).unwrap();
        let decoded = decode_doc(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn legacy_json_compat() {
        let val = json!({"_id": 42, "title": "hello"});
        let json_bytes = serde_json::to_vec(&val).unwrap();
        // decode_doc should handle legacy JSON text
        let decoded = decode_doc(&json_bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn legacy_json_array() {
        let val = json!([1, 2, 3]);
        let json_bytes = serde_json::to_vec(&val).unwrap();
        let decoded = decode_doc(&json_bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn empty_input_errors() {
        assert!(decode_doc(&[]).is_err());
    }

    #[test]
    fn roundtrip_empty_object() {
        let val = json!({});
        let encoded = encode_doc(&val).unwrap();
        let decoded = decode_doc(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn decode_to_text_jsonb_roundtrip() {
        let val = json!({"_id": 1, "name": "Alice", "tags": ["a", "b"], "active": true});
        let encoded = encode_doc(&val).unwrap();
        let text = decode_doc_to_text(&encoded).unwrap();
        // Reparse the text and compare structurally — JSON object key order
        // is not guaranteed to match the input.
        let reparsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(val, reparsed);
    }

    #[test]
    fn decode_to_text_passes_through_legacy_json() {
        let val = json!({"_id": 42});
        let json_bytes = serde_json::to_vec(&val).unwrap();
        let text = decode_doc_to_text(&json_bytes).unwrap();
        let reparsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(val, reparsed);
    }

    #[test]
    fn decode_to_text_empty_input_errors() {
        assert!(decode_doc_to_text(&[]).is_err());
    }

    #[test]
    fn decode_to_text_handles_nested() {
        let val = json!({
            "user": {"name": "Bob", "addr": {"city": "Istanbul", "zip": "34000"}},
            "tags": [1, 2, 3, 4, 5],
            "score": 87.5,
            "active": true,
            "deleted": null,
        });
        let encoded = encode_doc(&val).unwrap();
        let text = decode_doc_to_text(&encoded).unwrap();
        let reparsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(val, reparsed);
    }
}
