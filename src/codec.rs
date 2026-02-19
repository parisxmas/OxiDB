use serde_json::Value;

use crate::error::{Error, Result};

/// Encode a `serde_json::Value` into JSONB binary format.
pub fn encode_doc(value: &Value) -> Result<Vec<u8>> {
    let owned = jsonb::to_owned_jsonb(value)
        .map_err(|e| Error::Codec(e.to_string()))?;
    Ok(owned.to_vec())
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
}
