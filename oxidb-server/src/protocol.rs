use std::cell::Cell;
use std::io::{self, Read, Write};

/// Wire format indicator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFormat {
    Json,
    MsgPack,
    OxiWire,
}

/// Read a length-prefixed message: [u32 LE length][payload bytes].
pub fn read_message(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 16 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too large (>16 MiB)",
        ));
    }

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Write a length-prefixed message: [u32 LE length][payload bytes].
pub fn write_message(writer: &mut impl Write, data: &[u8]) -> io::Result<()> {
    let len = (data.len() as u32).to_le_bytes();
    writer.write_all(&len)?;
    writer.write_all(data)?;
    writer.flush()
}

thread_local! {
    static WIRE_FORMAT: Cell<WireFormat> = const { Cell::new(WireFormat::Json) };
}

/// Set the wire format for the current thread/connection.
pub fn set_wire_format(fmt: WireFormat) {
    WIRE_FORMAT.with(|f| f.set(fmt));
}

/// Get the wire format for the current thread/connection.
pub fn wire_format() -> WireFormat {
    WIRE_FORMAT.with(|f| f.get())
}

/// Detect the wire format from the first byte of a message payload.
/// JSON messages start with `{` (0x7B) or `[` (0x5B).
/// OxiWire messages start with `0xDB`.
/// Everything else is treated as MessagePack.
pub fn detect_format(msg: &[u8]) -> WireFormat {
    match msg.first() {
        Some(b'{') | Some(b'[') => WireFormat::Json,
        Some(&crate::oxiwire::MAGIC) => WireFormat::OxiWire,
        _ => WireFormat::MsgPack,
    }
}

/// Deserialize a message payload into a `serde_json::Value`.
/// OxiWire requests use a `0xDB` prefix byte followed by MsgPack body.
pub fn deserialize_message(msg: &[u8]) -> Result<(serde_json::Value, WireFormat), String> {
    let format = detect_format(msg);
    let value = match format {
        WireFormat::Json => {
            serde_json::from_slice(msg).map_err(|e| format!("invalid JSON: {e}"))?
        }
        WireFormat::MsgPack => {
            rmp_serde::from_slice(msg).map_err(|e| format!("invalid MsgPack: {e}"))?
        }
        WireFormat::OxiWire => {
            crate::oxiwire::decode_request(msg)
                .map_err(|e| format!("invalid OxiWire request: {e}"))?
        }
    };
    Ok((value, format))
}

/// Serialize a `serde_json::Value` into the given wire format.
pub fn serialize_response(value: &serde_json::Value, format: WireFormat) -> Vec<u8> {
    match format {
        WireFormat::Json => serde_json::to_vec(value).unwrap(),
        WireFormat::MsgPack => {
            let mut buf = Vec::with_capacity(256);
            value_to_msgpack(value, &mut buf);
            buf
        }
        WireFormat::OxiWire => {
            let mut buf = Vec::with_capacity(256);
            crate::oxiwire::encode_value(value, &mut buf);
            buf
        }
    }
}

/// Convert pre-serialized JSON response bytes to the target wire format.
/// For JSON format, returns the bytes as-is (zero-copy).
/// For MsgPack/OxiWire, re-parses JSON and re-serializes.
pub fn convert_response(json_bytes: Vec<u8>, format: WireFormat) -> Vec<u8> {
    match format {
        WireFormat::Json => json_bytes,
        WireFormat::MsgPack => {
            let value: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
            let mut buf = Vec::with_capacity(json_bytes.len());
            value_to_msgpack(&value, &mut buf);
            buf
        }
        WireFormat::OxiWire => {
            let value: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
            crate::oxiwire::ok_response(&value)
        }
    }
}

/// Manually encode `serde_json::Value` to MsgPack bytes.
///
/// This avoids `rmp_serde::to_vec(&Value)` which breaks when `serde_json` has
/// `arbitrary_precision` enabled — it serializes numbers as `["string"]` tuples
/// instead of proper MsgPack integers/floats.
pub fn value_to_msgpack(value: &serde_json::Value, buf: &mut Vec<u8>) {
    match value {
        serde_json::Value::Null => {
            rmp::encode::write_nil(buf).unwrap();
        }
        serde_json::Value::Bool(b) => {
            rmp::encode::write_bool(buf, *b).unwrap();
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rmp::encode::write_sint(buf, i).unwrap();
            } else if let Some(u) = n.as_u64() {
                rmp::encode::write_uint(buf, u).unwrap();
            } else if let Some(f) = n.as_f64() {
                rmp::encode::write_f64(buf, f).unwrap();
            }
        }
        serde_json::Value::String(s) => {
            rmp::encode::write_str(buf, s).unwrap();
        }
        serde_json::Value::Array(arr) => {
            rmp::encode::write_array_len(buf, arr.len() as u32).unwrap();
            for item in arr {
                value_to_msgpack(item, buf);
            }
        }
        serde_json::Value::Object(map) => {
            rmp::encode::write_map_len(buf, map.len() as u32).unwrap();
            for (key, val) in map {
                rmp::encode::write_str(buf, key).unwrap();
                value_to_msgpack(val, buf);
            }
        }
    }
}
