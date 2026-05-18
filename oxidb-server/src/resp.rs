//! RESP (Redis Serialization Protocol) parser and encoder.
//!
//! Implements RESP2 which is used by virtually all Redis clients.
//! See: https://redis.io/docs/reference/protocol-spec/

use std::io::{self, BufRead, Write};

/// Hard upper bound on a single RESP bulk-string length or array
/// element count (16 MiB), matching the main wire-protocol max in
/// `CLAUDE.md`. Real Redis defaults to `proto-max-bulk-len = 512 MB`,
/// but allocating that much up-front in a connection-handler thread
/// is a denial-of-service vector — better to refuse anything past
/// 16 MiB cleanly than to attempt the allocation and OOM the engine.
/// Surfaced by `wire_resp` / `resp_diff_redis` fuzz targets.
const MAX_RESP_BULK_LEN: i64 = 16 * 1024 * 1024;
const MAX_RESP_ARRAY_LEN: i64 = 16 * 1024 * 1024;

/// A parsed RESP value.
#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Null,
    Array(Vec<RespValue>),
}

impl RespValue {
    /// Extract as a string (for commands).
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RespValue::SimpleString(s) => Some(s),
            RespValue::BulkString(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Extract as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(b) => Some(b),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }
}

/// Read a single RESP value from a buffered reader.
pub fn read_value<R: BufRead>(reader: &mut R) -> io::Result<RespValue> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "client disconnected"));
    }
    // Strip exactly one trailing CRLF (or bare LF for tolerance),
    // preserving any embedded CR in the payload. The old
    // `trim_end_matches('\n').trim_end_matches('\r')` was too
    // greedy — it stripped ALL trailing CRs, silently truncating
    // SimpleString / Error payloads that ended with `\r` (which
    // RESP2 forbids in-spec but mixed clients may emit). Matches
    // redis-rs / canonical RESP2 framing.
    // Surfaced by `resp_diff_redis` fuzz target.
    let line = line.strip_suffix('\n').unwrap_or(&line);
    let line = line.strip_suffix('\r').unwrap_or(line);

    if line.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty line"));
    }

    let prefix = line.as_bytes()[0];
    // Byte 1 may not be a char boundary if the first byte is the start
    // of a multi-byte UTF-8 sequence (e.g. 0xCB 0x87 = 'ˇ'). Naive
    // `&line[1..]` would panic — surfaced by the wire-protocol fuzz
    // target `wire_resp` (PR #45). For *valid* protocol-prefixed lines
    // (+, -, :, $, *) byte 1 IS a char boundary, so this guard only
    // affects the inline-command fallback path, which uses `line`
    // directly anyway.
    let rest = if line.is_char_boundary(1) {
        &line[1..]
    } else {
        ""
    };

    match prefix {
        b'+' => Ok(RespValue::SimpleString(rest.to_string())),
        b'-' => Ok(RespValue::Error(rest.to_string())),
        b':' => {
            let n: i64 = rest.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid integer")
            })?;
            Ok(RespValue::Integer(n))
        }
        b'$' => {
            let len: i64 = rest.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid bulk string length")
            })?;
            if len < 0 {
                return Ok(RespValue::Null);
            }
            if len > MAX_RESP_BULK_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bulk string length {len} exceeds max ({MAX_RESP_BULK_LEN})"),
                ));
            }
            let len = len as usize;
            let mut buf = vec![0u8; len + 2]; // +2 for \r\n
            reader.read_exact(&mut buf)?;
            buf.truncate(len); // remove \r\n
            Ok(RespValue::BulkString(buf))
        }
        b'*' => {
            let count: i64 = rest.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid array length")
            })?;
            if count < 0 {
                return Ok(RespValue::Null);
            }
            if count > MAX_RESP_ARRAY_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("array length {count} exceeds max ({MAX_RESP_ARRAY_LEN})"),
                ));
            }
            let count = count as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(read_value(reader)?);
            }
            Ok(RespValue::Array(items))
        }
        _ => {
            // Inline command: plain text line (e.g. "PING\r\n" from redis-cli)
            let full = format!("{}{}", prefix as char, rest);
            let parts: Vec<RespValue> = full
                .split_whitespace()
                .map(|s| RespValue::BulkString(s.as_bytes().to_vec()))
                .collect();
            if parts.is_empty() {
                Err(io::Error::new(io::ErrorKind::InvalidData, "empty inline command"))
            } else {
                Ok(RespValue::Array(parts))
            }
        }
    }
}

/// Write a RESP value to a writer.
pub fn write_value<W: Write>(writer: &mut W, value: &RespValue) -> io::Result<()> {
    match value {
        RespValue::SimpleString(s) => {
            writer.write_all(b"+")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        RespValue::Error(s) => {
            writer.write_all(b"-")?;
            writer.write_all(s.as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        RespValue::Integer(n) => {
            writer.write_all(b":")?;
            writer.write_all(n.to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
        }
        RespValue::BulkString(b) => {
            writer.write_all(b"$")?;
            writer.write_all(b.len().to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
            writer.write_all(b)?;
            writer.write_all(b"\r\n")?;
        }
        RespValue::Null => {
            writer.write_all(b"$-1\r\n")?;
        }
        RespValue::Array(items) => {
            writer.write_all(b"*")?;
            writer.write_all(items.len().to_string().as_bytes())?;
            writer.write_all(b"\r\n")?;
            for item in items {
                write_value(writer, item)?;
            }
        }
    }
    Ok(())
}

// Helper constructors
pub fn ok() -> RespValue {
    RespValue::SimpleString("OK".to_string())
}

pub fn err(msg: &str) -> RespValue {
    RespValue::Error(format!("ERR {msg}"))
}

pub fn bulk(data: &[u8]) -> RespValue {
    RespValue::BulkString(data.to_vec())
}

pub fn bulk_string(s: &str) -> RespValue {
    RespValue::BulkString(s.as_bytes().to_vec())
}

pub fn integer(n: i64) -> RespValue {
    RespValue::Integer(n)
}

pub fn null() -> RespValue {
    RespValue::Null
}

pub fn array(items: Vec<RespValue>) -> RespValue {
    RespValue::Array(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parse_inline_ping() {
        let mut reader = Cursor::new(b"PING\r\n");
        let val = read_value(&mut reader).unwrap();
        match val {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].as_str().unwrap(), "PING");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn parse_bulk_string_array() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let mut reader = Cursor::new(data);
        let val = read_value(&mut reader).unwrap();
        match val {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].as_str().unwrap(), "GET");
                assert_eq!(items[1].as_str().unwrap(), "foo");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn write_simple_string() {
        let mut buf = Vec::new();
        write_value(&mut buf, &ok()).unwrap();
        assert_eq!(buf, b"+OK\r\n");
    }

    #[test]
    fn write_bulk_string() {
        let mut buf = Vec::new();
        write_value(&mut buf, &bulk_string("hello")).unwrap();
        assert_eq!(buf, b"$5\r\nhello\r\n");
    }

    #[test]
    fn write_null() {
        let mut buf = Vec::new();
        write_value(&mut buf, &null()).unwrap();
        assert_eq!(buf, b"$-1\r\n");
    }

    #[test]
    fn write_array() {
        let mut buf = Vec::new();
        write_value(&mut buf, &array(vec![bulk_string("a"), integer(42)])).unwrap();
        assert_eq!(buf, b"*2\r\n$1\r\na\r\n:42\r\n");
    }

    /// Regression for the resp_diff_redis fuzz finding (3rd run).
    /// `+\r\r\n` is an out-of-spec SimpleString (RESP2 forbids `\r`
    /// in the payload) but mixed clients may emit it. Old greedy
    /// `trim_end_matches('\r')` would silently drop the embedded
    /// CR, parsing as `SimpleString("")`. redis-rs parses as
    /// `SimpleString("\r")` per stricter RESP2 framing. We now
    /// match redis-rs.
    #[test]
    fn fuzz_regression_simple_string_preserves_embedded_cr() {
        let mut reader = Cursor::new(&b"+\r\r\n"[..]);
        match read_value(&mut reader).expect("must parse") {
            RespValue::SimpleString(s) => assert_eq!(s, "\r", "SimpleString must preserve embedded CR, got {s:?}"),
            other => panic!("expected SimpleString, got {other:?}"),
        }
    }

    /// Regression for the resp_diff_redis fuzz finding (post-PR
    /// #59). A 12-byte input `$12222222222` (no terminating CRLF)
    /// claims a bulk-string length of ~12 GB, which the old code
    /// passed straight to `vec![0u8; len + 2]` → OOM the connection
    /// handler thread. MUST now return `Err(InvalidData)` cleanly
    /// well before any allocation.
    #[test]
    fn fuzz_regression_bulk_string_length_bounded() {
        let mut reader = Cursor::new(&b"$12222222222\r\n"[..]);
        let err = read_value(&mut reader).expect_err("must reject huge bulk len");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("12222222222"), "{}", err);
    }

    /// Companion: array length is the same alloc-bomb shape (count
    /// is attacker-controlled, `Vec::with_capacity(count)` blows up).
    #[test]
    fn fuzz_regression_array_length_bounded() {
        let mut reader = Cursor::new(&b"*999999999\r\n"[..]);
        let err = read_value(&mut reader).expect_err("must reject huge array len");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("999999999"), "{}", err);
    }

    /// Sanity: legitimate values at sane lengths still parse.
    #[test]
    fn happy_path_bulk_string_still_works() {
        let mut reader = Cursor::new(&b"$5\r\nhello\r\n"[..]);
        match read_value(&mut reader).expect("happy path") {
            RespValue::BulkString(b) => assert_eq!(b, b"hello"),
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    /// Regression for the wire_resp fuzz finding (post-PR #45). Two
    /// bytes (`0xCB 0x87` = UTF-8 caron 'ˇ') followed by no CRLF used
    /// to panic at `&line[1..]` with "start byte index 1 is not a
    /// char boundary". MUST now return an `Err` cleanly — any panic
    /// here is a DoS vector on the OxiMem RESP listener.
    #[test]
    fn fuzz_regression_multibyte_utf8_prefix_does_not_panic() {
        // No CRLF — read_line consumes to EOF, returning a 2-byte line.
        let mut reader = Cursor::new(&[0xCBu8, 0x87u8][..]);
        // Must NOT panic. Either Ok with the inline-command fallback,
        // or Err — both are acceptable; what matters is no panic.
        let _ = read_value(&mut reader);

        // With a CRLF terminator the same input must also be safe.
        let mut reader = Cursor::new(&[0xCBu8, 0x87u8, b'\r', b'\n'][..]);
        let _ = read_value(&mut reader);
    }
}
