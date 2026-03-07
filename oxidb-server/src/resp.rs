//! RESP (Redis Serialization Protocol) parser and encoder.
//!
//! Implements RESP2 which is used by virtually all Redis clients.
//! See: https://redis.io/docs/reference/protocol-spec/

use std::io::{self, BufRead, Write};

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
    let line = line.trim_end_matches('\n').trim_end_matches('\r');

    if line.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty line"));
    }

    let prefix = line.as_bytes()[0];
    let rest = &line[1..];

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
}
