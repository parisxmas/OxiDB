use std::io::{self, Read, Write};

/// PostgreSQL protocol version 3.0
const PROTOCOL_VERSION_30: i32 = 196608; // 3 << 16

/// SSL request code (special startup message)
const SSL_REQUEST_CODE: i32 = 80877103;

/// Parsed startup message from the client.
pub struct StartupMessage {
    pub params: Vec<(String, String)>,
}

/// Frontend (client) message types we handle.
pub enum FrontendMessage {
    /// SSL negotiation request (no tag byte).
    SslRequest,
    /// Startup message with protocol version 3.0 and parameters.
    Startup(StartupMessage),
    /// Simple query ('Q').
    Query(String),
    /// Parse ('P') — Extended Query Protocol: prepare a statement.
    Parse {
        name: String,
        sql: String,
        param_types: Vec<i32>,
    },
    /// Bind ('B') — Extended Query Protocol: bind parameters to a portal.
    Bind {
        portal: String,
        statement: String,
        param_values: Vec<Option<Vec<u8>>>,
    },
    /// Describe ('D') — describe a statement ('S') or portal ('P').
    Describe { kind: u8, name: String },
    /// Execute ('E') — execute a bound portal.
    Execute { portal: String, max_rows: i32 },
    /// Sync ('S') — end of extended query batch.
    Sync,
    /// Flush ('H') — flush output.
    Flush,
    /// Close ('C') — close a statement or portal.
    Close { kind: u8, name: String },
    /// Terminate ('X').
    Terminate,
}

// ── Reading ──────────────────────────────────────────────────────────

/// Read the initial message from a client (startup or SSL request).
/// Startup messages have no tag byte — just `[i32 length][i32 version][params]`.
pub fn read_startup<R: Read>(r: &mut R) -> io::Result<FrontendMessage> {
    let len = read_i32(r)?;
    if len < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "startup message too short",
        ));
    }
    let code = read_i32(r)?;

    if code == SSL_REQUEST_CODE {
        // SSL request has exactly 8 bytes total (len + code), no more payload.
        return Ok(FrontendMessage::SslRequest);
    }

    if code != PROTOCOL_VERSION_30 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported protocol version: {code}"),
        ));
    }

    // Remaining bytes after length(4) + version(4)
    let remaining = (len - 8) as usize;
    let mut buf = vec![0u8; remaining];
    r.read_exact(&mut buf)?;

    // Parse null-terminated key-value pairs ending with a final \0.
    let params = parse_startup_params(&buf);
    Ok(FrontendMessage::Startup(StartupMessage { params }))
}

/// Read a tagged frontend message (after startup is complete).
/// Format: `[u8 tag][i32 length_including_self][payload]`
pub fn read_message<R: Read>(r: &mut R) -> io::Result<FrontendMessage> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    let tag = tag[0];

    let len = read_i32(r)?;
    let payload_len = (len - 4) as usize;

    match tag {
        b'Q' => {
            let mut buf = vec![0u8; payload_len];
            r.read_exact(&mut buf)?;
            let sql = cstr_from_bytes(&buf);
            Ok(FrontendMessage::Query(sql))
        }
        b'P' => {
            let mut buf = vec![0u8; payload_len];
            r.read_exact(&mut buf)?;
            let (name, rest) = read_cstr_from_buf(&buf);
            let (sql, rest) = read_cstr_from_buf(rest);
            let num_params = if rest.len() >= 2 {
                i16::from_be_bytes([rest[0], rest[1]]) as usize
            } else {
                0
            };
            let mut param_types = Vec::with_capacity(num_params);
            let mut offset = 2;
            for _ in 0..num_params {
                if offset + 4 <= rest.len() {
                    param_types.push(i32::from_be_bytes([
                        rest[offset],
                        rest[offset + 1],
                        rest[offset + 2],
                        rest[offset + 3],
                    ]));
                    offset += 4;
                }
            }
            Ok(FrontendMessage::Parse {
                name,
                sql,
                param_types,
            })
        }
        b'B' => {
            let mut buf = vec![0u8; payload_len];
            r.read_exact(&mut buf)?;
            let (portal, rest) = read_cstr_from_buf(&buf);
            let (statement, rest) = read_cstr_from_buf(rest);

            // Skip format codes
            let mut pos = if rest.len() >= 2 {
                let n = i16::from_be_bytes([rest[0], rest[1]]) as usize;
                2 + n * 2
            } else {
                2
            };

            // Read parameter values
            let num_params = if pos + 2 <= rest.len() {
                let n = i16::from_be_bytes([rest[pos], rest[pos + 1]]) as usize;
                pos += 2;
                n
            } else {
                0
            };
            let mut param_values = Vec::with_capacity(num_params);
            for _ in 0..num_params {
                if pos + 4 > rest.len() {
                    break;
                }
                let val_len = i32::from_be_bytes([
                    rest[pos],
                    rest[pos + 1],
                    rest[pos + 2],
                    rest[pos + 3],
                ]);
                pos += 4;
                if val_len == -1 {
                    param_values.push(None);
                } else {
                    let end = pos + val_len as usize;
                    if end <= rest.len() {
                        param_values.push(Some(rest[pos..end].to_vec()));
                    } else {
                        param_values.push(None);
                    }
                    pos = end;
                }
            }

            Ok(FrontendMessage::Bind {
                portal,
                statement,
                param_values,
            })
        }
        b'D' => {
            let mut buf = vec![0u8; payload_len];
            r.read_exact(&mut buf)?;
            let kind = buf[0];
            let (name, _) = read_cstr_from_buf(&buf[1..]);
            Ok(FrontendMessage::Describe { kind, name })
        }
        b'E' => {
            let mut buf = vec![0u8; payload_len];
            r.read_exact(&mut buf)?;
            let (portal, rest) = read_cstr_from_buf(&buf);
            let max_rows = if rest.len() >= 4 {
                i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]])
            } else {
                0
            };
            Ok(FrontendMessage::Execute { portal, max_rows })
        }
        b'S' => {
            if payload_len > 0 {
                let mut discard = vec![0u8; payload_len];
                r.read_exact(&mut discard)?;
            }
            Ok(FrontendMessage::Sync)
        }
        b'H' => {
            if payload_len > 0 {
                let mut discard = vec![0u8; payload_len];
                r.read_exact(&mut discard)?;
            }
            Ok(FrontendMessage::Flush)
        }
        b'C' => {
            let mut buf = vec![0u8; payload_len];
            r.read_exact(&mut buf)?;
            let kind = buf[0];
            let (name, _) = read_cstr_from_buf(&buf[1..]);
            Ok(FrontendMessage::Close { kind, name })
        }
        b'X' => {
            if payload_len > 0 {
                let mut discard = vec![0u8; payload_len];
                r.read_exact(&mut discard)?;
            }
            Ok(FrontendMessage::Terminate)
        }
        _ => {
            // Skip unknown messages.
            if payload_len > 0 {
                let mut discard = vec![0u8; payload_len];
                r.read_exact(&mut discard)?;
            }
            Err(io::Error::other(format!(
                "unsupported message tag: {}",
                tag as char
            )))
        }
    }
}

// ── Writing ──────────────────────────────────────────────────────────

/// Send `AuthenticationOk` (tag 'R', auth type 0).
pub fn write_auth_ok<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"R")?;
    write_i32(w, 8)?; // length: 4 (self) + 4 (auth type)
    write_i32(w, 0)?; // AuthenticationOk
    Ok(())
}

/// Send a `ParameterStatus` message (tag 'S').
pub fn write_parameter_status<W: Write>(w: &mut W, name: &str, value: &str) -> io::Result<()> {
    let len = 4 + name.len() + 1 + value.len() + 1;
    w.write_all(b"S")?;
    write_i32(w, len as i32)?;
    write_cstr(w, name)?;
    write_cstr(w, value)?;
    Ok(())
}

/// Send `BackendKeyData` (tag 'K'): process ID and secret key.
pub fn write_backend_key_data<W: Write>(w: &mut W, pid: i32, secret: i32) -> io::Result<()> {
    w.write_all(b"K")?;
    write_i32(w, 12)?; // 4 + 4 + 4
    write_i32(w, pid)?;
    write_i32(w, secret)?;
    Ok(())
}

/// Send `ReadyForQuery` (tag 'Z'): transaction status indicator.
///   'I' = idle, 'T' = in transaction, 'E' = failed transaction
pub fn write_ready_for_query<W: Write>(w: &mut W, status: u8) -> io::Result<()> {
    w.write_all(b"Z")?;
    write_i32(w, 5)?; // 4 + 1
    w.write_all(&[status])?;
    Ok(())
}

/// Column definition for `RowDescription`.
#[derive(Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_oid: i32,
    pub type_len: i16,
    pub type_mod: i32,
}

/// Send `RowDescription` (tag 'T').
pub fn write_row_description<W: Write>(w: &mut W, columns: &[ColumnDef]) -> io::Result<()> {
    // Calculate total length.
    let mut body_len: usize = 2; // i16 field count
    for col in columns {
        // name\0 + table_oid(4) + col_attr(2) + type_oid(4) + type_len(2) + type_mod(4) + format(2)
        body_len += col.name.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    }

    w.write_all(b"T")?;
    write_i32(w, (4 + body_len) as i32)?;
    write_i16(w, columns.len() as i16)?;

    for col in columns {
        write_cstr(w, &col.name)?;
        write_i32(w, 0)?; // table OID (0 = not from a table)
        write_i16(w, 0)?; // column attribute number
        write_i32(w, col.type_oid)?; // type OID
        write_i16(w, col.type_len)?; // type size
        write_i32(w, col.type_mod)?; // type modifier
        write_i16(w, 0)?; // format code: 0 = text
    }
    Ok(())
}

/// Send a `DataRow` (tag 'D') with text-format column values.
/// `None` values are sent as SQL NULL (length = -1).
pub fn write_data_row<W: Write>(w: &mut W, values: &[Option<String>]) -> io::Result<()> {
    // Calculate body length: 2 (field count) + per-field (4 length + data).
    let mut body_len: usize = 2;
    for v in values {
        body_len += 4; // i32 length (or -1 for NULL)
        if let Some(s) = v {
            body_len += s.len();
        }
    }

    w.write_all(b"D")?;
    write_i32(w, (4 + body_len) as i32)?;
    write_i16(w, values.len() as i16)?;

    for v in values {
        match v {
            Some(s) => {
                write_i32(w, s.len() as i32)?;
                w.write_all(s.as_bytes())?;
            }
            None => {
                write_i32(w, -1)?; // NULL
            }
        }
    }
    Ok(())
}

/// Send `CommandComplete` (tag 'C').
pub fn write_command_complete<W: Write>(w: &mut W, tag: &str) -> io::Result<()> {
    let len = 4 + tag.len() + 1;
    w.write_all(b"C")?;
    write_i32(w, len as i32)?;
    write_cstr(w, tag)?;
    Ok(())
}

/// Send `ErrorResponse` (tag 'E').
pub fn write_error_response<W: Write>(
    w: &mut W,
    severity: &str,
    code: &str,
    message: &str,
) -> io::Result<()> {
    // Fields: S (severity), V (severity non-localized), C (code), M (message), terminator \0
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'V');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0); // terminator

    w.write_all(b"E")?;
    write_i32(w, (4 + body.len()) as i32)?;
    w.write_all(&body)?;
    Ok(())
}

/// Send `EmptyQueryResponse` (tag 'I').
pub fn write_empty_query_response<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"I")?;
    write_i32(w, 4)?;
    Ok(())
}

/// Send `NoData` (tag 'n').
pub fn write_no_data<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"n")?;
    write_i32(w, 4)?;
    Ok(())
}

/// Send `ParseComplete` (tag '1').
pub fn write_parse_complete<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"1")?;
    write_i32(w, 4)?;
    Ok(())
}

/// Send `BindComplete` (tag '2').
pub fn write_bind_complete<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"2")?;
    write_i32(w, 4)?;
    Ok(())
}

/// Send `CloseComplete` (tag '3').
pub fn write_close_complete<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"3")?;
    write_i32(w, 4)?;
    Ok(())
}

/// Send `ParameterDescription` (tag 't') with zero parameters.
pub fn write_parameter_description_empty<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"t")?;
    write_i32(w, 6)?; // 4 + 2
    write_i16(w, 0)?; // zero parameters
    Ok(())
}

/// Send `ParameterDescription` (tag 't') with the given number of TEXT parameters.
pub fn write_parameter_description<W: Write>(w: &mut W, count: i16) -> io::Result<()> {
    if count == 0 {
        return write_parameter_description_empty(w);
    }
    let len = 4 + 2 + (count as i32 * 4);
    w.write_all(b"t")?;
    write_i32(w, len)?;
    write_i16(w, count)?;
    for _ in 0..count {
        write_i32(w, 25)?; // TEXT OID
    }
    Ok(())
}

/// Count the highest `$N` placeholder in a SQL string.
pub fn count_parameters(sql: &str) -> i16 {
    let mut max_param: i16 = 0;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i > start {
                if let Ok(n) = sql[start..i].parse::<i16>() {
                    if n > max_param {
                        max_param = n;
                    }
                }
            }
        } else {
            i += 1;
        }
    }
    max_param
}

// ── Helpers ──────────────────────────────────────────────────────────

fn read_i32<R: Read>(r: &mut R) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn write_i32<W: Write>(w: &mut W, v: i32) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

fn write_i16<W: Write>(w: &mut W, v: i16) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

fn write_cstr<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    w.write_all(s.as_bytes())?;
    w.write_all(&[0])?;
    Ok(())
}

/// Extract a null-terminated string from a byte slice.
pub fn cstr_from_bytes(buf: &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// Read a null-terminated string from a buffer, returning the string and remaining bytes.
fn read_cstr_from_buf(buf: &[u8]) -> (String, &[u8]) {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let s = String::from_utf8_lossy(&buf[..end]).into_owned();
    let rest = if end + 1 <= buf.len() {
        &buf[end + 1..]
    } else {
        &[]
    };
    (s, rest)
}

/// Parse startup message parameters (key\0value\0...key\0value\0\0).
fn parse_startup_params(buf: &[u8]) -> Vec<(String, String)> {
    let mut params = Vec::new();
    let mut i = 0;
    loop {
        if i >= buf.len() || buf[i] == 0 {
            break;
        }
        // Read key
        let key_start = i;
        while i < buf.len() && buf[i] != 0 {
            i += 1;
        }
        let key = String::from_utf8_lossy(&buf[key_start..i]).into_owned();
        i += 1; // skip null

        // Read value
        let val_start = i;
        while i < buf.len() && buf[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&buf[val_start..i]).into_owned();
        i += 1; // skip null

        params.push((key, value));
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_startup_ssl_request() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&8i32.to_be_bytes());
        buf.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
        let mut cursor = Cursor::new(buf);
        match read_startup(&mut cursor).unwrap() {
            FrontendMessage::SslRequest => {}
            _ => panic!("expected SslRequest"),
        }
    }

    #[test]
    fn test_read_startup_v3() {
        let mut buf = Vec::new();
        let params = b"user\0oxidb\0database\0mydb\0\0";
        let len = 8 + params.len() as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
        buf.extend_from_slice(params);

        let mut cursor = Cursor::new(buf);
        match read_startup(&mut cursor).unwrap() {
            FrontendMessage::Startup(msg) => {
                assert_eq!(msg.params.len(), 2);
                assert_eq!(msg.params[0], ("user".to_string(), "oxidb".to_string()));
                assert_eq!(msg.params[1], ("database".to_string(), "mydb".to_string()));
            }
            _ => panic!("expected Startup"),
        }
    }

    #[test]
    fn test_read_query_message() {
        let sql = "SELECT 1;\0";
        let len = 4 + sql.len() as i32;
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(sql.as_bytes());

        let mut cursor = Cursor::new(buf);
        match read_message(&mut cursor).unwrap() {
            FrontendMessage::Query(s) => assert_eq!(s, "SELECT 1;"),
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_read_terminate_message() {
        let mut buf = Vec::new();
        buf.push(b'X');
        buf.extend_from_slice(&4i32.to_be_bytes());

        let mut cursor = Cursor::new(buf);
        match read_message(&mut cursor).unwrap() {
            FrontendMessage::Terminate => {}
            _ => panic!("expected Terminate"),
        }
    }

    #[test]
    fn test_read_sync_message() {
        let mut buf = Vec::new();
        buf.push(b'S');
        buf.extend_from_slice(&4i32.to_be_bytes());

        let mut cursor = Cursor::new(buf);
        match read_message(&mut cursor).unwrap() {
            FrontendMessage::Sync => {}
            _ => panic!("expected Sync"),
        }
    }

    #[test]
    fn test_read_parse_message() {
        // Parse message: name="" sql="SELECT 1" param_types=[]
        let mut payload = Vec::new();
        payload.push(0); // empty statement name
        payload.extend_from_slice(b"SELECT 1");
        payload.push(0); // null terminator
        payload.extend_from_slice(&0i16.to_be_bytes()); // 0 param types

        let len = 4 + payload.len() as i32;
        let mut buf = Vec::new();
        buf.push(b'P');
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&payload);

        let mut cursor = Cursor::new(buf);
        match read_message(&mut cursor).unwrap() {
            FrontendMessage::Parse { name, sql, param_types } => {
                assert_eq!(name, "");
                assert_eq!(sql, "SELECT 1");
                assert!(param_types.is_empty());
            }
            _ => panic!("expected Parse"),
        }
    }

    #[test]
    fn test_write_auth_ok() {
        let mut buf = Vec::new();
        write_auth_ok(&mut buf).unwrap();
        assert_eq!(buf, vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    }

    #[test]
    fn test_write_ready_for_query() {
        let mut buf = Vec::new();
        write_ready_for_query(&mut buf, b'I').unwrap();
        assert_eq!(buf, vec![b'Z', 0, 0, 0, 5, b'I']);
    }

    #[test]
    fn test_write_command_complete() {
        let mut buf = Vec::new();
        write_command_complete(&mut buf, "SELECT 1").unwrap();
        assert_eq!(buf[0], b'C');
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 13);
        assert_eq!(&buf[5..13], b"SELECT 1");
        assert_eq!(buf[13], 0);
    }

    #[test]
    fn test_write_data_row() {
        let values = vec![Some("hello".to_string()), None, Some("42".to_string())];
        let mut buf = Vec::new();
        write_data_row(&mut buf, &values).unwrap();
        assert_eq!(buf[0], b'D');
        let field_count = i16::from_be_bytes([buf[5], buf[6]]);
        assert_eq!(field_count, 3);
    }

    #[test]
    fn test_write_error_response() {
        let mut buf = Vec::new();
        write_error_response(&mut buf, "ERROR", "42601", "syntax error").unwrap();
        assert_eq!(buf[0], b'E');
    }

    #[test]
    fn test_write_row_description() {
        let cols = vec![
            ColumnDef {
                name: "id".to_string(),
                type_oid: 20,
                type_len: 8,
                type_mod: -1,
            },
            ColumnDef {
                name: "name".to_string(),
                type_oid: 25,
                type_len: -1,
                type_mod: -1,
            },
        ];
        let mut buf = Vec::new();
        write_row_description(&mut buf, &cols).unwrap();
        assert_eq!(buf[0], b'T');
        let field_count = i16::from_be_bytes([buf[5], buf[6]]);
        assert_eq!(field_count, 2);
    }

    #[test]
    fn test_write_parse_complete() {
        let mut buf = Vec::new();
        write_parse_complete(&mut buf).unwrap();
        assert_eq!(buf, vec![b'1', 0, 0, 0, 4]);
    }

    #[test]
    fn test_write_bind_complete() {
        let mut buf = Vec::new();
        write_bind_complete(&mut buf).unwrap();
        assert_eq!(buf, vec![b'2', 0, 0, 0, 4]);
    }

    #[test]
    fn test_write_close_complete() {
        let mut buf = Vec::new();
        write_close_complete(&mut buf).unwrap();
        assert_eq!(buf, vec![b'3', 0, 0, 0, 4]);
    }

    #[test]
    fn test_parse_startup_params() {
        let buf = b"user\0alice\0database\0test\0\0";
        let params = parse_startup_params(buf);
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], ("user".into(), "alice".into()));
        assert_eq!(params[1], ("database".into(), "test".into()));
    }

    #[test]
    fn test_read_cstr_from_buf() {
        let buf = b"hello\0world\0";
        let (s, rest) = read_cstr_from_buf(buf);
        assert_eq!(s, "hello");
        let (s2, rest2) = read_cstr_from_buf(rest);
        assert_eq!(s2, "world");
        assert!(rest2.is_empty());
    }
}
