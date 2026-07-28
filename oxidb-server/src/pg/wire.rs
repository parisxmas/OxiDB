//! PostgreSQL v3 frontend/backend protocol codec — bytes in, bytes out, no
//! session state (the same split `amqp_wire.rs` uses).
//!
//! The format, for the next reader:
//!
//!   startup packet   = len:i32(incl self)  version:i32  (key\0 value\0)* \0
//!   everything after = tag:u8  len:i32(incl len, excl tag)  body[len-4]
//!
//! The startup packet is the odd one out: it has no tag byte, because at that
//! point the server doesn't yet know whether it is about to be spoken SSL,
//! cancellation, or the protocol proper — those are told apart by the version
//! field alone.

use std::io::{self, BufReader, Read, Write};

/// A connection's buffered reader plus its write half.
///
/// One type rather than a split pair because a TLS stream cannot be cloned:
/// reads go through the buffer, writes go straight to the stream underneath it
/// via [`Conn::w`]. Every backend-message helper takes `&mut impl Write`, so
/// they are called as `wire::ready_for_query(conn.w(), ..)`.
pub struct Conn<S: Read + Write> {
    r: BufReader<S>,
}

impl<S: Read + Write> Conn<S> {
    pub fn new(io: S) -> Self {
        Conn {
            r: BufReader::new(io),
        }
    }

    /// The write half. Nothing is buffered on this side, so a caller that
    /// writes several messages should [`flush`](Self::flush) once at the end.
    pub fn w(&mut self) -> &mut S {
        self.r.get_mut()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.r.get_mut().flush()
    }

    pub fn read(&mut self) -> io::Result<Msg> {
        read_message(&mut self.r)
    }

    pub fn read_startup(&mut self) -> io::Result<Startup> {
        read_startup(&mut self.r)
    }

    /// Hand back the stream (used to hand a plaintext socket to TLS).
    pub fn into_inner(self) -> S {
        self.r.into_inner()
    }
}

/// Protocol version 3.0, as it appears in the startup packet.
pub const PROTOCOL_V3: i32 = 196_608;
/// Magic "version" numbers that mean the packet is not a startup at all.
pub const SSL_REQUEST: i32 = 80_877_103;
pub const GSSENC_REQUEST: i32 = 80_877_104;
pub const CANCEL_REQUEST: i32 = 80_877_102;

/// Refuse absurd message lengths before allocating for them. PostgreSQL's own
/// limit is 1 GB; a query bigger than 64 MiB is a bug or an attack, not a
/// workload this server wants to buffer.
pub const MAX_MSG_LEN: usize = 64 * 1024 * 1024;

// ── frontend message tags ───────────────────────────────────────────────────
pub const F_QUERY: u8 = b'Q';
pub const F_PARSE: u8 = b'P';
pub const F_BIND: u8 = b'B';
pub const F_DESCRIBE: u8 = b'D';
pub const F_EXECUTE: u8 = b'E';
pub const F_SYNC: u8 = b'S';
pub const F_CLOSE: u8 = b'C';
pub const F_FLUSH: u8 = b'H';
pub const F_TERMINATE: u8 = b'X';
/// Also SASLInitialResponse / SASLResponse — the frontend reuses one tag for
/// every authentication reply, and which it is depends on what the server
/// asked for.
pub const F_PASSWORD: u8 = b'p';

/// One frontend message, body still unparsed.
pub struct Msg {
    pub tag: u8,
    pub body: Vec<u8>,
}

/// What the client opened the connection with.
#[derive(Debug)]
pub enum Startup {
    /// Protocol 3.0 with its parameter list (`user`, `database`, …).
    Params(Vec<(String, String)>),
    /// `SSLRequest` / `GSSENCRequest` — answer one byte and read a startup again.
    Ssl,
    GssEnc,
    /// `CancelRequest` for another backend: `(pid, secret)`.
    Cancel(i32, i32),
}

/// Read the untagged opening packet.
pub fn read_startup(r: &mut impl Read) -> io::Result<Startup> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = i32::from_be_bytes(len_buf);
    if !(8..=MAX_MSG_LEN as i32).contains(&len) {
        return Err(invalid(format!("startup packet length {len} out of range")));
    }
    let mut body = vec![0u8; len as usize - 4];
    r.read_exact(&mut body)?;
    let version = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    match version {
        SSL_REQUEST => return Ok(Startup::Ssl),
        GSSENC_REQUEST => return Ok(Startup::GssEnc),
        CANCEL_REQUEST => {
            if body.len() < 12 {
                return Err(invalid("truncated CancelRequest"));
            }
            let pid = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
            let key = i32::from_be_bytes([body[8], body[9], body[10], body[11]]);
            return Ok(Startup::Cancel(pid, key));
        }
        PROTOCOL_V3 => {}
        other => {
            // Major version is the high 16 bits. Anything but 3 we cannot speak.
            return Err(invalid(format!(
                "unsupported protocol version {}.{}",
                other >> 16,
                other & 0xffff
            )));
        }
    }
    // key\0value\0 … \0
    let mut params = Vec::new();
    let mut cur = Reader::new(&body[4..]);
    loop {
        let key = cur.cstring()?;
        if key.is_empty() {
            break;
        }
        let value = cur.cstring()?;
        params.push((key, value));
    }
    Ok(Startup::Params(params))
}

/// Read one tagged frontend message.
pub fn read_message(r: &mut impl Read) -> io::Result<Msg> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = i32::from_be_bytes(len_buf);
    if !(4..=MAX_MSG_LEN as i32).contains(&len) {
        return Err(invalid(format!(
            "message '{}' length {len} out of range",
            tag[0] as char
        )));
    }
    let mut body = vec![0u8; len as usize - 4];
    r.read_exact(&mut body)?;
    Ok(Msg { tag: tag[0], body })
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Cursor over a message body.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(invalid("truncated message"));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn i16(&mut self) -> io::Result<i16> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub fn i32(&mut self) -> io::Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// A NUL-terminated string. Invalid UTF-8 is replaced rather than fatal —
    /// a client's application_name is not worth dropping a connection over.
    pub fn cstring(&mut self) -> io::Result<String> {
        let end = self.buf[self.pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| invalid("unterminated string"))?;
        let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + end]).into_owned();
        self.pos += end + 1;
        Ok(s)
    }

    /// The rest of the body.
    pub fn rest(&mut self) -> &'a [u8] {
        let out = &self.buf[self.pos..];
        self.pos = self.buf.len();
        out
    }

    /// `n` bytes, or `None` for the protocol's `-1` = NULL length.
    pub fn nullable_bytes(&mut self) -> io::Result<Option<&'a [u8]>> {
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(self.take(len as usize)?))
    }
}

/// Builder for one backend message: tag, then a body that gets its length
/// prefix on `finish`.
pub struct Out {
    buf: Vec<u8>,
}

impl Out {
    pub fn new(tag: u8) -> Self {
        // tag + a placeholder for the length, filled in by `finish`.
        Out {
            buf: vec![tag, 0, 0, 0, 0],
        }
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    pub fn i16(&mut self, v: i16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn cstring(&mut self, s: &str) -> &mut Self {
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
        self
    }

    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self
    }

    /// A length-prefixed value, `-1` for NULL (DataRow columns).
    pub fn nullable_bytes(&mut self, b: Option<&[u8]>) -> &mut Self {
        match b {
            Some(b) => {
                self.i32(b.len() as i32);
                self.bytes(b);
            }
            None => {
                self.i32(-1);
            }
        }
        self
    }

    /// Stamp the length over the placeholder and hand back the whole message.
    pub fn finish(mut self) -> Vec<u8> {
        let len = (self.buf.len() - 1) as i32; // excludes the tag, includes itself
        self.buf[1..5].copy_from_slice(&len.to_be_bytes());
        self.buf
    }

    pub fn write_to(self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.finish())
    }
}

// ── backend messages ────────────────────────────────────────────────────────

/// Authentication request codes (the i32 that opens an `R` message).
pub const AUTH_OK: i32 = 0;
pub const AUTH_CLEARTEXT: i32 = 3;
pub const AUTH_SASL: i32 = 10;
pub const AUTH_SASL_CONTINUE: i32 = 11;
pub const AUTH_SASL_FINAL: i32 = 12;

pub fn auth_ok(w: &mut impl Write) -> io::Result<()> {
    let mut m = Out::new(b'R');
    m.i32(AUTH_OK);
    m.write_to(w)
}

pub fn auth_cleartext(w: &mut impl Write) -> io::Result<()> {
    let mut m = Out::new(b'R');
    m.i32(AUTH_CLEARTEXT);
    m.write_to(w)
}

/// Offer SASL mechanisms: each NUL-terminated, the list itself NUL-terminated.
pub fn auth_sasl(w: &mut impl Write, mechanisms: &[&str]) -> io::Result<()> {
    let mut m = Out::new(b'R');
    m.i32(AUTH_SASL);
    for mech in mechanisms {
        m.cstring(mech);
    }
    m.u8(0);
    m.write_to(w)
}

pub fn auth_sasl_continue(w: &mut impl Write, data: &str) -> io::Result<()> {
    let mut m = Out::new(b'R');
    m.i32(AUTH_SASL_CONTINUE).bytes(data.as_bytes());
    m.write_to(w)
}

pub fn auth_sasl_final(w: &mut impl Write, data: &str) -> io::Result<()> {
    let mut m = Out::new(b'R');
    m.i32(AUTH_SASL_FINAL).bytes(data.as_bytes());
    m.write_to(w)
}

pub fn parameter_status(w: &mut impl Write, name: &str, value: &str) -> io::Result<()> {
    let mut m = Out::new(b'S');
    m.cstring(name).cstring(value);
    m.write_to(w)
}

pub fn backend_key_data(w: &mut impl Write, pid: i32, key: i32) -> io::Result<()> {
    let mut m = Out::new(b'K');
    m.i32(pid).i32(key);
    m.write_to(w)
}

/// Transaction status in `ReadyForQuery`.
pub const TX_IDLE: u8 = b'I';
pub const TX_IN: u8 = b'T';
pub const TX_FAILED: u8 = b'E';

pub fn ready_for_query(w: &mut impl Write, status: u8) -> io::Result<()> {
    let mut m = Out::new(b'Z');
    m.u8(status);
    m.write_to(w)
}

/// One column of a `RowDescription`.
#[derive(Clone)]
pub struct FieldDesc {
    pub name: String,
    pub type_oid: i32,
    /// Fixed width, or `-1` for a variable-length type.
    pub type_len: i16,
    /// 0 = text, 1 = binary. Must match what `Bind` asked for.
    pub format: i16,
}

pub fn row_description(w: &mut impl Write, fields: &[FieldDesc]) -> io::Result<()> {
    let mut m = Out::new(b'T');
    m.i16(fields.len() as i16);
    for f in fields {
        m.cstring(&f.name)
            .i32(0) // table oid: not a real table
            .i16(0) // column attnum
            .i32(f.type_oid)
            .i16(f.type_len)
            .i32(-1) // type modifier
            .i16(f.format);
    }
    m.write_to(w)
}

pub fn data_row(w: &mut impl Write, cells: &[Option<Vec<u8>>]) -> io::Result<()> {
    let mut m = Out::new(b'D');
    m.i16(cells.len() as i16);
    for c in cells {
        m.nullable_bytes(c.as_deref());
    }
    m.write_to(w)
}

pub fn command_complete(w: &mut impl Write, tag: &str) -> io::Result<()> {
    let mut m = Out::new(b'C');
    m.cstring(tag);
    m.write_to(w)
}

pub fn empty_query_response(w: &mut impl Write) -> io::Result<()> {
    Out::new(b'I').write_to(w)
}

pub fn parse_complete(w: &mut impl Write) -> io::Result<()> {
    Out::new(b'1').write_to(w)
}

pub fn bind_complete(w: &mut impl Write) -> io::Result<()> {
    Out::new(b'2').write_to(w)
}

pub fn close_complete(w: &mut impl Write) -> io::Result<()> {
    Out::new(b'3').write_to(w)
}

pub fn no_data(w: &mut impl Write) -> io::Result<()> {
    Out::new(b'n').write_to(w)
}

pub fn portal_suspended(w: &mut impl Write) -> io::Result<()> {
    Out::new(b's').write_to(w)
}

pub fn parameter_description(w: &mut impl Write, oids: &[i32]) -> io::Result<()> {
    let mut m = Out::new(b't');
    m.i16(oids.len() as i16);
    for oid in oids {
        m.i32(*oid);
    }
    m.write_to(w)
}

/// `ErrorResponse` / `NoticeResponse` share a shape: a run of typed fields,
/// terminated by a zero byte.
fn diagnostic(
    w: &mut impl Write,
    tag: u8,
    severity: &str,
    code: &str,
    message: &str,
    detail: Option<&str>,
) -> io::Result<()> {
    let mut m = Out::new(tag);
    m.u8(b'S').cstring(severity);
    m.u8(b'V').cstring(severity); // non-localized severity (protocol 3.0+)
    m.u8(b'C').cstring(code);
    m.u8(b'M').cstring(message);
    if let Some(d) = detail {
        m.u8(b'D').cstring(d);
    }
    m.u8(0);
    m.write_to(w)
}

pub fn error_response(
    w: &mut impl Write,
    code: &str,
    message: &str,
    detail: Option<&str>,
) -> io::Result<()> {
    diagnostic(w, b'E', "ERROR", code, message, detail)
}

/// A fatal error closes the connection after it — used for startup failures,
/// where "ERROR" would leave the client waiting for a ReadyForQuery.
pub fn fatal_response(w: &mut impl Write, code: &str, message: &str) -> io::Result<()> {
    diagnostic(w, b'E', "FATAL", code, message, None)
}

pub fn notice_response(w: &mut impl Write, message: &str) -> io::Result<()> {
    diagnostic(w, b'N', "NOTICE", "00000", message, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_packet_roundtrip() {
        // len, version, user\0admin\0database\0oxidb\0\0
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        for s in ["user", "admin", "database", "oxidb"] {
            body.extend_from_slice(s.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);

        match read_startup(&mut packet.as_slice()).unwrap() {
            Startup::Params(p) => {
                assert_eq!(p[0], ("user".into(), "admin".into()));
                assert_eq!(p[1], ("database".into(), "oxidb".into()));
            }
            _ => panic!("expected params"),
        }
    }

    #[test]
    fn ssl_and_cancel_are_told_apart_from_a_startup() {
        let mut ssl = 8i32.to_be_bytes().to_vec();
        ssl.extend_from_slice(&SSL_REQUEST.to_be_bytes());
        assert!(matches!(
            read_startup(&mut ssl.as_slice()).unwrap(),
            Startup::Ssl
        ));

        let mut cancel = 16i32.to_be_bytes().to_vec();
        cancel.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        cancel.extend_from_slice(&42i32.to_be_bytes());
        cancel.extend_from_slice(&7i32.to_be_bytes());
        assert!(matches!(
            read_startup(&mut cancel.as_slice()).unwrap(),
            Startup::Cancel(42, 7)
        ));
    }

    #[test]
    fn a_v2_client_is_refused_by_version() {
        let mut old = 8i32.to_be_bytes().to_vec();
        old.extend_from_slice(&131_072i32.to_be_bytes()); // 2.0
        let e = read_startup(&mut old.as_slice()).unwrap_err();
        assert!(e.to_string().contains("2.0"), "{e}");
    }

    #[test]
    fn tagged_message_length_is_self_inclusive() {
        // A Query message the way a client sends it.
        let mut m = Out::new(F_QUERY);
        m.cstring("SELECT 1");
        let bytes = m.finish();
        assert_eq!(bytes[0], F_QUERY);
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len as usize, bytes.len() - 1);

        let msg = read_message(&mut bytes.as_slice()).unwrap();
        assert_eq!(msg.tag, F_QUERY);
        assert_eq!(Reader::new(&msg.body).cstring().unwrap(), "SELECT 1");
    }

    #[test]
    fn oversized_length_is_refused_before_allocating() {
        let mut bytes = vec![F_QUERY];
        bytes.extend_from_slice(&(MAX_MSG_LEN as i32 + 1).to_be_bytes());
        assert!(read_message(&mut bytes.as_slice()).is_err());
    }

    #[test]
    fn null_cells_are_minus_one_not_empty() {
        let mut out = Vec::new();
        data_row(&mut out, &[Some(b"x".to_vec()), None]).unwrap();
        // tag, len, count=2, len=1, 'x', len=-1
        assert_eq!(out[0], b'D');
        let tail = &out[out.len() - 4..];
        assert_eq!(i32::from_be_bytes([tail[0], tail[1], tail[2], tail[3]]), -1);
    }
}
