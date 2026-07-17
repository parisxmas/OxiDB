//! AMQP 0-9-1 wire codec — frames, methods, field tables. ADR-0016.
//!
//! Only the encoding layer lives here: bytes in, bytes out, no broker state.
//! The subset is Phase 1's method set; anything else is for the connection
//! layer to reject loudly, not for this file to half-parse.
//!
//! The format, for the next reader (the spec is a 100-page PDF):
//!
//!   frame  = type:u8  channel:u16  size:u32  payload[size]  0xCE
//!   method = class:u16  method:u16  args
//!   args   = shortstr(u8 len) | longstr(u32 len) | u8/u16/u32/u64
//!            | bits (consecutive booleans PACKED INTO ONE OCTET — the classic
//!              trap: five flags cost one byte, not five)
//!            | field-table (u32 byte-length, then key/value pairs we mostly
//!              need to SKIP correctly rather than understand)
//!
//! A content-bearing method (Basic.Publish/Deliver/GetOk) is followed by a
//! HEADER frame (class, weight, body size, property flags + properties) and
//! then BODY frames until body-size bytes have arrived.

use std::io::{self, Read, Write};

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

// Class ids.
pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_EXCHANGE: u16 = 40;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;
pub const CLASS_CONFIRM: u16 = 85;

// Method ids, per class.
pub const CONNECTION_START: u16 = 10;
pub const CONNECTION_START_OK: u16 = 11;
pub const CONNECTION_TUNE: u16 = 30;
pub const CONNECTION_TUNE_OK: u16 = 31;
pub const CONNECTION_OPEN: u16 = 40;
pub const CONNECTION_OPEN_OK: u16 = 41;
pub const CONNECTION_CLOSE: u16 = 50;
pub const CONNECTION_CLOSE_OK: u16 = 51;

pub const CHANNEL_OPEN: u16 = 10;
pub const CHANNEL_OPEN_OK: u16 = 11;
pub const CHANNEL_CLOSE: u16 = 40;
pub const CHANNEL_CLOSE_OK: u16 = 41;

pub const EXCHANGE_DECLARE: u16 = 10;
pub const EXCHANGE_DECLARE_OK: u16 = 11;

pub const QUEUE_DECLARE: u16 = 10;
pub const QUEUE_DECLARE_OK: u16 = 11;
pub const QUEUE_BIND: u16 = 20;
pub const QUEUE_BIND_OK: u16 = 21;

pub const BASIC_QOS: u16 = 10;
pub const BASIC_QOS_OK: u16 = 11;
pub const BASIC_CONSUME: u16 = 20;
pub const BASIC_CONSUME_OK: u16 = 21;
pub const BASIC_CANCEL: u16 = 30;
pub const BASIC_CANCEL_OK: u16 = 31;
pub const BASIC_PUBLISH: u16 = 40;
pub const BASIC_RETURN: u16 = 50;
pub const BASIC_DELIVER: u16 = 60;
pub const BASIC_GET: u16 = 70;
pub const BASIC_GET_OK: u16 = 71;
pub const BASIC_GET_EMPTY: u16 = 72;
pub const BASIC_ACK: u16 = 80;
pub const BASIC_REJECT: u16 = 90;
pub const BASIC_NACK: u16 = 120;

pub const CONFIRM_SELECT: u16 = 10;
pub const CONFIRM_SELECT_OK: u16 = 11;

/// The protocol header a client opens with. Version 0-9-1.
pub const PROTOCOL_HEADER: [u8; 8] = *b"AMQP\x00\x00\x09\x01";

/// One decoded frame.
pub struct Frame {
    pub kind: u8,
    pub channel: u16,
    pub payload: Vec<u8>,
}

/// Read one frame. The caller owns timeout policy on the underlying socket.
pub fn read_frame(r: &mut impl Read, max_size: usize) -> io::Result<Frame> {
    let mut head = [0u8; 7];
    r.read_exact(&mut head)?;
    let kind = head[0];
    let channel = u16::from_be_bytes([head[1], head[2]]);
    let size = u32::from_be_bytes([head[3], head[4], head[5], head[6]]) as usize;
    if size > max_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {size} bytes exceeds the negotiated maximum"),
        ));
    }
    let mut payload = vec![0u8; size];
    r.read_exact(&mut payload)?;
    let mut end = [0u8; 1];
    r.read_exact(&mut end)?;
    if end[0] != FRAME_END {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing frame-end octet",
        ));
    }
    Ok(Frame {
        kind,
        channel,
        payload,
    })
}

pub fn write_frame(w: &mut impl Write, kind: u8, channel: u16, payload: &[u8]) -> io::Result<()> {
    w.write_all(&[kind])?;
    w.write_all(&channel.to_be_bytes())?;
    w.write_all(&(payload.len() as u32).to_be_bytes())?;
    w.write_all(payload)?;
    w.write_all(&[FRAME_END])
}

// ── Argument reading ────────────────────────────────────────────────────

/// Cursor over a method's argument bytes. Every accessor returns io errors
/// rather than panicking: the bytes come off the network and a malformed
/// frame must close the connection, not the process.
pub struct Args<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Args<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Args { buf, pos: 0 }
    }

    fn need(&self, n: usize) -> io::Result<()> {
        if self.pos + n > self.buf.len() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short method arguments",
            ))
        } else {
            Ok(())
        }
    }

    pub fn u8(&mut self) -> io::Result<u8> {
        self.need(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn u16(&mut self) -> io::Result<u16> {
        self.need(2)?;
        let v = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn u32(&mut self) -> io::Result<u32> {
        self.need(4)?;
        let mut b = [0u8; 4];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(u32::from_be_bytes(b))
    }

    pub fn u64(&mut self) -> io::Result<u64> {
        self.need(8)?;
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_be_bytes(b))
    }

    pub fn shortstr(&mut self) -> io::Result<String> {
        let n = self.u8()? as usize;
        self.need(n)?;
        let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + n]).to_string();
        self.pos += n;
        Ok(s)
    }

    pub fn longstr(&mut self) -> io::Result<Vec<u8>> {
        let n = self.u32()? as usize;
        self.need(n)?;
        let v = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(v)
    }

    /// One octet of packed booleans — the caller unpacks the bits it cares
    /// about. AMQP packs CONSECUTIVE bit arguments into a single octet.
    pub fn bits(&mut self) -> io::Result<u8> {
        self.u8()
    }

    /// Skip a field table wholesale. We never act on client tables in Phase 1
    /// (client-properties, declare arguments), but they MUST be consumed
    /// correctly or every argument after them is misread.
    pub fn skip_table(&mut self) -> io::Result<()> {
        let n = self.u32()? as usize;
        self.need(n)?;
        self.pos += n;
        Ok(())
    }
}

// ── Argument writing ────────────────────────────────────────────────────

pub struct ArgsW {
    buf: Vec<u8>,
}

impl ArgsW {
    /// Start a method payload: class + method ids first.
    pub fn method(class: u16, method: u16) -> Self {
        let mut buf = Vec::with_capacity(32);
        buf.extend_from_slice(&class.to_be_bytes());
        buf.extend_from_slice(&method.to_be_bytes());
        ArgsW { buf }
    }

    pub fn u8(mut self, v: u8) -> Self {
        self.buf.push(v);
        self
    }

    pub fn u16(mut self, v: u16) -> Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u32(mut self, v: u32) -> Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u64(mut self, v: u64) -> Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn shortstr(mut self, s: &str) -> Self {
        // Short strings cap at 255; truncating silently would corrupt the
        // frame, so clamp at the boundary and keep the frame well-formed.
        let b = s.as_bytes();
        let n = b.len().min(255);
        self.buf.push(n as u8);
        self.buf.extend_from_slice(&b[..n]);
        self
    }

    pub fn longstr(mut self, b: &[u8]) -> Self {
        self.buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(b);
        self
    }

    /// A field table with string values only — all Phase 1 needs (the
    /// server-properties in Connection.Start).
    pub fn table(mut self, pairs: &[(&str, &str)]) -> Self {
        let mut t = Vec::new();
        for (k, v) in pairs {
            t.push(k.len() as u8);
            t.extend_from_slice(k.as_bytes());
            t.push(b'S'); // longstr value
            t.extend_from_slice(&(v.len() as u32).to_be_bytes());
            t.extend_from_slice(v.as_bytes());
        }
        self.buf.extend_from_slice(&(t.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(&t);
        self
    }

    /// A pre-built field table, written as (length, bytes). For tables with
    /// non-string values — nested tables, booleans — that `table()` cannot
    /// express.
    pub fn table_raw(mut self, bytes: &[u8]) -> Self {
        self.buf
            .extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(bytes);
        self
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// The server-properties for Connection.Start. Clients gate features on the
/// nested `capabilities` table — pika refuses to even SEND Confirm.Select
/// unless `publisher_confirms` is advertised here, so this is not decoration:
/// an un-advertised capability is an unusable one.
pub fn server_properties() -> Vec<u8> {
    fn push_key(t: &mut Vec<u8>, k: &str) {
        t.push(k.len() as u8);
        t.extend_from_slice(k.as_bytes());
    }
    let mut caps = Vec::new();
    // pika gates confirm_delivery() on publisher_confirms AND basic.nack —
    // found the hard way: advertising only the first makes pika refuse to
    // send Confirm.Select at all. Both are therefore implemented AND
    // advertised; an advertisement without the implementation would be the
    // says-it-does-X bug this whole ADR line exists to kill.
    for cap in ["publisher_confirms", "basic.nack"] {
        push_key(&mut caps, cap);
        caps.push(b't'); // boolean
        caps.push(1);
    }
    let mut t = Vec::new();
    push_key(&mut t, "product");
    t.push(b'S');
    t.extend_from_slice(&5u32.to_be_bytes());
    t.extend_from_slice(b"OxiDB");
    push_key(&mut t, "capabilities");
    t.push(b'F'); // nested field table
    t.extend_from_slice(&(caps.len() as u32).to_be_bytes());
    t.extend_from_slice(&caps);
    t
}

// ── Content headers ─────────────────────────────────────────────────────

/// What the broker needs from a content header: the body size (to reassemble
/// body frames), the raw property section (replayed VERBATIM on delivery, so
/// content-type, correlation ids and application headers survive without this
/// codec modelling any of them), and delivery-mode (the one property the
/// broker itself acts on — 2 = persistent).
pub struct ContentHeader {
    pub body_size: u64,
    /// Property flags + property list, exactly as received.
    pub props_raw: Vec<u8>,
    pub delivery_mode: Option<u8>,
}

pub fn parse_content_header(payload: &[u8]) -> io::Result<ContentHeader> {
    let mut a = Args::new(payload);
    let _class = a.u16()?;
    let _weight = a.u16()?;
    let body_size = a.u64()?;
    let props_raw = payload[a.pos..].to_vec();

    // Walk the property list far enough to find delivery-mode. Flags are a
    // bitfield, highest bit first; each set bit's value follows in order.
    let flags = a.u16()?;
    let mut delivery_mode = None;
    // bit 15 content-type, 14 content-encoding, 13 headers, 12 delivery-mode
    if flags & 0x8000 != 0 {
        let _ = a.shortstr()?; // content-type
    }
    if flags & 0x4000 != 0 {
        let _ = a.shortstr()?; // content-encoding
    }
    if flags & 0x2000 != 0 {
        a.skip_table()?; // headers
    }
    if flags & 0x1000 != 0 {
        delivery_mode = Some(a.u8()?);
    }
    Ok(ContentHeader {
        body_size,
        props_raw,
        delivery_mode,
    })
}

/// Build a content header frame payload from a stored raw property section.
pub fn build_content_header(body_size: u64, props_raw: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(12 + props_raw.len());
    p.extend_from_slice(&CLASS_BASIC.to_be_bytes());
    p.extend_from_slice(&0u16.to_be_bytes()); // weight, always 0
    p.extend_from_slice(&body_size.to_be_bytes());
    if props_raw.is_empty() {
        p.extend_from_slice(&0u16.to_be_bytes()); // no properties
    } else {
        p.extend_from_slice(props_raw);
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_roundtrip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, FRAME_METHOD, 3, &[1, 2, 3]).unwrap();
        let f = read_frame(&mut buf.as_slice(), 1 << 20).unwrap();
        assert_eq!(
            (f.kind, f.channel, f.payload),
            (FRAME_METHOD, 3, vec![1, 2, 3])
        );
    }

    #[test]
    fn a_frame_larger_than_the_negotiated_max_is_refused() {
        let mut buf = Vec::new();
        write_frame(&mut buf, FRAME_BODY, 1, &[0u8; 100]).unwrap();
        assert!(read_frame(&mut buf.as_slice(), 50).is_err());
    }

    #[test]
    fn args_roundtrip_incl_table_skip() {
        let payload = ArgsW::method(10, 11)
            .table(&[("product", "test")])
            .shortstr("PLAIN")
            .longstr(b"\0user\0pass")
            .shortstr("en_US")
            .finish();
        let mut a = Args::new(&payload);
        assert_eq!((a.u16().unwrap(), a.u16().unwrap()), (10, 11));
        a.skip_table().unwrap();
        assert_eq!(a.shortstr().unwrap(), "PLAIN");
        assert_eq!(a.longstr().unwrap(), b"\0user\0pass");
        assert_eq!(a.shortstr().unwrap(), "en_US");
    }

    #[test]
    fn content_header_finds_delivery_mode_behind_earlier_properties() {
        // content-type + headers table + delivery-mode set: the walk must skip
        // the earlier properties to reach it.
        let mut props = Vec::new();
        props.extend_from_slice(&(0x8000u16 | 0x2000 | 0x1000).to_be_bytes());
        props.push(10); // content-type shortstr
        props.extend_from_slice(b"text/plain");
        props.extend_from_slice(&0u32.to_be_bytes()); // empty headers table
        props.push(2); // delivery-mode = persistent

        let mut payload = Vec::new();
        payload.extend_from_slice(&CLASS_BASIC.to_be_bytes());
        payload.extend_from_slice(&0u16.to_be_bytes());
        payload.extend_from_slice(&42u64.to_be_bytes());
        payload.extend_from_slice(&props);

        let h = parse_content_header(&payload).unwrap();
        assert_eq!(h.body_size, 42);
        assert_eq!(h.delivery_mode, Some(2));
        assert_eq!(
            h.props_raw, props,
            "properties must be preserved verbatim for redelivery"
        );
    }
}
