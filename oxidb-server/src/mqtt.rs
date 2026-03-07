//! MQTT v3.1.1 protocol handler for pub/sub messaging.
//!
//! Shares the same pub/sub infrastructure as the OxiMem RESP layer.
//! An MQTT PUBLISH on topic "sensors/temp" is received by OxiMem
//! SUBSCRIBE "sensors/temp" and vice versa.

use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::oximem::OxiMemStore;

// MQTT packet types (upper 4 bits of fixed header byte)
const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const UNSUBSCRIBE: u8 = 10;
const UNSUBACK: u8 = 11;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn read_remaining_length(reader: &mut impl Read) -> io::Result<usize> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    loop {
        let mut b = [0u8; 1];
        reader.read_exact(&mut b)?;
        value += (b[0] & 0x7F) as usize * multiplier;
        if b[0] & 0x80 == 0 {
            return Ok(value);
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed remaining length"));
        }
    }
}

fn write_remaining_length(writer: &mut impl Write, mut len: usize) -> io::Result<()> {
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        writer.write_all(&[b])?;
        if len == 0 {
            return Ok(());
        }
    }
}

fn read_utf8(data: &[u8], offset: &mut usize) -> io::Result<String> {
    if *offset + 2 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short string length"));
    }
    let len = u16::from_be_bytes([data[*offset], data[*offset + 1]]) as usize;
    *offset += 2;
    if *offset + len > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short string data"));
    }
    let s = String::from_utf8_lossy(&data[*offset..*offset + len]).to_string();
    *offset += len;
    Ok(s)
}

fn write_utf8(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
}

fn write_packet(writer: &mut impl Write, pkt_type: u8, flags: u8, payload: &[u8]) -> io::Result<()> {
    writer.write_all(&[(pkt_type << 4) | (flags & 0x0F)])?;
    write_remaining_length(writer, payload.len())?;
    writer.write_all(payload)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

/// Handle a single MQTT client connection.
pub fn handle_client(stream: TcpStream, store: &OxiMemStore, log: bool) {
    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    // --- CONNECT handshake ---
    let mut hdr = [0u8; 1];
    if reader.read_exact(&mut hdr).is_err() {
        return;
    }
    if hdr[0] >> 4 != CONNECT {
        return;
    }
    let remaining = match read_remaining_length(&mut reader) {
        Ok(r) => r,
        Err(_) => return,
    };
    let mut payload = vec![0u8; remaining];
    if reader.read_exact(&mut payload).is_err() {
        return;
    }

    // Parse variable header: protocol name, level, flags, keepalive
    let mut off = 0;
    let _protocol = read_utf8(&payload, &mut off).unwrap_or_default();
    if off >= payload.len() { return; }
    let _level = payload[off]; off += 1; // 4 = MQTT 3.1.1
    if off >= payload.len() { return; }
    let _conn_flags = payload[off]; off += 1;
    if off + 1 >= payload.len() { return; }
    let _keepalive = u16::from_be_bytes([payload[off], payload[off + 1]]);
    off += 2;
    let client_id = read_utf8(&payload, &mut off).unwrap_or_default();

    if log {
        eprintln!("[mqtt] CONNECT client_id=\"{client_id}\" peer={peer}");
    }

    // CONNACK: session_present=0, return_code=0 (accepted)
    if write_packet(&mut writer, CONNACK, 0, &[0x00, 0x00]).is_err() { return; }
    if writer.flush().is_err() { return; }

    // --- Main loop ---
    let mut receivers: Vec<(String, std::sync::mpsc::Receiver<(String, String)>)> = Vec::new();
    let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));

    loop {
        // Deliver queued messages to subscribed client
        let mut wrote = false;
        for (_topic, rx) in &receivers {
            while let Ok((topic, message)) = rx.try_recv() {
                let mut buf = Vec::new();
                write_utf8(&mut buf, &topic);
                buf.extend_from_slice(message.as_bytes());
                if write_packet(&mut writer, PUBLISH, 0, &buf).is_err() { return; }
                if log {
                    eprintln!("[mqtt] >> PUBLISH topic=\"{topic}\" len={}", message.len());
                }
                wrote = true;
            }
        }
        if wrote && writer.flush().is_err() { return; }

        // Try to read next packet (non-blocking via read timeout)
        let mut hdr = [0u8; 1];
        match reader.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock
                || e.kind() == io::ErrorKind::TimedOut => continue,
            Err(_) => break,
        }

        let pkt_type = hdr[0] >> 4;
        let remaining = match read_remaining_length(&mut reader) {
            Ok(r) => r,
            Err(_) => break,
        };
        let mut payload = vec![0u8; remaining];
        if remaining > 0 && reader.read_exact(&mut payload).is_err() { break; }

        match pkt_type {
            PUBLISH => {
                let mut off = 0;
                let topic = match read_utf8(&payload, &mut off) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let qos = (hdr[0] >> 1) & 0x03;
                if qos > 0 && off + 2 <= payload.len() {
                    let pkt_id = u16::from_be_bytes([payload[off], payload[off + 1]]);
                    off += 2;
                    if qos == 1 {
                        let _ = write_packet(&mut writer, PUBACK, 0, &pkt_id.to_be_bytes());
                        let _ = writer.flush();
                    }
                }
                let message = String::from_utf8_lossy(&payload[off..]).to_string();
                if log {
                    eprintln!("[mqtt] << PUBLISH topic=\"{topic}\" msg=\"{message}\"");
                }
                store.publish(&topic, &message);
            }

            SUBSCRIBE => {
                if payload.len() < 2 { continue; }
                let pkt_id = u16::from_be_bytes([payload[0], payload[1]]);
                let mut off = 2;
                let mut return_codes = Vec::new();
                while off < payload.len() {
                    let topic = match read_utf8(&payload, &mut off) {
                        Ok(t) => t,
                        Err(_) => break,
                    };
                    let _qos = if off < payload.len() { let q = payload[off]; off += 1; q } else { 0 };
                    if log {
                        eprintln!("[mqtt] << SUBSCRIBE topic=\"{topic}\"");
                    }
                    let rx = store.subscribe(&topic);
                    receivers.push((topic, rx));
                    return_codes.push(0x00); // granted QoS 0
                }
                let mut suback = Vec::new();
                suback.extend_from_slice(&pkt_id.to_be_bytes());
                suback.extend_from_slice(&return_codes);
                let _ = write_packet(&mut writer, SUBACK, 0, &suback);
                let _ = writer.flush();
            }

            UNSUBSCRIBE => {
                if payload.len() < 2 { continue; }
                let pkt_id = u16::from_be_bytes([payload[0], payload[1]]);
                let mut off = 2;
                while off < payload.len() {
                    let topic = match read_utf8(&payload, &mut off) {
                        Ok(t) => t,
                        Err(_) => break,
                    };
                    if log {
                        eprintln!("[mqtt] << UNSUBSCRIBE topic=\"{topic}\"");
                    }
                    receivers.retain(|(name, _)| name != &topic);
                    store.unsubscribe(&topic);
                }
                let _ = write_packet(&mut writer, UNSUBACK, 0, &pkt_id.to_be_bytes());
                let _ = writer.flush();
            }

            PINGREQ => {
                let _ = write_packet(&mut writer, PINGRESP, 0, &[]);
                let _ = writer.flush();
            }

            DISCONNECT => {
                if log {
                    eprintln!("[mqtt] DISCONNECT client_id=\"{client_id}\"");
                }
                break;
            }

            _ => {} // ignore unknown packets
        }
    }

    if log {
        eprintln!("[mqtt] closed client_id=\"{client_id}\" peer={peer}");
    }
}
