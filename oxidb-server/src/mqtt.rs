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
const PUBREC: u8 = 5;
const PUBREL: u8 = 6;
const PUBCOMP: u8 = 7;
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed remaining length",
            ));
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
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short string length",
        ));
    }
    let len = u16::from_be_bytes([data[*offset], data[*offset + 1]]) as usize;
    *offset += 2;
    if *offset + len > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short string data",
        ));
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

fn write_packet(
    writer: &mut impl Write,
    pkt_type: u8,
    flags: u8,
    payload: &[u8],
) -> io::Result<()> {
    writer.write_all(&[(pkt_type << 4) | (flags & 0x0F)])?;
    write_remaining_length(writer, payload.len())?;
    writer.write_all(payload)?;
    Ok(())
}

/// MQTT topic filter → anchored regex: `+` matches one level, `#` (only as
/// the final level) matches the rest of the topic.
fn filter_to_regex(filter: &str) -> Option<regex::Regex> {
    let mut out = String::from("^");
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, lv) in levels.iter().enumerate() {
        if i > 0 {
            out.push('/');
        }
        match *lv {
            "+" => out.push_str("[^/]+"),
            "#" => {
                if i != levels.len() - 1 {
                    return None; // '#' must be last
                }
                // '#' also matches the parent level itself.
                if i > 0 {
                    out.pop(); // drop the '/'
                    out.push_str("(/.*)?");
                } else {
                    out.push_str(".*");
                }
                break;
            }
            plain => out.push_str(&regex::escape(plain)),
        }
    }
    out.push('$');
    regex::Regex::new(&out).ok()
}

/// True if the filter contains MQTT wildcards.
fn has_wildcard(f: &str) -> bool {
    f.split('/').any(|l| l == "+" || l == "#")
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

/// Handle a single MQTT client connection.
pub fn handle_client(stream: TcpStream, store: &OxiMemStore, log: bool) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
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
    if off >= payload.len() {
        return;
    }
    let _level = payload[off];
    off += 1; // 4 = MQTT 3.1.1
    if off >= payload.len() {
        return;
    }
    let conn_flags = payload[off];
    off += 1;
    if off + 1 >= payload.len() {
        return;
    }
    let keepalive = u16::from_be_bytes([payload[off], payload[off + 1]]);
    off += 2;
    let client_id = read_utf8(&payload, &mut off).unwrap_or_default();

    // Last Will & Testament (flag bit 2; qos bits 3-4; retain bit 5).
    let will_flag = conn_flags & 0x04 != 0;
    let will_retain = conn_flags & 0x20 != 0;
    let mut will: Option<(String, String)> = None;
    if will_flag {
        let t = read_utf8(&payload, &mut off).unwrap_or_default();
        let m = read_utf8(&payload, &mut off).unwrap_or_default();
        will = Some((t, m));
    }
    // Username (bit 7) / password (bit 6).
    let username = if conn_flags & 0x80 != 0 {
        read_utf8(&payload, &mut off).unwrap_or_default()
    } else {
        String::new()
    };
    let password = if conn_flags & 0x40 != 0 {
        read_utf8(&payload, &mut off).unwrap_or_default()
    } else {
        String::new()
    };

    if log {
        eprintln!("[mqtt] CONNECT client_id=\"{client_id}\" peer={peer}");
    }

    // Auth: if OXIDB_MQTT_USER/OXIDB_MQTT_PASSWORD are set, require a match.
    if let (Ok(want_u), Ok(want_p)) = (
        std::env::var("OXIDB_MQTT_USER"),
        std::env::var("OXIDB_MQTT_PASSWORD"),
    ) {
        if username != want_u || password != want_p {
            // 0x04 = bad user name or password
            let _ = write_packet(&mut writer, CONNACK, 0, &[0x00, 0x04]);
            let _ = writer.flush();
            return;
        }
    }

    // CONNACK: session_present=0 (stateless broker by design), accepted.
    if write_packet(&mut writer, CONNACK, 0, &[0x00, 0x00]).is_err() {
        return;
    }
    if writer.flush().is_err() {
        return;
    }

    // --- Main loop ---
    // (filter, granted_qos, receiver)
    let mut receivers: Vec<(String, u8, std::sync::mpsc::Receiver<(String, String)>)> = Vec::new();
    let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
    let mut next_pkt_id: u16 = 1;
    let mut last_activity = std::time::Instant::now();
    let mut clean_close = false;
    // MQTT-3.1.2-24: disconnect after 1.5 × keepalive of silence.
    let ka_limit = if keepalive > 0 {
        Some(Duration::from_millis(keepalive as u64 * 1500))
    } else {
        None
    };

    loop {
        // Deliver queued messages to subscribed client
        let mut wrote = false;
        for (_topic, granted_qos, rx) in &receivers {
            while let Ok((topic, message)) = rx.try_recv() {
                let mut buf = Vec::new();
                write_utf8(&mut buf, &topic);
                let flags = if *granted_qos >= 1 {
                    buf.extend_from_slice(&next_pkt_id.to_be_bytes());
                    next_pkt_id = next_pkt_id.wrapping_add(1).max(1);
                    0x02 // QoS 1
                } else {
                    0x00
                };
                buf.extend_from_slice(message.as_bytes());
                if write_packet(&mut writer, PUBLISH, flags, &buf).is_err() {
                    return;
                }
                if log {
                    eprintln!("[mqtt] >> PUBLISH topic=\"{topic}\" len={}", message.len());
                }
                wrote = true;
            }
        }
        if wrote && writer.flush().is_err() {
            return;
        }

        // Try to read next packet (non-blocking via read timeout)
        let mut hdr = [0u8; 1];
        match reader.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                if let Some(limit) = ka_limit {
                    if last_activity.elapsed() > limit {
                        break; // keepalive expired → abnormal close (will fires)
                    }
                }
                continue;
            }
            Err(_) => break,
        }
        last_activity = std::time::Instant::now();

        let pkt_type = hdr[0] >> 4;
        let remaining = match read_remaining_length(&mut reader) {
            Ok(r) => r,
            Err(_) => break,
        };
        let mut payload = vec![0u8; remaining];
        if remaining > 0 && reader.read_exact(&mut payload).is_err() {
            break;
        }

        match pkt_type {
            PUBLISH => {
                let mut off = 0;
                let topic = match read_utf8(&payload, &mut off) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let qos = (hdr[0] >> 1) & 0x03;
                let retain = hdr[0] & 0x01 != 0;
                if qos > 0 && off + 2 <= payload.len() {
                    let pkt_id = u16::from_be_bytes([payload[off], payload[off + 1]]);
                    off += 2;
                    if qos == 1 {
                        let _ = write_packet(&mut writer, PUBACK, 0, &pkt_id.to_be_bytes());
                        let _ = writer.flush();
                    } else if qos == 2 {
                        // Method A: publish now, complete the 4-way handshake.
                        let _ = write_packet(&mut writer, PUBREC, 0, &pkt_id.to_be_bytes());
                        let _ = writer.flush();
                    }
                }
                let message = String::from_utf8_lossy(&payload[off..]).to_string();
                if log {
                    eprintln!("[mqtt] << PUBLISH topic=\"{topic}\" msg=\"{message}\"");
                }
                if retain {
                    if message.is_empty() {
                        store.retain_clear(&topic); // empty retained = clear
                    } else {
                        store.retain_set(&topic, &message);
                    }
                }
                store.publish(&topic, &message);
            }

            SUBSCRIBE => {
                if payload.len() < 2 {
                    continue;
                }
                let pkt_id = u16::from_be_bytes([payload[0], payload[1]]);
                let mut off = 2;
                let mut return_codes = Vec::new();
                while off < payload.len() {
                    let topic = match read_utf8(&payload, &mut off) {
                        Ok(t) => t,
                        Err(_) => break,
                    };
                    let req_qos = if off < payload.len() {
                        let q = payload[off];
                        off += 1;
                        q
                    } else {
                        0
                    };
                    if log {
                        eprintln!("[mqtt] << SUBSCRIBE topic=\"{topic}\"");
                    }
                    let granted = req_qos.min(1); // QoS 2 subscriptions granted at 1
                    // Wildcard filters go through the pattern layer; exact
                    // topics use the fast exact-channel map.
                    let (rx, re) = if has_wildcard(&topic) {
                        match filter_to_regex(&topic) {
                            Some(re) => (store.psubscribe_regex(&topic, re.clone()), Some(re)),
                            None => {
                                return_codes.push(0x80); // failure
                                continue;
                            }
                        }
                    } else {
                        (store.subscribe(&topic), regex::Regex::new(&format!("^{}$", regex::escape(&topic))).ok())
                    };
                    // Deliver matching retained messages immediately (retain=1).
                    if let Some(re) = &re {
                        for (rt, rm) in store.retained_matching(re) {
                            let mut buf = Vec::new();
                            write_utf8(&mut buf, &rt);
                            if granted >= 1 {
                                buf.extend_from_slice(&next_pkt_id.to_be_bytes());
                                next_pkt_id = next_pkt_id.wrapping_add(1).max(1);
                            }
                            buf.extend_from_slice(rm.as_bytes());
                            let flags = 0x01 | if granted >= 1 { 0x02 } else { 0 };
                            let _ = write_packet(&mut writer, PUBLISH, flags, &buf);
                        }
                        let _ = writer.flush();
                    }
                    receivers.push((topic, granted, rx));
                    return_codes.push(granted);
                }
                let mut suback = Vec::new();
                suback.extend_from_slice(&pkt_id.to_be_bytes());
                suback.extend_from_slice(&return_codes);
                let _ = write_packet(&mut writer, SUBACK, 0, &suback);
                let _ = writer.flush();
            }

            UNSUBSCRIBE => {
                if payload.len() < 2 {
                    continue;
                }
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
                    receivers.retain(|(name, _, _)| name != &topic);
                    if has_wildcard(&topic) {
                        store.punsubscribe(&topic);
                    } else {
                        store.unsubscribe(&topic);
                    }
                }
                let _ = write_packet(&mut writer, UNSUBACK, 0, &pkt_id.to_be_bytes());
                let _ = writer.flush();
            }

            PUBREL => {
                if payload.len() >= 2 {
                    let _ = write_packet(&mut writer, PUBCOMP, 0, &payload[..2]);
                    let _ = writer.flush();
                }
            }

            PINGREQ => {
                let _ = write_packet(&mut writer, PINGRESP, 0, &[]);
                let _ = writer.flush();
            }

            DISCONNECT => {
                if log {
                    eprintln!("[mqtt] DISCONNECT client_id=\"{client_id}\"");
                }
                clean_close = true;
                break;
            }

            _ => {} // ignore unknown packets
        }
    }

    // Last Will: published on any abnormal termination (socket error or
    // keepalive expiry) — a clean DISCONNECT discards it (MQTT-3.14.4-3).
    if !clean_close {
        if let Some((wt, wm)) = &will {
            if will_retain {
                store.retain_set(wt, wm);
            }
            store.publish(wt, wm);
            if log {
                eprintln!("[mqtt] will published topic=\"{wt}\"");
            }
        }
    }

    if log {
        eprintln!("[mqtt] closed client_id=\"{client_id}\" peer={peer}");
    }
}
