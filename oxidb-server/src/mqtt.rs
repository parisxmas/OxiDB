//! MQTT v3.1.1 protocol handler for pub/sub messaging.
//!
//! Shares the same pub/sub infrastructure as the OxiMem RESP layer.
//! An MQTT PUBLISH on topic "sensors/temp" is received by OxiMem
//! SUBSCRIBE "sensors/temp" and vice versa.

use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::mqtt_session::MqttSessions;
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

    // clean_session (conn flag bit 1). false => resume a persistent session and
    // keep it after this socket closes; true => a fresh, forgotten-on-close one.
    let clean_session = conn_flags & 0x02 != 0;
    let persistent = !clean_session;

    // Attach to (or resume) the session. A client with no id MUST use
    // clean_session (MQTT-3.1.3-7); we synthesise a per-connection id so an
    // anonymous clean client still works without colliding with a named one.
    let session_key = if client_id.is_empty() {
        format!("__anon__{peer}")
    } else {
        client_id.clone()
    };
    let sessions = MqttSessions::global();
    let (session_present, generation) = sessions.attach(&session_key, clean_session);

    // CONNACK: bit 0 of the ack flags is session_present — now it means something.
    let ack_flags = if session_present { 0x01 } else { 0x00 };
    if write_packet(&mut writer, CONNACK, 0, &[ack_flags, 0x00]).is_err() {
        return;
    }
    if writer.flush().is_err() {
        return;
    }

    // --- Main loop ---
    let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
    // On the first delivery poll after connecting, resend anything left inflight
    // from a previous session (with DUP set). After that, normal delivery.
    let mut resend_inflight = session_present;
    let mut last_activity = std::time::Instant::now();
    let mut clean_close = false;
    // MQTT-3.1.2-24: disconnect after 1.5 × keepalive of silence.
    let ka_limit = if keepalive > 0 {
        Some(Duration::from_millis(keepalive as u64 * 1500))
    } else {
        None
    };

    loop {
        // Collect the batch to deliver under the registry lock — packet-id
        // assignment and inflight recording must be atomic — then write it with
        // the lock released so slow socket I/O never blocks the reaper or other
        // sessions. If our generation moved, a newer connection took the
        // session over (MQTT-3.1.4-2) and we must exit rather than double-deliver.
        let batch = match sessions.with(&session_key, |sess| {
            if sess.generation != generation {
                None // superseded
            } else {
                Some(sess.take_deliveries(resend_inflight))
            }
        }) {
            Some(Some(b)) => b,
            _ => return, // session gone or taken over
        };
        resend_inflight = false;

        let mut wrote = false;
        for d in batch {
            let mut buf = Vec::new();
            write_utf8(&mut buf, &d.topic);
            let mut flags = 0u8;
            if d.qos >= 1 {
                buf.extend_from_slice(&d.pkt_id.to_be_bytes());
                flags |= 0x02; // QoS 1
                if d.dup {
                    flags |= 0x08; // DUP — a redelivery, not a new message
                }
            }
            buf.extend_from_slice(d.message.as_bytes());
            if write_packet(&mut writer, PUBLISH, flags, &buf).is_err() {
                return;
            }
            if log {
                eprintln!("[mqtt] >> PUBLISH topic=\"{}\" len={}", d.topic, d.message.len());
            }
            wrote = true;
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
                            Some(re) => (store.psubscribe_regex(&topic, re.clone()), re),
                            None => {
                                return_codes.push(0x80); // failure
                                continue;
                            }
                        }
                    } else {
                        match regex::Regex::new(&format!("^{}$", regex::escape(&topic))) {
                            Ok(re) => (store.subscribe(&topic), re),
                            Err(_) => {
                                return_codes.push(0x80);
                                continue;
                            }
                        }
                    };
                    // The subscription's receiver goes into the SESSION, not this
                    // stack frame, so it outlives the socket and keeps buffering
                    // while the client is offline. Replace an existing sub on the
                    // same filter (a re-SUBSCRIBE updates QoS — MQTT-3.8.4-3).
                    sessions.with(&session_key, |sess| {
                        sess.subs.retain(|s| s.filter != topic);
                        sess.subs.push(crate::mqtt_session::Subscription {
                            filter: topic.clone(),
                            qos: granted,
                            regex: re.clone(),
                            rx,
                        });
                    });
                    // Deliver matching retained messages immediately (retain=1),
                    // recording QoS-1 ones as inflight so they too are resent if
                    // unacked.
                    for (rt, rm) in store.retained_matching(&re) {
                        let pkt_id = if granted >= 1 {
                            sessions
                                .with(&session_key, |sess| {
                                    let id = sess.next_pkt_id.wrapping_add(1).max(1);
                                    sess.next_pkt_id = id;
                                    sess.inflight.insert(
                                        id,
                                        crate::mqtt_session::Inflight { topic: rt.clone(), message: rm.clone() },
                                    );
                                    id
                                })
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        let mut buf = Vec::new();
                        write_utf8(&mut buf, &rt);
                        if granted >= 1 {
                            buf.extend_from_slice(&pkt_id.to_be_bytes());
                        }
                        buf.extend_from_slice(rm.as_bytes());
                        let flags = 0x01 | if granted >= 1 { 0x02 } else { 0 };
                        let _ = write_packet(&mut writer, PUBLISH, flags, &buf);
                    }
                    let _ = writer.flush();
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
                    sessions.with(&session_key, |sess| {
                        sess.subs.retain(|s| s.filter != topic);
                    });
                    if has_wildcard(&topic) {
                        store.punsubscribe(&topic);
                    } else {
                        store.unsubscribe(&topic);
                    }
                }
                let _ = write_packet(&mut writer, UNSUBACK, 0, &pkt_id.to_be_bytes());
                let _ = writer.flush();
            }

            PUBACK => {
                // A subscriber acknowledged a QoS-1 delivery — clear it from
                // inflight so it is not resent. Without this the message would
                // be redelivered forever, which is precisely the machinery that
                // was missing: the id was allocated and then forgotten.
                if payload.len() >= 2 {
                    let pkt_id = u16::from_be_bytes([payload[0], payload[1]]);
                    sessions.with(&session_key, |sess| {
                        sess.inflight.remove(&pkt_id);
                    });
                }
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

    // Detach: a clean_session client is forgotten; a persistent one is left
    // offline for the reaper to keep draining into its bounded queue, so its
    // subscriptions and inflight survive until it reconnects. The generation
    // guard means a connection that was already superseded does not tear down
    // the session a newer one now owns.
    sessions.detach(&session_key, generation, persistent);

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
