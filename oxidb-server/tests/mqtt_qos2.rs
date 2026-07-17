//! ADR-0015 Phase 3: outbound QoS 2 — exactly-once, including across a crash.
//!
//! These drive the broker with a raw-socket MQTT client written here, not
//! mosquitto. Deliberately: the core of exactly-once is what happens when a
//! packet is RETRANSMITTED or a handshake is abandoned halfway, and a
//! well-behaved client never does either on demand. To prove the state machine
//! we must be the badly-behaved client ourselves — duplicate PUBLISHes,
//! PUBRECs followed by a vanishing socket, reconnects mid-handshake.
//!
//! What is pinned:
//!   * a duplicate QoS-2 PUBLISH (same packet id, DUP set) is re-acked but NOT
//!     fanned out again — the subscriber sees exactly one copy;
//!   * a PUBREL owed at crash time is owed after restart: PUBREC deletes the
//!     durable message (the receiver owns it), the PUBREL debt survives as its
//!     own record, and the reconnecting subscriber gets PUBREL — not a
//!     duplicate PUBLISH;
//!   * a completed inbound QoS-2 publish to an offline durable subscriber
//!     survives SIGKILL and is delivered qos 2, once, after restart.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBREC: u8 = 5;
const PUBREL: u8 = 6;
const PUBCOMP: u8 = 7;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const DISCONNECT: u8 = 14;

// ── A minimal, deliberately manual MQTT 3.1.1 client ────────────────────

fn push_utf8(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn write_pkt(s: &mut TcpStream, pkt_type: u8, flags: u8, payload: &[u8]) {
    let mut frame = vec![(pkt_type << 4) | (flags & 0x0F)];
    let mut len = payload.len();
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        frame.push(b);
        if len == 0 {
            break;
        }
    }
    frame.extend_from_slice(payload);
    s.write_all(&frame).expect("write frame");
}

/// Read one packet, or None on timeout. Timeouts are how these tests assert a
/// NEGATIVE ("no second copy arrives"), so they are part of the contract.
fn read_pkt(s: &mut TcpStream) -> Option<(u8, u8, Vec<u8>)> {
    let mut hdr = [0u8; 1];
    match s.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(_) => return None, // timeout or closed
    }
    let mut len = 0usize;
    let mut mult = 1usize;
    loop {
        let mut b = [0u8; 1];
        s.read_exact(&mut b).ok()?;
        len += (b[0] & 0x7F) as usize * mult;
        if b[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        s.read_exact(&mut payload).ok()?;
    }
    Some((hdr[0] >> 4, hdr[0] & 0x0F, payload))
}

/// CONNECT and assert the CONNACK; returns (stream, session_present).
fn connect(port: u16, client_id: &str, clean: bool) -> (TcpStream, bool) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut vh = Vec::new();
    push_utf8(&mut vh, "MQTT");
    vh.push(4); // 3.1.1
    vh.push(if clean { 0x02 } else { 0x00 });
    vh.extend_from_slice(&[0, 0]); // keepalive 0 = never expire
    push_utf8(&mut vh, client_id);
    write_pkt(&mut s, CONNECT, 0, &vh);
    let (t, _f, p) = read_pkt(&mut s).expect("CONNACK");
    assert_eq!(t, CONNACK, "expected CONNACK");
    assert_eq!(p[1], 0, "connection refused: {}", p[1]);
    (s, p[0] & 0x01 != 0)
}

fn subscribe(s: &mut TcpStream, topic: &str, qos: u8) {
    let mut p = Vec::new();
    p.extend_from_slice(&1u16.to_be_bytes()); // pkt id 1
    push_utf8(&mut p, topic);
    p.push(qos);
    write_pkt(s, SUBSCRIBE, 0x02, &p);
    let (t, _f, sp) = read_pkt(s).expect("SUBACK");
    assert_eq!(t, SUBACK);
    assert_eq!(sp[2], qos, "granted qos {} != requested {qos}", sp[2]);
}

/// A received PUBLISH, decoded.
struct Rx {
    topic: String,
    payload: String,
    qos: u8,
    pkt_id: u16,
    dup: bool,
}

fn parse_publish(flags: u8, p: &[u8]) -> Rx {
    let tlen = u16::from_be_bytes([p[0], p[1]]) as usize;
    let topic = String::from_utf8_lossy(&p[2..2 + tlen]).to_string();
    let qos = (flags >> 1) & 0x03;
    let mut off = 2 + tlen;
    let pkt_id = if qos > 0 {
        let id = u16::from_be_bytes([p[off], p[off + 1]]);
        off += 2;
        id
    } else {
        0
    };
    Rx {
        topic,
        payload: String::from_utf8_lossy(&p[off..]).to_string(),
        qos,
        pkt_id,
        dup: flags & 0x08 != 0,
    }
}

/// Drain the socket until quiet, completing every QoS-2 handshake we are
/// offered. Returns the PUBLISHes received — the exactly-once assertions are
/// about this list's length.
fn drain_completing_handshakes(s: &mut TcpStream) -> Vec<Rx> {
    let mut got = Vec::new();
    while let Some((t, f, p)) = read_pkt(s) {
        match t {
            PUBLISH => {
                let rx = parse_publish(f, &p);
                if rx.qos == 2 {
                    write_pkt(s, PUBREC, 0, &rx.pkt_id.to_be_bytes());
                }
                got.push(rx);
            }
            PUBREL => {
                write_pkt(s, PUBCOMP, 0, &p[..2]);
            }
            _ => {}
        }
    }
    got
}

// ── The broker under test ───────────────────────────────────────────────

/// Every test gets its own port pair (port, port+1). Deriving from the pid
/// alone gives every test in the binary the SAME port: under the default
/// parallel runner the extra brokers fail to bind and the tests silently talk
/// to whichever broker won — the exact bug s3_etag.rs already documents. The
/// bands (21xxx/22xxx/23xxx per file) stay clear of the other suites' ranges.
fn test_port(base: u16, span_per_pid: u16) -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    base + (std::process::id() % 97) as u16 * span_per_pid + NEXT.fetch_add(2, Ordering::SeqCst)
}

fn start(port: u16, data: &Path, persist: bool, log: &str) -> Child {
    let bin = env!("CARGO_BIN_EXE_oxidb-server");
    let logfile = std::fs::File::create(data.join(log)).expect("create log");
    let child = Command::new(bin)
        .env("OXIDB_MQTT_PERSIST", if persist { "1" } else { "0" })
        .env("OXIDB_MQTT_PORT", port.to_string())
        .env("OXIDB_ADDR", format!("127.0.0.1:{}", port + 1))
        .env("OXIDB_DATA", data)
        .stdout(Stdio::null())
        .stderr(Stdio::from(logfile))
        .spawn()
        .expect("start oxidb-server");
    for _ in 0..600 {  // 60s: see oximem_tx_wire on why readiness must tolerate suite-wide load
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return child;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("broker never came up on {port}");
}

fn settle() {
    std::thread::sleep(Duration::from_millis(300));
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn a_duplicate_qos2_publish_is_delivered_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port(23000, 6);
    let mut broker = start(port, dir.path(), false, "b.log");

    let (mut sub, _) = connect(port, "q2-sub", true);
    subscribe(&mut sub, "eo/t", 2);
    settle();

    let (mut publ, _) = connect(port, "q2-pub", true);
    let mut body = Vec::new();
    push_utf8(&mut body, "eo/t");
    body.extend_from_slice(&7u16.to_be_bytes());
    body.extend_from_slice(b"only-once");

    // First transmission.
    write_pkt(&mut publ, PUBLISH, 2 << 1, &body);
    let (t, _f, p) = read_pkt(&mut publ).expect("PUBREC");
    assert_eq!((t, u16::from_be_bytes([p[0], p[1]])), (PUBREC, 7));

    // The retransmission a real publisher sends when the PUBREC gets lost:
    // same packet id, DUP set. The broker must re-ack it — and fan out nothing.
    write_pkt(&mut publ, PUBLISH, (2 << 1) | 0x08, &body);
    let (t, _f, p) = read_pkt(&mut publ).expect("PUBREC for the dup");
    assert_eq!((t, u16::from_be_bytes([p[0], p[1]])), (PUBREC, 7));

    // Finish the exchange.
    write_pkt(&mut publ, PUBREL, 0x02, &7u16.to_be_bytes());
    let (t, _f, _p) = read_pkt(&mut publ).expect("PUBCOMP");
    assert_eq!(t, PUBCOMP);

    // The subscriber's view is the verdict.
    let got = drain_completing_handshakes(&mut sub);
    assert_eq!(
        got.len(),
        1,
        "a duplicate QoS-2 PUBLISH must not fan out twice — got {:?}",
        got.iter().map(|r| &r.payload).collect::<Vec<_>>()
    );
    assert_eq!(got[0].payload, "only-once");
    assert_eq!(got[0].qos, 2, "a QoS-2 publish to a QoS-2 subscription delivers at 2");

    broker.kill().ok();
    broker.wait().ok();
}

#[test]
fn a_pubrel_owed_at_crash_time_is_owed_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port(23000, 6);
    let mut broker = start(port, dir.path(), true, "b1.log");

    let (mut sub, _) = connect(port, "rel-sub", false);
    subscribe(&mut sub, "rel/t", 2);
    settle();

    // A publisher completes its half; the broker now owes the subscriber a
    // QoS-2 delivery.
    let (mut publ, _) = connect(port, "rel-pub", true);
    let mut body = Vec::new();
    push_utf8(&mut body, "rel/t");
    body.extend_from_slice(&5u16.to_be_bytes());
    body.extend_from_slice(b"half-done");
    write_pkt(&mut publ, PUBLISH, 2 << 1, &body);
    read_pkt(&mut publ).expect("PUBREC");
    write_pkt(&mut publ, PUBREL, 0x02, &5u16.to_be_bytes());
    read_pkt(&mut publ).expect("PUBCOMP");

    // The subscriber takes delivery, PUBRECs it — and vanishes before PUBCOMP.
    let (t, f, p) = read_pkt(&mut sub).expect("the QoS-2 PUBLISH");
    assert_eq!(t, PUBLISH);
    let rx = parse_publish(f, &p);
    assert_eq!((rx.payload.as_str(), rx.qos), ("half-done", 2));
    write_pkt(&mut sub, PUBREC, 0, &rx.pkt_id.to_be_bytes());
    let (t, _f, relp) = read_pkt(&mut sub).expect("PUBREL");
    assert_eq!(t, PUBREL);
    assert_eq!(u16::from_be_bytes([relp[0], relp[1]]), rx.pkt_id);
    drop(sub); // no PUBCOMP — the socket just dies
    settle();

    // Crash. The PUBREC was the exactly-once point: the message record is gone,
    // the PUBREL debt is on disk.
    broker.kill().expect("SIGKILL");
    broker.wait().ok();
    let mut broker2 = start(port, dir.path(), true, "b2.log");

    // Reconnect: the debt is honoured — PUBREL for the SAME id, and crucially
    // no duplicate PUBLISH (that would be delivering the message twice).
    let (mut sub2, present) = connect(port, "rel-sub", false);
    assert!(present, "the session must resume");
    let mut got_rel = None;
    let mut got_publish = 0u32;
    while let Some((t, f, p)) = read_pkt(&mut sub2) {
        match t {
            PUBREL => {
                got_rel = Some(u16::from_be_bytes([p[0], p[1]]));
                write_pkt(&mut sub2, PUBCOMP, 0, &p[..2]);
            }
            PUBLISH => {
                let rx = parse_publish(f, &p);
                if rx.qos == 2 {
                    write_pkt(&mut sub2, PUBREC, 0, &rx.pkt_id.to_be_bytes());
                }
                got_publish += 1;
            }
            _ => {}
        }
    }
    assert_eq!(
        got_rel,
        Some(rx.pkt_id),
        "the PUBREL owed before the crash must be resent with the same id\n\
         broker2 log:\n{}",
        std::fs::read_to_string(dir.path().join("b2.log")).unwrap_or_default()
    );
    assert_eq!(
        got_publish, 0,
        "the message was PUBREC'd before the crash — redelivering it as a \
         PUBLISH would be delivering it twice"
    );

    broker2.kill().ok();
    broker2.wait().ok();
}

#[test]
fn a_qos2_message_for_an_offline_subscriber_survives_sigkill() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port(23000, 6);
    let mut broker = start(port, dir.path(), true, "b1.log");

    // A durable subscriber registers a QoS-2 subscription and leaves cleanly.
    let (mut sub, _) = connect(port, "off-sub", false);
    subscribe(&mut sub, "off/t", 2);
    write_pkt(&mut sub, DISCONNECT, 0, &[]);
    drop(sub);
    settle();

    // The publisher completes the full inbound QoS-2 exchange while the
    // subscriber is offline. Write-before-ack: by the time it has its PUBREC,
    // the message is on disk.
    let (mut publ, _) = connect(port, "off-pub", true);
    let mut body = Vec::new();
    push_utf8(&mut body, "off/t");
    body.extend_from_slice(&9u16.to_be_bytes());
    body.extend_from_slice(b"crash-proof");
    write_pkt(&mut publ, PUBLISH, 2 << 1, &body);
    read_pkt(&mut publ).expect("PUBREC");
    write_pkt(&mut publ, PUBREL, 0x02, &9u16.to_be_bytes());
    read_pkt(&mut publ).expect("PUBCOMP");
    settle();

    broker.kill().expect("SIGKILL");
    broker.wait().ok();
    let mut broker2 = start(port, dir.path(), true, "b2.log");

    // The subscriber returns and must get the message — at QoS 2, exactly once,
    // through the full handshake, from a broker that just lost its memory.
    let (mut sub2, present) = connect(port, "off-sub", false);
    assert!(present, "the session must resume");
    let got = drain_completing_handshakes(&mut sub2);
    assert_eq!(
        got.len(),
        1,
        "exactly one delivery expected, got {:?}\nbroker2 log:\n{}",
        got.iter().map(|r| &r.payload).collect::<Vec<_>>(),
        std::fs::read_to_string(dir.path().join("b2.log")).unwrap_or_default()
    );
    assert_eq!(got[0].payload, "crash-proof");
    assert_eq!(got[0].qos, 2);
    assert_eq!(got[0].topic, "off/t");

    // And it must not come again: the handshake completed, so a reconnect
    // delivers nothing.
    write_pkt(&mut sub2, DISCONNECT, 0, &[]);
    drop(sub2);
    settle();
    let (mut sub3, _) = connect(port, "off-sub", false);
    let again = drain_completing_handshakes(&mut sub3);
    assert!(
        again.is_empty(),
        "the completed delivery must not repeat, got {:?}",
        again.iter().map(|r| &r.payload).collect::<Vec<_>>()
    );

    broker2.kill().ok();
    broker2.wait().ok();
}
