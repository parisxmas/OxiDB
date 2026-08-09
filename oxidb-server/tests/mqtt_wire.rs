//! Wire-level MQTT 3.1.1 tests: spawn the real binary and speak raw MQTT
//! bytes — CONNECT/CONNACK, pub/sub, wildcards, retained messages, QoS 1/2
//! acknowledgements, auth rejection and Last Will delivery.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct Guard {
    child: Child,
    _dir: tempfile::TempDir,
    port: u16,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Wait for the child's ready file and return the mqtt port it names.
/// See pg_wire.rs for why probing a chosen port is not a readiness check.
fn wait_ready(child: &mut Child, ready: &std::path::Path) -> u16 {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(body) = std::fs::read_to_string(ready) {
            return body
                .lines()
                .find_map(|l| l.strip_prefix("mqtt="))
                .expect("ready file names the mqtt port")
                .parse()
                .expect("mqtt port is a u16");
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited before becoming ready: {status}");
        }
        assert!(Instant::now() < deadline, "server never became ready");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn spawn_with(envs: &[(&str, &str)]) -> Guard {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_oxidb-server"));
    cmd.env("OXIDB_DATA", dir.path().join("data"))
        .env("OXIDB_ADDR", "127.0.0.1:0")
        .env("OXIDB_MQTT_PORT", "auto")
        .env("OXIDB_READY_FILE", &ready)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().unwrap();
    let port = wait_ready(&mut child, &ready);
    Guard {
        child,
        _dir: dir,
        port,
    }
}
fn spawn() -> Guard {
    spawn_with(&[])
}

// ---- tiny MQTT client -------------------------------------------------------

fn utf8(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}
fn packet(t: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![(t << 4) | flags];
    let mut n = payload.len();
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(payload);
    out
}

struct Mqtt {
    s: TcpStream,
}
impl Mqtt {
    fn read_packet(&mut self) -> Option<(u8, u8, Vec<u8>)> {
        let mut hdr = [0u8; 1];
        self.s.read_exact(&mut hdr).ok()?;
        let mut n = 0usize;
        let mut mult = 1usize;
        loop {
            let mut b = [0u8; 1];
            self.s.read_exact(&mut b).ok()?;
            n += (b[0] & 0x7F) as usize * mult;
            if b[0] & 0x80 == 0 {
                break;
            }
            mult *= 128;
        }
        let mut payload = vec![0u8; n];
        self.s.read_exact(&mut payload).ok()?;
        Some((hdr[0] >> 4, hdr[0] & 0x0F, payload))
    }
    fn send(&mut self, bytes: &[u8]) {
        self.s.write_all(bytes).unwrap();
        self.s.flush().unwrap();
    }
}

/// Connect; returns (client, connack return code).
fn connect_opts(
    port: u16,
    id: &str,
    keepalive: u16,
    will: Option<(&str, &str)>,
    creds: Option<(&str, &str)>,
) -> (Mqtt, u8) {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(8))).unwrap();
    let mut c = Mqtt { s };
    let mut p = Vec::new();
    utf8(&mut p, "MQTT");
    p.push(4); // 3.1.1
    let mut flags = 0x02u8; // clean session
    if will.is_some() {
        flags |= 0x04;
    }
    if creds.is_some() {
        flags |= 0x80 | 0x40;
    }
    p.push(flags);
    p.extend_from_slice(&keepalive.to_be_bytes());
    utf8(&mut p, id);
    if let Some((wt, wm)) = will {
        utf8(&mut p, wt);
        utf8(&mut p, wm);
    }
    if let Some((u, pw)) = creds {
        utf8(&mut p, u);
        utf8(&mut p, pw);
    }
    c.send(&packet(1, 0, &p));
    let (t, _, pl) = c.read_packet().expect("connack");
    assert_eq!(t, 2, "expected CONNACK");
    (c, pl[1])
}
fn connect(port: u16, id: &str) -> Mqtt {
    let (c, rc) = connect_opts(port, id, 0, None, None);
    assert_eq!(rc, 0, "connect refused");
    c
}
fn subscribe(c: &mut Mqtt, filter: &str, qos: u8) -> u8 {
    let mut p = vec![0x00, 0x01]; // pkt id 1
    utf8(&mut p, filter);
    p.push(qos);
    c.send(&packet(8, 0x02, &p));
    // SUBACK may be preceded by retained PUBLISHes — scan for it.
    for _ in 0..8 {
        if let Some((t, _, pl)) = c.read_packet()
            && t == 9
        {
            return pl[2];
        }
    }
    panic!("no SUBACK");
}
fn publish(c: &mut Mqtt, topic: &str, msg: &str, qos: u8, retain: bool) {
    let mut p = Vec::new();
    utf8(&mut p, topic);
    if qos > 0 {
        p.extend_from_slice(&[0x00, 0x0A]);
    }
    p.extend_from_slice(msg.as_bytes());
    let flags = (qos << 1) | (retain as u8);
    c.send(&packet(3, flags, &p));
}
/// Wait for a PUBLISH; returns (topic, payload).
fn expect_publish(c: &mut Mqtt) -> (String, String) {
    for _ in 0..8 {
        if let Some((t, flags, pl)) = c.read_packet()
            && t == 3
        {
            let tl = u16::from_be_bytes([pl[0], pl[1]]) as usize;
            let topic = String::from_utf8_lossy(&pl[2..2 + tl]).to_string();
            let mut off = 2 + tl;
            if (flags >> 1) & 0x03 > 0 {
                off += 2; // skip pkt id
            }
            return (topic, String::from_utf8_lossy(&pl[off..]).to_string());
        }
    }
    panic!("no PUBLISH received");
}

// ---- tests ------------------------------------------------------------------

#[test]
fn pubsub_roundtrip_exact_topic() {
    let g = spawn();
    let mut sub = connect(g.port, "sub1");
    assert_eq!(subscribe(&mut sub, "sensors/temp", 0), 0);
    let mut publ = connect(g.port, "pub1");
    publish(&mut publ, "sensors/temp", "21.5", 0, false);
    let (t, m) = expect_publish(&mut sub);
    assert_eq!((t.as_str(), m.as_str()), ("sensors/temp", "21.5"));
}

#[test]
fn wildcard_plus_and_hash_match() {
    let g = spawn();
    let mut sub = connect(g.port, "wsub");
    assert_eq!(subscribe(&mut sub, "sensors/+/temp", 0), 0);
    let mut publ = connect(g.port, "wpub");
    publish(&mut publ, "sensors/kitchen/temp", "20", 0, false);
    assert_eq!(expect_publish(&mut sub).0, "sensors/kitchen/temp");
    // '#' catches deep levels.
    let mut sub2 = connect(g.port, "wsub2");
    assert_eq!(subscribe(&mut sub2, "alerts/#", 0), 0);
    publish(&mut publ, "alerts/fire/floor3", "!!", 0, false);
    assert_eq!(expect_publish(&mut sub2).0, "alerts/fire/floor3");
    // '+' must NOT cross levels.
    publish(&mut publ, "sensors/a/b/temp", "x", 0, false);
    publish(&mut publ, "sensors/hall/temp", "22", 0, false);
    assert_eq!(expect_publish(&mut sub).0, "sensors/hall/temp");
}

#[test]
fn retained_message_delivered_on_subscribe() {
    let g = spawn();
    let mut publ = connect(g.port, "rpub");
    publish(&mut publ, "status/device1", "online", 0, true);
    std::thread::sleep(Duration::from_millis(200));
    // A LATER subscriber still gets it, marked retained.
    let mut sub = connect(g.port, "rsub");
    let mut p = vec![0x00, 0x01];
    utf8(&mut p, "status/#");
    p.push(0);
    sub.send(&packet(8, 0x02, &p));
    let mut got_retained = false;
    for _ in 0..6 {
        if let Some((t, flags, pl)) = sub.read_packet()
            && t == 3
        {
            assert_eq!(flags & 0x01, 0x01, "retain flag must be set");
            let tl = u16::from_be_bytes([pl[0], pl[1]]) as usize;
            assert_eq!(&pl[2..2 + tl], b"status/device1");
            got_retained = true;
            break;
        }
    }
    assert!(got_retained, "retained message not delivered");
}

#[test]
fn qos1_puback_and_qos2_handshake() {
    let g = spawn();
    let mut c = connect(g.port, "qos");
    publish(&mut c, "q/one", "m", 1, false);
    let (t, _, pl) = c.read_packet().expect("puback");
    assert_eq!(t, 4, "PUBACK");
    assert_eq!(pl, vec![0x00, 0x0A]);
    publish(&mut c, "q/two", "m", 2, false);
    let (t, _, pl) = c.read_packet().expect("pubrec");
    assert_eq!(t, 5, "PUBREC");
    c.send(&packet(6, 0x02, &pl)); // PUBREL
    let (t, _, _) = c.read_packet().expect("pubcomp");
    assert_eq!(t, 7, "PUBCOMP");
}

#[test]
fn auth_rejects_wrong_credentials() {
    let g = spawn_with(&[
        ("OXIDB_MQTT_USER", "iot"),
        ("OXIDB_MQTT_PASSWORD", "s3cret"),
    ]);
    let (_c, rc) = connect_opts(g.port, "bad", 0, None, Some(("iot", "wrong")));
    assert_eq!(rc, 0x04, "bad credentials must be refused");
    let (_c, rc) = connect_opts(g.port, "anon", 0, None, None);
    assert_eq!(rc, 0x04, "anonymous must be refused when creds configured");
    let (_c, rc) = connect_opts(g.port, "good", 0, None, Some(("iot", "s3cret")));
    assert_eq!(rc, 0, "correct credentials accepted");
}

#[test]
fn last_will_fires_on_abnormal_disconnect() {
    let g = spawn();
    let mut watcher = connect(g.port, "watcher");
    assert_eq!(subscribe(&mut watcher, "clients/+/status", 0), 0);
    {
        let (dying, rc) = connect_opts(
            g.port,
            "dev42",
            0,
            Some(("clients/dev42/status", "offline")),
            None,
        );
        assert_eq!(rc, 0);
        drop(dying); // socket dies WITHOUT DISCONNECT → will must fire
    }
    let (t, m) = expect_publish(&mut watcher);
    assert_eq!(
        (t.as_str(), m.as_str()),
        ("clients/dev42/status", "offline")
    );
}
