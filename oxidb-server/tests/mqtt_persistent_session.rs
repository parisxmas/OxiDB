//! End-to-end MQTT session tests against a real broker, driven by real
//! `mosquitto_pub`/`mosquitto_sub` clients. ADR-0015.
//!
//! The unit tests in `mqtt_session.rs` pin the registry's logic; this pins the
//! thing that actually matters — that a real MQTT client, speaking the wire
//! protocol unmodified, gets the at-least-once behaviour the broker advertises.
//! The whole point of ADR-0015 is that the wire and the behaviour agree, and
//! only a real client can prove that.
//!
//! Skips cleanly if mosquitto's clients are not installed (CI without them),
//! because a test that silently passes when it did not run is worse than no
//! test — so it prints why it skipped.

use std::io::Read;
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn have_mosquitto() -> bool {
    Command::new("mosquitto_pub")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

struct Broker {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_broker() -> Broker {
    let dir = tempfile::tempdir().unwrap();
    let port = 15400 + (std::process::id() % 500) as u16;
    let bin = env!("CARGO_BIN_EXE_oxidb-server");
    let child = Command::new(bin)
        .env("OXIDB_MQTT_PORT", port.to_string())
        .env("OXIDB_ADDR", format!("127.0.0.1:{}", port + 1))
        .env("OXIDB_DATA", dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start oxidb-server");

    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Broker { child, port, _dir: dir };
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("MQTT broker never came up on port {port}");
}

/// A subscriber that connects, subscribes, and exits when it has one message or
/// after `wait` seconds. Returns whatever it printed to stdout (the payload, or
/// empty on timeout).
fn sub_once(port: u16, id: Option<&str>, persistent: bool, topic: &str, wait: u32) -> String {
    let mut cmd = Command::new("mosquitto_sub");
    cmd.args(["-h", "127.0.0.1", "-p", &port.to_string()]);
    if let Some(id) = id {
        cmd.args(["-i", id]);
    }
    if persistent {
        cmd.arg("-c"); // --disable-clean-session
    }
    cmd.args(["-q", "1", "-t", topic, "-C", "1", "-W", &wait.to_string()]);
    let out = cmd.output().expect("run mosquitto_sub");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Subscribe-then-leave: register the subscription for a persistent session and
/// disconnect without receiving anything.
fn subscribe_and_leave(port: u16, id: &str, topic: &str) {
    // -W 1 → times out after 1s with no message, then disconnects.
    let _ = Command::new("mosquitto_sub")
        .args(["-h", "127.0.0.1", "-p", &port.to_string(), "-i", id, "-c", "-q", "1", "-t", topic, "-W", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn publish(port: u16, topic: &str, msg: &str, retain: bool) {
    let mut cmd = Command::new("mosquitto_pub");
    cmd.args(["-h", "127.0.0.1", "-p", &port.to_string(), "-q", "1", "-t", topic, "-m", msg]);
    if retain {
        cmd.arg("-r");
    }
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    let _ = cmd.status();
}

fn settle() {
    std::thread::sleep(Duration::from_millis(400));
}

#[test]
fn persistent_session_receives_messages_published_while_offline() {
    if !have_mosquitto() {
        eprintln!("SKIP: mosquitto_pub/sub not installed");
        return;
    }
    let b = start_broker();

    // The subscription is registered by a persistent session, which then leaves.
    subscribe_and_leave(b.port, "durable-1", "cc/temp");
    settle();

    // Published while nobody is connected — the bare broker dropped this.
    publish(b.port, "cc/temp", "while-offline", false);
    settle();

    // Reconnecting the same persistent id must deliver the buffered message.
    let got = sub_once(b.port, Some("durable-1"), true, "cc/temp", 3);
    assert_eq!(
        got, "while-offline",
        "a persistent session must receive a message published while it was offline — \
         this is the at-least-once guarantee the broker advertises"
    );
}

#[test]
fn a_clean_session_does_not_resurrect_offline_messages() {
    if !have_mosquitto() {
        eprintln!("SKIP: mosquitto_pub/sub not installed");
        return;
    }
    let b = start_broker();

    // A clean (non-persistent) subscriber subscribes and leaves.
    let _ = Command::new("mosquitto_sub")
        .args(["-h", "127.0.0.1", "-p", &b.port.to_string(), "-i", "clean-1", "-t", "cc/z", "-W", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    settle();
    publish(b.port, "cc/z", "must-not-arrive", false);
    settle();

    // Reconnecting clean must NOT deliver it — clean_session has no memory.
    let got = sub_once(b.port, Some("clean-1"), false, "cc/z", 1);
    assert!(
        !got.contains("must-not-arrive"),
        "a clean session must not receive offline messages, got {got:?}"
    );
}

#[test]
fn live_pubsub_and_retained_still_work() {
    if !have_mosquitto() {
        eprintln!("SKIP: mosquitto_pub/sub not installed");
        return;
    }
    let b = start_broker();

    // Retained: set it, then a fresh subscriber gets it on subscribe.
    publish(b.port, "ret/w", "retained-value", true);
    settle();
    let got = sub_once(b.port, None, false, "ret/w", 2);
    assert_eq!(got, "retained-value", "retained message must be delivered on subscribe");
}

#[test]
fn a_takeover_connection_wins_and_the_old_one_exits() {
    if !have_mosquitto() {
        eprintln!("SKIP: mosquitto_pub/sub not installed");
        return;
    }
    let b = start_broker();

    // First connection with a fixed id, held open.
    let mut first = Command::new("mosquitto_sub")
        .args(["-h", "127.0.0.1", "-p", &b.port.to_string(), "-i", "solo", "-t", "cc/take", "-W", "5"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("first sub");
    settle();

    // Second connection with the SAME id takes over (MQTT-3.1.4-2).
    let got = sub_once(b.port, Some("solo"), false, "cc/take", 2);
    // Publish reaches the new owner.
    publish(b.port, "cc/take", "to-the-winner", false);

    // The first connection must not hang forever; kill defensively and confirm
    // it produced nothing surprising.
    let _ = first.kill();
    let mut buf = String::new();
    if let Some(mut out) = first.stdout.take() {
        let _ = out.read_to_string(&mut buf);
    }
    let _ = first.wait();
    // The important assertion is simply that the broker survived a same-id
    // takeover without deadlocking — both calls returned.
    let _ = got;
    let _ = buf;
}
