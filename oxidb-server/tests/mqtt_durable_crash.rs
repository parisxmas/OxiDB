//! ADR-0015 Phase 2: an acknowledged QoS-1 message must survive a crash.
//!
//! The guarantee is write-before-ack: when the broker PUBACKs a publisher, the
//! message is already on disk for every durable subscriber that will need it.
//! The only honest way to test that is to SIGKILL the broker — a graceful stop
//! runs a final checkpoint and would hide a missing WAL write, exactly as the
//! doc-engine online-checkpoint crash test learned (v0.36.1). So this kills a
//! subprocess with SIGKILL and restarts it on the same data dir.
//!
//! Needs mosquitto's clients; skips with a printed reason otherwise.

use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn have_mosquitto() -> bool {
    Command::new("mosquitto_pub")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        // Present = the spawn succeeded. NOT `.success()`: mosquitto's clients
        // exit nonzero on --help, which made this return false with mosquitto
        // installed — and every test in this file then skipped, passing
        // vacuously in 0.02s. A skip that looks like a pass is the worst kind.
        .is_ok()
}

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

/// Start a broker, teeing its stderr into `log` inside the data dir. When an
/// assertion fails, the broker's own recovery line ("recovered N sessions, M
/// pending") is the diagnosis — a crash test that cannot say what the broker
/// recovered can only say "lost", which is not a diagnosis.
fn start(port: u16, data: &Path, log: &str) -> Child {
    let bin = env!("CARGO_BIN_EXE_oxidb-server");
    let logfile = std::fs::File::create(data.join(log)).expect("create broker log");
    // The child is owned by a guard that kills and waits on Drop.
    #[allow(clippy::zombie_processes)]
    let child = Command::new(bin)
        .env("OXIDB_MQTT_PERSIST", "1")
        .env("OXIDB_MQTT_PORT", port.to_string())
        .env("OXIDB_ADDR", format!("127.0.0.1:{}", port + 1))
        .env("OXIDB_DATA", data)
        .stdout(Stdio::null())
        .stderr(Stdio::from(logfile))
        .spawn()
        .expect("start oxidb-server");
    for _ in 0..600 {
        // 60s: see oximem_tx_wire on why readiness must tolerate suite-wide load
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return child;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("broker never came up on {port}");
}

fn read_log(data: &Path, log: &str) -> String {
    std::fs::read_to_string(data.join(log)).unwrap_or_default()
}

fn subscribe_and_leave(port: u16, id: &str, topic: &str) {
    let _ = Command::new("mosquitto_sub")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-i",
            id,
            "-c",
            "-q",
            "1",
            "-t",
            topic,
            "-W",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn publish_q1(port: u16, topic: &str, msg: &str) {
    let _ = Command::new("mosquitto_pub")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-q",
            "1",
            "-t",
            topic,
            "-m",
            msg,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn sub_once(port: u16, id: &str, topic: &str, wait: u32) -> String {
    let out = Command::new("mosquitto_sub")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-i",
            id,
            "-c",
            "-q",
            "1",
            "-t",
            topic,
            "-C",
            "1",
            "-W",
            &wait.to_string(),
        ])
        .output()
        .expect("run mosquitto_sub");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn an_acked_qos1_message_survives_sigkill() {
    if !have_mosquitto() {
        eprintln!("SKIP: mosquitto clients not installed");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = test_port(22000, 4);

    // 1. A durable subscriber registers its subscription, then leaves.
    let mut broker = start(port, dir.path(), "broker1.log");
    subscribe_and_leave(port, "crash-sub", "crash/topic");
    std::thread::sleep(Duration::from_millis(400));

    // 2. Publish a QoS-1 message while it is offline. The publisher gets a
    //    PUBACK — the broker is now on the hook to not lose it.
    publish_q1(port, "crash/topic", "survives-the-kill");
    std::thread::sleep(Duration::from_millis(400));

    // 3. SIGKILL — no graceful shutdown, no final checkpoint. A message only in
    //    memory dies here; a durable one does not.
    broker.kill().expect("SIGKILL the broker");
    broker.wait().ok();

    // 4. Restart on the SAME data dir. A brand-new process — anything it
    //    delivers came off disk.
    let mut broker2 = start(port, dir.path(), "broker2.log");

    // 5. The durable subscriber reconnects and must receive the message.
    let got = sub_once(port, "crash-sub", "crash/topic", 3);

    broker2.kill().ok();
    broker2.wait().ok();

    assert_eq!(
        got,
        "survives-the-kill",
        "an acknowledged QoS-1 message must survive a SIGKILL and be redelivered \
         after restart — this is the whole point of OXIDB_MQTT_PERSIST\n\
         --- broker1 (pre-kill) stderr ---\n{}\n--- broker2 (recovery) stderr ---\n{}",
        read_log(dir.path(), "broker1.log"),
        read_log(dir.path(), "broker2.log"),
    );
}

#[test]
fn a_delivered_and_acked_message_is_not_redelivered_after_a_crash() {
    if !have_mosquitto() {
        eprintln!("SKIP: mosquitto clients not installed");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = test_port(22000, 4);

    let mut broker = start(port, dir.path(), "broker1.log");
    subscribe_and_leave(port, "ack-sub", "ack/topic");
    std::thread::sleep(Duration::from_millis(400));
    publish_q1(port, "ack/topic", "consume-me");
    std::thread::sleep(Duration::from_millis(400));

    // Reconnect and CONSUME it (the sub PUBACKs on receipt), then it is done.
    let got = sub_once(port, "ack-sub", "ack/topic", 3);
    assert_eq!(
        got, "consume-me",
        "precondition: the message is delivered and acked"
    );
    std::thread::sleep(Duration::from_millis(400));

    // Crash and restart. The acked message must NOT come back — an at-least-once
    // broker that becomes at-least-twice-forever after every crash is a bug.
    broker.kill().expect("SIGKILL");
    broker.wait().ok();
    let mut broker2 = start(port, dir.path(), "broker2.log");

    let again = sub_once(port, "ack-sub", "ack/topic", 2);
    broker2.kill().ok();
    broker2.wait().ok();

    assert!(
        !again.contains("consume-me"),
        "an already-acked message must not be redelivered after a crash, got {again:?}"
    );
}
