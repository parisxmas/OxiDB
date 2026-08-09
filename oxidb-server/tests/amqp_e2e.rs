//! AMQP 0-9-1 end-to-end, driven by pika — the real RabbitMQ Python client,
//! unmodified. ADR-0016 Phases 1–2.
//!
//! The unit tests pin the queue semantics; these pin the claim that matters:
//! code written for RabbitMQ points at OxiDB and works. pika is vendored into
//! `target/amqp-test-deps` (pip --target, no system pollution) and loaded via
//! PYTHONPATH — Phase 2 (topic/fanout, Basic.Qos, mandatory Basic.Return) is
//! covered here too; the availability check runs an actual `import pika` and reads
//! the exit code — which is meaningful for an import, unlike `--help` exit
//! codes (the have_mosquitto lesson, applied rather than repeated).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn pika_path() -> PathBuf {
    repo_root().join("target/amqp-test-deps")
}

fn have_pika() -> bool {
    Command::new("python3")
        .args(["-c", "import pika"])
        .env("PYTHONPATH", pika_path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run a python snippet against the broker; returns its stdout. The snippet
/// gets the port as argv[1]. Stderr is passed through so a pika traceback
/// lands in the test output instead of vanishing.
fn py(port: u16, script: &str) -> String {
    let out = Command::new("python3")
        .args(["-c", script, &port.to_string()])
        .env("PYTHONPATH", pika_path())
        .output()
        .expect("run python3");
    if !out.status.success() {
        panic!(
            "python client failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// A broker that dies with its guard. Assertions in these tests run while the
/// broker is alive, and a panic that leaks the process leaves it squatting the
/// port band for every later run — the first versions of this file leaked
/// fifteen of them. Drop is the only exit path a panic cannot skip.
struct BrokerGuard(Child);

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Wait for the child's ready file and return the port named `key`.
/// See pg_wire.rs for why probing a chosen port is not a readiness check.
fn wait_ready(child: &mut Child, ready: &Path, key: &str) -> u16 {
    for _ in 0..600 {
        if let Ok(body) = std::fs::read_to_string(ready) {
            return body
                .lines()
                .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
                .unwrap_or_else(|| panic!("ready file names the {key} port"))
                .parse()
                .expect("port is a u16");
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("broker exited before becoming ready: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("broker never became ready");
}

fn start(data: &Path, log: &str) -> (BrokerGuard, u16) {
    let bin = env!("CARGO_BIN_EXE_oxidb-server");
    let logfile = std::fs::File::create(data.join(log)).expect("create log");
    // A restart writes a fresh ready file (fresh kernel-assigned port).
    let ready = data.join("ready");
    let _ = std::fs::remove_file(&ready);
    // The child is owned by a guard that kills and waits on Drop.
    #[allow(clippy::zombie_processes)]
    let mut child = Command::new(bin)
        .env("OXIDB_AMQP_PORT", "auto")
        .env("OXIDB_ADDR", "127.0.0.1:0")
        .env("OXIDB_READY_FILE", &ready)
        .env("OXIDB_DATA", data.join("data"))
        .stdout(Stdio::null())
        .stderr(Stdio::from(logfile))
        .spawn()
        .expect("start oxidb-server");
    let port = wait_ready(&mut child, &ready, "amqp");
    (BrokerGuard(child), port)
}

/// Like `start`, but with the MQTT listener up too — for the ADR-0016
/// Phase 3 bridge tests. Returns (guard, amqp_port, mqtt_port).
fn start_with_mqtt(data: &Path, log: &str) -> (BrokerGuard, u16, u16) {
    let bin = env!("CARGO_BIN_EXE_oxidb-server");
    let logfile = std::fs::File::create(data.join(log)).expect("create log");
    let ready = data.join("ready");
    let _ = std::fs::remove_file(&ready);
    // The child is owned by a guard that kills and waits on Drop.
    #[allow(clippy::zombie_processes)]
    let mut child = Command::new(bin)
        .env("OXIDB_AMQP_PORT", "auto")
        .env("OXIDB_ADDR", "127.0.0.1:0")
        .env("OXIDB_MQTT_PORT", "auto")
        .env("OXIDB_READY_FILE", &ready)
        .env("OXIDB_DATA", data.join("data"))
        .stdout(Stdio::null())
        .stderr(Stdio::from(logfile))
        .spawn()
        .expect("start oxidb-server");
    let amqp = wait_ready(&mut child, &ready, "amqp");
    let mqtt = wait_ready(&mut child, &ready, "mqtt");
    (BrokerGuard(child), amqp, mqtt)
}

const PRELUDE: &str = "
import pika, sys
port = int(sys.argv[1])
params = pika.ConnectionParameters(host='127.0.0.1', port=port)
";

#[test]
fn pika_publishes_and_consumes_with_confirms() {
    if !have_pika() {
        eprintln!(
            "SKIP: pika not vendored (python3 -m pip install --target target/amqp-test-deps pika)"
        );
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), "b.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue='hello')
ch.confirm_delivery()
ch.basic_publish(exchange='', routing_key='hello', body=b'Hello OxiDB!')
m, props, body = ch.basic_get('hello', auto_ack=True)
print('BODY:' + body.decode())
empty = ch.basic_get('hello', auto_ack=True)
print('DRAINED:' + str(empty[0] is None))
conn.close()
"
        ),
    );
    assert!(out.contains("BODY:Hello OxiDB!"), "roundtrip failed: {out}");
    assert!(
        out.contains("DRAINED:True"),
        "queue must be empty after the get: {out}"
    );

    drop(broker);
}

#[test]
fn competing_consumers_each_message_delivered_exactly_once() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), "b.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
import time
c1 = pika.BlockingConnection(params); ch1 = c1.channel()
c2 = pika.BlockingConnection(params); ch2 = c2.channel()
ch1.queue_declare(queue='work')
got1, got2 = [], []
def cb(lst):
    def f(ch, method, props, body):
        lst.append(body.decode()); ch.basic_ack(method.delivery_tag)
    return f
ch1.basic_consume('work', cb(got1))
ch2.basic_consume('work', cb(got2))
pub = pika.BlockingConnection(params); chp = pub.channel(); chp.confirm_delivery()
for i in range(10):
    chp.basic_publish('', 'work', str(i).encode())
end = time.time() + 8
while time.time() < end and len(got1) + len(got2) < 10:
    c1.process_data_events(0.1); c2.process_data_events(0.1)
both = got1 + got2
print('TOTAL:' + str(len(both)))
print('UNIQUE:' + str(len(set(both))))
print('SPLIT:' + str(len(got1)) + '/' + str(len(got2)))
for c in (pub, c1, c2): c.close()
"
        ),
    );
    assert!(out.contains("TOTAL:10"), "all messages must arrive: {out}");
    assert!(
        out.contains("UNIQUE:10"),
        "no message may be delivered twice: {out}"
    );
    assert!(
        out.contains("SPLIT:5/5"),
        "round-robin must split the work evenly: {out}"
    );

    drop(broker);
}

#[test]
fn durable_persistent_messages_survive_sigkill() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (mut broker, port) = start(dir.path(), "b1.log");

    // Publisher confirms on a durable queue: when basic_publish returns, the
    // broker has confirmed, and the confirm promises the message is on disk.
    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue='dq', durable=True)
ch.confirm_delivery()
for i in range(3):
    ch.basic_publish('', 'dq', ('m' + str(i)).encode(),
                     properties=pika.BasicProperties(delivery_mode=2))
print('PUBLISHED')
conn.close()
"
        ),
    );
    assert!(out.contains("PUBLISHED"));

    // No graceful shutdown: the confirm is the only durability promise made.
    broker.0.kill().expect("SIGKILL");
    broker.0.wait().ok();
    let (broker2, port) = start(dir.path(), "b2.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue='dq', durable=True)
for _ in range(3):
    m, props, body = ch.basic_get('dq', auto_ack=True)
    print('GOT:' + body.decode() + ':redelivered=' + str(m.redelivered))
print('EMPTY:' + str(ch.basic_get('dq', auto_ack=True)[0] is None))
conn.close()
"
        ),
    );
    for i in 0..3 {
        assert!(
            out.contains(&format!("GOT:m{i}:redelivered=True")),
            "message m{i} must survive the SIGKILL, flagged redelivered\n{out}\nbroker2 log:\n{}",
            std::fs::read_to_string(dir.path().join("b2.log")).unwrap_or_default()
        );
    }
    assert!(
        out.contains("EMPTY:True"),
        "exactly three messages, no resurrection: {out}"
    );

    drop(broker2);
}

#[test]
fn an_unacked_delivery_requeues_when_its_connection_dies() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), "b.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
c1 = pika.BlockingConnection(params)
ch1 = c1.channel()
ch1.queue_declare(queue='q')
ch1.confirm_delivery()
ch1.basic_publish('', 'q', b'precious')
# Take the delivery WITHOUT acking, then let the connection die.
m, props, body = ch1.basic_get('q', auto_ack=False)
print('FIRST:' + body.decode() + ':redelivered=' + str(m.redelivered))
c1.close()  # channel close requeues the unacked delivery
c2 = pika.BlockingConnection(params)
ch2 = c2.channel()
m2, props2, body2 = ch2.basic_get('q', auto_ack=True)
print('SECOND:' + body2.decode() + ':redelivered=' + str(m2.redelivered))
c2.close()
"
        ),
    );
    assert!(out.contains("FIRST:precious:redelivered=False"), "{out}");
    assert!(
        out.contains("SECOND:precious:redelivered=True"),
        "the unacked message must requeue, flagged redelivered: {out}"
    );

    drop(broker);
}

#[test]
fn direct_exchange_routes_by_key_and_headers_is_refused_by_name() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), "b.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='router', exchange_type='direct')
ch.queue_declare(queue='qa'); ch.queue_declare(queue='qb')
ch.queue_bind('qa', 'router', 'a'); ch.queue_bind('qb', 'router', 'b')
ch.confirm_delivery()
ch.basic_publish('router', 'a', b'for-a')
ch.basic_publish('router', 'b', b'for-b')
print('A:' + ch.basic_get('qa', auto_ack=True)[2].decode())
print('B:' + ch.basic_get('qb', auto_ack=True)[2].decode())
print('A-EMPTY:' + str(ch.basic_get('qa', auto_ack=True)[0] is None))
# The honest refusal: headers exchanges are outside ADR-0016's scope, and
# the error says so.
ch2 = conn.channel()
try:
    ch2.exchange_declare(exchange='h', exchange_type='headers')
    print('HEADERS:accepted')
except pika.exceptions.ChannelClosedByBroker as e:
    print('HEADERS:refused:' + str(e.reply_code))
conn.close()
"
        ),
    );
    assert!(out.contains("A:for-a"), "{out}");
    assert!(out.contains("B:for-b"), "{out}");
    assert!(
        out.contains("A-EMPTY:True"),
        "key 'b' must not reach queue 'qa': {out}"
    );
    assert!(
        out.contains("HEADERS:refused:540"),
        "a headers exchange must be refused with 540 NOT_IMPLEMENTED, not accepted silently: {out}"
    );

    drop(broker);
}

#[test]
fn topic_exchange_matches_wildcards_and_fanout_copies_to_all() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), "b.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='t', exchange_type='topic')
ch.queue_declare(queue='qk'); ch.queue_declare(queue='qall')
ch.queue_bind('qk', 't', 'kern.*'); ch.queue_bind('qall', 't', '#')
ch.confirm_delivery()
ch.basic_publish('t', 'kern.crit', b'kc')
ch.basic_publish('t', 'app.info', b'ai')
print('QK:' + ch.basic_get('qk', auto_ack=True)[2].decode())
print('QK-EMPTY:' + str(ch.basic_get('qk', auto_ack=True)[0] is None))
print('QALL:' + ch.basic_get('qall', auto_ack=True)[2].decode()
             + ',' + ch.basic_get('qall', auto_ack=True)[2].decode())
ch.exchange_declare(exchange='f', exchange_type='fanout')
ch.queue_declare(queue='f1'); ch.queue_declare(queue='f2')
ch.queue_bind('f1', 'f', ''); ch.queue_bind('f2', 'f', '')
ch.basic_publish('f', 'ignored-key', b'copy')
print('F1:' + ch.basic_get('f1', auto_ack=True)[2].decode())
print('F2:' + ch.basic_get('f2', auto_ack=True)[2].decode())
conn.close()
"
        ),
    );
    assert!(out.contains("QK:kc"), "kern.* must match kern.crit: {out}");
    assert!(
        out.contains("QK-EMPTY:True"),
        "kern.* must not match app.info: {out}"
    );
    assert!(
        out.contains("QALL:kc,ai"),
        "# must match everything, in order: {out}"
    );
    assert!(
        out.contains("F1:copy") && out.contains("F2:copy"),
        "fanout must copy to every queue: {out}"
    );

    drop(broker);
}

#[test]
fn prefetch_caps_a_slow_consumer_and_the_rest_flows_to_the_other() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), "b.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
import time
# c1 never acks and has prefetch 1: it must hold exactly one delivery while
# every skipped turn passes to c2 — the work-queue pattern Basic.Qos exists for.
c1 = pika.BlockingConnection(params); ch1 = c1.channel()
c2 = pika.BlockingConnection(params); ch2 = c2.channel()
ch1.queue_declare(queue='work')
ch1.basic_qos(prefetch_count=1)
got1, got2 = [], []
ch1.basic_consume('work', lambda ch, m, p, b: got1.append(b.decode()))
def ack2(ch, m, p, b):
    got2.append(b.decode()); ch.basic_ack(m.delivery_tag)
ch2.basic_consume('work', ack2)
pub = pika.BlockingConnection(params); chp = pub.channel(); chp.confirm_delivery()
for i in range(6):
    chp.basic_publish('', 'work', str(i).encode())
end = time.time() + 8
while time.time() < end and len(got1) + len(got2) < 6:
    c1.process_data_events(0.1); c2.process_data_events(0.1)
print('STUCK:' + str(len(got1)))
print('FLOWED:' + str(len(got2)))
for c in (pub, c1, c2): c.close()
"
        ),
    );
    assert!(
        out.contains("STUCK:1"),
        "prefetch=1 with no ack must hold at one delivery: {out}"
    );
    assert!(
        out.contains("FLOWED:5"),
        "the capped consumer's turns must flow to the other: {out}"
    );

    drop(broker);
}

#[test]
fn the_mqtt_amqp_bridge_carries_both_directions() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port, mqtt_port) = start_with_mqtt(dir.path(), "b.log");

    // One script, both protocols: a raw-bytes MQTT 3.1.1 client (CONNECT,
    // SUBSCRIBE, PUBLISH — small enough to handroll) plus pika. The thesis
    // under test is ADR-0016 Phase 3: a sensor publishes MQTT and a worker
    // pool consumes AMQP from one binary, and back.
    let out = py(
        port,
        &format!(
            "{PRELUDE}
import socket, struct, time
mport = {mqtt_port}

def recvn(s, n):
    d = b''
    while len(d) < n:
        c = s.recv(n - len(d))
        assert c, 'socket closed'
        d += c
    return d

ms = socket.create_connection(('127.0.0.1', mport)); ms.settimeout(10)
cid = b'bridge-test'
vh = bytes([0,4]) + b'MQTT' + bytes([4,2,0,60]) + struct.pack('>H', len(cid)) + cid
ms.sendall(bytes([16, len(vh)]) + vh)
assert recvn(ms, 4)[0] == 32, 'no CONNACK'

f = b'alerts/+'
sp = bytes([0,1]) + struct.pack('>H', len(f)) + f + bytes([0])
ms.sendall(bytes([130, len(sp)]) + sp)
assert recvn(ms, 5)[0] == 144, 'no SUBACK'

conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue='workers')
ch.queue_bind('workers', 'amq.topic', 'sensors.#')  # amq.topic is pre-declared
ch.confirm_delivery()

# MQTT -> AMQP: publish over MQTT, consume over AMQP.
t = b'sensors/floor1/temp'; m = b'21.5'
pp = struct.pack('>H', len(t)) + t + m
ms.sendall(bytes([48, len(pp)]) + pp)
got = None
end = time.time() + 8
while time.time() < end and got is None:
    frame = ch.basic_get('workers', auto_ack=True)
    if frame[0] is not None:
        got = frame
    else:
        time.sleep(0.05)
assert got is not None, 'MQTT publish never reached the AMQP queue'
print('IN:' + got[0].routing_key + ':' + got[2].decode())

# AMQP -> MQTT: publish to amq.topic, receive over MQTT.
ch.basic_publish('amq.topic', 'alerts.fire', b'evacuate')
h = recvn(ms, 1)[0]
assert h >> 4 == 3, 'expected an MQTT PUBLISH, got packet type %d' % (h >> 4)
n = recvn(ms, 1)[0]
d = recvn(ms, n)
tl = struct.unpack('>H', d[0:2])[0]
print('OUT:' + d[2:2+tl].decode() + ':' + d[2+tl:].decode())
conn.close(); ms.close()
"
        ),
    );
    assert!(
        out.contains("IN:sensors.floor1.temp:21.5"),
        "MQTT -> AMQP must map '/' to '.': {out}"
    );
    assert!(
        out.contains("OUT:alerts/fire:evacuate"),
        "AMQP -> MQTT must map '.' to '/': {out}"
    );

    drop(broker);
}

#[test]
fn a_mandatory_unroutable_publish_comes_back_as_basic_return() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (broker, port) = start(dir.path(), "b.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.confirm_delivery()
try:
    ch.basic_publish('', 'no-such-queue', b'boomerang', mandatory=True)
    print('MANDATORY:accepted')
except pika.exceptions.UnroutableError as e:
    m = e.messages[0]
    print('MANDATORY:returned:' + str(m.method.reply_code) + ':' + m.body.decode())
# A routable mandatory publish must NOT return.
ch.queue_declare(queue='exists')
ch.basic_publish('', 'exists', b'lands', mandatory=True)
print('ROUTABLE:' + ch.basic_get('exists', auto_ack=True)[2].decode())
conn.close()
"
        ),
    );
    assert!(
        out.contains("MANDATORY:returned:312:boomerang"),
        "an unroutable mandatory publish must come back as Basic.Return 312 with its body: {out}"
    );
    assert!(
        out.contains("ROUTABLE:lands"),
        "a routable mandatory publish must deliver normally: {out}"
    );

    drop(broker);
}

#[test]
fn an_acked_durable_message_does_not_resurrect_after_sigkill() {
    if !have_pika() {
        eprintln!("SKIP: pika not vendored");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (mut broker, port) = start(dir.path(), "b1.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue='dq', durable=True)
ch.confirm_delivery()
ch.basic_publish('', 'dq', b'consume-me', properties=pika.BasicProperties(delivery_mode=2))
m, props, body = ch.basic_get('dq', auto_ack=False)
ch.basic_ack(m.delivery_tag)
print('ACKED:' + body.decode())
conn.close()
"
        ),
    );
    assert!(out.contains("ACKED:consume-me"), "{out}");

    // The ack deleted the durable record; a crash must not bring it back — an
    // at-least-once queue must not become at-least-twice-after-every-crash.
    broker.0.kill().expect("SIGKILL");
    drop(broker);
    let (broker2, port) = start(dir.path(), "b2.log");

    let out = py(
        port,
        &format!(
            "{PRELUDE}
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue='dq', durable=True)
print('EMPTY:' + str(ch.basic_get('dq', auto_ack=True)[0] is None))
conn.close()
"
        ),
    );
    assert!(
        out.contains("EMPTY:True"),
        "the acked message must stay consumed across the crash: {out}\nbroker2 log:\n{}",
        std::fs::read_to_string(dir.path().join("b2.log")).unwrap_or_default()
    );
    drop(broker2);
}
