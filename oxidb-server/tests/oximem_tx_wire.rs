//! Wire-level tests for OxiMem MULTI/EXEC/WATCH: spawns the real
//! `oxidb-server` binary and speaks RESP over TCP, so the per-connection
//! Session routing in main.rs (single-command path AND the pipelined-batch
//! path) is exercised end to end — not just the lib-level dispatcher.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Server process killed on drop, so a failing test can't leak it.
struct ServerGuard {
    child: Child,
    _dir: tempfile::TempDir,
    oximem_port: u16,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_server() -> ServerGuard {
    let dir = tempfile::tempdir().unwrap();
    let doc_port = free_port();
    let oximem_port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_oxidb-server"))
        .env("OXIDB_DATA", dir.path())
        .env("OXIDB_ADDR", format!("127.0.0.1:{doc_port}"))
        .env("OXIDB_OXIMEM_PORT", oximem_port.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn oxidb-server");
    let guard = ServerGuard {
        child,
        _dir: dir,
        oximem_port,
    };
    // Wait for the OxiMem listener to come up.
    // 60s, not 15: the full suite starts dozens of fsync-every-commit servers
    // concurrently, and under that disk load a cold engine open can genuinely
    // take tens of seconds. A readiness deadline is not a performance
    // assertion; a tight one just converts contention into flakes.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        // A completed PING/PONG, not merely a successful connect. The listener
        // accepts before it can serve, so "the port answers" let the test open
        // its real connection into a half-ready server and occasionally get it
        // reset — a flake that looked like a protocol bug and was not one.
        if ping_ok(guard.oximem_port) {
            return guard;
        }
        assert!(Instant::now() < deadline, "oximem port never answered PING");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// One RESP round trip on a throwaway connection: proof the server is serving.
fn ping_ok(port: u16) -> bool {
    let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    if stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .is_err()
    {
        return false;
    }
    let mut conn = BufReader::new(stream);
    if conn.get_mut().write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
        return false;
    }
    let mut line = String::new();
    matches!(conn.read_line(&mut line), Ok(n) if n > 0) && line.starts_with("+PONG")
}

// ---- tiny RESP client ------------------------------------------------------

fn connect(port: u16) -> BufReader<TcpStream> {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    BufReader::new(s)
}

fn encode(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend(format!("${}\r\n{}\r\n", p.len(), p).into_bytes());
    }
    out
}

fn send(conn: &mut BufReader<TcpStream>, parts: &[&str]) {
    conn.get_mut().write_all(&encode(parts)).unwrap();
    conn.get_mut().flush().unwrap();
}

/// Decoded RESP reply, just enough for assertions.
#[derive(Debug, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(String),
    Null,
    Array(Vec<Reply>),
    NullArray,
}

fn read_reply(conn: &mut BufReader<TcpStream>) -> Reply {
    let mut line = String::new();
    conn.read_line(&mut line).unwrap();
    let line = line.trim_end();
    let (tag, rest) = line.split_at(1);
    match tag {
        "+" => Reply::Simple(rest.to_string()),
        "-" => Reply::Error(rest.to_string()),
        ":" => Reply::Int(rest.parse().unwrap()),
        "$" => {
            let n: i64 = rest.parse().unwrap();
            if n < 0 {
                return Reply::Null;
            }
            let mut buf = vec![0u8; n as usize + 2];
            conn.read_exact(&mut buf).unwrap();
            Reply::Bulk(String::from_utf8_lossy(&buf[..n as usize]).to_string())
        }
        "*" => {
            let n: i64 = rest.parse().unwrap();
            if n < 0 {
                return Reply::NullArray;
            }
            Reply::Array((0..n).map(|_| read_reply(conn)).collect())
        }
        other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
    }
}

fn cmd(conn: &mut BufReader<TcpStream>, parts: &[&str]) -> Reply {
    send(conn, parts);
    read_reply(conn)
}

// ---- tests -----------------------------------------------------------------

/// MULTI/EXEC over the single-command wire path: queue, execute, verify.
#[test]
fn wire_multi_exec_roundtrip() {
    let srv = spawn_server();
    let mut c = connect(srv.oximem_port);

    assert_eq!(cmd(&mut c, &["MULTI"]), Reply::Simple("OK".into()));
    assert_eq!(
        cmd(&mut c, &["SET", "k", "1"]),
        Reply::Simple("QUEUED".into())
    );
    assert_eq!(cmd(&mut c, &["INCR", "k"]), Reply::Simple("QUEUED".into()));
    match cmd(&mut c, &["EXEC"]) {
        Reply::Array(v) => {
            assert_eq!(v.len(), 2);
            assert_eq!(v[1], Reply::Int(2));
        }
        other => panic!("expected array, got {other:?}"),
    }
    assert_eq!(cmd(&mut c, &["GET", "k"]), Reply::Bulk("2".into()));
}

/// WATCH conflict across two real connections: the second connection's write
/// aborts the first's EXEC (nil), and the transaction must not run.
#[test]
fn wire_watch_conflict_across_connections() {
    let srv = spawn_server();
    let mut a = connect(srv.oximem_port);
    let mut b = connect(srv.oximem_port);

    assert_eq!(
        cmd(&mut a, &["SET", "bal", "100"]),
        Reply::Simple("OK".into())
    );
    assert_eq!(cmd(&mut a, &["WATCH", "bal"]), Reply::Simple("OK".into()));
    // Connection B races us to the balance.
    assert_eq!(
        cmd(&mut b, &["SET", "bal", "999"]),
        Reply::Simple("OK".into())
    );
    assert_eq!(cmd(&mut a, &["MULTI"]), Reply::Simple("OK".into()));
    assert_eq!(
        cmd(&mut a, &["SET", "bal", "50"]),
        Reply::Simple("QUEUED".into())
    );
    assert_eq!(cmd(&mut a, &["EXEC"]), Reply::NullArray); // aborted
    assert_eq!(cmd(&mut a, &["GET", "bal"]), Reply::Bulk("999".into()));
}

/// A whole transaction sent as ONE pipelined burst must route through the
/// stateful path in main.rs (not the lock-coalesced fast path) and work.
#[test]
fn wire_pipelined_transaction_batch() {
    let srv = spawn_server();
    let mut c = connect(srv.oximem_port);

    let mut burst = Vec::new();
    burst.extend(encode(&["MULTI"]));
    burst.extend(encode(&["SET", "a", "5"]));
    burst.extend(encode(&["INCRBY", "a", "10"]));
    burst.extend(encode(&["EXEC"]));
    c.get_mut().write_all(&burst).unwrap();
    c.get_mut().flush().unwrap();

    assert_eq!(read_reply(&mut c), Reply::Simple("OK".into()));
    assert_eq!(read_reply(&mut c), Reply::Simple("QUEUED".into()));
    assert_eq!(read_reply(&mut c), Reply::Simple("QUEUED".into()));
    match read_reply(&mut c) {
        Reply::Array(v) => {
            assert_eq!(v.len(), 2);
            assert_eq!(v[1], Reply::Int(15));
        }
        other => panic!("expected array, got {other:?}"),
    }
    assert_eq!(cmd(&mut c, &["GET", "a"]), Reply::Bulk("15".into()));
}

/// Ordinary (non-tx) pipelines must still take the fast coalesced path and
/// return correct per-command replies — and their writes must be visible to a
/// WATCH on another connection (version bumps in the coalesced path).
#[test]
fn wire_plain_pipeline_still_works_and_bumps_watch() {
    let srv = spawn_server();
    let mut a = connect(srv.oximem_port);
    let mut b = connect(srv.oximem_port);

    assert_eq!(cmd(&mut b, &["SET", "w", "1"]), Reply::Simple("OK".into()));
    assert_eq!(cmd(&mut b, &["WATCH", "w"]), Reply::Simple("OK".into()));

    // Connection A fires a coalesced same-command burst that touches "w".
    let mut burst = Vec::new();
    burst.extend(encode(&["SET", "w", "2"]));
    burst.extend(encode(&["SET", "x", "9"]));
    a.get_mut().write_all(&burst).unwrap();
    a.get_mut().flush().unwrap();
    assert_eq!(read_reply(&mut a), Reply::Simple("OK".into()));
    assert_eq!(read_reply(&mut a), Reply::Simple("OK".into()));

    // B's transaction on the watched key must now abort.
    assert_eq!(cmd(&mut b, &["MULTI"]), Reply::Simple("OK".into()));
    assert_eq!(
        cmd(&mut b, &["SET", "w", "3"]),
        Reply::Simple("QUEUED".into())
    );
    assert_eq!(cmd(&mut b, &["EXEC"]), Reply::NullArray);
    assert_eq!(cmd(&mut b, &["GET", "w"]), Reply::Bulk("2".into()));
}

/// EXECABORT over the wire: an unknown command queued in MULTI poisons the
/// transaction.
#[test]
fn wire_execabort_on_unknown_command() {
    let srv = spawn_server();
    let mut c = connect(srv.oximem_port);

    assert_eq!(cmd(&mut c, &["MULTI"]), Reply::Simple("OK".into()));
    assert_eq!(
        cmd(&mut c, &["SET", "z", "1"]),
        Reply::Simple("QUEUED".into())
    );
    match cmd(&mut c, &["NOPE", "z"]) {
        Reply::Error(_) => {}
        other => panic!("expected error, got {other:?}"),
    }
    match cmd(&mut c, &["EXEC"]) {
        Reply::Error(e) => assert!(e.contains("EXECABORT"), "got {e}"),
        other => panic!("expected EXECABORT, got {other:?}"),
    }
    assert_eq!(cmd(&mut c, &["GET", "z"]), Reply::Null);
}

/// EVAL over the wire: KEYS/ARGV + redis.call, atomic result.
#[test]
fn wire_eval_roundtrip() {
    let srv = spawn_server();
    let mut c = connect(srv.oximem_port);
    assert_eq!(
        cmd(&mut c, &["SET", "bal", "100"]),
        Reply::Simple("OK".into())
    );
    let script = "local b = tonumber(redis.call('GET', KEYS[1])) \
                  if b >= tonumber(ARGV[1]) then \
                    redis.call('INCRBYFLOAT', KEYS[1], '-' .. ARGV[1]) return 1 \
                  end return 0";
    assert_eq!(
        cmd(&mut c, &["EVAL", script, "1", "bal", "30"]),
        Reply::Int(1)
    );
    assert_eq!(cmd(&mut c, &["GET", "bal"]), Reply::Bulk("70".into()));
}

/// PSUBSCRIBE over the wire: pattern match delivers a pmessage frame.
#[test]
fn wire_psubscribe_pmessage() {
    let srv = spawn_server();
    let mut sub = connect(srv.oximem_port);
    send(&mut sub, &["PSUBSCRIBE", "news.*"]);
    match read_reply(&mut sub) {
        Reply::Array(v) => assert_eq!(v[0], Reply::Bulk("psubscribe".into())),
        o => panic!("{o:?}"),
    }
    let mut publ = connect(srv.oximem_port);
    assert_eq!(
        cmd(&mut publ, &["PUBLISH", "news.tech", "hi"]),
        Reply::Int(1)
    );
    match read_reply(&mut sub) {
        Reply::Array(v) => {
            assert_eq!(v[0], Reply::Bulk("pmessage".into()));
            assert_eq!(v[1], Reply::Bulk("news.*".into()));
            assert_eq!(v[2], Reply::Bulk("news.tech".into()));
            assert_eq!(v[3], Reply::Bulk("hi".into()));
        }
        o => panic!("{o:?}"),
    }
}

/// BLPOP blocks on one connection until another pushes.
#[test]
fn wire_blpop_cross_connection() {
    let srv = spawn_server();
    let port = srv.oximem_port;
    let waiter = std::thread::spawn(move || {
        let mut c = connect(port);
        cmd(&mut c, &["BLPOP", "jobs", "5"])
    });
    std::thread::sleep(Duration::from_millis(300));
    let mut p = connect(srv.oximem_port);
    assert_eq!(cmd(&mut p, &["RPUSH", "jobs", "j1"]), Reply::Int(1));
    match waiter.join().unwrap() {
        Reply::Array(v) => {
            assert_eq!(v[0], Reply::Bulk("jobs".into()));
            assert_eq!(v[1], Reply::Bulk("j1".into()));
        }
        o => panic!("{o:?}"),
    }
}
