//! Wire-level SQL backup/restore under concurrent load.
//!
//! Spawns the real binary with the SQL engine on, drives it from 10 connections
//! that read and write continuously, and takes several backups + restores while
//! that load runs. Integrity is checked with a per-worker no-gap invariant: each
//! worker inserts rows `n = 1, 2, 3, …` sequentially (each only after the
//! previous is durably ACKed), so in ANY consistent snapshot a worker's rows are
//! a contiguous prefix `{1..k}` — `MIN(n) == 1` and `MAX(n) == COUNT(n)`. A torn
//! backup would show a gap (`MAX > COUNT`). Every restored snapshot and the live
//! database are verified against it.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const NWORKERS: usize = 10;

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

/// Spawn a server with the SQL engine enabled. `sql_data`, when given, points
/// the default-db SQL engine at an existing directory (used to open a restored
/// backup for verification).
fn spawn_sql(sql_data: Option<&Path>) -> Guard {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_oxidb-server"));
    cmd.env("OXIDB_SQL", "1")
        .env("OXIDB_DATA", dir.path().join("data"))
        .env("OXIDB_ADDR", "127.0.0.1:0")
        .env("OXIDB_READY_FILE", &ready)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    match sql_data {
        Some(p) => {
            cmd.env("OXIDB_SQL_DATA", p);
        }
        None => {
            cmd.env_remove("OXIDB_SQL_DATA");
        }
    }
    let mut child = cmd.spawn().unwrap();
    // Read the kernel-assigned OxiWire port back from the ready file — see
    // pg_wire.rs for why probing a chosen port is not a readiness check.
    let deadline = Instant::now() + Duration::from_secs(20);
    let port: u16 = loop {
        if let Ok(body) = std::fs::read_to_string(&ready) {
            let addr = body
                .lines()
                .find_map(|l| l.strip_prefix("addr="))
                .expect("ready file names the main listener");
            break addr
                .rsplit(':')
                .next()
                .and_then(|p| p.parse().ok())
                .expect("addr ends in a port");
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited before becoming ready: {status}");
        }
        assert!(Instant::now() < deadline, "server never became ready");
        std::thread::sleep(Duration::from_millis(50));
    };
    Guard {
        child,
        _dir: dir,
        port,
    }
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    s
}

/// One length-prefixed (4-byte LE) JSON request/response round trip.
fn call(s: &mut TcpStream, req: &Value) -> Value {
    let body = serde_json::to_vec(req).unwrap();
    s.write_all(&(body.len() as u32).to_le_bytes()).unwrap();
    s.write_all(&body).unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).unwrap();
    let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut buf).unwrap();
    serde_json::from_slice(&buf).unwrap()
}

fn sql(s: &mut TcpStream, q: &str) -> Value {
    call(s, &json!({"engine": "sql", "cmd": "sql", "sql": q}))
}

/// Rows of a SELECT reply (`{ok, data:[{rows:[...]}]}`).
fn rows(resp: &Value) -> Vec<Vec<Value>> {
    assert!(resp["ok"].as_bool().unwrap_or(false), "query error: {resp}");
    resp["data"][0]["rows"]
        .as_array()
        .map(|rs| rs.iter().map(|r| r.as_array().unwrap().clone()).collect())
        .unwrap_or_default()
}

/// Verify the per-worker no-gap invariant on a connection; return the row total.
fn check_no_gaps(s: &mut TcpStream, label: &str) -> i64 {
    let r = sql(
        s,
        "SELECT worker, COUNT(n), MIN(n), MAX(n) FROM t GROUP BY worker",
    );
    let mut total = 0;
    for row in rows(&r) {
        let (w, cnt, mn, mx) = (
            row[0].as_i64().unwrap(),
            row[1].as_i64().unwrap(),
            row[2].as_i64().unwrap(),
            row[3].as_i64().unwrap(),
        );
        total += cnt;
        assert_eq!(
            mn, 1,
            "{label}: worker {w} MIN(n)={mn} != 1 (missing prefix)"
        );
        assert_eq!(
            mx, cnt,
            "{label}: worker {w} MAX(n)={mx} != COUNT={cnt} — GAP, torn snapshot!"
        );
    }
    total
}

#[test]
fn sql_backup_restore_under_10_connection_load() {
    let server = spawn_sql(None);
    let port = server.port;

    {
        let mut c = connect(port);
        assert!(
            sql(
                &mut c,
                "CREATE TABLE t (id INT PRIMARY KEY, worker INT, n INT)"
            )["ok"]
                .as_bool()
                .unwrap()
        );
    }

    // 10 workers hammering inserts (+ interleaved reads) until stopped.
    let stop = Arc::new(AtomicBool::new(false));
    let counts: Arc<Vec<AtomicUsize>> =
        Arc::new((0..NWORKERS).map(|_| AtomicUsize::new(0)).collect());
    let mut handles = Vec::new();
    for w in 0..NWORKERS {
        let (stop, counts) = (stop.clone(), counts.clone());
        handles.push(std::thread::spawn(move || {
            let mut c = connect(port);
            let mut n = 0i64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let gid = w as i64 * 10_000_000 + n;
                let r = sql(&mut c, &format!("INSERT INTO t VALUES ({gid}, {w}, {n})"));
                if !r["ok"].as_bool().unwrap_or(false) {
                    n -= 1;
                    continue;
                }
                counts[w].store(n as usize, Ordering::Relaxed);
                if n % 4 == 0 {
                    let _ = sql(&mut c, &format!("SELECT n FROM t WHERE id = {gid}"));
                    let _ = sql(&mut c, "SELECT COUNT(*) FROM t");
                }
            }
        }));
    }

    // Take backups + restores *while the load runs*.
    let mut coord = connect(port);
    std::thread::sleep(Duration::from_millis(400)); // let the load ramp up
    let mut snapshots: Vec<tempfile::TempDir> = Vec::new();
    for i in 0..3 {
        let arc_dir = tempfile::tempdir().unwrap();
        let arc = arc_dir.path().join(format!("bk{i}.tar.gz"));
        let restored = tempfile::tempdir().unwrap(); // empty — restore accepts
        let rb = call(
            &mut coord,
            &json!({"engine":"sql","cmd":"backup","path": arc.to_str().unwrap()}),
        );
        assert!(rb["ok"].as_bool().unwrap(), "backup {i} failed: {rb}");
        let rr = call(
            &mut coord,
            &json!({"engine":"sql","cmd":"restore",
                    "archive": arc.to_str().unwrap(),
                    "target": restored.path().to_str().unwrap()}),
        );
        assert!(rr["ok"].as_bool().unwrap(), "restore {i} failed: {rr}");
        // The archive is consumed by the restore above; only the restored dir
        // is kept, for verification after the load stops.
        snapshots.push(restored);
        std::thread::sleep(Duration::from_millis(250));
    }

    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let recorded: i64 = counts
        .iter()
        .map(|a| a.load(Ordering::Relaxed) as i64)
        .sum();
    assert!(recorded > 0, "workers made no progress");

    // 1) The live database is consistent and complete.
    let live = check_no_gaps(&mut coord, "LIVE");
    assert_eq!(live, recorded, "live total {live} != recorded {recorded}");

    // 2) Every backup taken under load restores to a consistent snapshot.
    for (i, restored) in snapshots.iter().enumerate() {
        let v = spawn_sql(Some(restored.path()));
        let mut vc = connect(v.port);
        let snap = check_no_gaps(&mut vc, &format!("RESTORE#{i}"));
        assert!(
            snap <= recorded,
            "restore#{i} has {snap} rows, more than the {recorded} ever inserted"
        );
        assert!(snap > 0, "restore#{i} unexpectedly empty");
    }
}
