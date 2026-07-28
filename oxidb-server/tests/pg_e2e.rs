//! PostgreSQL end-to-end, driven by psycopg — the real driver, unmodified.
//!
//! `pg_wire.rs` pins the bytes; this pins the claim that matters: code written
//! for PostgreSQL points at OxiDB and works. psycopg uses the extended query
//! protocol for everything, sends its own opening `SET`s, and raises typed
//! exceptions off the SQLSTATEs — so a driver that is happy here is evidence
//! about all three.
//!
//! psycopg is vendored into `target/pg-test-deps` (pip --target, no system
//! pollution) and loaded via PYTHONPATH. The availability check runs an actual
//! `import psycopg` and reads the exit code, which is meaningful for an import
//! (unlike a `--help` exit code — the have_mosquitto lesson, applied).

use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn psycopg_path() -> PathBuf {
    repo_root().join("target/pg-test-deps")
}

fn have_psycopg() -> bool {
    Command::new("python3")
        .args(["-c", "import psycopg"])
        .env("PYTHONPATH", psycopg_path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Run a python snippet against the server; returns its stdout. The snippet
/// gets the port as argv[1]. Stderr passes through so a psycopg traceback
/// lands in the test output instead of vanishing.
fn py(port: u16, script: &str) -> String {
    let out = Command::new("python3")
        .args(["-c", script, &port.to_string()])
        .env("PYTHONPATH", psycopg_path())
        .output()
        .expect("run python3");
    assert!(
        out.status.success(),
        "psycopg client failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    26000 + (std::process::id() % 89) as u16 * 12 + NEXT.fetch_add(2, Ordering::SeqCst)
}

/// A server that dies with its guard — a panic that leaked the process would
/// leave it squatting the port band for every later run.
struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(port: u16, data: &Path) -> ServerGuard {
    let bin = env!("CARGO_BIN_EXE_oxidb-server");
    let logfile = std::fs::File::create(data.join("server.log")).expect("create log");
    // The child is owned by a guard that kills and waits on Drop.
    #[allow(clippy::zombie_processes)]
    let child = Command::new(bin)
        .env("OXIDB_PG_PORT", port.to_string())
        .env("OXIDB_ADDR", format!("127.0.0.1:{}", port + 1))
        .env("OXIDB_DATA", data)
        .env("OXIDB_SQL", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(logfile))
        .spawn()
        .expect("start oxidb-server");
    for _ in 0..600 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return ServerGuard(child);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("PostgreSQL listener never came up on {port}");
}

/// The connection line every snippet opens with.
const CONNECT: &str = r#"
import sys, psycopg
port = int(sys.argv[1])
conn = psycopg.connect(f"host=127.0.0.1 port={port} user=admin dbname=oxidb", autocommit=True)
"#;

#[test]
fn psycopg_round_trips_crud() {
    if !have_psycopg() {
        eprintln!("psycopg not available — skipping (see the module docs to vendor it)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let _g = start(port, dir.path());

    let out = py(
        port,
        &format!(
            r#"{CONNECT}
cur = conn.cursor()
cur.execute("CREATE TABLE people (id INT PRIMARY KEY, name TEXT, score DOUBLE)")
cur.execute("INSERT INTO people VALUES (1, 'ada', 99.5), (2, 'bob', 12.0)")
print("inserted", cur.rowcount)

cur.execute("SELECT id, name, score FROM people ORDER BY id")
print("cols", [d.name for d in cur.description])
for row in cur.fetchall():
    print("row", row)

# Types survive the round trip as Python types, not strings.
cur.execute("SELECT id, score FROM people WHERE id = 1")
id_, score = cur.fetchone()
print("types", type(id_).__name__, type(score).__name__)
"#
        ),
    );

    assert!(out.contains("inserted 2"), "{out}");
    assert!(out.contains("cols ['id', 'name', 'score']"), "{out}");
    assert!(out.contains("row (1, 'ada', 99.5)"), "{out}");
    assert!(out.contains("row (2, 'bob', 12.0)"), "{out}");
    assert!(out.contains("types int float"), "{out}");
}

#[test]
fn psycopg_binds_parameters_server_side() {
    if !have_psycopg() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let _g = start(port, dir.path());

    let out = py(
        port,
        &format!(
            r#"{CONNECT}
cur = conn.cursor()
cur.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, flag BOOL)")
# psycopg rewrites %s to $1.. and binds over the extended protocol.
cur.execute("INSERT INTO t VALUES (%s, %s, %s)", (1, "ada", True))
cur.execute("INSERT INTO t VALUES (%s, %s, %s)", (2, None, False))
cur.execute("SELECT id, name, flag FROM t WHERE id = %s", (1,))
print("one", cur.fetchone())
cur.execute("SELECT name FROM t WHERE id = %s", (2,))
print("null", cur.fetchone())
# executemany drives the same prepared statement repeatedly.
cur.executemany("INSERT INTO t VALUES (%s, %s, %s)", [(3, "c", True), (4, "d", False)])
cur.execute("SELECT COUNT(*) FROM t")
print("count", cur.fetchone()[0])
"#
        ),
    );

    assert!(out.contains("one (1, 'ada', True)"), "{out}");
    assert!(out.contains("null (None,)"), "{out}");
    assert!(out.contains("count 4"), "{out}");
}

#[test]
fn psycopg_maps_errors_to_typed_exceptions() {
    // This is the SQLSTATE mapping's real test: psycopg picks the exception
    // class off the code alone, so UniqueViolation proves 23505 arrived.
    if !have_psycopg() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let _g = start(port, dir.path());

    let out = py(
        port,
        &format!(
            r#"{CONNECT}
cur = conn.cursor()
cur.execute("CREATE TABLE t (id INT PRIMARY KEY)")
cur.execute("INSERT INTO t VALUES (1)")
try:
    cur.execute("INSERT INTO t VALUES (1)")
except psycopg.errors.UniqueViolation as e:
    print("unique", e.diag.sqlstate)
try:
    cur.execute("SELECT * FROM nope")
except psycopg.errors.UndefinedTable as e:
    print("table", e.diag.sqlstate)
try:
    cur.execute("SELECT nope FROM t")
except psycopg.errors.UndefinedColumn as e:
    print("column", e.diag.sqlstate)
"#
        ),
    );

    assert!(out.contains("unique 23505"), "{out}");
    assert!(out.contains("table 42P01"), "{out}");
    assert!(out.contains("column 42703"), "{out}");
}

#[test]
fn psycopg_transactions_commit_and_roll_back() {
    if !have_psycopg() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let _g = start(port, dir.path());

    let out = py(
        port,
        r#"
import sys, psycopg
port = int(sys.argv[1])
conn = psycopg.connect(f"host=127.0.0.1 port={port} user=admin dbname=oxidb", autocommit=True)
conn.execute("CREATE TABLE t (id INT PRIMARY KEY)")

# psycopg's transaction block: commits on exit...
conn.autocommit = False
with conn.transaction():
    conn.execute("INSERT INTO t VALUES (1)")
print("after commit", conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])

# ...and rolls back when the body raises.
try:
    with conn.transaction():
        conn.execute("INSERT INTO t VALUES (2)")
        raise RuntimeError("abort")
except RuntimeError:
    pass
print("after rollback", conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])

# A failed statement inside a transaction must poison it, and recovery must
# work — this is what the 'E' transaction-status byte drives.
try:
    with conn.transaction():
        conn.execute("INSERT INTO t VALUES (1)")
except psycopg.errors.UniqueViolation:
    print("poisoned then recovered")
print("final", conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])
"#,
    );

    assert!(out.contains("after commit 1"), "{out}");
    assert!(out.contains("after rollback 1"), "{out}");
    assert!(out.contains("poisoned then recovered"), "{out}");
    assert!(out.contains("final 1"), "{out}");
}

#[test]
fn psycopg_reads_the_server_it_connected_to() {
    if !have_psycopg() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let _g = start(port, dir.path());

    let out = py(
        port,
        &format!(
            r#"{CONNECT}
print("version", conn.execute("SELECT version()").fetchone()[0])
print("pgver", conn.info.server_version)
print("db", conn.info.dbname)
# fetchmany drives Execute with a row limit, which suspends and resumes the
# portal (a *named* server-side cursor needs DECLARE CURSOR, which this server
# does not implement — see the refusal test below).
conn.execute("CREATE TABLE t (id INT PRIMARY KEY)")
conn.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
cur = conn.cursor()
cur.execute("SELECT id FROM t ORDER BY id")
print("first two", [r[0] for r in cur.fetchmany(2)])
print("rest", [r[0] for r in cur.fetchall()])

# What is not implemented says so, rather than answering wrongly.
try:
    with conn.cursor(name="c1") as sc:
        sc.execute("SELECT id FROM t")
except psycopg.errors.FeatureNotSupported as e:
    print("cursor refused", e.diag.sqlstate)
except Exception as e:
    print("cursor refused", type(e).__name__)
"#
        ),
    );

    assert!(out.contains("OxiDB"), "{out}");
    assert!(out.contains("db oxidb"), "{out}");
    assert!(out.contains("first two [1, 2]"), "{out}");
    assert!(out.contains("rest [3, 4, 5]"), "{out}");
    assert!(out.contains("cursor refused"), "{out}");
}
