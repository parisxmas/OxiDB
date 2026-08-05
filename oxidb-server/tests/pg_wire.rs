//! Wire-level PostgreSQL v3 tests: spawn the real binary and speak raw
//! protocol bytes at it — startup, SCRAM, simple and extended query, the
//! transaction-status byte, SQLSTATEs, and the catalog interception without
//! which no real client completes a connection.
//!
//! No client library is involved on purpose: these pin the bytes. `pg_e2e.rs`
//! pins the other claim — that a real driver, unmodified, is happy with them.

use std::io::{Read, Write};
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

/// Bounds how many servers boot at the same time.
///
/// Every test gets its own server for isolation, and `cargo test` runs them in
/// parallel — so a whole file's worth can be starting at once, and on a busy
/// machine one loses the race against its own startup deadline. Only the
/// *boot* waits here, so tests still overlap; they just stop stampeding.
static BOOT_GATE: (std::sync::Mutex<usize>, std::sync::Condvar) =
    (std::sync::Mutex::new(0), std::sync::Condvar::new());
const MAX_CONCURRENT_BOOTS: usize = 4;

struct BootPermit;

impl BootPermit {
    fn acquire() -> BootPermit {
        let (lock, cv) = &BOOT_GATE;
        let mut in_flight = lock.lock().unwrap();
        while *in_flight >= MAX_CONCURRENT_BOOTS {
            in_flight = cv.wait(in_flight).unwrap();
        }
        *in_flight += 1;
        BootPermit
    }
}

impl Drop for BootPermit {
    fn drop(&mut self) {
        let (lock, cv) = &BOOT_GATE;
        *lock.lock().unwrap() -= 1;
        cv.notify_one();
    }
}

fn spawn_with(envs: &[(&str, &str)]) -> Guard {
    let _permit = BootPermit::acquire();
    let dir = tempfile::tempdir().unwrap();
    let doc = free_port();
    let pg = free_port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_oxidb-server"));
    cmd.env("OXIDB_DATA", dir.path())
        .env("OXIDB_ADDR", format!("127.0.0.1:{doc}"))
        .env("OXIDB_PG_PORT", pg.to_string())
        .env("OXIDB_SQL", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let child = cmd.spawn().unwrap();
    let g = Guard {
        child,
        _dir: dir,
        port: pg,
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", g.port)).is_err() {
        assert!(Instant::now() < deadline, "pg port never opened");
        std::thread::sleep(Duration::from_millis(100));
    }
    g
}

fn spawn() -> Guard {
    spawn_with(&[])
}

// ── a minimal client ────────────────────────────────────────────────────────

struct Client {
    sock: TcpStream,
}

/// One backend message: its tag and body.
#[derive(Debug, Clone)]
struct Msg {
    tag: u8,
    body: Vec<u8>,
}

impl Msg {
    fn cstring_at(&self, mut pos: usize) -> (String, usize) {
        let end = self.body[pos..].iter().position(|&b| b == 0).unwrap() + pos;
        let s = String::from_utf8_lossy(&self.body[pos..end]).into_owned();
        pos = end + 1;
        (s, pos)
    }

    /// `ErrorResponse` field by its type byte (`C` = SQLSTATE, `M` = message).
    fn field(&self, kind: u8) -> Option<String> {
        let mut pos = 0;
        while pos < self.body.len() && self.body[pos] != 0 {
            let k = self.body[pos];
            let (v, next) = self.cstring_at(pos + 1);
            if k == kind {
                return Some(v);
            }
            pos = next;
        }
        None
    }

    /// `DataRow` cells; `None` is SQL NULL.
    fn cells(&self) -> Vec<Option<String>> {
        let count = i16::from_be_bytes([self.body[0], self.body[1]]) as usize;
        let mut pos = 2;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let len = i32::from_be_bytes([
                self.body[pos],
                self.body[pos + 1],
                self.body[pos + 2],
                self.body[pos + 3],
            ]);
            pos += 4;
            if len < 0 {
                out.push(None);
            } else {
                let end = pos + len as usize;
                out.push(Some(
                    String::from_utf8_lossy(&self.body[pos..end]).into_owned(),
                ));
                pos = end;
            }
        }
        out
    }

    /// `RowDescription` columns as `(name, type_oid)`.
    fn fields(&self) -> Vec<(String, i32)> {
        let count = i16::from_be_bytes([self.body[0], self.body[1]]) as usize;
        let mut pos = 2;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let (name, next) = self.cstring_at(pos);
            pos = next + 4 + 2; // table oid, attnum
            let oid = i32::from_be_bytes([
                self.body[pos],
                self.body[pos + 1],
                self.body[pos + 2],
                self.body[pos + 3],
            ]);
            pos += 4 + 2 + 4 + 2; // oid, typlen, typmod, format
            out.push((name, oid));
        }
        out
    }

    fn tag_text(&self) -> String {
        self.cstring_at(0).0
    }
}

impl Client {
    fn connect(port: u16, params: &[(&str, &str)]) -> Client {
        let sock = TcpStream::connect(("127.0.0.1", port)).unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut c = Client { sock };
        let mut body = 196_608i32.to_be_bytes().to_vec();
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        c.sock.write_all(&packet).unwrap();
        c
    }

    fn send(&mut self, tag: u8, body: &[u8]) {
        let mut out = vec![tag];
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
        self.sock.write_all(&out).unwrap();
    }

    fn query(&mut self, sql: &str) -> Vec<Msg> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send(b'Q', &body);
        self.until_ready()
    }

    fn read_msg(&mut self) -> Msg {
        let mut head = [0u8; 5];
        self.sock.read_exact(&mut head).unwrap();
        let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize - 4;
        let mut body = vec![0u8; len];
        self.sock.read_exact(&mut body).unwrap();
        Msg { tag: head[0], body }
    }

    /// Read until `ReadyForQuery`, returning everything including it.
    fn until_ready(&mut self) -> Vec<Msg> {
        let mut out = Vec::new();
        loop {
            let m = self.read_msg();
            let done = m.tag == b'Z';
            out.push(m);
            if done {
                return out;
            }
        }
    }

    /// Complete an unauthenticated startup (auth off) and return the messages.
    fn handshake(&mut self) -> Vec<Msg> {
        let msgs = self.until_ready();
        assert_eq!(msgs[0].tag, b'R', "first message must be Authentication");
        assert_eq!(
            i32::from_be_bytes([
                msgs[0].body[0],
                msgs[0].body[1],
                msgs[0].body[2],
                msgs[0].body[3]
            ]),
            0,
            "auth is off, so AuthenticationOk"
        );
        msgs
    }
}

fn tags(msgs: &[Msg]) -> Vec<u8> {
    msgs.iter().map(|m| m.tag).collect()
}

fn tx_status(msgs: &[Msg]) -> u8 {
    msgs.iter().rev().find(|m| m.tag == b'Z').unwrap().body[0]
}

fn rows(msgs: &[Msg]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter(|m| m.tag == b'D')
        .map(|m| m.cells())
        .collect()
}

fn error(msgs: &[Msg]) -> Msg {
    msgs.iter()
        .find(|m| m.tag == b'E')
        .unwrap_or_else(|| panic!("expected an ErrorResponse, got {:?}", tags(msgs)))
        .clone()
}

fn command_tags(msgs: &[Msg]) -> Vec<String> {
    msgs.iter()
        .filter(|m| m.tag == b'C')
        .map(|m| m.tag_text())
        .collect()
}

// ── startup ─────────────────────────────────────────────────────────────────

#[test]
fn startup_reports_the_parameters_a_client_reads() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin"), ("database", "oxidb")]);
    let msgs = c.handshake();

    let params: Vec<(String, String)> = msgs
        .iter()
        .filter(|m| m.tag == b'S')
        .map(|m| {
            let (k, next) = m.cstring_at(0);
            let (v, _) = m.cstring_at(next);
            (k, v)
        })
        .collect();
    let get = |k: &str| params.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());

    // Clients gate behaviour on these; a missing one breaks the driver, not
    // just the query.
    assert!(get("server_version").unwrap().starts_with("16."));
    assert!(get("server_version").unwrap().contains("OxiDB"));
    assert_eq!(get("client_encoding").as_deref(), Some("UTF8"));
    assert_eq!(get("integer_datetimes").as_deref(), Some("on"));
    assert_eq!(get("standard_conforming_strings").as_deref(), Some("on"));
    assert_eq!(get("session_authorization").as_deref(), Some("admin"));
    // BackendKeyData, then "your turn", idle.
    assert!(tags(&msgs).contains(&b'K'));
    assert_eq!(tx_status(&msgs), b'I');
}

#[test]
fn a_startup_without_a_user_is_refused() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("database", "oxidb")]);
    let m = c.read_msg();
    assert_eq!(m.tag, b'E');
    assert_eq!(m.field(b'S').as_deref(), Some("FATAL"));
    assert!(m.field(b'M').unwrap().contains("user name"));
}

#[test]
fn ssl_is_declined_when_no_certificate_is_configured() {
    let g = spawn();
    let mut sock = TcpStream::connect(("127.0.0.1", g.port)).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    // SSLRequest: length 8, magic 80877103.
    let mut req = 8i32.to_be_bytes().to_vec();
    req.extend_from_slice(&80_877_103i32.to_be_bytes());
    sock.write_all(&req).unwrap();
    let mut answer = [0u8; 1];
    sock.read_exact(&mut answer).unwrap();
    assert_eq!(answer[0], b'N', "no TLS configured, so 'N'");

    // ...and the connection carries on in plaintext, as sslmode=prefer expects.
    let mut c = Client { sock };
    let mut body = 196_608i32.to_be_bytes().to_vec();
    for s in ["user", "admin"] {
        body.extend_from_slice(s.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    packet.extend_from_slice(&body);
    c.sock.write_all(&packet).unwrap();
    c.handshake();
}

#[test]
fn the_pg_port_needs_the_sql_engine() {
    let dir = tempfile::tempdir().unwrap();
    let doc = free_port();
    let pg = free_port();
    let _permit = BootPermit::acquire();
    let mut child = Command::new(env!("CARGO_BIN_EXE_oxidb-server"))
        .env("OXIDB_DATA", dir.path())
        .env("OXIDB_ADDR", format!("127.0.0.1:{doc}"))
        .env("OXIDB_PG_PORT", pg.to_string())
        .env_remove("OXIDB_SQL") // the engine is off
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", pg)).is_err() {
        assert!(Instant::now() < deadline, "pg port never opened");
        std::thread::sleep(Duration::from_millis(100));
    }
    let mut c = Client::connect(pg, &[("user", "admin")]);
    let m = c.read_msg();
    assert_eq!(m.tag, b'E');
    assert!(
        m.field(b'M').unwrap().contains("OXIDB_SQL"),
        "the refusal should name what to set: {:?}",
        m.field(b'M')
    );
    let _ = child.kill();
    let _ = child.wait();
}

// ── simple query ────────────────────────────────────────────────────────────

#[test]
fn select_returns_a_described_typed_result() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();

    let msgs = c.query("SELECT 1 AS n, 'ada' AS name, true AS flag");
    assert_eq!(tags(&msgs), vec![b'T', b'D', b'C', b'Z']);
    let fields = msgs[0].fields();
    assert_eq!(fields[0].0, "n");
    assert_eq!(fields[0].1, 20, "an engine Int is int8, never int4");
    assert_eq!(fields[1].1, 25, "text");
    assert_eq!(fields[2].1, 16, "bool");
    assert_eq!(
        rows(&msgs)[0],
        vec![
            Some("1".into()),
            Some("ada".into()),
            Some("t".into()) // booleans are t/f on the wire
        ]
    );
    assert_eq!(command_tags(&msgs), vec!["SELECT 1"]);
}

#[test]
fn command_tags_name_the_verb_and_carry_the_count() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();

    // Drivers read the affected count out of these tags, so the shape matters:
    // INSERT carries an OID field, the others do not.
    assert_eq!(
        command_tags(&c.query("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")),
        vec!["CREATE TABLE"]
    );
    assert_eq!(
        command_tags(&c.query("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")),
        vec!["INSERT 0 3"]
    );
    assert_eq!(
        command_tags(&c.query("UPDATE t SET v = 'z' WHERE id < 3")),
        vec!["UPDATE 2"]
    );
    assert_eq!(
        command_tags(&c.query("DELETE FROM t WHERE id = 1")),
        vec!["DELETE 1"]
    );
    assert_eq!(command_tags(&c.query("SELECT * FROM t")), vec!["SELECT 2"]);
    assert_eq!(command_tags(&c.query("DROP TABLE t")), vec!["DROP TABLE"]);
}

#[test]
fn several_statements_in_one_query_answer_in_order() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");
    let msgs = c.query("INSERT INTO t VALUES (1); SELECT id FROM t; SELECT 9");
    assert_eq!(
        command_tags(&msgs),
        vec!["INSERT 0 1", "SELECT 1", "SELECT 1"]
    );
    // One ReadyForQuery for the whole batch, at the end.
    assert_eq!(msgs.iter().filter(|m| m.tag == b'Z').count(), 1);
}

#[test]
fn an_empty_query_gets_the_empty_response() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    let msgs = c.query("");
    assert_eq!(tags(&msgs), vec![b'I', b'Z']);
    let msgs = c.query("  ;  ");
    assert_eq!(tags(&msgs), vec![b'I', b'Z']);
}

#[test]
fn nulls_are_distinct_from_empty_strings() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    c.query("INSERT INTO t VALUES (1, NULL), (2, '')");
    let msgs = c.query("SELECT v FROM t ORDER BY id");
    assert_eq!(rows(&msgs), vec![vec![None], vec![Some(String::new())]]);
}

// ── errors ──────────────────────────────────────────────────────────────────

#[test]
fn engine_errors_arrive_as_the_sqlstates_clients_recover_from() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");
    c.query("INSERT INTO t VALUES (1)");

    let e = error(&c.query("INSERT INTO t VALUES (1)"));
    assert_eq!(e.field(b'C').as_deref(), Some("23505"), "unique_violation");
    assert!(e.field(b'M').unwrap().contains("PRIMARY KEY"));

    let e = error(&c.query("SELECT * FROM nope"));
    assert_eq!(e.field(b'C').as_deref(), Some("42P01"), "undefined_table");

    let e = error(&c.query("SELECT nope FROM t"));
    assert_eq!(e.field(b'C').as_deref(), Some("42703"), "undefined_column");

    let e = error(&c.query("NOT SQL AT ALL"));
    assert_eq!(e.field(b'C').as_deref(), Some("42601"), "syntax_error");

    // An error is followed by ReadyForQuery — the session stays usable.
    assert_eq!(command_tags(&c.query("SELECT 1")), vec!["SELECT 1"]);
}

// ── transactions ────────────────────────────────────────────────────────────

#[test]
fn the_transaction_status_byte_tracks_the_session() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");

    assert_eq!(tx_status(&c.query("SELECT 1")), b'I');
    assert_eq!(tx_status(&c.query("BEGIN")), b'T', "in a transaction");
    assert_eq!(tx_status(&c.query("INSERT INTO t VALUES (1)")), b'T');
    assert_eq!(tx_status(&c.query("COMMIT")), b'I', "back to idle");
    assert_eq!(
        rows(&c.query("SELECT COUNT(*) FROM t"))[0][0].as_deref(),
        Some("1")
    );

    // A rollback discards the work.
    c.query("BEGIN");
    c.query("INSERT INTO t VALUES (2)");
    assert_eq!(tx_status(&c.query("ROLLBACK")), b'I');
    assert_eq!(
        rows(&c.query("SELECT COUNT(*) FROM t"))[0][0].as_deref(),
        Some("1")
    );
}

#[test]
fn a_failed_statement_puts_the_transaction_in_the_failed_state() {
    // psycopg refuses to continue in a failed transaction, and finds out from
    // this byte — reporting 'I' would make it silently run the rest of the
    // block outside any transaction.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");
    c.query("INSERT INTO t VALUES (1)");

    c.query("BEGIN");
    let msgs = c.query("INSERT INTO t VALUES (1)"); // duplicate key
    assert_eq!(error(&msgs).field(b'C').as_deref(), Some("23505"));
    assert_eq!(tx_status(&msgs), b'E', "failed transaction");

    // Everything but COMMIT/ROLLBACK is refused, with the code that says why.
    let msgs = c.query("SELECT 1");
    assert_eq!(error(&msgs).field(b'C').as_deref(), Some("25P02"));
    assert_eq!(tx_status(&msgs), b'E');

    // ROLLBACK clears it, and the session is usable again.
    let msgs = c.query("ROLLBACK");
    assert_eq!(command_tags(&msgs), vec!["ROLLBACK"]);
    assert_eq!(tx_status(&msgs), b'I');
    assert_eq!(command_tags(&c.query("SELECT 1")), vec!["SELECT 1"]);
}

#[test]
fn a_commit_in_a_failed_transaction_answers_rollback() {
    // PostgreSQL's behaviour: COMMIT after an error is a rollback, and says so.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("BEGIN");
    c.query("SELECT * FROM nope");
    let msgs = c.query("COMMIT");
    assert_eq!(command_tags(&msgs), vec!["ROLLBACK"]);
    assert_eq!(tx_status(&msgs), b'I');
}

// ── catalog interception ────────────────────────────────────────────────────

#[test]
fn the_settings_every_client_sends_are_accepted() {
    // pgjdbc and psycopg send these before they will talk at all, and the
    // engine's parser rejects SET outright.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();

    assert_eq!(
        command_tags(&c.query("SET extra_float_digits = 3")),
        vec!["SET"]
    );
    assert_eq!(
        command_tags(&c.query("SET application_name = 'test'")),
        vec!["SET"]
    );
    // Several in one round trip, the way a driver batches its opening volley.
    assert_eq!(
        command_tags(&c.query("SET client_encoding = 'UTF8'; SET timezone = 'UTC'")),
        vec!["SET", "SET"]
    );
    assert_eq!(command_tags(&c.query("RESET ALL")), vec!["RESET"]);
    assert_eq!(command_tags(&c.query("DISCARD ALL")), vec!["DISCARD ALL"]);

    // A SET is remembered, so SHOW answers it.
    c.query("SET application_name = 'myapp'");
    let msgs = c.query("SHOW application_name");
    assert_eq!(rows(&msgs)[0][0].as_deref(), Some("myapp"));
}

#[test]
fn version_and_the_session_functions_answer() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin"), ("database", "oxidb")]);
    c.handshake();

    let msgs = c.query("SELECT version()");
    assert!(rows(&msgs)[0][0].as_ref().unwrap().contains("OxiDB"));
    assert_eq!(
        rows(&c.query("SELECT current_database()"))[0][0].as_deref(),
        Some("oxidb")
    );
    assert_eq!(
        rows(&c.query("SELECT current_user"))[0][0].as_deref(),
        Some("admin")
    );
    assert_eq!(
        rows(&c.query("SELECT current_schema()"))[0][0].as_deref(),
        Some("public")
    );
    assert_eq!(
        rows(&c.query("SHOW transaction_isolation"))[0][0].as_deref(),
        Some("read committed")
    );
}

#[test]
fn the_engines_own_show_still_reaches_the_engine() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE zebra (id INT PRIMARY KEY)");
    let msgs = c.query("SHOW TABLES");
    let names: Vec<String> = rows(&msgs)
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
    assert!(names.contains(&"zebra".to_string()), "{names:?}");
}

#[test]
fn psql_dt_is_answered_from_the_engine_catalog() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE giraffe (id INT PRIMARY KEY)");
    // The shape psql's \dt reaches for.
    let msgs = c.query(
        "SELECT n.nspname as \"Schema\", c.relname as \"Name\" \
         FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r','p')",
    );
    let names: Vec<String> = rows(&msgs)
        .into_iter()
        .filter_map(|r| r[1].clone())
        .collect();
    assert!(names.contains(&"giraffe".to_string()), "{names:?}");
}

#[test]
fn a_batch_may_mix_intercepted_and_engine_statements() {
    // Npgsql opens with exactly this shape — an intercepted statement and a
    // catalog query in ONE simple-query message. Giving up on the whole batch
    // because one part was not interceptable is what broke its connect.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");

    let msgs = c.query("SET application_name = 'x'; INSERT INTO t VALUES (1); SELECT id FROM t");
    assert_eq!(
        command_tags(&msgs),
        vec!["SET", "INSERT 0 1", "SELECT 1"],
        "each statement answers in order: {:?}",
        tags(&msgs)
    );
    assert_eq!(rows(&msgs)[0][0].as_deref(), Some("1"));

    // Engine statements around an intercepted one still reach the engine.
    let msgs = c.query("INSERT INTO t VALUES (2); SET x = 1; INSERT INTO t VALUES (3)");
    assert_eq!(command_tags(&msgs), vec!["INSERT 0 1", "SET", "INSERT 0 1"]);
    assert_eq!(
        rows(&c.query("SELECT COUNT(*) FROM t"))[0][0].as_deref(),
        Some("3")
    );
}

#[test]
fn a_semicolon_inside_a_literal_does_not_split_a_batch() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    let msgs = c.query("INSERT INTO t VALUES (1, 'a;b')");
    assert_eq!(command_tags(&msgs), vec!["INSERT 0 1"]);
    assert_eq!(
        rows(&c.query("SELECT v FROM t"))[0][0].as_deref(),
        Some("a;b"),
        "the literal survived intact"
    );
}

#[test]
fn the_type_catalog_answers_what_drivers_load_on_connect() {
    // Npgsql will not connect without this (its escape hatch is a connection
    // string flag users should not have to know about).
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();

    let msgs = c.query(
        "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid \
         FROM pg_type AS t JOIN pg_namespace AS ns ON (ns.oid = typnamespace)",
    );
    let fields = msgs
        .iter()
        .find(|m| m.tag == b'T')
        .expect("RowDescription")
        .fields();
    // The oid columns must be described as `oid` (26), not int8 — a driver
    // reading pg_type.oid refuses anything else.
    assert_eq!(fields[1].0, "oid");
    assert_eq!(fields[1].1, 26, "oid type");
    assert_eq!(fields[3].1, 18, "typtype is char");
    assert_eq!(fields[4].1, 16, "typnotnull is bool");

    let names: Vec<String> = rows(&msgs)
        .into_iter()
        .filter_map(|r| r[2].clone())
        .collect();
    for expected in ["bool", "int8", "text", "timestamp", "numeric", "bytea"] {
        assert!(names.contains(&expected.to_string()), "{names:?}");
    }
    // Every row claims pg_catalog, base type, no element type.
    for r in rows(&msgs) {
        assert_eq!(r[0].as_deref(), Some("pg_catalog"));
        assert_eq!(r[3].as_deref(), Some("b"));
        assert_eq!(r[5], None);
    }

    // The two follow-ups are answered empty, which is true: this server has no
    // composite types and no enums.
    let msgs = c.query(
        "SELECT typ.oid, att.attname, att.atttypid FROM pg_type AS typ \
         JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid) \
         WHERE (typ.typtype = 'c' AND cls.relkind='c')",
    );
    assert_eq!(rows(&msgs).len(), 0);
    assert_eq!(command_tags(&msgs), vec!["SELECT 0"]);

    let msgs =
        c.query("SELECT pg_type.oid, enumlabel FROM pg_enum JOIN pg_type ON pg_type.oid=enumtypid");
    assert_eq!(rows(&msgs).len(), 0);
}

/// Column values of the first row, by column name from the RowDescription.
fn row_map(msgs: &[Msg], row: usize) -> std::collections::HashMap<String, Option<String>> {
    let fields = msgs
        .iter()
        .find(|m| m.tag == b'T')
        .expect("RowDescription")
        .fields();
    let cells = rows(msgs).remove(row);
    fields.into_iter().map(|(n, _)| n).zip(cells).collect()
}

#[test]
fn jdbc_metadata_reports_the_engines_own_schema() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE parent (id INT PRIMARY KEY, label TEXT)");
    c.query(
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id), note VARCHAR(50))",
    );
    c.query("CREATE TABLE enrol (a INT, b TEXT, CONSTRAINT pk PRIMARY KEY (a, b))");
    c.query("CREATE INDEX idx_note ON child (note)");

    // getTables — a pass-through query, so the result IS the JDBC shape.
    let msgs = c.query(
        "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, \
         '' AS SELF_REFERENCING_COL_NAME FROM pg_catalog.pg_class c \
         WHERE c.relname LIKE '%' AND ( c.relkind = 'r' )",
    );
    let names: Vec<String> = rows(&msgs)
        .into_iter()
        .filter_map(|r| r[2].clone())
        .collect();
    assert!(names.contains(&"parent".to_string()), "{names:?}");
    assert!(names.contains(&"child".to_string()), "{names:?}");
    // ...and the name pattern is honoured, not ignored.
    let msgs = c.query(
        "SELECT NULL AS TABLE_CAT, c.relname AS TABLE_NAME, '' AS SELF_REFERENCING_COL_NAME \
         FROM pg_catalog.pg_class c WHERE c.relname LIKE 'child' AND ( c.relkind = 'r' )",
    );
    assert_eq!(rows(&msgs).len(), 1);

    // getColumns — pgjdbc reads these by name, so the names are the contract.
    let msgs = c.query(
        "SELECT a.attname, a.atttypid, nullif(a.attidentity, '') as attidentity \
         FROM pg_catalog.pg_attribute a WHERE c.relname LIKE 'child' AND attname LIKE '%'",
    );
    let cols: Vec<String> = row_names(&msgs, "attname");
    assert_eq!(cols, vec!["id", "pid", "note"]);
    let first = row_map(&msgs, 0);
    // Metadata reports the *declared* width, which enforcement makes safe: an
    // INT column cannot hold a value that would not fit an int4, so a client
    // generating a 32-bit field from this cannot be handed something bigger.
    assert_eq!(first["atttypid"].as_deref(), Some("23"), "INT is int4");
    assert_eq!(first["attnotnull"].as_deref(), Some("t"), "PK is NOT NULL");
    assert_eq!(first["attnum"].as_deref(), Some("1"));
    // VARCHAR(50) is varchar with a length, not unbounded text.
    let msgs = c.query(
        "SELECT a.attname, nullif(a.attidentity, '') as attidentity \
         FROM pg_catalog.pg_attribute a WHERE c.relname LIKE 'child' AND attname LIKE 'note'",
    );
    let note = row_map(&msgs, 0);
    assert_eq!(note["atttypid"].as_deref(), Some("1043"), "varchar");
    assert_eq!(
        note["atttypmod"].as_deref(),
        Some("54"),
        "50 + varlena header"
    );

    // A `BIGINT` column keeps int8, and an out-of-range write is refused with
    // the SQLSTATE a client recovers from.
    c.query("CREATE TABLE widths (small SMALLINT, n INT, big BIGINT)");
    let msgs = c.query(
        "SELECT a.attname, nullif(a.attidentity, '') as attidentity \
         FROM pg_catalog.pg_attribute a WHERE c.relname LIKE 'widths' AND attname LIKE '%'",
    );
    assert_eq!(
        row_names(&msgs, "atttypid"),
        vec!["21", "23", "20"],
        "int2, int4, int8 — the declared widths"
    );
    let e = error(&c.query("INSERT INTO widths VALUES (40000, 1, 1)"));
    assert_eq!(
        e.field(b'C').as_deref(),
        Some("22003"),
        "numeric_value_out_of_range"
    );
    // ...while a query result still describes the storage type, which every
    // client can read.
    c.query("INSERT INTO widths VALUES (1, 2, 3)");
    let msgs = c.query("SELECT small FROM widths");
    assert_eq!(msgs[0].fields()[0].1, 20, "results report int8");

    // getPrimaryKeys — every part of a composite key, in order.
    let msgs = c.query(
        "SELECT ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, i.indkey AS KEY_SEQ, \
         ci.relname AS PK_NAME FROM pg_index i WHERE ct.relname = 'enrol'",
    );
    assert_eq!(row_names(&msgs, "column_name"), vec!["a", "b"]);
    assert_eq!(row_names(&msgs, "key_seq"), vec!["1", "2"]);

    // getIndexInfo — the primary key counts as a unique index, as it does in
    // PostgreSQL, plus the secondary index.
    let msgs = c.query(
        "SELECT tmp.INDEX_QUALIFIER, tmp.INDEX_NAME FROM pg_index tmp WHERE ct.relname = 'child'",
    );
    let ix = row_names(&msgs, "index_name");
    assert!(ix.contains(&"child_pkey".to_string()), "{ix:?}");
    assert!(ix.contains(&"idx_note".to_string()), "{ix:?}");
    let unique: Vec<String> = row_names(&msgs, "non_unique");
    assert_eq!(unique[0], "f", "the primary key is unique");

    // getImportedKeys — the FK the engine actually holds.
    let msgs = c.query(
        "SELECT fk.FKCOLUMN_NAME, fk.FK_NAME, fk.KEY_SEQ, fk.PK_NAME \
         FROM pg_constraint WHERE fkt.relname = 'child'",
    );
    let m = row_map(&msgs, 0);
    assert_eq!(m["fktable_name"].as_deref(), Some("child"));
    assert_eq!(m["fkcolumn_name"].as_deref(), Some("pid"));
    assert_eq!(m["pktable_name"].as_deref(), Some("parent"));
    assert_eq!(m["pkcolumn_name"].as_deref(), Some("id"));
    // A table with no foreign keys reports none — not its primary key.
    let msgs = c.query(
        "SELECT fk.FKCOLUMN_NAME, fk.FK_NAME, fk.KEY_SEQ, fk.PK_NAME \
         FROM pg_constraint WHERE fkt.relname = 'parent'",
    );
    assert_eq!(rows(&msgs).len(), 0);
}

#[test]
fn catalog_rows_come_from_the_table_being_selected_from() {
    // DBeaver's table list joins pg_description onto pg_class. Dispatching on
    // "mentions pg_description" answered it with that table's columns and no
    // rows — the table list, silently empty.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(80))");
    c.query("CREATE INDEX idx_name ON customers (name)");

    let msgs = c.query(
        "SELECT c.oid,c.*,d.description FROM pg_catalog.pg_class c \
         LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid \
         WHERE c.relnamespace=2200 AND c.relkind not in ('i','I','c')",
    );
    let names = row_names(&msgs, "relname");
    assert_eq!(
        names,
        vec!["customers"],
        "the table, and not the index — relkind filters are honoured"
    );
    let m = row_map(&msgs, 0);
    assert_eq!(m["relkind"].as_deref(), Some("r"));
    assert_eq!(m["relnatts"].as_deref(), Some("2"));
    assert_eq!(m["description"], None, "no comments, reported as NULL");
}

#[test]
fn catalog_tables_report_the_engines_schema() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE parent (id INT PRIMARY KEY)");
    c.query("CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))");
    c.query("CREATE INDEX idx_pid ON child (pid)");

    // Constraints: the primary keys and the foreign key, by kind.
    let msgs = c.query("SELECT c.oid,c.* FROM pg_catalog.pg_constraint c");
    let kinds = row_names(&msgs, "contype");
    assert_eq!(kinds.iter().filter(|k| *k == "p").count(), 2, "two PKs");
    assert_eq!(kinds.iter().filter(|k| *k == "f").count(), 1, "one FK");
    assert!(row_names(&msgs, "conname").contains(&"child_pid_fkey".to_string()));

    // Indexes.
    let msgs = c.query("SELECT i.* FROM pg_catalog.pg_index i");
    assert_eq!(rows(&msgs).len(), 1);

    // Attributes carry their real types and lengths.
    let msgs = c.query("SELECT a.* FROM pg_catalog.pg_attribute a");
    let names = row_names(&msgs, "attname");
    assert!(names.contains(&"pid".to_string()), "{names:?}");

    // Namespaces, and the one role there is.
    let msgs = c.query("SELECT n.oid,n.* FROM pg_catalog.pg_namespace n");
    let ns = row_names(&msgs, "nspname");
    assert!(ns.contains(&"public".to_string()) && ns.contains(&"pg_catalog".to_string()));
    let msgs = c.query("SELECT r.* FROM pg_catalog.pg_roles r");
    assert_eq!(row_names(&msgs, "rolname"), vec!["admin"]);

    // Catalogs for things this engine does not have report none — with the
    // right columns, so a client reads "empty" rather than failing.
    for (sql, column) in [
        ("SELECT p.* FROM pg_catalog.pg_proc p", "proname"),
        ("SELECT e.* FROM pg_catalog.pg_extension e", "extname"),
        ("SELECT t.* FROM pg_catalog.pg_trigger t", "tgname"),
    ] {
        let msgs = c.query(sql);
        assert_eq!(rows(&msgs).len(), 0, "{sql}");
        let fields: Vec<String> = msgs
            .iter()
            .find(|m| m.tag == b'T')
            .expect("RowDescription")
            .fields()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert!(fields.contains(&column.to_string()), "{sql}: {fields:?}");
    }
}

#[test]
fn a_shape_probe_returns_the_columns_it_asked_for_and_no_rows() {
    // `WHERE 1<>1` asks what columns exist. Answering it with a refusal is
    // what stopped DBeaver connecting at all.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");

    let msgs = c.query("SELECT reltype FROM pg_catalog.pg_class WHERE 1<>1 LIMIT 1");
    assert_eq!(rows(&msgs).len(), 0);
    let fields: Vec<String> = msgs
        .iter()
        .find(|m| m.tag == b'T')
        .expect("RowDescription")
        .fields()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(fields, vec!["reltype"], "exactly what was selected");
}

#[test]
fn session_functions_resolve_in_any_combination() {
    // Matching whole query strings meant every combination a client invented
    // was a miss — `SELECT current_schema(),session_user` is DBeaver's.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin"), ("database", "oxidb")]);
    c.handshake();

    let msgs = c.query("SELECT current_schema(),session_user");
    assert_eq!(
        rows(&msgs)[0],
        vec![Some("public".into()), Some("admin".into())]
    );
    let msgs = c.query("SELECT current_database(), current_user, version()");
    let r = rows(&msgs).remove(0);
    assert_eq!(r[0].as_deref(), Some("oxidb"));
    assert_eq!(r[1].as_deref(), Some("admin"));
    assert!(r[2].as_ref().unwrap().contains("OxiDB"));
    // An alias is honoured.
    let msgs = c.query("SELECT current_schema() AS sch");
    let fields = msgs.iter().find(|m| m.tag == b'T').unwrap().fields();
    assert_eq!(fields[0].0, "sch");
    // A select that is not all session functions still reaches the engine.
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");
    c.query("INSERT INTO t VALUES (7)");
    assert_eq!(
        rows(&c.query("SELECT id FROM t"))[0][0].as_deref(),
        Some("7")
    );
}

#[test]
fn a_catalog_query_is_never_answered_in_another_ones_shape() {
    // Every one of these once matched a *different* answer and came back with
    // the wrong columns — or worse, the right columns holding the wrong rows
    // (getIndexInfo returned the table list). A caller cannot tell that from a
    // correct answer, so precision here is the whole safety property.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");

    // An index query must not come back as the table list.
    let msgs = c.query(
        "SELECT tmp.INDEX_QUALIFIER, tmp.INDEX_NAME FROM pg_index tmp WHERE ct.relname = 't'",
    );
    let fields: Vec<String> = msgs
        .iter()
        .find(|m| m.tag == b'T')
        .expect("RowDescription")
        .fields()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert!(fields.contains(&"index_name".to_string()), "{fields:?}");
    assert!(
        !fields.contains(&"Name".to_string()),
        "that is the \\dt shape"
    );

    // A type query must not come back as the table list either.
    let msgs = c.query(
        "SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, \
         pg_type.oid FROM pg_catalog.pg_type",
    );
    let names = row_names(&msgs, "typname");
    assert!(names.contains(&"int8".to_string()), "{names:?}");
    assert!(!names.contains(&"t".to_string()), "that is the table list");
}

/// The values of one named column, in row order.
fn row_names(msgs: &[Msg], column: &str) -> Vec<String> {
    let fields = msgs
        .iter()
        .find(|m| m.tag == b'T')
        .expect("RowDescription")
        .fields();
    let idx = fields
        .iter()
        .position(|(n, _)| n == column)
        .unwrap_or_else(|| panic!("no column {column:?} in {fields:?}"));
    rows(msgs)
        .into_iter()
        .filter_map(|r| r[idx].clone())
        .collect()
}

#[test]
fn per_table_introspection_is_still_refused_not_answered_empty() {
    // The type catalog is static and safe to answer from a constant; per-table
    // metadata is not, and must keep saying so rather than reporting a table
    // with no columns.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE jt (id INT PRIMARY KEY)");
    // JDBC's getColumns: joins pg_attribute like Npgsql's composite query, but
    // for relations rather than composite types.
    let e = error(&c.query(
        "SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid \
         FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) \
         JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) \
         WHERE c.relkind in ('r','p','v','f','m') AND c.relname LIKE 'jt') c",
    ));
    assert_eq!(e.field(b'C').as_deref(), Some("0A000"));
    assert!(
        e.field(b'M').unwrap().contains("DESCRIBE"),
        "{:?}",
        e.field(b'M')
    );
}

#[test]
fn psql_backslash_d_on_one_table_is_refused_rather_than_mis_shaped() {
    // Answering psql's `\d <table>` probe with the table list made psql fail on
    // a column it expected ("column number 4 is out of range"). A refusal that
    // names DESCRIBE is the useful answer.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE demo (id INT PRIMARY KEY)");
    let e = error(&c.query(
        "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c \
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname OPERATOR(pg_catalog.~) '^(demo)$'",
    ));
    assert_eq!(e.field(b'C').as_deref(), Some("0A000"));
    assert!(
        e.field(b'M').unwrap().contains("DESCRIBE"),
        "{:?}",
        e.field(b'M')
    );
}

#[test]
fn an_unimplemented_catalog_query_is_refused_by_name() {
    // The alternative — an empty result — would be believed. (The *type*
    // catalog is answered, because its content is static and knowable; these
    // are not.)
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    for sql in [
        "SELECT rolname FROM pg_catalog.pg_roles",
        "SELECT indexname FROM pg_catalog.pg_index",
        "SELECT table_name FROM information_schema.tables",
    ] {
        let e = error(&c.query(sql));
        assert_eq!(e.field(b'C').as_deref(), Some("0A000"), "for {sql}");
        let m = e.field(b'M').unwrap();
        assert!(
            m.contains("system catalogs") && m.contains("SHOW TABLES"),
            "{m}"
        );
    }
}

// ── extended query ──────────────────────────────────────────────────────────

impl Client {
    fn parse(&mut self, name: &str, sql: &str, oids: &[i32]) {
        let mut b = Vec::new();
        b.extend_from_slice(name.as_bytes());
        b.push(0);
        b.extend_from_slice(sql.as_bytes());
        b.push(0);
        b.extend_from_slice(&(oids.len() as i16).to_be_bytes());
        for o in oids {
            b.extend_from_slice(&o.to_be_bytes());
        }
        self.send(b'P', &b);
    }

    fn bind(&mut self, portal: &str, stmt: &str, params: &[Option<&[u8]>]) {
        let mut b = Vec::new();
        b.extend_from_slice(portal.as_bytes());
        b.push(0);
        b.extend_from_slice(stmt.as_bytes());
        b.push(0);
        b.extend_from_slice(&0i16.to_be_bytes()); // all params in text format
        b.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            match p {
                Some(v) => {
                    b.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    b.extend_from_slice(v);
                }
                None => b.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        b.extend_from_slice(&0i16.to_be_bytes()); // all results in text format
        self.send(b'B', &b);
    }

    fn describe(&mut self, kind: u8, name: &str) {
        let mut b = vec![kind];
        b.extend_from_slice(name.as_bytes());
        b.push(0);
        self.send(b'D', &b);
    }

    fn execute_portal(&mut self, name: &str, max_rows: i32) {
        let mut b = Vec::new();
        b.extend_from_slice(name.as_bytes());
        b.push(0);
        b.extend_from_slice(&max_rows.to_be_bytes());
        self.send(b'E', &b);
    }

    fn sync(&mut self) -> Vec<Msg> {
        self.send(b'S', &[]);
        self.until_ready()
    }
}

#[test]
fn extended_query_binds_parameters() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");

    // Parameterized INSERT, the shape psycopg emits.
    c.parse("", "INSERT INTO t VALUES ($1, $2)", &[20, 25]);
    c.bind("", "", &[Some(b"1"), Some(b"ada")]);
    c.execute_portal("", 0);
    let msgs = c.sync();
    assert_eq!(tags(&msgs), vec![b'1', b'2', b'C', b'Z']);
    assert_eq!(command_tags(&msgs), vec!["INSERT 0 1"]);

    // ...and reading it back, with the parameter typed by the client.
    c.parse("", "SELECT v FROM t WHERE id = $1", &[20]);
    c.bind("", "", &[Some(b"1")]);
    c.execute_portal("", 0);
    let msgs = c.sync();
    assert_eq!(rows(&msgs), vec![vec![Some("ada".into())]]);

    // A NULL parameter is a NULL, not the string "NULL".
    c.parse("", "INSERT INTO t VALUES ($1, $2)", &[20, 25]);
    c.bind("", "", &[Some(b"2"), None]);
    c.execute_portal("", 0);
    c.sync();
    c.parse("", "SELECT v FROM t WHERE id = 2", &[]);
    c.bind("", "", &[]);
    c.execute_portal("", 0);
    assert_eq!(rows(&c.sync()), vec![vec![None]]);
}

#[test]
fn describe_reports_the_parameters_it_was_given() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.parse("s1", "SELECT $1", &[20]);
    c.describe(b'S', "s1");
    let msgs = c.sync();
    let pd = msgs
        .iter()
        .find(|m| m.tag == b't')
        .expect("ParameterDescription");
    assert_eq!(i16::from_be_bytes([pd.body[0], pd.body[1]]), 1);
    assert_eq!(
        i32::from_be_bytes([pd.body[2], pd.body[3], pd.body[4], pd.body[5]]),
        20
    );
    // Column types need execution to know, so Describe answers NoData.
    assert!(tags(&msgs).contains(&b'n'));
}

#[test]
fn an_unspecified_parameter_type_is_described_as_text() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.parse("s", "SELECT $1", &[0]); // 0 = "server, you decide"
    c.describe(b'S', "s");
    let msgs = c.sync();
    let pd = msgs.iter().find(|m| m.tag == b't').unwrap();
    assert_eq!(
        i32::from_be_bytes([pd.body[2], pd.body[3], pd.body[4], pd.body[5]]),
        25,
        "text"
    );
}

#[test]
fn a_row_limit_suspends_the_portal_and_resumes() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.query("CREATE TABLE t (id INT PRIMARY KEY)");
    c.query("INSERT INTO t VALUES (1),(2),(3),(4),(5)");

    c.parse("", "SELECT id FROM t ORDER BY id", &[]);
    c.bind("p", "", &[]);
    c.execute_portal("p", 2);
    c.send(b'H', &[]); // Flush: give me what you have without a Sync
    let mut got = Vec::new();
    loop {
        let m = c.read_msg();
        let done = m.tag == b's'; // PortalSuspended
        got.push(m);
        if done {
            break;
        }
    }
    assert_eq!(rows(&got).len(), 2, "the row limit was honoured");
    assert_eq!(rows(&got)[0][0].as_deref(), Some("1"));

    // Resuming returns the rest and finishes.
    c.execute_portal("p", 0);
    let msgs = c.sync();
    let rest: Vec<String> = rows(&msgs)
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
    assert_eq!(rest, vec!["3", "4", "5"]);
}

#[test]
fn an_error_skips_the_rest_of_the_batch_until_sync() {
    // The protocol's rule: after an error the server ignores everything up to
    // the next Sync. A client that pipelined a batch must not receive replies
    // for work it has already abandoned.
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();

    c.parse("", "SELECT * FROM nope", &[]);
    c.bind("", "", &[]);
    c.execute_portal("", 0);
    // Queued behind the failure; must be discarded, not answered.
    c.parse("", "SELECT 1", &[]);
    c.bind("", "", &[]);
    c.execute_portal("", 0);
    let msgs = c.sync();

    assert_eq!(error(&msgs).field(b'C').as_deref(), Some("42P01"));
    assert_eq!(
        msgs.iter().filter(|m| m.tag == b'D').count(),
        0,
        "nothing after the error should have run: {:?}",
        tags(&msgs)
    );

    // After the Sync the session is clean.
    c.parse("", "SELECT 1", &[]);
    c.bind("", "", &[]);
    c.execute_portal("", 0);
    assert_eq!(rows(&c.sync()), vec![vec![Some("1".into())]]);
}

#[test]
fn close_releases_a_prepared_statement() {
    let g = spawn();
    let mut c = Client::connect(g.port, &[("user", "admin")]);
    c.handshake();
    c.parse("s", "SELECT 1", &[]);
    let mut b = vec![b'S'];
    b.extend_from_slice(b"s\0");
    c.send(b'C', &b);
    let msgs = c.sync();
    assert!(tags(&msgs).contains(&b'3'), "CloseComplete");

    // Binding it again is a protocol error, not a panic.
    c.bind("", "s", &[]);
    let msgs = c.sync();
    assert_eq!(error(&msgs).field(b'C').as_deref(), Some("08P01"));
}

// ── authentication ──────────────────────────────────────────────────────────

#[test]
fn scram_is_offered_and_a_wrong_password_is_refused() {
    // The account is created through the server's own `UserStore`, so the
    // SCRAM verifier is exactly the one the native port would check against.
    let dir = tempfile::tempdir().unwrap();
    let doc = free_port();
    let pg = free_port();
    {
        let mut store = oxidb_server::auth::UserStore::open(dir.path()).unwrap();
        store
            .create_user(
                "alice",
                "s3cret-passphrase",
                oxidb_server::auth::Role::ReadWrite,
            )
            .unwrap();
    }

    let _permit = BootPermit::acquire();
    let mut child = Command::new(env!("CARGO_BIN_EXE_oxidb-server"))
        .env("OXIDB_DATA", dir.path())
        .env("OXIDB_ADDR", format!("127.0.0.1:{doc}"))
        .env("OXIDB_PG_PORT", pg.to_string())
        .env("OXIDB_SQL", "1")
        .env("OXIDB_AUTH", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", pg)).is_err() {
        assert!(Instant::now() < deadline, "pg port never opened");
        std::thread::sleep(Duration::from_millis(100));
    }

    let mut c = Client::connect(pg, &[("user", "alice"), ("database", "oxidb")]);
    let m = c.read_msg();
    assert_eq!(m.tag, b'R');
    assert_eq!(
        i32::from_be_bytes([m.body[0], m.body[1], m.body[2], m.body[3]]),
        10,
        "AuthenticationSASL"
    );
    let mechanisms = String::from_utf8_lossy(&m.body[4..]);
    assert!(
        mechanisms.contains("SCRAM-SHA-256"),
        "offered: {mechanisms:?}"
    );

    // A garbage proof is refused without revealing which part was wrong.
    let mut b = Vec::new();
    b.extend_from_slice(b"SCRAM-SHA-256\0");
    let first = "n,,n=alice,r=notarealnonce";
    b.extend_from_slice(&(first.len() as i32).to_be_bytes());
    b.extend_from_slice(first.as_bytes());
    c.send(b'p', &b);
    let m = c.read_msg();
    assert_eq!(m.tag, b'R', "server-first");
    let mut b = Vec::new();
    b.extend_from_slice(b"c=biws,r=notarealnonce,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    c.send(b'p', &b);
    let m = c.read_msg();
    assert_eq!(m.tag, b'E');
    assert_eq!(m.field(b'C').as_deref(), Some("28P01"));
    assert_eq!(
        m.field(b'M').as_deref(),
        Some("password authentication failed"),
        "the reason must not distinguish a bad user from a bad password"
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// Complete a real SCRAM-SHA-256 exchange as a PostgreSQL client would, using
/// the server's own client-side implementation (`scram_client`) for the proof.
/// Returns once `ReadyForQuery` has arrived.
fn scram_login(c: &mut Client, user: &str, password: &str) {
    use oxidb_server::scram_client::{ScramClient, verify_server_final};

    let m = c.read_msg();
    assert_eq!(m.tag, b'R');
    assert_eq!(
        i32::from_be_bytes([m.body[0], m.body[1], m.body[2], m.body[3]]),
        10,
        "AuthenticationSASL"
    );
    assert!(String::from_utf8_lossy(&m.body[4..]).contains("SCRAM-SHA-256"));

    let mut scram = ScramClient::new(user, password);
    let first = scram.client_first();
    let mut b = Vec::new();
    b.extend_from_slice(b"SCRAM-SHA-256\0");
    b.extend_from_slice(&(first.len() as i32).to_be_bytes());
    b.extend_from_slice(first.as_bytes());
    c.send(b'p', &b);

    let m = c.read_msg();
    assert_eq!(m.tag, b'R');
    assert_eq!(
        i32::from_be_bytes([m.body[0], m.body[1], m.body[2], m.body[3]]),
        11,
        "AuthenticationSASLContinue"
    );
    let server_first = String::from_utf8_lossy(&m.body[4..]).into_owned();
    let (final_msg, expected_sig) = scram.client_final(&server_first).unwrap();
    c.send(b'p', final_msg.as_bytes());

    let m = c.read_msg();
    assert_eq!(m.tag, b'R');
    assert_eq!(
        i32::from_be_bytes([m.body[0], m.body[1], m.body[2], m.body[3]]),
        12,
        "AuthenticationSASLFinal"
    );
    // The server proved it knows the verifier too — mutual authentication.
    verify_server_final(&String::from_utf8_lossy(&m.body[4..]), &expected_sig)
        .expect("server signature must verify");

    let m = c.read_msg();
    assert_eq!(m.tag, b'R');
    assert_eq!(
        i32::from_be_bytes([m.body[0], m.body[1], m.body[2], m.body[3]]),
        0,
        "AuthenticationOk"
    );
    c.until_ready();
}

fn spawn_authenticated(users: &[(&str, &str, oxidb_server::auth::Role)]) -> (Child, u16) {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut store = oxidb_server::auth::UserStore::open(dir.path()).unwrap();
        for (name, pw, role) in users {
            store.create_user(name, pw, *role).unwrap();
        }
    }
    let doc = free_port();
    let pg = free_port();
    let _permit = BootPermit::acquire();
    let child = Command::new(env!("CARGO_BIN_EXE_oxidb-server"))
        .env("OXIDB_DATA", dir.path())
        .env("OXIDB_ADDR", format!("127.0.0.1:{doc}"))
        .env("OXIDB_PG_PORT", pg.to_string())
        .env("OXIDB_SQL", "1")
        .env("OXIDB_AUTH", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", pg)).is_err() {
        assert!(Instant::now() < deadline, "pg port never opened");
        std::thread::sleep(Duration::from_millis(100));
    }
    // The tempdir has to outlive the server; leak it deliberately rather than
    // have the server lose its data directory mid-test.
    std::mem::forget(dir);
    (child, pg)
}

#[test]
fn a_real_scram_login_succeeds_and_the_read_role_may_not_write() {
    use oxidb_server::auth::Role;
    let (mut child, pg) = spawn_authenticated(&[
        ("reader", "reader-passphrase", Role::Read),
        ("writer", "writer-passphrase", Role::ReadWrite),
    ]);

    // A writer sets the table up.
    let mut w = Client::connect(pg, &[("user", "writer"), ("database", "oxidb")]);
    scram_login(&mut w, "writer", "writer-passphrase");
    assert_eq!(
        command_tags(&w.query("CREATE TABLE t (id INT PRIMARY KEY)")),
        vec!["CREATE TABLE"]
    );
    w.query("INSERT INTO t VALUES (1)");

    // The reader authenticates, reads, and is refused a write — the same gate
    // the native port applies to a `read` role.
    let mut r = Client::connect(pg, &[("user", "reader"), ("database", "oxidb")]);
    scram_login(&mut r, "reader", "reader-passphrase");
    assert_eq!(
        rows(&r.query("SELECT id FROM t"))[0][0].as_deref(),
        Some("1")
    );
    let e = error(&r.query("INSERT INTO t VALUES (2)"));
    assert_eq!(
        e.field(b'C').as_deref(),
        Some("42501"),
        "insufficient_privilege"
    );
    let e = error(&r.query("DROP TABLE t"));
    assert_eq!(e.field(b'C').as_deref(), Some("42501"));

    let _ = child.kill();
    let _ = child.wait();
}
