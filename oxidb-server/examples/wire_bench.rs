//! OxiWire vs the PostgreSQL wire, over the **same** SQL engine.
//!
//! Both ports of one server process, one engine, one dataset, the same SQL
//! text — so what is left is the protocol and the per-request work each
//! listener does around it.
//!
//! Both clients are hand-rolled here and kept deliberately minimal. Comparing
//! `oxidb-client` against `psycopg` would measure two libraries (and two
//! languages); comparing two thin clients in one binary measures the wires.
//! Neither does anything the other does not: connect once, send, read the
//! whole reply, decode it into owned values.
//!
//! Run:
//! ```text
//! OXIDB_SQL=1 OXIDB_ADDR=127.0.0.1:4444 OXIDB_PG_PORT=5432 oxidb-server &
//! cargo run --release -p oxidb-server --example wire_bench
//! ```
//!
//! Two shapes are measured. **Sequential** (one connection, one request in
//! flight) is the latency comparison. **Concurrent** (N connections, each with
//! its own thread) is the throughput one — both listeners are
//! thread-per-connection in standalone mode, so neither is throttled by a pool
//! the other does not have.
//!
//! What this does *not* measure: TLS, and auth, which is a per-connection cost
//! on both sides.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

// ── measurement ─────────────────────────────────────────────────────────────

#[derive(Default)]
struct Stats {
    latencies: Vec<Duration>,
    bytes_out: usize,
    bytes_in: usize,
    rows: usize,
}

impl Stats {
    fn percentile(&self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let mut v = self.latencies.clone();
        v.sort_unstable();
        let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
        v[idx]
    }

    fn ops_per_sec(&self) -> f64 {
        let total: Duration = self.latencies.iter().sum();
        if total.is_zero() {
            return 0.0;
        }
        self.latencies.len() as f64 / total.as_secs_f64()
    }

    fn bytes_per_op(&self) -> (f64, f64) {
        let n = self.latencies.len().max(1) as f64;
        (self.bytes_out as f64 / n, self.bytes_in as f64 / n)
    }
}

/// A client that can run one named workload, so the driver loop is identical
/// for both wires and only the encoding differs.
trait Wire {
    // Part of the Wire trait's shape, used when a run prints per-wire labels.
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    /// Run `sql` and return how many rows came back. Implementations count
    /// their own bytes.
    fn run(&mut self, sql: &str, params: &[i64]) -> usize;
    fn bytes(&self) -> (usize, usize);
    fn reset_bytes(&mut self);
}

fn measure(wire: &mut dyn Wire, sql: &str, params: &[i64], warmup: usize, iters: usize) -> Stats {
    for _ in 0..warmup {
        wire.run(sql, params);
    }
    wire.reset_bytes();
    let mut stats = Stats::default();
    stats.latencies.reserve(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let rows = wire.run(sql, params);
        stats.latencies.push(t0.elapsed());
        stats.rows += rows;
    }
    let (out, inb) = wire.bytes();
    stats.bytes_out = out;
    stats.bytes_in = inb;
    stats
}

// ── OxiWire: u32 length prefix + JSON ───────────────────────────────────────

struct OxiWire {
    /// Buffered too, so neither wire is favoured by its read strategy.
    reader: BufReader<TcpStream>,
    sock: TcpStream,
    out: usize,
    inb: usize,
}

impl OxiWire {
    fn connect(addr: &str) -> OxiWire {
        let sock = TcpStream::connect(addr).expect("connect oxiwire");
        sock.set_nodelay(true).unwrap();
        let reader = BufReader::with_capacity(64 * 1024, sock.try_clone().unwrap());
        OxiWire {
            reader,
            sock,
            out: 0,
            inb: 0,
        }
    }

    fn call(&mut self, request: &Value) -> Value {
        let body = serde_json::to_vec(request).unwrap();
        // OxiWire's length prefix is little-endian (PostgreSQL's is big-endian).
        let mut frame = (body.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&body);
        self.out += frame.len();
        self.sock.write_all(&frame).unwrap();

        let mut len = [0u8; 4];
        self.reader.read_exact(&mut len).unwrap();
        let n = u32::from_le_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        self.reader.read_exact(&mut buf).unwrap();
        self.inb += 4 + n;
        serde_json::from_slice(&buf).unwrap()
    }

    fn sql(&mut self, sql: &str, params: &[i64]) -> Value {
        let mut req = json!({ "engine": "sql", "cmd": "sql", "sql": sql });
        if !params.is_empty() {
            req["params"] = json!(params);
        }
        let v = self.call(&req);
        assert!(
            v.get("ok").and_then(|b| b.as_bool()).unwrap_or(false),
            "oxiwire error: {v}"
        );
        v
    }
}

impl Wire for OxiWire {
    fn name(&self) -> &'static str {
        "OxiWire"
    }

    fn run(&mut self, sql: &str, params: &[i64]) -> usize {
        let v = self.sql(sql, params);
        // Decode the rows the way a client would: count what came back.
        v.get("data")
            .and_then(|d| d.as_array())
            .and_then(|a| a.first())
            .and_then(|r| r.get("rows"))
            .and_then(|r| r.as_array())
            .map_or(0, |rows| rows.len())
    }

    fn bytes(&self) -> (usize, usize) {
        (self.out, self.inb)
    }
    fn reset_bytes(&mut self) {
        self.out = 0;
        self.inb = 0;
    }
}

// ── PostgreSQL v3 ───────────────────────────────────────────────────────────

struct Pg {
    /// Buffered on purpose. PostgreSQL sends one `DataRow` message per row, so
    /// an unbuffered reader makes two syscalls per row and a large result set
    /// ends up measuring the client's read strategy rather than the protocol.
    /// Every real driver buffers; so does this.
    reader: BufReader<TcpStream>,
    sock: TcpStream,
    out: usize,
    inb: usize,
    /// Use the extended protocol (Parse/Bind/Execute/Sync), as real drivers do
    /// for parameterized statements.
    extended: bool,
}

impl Pg {
    fn connect(addr: &str, extended: bool) -> Pg {
        let sock = TcpStream::connect(addr).expect("connect pg");
        sock.set_nodelay(true).unwrap();
        let reader = BufReader::with_capacity(64 * 1024, sock.try_clone().unwrap());
        let mut pg = Pg {
            reader,
            sock,
            out: 0,
            inb: 0,
            extended,
        };
        let mut body = 196_608i32.to_be_bytes().to_vec();
        for s in ["user", "bench", "database", "oxidb"] {
            body.extend_from_slice(s.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        pg.send_raw(&packet);
        pg.read_until_ready();
        pg
    }

    fn send_raw(&mut self, bytes: &[u8]) {
        self.out += bytes.len();
        self.sock.write_all(bytes).unwrap();
    }

    fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// Read backend messages until `ReadyForQuery`, returning the row count and
    /// decoding each cell (so the work is comparable to parsing the JSON).
    fn read_until_ready(&mut self) -> usize {
        let mut rows = 0;
        loop {
            let mut head = [0u8; 5];
            self.reader.read_exact(&mut head).unwrap();
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize - 4;
            let mut body = vec![0u8; len];
            self.reader.read_exact(&mut body).unwrap();
            self.inb += 5 + len;
            match head[0] {
                b'D' => {
                    rows += 1;
                    // Decode the cells, as a client must, so the comparison is
                    // not "parse JSON" versus "count bytes".
                    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut pos = 2;
                    for _ in 0..count {
                        let n = i32::from_be_bytes([
                            body[pos],
                            body[pos + 1],
                            body[pos + 2],
                            body[pos + 3],
                        ]);
                        pos += 4;
                        if n >= 0 {
                            let end = pos + n as usize;
                            let _ = std::str::from_utf8(&body[pos..end]).unwrap().to_string();
                            pos = end;
                        }
                    }
                }
                b'E' => {
                    let text = String::from_utf8_lossy(&body);
                    panic!("pg error: {text}");
                }
                b'Z' => return rows,
                _ => {}
            }
        }
    }
}

impl Wire for Pg {
    fn name(&self) -> &'static str {
        if self.extended {
            "PG (extended)"
        } else {
            "PG (simple)"
        }
    }

    fn run(&mut self, sql: &str, params: &[i64]) -> usize {
        if !self.extended && params.is_empty() {
            let mut body = sql.as_bytes().to_vec();
            body.push(0);
            let m = Pg::msg(b'Q', &body);
            self.send_raw(&m);
            return self.read_until_ready();
        }

        // Parse (unnamed) / Bind / Execute / Sync — one round trip, as a driver
        // pipelines them.
        let mut batch = Vec::new();
        let mut parse = vec![0u8]; // unnamed statement
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for _ in params {
            parse.extend_from_slice(&20i32.to_be_bytes()); // int8
        }
        batch.extend_from_slice(&Pg::msg(b'P', &parse));

        let mut bind = vec![0u8, 0u8]; // unnamed portal, unnamed statement
        bind.extend_from_slice(&0i16.to_be_bytes()); // all params text
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            let s = p.to_string();
            bind.extend_from_slice(&(s.len() as i32).to_be_bytes());
            bind.extend_from_slice(s.as_bytes());
        }
        bind.extend_from_slice(&0i16.to_be_bytes()); // all results text
        batch.extend_from_slice(&Pg::msg(b'B', &bind));

        let mut exec = vec![0u8];
        exec.extend_from_slice(&0i32.to_be_bytes()); // no row limit
        batch.extend_from_slice(&Pg::msg(b'E', &exec));
        batch.extend_from_slice(&Pg::msg(b'S', &[]));

        self.send_raw(&batch);
        self.read_until_ready()
    }

    fn bytes(&self) -> (usize, usize) {
        (self.out, self.inb)
    }
    fn reset_bytes(&mut self) {
        self.out = 0;
        self.inb = 0;
    }
}

// ── the run ─────────────────────────────────────────────────────────────────

struct Workload {
    name: &'static str,
    sql: &'static str,
    params: Vec<i64>,
    iters: usize,
}

fn main() {
    let args: HashMap<String, String> = std::env::args()
        .skip(1)
        .collect::<Vec<_>>()
        .chunks(2)
        .filter(|c| c.len() == 2)
        .map(|c| (c[0].trim_start_matches("--").to_string(), c[1].clone()))
        .collect();
    let oxi_addr = args
        .get("oxiwire")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:4444".into());
    let pg_addr = args
        .get("pg")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:5432".into());
    let scale: usize = args
        .get("rows")
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    // Setup over OxiWire, so both wires read exactly the same data.
    let mut setup = OxiWire::connect(&oxi_addr);
    setup.sql("DROP TABLE IF EXISTS bench", &[]);
    setup.sql(
        "CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, score DOUBLE, tag TEXT)",
        &[],
    );
    eprintln!("seeding {scale} rows...");
    let mut i = 0;
    while i < scale {
        let n = 500.min(scale - i);
        let values: Vec<String> = (i..i + n)
            .map(|k| format!("({k}, 'name-{k}', {}.5, 'tag-{}')", k % 97, k % 7))
            .collect();
        setup.sql(
            &format!("INSERT INTO bench VALUES {}", values.join(",")),
            &[],
        );
        i += n;
    }
    setup.sql("DROP TABLE IF EXISTS bench_w", &[]);
    setup.sql("CREATE TABLE bench_w (id INT PRIMARY KEY, v TEXT)", &[]);
    setup.sql("DROP TABLE IF EXISTS bench_c", &[]);
    setup.sql("CREATE TABLE bench_c (id INT PRIMARY KEY, v TEXT)", &[]);

    let mode = args.get("mode").map(String::as_str).unwrap_or("both");
    let levels: Vec<usize> = args
        .get("conns")
        .map(|s| s.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 2, 4, 8, 16, 32]);
    let secs: u64 = args.get("secs").and_then(|s| s.parse().ok()).unwrap_or(2);

    if mode == "conc" {
        run_concurrency(&oxi_addr, &pg_addr, &levels, secs);
        return;
    }

    let workloads = vec![
        Workload {
            name: "SELECT 1 (round trip)",
            sql: "SELECT 1",
            params: vec![],
            iters: 3000,
        },
        Workload {
            name: "point SELECT by PK",
            sql: "SELECT id, name, score, tag FROM bench WHERE id = 4242",
            params: vec![],
            iters: 3000,
        },
        Workload {
            name: "SELECT 100 rows",
            sql: "SELECT id, name, score, tag FROM bench WHERE id < 100",
            params: vec![],
            iters: 1000,
        },
        Workload {
            name: "SELECT 1000 rows",
            sql: "SELECT id, name, score, tag FROM bench WHERE id < 1000",
            params: vec![],
            iters: 200,
        },
        Workload {
            name: "aggregate over all rows",
            sql: "SELECT COUNT(*), AVG(score) FROM bench",
            params: vec![],
            iters: 300,
        },
        Workload {
            name: "point SELECT, parameterized",
            sql: "SELECT id, name, score, tag FROM bench WHERE id = $1",
            params: vec![4242],
            iters: 3000,
        },
    ];

    println!(
        "\n{:<30} {:>14} {:>10} {:>10} {:>9} {:>9}",
        "workload / wire", "ops/sec", "p50", "p99", "out B/op", "in B/op"
    );
    println!("{}", "─".repeat(88));

    for w in &workloads {
        let mut oxi = OxiWire::connect(&oxi_addr);
        let mut pg_simple = Pg::connect(&pg_addr, false);
        let mut pg_ext = Pg::connect(&pg_addr, true);

        let warmup = (w.iters / 10).max(20);
        let a = measure(&mut oxi, w.sql, &w.params, warmup, w.iters);
        // The simple protocol cannot carry parameters; inline it instead so the
        // row work is the same, and say so in the label.
        let simple_sql = w.sql.replace("$1", "4242");
        let b = measure(&mut pg_simple, &simple_sql, &[], warmup, w.iters);
        let c = measure(&mut pg_ext, w.sql, &w.params, warmup, w.iters);

        println!("{}", w.name);
        for s in [(&a, "OxiWire"), (&b, "PG simple"), (&c, "PG extended")] {
            let (out, inb) = s.0.bytes_per_op();
            println!(
                "  {:<26} {:>14.0} {:>10} {:>10} {:>9.0} {:>9.0}",
                s.1,
                s.0.ops_per_sec(),
                fmt_dur(s.0.percentile(0.50)),
                fmt_dur(s.0.percentile(0.99)),
                out,
                inb
            );
        }
        let ratio = c.ops_per_sec() / a.ops_per_sec();
        println!(
            "  {:<26} PG extended is {:.2}x OxiWire ({} rows/op)",
            "",
            ratio,
            a.rows / w.iters.max(1)
        );
    }

    // Writes, measured separately so each iteration inserts a fresh key.
    println!("\nsingle-row INSERT");
    let mut oxi = OxiWire::connect(&oxi_addr);
    let mut pg = Pg::connect(&pg_addr, true);
    let iters = 2000;
    let mut a = Stats::default();
    for k in 0..iters {
        let sql = format!("INSERT INTO bench_w VALUES ({k}, 'v')");
        let t0 = Instant::now();
        oxi.run(&sql, &[]);
        a.latencies.push(t0.elapsed());
    }
    let (ao, ai) = oxi.bytes();
    a.bytes_out = ao;
    a.bytes_in = ai;
    let mut b = Stats::default();
    for k in 0..iters {
        let sql = format!("INSERT INTO bench_w VALUES ({}, 'v')", k + iters);
        let t0 = Instant::now();
        pg.run(&sql, &[]);
        b.latencies.push(t0.elapsed());
    }
    let (bo, bi) = pg.bytes();
    b.bytes_out = bo;
    b.bytes_in = bi;
    for s in [(&a, "OxiWire"), (&b, "PG extended")] {
        let (out, inb) = s.0.bytes_per_op();
        println!(
            "  {:<26} {:>14.0} {:>10} {:>10} {:>9.0} {:>9.0}",
            s.1,
            s.0.ops_per_sec(),
            fmt_dur(s.0.percentile(0.50)),
            fmt_dur(s.0.percentile(0.99)),
            out,
            inb
        );
    }
    println!();

    if mode == "both" {
        run_concurrency(&oxi_addr, &pg_addr, &levels, secs);
    }
}

// ── concurrency ─────────────────────────────────────────────────────────────

/// How a wire's client is built, so the sweep can make one per thread.
#[derive(Clone, Copy)]
enum Kind {
    Oxi,
    // Reachable via --mode seq; kept so the enum names every wire this measures.
    #[allow(dead_code)]
    PgSimple,
    PgExtended,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::Oxi => "OxiWire",
            Kind::PgSimple => "PG simple",
            Kind::PgExtended => "PG extended",
        }
    }

    fn connect(self, oxi: &str, pg: &str) -> Box<dyn Wire + Send> {
        match self {
            Kind::Oxi => Box::new(OxiWire::connect(oxi)),
            Kind::PgSimple => Box::new(Pg::connect(pg, false)),
            Kind::PgExtended => Box::new(Pg::connect(pg, true)),
        }
    }
}

// One sweep cell's parameters, passed as the sweep defines them.
#[allow(clippy::too_many_arguments)]
/// Run `sql` from `threads` connections at once for `duration`, and report the
/// aggregate.
///
/// Each thread owns a connection and runs closed-loop: send, wait, send again.
/// That is the shape a connection pool produces, and it means the reported
/// throughput and the latency percentiles describe the same run.
fn sweep(
    kind: Kind,
    oxi_addr: &str,
    pg_addr: &str,
    sql: &(dyn Fn(usize, usize) -> String + Sync),
    params: &[i64],
    threads: usize,
    duration: Duration,
    reset: Option<&str>,
) -> (f64, Duration, Duration) {
    // A write workload starts each cell from an empty table: keys stay unique
    // across cells, and a table that grew through the sweep would make later
    // cells slower for reasons that are not the wire.
    if let Some(table) = reset {
        let mut setup = OxiWire::connect(oxi_addr);
        setup.sql(&format!("DROP TABLE IF EXISTS {table}"), &[]);
        setup.sql(
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, v TEXT)"),
            &[],
        );
    }
    let start = Instant::now();
    let deadline = start + duration;
    let mut all: Vec<Duration> = Vec::new();

    // Scoped threads so the statement builder can be borrowed rather than
    // cloned into each thread.
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            handles.push(scope.spawn(move || {
                let mut wire = kind.connect(oxi_addr, pg_addr);
                let mut lat = Vec::with_capacity(4096);
                let mut i = 0usize;
                while Instant::now() < deadline {
                    // Rendered per iteration — a write workload needs a fresh
                    // key every time, and cycling a fixed list collides on the
                    // second lap. Built *before* the timer starts, so the
                    // formatting is not measured.
                    let stmt = sql(t, i);
                    let t0 = Instant::now();
                    wire.run(&stmt, params);
                    lat.push(t0.elapsed());
                    i += 1;
                }
                lat
            }));
        }
        for h in handles {
            all.extend(h.join().expect("bench thread"));
        }
    });
    let elapsed = start.elapsed();
    let stats = Stats {
        latencies: all,
        ..Stats::default()
    };
    let throughput = stats.latencies.len() as f64 / elapsed.as_secs_f64();
    (throughput, stats.percentile(0.50), stats.percentile(0.99))
}

fn run_concurrency(oxi_addr: &str, pg_addr: &str, levels: &[usize], secs: u64) {
    let duration = Duration::from_secs(secs);
    // (name, statement builder, params). The builder takes (thread, counter) so
    // each connection writes its own keys.
    type Build = Box<dyn Fn(usize, usize) -> String + Sync>;
    // (name, statement builder, params, table to reset before each cell)
    let cases: Vec<(&str, Build, Vec<i64>, Option<&str>)> = vec![
        (
            "point SELECT by PK",
            Box::new(|t: usize, i: usize| {
                format!(
                    "SELECT id, name, score, tag FROM bench WHERE id = {}",
                    (t * 37 + i * 11) % 10_000
                )
            }),
            vec![],
            None,
        ),
        (
            "SELECT 100 rows",
            Box::new(|_t: usize, _i: usize| {
                "SELECT id, name, score, tag FROM bench WHERE id < 100".to_string()
            }),
            vec![],
            None,
        ),
        (
            "single-row INSERT",
            Box::new(|t: usize, i: usize| {
                // A fresh key per (thread, iteration) round, so a repeat within
                // one run overwrites rather than colliding — the write path is
                // what is being measured, not conflict handling.
                format!("INSERT INTO bench_c VALUES ({}, 'v')", t * 1_000_000 + i)
            }),
            vec![],
            Some("bench_c"),
        ),
    ];

    for (name, build, params, reset) in &cases {
        println!("\n{name} — throughput by concurrent connections");
        println!(
            "  {:<14} {:>12} {:>10} {:>10}   scaling vs 1 conn",
            "wire", "ops/sec", "p50", "p99"
        );
        for kind in [Kind::Oxi, Kind::PgExtended] {
            let mut base = 0.0;
            for (n, &threads) in levels.iter().enumerate() {
                let (ops, p50, p99) = sweep(
                    kind, oxi_addr, pg_addr, build, params, threads, duration, *reset,
                );
                if n == 0 {
                    base = ops;
                }
                println!(
                    "  {:<14} {:>12.0} {:>10} {:>10}   {:>2} conn  {:.2}x",
                    if n == 0 { kind.label() } else { "" },
                    ops,
                    fmt_dur(p50),
                    fmt_dur(p99),
                    threads,
                    ops / base.max(1.0)
                );
            }
        }
    }
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us < 1000.0 {
        format!("{us:.0}µs")
    } else {
        format!("{:.2}ms", us / 1000.0)
    }
}
