//! OxiDB against PostgreSQL, on the same data, through the same client.
//!
//! Every other benchmark in this tree measures memory. This one measures the
//! thing a database is usually judged on, and the one OxiDB had never been
//! compared against PostgreSQL on: how fast it answers queries.
//!
//! The design is the same trick the memory benchmarks use — OxiDB speaks the
//! PostgreSQL v3 wire, so **one hand-rolled client drives both servers**. No
//! driver difference, no language difference, no library caching one side's
//! results. What is left is the two engines.
//!
//! Deliberately minimal: connect once, send a simple-query message, read every
//! backend message to `ReadyForQuery`, and **decode every cell** — so neither
//! side is credited for skipping work the other does. Simple query rather than
//! extended, because it is one message on both and removes prepare/bind from
//! the comparison.
//!
//! ```bash
//! cargo run --release -p oxidb-server --example query_bench -- \
//!     --oxidb 127.0.0.1:5432/oxidb --postgres 127.0.0.1:5480/bench --secs 3
//! ```
//!
//! The dataset is the one from `bench/pg-memory/` (schema.sql + gen.py), loaded
//! into both.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// A minimal PostgreSQL v3 client: enough to log in with trust auth, run a
/// simple query, and decode the reply.
struct Pg {
    reader: BufReader<TcpStream>,
    sock: TcpStream,
}

impl Pg {
    /// `addr` is `host:port/database`.
    fn connect(target: &str) -> Pg {
        let (addr, db) = target.split_once('/').expect("expected host:port/database");
        let sock = TcpStream::connect(addr).unwrap_or_else(|e| panic!("connect {addr}: {e}"));
        sock.set_nodelay(true).unwrap();
        let mut pg = Pg {
            reader: BufReader::with_capacity(64 * 1024, sock.try_clone().unwrap()),
            sock,
        };
        let mut body = 196_608i32.to_be_bytes().to_vec(); // protocol 3.0
        for s in ["user", "bench", "database", db] {
            body.extend_from_slice(s.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        pg.sock.write_all(&packet).unwrap();
        pg.drain();
        pg
    }

    /// Run one statement, returning the number of rows it produced.
    fn query(&mut self, sql: &str) -> usize {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        self.sock.write_all(&msg).unwrap();
        self.drain()
    }

    /// Read to `ReadyForQuery`, decoding every cell on the way. Unknown message
    /// types are skipped, which is what lets the same client handle both
    /// servers' startup chatter (PostgreSQL sends far more of it).
    fn drain(&mut self) -> usize {
        let mut rows = 0usize;
        loop {
            let mut head = [0u8; 5];
            self.reader.read_exact(&mut head).expect("read header");
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize - 4;
            let mut body = vec![0u8; len];
            self.reader.read_exact(&mut body).expect("read body");
            match head[0] {
                b'D' => {
                    rows += 1;
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut pos = 2;
                    for _ in 0..n {
                        let l = i32::from_be_bytes([
                            body[pos],
                            body[pos + 1],
                            body[pos + 2],
                            body[pos + 3],
                        ]);
                        pos += 4;
                        if l >= 0 {
                            // Touch the bytes, as a real client decoding a value
                            // would, so this is not "count lengths" on one side.
                            let _ = std::str::from_utf8(&body[pos..pos + l as usize]);
                            pos += l as usize;
                        }
                    }
                }
                b'E' => {
                    let msg = String::from_utf8_lossy(&body).replace('\0', " ");
                    panic!("server error: {}", msg.trim());
                }
                b'Z' => return rows,
                _ => {}
            }
        }
    }
}

/// One measured workload. `sql` is a template; `{}` is replaced per iteration so
/// the queries are not all identical (which would let either side cache a plan
/// for a single value and flatter itself).
struct Workload {
    name: &'static str,
    sql: &'static str,
    /// Values substituted into the template, cycled.
    span: i64,
}

const WORKLOADS: &[Workload] = &[
    Workload {
        name: "point SELECT by PK",
        sql: "SELECT id, customer_id, status, total FROM orders WHERE id = {}",
        span: 400_000,
    },
    Workload {
        name: "composite PK lookup",
        sql: "SELECT product, qty, amount FROM order_items WHERE order_id = {} AND line_no = 2",
        span: 100_000,
    },
    Workload {
        name: "secondary index eq",
        sql: "SELECT count(*) FROM orders WHERE customer_id = {}",
        span: 200_000,
    },
    Workload {
        name: "index, low selectivity",
        sql: "SELECT count(*) FROM customers WHERE country = 'TR' AND created > TIMESTAMP '2024-01-01 00:00:00' AND id > {}",
        span: 200_000,
    },
    Workload {
        name: "full scan aggregate",
        sql: "SELECT sum(total) FROM orders WHERE id > {}",
        span: 100,
    },
    Workload {
        name: "GROUP BY",
        sql: "SELECT status, count(*) FROM orders WHERE id > {} GROUP BY status",
        span: 100,
    },
    Workload {
        name: "join + filter",
        sql: "SELECT count(*) FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.country = 'TR' AND o.id > {}",
        span: 100,
    },
    Workload {
        name: "range scan + ORDER BY",
        sql: "SELECT id, total FROM orders WHERE customer_id = {} ORDER BY total DESC LIMIT 10",
        span: 200_000,
    },
];

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn fmt(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us < 1000.0 {
        format!("{us:.0}µs")
    } else {
        format!("{:.2}ms", us / 1000.0)
    }
}

/// Run `w` for `secs`, returning (ops/sec, p50, p99, rows seen, iterations).
fn measure(pg: &mut Pg, w: &Workload, secs: u64) -> (f64, Duration, Duration, usize, usize) {
    // Warm up: the first execution of a shape pays for whatever caching each
    // side does, and that is not what is being compared.
    for i in 0..20 {
        pg.query(&w.sql.replace("{}", &(i % w.span).to_string()));
    }
    let mut lat = Vec::new();
    let mut rows = 0;
    let start = Instant::now();
    let mut i = 0i64;
    while start.elapsed() < Duration::from_secs(secs) {
        let sql = w.sql.replace("{}", &(i % w.span).to_string());
        let t = Instant::now();
        rows += pg.query(&sql);
        lat.push(t.elapsed());
        i += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    lat.sort();
    (
        lat.len() as f64 / elapsed,
        percentile(&lat, 0.50),
        percentile(&lat, 0.99),
        rows,
        lat.len(),
    )
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flags: HashMap<String, String> = args
        .chunks(2)
        .filter(|c| c.len() == 2)
        .map(|c| (c[0].trim_start_matches("--").to_string(), c[1].clone()))
        .collect();
    let oxi = flags
        .get("oxidb")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:5432/oxidb".into());
    let pgt = flags
        .get("postgres")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:5480/bench".into());
    let secs: u64 = flags.get("secs").and_then(|s| s.parse().ok()).unwrap_or(3);

    println!("oxidb    {oxi}");
    println!("postgres {pgt}");
    println!("{secs}s per workload, simple query protocol, every cell decoded\n");
    println!(
        "{:<26} {:>10} {:>10} {:>8}   {:>10} {:>10} {:>8}   {:>7}",
        "workload", "oxidb/s", "p50", "p99", "pg/s", "p50", "p99", "ratio"
    );

    let mut o = Pg::connect(&oxi);
    let mut p = Pg::connect(&pgt);

    for w in WORKLOADS {
        let (ops_o, p50_o, p99_o, rows_o, iters_o) = measure(&mut o, w, secs);
        let (ops_p, p50_p, p99_p, rows_p, iters_p) = measure(&mut p, w, secs);
        // Both must have answered the same question the same way, or the
        // comparison is meaningless. The two run different iteration counts, so
        // compare rows *per query*, not totals — and allow a little slack,
        // since they stop at different points in the value cycle.
        let per_o = rows_o as f64 / iters_o as f64;
        let per_p = rows_p as f64 / iters_p as f64;
        let flag = if (per_o - per_p).abs() <= 0.05 * per_p.max(1.0) {
            ""
        } else {
            "  <- ROWS PER QUERY DIFFER"
        };
        println!(
            "{:<26} {:>10.0} {:>10} {:>8}   {:>10.0} {:>10} {:>8}   {:>6.2}x{}",
            w.name,
            ops_o,
            fmt(p50_o),
            fmt(p99_o),
            ops_p,
            fmt(p50_p),
            fmt(p99_p),
            ops_o / ops_p,
            flag
        );
    }
    println!("\nratio > 1 means OxiDB is faster.");
}
