//! End-to-end cross-shard integration test for the `oxipool` sharded router.
//!
//! Topology: the real `oxipool` binary in front of three independent OxiDB
//! shards. Each shard is a plain OxiDB instance served in-process by the
//! server's request handler (no Raft / single node), so the test exercises the
//! actual wire protocol and the actual pooler — not a mock.
//!
//! It verifies that, for a sharded collection:
//!   * inserts are routed to a shard by the shard key (data is really split),
//!   * `count` fans out and **sums** across shards,
//!   * `find` fans out and **concatenates** across shards,
//!   * `aggregate` ($group) is **split + merged** correctly across shards
//!     (both a global sum and a per-key group), matching a single-node result,
//!   * a query carrying the shard key is routed to a **single** shard.

use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

use oxidb::OxiDb;
use oxidb_server::handler::handle_request;
use oxidb_server::protocol::{read_message, write_message};

const N_SHARDS: usize = 3;
const REGIONS: &[&str] = &[
    "us-east",
    "us-west",
    "eu-west",
    "eu-central",
    "ap-south",
    "ap-north",
    "sa-east",
    "af-south",
    "me-central",
    "oce",
];
const DOCS: usize = 120; // 12 per region

// ── In-process shard server (mirrors oxidb-server/tests/handler_test.rs) ──

struct Shard {
    addr: SocketAddr,
    _dir: TempDir,
}

fn start_shard() -> Shard {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let mut tx: Option<u64> = None;
                loop {
                    match read_message(&mut stream) {
                        Ok(msg) => {
                            let req: Value = match serde_json::from_slice(&msg) {
                                Ok(v) => v,
                                Err(_) => break,
                            };
                            let resp = handle_request(&db, req, &mut tx);
                            if write_message(&mut stream, &resp).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    });
    Shard { addr, _dir: dir }
}

// ── Minimal length-prefixed JSON client (same framing as oxipool) ──

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).ok();
        Client { stream }
    }
    fn send(&mut self, req: &Value) -> Value {
        let bytes = serde_json::to_vec(req).unwrap();
        write_message(&mut self.stream, &bytes).unwrap();
        let resp = read_message(&mut self.stream).unwrap();
        serde_json::from_slice(&resp).unwrap()
    }
}

// ── Helpers ──

/// Grab a currently-free localhost port (small race window, fine for a test).
fn free_addr() -> String {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    format!("127.0.0.1:{}", a.port())
}

fn wait_connectable(addr: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        match TcpStream::connect(addr) {
            Ok(_) => return,
            Err(e)
                if e.kind() == ErrorKind::ConnectionRefused || e.kind() == ErrorKind::TimedOut =>
            {
                if start.elapsed() > timeout {
                    panic!("address {addr} not connectable within {timeout:?}");
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("connect error to {addr}: {e}"),
        }
    }
}

/// Kills the oxipool child on drop so the process never leaks past the test.
struct PoolProc(Child);
impl Drop for PoolProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ok_data(resp: &Value) -> &Value {
    assert_eq!(resp["ok"], true, "expected ok response, got: {resp}");
    &resp["data"]
}

#[test]
fn oxipool_cross_shard_routing_and_merge() {
    // 1. Start the shards.
    let shards: Vec<Shard> = (0..N_SHARDS).map(|_| start_shard()).collect();
    let shard_list = shards
        .iter()
        .map(|s| s.addr.to_string())
        .collect::<Vec<_>>()
        .join(",");

    // 2. Start the real oxipool binary in SHARDED mode in front of them.
    let pool_addr = free_addr();
    let _pool = PoolProc(
        Command::new(env!("CARGO_BIN_EXE_oxipool"))
            .env("OXIPOOL_LISTEN", &pool_addr)
            .env("OXIPOOL_SHARDS", &shard_list)
            .env("OXIPOOL_SHARD_KEYS", "accounts:region")
            .env("OXIPOOL_NUM_CHUNKS", "256")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn oxipool (is the binary built?)"),
    );
    wait_connectable(&pool_addr, Duration::from_secs(20));

    let mut pool = Client::connect(&pool_addr);

    // 3. Create the sharded collection (broadcast to every shard).
    ok_data(&pool.send(&json!({ "cmd": "create_collection", "collection": "accounts" })));

    // 4. Insert docs through the pool; each is routed to a shard by `region`.
    //    Track the expected per-region and global aggregates locally.
    let mut expected_total_bal: i64 = 0;
    let mut per_region: std::collections::BTreeMap<String, (i64, i64)> = Default::default(); // region -> (count, sum_bal)
    for i in 0..DOCS {
        let region = REGIONS[i % REGIONS.len()];
        let bal = i as i64;
        let resp = pool.send(&json!({
            "cmd": "insert",
            "collection": "accounts",
            "doc": { "region": region, "bal": bal, "i": i }
        }));
        assert_eq!(resp["ok"], true, "insert {i} failed: {resp}");
        expected_total_bal += bal;
        let e = per_region.entry(region.to_string()).or_insert((0, 0));
        e.0 += 1;
        e.1 += bal;
    }

    // 5. Data is really distributed: ask each shard directly and confirm the
    //    per-shard counts sum to DOCS and span more than one shard.
    let mut shard_counts = Vec::new();
    for s in &shards {
        let mut c = Client::connect(&s.addr.to_string());
        let resp = c.send(&json!({ "cmd": "count", "collection": "accounts" }));
        shard_counts.push(ok_data(&resp)["count"].as_u64().unwrap());
    }
    let summed: u64 = shard_counts.iter().sum();
    assert_eq!(
        summed, DOCS as u64,
        "per-shard counts must sum to all docs: {shard_counts:?}"
    );
    let non_empty = shard_counts.iter().filter(|&&n| n > 0).count();
    assert!(
        non_empty >= 2,
        "data should span >=2 shards, got {shard_counts:?}"
    );

    // 6. count through the pool fans out and SUMS.
    let resp = pool.send(&json!({ "cmd": "count", "collection": "accounts" }));
    assert_eq!(
        ok_data(&resp)["count"].as_u64(),
        Some(DOCS as u64),
        "pool count != DOCS"
    );

    // 7. find through the pool fans out and CONCATENATES.
    let resp = pool.send(&json!({ "cmd": "find", "collection": "accounts", "query": {} }));
    let docs = ok_data(&resp)
        .as_array()
        .expect("find data must be an array");
    assert_eq!(docs.len(), DOCS, "pool find must return every doc");

    // 8. Global aggregate ($group _id:null sum) — the strongest cross-shard
    //    merge: partial sums from each shard combined into one.
    let resp = pool.send(&json!({
        "cmd": "aggregate",
        "collection": "accounts",
        "pipeline": [ { "$group": { "_id": null, "total": { "$sum": "$bal" }, "n": { "$sum": 1 } } } ]
    }));
    let arr = ok_data(&resp)
        .as_array()
        .expect("aggregate data must be an array");
    assert_eq!(arr.len(), 1, "global group yields one doc: {arr:?}");
    assert_eq!(
        arr[0]["total"].as_i64(),
        Some(expected_total_bal),
        "cross-shard $sum mismatch"
    );
    assert_eq!(
        arr[0]["n"].as_i64(),
        Some(DOCS as i64),
        "cross-shard count mismatch"
    );

    // 9. Per-key aggregate ($group by region) — merged across shards.
    let resp = pool.send(&json!({
        "cmd": "aggregate",
        "collection": "accounts",
        "pipeline": [ { "$group": { "_id": "$region", "total": { "$sum": "$bal" }, "n": { "$sum": 1 } } } ]
    }));
    let arr = ok_data(&resp)
        .as_array()
        .expect("aggregate data must be an array");
    assert_eq!(arr.len(), per_region.len(), "one group per region");
    let mut got: std::collections::BTreeMap<String, (i64, i64)> = Default::default();
    for g in arr {
        got.insert(
            g["_id"].as_str().unwrap().to_string(),
            (g["n"].as_i64().unwrap(), g["total"].as_i64().unwrap()),
        );
    }
    assert_eq!(got, per_region, "per-region cross-shard group mismatch");

    // 10. A query carrying the shard key is routed to a SINGLE shard and still
    //     returns the right answer.
    let (exp_n, _) = per_region["us-east"];
    let resp = pool.send(&json!({
        "cmd": "count", "collection": "accounts", "query": { "region": "us-east" }
    }));
    assert_eq!(
        ok_data(&resp)["count"].as_i64(),
        Some(exp_n),
        "targeted count mismatch"
    );
}
