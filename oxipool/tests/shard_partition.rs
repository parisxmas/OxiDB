//! What a sharded router does when a shard is gone.
//!
//! `cross_shard_e2e.rs` proves the happy path: routing, fan-out, merge. This
//! one removes a shard mid-flight, because that is where a sharded query
//! engine is most dangerous — not by erroring, but by **succeeding**.
//!
//! A scatter-gather has every shard's answer in hand and must fold them into
//! one. If it folds only the ones that came back, the client gets `ok:true`
//! and a plausible-looking answer that is quietly missing a third of the
//! data: a `count` that undercounts, a `find` short some documents, a
//! `find_one` reporting "not found" for a document that exists on the shard
//! that happens to be down. Those are worse than an outage — an outage is
//! visible. `scatter.rs` guards each merge against exactly this; nothing
//! proved the guards actually hold end-to-end, which is what these tests do.
//!
//! Two failure modes, because they are not the same:
//!   * **down** — the shard closes connections (a crash). Pooled connections
//!     already established break too, which is the point: stopping `accept()`
//!     alone would leave the pool happily using its existing sockets.
//!   * **blackhole** — the shard accepts and reads but never answers (a
//!     network partition, or a wedged shard). Nothing errors; it just never
//!     returns. Only `OXIPOOL_REQUEST_TIMEOUT` turns that into an error
//!     instead of a client hanging forever and permanently consuming the
//!     pooled connection it borrowed.
//!
//! Also asserted: a dead shard must not take down the shards that are fine
//! (a key-routed query to a live shard still answers), and the pool must
//! recover once the shard returns — not stay poisoned.
//!
//! Run: cargo test -p oxipool --test shard_partition

use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

use oxidb::OxiDb;
use oxidb_server::handler::handle_request;
use oxidb_server::protocol::{read_message, write_message};

const N_SHARDS: usize = 3;
/// One region per shard-key value; 3 shards, so every shard holds some.
const REGIONS: &[&str] = &["eu", "us", "apac", "sa", "af", "me"];
const DOCS: usize = 60;

// ── Shard modes ──
const LIVE: u8 = 0;
/// Accept, read, answer nothing — a partition. Detectable only by timeout.
const BLACKHOLE: u8 = 1;
/// Close every connection, established ones included — a crash.
const DOWN: u8 = 2;

struct Shard {
    addr: SocketAddr,
    mode: Arc<AtomicU8>,
    _dir: TempDir,
}

impl Shard {
    fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }
}

/// An in-process OxiDB shard whose reachability we can flip at will.
fn start_shard() -> Shard {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mode = Arc::new(AtomicU8::new(LIVE));

    let m = Arc::clone(&mode);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let db = Arc::clone(&db);
            let m = Arc::clone(&m);
            thread::spawn(move || {
                let mut tx: Option<u64> = None;
                loop {
                    // Checked per request, so a mode flip also kills
                    // connections the pool established while we were LIVE.
                    if m.load(Ordering::SeqCst) == DOWN {
                        break; // close → the pool sees EOF, like a crash
                    }
                    match read_message(&mut stream) {
                        Ok(msg) => {
                            while m.load(Ordering::SeqCst) == BLACKHOLE {
                                // Read it, answer nothing: a partition looks
                                // exactly like a very slow shard.
                                thread::sleep(Duration::from_millis(50));
                            }
                            if m.load(Ordering::SeqCst) == DOWN {
                                break;
                            }
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
    Shard {
        addr,
        mode,
        _dir: dir,
    }
}

// ── Minimal length-prefixed JSON client (same framing as oxipool) ──

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).ok();
        // Nothing here may hang the test harness itself.
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        Client { stream }
    }
    fn send(&mut self, req: &Value) -> Value {
        let bytes = serde_json::to_vec(req).unwrap();
        write_message(&mut self.stream, &bytes).unwrap();
        let resp = read_message(&mut self.stream).expect("pool answered");
        serde_json::from_slice(&resp).unwrap()
    }
}

struct PoolProc(Child);
impl Drop for PoolProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_addr() -> String {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("127.0.0.1:{}", l.local_addr().unwrap().port())
}

fn wait_connectable(addr: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        match TcpStream::connect(addr) {
            Ok(_) => return,
            Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
                assert!(start.elapsed() <= timeout, "{addr} never came up");
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("connect {addr}: {e}"),
        }
    }
}

fn ok(resp: &Value) -> bool {
    resp["ok"].as_bool().unwrap_or(false)
}

/// Assert the router refused rather than answering from the shards it could
/// reach. The message matters as much as the failure: an operator has to be
/// able to tell "a shard is missing" from any other error.
fn assert_refused(resp: &Value, what: &str) {
    assert!(
        !ok(resp),
        "{what}: router answered ok:true while a shard was unreachable — \
         this is the silent-partial-result bug: {resp}"
    );
    let err = resp["error"].as_str().unwrap_or_default().to_lowercase();
    assert!(
        !err.is_empty(),
        "{what}: failed without an error message: {resp}"
    );
}

struct Cluster {
    shards: Vec<Shard>,
    pool_addr: String,
    _pool: PoolProc,
}

/// 3 shards behind the real oxipool binary, seeded with `DOCS` docs.
fn setup(request_timeout_secs: u64) -> Cluster {
    let shards: Vec<Shard> = (0..N_SHARDS).map(|_| start_shard()).collect();
    let shard_list = shards
        .iter()
        .map(|s| s.addr.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let pool_addr = free_addr();
    let pool = PoolProc(
        Command::new(env!("CARGO_BIN_EXE_oxipool"))
            .env("OXIPOOL_LISTEN", &pool_addr)
            .env("OXIPOOL_SHARDS", &shard_list)
            .env("OXIPOOL_SHARD_KEYS", "accounts:region")
            .env("OXIPOOL_NUM_CHUNKS", "256")
            .env("OXIPOOL_REQUEST_TIMEOUT", request_timeout_secs.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn oxipool"),
    );
    wait_connectable(&pool_addr, Duration::from_secs(20));

    let mut c = Client::connect(&pool_addr);
    assert!(ok(&c.send(
        &json!({"cmd": "create_collection", "collection": "accounts"})
    )));
    for i in 0..DOCS {
        let resp = c.send(&json!({
            "cmd": "insert", "collection": "accounts",
            "doc": {"region": REGIONS[i % REGIONS.len()], "i": i}
        }));
        assert!(ok(&resp), "seed insert {i}: {resp}");
    }
    Cluster {
        shards,
        pool_addr,
        _pool: pool,
    }
}

/// Which shard holds a given region — the one we must avoid when we want a
/// routed query to survive, and target when we want it to fail.
fn shard_of_region(c: &Cluster, region: &str) -> usize {
    // Ask each shard directly; exactly one holds the region.
    for (i, s) in c.shards.iter().enumerate() {
        let mut direct = Client::connect(&s.addr.to_string());
        let resp = direct.send(&json!({
            "cmd": "count", "collection": "accounts", "query": {"region": region}
        }));
        if resp["data"]["count"].as_u64().unwrap_or(0) > 0 {
            return i;
        }
    }
    panic!("no shard holds region {region}");
}

#[test]
fn dead_shard_is_refused_not_silently_partial() {
    let c = setup(0); // seconds; 0 = disabled — a DOWN shard errors immediately anyway
    let mut pool = Client::connect(&c.pool_addr);

    // Baseline: the whole data set is visible.
    let total = pool.send(&json!({"cmd": "count", "collection": "accounts"}));
    assert_eq!(total["data"]["count"].as_u64().unwrap(), DOCS as u64);

    let victim_region = "eu";
    let victim = shard_of_region(&c, victim_region);
    let live_region = REGIONS
        .iter()
        .find(|r| shard_of_region(&c, r) != victim)
        .expect("a region on another shard");
    c.shards[victim].set(DOWN);

    // Every fan-out must refuse. The undercount is the whole danger: a
    // `count` of 40 instead of 60 looks like a perfectly good answer.
    assert_refused(
        &pool.send(&json!({"cmd": "count", "collection": "accounts"})),
        "count",
    );
    assert_refused(
        &pool.send(&json!({"cmd": "find", "collection": "accounts", "query": {}})),
        "find",
    );
    assert_refused(
        &pool.send(&json!({
            "cmd": "update", "collection": "accounts",
            "query": {}, "update": {"$set": {"touched": true}}
        })),
        "update",
    );
    assert_refused(
        &pool.send(&json!({"cmd": "delete", "collection": "accounts", "query": {"i": 999}})),
        "delete",
    );
    // DDL must not apply to a subset — that diverges the shards' schemas.
    assert_refused(
        &pool.send(&json!({"cmd": "create_collection", "collection": "later"})),
        "create_collection (broadcast)",
    );
    // A miss is only a miss if every shard was asked: the document could be
    // living on precisely the shard that is down.
    assert_refused(
        &pool.send(&json!({
            "cmd": "find_one", "collection": "accounts", "query": {"i": 100_000}
        })),
        "find_one (no match anywhere)",
    );

    // AVAILABILITY: one dead shard must not take down the others. A query
    // carrying the shard key routes to a single shard and must still answer.
    let routed = pool.send(&json!({
        "cmd": "count", "collection": "accounts", "query": {"region": live_region}
    }));
    assert!(
        ok(&routed),
        "a key-routed query to a LIVE shard must still answer while another \
         shard is down: {routed}"
    );
    assert_eq!(
        routed["data"]["count"].as_u64().unwrap(),
        (DOCS / REGIONS.len()) as u64
    );

    // RECOVERY: the pool must not stay poisoned once the shard is back.
    c.shards[victim].set(LIVE);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = pool.send(&json!({"cmd": "count", "collection": "accounts"}));
        if ok(&r) {
            assert_eq!(
                r["data"]["count"].as_u64().unwrap(),
                DOCS as u64,
                "after recovery the count must be whole again: {r}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "pool never recovered after the shard returned: {r}"
        );
        thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn blackholed_shard_times_out_and_the_pool_survives() {
    // A partitioned shard never answers, so only the deadline can end the
    // request. OXIPOOL_REQUEST_TIMEOUT is in SECONDS; 1 keeps the test quick
    // while still being a real deadline (the default is 30).
    let c = setup(1);
    let mut pool = Client::connect(&c.pool_addr);
    assert!(ok(
        &pool.send(&json!({"cmd": "count", "collection": "accounts"}))
    ));

    let victim = shard_of_region(&c, "eu");
    c.shards[victim].set(BLACKHOLE);

    // Repeat: each blackholed request borrows a pooled connection and must
    // give it back. If it doesn't, the pool drains and later requests hang
    // rather than erroring — the failure this timeout exists to prevent.
    for attempt in 1..=4 {
        let started = Instant::now();
        let resp = pool.send(&json!({"cmd": "count", "collection": "accounts"}));
        assert_refused(
            &resp,
            &format!("count over a blackholed shard (attempt {attempt})"),
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "attempt {attempt}: request took {:?} — the deadline is not being applied",
            started.elapsed()
        );
    }

    // The shard comes back; the pool must too.
    c.shards[victim].set(LIVE);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let r = pool.send(&json!({"cmd": "count", "collection": "accounts"}));
        if ok(&r) {
            assert_eq!(r["data"]["count"].as_u64().unwrap(), DOCS as u64);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "pool never recovered after the blackholed shard returned: {r}"
        );
        thread::sleep(Duration::from_millis(200));
    }
}
