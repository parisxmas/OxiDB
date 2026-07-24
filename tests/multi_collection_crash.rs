//! Multi-collection crash atomicity — a real exchange order is not a
//! single-collection write. Filling an order atomically touches:
//!
//!   receipts  — idempotency key (unique request_id)
//!   accounts  — debit taker (amount+fee), credit maker, credit fee acct
//!   trades    — the executed-trade record
//!   orders    — the order marked "filled"
//!   journal   — double-entry lines (must sum to zero)
//!
//! all in ONE transaction. Under a crash, recovery must be
//! all-collections-or-none: a trade without its journal lines, an order
//! filled without the balance debit, or a receipt without the trade
//! would each be a cross-collection atomicity violation — and in a
//! ledger, a corruption you can't reconcile.
//!
//! This SIGKILLs a victim mid-workload (self-spawn pattern), reopens,
//! and checks — across five collections simultaneously — that every
//! uid is present in ALL of them or NONE, that acked orders survived
//! whole, that money is conserved, that the journal balances, and that
//! every account's balance is exactly reproducible from the journal.
//! Rounds accumulate on one data dir, so recovery-of-recovery is tested.
//!
//! Run with:
//!   cargo test --release --test multi_collection_crash -- --ignored --nocapture

#![cfg(unix)]

use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use oxidb::{Error, OxiDb};

const N_ACCOUNTS: usize = 100;
const START_BALANCE: i64 = 1_000_000;
const MAX_AMOUNT: i64 = 500;
const FEE: i64 = 1;
const WORKERS: usize = 6;

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn setup(db: &OxiDb) {
    for i in 0..N_ACCOUNTS {
        db.insert(
            "accounts",
            json!({"id": format!("acct-{i}"), "bal": START_BALANCE}),
        )
        .unwrap();
    }
    db.insert("accounts", json!({"id": "fee", "bal": 0}))
        .unwrap();
    db.create_index("accounts", "id").unwrap();
    db.create_unique_index("receipts", "uid").unwrap();
    db.create_index("trades", "uid").unwrap();
    db.create_index("orders", "uid").unwrap();
    db.create_index("journal", "uid").unwrap();
}

/// Fill one order as a single 5-collection transaction. Returns
/// Ok(true) applied, Ok(false) already applied (idempotent retry),
/// Err for conflict / other.
fn fill_order(db: &OxiDb, uid: &str, taker: &str, maker: &str, amount: i64) -> Result<bool, Error> {
    let tx = db.begin_transaction();
    let debit = amount + FEE;
    let steps = (|| -> Result<(), Error> {
        db.tx_insert(tx, "receipts", json!({"uid": uid}))?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": taker}),
            &json!({"$inc": {"bal": -debit}}),
        )?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": maker}),
            &json!({"$inc": {"bal": amount}}),
        )?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": "fee"}),
            &json!({"$inc": {"bal": FEE}}),
        )?;
        db.tx_insert(
            tx,
            "trades",
            json!({"uid": uid, "taker": taker, "maker": maker, "amount": amount}),
        )?;
        db.tx_insert(tx, "orders", json!({"uid": uid, "status": "filled"}))?;
        db.tx_insert(
            tx,
            "journal",
            json!({"uid": uid, "acct": taker, "delta": -debit}),
        )?;
        db.tx_insert(
            tx,
            "journal",
            json!({"uid": uid, "acct": maker, "delta": amount}),
        )?;
        db.tx_insert(
            tx,
            "journal",
            json!({"uid": uid, "acct": "fee", "delta": FEE}),
        )?;
        Ok(())
    })();
    if let Err(e) = steps {
        db.rollback_transaction(tx).ok();
        return Err(e);
    }
    match db.commit_transaction(tx) {
        Ok(()) => Ok(true),
        Err(Error::UniqueViolation { .. }) => Ok(false),
        Err(e) => Err(e),
    }
}

// ── Victim role ──────────────────────────────────────────────────────

fn run_victim() -> ! {
    let path = std::env::var("OXIDB_VICTIM_DATA").expect("victim: OXIDB_VICTIM_DATA");
    let round: u64 = std::env::var("OXIDB_MC_ROUND").unwrap().parse().unwrap();
    let db = Arc::new(OxiDb::open(std::path::Path::new(&path)).expect("victim: open"));
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let seq = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let db = Arc::clone(&db);
        let out = Arc::clone(&out);
        let seq = Arc::clone(&seq);
        handles.push(thread::spawn(move || {
            let mut rng = Rng::new(round * 1_000_003 + w as u64 * 7919 + 1);
            loop {
                let taker_i = rng.below(N_ACCOUNTS as u64);
                let mut maker_i = rng.below(N_ACCOUNTS as u64);
                if maker_i == taker_i {
                    maker_i = (maker_i + 1) % N_ACCOUNTS as u64;
                }
                let taker = format!("acct-{taker_i}");
                let maker = format!("acct-{maker_i}");
                let amount = 1 + rng.below(MAX_AMOUNT as u64) as i64;
                let uid = format!("r{round}-w{w}-{}", seq.fetch_add(1, Ordering::Relaxed));

                match fill_order(&db, &uid, &taker, &maker, amount) {
                    Ok(true) => {
                        let mut o = out.lock().unwrap();
                        writeln!(o, "OK {uid}").unwrap();
                        o.flush().unwrap();
                    }
                    Ok(false) => {}
                    Err(Error::TransactionConflict { .. }) => {}
                    Err(e) => panic!("victim: {e}"),
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    unreachable!()
}

// ── Cross-collection checker ─────────────────────────────────────────

fn check(db: &OxiDb, acked: &HashSet<String>, round: usize) {
    let trades = db.find("trades", &json!({})).unwrap();
    let orders = db.find("orders", &json!({})).unwrap();
    let receipts = db.find("receipts", &json!({})).unwrap();
    let journal = db.find("journal", &json!({})).unwrap();

    let trade_uids: HashSet<String> = trades
        .iter()
        .map(|d| d["uid"].as_str().unwrap().to_string())
        .collect();
    let order_uids: HashSet<String> = orders
        .iter()
        .map(|d| d["uid"].as_str().unwrap().to_string())
        .collect();
    let receipt_uids: HashSet<String> = receipts
        .iter()
        .map(|d| d["uid"].as_str().unwrap().to_string())
        .collect();

    // Uniqueness: exactly one trade / order / receipt per uid.
    assert_eq!(
        trade_uids.len(),
        trades.len(),
        "round {round}: duplicate trade uid"
    );
    assert_eq!(
        order_uids.len(),
        orders.len(),
        "round {round}: duplicate order uid"
    );
    assert_eq!(
        receipt_uids.len(),
        receipts.len(),
        "round {round}: duplicate receipt uid"
    );

    // Journal lines per uid (must be exactly 3 for a present order).
    let mut journal_lines: HashMap<String, u32> = HashMap::new();
    for d in &journal {
        *journal_lines
            .entry(d["uid"].as_str().unwrap().to_string())
            .or_default() += 1;
    }

    // ── All-collections-or-none: every uid seen anywhere must be
    // present in trades, orders, receipts (1 each) AND journal (3). ──
    let mut all_uids: HashSet<String> = HashSet::new();
    all_uids.extend(trade_uids.iter().cloned());
    all_uids.extend(order_uids.iter().cloned());
    all_uids.extend(receipt_uids.iter().cloned());
    all_uids.extend(journal_lines.keys().cloned());
    for uid in &all_uids {
        let in_trades = trade_uids.contains(uid);
        let in_orders = order_uids.contains(uid);
        let in_receipts = receipt_uids.contains(uid);
        let jlines = journal_lines.get(uid).copied().unwrap_or(0);
        assert!(
            in_trades && in_orders && in_receipts && jlines == 3,
            "round {round}: PARTIAL COMMIT for {uid} — trades={in_trades} \
             orders={in_orders} receipts={in_receipts} journal_lines={jlines} \
             (a transaction's effects split across collections on recovery)"
        );
    }

    // Every ACKed order is fully present.
    for uid in acked {
        assert!(
            trade_uids.contains(uid),
            "round {round}: ACKed order {uid} lost after crash"
        );
    }

    // Journal double-entry: all deltas sum to zero.
    let journal_sum: i64 = journal.iter().map(|d| d["delta"].as_i64().unwrap()).sum();
    assert_eq!(
        journal_sum, 0,
        "round {round}: journal doesn't balance (Σ deltas != 0)"
    );

    // Money conservation.
    let accounts = db.find("accounts", &json!({})).unwrap();
    let total: i64 = accounts.iter().map(|d| d["bal"].as_i64().unwrap()).sum();
    assert_eq!(
        total,
        START_BALANCE * N_ACCOUNTS as i64,
        "round {round}: money not conserved"
    );

    // Balance reproducible from the journal: bal(acct) == initial + Σ journal deltas.
    let mut net: HashMap<String, i64> = HashMap::new();
    for d in &journal {
        *net.entry(d["acct"].as_str().unwrap().to_string())
            .or_default() += d["delta"].as_i64().unwrap();
    }
    for a in &accounts {
        let id = a["id"].as_str().unwrap();
        let bal = a["bal"].as_i64().unwrap();
        let initial = if id == "fee" { 0 } else { START_BALANCE };
        assert_eq!(
            bal,
            initial + net.get(id).copied().unwrap_or(0),
            "round {round}: account {id} balance not reproducible from journal"
        );
    }

    println!(
        "round {round}: OK — {} orders across 5 collections, all atomic \
         (acked={}, journal balanced, money conserved, balances reconcile)",
        trades.len(),
        acked.len()
    );
}

#[test]
#[ignore]
fn orders_are_atomic_across_collections_under_crash() {
    if std::env::var("OXIDB_MC_VICTIM").as_deref() == Ok("1") {
        run_victim();
    }
    let rounds = env_usize("MC_ROUNDS", 12);
    let min_acks = env_usize("MC_MIN_ACKS", 40);

    let dir = tempdir().unwrap();
    {
        let db = OxiDb::open(dir.path()).unwrap();
        setup(&db);
    }
    let exe = std::env::current_exe().unwrap();
    let mut acked: HashSet<String> = HashSet::new();
    let mut kill_rng = Rng::new(0xA71C);

    println!("\nmulti-collection crash atomicity: {rounds} rounds x SIGKILL, 5 collections/order");

    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .arg("--ignored")
            .arg("--nocapture")
            .arg("orders_are_atomic_across_collections_under_crash")
            .env("OXIDB_MC_VICTIM", "1")
            .env("OXIDB_VICTIM_DATA", dir.path())
            .env("OXIDB_MC_ROUND", round.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn victim");

        let stdout = child.stdout.take().unwrap();
        let round_acks: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let reader_acks = Arc::clone(&round_acks);
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(uid) = line.strip_prefix("OK ") {
                    reader_acks.lock().unwrap().insert(uid.to_string());
                }
            }
        });

        let start = Instant::now();
        loop {
            if round_acks.lock().unwrap().len() >= min_acks {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "round {round}: victim produced < {min_acks} acks in 60s"
            );
            thread::sleep(Duration::from_millis(10));
        }
        thread::sleep(Duration::from_millis(kill_rng.below(1200)));

        child.kill().expect("SIGKILL");
        child.wait().expect("reap");
        reader.join().unwrap();
        acked.extend(Arc::try_unwrap(round_acks).unwrap().into_inner().unwrap());

        let db = OxiDb::open(dir.path()).unwrap();
        check(&db, &acked, round);
        drop(db);
    }
}
