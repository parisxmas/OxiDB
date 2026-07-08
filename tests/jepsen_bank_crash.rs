//! Jepsen-style bank test with crash faults — history-checked ACID
//! verification of the transaction engine under repeated SIGKILL.
//!
//! The Jepsen "bank" workload adapted to a single-node embedded engine:
//! concurrent clients move money between accounts (plus a fee leg to one
//! hot shared account, exercising both the OCC and the
//! `tx_find_for_update` commit paths), every transfer carries a unique
//! journal document *inside the same transaction*, and the parent
//! process records which transfers were ACKed. A fault injector then
//! SIGKILLs the victim process at a random moment — mid-fsync,
//! mid-apply, anywhere — the parent reopens the data dir (WAL + commit-
//! log recovery) and checks the recovered state against the recorded
//! history:
//!
//!   1. **Durability** — every ACKed transfer's journal entry exists.
//!      `commit_transaction` acks only after the WAL fsync AND the
//!      commit-log fsync, so an ack the parent saw must survive.
//!   2. **Atomicity** — every account's balance equals its initial
//!      balance plus the net of ALL journal entries touching it. A
//!      half-applied transfer (balance moved without its journal doc,
//!      or vice versa) breaks the equation. Unacked-but-present
//!      transfers are legal (crash between durability and ack) — they
//!      must still be *complete*.
//!   3. **No double-replay** — journal uids are unique; a WAL entry
//!      applied twice would show up as a duplicate.
//!   4. **Conservation** — the global sum never drifts.
//!
//! Rounds accumulate on one data dir, so each round also verifies
//! recovery-after-recovery. What this does NOT cover (future work,
//! needs the cluster): network partitions and Raft fault injection.
//!
//! Self-spawn pattern (same as cern_sigkill_drill): this test binary
//! runs as the victim when `OXIDB_JEPSEN_VICTIM=1`.
//!
//! Marked `#[ignore]`; run with:
//!   cargo test --release --test jepsen_bank_crash -- --ignored --nocapture
//!
//! Tunables (env): JEPSEN_ROUNDS (5), JEPSEN_WORKERS (6),
//! JEPSEN_ACCOUNTS (200), JEPSEN_MIN_ACKS (50).

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

const STARTING_BALANCE: i64 = 1_000_000;
const FEE: i64 = 1;
const MAX_AMOUNT: i64 = 100;
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
/// Hard deadline for a round to gather its minimum ack count.
const ACK_DEADLINE: Duration = Duration::from_secs(60);

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Deterministic xorshift64* (no rand dev-dependency).
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

// ---------------------------------------------------------------------------
// Victim role
// ---------------------------------------------------------------------------

/// One transfer transaction: journal insert + three balance $incs, all
/// atomic. Returns the uid on commit. `for_update` selects the
/// pessimistic-lock path; both paths must uphold the invariants.
fn try_transfer(
    db: &OxiDb,
    uid: &str,
    from: &str,
    to: &str,
    amount: i64,
    for_update: bool,
) -> Result<bool, Error> {
    let tx = db.begin_transaction();

    let read = |acct: &str| -> Result<Option<i64>, Error> {
        let docs = if for_update {
            db.tx_find_for_update(tx, "accounts", &json!({"id": acct}), LOCK_TIMEOUT)?
        } else {
            db.tx_find(tx, "accounts", &json!({"id": acct}))?
        };
        Ok(docs.first().and_then(|d| d["balance"].as_i64()))
    };

    // Lock order (for_update path): sorted account ids, then "fee",
    // which sorts after every "acct-*" — globally consistent, no
    // deadlock.
    let (first, second) = if from < to { (from, to) } else { (to, from) };
    let r = (|| -> Result<Option<i64>, Error> {
        let a = read(first)?;
        let b = read(second)?;
        read("fee")?;
        Ok(if first == from { a } else { b })
    })();
    let from_balance = match r {
        Ok(Some(b)) => b,
        Ok(None) => {
            db.rollback_transaction(tx).ok();
            return Ok(false);
        }
        Err(e) => {
            db.rollback_transaction(tx).ok();
            return Err(e);
        }
    };
    if from_balance < amount + FEE {
        db.rollback_transaction(tx).ok();
        return Ok(false);
    }

    let steps = (|| -> Result<(), Error> {
        db.tx_insert(
            tx,
            "journal",
            json!({"uid": uid, "from": from, "to": to, "amount": amount, "fee": FEE}),
        )?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": from}),
            &json!({"$inc": {"balance": -(amount + FEE)}}),
        )?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": to}),
            &json!({"$inc": {"balance": amount}}),
        )?;
        db.tx_update(
            tx,
            "accounts",
            &json!({"id": "fee"}),
            &json!({"$inc": {"balance": FEE}}),
        )?;
        Ok(())
    })();
    if let Err(e) = steps {
        db.rollback_transaction(tx).ok();
        return Err(e);
    }

    match db.commit_transaction(tx) {
        Ok(()) => Ok(true),
        Err(e) => Err(e),
    }
}

/// Victim loop: hammer the bank with concurrent transfers, ack each
/// committed uid on stdout, run until SIGKILLed. Half the workers use
/// the OCC path, half `tx_find_for_update`.
fn run_victim() -> ! {
    let path =
        std::env::var("OXIDB_VICTIM_DATA").expect("victim role: OXIDB_VICTIM_DATA must be set");
    let round: u64 = std::env::var("OXIDB_JEPSEN_ROUND")
        .expect("victim role: OXIDB_JEPSEN_ROUND must be set")
        .parse()
        .unwrap();
    let workers = env_usize("JEPSEN_WORKERS", 6);
    let n_accounts = env_usize("JEPSEN_ACCOUNTS", 200);

    let db = Arc::new(OxiDb::open(std::path::Path::new(&path)).expect("victim role: open db"));
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let seq = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for w in 0..workers {
        let db = Arc::clone(&db);
        let out = Arc::clone(&out);
        let seq = Arc::clone(&seq);
        handles.push(thread::spawn(move || {
            let mut rng = Rng::new(round * 1_000_003 + w as u64 * 7_919 + 1);
            let for_update = w % 2 == 0;
            loop {
                let from_i = rng.below(n_accounts as u64);
                let mut to_i = rng.below(n_accounts as u64);
                if to_i == from_i {
                    to_i = (to_i + 1) % n_accounts as u64;
                }
                let from = format!("acct-{from_i}");
                let to = format!("acct-{to_i}");
                let amount = 1 + rng.below(MAX_AMOUNT as u64) as i64;
                let uid = format!("r{round}-w{w}-{}", seq.fetch_add(1, Ordering::Relaxed));

                match try_transfer(&db, &uid, &from, &to, amount, for_update) {
                    Ok(true) => {
                        // Ack ONLY after commit_transaction returned Ok —
                        // by then the WAL and commit-log fsyncs are done.
                        let mut o = out.lock().unwrap();
                        writeln!(o, "OK {uid}").expect("victim role: ack write");
                        o.flush().expect("victim role: ack flush");
                    }
                    Ok(false) => {}
                    Err(Error::TransactionConflict { .. }) | Err(Error::LockTimeout { .. }) => {
                        // Retry with a fresh uid next iteration.
                    }
                    Err(e) => panic!("victim role: unexpected error: {e}"),
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    unreachable!("victim workers never return");
}

// ---------------------------------------------------------------------------
// Checker
// ---------------------------------------------------------------------------

/// Verify the recovered state against the ACK history and the journal.
fn check_state(db: &OxiDb, acked: &HashSet<String>, n_accounts: usize, round: usize) {
    let journal = db.find("journal", &json!({})).unwrap();

    // 3. No double-replay: journal uids unique.
    let mut uids: HashSet<&str> = HashSet::with_capacity(journal.len());
    for doc in &journal {
        let uid = doc["uid"].as_str().expect("journal doc has uid");
        assert!(
            uids.insert(uid),
            "round {round}: duplicate journal uid {uid} — a transaction was applied twice"
        );
    }

    // 1. Durability: every acked uid is present.
    let mut lost = 0usize;
    for uid in acked {
        if !uids.contains(uid.as_str()) {
            eprintln!("round {round}: ACKED-BUT-LOST {uid}");
            lost += 1;
        }
    }
    assert_eq!(
        lost, 0,
        "round {round}: {lost} acknowledged transfers vanished after crash recovery"
    );

    // 2+4. Atomicity + conservation: replay the journal against the
    // initial balances; every account must match exactly.
    let mut net: HashMap<String, i64> = HashMap::new();
    for doc in &journal {
        let from = doc["from"].as_str().unwrap().to_string();
        let to = doc["to"].as_str().unwrap().to_string();
        let amount = doc["amount"].as_i64().unwrap();
        let fee = doc["fee"].as_i64().unwrap();
        *net.entry(from).or_insert(0) -= amount + fee;
        *net.entry(to).or_insert(0) += amount;
        *net.entry("fee".to_string()).or_insert(0) += fee;
    }

    let accounts = db.find("accounts", &json!({})).unwrap();
    assert_eq!(
        accounts.len(),
        n_accounts + 1,
        "round {round}: account count changed"
    );
    let mut total = 0i64;
    for doc in &accounts {
        let id = doc["id"].as_str().unwrap();
        let balance = doc["balance"].as_i64().unwrap();
        total += balance;
        let initial = if id == "fee" { 0 } else { STARTING_BALANCE };
        let expected = initial + net.get(id).copied().unwrap_or(0);
        assert_eq!(
            balance, expected,
            "round {round}: account {id} balance {balance} != initial {initial} + journal net \
             {} — a transaction was applied partially",
            net.get(id).copied().unwrap_or(0)
        );
    }
    assert_eq!(
        total,
        STARTING_BALANCE * n_accounts as i64,
        "round {round}: money conservation violated"
    );

    println!(
        "round {round}: OK — journal={} acked={} (unacked-but-durable={}) accounts verified",
        journal.len(),
        acked.len(),
        journal.len().saturating_sub(acked.len()),
    );
}

// ---------------------------------------------------------------------------
// Parent / fault injector
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn jepsen_bank_survives_sigkill_rounds() {
    if std::env::var("OXIDB_JEPSEN_VICTIM").as_deref() == Ok("1") {
        run_victim();
    }

    let rounds = env_usize("JEPSEN_ROUNDS", 5);
    let workers = env_usize("JEPSEN_WORKERS", 6);
    let n_accounts = env_usize("JEPSEN_ACCOUNTS", 200);
    let min_acks = env_usize("JEPSEN_MIN_ACKS", 50);

    let dir = tempdir().unwrap();
    let data = dir.path().to_str().unwrap().to_string();

    // Seed once, crash-free, then close so the victim owns the dir.
    {
        let db = OxiDb::open(dir.path()).unwrap();
        for i in 0..n_accounts {
            db.insert(
                "accounts",
                json!({"id": format!("acct-{i}"), "balance": STARTING_BALANCE}),
            )
            .unwrap();
        }
        db.insert("accounts", json!({"id": "fee", "balance": 0}))
            .unwrap();
        db.create_index("accounts", "id").unwrap();
        db.create_index("journal", "uid").unwrap();
    }

    let exe = std::env::current_exe().unwrap();
    let mut acked: HashSet<String> = HashSet::new();
    let mut kill_rng = Rng::new(0xC0FFEE);

    println!(
        "\njepsen bank: {rounds} rounds × SIGKILL — {workers} workers, {n_accounts} accounts, \
         mixed occ/for-update"
    );

    for round in 1..=rounds {
        let mut child = Command::new(&exe)
            .arg("--ignored")
            .arg("--nocapture")
            .arg("jepsen_bank_survives_sigkill_rounds")
            .env("OXIDB_JEPSEN_VICTIM", "1")
            .env("OXIDB_VICTIM_DATA", &data)
            .env("OXIDB_JEPSEN_ROUND", round.to_string())
            .env("JEPSEN_WORKERS", workers.to_string())
            .env("JEPSEN_ACCOUNTS", n_accounts.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn victim");

        // Reader thread drains acks into the shared history; it keeps
        // reading until EOF, so acks the victim flushed just before the
        // SIGKILL still land in the history.
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

        // Fault schedule: wait for min_acks (proof the workload is
        // live), then a random extra 0–1500 ms, then SIGKILL.
        let start = Instant::now();
        loop {
            if round_acks.lock().unwrap().len() >= min_acks {
                break;
            }
            assert!(
                start.elapsed() < ACK_DEADLINE,
                "round {round}: victim produced <{min_acks} acks in {ACK_DEADLINE:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }
        thread::sleep(Duration::from_millis(kill_rng.below(1500)));

        child.kill().expect("SIGKILL victim"); // SIGKILL on unix
        child.wait().expect("reap victim");
        reader.join().unwrap();

        let this_round = Arc::try_unwrap(round_acks).unwrap().into_inner().unwrap();
        acked.extend(this_round);

        // Recovery + history check on the accumulated state.
        let db = OxiDb::open(dir.path()).unwrap();
        check_state(&db, &acked, n_accounts, round);
        drop(db);
    }
}
