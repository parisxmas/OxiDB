//! CERN-grade ACID test #1 — concurrent transfers must not create or destroy money.
//!
//! Category 1 (ACID & isolation) in `docs/testing-roadmap.md`. The classic
//! money-transfer atomicity invariant: N threads each move random amounts
//! between two accounts inside transactions; after the storm settles,
//! `sum(balances)` MUST still equal the starting total — money can't be
//! created or destroyed by any combination of concurrent commits.
//!
//! Catches: lost update, partial commit, isolation breakage on the
//! critical-section path, WAL replay correctness for committed txns.
//!
//! Marked `#[ignore]` so `cargo test` stays fast; run with:
//!   cargo test --test cern_acid_transfers -- --ignored
//!
//! Observed during initial development: this invariant failed once in
//! 8 runs (money was created — total = 2_001_836 vs expected 2_000_000)
//! on a cold-start, then passed 7×7 subsequent runs. The anomaly hasn't
//! reproduced since, but the test stays exactly as written — a rare
//! engine race that fires under specific thread interleaving is *the*
//! shape of bug this category of test exists to surface, and we'd
//! rather see it fail loudly the next time it happens than have a
//! looser invariant paper over the signal.

use serde_json::{Value, json};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

use oxidb::OxiDb;

const STARTING_BALANCE: i64 = 1_000_000;
const NUM_THREADS: usize = 8;
const TRANSFERS_PER_THREAD: usize = 500;
const MAX_RETRIES_PER_TRANSFER: usize = 200;

fn balance_of(db: &OxiDb, account: &str) -> i64 {
    let doc = db
        .find_one("accounts", &json!({"id": account}))
        .unwrap()
        .expect("account must exist");
    doc["balance"].as_i64().unwrap()
}

fn total_balance(db: &OxiDb) -> i64 {
    db.find("accounts", &json!({}))
        .unwrap()
        .iter()
        .map(|d| d["balance"].as_i64().unwrap())
        .sum()
}

/// One transfer attempt — returns Ok on commit, Err on commit conflict
/// (caller retries) or insufficient-funds (caller drops the transfer).
fn try_transfer(db: &OxiDb, from: &str, to: &str, amount: i64) -> Result<(), &'static str> {
    let tx = db.begin_transaction();

    let from_docs = db
        .tx_find(tx, "accounts", &json!({"id": from}))
        .map_err(|_| "tx_find from")?;
    if from_docs.is_empty() {
        db.rollback_transaction(tx).ok();
        return Err("from account missing");
    }
    let from_balance = from_docs[0]["balance"].as_i64().unwrap();
    if from_balance < amount {
        db.rollback_transaction(tx).ok();
        return Err("insufficient funds");
    }

    let dec: Value = json!({ "$inc": { "balance": -amount } });
    let inc: Value = json!({ "$inc": { "balance":  amount } });
    if db
        .tx_update(tx, "accounts", &json!({"id": from}), &dec)
        .is_err()
    {
        db.rollback_transaction(tx).ok();
        return Err("tx_update from");
    }
    if db
        .tx_update(tx, "accounts", &json!({"id": to}), &inc)
        .is_err()
    {
        db.rollback_transaction(tx).ok();
        return Err("tx_update to");
    }

    db.commit_transaction(tx).map_err(|_| "commit conflict")?;
    Ok(())
}

#[test]
#[ignore]
fn no_money_lost_under_concurrent_transfers() {
    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());

    db.insert("accounts", json!({"id": "a", "balance": STARTING_BALANCE}))
        .unwrap();
    db.insert("accounts", json!({"id": "b", "balance": STARTING_BALANCE}))
        .unwrap();

    let initial_total = STARTING_BALANCE * 2;
    assert_eq!(total_balance(&db), initial_total, "setup must net out");

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|tid| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                // Use a per-thread deterministic-ish seed for reproducibility.
                let mut state: u64 = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(tid as u64 + 1);
                let mut committed = 0usize;
                let mut conflicts = 0usize;
                let mut insufficient = 0usize;

                for _ in 0..TRANSFERS_PER_THREAD {
                    // xorshift for cheap PRNG without bringing rand into the
                    // dev-deps; we don't need cryptographic quality.
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let amount = 1 + (state % 1_000) as i64;
                    // Include self-transfers (a->a, b->b): two updates to
                    // the same document in one transaction. They net zero,
                    // so the money-conservation invariant must still hold —
                    // this exercises the same-doc write composition that a
                    // prior commit-prepare bug clobbered.
                    let (from, to) = match state % 4 {
                        0 => ("a", "b"),
                        1 => ("b", "a"),
                        2 => ("a", "a"),
                        _ => ("b", "b"),
                    };

                    let mut retry = 0;
                    loop {
                        match try_transfer(&db, from, to, amount) {
                            Ok(()) => {
                                committed += 1;
                                break;
                            }
                            Err("commit conflict") | Err("tx_update from")
                            | Err("tx_update to") | Err("tx_find from") => {
                                conflicts += 1;
                                retry += 1;
                                if retry > MAX_RETRIES_PER_TRANSFER {
                                    panic!("transfer retried > {} times — \
                                            either the engine is making no progress \
                                            or the conflict resolution is broken",
                                           MAX_RETRIES_PER_TRANSFER);
                                }
                                // Small jitter so threads don't lockstep.
                                thread::sleep(std::time::Duration::from_micros(state & 0xff));
                            }
                            Err("insufficient funds") => {
                                insufficient += 1;
                                break;
                            }
                            Err(other) => panic!("unexpected transfer error: {other}"),
                        }
                    }
                }
                eprintln!(
                    "[tid {tid}] committed={committed} conflicts={conflicts} insufficient={insufficient}"
                );
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let final_total = total_balance(&db);
    assert_eq!(
        final_total, initial_total,
        "money must be conserved across concurrent transfers \
         (started with {initial_total}, ended with {final_total})"
    );

    // Also: neither account went negative (insufficient-funds check held).
    let a = balance_of(&db, "a");
    let b = balance_of(&db, "b");
    assert!(a >= 0, "account a went negative: {a}");
    assert!(b >= 0, "account b went negative: {b}");
}
