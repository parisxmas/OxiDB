//! Hot-account contention benchmark — how does OCC behave when every
//! transaction touches the same document?
//!
//! The exchange-ledger pattern: every trade debits/credits two user
//! accounts AND credits a single fee account. That fee document becomes
//! a version-check hotspot: under OCC each concurrent commit that read
//! it gets `TransactionConflict` and must retry, so committed
//! throughput collapses as the hot ratio rises.
//!
//! This bench sweeps `hot_ratio` (the fraction of transfers that also
//! carry a fee leg to the one shared account) from 0.0 to 1.0 and
//! reports committed tx/s, conflict-aborts per commit, and latency
//! percentiles per completed transfer (retries included). After every
//! mode it asserts the money-conservation invariant, so a lost update
//! under contention fails the run loudly.
//!
//! Marked `#[ignore]` so `cargo test` stays fast; run with:
//!   cargo test --release --test hot_account_bench -- --ignored --nocapture
//!
//! Tunables (env): HOT_WORKERS (8), HOT_DURATION_SECS (5),
//! HOT_ACCOUNTS (1000), HOT_MAX_RETRIES (10000).

use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use oxidb::{Error, OxiDb};

const STARTING_BALANCE: i64 = 1_000_000;
const FEE: i64 = 1;
const MAX_AMOUNT: i64 = 100;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Deterministic xorshift64* — no `rand` dev-dependency needed.
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
    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[derive(Default)]
struct WorkerStats {
    commits: u64,
    conflicts: u64,
    insufficient: u64,
    give_ups: u64,
    /// Latency of each completed transfer in µs, retries included.
    latencies_us: Vec<u64>,
}

/// One transfer with retry-on-conflict. Returns (committed, conflicts_seen).
fn transfer_with_retries(
    db: &OxiDb,
    from: &str,
    to: &str,
    amount: i64,
    with_fee: bool,
    max_retries: usize,
    stats: &mut WorkerStats,
) -> bool {
    for _ in 0..max_retries {
        let tx = db.begin_transaction();

        let from_docs = match db.tx_find(tx, "accounts", &json!({"id": from})) {
            Ok(d) => d,
            Err(_) => {
                db.rollback_transaction(tx).ok();
                continue;
            }
        };
        let from_balance = from_docs[0]["balance"].as_i64().unwrap();
        let total_debit = amount + if with_fee { FEE } else { 0 };
        if from_balance < total_debit {
            db.rollback_transaction(tx).ok();
            stats.insufficient += 1;
            return false;
        }

        let dec: Value = json!({ "$inc": { "balance": -total_debit } });
        let inc: Value = json!({ "$inc": { "balance": amount } });
        let ok = db
            .tx_update(tx, "accounts", &json!({"id": from}), &dec)
            .and_then(|_| db.tx_update(tx, "accounts", &json!({"id": to}), &inc))
            .and_then(|_| {
                if with_fee {
                    db.tx_update(
                        tx,
                        "accounts",
                        &json!({"id": "fee"}),
                        &json!({ "$inc": { "balance": FEE } }),
                    )
                    .map(|_| ())
                } else {
                    Ok(())
                }
            });
        if ok.is_err() {
            db.rollback_transaction(tx).ok();
            continue;
        }

        match db.commit_transaction(tx) {
            Ok(()) => {
                stats.commits += 1;
                return true;
            }
            Err(Error::TransactionConflict { .. }) => {
                stats.conflicts += 1;
                // Immediate retry — mirrors a client library's tight loop.
                continue;
            }
            Err(_) => {
                // Non-conflict commit failure: don't spin on it.
                return false;
            }
        }
    }
    stats.give_ups += 1;
    false
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn run_mode(
    db: &Arc<OxiDb>,
    hot_ratio: f64,
    workers: usize,
    duration: Duration,
    n_accounts: usize,
    max_retries: usize,
) -> WorkerStats {
    let stop = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..workers)
        .map(|w| {
            let db = Arc::clone(db);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut rng = Rng::new((w as u64 + 1) * 7_919 + (hot_ratio * 1000.0) as u64);
                let mut stats = WorkerStats::default();
                while !stop.load(Ordering::Relaxed) {
                    let from_i = rng.below(n_accounts as u64);
                    let mut to_i = rng.below(n_accounts as u64);
                    if to_i == from_i {
                        to_i = (to_i + 1) % n_accounts as u64;
                    }
                    let from = format!("acct-{from_i}");
                    let to = format!("acct-{to_i}");
                    let amount = 1 + rng.below(MAX_AMOUNT as u64) as i64;
                    let with_fee = rng.unit() < hot_ratio;

                    let t0 = Instant::now();
                    let committed = transfer_with_retries(
                        &db,
                        &from,
                        &to,
                        amount,
                        with_fee,
                        max_retries,
                        &mut stats,
                    );
                    if committed {
                        stats.latencies_us.push(t0.elapsed().as_micros() as u64);
                    }
                }
                stats
            })
        })
        .collect();

    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let mut total = WorkerStats::default();
    for h in handles {
        let s = h.join().unwrap();
        total.commits += s.commits;
        total.conflicts += s.conflicts;
        total.insufficient += s.insufficient;
        total.give_ups += s.give_ups;
        total.latencies_us.extend(s.latencies_us);
    }
    total
}

#[test]
#[ignore]
fn hot_account_contention_sweep() {
    let workers = env_usize("HOT_WORKERS", 8);
    let duration_secs = env_usize("HOT_DURATION_SECS", 5);
    let n_accounts = env_usize("HOT_ACCOUNTS", 1000);
    let max_retries = env_usize("HOT_MAX_RETRIES", 10_000);
    let duration = Duration::from_secs(duration_secs as u64);

    let dir = tempdir().unwrap();
    let db = Arc::new(OxiDb::open(dir.path()).unwrap());

    // Seed: n user accounts + the single hot fee account.
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
    let expected_total = STARTING_BALANCE * n_accounts as i64;

    println!(
        "\nhot-account contention sweep — {workers} workers, {duration_secs}s/mode, \
         {n_accounts} accounts, 1 shared fee account"
    );
    println!(
        "{:>9} | {:>9} | {:>9} | {:>13} | {:>9} | {:>9} | {:>9} | {:>8}",
        "hot_ratio", "commits", "tx/s", "conflicts/tx", "p50 µs", "p99 µs", "max µs", "give-ups"
    );
    println!("{}", "-".repeat(96));

    for &hot_ratio in &[0.0, 0.1, 0.5, 1.0] {
        let mut stats = run_mode(&db, hot_ratio, workers, duration, n_accounts, max_retries);
        stats.latencies_us.sort_unstable();

        let tx_s = stats.commits as f64 / duration_secs as f64;
        let conflicts_per_commit = if stats.commits > 0 {
            stats.conflicts as f64 / stats.commits as f64
        } else {
            f64::NAN
        };
        println!(
            "{:>9.2} | {:>9} | {:>9.0} | {:>13.2} | {:>9} | {:>9} | {:>9} | {:>8}",
            hot_ratio,
            stats.commits,
            tx_s,
            conflicts_per_commit,
            percentile(&stats.latencies_us, 0.50),
            percentile(&stats.latencies_us, 0.99),
            stats.latencies_us.last().copied().unwrap_or(0),
            stats.give_ups,
        );

        // Money-conservation invariant: transfers and fee legs only move
        // balance between documents, so the sum must never drift. A lost
        // update under contention shows up here.
        let total: i64 = db
            .find("accounts", &json!({}))
            .unwrap()
            .iter()
            .map(|d| d["balance"].as_i64().unwrap())
            .sum();
        assert_eq!(
            total, expected_total,
            "money conservation violated after hot_ratio={hot_ratio}: \
             total {total} != expected {expected_total}"
        );
    }
}
