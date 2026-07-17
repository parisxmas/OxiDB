//! TLP — Ternary Logic Partitioning: a metamorphic oracle for the WHERE clause.
//!
//! The problem this solves is that we can generate a million random queries but
//! we do not know any of their answers, so we cannot tell whether the engine is
//! lying. TLP sidesteps knowing: for any predicate `p`, every row is in exactly
//! one of three buckets — `p` is TRUE, `p` is FALSE, or `p` is NULL. So
//!
//!     {rows where p}  ⊎  {rows where NOT p}  ⊎  {rows where p IS NULL}
//!         must equal  {all rows}
//!
//! as a multiset, whatever `p` is and whatever the right answer happens to be.
//! When it does not, the engine has a bug, and we did not need an oracle that
//! knows SQL to find it. (SQLancer, which introduced TLP, found hundreds of
//! logic bugs in SQLite/MySQL/Postgres/TiDB this way.)
//!
//! Why this engine needs it. Every one of `scan_pruned`, the allocation-free
//! LIKE fast paths, the AND/OR short-circuit, `index_lookup_eq` and the
//! contiguous scan cache rests on the same unproven claim: *the fast path
//! returns what the slow path would*. Hand-written tests check the cases we
//! thought of. This checks the ones we did not — and the case that already bit
//! us once (the 0.31.1 `$ne`/`$nin` data-loss bug) was exactly this shape: the
//! index path and the scan path disagreed about NULL-ish rows.
//!
//! **What TLP does not catch, and it matters.** The invariant is that the three
//! partitions are complete and disjoint — not that each row lands in the *right*
//! one. A uniformly wrong three-valued logic passes: mutate `NULL AND x` to
//! FALSE and the affected rows simply move from the NULL bucket to the FALSE
//! bucket, the union is unchanged, and this file stays green. (Verified — that
//! was the first mutation tried here, and it sailed through.) TLP earns its
//! keep on the *other* class: when the three partitions are served by different
//! code paths and one of them is wrong. `WHERE a = 8` can use the index while
//! `WHERE NOT (a = 8)` cannot, so a row lost by `index_lookup_eq` falls out of
//! every partition and the union comes up short. Mutating exactly that is
//! caught, naming the seed and the row. Three-valued logic itself is pinned by
//! hand in `t_short_circuit.rs`; the two files cover different halves.
//!
//! Three deliberate choices:
//!
//! * **The union is computed in Rust, not with SQL `UNION ALL`.** The classic
//!   formulation unions in SQL, but then a bug in UNION ALL reports itself as a
//!   predicate bug. Doing it here keeps the WHERE path the only thing on trial.
//! * **Indexes exist.** Without them every query is a table scan, the optimizer
//!   has no choice to make, and TLP proves almost nothing. The whole point is to
//!   catch a fast path diverging.
//! * **The RNG is ours and seeded.** A failure prints its seed, and that seed
//!   alone reproduces the exact query — no saved corpus, no flake.

mod common;

use common::*;
use oxidb_sql::{SqlEngine, Value};

// ── A deterministic PRNG ────────────────────────────────────────────────
//
// xorshift64*. Not for cryptography — for reproducibility. `rand` is not a
// dev-dependency here and pulling it in for this would be a poor trade.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Any nonzero state works; the constant just avoids a zero seed.
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
        self.next() % n.max(1)
    }

    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }

    /// True `pct` percent of the time.
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

// ── The table under test ────────────────────────────────────────────────

const INT_COLS: [&str; 2] = ["a", "b"];
const TXT_COLS: [&str; 1] = ["s"];
const DBL_COLS: [&str; 1] = ["f"];
const BOOL_COLS: [&str; 1] = ["g"];

/// Small domains on purpose: a predicate that never matches anything partitions
/// trivially and proves nothing. Values have to collide.
fn build_table(db: &SqlEngine, rng: &mut Rng, rows: usize) {
    db.execute("CREATE TABLE t (id INT, a INT, b INT, s TEXT, f DOUBLE, g BOOL)")
        .unwrap();

    // The optimizer needs something to choose between. Without these, every
    // query below is a scan and this whole file is a slow no-op.
    db.execute("CREATE INDEX idx_a ON t (a)").unwrap();
    db.execute("CREATE INDEX idx_s ON t (s)").unwrap();

    for id in 0..rows {
        // ~25% NULLs: NULL is the entire subject of the third partition, and a
        // table without NULLs makes TLP a much weaker test than it looks.
        let a = if rng.chance(25) { "NULL".into() } else { format!("{}", rng.below(10)) };
        let b = if rng.chance(25) { "NULL".into() } else { format!("{}", rng.below(10)) };
        let s = if rng.chance(25) {
            "NULL".into()
        } else {
            // A tiny alphabet so LIKE patterns actually hit.
            let n = 1 + rng.below(3);
            let txt: String = (0..n).map(|_| *rng.pick(&['a', 'b', 'c'])).collect();
            format!("'{txt}'")
        };
        let f = if rng.chance(25) {
            "NULL".into()
        } else {
            format!("{}.{}", rng.below(10), rng.below(10))
        };
        let g: String = if rng.chance(25) {
            "NULL".into()
        } else if rng.chance(50) {
            "TRUE".into()
        } else {
            "FALSE".into()
        };
        db.execute(&format!(
            "INSERT INTO t (id, a, b, s, f, g) VALUES ({id}, {a}, {b}, {s}, {f}, {g})"
        ))
        .unwrap();
    }
}

// ── Predicate generation ────────────────────────────────────────────────

/// A random boolean-valued expression over `t`.
///
/// Everything here is deterministic and side-effect free — `NOW()`/`RANDOM()`
/// would break the invariant for reasons that are not bugs.
fn predicate(rng: &mut Rng, depth: u32) -> String {
    // Leaves near the bottom, connectives above.
    if depth == 0 || rng.chance(35) {
        return leaf(rng);
    }
    match rng.below(4) {
        0 => format!("NOT ({})", predicate(rng, depth - 1)),
        1 => format!("({}) AND ({})", predicate(rng, depth - 1), predicate(rng, depth - 1)),
        2 => format!("({}) OR ({})", predicate(rng, depth - 1), predicate(rng, depth - 1)),
        _ => leaf(rng),
    }
}

fn leaf(rng: &mut Rng) -> String {
    let cmp = ["=", "<>", ">", ">=", "<", "<="];
    match rng.below(10) {
        // int vs literal — the form `index_lookup_eq` can serve
        0 | 1 => format!("{} {} {}", rng.pick(&INT_COLS), rng.pick(&cmp), rng.below(10)),
        // int vs int: no index can serve this, so the two paths must agree
        2 => format!("{} {} {}", rng.pick(&INT_COLS), rng.pick(&cmp), rng.pick(&INT_COLS)),
        // arithmetic in an operand
        3 => format!(
            "{} + {} {} {}",
            rng.pick(&INT_COLS),
            rng.below(3),
            rng.pick(&cmp),
            rng.pick(&INT_COLS)
        ),
        // IS NULL / IS NOT NULL
        4 => {
            let col = *rng.pick(&["a", "b", "s", "f", "g"]);
            if rng.chance(50) {
                format!("{col} IS NULL")
            } else {
                format!("{col} IS NOT NULL")
            }
        }
        // IN — three-valued when the column is NULL
        5 => format!(
            "{} IN ({}, {}, {})",
            rng.pick(&INT_COLS),
            rng.below(10),
            rng.below(10),
            rng.below(10)
        ),
        6 => {
            let lo = rng.below(8);
            format!("{} BETWEEN {} AND {}", rng.pick(&INT_COLS), lo, lo + rng.below(4))
        }
        // LIKE — each shape hits a different fast path (prefix/suffix/contains/exact)
        7 => {
            let ch = *rng.pick(&['a', 'b', 'c']);
            let pat = match rng.below(4) {
                0 => format!("{ch}%"),
                1 => format!("%{ch}"),
                2 => format!("%{ch}%"),
                _ => format!("{ch}"),
            };
            format!("{} LIKE '{}'", rng.pick(&TXT_COLS), pat)
        }
        // a bare BOOL column: TRUE / FALSE / NULL straight from the data
        8 => rng.pick(&BOOL_COLS).to_string(),
        _ => format!("{} {} {}.{}", rng.pick(&DBL_COLS), rng.pick(&cmp), rng.below(10), rng.below(10)),
    }
}

// ── The oracle ──────────────────────────────────────────────────────────

/// Rows rendered stably so they can be compared as a multiset. `Value` is only
/// PartialEq (floats), so sorting the Debug form gives a total order we can
/// diff without imposing one on the engine's type.
fn ids(db: &SqlEngine, sql: &str) -> Result<Vec<String>, String> {
    match db.execute(sql) {
        Ok(mut res) => match res.pop() {
            Some(oxidb_sql::QueryResult::Select { rows, .. }) => {
                Ok(rows.iter().map(|r| format!("{:?}", r[0])).collect())
            }
            other => Err(format!("expected a SELECT, got {other:?}")),
        },
        Err(e) => Err(e.to_string()),
    }
}

/// What one TLP round observed — so the test can prove it actually tested.
#[derive(Default)]
struct Coverage {
    checked: usize,
    errored: usize,
    /// Predicates where some row evaluated to NULL — the third partition is
    /// the reason TLP exists; if this stays 0 we are testing two-valued logic.
    null_partition_nonempty: usize,
    /// Predicates that split the table rather than taking all or nothing.
    discriminating: usize,
}

fn tlp_round(db: &SqlEngine, rng: &mut Rng, seed: u64, cov: &mut Coverage) {
    let p = predicate(rng, 3);

    let all = match ids(db, "SELECT id FROM t") {
        Ok(v) => v,
        Err(e) => panic!("the unpartitioned query failed: {e}"),
    };

    let t = ids(db, &format!("SELECT id FROM t WHERE {p}"));
    let f = ids(db, &format!("SELECT id FROM t WHERE NOT ({p})"));
    let n = ids(db, &format!("SELECT id FROM t WHERE ({p}) IS NULL"));

    let (t, f, n) = match (t, f, n) {
        (Ok(t), Ok(f), Ok(n)) => (t, f, n),
        (t, f, n) => {
            // One partition parsing while another does not would itself be a
            // finding; the same error on all three is just an unsupported form.
            let errs: Vec<String> = [t, f, n].into_iter().filter_map(|r| r.err()).collect();
            assert_eq!(
                errs.len(),
                3,
                "seed {seed}: the partitions of `{p}` disagree about whether they are valid SQL — \
                 one ran and another did not: {errs:?}"
            );
            cov.errored += 1;
            if std::env::var("TLP_SHOW_ERRORS").is_ok() {
                // How BETWEEN was found: an unsupported form shows up here as a
                // skip, and the coverage assertions below refuse to let skips
                // accumulate quietly.
                eprintln!("SKIP  {p}\n      {}", errs[0]);
            }
            return;
        }
    };

    cov.checked += 1;
    if !n.is_empty() {
        cov.null_partition_nonempty += 1;
    }
    if !t.is_empty() && t.len() < all.len() {
        cov.discriminating += 1;
    }

    // The invariant. Multiset, so sort — row order is not promised.
    let mut union: Vec<String> = t.iter().chain(f.iter()).chain(n.iter()).cloned().collect();
    let mut expect = all.clone();
    union.sort();
    expect.sort();

    if union != expect {
        // Say exactly what to run, not just that something is wrong.
        let missing: Vec<_> = expect.iter().filter(|x| !union.contains(x)).take(5).collect();
        let extra: Vec<_> = union.iter().filter(|x| !expect.contains(x)).take(5).collect();
        panic!(
            "TLP violation (seed {seed})\n\n\
             \x20 predicate: {p}\n\n\
             \x20 SELECT id FROM t                     -> {} rows\n\
             \x20 SELECT id FROM t WHERE <p>           -> {} rows\n\
             \x20 SELECT id FROM t WHERE NOT (<p>)     -> {} rows\n\
             \x20 SELECT id FROM t WHERE (<p>) IS NULL -> {} rows\n\n\
             \x20 the three partitions total {} rows, the table has {}\n\
             \x20 rows the partitions LOST: {missing:?}\n\
             \x20 rows double-counted:      {extra:?}\n\n\
             \x20 Every row must satisfy exactly one of TRUE / FALSE / NULL.",
            all.len(),
            t.len(),
            f.len(),
            n.len(),
            union.len(),
            expect.len(),
        );
    }
}

/// The same invariant over an aggregate: partitioning cannot change a count.
fn tlp_count_round(db: &SqlEngine, rng: &mut Rng, seed: u64) {
    let p = predicate(rng, 2);

    let count = |sql: &str| -> Option<i64> {
        match db.execute(sql).ok()?.pop()? {
            oxidb_sql::QueryResult::Select { rows, .. } => match rows.first()?.first()? {
                Value::Int(n) => Some(*n),
                _ => None,
            },
            _ => None,
        }
    };

    let all = match count("SELECT COUNT(*) FROM t") {
        Some(n) => n,
        None => return,
    };
    let parts: Vec<Option<i64>> = vec![
        count(&format!("SELECT COUNT(*) FROM t WHERE {p}")),
        count(&format!("SELECT COUNT(*) FROM t WHERE NOT ({p})")),
        count(&format!("SELECT COUNT(*) FROM t WHERE ({p}) IS NULL")),
    ];
    if parts.iter().any(|x| x.is_none()) {
        return; // unsupported form; the row-set round already asserts consistency
    }
    let sum: i64 = parts.iter().map(|x| x.unwrap()).sum();
    assert_eq!(
        sum, all,
        "TLP violation on COUNT (seed {seed})\n  predicate: {p}\n  \
         partitions sum to {sum}, COUNT(*) says {all}"
    );
}

// ── Tests ───────────────────────────────────────────────────────────────

fn run(seed: u64, queries: usize) -> Coverage {
    let (_dir, db) = open();
    let mut rng = Rng::new(seed);
    build_table(&db, &mut rng, 60);

    let mut cov = Coverage::default();
    for _ in 0..queries {
        tlp_round(&db, &mut rng, seed, &mut cov);
        tlp_count_round(&db, &mut rng, seed);
    }
    cov
}

#[test]
fn tlp_holds_across_random_predicates() {
    let mut total = Coverage::default();
    // Fixed seeds: deterministic in CI, and a failure names the one to re-run.
    for seed in [1u64, 7, 42, 1337, 90210] {
        let c = run(seed, 120);
        total.checked += c.checked;
        total.errored += c.errored;
        total.null_partition_nonempty += c.null_partition_nonempty;
        total.discriminating += c.discriminating;
    }

    // A generator that emits garbage would make every round bail out early and
    // this test would pass having proved nothing. Refuse to be that test.
    let attempted = total.checked + total.errored;
    assert!(
        total.checked * 10 >= attempted * 9,
        "only {}/{attempted} generated predicates were valid SQL — the generator \
         is broken, not the engine",
        total.checked
    );
    assert!(
        total.null_partition_nonempty * 5 >= total.checked,
        "only {} of {} predicates ever evaluated to NULL for any row — three-valued \
         logic is what TLP is for, so this is not testing it",
        total.null_partition_nonempty,
        total.checked
    );
    assert!(
        total.discriminating * 2 >= total.checked,
        "only {} of {} predicates actually split the table; the rest matched all \
         rows or none, which partitions trivially",
        total.discriminating,
        total.checked
    );
}

/// A longer run for hunting rather than regression-checking.
///
/// ```text
/// TLP_SEEDS=200 TLP_QUERIES=500 cargo test -p oxidb-sql --test tlp_oracle -- --ignored --nocapture
/// ```
#[test]
#[ignore = "soak: run explicitly, tune with TLP_SEEDS / TLP_QUERIES"]
fn tlp_soak() {
    let seeds: u64 = std::env::var("TLP_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    let queries: usize = std::env::var("TLP_QUERIES").ok().and_then(|v| v.parse().ok()).unwrap_or(200);

    let mut checked = 0;
    for seed in 0..seeds {
        let c = run(seed, queries);
        checked += c.checked;
        if seed % 10 == 0 {
            println!("  seed {seed}/{seeds} — {checked} predicates checked, no divergence");
        }
    }
    println!("\n  {checked} predicates checked across {seeds} seeds. No TLP violation.");
}
