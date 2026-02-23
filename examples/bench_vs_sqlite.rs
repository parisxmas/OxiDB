///! OxiDB (embedded) vs SQLite — 100K document side-by-side benchmark.
///!
///! Both databases run in-process with data on a temp directory.
///! Measures insert, index creation, queries, aggregations, updates, and deletes.
use std::time::Instant;

use oxidb::OxiDb;
use rusqlite::{params, Connection};
use serde_json::{json, Value};

const TOTAL_DOCS: usize = 100_000;
const BATCH_SIZE: usize = 5_000;
const COLLECTION: &str = "bench";

const STATUSES: &[&str] = &["completed", "pending", "cancelled", "refunded"];
const CATEGORIES: &[&str] = &[
    "electronics",
    "clothing",
    "books",
    "home",
    "sports",
    "toys",
    "food",
    "beauty",
    "automotive",
    "garden",
];
const COUNTRIES: &[&str] = &["TR", "US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU"];

// ── Simple deterministic RNG (xorshift64) ────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn usize(&mut self, max: usize) -> usize {
        (self.next() % max as u64) as usize
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
    fn float(&mut self, lo: f64, hi: f64) -> f64 {
        let frac = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        lo + frac * (hi - lo)
    }
    fn choice<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.usize(items.len())]
    }
}

// ── Timer helper ─────────────────────────────────────────────────────────────

struct Timer(Instant);

impl Timer {
    fn start() -> Self {
        Self(Instant::now())
    }
    fn ms(&self) -> f64 {
        self.0.elapsed().as_secs_f64() * 1000.0
    }
}

// ── Generate a batch of documents ────────────────────────────────────────────

fn generate_batch(rng: &mut Rng, start_id: usize, count: usize) -> Vec<Value> {
    (0..count)
        .map(|i| {
            let oid = start_id + i;
            json!({
                "order_id": oid,
                "customer_id": rng.range(1, 100_000),
                "amount": (rng.float(5.0, 5000.0) * 100.0).round() / 100.0,
                "status": rng.choice(STATUSES),
                "category": rng.choice(CATEGORIES),
                "country": rng.choice(COUNTRIES),
                "priority": rng.range(1, 6),
            })
        })
        .collect()
}

// ── Benchmark runner (best of 3) ─────────────────────────────────────────────

fn bench<F, G>(label: &str, oxi_fn: F, sql_fn: G, results: &mut Vec<BenchResult>)
where
    F: Fn() -> usize,
    G: Fn() -> usize,
{
    let runs = 3;
    let mut oxi_times = Vec::with_capacity(runs);
    let mut sql_times = Vec::with_capacity(runs);
    let mut oxi_count = 0;
    let mut sql_count = 0;

    for _ in 0..runs {
        let t = Timer::start();
        oxi_count = oxi_fn();
        oxi_times.push(t.ms());

        let t = Timer::start();
        sql_count = sql_fn();
        sql_times.push(t.ms());
    }

    let oxi_best = oxi_times.iter().cloned().fold(f64::MAX, f64::min);
    let sql_best = sql_times.iter().cloned().fold(f64::MAX, f64::min);
    let ratio = sql_best / oxi_best.max(0.001);
    let winner = if oxi_best <= sql_best {
        "OxiDB"
    } else {
        "SQLite"
    };

    let w_color = if winner == "OxiDB" {
        "\x1b[92m"
    } else {
        "\x1b[93m"
    };

    println!(
        "  {:<48} {:>10.1}ms {:>10.1}ms  {}{:>6.2}x {}\x1b[0m  ({:>7} | {:>7})",
        label, oxi_best, sql_best, w_color, ratio, winner, oxi_count, sql_count
    );

    results.push(BenchResult {
        test: label.to_string(),
        oxidb_ms: oxi_best,
        sqlite_ms: sql_best,
        ratio,
        winner: winner.to_string(),
    });
}

struct BenchResult {
    test: String,
    oxidb_ms: f64,
    sqlite_ms: f64,
    ratio: f64,
    winner: String,
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let oxi_dir = tmp.path().join("oxidb_data");
    let sql_path = tmp.path().join("bench.sqlite3");

    // Open databases
    let db = OxiDb::open(&oxi_dir).expect("open OxiDB");
    let sq = Connection::open(&sql_path).expect("open SQLite");

    // SQLite pragmas for performance
    sq.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA cache_size=-64000;
         PRAGMA mmap_size=268435456;",
    )
    .unwrap();

    // Create SQLite table
    sq.execute_batch(
        "CREATE TABLE bench (
            id INTEGER PRIMARY KEY,
            data JSON NOT NULL,
            order_id INTEGER,
            customer_id INTEGER,
            amount REAL,
            status TEXT,
            category TEXT,
            country TEXT,
            priority INTEGER
        )",
    )
    .unwrap();

    println!();
    println!(
        "  \x1b[1m\u{2554}{}\u{2557}\x1b[0m",
        "\u{2550}".repeat(78)
    );
    println!(
        "  \x1b[1m\u{2551}{:^78}\u{2551}\x1b[0m",
        "OxiDB (embedded) vs SQLite — 100K Document Benchmark"
    );
    println!(
        "  \x1b[1m\u{255a}{}\u{255d}\x1b[0m",
        "\u{2550}".repeat(78)
    );
    println!();
    println!("  Documents:  {TOTAL_DOCS:>10}");
    println!("  Batch size: {BATCH_SIZE:>10}");
    println!("  Storage:    temp directory");
    println!();

    // ── Phase 1: Insert ──────────────────────────────────────────────────────
    println!("  \x1b[1m\u{2500}\u{2500} Phase 1: Insert {TOTAL_DOCS:} documents \u{2500}\u{2500}\x1b[0m");
    println!();

    // OxiDB insert
    let t = Timer::start();
    {
        let mut rng = Rng::new(42);
        for batch_start in (0..TOTAL_DOCS).step_by(BATCH_SIZE) {
            let count = BATCH_SIZE.min(TOTAL_DOCS - batch_start);
            let docs = generate_batch(&mut rng, batch_start, count);
            db.insert_many(COLLECTION, docs).unwrap();
        }
    }
    let oxi_insert_ms = t.ms();
    let oxi_rate = TOTAL_DOCS as f64 / (oxi_insert_ms / 1000.0);

    // SQLite insert
    let t = Timer::start();
    {
        let mut rng = Rng::new(42);
        for batch_start in (0..TOTAL_DOCS).step_by(BATCH_SIZE) {
            let count = BATCH_SIZE.min(TOTAL_DOCS - batch_start);
            let docs = generate_batch(&mut rng, batch_start, count);
            let tx = sq.unchecked_transaction().unwrap();
            {
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO bench (data, order_id, customer_id, amount, status, category, country, priority)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    )
                    .unwrap();
                for doc in &docs {
                    stmt.execute(params![
                        doc.to_string(),
                        doc["order_id"].as_i64(),
                        doc["customer_id"].as_i64(),
                        doc["amount"].as_f64(),
                        doc["status"].as_str(),
                        doc["category"].as_str(),
                        doc["country"].as_str(),
                        doc["priority"].as_i64(),
                    ])
                    .unwrap();
                }
            }
            tx.commit().unwrap();
        }
    }
    let sql_insert_ms = t.ms();
    let sql_rate = TOTAL_DOCS as f64 / (sql_insert_ms / 1000.0);

    let insert_ratio = sql_insert_ms / oxi_insert_ms.max(0.001);
    let insert_winner = if oxi_insert_ms <= sql_insert_ms {
        "OxiDB"
    } else {
        "SQLite"
    };
    let w = if insert_winner == "OxiDB" {
        "\x1b[92m"
    } else {
        "\x1b[93m"
    };
    println!(
        "  OxiDB:      {:.2}s ({:.0} docs/s)",
        oxi_insert_ms / 1000.0,
        oxi_rate
    );
    println!(
        "  SQLite:     {:.2}s ({:.0} docs/s)",
        sql_insert_ms / 1000.0,
        sql_rate
    );
    println!(
        "  Winner:     {w}{insert_winner} ({insert_ratio:.2}x)\x1b[0m"
    );
    println!();

    // Verify
    let oxi_count = db.count(COLLECTION, &json!({})).unwrap();
    let sql_count: i64 = sq
        .query_row("SELECT COUNT(*) FROM bench", [], |r| r.get(0))
        .unwrap();
    println!("  Verified:   OxiDB={oxi_count}  SQLite={sql_count}");
    println!();

    // ── Phase 2: Create indexes ──────────────────────────────────────────────
    println!("  \x1b[1m\u{2500}\u{2500} Phase 2: Create indexes \u{2500}\u{2500}\x1b[0m");
    println!();
    println!(
        "  {:<35} {:>10} {:>10}  Winner",
        "Index", "OxiDB", "SQLite"
    );
    println!(
        "  {0} {0} {0}",
        "\u{2500}".repeat(10)
    );

    let index_fields = &["status", "category", "country", "amount", "order_id"];
    for &field in index_fields {
        let t = Timer::start();
        db.create_index(COLLECTION, field).unwrap();
        let oxi_ms = t.ms();

        let t = Timer::start();
        sq.execute(
            &format!("CREATE INDEX idx_{field} ON bench ({field})"),
            [],
        )
        .unwrap();
        let sql_ms = t.ms();

        let ratio = sql_ms / oxi_ms.max(0.001);
        let winner = if oxi_ms <= sql_ms { "OxiDB" } else { "SQLite" };
        let w = if winner == "OxiDB" {
            "\x1b[92m"
        } else {
            "\x1b[93m"
        };
        println!(
            "  {:<35} {:>9.1}ms {:>9.1}ms  {w}{ratio:.2}x {winner}\x1b[0m",
            field, oxi_ms, sql_ms
        );
    }

    // SQLite ANALYZE
    sq.execute("ANALYZE", []).unwrap();
    println!();

    // ── Phase 3: Queries ─────────────────────────────────────────────────────
    println!("  \x1b[1m\u{2500}\u{2500} Phase 3: Queries (best of 3 runs, {TOTAL_DOCS} docs) \u{2500}\u{2500}\x1b[0m");
    println!();
    println!(
        "  {:<48} {:>10}  {:>10}  {:>7} {:<8}  ({:>7} | {:<7})",
        "Query", "OxiDB", "SQLite", "Ratio", "Winner", "Oxi#", "SQL#"
    );
    println!(
        "  {} {} {} {} {} {}",
        "\u{2500}".repeat(48),
        "\u{2500}".repeat(10),
        "\u{2500}".repeat(10),
        "\u{2500}".repeat(7),
        "\u{2500}".repeat(8),
        "\u{2500}".repeat(17)
    );

    let mut results: Vec<BenchResult> = Vec::new();

    // -- Find queries --

    bench(
        "Find: status=completed",
        || db.find(COLLECTION, &json!({"status": "completed"})).unwrap().len(),
        || {
            let mut stmt = sq.prepare_cached("SELECT data FROM bench WHERE status = 'completed'").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "Find: category=electronics",
        || db.find(COLLECTION, &json!({"category": "electronics"})).unwrap().len(),
        || {
            let mut stmt = sq.prepare_cached("SELECT data FROM bench WHERE category = 'electronics'").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "Find: amount > 4000",
        || db.find(COLLECTION, &json!({"amount": {"$gt": 4000}})).unwrap().len(),
        || {
            let mut stmt = sq.prepare_cached("SELECT data FROM bench WHERE amount > 4000").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "Find: country=TR + status=completed",
        || {
            db.find(COLLECTION, &json!({"country": "TR", "status": "completed"}))
                .unwrap()
                .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT data FROM bench WHERE country = 'TR' AND status = 'completed'",
            ).unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "Find: priority >= 4",
        || db.find(COLLECTION, &json!({"priority": {"$gte": 4}})).unwrap().len(),
        || {
            let mut stmt = sq.prepare_cached("SELECT data FROM bench WHERE priority >= 4").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "Find: $or country TR|US",
        || {
            db.find(
                COLLECTION,
                &json!({"$or": [{"country": "TR"}, {"country": "US"}]}),
            )
            .unwrap()
            .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT data FROM bench WHERE country IN ('TR', 'US')",
            ).unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "Find: $in category [books,food,toys]",
        || {
            db.find(
                COLLECTION,
                &json!({"category": {"$in": ["books", "food", "toys"]}}),
            )
            .unwrap()
            .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT data FROM bench WHERE category IN ('books', 'food', 'toys')",
            ).unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "Find: sort amount desc, limit 10",
        || {
            db.find_with_options(
                COLLECTION,
                &json!({}),
                &oxidb::query::FindOptions {
                    sort: Some(vec![("amount".into(), oxidb::query::SortOrder::Desc)]),
                    limit: Some(10),
                    skip: Some(0),
                },
            )
            .unwrap()
            .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT data FROM bench ORDER BY amount DESC LIMIT 10",
            ).unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    bench(
        "FindOne: order_id=50000",
        || {
            if db.find_one(COLLECTION, &json!({"order_id": 50000})).unwrap().is_some() { 1 } else { 0 }
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT data FROM bench WHERE order_id = 50000 LIMIT 1",
            ).unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().count()
        },
        &mut results,
    );

    // -- Count queries --
    println!();

    bench(
        "Count: all documents",
        || db.count(COLLECTION, &json!({})).unwrap(),
        || {
            sq.query_row("SELECT COUNT(*) FROM bench", [], |r| r.get::<_, usize>(0)).unwrap()
        },
        &mut results,
    );

    bench(
        "Count: status=completed",
        || db.count(COLLECTION, &json!({"status": "completed"})).unwrap(),
        || {
            sq.query_row(
                "SELECT COUNT(*) FROM bench WHERE status = 'completed'",
                [],
                |r| r.get::<_, usize>(0),
            ).unwrap()
        },
        &mut results,
    );

    bench(
        "Count: amount 100-500",
        || {
            db.count(
                COLLECTION,
                &json!({"$and": [{"amount": {"$gte": 100}}, {"amount": {"$lte": 500}}]}),
            )
            .unwrap()
        },
        || {
            sq.query_row(
                "SELECT COUNT(*) FROM bench WHERE amount >= 100 AND amount <= 500",
                [],
                |r| r.get::<_, usize>(0),
            ).unwrap()
        },
        &mut results,
    );

    // -- Aggregation queries --
    println!();

    bench(
        "Agg: group by status, count",
        || {
            db.aggregate(
                COLLECTION,
                &json!([
                    {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]),
            )
            .unwrap()
            .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT status, COUNT(*) FROM bench GROUP BY status ORDER BY COUNT(*) DESC",
            ).unwrap();
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
                .unwrap()
                .count()
        },
        &mut results,
    );

    bench(
        "Agg: group by category, sum amount",
        || {
            db.aggregate(
                COLLECTION,
                &json!([
                    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
                    {"$sort": {"total": -1}}
                ]),
            )
            .unwrap()
            .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT category, SUM(amount), COUNT(*) FROM bench GROUP BY category ORDER BY SUM(amount) DESC",
            ).unwrap();
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?, r.get::<_, i64>(2)?)))
                .unwrap()
                .count()
        },
        &mut results,
    );

    bench(
        "Agg: group by country, avg amount",
        || {
            db.aggregate(
                COLLECTION,
                &json!([
                    {"$group": {"_id": "$country", "avg_amt": {"$avg": "$amount"}}},
                    {"$sort": {"avg_amt": -1}},
                    {"$limit": 5}
                ]),
            )
            .unwrap()
            .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT country, AVG(amount) FROM bench GROUP BY country ORDER BY AVG(amount) DESC LIMIT 5",
            ).unwrap();
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?)))
                .unwrap()
                .count()
        },
        &mut results,
    );

    bench(
        "Agg: match completed + group category",
        || {
            db.aggregate(
                COLLECTION,
                &json!([
                    {"$match": {"status": "completed"}},
                    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
                    {"$sort": {"total": -1}},
                    {"$limit": 5}
                ]),
            )
            .unwrap()
            .len()
        },
        || {
            let mut stmt = sq.prepare_cached(
                "SELECT category, SUM(amount) FROM bench WHERE status = 'completed' GROUP BY category ORDER BY SUM(amount) DESC LIMIT 5",
            ).unwrap();
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?)))
                .unwrap()
                .count()
        },
        &mut results,
    );

    // -- Update / Delete --
    println!();

    bench(
        "Update: set amount for order_id=1",
        || {
            db.update(
                COLLECTION,
                &json!({"order_id": 1}),
                &json!({"$inc": {"amount": 1}}),
            )
            .unwrap() as usize
        },
        || {
            sq.execute(
                "UPDATE bench SET amount = amount + 1, data = json_set(data, '$.amount', amount + 1) WHERE order_id = 1",
                [],
            ).unwrap()
        },
        &mut results,
    );

    bench(
        "Update: set status for order_id=2",
        || {
            db.update(
                COLLECTION,
                &json!({"order_id": 2}),
                &json!({"$set": {"status": "shipped"}}),
            )
            .unwrap() as usize
        },
        || {
            sq.execute(
                "UPDATE bench SET status = 'shipped', data = json_set(data, '$.status', 'shipped') WHERE order_id = 2",
                [],
            ).unwrap()
        },
        &mut results,
    );

    bench(
        "Delete: order_id=99999",
        || db.delete(COLLECTION, &json!({"order_id": 99999})).unwrap() as usize,
        || {
            sq.execute("DELETE FROM bench WHERE order_id = 99999", []).unwrap()
        },
        &mut results,
    );

    // ── Summary ──────────────────────────────────────────────────────────────
    println!();
    println!(
        "  \x1b[1m\u{2554}{}\u{2557}\x1b[0m",
        "\u{2550}".repeat(78)
    );
    println!(
        "  \x1b[1m\u{2551}{:^78}\u{2551}\x1b[0m",
        "SUMMARY"
    );
    println!(
        "  \x1b[1m\u{255a}{}\u{255d}\x1b[0m",
        "\u{2550}".repeat(78)
    );
    println!();

    let oxi_wins = results.iter().filter(|r| r.winner == "OxiDB").count();
    let sql_wins = results.iter().filter(|r| r.winner == "SQLite").count();
    let total = results.len();
    let oxi_total: f64 = results.iter().map(|r| r.oxidb_ms).sum();
    let sql_total: f64 = results.iter().map(|r| r.sqlite_ms).sum();

    println!(
        "  Insert 100K:     OxiDB {:.2}s vs SQLite {:.2}s  ({:.2}x)",
        oxi_insert_ms / 1000.0,
        sql_insert_ms / 1000.0,
        insert_ratio
    );
    println!("  Query tests:     {total}");
    println!("  OxiDB wins:      \x1b[92m{oxi_wins}\x1b[0m / {total}");
    println!("  SQLite wins:     \x1b[93m{sql_wins}\x1b[0m / {total}");
    println!("  OxiDB total:     {oxi_total:.2} ms");
    println!("  SQLite total:    {sql_total:.2} ms");
    if oxi_total > 0.0 {
        println!("  Overall ratio:   {:.2}x", sql_total / oxi_total);
    }

    // Disk usage
    let oxi_size = dir_size(&oxi_dir);
    let sql_size = std::fs::metadata(&sql_path).map(|m| m.len()).unwrap_or(0);
    println!();
    println!("  \x1b[1m\u{2500}\u{2500} Disk Usage \u{2500}\u{2500}\x1b[0m");
    println!();
    println!("  OxiDB:   {:.1} MB", oxi_size as f64 / 1_048_576.0);
    println!("  SQLite:  {:.1} MB", sql_size as f64 / 1_048_576.0);
    println!();
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                total += dir_size(&entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}
