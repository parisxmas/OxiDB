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

    // ── HTML Report ──────────────────────────────────────────────────────────
    generate_html_report(
        &results,
        oxi_insert_ms,
        sql_insert_ms,
        insert_ratio,
        oxi_size,
        sql_size,
    );
}

fn get_cpu_name() -> String {
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(out) = Command::new("sysctl").args(["-n", "machdep.cpu.brand_string"]).output() {
            return String::from_utf8_lossy(&out.stdout).trim().to_string();
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/cpuinfo") {
            for line in content.lines() {
                if line.starts_with("model name") {
                    if let Some(name) = line.split(':').nth(1) {
                        return name.trim().to_string();
                    }
                }
            }
        }
    }
    format!("{} ({} cores)", std::env::consts::ARCH, num_cpus())
}

fn get_total_ram() -> String {
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(out) = Command::new("sysctl").args(["-n", "hw.memsize"]).output() {
            if let Ok(bytes) = String::from_utf8_lossy(&out.stdout).trim().parse::<u64>() {
                return format!("{} GB", bytes / (1024 * 1024 * 1024));
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("MemTotal") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if let Ok(kb) = parts.get(1).unwrap_or(&"0").parse::<u64>() {
                        return format!("{} GB", kb / (1024 * 1024));
                    }
                }
            }
        }
    }
    "unknown".to_string()
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

fn generate_html_report(
    results: &[BenchResult],
    oxi_insert_ms: f64,
    sql_insert_ms: f64,
    insert_ratio: f64,
    oxi_disk: u64,
    sql_disk: u64,
) {
    let oxi_wins = 1_usize.wrapping_sub(1) // start at 0
        + if oxi_insert_ms <= sql_insert_ms { 1 } else { 0 }
        + results.iter().filter(|r| r.winner == "OxiDB").count();
    let sql_wins = if sql_insert_ms < oxi_insert_ms { 1 } else { 0 }
        + results.iter().filter(|r| r.winner == "SQLite").count();

    let cpu = get_cpu_name();
    let ram = get_total_ram();
    let cores = num_cpus();
    let os_arch = format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH);
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M").to_string();
    let total_tests = results.len() + 1; // +1 for insert

    // Categorize results
    let mut categories: Vec<(&str, Vec<&BenchResult>)> = Vec::new();
    let mut finds = Vec::new();
    let mut counts = Vec::new();
    let mut aggs = Vec::new();
    let mut updates = Vec::new();
    for r in results {
        if r.test.starts_with("Find") {
            finds.push(r);
        } else if r.test.starts_with("Count") {
            counts.push(r);
        } else if r.test.starts_with("Agg") {
            aggs.push(r);
        } else {
            updates.push(r);
        }
    }
    if !finds.is_empty() { categories.push(("Queries", finds)); }
    if !counts.is_empty() { categories.push(("Counts", counts)); }
    if !aggs.is_empty() { categories.push(("Aggregation", aggs)); }
    if !updates.is_empty() { categories.push(("Updates &amp; Deletes", updates)); }

    let cat_icon = |name: &str| -> &str {
        match name {
            "Queries" => "&#128269;",
            "Counts" => "&#35;",
            "Aggregation" => "&#8721;",
            "Updates &amp; Deletes" => "&#9998;",
            _ => "&#9679;",
        }
    };

    let fmt_ms = |ms: f64| -> String {
        if ms < 1.0 { format!("{:.0}&micro;s", ms * 1000.0) }
        else if ms < 1000.0 { format!("{:.1}ms", ms) }
        else { format!("{:.2}s", ms / 1000.0) }
    };

    let mut html = String::with_capacity(32_000);

    html.push_str(&format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OxiDB vs SQLite — Embedded Benchmark</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&family=Outfit:wght@300;400;500;600;700;800;900&display=swap');

*{{margin:0;padding:0;box-sizing:border-box}}

:root{{
  --bg:#06080c;--surface:#0c1018;--card:#111820;--border:#1a2332;
  --text:#c8d6e5;--dim:#5a6a7e;--bright:#e8f0f8;
  --oxi:#22d666;--oxi-dim:#1a4a2e;--oxi-glow:rgba(34,214,102,0.12);
  --sql:#60a5fa;--sql-dim:#1a2a4a;--sql-glow:rgba(96,165,250,0.12);
  --accent:#f0c040;--red:#e84057;
}}

body{{
  font-family:'Outfit',system-ui,sans-serif;
  background:var(--bg);color:var(--text);
  min-height:100vh;overflow-x:hidden;
}}

body::before{{
  content:'';position:fixed;inset:0;z-index:9999;pointer-events:none;
  opacity:0.025;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
}}

body::after{{
  content:'';position:fixed;top:0;left:0;right:0;height:2px;z-index:100;
  background:linear-gradient(90deg,transparent,var(--oxi),var(--accent),var(--sql),transparent);
}}

.wrap{{max-width:1100px;margin:0 auto;padding:48px 24px 80px}}

header{{margin-bottom:32px}}
header h1{{
  font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:500;
  color:var(--dim);letter-spacing:3px;text-transform:uppercase;margin-bottom:12px;
}}
header .title{{
  font-size:42px;font-weight:800;letter-spacing:-1px;
  background:linear-gradient(135deg,var(--oxi) 0%,#40e8a0 40%,var(--sql) 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
  line-height:1.1;margin-bottom:16px;
}}
header .meta{{
  font-family:'JetBrains Mono',monospace;font-size:12px;color:var(--dim);
  display:flex;gap:24px;flex-wrap:wrap;
}}
header .meta span::before{{content:'› ';color:var(--border)}}

.methodology{{
  font-family:'JetBrains Mono',monospace;font-size:12px;line-height:1.7;
  color:var(--dim);margin-bottom:32px;padding:20px 24px;
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
}}
.methodology strong{{color:var(--text);font-weight:600}}
.methodology .hl-oxi{{color:var(--oxi)}}
.methodology .hl-sql{{color:var(--sql)}}

.env-panel{{
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
  padding:24px 28px;margin-bottom:36px;position:relative;overflow:hidden;
}}
.env-panel::before{{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--oxi),var(--border),var(--sql));
}}
.env-title{{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  color:var(--dim);letter-spacing:2px;text-transform:uppercase;margin-bottom:16px;
}}
.env-grid{{
  display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px 24px;
}}
.env-item{{display:flex;flex-direction:column;gap:2px}}
.env-label{{
  font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:600;
  color:var(--dim);letter-spacing:1px;text-transform:uppercase;
}}
.env-value{{
  font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:500;
  color:var(--bright);
}}

.score-strip{{
  display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:0;
  background:var(--surface);border:1px solid var(--border);border-radius:16px;
  padding:32px 40px;margin-bottom:48px;position:relative;overflow:hidden;
}}
.score-strip::before{{
  content:'';position:absolute;inset:0;
  background:linear-gradient(135deg,var(--oxi-glow),transparent 50%,var(--sql-glow));
  pointer-events:none;
}}
.score-side{{text-align:center;position:relative}}
.score-side .db-label{{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  letter-spacing:2px;text-transform:uppercase;margin-bottom:12px;
}}
.score-side .db-label.oxi{{color:var(--oxi)}}
.score-side .db-label.sql{{color:var(--sql)}}
.score-num{{font-size:72px;font-weight:900;line-height:1;letter-spacing:-3px}}
.score-num.oxi{{color:var(--oxi)}}
.score-num.sql{{color:var(--sql)}}
.score-sub{{font-size:13px;color:var(--dim);margin-top:6px;font-weight:500}}
.score-vs{{
  font-family:'JetBrains Mono',monospace;font-size:14px;font-weight:700;
  color:var(--border);padding:0 24px;
  display:flex;flex-direction:column;align-items:center;gap:4px;
}}
.score-vs::before,.score-vs::after{{content:'';width:1px;height:32px;background:var(--border)}}

.cat{{margin-bottom:36px}}
.cat-head{{
  display:flex;align-items:center;gap:10px;
  padding:14px 0;margin-bottom:2px;border-bottom:1px solid var(--border);
}}
.cat-icon{{
  width:28px;height:28px;display:flex;align-items:center;justify-content:center;
  background:var(--card);border:1px solid var(--border);border-radius:8px;font-size:14px;flex-shrink:0;
}}
.cat-name{{font-size:16px;font-weight:700;color:var(--bright)}}
.cat-count{{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim);margin-left:auto}}

.row{{
  display:grid;grid-template-columns:1fr 300px 100px;align-items:center;gap:16px;
  padding:14px 0;border-bottom:1px solid rgba(26,35,50,0.6);transition:background 0.15s;
}}
.row:last-child{{border-bottom:none}}
.row:hover{{background:rgba(34,214,102,0.02)}}
.row-label{{font-size:14px;font-weight:500;color:var(--text)}}
.row-label .row-counts{{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim)}}

.duel{{display:flex;flex-direction:column;gap:5px}}
.duel-row{{display:flex;align-items:center;gap:8px}}
.duel-label{{
  font-family:'JetBrains Mono',monospace;font-size:10px;font-weight:600;
  width:50px;text-align:right;flex-shrink:0;
}}
.duel-label.oxi{{color:var(--oxi)}}
.duel-label.sql{{color:var(--sql)}}
.duel-track{{flex:1;height:18px;background:var(--bg);border-radius:3px;overflow:hidden;position:relative}}
.duel-fill{{height:100%;border-radius:3px;position:relative;transition:width 0.8s cubic-bezier(0.16,1,0.3,1)}}
.duel-fill.oxi{{background:linear-gradient(90deg,var(--oxi-dim),var(--oxi))}}
.duel-fill.sql{{background:linear-gradient(90deg,var(--sql-dim),var(--sql))}}
.duel-fill.winner{{box-shadow:0 0 12px rgba(34,214,102,0.3)}}
.duel-fill.winner.sql{{box-shadow:0 0 12px rgba(96,165,250,0.3)}}
.duel-time{{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:500;
  color:var(--dim);width:70px;flex-shrink:0;
}}

.result{{text-align:right}}
.badge{{
  display:inline-block;font-family:'JetBrains Mono',monospace;
  font-size:11px;font-weight:700;padding:3px 10px;border-radius:4px;letter-spacing:0.5px;
}}
.badge-oxi{{background:var(--oxi-dim);color:var(--oxi)}}
.badge-sql{{background:var(--sql-dim);color:var(--sql)}}
.badge-tie{{background:var(--card);color:var(--dim)}}
.speedup{{font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--accent);display:block;margin-top:3px}}

.res-grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:8px}}
.res-card{{
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
  padding:24px;position:relative;overflow:hidden;
}}
.res-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px}}
.res-card.disk::before{{background:linear-gradient(90deg,var(--oxi),var(--sql))}}
.res-title{{
  font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;
  color:var(--dim);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:20px;
}}
.res-bars{{display:flex;flex-direction:column;gap:12px}}
.res-row{{display:flex;align-items:center;gap:12px}}
.res-db{{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;width:60px;flex-shrink:0}}
.res-db.oxi{{color:var(--oxi)}}
.res-db.sql{{color:var(--sql)}}
.res-track{{flex:1;height:24px;background:var(--bg);border-radius:4px;overflow:hidden}}
.res-fill{{height:100%;border-radius:4px;display:flex;align-items:center;padding:0 10px;min-width:fit-content}}
.res-fill.oxi{{background:linear-gradient(90deg,var(--oxi-dim),rgba(34,214,102,0.35))}}
.res-fill.sql{{background:linear-gradient(90deg,var(--sql-dim),rgba(96,165,250,0.35))}}
.res-val{{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:var(--bright);white-space:nowrap}}

footer{{
  margin-top:56px;padding-top:24px;border-top:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:center;
  font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--dim);
}}
footer .oxi-tag{{color:var(--oxi)}}

@keyframes grow{{from{{width:0}}to{{width:var(--w)}}}}
.duel-fill,.res-fill{{animation:grow 0.8s cubic-bezier(0.16,1,0.3,1) forwards;width:0}}

@media(max-width:768px){{
  .score-strip{{grid-template-columns:1fr;gap:16px;padding:24px}}
  .score-vs{{flex-direction:row;padding:8px 0}}
  .score-vs::before,.score-vs::after{{width:32px;height:1px}}
  .row{{grid-template-columns:1fr;gap:8px}}
  .res-grid{{grid-template-columns:1fr}}
  header .title{{font-size:28px}}
  .score-num{{font-size:48px}}
}}
</style>
</head>
<body>
<div class="wrap">

<header>
  <h1>embedded benchmark report</h1>
  <div class="title">OxiDB vs SQLite</div>
  <div class="meta">
    <span>{total_tests} tests</span>
    <span>{TOTAL_DOCS_FMT}K documents</span>
    <span>{now}</span>
    <span>best of 3 runs</span>
  </div>
</header>

<div class="methodology">
  Both databases run <strong>in-process</strong> (embedded, no network overhead).
  <span class="hl-oxi">OxiDB</span> uses its native Rust API;
  <span class="hl-sql">SQLite</span> uses <strong>rusqlite</strong> with WAL mode, mmap, and dedicated columns with indexes.
  Data is stored on a temp directory. Each query test runs <strong>3 times</strong> and reports the best result.
  SQLite has <strong>ANALYZE</strong> run after index creation for optimal query planning.
</div>

<div class="env-panel">
  <div class="env-title">&#9881; Test Environment</div>
  <div class="env-grid">
    <div class="env-item"><div class="env-label">CPU</div><div class="env-value">{cpu}</div></div>
    <div class="env-item"><div class="env-label">Cores</div><div class="env-value">{cores}</div></div>
    <div class="env-item"><div class="env-label">Memory</div><div class="env-value">{ram}</div></div>
    <div class="env-item"><div class="env-label">OS / Arch</div><div class="env-value">{os_arch}</div></div>
    <div class="env-item"><div class="env-label">Rust</div><div class="env-value">{rust_ver}</div></div>
    <div class="env-item"><div class="env-label">Storage</div><div class="env-value">temp directory (SSD)</div></div>
  </div>
</div>

<div class="score-strip">
  <div class="score-side">
    <div class="db-label oxi">OxiDB</div>
    <div class="score-num oxi">{oxi_wins}</div>
    <div class="score-sub">wins</div>
  </div>
  <div class="score-vs">VS</div>
  <div class="score-side">
    <div class="db-label sql">SQLite</div>
    <div class="score-num sql">{sql_wins}</div>
    <div class="score-sub">wins</div>
  </div>
</div>
"#,
        total_tests = total_tests,
        TOTAL_DOCS_FMT = TOTAL_DOCS / 1000,
        now = now,
        cpu = cpu,
        cores = cores,
        ram = ram,
        os_arch = os_arch,
        rust_ver = env!("CARGO_PKG_RUST_VERSION", "stable"),
        oxi_wins = oxi_wins,
        sql_wins = sql_wins,
    ));

    // Insert section
    {
        let max_dur = oxi_insert_ms.max(sql_insert_ms);
        let oxi_pct = oxi_insert_ms / max_dur * 100.0;
        let sql_pct = sql_insert_ms / max_dur * 100.0;
        let (badge, speedup, oxi_w, sql_w) = if oxi_insert_ms <= sql_insert_ms {
            (r#"<span class="badge badge-oxi">OxiDB</span>"#, insert_ratio, " winner", "")
        } else {
            (r#"<span class="badge badge-sql">SQLite</span>"#, oxi_insert_ms / sql_insert_ms, "", " winner")
        };

        html.push_str(&format!(r#"<div class="cat">
<div class="cat-head">
  <div class="cat-icon">&#9654;</div>
  <div class="cat-name">Bulk Insert</div>
  <div class="cat-count">1 test</div>
</div>
<div class="row">
  <div class="row-label">Insert {total} documents <span class="row-counts">{total} / {total}</span></div>
  <div class="duel">
    <div class="duel-row">
      <div class="duel-label oxi">OxiDB</div>
      <div class="duel-track"><div class="duel-fill oxi{oxi_w}" style="--w:{oxi_pct:.0}%;width:0"></div></div>
      <div class="duel-time">{oxi_time}</div>
    </div>
    <div class="duel-row">
      <div class="duel-label sql">SQLite</div>
      <div class="duel-track"><div class="duel-fill sql{sql_w}" style="--w:{sql_pct:.0}%;width:0"></div></div>
      <div class="duel-time">{sql_time}</div>
    </div>
  </div>
  <div class="result">{badge}<span class="speedup">{speedup:.1}x faster</span></div>
</div>
</div>
"#,
            total = TOTAL_DOCS,
            oxi_w = oxi_w,
            oxi_pct = oxi_pct,
            oxi_time = fmt_ms(oxi_insert_ms),
            sql_w = sql_w,
            sql_pct = sql_pct,
            sql_time = fmt_ms(sql_insert_ms),
            badge = badge,
            speedup = speedup,
        ));
    }

    // Category sections
    for (cat_name, entries) in &categories {
        html.push_str(&format!(
            r#"<div class="cat">
<div class="cat-head">
  <div class="cat-icon">{icon}</div>
  <div class="cat-name">{name}</div>
  <div class="cat-count">{count} tests</div>
</div>
"#,
            icon = cat_icon(cat_name),
            name = cat_name,
            count = entries.len(),
        ));

        for r in entries {
            let max_dur = r.oxidb_ms.max(r.sqlite_ms);
            let oxi_pct = if max_dur > 0.0 { r.oxidb_ms / max_dur * 100.0 } else { 50.0 };
            let sql_pct = if max_dur > 0.0 { r.sqlite_ms / max_dur * 100.0 } else { 50.0 };

            let (badge, speedup_html, oxi_w, sql_w) = if r.oxidb_ms <= r.sqlite_ms {
                (
                    r#"<span class="badge badge-oxi">OxiDB</span>"#,
                    format!(r#"<span class="speedup">{:.1}x faster</span>"#, r.ratio),
                    " winner", "",
                )
            } else {
                (
                    r#"<span class="badge badge-sql">SQLite</span>"#,
                    format!(r#"<span class="speedup">{:.1}x faster</span>"#, r.oxidb_ms / r.sqlite_ms),
                    "", " winner",
                )
            };

            html.push_str(&format!(
                r#"<div class="row">
  <div class="row-label">{label}</div>
  <div class="duel">
    <div class="duel-row">
      <div class="duel-label oxi">OxiDB</div>
      <div class="duel-track"><div class="duel-fill oxi{oxi_w}" style="--w:{oxi_pct:.0}%;width:0"></div></div>
      <div class="duel-time">{oxi_time}</div>
    </div>
    <div class="duel-row">
      <div class="duel-label sql">SQLite</div>
      <div class="duel-track"><div class="duel-fill sql{sql_w}" style="--w:{sql_pct:.0}%;width:0"></div></div>
      <div class="duel-time">{sql_time}</div>
    </div>
  </div>
  <div class="result">{badge}{speedup}</div>
</div>
"#,
                label = r.test,
                oxi_w = oxi_w,
                oxi_pct = oxi_pct,
                oxi_time = fmt_ms(r.oxidb_ms),
                sql_w = sql_w,
                sql_pct = sql_pct,
                sql_time = fmt_ms(r.sqlite_ms),
                badge = badge,
                speedup = speedup_html,
            ));
        }
        html.push_str("</div>\n");
    }

    // Disk usage section
    let oxi_mb = oxi_disk as f64 / 1_048_576.0;
    let sql_mb = sql_disk as f64 / 1_048_576.0;
    let max_mb = oxi_mb.max(sql_mb);
    let oxi_pct = if max_mb > 0.0 { oxi_mb / max_mb * 100.0 } else { 50.0 };
    let sql_pct = if max_mb > 0.0 { sql_mb / max_mb * 100.0 } else { 50.0 };

    html.push_str(&format!(r#"<div class="cat">
<div class="cat-head">
  <div class="cat-icon">&#9881;</div>
  <div class="cat-name">Resources</div>
</div>
<div class="res-grid">
  <div class="res-card disk">
    <div class="res-title">Disk Usage</div>
    <div class="res-bars">
      <div class="res-row">
        <div class="res-db oxi">OxiDB</div>
        <div class="res-track"><div class="res-fill oxi" style="--w:{oxi_pct:.0}%;width:0"><span class="res-val">{oxi_mb:.1} MB</span></div></div>
      </div>
      <div class="res-row">
        <div class="res-db sql">SQLite</div>
        <div class="res-track"><div class="res-fill sql" style="--w:{sql_pct:.0}%;width:0"><span class="res-val">{sql_mb:.1} MB</span></div></div>
      </div>
    </div>
  </div>
</div>
</div>
"#,
        oxi_pct = oxi_pct,
        oxi_mb = oxi_mb,
        sql_pct = sql_pct,
        sql_mb = sql_mb,
    ));

    html.push_str(&format!(r#"
<footer>
  <span><span class="oxi-tag">OxiDB</span> embedded benchmark suite</span>
  <span>{} tests</span>
</footer>

</div>
</body>
</html>"#, total_tests));

    let report_path = "tests/comparison-sqlite/report.html";
    let _ = std::fs::create_dir_all("tests/comparison-sqlite");
    if let Err(e) = std::fs::write(report_path, &html) {
        eprintln!("Failed to write HTML report: {e}");
        return;
    }
    println!("  HTML Report: {report_path}");
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
