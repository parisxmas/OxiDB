//! Measure resident-set size of resident vs disk-first mode over the same
//! data. Two phases so the (RAM-hungry) seed doesn't pollute `ru_maxrss` of
//! the measured process:
//!
//!   disk_first_rss seed     <dir> [rows]   # create + checkpoint the dataset
//!   disk_first_rss resident <dir>          # open + full scan, report peak RSS
//!   disk_first_rss disk     <dir>          # same, disk-first

use oxidb_sql::{SqlEngine, SqlOptions};

fn rss_mb() -> f64 {
    // macOS/Linux: ru_maxrss (bytes on macOS, KiB on Linux).
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let raw = ru.ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        raw / (1 << 20) as f64
    } else {
        raw / 1024.0
    }
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let dir = std::env::args()
        .nth(2)
        .expect("usage: disk_first_rss <seed|resident|disk> <dir> [rows]");

    if mode == "seed" {
        let rows: u64 = std::env::args()
            .nth(3)
            .and_then(|a| a.parse().ok())
            .unwrap_or(1_000_000);
        let db = SqlEngine::open_with_options(
            &dir,
            SqlOptions {
                disk_first: false,
                checkpoint_bytes: 0,
            },
        )
        .unwrap();
        db.execute(
            "CREATE TABLE events (id INT PRIMARY KEY, user_id INT, kind TEXT, amount DOUBLE)",
        )
        .unwrap();
        let mut batch = Vec::with_capacity(1000);
        for i in 0..rows {
            batch.push(vec![
                oxidb_sql::Value::Int(i as i64),
                oxidb_sql::Value::Int((i % 9973) as i64),
                oxidb_sql::Value::Text(format!("kind-{}", i % 17).into()),
                oxidb_sql::Value::Double(i as f64 * 0.5),
            ]);
            if batch.len() == 1000 {
                db.insert_many("events", std::mem::take(&mut batch))
                    .unwrap();
            }
        }
        if !batch.is_empty() {
            db.insert_many("events", batch).unwrap();
        }
        db.checkpoint().unwrap();
        println!("seeded {rows} rows into {dir}");
        return;
    }

    let disk = mode == "disk";
    let t0 = std::time::Instant::now();
    let db = SqlEngine::open_with_options(
        &dir,
        SqlOptions {
            disk_first: disk,
            checkpoint_bytes: 64 << 20,
        },
    )
    .unwrap();
    let open_ms = t0.elapsed().as_millis();
    let opened = rss_mb();

    // One full scan (forces every row to be visited in both modes).
    let t1 = std::time::Instant::now();
    let r = db
        .execute("SELECT COUNT(*), SUM(amount) FROM events")
        .unwrap();
    let scan_ms = t1.elapsed().as_millis();

    println!(
        "mode={} open={open_ms}ms rss_after_open={opened:.0}MB peak_after_scan={:.0}MB scan={scan_ms}ms result={r:?}",
        if disk { "disk-first" } else { "resident" },
        rss_mb(),
    );
}
