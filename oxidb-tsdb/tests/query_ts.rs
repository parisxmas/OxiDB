// A whole-range aggregate (no `interval`) must be timestamped at the earliest
// actual data point — not at the query's synthetic lower bound. Regression for
// the PostgREST/tsdb surface returning ts = i64::MIN/2 when no time filter was
// given (`.schema("tsdb").from("cpu").select("usage")`).

use oxidb_tsdb::{Agg, Point, QuerySpec, Tsdb};

fn tempdir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut p = std::env::temp_dir();
    p.push(format!(
        "oxidb-tsdb-qts-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn spec_no_interval(agg: Agg) -> QuerySpec {
    QuerySpec {
        measurement: "cpu".into(),
        field: "usage".into(),
        tag_filters: vec![],
        // Unbounded defaults, exactly what postgrest_tsdb passes with no ts filter.
        start: i64::MIN / 2,
        end: i64::MAX / 2,
        group_tags: vec![],
        interval: None,
        agg,
    }
}

#[test]
fn whole_range_aggregate_timestamped_at_first_point() {
    let dir = tempdir();
    let mut db = Tsdb::open(&dir).unwrap();
    let base = 1_700_000_000_000i64;
    for i in 0..5 {
        db.write(
            &Point::new("cpu", base + i * 1000)
                .tag("host", "a")
                .field("usage", i as f64),
        );
    }

    let res = db.query(&spec_no_interval(Agg::Mean));
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].points.len(), 1);
    // The bug returned i64::MIN/2 here.
    assert_eq!(res[0].points[0].ts, base, "ts must be the earliest point");
    assert!((res[0].points[0].value - 2.0).abs() < 1e-9, "mean of 0..5 == 2");

    // Points inserted out of order still report the minimum ts.
    let dir2 = tempdir();
    let mut db2 = Tsdb::open(&dir2).unwrap();
    for &t in &[base + 3000, base, base + 1000] {
        db2.write(&Point::new("cpu", t).tag("host", "a").field("usage", 1.0));
    }
    let res2 = db2.query(&spec_no_interval(Agg::Sum));
    assert_eq!(res2[0].points[0].ts, base, "min ts regardless of insert order");
}

#[test]
fn interval_query_still_uses_bucket_start() {
    let dir = tempdir();
    let mut db = Tsdb::open(&dir).unwrap();
    let base = 1_700_000_000_000i64; // a multiple we can bucket cleanly
    for i in 0..5 {
        db.write(
            &Point::new("cpu", base + i * 1000)
                .tag("host", "a")
                .field("usage", i as f64),
        );
    }
    let mut spec = spec_no_interval(Agg::Mean);
    spec.interval = Some(60_000); // 1-minute buckets; all 5 points share one
    let res = db.query(&spec);
    let bucket = base - base.rem_euclid(60_000);
    assert_eq!(res[0].points[0].ts, bucket, "interval query keeps bucket-start ts");
}
