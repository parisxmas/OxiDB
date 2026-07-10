use oxidb_tsdb::{Agg, Point, QuerySpec, RollupSpec, Tsdb};

fn q(m: &str, field: &str, agg: Agg) -> QuerySpec {
    QuerySpec {
        measurement: m.into(),
        field: field.into(),
        tag_filters: vec![],
        start: i64::MIN / 2,
        end: i64::MAX / 2,
        group_tags: vec![],
        interval: None,
        agg,
    }
}

#[test]
fn rollup_1s_to_1m_mean_max_count() {
    let mut db = Tsdb::new();
    let base = 1_699_999_800_000i64; // aligned to a minute
    // 5 minutes of 1s samples: value = second-of-minute (0..59).
    for i in 0..300i64 {
        db.write(
            &Point::new("cpu", base + i * 1000)
                .tag("host", "a")
                .field("usage", (i % 60) as f64),
        );
    }
    db.add_rollup(RollupSpec {
        measurement: "cpu".into(),
        label: "1m".into(),
        interval: 60_000,
        aggs: vec![Agg::Mean, Agg::Max, Agg::Count],
    });
    // now = just past the 5th minute → 5 complete buckets.
    let now = base + 300 * 1000 + 1;
    let written = db.refresh_rollups(now);
    assert_eq!(written, 5); // 5 buckets × 1 source series

    // The rollup measurement cpu@1m, field usage_mean.
    let mean = db.query(&q("cpu@1m", "usage_mean", Agg::Last));
    assert_eq!(mean[0].points.len(), 1);
    // Each minute has values 0..59 → mean 29.5.
    assert!((mean[0].points[0].value - 29.5).abs() < 1e-6);
    let max = db.query(&q("cpu@1m", "usage_max", Agg::Max));
    assert!((max[0].points[0].value - 59.0).abs() < 1e-6);
    let count = db.query(&q("cpu@1m", "usage_count", Agg::Sum));
    assert!((count[0].points[0].value - 300.0).abs() < 1e-6); // 5×60

    // Incremental: refreshing again with no new complete buckets writes nothing.
    assert_eq!(db.refresh_rollups(now), 0);
}

#[test]
fn rollup_watermark_persists_no_double_count() {
    let dir = std::env::temp_dir().join(format!("oxidb-tsdb-roll-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let base = 1_699_999_800_000i64;
    let spec = || RollupSpec {
        measurement: "m".into(),
        label: "1m".into(),
        interval: 60_000,
        aggs: vec![Agg::Mean],
    };
    {
        let mut db = Tsdb::open(&dir).unwrap();
        for i in 0..180i64 {
            db.write(&Point::new("m", base + i * 1000).field("v", (i % 60) as f64));
        }
        db.add_rollup(spec());
        let now = base + 180 * 1000 + 1;
        assert_eq!(db.refresh_rollups(now), 3); // 3 complete minute buckets
        db.checkpoint().unwrap();
    }
    // Reopen: watermark loaded → a refresh with the same `now` adds nothing.
    let mut db = Tsdb::open(&dir).unwrap();
    db.add_rollup(spec());
    let now = base + 180 * 1000 + 1;
    assert_eq!(
        db.refresh_rollups(now),
        0,
        "watermark must prevent re-materializing"
    );
    // The 3 rollup points survived the restart.
    let mean = db.query(&q("m@1m", "v_mean", Agg::Count));
    assert_eq!(mean[0].points[0].value as i64, 3);
    std::fs::remove_dir_all(&dir).ok();
}
