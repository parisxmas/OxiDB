use oxidb_tsdb::{Agg, Point, QuerySpec, TagPredicate, Tsdb};

fn spec(m: &str, field: &str) -> QuerySpec {
    QuerySpec {
        measurement: m.into(),
        field: field.into(),
        tag_filters: vec![],
        start: i64::MIN / 2,
        end: i64::MAX / 2,
        group_tags: vec![],
        interval: None,
        agg: Agg::Mean,
    }
}

#[test]
fn ingest_query_downsample_group() {
    let mut db = Tsdb::new().with_block_points(256);
    // Aligned to a 10-minute boundary so epoch-aligned buckets come out clean.
    let base = 1_699_999_800_000i64;
    // 2 hosts, 1 point/sec for 1 hour, usage = host offset + slow sine.
    for host in ["a", "b"] {
        let off = if host == "a" { 10.0 } else { 50.0 };
        for i in 0..3600 {
            let p = Point::new("cpu", base + i * 1000)
                .tag("host", host)
                .tag("region", if host == "a" { "eu" } else { "us" })
                .field("usage", off + (i as f64 / 600.0).sin());
            db.write(&p);
        }
    }
    assert_eq!(db.series_count(), 2);
    assert_eq!(db.point_count(), 7200);

    // Whole-range mean per host.
    let mut s = spec("cpu", "usage");
    s.group_tags = vec!["host".into()];
    let res = db.query(&s);
    assert_eq!(res.len(), 2);
    let a = res
        .iter()
        .find(|r| r.tags == vec![("host".into(), "a".into())])
        .unwrap();
    assert_eq!(a.points.len(), 1);
    assert!((a.points[0].value - 10.0).abs() < 0.5);

    // Filter host=b, downsample to 10-minute means → 6 buckets.
    let mut s = spec("cpu", "usage");
    s.tag_filters = vec![TagPredicate {
        key: "host".into(),
        value: "b".into(),
    }];
    s.interval = Some(600_000);
    let res = db.query(&s);
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].points.len(), 6);
    for p in &res[0].points {
        assert!((p.value - 50.0).abs() < 1.5);
    }

    // count agg over the hour for host=a.
    let mut s = spec("cpu", "usage");
    s.agg = Agg::Count;
    s.tag_filters = vec![TagPredicate {
        key: "host".into(),
        value: "a".into(),
    }];
    let res = db.query(&s);
    assert_eq!(res[0].points[0].value as i64, 3600);
}

#[test]
fn compression_ratio_and_retention() {
    let mut db = Tsdb::new().with_block_points(1024);
    let base = 1_700_000_000_000i64;
    for i in 0..100_000 {
        // Rounded gauge (holds for stretches) — the realistic TSDB shape.
        let v = ((42.0 + (i as f64 / 1000.0).sin() * 5.0) * 2.0).round() / 2.0;
        db.write(
            &Point::new("m", base + i * 1000)
                .tag("id", "x")
                .field("v", v),
        );
    }
    let raw = db.point_count() * 16;
    let comp = db.compressed_bytes();
    assert!(comp * 8 < raw, "compressed {comp} vs raw {raw}");

    // Retention: drop everything before the midpoint (block-granular).
    let cutoff = base + 50_000 * 1000;
    let removed = db.enforce_retention(cutoff);
    assert!(removed > 40_000 && removed <= 50_000, "removed {removed}");
}
