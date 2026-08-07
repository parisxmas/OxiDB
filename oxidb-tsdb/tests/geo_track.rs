// Geo awareness over lat/lon field pairs: `distance_query` (path length in
// meters — "how many km did u42 travel today") and `track_query`
// (Douglas-Peucker-simplified fix lists for drawing a day on a map).
// Distances are checked against independently computed haversine values;
// simplification is checked by its defining property (kept points bound the
// dropped ones within tolerance), not by implementation details.

use oxidb_tsdb::{Point, TagPredicate, TrackSpec, Tsdb};

const KM_PER_DEG_LAT: f64 = 110.574;

fn spec(measurement: &str) -> TrackSpec {
    TrackSpec {
        measurement: measurement.into(),
        lat_field: "lat".into(),
        lon_field: "lon".into(),
        tag_filters: vec![],
        start: i64::MIN / 2,
        end: i64::MAX / 2,
        group_tags: vec![],
        interval: None,
    }
}

fn write_fix(db: &mut Tsdb, driver: &str, ts: i64, lat: f64, lon: f64) {
    db.write(
        &Point::new("pos", ts)
            .tag("driver", driver)
            .field("lat", lat)
            .field("lon", lon),
    );
}

#[test]
fn distance_sums_consecutive_fixes_per_tag_set() {
    let mut db = Tsdb::new();
    // u42 drives 0.01° north twice (~1.1057 km each); u7 stands still.
    write_fix(&mut db, "u42", 1_000, 41.00, 29.0);
    write_fix(&mut db, "u42", 2_000, 41.01, 29.0);
    write_fix(&mut db, "u42", 3_000, 41.02, 29.0);
    write_fix(&mut db, "u7", 1_500, 39.0, 32.0);
    write_fix(&mut db, "u7", 2_500, 39.0, 32.0);

    let mut s = spec("pos");
    s.group_tags = vec!["driver".into()];
    let mut out = db.distance_query(&s);
    out.sort_by(|a, b| a.tags.cmp(&b.tags));
    assert_eq!(out.len(), 2);

    let u42 = &out[0]; // "u42" < "u7" lexicographically
    assert_eq!(u42.tags, vec![("driver".to_string(), "u42".to_string())]);
    let meters = u42.points[0].value;
    let expect = 2.0 * 0.01 * KM_PER_DEG_LAT * 1000.0;
    assert!(
        (meters - expect).abs() / expect < 0.01,
        "expected ~{expect} m, got {meters}"
    );

    let u7 = &out[1];
    assert_eq!(u7.points[0].value, 0.0, "a parked vehicle travels 0 m");
}

#[test]
fn distance_buckets_by_interval_and_filters_by_tag() {
    let mut db = Tsdb::new();
    // Two segments completing in bucket [0,60s), one in [60s,120s).
    write_fix(&mut db, "u42", 10_000, 41.00, 29.0);
    write_fix(&mut db, "u42", 30_000, 41.01, 29.0);
    write_fix(&mut db, "u42", 50_000, 41.02, 29.0);
    write_fix(&mut db, "u42", 70_000, 41.03, 29.0);
    // Another driver's movement must not leak into u42's answer.
    write_fix(&mut db, "u9", 20_000, 10.0, 10.0);
    write_fix(&mut db, "u9", 40_000, 11.0, 10.0);

    let mut s = spec("pos");
    s.tag_filters = vec![TagPredicate {
        key: "driver".into(),
        value: "u42".into(),
    }];
    s.interval = Some(60_000);
    let out = db.distance_query(&s);
    assert_eq!(out.len(), 1);
    let seg = 0.01 * KM_PER_DEG_LAT * 1000.0;
    let points = &out[0].points;
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].ts, 0);
    assert!((points[0].value - 2.0 * seg).abs() / seg < 0.02);
    assert_eq!(points[1].ts, 60_000);
    assert!((points[1].value - seg).abs() / seg < 0.02);
}

#[test]
fn unpaired_samples_are_not_fixes() {
    let mut db = Tsdb::new();
    write_fix(&mut db, "u1", 1_000, 41.0, 29.0);
    // A lat sample with no lon twin: not a position, contributes nothing.
    db.write(
        &Point::new("pos", 2_000)
            .tag("driver", "u1")
            .field("lat", 45.0),
    );
    write_fix(&mut db, "u1", 3_000, 41.0, 29.0);

    let out = db.distance_query(&spec("pos"));
    assert_eq!(out.len(), 1);
    assert!(
        out[0].points[0].value < 1.0,
        "the phantom 45° detour must not count: {} m",
        out[0].points[0].value
    );
}

#[test]
fn track_simplification_keeps_the_shape_within_tolerance() {
    let mut db = Tsdb::new();
    // A straight east-bound track of 100 fixes with one sharp detour.
    for i in 0..100i64 {
        let lat = if i == 50 { 41.02 } else { 41.0 };
        write_fix(&mut db, "u42", i * 1_000, lat, 29.0 + i as f64 * 0.001);
    }

    // Tolerance 0: everything is returned.
    let raw = db.track_query(&spec("pos"), 0.0);
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].1.len(), 100);

    // 50 m tolerance: the straight run collapses, the ~2.2 km detour stays.
    let simplified = db.track_query(&spec("pos"), 50.0);
    let fixes = &simplified[0].1;
    assert!(
        fixes.len() < 10,
        "a straight line must collapse: {} fixes kept",
        fixes.len()
    );
    assert!(
        fixes.iter().any(|(_, lat, _)| *lat > 41.015),
        "the detour must survive simplification: {fixes:?}"
    );
    // Endpoints always survive.
    assert_eq!(fixes.first().unwrap().0, 0);
    assert_eq!(fixes.last().unwrap().0, 99_000);

    // A tolerance larger than the detour flattens it too.
    let flat = db.track_query(&spec("pos"), 5_000.0);
    assert_eq!(flat[0].1.len(), 2, "only the endpoints remain");
}
