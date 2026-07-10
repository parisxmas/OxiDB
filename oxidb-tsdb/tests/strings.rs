use oxidb_tsdb::{Agg, Point, QuerySpec, StrValue, Tsdb};

fn spec(m: &str, field: &str, agg: Agg) -> QuerySpec {
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
fn string_field_first_last_count_distinct() {
    let mut db = Tsdb::new();
    let base = 1_700_000_000_000i64;
    let states = ["ok", "ok", "warn", "error", "error", "ok"];
    for (i, s) in states.iter().enumerate() {
        db.write(
            &Point::new("svc", base + i as i64 * 1000)
                .tag("host", "a")
                .field_str("status", s),
        );
    }
    assert!(db.is_string_field("svc", "status"));
    assert!(!db.is_string_field("svc", "nope"));

    let last = &db.query_str(&spec("svc", "status", Agg::Last))[0].points[0].value;
    assert_eq!(*last, StrValue::Text("ok".into()));
    let first = &db.query_str(&spec("svc", "status", Agg::First))[0].points[0].value;
    assert_eq!(*first, StrValue::Text("ok".into()));
    let count = &db.query_str(&spec("svc", "status", Agg::Count))[0].points[0].value;
    assert_eq!(*count, StrValue::Num(6.0));
    let distinct = &db.query_str(&spec("svc", "status", Agg::Distinct))[0].points[0].value;
    assert_eq!(*distinct, StrValue::Num(3.0)); // ok, warn, error
}

#[test]
fn string_fields_persist_across_reopen() {
    let dir = std::env::temp_dir().join(format!("oxidb-tsdb-str-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let base = 1_700_000_000_000i64;
    {
        let mut db = Tsdb::open(&dir).unwrap();
        db.write(
            &Point::new("svc", base)
                .tag("h", "a")
                .field_str("status", "ok")
                .field("load", 0.5),
        );
        db.write(
            &Point::new("svc", base + 1000)
                .tag("h", "a")
                .field_str("status", "error"),
        );
        db.checkpoint().unwrap();
        db.write(
            &Point::new("svc", base + 2000)
                .tag("h", "a")
                .field_str("status", "ok"),
        ); // WAL only
    }
    let db = Tsdb::open(&dir).unwrap();
    // 3 status points + 1 numeric load point.
    assert_eq!(db.point_count(), 4);
    let last = &db.query_str(&spec("svc", "status", Agg::Last))[0].points[0].value;
    assert_eq!(*last, StrValue::Text("ok".into()));
    let distinct = &db.query_str(&spec("svc", "status", Agg::Distinct))[0].points[0].value;
    assert_eq!(*distinct, StrValue::Num(2.0));
    std::fs::remove_dir_all(&dir).ok();
}
