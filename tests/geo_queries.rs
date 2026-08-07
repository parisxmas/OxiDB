//! Geospatial v1 (document engine): `$geoWithin` / `$near` on point fields,
//! with and without a geohash index.
//!
//! The load-bearing property is **differential**: the geo index only
//! nominates candidates and every candidate is verified against the exact
//! predicate, so an indexed query must return byte-for-byte what the
//! unindexed scan returns — across inserts, updates, deletes, and a reopen
//! (the text index taught us the reopen half: a definition that isn't
//! persisted or rebuilt answers wrongly after every deploy).

use oxidb::OxiDb;
use oxidb::query::FindOptions;
use serde_json::{Value, json};
use tempfile::tempdir;

/// Istanbul city center.
const CENTER: (f64, f64) = (28.9784, 41.0082);

fn seed(db: &OxiDb, col: &str) {
    // A 21×21 grid (~1.1 km spacing) around Istanbul, one far-away point,
    // one doc with no location, one with each accepted point shape.
    let mut docs = Vec::new();
    for dx in -10..=10 {
        for dy in -10..=10 {
            docs.push(json!({
                "name": format!("g{dx}_{dy}"),
                "loc": [CENTER.0 + dx as f64 * 0.01, CENTER.1 + dy as f64 * 0.01],
                "kind": if dx < 0 { "west" } else { "east" },
            }));
        }
    }
    docs.push(json!({"name": "toronto", "loc": [-79.3829, 43.6544], "kind": "far"}));
    docs.push(json!({"name": "nowhere", "kind": "lost"}));
    docs.push(json!({"name": "geojson",
        "loc": {"type": "Point", "coordinates": [CENTER.0, CENTER.1]}, "kind": "east"}));
    docs.push(json!({"name": "lonlat",
        "loc": {"lon": CENTER.0 + 0.001, "lat": CENTER.1}, "kind": "east"}));
    db.insert_many(col, docs).unwrap();
}

fn names(docs: &[Value]) -> Vec<String> {
    docs.iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect()
}

fn find(db: &OxiDb, col: &str, q: Value) -> Vec<Value> {
    db.find_with_options(col, &q, &FindOptions::default())
        .unwrap()
}

const WITHIN_2KM: &str =
    r#"{"loc": {"$geoWithin": {"$centerSphere": [[28.9784, 41.0082], 0.000313926]}}}"#;

#[test]
fn geowithin_and_near_answer_identically_with_and_without_the_index() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, "places");

    let queries: Vec<Value> = vec![
        serde_json::from_str(WITHIN_2KM).unwrap(), // 2 km ≈ 0.000313926 rad
        json!({"loc": {"$geoWithin": {"$box": [[28.95, 40.99], [29.00, 41.02]]}}}),
        json!({"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [28.9784, 41.0082]},
                                  "$maxDistance": 3000.0}}}),
        json!({"loc": {"$near": [28.9784, 41.0082], "$maxDistance": 1500.0}}),
        // Geo AND a plain predicate.
        json!({"kind": "east",
               "loc": {"$geoWithin": {"$centerSphere": [[28.9784, 41.0082], 0.000313926]}}}),
    ];

    // Scan answers first (no index exists yet)… Compared as SORTED sets:
    // without an explicit sort, result order is unspecified (the index
    // iterates ids, the scan iterates storage) — `$near` ordering has its
    // own test, which asserts non-decreasing distance rather than a fixed
    // permutation (grid points tie).
    let sorted = |mut v: Vec<String>| {
        v.sort();
        v
    };
    let scanned: Vec<Vec<String>> = queries
        .iter()
        .map(|q| sorted(names(&find(&db, "places", q.clone()))))
        .collect();
    assert!(
        scanned[0].len() > 3 && scanned[0].len() < 100,
        "seed geometry sanity: {} in 2km",
        scanned[0].len()
    );
    assert!(scanned.iter().all(|r| !r.contains(&"toronto".to_string())));
    assert!(scanned.iter().all(|r| !r.contains(&"nowhere".to_string())));
    // Every accepted point shape matched the circle.
    assert!(scanned[0].contains(&"geojson".to_string()));
    assert!(scanned[0].contains(&"lonlat".to_string()));

    // …then the same queries through the index must agree exactly.
    db.create_geo_index("places", "loc").unwrap();
    for (q, want) in queries.iter().zip(&scanned) {
        let got = sorted(names(&find(&db, "places", q.clone())));
        assert_eq!(&got, want, "index/scan divergence for {q}");
    }
}

#[test]
fn near_sorts_nearest_first_and_respects_explicit_sort() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, "places");
    db.create_geo_index("places", "loc").unwrap();

    let q = json!({"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [28.9784, 41.0082]},
                                      "$maxDistance": 5000.0}}});
    let docs = find(&db, "places", q.clone());
    assert!(docs.len() > 5);
    // Distances must be non-decreasing.
    let dist = |d: &Value| -> f64 {
        let l = &d["loc"];
        let (lon, lat) = if l.is_array() {
            (l[0].as_f64().unwrap(), l[1].as_f64().unwrap())
        } else if l.get("coordinates").is_some() {
            (
                l["coordinates"][0].as_f64().unwrap(),
                l["coordinates"][1].as_f64().unwrap(),
            )
        } else {
            (l["lon"].as_f64().unwrap(), l["lat"].as_f64().unwrap())
        };
        let dlat = (lat - CENTER.1).to_radians();
        let dlon = (lon - CENTER.0).to_radians();
        let h = (dlat / 2.0).sin().powi(2)
            + CENTER.1.to_radians().cos() * lat.to_radians().cos() * (dlon / 2.0).sin().powi(2);
        2.0 * 6_371_008.8 * h.sqrt().asin()
    };
    let ds: Vec<f64> = docs.iter().map(dist).collect();
    assert!(
        ds.windows(2).all(|w| w[0] <= w[1] + 1e-6),
        "not nearest-first: {ds:?}"
    );
    // The nearest is the exact-center GeoJSON doc (distance 0) or g0_0.
    assert!(ds[0] < 1.0);

    // An explicit sort overrides the implicit distance order.
    let opts = FindOptions {
        sort: Some(vec![("name".to_string(), oxidb::query::SortOrder::Asc)]),
        ..Default::default()
    };
    let sorted = db.find_with_options("places", &q, &opts).unwrap();
    let ns = names(&sorted);
    let mut expect = ns.clone();
    expect.sort();
    assert_eq!(ns, expect, "explicit sort must win over $near ordering");

    // $minDistance excludes the closest ring.
    let q = json!({"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [28.9784, 41.0082]},
                                      "$maxDistance": 5000.0, "$minDistance": 2000.0}}});
    let docs = find(&db, "places", q);
    assert!(!docs.is_empty());
    assert!(
        docs.iter()
            .map(dist)
            .all(|d| (2000.0..=5000.0).contains(&d))
    );
}

#[test]
fn updates_deletes_and_reopen_keep_the_index_truthful() {
    let dir = tempdir().unwrap();
    {
        let db = OxiDb::open(dir.path()).unwrap();
        seed(&db, "places");
        db.create_geo_index("places", "loc").unwrap();

        let within: Value = serde_json::from_str(WITHIN_2KM).unwrap();
        let before = names(&find(&db, "places", within.clone()));
        assert!(before.contains(&"g0_0".to_string()));

        // Move a matching doc to Toronto: it must vanish from the circle.
        db.update(
            "places",
            &json!({"name": "g0_0"}),
            &json!({"$set": {"loc": [-79.3829, 43.6544]}}),
        )
        .unwrap();
        // Move the far doc into the circle: it must appear.
        db.update(
            "places",
            &json!({"name": "toronto"}),
            &json!({"$set": {"loc": [28.9784, 41.0082]}}),
        )
        .unwrap();
        // Delete one in-circle doc outright.
        db.delete("places", &json!({"name": "geojson"})).unwrap();

        let after = names(&find(&db, "places", within.clone()));
        assert!(
            !after.contains(&"g0_0".to_string()),
            "moved-out doc still matches"
        );
        assert!(
            after.contains(&"toronto".to_string()),
            "moved-in doc missing"
        );
        assert!(
            !after.contains(&"geojson".to_string()),
            "deleted doc still matches"
        );
    }

    // Reopen: the definition persisted, the table rebuilt, answers identical
    // to a fresh scan (drop the index and compare).
    let db = OxiDb::open(dir.path()).unwrap();
    let listed = db.list_indexes("places").unwrap();
    assert!(
        listed
            .iter()
            .any(|i| i.index_type == "geo" && i.fields == vec!["loc".to_string()]),
        "geo index definition lost across reopen: {listed:?}"
    );
    let within: Value = serde_json::from_str(WITHIN_2KM).unwrap();
    let mut indexed = names(&find(&db, "places", within.clone()));
    indexed.sort();
    db.drop_index("places", "_geo_loc").unwrap();
    assert!(
        db.list_indexes("places")
            .unwrap()
            .iter()
            .all(|i| i.index_type != "geo"),
        "drop_index left the geo definition behind"
    );
    let mut scanned = names(&find(&db, "places", within));
    scanned.sort();
    assert_eq!(indexed, scanned, "post-reopen index diverges from scan");
    assert!(indexed.contains(&"toronto".to_string()));
    assert!(!indexed.contains(&"g0_0".to_string()));
}

#[test]
fn unsupported_shapes_are_refused_not_ignored() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, "places");
    for q in [
        json!({"loc": {"$geoWithin": {"$center": [[28.9, 41.0], 5.0]}}}),
        json!({"loc": {"$geoWithin": {"$geometry": {"type": "Polygon", "coordinates": []}}}}),
        json!({"loc": {"$geoWithin": {"$centerSphere": [[28.9, 41.0], -1.0]}}}),
        json!({"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [200.0, 0.0]}}}}),
    ] {
        assert!(
            db.find_with_options("places", &q, &FindOptions::default())
                .is_err(),
            "should refuse: {q}"
        );
    }
}

#[test]
fn compound_query_intersects_geo_with_field_indexes() {
    // "active AND in this viewport" — the live-tracking shape. Before
    // 0.42.10 any usable field index switched the geo index OFF: the plan
    // examined every "east" row and verified each against the circle. The
    // candidate sets must intersect instead, and explain (which reuses the
    // real planner) is where that is observable.
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, "places"); // 441 grid points + extras; ~220 are "east"
    db.create_index("places", "kind").unwrap();
    db.create_geo_index("places", "loc").unwrap();

    let q = json!({"kind": "east",
        "loc": {"$geoWithin": {"$centerSphere": [[28.9784, 41.0082], 0.000313926]}}});

    // Correctness first: same rows as the unindexed differential baseline.
    let mut with_idx = names(&find(&db, "places", q.clone()));
    let baseline: Vec<String> = find(&db, "places", json!({"kind": "east"}))
        .iter()
        .filter(|d| {
            names(&find(
                &db,
                "places",
                serde_json::from_str::<Value>(WITHIN_2KM).unwrap(),
            ))
            .contains(&d["name"].as_str().unwrap().to_string())
        })
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    let mut baseline = baseline;
    with_idx.sort();
    baseline.sort();
    assert_eq!(with_idx, baseline);
    assert!(!with_idx.is_empty(), "the viewport must contain east rows");

    // Plan: candidates must be far fewer than the ~220 "east" rows — the
    // geo cover intersected the field-index set.
    let plan = db
        .explain_find("places", &q, &FindOptions::default())
        .unwrap();
    let candidates = plan["candidates"]
        .as_u64()
        .expect("INDEX_SCAN with candidates") as usize;
    let east_rows = find(&db, "places", json!({"kind": "east"})).len();
    assert!(
        candidates < east_rows / 2,
        "geo cover must shrink the field-index candidate set: {candidates} vs {east_rows} east rows (plan: {plan})"
    );
    assert_eq!(plan["strategy"], "INDEX_SCAN", "plan: {plan}");
}

#[test]
fn unbounded_near_with_limit_uses_expanding_rings_and_matches_the_scan() {
    // "The 10 nearest" with NO $maxDistance: before 0.42.10 the geo index
    // had no circle to cover, so this always scanned the collection. The
    // expanding-ring path must (a) be chosen — explain says GEO_KNN — and
    // (b) answer exactly what the unindexed scan answers. Grid spacing is
    // ~1.1 km and the ring starts at 500 m, so k=10 forces several
    // expansions. Distances (not names) are compared per position: the
    // grid is symmetric, so equal-distance ties may legally order
    // differently between the two paths.
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, "idx");
    seed(&db, "raw");
    db.create_geo_index("idx", "loc").unwrap();

    let q = json!({"loc": {"$near": {"$geometry": [CENTER.0, CENTER.1]}}});
    let opts = FindOptions {
        sort: None,
        skip: None,
        limit: Some(10),
    };
    let center = oxidb::geo::Point {
        lon: CENTER.0,
        lat: CENTER.1,
    };
    let dist_of = |d: &Value| {
        oxidb::geo::point_of_value(&d["loc"])
            .map(|p| oxidb::geo::haversine_m(center, p))
            .unwrap()
    };

    let with_idx = db.find_with_options("idx", &q, &opts).unwrap();
    let scanned = db.find_with_options("raw", &q, &opts).unwrap();
    assert_eq!(with_idx.len(), 10);
    let da: Vec<i64> = with_idx.iter().map(|d| dist_of(d).round() as i64).collect();
    let db_: Vec<i64> = scanned.iter().map(|d| dist_of(d).round() as i64).collect();
    assert_eq!(da, db_, "ring answer must equal the scan answer");

    let plan = db.explain_find("idx", &q, &opts).unwrap();
    assert_eq!(plan["strategy"], "GEO_KNN", "plan: {plan}");
    // The unindexed collection keeps the scan plan.
    let plan_raw = db.explain_find("raw", &q, &opts).unwrap();
    assert_eq!(plan_raw["strategy"], "COLLSCAN", "plan: {plan_raw}");
}

#[test]
fn unbounded_near_ring_respects_the_rest_of_the_query() {
    // The ring counts only FULL-query matches: asking for the 5 nearest
    // "west" docs from an east-side viewpoint must keep expanding past all
    // the nearer east docs — stopping when 5 of ANY kind are in the ring
    // would under-deliver. Differential against the unindexed scan.
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    seed(&db, "idx");
    seed(&db, "raw");
    db.create_geo_index("idx", "loc").unwrap();

    // Viewpoint far on the east edge of the grid; "west" docs are ≥ ~11 km.
    let east_edge = [CENTER.0 + 0.10, CENTER.1];
    let q = json!({"kind": "west",
        "loc": {"$near": {"$geometry": east_edge}}});
    let opts = FindOptions {
        sort: None,
        skip: None,
        limit: Some(5),
    };
    let with_idx = db.find_with_options("idx", &q, &opts).unwrap();
    let scanned = db.find_with_options("raw", &q, &opts).unwrap();
    assert_eq!(with_idx.len(), 5);
    assert!(with_idx.iter().all(|d| d["kind"] == "west"));
    let names_a = names(&with_idx);
    let names_b = names(&scanned);
    assert_eq!(names_a, names_b, "ring must match the scan");
}
