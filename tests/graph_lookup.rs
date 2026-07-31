//! `$graphLookup` — breadth-first traversal in the aggregation pipeline.
//!
//! The semantics worth pinning are the ones a naive implementation gets
//! wrong: a cycle must terminate (values expand once), a diamond must emit
//! the shared descendant once (`_id` dedup), `maxDepth: 0` means "only the
//! first hop", `depthField` records the round a document was found in, and
//! `restrictSearchWithMatch` filters the traversal itself — a pruned node's
//! children are unreachable through it.

use oxidb::OxiDb;
use serde_json::{Value, json};
use tempfile::tempdir;

fn names(arr: &Value) -> Vec<String> {
    let mut v: Vec<String> = arr
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

/// The MongoDB documentation's own example shape: an org chart traversed
/// upward through `reportsTo`.
#[test]
fn org_chart_walks_the_reporting_chain() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    for (name, boss) in [
        ("dev", Some("eliot")),
        ("eliot", Some("ron")),
        ("ron", Some("andrew")),
        ("andrew", None),
        ("asya", Some("ron")),
    ] {
        let doc = match boss {
            Some(b) => json!({"name": name, "reportsTo": b}),
            None => json!({"name": name}),
        };
        db.insert("employees", doc).unwrap();
    }

    let out = db
        .aggregate(
            "employees",
            &json!([
                {"$match": {"name": "dev"}},
                {"$graphLookup": {
                    "from": "employees",
                    "startWith": "$reportsTo",
                    "connectFromField": "reportsTo",
                    "connectToField": "name",
                    "as": "chain",
                    "depthField": "up"
                }}
            ]),
        )
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(names(&out[0]["chain"]), vec!["andrew", "eliot", "ron"]);
    // Depth: eliot found in round 0, ron in 1, andrew in 2.
    for d in out[0]["chain"].as_array().unwrap() {
        let want = match d["name"].as_str().unwrap() {
            "eliot" => 0,
            "ron" => 1,
            "andrew" => 2,
            other => panic!("unexpected {other}"),
        };
        assert_eq!(d["up"], json!(want));
    }
}

#[test]
fn cycles_terminate_and_diamonds_deduplicate() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    // A → B → D, A → C → D, and D → A closing a cycle.
    for (name, next) in [
        ("A", json!(["B", "C"])),
        ("B", json!(["D"])),
        ("C", json!(["D"])),
        ("D", json!(["A"])),
    ] {
        db.insert("nodes", json!({"name": name, "next": next}))
            .unwrap();
    }
    let out = db
        .aggregate(
            "nodes",
            &json!([
                {"$match": {"name": "A"}},
                {"$graphLookup": {
                    "from": "nodes",
                    "startWith": "$next",
                    "connectFromField": "next",
                    "connectToField": "name",
                    "as": "reach"
                }}
            ]),
        )
        .unwrap();
    // Everything is reachable; each node exactly once; and it returned at
    // all (the cycle did not loop forever).
    assert_eq!(names(&out[0]["reach"]), vec!["A", "B", "C", "D"]);
}

#[test]
fn max_depth_zero_is_one_hop() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    for (name, next) in [("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")] {
        db.insert("hops", json!({"name": name, "next": next}))
            .unwrap();
    }
    db.insert("hops", json!({"name": "e"})).unwrap(); // terminal node
    let run = |max_depth: u64| -> Vec<String> {
        let out = db
            .aggregate(
                "hops",
                &json!([
                    {"$match": {"name": "a"}},
                    {"$graphLookup": {
                        "from": "hops",
                        "startWith": "$next",
                        "connectFromField": "next",
                        "connectToField": "name",
                        "as": "r",
                        "maxDepth": max_depth
                    }}
                ]),
            )
            .unwrap();
        names(&out[0]["r"])
    };
    assert_eq!(run(0), vec!["b"]);
    assert_eq!(run(1), vec!["b", "c"]);
    assert_eq!(run(10), vec!["b", "c", "d", "e"]);
}

#[test]
fn restriction_prunes_the_traversal_not_just_the_output() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    // a → blocked → c: with `blocked` filtered out, c must be unreachable —
    // a post-filter on the result would still contain c.
    for (name, next, ok) in [
        ("a", Some("blocked"), true),
        ("blocked", Some("c"), false),
        ("c", None, true),
    ] {
        let mut d = json!({"name": name, "ok": ok});
        if let Some(n) = next {
            d["next"] = json!(n);
        }
        db.insert("filtered", d).unwrap();
    }
    let out = db
        .aggregate(
            "filtered",
            &json!([
                {"$match": {"name": "a"}},
                {"$graphLookup": {
                    "from": "filtered",
                    "startWith": "$next",
                    "connectFromField": "next",
                    "connectToField": "name",
                    "as": "r",
                    "restrictSearchWithMatch": {"ok": true}
                }}
            ]),
        )
        .unwrap();
    assert_eq!(names(&out[0]["r"]), Vec::<String>::new());
}

/// Route-shaped data: edges as arrays (`connects: [...]`), traversal from a
/// literal-ish start (a field), across another collection.
#[test]
fn airline_routes_reach_across_collections() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    for (ap, connects) in [
        ("JFK", json!(["BOS", "ORD"])),
        ("BOS", json!(["JFK", "PWM"])),
        ("ORD", json!(["JFK"])),
        ("PWM", json!(["BOS", "LHR"])),
        ("LHR", json!(["PWM"])),
        ("SYD", json!([])), // unreachable island
    ] {
        db.insert("airports", json!({"name": ap, "connects": connects}))
            .unwrap();
    }
    db.insert("travelers", json!({"who": "dev", "nearestAirport": "JFK"}))
        .unwrap();

    let out = db
        .aggregate(
            "travelers",
            &json!([
                {"$graphLookup": {
                    "from": "airports",
                    "startWith": "$nearestAirport",
                    "connectFromField": "connects",
                    "connectToField": "name",
                    "as": "destinations",
                    "depthField": "hops"
                }}
            ]),
        )
        .unwrap();
    let dests = names(&out[0]["destinations"]);
    assert_eq!(dests, vec!["BOS", "JFK", "LHR", "ORD", "PWM"]);
    assert!(!dests.contains(&"SYD".to_string()));
    // LHR is 3 hops out (JFK:0 → BOS/ORD:1 → PWM:2 → LHR:3).
    for d in out[0]["destinations"].as_array().unwrap() {
        if d["name"] == "LHR" {
            assert_eq!(d["hops"], json!(3));
        }
    }
}

#[test]
fn missing_start_and_bad_specs() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    db.insert("things", json!({"name": "loner"})).unwrap();
    // Missing startWith field → empty result, not an error.
    let out = db
        .aggregate(
            "things",
            &json!([
                {"$graphLookup": {
                    "from": "things",
                    "startWith": "$nope",
                    "connectFromField": "next",
                    "connectToField": "name",
                    "as": "r"
                }}
            ]),
        )
        .unwrap();
    assert_eq!(out[0]["r"], json!([]));
    // Spec errors are loud, at parse time.
    assert!(
        db.aggregate("things", &json!([{"$graphLookup": {"from": "things"}}]))
            .is_err()
    );
    assert!(
        db.aggregate(
            "things",
            &json!([{"$graphLookup": {
                "from": "things", "startWith": "$x", "connectFromField": "a",
                "connectToField": "b", "as": "r", "maxDepth": -1
            }}])
        )
        .is_err()
    );
}
