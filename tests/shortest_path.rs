//! `$shortestPath` — Dijkstra over an edge collection.
//!
//! Pinned semantics: the cheap-but-long route beats the expensive direct
//! edge (this is what separates Dijkstra from BFS), parallel edges resolve
//! to the cheapest, direction matters unless `undirected`, an unreachable
//! target is `[] + null` (not an error), source == target is a zero-cost
//! empty path, `restrictSearchWithMatch` prunes edges out of the SEARCH,
//! and a negative weight is refused by name — Dijkstra's precondition.

use oxidb::OxiDb;
use serde_json::{Value, json};
use tempfile::tempdir;

fn edge(db: &OxiDb, a: &str, b: &str, km: f64) {
    db.insert("roads", json!({"a": a, "b": b, "km": km}))
        .unwrap();
}

fn route_names(doc: &Value) -> Vec<(String, String)> {
    doc["route"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["a"].as_str().unwrap().to_string(),
                e["b"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn run(db: &OxiDb, spec: Value) -> Value {
    let out = db
        .aggregate("trips", &json!([{ "$shortestPath": spec }]))
        .unwrap();
    out.into_iter().next().unwrap()
}

fn base_spec() -> Value {
    json!({
        "from": "roads",
        "source": "$src", "target": "$dst",
        "edgeFrom": "a", "edgeTo": "b",
        "weight": "km",
        "as": "route", "costField": "total"
    })
}

#[test]
fn cheapest_route_beats_the_direct_edge() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    // Direct A→D costs 100; the scenic A→B→C→D costs 30.
    edge(&db, "A", "D", 100.0);
    edge(&db, "A", "B", 10.0);
    edge(&db, "B", "C", 10.0);
    edge(&db, "C", "D", 10.0);
    db.insert("trips", json!({"src": "A", "dst": "D"})).unwrap();

    let doc = run(&db, base_spec());
    assert_eq!(doc["total"], json!(30.0));
    assert_eq!(
        route_names(&doc),
        vec![
            ("A".into(), "B".into()),
            ("B".into(), "C".into()),
            ("C".into(), "D".into())
        ]
    );

    // Without weights every edge counts 1 — now the direct hop wins.
    let mut spec = base_spec();
    spec.as_object_mut().unwrap().remove("weight");
    let doc = run(&db, spec);
    assert_eq!(doc["total"], json!(1.0));
    assert_eq!(route_names(&doc), vec![("A".into(), "D".into())]);
}

#[test]
fn direction_matters_unless_undirected() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    edge(&db, "B", "A", 5.0); // only B→A exists
    db.insert("trips", json!({"src": "A", "dst": "B"})).unwrap();

    // Directed: A cannot reach B.
    let doc = run(&db, base_spec());
    assert_eq!(doc["route"], json!([]));
    assert_eq!(doc["total"], Value::Null);

    // Undirected: the same edge carries the trip.
    let mut spec = base_spec();
    spec["undirected"] = json!(true);
    let doc = run(&db, spec);
    assert_eq!(doc["total"], json!(5.0));
    assert_eq!(doc["route"].as_array().unwrap().len(), 1);
}

#[test]
fn parallel_edges_resolve_to_the_cheapest() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    edge(&db, "A", "B", 9.0);
    edge(&db, "A", "B", 3.0); // the toll road and the free one
    db.insert("trips", json!({"src": "A", "dst": "B"})).unwrap();
    let doc = run(&db, base_spec());
    assert_eq!(doc["total"], json!(3.0));
    assert_eq!(doc["route"][0]["km"], json!(3.0));
}

#[test]
fn zero_length_and_pruning_and_max_cost() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    edge(&db, "A", "B", 10.0);
    db.insert(
        "roads",
        json!({"a": "A", "b": "B", "km": 1.0, "closed": true}),
    )
    .unwrap();
    db.insert("trips", json!({"src": "A", "dst": "A"})).unwrap();

    // Source == target: zero-cost empty path.
    let doc = run(&db, base_spec());
    assert_eq!(doc["total"], json!(0.0));
    assert_eq!(doc["route"], json!([]));

    // Pruning: the closed 1 km shortcut must not be searched — the open
    // 10 km edge wins. A post-filter would have used the shortcut.
    db.delete("trips", &json!({})).unwrap();
    db.insert("trips", json!({"src": "A", "dst": "B"})).unwrap();
    let mut spec = base_spec();
    spec["restrictSearchWithMatch"] = json!({"closed": {"$ne": true}});
    let doc = run(&db, spec);
    assert_eq!(doc["total"], json!(10.0));

    // maxCost below the only open route: honest "not found". (The closed
    // 1 km shortcut stays filtered — without the filter it would satisfy
    // maxCost, which is exactly what the first run of this test proved.)
    let mut spec = base_spec();
    spec["restrictSearchWithMatch"] = json!({"closed": {"$ne": true}});
    spec["maxCost"] = json!(5.0);
    let doc = run(&db, spec);
    assert_eq!(doc["total"], Value::Null);
    assert_eq!(doc["route"], json!([]));
}

#[test]
fn a_real_little_road_network() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    // Marmara sketch, undirected weighted edges.
    for (a, b, km) in [
        ("istanbul", "izmit", 100.0),
        ("istanbul", "tekirdag", 130.0),
        ("izmit", "bursa", 130.0),
        ("istanbul", "bursa", 150.0), // via ferry-bridge, direct
        ("bursa", "eskisehir", 150.0),
        ("izmit", "eskisehir", 200.0),
        ("tekirdag", "edirne", 140.0),
    ] {
        edge(&db, a, b, km);
    }
    db.insert("trips", json!({"src": "edirne", "dst": "eskisehir"}))
        .unwrap();
    let mut spec = base_spec();
    spec["undirected"] = json!(true);
    let doc = run(&db, spec);
    // edirne→tekirdag(140)→istanbul(130)→bursa(150)→eskisehir(150) = 570
    // vs …→istanbul→izmit(100)→eskisehir(200) = 570 — tie; either is 570.
    assert_eq!(doc["total"], json!(570.0));
    let hops = route_names(&doc);
    assert_eq!(hops.len(), 4);

    // Restricting away the direct istanbul–bursa edge forces the izmit leg.
    let mut spec = base_spec();
    spec["undirected"] = json!(true);
    spec["restrictSearchWithMatch"] = json!({"km": {"$ne": 150.0}});
    let doc = run(&db, spec);
    // Without both 150 km edges, eskisehir is reached via izmit: 140+130+200=470?
    // No — izmit→eskisehir is 200, istanbul→izmit 100: 140+130+100+200 = 570;
    // but bursa legs (150) are gone, so 570 via izmit is the only route.
    assert_eq!(doc["total"], json!(570.0));
    assert!(
        route_names(&doc)
            .iter()
            .any(|(a, b)| a == "izmit" || b == "izmit")
    );
}

#[test]
fn refusals_are_loud() {
    let dir = tempdir().unwrap();
    let db = OxiDb::open(dir.path()).unwrap();
    edge(&db, "A", "B", -1.0);
    db.insert("trips", json!({"src": "A", "dst": "B"})).unwrap();
    // Negative weight: Dijkstra's precondition, refused by name.
    let err = db
        .aggregate("trips", &json!([{ "$shortestPath": base_spec() }]))
        .unwrap_err();
    assert!(err.to_string().contains("negative"), "{err}");
    // Missing required keys.
    assert!(
        db.aggregate("trips", &json!([{ "$shortestPath": {"from": "roads"} }]))
            .is_err()
    );
    // Edges without the named weight field.
    db.insert("roads2", json!({"a": "A", "b": "B"})).unwrap();
    let mut spec = base_spec();
    spec["from"] = json!("roads2");
    let err = db
        .aggregate("trips", &json!([{ "$shortestPath": spec }]))
        .unwrap_err();
    assert!(err.to_string().contains("weight"), "{err}");
}
