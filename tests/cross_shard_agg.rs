//! Cross-shard aggregation correctness.
//!
//! Proves that the [`oxidb_agg_merge::split_pipeline`] split, when executed as
//! "run the shard pipeline on each partition, concatenate, run the merge
//! pipeline once", produces the **same** result as running the whole pipeline
//! on the full data set on a single node — for every pipeline shape OxiPool
//! claims to support. This is the network-free heart of the cross-shard
//! aggregation feature (ADR-0008): the actual proxy just moves the partials
//! over TCP and calls the same executor (`aggregate_docs`) for the merge.

use oxidb::OxiDb;
use oxidb_agg_merge::{SplitPlan, split_pipeline};
use serde_json::{Value, json};

/// Round-robin `docs` into `n` partitions. Round-robin maximally scatters each
/// group's members across shards — the stress case for cross-shard merge.
fn partition(docs: &[Value], n: usize) -> Vec<Vec<Value>> {
    let mut shards = vec![Vec::new(); n];
    for (i, d) in docs.iter().enumerate() {
        shards[i % n].push(d.clone());
    }
    shards
}

/// Execute `pipeline` over `docs` split across `n_shards`, using the real
/// engine for both the per-shard pass and the merge pass — mirroring exactly
/// what OxiPool + the `aggregate_docs` server command do across the network.
fn run_cross_shard(db: &OxiDb, pipeline: &[Value], docs: &[Value], n_shards: usize) -> Vec<Value> {
    let plan = split_pipeline(pipeline);
    let pipeline_val = Value::Array(pipeline.to_vec());
    match plan {
        SplitPlan::Passthrough => {
            // Each shard runs the whole pipeline; concatenate.
            let mut out = Vec::new();
            for shard in partition(docs, n_shards) {
                out.extend(db.aggregate_docs(&pipeline_val, shard).unwrap());
            }
            out
        }
        SplitPlan::Split {
            shard_pipeline,
            merge_pipeline,
        } => {
            let shard_val = Value::Array(shard_pipeline);
            let merge_val = Value::Array(merge_pipeline);
            let mut partials = Vec::new();
            for shard in partition(docs, n_shards) {
                partials.extend(db.aggregate_docs(&shard_val, shard).unwrap());
            }
            db.aggregate_docs(&merge_val, partials).unwrap()
        }
        SplitPlan::Unsupported(reason) => {
            panic!("pipeline unexpectedly unsupported: {reason}");
        }
    }
}

/// Single-node baseline.
fn run_single(db: &OxiDb, pipeline: &[Value], docs: &[Value]) -> Vec<Value> {
    db.aggregate_docs(&Value::Array(pipeline.to_vec()), docs.to_vec())
        .unwrap()
}

/// Recursively canonicalize a JSON value: sort object keys and round floats so
/// that group results (whose order is undefined) and `$avg` (whose float
/// arithmetic may differ in summation order) compare equal.
fn canon(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for k in keys {
                out.insert(k.clone(), canon(&m[k]));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(canon).collect()),
        Value::Number(n) => {
            if let Some(f) = n.as_f64()
                && n.as_i64().is_none()
                && n.as_u64().is_none()
            {
                // Round floats to 6 decimals for stable comparison.
                let r = (f * 1_000_000.0).round() / 1_000_000.0;
                return json!(r);
            }
            v.clone()
        }
        _ => v.clone(),
    }
}

/// Assert two result sets are equal as unordered multisets (for $group, whose
/// output order is undefined).
fn assert_same_unordered(a: &[Value], b: &[Value], label: &str) {
    let mut ca: Vec<String> = a
        .iter()
        .map(|v| serde_json::to_string(&canon(v)).unwrap())
        .collect();
    let mut cb: Vec<String> = b
        .iter()
        .map(|v| serde_json::to_string(&canon(v)).unwrap())
        .collect();
    ca.sort();
    cb.sort();
    assert_eq!(ca, cb, "{label}: cross-shard merge != single-node baseline");
}

/// Assert two result sets are equal in order (for $sort / $limit).
fn assert_same_ordered(a: &[Value], b: &[Value], label: &str) {
    let ca: Vec<Value> = a.iter().map(canon).collect();
    let cb: Vec<Value> = b.iter().map(canon).collect();
    assert_eq!(ca, cb, "{label}: cross-shard merge != single-node baseline");
}

/// Sample dataset: sales across cities/regions with amounts.
fn dataset() -> Vec<Value> {
    let cities = [
        ("Tokyo", "APAC"),
        ("Paris", "EU"),
        ("Berlin", "EU"),
        ("Osaka", "APAC"),
        ("Madrid", "EU"),
    ];
    let mut docs = Vec::new();
    for i in 0..60u64 {
        let (city, region) = cities[(i % cities.len() as u64) as usize];
        docs.push(json!({
            "_id": i,
            "city": city,
            "region": region,
            "amt": (i * 7 % 50) as i64 + 1,
            "qty": (i % 4) as i64,
        }));
    }
    docs
}

fn db() -> OxiDb {
    OxiDb::open_in_memory().unwrap()
}

#[test]
fn group_sum_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![json!({"$group": {"_id": "$city", "total": {"$sum": "$amt"}}})];
    for n in [1usize, 2, 3, 5] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_unordered(&merged, &base, &format!("group_sum n={n}"));
    }
}

#[test]
fn group_count_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![json!({"$group": {"_id": "$region", "n": {"$sum": 1}}})];
    for n in [1usize, 3, 4] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_unordered(&merged, &base, &format!("group_count n={n}"));
    }
}

#[test]
fn group_min_max_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![json!({"$group": {
        "_id": "$city",
        "lo": {"$min": "$amt"},
        "hi": {"$max": "$amt"}
    }})];
    for n in [1usize, 2, 5] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_unordered(&merged, &base, &format!("group_min_max n={n}"));
    }
}

#[test]
fn group_avg_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![json!({"$group": {"_id": "$city", "avg_amt": {"$avg": "$amt"}}})];
    for n in [1usize, 2, 3, 5] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_unordered(&merged, &base, &format!("group_avg n={n}"));
    }
}

#[test]
fn group_mixed_accumulators_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![json!({"$group": {
        "_id": "$region",
        "total": {"$sum": "$amt"},
        "n": {"$sum": 1},
        "lo": {"$min": "$amt"},
        "avg_amt": {"$avg": "$amt"}
    }})];
    for n in [1usize, 2, 3, 4, 5] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_unordered(&merged, &base, &format!("group_mixed n={n}"));
    }
}

#[test]
fn match_then_group_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![
        json!({"$match": {"region": "EU"}}),
        json!({"$group": {"_id": "$city", "total": {"$sum": "$amt"}}}),
    ];
    for n in [1usize, 2, 3] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_unordered(&merged, &base, &format!("match_group n={n}"));
    }
}

#[test]
fn group_sort_limit_matches_baseline() {
    let db = db();
    let docs = dataset();
    // Tiebreaker on _id keeps the top-k deterministic.
    let p = vec![
        json!({"$group": {"_id": "$city", "total": {"$sum": "$amt"}}}),
        json!({"$sort": {"total": -1, "_id": 1}}),
        json!({"$limit": 3}),
    ];
    for n in [1usize, 2, 3, 5] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_ordered(&merged, &base, &format!("group_sort_limit n={n}"));
    }
}

#[test]
fn sort_limit_topk_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![
        json!({"$sort": {"amt": -1, "_id": 1}}),
        json!({"$limit": 7}),
    ];
    for n in [1usize, 2, 3, 4] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_ordered(&merged, &base, &format!("sort_limit n={n}"));
    }
}

#[test]
fn count_stage_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![
        json!({"$match": {"region": "EU"}}),
        json!({"$count": "total"}),
    ];
    for n in [1usize, 2, 3, 5] {
        let merged = run_cross_shard(&db, &p, &docs, n);
        let base = run_single(&db, &p, &docs);
        assert_same_unordered(&merged, &base, &format!("count n={n}"));
    }
}

#[test]
fn passthrough_match_project_matches_baseline() {
    let db = db();
    let docs = dataset();
    let p = vec![
        json!({"$match": {"region": "APAC"}}),
        json!({"$project": {"city": 1, "amt": 1}}),
    ];
    // Passthrough: concatenation must equal the baseline as an unordered set.
    let merged = run_cross_shard(&db, &p, &docs, 3);
    let base = run_single(&db, &p, &docs);
    assert_same_unordered(&merged, &base, "passthrough");
}

#[test]
fn unsupported_pipelines_are_reported() {
    // These must be flagged (OxiPool returns a clear error) rather than merged.
    let push = vec![json!({"$group": {"_id": "$c", "items": {"$push": "$amt"}}})];
    assert!(matches!(split_pipeline(&push), SplitPlan::Unsupported(_)));

    let lookup =
        vec![json!({"$lookup": {"from": "x", "localField": "a", "foreignField": "b", "as": "j"}})];
    assert!(matches!(split_pipeline(&lookup), SplitPlan::Unsupported(_)));
}
