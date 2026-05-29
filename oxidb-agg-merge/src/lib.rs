//! Cross-shard aggregation pipeline splitting.
//!
//! When OxiPool fans an `aggregate` out to multiple shards, each shard can
//! only see its own slice of the data. Concatenating the shards' final results
//! is correct **only** for per-document pipelines (`$match`, `$project`,
//! `$addFields`, `$unwind`). Any *reducing* stage (`$group`, `$sort`,
//! `$limit`, `$skip`, `$count`) needs a two-phase split, exactly like
//! MongoDB's `splitPipelineForSharding`:
//!
//! * a **shard pipeline** that runs on every shard and emits *partial* results,
//!   and
//! * a **merge pipeline** that runs once over the concatenation of all shards'
//!   partials to produce the final answer.
//!
//! [`split_pipeline`] computes that split as plain JSON so OxiPool (which has
//! no dependency on the core engine) can rewrite what it sends to shards and
//! hand the merge pipeline to a single node's executor. Pipelines that cannot
//! be merged correctly (e.g. `$push`/`$addToSet`/`$percentile`/`$first`/
//! `$last` group accumulators, `$facet`, `$bucket`, `$lookup`) are reported as
//! [`SplitPlan::Unsupported`] with a human-readable reason — callers should
//! return that as an error rather than produce a silently-wrong result.
//!
//! ## Accumulator decomposition
//!
//! | Accumulator        | Shard emits                  | Merge step                       |
//! |--------------------|------------------------------|----------------------------------|
//! | `$sum: e`          | partial sum                  | `$sum` of partials               |
//! | `$sum: 1` / `$count`| partial count               | `$sum` of partials               |
//! | `$min` / `$max`    | partial min / max            | `$min` / `$max` of partials      |
//! | `$avg: e`          | `{sum, count}` partials      | `Σsum / Σcount` (finalize)       |
//!
//! `$sort` (+ optional trailing `$limit`) → each shard sorts (and top-k
//! limits); the merge re-sorts the union and applies the global limit/skip.
//! `$count` → each shard counts; the merge sums.

use serde_json::{Map, Value, json};

/// The outcome of splitting a pipeline for cross-shard execution.
#[derive(Debug, Clone, PartialEq)]
pub enum SplitPlan {
    /// No reducing stage — every stage is per-document, so concatenating the
    /// shards' outputs is exact. Run the original pipeline on each shard and
    /// concatenate.
    Passthrough,
    /// Run `shard_pipeline` on each shard, concatenate the results, then run
    /// `merge_pipeline` once over the concatenation.
    Split {
        shard_pipeline: Vec<Value>,
        merge_pipeline: Vec<Value>,
    },
    /// The pipeline cannot be merged correctly across shards. The string is a
    /// human-readable reason suitable for returning to the client.
    Unsupported(String),
}

/// Stages that require seeing all matching documents at once — the point at
/// which a single-node pipeline must be split into shard + merge halves.
fn is_reducing(stage_name: &str) -> bool {
    matches!(
        stage_name,
        "$group" | "$sort" | "$limit" | "$skip" | "$count"
    )
}

/// Per-document stages that are safe to run on a shard ahead of the split
/// boundary (they never combine documents across the data set).
fn is_streaming_safe(stage_name: &str) -> bool {
    matches!(stage_name, "$match" | "$project" | "$addFields" | "$unwind")
}

/// Extract the single `{"$stage": body}` pair from a stage object.
fn stage_parts(stage: &Value) -> Option<(&str, &Value)> {
    let obj = stage.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (k, v) = obj.iter().next()?;
    Some((k.as_str(), v))
}

/// Compute the shard/merge split for an aggregation `pipeline`.
///
/// `pipeline` is the array of stage objects (the value of the `pipeline`
/// field). See the module docs for semantics.
pub fn split_pipeline(pipeline: &[Value]) -> SplitPlan {
    // A $lookup or $out anywhere is unsafe across shards: $lookup joins against
    // a foreign collection that may itself be sharded (each shard would only
    // see local foreign docs); $out writes results and has no meaningful
    // per-shard partial form.
    for stage in pipeline {
        if let Some((name, _)) = stage_parts(stage) {
            match name {
                "$lookup" => {
                    return SplitPlan::Unsupported(
                        "$lookup is not supported in cross-shard aggregation (foreign collection may be sharded)".into(),
                    );
                }
                "$out" | "$merge" => {
                    return SplitPlan::Unsupported(
                        "$out/$merge is not supported in cross-shard aggregation".into(),
                    );
                }
                "$facet" | "$bucket" | "$bucketAuto" | "$sortByCount" | "$sample"
                | "$dateHistogram" => {
                    return SplitPlan::Unsupported(format!(
                        "{name} is not yet supported in cross-shard aggregation"
                    ));
                }
                _ => {}
            }
        } else {
            return SplitPlan::Unsupported("malformed pipeline stage".into());
        }
    }

    // Find the first reducing stage — the split boundary.
    let boundary = pipeline
        .iter()
        .position(|s| stage_parts(s).map(|(n, _)| is_reducing(n)).unwrap_or(false));

    let boundary = match boundary {
        // Purely per-document pipeline → concatenation is exact.
        None => return SplitPlan::Passthrough,
        Some(b) => b,
    };

    // Everything before the boundary is per-document and runs only on shards.
    // (It can't contain a reducing stage by construction; verify it's a stage
    // shape we recognize as safe.)
    for stage in &pipeline[..boundary] {
        if let Some((name, _)) = stage_parts(stage) {
            if !is_streaming_safe(name) {
                return SplitPlan::Unsupported(format!(
                    "stage {name} before the first reducing stage is not supported across shards"
                ));
            }
        }
    }

    let pre = &pipeline[..boundary];
    let blocker = &pipeline[boundary];
    let tail = &pipeline[boundary + 1..];

    let (name, body) = stage_parts(blocker).expect("validated above");

    // shard_half: stages appended after `pre` on each shard.
    // merge_half: stages that run on the concatenated partials, *before* `tail`.
    let (shard_half, merge_half): (Vec<Value>, Vec<Value>) = match name {
        "$group" => match split_group(body) {
            Ok(g) => (vec![g.shard_group], g.merge_stages),
            Err(reason) => return SplitPlan::Unsupported(reason),
        },
        "$sort" => {
            // Each shard sorts; if the very next stage is a $limit, push it down
            // as a per-shard top-k optimization (the global top-k is a subset of
            // the union of per-shard top-ks). The merge re-sorts and re-applies
            // the limit (it stays in `tail`).
            let mut shard = vec![blocker.clone()];
            if let Some((tn, tb)) = tail.first().and_then(stage_parts) {
                if tn == "$limit" {
                    shard.push(json!({ "$limit": tb.clone() }));
                }
            }
            (shard, vec![blocker.clone()])
        }
        "$limit" => {
            // Each shard returns at most N; the merge re-applies the global N.
            (vec![blocker.clone()], vec![blocker.clone()])
        }
        "$skip" => {
            // Skipping per shard is wrong; only the merge skips.
            (vec![], vec![blocker.clone()])
        }
        "$count" => {
            // Each shard counts its docs into `field`; the merge sums those
            // counts back into the same field.
            let field = match body.as_str() {
                Some(f) => f.to_string(),
                None => return SplitPlan::Unsupported("$count field must be a string".into()),
            };
            let merge = vec![
                json!({ "$group": { "_id": Value::Null, &field: { "$sum": format!("${field}") } } }),
                json!({ "$project": { "_id": 0 } }),
            ];
            (vec![blocker.clone()], merge)
        }
        other => {
            return SplitPlan::Unsupported(format!(
                "reducing stage {other} is not supported across shards"
            ));
        }
    };

    let mut shard_pipeline: Vec<Value> = Vec::with_capacity(pre.len() + shard_half.len());
    shard_pipeline.extend_from_slice(pre);
    shard_pipeline.extend(shard_half);

    let mut merge_pipeline: Vec<Value> = Vec::with_capacity(merge_half.len() + tail.len());
    merge_pipeline.extend(merge_half);
    merge_pipeline.extend_from_slice(tail);

    SplitPlan::Split {
        shard_pipeline,
        merge_pipeline,
    }
}

/// Result of splitting a single `$group` stage.
struct GroupSplit {
    /// The `$group` stage to run on each shard (emits partials).
    shard_group: Value,
    /// The merge-side stages: a combining `$group` plus any finalize stages
    /// (e.g. the `$divide` for `$avg` and a `$project` dropping temp fields).
    merge_stages: Vec<Value>,
}

/// Temp-field prefix for accumulator partials that must not collide with user
/// field names.
const TMP_PREFIX: &str = "__oxm_";

fn split_group(body: &Value) -> Result<GroupSplit, String> {
    let obj = body
        .as_object()
        .ok_or_else(|| "$group body must be an object".to_string())?;

    let id = obj
        .get("_id")
        .cloned()
        .ok_or_else(|| "$group requires an _id".to_string())?;

    let mut shard_accs = Map::new();
    let mut merge_accs = Map::new();
    let mut finalize_add = Map::new();
    let mut drop_fields: Vec<String> = Vec::new();

    for (out_name, spec) in obj {
        if out_name == "_id" {
            continue;
        }
        let (op, arg) = single_accumulator(spec)
            .ok_or_else(|| format!("$group accumulator for '{out_name}' is malformed"))?;
        let field_ref = Value::String(format!("${out_name}"));
        match op {
            "$sum" => {
                shard_accs.insert(out_name.clone(), json!({ "$sum": arg }));
                merge_accs.insert(out_name.clone(), json!({ "$sum": field_ref }));
            }
            "$count" => {
                // Count of shard docs; merge sums the partial counts.
                shard_accs.insert(out_name.clone(), json!({ "$count": arg }));
                merge_accs.insert(out_name.clone(), json!({ "$sum": field_ref }));
            }
            "$min" => {
                shard_accs.insert(out_name.clone(), json!({ "$min": arg }));
                merge_accs.insert(out_name.clone(), json!({ "$min": field_ref }));
            }
            "$max" => {
                shard_accs.insert(out_name.clone(), json!({ "$max": arg }));
                merge_accs.insert(out_name.clone(), json!({ "$max": field_ref }));
            }
            "$avg" => {
                // Ship partial {sum, count}; finalize as Σsum / Σcount.
                let sum_f = format!("{TMP_PREFIX}sum_{out_name}");
                let cnt_f = format!("{TMP_PREFIX}cnt_{out_name}");
                shard_accs.insert(sum_f.clone(), json!({ "$sum": arg }));
                shard_accs.insert(cnt_f.clone(), json!({ "$sum": 1 }));
                merge_accs.insert(sum_f.clone(), json!({ "$sum": format!("${sum_f}") }));
                merge_accs.insert(cnt_f.clone(), json!({ "$sum": format!("${cnt_f}") }));
                finalize_add.insert(
                    out_name.clone(),
                    json!({ "$divide": [format!("${sum_f}"), format!("${cnt_f}")] }),
                );
                drop_fields.push(sum_f);
                drop_fields.push(cnt_f);
            }
            "$push" | "$addToSet" => {
                return Err(format!(
                    "$group accumulator {op} is not supported across shards (no array-merge expression available)"
                ));
            }
            "$first" | "$last" => {
                return Err(format!(
                    "$group accumulator {op} is not supported across shards (order is undefined across shards)"
                ));
            }
            "$percentile" | "$median" | "$stdDevPop" | "$stdDevSamp" => {
                return Err(format!(
                    "$group accumulator {op} is not supported across shards"
                ));
            }
            other => {
                return Err(format!(
                    "$group accumulator {other} is not supported across shards"
                ));
            }
        }
    }

    let shard_group = json!({ "$group": build_group_obj(id, &shard_accs) });

    // Merge re-groups by the already-computed _id.
    let mut merge_stages = vec![json!({
        "$group": build_group_obj(Value::String("$_id".to_string()), &merge_accs)
    })];
    if !finalize_add.is_empty() {
        merge_stages.push(json!({ "$addFields": Value::Object(finalize_add) }));
        // Drop the temp partial fields via projection exclusion (keeps _id and
        // every real output field).
        let mut proj = Map::new();
        for f in &drop_fields {
            proj.insert(f.clone(), json!(0));
        }
        merge_stages.push(json!({ "$project": Value::Object(proj) }));
    }

    Ok(GroupSplit {
        shard_group,
        merge_stages,
    })
}

/// Build a `$group` body object from an `_id` expression and an accumulator
/// map, preserving `_id` first.
fn build_group_obj(id: Value, accs: &Map<String, Value>) -> Value {
    let mut m = Map::new();
    m.insert("_id".to_string(), id);
    for (k, v) in accs {
        m.insert(k.clone(), v.clone());
    }
    Value::Object(m)
}

/// Extract `("$op", arg)` from a single-key accumulator spec like
/// `{"$sum": "$amt"}`.
fn single_accumulator(spec: &Value) -> Option<(&str, Value)> {
    let obj = spec.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (k, v) = obj.iter().next()?;
    Some((k.as_str(), v.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipe(v: Value) -> Vec<Value> {
        v.as_array().unwrap().clone()
    }

    #[test]
    fn passthrough_for_per_document_pipeline() {
        let p = pipe(json!([
            {"$match": {"status": "active"}},
            {"$project": {"name": 1}},
            {"$unwind": "$tags"}
        ]));
        assert_eq!(split_pipeline(&p), SplitPlan::Passthrough);
    }

    #[test]
    fn split_group_sum() {
        let p = pipe(json!([
            {"$group": {"_id": "$city", "total": {"$sum": "$amt"}}}
        ]));
        match split_pipeline(&p) {
            SplitPlan::Split {
                shard_pipeline,
                merge_pipeline,
            } => {
                assert_eq!(
                    shard_pipeline,
                    pipe(json!([{"$group": {"_id": "$city", "total": {"$sum": "$amt"}}}]))
                );
                assert_eq!(
                    merge_pipeline,
                    pipe(json!([{"$group": {"_id": "$_id", "total": {"$sum": "$total"}}}]))
                );
            }
            other => panic!("expected Split, got {other:?}"),
        }
    }

    #[test]
    fn split_group_count_shorthand() {
        // {$sum: 1} is the canonical "count" — shard counts, merge sums.
        let p = pipe(json!([
            {"$group": {"_id": "$city", "n": {"$sum": 1}}}
        ]));
        match split_pipeline(&p) {
            SplitPlan::Split {
                shard_pipeline,
                merge_pipeline,
            } => {
                assert_eq!(
                    shard_pipeline,
                    pipe(json!([{"$group": {"_id": "$city", "n": {"$sum": 1}}}]))
                );
                assert_eq!(
                    merge_pipeline,
                    pipe(json!([{"$group": {"_id": "$_id", "n": {"$sum": "$n"}}}]))
                );
            }
            other => panic!("expected Split, got {other:?}"),
        }
    }

    #[test]
    fn split_group_avg_ships_sum_and_count() {
        let p = pipe(json!([
            {"$group": {"_id": "$city", "avg_amt": {"$avg": "$amt"}}}
        ]));
        match split_pipeline(&p) {
            SplitPlan::Split {
                shard_pipeline,
                merge_pipeline,
            } => {
                // Shard emits sum + count temps.
                assert_eq!(
                    shard_pipeline,
                    pipe(json!([{"$group": {
                        "_id": "$city",
                        "__oxm_sum_avg_amt": {"$sum": "$amt"},
                        "__oxm_cnt_avg_amt": {"$sum": 1}
                    }}]))
                );
                // Merge sums the temps, divides, drops them.
                assert_eq!(
                    merge_pipeline,
                    pipe(json!([
                        {"$group": {
                            "_id": "$_id",
                            "__oxm_sum_avg_amt": {"$sum": "$__oxm_sum_avg_amt"},
                            "__oxm_cnt_avg_amt": {"$sum": "$__oxm_cnt_avg_amt"}
                        }},
                        {"$addFields": {"avg_amt": {"$divide": ["$__oxm_sum_avg_amt", "$__oxm_cnt_avg_amt"]}}},
                        {"$project": {"__oxm_sum_avg_amt": 0, "__oxm_cnt_avg_amt": 0}}
                    ]))
                );
            }
            other => panic!("expected Split, got {other:?}"),
        }
    }

    #[test]
    fn split_match_then_group_keeps_match_on_shard() {
        let p = pipe(json!([
            {"$match": {"region": "EU"}},
            {"$group": {"_id": "$city", "total": {"$sum": "$amt"}}},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ]));
        match split_pipeline(&p) {
            SplitPlan::Split {
                shard_pipeline,
                merge_pipeline,
            } => {
                assert_eq!(
                    shard_pipeline,
                    pipe(json!([
                        {"$match": {"region": "EU"}},
                        {"$group": {"_id": "$city", "total": {"$sum": "$amt"}}}
                    ]))
                );
                // tail ($sort, $limit) runs entirely at merge after re-grouping.
                assert_eq!(
                    merge_pipeline,
                    pipe(json!([
                        {"$group": {"_id": "$_id", "total": {"$sum": "$total"}}},
                        {"$sort": {"total": -1}},
                        {"$limit": 10}
                    ]))
                );
            }
            other => panic!("expected Split, got {other:?}"),
        }
    }

    #[test]
    fn split_sort_limit_pushes_topk_to_shard() {
        let p = pipe(json!([
            {"$sort": {"score": -1}},
            {"$limit": 5}
        ]));
        match split_pipeline(&p) {
            SplitPlan::Split {
                shard_pipeline,
                merge_pipeline,
            } => {
                // Shard does top-5; merge re-sorts and limits to 5.
                assert_eq!(
                    shard_pipeline,
                    pipe(json!([
                        {"$sort": {"score": -1}},
                        {"$limit": 5}
                    ]))
                );
                assert_eq!(
                    merge_pipeline,
                    pipe(json!([
                        {"$sort": {"score": -1}},
                        {"$limit": 5}
                    ]))
                );
            }
            other => panic!("expected Split, got {other:?}"),
        }
    }

    #[test]
    fn split_skip_only_at_merge() {
        let p = pipe(json!([{"$skip": 20}]));
        match split_pipeline(&p) {
            SplitPlan::Split {
                shard_pipeline,
                merge_pipeline,
            } => {
                assert!(shard_pipeline.is_empty());
                assert_eq!(merge_pipeline, pipe(json!([{"$skip": 20}])));
            }
            other => panic!("expected Split, got {other:?}"),
        }
    }

    #[test]
    fn split_count_sums_at_merge() {
        let p = pipe(json!([{"$count": "total"}]));
        match split_pipeline(&p) {
            SplitPlan::Split {
                shard_pipeline,
                merge_pipeline,
            } => {
                assert_eq!(shard_pipeline, pipe(json!([{"$count": "total"}])));
                assert_eq!(
                    merge_pipeline,
                    pipe(json!([
                        {"$group": {"_id": null, "total": {"$sum": "$total"}}},
                        {"$project": {"_id": 0}}
                    ]))
                );
            }
            other => panic!("expected Split, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_push() {
        let p = pipe(json!([{"$group": {"_id": "$c", "items": {"$push": "$x"}}}]));
        assert!(matches!(split_pipeline(&p), SplitPlan::Unsupported(_)));
    }

    #[test]
    fn unsupported_lookup() {
        let p = pipe(
            json!([{"$lookup": {"from": "other", "localField": "a", "foreignField": "b", "as": "j"}}]),
        );
        assert!(matches!(split_pipeline(&p), SplitPlan::Unsupported(_)));
    }

    #[test]
    fn unsupported_facet() {
        let p = pipe(json!([{"$facet": {"a": []}}]));
        assert!(matches!(split_pipeline(&p), SplitPlan::Unsupported(_)));
    }
}
