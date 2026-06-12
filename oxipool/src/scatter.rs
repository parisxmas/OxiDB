use std::sync::Arc;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::Pool;

// ─── Wire helpers (same as main.rs) ─────────────────────────────────

const MAX_FRAME: usize = 16 * 1024 * 1024;

async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes", len),
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), std::io::Error> {
    let len_buf = (payload.len() as u32).to_le_bytes();
    stream.write_all(&len_buf).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

// ─── Forward to one shard and read response ─────────────────────────

async fn forward_and_read(pool: &Arc<Pool>, payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut backend = pool.get().await.map_err(|e| format!("pool get: {}", e))?;

    let exchange = async {
        write_frame(&mut backend, payload)
            .await
            .map_err(|e| format!("write: {}", e))?;
        read_frame(&mut backend)
            .await
            .map_err(|e| format!("read: {}", e))
    };
    let result = match crate::request_timeout() {
        Some(d) => match tokio::time::timeout(d, exchange).await {
            Ok(r) => r,
            Err(_) => Err(format!("request timed out after {:?}", d)),
        },
        None => exchange.await,
    };

    match result {
        Ok(response) => {
            pool.put(backend).await;
            Ok(response)
        }
        Err(e) => {
            // The connection's framing state is unknown after ANY failure
            // (partial write, timeout mid-response) — never return it to the
            // pool. This must run for write errors too: `Pool::get` forgets
            // a semaphore permit, so a dropped-without-replace connection
            // permanently shrank the pool until it drained to zero and every
            // request to this shard blocked forever.
            Pool::spawn_replace(Arc::clone(pool));
            Err(e)
        }
    }
}

/// Forward a request to a specific shard pool. Returns the raw response bytes.
pub async fn forward_to_shard(pool: &Arc<Pool>, payload: &[u8]) -> Result<Vec<u8>, String> {
    forward_and_read(pool, payload).await
}

// ─── Concurrent fan-out (parse once) ────────────────────────────────

/// Fan a request out to every pool concurrently. Returns one entry per
/// shard, in shard order: the PARSED response, or an error string. Parsing
/// once here means the merge strategies move values out instead of
/// re-parsing (and deep-cloning) every response a second time.
async fn gather_all(pools: &[Arc<Pool>], payload: &[u8]) -> Vec<Result<Value, String>> {
    let shared: Arc<[u8]> = Arc::from(payload);
    let handles: Vec<_> = pools
        .iter()
        .map(|pool| {
            let pool = Arc::clone(pool);
            let payload = Arc::clone(&shared);
            tokio::spawn(async move {
                let bytes = forward_and_read(&pool, &payload).await?;
                serde_json::from_slice::<Value>(&bytes)
                    .map_err(|e| format!("malformed shard response: {}", e))
            })
        })
        .collect();

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(match handle.await {
            Ok(r) => r,
            Err(e) => Err(format!("task error: {}", e)),
        });
    }
    results
}

/// First shard-side failure (transport error, malformed response, or
/// `ok:false`), if any. The aggregating merges fail fast on it — silently
/// dropping a shard's contribution would produce a truthy-but-incomplete
/// result, which is worse than a clear error.
fn first_shard_error(results: &[Result<Value, String>]) -> Option<String> {
    for r in results {
        match r {
            Err(e) => return Some(e.clone()),
            Ok(v) => {
                if v.get("ok").and_then(|b| b.as_bool()) != Some(true) {
                    let msg = v
                        .get("error")
                        .and_then(|e| e.as_str())
                        .unwrap_or("shard returned ok:false without an error message");
                    return Some(msg.to_string());
                }
            }
        }
    }
    None
}

/// Move each successful response's `data` array out (no clones).
fn take_doc_arrays(results: Vec<Result<Value, String>>) -> Vec<Value> {
    let mut all_docs = Vec::new();
    for r in results.into_iter().flatten() {
        let mut r = r;
        if let Some(Value::Array(arr)) = r.get_mut("data").map(Value::take) {
            all_docs.extend(arr);
        }
    }
    all_docs
}

fn error_response(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"ok": false, "error": msg})).unwrap()
}

// ─── Merge Strategy ─────────────────────────────────────────────────

pub enum MergeStrategy {
    /// Concatenate document arrays from all shards.
    ConcatDocs,
    /// Sum count values from all shards.
    SumCounts,
    /// Sum modified/deleted counts from all shards.
    SumModified,
    /// First shard with a matching document (find_one — reads only; the
    /// `_one` WRITES must never use this, see `scatter_one_write`).
    FirstMatch,
    /// Collect all responses for broadcast (DDL) — return last ok.
    BroadcastAll,
}

impl MergeStrategy {
    pub fn for_command(cmd: &str) -> Self {
        match cmd {
            "find" | "aggregate" | "search" => MergeStrategy::ConcatDocs,
            "count" => MergeStrategy::SumCounts,
            "update" | "delete" => MergeStrategy::SumModified,
            "find_one" => MergeStrategy::FirstMatch,
            // DDL / broadcast
            _ => MergeStrategy::BroadcastAll,
        }
    }
}

// ─── Scatter-Gather ─────────────────────────────────────────────────

/// Fan out a request to all shard pools, collect responses, and merge.
pub async fn scatter_gather(
    pools: &[Arc<Pool>],
    payload: &[u8],
    strategy: MergeStrategy,
) -> Vec<u8> {
    let results = gather_all(pools, payload).await;
    merge_responses(results, strategy)
}

/// Broadcast a request to all shard pools. Returns the last successful response.
pub async fn broadcast(pools: &[Arc<Pool>], payload: &[u8]) -> Vec<u8> {
    scatter_gather(pools, payload, MergeStrategy::BroadcastAll).await
}

// ─── update_one / delete_one ────────────────────────────────────────

/// `update_one` / `delete_one` without a shard key: probe the shards
/// SERIALLY and stop at the first one that actually modified or deleted a
/// document, so at most one document changes cluster-wide. The old
/// concurrent fan-out sent the write to EVERY shard — each shard applied it
/// to one local document (up to N modifications for a `_one` command) and
/// the merge merely picked which response to show the client.
pub async fn scatter_one_write(pools: &[Arc<Pool>], payload: &[u8]) -> Vec<u8> {
    let mut first_error: Option<String> = None;
    let mut last_ok: Option<Vec<u8>> = None;

    for pool in pools {
        match forward_and_read(pool, payload).await {
            Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(v) => {
                    if v.get("ok").and_then(|b| b.as_bool()) == Some(true) {
                        let n = v
                            .get("data")
                            .map(|d| {
                                d.get("modified").and_then(|x| x.as_u64()).unwrap_or(0)
                                    + d.get("deleted").and_then(|x| x.as_u64()).unwrap_or(0)
                            })
                            .unwrap_or(0);
                        if n > 0 {
                            return bytes;
                        }
                        last_ok = Some(bytes);
                    } else if first_error.is_none() {
                        first_error = Some(
                            v.get("error")
                                .and_then(|e| e.as_str())
                                .unwrap_or("shard returned ok:false")
                                .to_string(),
                        );
                    }
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(format!("malformed shard response: {}", e));
                    }
                }
            },
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    // Nothing modified. If a shard errored, IT might have held the match —
    // surface the error instead of a clean "0 modified".
    if let Some(e) = first_error {
        return error_response(&format!("single-document write failed on a shard: {}", e));
    }
    last_ok.unwrap_or_else(|| error_response("no shards configured"))
}

// ─── find with sort/skip/limit ──────────────────────────────────────

/// Scatter a `find`, honoring `sort` / `skip` / `limit` globally.
///
/// Per-shard rewrite: `skip` is removed (a per-shard skip silently DROPS up
/// to `skip × (shards − 1)` documents that belong in the result) and `limit`
/// becomes `skip + limit` (the global window is a subset of the union of
/// per-shard windows). The merge re-sorts globally — through a shard's
/// executor via `aggregate_docs`, so the comparator (including date-string
/// ordering) is byte-identical to a single node's — then applies the global
/// skip/limit. Without sort/skip/limit this is plain concatenation.
pub async fn scatter_find(pools: &[Arc<Pool>], payload: &[u8]) -> Vec<u8> {
    let req: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return scatter_gather(pools, payload, MergeStrategy::ConcatDocs).await,
    };

    let sort = req
        .get("sort")
        .filter(|s| s.as_object().is_some_and(|o| !o.is_empty()))
        .cloned();
    let skip = req.get("skip").and_then(|v| v.as_u64()).unwrap_or(0);
    let limit = req.get("limit").and_then(|v| v.as_u64());

    if sort.is_none() && skip == 0 && limit.is_none() {
        return scatter_gather(pools, payload, MergeStrategy::ConcatDocs).await;
    }

    let mut shard_req = req;
    if let Some(obj) = shard_req.as_object_mut() {
        obj.remove("skip");
        match limit {
            Some(n) => {
                obj.insert("limit".to_string(), json!(skip.saturating_add(n)));
            }
            None => {
                obj.remove("limit");
            }
        }
    }
    let shard_payload = match serde_json::to_vec(&shard_req) {
        Ok(b) => b,
        Err(e) => return error_response(&format!("failed to encode shard find: {}", e)),
    };

    let results = gather_all(pools, &shard_payload).await;
    if let Some(err) = first_shard_error(&results) {
        return error_response(&format!(
            "scatter-gather find failed on one or more shards: {}",
            err
        ));
    }
    let all_docs = take_doc_arrays(results);

    match sort {
        Some(sort_spec) => {
            let mut pipeline = vec![json!({ "$sort": sort_spec })];
            if skip > 0 {
                pipeline.push(json!({ "$skip": skip }));
            }
            if let Some(n) = limit {
                pipeline.push(json!({ "$limit": n }));
            }
            run_merge_docs(pools, all_docs, pipeline).await
        }
        None => {
            // No ordering requested — apply the global window locally.
            let docs: Vec<Value> = all_docs
                .into_iter()
                .skip(skip as usize)
                .take(limit.map(|n| n as usize).unwrap_or(usize::MAX))
                .collect();
            serde_json::to_vec(&json!({"ok": true, "data": docs})).unwrap()
        }
    }
}

// ─── text_search / vector_search ────────────────────────────────────

/// Ranked searches: each shard returns its local top-N with a score field
/// injected by the engine (`_score` for text, `_similarity` for vector).
/// Merge by score descending and re-apply the global limit — naive concat
/// returned shard0's block before shard1's regardless of rank, and up to
/// N × shards results.
pub async fn scatter_search(pools: &[Arc<Pool>], payload: &[u8], cmd: &str) -> Vec<u8> {
    let req: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return scatter_gather(pools, payload, MergeStrategy::ConcatDocs).await,
    };
    // Default limit mirrors the server's (10).
    let limit = req.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
    let score_key = if cmd == "vector_search" {
        "_similarity"
    } else {
        "_score"
    };

    let results = gather_all(pools, payload).await;
    if let Some(err) = first_shard_error(&results) {
        return error_response(&format!(
            "scatter-gather {} failed on one or more shards: {}",
            cmd, err
        ));
    }
    let mut all_docs = take_doc_arrays(results);
    all_docs.sort_by(|a, b| {
        let sa = a.get(score_key).and_then(|v| v.as_f64()).unwrap_or(0.0);
        let sb = b.get(score_key).and_then(|v| v.as_f64()).unwrap_or(0.0);
        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
    });
    all_docs.truncate(limit);

    serde_json::to_vec(&json!({"ok": true, "data": all_docs})).unwrap()
}

// ─── cross-shard aggregation ────────────────────────────────────────

/// Scatter-gather an `aggregate` command with a correct cross-shard merge.
///
/// Splits the pipeline (via `oxidb_agg_merge`) into a shard-local half and a
/// merge half. The shard half runs on every shard and emits *partial* results;
/// those are concatenated and the merge half runs once (on a single shard's
/// executor, via the `aggregate_docs` command) to produce the final answer.
/// Per-document pipelines fall back to plain concatenation; pipelines that
/// can't be merged correctly (e.g. `$push`/`$percentile`/`$lookup`/`$facet`)
/// return a clear error instead of a silently-wrong result.
pub async fn scatter_aggregate(pools: &[Arc<Pool>], payload: &[u8]) -> Vec<u8> {
    let req: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        // Not JSON we can introspect — fall back to plain concat.
        Err(_) => return scatter_gather(pools, payload, MergeStrategy::ConcatDocs).await,
    };

    let pipeline = match req.get("pipeline").and_then(|v| v.as_array()) {
        Some(p) => p.as_slice(),
        // No pipeline array — let the shards reject it; just concat.
        None => return scatter_gather(pools, payload, MergeStrategy::ConcatDocs).await,
    };

    match oxidb_agg_merge::split_pipeline(pipeline) {
        oxidb_agg_merge::SplitPlan::Passthrough => {
            // Per-document pipeline: concatenation is exact.
            scatter_gather(pools, payload, MergeStrategy::ConcatDocs).await
        }
        oxidb_agg_merge::SplitPlan::Unsupported(reason) => error_response(&format!(
            "cross-shard aggregation not supported: {reason}. \
             Add a $match on the shard key to target a single shard, or run against a single node."
        )),
        oxidb_agg_merge::SplitPlan::Split {
            shard_pipeline,
            merge_pipeline,
        } => {
            // 1. Run the shard pipeline on every shard, gather the partials.
            let mut shard_req = req.clone();
            shard_req["pipeline"] = Value::Array(shard_pipeline);
            let shard_payload = match serde_json::to_vec(&shard_req) {
                Ok(b) => b,
                Err(e) => return error_response(&format!("failed to encode shard pipeline: {e}")),
            };
            let results = gather_all(pools, &shard_payload).await;
            if let Some(err) = first_shard_error(&results) {
                return error_response(&format!(
                    "cross-shard aggregation failed on one or more shards: {}",
                    err
                ));
            }
            let partials = take_doc_arrays(results);

            // 2. Run the merge pipeline once over the partials, on one shard's
            //    executor (aggregate_docs touches no stored collection).
            run_merge_docs(pools, partials, merge_pipeline).await
        }
    }
}

/// Run a pipeline over `docs` on one shard's executor (`aggregate_docs`).
/// Used as the merge step for cross-shard aggregations and sorted finds.
async fn run_merge_docs(pools: &[Arc<Pool>], docs: Vec<Value>, pipeline: Vec<Value>) -> Vec<u8> {
    let merge_req = json!({
        "cmd": "aggregate_docs",
        "pipeline": Value::Array(pipeline),
        "docs": docs,
    });
    let merge_payload = match serde_json::to_vec(&merge_req) {
        Ok(b) => b,
        Err(e) => return error_response(&format!("failed to encode merge request: {e}")),
    };
    // Check the size up-front: an oversized frame would be rejected by EVERY
    // shard — the old code retried the identical payload on each one (losing
    // a pooled connection per attempt) and then reported a misleading "no
    // shard available".
    if merge_payload.len() > MAX_FRAME {
        return error_response(&format!(
            "cross-shard merge input is {} bytes, over the {} byte frame limit; \
             narrow the query (add a $match / $limit) or target a single shard",
            merge_payload.len(),
            MAX_FRAME
        ));
    }

    // Try shards in order so a single down node doesn't fail the merge.
    let mut last_err = String::from("no shards configured");
    for pool in pools {
        match forward_to_shard(pool, &merge_payload).await {
            Ok(resp) => return resp,
            Err(e) => last_err = e,
        }
    }
    error_response(&format!(
        "cross-shard merge failed on every shard: {last_err}"
    ))
}

// ─── insert_many splitting ──────────────────────────────────────────

/// Split an insert_many request by shard key, send each subset to its target shard.
pub async fn scatter_insert_many(
    pools: &[Arc<Pool>],
    payload: &[u8],
    router: &crate::shard::ShardRouter,
) -> Vec<u8> {
    let json: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => return error_response(&format!("invalid JSON: {}", e)),
    };

    let collection = json
        .get("collection")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let docs = match json.get("docs").and_then(|v| v.as_array()) {
        Some(d) => d,
        None => return error_response("missing docs array"),
    };
    let doc_count = docs.len();

    // Group docs by target shard, remembering each doc's ORIGINAL index so
    // the returned ids can be put back in input order — clients map
    // `ids[i]` to `docs[i]` positionally, exactly like the single-node
    // response, and shard-completion order scrambled that mapping.
    let num_shards = pools.len();
    let mut shard_docs: Vec<Vec<(usize, &Value)>> = vec![vec![]; num_shards];

    // Take the router lock ONCE for the whole batch (it was previously
    // acquired per document).
    {
        let config = router.config.read().await;
        for (i, doc) in docs.iter().enumerate() {
            let shard_id = config.route_value(collection, doc).unwrap_or(0) as usize;
            let target = if shard_id < num_shards { shard_id } else { 0 };
            shard_docs[target].push((i, doc));
        }
    }

    // Send to each shard that has docs.
    let mut handles = Vec::new();
    for (shard_id, entries) in shard_docs.into_iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        let pool = Arc::clone(&pools[shard_id]);
        let indices: Vec<usize> = entries.iter().map(|(i, _)| *i).collect();
        let mut req = json.clone();
        req["docs"] = Value::Array(entries.into_iter().map(|(_, d)| d.clone()).collect());
        let payload = serde_json::to_vec(&req).unwrap();

        handles.push(tokio::spawn(async move {
            (indices, forward_and_read(&pool, &payload).await)
        }));
    }

    // Collect results — ids placed by original index; ANY shard failure
    // fails the whole call (the old code returned ok:true whenever at least
    // one shard succeeded, hiding the failed half from the client).
    let mut ids_by_index: Vec<Value> = vec![Value::Null; doc_count];
    let mut inserted = 0usize;
    let mut first_error: Option<String> = None;

    for handle in handles {
        match handle.await {
            Ok((indices, Ok(data))) => match serde_json::from_slice::<Value>(&data) {
                Ok(resp) => {
                    if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                        if let Some(ids) = resp.get("data").and_then(|v| v.as_array()) {
                            for (idx, id) in indices.iter().zip(ids.iter()) {
                                ids_by_index[*idx] = id.clone();
                                inserted += 1;
                            }
                        }
                    } else if first_error.is_none() {
                        first_error = Some(
                            resp.get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("shard returned ok:false")
                                .to_string(),
                        );
                    }
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(format!("malformed shard response: {}", e));
                    }
                }
            },
            Ok((_, Err(e))) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e.to_string());
                }
            }
        }
    }

    if let Some(err) = first_error {
        return error_response(&format!(
            "insert_many failed on one or more shards ({} of {} documents inserted): {}",
            inserted, doc_count, err
        ));
    }
    serde_json::to_vec(&json!({"ok": true, "data": ids_by_index})).unwrap()
}

// ─── Response Merging ───────────────────────────────────────────────

fn merge_responses(results: Vec<Result<Value, String>>, strategy: MergeStrategy) -> Vec<u8> {
    match strategy {
        MergeStrategy::ConcatDocs => merge_doc_arrays(results),
        MergeStrategy::SumCounts => merge_counts(results),
        MergeStrategy::SumModified => merge_modified(results),
        MergeStrategy::FirstMatch => merge_first_match(results),
        MergeStrategy::BroadcastAll => merge_broadcast(results),
    }
}

/// Merge "find" responses: concatenate the "data" arrays. Fails fast if any
/// shard errored — silently dropping a shard's docs would produce a
/// truthy-but-incomplete result set, which is worse than a clear error.
fn merge_doc_arrays(results: Vec<Result<Value, String>>) -> Vec<u8> {
    if let Some(err) = first_shard_error(&results) {
        return error_response(&format!(
            "scatter-gather find failed on one or more shards: {}",
            err
        ));
    }
    let all_docs = take_doc_arrays(results);
    serde_json::to_vec(&json!({"ok": true, "data": all_docs})).unwrap()
}

/// Merge "count" responses: sum all counts. Fails fast if any shard errored
/// (otherwise a stale/down shard would silently undercount the total).
fn merge_counts(results: Vec<Result<Value, String>>) -> Vec<u8> {
    if let Some(err) = first_shard_error(&results) {
        return error_response(&format!(
            "scatter-gather count failed on one or more shards: {}",
            err
        ));
    }

    let mut total: u64 = 0;
    for resp in results.into_iter().flatten() {
        if let Some(n) = resp
            .get("data")
            .and_then(|d| d.get("count"))
            .and_then(|v| v.as_u64())
        {
            total += n;
        }
    }
    serde_json::to_vec(&json!({"ok": true, "data": {"count": total}})).unwrap()
}

/// Merge update/delete responses: sum modified/deleted counts. Fails fast
/// on any shard error — partial application would leave the client with no
/// way to detect that some shards didn't apply the update. `deleted` keeps
/// its own key: the server answers `delete` with `{"deleted": n}` and
/// folding it into `modified` was observably different from a single node.
fn merge_modified(results: Vec<Result<Value, String>>) -> Vec<u8> {
    if let Some(err) = first_shard_error(&results) {
        return error_response(&format!(
            "scatter-gather update/delete failed on one or more shards: {}",
            err
        ));
    }

    let mut total_modified: u64 = 0;
    let mut total_deleted: u64 = 0;
    let mut total_matched: u64 = 0;
    let mut saw_modified = false;
    let mut saw_deleted = false;
    for resp in results.into_iter().flatten() {
        let src = resp.get("data").unwrap_or(&resp);
        if let Some(n) = src.get("modified").and_then(|v| v.as_u64()) {
            total_modified += n;
            saw_modified = true;
        }
        if let Some(n) = src.get("deleted").and_then(|v| v.as_u64()) {
            total_deleted += n;
            saw_deleted = true;
        }
        if let Some(n) = src.get("matched").and_then(|v| v.as_u64()) {
            total_matched += n;
        }
    }

    let mut data = serde_json::Map::new();
    if saw_modified || !saw_deleted {
        data.insert("modified".to_string(), json!(total_modified));
    }
    if saw_deleted {
        data.insert("deleted".to_string(), json!(total_deleted));
    }
    if total_matched > 0 {
        data.insert("matched".to_string(), json!(total_matched));
    }
    serde_json::to_vec(&json!({"ok": true, "data": Value::Object(data)})).unwrap()
}

/// find_one: return the first shard whose response carries a non-null
/// document. This merge serves READS only — see `scatter_one_write` for the
/// `_one` writes — so it no longer sniffs `modified`/`deleted` keys out of
/// the document (a user doc containing a field literally named `modified`
/// used to be misread as a write response and dropped).
fn merge_first_match(results: Vec<Result<Value, String>>) -> Vec<u8> {
    let mut first_error: Option<String> = None;
    for r in &results {
        match r {
            Ok(v) => {
                if v.get("ok").and_then(|b| b.as_bool()) == Some(true) {
                    if let Some(data) = v.get("data") {
                        if !data.is_null() {
                            return serde_json::to_vec(v).unwrap();
                        }
                    }
                } else if first_error.is_none() {
                    first_error = Some(
                        v.get("error")
                            .and_then(|e| e.as_str())
                            .unwrap_or("shard returned ok:false")
                            .to_string(),
                    );
                }
            }
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e.clone());
                }
            }
        }
    }

    // No match anywhere. If a shard failed, the document might live exactly
    // there — surface the error instead of a clean "not found".
    if let Some(err) = first_error {
        return error_response(&format!(
            "find_one failed on one or more shards (the document may be on the failed shard): {}",
            err
        ));
    }
    serde_json::to_vec(&json!({"ok": true, "data": Value::Null})).unwrap()
}

/// For broadcast (DDL): return last ok, or first error.
fn merge_broadcast(results: Vec<Result<Value, String>>) -> Vec<u8> {
    let mut last_ok: Option<Value> = None;

    for r in results {
        match r {
            Ok(resp) => {
                if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                    last_ok = Some(resp);
                } else {
                    // Return first error immediately.
                    return serde_json::to_vec(&resp).unwrap();
                }
            }
            Err(e) => return error_response(&format!("shard error: {}", e)),
        }
    }

    match last_ok {
        Some(v) => serde_json::to_vec(&v).unwrap(),
        None => serde_json::to_vec(&json!({"ok": true})).unwrap(),
    }
}
