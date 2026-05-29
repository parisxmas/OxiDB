use std::sync::Arc;

use serde_json::{json, Value};
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

async fn forward_and_read(
    pool: &Arc<Pool>,
    payload: &[u8],
) -> Result<(Vec<u8>, Arc<Pool>), String> {
    let mut backend = pool
        .get()
        .await
        .map_err(|e| format!("pool get: {}", e))?;

    write_frame(&mut backend, payload)
        .await
        .map_err(|e| format!("write: {}", e))?;

    let response = read_frame(&mut backend)
        .await
        .map_err(|e| {
            Pool::spawn_replace(Arc::clone(pool));
            format!("read: {}", e)
        })?;

    pool.put(backend).await;
    Ok((response, Arc::clone(pool)))
}

// ─── Merge Strategy ─────────────────────────────────────────────────

pub enum MergeStrategy {
    /// Concatenate document arrays from all shards (find, aggregate).
    ConcatDocs,
    /// Sum count values from all shards.
    SumCounts,
    /// Sum modified/deleted counts from all shards.
    SumModified,
    /// Take first successful response (find_one, delete_one, update_one).
    FirstMatch,
    /// Collect all responses for broadcast (DDL) — return last ok.
    BroadcastAll,
}

impl MergeStrategy {
    pub fn for_command(cmd: &str) -> Self {
        match cmd {
            "find" | "aggregate" | "text_search" | "vector_search" | "search" => {
                MergeStrategy::ConcatDocs
            }
            "count" => MergeStrategy::SumCounts,
            "update" | "delete" => MergeStrategy::SumModified,
            "find_one" | "update_one" | "delete_one" => MergeStrategy::FirstMatch,
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
    let handles: Vec<_> = pools
        .iter()
        .map(|pool| {
            let pool = Arc::clone(pool);
            let payload = payload.to_vec();
            tokio::spawn(async move { forward_and_read(&pool, &payload).await })
        })
        .collect();

    let mut responses = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(Ok((data, _))) => responses.push(data),
            Ok(Err(e)) => {
                let err = json!({"ok": false, "error": format!("shard error: {}", e)});
                responses.push(serde_json::to_vec(&err).unwrap());
            }
            Err(e) => {
                let err = json!({"ok": false, "error": format!("task error: {}", e)});
                responses.push(serde_json::to_vec(&err).unwrap());
            }
        }
    }

    merge_responses(responses, strategy)
}

/// Broadcast a request to all shard pools. Returns the last successful response.
pub async fn broadcast(pools: &[Arc<Pool>], payload: &[u8]) -> Vec<u8> {
    scatter_gather(pools, payload, MergeStrategy::BroadcastAll).await
}

/// Forward a request to a specific shard pool. Returns the raw response bytes.
#[allow(dead_code)]
pub async fn forward_to_shard(pool: &Arc<Pool>, payload: &[u8]) -> Result<Vec<u8>, String> {
    let (response, _) = forward_and_read(pool, payload).await?;
    Ok(response)
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
            // 1. Run the shard pipeline on every shard, gather concatenated partials.
            let mut shard_req = req.clone();
            shard_req["pipeline"] = Value::Array(shard_pipeline);
            let shard_payload = match serde_json::to_vec(&shard_req) {
                Ok(b) => b,
                Err(e) => return error_response(&format!("failed to encode shard pipeline: {e}")),
            };
            let concat = scatter_gather(pools, &shard_payload, MergeStrategy::ConcatDocs).await;

            // Propagate a shard-side error verbatim (ConcatDocs fails fast).
            let concat_val: Value = match serde_json::from_slice(&concat) {
                Ok(v) => v,
                Err(e) => return error_response(&format!("malformed shard response: {e}")),
            };
            if concat_val.get("ok").and_then(|v| v.as_bool()) != Some(true) {
                return concat;
            }
            let partials = concat_val
                .get("data")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            // 2. Run the merge pipeline once over the partials, on one shard's
            //    executor (aggregate_docs touches no stored collection).
            let merge_req = json!({
                "cmd": "aggregate_docs",
                "pipeline": Value::Array(merge_pipeline),
                "docs": partials,
            });
            let merge_payload = match serde_json::to_vec(&merge_req) {
                Ok(b) => b,
                Err(e) => return error_response(&format!("failed to encode merge pipeline: {e}")),
            };

            // Try shards in order so a single down node doesn't fail the merge.
            for pool in pools {
                match forward_to_shard(pool, &merge_payload).await {
                    Ok(resp) => return resp,
                    Err(_) => continue,
                }
            }
            error_response("cross-shard aggregation merge failed: no shard available")
        }
    }
}

fn error_response(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"ok": false, "error": msg})).unwrap()
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
        Err(e) => {
            return serde_json::to_vec(&json!({"ok": false, "error": format!("invalid JSON: {}", e)}))
                .unwrap();
        }
    };

    let collection = json
        .get("collection")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let docs = match json.get("docs").and_then(|v| v.as_array()) {
        Some(d) => d,
        None => {
            return serde_json::to_vec(&json!({"ok": false, "error": "missing docs array"}))
                .unwrap();
        }
    };

    // Group docs by target shard
    let num_shards = pools.len();
    let mut shard_docs: Vec<Vec<&Value>> = vec![vec![]; num_shards];

    for doc in docs {
        let shard_id = router.route_insert(collection, doc).await as usize;
        if shard_id < num_shards {
            shard_docs[shard_id].push(doc);
        } else {
            shard_docs[0].push(doc); // fallback to shard 0
        }
    }

    // Send to each shard that has docs
    let mut handles = Vec::new();
    for (shard_id, docs) in shard_docs.into_iter().enumerate() {
        if docs.is_empty() {
            continue;
        }
        let pool = Arc::clone(&pools[shard_id]);
        let mut req = json.clone();
        req["docs"] = Value::Array(docs.into_iter().cloned().collect());
        let payload = serde_json::to_vec(&req).unwrap();

        handles.push(tokio::spawn(async move {
            forward_and_read(&pool, &payload).await
        }));
    }

    // Collect results — sum inserted counts
    let mut total_inserted = 0u64;
    let mut all_ids: Vec<Value> = Vec::new();
    let mut last_error: Option<String> = None;

    for handle in handles {
        match handle.await {
            Ok(Ok((data, _))) => {
                if let Ok(resp) = serde_json::from_slice::<Value>(&data) {
                    if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                        // OxiDB returns {"ok": true, "data": [ids]} for insert_many
                        if let Some(data) = resp.get("data") {
                            if let Some(ids) = data.as_array() {
                                total_inserted += ids.len() as u64;
                                all_ids.extend(ids.iter().cloned());
                            }
                        }
                    } else if let Some(e) = resp.get("error").and_then(|v| v.as_str()) {
                        last_error = Some(e.to_string());
                    }
                }
            }
            Ok(Err(e)) => last_error = Some(e),
            Err(e) => last_error = Some(e.to_string()),
        }
    }

    if total_inserted > 0 {
        serde_json::to_vec(&json!({"ok": true, "data": all_ids})).unwrap()
    } else if let Some(err) = last_error {
        serde_json::to_vec(&json!({"ok": false, "error": err})).unwrap()
    } else {
        serde_json::to_vec(&json!({"ok": true, "data": []})).unwrap()
    }
}

// ─── Response Merging ───────────────────────────────────────────────

fn merge_responses(responses: Vec<Vec<u8>>, strategy: MergeStrategy) -> Vec<u8> {
    match strategy {
        MergeStrategy::ConcatDocs => merge_doc_arrays(responses),
        MergeStrategy::SumCounts => merge_counts(responses),
        MergeStrategy::SumModified => merge_modified(responses),
        MergeStrategy::FirstMatch => merge_first_match(responses),
        MergeStrategy::BroadcastAll => merge_broadcast(responses),
    }
}

/// If any shard returned `ok:false` (or a malformed response), return the
/// first such error message. Used by the merge strategies that aggregate
/// across shards (`SumCounts`, `ConcatDocs`, `SumModified`) so that a partial
/// failure surfaces as a real error to the client instead of being silently
/// excluded from the aggregate.
fn first_partial_error(responses: &[Vec<u8>]) -> Option<String> {
    for resp_bytes in responses {
        match serde_json::from_slice::<Value>(resp_bytes) {
            Ok(resp) => {
                if !resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                    let msg = resp
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("shard returned ok:false without an error message");
                    return Some(msg.to_string());
                }
            }
            Err(e) => return Some(format!("shard returned malformed response: {}", e)),
        }
    }
    None
}

/// Merge "find" responses: concatenate the "docs" arrays. Fails fast if any
/// shard errored — silently dropping a shard's docs would produce a
/// truthy-but-incomplete result set, which is worse than a clear error.
fn merge_doc_arrays(responses: Vec<Vec<u8>>) -> Vec<u8> {
    if let Some(err) = first_partial_error(&responses) {
        return serde_json::to_vec(&json!({
            "ok": false,
            "error": format!("scatter-gather find failed on one or more shards: {}", err),
        })).unwrap();
    }

    let mut all_docs: Vec<Value> = Vec::new();
    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if let Some(data) = resp.get("data").and_then(|v| v.as_array()) {
                all_docs.extend(data.iter().cloned());
            }
        }
    }

    serde_json::to_vec(&json!({"ok": true, "data": all_docs})).unwrap()
}

/// Merge "count" responses: sum all counts. Fails fast if any shard errored
/// (otherwise a stale/down shard would silently undercount the total).
fn merge_counts(responses: Vec<Vec<u8>>) -> Vec<u8> {
    if let Some(err) = first_partial_error(&responses) {
        return serde_json::to_vec(&json!({
            "ok": false,
            "error": format!("scatter-gather count failed on one or more shards: {}", err),
        })).unwrap();
    }

    let mut total: u64 = 0;
    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if let Some(data) = resp.get("data") {
                if let Some(n) = data.get("count").and_then(|v| v.as_u64()) {
                    total += n;
                }
            }
        }
    }

    serde_json::to_vec(&json!({"ok": true, "data": {"count": total}})).unwrap()
}

/// Merge update/delete responses: sum modified/deleted counts. Fails fast
/// on any shard error — partial application would leave the client with no
/// way to detect that some shards didn't apply the update.
fn merge_modified(responses: Vec<Vec<u8>>) -> Vec<u8> {
    if let Some(err) = first_partial_error(&responses) {
        return serde_json::to_vec(&json!({
            "ok": false,
            "error": format!("scatter-gather update/delete failed on one or more shards: {}", err),
        })).unwrap();
    }

    let mut total_modified: u64 = 0;
    let mut total_matched: u64 = 0;
    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            let src = resp.get("data").unwrap_or(&resp);
            if let Some(n) = src.get("modified").and_then(|v| v.as_u64()) {
                total_modified += n;
            }
            if let Some(n) = src.get("deleted").and_then(|v| v.as_u64()) {
                total_modified += n;
            }
            if let Some(n) = src.get("matched").and_then(|v| v.as_u64()) {
                total_matched += n;
            }
        }
    }

    let mut data = json!({"modified": total_modified});
    if total_matched > 0 {
        data["matched"] = json!(total_matched);
    }
    serde_json::to_vec(&json!({"ok": true, "data": data})).unwrap()
}

/// For find_one/update_one/delete_one: return the first successful match.
fn merge_first_match(responses: Vec<Vec<u8>>) -> Vec<u8> {
    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                // OxiDB returns {"ok": true, "data": <doc|result>}
                if let Some(data) = resp.get("data") {
                    if !data.is_null() {
                        // find_one: data is a document (object)
                        if data.is_object() {
                            // update_one/delete_one with 0 modifications — skip
                            if let Some(m) = data.get("modified").and_then(|v| v.as_u64()) {
                                if m > 0 { return resp_bytes.clone(); }
                            } else if let Some(d) = data.get("deleted").and_then(|v| v.as_u64()) {
                                if d > 0 { return resp_bytes.clone(); }
                            } else {
                                // Regular document (find_one)
                                return resp_bytes.clone();
                            }
                        }
                        // data is array or other non-null — return it
                        if data.is_array() {
                            return resp_bytes.clone();
                        }
                    }
                }
            }
        }
    }

    // No match found — return first response (or a not-found)
    responses
        .into_iter()
        .next()
        .unwrap_or_else(|| serde_json::to_vec(&json!({"ok": true, "doc": null})).unwrap())
}

/// For broadcast (DDL): return last ok, or first error.
fn merge_broadcast(responses: Vec<Vec<u8>>) -> Vec<u8> {
    let mut last_ok: Option<Vec<u8>> = None;

    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                last_ok = Some(resp_bytes.clone());
            } else {
                // Return first error immediately
                return resp_bytes.clone();
            }
        }
    }

    last_ok.unwrap_or_else(|| serde_json::to_vec(&json!({"ok": true})).unwrap())
}
