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
                        if let Some(n) = resp.get("inserted").and_then(|v| v.as_u64()) {
                            total_inserted += n;
                        }
                        if let Some(ids) = resp.get("ids").and_then(|v| v.as_array()) {
                            all_ids.extend(ids.iter().cloned());
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
        let mut resp = json!({"ok": true, "inserted": total_inserted});
        if !all_ids.is_empty() {
            resp["ids"] = Value::Array(all_ids);
        }
        serde_json::to_vec(&resp).unwrap()
    } else if let Some(err) = last_error {
        serde_json::to_vec(&json!({"ok": false, "error": err})).unwrap()
    } else {
        serde_json::to_vec(&json!({"ok": true, "inserted": 0})).unwrap()
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

/// Merge "find" responses: concatenate the "docs" arrays.
fn merge_doc_arrays(responses: Vec<Vec<u8>>) -> Vec<u8> {
    let mut all_docs: Vec<Value> = Vec::new();

    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                if let Some(docs) = resp.get("docs").and_then(|v| v.as_array()) {
                    all_docs.extend(docs.iter().cloned());
                }
                // Also handle "results" key (text_search, vector_search)
                if let Some(results) = resp.get("results").and_then(|v| v.as_array()) {
                    all_docs.extend(results.iter().cloned());
                }
            }
        }
    }

    serde_json::to_vec(&json!({"ok": true, "docs": all_docs})).unwrap()
}

/// Merge "count" responses: sum all counts.
fn merge_counts(responses: Vec<Vec<u8>>) -> Vec<u8> {
    let mut total: u64 = 0;

    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                if let Some(n) = resp.get("count").and_then(|v| v.as_u64()) {
                    total += n;
                }
            }
        }
    }

    serde_json::to_vec(&json!({"ok": true, "count": total})).unwrap()
}

/// Merge update/delete responses: sum modified/deleted counts.
fn merge_modified(responses: Vec<Vec<u8>>) -> Vec<u8> {
    let mut total_modified: u64 = 0;
    let mut total_matched: u64 = 0;

    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                if let Some(n) = resp.get("modified").and_then(|v| v.as_u64()) {
                    total_modified += n;
                }
                if let Some(n) = resp.get("deleted").and_then(|v| v.as_u64()) {
                    total_modified += n;
                }
                if let Some(n) = resp.get("matched").and_then(|v| v.as_u64()) {
                    total_matched += n;
                }
            }
        }
    }

    let mut resp = json!({"ok": true, "modified": total_modified});
    if total_matched > 0 {
        resp["matched"] = json!(total_matched);
    }
    serde_json::to_vec(&resp).unwrap()
}

/// For find_one/update_one/delete_one: return the first successful match.
fn merge_first_match(responses: Vec<Vec<u8>>) -> Vec<u8> {
    for resp_bytes in &responses {
        if let Ok(resp) = serde_json::from_slice::<Value>(resp_bytes) {
            if resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                // find_one: has "doc" field
                if resp.get("doc").is_some() && !resp["doc"].is_null() {
                    return resp_bytes.clone();
                }
                // update_one/delete_one: modified > 0
                if resp.get("modified").and_then(|v| v.as_u64()).unwrap_or(0) > 0 {
                    return resp_bytes.clone();
                }
                if resp.get("deleted").and_then(|v| v.as_u64()).unwrap_or(0) > 0 {
                    return resp_bytes.clone();
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
