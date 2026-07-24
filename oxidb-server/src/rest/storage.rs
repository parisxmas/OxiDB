//! Per-project **file storage** over the tenant-aware REST listener — the
//! Supabase Storage analog (OxiBase gap #2).
//!
//! The engine's blob store is **per-database** (each `OxiDb` owns its own
//! `_blobs/` under its data dir), so `?db=<ref>` targeting — resolved by the
//! caller, with per-project JWT verification — gives every OxiBase project an
//! isolated store with no new state. This module is pure routing + quota.
//!
//! Routes (after the `api`/`storage` prefix; object keys may contain `/`):
//!
//! | Method | Path                          | Action                          |
//! |--------|-------------------------------|---------------------------------|
//! | GET    | /api/storage                  | list buckets                    |
//! | POST   | /api/storage/{bucket}         | create bucket                   |
//! | DELETE | /api/storage/{bucket}         | delete bucket (must be empty)   |
//! | GET    | /api/storage/{bucket}         | list objects (?prefix=&limit=)  |
//! | PUT    | /api/storage/{bucket}/{key…}  | upload (raw body + Content-Type)|
//! | GET    | /api/storage/{bucket}/{key…}  | download (stored Content-Type)  |
//! | HEAD   | /api/storage/{bucket}/{key…}  | metadata only                   |
//! | DELETE | /api/storage/{bucket}/{key…}  | delete object                   |
//!
//! AuthZ: the listener's `rest_permitted` role gate applies before dispatch —
//! a Read-role key (the browser-safe anon key) can only GET/HEAD; writes need
//! ReadWrite/Admin (the service_role key, server-side).
//!
//! Quota: for OxiBase projects, uploads are capped by the project row's
//! `max_storage_bytes` (default `OXIDB_PROJECT_DEFAULT_MAX_STORAGE_BYTES`,
//! 100 MiB; 0 = unlimited). Usage is the byte sum over all buckets.

use std::collections::HashMap;

use serde_json::json;

use super::{HttpRequest, HttpResponse, RestState, json_response, url_decode};

pub(super) fn handle(
    req: &HttpRequest,
    state: &RestState,
    tail: &[&str], // segments after ["api", "storage"]
    db_name: Option<&str>,
) -> HttpResponse {
    let db = &state.db;
    let method = req.method.as_str();

    match tail {
        // ── Buckets ────────────────────────────────────────────────────
        [] => match method {
            "GET" => json_response(
                200,
                "OK",
                json!({"buckets": db.list_buckets(), "total_bytes": total_bytes(db)}),
            ),
            _ => method_not_allowed(),
        },
        [bucket] => {
            let bucket = url_decode(bucket);
            match method {
                "POST" => match db.create_bucket(&bucket) {
                    Ok(()) => json_response(201, "Created", json!({"bucket": bucket})),
                    Err(e) => storage_err(&e),
                },
                "DELETE" => {
                    // The engine's delete is recursive (S3-force semantics);
                    // this surface refuses unless the bucket is empty, so a
                    // single misdirected call can't wipe a project's files.
                    match db.list_objects(&bucket, None, Some(1)) {
                        Err(e) => return storage_err(&e),
                        Ok(objects) if !objects.is_empty() => {
                            return json_response(
                                409,
                                "Conflict",
                                json!({"error": "bucket is not empty (delete its objects first)"}),
                            );
                        }
                        Ok(_) => {}
                    }
                    match db.delete_bucket(&bucket) {
                        Ok(()) => json_response(200, "OK", json!({"deleted": bucket})),
                        Err(e) => storage_err(&e),
                    }
                }
                "GET" => {
                    let params = super::parse_query_string(&req.query);
                    let prefix = params.get("prefix").map(|v| url_decode(v));
                    let limit = params
                        .get("limit")
                        .and_then(|v| v.parse::<usize>().ok())
                        .map(|n| n.min(10_000));
                    match db.list_objects(&bucket, prefix.as_deref(), limit) {
                        Ok(objects) => json_response(200, "OK", json!({"objects": objects})),
                        Err(e) => storage_err(&e),
                    }
                }
                _ => method_not_allowed(),
            }
        }
        // ── Objects (key may span segments — rejoin with '/') ──────────
        [bucket, key_parts @ ..] => {
            let bucket = url_decode(bucket);
            let key = key_parts
                .iter()
                .map(|s| url_decode(s))
                .collect::<Vec<_>>()
                .join("/");
            match method {
                "PUT" | "POST" => {
                    // OxiBase project quota: usage + this upload must fit.
                    if let (Some(name), Some(mgr)) = (db_name, &state.db_manager)
                        && let Some(max) =
                            crate::tenant_auth::project_storage_limit(mgr, name)
                        && max > 0
                    {
                        let used = total_bytes(db);
                        if used.saturating_add(req.body.len() as u64) > max {
                            return json_response(
                                403,
                                "Forbidden",
                                json!({"error": format!(
                                    "storage quota exceeded: {used} of {max} bytes used, upload is {} bytes",
                                    req.body.len()
                                )}),
                            );
                        }
                    }
                    let content_type = req
                        .headers
                        .get("content-type")
                        .cloned()
                        .unwrap_or_else(|| "application/octet-stream".to_string());
                    // Buckets are implicit on first upload (like collections).
                    let _ = db.create_bucket(&bucket);
                    match db.put_object(&bucket, &key, &req.body, &content_type, HashMap::new()) {
                        Ok(meta) => json_response(201, "Created", meta),
                        Err(e) => storage_err(&e),
                    }
                }
                "GET" => match db.get_object(&bucket, &key) {
                    Ok((data, meta)) => {
                        let content_type = meta
                            .get("content_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("application/octet-stream")
                            .to_string();
                        let etag = meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
                        HttpResponse {
                            status: 200,
                            status_text: "OK",
                            content_type,
                            headers: vec![("ETag".to_string(), format!("\"{etag}\""))],
                            body: data,
                            content_length_override: None,
                        }
                    }
                    Err(e) => storage_err(&e),
                },
                "HEAD" => match db.head_object(&bucket, &key) {
                    Ok(meta) => json_response(200, "OK", meta),
                    Err(e) => storage_err(&e),
                },
                "DELETE" => match db.delete_object(&bucket, &key) {
                    Ok(()) => json_response(200, "OK", json!({"deleted": key})),
                    Err(e) => storage_err(&e),
                },
                _ => method_not_allowed(),
            }
        }
    }
}

/// Total stored bytes across every bucket of `db` — the project's usage.
pub(super) fn total_bytes(db: &oxidb::OxiDb) -> u64 {
    db.list_buckets()
        .iter()
        .filter_map(|b| db.list_objects(b, None, None).ok())
        .flatten()
        .filter_map(|o| o.get("size").and_then(|v| v.as_u64()))
        .sum()
}

fn method_not_allowed() -> HttpResponse {
    json_response(405, "Method Not Allowed", json!({"error": "method not allowed"}))
}

/// Map engine errors: missing bucket/object → 404, everything else → 400.
fn storage_err(e: &oxidb::Error) -> HttpResponse {
    let msg = e.to_string();
    let status = if msg.contains("not found") { 404 } else { 400 };
    json_response(
        status,
        if status == 404 { "Not Found" } else { "Bad Request" },
        json!({"error": msg}),
    )
}
