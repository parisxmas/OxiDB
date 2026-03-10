//! S3-compatible HTTP REST API for OxiDB blob storage.
//!
//! Enabled via `--features s3` and `OXIDB_S3_PORT=9000`.
//! Supports path-style requests: `PUT /bucket/key`, `GET /bucket/key`, etc.
//! Compatible with AWS CLI, boto3, and other S3 clients.
//!
//! Module structure:
//! - `auth`      — AWS Signature V4 (header + presigned URL)
//! - `http`      — HTTP/1.1 parser and response builder
//! - `bucket`    — Bucket CRUD handlers
//! - `object`    — Object CRUD, copy, range, conditional requests
//! - `multipart` — Multipart upload lifecycle
//! - `tagging`   — Object tagging (GET/PUT/DELETE ?tagging)
//! - `batch`     — Batch delete (POST ?delete)
//! - `helpers`   — XML, URL, CRC32 utilities

mod auth;
mod batch;
mod bucket;
mod helpers;
mod http;
mod multipart;
mod object;
mod tagging;

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use oxidb::OxiDb;

use auth::{S3Auth, verify_auth};
use helpers::{url_decode, parse_query};
use http::{HttpRequest, HttpResponse, error_response, parse_request};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// In-progress multipart upload.
struct MultipartUpload {
    bucket: String,
    key: String,
    content_type: String,
    metadata: HashMap<String, String>,
    parts: HashMap<u32, Vec<u8>>,
}

/// Shared state for all S3 connections.
struct S3State {
    db: Arc<OxiDb>,
    auth: Option<Arc<S3Auth>>,
    uploads: Mutex<HashMap<String, MultipartUpload>>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn start_s3_listener(addr: &str, db: Arc<OxiDb>) -> std::thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).expect("failed to bind S3 HTTP listener");

    let auth = match (
        std::env::var("OXIDB_S3_ACCESS_KEY"),
        std::env::var("OXIDB_S3_SECRET_KEY"),
    ) {
        (Ok(ak), Ok(sk)) if !ak.is_empty() && !sk.is_empty() => {
            eprintln!("[s3] authentication enabled (access_key={}...)", &ak[..ak.len().min(4)]);
            Some(Arc::new(S3Auth { access_key: ak, secret_key: sk }))
        }
        _ => {
            eprintln!("[s3] authentication disabled (set OXIDB_S3_ACCESS_KEY and OXIDB_S3_SECRET_KEY to enable)");
            None
        }
    };

    let state = Arc::new(S3State {
        db,
        auth,
        uploads: Mutex::new(HashMap::new()),
    });

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    let state = Arc::clone(&state);
                    std::thread::spawn(move || {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            handle_connection(s, &state);
                        }));
                        if let Err(e) = result {
                            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                                s.to_string()
                            } else if let Some(s) = e.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "unknown panic".to_string()
                            };
                            eprintln!("[s3] connection handler panicked: {msg}");
                        }
                    });
                }
                Err(e) => eprintln!("[s3] accept error: {e}"),
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Connection handling & routing
// ---------------------------------------------------------------------------

fn handle_connection(mut stream: TcpStream, state: &S3State) {
    let req = match parse_request(&stream) {
        Some(r) => r,
        None => return,
    };

    // CORS preflight
    if req.method == "OPTIONS" {
        HttpResponse::no_content().with_cors().write_to(&mut stream);
        return;
    }

    // Authenticate
    if let Some(auth) = &state.auth {
        if !verify_auth(&req, auth) {
            error_response(403, "AccessDenied", "Access Denied", &req.path)
                .with_cors()
                .write_to(&mut stream);
            return;
        }
    }

    let path = url_decode(&req.path);
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let params = parse_query(&req.query);

    let resp = route_request(&req, &segments, &params, state);
    resp.with_cors().write_to(&mut stream);
}

fn route_request(
    req: &HttpRequest,
    segments: &[&str],
    params: &HashMap<String, String>,
    state: &S3State,
) -> HttpResponse {
    let db = &state.db;
    let method = req.method.as_str();

    match (method, segments.len()) {
        // --- Service-level ---
        ("GET", 0) => bucket::handle_list_buckets(db),

        // --- Bucket-level ---
        ("PUT", 1) => bucket::handle_create_bucket(db, segments[0]),
        ("DELETE", 1) if !params.contains_key("delete") => bucket::handle_delete_bucket(db, segments[0]),
        ("HEAD", 1) => bucket::handle_head_bucket(db, segments[0]),
        ("GET", 1) => bucket::handle_list_objects(db, segments[0], params),

        // Batch delete: POST /bucket?delete
        ("POST", 1) if params.contains_key("delete") => batch::handle_batch_delete(db, segments[0], req),

        // --- Object-level ---

        // Multipart: POST /bucket/key?uploads → initiate
        ("POST", n) if n >= 2 && params.contains_key("uploads") => {
            let key = segments[1..].join("/");
            multipart::handle_create_multipart(state, segments[0], &key, req)
        }

        // Multipart: PUT /bucket/key?partNumber=N&uploadId=ID → upload part
        ("PUT", n) if n >= 2 && params.contains_key("partNumber") && params.contains_key("uploadId") => {
            let key = segments[1..].join("/");
            multipart::handle_upload_part(state, segments[0], &key, req, params)
        }

        // Multipart: POST /bucket/key?uploadId=ID → complete
        ("POST", n) if n >= 2 && params.contains_key("uploadId") => {
            let key = segments[1..].join("/");
            multipart::handle_complete_multipart(state, segments[0], &key, params)
        }

        // Multipart: DELETE /bucket/key?uploadId=ID → abort
        ("DELETE", n) if n >= 2 && params.contains_key("uploadId") => {
            let key = segments[1..].join("/");
            multipart::handle_abort_multipart(state, &key, params)
        }

        // Tagging: GET/PUT/DELETE /bucket/key?tagging
        ("GET", n) if n >= 2 && params.contains_key("tagging") => {
            let key = segments[1..].join("/");
            tagging::handle_get_tagging(db, segments[0], &key)
        }
        ("PUT", n) if n >= 2 && params.contains_key("tagging") => {
            let key = segments[1..].join("/");
            tagging::handle_put_tagging(db, segments[0], &key, req)
        }
        ("DELETE", n) if n >= 2 && params.contains_key("tagging") => {
            let key = segments[1..].join("/");
            tagging::handle_delete_tagging(db, segments[0], &key)
        }

        // Copy object: PUT with x-amz-copy-source header
        ("PUT", n) if n >= 2 && req.headers.contains_key("x-amz-copy-source") => {
            let key = segments[1..].join("/");
            object::handle_copy_object(db, segments[0], &key, req)
        }

        // Regular PUT object
        ("PUT", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_put_object(db, segments[0], &key, req)
        }

        // GET object
        ("GET", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_get_object(db, segments[0], &key, req)
        }

        // HEAD object
        ("HEAD", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_head_object(db, segments[0], &key)
        }

        // DELETE object
        ("DELETE", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_delete_object(db, segments[0], &key)
        }

        _ => error_response(405, "MethodNotAllowed", "Method not allowed", ""),
    }
}
