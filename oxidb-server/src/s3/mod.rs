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
mod encryption;
mod helpers;
pub mod http;
mod lifecycle;
mod multipart;
mod object;
mod tagging;

use std::collections::HashMap;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Thread pool size for S3 connections.
const POOL_SIZE: usize = 256;
/// Maximum queued connections before rejecting.
const MAX_QUEUED: usize = 1024;

use oxidb::OxiDb;

use auth::{S3Auth, verify_auth};
use encryption::S3Encryption;
use helpers::{parse_query, url_decode};
use http::{HttpRequest, HttpResponse, error_response, parse_request_from_reader};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Maximum parts per multipart upload.
const MAX_MULTIPART_PARTS: u32 = 10_000;
/// Maximum total size for all parts in a single multipart upload (5 GiB).
const MAX_MULTIPART_TOTAL: usize = 5 * 1024 * 1024 * 1024;
/// Abandoned uploads are cleaned up after this duration.
const UPLOAD_TTL_SECS: u64 = 86400; // 24 hours

/// In-progress multipart upload.
struct MultipartUpload {
    bucket: String,
    key: String,
    content_type: String,
    metadata: HashMap<String, String>,
    parts: HashMap<u32, Vec<u8>>,
    total_bytes: usize,
    created_at: std::time::Instant,
    /// SSE marker: None, "AES256" (SSE-S3), or "SSE-C:<base64-key>" (SSE-C).
    sse_marker: Option<String>,
}

impl MultipartUpload {
    /// Zeroize SSE-C key material from memory.
    fn zeroize_key(&mut self) {
        if let Some(ref mut marker) = self.sse_marker
            && marker.starts_with("SSE-C:")
        {
            let bytes = unsafe { marker.as_bytes_mut() };
            for b in bytes.iter_mut() {
                unsafe {
                    std::ptr::write_volatile(b, 0);
                }
            }
        }
    }
}

/// Shared state for all S3 connections.
struct S3State {
    db: Arc<OxiDb>,
    auth: Option<Arc<S3Auth>>,
    encryption: Option<Arc<S3Encryption>>,
    uploads: Mutex<HashMap<String, MultipartUpload>>,
    active_connections: AtomicUsize,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn start_s3_listener(addr: &str, db: Arc<OxiDb>) -> std::thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).expect("failed to bind S3 HTTP listener");

    let auth = match S3Auth::from_env() {
        Some(a) => {
            eprintln!(
                "[s3] authentication enabled ({} credential(s))",
                a.credentials.len()
            );
            Some(Arc::new(a))
        }
        None => {
            eprintln!("[s3] WARNING: authentication DISABLED — S3 API is open to anyone!");
            eprintln!(
                "[s3] Set OXIDB_S3_ACCESS_KEY/OXIDB_S3_SECRET_KEY or OXIDB_S3_CREDENTIALS to enable auth."
            );
            None
        }
    };

    let default_enc = std::env::var("OXIDB_S3_DEFAULT_ENCRYPTION")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let encryption = match std::env::var("OXIDB_S3_ENCRYPTION_KEY") {
        Ok(hex_key) if !hex_key.is_empty() => {
            match S3Encryption::from_hex_key(&hex_key, default_enc) {
                Some(enc) => {
                    eprintln!(
                        "[s3] server-side encryption enabled (SSE-S3){}",
                        if default_enc {
                            " [default for all objects]"
                        } else {
                            ""
                        }
                    );
                    Some(enc)
                }
                None => {
                    eprintln!("[s3] WARNING: invalid OXIDB_S3_ENCRYPTION_KEY, encryption disabled");
                    None
                }
            }
        }
        _ => {
            if default_enc {
                eprintln!(
                    "[s3] WARNING: OXIDB_S3_DEFAULT_ENCRYPTION=true but no OXIDB_S3_ENCRYPTION_KEY set"
                );
            }
            None
        }
    };

    let state = Arc::new(S3State {
        db,
        auth,
        encryption,
        uploads: Mutex::new(HashMap::new()),
        active_connections: AtomicUsize::new(0),
    });

    // Lifecycle expiration sweeper (every 5 min; cheap when no rules).
    let db_sweep = Arc::clone(&state.db);
    {
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(300));
                let n = lifecycle::sweep(&db_sweep);
                if n > 0 {
                    eprintln!("[s3] lifecycle expired {n} objects");
                }
            }
        });
    }

    // Background cleanup of abandoned multipart uploads
    {
        let state_cleanup = Arc::clone(&state);
        std::thread::Builder::new()
            .name("s3-upload-gc".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(3600)); // check every hour
                    let mut uploads = state_cleanup.uploads.lock().unwrap();
                    let before = uploads.len();
                    uploads.retain(|_, u| u.created_at.elapsed().as_secs() < UPLOAD_TTL_SECS);
                    let removed = before - uploads.len();
                    if removed > 0 {
                        eprintln!("[s3] cleaned up {removed} abandoned multipart uploads");
                    }
                }
            })
            .expect("failed to spawn s3-upload-gc");
    }

    // Thread pool: fixed workers with bounded queue
    let (conn_tx, conn_rx) = std::sync::mpsc::sync_channel::<TcpStream>(MAX_QUEUED);
    let conn_rx = Arc::new(Mutex::new(conn_rx));

    for i in 0..POOL_SIZE {
        let rx = Arc::clone(&conn_rx);
        let state = Arc::clone(&state);
        std::thread::Builder::new()
            .name(format!("s3-worker-{i}"))
            .spawn(move || {
                loop {
                    let stream = match rx.lock().unwrap().recv() {
                        Ok(s) => s,
                        Err(_) => return, // channel closed
                    };
                    state.active_connections.fetch_add(1, Ordering::Relaxed);
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handle_connection(stream, &state);
                    }));
                    state.active_connections.fetch_sub(1, Ordering::Relaxed);
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
                }
            })
            .expect("failed to spawn s3 worker");
    }
    eprintln!("[s3] thread pool: {POOL_SIZE} workers, queue depth {MAX_QUEUED}");

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    if conn_tx.try_send(s).is_err() {
                        eprintln!("[s3] connection rejected: queue full");
                    }
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
    // Keep-alive timeout: close idle connections after 30s
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let read_stream = stream.try_clone().expect("failed to clone stream");
    let mut reader = BufReader::new(read_stream);

    loop {
        let req = match parse_request_from_reader(&mut reader, &stream) {
            Some(r) => r,
            None => return, // connection closed or timeout
        };

        let wants_close = req
            .headers
            .get("connection")
            .map(|v| v.eq_ignore_ascii_case("close"))
            .unwrap_or(false);

        // CORS preflight
        if req.method == "OPTIONS" {
            HttpResponse::no_content()
                .with_cors()
                .write_to_keepalive(&mut stream, !wants_close);
            if wants_close {
                return;
            }
            continue;
        }

        // Authenticate
        if let Some(auth) = &state.auth
            && !verify_auth(&req, auth)
        {
            error_response(403, "AccessDenied", "Access Denied", &req.path)
                .with_cors()
                .write_to_keepalive(&mut stream, !wants_close);
            if wants_close {
                return;
            }
            continue;
        }

        let path = url_decode(&req.path);
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let params = parse_query(&req.query);

        let resp = route_request(&req, &segments, &params, state);
        resp.with_cors()
            .write_to_keepalive(&mut stream, !wants_close);

        if wants_close {
            return;
        }
    }
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
        // Lifecycle: PUT/GET/DELETE /bucket?lifecycle
        ("PUT", 1) if params.contains_key("lifecycle") => {
            lifecycle::handle_put_lifecycle(db, segments[0], &req.body)
        }
        ("GET", 1) if params.contains_key("lifecycle") => {
            lifecycle::handle_get_lifecycle(db, segments[0])
        }
        ("DELETE", 1) if params.contains_key("lifecycle") => {
            lifecycle::handle_delete_lifecycle(db, segments[0])
        }
        ("PUT", 1) => bucket::handle_create_bucket(db, segments[0]),
        ("DELETE", 1) if !params.contains_key("delete") => {
            bucket::handle_delete_bucket(db, segments[0])
        }
        ("HEAD", 1) => bucket::handle_head_bucket(db, segments[0]),
        ("GET", 1) => bucket::handle_list_objects(db, segments[0], params),

        // Batch delete: POST /bucket?delete
        ("POST", 1) if params.contains_key("delete") => {
            batch::handle_batch_delete(db, segments[0], req)
        }

        // --- Object-level ---

        // Multipart: POST /bucket/key?uploads → initiate
        ("POST", n) if n >= 2 && params.contains_key("uploads") => {
            let key = segments[1..].join("/");
            multipart::handle_create_multipart(state, segments[0], &key, req)
        }

        // Multipart: PUT /bucket/key?partNumber=N&uploadId=ID → upload part
        ("PUT", n)
            if n >= 2 && params.contains_key("partNumber") && params.contains_key("uploadId") =>
        {
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
            object::handle_copy_object(state, segments[0], &key, req)
        }

        // Regular PUT object
        ("PUT", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_put_object(state, segments[0], &key, req)
        }

        // GET object
        ("GET", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_get_object(state, segments[0], &key, req)
        }

        // HEAD object
        ("HEAD", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_head_object(state, segments[0], &key)
        }

        // DELETE object
        ("DELETE", n) if n >= 2 => {
            let key = segments[1..].join("/");
            object::handle_delete_object(db, segments[0], &key)
        }

        _ => error_response(405, "MethodNotAllowed", "Method not allowed", ""),
    }
}
