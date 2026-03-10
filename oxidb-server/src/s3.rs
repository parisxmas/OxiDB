//! S3-compatible HTTP REST API for OxiDB blob storage.
//!
//! Enabled via `--features s3` and `OXIDB_S3_PORT=9000`.
//! Supports path-style requests: `PUT /bucket/key`, `GET /bucket/key`, etc.
//! Compatible with AWS CLI, boto3, and other S3 clients.
//!
//! Features:
//! - Full bucket & object CRUD
//! - AWS Signature V4 authentication (header + presigned URL)
//! - Multipart uploads (CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload)
//! - Copy object (x-amz-copy-source)
//! - Range requests (Range: bytes=start-end)
//! - Conditional requests (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since)
//! - Batch delete (POST /?delete)
//! - Object tagging (GET/PUT/DELETE ?tagging)
//! - CORS (OPTIONS preflight)
//! - Presigned URLs (query-string auth)

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use oxidb::OxiDb;
use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// S3 authentication credentials (optional).
struct S3Auth {
    access_key: String,
    secret_key: String,
}

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
// HTTP/1.1 parsing
// ---------------------------------------------------------------------------

struct HttpRequest {
    method: String,
    path: String,
    query: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn parse_request(stream: &TcpStream) -> Option<HttpRequest> {
    let mut reader = BufReader::new(stream);

    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let parts: Vec<&str> = line.trim().splitn(3, ' ').collect();
    if parts.len() < 2 {
        return None;
    }
    let method = parts[0].to_uppercase();
    let raw_path = parts[1];

    let (path, query) = match raw_path.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (raw_path.to_string(), String::new()),
    };

    let mut headers = HashMap::new();
    loop {
        let mut hline = String::new();
        if reader.read_line(&mut hline).ok()? == 0 {
            break;
        }
        let hline = hline.trim_end().to_string();
        if hline.is_empty() {
            break;
        }
        if let Some((k, v)) = hline.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    // Handle Expect: 100-continue
    if headers.get("expect").is_some_and(|v| v.contains("100-continue")) {
        let cont = b"HTTP/1.1 100 Continue\r\n\r\n";
        let _ = stream.try_clone().ok()?.write_all(cont);
    }

    let body = if let Some(cl) = headers.get("content-length") {
        let len: usize = cl.parse().unwrap_or(0);
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).ok()?;
        buf
    } else if headers.get("transfer-encoding").is_some_and(|v| v.contains("chunked")) {
        read_chunked(&mut reader).unwrap_or_default()
    } else {
        Vec::new()
    };

    Some(HttpRequest { method, path, query, headers, body })
}

fn read_chunked(reader: &mut BufReader<&TcpStream>) -> Option<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let size = usize::from_str_radix(line.trim(), 16).ok()?;
        if size == 0 {
            let mut trail = String::new();
            let _ = reader.read_line(&mut trail);
            break;
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk).ok()?;
        body.extend_from_slice(&chunk);
        let mut trail = String::new();
        let _ = reader.read_line(&mut trail);
    }
    Some(body)
}

// ---------------------------------------------------------------------------
// HTTP response
// ---------------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    status_text: &'static str,
    content_type: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    content_length_override: Option<u64>,
}

impl HttpResponse {
    fn xml(status: u16, status_text: &'static str, body: String) -> Self {
        Self {
            status,
            status_text,
            content_type: "application/xml".to_string(),
            headers: Vec::new(),
            body: body.into_bytes(),
            content_length_override: None,
        }
    }

    fn ok_xml(body: String) -> Self {
        Self::xml(200, "OK", body)
    }

    fn no_content() -> Self {
        Self {
            status: 204,
            status_text: "No Content",
            content_type: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            content_length_override: None,
        }
    }

    fn data(body: Vec<u8>, content_type: &str) -> Self {
        Self {
            status: 200,
            status_text: "OK",
            content_type: content_type.to_string(),
            headers: Vec::new(),
            body,
            content_length_override: None,
        }
    }

    fn partial(body: Vec<u8>, content_type: &str, range: &str, total: u64) -> Self {
        Self {
            status: 206,
            status_text: "Partial Content",
            content_type: content_type.to_string(),
            headers: vec![
                ("Content-Range".to_string(), format!("bytes {range}/{total}")),
            ],
            body,
            content_length_override: None,
        }
    }

    fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }

    fn with_cors(self) -> Self {
        self.with_header("Access-Control-Allow-Origin", "*")
            .with_header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
            .with_header("Access-Control-Allow-Headers", "Content-Type, Authorization, x-amz-date, x-amz-content-sha256, x-amz-copy-source, Range, If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since")
            .with_header("Access-Control-Expose-Headers", "ETag, x-amz-request-id, Content-Range, Accept-Ranges")
            .with_header("Access-Control-Max-Age", "3600")
    }

    fn write_to(self, stream: &mut TcpStream) {
        let content_length = self.content_length_override.unwrap_or(self.body.len() as u64);
        let mut resp = format!(
            "HTTP/1.1 {} {}\r\nConnection: close\r\nContent-Length: {}\r\nServer: OxiDB-S3\r\n",
            self.status, self.status_text, content_length
        );
        if !self.content_type.is_empty() {
            resp.push_str(&format!("Content-Type: {}\r\n", self.content_type));
        }
        for (k, v) in &self.headers {
            resp.push_str(&format!("{k}: {v}\r\n"));
        }
        resp.push_str("\r\n");
        let _ = stream.write_all(resp.as_bytes());
        if !self.body.is_empty() {
            let _ = stream.write_all(&self.body);
        }
        let _ = stream.flush();
    }
}

// ---------------------------------------------------------------------------
// AWS Signature V4 verification
// ---------------------------------------------------------------------------

fn verify_auth(req: &HttpRequest, auth: &S3Auth) -> bool {
    // Check for presigned URL auth first
    let params = parse_query(&req.query);
    if params.contains_key("X-Amz-Signature") {
        return verify_presigned(req, auth, &params);
    }

    let auth_header = match req.headers.get("authorization") {
        Some(h) => h,
        None => return false,
    };

    if !auth_header.starts_with("AWS4-HMAC-SHA256") {
        return false;
    }

    let cred_start = match auth_header.find("Credential=") {
        Some(i) => i + 11,
        None => return false,
    };
    let cred_end = auth_header[cred_start..].find('/').unwrap_or(0) + cred_start;
    let access_key = &auth_header[cred_start..cred_end];

    if access_key != auth.access_key {
        return false;
    }

    let sig_start = match auth_header.find("Signature=") {
        Some(i) => i + 10,
        None => return false,
    };
    let signature = auth_header[sig_start..].trim();

    let signed_headers_start = match auth_header.find("SignedHeaders=") {
        Some(i) => i + 14,
        None => return false,
    };
    let signed_headers_end = auth_header[signed_headers_start..].find(',').unwrap_or(auth_header.len() - signed_headers_start) + signed_headers_start;
    let signed_headers_str = &auth_header[signed_headers_start..signed_headers_end];

    let scope_end = auth_header[cred_start..].find(',').unwrap_or(auth_header.len() - cred_start) + cred_start;
    let credential_scope = &auth_header[cred_end + 1..scope_end];
    let date_stamp = credential_scope.split('/').next().unwrap_or("");

    let method = &req.method;
    let canonical_uri = if req.path.is_empty() { "/" } else { &req.path };
    let canonical_querystring = canonical_query(&req.query);

    let signed_headers: Vec<&str> = signed_headers_str.split(';').collect();
    let mut canonical_headers = String::new();
    for h in &signed_headers {
        let val = req.headers.get(*h).map(|v| v.as_str()).unwrap_or("");
        canonical_headers.push_str(&format!("{}:{}\n", h, val.trim()));
    }

    let payload_hash = req.headers.get("x-amz-content-sha256")
        .cloned()
        .unwrap_or_else(|| sha256_hex(&req.body));

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_querystring,
        canonical_headers, signed_headers_str, payload_hash
    );

    let amz_date = req.headers.get("x-amz-date").map(|v| v.as_str()).unwrap_or("");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date, credential_scope, sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(&auth.secret_key, date_stamp, credential_scope);
    let computed_sig = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

    constant_time_eq(signature.as_bytes(), computed_sig.as_bytes())
}

fn verify_presigned(req: &HttpRequest, auth: &S3Auth, params: &HashMap<String, String>) -> bool {
    let algorithm = params.get("X-Amz-Algorithm").map(|s| s.as_str()).unwrap_or("");
    if algorithm != "AWS4-HMAC-SHA256" {
        return false;
    }

    let credential = params.get("X-Amz-Credential").map(|s| s.as_str()).unwrap_or("");
    let parts: Vec<&str> = credential.splitn(2, '/').collect();
    if parts.len() < 2 {
        return false;
    }
    let access_key = parts[0];
    let credential_scope = parts[1];

    if access_key != auth.access_key {
        return false;
    }

    let date_stamp = credential_scope.split('/').next().unwrap_or("");
    let amz_date = params.get("X-Amz-Date").map(|s| s.as_str()).unwrap_or("");
    let signed_headers_str = params.get("X-Amz-SignedHeaders").map(|s| s.as_str()).unwrap_or("host");
    let signature = params.get("X-Amz-Signature").map(|s| s.as_str()).unwrap_or("");

    // Check expiration
    if let Some(expires) = params.get("X-Amz-Expires").and_then(|v| v.parse::<u64>().ok()) {
        if let Some(date_str) = params.get("X-Amz-Date") {
            if let Ok(request_time) = parse_amz_date(date_str) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now > request_time + expires {
                    return false; // Expired
                }
            }
        }
    }

    let method = &req.method;
    let canonical_uri = if req.path.is_empty() { "/" } else { &req.path };

    // Build canonical query string WITHOUT the signature param
    let mut qpairs: Vec<(&str, &str)> = req.query.split('&')
        .filter_map(|p| {
            let mut parts = p.splitn(2, '=');
            let k = parts.next()?;
            let v = parts.next().unwrap_or("");
            Some((k, v))
        })
        .filter(|(k, _)| *k != "X-Amz-Signature")
        .collect();
    qpairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let canonical_querystring = qpairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");

    let signed_headers: Vec<&str> = signed_headers_str.split(';').collect();
    let mut canonical_headers = String::new();
    for h in &signed_headers {
        let val = req.headers.get(*h).map(|v| v.as_str()).unwrap_or("");
        canonical_headers.push_str(&format!("{}:{}\n", h, val.trim()));
    }

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
        method, canonical_uri, canonical_querystring,
        canonical_headers, signed_headers_str
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date, credential_scope, sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(&auth.secret_key, date_stamp, credential_scope);
    let computed_sig = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

    constant_time_eq(signature.as_bytes(), computed_sig.as_bytes())
}

fn parse_amz_date(s: &str) -> Result<u64, ()> {
    // Parse 20260310T120000Z format
    if s.len() < 15 { return Err(()); }
    let year: u64 = s[0..4].parse().map_err(|_| ())?;
    let month: u64 = s[4..6].parse().map_err(|_| ())?;
    let day: u64 = s[6..8].parse().map_err(|_| ())?;
    let hour: u64 = s[9..11].parse().map_err(|_| ())?;
    let min: u64 = s[11..13].parse().map_err(|_| ())?;
    let sec: u64 = s[13..15].parse().map_err(|_| ())?;
    // Rough epoch calculation (good enough for expiration checks)
    let days = (year - 1970) * 365 + (year - 1969) / 4 + days_before_month(month, year) + day - 1;
    Ok(days * 86400 + hour * 3600 + min * 60 + sec)
}

fn days_before_month(month: u64, year: u64) -> u64 {
    let leap = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 1 } else { 0 };
    match month {
        1 => 0,
        2 => 31,
        3 => 59 + leap,
        4 => 90 + leap,
        5 => 120 + leap,
        6 => 151 + leap,
        7 => 181 + leap,
        8 => 212 + leap,
        9 => 243 + leap,
        10 => 273 + leap,
        11 => 304 + leap,
        12 => 334 + leap,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Crypto helpers
// ---------------------------------------------------------------------------

fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&str, &str)> = query.split('&')
        .filter_map(|p| {
            let mut parts = p.splitn(2, '=');
            let k = parts.next()?;
            let v = parts.next().unwrap_or("");
            Some((k, v))
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
}

fn sha256_hex(data: &[u8]) -> String {
    hex_encode(&Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex_encode(&hmac_sha256(key, data))
}

fn derive_signing_key(secret_key: &str, date_stamp: &str, credential_scope: &str) -> Vec<u8> {
    let parts: Vec<&str> = credential_scope.split('/').collect();
    let region = parts.get(1).unwrap_or(&"us-east-1");
    let service = parts.get(2).unwrap_or(&"s3");

    let k_date = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ---------------------------------------------------------------------------
// Request routing
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
        ("GET", 0) => handle_list_buckets(db),

        // --- Bucket-level ---
        ("PUT", 1) => handle_create_bucket(db, segments[0]),
        ("DELETE", 1) if !params.contains_key("delete") => handle_delete_bucket(db, segments[0]),
        ("HEAD", 1) => handle_head_bucket(db, segments[0]),
        ("GET", 1) => handle_list_objects(db, segments[0], params),

        // Batch delete: POST /bucket?delete
        ("POST", 1) if params.contains_key("delete") => handle_batch_delete(db, segments[0], req),

        // --- Object-level ---

        // Multipart: POST /bucket/key?uploads → initiate
        ("POST", n) if n >= 2 && params.contains_key("uploads") => {
            let key = segments[1..].join("/");
            handle_create_multipart(state, segments[0], &key, req)
        }

        // Multipart: PUT /bucket/key?partNumber=N&uploadId=ID → upload part
        ("PUT", n) if n >= 2 && params.contains_key("partNumber") && params.contains_key("uploadId") => {
            let key = segments[1..].join("/");
            handle_upload_part(state, segments[0], &key, req, params)
        }

        // Multipart: POST /bucket/key?uploadId=ID → complete
        ("POST", n) if n >= 2 && params.contains_key("uploadId") => {
            let key = segments[1..].join("/");
            handle_complete_multipart(state, segments[0], &key, params)
        }

        // Multipart: DELETE /bucket/key?uploadId=ID → abort
        ("DELETE", n) if n >= 2 && params.contains_key("uploadId") => {
            let key = segments[1..].join("/");
            handle_abort_multipart(state, &key, params)
        }

        // Tagging: GET/PUT/DELETE /bucket/key?tagging
        ("GET", n) if n >= 2 && params.contains_key("tagging") => {
            let key = segments[1..].join("/");
            handle_get_tagging(db, segments[0], &key)
        }
        ("PUT", n) if n >= 2 && params.contains_key("tagging") => {
            let key = segments[1..].join("/");
            handle_put_tagging(db, segments[0], &key, req)
        }
        ("DELETE", n) if n >= 2 && params.contains_key("tagging") => {
            let key = segments[1..].join("/");
            handle_delete_tagging(db, segments[0], &key)
        }

        // Copy object: PUT with x-amz-copy-source header
        ("PUT", n) if n >= 2 && req.headers.contains_key("x-amz-copy-source") => {
            let key = segments[1..].join("/");
            handle_copy_object(db, segments[0], &key, req)
        }

        // Regular PUT object
        ("PUT", n) if n >= 2 => {
            let key = segments[1..].join("/");
            handle_put_object(db, segments[0], &key, req)
        }

        // GET object (with optional range)
        ("GET", n) if n >= 2 => {
            let key = segments[1..].join("/");
            handle_get_object(db, segments[0], &key, req)
        }

        // HEAD object
        ("HEAD", n) if n >= 2 => {
            let key = segments[1..].join("/");
            handle_head_object(db, segments[0], &key)
        }

        // DELETE object
        ("DELETE", n) if n >= 2 => {
            let key = segments[1..].join("/");
            handle_delete_object(db, segments[0], &key)
        }

        _ => error_response(405, "MethodNotAllowed", "Method not allowed", ""),
    }
}

// ---------------------------------------------------------------------------
// Bucket handlers
// ---------------------------------------------------------------------------

fn handle_list_buckets(db: &OxiDb) -> HttpResponse {
    let buckets = db.list_buckets();
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Owner><ID>oxidb</ID><DisplayName>oxidb</DisplayName></Owner>\n  <Buckets>\n");
    for name in &buckets {
        xml.push_str(&format!(
            "    <Bucket><Name>{}</Name><CreationDate>2026-01-01T00:00:00Z</CreationDate></Bucket>\n",
            xml_escape(name)
        ));
    }
    xml.push_str("  </Buckets>\n</ListAllMyBucketsResult>");
    HttpResponse::ok_xml(xml)
}

fn handle_create_bucket(db: &OxiDb, bucket: &str) -> HttpResponse {
    match db.create_bucket(bucket) {
        Ok(_) => HttpResponse::ok_xml(String::new())
            .with_header("Location", &format!("/{bucket}")),
        Err(e) => error_response(500, "InternalError", &e.to_string(), bucket),
    }
}

fn handle_delete_bucket(db: &OxiDb, bucket: &str) -> HttpResponse {
    match db.delete_bucket(bucket) {
        Ok(_) => HttpResponse::no_content(),
        Err(e) if e.to_string().contains("bucket not found") => {
            error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), bucket),
    }
}

fn handle_head_bucket(db: &OxiDb, bucket: &str) -> HttpResponse {
    let buckets = db.list_buckets();
    if buckets.iter().any(|b| b == bucket) {
        HttpResponse {
            status: 200,
            status_text: "OK",
            content_type: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            content_length_override: None,
        }
    } else {
        error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
    }
}

fn handle_list_objects(db: &OxiDb, bucket: &str, params: &HashMap<String, String>) -> HttpResponse {
    let prefix = params.get("prefix").map(|s| s.as_str());
    let max_keys: usize = params.get("max-keys").and_then(|v| v.parse().ok()).unwrap_or(1000);
    let delimiter = params.get("delimiter").map(|s| s.as_str());
    let start_after = params.get("start-after").map(|s| s.as_str());

    match db.list_objects(bucket, prefix, Some(max_keys + 1000)) {
        Ok(all_objects) => {
            // Filter by start-after
            let objects: Vec<_> = all_objects.into_iter()
                .filter(|obj| {
                    if let Some(start) = start_after {
                        let meta = serde_json::to_value(obj).unwrap_or_default();
                        let key = meta["key"].as_str().unwrap_or("");
                        key > start
                    } else {
                        true
                    }
                })
                .take(max_keys)
                .collect();

            // Collect common prefixes if delimiter is set
            let mut common_prefixes: Vec<String> = Vec::new();
            if let Some(delim) = delimiter {
                let pfx = prefix.unwrap_or("");
                let mut seen = std::collections::HashSet::new();
                for obj in &objects {
                    let meta = serde_json::to_value(obj).unwrap_or_default();
                    let key = meta["key"].as_str().unwrap_or("");
                    if key.starts_with(pfx) {
                        let rest = &key[pfx.len()..];
                        if let Some(idx) = rest.find(delim) {
                            let cp = format!("{}{}{}", pfx, &rest[..idx], delim);
                            if seen.insert(cp.clone()) {
                                common_prefixes.push(cp);
                            }
                        }
                    }
                }
                common_prefixes.sort();
            }

            let truncated = objects.len() >= max_keys;
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Name>{}</Name>\n  <Prefix>{}</Prefix>\n  <MaxKeys>{}</MaxKeys>\n  <IsTruncated>{}</IsTruncated>\n  <KeyCount>{}</KeyCount>\n",
                xml_escape(bucket),
                xml_escape(prefix.unwrap_or("")),
                max_keys,
                truncated,
                objects.len(),
            );
            if let Some(d) = delimiter {
                xml.push_str(&format!("  <Delimiter>{}</Delimiter>\n", xml_escape(d)));
            }
            for obj in &objects {
                let meta = serde_json::to_value(obj).unwrap_or_default();
                let key = meta["key"].as_str().unwrap_or("");
                let size = meta["size"].as_u64().unwrap_or(0);
                let etag = meta["etag"].as_str().unwrap_or("");
                let created = meta["created_at"].as_str().unwrap_or("");

                // Skip keys that fall under a common prefix when delimiter is set
                if delimiter.is_some() && common_prefixes.iter().any(|cp| key.starts_with(cp.as_str()) && key != cp.as_str()) {
                    continue;
                }

                xml.push_str(&format!(
                    "  <Contents>\n    <Key>{}</Key>\n    <LastModified>{}</LastModified>\n    <ETag>\"{}\"</ETag>\n    <Size>{}</Size>\n    <StorageClass>STANDARD</StorageClass>\n  </Contents>\n",
                    xml_escape(key), xml_escape(created), xml_escape(etag), size
                ));
            }
            for cp in &common_prefixes {
                xml.push_str(&format!(
                    "  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>\n",
                    xml_escape(cp)
                ));
            }
            xml.push_str("</ListBucketResult>");
            HttpResponse::ok_xml(xml)
        }
        Err(e) if e.to_string().contains("bucket not found") => {
            error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), bucket),
    }
}

// ---------------------------------------------------------------------------
// Object handlers
// ---------------------------------------------------------------------------

fn handle_put_object(db: &OxiDb, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    let content_type = req.headers.get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let mut metadata = HashMap::new();
    for (k, v) in &req.headers {
        if let Some(meta_key) = k.strip_prefix("x-amz-meta-") {
            metadata.insert(meta_key.to_string(), v.clone());
        }
    }

    match db.put_object(bucket, key, &req.body, &content_type, metadata) {
        Ok(meta) => {
            let etag = meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
            HttpResponse::ok_xml(String::new())
                .with_header("ETag", &format!("\"{etag}\""))
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

fn handle_get_object(db: &OxiDb, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    match db.get_object(bucket, key) {
        Ok((data, meta)) => {
            let content_type = meta["content_type"].as_str().unwrap_or("application/octet-stream");
            let etag_val = meta["etag"].as_str().unwrap_or("");
            let created = meta["created_at"].as_str().unwrap_or("");
            let etag_quoted = format!("\"{etag_val}\"");

            // Conditional: If-None-Match
            if let Some(inm) = req.headers.get("if-none-match") {
                if inm.trim_matches('"') == etag_val || inm == "*" {
                    return HttpResponse {
                        status: 304,
                        status_text: "Not Modified",
                        content_type: String::new(),
                        headers: Vec::new(),
                        body: Vec::new(),
                        content_length_override: None,
                    }.with_header("ETag", &etag_quoted);
                }
            }

            // Conditional: If-Match
            if let Some(im) = req.headers.get("if-match") {
                if im != "*" && im.trim_matches('"') != etag_val {
                    return error_response(412, "PreconditionFailed", "Precondition Failed", key);
                }
            }

            // Conditional: If-Modified-Since
            if let Some(ims) = req.headers.get("if-modified-since") {
                if created == ims.as_str() {
                    return HttpResponse {
                        status: 304,
                        status_text: "Not Modified",
                        content_type: String::new(),
                        headers: Vec::new(),
                        body: Vec::new(),
                        content_length_override: None,
                    }.with_header("ETag", &etag_quoted);
                }
            }

            // Conditional: If-Unmodified-Since
            if let Some(ius) = req.headers.get("if-unmodified-since") {
                if created != ius.as_str() {
                    return error_response(412, "PreconditionFailed", "Precondition Failed", key);
                }
            }

            // Range request
            if let Some(range_header) = req.headers.get("range") {
                if let Some(range) = parse_range(range_header, data.len() as u64) {
                    let total = data.len() as u64;
                    let slice = data[range.0 as usize..=range.1 as usize].to_vec();
                    let range_str = format!("{}-{}", range.0, range.1);
                    let mut resp = HttpResponse::partial(slice, content_type, &range_str, total)
                        .with_header("ETag", &etag_quoted)
                        .with_header("Last-Modified", created)
                        .with_header("Accept-Ranges", "bytes");
                    if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                        for (k, v) in user_meta {
                            if let Some(val) = v.as_str() {
                                resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                            }
                        }
                    }
                    return resp;
                }
                // Invalid range
                return error_response(416, "InvalidRange", "The requested range is not satisfiable", key);
            }

            // Full response
            let mut resp = HttpResponse::data(data, content_type)
                .with_header("ETag", &etag_quoted)
                .with_header("Last-Modified", created)
                .with_header("Accept-Ranges", "bytes");

            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if let Some(val) = v.as_str() {
                        resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                    }
                }
            }
            resp
        }
        Err(e) if e.to_string().contains("blob not found") || e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(e) if e.to_string().contains("bucket not found") => {
            error_response(404, "NoSuchBucket", "The specified bucket does not exist", bucket)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

fn handle_head_object(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    match db.head_object(bucket, key) {
        Ok(meta) => {
            let content_type = meta["content_type"].as_str().unwrap_or("application/octet-stream");
            let etag = meta["etag"].as_str().unwrap_or("");
            let size = meta["size"].as_u64().unwrap_or(0);
            let created = meta["created_at"].as_str().unwrap_or("");
            let mut resp = HttpResponse {
                status: 200,
                status_text: "OK",
                content_type: content_type.to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                content_length_override: Some(size),
            }
            .with_header("ETag", &format!("\"{etag}\""))
            .with_header("Last-Modified", created)
            .with_header("Accept-Ranges", "bytes");

            // Add user metadata
            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if let Some(val) = v.as_str() {
                        resp = resp.with_header(&format!("x-amz-meta-{k}"), val);
                    }
                }
            }
            resp
        }
        Err(e) if e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

fn handle_delete_object(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    match db.delete_object(bucket, key) {
        Ok(_) => HttpResponse::no_content(),
        Err(e) if e.to_string().contains("not found") => HttpResponse::no_content(),
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

// ---------------------------------------------------------------------------
// Copy object
// ---------------------------------------------------------------------------

fn handle_copy_object(db: &OxiDb, dest_bucket: &str, dest_key: &str, req: &HttpRequest) -> HttpResponse {
    let copy_source = req.headers.get("x-amz-copy-source").unwrap();
    let source = url_decode(copy_source);
    let source = source.strip_prefix('/').unwrap_or(&source);
    let source_parts: Vec<&str> = source.splitn(2, '/').collect();
    if source_parts.len() < 2 {
        return error_response(400, "InvalidArgument", "Invalid x-amz-copy-source", dest_key);
    }
    let src_bucket = source_parts[0];
    let src_key = source_parts[1];

    // Conditional copy
    if let Some(im) = req.headers.get("x-amz-copy-source-if-match") {
        if let Ok(meta) = db.head_object(src_bucket, src_key) {
            let etag = meta["etag"].as_str().unwrap_or("");
            if im.trim_matches('"') != etag && im != "*" {
                return error_response(412, "PreconditionFailed", "Precondition Failed", src_key);
            }
        }
    }
    if let Some(inm) = req.headers.get("x-amz-copy-source-if-none-match") {
        if let Ok(meta) = db.head_object(src_bucket, src_key) {
            let etag = meta["etag"].as_str().unwrap_or("");
            if inm.trim_matches('"') == etag {
                return error_response(412, "PreconditionFailed", "Precondition Failed", src_key);
            }
        }
    }

    match db.get_object(src_bucket, src_key) {
        Ok((data, src_meta)) => {
            let content_type = src_meta["content_type"].as_str().unwrap_or("application/octet-stream");

            // Use source metadata unless x-amz-metadata-directive=REPLACE
            let metadata = if req.headers.get("x-amz-metadata-directive").map(|v| v.as_str()) == Some("REPLACE") {
                let mut m = HashMap::new();
                for (k, v) in &req.headers {
                    if let Some(mk) = k.strip_prefix("x-amz-meta-") {
                        m.insert(mk.to_string(), v.clone());
                    }
                }
                m
            } else {
                src_meta.get("metadata")
                    .and_then(|v| v.as_object())
                    .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
                    .unwrap_or_default()
            };

            let ct = req.headers.get("content-type").map(|s| s.as_str()).unwrap_or(content_type);

            match db.put_object(dest_bucket, dest_key, &data, ct, metadata) {
                Ok(new_meta) => {
                    let etag = new_meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
                    let xml = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult>\n  <ETag>\"{}\"</ETag>\n  <LastModified>{}</LastModified>\n</CopyObjectResult>",
                        xml_escape(etag),
                        new_meta.get("created_at").and_then(|v| v.as_str()).unwrap_or("")
                    );
                    HttpResponse::ok_xml(xml)
                }
                Err(e) => error_response(500, "InternalError", &e.to_string(), dest_key),
            }
        }
        Err(e) if e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", src_key)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), src_key),
    }
}

// ---------------------------------------------------------------------------
// Multipart upload handlers
// ---------------------------------------------------------------------------

fn handle_create_multipart(state: &S3State, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    let content_type = req.headers.get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let mut metadata = HashMap::new();
    for (k, v) in &req.headers {
        if let Some(mk) = k.strip_prefix("x-amz-meta-") {
            metadata.insert(mk.to_string(), v.clone());
        }
    }

    let upload_id = format!("{:016x}", rand::random::<u64>());

    let upload = MultipartUpload {
        bucket: bucket.to_string(),
        key: key.to_string(),
        content_type,
        metadata,
        parts: HashMap::new(),
    };

    state.uploads.lock().unwrap().insert(upload_id.clone(), upload);

    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n</InitiateMultipartUploadResult>",
        xml_escape(bucket), xml_escape(key), xml_escape(&upload_id)
    );
    HttpResponse::ok_xml(xml)
}

fn handle_upload_part(state: &S3State, _bucket: &str, _key: &str, req: &HttpRequest, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();
    let part_number: u32 = match params.get("partNumber").and_then(|v| v.parse().ok()) {
        Some(n) => n,
        None => return error_response(400, "InvalidArgument", "Invalid partNumber", ""),
    };

    let mut uploads = state.uploads.lock().unwrap();
    match uploads.get_mut(upload_id.as_str()) {
        Some(upload) => {
            let etag = format!("{:08x}", crc32(&req.body));
            upload.parts.insert(part_number, req.body.clone());
            HttpResponse::ok_xml(String::new())
                .with_header("ETag", &format!("\"{etag}\""))
        }
        None => error_response(404, "NoSuchUpload", "The specified upload does not exist", upload_id),
    }
}

fn handle_complete_multipart(state: &S3State, _bucket: &str, _key: &str, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();

    let upload = {
        let mut uploads = state.uploads.lock().unwrap();
        match uploads.remove(upload_id.as_str()) {
            Some(u) => u,
            None => return error_response(404, "NoSuchUpload", "The specified upload does not exist", upload_id),
        }
    };

    // Assemble parts in order
    let mut part_nums: Vec<u32> = upload.parts.keys().copied().collect();
    part_nums.sort();

    let mut assembled = Vec::new();
    for num in &part_nums {
        if let Some(part) = upload.parts.get(num) {
            assembled.extend_from_slice(part);
        }
    }

    match state.db.put_object(&upload.bucket, &upload.key, &assembled, &upload.content_type, upload.metadata) {
        Ok(meta) => {
            let etag = meta.get("etag").and_then(|v| v.as_str()).unwrap_or("");
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <ETag>\"{}\"</ETag>\n</CompleteMultipartUploadResult>",
                xml_escape(&upload.bucket), xml_escape(&upload.key), xml_escape(etag)
            );
            HttpResponse::ok_xml(xml)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), &upload.key),
    }
}

fn handle_abort_multipart(state: &S3State, _key: &str, params: &HashMap<String, String>) -> HttpResponse {
    let upload_id = params.get("uploadId").unwrap();
    let mut uploads = state.uploads.lock().unwrap();
    uploads.remove(upload_id.as_str());
    HttpResponse::no_content()
}

// ---------------------------------------------------------------------------
// Batch delete
// ---------------------------------------------------------------------------

fn handle_batch_delete(db: &OxiDb, bucket: &str, req: &HttpRequest) -> HttpResponse {
    // Parse simple XML: <Delete><Object><Key>k</Key></Object>...</Delete>
    let body = String::from_utf8_lossy(&req.body);
    let keys = extract_xml_values(&body, "Key");

    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");

    for key in &keys {
        match db.delete_object(bucket, key) {
            Ok(_) | Err(_) => {
                // S3 reports deleted for all (even not found)
                xml.push_str(&format!("  <Deleted><Key>{}</Key></Deleted>\n", xml_escape(key)));
            }
        }
    }

    xml.push_str("</DeleteResult>");
    HttpResponse::ok_xml(xml)
}

// ---------------------------------------------------------------------------
// Tagging handlers
// ---------------------------------------------------------------------------

fn handle_get_tagging(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    match db.head_object(bucket, key) {
        Ok(meta) => {
            let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging>\n  <TagSet>\n");

            if let Some(user_meta) = meta.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in user_meta {
                    if let Some(tag_key) = k.strip_prefix("tag-") {
                        if let Some(val) = v.as_str() {
                            xml.push_str(&format!(
                                "    <Tag><Key>{}</Key><Value>{}</Value></Tag>\n",
                                xml_escape(tag_key), xml_escape(val)
                            ));
                        }
                    }
                }
            }

            xml.push_str("  </TagSet>\n</Tagging>");
            HttpResponse::ok_xml(xml)
        }
        Err(e) if e.to_string().contains("not found") => {
            error_response(404, "NoSuchKey", "The specified key does not exist", key)
        }
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

fn handle_put_tagging(db: &OxiDb, bucket: &str, key: &str, req: &HttpRequest) -> HttpResponse {
    // First get existing object
    let (data, meta) = match db.get_object(bucket, key) {
        Ok(r) => r,
        Err(e) if e.to_string().contains("not found") => {
            return error_response(404, "NoSuchKey", "The specified key does not exist", key);
        }
        Err(e) => return error_response(500, "InternalError", &e.to_string(), key),
    };

    let content_type = meta["content_type"].as_str().unwrap_or("application/octet-stream");

    // Get existing metadata, remove old tags, add new ones
    let mut metadata: HashMap<String, String> = meta.get("metadata")
        .and_then(|v| v.as_object())
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
        .unwrap_or_default();

    // Remove old tags
    metadata.retain(|k, _| !k.starts_with("tag-"));

    // Parse new tags from XML body
    let body_str = String::from_utf8_lossy(&req.body);
    let tag_keys = extract_xml_tag_pairs(&body_str);
    for (tk, tv) in &tag_keys {
        metadata.insert(format!("tag-{tk}"), tv.clone());
    }

    match db.put_object(bucket, key, &data, content_type, metadata) {
        Ok(_) => HttpResponse::ok_xml(String::new()),
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

fn handle_delete_tagging(db: &OxiDb, bucket: &str, key: &str) -> HttpResponse {
    let (data, meta) = match db.get_object(bucket, key) {
        Ok(r) => r,
        Err(e) if e.to_string().contains("not found") => {
            return error_response(404, "NoSuchKey", "The specified key does not exist", key);
        }
        Err(e) => return error_response(500, "InternalError", &e.to_string(), key),
    };

    let content_type = meta["content_type"].as_str().unwrap_or("application/octet-stream");
    let mut metadata: HashMap<String, String> = meta.get("metadata")
        .and_then(|v| v.as_object())
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect())
        .unwrap_or_default();

    metadata.retain(|k, _| !k.starts_with("tag-"));

    match db.put_object(bucket, key, &data, content_type, metadata) {
        Ok(_) => HttpResponse::no_content(),
        Err(e) => error_response(500, "InternalError", &e.to_string(), key),
    }
}

// ---------------------------------------------------------------------------
// Range parsing
// ---------------------------------------------------------------------------

fn parse_range(header: &str, total: u64) -> Option<(u64, u64)> {
    let s = header.strip_prefix("bytes=")?;
    let (start_s, end_s) = s.split_once('-')?;

    if start_s.is_empty() {
        // suffix range: bytes=-N means last N bytes
        let suffix: u64 = end_s.parse().ok()?;
        if suffix > total { return None; }
        Some((total - suffix, total - 1))
    } else {
        let start: u64 = start_s.parse().ok()?;
        if start >= total { return None; }
        let end = if end_s.is_empty() {
            total - 1
        } else {
            let e: u64 = end_s.parse().ok()?;
            e.min(total - 1)
        };
        if start > end { return None; }
        Some((start, end))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB88320 } else { crc >> 1 };
        }
    }
    !crc
}

fn error_response(status: u16, code: &str, message: &str, resource: &str) -> HttpResponse {
    let status_text = match status {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        412 => "Precondition Failed",
        416 => "Requested Range Not Satisfiable",
        _ => "Internal Server Error",
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>{}</Code>\n  <Message>{}</Message>\n  <Resource>{}</Resource>\n  <RequestId>0</RequestId>\n</Error>",
        xml_escape(code), xml_escape(message), xml_escape(resource)
    );
    HttpResponse::xml(status, status_text, xml)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            let hex = [hi, lo];
            if let Ok(val) = u8::from_str_radix(std::str::from_utf8(&hex).unwrap_or("00"), 16) {
                result.push(val as char);
            }
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

fn parse_query(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if query.is_empty() {
        return map;
    }
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(url_decode(k), url_decode(v));
        } else {
            map.insert(url_decode(pair), String::new());
        }
    }
    map
}

/// Extract values between <Tag>value</Tag> from XML.
fn extract_xml_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut results = Vec::new();
    let mut pos = 0;
    while let Some(start) = xml[pos..].find(&open) {
        let start = pos + start + open.len();
        if let Some(end) = xml[start..].find(&close) {
            results.push(xml[start..start + end].to_string());
            pos = start + end + close.len();
        } else {
            break;
        }
    }
    results
}

/// Extract <Tag><Key>k</Key><Value>v</Value></Tag> pairs from XML.
fn extract_xml_tag_pairs(xml: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let keys = extract_xml_values(xml, "Key");
    let values = extract_xml_values(xml, "Value");
    for (k, v) in keys.into_iter().zip(values.into_iter()) {
        results.push((k, v));
    }
    results
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("bytes=0-499", 1000), Some((0, 499)));
        assert_eq!(parse_range("bytes=500-999", 1000), Some((500, 999)));
        assert_eq!(parse_range("bytes=500-", 1000), Some((500, 999)));
        assert_eq!(parse_range("bytes=-200", 1000), Some((800, 999)));
        assert_eq!(parse_range("bytes=0-0", 1000), Some((0, 0)));
        assert_eq!(parse_range("bytes=1000-", 1000), None); // past end
    }

    #[test]
    fn test_xml_extract() {
        let xml = "<Delete><Object><Key>file1.txt</Key></Object><Object><Key>file2.txt</Key></Object></Delete>";
        let keys = extract_xml_values(xml, "Key");
        assert_eq!(keys, vec!["file1.txt", "file2.txt"]);
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let c = crc32(data);
        assert_eq!(format!("{:08x}", c), "0d4a1185");
    }

    #[test]
    fn test_canonical_query() {
        assert_eq!(canonical_query("b=2&a=1"), "a=1&b=2");
        assert_eq!(canonical_query(""), "");
    }

    #[test]
    fn test_parse_amz_date() {
        let ts = parse_amz_date("20260310T120000Z").unwrap();
        assert!(ts > 0);
    }
}
