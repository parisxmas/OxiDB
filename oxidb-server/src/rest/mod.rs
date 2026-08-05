//! REST API — JSON-over-HTTP interface for OxiDB document operations.
//!
//! Enabled via `OXIDB_HTTP_PORT`. Provides CRUD, aggregation, indexes, SQL,
//! and stored procedures over standard HTTP with JSON request/response bodies.
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET    | /api/ping | Health check |
//! | GET    | /metrics | Prometheus exposition (text format 0.0.4, public) |
//! | GET    | /api/collections | List collections |
//! | POST   | /api/collections | Create collection |
//! | DELETE | /api/collections/{name} | Drop collection |
//! | POST   | /api/{collection}/documents | Insert (doc or docs) |
//! | GET    | /api/{collection}/documents | Find (query in ?q=) |
//! | PATCH  | /api/{collection}/documents | Update |
//! | DELETE | /api/{collection}/documents | Delete |
//! | GET    | /api/{collection}/count | Count (?q=) |
//! | POST   | /api/{collection}/aggregate | Aggregation pipeline |
//! | GET    | /api/{collection}/indexes | List indexes |
//! | POST   | /api/{collection}/indexes | Create index |
//! | DELETE | /api/{collection}/indexes/{name} | Drop index |
//! | POST   | /api/sql | SQL engine query (requires `OXIDB_SQL=1`) |
//! | POST   | /api/procedures | Create procedure |
//! | GET    | /api/procedures | List procedures |
//! | POST   | /api/procedures/{name}/call | Call procedure |
//! | DELETE | /api/procedures/{name} | Delete procedure |

use std::collections::HashMap;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxidb::OxiDb;
use oxidb::query::FindOptions;
use serde_json::{Value, json};

use crate::auth;
use crate::jwt;
use crate::rules::{self, AuthContext, Operation};
use crate::s3::http::{HttpRequest, HttpResponse, parse_request_from_reader};

mod postgrest;
mod postgrest_sql;
mod postgrest_tsdb;
mod storage;

const POOL_SIZE: usize = 64;
const MAX_QUEUED: usize = 512;

struct RestState {
    db: Arc<OxiDb>,
    active: AtomicUsize,
    /// JWT secret. When `Some`, auth is enforced on all non-public endpoints.
    jwt_secret: Option<String>,
    /// Database registry (ADR-0012). `?db=<name>` targets a named database;
    /// `None` (or no param) serves the default database as before.
    db_manager: Option<Arc<oxidb::DatabaseManager>>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn start_rest_listener(
    addr: &str,
    db: Arc<OxiDb>,
    jwt_secret: Option<String>,
) -> std::thread::JoinHandle<()> {
    start_rest_listener_with_manager(addr, db, jwt_secret, None)
}

/// [`start_rest_listener`] with a database registry, enabling `?db=<name>`
/// targeting (ADR-0012).
pub fn start_rest_listener_with_manager(
    addr: &str,
    db: Arc<OxiDb>,
    jwt_secret: Option<String>,
    db_manager: Option<Arc<oxidb::DatabaseManager>>,
) -> std::thread::JoinHandle<()> {
    let listener = TcpListener::bind(addr).expect("failed to bind REST HTTP listener");

    if jwt_secret.is_some() {
        eprintln!("[rest] JWT authentication enabled");
    } else {
        eprintln!("[rest] WARNING: no OXIDB_JWT_SECRET — REST API is open (no auth)");
    }

    let state = Arc::new(RestState {
        db,
        active: AtomicUsize::new(0),
        jwt_secret,
        db_manager,
    });

    let (conn_tx, conn_rx) = std::sync::mpsc::sync_channel::<TcpStream>(MAX_QUEUED);
    let conn_rx = Arc::new(Mutex::new(conn_rx));

    for i in 0..POOL_SIZE {
        let rx = Arc::clone(&conn_rx);
        let state = Arc::clone(&state);
        std::thread::Builder::new()
            .name(format!("rest-worker-{i}"))
            .spawn(move || {
                loop {
                    let stream = match rx.lock().unwrap().recv() {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    state.active.fetch_add(1, Ordering::Relaxed);
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        handle_connection(stream, &state);
                    }));
                    state.active.fetch_sub(1, Ordering::Relaxed);
                    if let Err(e) = result {
                        let msg = if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        eprintln!("[rest] handler panicked: {msg}");
                    }
                }
            })
            .expect("failed to spawn rest worker");
    }
    eprintln!("[rest] thread pool: {POOL_SIZE} workers, queue depth {MAX_QUEUED}");

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    if conn_tx.try_send(s).is_err() {
                        eprintln!("[rest] connection rejected: queue full");
                    }
                }
                Err(e) => eprintln!("[rest] accept error: {e}"),
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------

fn handle_connection(mut stream: TcpStream, state: &RestState) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let read_stream = stream.try_clone().expect("failed to clone stream");
    let mut reader = BufReader::new(read_stream);

    loop {
        let req = match parse_request_from_reader(&mut reader, &stream) {
            Some(r) => r,
            None => return,
        };

        let wants_close = req
            .headers
            .get("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));

        let start = std::time::Instant::now();
        let resp = route_request(&req, state);
        if crate::gelf::enabled() {
            let ms = start.elapsed().as_millis().to_string();
            let status = resp.status.to_string();
            let level = match resp.status {
                s if s >= 500 => crate::gelf::GelfLevel::Error,
                s if s >= 400 => crate::gelf::GelfLevel::Warning,
                _ => crate::gelf::GelfLevel::Informational,
            };
            // The database the request targeted (`?db=<ref>` or a path-based
            // tenant segment) — what lets a per-project log view filter its
            // own traffic out of the shared sink. Logged as-given (ref or
            // slug); readers match either.
            let db = req
                .query
                .split('&')
                .find_map(|kv| kv.strip_prefix("db="))
                .map(|v| v.to_string())
                .or_else(|| {
                    let mut segs = req.path.split('/').filter(|s| !s.is_empty());
                    let first = segs.next()?;
                    matches!(segs.next(), Some("rest" | "api")).then(|| first.to_string())
                })
                .unwrap_or_default();
            // Resolved to the database's real name, not the segment as typed. A
            // project is addressable by ref or slug, and a log that records
            // whichever the caller used cannot be routed to that project's own
            // database — every record would fall back to the shared one.
            let db = state
                .db_manager
                .as_ref()
                .filter(|_| !db.is_empty())
                .and_then(|mgr| crate::tenant_auth::resolve_tenant(mgr, &db))
                .unwrap_or(db);
            // Who asked, and from where. Behind a proxy the socket peer is the
            // proxy, so this comes off the edge's headers — empty and free when
            // there is no edge.
            let who = req.client_meta();
            // Who was acting, when the request succeeded. A 2xx means the token
            // was verified on the way in, so the subject it claims is the
            // identity that was actually used — the project's anon key, its
            // service key, or a signed-in end user.
            let identity = (resp.status < 400)
                .then(|| {
                    let header = req
                        .headers
                        .get("authorization")
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    crate::jwt::extract_bearer(header).and_then(crate::jwt::peek_claims)
                })
                .flatten();
            let mut fields = vec![
                ("app", "oxidb-server"),
                ("method", req.method.as_str()),
                ("path", req.path.as_str()),
                ("status", status.as_str()),
                ("ms", ms.as_str()),
                ("db", db.as_str()),
            ];
            fields.extend(who.fields());
            if let Some((sub, role)) = identity.as_ref() {
                fields.push(("user", sub.as_str()));
                if !role.is_empty() {
                    fields.push(("role", role.as_str()));
                }
            }
            crate::gelf::log(level, &format!("{} {}", req.method, req.path), &fields);
        }
        resp.write_to_keepalive(&mut stream, !wants_close);

        if wants_close {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

fn route_request(req: &HttpRequest, state: &RestState) -> HttpResponse {
    crate::metrics::METRICS
        .http_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = req.path.trim_end_matches('/');
    let raw_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // CORS preflight
    if req.method == "OPTIONS" {
        return with_rest_cors(json_response(204, "No Content", json!(null)));
    }

    // ── Stable URL prefix (ADR-0003): strip a leading `/v1/`. Both `/v1/api/…`
    // and the legacy bare `/api/…` resolve to the same handlers.
    let mut segments: Vec<&str> = if raw_segments.first() == Some(&"v1") {
        raw_segments[1..].to_vec()
    } else {
        raw_segments.clone()
    };

    // ── Path-based tenant (ADR-0012): `/<tenant>/rest/v1/…` or `/<tenant>/api/…`
    // addresses a project by its ref OR slug. Resolve it to a database and route
    // the remaining path exactly like an untenanted request. `?db=` still works
    // and takes precedence.
    let mut path_db: Option<String> = None;
    {
        const TOP: &[&str] = &["api", "rest", "hello", "metrics", "health"];
        if segments.len() >= 2
            && !TOP.contains(&segments[0])
            && matches!(segments[1], "rest" | "api")
        {
            match &state.db_manager {
                Some(mgr) => match crate::tenant_auth::resolve_tenant(mgr, segments[0]) {
                    Some(db) => {
                        path_db = Some(db);
                        segments = segments[1..].to_vec();
                    }
                    None => {
                        return with_rest_cors(json_response(
                            404,
                            "Not Found",
                            json!({"error": "unknown tenant"}),
                        ));
                    }
                },
                None => {
                    return with_rest_cors(json_response(
                        400,
                        "Bad Request",
                        json!({"error": "path-based tenant routing is not available"}),
                    ));
                }
            }
        }
    }

    // ── Database targeting (ADR-0012): `?db=<name>` wins, else the path tenant;
    // neither keeps the default database exactly as before. A `?db=` value that
    // names a project by ref or slug is resolved to the project's database; a
    // plain (non-project) database name passes through unchanged.
    let db_name = match parse_query_string(&req.query)
        .get("db")
        .map(|v| url_decode(v))
    {
        Some(raw) => Some(
            state
                .db_manager
                .as_ref()
                .and_then(|mgr| crate::tenant_auth::resolve_tenant(mgr, &raw))
                .unwrap_or(raw),
        ),
        None => path_db,
    };
    // Reserved control-plane stores (the OxiBase `oxibase` metadata database)
    // are never served over the data plane — return the same "not found" as any
    // other unreachable database, without confirming it exists.
    if let Some(name) = &db_name
        && crate::tenant_auth::is_reserved_db(name)
    {
        return with_rest_cors(json_response(
            404,
            "Not Found",
            json!({"error": format!("database not found: {name}")}),
        ));
    }
    let scoped_state: RestState;
    let state = match &db_name {
        None => state,
        Some(name) => match &state.db_manager {
            None => {
                return with_rest_cors(json_response(
                    400,
                    "Bad Request",
                    json!({"error": "database targeting is not available"}),
                ));
            }
            Some(mgr) => match mgr.get_database(name) {
                Ok(db) => {
                    // Arm this tenant database with its OxiBase collection quota
                    // (owned by the control plane, read from the project row).
                    // The engine then rejects a new collection past the cap on
                    // every path; refreshing it per request reflects plan changes.
                    if let Some(limits) = crate::tenant_auth::project_limits(mgr, name) {
                        db.set_max_collections(limits.max_collections);
                        db.set_max_documents(limits.max_documents);
                        // Rate limit here, at the one point where a request is
                        // attributed to a tenant, so every surface on this
                        // listener (REST, /api, storage, SQL) is covered by the
                        // single check — and refused before any work is done.
                        if let Some(retry) =
                            crate::tenant_auth::rate_limit_hit(name, limits.max_rpm)
                        {
                            return with_rest_cors(
                                json_response(
                                    429,
                                    "Too Many Requests",
                                    json!({
                                        "error": "rate limit exceeded for this project",
                                        "limit": limits.max_rpm,
                                        "retry_after": retry,
                                    }),
                                )
                                .with_header("Retry-After", &retry.to_string()),
                            );
                        }
                    }
                    // `active` belongs to the listener's shared state; this
                    // per-request scope only redirects `db`.
                    scoped_state = RestState {
                        db,
                        active: AtomicUsize::new(0),
                        jwt_secret: state.jwt_secret.clone(),
                        db_manager: state.db_manager.clone(),
                    };
                    &scoped_state
                }
                Err(e) => {
                    return with_rest_cors(json_response(
                        404,
                        "Not Found",
                        json!({"error": e.to_string()}),
                    ));
                }
            },
        },
    };

    // ── OxiBase control plane (ADR-0020/0021) ───────────────────────────
    // The control plane now runs as a separate `oxibase` binary; the data
    // plane keeps only the `tenant_auth::project_secret` hook (consulted in the
    // JWT gate below) and no longer serves `/platform/v1` itself.

    // Top-level HELLO equivalent for REST: GET /v1/hello returns server info.
    // Same fields as the OxiWire HELLO so a REST-only client can discover
    // version + features without authenticating.
    if (req.method.as_str(), segments.as_slice()) == ("GET", &["hello"][..]) {
        return with_rest_cors(json_response(
            200,
            "OK",
            json!({
                "server": {
                    "name": "oxidb-server",
                    "version": crate::hello::SERVER_VERSION,
                    "stable_surface_version": crate::hello::STABLE_SURFACE_VERSION,
                    "rest_api_version": "v1",
                    "supported_wire_versions": crate::hello::SUPPORTED_WIRE_VERSIONS,
                }
            }),
        ));
    }

    // ── Prometheus exposition (always public — standard practice for
    // scrapers; bind OXIDB_HTTP_PORT privately if the API is private).
    // Serves the DEFAULT database's gauges regardless of `?db=`.
    if (req.method.as_str(), segments.as_slice()) == ("GET", &["metrics"][..]) {
        let body = crate::metrics::render_prometheus(&state.db);
        return HttpResponse {
            status: 200,
            status_text: "OK",
            content_type: "text/plain; version=0.0.4; charset=utf-8".to_string(),
            headers: Vec::new(),
            body: body.into_bytes(),
            content_length_override: None,
        };
    }

    // ── Auth endpoints (always public) ──────────────────────────────────
    match (req.method.as_str(), segments.as_slice()) {
        ("POST", ["api", "auth", "signup"]) => {
            return with_rest_cors(handle_auth_signup(req, state));
        }
        ("POST", ["api", "auth", "login"]) => return with_rest_cors(handle_auth_login(req, state)),
        ("GET", ["api", "auth", "verify"]) => {
            return with_rest_cors(handle_auth_verify(req, state));
        }
        ("GET", ["api", "ping"]) => {
            return with_rest_cors(json_response(
                200,
                "OK",
                json!({"status": "ok", "data": "pong"}),
            ));
        }
        _ => {}
    }

    // ── JWT enforcement + extract auth context ─────────────────────
    // `enforced_role` is `Some` only when auth is enabled (a JWT secret is
    // configured). When it is `Some`, we gate every protected endpoint on the
    // caller's role below — without this, any valid token (even `read`) could
    // drop collections, rewrite security rules, or create stored procedures.
    //
    // Per-database secret (ADR-0020): a request targeting an OxiBase project
    // (`?db=<ref>`) is verified with that project's own JWT secret; everything
    // else uses the global `OXIDB_JWT_SECRET`.
    // An OxiBase project is verified with its ES256 public key alone — no seal
    // key, no shared secret (the multi-node property). Everything else (native
    // API, non-project databases) uses the global `OXIDB_JWT_SECRET`.
    let project_pubkey: Option<Vec<u8>> = match (&db_name, &state.db_manager) {
        (Some(name), Some(mgr)) if crate::tenant_auth::enabled() => {
            crate::tenant_auth::project_pubkey(mgr, name)
        }
        _ => None,
    };
    let effective_secret: Option<String> = state.jwt_secret.clone();
    let (auth_ctx, enforced_role) = if project_pubkey.is_some() || effective_secret.is_some() {
        let auth_header = req
            .headers
            .get("authorization")
            .map(|s| s.as_str())
            .unwrap_or("");
        if let Some(token) = jwt::extract_bearer(auth_header) {
            let verified = if let Some(pubkey) = &project_pubkey {
                jwt::verify_es256(token, pubkey)
            } else {
                jwt::verify(token, effective_secret.as_deref().unwrap_or(""))
            };
            match verified {
                Ok(claims) => {
                    let role = auth::Role::from_str(&claims.role).unwrap_or(auth::Role::Read);
                    (
                        AuthContext::from_claims(&claims.sub, &claims.role),
                        Some(role),
                    )
                }
                Err(e) => {
                    return with_rest_cors(json_response(401, "Unauthorized", json!({"error": e})));
                }
            }
        } else {
            return with_rest_cors(json_response(
                401,
                "Unauthorized",
                json!({"error": "missing Authorization: Bearer <token>"}),
            ));
        }
    } else {
        (AuthContext::anonymous(), None)
    };

    // ── Authorization (role) gate ─────────────────────────────────────
    // The schema profile matters here: `/rest/v1/{x}` is one URL over three
    // engines, and only the document one has per-row rules to fall back on.
    let tsdb_profile = req
        .headers
        .get("accept-profile")
        .or_else(|| req.headers.get("content-profile"))
        .map(|s| s.as_str())
        == Some("tsdb");
    // Likewise for the SQL engine: `/rest/v1/{x}` writes are let through so the
    // document rules can adjudicate them, and a SQL table has no rules either.
    let sql_table_target = !tsdb_profile
        && matches!(req.method.as_str(), "POST" | "PATCH" | "DELETE")
        && match segments.as_slice() {
            ["rest", "v1", table] => crate::sql_bridge::sql_table_exists(
                db_name
                    .as_deref()
                    .unwrap_or(oxidb::database_manager::DEFAULT_DATABASE),
                table,
            ),
            _ => false,
        };
    if let Some(role) = enforced_role
        && !rest_permitted(
            role,
            req.method.as_str(),
            segments.as_slice(),
            tsdb_profile || sql_table_target,
        )
    {
        return with_rest_cors(json_response(
            403,
            "Forbidden",
            json!({"error": "insufficient privileges for this operation"}),
        ));
    }

    // ── SQL engine (ADR-0010) ─────────────────────────────────────────
    // Handled outside the generic match so the dynamic error message from
    // the SQL engine (syntax errors, etc.) reaches the client. Read-role
    // callers are restricted to SELECT statements by the SQL bridge.
    if let ("POST", ["api", "sql"]) = (req.method.as_str(), segments.as_slice()) {
        let readonly = enforced_role == Some(auth::Role::Read);
        return with_rest_cors(handle_sql_endpoint_gated(
            req,
            readonly,
            db_name.as_deref(),
            state,
            enforced_role,
            &auth_ctx,
        ));
    }

    // ── On-demand backup download (`POST /api/backup`) ────────────────
    // Runs the document engine's consistent backup for the TARGET database
    // (`?db=<ref>` — the whole point for OxiBase projects) into a temp file,
    // streams the tar back, and removes the file. Stateless: nothing to
    // retain or expire server-side. Admin-only (the service_role key).
    if let ("POST", ["api", "backup"]) = (req.method.as_str(), segments.as_slice()) {
        let name = db_name.as_deref().unwrap_or("default");
        let tmp = std::env::temp_dir().join(format!(
            "oxidb-backup-{}-{}.tar.gz",
            name.replace(['/', '\\', '.'], "_"),
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp); // stale leftover from a crash
        let result = state.db.backup(&tmp);
        let resp = match result {
            Ok(info) => match std::fs::read(&tmp) {
                Ok(bytes) => HttpResponse {
                    status: 200,
                    status_text: "OK",
                    content_type: "application/gzip".into(),
                    headers: vec![
                        (
                            "Content-Disposition".to_string(),
                            format!("attachment; filename=\"{name}-backup.tar.gz\""),
                        ),
                        (
                            "X-Backup-Collections".to_string(),
                            info.collections.to_string(),
                        ),
                    ],
                    body: bytes,
                    content_length_override: None,
                },
                Err(e) => json_response(
                    500,
                    "Internal Server Error",
                    json!({"error": e.to_string()}),
                ),
            },
            Err(e) => json_response(
                500,
                "Internal Server Error",
                json!({"error": e.to_string()}),
            ),
        };
        let _ = std::fs::remove_file(&tmp);
        return with_rest_cors(resp);
    }

    // ── Per-project file storage (`/api/storage/…`) ───────────────────
    // Handled outside the generic match: downloads return raw bytes with the
    // stored Content-Type, which the JSON-only match below cannot express.
    if segments.len() >= 2 && segments[0] == "api" && segments[1] == "storage" {
        // Full-text search over stored *file contents* (the blob FTS index:
        // HTML, XML, JSON, PDF, DOCX, XLSX, and OCR'd images when built with
        // that feature). Matched here rather than in `storage::handle` because
        // the rule check needs the caller, and ahead of the delegate because
        // `POST /api/storage/{x}` already means "create bucket x" — `_search`
        // is therefore not a usable bucket name on this surface, which matches
        // the `_`-prefix convention everywhere else.
        if req.method == "POST" && segments.len() == 3 && segments[2] == "_search" {
            let result = handle_storage_search(req, state, &auth_ctx);
            return with_rest_cors(match result {
                Ok(v) => json_response(200, "OK", v),
                Err((status, msg)) => {
                    let text = match status {
                        400 => "Bad Request",
                        403 => "Forbidden",
                        404 => "Not Found",
                        _ => "Internal Server Error",
                    };
                    json_response(status, text, json!({ "error": msg }))
                }
            });
        }

        // A bucket is a named object like any other, so a rule on its name
        // decides who may read it — the only way to keep stored files private
        // from a key that ships in a browser.
        if (req.method == "GET" || req.method == "HEAD")
            && let Some(bucket) = segments.get(2)
            && let Err((status, msg)) = read_allowed(state, bucket, &auth_ctx)
        {
            return with_rest_cors(json_response(status, "Forbidden", json!({ "error": msg })));
        }
        return with_rest_cors(storage::handle(
            req,
            state,
            &segments[2..],
            db_name.as_deref(),
        ));
    }

    // ── PostgREST-compatible surface (ADR-0019): /rest/v1/{table} ──────
    // Auto-generated REST over the document engine — filters, select, order,
    // and pagination live in the URL. Each request still runs through the
    // security-rules layer (check_access) inside these handlers, OxiDB's
    // row-level-security analog; role gating already happened via
    // `rest_permitted` above. A "table" is a document collection.
    //
    // Phase 2b: when SQL is enabled and `{table}` names a SQL table, the SAME
    // grammar is served by the SQL engine instead (parameterized SQL). The
    // architecture guarantees a collection and a SQL table never share a name,
    // so this dispatch is unambiguous; SQL-off or a non-SQL name keeps the
    // document path byte-for-byte.
    {
        let sql_db = db_name
            .as_deref()
            .unwrap_or(oxidb::database_manager::DEFAULT_DATABASE);
        // PostgREST schema selection (`Accept-Profile`/`Content-Profile`, what
        // postgrest-js `.schema('tsdb')` emits) picks the time-series engine.
        // The default (no profile) keeps the SQL-if-table-exists-else-document
        // dispatch below. A profile check beats existence-routing for TSDB,
        // whose measurements only exist after the first write.
        let profile = req
            .headers
            .get("accept-profile")
            .or_else(|| req.headers.get("content-profile"))
            .map(|s| s.as_str());
        let is_tsdb = profile == Some("tsdb");
        match (req.method.as_str(), segments.as_slice()) {
            ("GET", ["rest", "v1", m]) if is_tsdb => {
                if let Err((status, msg)) = read_allowed(state, m, &auth_ctx) {
                    return with_rest_cors(json_response(
                        status,
                        "Forbidden",
                        json!({ "error": msg }),
                    ));
                }
                return with_rest_cors(postgrest_tsdb::handle_get(sql_db, m, req));
            }
            ("POST", ["rest", "v1", m]) if is_tsdb => {
                return with_rest_cors(postgrest_tsdb::handle_post(sql_db, m, req));
            }
            ("PATCH" | "DELETE", ["rest", "v1", _]) if is_tsdb => {
                return with_rest_cors(postgrest_tsdb::handle_unsupported());
            }
            ("GET", ["rest", "v1", table]) => {
                return with_rest_cors(if crate::sql_bridge::sql_table_exists(sql_db, table) {
                    // The document path applies rules inside its handler; the
                    // SQL path has none of its own, so the same rule store is
                    // consulted here by table name.
                    if let Err((status, msg)) = read_allowed(state, table, &auth_ctx) {
                        json_response(status, "Forbidden", json!({ "error": msg }))
                    } else {
                        postgrest_sql::handle_get(sql_db, table, req)
                    }
                } else {
                    postgrest::handle_get(table, req, state, &auth_ctx)
                });
            }
            ("POST", ["rest", "v1", table]) => {
                return with_rest_cors(if crate::sql_bridge::sql_table_exists(sql_db, table) {
                    postgrest_sql::handle_post(sql_db, table, req)
                } else {
                    postgrest::handle_post(table, req, state, &auth_ctx)
                });
            }
            ("PATCH", ["rest", "v1", table]) => {
                return with_rest_cors(if crate::sql_bridge::sql_table_exists(sql_db, table) {
                    postgrest_sql::handle_patch(sql_db, table, req)
                } else {
                    postgrest::handle_patch(table, req, state, &auth_ctx)
                });
            }
            ("DELETE", ["rest", "v1", table]) => {
                return with_rest_cors(if crate::sql_bridge::sql_table_exists(sql_db, table) {
                    postgrest_sql::handle_delete(sql_db, table, req)
                } else {
                    postgrest::handle_delete(table, req, state, &auth_ctx)
                });
            }
            _ => {}
        }
    }

    // ── Protected endpoints ──────────────────────────────────────────
    let result = match (req.method.as_str(), segments.as_slice()) {
        // Health (also available above without auth, but keep for pattern match)
        ("GET", ["api", "ping"]) => Ok(json!({"status": "ok", "data": "pong"})),

        // Collections
        ("GET", ["api", "collections"]) => handle_list_collections(state),
        ("POST", ["api", "collections"]) => handle_create_collection(req, state),
        ("DELETE", ["api", "collections", name]) => handle_drop_collection(name, state),

        // Documents (with security rules enforcement)
        ("POST", ["api", col, "documents"]) => handle_insert_with_rules(col, req, state, &auth_ctx),
        ("GET", ["api", col, "documents"]) => handle_find_with_rules(col, req, state, &auth_ctx),
        ("PATCH", ["api", col, "documents"]) => {
            handle_update_with_rules(col, req, state, &auth_ctx)
        }
        ("DELETE", ["api", col, "documents"]) => {
            handle_delete_with_rules(col, req, state, &auth_ctx)
        }

        // Count
        ("GET", ["api", col, "count"]) => handle_count_with_rules(col, req, state, &auth_ctx),

        // Aggregation
        ("POST", ["api", col, "aggregate"]) => handle_aggregate(col, req, state, &auth_ctx),

        // Full-text search over a collection (BM25). The engine has had this all
        // along; it was reachable only over the wire, so no OxiBase project
        // could use it.
        ("POST", ["api", col, "text_search"]) => handle_text_search(col, req, state, &auth_ctx),
        ("POST", ["api", col, "text_index"]) => handle_create_text_index(col, req, state),

        // Indexes
        ("GET", ["api", col, "indexes"]) => handle_list_indexes(col, state),
        ("POST", ["api", col, "indexes"]) => handle_create_index(col, req, state),
        ("DELETE", ["api", col, "indexes", name]) => handle_drop_index(col, name, state),

        // Procedures
        ("GET", ["api", "procedures"]) => handle_list_procedures(state),
        ("POST", ["api", "procedures"]) => handle_create_procedure(req, state),
        ("POST", ["api", "procedures", name, "call"]) => handle_call_procedure(name, req, state),
        ("DELETE", ["api", "procedures", name]) => handle_delete_procedure(name, state),

        // Security rules
        ("POST", ["api", "rules", col]) => handle_set_rules(col, req, state),
        ("GET", ["api", "rules", col]) => handle_get_rules(col, state),
        ("DELETE", ["api", "rules", col]) => handle_delete_rules(col, state),

        // Retention policies
        ("POST", ["api", "retention", col]) => handle_set_retention(col, req, state),
        ("GET", ["api", "retention", col]) => handle_get_retention(col, state),
        ("DELETE", ["api", "retention", col]) => handle_delete_retention(col, state),
        ("GET", ["api", "retention"]) => handle_list_retentions(state),

        // Alerts
        ("POST", ["api", "alerts"]) => handle_create_alert(req, state),
        ("GET", ["api", "alerts"]) => handle_list_alerts(state),
        ("GET", ["api", "alerts", name]) => handle_get_alert(name, state),
        ("DELETE", ["api", "alerts", name]) => handle_delete_alert(name, state),
        ("POST", ["api", "alerts", name, "test"]) => handle_test_alert(name, state),
        ("GET", ["api", "alert-history"]) => handle_alert_history(state),

        _ => Err((404, "not found")),
    };

    match result {
        Ok(data) => with_rest_cors(json_response(200, "OK", data)),
        Err((status, msg)) => {
            let status_text = match status {
                400 => "Bad Request",
                404 => "Not Found",
                405 => "Method Not Allowed",
                409 => "Conflict",
                _ => "Internal Server Error",
            };
            with_rest_cors(json_response(status, status_text, json!({"error": msg})))
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn handle_list_collections(state: &RestState) -> Result<Value, (u16, &'static str)> {
    let cols = state.db.list_collections();
    Ok(json!({"collections": cols}))
}

fn handle_create_collection(
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let name = body["name"].as_str().ok_or((400, "missing 'name'"))?;
    // Optional per-collection storage options (disk-first, compression,
    // compaction policy). Omitted fields fall back to server defaults.
    match body.get("options") {
        Some(opts_val) if !opts_val.is_null() => {
            let opts: oxidb::StorageOptions =
                serde_json::from_value(opts_val.clone()).map_err(|_| (400, "invalid 'options'"))?;
            state
                .db
                .create_collection_with_options(name, opts)
                .map_err(|_| (409, "collection already exists"))?;
        }
        _ => {
            state
                .db
                .create_collection(name)
                .map_err(|_| (409, "collection already exists"))?;
        }
    }
    Ok(json!({"created": name}))
}

fn handle_drop_collection(name: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    state
        .db
        .drop_collection(name)
        .map_err(|_| (404, "collection not found"))?;
    Ok(json!({"dropped": name}))
}

fn handle_find(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let query = query_param_json(&req.query, "q").unwrap_or(json!({}));
    let skip = query_param_u64(&req.query, "skip");
    let limit = query_param_u64(&req.query, "limit");

    // Build sort from ?sort={"field":1} query param
    let sort_parsed = query_param_json(&req.query, "sort").and_then(|v| {
        v.as_object().map(|obj| {
            obj.iter()
                .map(|(k, v)| {
                    let order = if v.as_i64().unwrap_or(1) < 0 {
                        oxidb::query::SortOrder::Desc
                    } else {
                        oxidb::query::SortOrder::Asc
                    };
                    (k.clone(), order)
                })
                .collect::<Vec<_>>()
        })
    });

    let opts = FindOptions {
        sort: sort_parsed,
        skip,
        limit,
    };

    let docs = state
        .db
        .find_with_options(col, &query, &opts)
        .map_err(db_err)?;
    Ok(json!(docs))
}

fn handle_count(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let query = query_param_json(&req.query, "q").unwrap_or(json!({}));
    let n = state.db.count(col, &query).map_err(db_err)?;
    Ok(json!({"count": n}))
}

/// A refusal from the rules layer, as a status for the native `/api` surface.
/// The PostgREST surface carries `Retry-After`; here the shape is a fixed
/// message, so the status has to do the talking.
fn denied_status(e: rules::Denied) -> (u16, &'static str) {
    if e.retry_after.is_some() {
        (429, "rate limit exceeded")
    } else {
        (403, "access denied")
    }
}

/// May this caller read the named object?
///
/// The rules store is keyed by name across every engine — a collection, a SQL
/// table, a time-series measurement, a storage bucket. Only the document
/// engine can honour a *row-level* rule, so for the others a rule that filters
/// per row is treated as a refusal rather than quietly ignored. No rule means
/// readable, as it always has for documents.
fn read_allowed(
    state: &RestState,
    name: &str,
    auth: &AuthContext,
) -> Result<(), (u16, &'static str)> {
    match rules::read_access(&state.db, name, auth) {
        rules::ReadAccess::All => Ok(()),
        rules::ReadAccess::None => Err((403, "access denied: read on this object is not allowed")),
        rules::ReadAccess::Filter(_) => Err((
            403,
            "access denied: a row-level read rule cannot be applied to this engine, so the read is refused",
        )),
    }
}

fn handle_aggregate(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    // A pipeline reads the collection, so the read rule governs it — and this
    // is the one read path that cannot simply filter its output, because
    // $group and friends turn rows into something else. A rule that admits
    // only some rows therefore cannot be honoured here at all, and the request
    // is refused rather than answered from rows the caller may not see.
    //
    // Without this, `$group` over a collection whose read rule is
    // `auth.username == doc.owner` returned every owner's values to anyone
    // holding the public anon key.
    match rules::read_access(&state.db, col, auth) {
        rules::ReadAccess::All => {}
        rules::ReadAccess::None => {
            return Err((403, "access denied: read on this collection is not allowed"));
        }
        rules::ReadAccess::Filter(_) => {
            return Err((
                403,
                "access denied: this collection has a row-level read rule, so it cannot be aggregated with this key",
            ));
        }
    }
    let body = parse_json_body(req)?;
    let pipeline = body.get("pipeline").ok_or((400, "missing 'pipeline'"))?;
    let results = state.db.aggregate(col, pipeline).map_err(db_err)?;
    Ok(json!(results))
}

/// `POST /api/{col}/text_search` — ranked full-text search (BM25).
///
/// Body: `{ "query": "...", "limit": 20, "highlight": true | {snippet_chars, max_snippets} }`.
///
/// Unlike `aggregate`, this returns whole documents, so a row-level read rule
/// *can* be honoured: the matches are filtered the way `find` filters them,
/// rather than the request being refused. A filtered search can therefore return
/// fewer than `limit` rows — the alternative would be telling the caller how many
/// matches it is not allowed to see.
fn handle_text_search(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    let access = rules::read_access(&state.db, col, auth);
    if matches!(access, rules::ReadAccess::None) {
        return Err((403, "access denied: read on this collection is not allowed"));
    }

    let body = parse_json_body(req)?;
    let query = body
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or((400, "missing 'query'"))?;
    let limit = body
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(20)
        .min(500) as usize;

    // Highlighting trims and marks up the matched text, which costs more than
    // scoring it — so it happens only when asked for.
    let highlight = body.get("highlight").and_then(|h| {
        if h.as_bool() == Some(true) {
            Some((80usize, 3usize))
        } else {
            h.as_object().map(|o| {
                (
                    o.get("snippet_chars")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(80) as usize,
                    o.get("max_snippets").and_then(|v| v.as_u64()).unwrap_or(3) as usize,
                )
            })
        }
    });

    // "No text index yet" is the caller's problem to fix, not a server fault:
    // say so with a 400 and the remedy, instead of the generic 500 that
    // `db_err` would give an InvalidQuery.
    let searched = match highlight {
        Some((chars, max)) => state
            .db
            .text_search_highlighted(col, query, limit, chars, max),
        None => state.db.text_search(col, query, limit),
    };
    let hits = searched.map_err(|e| match e {
        oxidb::Error::InvalidQuery(ref m) if m.contains("no text index") => (
            400,
            "no text index on this collection — build one with POST /api/{collection}/text_index",
        ),
        other => db_err(other),
    })?;

    let mut out = json!(hits);
    if let rules::ReadAccess::Filter(expr) = access
        && let Value::Array(arr) = &mut out
    {
        arr.retain(|d| rules::row_visible(&expr, auth, d));
    }
    Ok(out)
}

/// `POST /api/storage/_search` — full-text search over stored file contents.
///
/// Body: `{ "bucket": "docs", "query": "...", "limit": 20, "highlight": true }`.
/// Hits are `{bucket, key, score}` (plus `highlights` when asked), not documents:
/// the index holds text extracted from the files themselves.
///
/// **The bucket is required.** A bucket's read rule is per bucket, so a search
/// across all of them could report keys and snippets out of a bucket this key may
/// not read. Searching one bucket at a time makes the check exactly the one the
/// download path already performs.
fn handle_storage_search(
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let bucket = body.get("bucket").and_then(|v| v.as_str()).ok_or((
        400,
        "missing 'bucket' (search is per bucket, so its read rule applies)",
    ))?;
    let query = body
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or((400, "missing 'query'"))?;
    let limit = body
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(20)
        .min(200) as usize;

    // The same gate a download goes through — including refusing a row-level
    // rule, which cannot mean anything for a file.
    read_allowed(state, bucket, auth)?;

    // Highlighting re-extracts the text of every hit (a PDF is not cheap), so it
    // happens only when asked for.
    let highlight = body.get("highlight").and_then(|h| {
        if h.as_bool() == Some(true) {
            Some((80usize, 3usize))
        } else {
            h.as_object().map(|o| {
                (
                    o.get("snippet_chars")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(80) as usize,
                    o.get("max_snippets").and_then(|v| v.as_u64()).unwrap_or(3) as usize,
                )
            })
        }
    });

    let hits = match highlight {
        Some((chars, max)) => state
            .db
            .search_highlighted(Some(bucket), query, limit, chars, max)
            .map_err(db_err)?,
        None => state
            .db
            .search(Some(bucket), query, limit)
            .map_err(db_err)?,
    };
    Ok(json!(hits))
}

/// `POST /api/{col}/text_index` — build the BM25 index over the given fields.
///
/// Body: `{ "fields": ["title", "body"] }`. Indexing is schema work, so
/// `rest_permitted` keeps this to a ReadWrite/service_role key — otherwise a
/// published browser key could index whatever it liked.
fn handle_create_text_index(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let fields: Vec<String> = body
        .get("fields")
        .and_then(|v| v.as_array())
        .ok_or((400, "missing 'fields' array"))?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    if fields.is_empty() {
        return Err((400, "'fields' must list at least one field"));
    }
    state
        .db
        .create_text_index(col, fields.clone())
        .map_err(db_err)?;
    Ok(json!({ "created": fields, "type": "text" }))
}

fn handle_list_indexes(col: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    let indexes = state.db.list_indexes(col).map_err(db_err)?;
    Ok(json!(indexes))
}

fn handle_create_index(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let idx_type = body.get("type").and_then(|v| v.as_str()).unwrap_or("field");

    match idx_type {
        "unique" => {
            let field = body["field"].as_str().ok_or((400, "missing 'field'"))?;
            state.db.create_unique_index(col, field).map_err(db_err)?;
            Ok(json!({"created": field, "type": "unique"}))
        }
        "composite" => {
            let fields: Vec<String> = body["fields"]
                .as_array()
                .ok_or((400, "missing 'fields' array"))?
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            let name = state
                .db
                .create_composite_index(col, fields)
                .map_err(db_err)?;
            Ok(json!({"created": name, "type": "composite"}))
        }
        "ttl" => {
            let field = body["field"].as_str().ok_or((400, "missing 'field'"))?;
            let expire = body["expireAfterSeconds"]
                .as_u64()
                .ok_or((400, "missing 'expireAfterSeconds'"))?;
            state
                .db
                .create_ttl_index(col, field, expire)
                .map_err(db_err)?;
            Ok(json!({"created": format!("{field}_ttl"), "type": "ttl"}))
        }
        _ => {
            let field = body["field"].as_str().ok_or((400, "missing 'field'"))?;
            state.db.create_index(col, field).map_err(db_err)?;
            Ok(json!({"created": field, "type": "field"}))
        }
    }
}

fn handle_drop_index(
    col: &str,
    name: &str,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    state.db.drop_index(col, name).map_err(db_err)?;
    Ok(json!({"dropped": name}))
}

/// `POST /api/sql` — the standalone SQL engine (ADR-0010). Body:
/// `{"sql": "...", "params": [...]}`; `params` optionally binds `?`/`$N`
/// placeholders. Responds `{"results": [...]}` (one entry per statement) or
/// 400 `{"error": "..."}` with the engine's message. Requires `OXIDB_SQL=1`
/// on the server; the engine's data is entirely separate from collections.
/// `/api/sql` with the read rules applied.
///
/// SQL has no per-row policy, so the gate is per *table*: an untrusted caller
/// may only run a statement whose every referenced table it is allowed to
/// read. A statement that cannot be parsed is refused for those callers rather
/// than passed through unexamined — the engine would reject it anyway, and
/// guessing is not a safe basis for an authorization decision.
#[allow(clippy::too_many_arguments)]
fn handle_sql_endpoint_gated(
    req: &HttpRequest,
    readonly: bool,
    db_name: Option<&str>,
    state: &RestState,
    enforced_role: Option<auth::Role>,
    auth: &AuthContext,
) -> HttpResponse {
    let untrusted = matches!(
        enforced_role,
        Some(auth::Role::Read) | Some(auth::Role::Authenticated)
    );
    if untrusted {
        let sql = parse_json_body(req)
            .ok()
            .and_then(|b| b.get("sql").and_then(|v| v.as_str()).map(String::from))
            .unwrap_or_default();
        match oxidb_sql::referenced_tables(&sql) {
            Ok(tables) => {
                for table in tables {
                    if let Err((status, msg)) = read_allowed(state, &table, auth) {
                        return json_response(status, "Forbidden", json!({ "error": msg }));
                    }
                }
            }
            Err(_) => {
                return json_response(
                    400,
                    "Bad Request",
                    json!({ "error": "could not parse the statement" }),
                );
            }
        }
    }
    handle_sql_endpoint(req, readonly, db_name, state, enforced_role)
}

fn handle_sql_endpoint(
    req: &HttpRequest,
    readonly: bool,
    db_name: Option<&str>,
    state: &RestState,
    enforced_role: Option<auth::Role>,
) -> HttpResponse {
    let body = match parse_json_body(req) {
        Ok(b) => b,
        Err((status, msg)) => {
            return json_response(status, "Bad Request", json!({"error": msg}));
        }
    };
    let Some(sql) = body.get("sql").and_then(|v| v.as_str()) else {
        return json_response(400, "Bad Request", json!({"error": "missing 'sql'"}));
    };

    // ── SQL-text user management is wire-protocol-only: REST authenticates
    // with JWT against `_auth_users`, not the SCRAM user store these
    // statements manage. Reject clearly instead of confusing engine errors.
    if oxidb_sql::parse_user_statement(sql).is_some() {
        return json_response(
            400,
            "Bad Request",
            json!({"error": "user management statements are not available over REST; use the wire protocol"}),
        );
    }

    // ── SQL-text database DDL (ADR-0012). REST is stateless, so `USE` has
    // no session to act on — `?db=<name>` is the REST equivalent.
    if let Some(stmt) = oxidb_sql::parse_database_statement(sql) {
        use oxidb_sql::DatabaseStatement as Ds;
        let Some(mgr) = &state.db_manager else {
            return json_response(
                400,
                "Bad Request",
                json!({"error": "database management is not available"}),
            );
        };
        if matches!(stmt, Ds::Create { .. } | Ds::Drop { .. })
            && enforced_role.is_some()
            && enforced_role != Some(auth::Role::Admin)
        {
            return json_response(
                403,
                "Forbidden",
                json!({"error": "creating or dropping databases requires the admin role"}),
            );
        }
        return match stmt {
            Ds::Create {
                name,
                if_not_exists,
            } => match mgr.create_database(&name) {
                Ok(()) => json_response(200, "OK", json!({"results": [{ "ddl": true }]})),
                Err(oxidb::Error::DatabaseAlreadyExists(_)) if if_not_exists => {
                    json_response(200, "OK", json!({"results": [{ "ddl": true }]}))
                }
                Err(e) => json_response(400, "Bad Request", json!({"error": e.to_string()})),
            },
            Ds::Drop { name, if_exists } => match mgr.drop_database(&name) {
                Ok(()) => {
                    crate::sql_bridge::forget_database(&name);
                    json_response(200, "OK", json!({"results": [{ "ddl": true }]}))
                }
                Err(oxidb::Error::DatabaseNotFound(_)) if if_exists => {
                    json_response(200, "OK", json!({"results": [{ "ddl": true }]}))
                }
                Err(e) => json_response(400, "Bad Request", json!({"error": e.to_string()})),
            },
            Ds::Show => {
                let rows: Vec<Value> = mgr
                    .list_databases()
                    .into_iter()
                    .map(|n| json!([n]))
                    .collect();
                json_response(
                    200,
                    "OK",
                    json!({"results": [{ "columns": ["database"], "rows": rows }]}),
                )
            }
            Ds::Use { .. } => json_response(
                400,
                "Bad Request",
                json!({"error": "USE has no session over REST; target a database with ?db=<name>"}),
            ),
        };
    }

    let db = db_name.unwrap_or(oxidb::database_manager::DEFAULT_DATABASE);
    // Arm the tenant's SQL engine with its OxiBase table quota before executing,
    // so a CREATE TABLE past the cap is rejected. Non-tenant databases get no
    // limit (project_limits → None).
    if let (Some(name), Some(mgr)) = (db_name, &state.db_manager)
        && let Some(limits) = crate::tenant_auth::project_limits(mgr, name)
    {
        crate::sql_bridge::set_table_limit(name, limits.max_tables);
    }
    match crate::sql_bridge::execute_json_in(db, sql, body.get("params"), readonly) {
        Ok(results) => json_response(200, "OK", json!({"results": results})),
        Err(msg) => json_response(400, "Bad Request", json!({"error": msg})),
    }
}

fn handle_list_procedures(state: &RestState) -> Result<Value, (u16, &'static str)> {
    let procs = state.db.list_procedures().map_err(db_err)?;
    Ok(json!(procs))
}

fn handle_create_procedure(
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    if let Some(script) = body.get("script").and_then(|v| v.as_str()) {
        let compiled = oxidb::oxiscript::compile(script)
            .map_err(|e| (400, Box::leak(e.into_boxed_str()) as &'static str))?;
        let name = compiled["name"].as_str().unwrap_or("unknown").to_string();
        state.db.create_procedure(&name, compiled).map_err(db_err)?;
        Ok(json!({"created": name}))
    } else {
        let name = body["name"]
            .as_str()
            .ok_or((400, "missing 'name' or 'script'"))?;
        state
            .db
            .create_procedure(name, body.clone())
            .map_err(db_err)?;
        Ok(json!({"created": name}))
    }
}

fn handle_call_procedure(
    name: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let params = if req.body.is_empty() {
        json!({})
    } else {
        parse_json_body(req)?
    };
    let result = state.db.call_procedure(name, params).map_err(db_err)?;
    Ok(result)
}

fn handle_delete_procedure(name: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    state.db.delete_procedure(name).map_err(db_err)?;
    Ok(json!({"deleted": name}))
}

// ---------------------------------------------------------------------------
// CRUD with security rules
// ---------------------------------------------------------------------------

fn handle_insert_with_rules(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    if let Some(doc) = body.get("doc") {
        rules::check_access(&state.db, col, Operation::Create, auth, None, Some(doc))
            .map_err(denied_status)?;
        let id = state.db.insert(col, doc.clone()).map_err(db_err)?;
        Ok(json!({"id": id}))
    } else if let Some(docs) = body.get("docs").and_then(|v| v.as_array()) {
        // Check rules on first doc as representative (batch check)
        if let Some(first) = docs.first() {
            rules::check_access(&state.db, col, Operation::Create, auth, None, Some(first))
                .map_err(denied_status)?;
        }
        let ids = state.db.insert_many(col, docs.clone()).map_err(db_err)?;
        Ok(json!({"ids": ids}))
    } else {
        Err((400, "missing 'doc' or 'docs'"))
    }
}

fn handle_find_with_rules(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    // Read access, with per-row filtering when the rule references the row.
    match rules::read_access(&state.db, col, auth) {
        rules::ReadAccess::None => Err((403, "access denied")),
        rules::ReadAccess::All => handle_find(col, req, state),
        rules::ReadAccess::Filter(expr) => {
            let mut v = handle_find(col, req, state)?;
            if let Value::Array(arr) = &mut v {
                arr.retain(|d| rules::row_visible(&expr, auth, d));
            }
            Ok(v)
        }
    }
}

/// `GET /api/{col}/count` under the read rule.
///
/// Counting was the one read path that never consulted the rules, which made
/// it a **disclosure oracle**: a collection with `read: false` answered
/// `find` with 403 and `count` with the number anyway, and because the count
/// takes an arbitrary `?q=` filter, a caller could ask
/// `count?q={"email":"someone@example.com"}` — or binary-search a numeric
/// field with `$gte` — and read out values one bit at a time without ever
/// being allowed to see a document. Same class as the `aggregate` gap closed
/// in 0.39.21; this path was missed in that pass.
///
/// A row-level rule is *filtered* rather than refused, matching `find`: the
/// answer is the number of rows this caller may see. That is honest and leaks
/// nothing — unlike `aggregate`, a count can be computed over the visible
/// subset, so there is no reason to refuse it.
fn handle_count_with_rules(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    match rules::read_access(&state.db, col, auth) {
        rules::ReadAccess::None => Err((403, "access denied")),
        rules::ReadAccess::All => handle_count(col, req, state),
        rules::ReadAccess::Filter(expr) => {
            let query = query_param_json(&req.query, "q").unwrap_or(json!({}));
            let docs = state.db.find(col, &query).map_err(db_err)?;
            let n = docs
                .iter()
                .filter(|d| rules::row_visible(&expr, auth, d))
                .count();
            Ok(json!({ "count": n }))
        }
    }
}

fn handle_update_with_rules(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let query = body.get("query").ok_or((400, "missing 'query'"))?;
    let update = body.get("update").ok_or((400, "missing 'update'"))?;

    // Check rules against matching documents
    let docs = state.db.find(col, query).map_err(db_err)?;
    for doc in &docs {
        rules::check_access(&state.db, col, Operation::Update, auth, Some(doc), None)
            .map_err(denied_status)?;
    }

    let one = body.get("one").and_then(|v| v.as_bool()).unwrap_or(false);
    if one {
        let n = state.db.update_one(col, query, update).map_err(db_err)?;
        Ok(json!({"modified": n}))
    } else {
        let n = state.db.update(col, query, update).map_err(db_err)?;
        Ok(json!({"modified": n}))
    }
}

fn handle_delete_with_rules(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> Result<Value, (u16, &'static str)> {
    let body = if req.body.is_empty() {
        let q = query_param_json(&req.query, "q").unwrap_or(json!({}));
        json!({"query": q})
    } else {
        parse_json_body(req)?
    };
    let query = body.get("query").ok_or((400, "missing 'query'"))?;

    // Check rules against matching documents
    let docs = state.db.find(col, query).map_err(db_err)?;
    for doc in &docs {
        rules::check_access(&state.db, col, Operation::Delete, auth, Some(doc), None)
            .map_err(denied_status)?;
    }

    let n = state.db.delete(col, query).map_err(db_err)?;
    Ok(json!({"deleted": n}))
}

// ---------------------------------------------------------------------------
// Rules management handlers
// ---------------------------------------------------------------------------

fn handle_set_rules(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    // Reject a malformed expression with 400 (not a generic 500) so a typo can't
    // be stored as a silent fail-closed rule. The dashboard mirrors this grammar
    // and surfaces the precise per-field reason inline before ever POSTing.
    for field in ["read", "create", "update", "delete"] {
        if let Some(expr) = body[field].as_str() {
            rules::validate_rule_expr(expr).map_err(|_| (400, "invalid rule expression"))?;
        }
    }
    rules::set_rules(&state.db, col, &body).map_err(|_| (500, "failed to set rules"))?;
    Ok(json!({"collection": col, "rules": "set"}))
}

fn handle_get_rules(col: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    match rules::get_rules(&state.db, col) {
        Some(r) => Ok(json!({
            "collection": col,
            "read": r.read,
            "create": r.create,
            "update": r.update,
            "delete": r.delete,
            // Rates are part of the policy: leaving them out of the read makes
            // a limit invisible to whoever set it.
            "rate": r.rate.iter().map(|(op, rate)| (op.clone(), rate.spec()))
                .collect::<std::collections::BTreeMap<_, _>>(),
        })),
        None => Err((404, "no rules defined for this collection")),
    }
}

fn handle_delete_rules(col: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    rules::delete_rules(&state.db, col).map_err(|_| (500, "failed to delete rules"))?;
    Ok(json!({"collection": col, "rules": "deleted"}))
}

// ---------------------------------------------------------------------------
// Retention policy handlers
// ---------------------------------------------------------------------------

fn handle_set_retention(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let days = body
        .get("days")
        .and_then(|v| v.as_u64())
        .ok_or((400, "missing 'days'"))?;
    state
        .db
        .set_retention(col, days)
        .map_err(|_| (500, "failed to set retention"))?;
    Ok(json!({"collection": col, "retain_days": days}))
}

fn handle_get_retention(col: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    state
        .db
        .get_retention(col)
        .map_err(|_| (404, "no retention policy for this collection"))
}

fn handle_delete_retention(col: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    state
        .db
        .delete_retention(col)
        .map_err(|_| (404, "no retention policy for this collection"))?;
    Ok(json!({"collection": col, "retention": "deleted"}))
}

fn handle_list_retentions(state: &RestState) -> Result<Value, (u16, &'static str)> {
    let policies = state
        .db
        .list_retentions()
        .map_err(|_| (500, "failed to list retentions"))?;
    Ok(json!(policies))
}

// ---------------------------------------------------------------------------
// Alert handlers
// ---------------------------------------------------------------------------

fn handle_create_alert(req: &HttpRequest, state: &RestState) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let name = body
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or((400, "missing 'name'"))?;
    state
        .db
        .create_alert(name, body.clone())
        .map_err(|_| (400, "failed to create alert"))?;
    Ok(json!({"alert": name, "status": "created"}))
}

fn handle_list_alerts(state: &RestState) -> Result<Value, (u16, &'static str)> {
    let alerts = state
        .db
        .list_alerts()
        .map_err(|_| (500, "failed to list alerts"))?;
    Ok(json!(alerts))
}

fn handle_get_alert(name: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    state
        .db
        .get_alert(name)
        .map_err(|_| (404, "alert not found"))
}

fn handle_delete_alert(name: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    state
        .db
        .delete_alert(name)
        .map_err(|_| (404, "alert not found"))?;
    Ok(json!({"alert": name, "status": "deleted"}))
}

fn handle_test_alert(name: &str, state: &RestState) -> Result<Value, (u16, &'static str)> {
    state
        .db
        .test_alert(name)
        .map_err(|_| (404, "alert not found"))
}

fn handle_alert_history(state: &RestState) -> Result<Value, (u16, &'static str)> {
    let history = state
        .db
        .list_alert_history()
        .map_err(|_| (500, "failed to list alert history"))?;
    Ok(json!(history))
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// Decide whether `role` may invoke the REST endpoint addressed by
/// `(method, segments)`. Mirrors the TCP `rbac::is_permitted` default-deny
/// posture for the HTTP surface:
///
/// - **Admin** — everything.
/// - **ReadWrite** — document CRUD, counts, aggregation, index management, and
///   procedure calls. NOT collection drops, procedure/rule/retention/alert
///   management (those are admin-only).
/// - **Read** — only read-only endpoints (any `GET`, plus the read-only
///   `aggregate` POST).
///
/// `unruled_engine` marks a `/rest/v1/{x}` request bound for an engine with no
/// per-row security rules — the time-series engine, or a SQL table.
fn rest_permitted(role: auth::Role, method: &str, segments: &[&str], unruled_engine: bool) -> bool {
    use auth::Role::*;
    if role == Admin {
        return true;
    }

    // Mutating administrative endpoints — admin only.
    let admin_only = matches!(
        (method, segments),
        ("POST", ["api", "backup"])
            | ("DELETE", ["api", "collections", _])
            | ("POST", ["api", "procedures"])
            | ("DELETE", ["api", "procedures", _])
            | ("POST", ["api", "rules", _])
            | ("DELETE", ["api", "rules", _])
            | ("POST", ["api", "retention", _])
            | ("DELETE", ["api", "retention", _])
            | ("POST", ["api", "alerts"])
            | ("DELETE", ["api", "alerts", _])
            | ("POST", ["api", "alerts", _, "test"])
            // The security policy itself: readable by the operator, not by the
            // browser key it is written to constrain.
            | ("GET", ["api", "rules", _])
    );
    if admin_only {
        return false;
    }

    match role {
        Admin => true,
        // ReadWrite gets everything that is not admin-only.
        ReadWrite => true,
        // Read (the anon key) and Authenticated (a signed-in end-user) get the
        // same coarse privileges: any GET, the read-only aggregation POST, and
        // SQL (SELECT-only, enforced by the SQL bridge). Writes to the PostgREST
        // surface (`/rest/v1/{table}`) are also let through here so the
        // per-collection security rules (`check_access`) can decide — the
        // Supabase RLS model. A collection with no rule denies these writes by
        // default (see `rules::check_access`).
        Read | Authenticated => {
            // A write to `/rest/v1/{x}` is let through below so that the
            // collection's security rules can adjudicate it. Two engines
            // served by that same URL have no rules to adjudicate with — the
            // time-series engine, and the SQL engine (whose authorization is
            // RBAC-only). Letting those through would mean the browser-safe
            // anon key could append arbitrary points, or insert into and
            // delete from any table. Reads stay open; writing needs a
            // service_role key, like storage.
            if unruled_engine && method != "GET" {
                return false;
            }
            matches!(
                (method, segments),
                ("GET", _)
                    | ("HEAD", ["api", "storage", ..])
                    | ("POST", ["api", _, "aggregate"])
                    // A text search reads rows and hands them back, so the read
                    // rules can filter it and a browser key may search. Building
                    // the index is not a read, and is deliberately not here.
                    | ("POST", ["api", _, "text_search"])
                    // Searching a bucket's file text is a read of those files,
                    // which this key may already download; the per-bucket rule is
                    // checked in the handler.
                    | ("POST", ["api", "storage", "_search"])
                    | ("POST", ["api", "sql"])
                    | ("POST" | "PATCH" | "DELETE", ["rest", "v1", _])
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Auth handlers
// ---------------------------------------------------------------------------

fn handle_auth_signup(req: &HttpRequest, state: &RestState) -> HttpResponse {
    let secret = match &state.jwt_secret {
        Some(s) => s,
        None => {
            return json_response(
                400,
                "Bad Request",
                json!({"error": "auth not enabled (set OXIDB_JWT_SECRET)"}),
            );
        }
    };
    let body = match serde_json::from_slice::<Value>(&req.body) {
        Ok(v) => v,
        Err(_) => return json_response(400, "Bad Request", json!({"error": "invalid JSON"})),
    };
    let username = match body["username"].as_str() {
        Some(u) => u,
        None => return json_response(400, "Bad Request", json!({"error": "missing 'username'"})),
    };
    let password = match body["password"].as_str() {
        Some(p) => p,
        None => return json_response(400, "Bad Request", json!({"error": "missing 'password'"})),
    };
    // Public self-service signup must NOT let a caller mint themselves an
    // admin account. Reject any attempt to self-assign `admin`; privileged
    // accounts have to be provisioned out-of-band. Default to the lowest
    // write-capable role.
    let role = body["role"].as_str().unwrap_or("readwrite");
    if role.eq_ignore_ascii_case("admin") {
        return json_response(
            403,
            "Forbidden",
            json!({"error": "cannot self-assign 'admin' role via public signup"}),
        );
    }

    match jwt::signup(&state.db, username, password, role) {
        Ok(user) => {
            // Auto-login: return a token immediately
            match jwt::login(&state.db, username, password, secret) {
                Ok(token) => json_response(201, "Created", json!({"user": user, "token": token})),
                Err(e) => json_response(500, "Internal Server Error", json!({"error": e})),
            }
        }
        Err(e) => json_response(409, "Conflict", json!({"error": e})),
    }
}

fn handle_auth_login(req: &HttpRequest, state: &RestState) -> HttpResponse {
    let secret = match &state.jwt_secret {
        Some(s) => s,
        None => {
            return json_response(
                400,
                "Bad Request",
                json!({"error": "auth not enabled (set OXIDB_JWT_SECRET)"}),
            );
        }
    };
    let body = match serde_json::from_slice::<Value>(&req.body) {
        Ok(v) => v,
        Err(_) => return json_response(400, "Bad Request", json!({"error": "invalid JSON"})),
    };
    let username = match body["username"].as_str() {
        Some(u) => u,
        None => return json_response(400, "Bad Request", json!({"error": "missing 'username'"})),
    };
    let password = match body["password"].as_str() {
        Some(p) => p,
        None => return json_response(400, "Bad Request", json!({"error": "missing 'password'"})),
    };

    match jwt::login(&state.db, username, password, secret) {
        Ok(token) => json_response(200, "OK", json!({"token": token})),
        Err(e) => json_response(401, "Unauthorized", json!({"error": e})),
    }
}

fn handle_auth_verify(req: &HttpRequest, state: &RestState) -> HttpResponse {
    let secret = match &state.jwt_secret {
        Some(s) => s,
        None => return json_response(400, "Bad Request", json!({"error": "auth not enabled"})),
    };
    let auth_header = req
        .headers
        .get("authorization")
        .map(|s| s.as_str())
        .unwrap_or("");
    let token = match jwt::extract_bearer(auth_header) {
        Some(t) => t,
        None => {
            return json_response(
                401,
                "Unauthorized",
                json!({"error": "missing Bearer token"}),
            );
        }
    };
    match jwt::verify(token, secret) {
        Ok(claims) => json_response(
            200,
            "OK",
            json!({
                "valid": true,
                "username": claims.sub,
                "role": claims.role,
                "exp": claims.exp,
            }),
        ),
        Err(e) => json_response(401, "Unauthorized", json!({"error": e})),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_json_body(req: &HttpRequest) -> Result<Value, (u16, &'static str)> {
    if req.body.is_empty() {
        return Err((400, "empty request body"));
    }
    serde_json::from_slice(&req.body).map_err(|_| (400, "invalid JSON"))
}

fn query_param_json(query: &str, key: &str) -> Option<Value> {
    parse_query_string(query).get(key).and_then(|v| {
        // Try URL-decoded JSON parse
        let decoded = url_decode(v);
        serde_json::from_str(&decoded).ok()
    })
}

fn query_param_u64(query: &str, key: &str) -> Option<u64> {
    parse_query_string(query)
        .get(key)
        .and_then(|v| v.parse().ok())
}

fn parse_query_string(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if query.is_empty() {
        return map;
    }
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next().unwrap_or(b'0');
            let h2 = chars.next().unwrap_or(b'0');
            let hex = [h1, h2];
            if let Ok(byte) = u8::from_str_radix(std::str::from_utf8(&hex).unwrap_or("00"), 16) {
                result.push(byte as char);
            }
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

fn db_err(e: oxidb::Error) -> (u16, &'static str) {
    match e {
        // A tenant quota (OxiBase per-project caps) → 403, not 500.
        oxidb::Error::CollectionLimitExceeded(_) => {
            (403, "collection limit reached for this project")
        }
        oxidb::Error::DocumentLimitExceeded(_) => (403, "document limit reached for this project"),
        // A duplicate on a unique index is a conflict the caller can act on, not a
        // server fault. Deduplicating by key is the whole use of a unique index,
        // and a 500 makes it indistinguishable from a broken server.
        oxidb::Error::UniqueViolation { .. } => (409, "duplicate value for a unique field"),
        _ => (500, "database error"),
    }
}

fn json_response(status: u16, status_text: &'static str, body: Value) -> HttpResponse {
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    HttpResponse {
        status,
        status_text,
        content_type: "application/json".to_string(),
        headers: Vec::new(),
        body: bytes,
        content_length_override: None,
    }
}

fn with_rest_cors(resp: HttpResponse) -> HttpResponse {
    resp.with_header("Access-Control-Allow-Origin", "*")
        .with_header(
            "Access-Control-Allow-Methods",
            "GET, POST, PATCH, DELETE, OPTIONS",
        )
        .with_header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization",
        )
        .with_header("Access-Control-Max-Age", "3600")
}

#[cfg(test)]
mod permission_tests {
    use super::rest_permitted;
    use crate::auth::Role;

    #[test]
    fn browser_keys_may_never_write_a_sql_table() {
        // Same reasoning as the time-series case: SQL authorization is
        // RBAC-only, so a write let through "for the rules to decide" would be
        // decided by nothing. A published anon key could otherwise delete rows.
        for role in [Role::Read, Role::Authenticated] {
            assert!(rest_permitted(role, "GET", &["rest", "v1", "races"], true));
            assert!(!rest_permitted(
                role,
                "POST",
                &["rest", "v1", "races"],
                true
            ));
            assert!(!rest_permitted(
                role,
                "DELETE",
                &["rest", "v1", "races"],
                true
            ));
            assert!(!rest_permitted(
                role,
                "PATCH",
                &["rest", "v1", "races"],
                true
            ));
        }
        assert!(rest_permitted(
            Role::ReadWrite,
            "DELETE",
            &["rest", "v1", "races"],
            true
        ));
    }

    #[test]
    fn browser_keys_may_read_but_never_write_time_series() {
        for role in [Role::Read, Role::Authenticated] {
            // Reading a series is fine — that is what a dashboard does.
            assert!(rest_permitted(role, "GET", &["rest", "v1", "cpu"], true));
            // Writing is not: the time-series engine has no per-row rules, so
            // nothing would adjudicate a write from a key that ships in a
            // browser. (Regression: this used to be allowed, letting anyone
            // holding a published anon key append points to a project.)
            assert!(!rest_permitted(role, "POST", &["rest", "v1", "cpu"], true));
            assert!(!rest_permitted(role, "PATCH", &["rest", "v1", "cpu"], true));
            assert!(!rest_permitted(
                role,
                "DELETE",
                &["rest", "v1", "cpu"],
                true
            ));
        }
    }

    #[test]
    fn document_writes_are_still_delegated_to_the_rules() {
        // Without the tsdb profile the same URL is the document engine, whose
        // per-collection rules decide — so the gate must let it through.
        for role in [Role::Read, Role::Authenticated] {
            assert!(rest_permitted(
                role,
                "POST",
                &["rest", "v1", "notes"],
                false
            ));
            assert!(rest_permitted(
                role,
                "PATCH",
                &["rest", "v1", "notes"],
                false
            ));
            assert!(rest_permitted(
                role,
                "DELETE",
                &["rest", "v1", "notes"],
                false
            ));
        }
    }

    #[test]
    fn service_role_keys_may_write_time_series() {
        assert!(rest_permitted(
            Role::ReadWrite,
            "POST",
            &["rest", "v1", "cpu"],
            true
        ));
        assert!(rest_permitted(
            Role::Admin,
            "POST",
            &["rest", "v1", "cpu"],
            true
        ));
    }
}
