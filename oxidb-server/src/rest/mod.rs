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

        let resp = route_request(&req, state);
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

    // ── Database targeting (ADR-0012): `?db=<name>` selects the database;
    // no parameter keeps the default database exactly as before.
    let db_name = parse_query_string(&req.query)
        .get("db")
        .map(|v| url_decode(v));
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

    // ── Phase 2 (ADR-0003): /v1/ is the 1.0 stable URL prefix. ──────────
    // Both `/v1/api/...` and the legacy bare `/api/...` resolve to the
    // same handlers during the deprecation window; see docs/format/compat-matrix.md.
    // When a future major (2.0) ships, `/v1/api/...` continues working with
    // 1.0 semantics, `/v2/api/...` gets the new semantics, and bare
    // `/api/...` either redirects or errors per release notes.
    let segments: Vec<&str> = if raw_segments.first() == Some(&"v1") {
        raw_segments[1..].to_vec()
    } else {
        raw_segments.clone()
    };

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
    let result = match (req.method.as_str(), segments.as_slice()) {
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
        _ => None::<Value>,
    };
    if let Some(_) = result {
        // handled above via early return
    }

    // ── JWT enforcement + extract auth context ─────────────────────
    // `enforced_role` is `Some` only when auth is enabled (a JWT secret is
    // configured). When it is `Some`, we gate every protected endpoint on the
    // caller's role below — without this, any valid token (even `read`) could
    // drop collections, rewrite security rules, or create stored procedures.
    let (auth_ctx, enforced_role) = if let Some(ref secret) = state.jwt_secret {
        let auth_header = req
            .headers
            .get("authorization")
            .map(|s| s.as_str())
            .unwrap_or("");
        if let Some(token) = jwt::extract_bearer(auth_header) {
            match jwt::verify(token, secret) {
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
    if let Some(role) = enforced_role {
        if !rest_permitted(role, req.method.as_str(), segments.as_slice()) {
            return with_rest_cors(json_response(
                403,
                "Forbidden",
                json!({"error": "insufficient privileges for this operation"}),
            ));
        }
    }

    // ── SQL engine (ADR-0010) ─────────────────────────────────────────
    // Handled outside the generic match so the dynamic error message from
    // the SQL engine (syntax errors, etc.) reaches the client. Read-role
    // callers are restricted to SELECT statements by the SQL bridge.
    if let ("POST", ["api", "sql"]) = (req.method.as_str(), segments.as_slice()) {
        let readonly = enforced_role == Some(auth::Role::Read);
        return with_rest_cors(handle_sql_endpoint(
            req,
            readonly,
            db_name.as_deref(),
            state,
            enforced_role,
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
        match (req.method.as_str(), segments.as_slice()) {
            ("GET", ["rest", "v1", table]) => {
                return with_rest_cors(if crate::sql_bridge::sql_table_exists(sql_db, table) {
                    postgrest_sql::handle_get(sql_db, table, req)
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
        ("GET", ["api", col, "count"]) => handle_count(col, req, state),

        // Aggregation
        ("POST", ["api", col, "aggregate"]) => handle_aggregate(col, req, state),

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

fn handle_aggregate(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
) -> Result<Value, (u16, &'static str)> {
    let body = parse_json_body(req)?;
    let pipeline = body.get("pipeline").ok_or((400, "missing 'pipeline'"))?;
    let results = state.db.aggregate(col, pipeline).map_err(db_err)?;
    Ok(json!(results))
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
            .map_err(|_| (403, "access denied"))?;
        let id = state.db.insert(col, doc.clone()).map_err(db_err)?;
        Ok(json!({"id": id}))
    } else if let Some(docs) = body.get("docs").and_then(|v| v.as_array()) {
        // Check rules on first doc as representative (batch check)
        if let Some(first) = docs.first() {
            rules::check_access(&state.db, col, Operation::Create, auth, None, Some(first))
                .map_err(|_| (403, "access denied"))?;
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
    // Check read access (without specific doc — collection-level check)
    rules::check_access(&state.db, col, Operation::Read, auth, None, None)
        .map_err(|_| (403, "access denied"))?;
    handle_find(col, req, state)
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
            .map_err(|_| (403, "access denied"))?;
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
            .map_err(|_| (403, "access denied"))?;
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
fn rest_permitted(role: auth::Role, method: &str, segments: &[&str]) -> bool {
    use auth::Role::*;
    if role == Admin {
        return true;
    }

    // Mutating administrative endpoints — admin only.
    let admin_only = matches!(
        (method, segments),
        ("DELETE", ["api", "collections", _])
            | ("POST", ["api", "procedures"])
            | ("DELETE", ["api", "procedures", _])
            | ("POST", ["api", "rules", _])
            | ("DELETE", ["api", "rules", _])
            | ("POST", ["api", "retention", _])
            | ("DELETE", ["api", "retention", _])
            | ("POST", ["api", "alerts"])
            | ("DELETE", ["api", "alerts", _])
            | ("POST", ["api", "alerts", _, "test"])
    );
    if admin_only {
        return false;
    }

    match role {
        Admin => true,
        // ReadWrite gets everything that is not admin-only.
        ReadWrite => true,
        // Read is restricted to read-only operations: any GET, the read-only
        // aggregation POST, and SQL (SELECT-only, enforced by the SQL bridge).
        Read => matches!(
            (method, segments),
            ("GET", _) | ("POST", ["api", _, "aggregate"]) | ("POST", ["api", "sql"])
        ),
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

fn db_err(_e: oxidb::Error) -> (u16, &'static str) {
    (500, "database error")
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
