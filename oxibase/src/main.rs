//! OxiBase — the OxiDB control plane as its own lean binary (ADR-0021).
//!
//! Serves `/platform/v1/*` (developer signup/login + project provisioning) and
//! reaches the data plane (`oxidb-server`) over OxiDB's **native OxiWire
//! protocol** as an admin client (`oxidb-client`): tenant databases via
//! `create_database`, its own state stored in an `oxibase` metadata database.
//! It links no database engine — only `oxidb-http` (its listener) + `oxidb-client`
//! (the wire) + small crypto crates.
//!
//! Config (env):
//! - `OXIBASE_ADDR`             — listen address (default `127.0.0.1:4460`)
//! - `OXIBASE_UPSTREAM`         — data-plane wire endpoint, `host:port` (default `127.0.0.1:4444`)
//! - `OXIBASE_UPSTREAM_USER`/`_PASSWORD` — optional SCRAM credentials for the wire
//! - `OXIDB_PLATFORM_SECRET`    — signs developer sessions (required)
//! - `OXIDB_SEAL_KEY`           — seals per-project secrets (falls back to the
//!                                platform secret); the data plane unseals with the same
//! plus the reused guard knobs `OXIDB_PLATFORM_SIGNUP_RATE/_CODE/MAX_ACCOUNTS/MAX_PROJECTS`.

mod crypto;
mod handlers;
mod upstream;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use oxidb_http::message::{HttpRequest, HttpResponse};
use serde_json::{Value, json};

use upstream::Upstream;

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub struct State {
    pub upstream: Upstream,
    pub platform_secret: String,
    pub seal_key: [u8; 32],
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn main() {
    let addr = env("OXIBASE_ADDR").unwrap_or_else(|| "127.0.0.1:4460".to_string());
    // Native OxiWire endpoint of the data plane (host:port).
    let upstream_addr = env("OXIBASE_UPSTREAM").unwrap_or_else(|| "127.0.0.1:4444".to_string());
    let upstream_user = env("OXIBASE_UPSTREAM_USER");
    let upstream_pass = env("OXIBASE_UPSTREAM_PASSWORD");

    let Some(platform_secret) = env("OXIDB_PLATFORM_SECRET") else {
        eprintln!("[oxibase] FATAL: set OXIDB_PLATFORM_SECRET (signs developer sessions)");
        std::process::exit(1);
    };
    let seal_material = env("OXIDB_SEAL_KEY").unwrap_or_else(|| platform_secret.clone());

    let upstream = Upstream::new(upstream_addr.clone(), upstream_user, upstream_pass);
    if let Err(e) = upstream.ensure_meta_db() {
        eprintln!("[oxibase] WARNING: could not ensure the metadata database yet: {e}");
        eprintln!("[oxibase]          (is {upstream_addr} up? it will retry on first write)");
    }

    let state = Arc::new(State {
        upstream,
        platform_secret,
        seal_key: crypto::derive_key(&seal_material),
    });

    eprintln!("[oxibase] control plane on {addr} → data plane {upstream_addr}");
    let handler = move |req: &HttpRequest| route(req, &state);
    if let Err(e) = oxidb_http::server::serve(&addr, 8, 256, handler) {
        eprintln!("[oxibase] FATAL: failed to bind {addr}: {e}");
        std::process::exit(1);
    }
}

fn route(req: &HttpRequest, state: &State) -> HttpResponse {
    if req.method == "OPTIONS" {
        return resp(204, json!(null));
    }
    let segs: Vec<&str> = req.path.trim_matches('/').split('/').collect();
    match (req.method.as_str(), segs.as_slice()) {
        ("GET", ["platform", "v1", "health"]) => resp(200, json!({ "status": "ok" })),
        ("POST", ["platform", "v1", "signup"]) => handlers::signup(req, state),
        ("POST", ["platform", "v1", "login"]) => handlers::login(req, state),
        ("POST", ["platform", "v1", "projects"]) => handlers::create_project(req, state),
        ("GET", ["platform", "v1", "projects"]) => handlers::list_projects(req, state),
        ("GET", ["platform", "v1", "projects", r]) => handlers::get_project(req, state, r),
        ("GET", ["platform", "v1", "projects", r, "jwks"]) => handlers::project_jwks(state, r),
        // Public per-project end-user auth (an app's own users).
        ("POST", ["platform", "v1", "projects", r, "auth", "signup"]) => {
            handlers::end_user_signup(req, state, r)
        }
        ("POST", ["platform", "v1", "projects", r, "auth", "login"]) => {
            handlers::end_user_login(req, state, r)
        }
        ("POST", ["platform", "v1", "projects", r, "auth", "refresh"]) => {
            handlers::end_user_refresh(req, state, r)
        }
        ("DELETE", ["platform", "v1", "projects", r]) => handlers::delete_project(req, state, r),
        ("POST", ["platform", "v1", "projects", r, "keys", "rotate"]) => {
            handlers::rotate_keys(req, state, r)
        }
        _ => resp(404, json!({ "message": "no such platform route" })),
    }
}

pub fn resp(status: u16, body: Value) -> HttpResponse {
    let status_text = match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        429 => "Too Many Requests",
        501 => "Not Implemented",
        _ => "Internal Server Error",
    };
    HttpResponse {
        status,
        status_text,
        content_type: "application/json".to_string(),
        headers: Vec::new(),
        body: serde_json::to_vec(&body).unwrap_or_default(),
        content_length_override: None,
    }
    .with_header("Access-Control-Allow-Origin", "*")
    .with_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
    .with_header(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization",
    )
}
