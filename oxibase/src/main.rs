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
//! - `OXIDB_SEAL_KEY` — seals per-project secrets (falls back to the platform
//!   secret); the data plane unseals with the same
//!
//! plus the reused guard knobs `OXIDB_PLATFORM_SIGNUP_RATE/_CODE/MAX_ACCOUNTS/MAX_PROJECTS`.

mod crypto;
mod gelf;
mod handlers;
mod mail;
mod oauth;
mod typegen;
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
    /// Outbound email (verification / password reset). `None` = flows disabled.
    pub mailer: Option<mail::Mailer>,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// The `sub` a bearer token claims, unverified — for logging a request that was
/// already accepted. On a refusal there is no identity to report, only an
/// assertion the caller made, so callers pass this only for 2xx/3xx.
fn bearer_subject(req: &HttpRequest) -> Option<String> {
    let header = req.headers.get("authorization")?;
    let token = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))?;
    let payload = token.split('.').nth(1)?;
    let bytes = crypto::b64url_decode(payload)?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    Some(json.get("sub")?.as_str()?.to_string())
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

    let mailer = mail::Mailer::from_env();
    if mailer.is_some() {
        eprintln!("[oxibase] SMTP configured — email verification + password reset enabled");
    }
    let state = Arc::new(State {
        upstream,
        platform_secret,
        seal_key: crypto::derive_key(&seal_material),
        mailer,
    });

    gelf::init(); // arm GELF (OXIDB_GELF_ADDR) — logs every request; no-op if unset
    eprintln!("[oxibase] control plane on {addr} → data plane {upstream_addr}");
    // Access logging to stderr, so `docker logs` shows who tried what even with
    // no log sink configured. `OXIBASE_ACCESS_LOG=0` silences it.
    let access_log = std::env::var("OXIBASE_ACCESS_LOG")
        .map(|v| !matches!(v.as_str(), "0" | "false" | "no" | "off"))
        .unwrap_or(true);
    let handler = move |req: &HttpRequest| {
        let start = SystemTime::now();
        let resp = route(req, &state);
        if access_log {
            let who = req.client_meta();
            let actor = if resp.status < 400 {
                bearer_subject(req)
            } else {
                None
            };
            let ms = start.elapsed().map(|d| d.as_millis()).unwrap_or(0);
            let mut where_from = String::new();
            if !who.country.is_empty() {
                where_from.push(' ');
                where_from.push_str(who.country);
                if !who.city.is_empty() {
                    where_from.push('/');
                    where_from.push_str(who.city);
                }
            }
            if !who.ray.is_empty() {
                where_from.push_str(" ray=");
                where_from.push_str(who.ray);
            }
            eprintln!(
                "[oxibase] {} {} {} {}ms ip={}{}{}",
                resp.status,
                req.method,
                req.path,
                ms,
                if who.ip.is_empty() { "-" } else { who.ip },
                where_from,
                actor
                    .as_ref()
                    .map(|a| format!(" user={a}"))
                    .unwrap_or_default(),
            );
        }
        if gelf::enabled() {
            let ms = start
                .elapsed()
                .map(|d| d.as_millis())
                .unwrap_or(0)
                .to_string();
            let status = resp.status.to_string();
            let level = match resp.status {
                s if s >= 500 => gelf::Level::Error,
                s if s >= 400 => gelf::Level::Warning,
                _ => gelf::Level::Info,
            };
            let who = req.client_meta();
            let actor = if resp.status < 400 {
                bearer_subject(req)
            } else {
                None
            };
            let mut fields = vec![
                ("app", "oxibase"),
                ("method", req.method.as_str()),
                ("path", req.path.as_str()),
                ("status", status.as_str()),
                ("ms", ms.as_str()),
            ];
            fields.extend(who.fields());
            if let Some(a) = actor.as_deref() {
                fields.push(("user", a));
            }
            gelf::log(level, &format!("{} {}", req.method, req.path), &fields);
        }
        resp
    };
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
        // Public bootstrap config for the dashboard (which auth methods exist).
        ("GET", ["platform", "v1", "config"]) => handlers::config(),
        // Developer sign-in — Google only.
        ("POST", ["platform", "v1", "auth", "google"]) => handlers::auth_google(req, state),
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
        // Passwordless sign-in: request a link, then the link itself.
        ("POST", ["platform", "v1", "projects", r, "auth", "magiclink"]) => {
            handlers::end_user_magiclink(req, state, r)
        }
        ("GET", ["platform", "v1", "projects", r, "auth", "magiclink", v]) if *v == "verify" => {
            handlers::end_user_magiclink_verify(req, state, r)
        }
        // Public: what sign-in methods this project offers.
        ("GET", ["platform", "v1", "projects", r, "auth", "settings"]) => {
            handlers::auth_settings(state, r)
        }
        // Social sign-in — browser redirect flow (Google, GitHub) …
        ("GET", ["platform", "v1", "projects", r, "auth", "authorize", p]) => {
            handlers::oauth_authorize(req, state, r, p)
        }
        ("GET", ["platform", "v1", "projects", r, "auth", "callback", p]) => {
            handlers::oauth_callback(req, state, r, p)
        }
        // … and the Google ID-token flow for apps that already run GIS.
        ("POST", ["platform", "v1", "projects", r, "auth", "oauth", "google"]) => {
            handlers::end_user_oauth_google(req, state, r)
        }
        // Owner-only provider configuration.
        ("GET", ["platform", "v1", "projects", r, "auth", "providers"]) => {
            handlers::auth_providers_get(req, state, r)
        }
        ("PATCH", ["platform", "v1", "projects", r, "auth", "providers"]) => {
            handlers::auth_providers_set(req, state, r)
        }
        ("GET", ["platform", "v1", "projects", r, "auth", "verify"]) => {
            handlers::end_user_verify(req, state, r)
        }
        ("POST", ["platform", "v1", "projects", r, "auth", "resend"]) => {
            handlers::end_user_resend(req, state, r)
        }
        ("POST", ["platform", "v1", "projects", r, "auth", "recover"]) => {
            handlers::end_user_recover(req, state, r)
        }
        ("POST", ["platform", "v1", "projects", r, "auth", "reset"]) => {
            handlers::end_user_reset(req, state, r)
        }
        ("GET", ["platform", "v1", "projects", r, "users"]) => handlers::list_users(req, state, r),
        ("GET", ["platform", "v1", "projects", r, "logs"]) => handlers::project_logs(req, state, r),
        ("GET", ["platform", "v1", "projects", r, "types"]) => {
            handlers::project_types(req, state, r)
        }
        ("DELETE", ["platform", "v1", "projects", r, "users", email]) => {
            handlers::delete_user(req, state, r, email)
        }
        ("POST", ["platform", "v1", "projects", r, "users", email, "password"]) => {
            handlers::admin_set_user_password(req, state, r, email)
        }
        ("POST", ["platform", "v1", "projects", r, "users", email, "verify"]) => {
            handlers::admin_verify_user(req, state, r, email)
        }
        ("DELETE", ["platform", "v1", "projects", r]) => handlers::delete_project(req, state, r),
        ("POST", ["platform", "v1", "projects", r, "keys", "rotate"]) => {
            handlers::rotate_keys(req, state, r)
        }
        ("PATCH", ["platform", "v1", "projects", r, "limits"]) => {
            handlers::update_limits(req, state, r)
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
    .with_header(
        "Access-Control-Allow-Methods",
        "GET, POST, PATCH, DELETE, OPTIONS",
    )
    .with_header(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization",
    )
}
