//! `/platform/v1/*` handlers. Same logic as the in-server skeleton, but storage
//! goes through [`Upstream`](crate::upstream::Upstream) (REST to the data plane)
//! instead of a local engine, and crypto is the crate's self-contained
//! primitives.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use base64::Engine;
use oxidb_http::message::{HttpRequest, HttpResponse};
use serde_json::{Value, json};

use crate::crypto::{self, Claims};
use crate::{State, now_secs, resp};

const KEY_EXPIRY_SECS: u64 = 10 * 365 * 86_400;
const SESSION_EXPIRY_SECS: u64 = 86_400;
/// End-user **refresh** token lifetime.
const REFRESH_EXPIRY_SECS: u64 = 30 * 86_400;

/// End-user **access** token lifetime — short-lived; the client refreshes it
/// with the long-lived refresh token. Configurable via `OXIDB_PLATFORM_ACCESS_TTL`
/// (seconds, default 3600).
fn access_expiry_secs() -> u64 {
    std::env::var("OXIDB_PLATFORM_ACCESS_TTL")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&s| s > 0)
        .unwrap_or(3_600)
}
const MIN_PASSWORD_LEN: usize = 8;
const LOGIN_MAX_FAILS: u32 = 5;
const LOGIN_LOCKOUT_SECS: u64 = 300;
const SIGNUP_WINDOW_SECS: u64 = 60;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn max_projects() -> usize {
    env_usize("OXIDB_PLATFORM_MAX_PROJECTS", 100)
}
fn max_accounts() -> usize {
    env_usize("OXIDB_PLATFORM_MAX_ACCOUNTS", 10_000)
}
fn max_users_per_project() -> usize {
    env_usize("OXIDB_PLATFORM_MAX_USERS", 100_000)
}
fn signup_rate() -> u32 {
    std::env::var("OXIDB_PLATFORM_SIGNUP_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}
fn signup_code() -> Option<String> {
    std::env::var("OXIDB_PLATFORM_SIGNUP_CODE")
        .ok()
        .filter(|s| !s.is_empty())
}

// ---------------------------------------------------------------------------
// Account handlers
// ---------------------------------------------------------------------------

pub fn signup(req: &HttpRequest, state: &State) -> HttpResponse {
    if !signup_allowed(&client_ip(req)) {
        return resp(
            429,
            json!({ "message": "signup rate limit exceeded; slow down" }),
        );
    }
    let body = parse_body(req);
    if let Some(code) = signup_code() {
        if body.get("code").and_then(|v| v.as_str()) != Some(code.as_str()) {
            return resp(403, json!({ "message": "a valid invite code is required" }));
        }
    }
    if state.upstream.count("accounts", &json!({})).unwrap_or(0) >= max_accounts() {
        return resp(403, json!({ "message": "signups are closed" }));
    }
    let (Some(email), Some(password)) = (str_field(&body, "email"), str_field(&body, "password"))
    else {
        return resp(400, json!({ "message": "email and password required" }));
    };
    if password.len() < MIN_PASSWORD_LEN {
        return resp(
            400,
            json!({ "message": format!("password must be at least {MIN_PASSWORD_LEN} characters") }),
        );
    }
    match state.upstream.find("accounts", &json!({ "email": email })) {
        Ok(existing) if !existing.is_empty() => {
            return resp(409, json!({ "message": "email already registered" }));
        }
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
        _ => {}
    }
    let pw_hash = match crypto::hash_password(&password) {
        Ok(h) => h,
        Err(e) => return resp(500, json!({ "message": e })),
    };
    let doc = json!({ "email": email, "pw_hash": pw_hash, "created_at": now_secs() });
    if let Err(e) = state.upstream.insert("accounts", &doc) {
        return resp(502, json!({ "message": format!("upstream: {e}") }));
    }
    let token = session_token(state, &email);
    resp(
        201,
        json!({ "account": { "email": email }, "token": token }),
    )
}

pub fn login(req: &HttpRequest, state: &State) -> HttpResponse {
    let body = parse_body(req);
    let (Some(email), Some(password)) = (str_field(&body, "email"), str_field(&body, "password"))
    else {
        return resp(400, json!({ "message": "email and password required" }));
    };
    if login_locked(&email) {
        return resp(
            429,
            json!({ "message": "too many failed attempts; try again later" }),
        );
    }
    let account = match state.upstream.find("accounts", &json!({ "email": email })) {
        Ok(mut a) => a.pop(),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let ok = account
        .as_ref()
        .and_then(|a| a.get("pw_hash").and_then(|v| v.as_str()))
        .is_some_and(|h| crypto::verify_password(&password, h));
    if ok {
        login_clear(&email);
        resp(200, json!({ "token": session_token(state, &email) }))
    } else {
        login_record_failure(&email);
        resp(401, json!({ "message": "invalid credentials" }))
    }
}

// ---------------------------------------------------------------------------
// Project handlers
// ---------------------------------------------------------------------------

pub fn create_project(req: &HttpRequest, state: &State) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    if state
        .upstream
        .count("projects", &json!({ "owner": owner }))
        .unwrap_or(0)
        >= max_projects()
    {
        return resp(
            403,
            json!({ "message": format!("project limit reached ({})", max_projects()) }),
        );
    }
    let body = parse_body(req);
    let name = str_field(&body, "name").unwrap_or_default();
    let project_ref = gen_ref();
    let (priv_scalar, pub_point) = crypto::gen_es256_keypair();
    let created_at = now_secs();

    if let Err(e) = state.upstream.create_database(&project_ref) {
        return resp(
            502,
            json!({ "message": format!("provisioning failed: {e}") }),
        );
    }
    let doc = json!({
        "ref": project_ref,
        "owner": owner,
        "name": name,
        // ES256 asymmetric keys: the public key is stored in the clear (it is
        // public and lets data-plane nodes verify without the seal key), the
        // private scalar is sealed.
        "pubkey": b64std(&pub_point),
        "priv_enc": b64std(&crypto::seal(&state.seal_key, &priv_scalar)),
        "isolation": "shared",
        "created_at": created_at,
        "key_iat": created_at,
    });
    if let Err(e) = state.upstream.insert("projects", &doc) {
        let _ = state.upstream.drop_database(&project_ref);
        return resp(
            502,
            json!({ "message": format!("failed to record project: {e}") }),
        );
    }
    resp(
        201,
        project_view(
            &project_ref,
            &name,
            created_at,
            created_at,
            &priv_scalar,
            true,
        ),
    )
}

pub fn list_projects(req: &HttpRequest, state: &State) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    match state.upstream.find("projects", &json!({ "owner": owner })) {
        Ok(docs) => {
            let list: Vec<Value> = docs
                .iter()
                .map(|d| json!({ "ref": d.get("ref"), "name": d.get("name"), "created_at": d.get("created_at") }))
                .collect();
            resp(200, json!(list))
        }
        Err(e) => resp(502, json!({ "message": format!("upstream: {e}") })),
    }
}

pub fn get_project(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let doc = match owned_project(state, project_ref, &owner) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let (created_at, key_iat, name) = meta(&doc);
    let priv_scalar = match project_priv(state, &doc) {
        Some(s) => s,
        None => return resp(500, json!({ "message": "unseal failed" })),
    };
    resp(
        200,
        project_view(project_ref, &name, created_at, key_iat, &priv_scalar, true),
    )
}

/// Public JWKS for a project: its ES256 verification key as a JWK set. No auth —
/// a JWKS is meant to be world-readable so any party can verify the project's
/// tokens with the public key alone. Legacy HS256 projects have no public key
/// and return an empty set.
pub fn project_jwks(state: &State, project_ref: &str) -> HttpResponse {
    let doc = match state.upstream.find("projects", &json!({ "ref": project_ref })) {
        Ok(mut d) => d.pop(),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let Some(doc) = doc else {
        return resp(404, json!({ "message": "project not found" }));
    };
    let Some(pub_b64) = doc.get("pubkey").and_then(|v| v.as_str()) else {
        return resp(200, json!({ "keys": [] }));
    };
    let Some(pub_point) = base64::engine::general_purpose::STANDARD.decode(pub_b64).ok() else {
        return resp(500, json!({ "message": "malformed key" }));
    };
    let kid = format!(
        "{project_ref}-{}",
        doc.get("key_iat").and_then(|v| v.as_u64()).unwrap_or(0)
    );
    match crypto::jwk_from_pub(&pub_point, &kid) {
        Some(jwk) => resp(200, json!({ "keys": [jwk] })),
        None => resp(500, json!({ "message": "malformed key" })),
    }
}

// ---------------------------------------------------------------------------
// Per-project end-user auth (the Supabase GoTrue analog).
//
// Public endpoints — an app's own users sign themselves up against a project.
// Users live in the reserved `oxibase` metadata db (collection `users`, scoped
// by `project_ref`), never in a data-plane-readable collection. The token is
// signed with the PROJECT's ES256 private key, so the data plane verifies it
// with the project's public key alone and rules see `auth.username` (the email)
// and `auth.role == "authenticated"`.
// ---------------------------------------------------------------------------

/// `POST /platform/v1/projects/{ref}/auth/signup` — create an end-user.
pub fn end_user_signup(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    // Per-project + per-actor rate limit — one project's abuse can't throttle
    // another's, and a single client can't flood the user table.
    let actor = format!("{project_ref}:{}", client_ip(req));
    if !signup_allowed(&actor) {
        return resp(429, json!({ "message": "signup rate limit exceeded; slow down" }));
    }
    let body = parse_body(req);
    let (Some(email), Some(password)) = (str_field(&body, "email"), str_field(&body, "password"))
    else {
        return resp(400, json!({ "message": "email and password required" }));
    };
    if password.len() < MIN_PASSWORD_LEN {
        return resp(
            400,
            json!({ "message": format!("password must be at least {MIN_PASSWORD_LEN} characters") }),
        );
    }
    let pdoc = match project_by_ref(state, project_ref) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    // Hard ceiling on a project's user directory (defense in depth over the rate
    // limit) — `OXIDB_PLATFORM_MAX_USERS` per project.
    if state
        .upstream
        .count("users", &json!({ "project_ref": project_ref }))
        .unwrap_or(0)
        >= max_users_per_project()
    {
        return resp(403, json!({ "message": "user limit reached for this project" }));
    }
    match state
        .upstream
        .find("users", &json!({ "project_ref": project_ref, "email": email }))
    {
        Ok(u) if !u.is_empty() => {
            return resp(409, json!({ "message": "email already registered" }));
        }
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
        _ => {}
    }
    let pw_hash = match crypto::hash_password(&password) {
        Ok(h) => h,
        Err(e) => return resp(500, json!({ "message": e })),
    };
    let doc = json!({
        "project_ref": project_ref,
        "email": email,
        "pw_hash": pw_hash,
        "created_at": now_secs(),
    });
    if let Err(e) = state.upstream.insert("users", &doc) {
        return resp(502, json!({ "message": format!("upstream: {e}") }));
    }
    match issue_session(state, &pdoc, project_ref, &email) {
        Some((token, refresh)) => resp(
            201,
            json!({ "user": { "email": email }, "token": token, "refresh_token": refresh }),
        ),
        None => resp(500, json!({ "message": "failed to mint token" })),
    }
}

/// `POST /platform/v1/projects/{ref}/auth/login` — end-user login.
pub fn end_user_login(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let body = parse_body(req);
    let (Some(email), Some(password)) = (str_field(&body, "email"), str_field(&body, "password"))
    else {
        return resp(400, json!({ "message": "email and password required" }));
    };
    // Brute-force lockout, scoped per (project, email) so an attacker guessing
    // one project's user can't lock out the same email in another project.
    let lock_key = format!("{project_ref}:{email}");
    if login_locked(&lock_key) {
        return resp(
            429,
            json!({ "message": "too many failed attempts; try again later" }),
        );
    }
    let pdoc = match project_by_ref(state, project_ref) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let user = match state
        .upstream
        .find("users", &json!({ "project_ref": project_ref, "email": email }))
    {
        Ok(mut u) => u.pop(),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let ok = user
        .as_ref()
        .and_then(|u| u.get("pw_hash").and_then(|v| v.as_str()))
        .is_some_and(|h| crypto::verify_password(&password, h));
    if !ok {
        login_record_failure(&lock_key);
        return resp(401, json!({ "message": "invalid credentials" }));
    }
    login_clear(&lock_key);
    match issue_session(state, &pdoc, project_ref, &email) {
        Some((token, refresh)) => resp(200, json!({ "token": token, "refresh_token": refresh })),
        None => resp(500, json!({ "message": "failed to mint token" })),
    }
}

/// `POST /platform/v1/projects/{ref}/auth/refresh` — exchange a refresh token
/// for a fresh access token. The refresh token is **rotated** (single-use): the
/// old one is revoked and a new one issued, so a stolen-and-replayed token is
/// caught (both copies stop working after the first use).
pub fn end_user_refresh(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let body = parse_body(req);
    let Some(refresh) = str_field(&body, "refresh_token") else {
        return resp(400, json!({ "message": "refresh_token required" }));
    };
    let hash = crypto::sha256_hex(refresh.as_bytes());
    let row = match state
        .upstream
        .find("refresh_tokens", &json!({ "project_ref": project_ref, "token_hash": hash }))
    {
        Ok(mut v) => v.pop(),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let Some(row) = row else {
        return resp(401, json!({ "message": "invalid refresh token" }));
    };
    // Consume the presented token regardless (rotation / expiry cleanup).
    let _ = state
        .upstream
        .delete("refresh_tokens", &json!({ "token_hash": hash }));
    if now_secs() > row.get("exp").and_then(|v| v.as_u64()).unwrap_or(0) {
        return resp(401, json!({ "message": "refresh token expired" }));
    }
    let Some(email) = row.get("email").and_then(|v| v.as_str()) else {
        return resp(500, json!({ "message": "corrupt session" }));
    };
    let pdoc = match project_by_ref(state, project_ref) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    match issue_session(state, &pdoc, project_ref, email) {
        Some((token, new_refresh)) => {
            resp(200, json!({ "token": token, "refresh_token": new_refresh }))
        }
        None => resp(500, json!({ "message": "failed to mint token" })),
    }
}

/// Look up a project by ref alone (no owner filter) — end-user auth is public.
fn project_by_ref(state: &State, project_ref: &str) -> Result<Option<Value>, String> {
    Ok(state
        .upstream
        .find("projects", &json!({ "ref": project_ref }))?
        .into_iter()
        .next())
}

/// Mint an ES256 end-user access token signed with the project's private key.
fn mint_user_token(state: &State, project_doc: &Value, email: &str) -> Option<String> {
    let priv_scalar = project_priv(state, project_doc)?;
    let now = now_secs();
    crypto::encode_jwt_es256(
        &Claims {
            sub: email.to_string(),
            role: "authenticated".to_string(),
            iat: now,
            exp: now + access_expiry_secs(),
        },
        &priv_scalar,
    )
}

/// A random opaque token (refresh tokens are not JWTs — they are server-side
/// session handles that can be revoked).
fn gen_token() -> String {
    use rand::RngCore;
    let mut b = [0u8; 32];
    rand::rng().fill_bytes(&mut b);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

/// Issue a session: a short-lived access token + a stored (hashed) refresh
/// token. Returns `(access_token, refresh_token)`.
fn issue_session(
    state: &State,
    project_doc: &Value,
    project_ref: &str,
    email: &str,
) -> Option<(String, String)> {
    let access = mint_user_token(state, project_doc, email)?;
    let refresh = gen_token();
    let row = json!({
        "project_ref": project_ref,
        "email": email,
        "token_hash": crypto::sha256_hex(refresh.as_bytes()),
        "exp": now_secs() + REFRESH_EXPIRY_SECS,
    });
    state.upstream.insert("refresh_tokens", &row).ok()?;
    Some((access, refresh))
}

pub fn delete_project(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    match owned_project(state, project_ref, &owner) {
        Ok(Some(_)) => {}
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    }
    let _ = state.upstream.drop_database(project_ref);
    let _ = state
        .upstream
        .delete("projects", &json!({ "ref": project_ref }));
    resp(200, json!({ "deleted": project_ref }))
}

pub fn rotate_keys(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let doc = match owned_project(state, project_ref, &owner) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let (created_at, _, name) = meta(&doc);
    let (priv_scalar, pub_point) = crypto::gen_es256_keypair();
    let new_iat = now_secs();
    let query = json!({ "ref": project_ref });
    let patch = json!({ "$set": {
        "pubkey": b64std(&pub_point),
        "priv_enc": b64std(&crypto::seal(&state.seal_key, &priv_scalar)),
        "key_iat": new_iat,
    } });
    if let Err(e) = state.upstream.update("projects", &query, &patch) {
        return resp(
            502,
            json!({ "message": format!("failed to persist rotated key: {e}") }),
        );
    }
    resp(
        200,
        project_view(
            project_ref,
            &name,
            created_at,
            new_iat,
            &priv_scalar,
            true,
        ),
    )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn owned_project(state: &State, project_ref: &str, owner: &str) -> Result<Option<Value>, String> {
    let query = json!({ "ref": project_ref, "owner": owner });
    Ok(state.upstream.find("projects", &query)?.into_iter().next())
}

fn meta(doc: &Value) -> (u64, u64, String) {
    let created_at = doc.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0);
    let key_iat = doc
        .get("key_iat")
        .and_then(|v| v.as_u64())
        .unwrap_or(created_at);
    let name = doc
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    (created_at, key_iat, name)
}

fn b64std(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// A project's ES256 private scalar, unsealed. Projects always carry an
/// asymmetric key; `None` only on a corrupt record.
fn project_priv(state: &State, doc: &Value) -> Option<Vec<u8>> {
    let b64 = doc.get("priv_enc")?.as_str()?;
    let sealed = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    crypto::unseal(&state.seal_key, &sealed)
}

fn project_view(
    project_ref: &str,
    name: &str,
    created_at: u64,
    key_iat: u64,
    priv_scalar: &[u8],
    keys: bool,
) -> Value {
    let base = std::env::var("OXIDB_PLATFORM_BASE_URL").unwrap_or_default();
    let url = if base.is_empty() {
        json!(null)
    } else {
        json!(format!("{base}/rest/v1?db={project_ref}"))
    };
    let mut v = json!({
        "ref": project_ref, "name": name, "db": project_ref,
        "endpoint": "/rest/v1", "url": url, "isolation": "shared", "created_at": created_at,
    });
    if keys {
        let o = v.as_object_mut().unwrap();
        o.insert(
            "anon_key".into(),
            json!(mint_key(priv_scalar, project_ref, "read", key_iat)),
        );
        o.insert(
            "service_role_key".into(),
            json!(mint_key(priv_scalar, project_ref, "admin", key_iat)),
        );
    }
    v
}

/// Mint an ES256 API-key token for a role (deterministic — stable across re-mint).
fn mint_key(priv_scalar: &[u8], project_ref: &str, role: &str, iat: u64) -> String {
    let claims = Claims {
        sub: format!("{role}@{project_ref}"),
        role: role.to_string(),
        iat,
        exp: iat + KEY_EXPIRY_SECS,
    };
    crypto::encode_jwt_es256(&claims, priv_scalar).unwrap_or_default()
}

fn session_token(state: &State, email: &str) -> String {
    let now = now_secs();
    crypto::encode_jwt(
        &Claims {
            sub: email.to_string(),
            role: "admin".into(),
            iat: now,
            exp: now + SESSION_EXPIRY_SECS,
        },
        &state.platform_secret,
    )
}

fn authenticate(req: &HttpRequest, state: &State) -> Result<Claims, HttpResponse> {
    let header = req
        .headers
        .get("authorization")
        .map(|s| s.as_str())
        .unwrap_or("");
    let token = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .ok_or_else(|| {
            resp(
                401,
                json!({ "message": "missing Authorization: Bearer <platform token>" }),
            )
        })?;
    crypto::decode_jwt(token, &state.platform_secret)
        .map_err(|e| resp(401, json!({ "message": e })))
}

fn parse_body(req: &HttpRequest) -> Value {
    if req.body.is_empty() {
        return json!({});
    }
    serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}))
}

fn str_field(body: &Value, key: &str) -> Option<String> {
    body.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn gen_ref() -> String {
    use rand::Rng;
    const A: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..16)
        .map(|_| A[rng.random_range(0..A.len())] as char)
        .collect()
}

fn client_ip(req: &HttpRequest) -> String {
    req.headers
        .get("cf-connecting-ip")
        .or_else(|| req.headers.get("x-forwarded-for"))
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "global".to_string())
}

// ---------------------------------------------------------------------------
// In-memory limiters
// ---------------------------------------------------------------------------

fn signup_limiter() -> &'static Mutex<HashMap<String, (u32, u64)>> {
    static L: OnceLock<Mutex<HashMap<String, (u32, u64)>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(HashMap::new()))
}
fn signup_allowed(actor: &str) -> bool {
    let mut m = signup_limiter().lock().unwrap();
    let now = now_secs();
    let e = m.entry(actor.to_string()).or_insert((0, now));
    if now.saturating_sub(e.1) >= SIGNUP_WINDOW_SECS {
        *e = (0, now);
    }
    if e.0 >= signup_rate() {
        return false;
    }
    e.0 += 1;
    true
}

fn login_limiter() -> &'static Mutex<HashMap<String, (u32, u64)>> {
    static L: OnceLock<Mutex<HashMap<String, (u32, u64)>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(HashMap::new()))
}
fn login_locked(email: &str) -> bool {
    let mut m = login_limiter().lock().unwrap();
    match m.get(email).copied() {
        Some((fails, first)) => {
            if now_secs().saturating_sub(first) >= LOGIN_LOCKOUT_SECS {
                m.remove(email);
                false
            } else {
                fails >= LOGIN_MAX_FAILS
            }
        }
        None => false,
    }
}
fn login_record_failure(email: &str) {
    let mut m = login_limiter().lock().unwrap();
    let now = now_secs();
    let e = m.entry(email.to_string()).or_insert((0, now));
    if now.saturating_sub(e.1) >= LOGIN_LOCKOUT_SECS {
        *e = (1, now);
    } else {
        e.0 += 1;
    }
}
fn login_clear(email: &str) {
    login_limiter().lock().unwrap().remove(email);
}
