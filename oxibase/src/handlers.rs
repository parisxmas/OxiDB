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
/// Per-project resource caps written onto each new project (the plan's quota;
/// the data plane reads and enforces them). Default 5, overridable via env.
fn project_max_collections() -> usize {
    env_usize("OXIDB_PROJECT_MAX_COLLECTIONS", 5)
}
fn project_max_tables() -> usize {
    env_usize("OXIDB_PROJECT_MAX_TABLES", 5)
}
fn project_max_documents() -> usize {
    env_usize("OXIDB_PROJECT_MAX_DOCUMENTS", 10_000)
}
/// Per-project blob-storage cap in bytes (default 100 MiB; 0 = unlimited).
fn project_max_storage_bytes() -> u64 {
    std::env::var("OXIDB_PROJECT_MAX_STORAGE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(104_857_600)
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
// Developer account handlers — Google sign-in only.
//
// Developer accounts authenticate exclusively with "Sign in with Google": the
// browser obtains a Google ID token (JWT) via Google Identity Services and
// POSTs it here. We verify it against Google, then find-or-create an account
// keyed by the **verified** email. Because Google guarantees the email is
// verified and one Google identity maps to one email, this gives one account
// per person with no email-verification flow of our own and no way to spin up
// duplicate password accounts.
// ---------------------------------------------------------------------------

/// The Google OAuth **Web client ID** this deployment accepts tokens for
/// (`aud`). When unset, Google sign-in is disabled and the endpoint 501s.
fn google_client_id() -> Option<String> {
    std::env::var("OXIBASE_GOOGLE_CLIENT_ID")
        .ok()
        .filter(|s| !s.is_empty())
}

/// `GET /platform/v1/config` — public bootstrap config for the dashboard SPA:
/// which auth methods are enabled. Contains no secrets (the client ID is public
/// by design — it ships in the browser).
pub fn config() -> HttpResponse {
    resp(
        200,
        json!({
            "google_client_id": google_client_id(),
            "password_auth": false,
        }),
    )
}

struct GoogleIdentity {
    email: String,
    sub: String,
    name: Option<String>,
}

/// Pure validation of the claims Google returns for an ID token: the audience
/// must be *our* client ID, the issuer must be Google, and the email must be
/// verified. Split out from the network fetch so it is unit-testable.
fn check_google_claims(v: &Value, client_id: &str) -> Result<GoogleIdentity, String> {
    if v.get("aud").and_then(|x| x.as_str()) != Some(client_id) {
        return Err("token audience mismatch".into());
    }
    let iss = v.get("iss").and_then(|x| x.as_str()).unwrap_or("");
    if iss != "accounts.google.com" && iss != "https://accounts.google.com" {
        return Err("unexpected token issuer".into());
    }
    let verified = match v.get("email_verified") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    };
    if !verified {
        return Err("Google email is not verified".into());
    }
    let email = v
        .get("email")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .ok_or("no email in Google token")?
        .to_lowercase();
    let sub = v
        .get("sub")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let name = v.get("name").and_then(|x| x.as_str()).map(String::from);
    Ok(GoogleIdentity { email, sub, name })
}

/// Verify a Google ID token by asking Google's `tokeninfo` endpoint (Google
/// checks the RS256 signature + expiry against its rotating keys), then apply
/// [`check_google_claims`]. Suitable for a developer console's low sign-in
/// volume; local JWKS verification is the path if this ever needs to scale.
fn verify_google_credential(credential: &str, client_id: &str) -> Result<GoogleIdentity, String> {
    let url = format!("https://oauth2.googleapis.com/tokeninfo?id_token={credential}");
    let body = match ureq::get(&url).call() {
        Ok(r) => r.into_string().map_err(|e| e.to_string())?,
        // Google returns 400 for a bad/expired token.
        Err(ureq::Error::Status(_, _)) => return Err("invalid Google credential".into()),
        Err(e) => return Err(format!("could not reach Google to verify: {e}")),
    };
    let v: Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    check_google_claims(&v, client_id)
}

/// `POST /platform/v1/auth/google` — developer sign-in with Google. Body:
/// `{ "credential": "<Google ID token>" }`. Find-or-create by verified email.
pub fn auth_google(req: &HttpRequest, state: &State) -> HttpResponse {
    let Some(client_id) = google_client_id() else {
        return resp(501, json!({ "message": "Google sign-in is not configured" }));
    };
    let body = parse_body(req);
    let Some(credential) = str_field(&body, "credential") else {
        return resp(400, json!({ "message": "credential (Google ID token) required" }));
    };
    let ident = match verify_google_credential(&credential, &client_id) {
        Ok(i) => i,
        Err(e) => return resp(401, json!({ "message": e })),
    };

    // Existing account → sign in. Identity is already proven by Google, so no
    // rate limit on the login path.
    match state.upstream.find("accounts", &json!({ "email": ident.email })) {
        Ok(existing) if !existing.is_empty() => {
            return resp(
                200,
                json!({ "account": { "email": ident.email }, "token": session_token(state, &ident.email) }),
            );
        }
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
        _ => {}
    }

    // New account — apply the signup guards (per-IP rate limit, optional invite
    // code, global account ceiling).
    if !signup_allowed(&client_ip(req)) {
        return resp(429, json!({ "message": "signup rate limit exceeded; slow down" }));
    }
    if let Some(code) = signup_code() {
        if str_field(&body, "code").as_deref() != Some(code.as_str()) {
            return resp(403, json!({ "message": "a valid invite code is required" }));
        }
    }
    if state.upstream.count("accounts", &json!({})).unwrap_or(0) >= max_accounts() {
        return resp(403, json!({ "message": "signups are closed" }));
    }
    let doc = json!({
        "email": ident.email,
        "provider": "google",
        "google_sub": ident.sub,
        "name": ident.name,
        "created_at": now_secs(),
    });
    if let Err(e) = state.upstream.insert("accounts", &doc) {
        return resp(502, json!({ "message": format!("upstream: {e}") }));
    }
    resp(
        201,
        json!({ "account": { "email": ident.email }, "token": session_token(state, &ident.email) }),
    )
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
    // A friendly, globally-unique slug for path-based addressing
    // (`<host>/<slug>/rest/v1/…`). Prefer a caller-supplied `slug`, else derive
    // one from the name, else fall back to the ref.
    let slug_base = str_field(&body, "slug")
        .and_then(|s| slugify(&s))
        .or_else(|| slugify(&name))
        .unwrap_or_else(|| project_ref.clone());
    let slug = unique_slug(state, &slug_base);
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
        "slug": slug,
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
        // Per-project resource quotas (plan-based). The data plane reads these
        // from the project row and enforces them at collection/table/document
        // creation.
        "max_collections": project_max_collections(),
        "max_tables": project_max_tables(),
        "max_documents": project_max_documents(),
        "max_storage_bytes": project_max_storage_bytes(),
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
            &slug,
            &name,
            created_at,
            created_at,
            &priv_scalar,
            true,
            project_max_collections() as u64,
            project_max_tables() as u64,
            project_max_documents() as u64,
            project_max_storage_bytes(),
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
                .map(|d| json!({ "ref": d.get("ref"), "slug": d.get("slug"), "name": d.get("name"), "created_at": d.get("created_at") }))
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
    let slug = doc_slug(&doc, project_ref);
    let priv_scalar = match project_priv(state, &doc) {
        Some(s) => s,
        None => return resp(500, json!({ "message": "unseal failed" })),
    };
    let (mc, mt, md, ms) = doc_limits(&doc);
    resp(
        200,
        project_view(
            project_ref, &slug, &name, created_at, key_iat, &priv_scalar, true, mc, mt, md, ms,
        ),
    )
}

/// Public JWKS for a project: its ES256 verification key as a JWK set. No auth —
/// a JWKS is meant to be world-readable so any party can verify the project's
/// tokens with the public key alone. Legacy HS256 projects have no public key
/// and return an empty set.
pub fn project_jwks(state: &State, project_ref: &str) -> HttpResponse {
    let doc = match project_by_ref(state, project_ref) {
        Ok(d) => d,
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let Some(doc) = doc else {
        return resp(404, json!({ "message": "project not found" }));
    };
    let project_ref = doc.get("ref").and_then(|v| v.as_str()).unwrap_or(project_ref);
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
    // Normalize to the canonical ref so a caller using the slug and one using the
    // ref address the same user directory / sessions.
    let project_ref = pdoc.get("ref").and_then(|v| v.as_str()).unwrap_or(project_ref);
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
    // With email configured, new users must verify their address before they
    // can sign in (the Supabase model). Without SMTP the old immediate-session
    // behavior is kept.
    let verify = state.mailer.is_some();
    let mut doc = json!({
        "project_ref": project_ref,
        "email": email,
        "pw_hash": pw_hash,
        "created_at": now_secs(),
        "verified": !verify,
    });
    let mut token_plain = String::new();
    if verify {
        let (plain, hash, exp) = one_time_token(24 * 3600);
        token_plain = plain;
        let obj = doc.as_object_mut().unwrap();
        obj.insert("verify_hash".into(), json!(hash));
        obj.insert("verify_exp".into(), json!(exp));
    }
    if let Err(e) = state.upstream.insert("users", &doc) {
        return resp(502, json!({ "message": format!("upstream: {e}") }));
    }
    if verify {
        send_verification_email(state, project_ref, &email, &token_plain);
        return resp(
            201,
            json!({
                "user": { "email": email },
                "verification_required": true,
                "message": "check your inbox — a verification link was sent"
            }),
        );
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
    let project_ref = pdoc.get("ref").and_then(|v| v.as_str()).unwrap_or(project_ref);
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
    // Rows without a `verified` field predate email verification — treated as
    // verified (grandfathered).
    let unverified = user
        .as_ref()
        .and_then(|u| u.get("verified"))
        .and_then(|v| v.as_bool())
        == Some(false);
    if unverified {
        return resp(
            403,
            json!({ "message": "email not verified — check your inbox (or request a new link via /auth/resend)" }),
        );
    }
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
    let pdoc = match project_by_ref(state, project_ref) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let project_ref = pdoc.get("ref").and_then(|v| v.as_str()).unwrap_or(project_ref);
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
    match issue_session(state, &pdoc, project_ref, email) {
        Some((token, new_refresh)) => {
            resp(200, json!({ "token": token, "refresh_token": new_refresh }))
        }
        None => resp(500, json!({ "message": "failed to mint token" })),
    }
}

/// Look up a project by ref alone (no owner filter) — end-user auth is public.
/// Look up a project by its ref **or** slug — so every project-scoped URL
/// (`/projects/{ref-or-slug}/…`, path-based data access) accepts either.
fn project_by_ref(state: &State, ident: &str) -> Result<Option<Value>, String> {
    Ok(state
        .upstream
        .find(
            "projects",
            &json!({ "$or": [{ "ref": ident }, { "slug": ident }] }),
        )?
        .into_iter()
        .next())
}

/// The project's slug, falling back to its ref for pre-slug records.
fn doc_slug(doc: &Value, project_ref: &str) -> String {
    doc.get("slug")
        .and_then(|v| v.as_str())
        .unwrap_or(project_ref)
        .to_string()
}

/// A URL-safe slug from a project name: lowercase alphanumerics, other runs
/// collapsed to single dashes. `None` if nothing usable remains.
fn slugify(name: &str) -> Option<String> {
    let mut s = String::new();
    let mut prev_dash = false;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            s.push(c.to_ascii_lowercase());
            prev_dash = false;
        } else if !s.is_empty() && !prev_dash {
            s.push('-');
            prev_dash = true;
        }
    }
    let s = s.trim_end_matches('-').to_string();
    (!s.is_empty()).then_some(s)
}

/// Make a slug globally unique and not a reserved path word (append `-2`, `-3`…).
fn unique_slug(state: &State, base: &str) -> String {
    const RESERVED: &[&str] = &[
        "api", "rest", "hello", "metrics", "health", "platform", "v1", "oxibase", "postgres",
        "oxidb", "sql", "auth",
    ];
    let mut candidate = base.to_string();
    let mut n = 2u32;
    loop {
        let taken = RESERVED.contains(&candidate.as_str())
            || state
                .upstream
                .count("projects", &json!({ "slug": candidate }))
                .unwrap_or(0)
                > 0;
        if !taken {
            return candidate;
        }
        candidate = format!("{base}-{n}");
        n += 1;
    }
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
    let (mc, mt, md, ms) = doc_limits(&doc);
    let slug = doc_slug(&doc, project_ref);
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
            &slug,
            &name,
            created_at,
            new_iat,
            &priv_scalar,
            true,
            mc,
            mt,
            md,
            ms,
        ),
    )
}

/// `PATCH /platform/v1/projects/{ref}/limits` — update a project's resource
/// caps (owner only). Body: any of `{ max_collections, max_tables,
/// max_documents }` (`0` = unlimited). The data plane reads these per request,
/// so the change takes effect on the next request without a restart.
pub fn update_limits(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let doc = match owned_project(state, project_ref, &owner) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let body = parse_body(req);
    let (cur_mc, cur_mt, cur_md, cur_ms) = doc_limits(&doc);
    let field = |key: &str, cur: u64| body.get(key).and_then(|v| v.as_u64()).unwrap_or(cur);
    let mc = field("max_collections", cur_mc);
    let mt = field("max_tables", cur_mt);
    let md = field("max_documents", cur_md);
    let ms = field("max_storage_bytes", cur_ms);
    const CEIL: u64 = 10_000_000;
    // Storage is bytes, not a count — allow up to 100 GiB.
    if mc > 100_000 || mt > 100_000 || md > CEIL || ms > 107_374_182_400 {
        return resp(400, json!({ "message": "limit out of range (0 = unlimited)" }));
    }
    let query = json!({ "ref": project_ref });
    let patch = json!({ "$set": { "max_collections": mc, "max_tables": mt, "max_documents": md, "max_storage_bytes": ms } });
    if let Err(e) = state.upstream.update("projects", &query, &patch) {
        return resp(
            502,
            json!({ "message": format!("failed to update limits: {e}") }),
        );
    }
    let (created_at, key_iat, name) = meta(&doc);
    let slug = doc_slug(&doc, project_ref);
    let priv_scalar = match project_priv(state, &doc) {
        Some(s) => s,
        None => return resp(500, json!({ "message": "unseal failed" })),
    };
    resp(
        200,
        project_view(
            project_ref, &slug, &name, created_at, key_iat, &priv_scalar, true, mc, mt, md, ms,
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

/// A project's resource caps `(max_collections, max_tables, max_documents)`,
/// from its row (falling back to the configured defaults for rows created
/// before a given quota existed).
fn doc_limits(doc: &Value) -> (u64, u64, u64, u64) {
    let get = |key: &str, default: u64| doc.get(key).and_then(|v| v.as_u64()).unwrap_or(default);
    (
        get("max_collections", project_max_collections() as u64),
        get("max_tables", project_max_tables() as u64),
        get("max_documents", project_max_documents() as u64),
        get("max_storage_bytes", project_max_storage_bytes()),
    )
}

#[allow(clippy::too_many_arguments)]
fn project_view(
    project_ref: &str,
    slug: &str,
    name: &str,
    created_at: u64,
    key_iat: u64,
    priv_scalar: &[u8],
    keys: bool,
    max_collections: u64,
    max_tables: u64,
    max_documents: u64,
    max_storage_bytes: u64,
) -> Value {
    let base = std::env::var("OXIDB_PLATFORM_BASE_URL").unwrap_or_default();
    // Path-based addressing is the friendly default: `<host>/<slug>/rest/v1`.
    let url = if base.is_empty() {
        json!(null)
    } else {
        json!(format!("{base}/{slug}/rest/v1"))
    };
    let mut v = json!({
        "ref": project_ref, "slug": slug, "name": name, "db": project_ref,
        "endpoint": format!("/{slug}/rest/v1"), "url": url, "isolation": "shared", "created_at": created_at,
        "max_collections": max_collections, "max_tables": max_tables, "max_documents": max_documents,
        "max_storage_bytes": max_storage_bytes,
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

#[cfg(test)]
mod tests {
    use super::{check_google_claims, slugify};
    use serde_json::json;

    #[test]
    fn google_claims_accepts_verified_matching_audience() {
        let v = json!({
            "aud": "cid.apps.googleusercontent.com",
            "iss": "https://accounts.google.com",
            "email": "Dev@Example.com",
            "email_verified": "true",
            "sub": "1234567890",
            "name": "Dev"
        });
        let id = check_google_claims(&v, "cid.apps.googleusercontent.com").unwrap();
        assert_eq!(id.email, "dev@example.com"); // lowercased
        assert_eq!(id.sub, "1234567890");
    }

    #[test]
    fn google_claims_rejects_wrong_audience_unverified_or_bad_issuer() {
        let base = json!({
            "aud": "cid.apps.googleusercontent.com",
            "iss": "https://accounts.google.com",
            "email": "dev@example.com",
            "email_verified": true,
            "sub": "1"
        });
        // audience for a different client
        assert!(check_google_claims(&base, "someone-else").is_err());
        // unverified email
        let mut unv = base.clone();
        unv["email_verified"] = json!(false);
        assert!(check_google_claims(&unv, "cid.apps.googleusercontent.com").is_err());
        // forged issuer
        let mut iss = base.clone();
        iss["iss"] = json!("evil.example.com");
        assert!(check_google_claims(&iss, "cid.apps.googleusercontent.com").is_err());
    }

    #[test]
    fn slugify_basics() {
        assert_eq!(slugify("My Cool App").as_deref(), Some("my-cool-app"));
        assert_eq!(slugify("  Hello,  World!  ").as_deref(), Some("hello-world"));
        assert_eq!(slugify("Aa_Bb-Cc.99").as_deref(), Some("aa-bb-cc-99"));
        assert_eq!(slugify("test123").as_deref(), Some("test123"));
        assert_eq!(slugify("---"), None);
        assert_eq!(slugify(""), None);
        assert_eq!(slugify("日本語"), None); // no ascii alphanumerics → nothing usable
    }
}

// ---------------------------------------------------------------------------
// Email verification + password reset (SMTP via the deployment's mail server)
// ---------------------------------------------------------------------------

/// A one-time token: `(plaintext, sha256-hex, expiry)`. Only the hash is
/// stored, so a metadata-store leak never exposes usable links.
fn one_time_token(ttl_secs: u64) -> (String, String, u64) {
    let bytes: [u8; 32] = rand::random();
    let plain: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let hash = crypto::sha256_hex(plain.as_bytes());
    (plain, hash, now_secs() + ttl_secs)
}

/// The public origin links are built on (`OXIDB_PLATFORM_BASE_URL`).
fn public_base() -> String {
    std::env::var("OXIDB_PLATFORM_BASE_URL")
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_string()
}

fn send_verification_email(state: &State, project_ref: &str, email: &str, token: &str) {
    let Some(mailer) = &state.mailer else { return };
    let link = format!(
        "{}/platform/v1/projects/{}/auth/verify?token={}",
        public_base(),
        project_ref,
        token
    );
    mailer.send_async(
        email.to_string(),
        "Verify your email address".into(),
        format!(
            "Welcome!\n\nConfirm this address to activate your account:\n\n  {link}\n\nThe link is valid for 24 hours. If you didn't sign up, ignore this message.\n"
        ),
    );
}

/// `GET /platform/v1/projects/{ref}/auth/verify?token=…` — the link from the
/// verification email. Responds with a small human-readable HTML page.
pub fn end_user_verify(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let token = req
        .query
        .split('&')
        .find_map(|kv| kv.strip_prefix("token="))
        .unwrap_or("");
    let page = |status: u16, title: &str, detail: &str| HttpResponse {
        status,
        status_text: if status == 200 { "OK" } else { "Bad Request" },
        content_type: "text/html; charset=utf-8".into(),
        headers: Vec::new(),
        body: format!(
            "<!doctype html><meta charset=utf-8><title>{title}</title><body style=\"font-family:system-ui;display:grid;place-items:center;min-height:90vh\"><div style=\"text-align:center\"><h2>{title}</h2><p>{detail}</p></div>"
        )
        .into_bytes(),
        content_length_override: None,
    };
    if token.is_empty() {
        return page(400, "Missing token", "The verification link is incomplete.");
    }
    let hash = crypto::sha256_hex(token.as_bytes());
    let user = match state
        .upstream
        .find("users", &json!({ "project_ref": project_ref, "verify_hash": hash }))
    {
        Ok(mut v) => v.pop(),
        Err(_) => return page(502, "Temporarily unavailable", "Please try again shortly."),
    };
    let Some(user) = user else {
        return page(400, "Invalid link", "This verification link is unknown or already used.");
    };
    if now_secs() > user.get("verify_exp").and_then(|v| v.as_u64()).unwrap_or(0) {
        return page(400, "Link expired", "Request a new verification email and try again.");
    }
    let email = user.get("email").and_then(|v| v.as_str()).unwrap_or_default();
    let _ = state.upstream.update(
        "users",
        &json!({ "project_ref": project_ref, "email": email }),
        &json!({ "$set": { "verified": true }, "$unset": { "verify_hash": "", "verify_exp": "" } }),
    );
    page(200, "Email verified ✓", "Your address is confirmed — you can sign in now.")
}

/// `POST /platform/v1/projects/{ref}/auth/resend` — send a fresh verification
/// link. Same per-IP rate limit as signup (it sends email on demand).
pub fn end_user_resend(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    if state.mailer.is_none() {
        return resp(501, json!({ "message": "email is not configured on this server" }));
    }
    let actor = client_ip(req);
    if !signup_allowed(&actor) {
        return resp(429, json!({ "message": "rate limit exceeded; slow down" }));
    }
    let body = parse_body(req);
    let Some(email) = str_field(&body, "email") else {
        return resp(400, json!({ "message": "email required" }));
    };
    // Always 200 — never confirm whether an address exists.
    let neutral = resp(200, json!({ "message": "if that address is registered, a link was sent" }));
    let Ok(Some(pdoc)) = project_by_ref(state, project_ref) else { return neutral };
    let project_ref = pdoc.get("ref").and_then(|v| v.as_str()).unwrap_or(project_ref);
    let user = state
        .upstream
        .find("users", &json!({ "project_ref": project_ref, "email": email }))
        .ok()
        .and_then(|mut v| v.pop());
    if let Some(user) = user
        && user.get("verified").and_then(|v| v.as_bool()) == Some(false)
    {
        let (plain, hash, exp) = one_time_token(24 * 3600);
        let _ = state.upstream.update(
            "users",
            &json!({ "project_ref": project_ref, "email": email }),
            &json!({ "$set": { "verify_hash": hash, "verify_exp": exp } }),
        );
        send_verification_email(state, project_ref, &email, &plain);
    }
    neutral
}

/// `POST /platform/v1/projects/{ref}/auth/recover` — start a password reset.
/// Always 200 (no user enumeration); sends a link to the dashboard's reset
/// page when the address exists.
pub fn end_user_recover(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    if state.mailer.is_none() {
        return resp(501, json!({ "message": "email is not configured on this server" }));
    }
    let actor = client_ip(req);
    if !signup_allowed(&actor) {
        return resp(429, json!({ "message": "rate limit exceeded; slow down" }));
    }
    let body = parse_body(req);
    let Some(email) = str_field(&body, "email") else {
        return resp(400, json!({ "message": "email required" }));
    };
    let neutral = resp(200, json!({ "message": "if that address is registered, a reset link was sent" }));
    let Ok(Some(pdoc)) = project_by_ref(state, project_ref) else { return neutral };
    let project_ref = pdoc.get("ref").and_then(|v| v.as_str()).unwrap_or(project_ref);
    let exists = state
        .upstream
        .find("users", &json!({ "project_ref": project_ref, "email": email }))
        .ok()
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if exists {
        let (plain, hash, exp) = one_time_token(3600);
        let _ = state.upstream.update(
            "users",
            &json!({ "project_ref": project_ref, "email": email }),
            &json!({ "$set": { "reset_hash": hash, "reset_exp": exp } }),
        );
        if let Some(mailer) = &state.mailer {
            let link = format!("{}/reset?ref={}&token={}", public_base(), project_ref, plain);
            mailer.send_async(
                email.clone(),
                "Reset your password".into(),
                format!(
                    "A password reset was requested for this address.\n\nSet a new password here:\n\n  {link}\n\nThe link is valid for 1 hour. If you didn't request this, ignore this message — your password is unchanged.\n"
                ),
            );
        }
    }
    neutral
}

/// `POST /platform/v1/projects/{ref}/auth/reset` — complete a password reset:
/// `{ token, password }`. Consumes the token, revokes every session (refresh
/// token) of the user, and marks the address verified (the link proves it).
pub fn end_user_reset(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let body = parse_body(req);
    let (Some(token), Some(password)) = (str_field(&body, "token"), str_field(&body, "password"))
    else {
        return resp(400, json!({ "message": "token and password required" }));
    };
    if password.len() < MIN_PASSWORD_LEN {
        return resp(
            400,
            json!({ "message": format!("password must be at least {MIN_PASSWORD_LEN} characters") }),
        );
    }
    let Ok(Some(pdoc)) = project_by_ref(state, project_ref) else {
        return resp(404, json!({ "message": "project not found" }));
    };
    let project_ref = pdoc.get("ref").and_then(|v| v.as_str()).unwrap_or(project_ref);
    let hash = crypto::sha256_hex(token.as_bytes());
    let user = match state
        .upstream
        .find("users", &json!({ "project_ref": project_ref, "reset_hash": hash }))
    {
        Ok(mut v) => v.pop(),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let Some(user) = user else {
        return resp(400, json!({ "message": "invalid or already-used reset link" }));
    };
    if now_secs() > user.get("reset_exp").and_then(|v| v.as_u64()).unwrap_or(0) {
        return resp(400, json!({ "message": "reset link expired — request a new one" }));
    }
    let email = user.get("email").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let pw_hash = match crypto::hash_password(&password) {
        Ok(h) => h,
        Err(e) => return resp(500, json!({ "message": e })),
    };
    if let Err(e) = state.upstream.update(
        "users",
        &json!({ "project_ref": project_ref, "email": email }),
        &json!({ "$set": { "pw_hash": pw_hash, "verified": true }, "$unset": { "reset_hash": "", "reset_exp": "" } }),
    ) {
        return resp(502, json!({ "message": format!("upstream: {e}") }));
    }
    // A reset invalidates every live session of the account.
    let _ = state
        .upstream
        .delete("refresh_tokens", &json!({ "project_ref": project_ref, "email": email }));
    resp(200, json!({ "message": "password updated — you can sign in now" }))
}

// ---------------------------------------------------------------------------
// User management (developer console — owner-authenticated)
// ---------------------------------------------------------------------------

/// `GET /platform/v1/projects/{ref}/users` — the project's end users.
pub fn list_users(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    match owned_project(state, project_ref, &owner) {
        Ok(Some(_)) => {}
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    }
    match state
        .upstream
        .find("users", &json!({ "project_ref": project_ref }))
    {
        Ok(users) => {
            let out: Vec<Value> = users
                .iter()
                .map(|u| {
                    json!({
                        "email": u.get("email"),
                        "created_at": u.get("created_at"),
                        // Absent field = pre-verification row = verified.
                        "verified": u.get("verified").and_then(|v| v.as_bool()).unwrap_or(true),
                    })
                })
                .collect();
            resp(200, json!(out))
        }
        Err(e) => resp(502, json!({ "message": format!("upstream: {e}") })),
    }
}

/// Shared owner gate for the per-user admin endpoints.
fn owned_user<'a>(
    req: &HttpRequest,
    state: &State,
    project_ref: &str,
    email: &'a str,
) -> Result<&'a str, HttpResponse> {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return Err(r),
    };
    match owned_project(state, project_ref, &owner) {
        Ok(Some(_)) => {}
        Ok(None) => return Err(resp(404, json!({ "message": "project not found" }))),
        Err(e) => return Err(resp(502, json!({ "message": format!("upstream: {e}") }))),
    }
    let exists = state
        .upstream
        .find("users", &json!({ "project_ref": project_ref, "email": email }))
        .ok()
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if !exists {
        return Err(resp(404, json!({ "message": "user not found" })));
    }
    Ok(email)
}

/// `DELETE /platform/v1/projects/{ref}/users/{email}` — remove a user and
/// their sessions.
pub fn delete_user(req: &HttpRequest, state: &State, project_ref: &str, email: &str) -> HttpResponse {
    let email = match owned_user(req, state, project_ref, email) {
        Ok(e) => e,
        Err(r) => return r,
    };
    let _ = state
        .upstream
        .delete("refresh_tokens", &json!({ "project_ref": project_ref, "email": email }));
    match state
        .upstream
        .delete("users", &json!({ "project_ref": project_ref, "email": email }))
    {
        Ok(_) => resp(200, json!({ "deleted": email })),
        Err(e) => resp(502, json!({ "message": format!("upstream: {e}") })),
    }
}

/// `POST /platform/v1/projects/{ref}/users/{email}/password` — operator sets a
/// user's password (`{ password }`). Revokes the user's sessions.
pub fn admin_set_user_password(
    req: &HttpRequest,
    state: &State,
    project_ref: &str,
    email: &str,
) -> HttpResponse {
    let email = match owned_user(req, state, project_ref, email) {
        Ok(e) => e,
        Err(r) => return r,
    };
    let body = parse_body(req);
    let Some(password) = str_field(&body, "password") else {
        return resp(400, json!({ "message": "password required" }));
    };
    if password.len() < MIN_PASSWORD_LEN {
        return resp(
            400,
            json!({ "message": format!("password must be at least {MIN_PASSWORD_LEN} characters") }),
        );
    }
    let pw_hash = match crypto::hash_password(&password) {
        Ok(h) => h,
        Err(e) => return resp(500, json!({ "message": e })),
    };
    if let Err(e) = state.upstream.update(
        "users",
        &json!({ "project_ref": project_ref, "email": email }),
        &json!({ "$set": { "pw_hash": pw_hash } }),
    ) {
        return resp(502, json!({ "message": format!("upstream: {e}") }));
    }
    let _ = state
        .upstream
        .delete("refresh_tokens", &json!({ "project_ref": project_ref, "email": email }));
    resp(200, json!({ "message": "password updated" }))
}

/// `POST /platform/v1/projects/{ref}/users/{email}/verify` — operator marks a
/// user's address verified (support path when email is unreachable).
pub fn admin_verify_user(
    req: &HttpRequest,
    state: &State,
    project_ref: &str,
    email: &str,
) -> HttpResponse {
    let email = match owned_user(req, state, project_ref, email) {
        Ok(e) => e,
        Err(r) => return r,
    };
    match state.upstream.update(
        "users",
        &json!({ "project_ref": project_ref, "email": email }),
        &json!({ "$set": { "verified": true }, "$unset": { "verify_hash": "", "verify_exp": "" } }),
    ) {
        Ok(_) => resp(200, json!({ "message": "verified" })),
        Err(e) => resp(502, json!({ "message": format!("upstream: {e}") })),
    }
}

// ---------------------------------------------------------------------------
// Per-project request logs (developer console)
// ---------------------------------------------------------------------------

/// `GET /platform/v1/projects/{ref}/logs?limit=100` — the project's recent
/// data-plane requests, read from the shared MessagePack log sink in the
/// default database (`_msgpack_logs`, written when the data plane runs with
/// `OXIDB_MSGPACK_PORT`/`OXIDB_MSGPACK_ADDR`). Owner-authenticated; filtered
/// to rows whose logged `db` is this project's ref or slug.
pub fn project_logs(req: &HttpRequest, state: &State, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req, state) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let doc = match owned_project(state, project_ref, &owner) {
        Ok(Some(d)) => d,
        Ok(None) => return resp(404, json!({ "message": "project not found" })),
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let slug = doc_slug(&doc, project_ref);
    let limit: u64 = req
        .query
        .split('&')
        .find_map(|kv| kv.strip_prefix("limit="))
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
        .min(500);
    let rows = match state.upstream.find_sorted_in(
        "oxidb",
        "_msgpack_logs",
        &json!({ "db": { "$in": [project_ref, slug] } }),
        &json!({ "ts": -1 }),
        limit,
    ) {
        Ok(r) => r,
        Err(e) if e.contains("not found") => Vec::new(), // sink not created yet
        Err(e) => return resp(502, json!({ "message": format!("upstream: {e}") })),
    };
    let out: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "ts": r.get("ts"),
                "method": r.get("method"),
                "path": r.get("path"),
                "status": r.get("status").and_then(|v| v.as_str()).and_then(|s| s.parse::<u16>().ok()),
                "ms": r.get("ms").and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok()),
            })
        })
        .collect();
    resp(200, json!(out))
}
