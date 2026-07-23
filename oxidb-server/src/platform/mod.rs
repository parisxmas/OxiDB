//! OxiBase control plane (ADR-0020) — multi-tenant provisioning.
//!
//! A thin `/platform/v1/*` route family that turns "a developer signs up" into
//! "here is an isolated database + two API keys". It reuses the data-plane
//! parts wholesale: `DatabaseManager` (tenant isolation, ADR-0012), `jwt`
//! (accounts, key signing), `crypto` (at-rest secret encryption), and the
//! `/rest/v1` surface (ADR-0019) a provisioned project exposes.
//!
//! Off by default; enabled with `OXIDB_PLATFORM=1` **and** a
//! `OXIDB_PLATFORM_SECRET` (the *platform master secret* that signs developer
//! sessions). Each project gets its **own** `jwt_secret`, stored AES-GCM
//! encrypted in the `_oxibase` system database — so a leaked project key is
//! blast-radius-limited to one tenant (Supabase's model).
//!
//! Two distinct secrets (ADR-0021 boundary): `OXIDB_PLATFORM_SECRET` signs
//! developer sessions (control-plane only); `OXIDB_SEAL_KEY` seals/unseals
//! per-project secrets. The **data-plane hook** [`project_secret`] unseals with
//! the seal key **alone** — it never needs the session-signing master secret.
//! For the single-binary skeleton the seal key falls back to the master secret
//! when `OXIDB_SEAL_KEY` is unset.
//!
//! Key roles: `anon` = a `read` JWT (safe in a browser); `service_role` =
//! an `admin` JWT (bypasses rules — server-side only). Both are minted with a
//! fixed `iat` (the project's creation time) so they are stable across reads.
//!
//! Endpoints:
//! ```text
//! POST   /platform/v1/signup                       {email, password} -> {account, token}
//! POST   /platform/v1/login                         {email, password} -> {token}
//! POST   /platform/v1/projects   [platform JWT]     {name?} -> {ref, anon_key, service_role_key, …}
//! GET    /platform/v1/projects   [platform JWT]     -> [{ref, name, created_at}]
//! GET    /platform/v1/projects/{ref} [platform JWT] -> {ref, …, anon_key, service_role_key}
//! DELETE /platform/v1/projects/{ref} [platform JWT] -> {deleted}
//! POST   /platform/v1/projects/{ref}/keys/rotate [platform JWT] -> fresh keys (old ones die)
//! ```
//!
//! Hardening: signup enforces a minimum password length, a per-actor rate limit
//! (forwarded client IP, `OXIDB_PLATFORM_SIGNUP_RATE`), an optional invite code
//! (`OXIDB_PLATFORM_SIGNUP_CODE`), and a global account ceiling
//! (`OXIDB_PLATFORM_MAX_ACCOUNTS`); login has a per-email brute-force lockout;
//! projects-per-account is capped (`OXIDB_PLATFORM_MAX_PROJECTS`); per-project
//! secrets are cached and the cache is invalidated on rotate/delete so a revoked
//! key can never keep verifying.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use oxidb::{DatabaseManager, EncryptionKey, OxiDb};
use rand::Rng;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::jwt::{self, Claims};
use crate::s3::http::{HttpRequest, HttpResponse};

const PLATFORM_DB_DIR: &str = "_oxibase";
const PROJECTS: &str = "projects";
/// API keys live ~10 years (they are long-lived, rotate-on-demand credentials).
/// Developer *session* expiry is handled by `jwt::login` (24h) — sessions are
/// short-lived by design, API keys are not.
const KEY_EXPIRY_SECS: u64 = 10 * 365 * 86_400;
/// Minimum developer password length at signup.
const MIN_PASSWORD_LEN: usize = 8;
/// Failed logins per email before a temporary lockout (brute-force guard).
const LOGIN_MAX_FAILS: u32 = 5;
/// How long an email stays locked out after `LOGIN_MAX_FAILS` failures.
const LOGIN_LOCKOUT_SECS: u64 = 300;
/// Sliding window for the signup rate limit.
const SIGNUP_WINDOW_SECS: u64 = 60;

/// Cap on projects a single account may provision — a resource-exhaustion guard
/// for the `shared` isolation model. Overridable via `OXIDB_PLATFORM_MAX_PROJECTS`.
fn max_projects() -> usize {
    std::env::var("OXIDB_PLATFORM_MAX_PROJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
}

/// Hard ceiling on total developer accounts (backstop against unbounded growth
/// even under the rate limit). Overridable via `OXIDB_PLATFORM_MAX_ACCOUNTS`.
fn max_accounts() -> usize {
    std::env::var("OXIDB_PLATFORM_MAX_ACCOUNTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000)
}

/// Signups allowed per actor per [`SIGNUP_WINDOW_SECS`]. Overridable via
/// `OXIDB_PLATFORM_SIGNUP_RATE`.
fn signup_rate() -> u32 {
    std::env::var("OXIDB_PLATFORM_SIGNUP_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

/// An optional invite code that gates signup. When set, a signup body must
/// carry a matching `code`. Unset ⇒ open signup (still rate-limited + capped).
fn signup_code() -> Option<String> {
    std::env::var("OXIDB_PLATFORM_SIGNUP_CODE")
        .ok()
        .filter(|s| !s.is_empty())
}

// ---------------------------------------------------------------------------
// Enablement + secrets
// ---------------------------------------------------------------------------

pub fn enabled() -> bool {
    matches!(
        std::env::var("OXIDB_PLATFORM")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The platform master secret (signs developer sessions). `None` disables the
/// control plane's auth entirely.
fn master_secret() -> Option<String> {
    std::env::var("OXIDB_PLATFORM_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
}

/// The AES-GCM key that seals per-project secrets. Prefers a **dedicated**
/// `OXIDB_SEAL_KEY`, so the data plane can unseal a project secret without ever
/// holding the session-signing master secret (ADR-0021's boundary) — falling
/// back to deriving from the master secret for the single-binary skeleton.
/// `None` when neither is configured.
fn seal_key() -> Option<Arc<EncryptionKey>> {
    let material = std::env::var("OXIDB_SEAL_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(master_secret)?;
    Some(derive_key(&material))
}

/// Derive a 32-byte AES key from arbitrary key material (SHA-256).
fn derive_key(material: &str) -> Arc<EncryptionKey> {
    let digest = Sha256::digest(material.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    EncryptionKey::from_bytes(&key)
}

/// The `_oxibase` system database, opened once from the manager's data dir.
/// It is intentionally outside `DatabaseManager`'s registry (a `_`-prefixed
/// system store, not a user database).
fn platform_db(mgr: &DatabaseManager) -> Option<Arc<OxiDb>> {
    static DB: OnceLock<Option<Arc<OxiDb>>> = OnceLock::new();
    DB.get_or_init(|| {
        let dir = mgr.data_dir().join(PLATFORM_DB_DIR);
        std::fs::create_dir_all(&dir).ok()?;
        OxiDb::open(&dir).ok().map(Arc::new)
    })
    .clone()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Data-plane hook: resolve a project's JWT secret
// ---------------------------------------------------------------------------

/// Cache of `ref → decrypted jwt_secret`, so a project's secret is not read and
/// AES-decrypted from `_oxibase` on *every* authenticated data-plane request
/// (a real per-request cost / mild DoS surface). Invalidated on rotate/delete.
fn secret_cache() -> &'static Mutex<HashMap<String, String>> {
    static CACHE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Drop a project's cached secret — called after rotation or deletion so a
/// stale (revoked) secret can never keep verifying tokens.
fn invalidate_secret(db_ref: &str) {
    secret_cache().lock().unwrap().remove(db_ref);
}

/// The per-project JWT secret for `db_ref`, if it names an OxiBase project.
/// The REST listener calls this so a request to `?db=<ref>` is verified with
/// that project's secret rather than the global `OXIDB_JWT_SECRET`.
pub fn project_secret(mgr: &DatabaseManager, db_ref: &str) -> Option<String> {
    if let Some(cached) = secret_cache().lock().unwrap().get(db_ref).cloned() {
        return Some(cached);
    }
    // The data-plane hook (ADR-0021): unseal with the seal key alone — it never
    // needs the master secret that signs developer sessions.
    let pdb = platform_db(mgr)?;
    let doc = pdb.find_one(PROJECTS, &json!({ "ref": db_ref })).ok()??;
    let sealed_b64 = doc.get("secret_enc")?.as_str()?;
    let sealed = base64::engine::general_purpose::STANDARD
        .decode(sealed_b64)
        .ok()?;
    let plain = seal_key()?.decrypt(&sealed).ok()?;
    let secret = String::from_utf8(plain).ok()?;
    secret_cache()
        .lock()
        .unwrap()
        .insert(db_ref.to_string(), secret.clone());
    Some(secret)
}

// ---------------------------------------------------------------------------
// Login brute-force limiter (in-memory, per email)
// ---------------------------------------------------------------------------

fn login_limiter() -> &'static Mutex<HashMap<String, (u32, u64)>> {
    static L: OnceLock<Mutex<HashMap<String, (u32, u64)>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(HashMap::new()))
}

/// True while `email` is locked out after too many recent failures.
fn login_locked(email: &str) -> bool {
    let mut m = login_limiter().lock().unwrap();
    match m.get(email).copied() {
        Some((fails, first)) => {
            if now_secs().saturating_sub(first) >= LOGIN_LOCKOUT_SECS {
                m.remove(email); // window expired
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
    let entry = m.entry(email.to_string()).or_insert((0, now));
    if now.saturating_sub(entry.1) >= LOGIN_LOCKOUT_SECS {
        *entry = (1, now); // fresh window
    } else {
        entry.0 += 1;
    }
}

fn login_clear(email: &str) {
    login_limiter().lock().unwrap().remove(email);
}

// ---------------------------------------------------------------------------
// Signup abuse guard (invite code + per-actor rate limit)
// ---------------------------------------------------------------------------

fn signup_limiter() -> &'static Mutex<HashMap<String, (u32, u64)>> {
    static L: OnceLock<Mutex<HashMap<String, (u32, u64)>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Consume one signup token for `actor` in the current window; `false` when the
/// rate is exceeded. Behind a reverse proxy the actor is the forwarded client
/// IP; absent that, a single global bucket still bounds a flood.
fn signup_allowed(actor: &str) -> bool {
    let mut m = signup_limiter().lock().unwrap();
    let now = now_secs();
    let entry = m.entry(actor.to_string()).or_insert((0, now));
    if now.saturating_sub(entry.1) >= SIGNUP_WINDOW_SECS {
        *entry = (0, now); // window rolled over
    }
    if entry.0 >= signup_rate() {
        return false;
    }
    entry.0 += 1;
    true
}

/// The client's address for rate-limiting. Trusts the reverse proxy's forwarded
/// headers (this server is meant to sit behind nginx/Cloudflare), falling back
/// to a single `global` bucket when none are present.
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
// Router
// ---------------------------------------------------------------------------

/// Handle a `/platform/v1/*` request. Returns `Some` when the path is a
/// platform route (the caller returns the response); `None` otherwise so the
/// normal router continues. Only matches when the platform is enabled.
pub fn route(
    req: &HttpRequest,
    segments: &[&str],
    mgr: Option<&DatabaseManager>,
) -> Option<HttpResponse> {
    if segments.first() != Some(&"platform") {
        return None;
    }
    if !enabled() {
        return Some(resp(
            404,
            json!({ "message": "control plane is not enabled" }),
        ));
    }
    let Some(mgr) = mgr else {
        return Some(resp(
            400,
            json!({ "message": "control plane requires a database registry" }),
        ));
    };

    Some(match (req.method.as_str(), segments) {
        ("POST", ["platform", "v1", "signup"]) => handle_signup(req, mgr),
        ("POST", ["platform", "v1", "login"]) => handle_login(req, mgr),
        ("POST", ["platform", "v1", "projects"]) => handle_create_project(req, mgr),
        ("GET", ["platform", "v1", "projects"]) => handle_list_projects(req, mgr),
        ("GET", ["platform", "v1", "projects", r]) => handle_get_project(req, mgr, r),
        ("DELETE", ["platform", "v1", "projects", r]) => handle_delete_project(req, mgr, r),
        ("POST", ["platform", "v1", "projects", r, "keys", "rotate"]) => {
            handle_rotate_keys(req, mgr, r)
        }
        _ => resp(404, json!({ "message": "no such platform route" })),
    })
}

// ---------------------------------------------------------------------------
// Account handlers
// ---------------------------------------------------------------------------

fn handle_signup(req: &HttpRequest, mgr: &DatabaseManager) -> HttpResponse {
    let Some(master) = master_secret() else {
        return resp(501, json!({ "message": "set OXIDB_PLATFORM_SECRET" }));
    };
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };

    // ── Abuse guards, cheapest-and-broadest first ──────────────────────
    // 1) Per-actor rate limit (also throttles invite-code guessing below).
    if !signup_allowed(&client_ip(req)) {
        return resp(
            429,
            json!({ "message": "signup rate limit exceeded; slow down" }),
        );
    }
    let body = parse_body(req).unwrap_or_else(|_| json!({}));
    // 2) Optional invite code gate (operator-controlled private signup).
    if let Some(code) = signup_code() {
        if body.get("code").and_then(|v| v.as_str()) != Some(code.as_str()) {
            return resp(403, json!({ "message": "a valid invite code is required" }));
        }
    }
    // 3) Global account ceiling (backstop).
    if pdb.count("_auth_users", &json!({})).unwrap_or(0) >= max_accounts() {
        return resp(403, json!({ "message": "signups are closed" }));
    }

    let (email, password) = match credentials(req) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if password.len() < MIN_PASSWORD_LEN {
        return resp(
            400,
            json!({ "message": format!("password must be at least {MIN_PASSWORD_LEN} characters") }),
        );
    }
    // Developers are admins of the projects they create; the account row lives
    // in the platform store's `_auth_users` (reusing the jwt user machinery).
    if let Err(e) = jwt::signup(&pdb, &email, &password, "admin") {
        return resp(409, json!({ "message": e }));
    }
    match jwt::login(&pdb, &email, &password, &master) {
        Ok(token) => resp(
            201,
            json!({ "account": { "email": email }, "token": token }),
        ),
        Err(e) => resp(500, json!({ "message": e })),
    }
}

fn handle_login(req: &HttpRequest, mgr: &DatabaseManager) -> HttpResponse {
    let Some(master) = master_secret() else {
        return resp(501, json!({ "message": "set OXIDB_PLATFORM_SECRET" }));
    };
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };
    let (email, password) = match credentials(req) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if login_locked(&email) {
        return resp(
            429,
            json!({ "message": "too many failed attempts; try again later" }),
        );
    }
    match jwt::login(&pdb, &email, &password, &master) {
        Ok(token) => {
            login_clear(&email);
            resp(200, json!({ "token": token }))
        }
        Err(e) => {
            login_record_failure(&email);
            resp(401, json!({ "message": e }))
        }
    }
}

// ---------------------------------------------------------------------------
// Project handlers
// ---------------------------------------------------------------------------

fn handle_create_project(req: &HttpRequest, mgr: &DatabaseManager) -> HttpResponse {
    let owner = match authenticate(req) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };
    let body = parse_body(req).unwrap_or_else(|_| json!({}));
    let name = body
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Resource-exhaustion guard: cap projects per account.
    let owned = pdb.count(PROJECTS, &json!({ "owner": owner })).unwrap_or(0);
    if owned >= max_projects() {
        return resp(
            403,
            json!({ "message": format!("project limit reached ({})", max_projects()) }),
        );
    }

    // Mint an unguessable ref that is not already a database.
    let mut project_ref = gen_ref();
    for _ in 0..8 {
        if !mgr.database_exists(&project_ref) {
            break;
        }
        project_ref = gen_ref();
    }
    let secret = gen_secret();
    let created_at = now_secs();

    if let Err(e) = mgr.create_database(&project_ref) {
        return resp(
            500,
            json!({ "message": format!("provisioning failed: {e}") }),
        );
    }

    let Some(seal) = seal_key() else {
        return resp(500, json!({ "message": "no seal key configured" }));
    };
    let sealed = match seal.encrypt(secret.as_bytes()) {
        Ok(s) => base64::engine::general_purpose::STANDARD.encode(s),
        Err(e) => return resp(500, json!({ "message": format!("seal failed: {e}") })),
    };
    let doc = json!({
        "ref": project_ref,
        "owner": owner,
        "name": name,
        "secret_enc": sealed,
        "isolation": "shared",
        "created_at": created_at,
        "key_iat": created_at,
    });
    if pdb.insert(PROJECTS, doc).is_err() {
        let _ = mgr.drop_database(&project_ref);
        return resp(500, json!({ "message": "failed to record project" }));
    }
    let _ = pdb.create_index(PROJECTS, "ref");

    resp(
        201,
        project_view(&project_ref, &name, created_at, created_at, &secret, true),
    )
}

fn handle_list_projects(req: &HttpRequest, mgr: &DatabaseManager) -> HttpResponse {
    let owner = match authenticate(req) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };
    let docs = pdb
        .find(PROJECTS, &json!({ "owner": owner }))
        .unwrap_or_default();
    let list: Vec<Value> = docs
        .iter()
        .map(|d| {
            json!({
                "ref": d.get("ref"),
                "name": d.get("name"),
                "isolation": d.get("isolation"),
                "created_at": d.get("created_at"),
            })
        })
        .collect();
    resp(200, json!(list))
}

fn handle_get_project(req: &HttpRequest, mgr: &DatabaseManager, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let Some(seal) = seal_key() else {
        return resp(500, json!({ "message": "no seal key configured" }));
    };
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };
    let Some(doc) = pdb
        .find_one(PROJECTS, &json!({ "ref": project_ref, "owner": owner }))
        .ok()
        .flatten()
    else {
        return resp(404, json!({ "message": "project not found" }));
    };
    let created_at = doc.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0);
    let key_iat = doc
        .get("key_iat")
        .and_then(|v| v.as_u64())
        .unwrap_or(created_at);
    let name = doc.get("name").and_then(|v| v.as_str()).unwrap_or("");
    // Re-derive the keys from the stored (encrypted) secret.
    let Some(sealed_b64) = doc.get("secret_enc").and_then(|v| v.as_str()) else {
        return resp(500, json!({ "message": "corrupt project record" }));
    };
    let secret = match base64::engine::general_purpose::STANDARD.decode(sealed_b64) {
        Ok(sealed) => match seal.decrypt(&sealed) {
            Ok(p) => String::from_utf8(p).unwrap_or_default(),
            Err(_) => return resp(500, json!({ "message": "unseal failed" })),
        },
        Err(_) => return resp(500, json!({ "message": "corrupt secret" })),
    };
    resp(
        200,
        project_view(project_ref, name, created_at, key_iat, &secret, true),
    )
}

fn handle_delete_project(
    req: &HttpRequest,
    mgr: &DatabaseManager,
    project_ref: &str,
) -> HttpResponse {
    let owner = match authenticate(req) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };
    if pdb
        .find_one(PROJECTS, &json!({ "ref": project_ref, "owner": owner }))
        .ok()
        .flatten()
        .is_none()
    {
        return resp(404, json!({ "message": "project not found" }));
    }
    let _ = mgr.drop_database(project_ref);
    crate::sql_bridge::forget_database(project_ref);
    crate::tsdb_bridge::forget_database(project_ref);
    let _ = pdb.delete(PROJECTS, &json!({ "ref": project_ref }));
    invalidate_secret(project_ref);
    resp(200, json!({ "deleted": project_ref }))
}

/// `POST /platform/v1/projects/{ref}/keys/rotate` — generate a fresh project
/// secret, re-mint both keys, and invalidate the cache. Every previously issued
/// key for this project (anon + service_role) stops verifying immediately — the
/// escape hatch for a leaked key.
fn handle_rotate_keys(req: &HttpRequest, mgr: &DatabaseManager, project_ref: &str) -> HttpResponse {
    let owner = match authenticate(req) {
        Ok(c) => c.sub,
        Err(r) => return r,
    };
    let Some(seal) = seal_key() else {
        return resp(500, json!({ "message": "no seal key configured" }));
    };
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };
    let Some(doc) = pdb
        .find_one(PROJECTS, &json!({ "ref": project_ref, "owner": owner }))
        .ok()
        .flatten()
    else {
        return resp(404, json!({ "message": "project not found" }));
    };
    let created_at = doc.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0);
    let name = doc.get("name").and_then(|v| v.as_str()).unwrap_or("");

    let new_secret = gen_secret();
    let new_iat = now_secs();
    let sealed = match seal.encrypt(new_secret.as_bytes()) {
        Ok(s) => base64::engine::general_purpose::STANDARD.encode(s),
        Err(e) => return resp(500, json!({ "message": format!("seal failed: {e}") })),
    };
    if pdb
        .update(
            PROJECTS,
            &json!({ "ref": project_ref }),
            &json!({ "$set": { "secret_enc": sealed, "key_iat": new_iat } }),
        )
        .is_err()
    {
        return resp(
            500,
            json!({ "message": "failed to persist rotated secret" }),
        );
    }
    invalidate_secret(project_ref); // old secret must stop verifying at once
    resp(
        200,
        project_view(project_ref, name, created_at, new_iat, &new_secret, true),
    )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The public view of a project, optionally including the (secret) API keys.
/// `key_iat` is the `iat` the keys are minted with (the creation time, or the
/// last rotation time) — fixing it keeps re-derived keys stable.
fn project_view(
    project_ref: &str,
    name: &str,
    created_at: u64,
    key_iat: u64,
    secret: &str,
    keys: bool,
) -> Value {
    let base = std::env::var("OXIDB_PLATFORM_BASE_URL").unwrap_or_default();
    let url = if base.is_empty() {
        json!(null)
    } else {
        json!(format!("{base}/rest/v1?db={project_ref}"))
    };
    let mut v = json!({
        "ref": project_ref,
        "name": name,
        "db": project_ref,
        "endpoint": "/rest/v1",
        "url": url,
        "isolation": "shared",
        "created_at": created_at,
    });
    if keys {
        let obj = v.as_object_mut().unwrap();
        obj.insert(
            "anon_key".into(),
            json!(mint_key(secret, project_ref, "read", key_iat)),
        );
        obj.insert(
            "service_role_key".into(),
            json!(mint_key(secret, project_ref, "admin", key_iat)),
        );
    }
    v
}

/// Mint a stable API key (JWT) for a project. The fixed `iat` (project creation
/// time) makes the token deterministic, so `GET` re-derives the same string.
fn mint_key(secret: &str, project_ref: &str, role: &str, iat: u64) -> String {
    let claims = Claims {
        sub: format!("{role}@{project_ref}"),
        role: role.to_string(),
        iat,
        exp: iat + KEY_EXPIRY_SECS,
    };
    jwt::encode_jwt(&claims, secret)
}

/// Verify a `Bearer` platform-session token against the master secret.
fn authenticate(req: &HttpRequest) -> Result<Claims, HttpResponse> {
    let Some(master) = master_secret() else {
        return Err(resp(501, json!({ "message": "set OXIDB_PLATFORM_SECRET" })));
    };
    let header = req
        .headers
        .get("authorization")
        .map(|s| s.as_str())
        .unwrap_or("");
    let Some(token) = jwt::extract_bearer(header) else {
        return Err(resp(
            401,
            json!({ "message": "missing Authorization: Bearer <platform token>" }),
        ));
    };
    jwt::verify(token, &master).map_err(|e| resp(401, json!({ "message": e })))
}

fn credentials(req: &HttpRequest) -> Result<(String, String), HttpResponse> {
    let body = parse_body(req).map_err(|r| r)?;
    let email = body
        .get("email")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let password = body
        .get("password")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    match (email, password) {
        (Some(e), Some(p)) => Ok((e.to_string(), p.to_string())),
        _ => Err(resp(
            400,
            json!({ "message": "email and password required" }),
        )),
    }
}

fn parse_body(req: &HttpRequest) -> Result<Value, HttpResponse> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body).map_err(|_| resp(400, json!({ "message": "invalid JSON" })))
}

fn gen_ref() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..16)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}

fn gen_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn resp(status: u16, body: Value) -> HttpResponse {
    let status_text = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        501 => "Not Implemented",
        _ => "Internal Server Error",
    };
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    HttpResponse {
        status,
        status_text,
        content_type: "application/json".to_string(),
        headers: Vec::new(),
        body: bytes,
        content_length_override: None,
    }
    .with_header("Access-Control-Allow-Origin", "*")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refs_are_16_char_slugs() {
        let r = gen_ref();
        assert_eq!(r.len(), 16);
        assert!(
            r.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
        assert_ne!(gen_ref(), gen_ref());
    }

    #[test]
    fn minted_key_is_stable_and_verifies() {
        let secret = "test-secret";
        let iat = now_secs(); // real-ish iat so exp = iat + 10y is in the future
        let k1 = mint_key(secret, "abc", "read", iat);
        let k2 = mint_key(secret, "abc", "read", iat);
        assert_eq!(k1, k2, "fixed iat → deterministic key");
        let claims = jwt::verify(&k1, secret).unwrap();
        assert_eq!(claims.role, "read");
        assert_eq!(claims.sub, "read@abc");
    }

    #[test]
    fn seal_roundtrip() {
        let key = derive_key("seal-material");
        let sealed = key.encrypt(b"project-secret").unwrap();
        assert_eq!(key.decrypt(&sealed).unwrap(), b"project-secret");
    }

    #[test]
    fn only_the_seal_key_unseals() {
        // ADR-0021: the data plane unseals with the SEAL key, independent of the
        // master session-signing secret. A different key must fail to unseal.
        let sealed = derive_key("seal-key-A").encrypt(b"tenant-secret").unwrap();
        assert_eq!(
            derive_key("seal-key-A").decrypt(&sealed).unwrap(),
            b"tenant-secret"
        );
        assert!(
            derive_key("master-signing-secret")
                .decrypt(&sealed)
                .is_err(),
            "the session-signing secret must NOT be able to unseal"
        );
    }

    #[test]
    fn login_lockout_after_max_fails() {
        let email = "lockme@example.com";
        login_clear(email);
        assert!(!login_locked(email));
        for _ in 0..LOGIN_MAX_FAILS {
            login_record_failure(email);
        }
        assert!(login_locked(email), "locked after MAX fails");
        login_clear(email); // a successful login clears it
        assert!(!login_locked(email));
    }

    #[test]
    fn signup_rate_limit_trips() {
        let actor = "203.0.113.7";
        // Default rate is 5/window; the 6th is rejected.
        for _ in 0..signup_rate() {
            assert!(signup_allowed(actor));
        }
        assert!(!signup_allowed(actor), "over the rate → blocked");
    }

    #[test]
    fn client_ip_prefers_forwarded_headers() {
        let mut req = HttpRequest {
            method: "POST".into(),
            path: "/platform/v1/signup".into(),
            query: String::new(),
            headers: std::collections::HashMap::new(),
            body: Vec::new(),
        };
        assert_eq!(client_ip(&req), "global");
        req.headers
            .insert("x-forwarded-for".into(), "198.51.100.9, 10.0.0.1".into());
        assert_eq!(client_ip(&req), "198.51.100.9");
        req.headers
            .insert("cf-connecting-ip".into(), "198.51.100.42".into());
        assert_eq!(client_ip(&req), "198.51.100.42", "CF header wins");
    }

    #[test]
    fn rotation_changes_the_signing_secret() {
        // Keys minted under different secrets never cross-verify — the property
        // that makes rotation a real revocation.
        let iat = now_secs();
        let old = mint_key("secret-A", "p", "admin", iat);
        assert!(jwt::verify(&old, "secret-A").is_ok());
        assert!(
            jwt::verify(&old, "secret-B").is_err(),
            "old key must not verify under the rotated secret"
        );
    }
}
