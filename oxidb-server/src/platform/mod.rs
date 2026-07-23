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
//! ```

use std::sync::{Arc, OnceLock};
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

/// The AES-GCM key that seals per-project secrets, derived from the master
/// secret so no separate keyfile is needed.
fn enc_key(master: &str) -> Arc<EncryptionKey> {
    let digest = Sha256::digest(master.as_bytes());
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

/// The per-project JWT secret for `db_ref`, if it names an OxiBase project.
/// The REST listener calls this so a request to `?db=<ref>` is verified with
/// that project's secret rather than the global `OXIDB_JWT_SECRET`.
pub fn project_secret(mgr: &DatabaseManager, db_ref: &str) -> Option<String> {
    let master = master_secret()?;
    let pdb = platform_db(mgr)?;
    let doc = pdb.find_one(PROJECTS, &json!({ "ref": db_ref })).ok()??;
    let sealed_b64 = doc.get("secret_enc")?.as_str()?;
    let sealed = base64::engine::general_purpose::STANDARD
        .decode(sealed_b64)
        .ok()?;
    let plain = enc_key(&master).decrypt(&sealed).ok()?;
    String::from_utf8(plain).ok()
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
    let (email, password) = match credentials(req) {
        Ok(c) => c,
        Err(r) => return r,
    };
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
    match jwt::login(&pdb, &email, &password, &master) {
        Ok(token) => resp(200, json!({ "token": token })),
        Err(e) => resp(401, json!({ "message": e })),
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
    let master = master_secret().unwrap();
    let Some(pdb) = platform_db(mgr) else {
        return resp(500, json!({ "message": "control-plane store unavailable" }));
    };
    let body = parse_body(req).unwrap_or_else(|_| json!({}));
    let name = body
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

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

    let sealed = match enc_key(&master).encrypt(secret.as_bytes()) {
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
    });
    if pdb.insert(PROJECTS, doc).is_err() {
        let _ = mgr.drop_database(&project_ref);
        return resp(500, json!({ "message": "failed to record project" }));
    }
    let _ = pdb.create_index(PROJECTS, "ref");

    resp(
        201,
        project_view(&project_ref, &name, created_at, &secret, true),
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
    let master = master_secret().unwrap();
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
    // Re-derive the keys from the stored (encrypted) secret.
    let Some(sealed_b64) = doc.get("secret_enc").and_then(|v| v.as_str()) else {
        return resp(500, json!({ "message": "corrupt project record" }));
    };
    let secret = match base64::engine::general_purpose::STANDARD.decode(sealed_b64) {
        Ok(sealed) => match enc_key(&master).decrypt(&sealed) {
            Ok(p) => String::from_utf8(p).unwrap_or_default(),
            Err(_) => return resp(500, json!({ "message": "unseal failed" })),
        },
        Err(_) => return resp(500, json!({ "message": "corrupt secret" })),
    };
    resp(
        200,
        project_view(project_ref, name, created_at, &secret, true),
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
    resp(200, json!({ "deleted": project_ref }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The public view of a project, optionally including the (secret) API keys.
fn project_view(project_ref: &str, name: &str, created_at: u64, secret: &str, keys: bool) -> Value {
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
            json!(mint_key(secret, project_ref, "read", created_at)),
        );
        obj.insert(
            "service_role_key".into(),
            json!(mint_key(secret, project_ref, "admin", created_at)),
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
        let key = enc_key("master");
        let sealed = key.encrypt(b"project-secret").unwrap();
        assert_eq!(key.decrypt(&sealed).unwrap(), b"project-secret");
    }
}
