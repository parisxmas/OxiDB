//! JWT authentication — HMAC-SHA256 token generation and validation.
//!
//! Provides signup, login, and token verification for the REST and WebSocket APIs.
//! Users are stored in the `_auth_users` system collection with argon2-hashed passwords.
//!
//! Enabled when `OXIDB_JWT_SECRET` is set. Without it, REST/WS APIs are open.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use serde_json::{json, Value};

use oxidb::OxiDb;

type HmacSha256 = Hmac<Sha256>;

const AUTH_COLLECTION: &str = "_auth_users";
const TOKEN_EXPIRY_SECS: u64 = 86400; // 24 hours

/// JWT claims payload.
#[derive(Debug, Clone)]
pub struct Claims {
    pub sub: String,     // user ID / username
    pub role: String,    // "admin", "readwrite", "read"
    pub iat: u64,        // issued at (unix timestamp)
    pub exp: u64,        // expiry (unix timestamp)
}

// ---------------------------------------------------------------------------
// Base64 URL-safe helpers
// ---------------------------------------------------------------------------

fn b64url_encode(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s).ok()
}

// ---------------------------------------------------------------------------
// JWT encode / decode
// ---------------------------------------------------------------------------

pub fn encode_jwt(claims: &Claims, secret: &str) -> String {
    let header = b64url_encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload_json = json!({
        "sub": claims.sub,
        "role": claims.role,
        "iat": claims.iat,
        "exp": claims.exp,
    });
    let payload = b64url_encode(serde_json::to_string(&payload_json).unwrap().as_bytes());

    let signing_input = format!("{header}.{payload}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signing_input.as_bytes());
    let signature = b64url_encode(&mac.finalize().into_bytes());

    format!("{signing_input}.{signature}")
}

pub fn decode_jwt(token: &str, secret: &str) -> Result<Claims, &'static str> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("invalid token format");
    }

    // Verify signature
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signing_input.as_bytes());
    let expected_sig = b64url_encode(&mac.finalize().into_bytes());
    if parts[2] != expected_sig {
        return Err("invalid signature");
    }

    // Decode payload
    let payload_bytes = b64url_decode(parts[1]).ok_or("invalid base64")?;
    let payload: Value = serde_json::from_slice(&payload_bytes).map_err(|_| "invalid JSON")?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let exp = payload["exp"].as_u64().ok_or("missing exp")?;
    if now > exp {
        return Err("token expired");
    }

    Ok(Claims {
        sub: payload["sub"].as_str().ok_or("missing sub")?.to_string(),
        role: payload["role"].as_str().unwrap_or("read").to_string(),
        iat: payload["iat"].as_u64().unwrap_or(0),
        exp,
    })
}

// ---------------------------------------------------------------------------
// User management
// ---------------------------------------------------------------------------

pub fn signup(db: &OxiDb, username: &str, password: &str, role: &str) -> Result<Value, String> {
    // Check if user already exists
    let existing = db.find_one(AUTH_COLLECTION, &json!({"username": username}))
        .map_err(|e| e.to_string())?;
    if existing.is_some() {
        return Err("username already exists".to_string());
    }

    // Validate role
    let role = match role {
        "admin" | "readwrite" | "read" => role,
        _ => return Err("invalid role: must be admin, readwrite, or read".to_string()),
    };

    // Hash password with argon2
    let salt = generate_salt();
    let password_hash = hash_password(password, &salt)?;

    // Store user
    let user_doc = json!({
        "username": username,
        "password_hash": password_hash,
        "salt": salt,
        "role": role,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });
    db.insert(AUTH_COLLECTION, user_doc).map_err(|e| e.to_string())?;

    // Create index on username for fast lookups
    let _ = db.create_unique_index(AUTH_COLLECTION, "username");

    Ok(json!({"username": username, "role": role}))
}

pub fn login(db: &OxiDb, username: &str, password: &str, secret: &str) -> Result<String, String> {
    let user = db.find_one(AUTH_COLLECTION, &json!({"username": username}))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "invalid credentials".to_string())?;

    let stored_hash = user["password_hash"].as_str().ok_or("corrupt user record")?;
    let salt = user["salt"].as_str().ok_or("corrupt user record")?;

    // Verify password
    let computed_hash = hash_password(password, salt)?;
    if computed_hash != stored_hash {
        return Err("invalid credentials".to_string());
    }

    let role = user["role"].as_str().unwrap_or("read");

    // Generate JWT
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let claims = Claims {
        sub: username.to_string(),
        role: role.to_string(),
        iat: now,
        exp: now + TOKEN_EXPIRY_SECS,
    };

    Ok(encode_jwt(&claims, secret))
}

pub fn verify(token: &str, secret: &str) -> Result<Claims, &'static str> {
    decode_jwt(token, secret)
}

/// Extract Bearer token from Authorization header value.
pub fn extract_bearer(auth_header: &str) -> Option<&str> {
    auth_header.strip_prefix("Bearer ").or_else(|| auth_header.strip_prefix("bearer "))
}

// ---------------------------------------------------------------------------
// Password hashing
// ---------------------------------------------------------------------------

fn hash_password(password: &str, salt: &str) -> Result<String, String> {
    use argon2::Argon2;
    use argon2::password_hash::{PasswordHasher, SaltString};

    let salt = SaltString::from_b64(salt).map_err(|e| e.to_string())?;
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| e.to_string())?;
    Ok(hash.to_string())
}

fn generate_salt() -> String {
    use argon2::password_hash::SaltString;
    // Generate 16 random bytes and encode as base64 salt
    let mut salt_bytes = [0u8; 16];
    use rand::RngCore;
    rand::rng().fill_bytes(&mut salt_bytes);
    SaltString::encode_b64(&salt_bytes)
        .expect("failed to encode salt")
        .to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SECRET: &str = "test-secret-key-for-jwt-signing";

    #[test]
    fn jwt_roundtrip() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = Claims {
            sub: "alice".to_string(),
            role: "admin".to_string(),
            iat: now,
            exp: now + 3600,
        };

        let token = encode_jwt(&claims, TEST_SECRET);
        assert!(token.contains('.'));
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let decoded = decode_jwt(&token, TEST_SECRET).unwrap();
        assert_eq!(decoded.sub, "alice");
        assert_eq!(decoded.role, "admin");
        assert_eq!(decoded.exp, now + 3600);
    }

    #[test]
    fn jwt_invalid_signature() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = Claims {
            sub: "alice".to_string(),
            role: "admin".to_string(),
            iat: now,
            exp: now + 3600,
        };

        let token = encode_jwt(&claims, TEST_SECRET);
        let result = decode_jwt(&token, "wrong-secret");
        assert_eq!(result.unwrap_err(), "invalid signature");
    }

    #[test]
    fn jwt_expired() {
        let claims = Claims {
            sub: "alice".to_string(),
            role: "admin".to_string(),
            iat: 1000,
            exp: 1001, // expired long ago
        };

        let token = encode_jwt(&claims, TEST_SECRET);
        let result = decode_jwt(&token, TEST_SECRET);
        assert_eq!(result.unwrap_err(), "token expired");
    }

    #[test]
    fn extract_bearer_token() {
        assert_eq!(extract_bearer("Bearer abc123"), Some("abc123"));
        assert_eq!(extract_bearer("bearer abc123"), Some("abc123"));
        assert_eq!(extract_bearer("Basic abc123"), None);
    }
}
