//! Self-contained crypto for the control plane — HS256 JWTs, argon2 password
//! hashing, and AES-256-GCM sealing. Kept independent so `oxibase` links no
//! database engine (ADR-0021); the seal format matches the data plane's
//! `oxidb::EncryptionKey` (`[nonce:12][ciphertext+tag]`) so the two agree.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// JWT (HS256) — must match the data plane's jwt module byte-for-byte.
// ---------------------------------------------------------------------------

pub struct Claims {
    pub sub: String,
    pub role: String,
    pub iat: u64,
    pub exp: u64,
}

fn b64url(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .ok()
}

pub fn encode_jwt(claims: &Claims, secret: &str) -> String {
    let header = b64url(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = format!(
        r#"{{"sub":{},"role":{},"iat":{},"exp":{}}}"#,
        json_str(&claims.sub),
        json_str(&claims.role),
        claims.iat,
        claims.exp
    );
    let payload = b64url(payload.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signing_input.as_bytes());
    let sig = b64url(&mac.finalize().into_bytes());
    format!("{signing_input}.{sig}")
}

pub fn decode_jwt(token: &str, secret: &str) -> Result<Claims, &'static str> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("invalid token format");
    }
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signing_input.as_bytes());
    if parts[2] != b64url(&mac.finalize().into_bytes()) {
        return Err("invalid signature");
    }
    let payload = b64url_decode(parts[1]).ok_or("invalid base64")?;
    let v: serde_json::Value = serde_json::from_slice(&payload).map_err(|_| "invalid JSON")?;
    let now = crate::now_secs();
    let exp = v["exp"].as_u64().ok_or("missing exp")?;
    if now > exp {
        return Err("token expired");
    }
    Ok(Claims {
        sub: v["sub"].as_str().ok_or("missing sub")?.to_string(),
        role: v["role"].as_str().unwrap_or("read").to_string(),
        iat: v["iat"].as_u64().unwrap_or(0),
        exp,
    })
}

/// Minimal JSON string escaping for the two fields we encode (sub/role).
fn json_str(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

// ---------------------------------------------------------------------------
// Password hashing (argon2 PHC strings)
// ---------------------------------------------------------------------------

pub fn hash_password(password: &str) -> Result<String, String> {
    use argon2::Argon2;
    use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

pub fn verify_password(password: &str, phc: &str) -> bool {
    use argon2::Argon2;
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    match PasswordHash::new(phc) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

// ---------------------------------------------------------------------------
// AES-256-GCM sealing (format compatible with oxidb::EncryptionKey)
// ---------------------------------------------------------------------------

const NONCE_LEN: usize = 12;

/// Derive a 32-byte key from arbitrary material (SHA-256) — same as the data
/// plane's seal-key derivation.
pub fn derive_key(material: &str) -> [u8; 32] {
    let digest = Sha256::digest(material.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}

/// Seal plaintext → `[nonce:12][ciphertext+tag]` (matches the data plane).
pub fn seal(key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let cipher = Aes256Gcm::new_from_slice(key).expect("32-byte key");
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher.encrypt(nonce, plaintext).expect("encrypt");
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    out
}

/// Reverse of [`seal`]. `None` if the buffer is malformed or the key is wrong.
pub fn unseal(key: &[u8; 32], sealed: &[u8]) -> Option<Vec<u8>> {
    if sealed.len() < NONCE_LEN {
        return None;
    }
    let cipher = Aes256Gcm::new_from_slice(key).ok()?;
    let nonce = Nonce::from_slice(&sealed[..NONCE_LEN]);
    cipher.decrypt(nonce, &sealed[NONCE_LEN..]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_round_trip() {
        let c = Claims {
            sub: "read@abc".into(),
            role: "read".into(),
            iat: crate::now_secs(),
            exp: crate::now_secs() + 3600,
        };
        let tok = encode_jwt(&c, "secret");
        let back = decode_jwt(&tok, "secret").unwrap();
        assert_eq!(back.sub, "read@abc");
        assert_eq!(back.role, "read");
        assert!(decode_jwt(&tok, "other").is_err());
    }

    #[test]
    fn password_hash_verify() {
        let h = hash_password("hunter2!").unwrap();
        assert!(verify_password("hunter2!", &h));
        assert!(!verify_password("wrong", &h));
    }

    #[test]
    fn seal_produces_data_plane_format() {
        let key = derive_key("seal-material");
        let sealed = seal(&key, b"tenant-secret");
        // nonce(12) + ciphertext + 16-byte GCM tag
        assert!(sealed.len() >= NONCE_LEN + 16);
    }
}
