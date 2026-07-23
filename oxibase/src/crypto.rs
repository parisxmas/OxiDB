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

// ---------------------------------------------------------------------------
// JWT (ES256 / P-256) — asymmetric project keys. The control plane signs with a
// per-project private key; data-plane nodes verify with the public key alone
// (no shared secret, no seal key), so verification scales to many nodes and the
// public key can be published as a JWK. Signing is RFC 6979 deterministic, so
// re-minting the same claims yields byte-identical tokens (stable keys).
// ---------------------------------------------------------------------------

use p256::EncodedPoint;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};

/// A fresh P-256 keypair: `(private scalar [32B], public SEC1 uncompressed [65B])`.
pub fn gen_es256_keypair() -> (Vec<u8>, Vec<u8>) {
    loop {
        let mut buf = [0u8; 32];
        rand::rng().fill_bytes(&mut buf);
        if let Ok(sk) = SigningKey::from_slice(&buf) {
            let point = sk.verifying_key().to_encoded_point(false);
            return (buf.to_vec(), point.as_bytes().to_vec());
        }
        // `buf` was >= the curve order or zero — astronomically rare; retry.
    }
}

/// ES256-sign the standard claims with a P-256 private scalar. `None` if the
/// scalar is malformed.
pub fn encode_jwt_es256(claims: &Claims, priv_scalar: &[u8]) -> Option<String> {
    let sk = SigningKey::from_slice(priv_scalar).ok()?;
    let header = b64url(br#"{"alg":"ES256","typ":"JWT"}"#);
    let payload = format!(
        r#"{{"sub":{},"role":{},"iat":{},"exp":{}}}"#,
        json_str(&claims.sub),
        json_str(&claims.role),
        claims.iat,
        claims.exp
    );
    let payload = b64url(payload.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let sig: Signature = sk.sign(signing_input.as_bytes());
    Some(format!("{signing_input}.{}", b64url(&sig.to_bytes())))
}

/// The public key (SEC1 uncompressed 65B) as a JWK for a JWKS document.
pub fn jwk_from_pub(pub_sec1: &[u8], kid: &str) -> Option<serde_json::Value> {
    let point = EncodedPoint::from_bytes(pub_sec1).ok()?;
    let x = point.x()?;
    let y = point.y()?;
    Some(serde_json::json!({
        "kty": "EC",
        "crv": "P-256",
        "alg": "ES256",
        "use": "sig",
        "kid": kid,
        "x": b64url(x.as_slice()),
        "y": b64url(y.as_slice()),
    }))
}

/// SHA-256 of `data` as lowercase hex — used to store refresh tokens hashed, so
/// a leak of the metadata store never exposes usable tokens.
pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
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
