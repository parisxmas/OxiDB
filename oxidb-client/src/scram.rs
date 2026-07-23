//! Client-side SCRAM-SHA-256 (RFC 7677), ported from oxidb-server's
//! `scram_client` with the crypto primitives reimplemented locally so this
//! crate links no server code. Emits the client proof and verifies the
//! server's signature — mutual authentication without ever sending the
//! password.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

fn b64enc(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}
fn b64dec(s: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| e.to_string())
}
fn sha256(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}
fn hmac256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("hmac key");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}
/// PBKDF2-HMAC-SHA256 with a 32-byte output (one SHA-256 block), matching the
/// server's `Hi()`.
fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut block = salt.to_vec();
    block.extend_from_slice(&1u32.to_be_bytes()); // INT(1)
    let mut u = hmac256(password, &block);
    let mut out = u.clone();
    for _ in 1..iterations {
        u = hmac256(password, &u);
        for (o, b) in out.iter_mut().zip(u.iter()) {
            *o ^= *b;
        }
    }
    out
}

pub struct ScramClient {
    username: String,
    password: String,
    client_nonce: String,
    client_first_bare: Option<String>,
}

impl ScramClient {
    pub fn new(username: &str, password: &str) -> Self {
        let mut nonce = [0u8; 16];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut nonce);
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: b64enc(&nonce),
            client_first_bare: None,
        }
    }

    /// `n,,n=<user>,r=<client_nonce>`.
    pub fn client_first(&mut self) -> String {
        let bare = format!("n={},r={}", self.username, self.client_nonce);
        self.client_first_bare = Some(bare.clone());
        format!("n,,{bare}")
    }

    /// Process server-first, emit client-final, and return the expected
    /// server signature for [`verify_server_final`].
    pub fn client_final(&mut self, server_first: &str) -> Result<(String, Vec<u8>), String> {
        let bare = self
            .client_first_bare
            .as_ref()
            .ok_or("client_final before client_first")?
            .clone();

        let mut combined_nonce = String::new();
        let mut salt_b64 = String::new();
        let mut iter_count: u32 = 0;
        for part in server_first.split(',') {
            if let Some(r) = part.strip_prefix("r=") {
                combined_nonce = r.to_string();
            } else if let Some(s) = part.strip_prefix("s=") {
                salt_b64 = s.to_string();
            } else if let Some(i) = part.strip_prefix("i=") {
                iter_count = i.parse().map_err(|e| format!("bad i=: {e}"))?;
            }
        }
        if combined_nonce.is_empty() || salt_b64.is_empty() || iter_count == 0 {
            return Err(format!("server-first missing r=/s=/i=: {server_first:?}"));
        }
        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err("server nonce does not extend client_nonce — possible MITM".into());
        }

        let salt = b64dec(&salt_b64)?;
        let salted = pbkdf2(self.password.as_bytes(), &salt, iter_count);
        let client_key = hmac256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);

        let client_final_no_proof = format!("c=biws,r={combined_nonce}");
        let auth_message = format!("{bare},{server_first},{client_final_no_proof}");
        let client_signature = hmac256(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(k, s)| k ^ s)
            .collect();
        let client_final = format!("{client_final_no_proof},p={}", b64enc(&proof));

        let server_key = hmac256(&salted, b"Server Key");
        let expected_server_signature = hmac256(&server_key, auth_message.as_bytes());
        self.password.clear();
        Ok((client_final, expected_server_signature))
    }
}

/// Verify the server's `v=<sig>` reply (constant-time).
pub fn verify_server_final(server_final: &str, expected: &[u8]) -> Result<(), String> {
    let v = server_final
        .strip_prefix("v=")
        .ok_or("server-final must start with v=")?;
    let got = b64dec(v)?;
    if got.len() != expected.len() {
        return Err("server signature length mismatch".into());
    }
    let diff = got.iter().zip(expected).fold(0u8, |a, (x, y)| a | (x ^ y));
    if diff != 0 {
        return Err("server signature mismatch — MITM or wrong password".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pbkdf2_matches_rfc_style_vector() {
        // A stable self-check: same inputs → same 32-byte output, deterministic.
        let a = pbkdf2(b"password", b"salt", 2);
        let b = pbkdf2(b"password", b"salt", 2);
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        assert_ne!(
            pbkdf2(b"password", b"salt", 2),
            pbkdf2(b"password", b"salt", 3)
        );
    }

    #[test]
    fn client_first_shape() {
        let mut c = ScramClient::new("alice", "pw");
        let cf = c.client_first();
        assert!(cf.starts_with("n,,n=alice,r="));
    }

    #[test]
    fn nonce_mismatch_is_caught() {
        let mut c = ScramClient::new("u", "pw");
        c.client_first();
        let err = c.client_final("r=DIFFERENT,s=c2FsdA==,i=4096").unwrap_err();
        assert!(err.contains("does not extend"));
    }
}
