//! Client-side SCRAM-SHA-256 (RFC 7677), ported from the server's
//! `scram_client.rs` but standalone — it uses the `sha2`/`hmac`/`pbkdf2`
//! crates directly instead of the server's crypto helpers, so the desktop
//! client can authenticate without pulling in the whole server crate.
//!
//! Handshake framing (length-prefixed JSON, same as every other request):
//!   1. `{"command":"authenticate","payload":<client-first>}` → data.payload = server-first
//!   2. `{"command":"authenticate_continue","payload":<client-final>}` → data.payload = server-final
//!   3. verify the server signature in server-final.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

fn b64enc(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}
fn b64dec(s: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| format!("base64: {e}"))
}
fn hmac256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    m.update(msg);
    m.finalize().into_bytes().to_vec()
}
fn sha256(data: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().to_vec()
}
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut out = [0u8; 32];
    pbkdf2::pbkdf2::<HmacSha256>(password, salt, iterations, &mut out)
        .expect("pbkdf2 output length is valid");
    out.to_vec()
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
        getrandom::getrandom(&mut nonce).expect("OS RNG available");
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: b64enc(&nonce),
            client_first_bare: None,
        }
    }

    /// Test-only constructor with a fixed client nonce, so a known RFC 7677
    /// test vector can be reproduced deterministically.
    #[cfg(test)]
    fn with_nonce(username: &str, password: &str, nonce: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: nonce.to_string(),
            client_first_bare: None,
        }
    }

    /// `n,,n=<user>,r=<client_nonce>`
    pub fn client_first(&mut self) -> String {
        let bare = format!("n={},r={}", self.username, self.client_nonce);
        self.client_first_bare = Some(bare.clone());
        format!("n,,{bare}")
    }

    /// Process server-first, emit (client-final, expected server signature).
    pub fn client_final(&mut self, server_first: &str) -> Result<(String, Vec<u8>), String> {
        let bare = self
            .client_first_bare
            .clone()
            .ok_or("client_final before client_first")?;

        let mut combined_nonce = String::new();
        let mut salt_b64 = String::new();
        let mut iters: u32 = 0;
        for part in server_first.split(',') {
            if let Some(r) = part.strip_prefix("r=") {
                combined_nonce = r.to_string();
            } else if let Some(s) = part.strip_prefix("s=") {
                salt_b64 = s.to_string();
            } else if let Some(i) = part.strip_prefix("i=") {
                iters = i.parse().map_err(|e| format!("bad i=: {e}"))?;
            }
        }
        if combined_nonce.is_empty() || salt_b64.is_empty() || iters == 0 {
            return Err(format!("malformed server-first: {server_first:?}"));
        }
        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err("server nonce does not extend client nonce — possible MITM".into());
        }

        let salt = b64dec(&salt_b64)?;
        let salted = pbkdf2_sha256(self.password.as_bytes(), &salt, iters);
        let client_key = hmac256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);

        let client_final_no_proof = format!("c=biws,r={combined_nonce}");
        let auth_message = format!("{bare},{server_first},{client_final_no_proof}");

        let client_sig = hmac256(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_sig.iter())
            .map(|(k, s)| k ^ s)
            .collect();
        let client_final = format!("{client_final_no_proof},p={}", b64enc(&proof));

        let server_key = hmac256(&salted, b"Server Key");
        let expected_server_sig = hmac256(&server_key, auth_message.as_bytes());
        self.password.clear();
        Ok((client_final, expected_server_sig))
    }
}

/// Verify `v=<server_signature>` from server-final (constant-time).
pub fn verify_server_final(server_final: &str, expected: &[u8]) -> Result<(), String> {
    let v = server_final
        .strip_prefix("v=")
        .ok_or("server-final must start with v=")?;
    let got = b64dec(v)?;
    if got.len() != expected.len() {
        return Err("server signature length mismatch".into());
    }
    let mut diff = 0u8;
    for (a, b) in got.iter().zip(expected.iter()) {
        diff |= a ^ b;
    }
    if diff != 0 {
        return Err("server signature mismatch — wrong password or MITM".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 §5 worked example — the canonical SCRAM-SHA-256 test vector.
    /// If our crypto (pbkdf2/hmac/sha256/base64 wiring) is byte-correct, the
    /// client proof and the expected server signature match the RFC exactly.
    #[test]
    fn rfc7677_test_vector() {
        let mut c = ScramClient::with_nonce("user", "pencil", "rOprNGfwEbeRWgbNEkqO");
        let first = c.client_first();
        assert_eq!(first, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

        let server_first =
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let (client_final, expected_sig) = c.client_final(server_first).unwrap();

        // Client proof from the RFC.
        assert_eq!(
            client_final,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        // Server signature from the RFC.
        assert_eq!(b64enc(&expected_sig), "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");

        // And verify_server_final accepts the RFC's server-final.
        verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=", &expected_sig)
            .unwrap();
    }

    #[test]
    fn nonce_mismatch_refused() {
        let mut c = ScramClient::with_nonce("u", "pw", "AAAA");
        c.client_first();
        let err = c
            .client_final("r=DIFFERENT,s=ZGVhZGJlZWY=,i=4096")
            .unwrap_err();
        assert!(err.contains("does not extend"), "got: {err}");
    }
}
