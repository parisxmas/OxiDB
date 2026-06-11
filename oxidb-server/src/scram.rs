use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::auth::{Role, UserStore};

type HmacSha256 = Hmac<Sha256>;

// Public re-exports used by crate::auth::derive_scram_verifier to
// avoid duplicating the SHA-256 / HMAC / PBKDF2 / base64 helpers in
// two places. The "_pub" suffix keeps the existing private helpers
// untouched and signals "this is module-internal, used by other
// modules in the same crate".
pub fn hmac_sha256_pub(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac_sha256(key, data)
}
pub fn sha256_hash_pub(data: &[u8]) -> Vec<u8> {
    sha256_hash(data)
}
pub fn pbkdf2_sha256_pub(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    pbkdf2_sha256(password, salt, iterations)
}
pub fn generate_salt_pub() -> Vec<u8> {
    generate_salt()
}
pub fn base64_encode_simple_pub(data: &[u8]) -> String {
    base64_encode_simple(data)
}
pub fn base64_decode_simple_pub(input: &str) -> Result<Vec<u8>, String> {
    base64_decode_simple(input)
}

/// Server-side SCRAM-SHA-256 state machine (simplified RFC 7677).
///
/// Flow:
/// 1. Client sends: `n,,n=<user>,r=<client_nonce>`
/// 2. Server responds: `r=<client_nonce><server_nonce>,s=<salt>,i=<iterations>`
/// 3. Client sends: `c=biws,r=<combined_nonce>,p=<client_proof>`
/// 4. Server verifies and responds: `v=<server_signature>` (success) or error
pub struct ScramState {
    username: String,
    #[allow(dead_code)]
    client_nonce: String,
    #[allow(dead_code)]
    server_nonce: String,
    combined_nonce: String,
    #[allow(dead_code)]
    salt: Vec<u8>,
    auth_message: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

impl ScramState {
    /// Process client-first message. Returns (server_first_message, state).
    ///
    /// RFC 7677 §3: the server's stored credentials are
    /// (salt, iter_count, stored_key, server_key) — derived ONCE
    /// from the plaintext password at user-creation / password-update
    /// time. This function looks them up on the UserRecord and
    /// echoes salt+iter_count back to the client in server-first.
    ///
    /// Users created before the SCRAM-verifier migration have
    /// `scram_*` fields = None; this function refuses SCRAM auth for
    /// them with a clear error pointing the operator at
    /// `account passwd` (which now writes the verifier on save).
    pub fn process_client_first(
        client_msg: &str,
        user_store: &UserStore,
    ) -> Result<(String, Self), String> {
        // Parse: n,,n=<user>,r=<nonce>
        let msg = if let Some(stripped) = client_msg.strip_prefix("n,,") {
            stripped
        } else {
            return Err("invalid client-first: must start with 'n,,'".into());
        };

        let mut username = None;
        let mut client_nonce = None;

        for part in msg.split(',') {
            if let Some(u) = part.strip_prefix("n=") {
                username = Some(u.to_string());
            } else if let Some(r) = part.strip_prefix("r=") {
                client_nonce = Some(r.to_string());
            }
        }

        let username = username.ok_or("missing username in client-first")?;
        let client_nonce = client_nonce.ok_or("missing nonce in client-first")?;

        // Verify user exists and has SCRAM credentials. Legacy users
        // (pre-verifier migration) lack scram_* fields and must
        // have their password re-set with a SCRAM-aware
        // `account passwd` to be SCRAM-auth'able.
        let user = user_store
            .get_user(&username)
            .ok_or_else(|| format!("user '{}' not found", username))?;
        let salt_b64 = user.scram_salt.as_ref().ok_or_else(|| {
            format!(
                "user '{}' has no SCRAM verifier — update its password \
                via `account passwd` so the verifier is generated",
                username
            )
        })?;
        let iter_count = user
            .scram_iter_count
            .ok_or_else(|| "user has SCRAM salt but no iter_count".to_string())?;
        let stored_key_b64 = user
            .scram_stored_key
            .as_ref()
            .ok_or_else(|| "user has SCRAM salt but no stored_key".to_string())?;
        let server_key_b64 = user
            .scram_server_key
            .as_ref()
            .ok_or_else(|| "user has SCRAM salt but no server_key".to_string())?;

        let salt = base64_decode_simple(salt_b64).map_err(|e| format!("decode salt: {}", e))?;
        let stored_key = base64_decode_simple(stored_key_b64)
            .map_err(|e| format!("decode stored_key: {}", e))?;
        let server_key = base64_decode_simple(server_key_b64)
            .map_err(|e| format!("decode server_key: {}", e))?;

        // Generate server nonce
        let server_nonce = generate_nonce();
        let combined_nonce = format!("{}{}", client_nonce, server_nonce);

        // client-first-message-bare (without GS2 header)
        let client_first_bare = msg.to_string();

        let server_first = format!("r={},s={},i={}", combined_nonce, salt_b64, iter_count);

        let auth_message = format!(
            "{},{},c=biws,r={}",
            client_first_bare, server_first, combined_nonce
        );

        Ok((
            server_first,
            Self {
                username,
                client_nonce,
                server_nonce,
                combined_nonce,
                salt,
                auth_message,
                stored_key,
                server_key,
            },
        ))
    }

    /// Process client-final message. Returns (server_final_message, role).
    pub fn process_client_final(
        &self,
        client_msg: &str,
        user_store: &UserStore,
    ) -> Result<(String, Role), String> {
        // Parse: c=biws,r=<combined_nonce>,p=<proof>
        let mut received_nonce = None;
        let mut proof_b64 = None;

        for part in client_msg.split(',') {
            if let Some(r) = part.strip_prefix("r=") {
                received_nonce = Some(r.to_string());
            } else if let Some(p) = part.strip_prefix("p=") {
                proof_b64 = Some(p.to_string());
            }
        }

        let received_nonce = received_nonce.ok_or("missing nonce in client-final")?;
        let proof_b64 = proof_b64.ok_or("missing proof in client-final")?;

        // Verify nonce
        if received_nonce != self.combined_nonce {
            return Err("nonce mismatch".into());
        }

        // Verify proof
        let client_proof = base64_decode_simple(&proof_b64)?;
        let client_signature = hmac_sha256(&self.stored_key, self.auth_message.as_bytes());

        // Recover client_key = client_proof XOR client_signature
        let mut recovered_client_key = vec![0u8; client_proof.len()];
        if client_proof.len() != client_signature.len() {
            return Err("proof length mismatch".into());
        }
        for i in 0..client_proof.len() {
            recovered_client_key[i] = client_proof[i] ^ client_signature[i];
        }

        // Verify: SHA-256(recovered_client_key) == stored_key
        let recovered_stored_key = sha256_hash(&recovered_client_key);
        if recovered_stored_key != self.stored_key {
            return Err("authentication failed".into());
        }

        // Compute server signature
        let server_signature = hmac_sha256(&self.server_key, self.auth_message.as_bytes());
        let server_final = format!("v={}", base64_encode_simple(&server_signature));

        let role = user_store
            .get_user(&self.username)
            .map(|u| u.role)
            .ok_or("user disappeared during auth")?;

        Ok((server_final, role))
    }

    pub fn username(&self) -> &str {
        &self.username
    }
}

// ---------------------------------------------------------------------------
// Crypto helpers
// ---------------------------------------------------------------------------

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hash(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    // PBKDF2-HMAC-SHA-256: single block (dkLen = 32)
    let mut u_prev = {
        let mut mac = HmacSha256::new_from_slice(password).expect("HMAC key length");
        mac.update(salt);
        mac.update(&1u32.to_be_bytes()); // block index = 1
        mac.finalize().into_bytes().to_vec()
    };
    let mut result = u_prev.clone();

    for _ in 1..iterations {
        let mut mac = HmacSha256::new_from_slice(password).expect("HMAC key length");
        mac.update(&u_prev);
        u_prev = mac.finalize().into_bytes().to_vec();
        for j in 0..result.len() {
            result[j] ^= u_prev[j];
        }
    }

    result
}

fn generate_nonce() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 18];
    rand::rng().fill_bytes(&mut bytes);
    base64_encode_simple(&bytes)
}

fn generate_salt() -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes.to_vec()
}

fn base64_encode_simple(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        result.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        result.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn base64_decode_simple(input: &str) -> Result<Vec<u8>, String> {
    let input = input.trim_end_matches('=');
    let mut buf = Vec::new();
    let mut accum: u32 = 0;
    let mut bits = 0;

    for c in input.chars() {
        let val = match c {
            'A'..='Z' => c as u32 - 'A' as u32,
            'a'..='z' => c as u32 - 'a' as u32 + 26,
            '0'..='9' => c as u32 - '0' as u32 + 52,
            '+' => 62,
            '/' => 63,
            _ => return Err(format!("invalid base64 char: {}", c)),
        };
        accum = (accum << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            buf.push((accum >> bits) as u8);
            accum &= (1 << bits) - 1;
        }
    }

    Ok(buf)
}
