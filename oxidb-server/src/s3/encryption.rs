//! S3 Server-Side Encryption (SSE-S3 and SSE-C).
//!
//! - **SSE-S3**: Server-managed key via `OXIDB_S3_ENCRYPTION_KEY` env var (32 hex bytes).
//!   Triggered by header `x-amz-server-side-encryption: AES256`.
//! - **SSE-C**: Customer-provided key per request.
//!   Headers: `x-amz-server-side-encryption-customer-algorithm: AES256`,
//!            `x-amz-server-side-encryption-customer-key: <base64 32-byte key>`,
//!            `x-amz-server-side-encryption-customer-key-md5: <base64 MD5 of key>`.
//! - **Default bucket encryption**: If `OXIDB_S3_DEFAULT_ENCRYPTION=true`, all PUTs
//!   without explicit encryption headers are auto-encrypted with the server key.

use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use md5::Digest as Md5Digest;
use md5::Md5;
use rand::RngCore;

use super::http::HttpResponse;

const NONCE_LEN: usize = 12;

/// Encryption mode determined from request headers.
pub enum SseMode {
    /// No encryption.
    None,
    /// SSE-S3: use the server-managed key.
    S3,
    /// SSE-C: use the customer-provided key (raw 32 bytes).
    CustomerKey(Vec<u8>),
}

/// Server-side encryption configuration.
pub struct S3Encryption {
    /// Server-managed AES-256-GCM key (for SSE-S3).
    server_key: Aes256Gcm,
    /// If true, all objects are encrypted with server key by default.
    pub default_encryption: bool,
}

impl S3Encryption {
    /// Create from a 32-byte hex-encoded key string.
    pub fn from_hex_key(hex_key: &str, default_encryption: bool) -> Option<Arc<Self>> {
        let key_bytes = hex_decode(hex_key)?;
        if key_bytes.len() != 32 {
            eprintln!(
                "[s3] encryption key must be 64 hex chars (32 bytes), got {} bytes",
                key_bytes.len()
            );
            return None;
        }
        let cipher = Aes256Gcm::new_from_slice(&key_bytes).ok()?;
        Some(Arc::new(Self {
            server_key: cipher,
            default_encryption,
        }))
    }

    /// Encrypt data with the server-managed key.
    pub fn encrypt_s3(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
        encrypt_aes256gcm(&self.server_key, plaintext)
    }

    /// Decrypt data with the server-managed key.
    pub fn decrypt_s3(&self, sealed: &[u8]) -> Result<Vec<u8>, String> {
        decrypt_aes256gcm(&self.server_key, sealed)
    }
}

/// Encrypt data with a customer-provided raw key (SSE-C).
pub fn encrypt_ssec(customer_key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(customer_key)
        .map_err(|e| format!("invalid customer key: {e}"))?;
    encrypt_aes256gcm(&cipher, plaintext)
}

/// Decrypt data with a customer-provided raw key (SSE-C).
pub fn decrypt_ssec(customer_key: &[u8], sealed: &[u8]) -> Result<Vec<u8>, String> {
    let cipher = Aes256Gcm::new_from_slice(customer_key)
        .map_err(|e| format!("invalid customer key: {e}"))?;
    decrypt_aes256gcm(&cipher, sealed)
}

/// Determine encryption mode from request headers.
pub fn parse_sse_headers(
    headers: &HashMap<String, String>,
    server_encryption: Option<&S3Encryption>,
) -> Result<SseMode, HttpResponse> {
    // Check SSE-C headers first
    if let Some(algo) = headers.get("x-amz-server-side-encryption-customer-algorithm") {
        if algo != "AES256" {
            return Err(super::http::error_response(
                400,
                "InvalidEncryptionAlgorithmError",
                "Only AES256 is supported for SSE-C",
                "",
            ));
        }
        let key_b64 = headers
            .get("x-amz-server-side-encryption-customer-key")
            .ok_or_else(|| {
                super::http::error_response(
                    400,
                    "InvalidArgument",
                    "x-amz-server-side-encryption-customer-key header is required for SSE-C",
                    "",
                )
            })?;
        let key_bytes = base64_decode(key_b64).ok_or_else(|| {
            super::http::error_response(
                400,
                "InvalidArgument",
                "Invalid base64 in x-amz-server-side-encryption-customer-key",
                "",
            )
        })?;
        if key_bytes.len() != 32 {
            return Err(super::http::error_response(
                400,
                "InvalidArgument",
                "SSE-C key must be exactly 32 bytes (256 bits)",
                "",
            ));
        }
        // Verify MD5 if provided
        if let Some(expected_md5) = headers.get("x-amz-server-side-encryption-customer-key-md5") {
            let actual_md5 = base64_encode(&md5_hash(&key_bytes));
            if actual_md5 != *expected_md5 {
                return Err(super::http::error_response(
                    400,
                    "InvalidArgument",
                    "SSE-C key MD5 does not match",
                    "",
                ));
            }
        }
        return Ok(SseMode::CustomerKey(key_bytes));
    }

    // Check SSE-S3 header
    if let Some(algo) = headers.get("x-amz-server-side-encryption") {
        if algo == "AES256" {
            if server_encryption.is_none() {
                return Err(super::http::error_response(
                    400,
                    "InvalidArgument",
                    "SSE-S3 requested but no server encryption key configured (set OXIDB_S3_ENCRYPTION_KEY)",
                    "",
                ));
            }
            return Ok(SseMode::S3);
        }
        return Err(super::http::error_response(
            400,
            "InvalidEncryptionAlgorithmError",
            "Only AES256 is supported",
            "",
        ));
    }

    // Default encryption if configured
    if let Some(enc) = server_encryption {
        if enc.default_encryption {
            return Ok(SseMode::S3);
        }
    }

    Ok(SseMode::None)
}

/// Encrypt data based on the determined SSE mode.
pub fn encrypt_data(
    data: &[u8],
    mode: &SseMode,
    server_encryption: Option<&S3Encryption>,
) -> Result<Vec<u8>, String> {
    match mode {
        SseMode::None => Ok(data.to_vec()),
        SseMode::S3 => server_encryption
            .ok_or_else(|| "no server encryption key".to_string())?
            .encrypt_s3(data),
        SseMode::CustomerKey(key) => encrypt_ssec(key, data),
    }
}

/// Decrypt data based on the determined SSE mode.
pub fn decrypt_data(
    data: &[u8],
    mode: &SseMode,
    server_encryption: Option<&S3Encryption>,
) -> Result<Vec<u8>, String> {
    match mode {
        SseMode::None => Ok(data.to_vec()),
        SseMode::S3 => server_encryption
            .ok_or_else(|| "no server encryption key".to_string())?
            .decrypt_s3(data),
        SseMode::CustomerKey(key) => decrypt_ssec(key, data),
    }
}

/// Add encryption response headers based on the mode used.
pub fn add_encryption_headers(resp: HttpResponse, mode: &SseMode) -> HttpResponse {
    match mode {
        SseMode::None => resp,
        SseMode::S3 => resp.with_header("x-amz-server-side-encryption", "AES256"),
        SseMode::CustomerKey(_) => {
            resp.with_header("x-amz-server-side-encryption-customer-algorithm", "AES256")
        }
    }
}

/// Store the encryption mode indicator in object metadata so we know how to decrypt.
pub fn sse_metadata_marker(mode: &SseMode) -> Option<(String, String)> {
    match mode {
        SseMode::None => None,
        SseMode::S3 => Some(("_sse".to_string(), "AES256".to_string())),
        SseMode::CustomerKey(_) => Some(("_sse".to_string(), "SSE-C".to_string())),
    }
}

/// Encode a key for storage in multipart upload marker.
pub fn base64_encode_key(key: &[u8]) -> String {
    base64_encode(key)
}

/// Decode a key from multipart upload marker.
pub fn base64_decode_key(encoded: &str) -> Option<Vec<u8>> {
    base64_decode(encoded)
}

/// Check stored metadata to determine if an object was encrypted with SSE-S3.
pub fn is_sse_s3(metadata: &serde_json::Value) -> bool {
    metadata
        .get("metadata")
        .and_then(|m| m.get("_sse"))
        .and_then(|v| v.as_str())
        == Some("AES256")
}

/// Check stored metadata to determine if an object was encrypted with SSE-C.
pub fn is_sse_c(metadata: &serde_json::Value) -> bool {
    metadata
        .get("metadata")
        .and_then(|m| m.get("_sse"))
        .and_then(|v| v.as_str())
        == Some("SSE-C")
}

// ---------------------------------------------------------------------------
// Crypto helpers
// ---------------------------------------------------------------------------

fn encrypt_aes256gcm(cipher: &Aes256Gcm, plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| format!("encryption failed: {e}"))?;

    let mut sealed = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    sealed.extend_from_slice(&nonce_bytes);
    sealed.extend_from_slice(&ciphertext);
    Ok(sealed)
}

fn decrypt_aes256gcm(cipher: &Aes256Gcm, sealed: &[u8]) -> Result<Vec<u8>, String> {
    if sealed.len() < NONCE_LEN {
        return Err("sealed data too short".to_string());
    }
    let nonce = Nonce::from_slice(&sealed[..NONCE_LEN]);
    let ciphertext = &sealed[NONCE_LEN..];

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| format!("decryption failed: {e}"))
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    let hex = hex.trim();
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    B64.decode(input.trim()).ok()
}

fn base64_encode(data: &[u8]) -> String {
    B64.encode(data)
}

fn md5_hash(data: &[u8]) -> [u8; 16] {
    let result = Md5::digest(data);
    let mut out = [0u8; 16];
    out.copy_from_slice(&result);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let bytes =
            hex_decode("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
        assert_eq!(bytes.len(), 32);
    }

    #[test]
    fn base64_roundtrip() {
        let data = b"Hello, World!";
        let encoded = base64_encode(data);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn md5_known_vector() {
        // MD5("") = d41d8cd98f00b204e9800998ecf8427e
        let hash = md5_hash(b"");
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn encrypt_decrypt_s3() {
        let key_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let enc = S3Encryption::from_hex_key(key_hex, false).unwrap();
        let plaintext = b"secret S3 data";
        let sealed = enc.encrypt_s3(plaintext).unwrap();
        assert_ne!(&sealed[..], plaintext);
        let decrypted = enc.decrypt_s3(&sealed).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_ssec() {
        let key = [0x42u8; 32];
        let plaintext = b"customer encrypted data";
        let sealed = encrypt_ssec(&key, plaintext).unwrap();
        let decrypted = decrypt_ssec(&key, &sealed).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn ssec_wrong_key_fails() {
        let key1 = [0x42u8; 32];
        let key2 = [0x99u8; 32];
        let sealed = encrypt_ssec(&key1, b"data").unwrap();
        assert!(decrypt_ssec(&key2, &sealed).is_err());
    }

    #[test]
    fn parse_sse_none() {
        let headers = HashMap::new();
        let mode = parse_sse_headers(&headers, None).unwrap();
        assert!(matches!(mode, SseMode::None));
    }

    #[test]
    fn parse_sse_s3() {
        let mut headers = HashMap::new();
        headers.insert(
            "x-amz-server-side-encryption".to_string(),
            "AES256".to_string(),
        );
        let key_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let enc = S3Encryption::from_hex_key(key_hex, false).unwrap();
        let mode = parse_sse_headers(&headers, Some(&enc)).unwrap();
        assert!(matches!(mode, SseMode::S3));
    }

    #[test]
    fn parse_sse_c() {
        let key = [0x42u8; 32];
        let key_b64 = base64_encode(&key);
        let key_md5 = base64_encode(&md5_hash(&key));
        let mut headers = HashMap::new();
        headers.insert(
            "x-amz-server-side-encryption-customer-algorithm".to_string(),
            "AES256".to_string(),
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key".to_string(),
            key_b64,
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5".to_string(),
            key_md5,
        );
        let mode = parse_sse_headers(&headers, None).unwrap();
        assert!(matches!(mode, SseMode::CustomerKey(_)));
    }

    #[test]
    fn default_encryption_kicks_in() {
        let headers = HashMap::new();
        let key_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let enc = S3Encryption::from_hex_key(key_hex, true).unwrap();
        let mode = parse_sse_headers(&headers, Some(&enc)).unwrap();
        assert!(matches!(mode, SseMode::S3));
    }
}
