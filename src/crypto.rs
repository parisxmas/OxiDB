use std::path::Path;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;

use crate::error::{Error, Result};

const NONCE_LEN: usize = 12;

/// Wraps an AES-256-GCM cipher for encrypt/decrypt operations.
#[derive(Clone)]
pub struct EncryptionKey {
    cipher: Aes256Gcm,
}

impl EncryptionKey {
    /// Load a 32-byte key from a file.
    pub fn load_from_file(path: &Path) -> Result<Arc<Self>> {
        let key_bytes = std::fs::read(path).map_err(|e| {
            Error::Encryption(format!("failed to read key file {}: {}", path.display(), e))
        })?;
        if key_bytes.len() != 32 {
            return Err(Error::Encryption(format!(
                "encryption key must be exactly 32 bytes, got {}",
                key_bytes.len()
            )));
        }
        let cipher = Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| {
            Error::Encryption(format!("invalid encryption key: {e}"))
        })?;
        Ok(Arc::new(Self { cipher }))
    }

    /// Encrypt plaintext. Returns `[nonce:12][ciphertext+tag]`.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self.cipher.encrypt(nonce, plaintext).map_err(|e| {
            Error::Encryption(format!("encryption failed: {e}"))
        })?;

        let mut sealed = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        sealed.extend_from_slice(&nonce_bytes);
        sealed.extend_from_slice(&ciphertext);
        Ok(sealed)
    }

    /// Decrypt a sealed buffer `[nonce:12][ciphertext+tag]`.
    pub fn decrypt(&self, sealed: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < NONCE_LEN {
            return Err(Error::Decryption("sealed data too short".into()));
        }
        let nonce = Nonce::from_slice(&sealed[..NONCE_LEN]);
        let ciphertext = &sealed[NONCE_LEN..];

        self.cipher.decrypt(nonce, ciphertext).map_err(|e| {
            Error::Decryption(format!("decryption failed: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let mut file = NamedTempFile::new().unwrap();
        let key = [0x42u8; 32];
        file.write_all(&key).unwrap();
        file.flush().unwrap();

        let ek = EncryptionKey::load_from_file(file.path()).unwrap();
        let plaintext = b"hello world encryption test";
        let sealed = ek.encrypt(plaintext).unwrap();
        assert_ne!(&sealed, plaintext);
        let decrypted = ek.decrypt(&sealed).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let mut file1 = NamedTempFile::new().unwrap();
        file1.write_all(&[0x42u8; 32]).unwrap();
        file1.flush().unwrap();
        let ek1 = EncryptionKey::load_from_file(file1.path()).unwrap();

        let mut file2 = NamedTempFile::new().unwrap();
        file2.write_all(&[0x99u8; 32]).unwrap();
        file2.flush().unwrap();
        let ek2 = EncryptionKey::load_from_file(file2.path()).unwrap();

        let sealed = ek1.encrypt(b"secret data").unwrap();
        assert!(ek2.decrypt(&sealed).is_err());
    }

    #[test]
    fn invalid_key_size() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&[0u8; 16]).unwrap();
        file.flush().unwrap();
        assert!(EncryptionKey::load_from_file(file.path()).is_err());
    }
}
