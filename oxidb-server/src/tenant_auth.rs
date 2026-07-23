//! The data plane's only piece of OxiBase (ADR-0021): resolving a tenant
//! project's JWT secret so the REST listener can verify a `?db=<ref>` token.
//!
//! The control plane (accounts, provisioning, key rotation, the dashboard) is a
//! separate `oxibase` binary; it writes project rows — with the per-project
//! secret sealed under `OXIDB_SEAL_KEY` — into a normal `oxibase` metadata
//! database over the wire. This hook reads that database **locally** on the
//! authenticated hot path and unseals with the seal key alone (never the
//! session-signing master secret, which lives only in the control plane).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use oxidb::{DatabaseManager, EncryptionKey};
use serde_json::json;
use sha2::{Digest, Sha256};

/// The metadata database OxiBase records projects in.
const META_DB: &str = "oxibase";
/// The collection of project rows within [`META_DB`].
const PROJECTS: &str = "projects";
/// How long a resolved project secret stays cached. Bounded (not permanent
/// invalidation) because the control plane runs in a separate process and cannot
/// clear this cache on rotation; a rotated key takes effect within the TTL.
const SECRET_CACHE_TTL_SECS: u64 = 5;

/// `true` when this data plane participates in OxiBase (`OXIDB_PLATFORM=1`), so
/// the REST listener consults [`project_secret`] for `?db=<ref>` requests.
pub fn enabled() -> bool {
    matches!(
        std::env::var("OXIDB_PLATFORM")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The session-signing master secret lives only in the control plane; the data
/// plane reads it here **solely** as a fallback for the seal key on a
/// single-host deployment where `OXIDB_SEAL_KEY` is not set separately.
fn master_secret() -> Option<String> {
    std::env::var("OXIDB_PLATFORM_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
}

/// The AES-GCM key that unseals per-project secrets: a dedicated
/// `OXIDB_SEAL_KEY`, else derived from the master secret (single-binary
/// compatibility). Must match the control plane's sealing key.
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Cache of `ref → (decrypted jwt_secret, cached_at)` so a project's secret is
/// not re-read and AES-decrypted on *every* authenticated request. Entries
/// expire after [`SECRET_CACHE_TTL_SECS`].
fn secret_cache() -> &'static Mutex<HashMap<String, (String, u64)>> {
    static CACHE: OnceLock<Mutex<HashMap<String, (String, u64)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The per-project JWT secret for `db_ref`, if it names an OxiBase project.
/// `None` for the metadata db itself, an unknown ref, or when nothing is
/// configured — the caller then falls back to the global `OXIDB_JWT_SECRET`.
pub fn project_secret(mgr: &DatabaseManager, db_ref: &str) -> Option<String> {
    if db_ref == META_DB {
        return None; // the platform's own store is not a tenant project
    }
    let now = now_secs();
    if let Some((secret, at)) = secret_cache().lock().unwrap().get(db_ref) {
        if now.saturating_sub(*at) < SECRET_CACHE_TTL_SECS {
            return Some(secret.clone());
        }
    }
    let pdb = mgr.get_database(META_DB).ok()?;
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
        .insert(db_ref.to_string(), (secret.clone(), now));
    Some(secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_roundtrip() {
        let key = derive_key("seal-material");
        let sealed = key.encrypt(b"project-secret").unwrap();
        assert_eq!(key.decrypt(&sealed).unwrap(), b"project-secret");
    }

    #[test]
    fn only_the_seal_key_unseals() {
        // The data plane unseals with the seal key, independent of the master
        // session-signing secret. A different key must fail to unseal.
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
}
