use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use argon2::Argon2;
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    ReadWrite,
    Read,
    /// A signed-in **end-user** of an OxiBase project (a person, not a client
    /// tier). Coarse privileges match `Read` — writes are let through to the
    /// security rules — but the token carries the user's identity, so
    /// `auth.username` / `auth.role == 'authenticated'` drive per-row rules.
    Authenticated,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::ReadWrite => "readWrite",
            Role::Read => "read",
            Role::Authenticated => "authenticated",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(Role::Admin),
            "readWrite" | "readwrite" => Some(Role::ReadWrite),
            "read" => Some(Role::Read),
            "authenticated" => Some(Role::Authenticated),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub username: String,
    /// Argon2 hash of the password. Used by the direct `authenticate`
    /// (plaintext-over-the-wire) code path. SCRAM does NOT use this —
    /// SCRAM has its own verifier below, derived from the same
    /// plaintext password at creation time.
    pub password_hash: String,
    pub role: Role,
    /// Per-database role overrides. If a user has a role set for a specific
    /// database, that role is used instead of the global `role` when operating
    /// on that database.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub db_roles: HashMap<String, Role>,

    /// SCRAM-SHA-256 verifier (RFC 7677 §3). The four fields together
    /// let the server complete the SCRAM exchange without ever
    /// touching the plaintext password again — they're derived once,
    /// at user creation / password update, from the user's plaintext.
    ///
    /// All four are `Option`s for backward compatibility: a user
    /// record stored before this revision has none of them; the
    /// server refuses SCRAM auth for such users with a clear error
    /// pointing the operator at `account passwd`.
    ///
    /// Encodings: scram_salt / scram_stored_key / scram_server_key
    /// are base64-encoded byte strings; scram_iter_count is the
    /// integer iteration count fed to PBKDF2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_salt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_iter_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_stored_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_server_key: Option<String>,
}

/// SCRAM iteration count used at user-creation / password-update time.
/// 4096 matches what the per-call generator used to produce; high
/// enough to slow a stolen-verifier attacker without making auth
/// noticeably slow for a real client.
pub const SCRAM_ITER_COUNT: u32 = 4096;

pub struct UserStore {
    users: HashMap<String, UserRecord>,
    store_path: PathBuf,
}

impl UserStore {
    /// Load or create the user store. On first startup, creates a default admin user
    /// and prints the random password to stderr.
    pub fn open(data_dir: &Path) -> Result<Self, String> {
        let auth_dir = data_dir.join("_auth");
        fs::create_dir_all(&auth_dir).map_err(|e| format!("failed to create auth dir: {e}"))?;

        let store_path = auth_dir.join("users.json");
        let mut users = HashMap::new();

        if store_path.exists() {
            let data = fs::read_to_string(&store_path)
                .map_err(|e| format!("failed to read users.json: {e}"))?;
            let records: Vec<UserRecord> = serde_json::from_str(&data)
                .map_err(|e| format!("failed to parse users.json: {e}"))?;
            for record in records {
                users.insert(record.username.clone(), record);
            }
        }

        let mut store = Self { users, store_path };

        // Create default admin if no users exist
        if store.users.is_empty() {
            let password = generate_random_password();
            store.create_user_internal("admin", &password, Role::Admin)?;
            eprintln!("=== FIRST STARTUP: default admin user created ===");
            eprintln!("  username: admin");
            eprintln!("  password: {password}");
            eprintln!("  CHANGE THIS PASSWORD IMMEDIATELY");
            eprintln!("================================================");
        }

        Ok(store)
    }

    /// Authenticate a user. Returns their role on success.
    pub fn authenticate(&self, username: &str, password: &str) -> Option<Role> {
        let record = self.users.get(username)?;
        let parsed_hash = PasswordHash::new(&record.password_hash).ok()?;
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .ok()?;
        Some(record.role)
    }

    /// Look up a user's stored password hash (for SCRAM).
    pub fn get_user(&self, username: &str) -> Option<&UserRecord> {
        self.users.get(username)
    }

    pub fn create_user(
        &mut self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<(), String> {
        if self.users.contains_key(username) {
            return Err(format!("user '{}' already exists", username));
        }
        self.create_user_internal(username, password, role)
    }

    pub fn drop_user(&mut self, username: &str) -> Result<(), String> {
        if !self.users.contains_key(username) {
            return Err(format!("user '{}' not found", username));
        }
        self.users.remove(username);
        self.save()
    }

    pub fn update_user(
        &mut self,
        username: &str,
        password: Option<&str>,
        role: Option<Role>,
    ) -> Result<(), String> {
        let record = self
            .users
            .get_mut(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;

        if let Some(pw) = password {
            record.password_hash = hash_password(pw)?;
            // Re-derive the SCRAM verifier from the new plaintext.
            // Skipping this would leave SCRAM auth on the OLD
            // password — confusing and a security gap.
            let verifier = derive_scram_verifier(pw);
            record.scram_salt = Some(verifier.salt_b64);
            record.scram_iter_count = Some(verifier.iter_count);
            record.scram_stored_key = Some(verifier.stored_key_b64);
            record.scram_server_key = Some(verifier.server_key_b64);
        }
        if let Some(r) = role {
            record.role = r;
        }
        self.save()
    }

    pub fn list_users(&self) -> Vec<serde_json::Value> {
        self.users
            .values()
            .map(|r| {
                let mut user = serde_json::json!({
                    "username": r.username,
                    "role": r.role.as_str(),
                });
                if !r.db_roles.is_empty() {
                    let db_roles: serde_json::Map<String, serde_json::Value> = r
                        .db_roles
                        .iter()
                        .map(|(db, role)| {
                            (
                                db.clone(),
                                serde_json::Value::String(role.as_str().to_string()),
                            )
                        })
                        .collect();
                    user.as_object_mut()
                        .unwrap()
                        .insert("db_roles".to_string(), serde_json::Value::Object(db_roles));
                }
                user
            })
            .collect()
    }

    /// Grant a per-database role override for a user.
    pub fn grant_db_role(
        &mut self,
        username: &str,
        database: &str,
        role: Role,
    ) -> Result<(), String> {
        let record = self
            .users
            .get_mut(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;
        record.db_roles.insert(database.to_string(), role);
        self.save()
    }

    /// Revoke a per-database role override, reverting to the global role.
    pub fn revoke_db_role(&mut self, username: &str, database: &str) -> Result<(), String> {
        let record = self
            .users
            .get_mut(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;
        if !record.db_roles.contains_key(database) {
            return Err(format!(
                "user '{}' has no role override for database '{}'",
                username, database
            ));
        }
        record.db_roles.remove(database);
        self.save()
    }

    /// Get the effective role for a user on a specific database.
    /// Returns the database-specific role if set, otherwise the global role.
    pub fn effective_role(&self, username: &str, database: &str) -> Option<Role> {
        let record = self.users.get(username)?;
        Some(
            record
                .db_roles
                .get(database)
                .copied()
                .unwrap_or(record.role),
        )
    }

    fn create_user_internal(
        &mut self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<(), String> {
        let password_hash = hash_password(password)?;
        let verifier = derive_scram_verifier(password);
        let record = UserRecord {
            username: username.to_string(),
            password_hash,
            role,
            db_roles: HashMap::new(),
            scram_salt: Some(verifier.salt_b64),
            scram_iter_count: Some(verifier.iter_count),
            scram_stored_key: Some(verifier.stored_key_b64),
            scram_server_key: Some(verifier.server_key_b64),
        };
        self.users.insert(username.to_string(), record);
        self.save()
    }

    fn save(&self) -> Result<(), String> {
        let records: Vec<&UserRecord> = self.users.values().collect();
        let data = serde_json::to_string_pretty(&records)
            .map_err(|e| format!("failed to serialize users: {e}"))?;
        fs::write(&self.store_path, data)
            .map_err(|e| format!("failed to write users.json: {e}"))?;
        Ok(())
    }
}

fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| format!("password hashing failed: {e}"))?;
    Ok(hash.to_string())
}

/// ScramVerifier is the four pieces a SCRAM-SHA-256 server needs to
/// finish an authentication exchange against a given user without
/// knowing their plaintext password: a fixed salt + iter count, and
/// the stored_key + server_key derived per RFC 7677 §3.
pub struct ScramVerifier {
    pub salt_b64: String,
    pub iter_count: u32,
    pub stored_key_b64: String,
    pub server_key_b64: String,
}

/// derive_scram_verifier follows RFC 7677 §3 to turn a plaintext
/// password into a SCRAM verifier:
///
///   1. salt = random 16 bytes
///   2. salted_password = PBKDF2-HMAC-SHA-256(password, salt, iters)
///   3. client_key = HMAC-SHA-256(salted_password, "Client Key")
///   4. stored_key = SHA-256(client_key)
///   5. server_key = HMAC-SHA-256(salted_password, "Server Key")
///
/// The salt + iter_count are stored alongside stored_key and
/// server_key on the user record; the plaintext is never persisted.
/// At auth time the server sends salt+iters to the client, the
/// client derives client_key from its plaintext and provides a
/// proof, and the server checks the proof against stored_key.
pub fn derive_scram_verifier(password: &str) -> ScramVerifier {
    use crate::scram::{
        base64_encode_simple_pub as base64_encode, generate_salt_pub as generate_salt,
        hmac_sha256_pub as hmac_sha256, pbkdf2_sha256_pub as pbkdf2_sha256,
        sha256_hash_pub as sha256_hash,
    };

    let salt = generate_salt();
    let salted_password = pbkdf2_sha256(password.as_bytes(), &salt, SCRAM_ITER_COUNT);
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = sha256_hash(&client_key);
    let server_key = hmac_sha256(&salted_password, b"Server Key");

    ScramVerifier {
        salt_b64: base64_encode(&salt),
        iter_count: SCRAM_ITER_COUNT,
        stored_key_b64: base64_encode(&stored_key),
        server_key_b64: base64_encode(&server_key),
    }
}

fn generate_random_password() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 24];
    rand::rng().fill_bytes(&mut bytes);
    base64_encode(&bytes)
}

fn base64_encode(data: &[u8]) -> String {
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
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[(n & 0x3F) as usize] as char);
        }
    }
    result
}
