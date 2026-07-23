//! The control plane's client to the data plane (`oxidb-server`). OxiBase is,
//! architecturally, just an admin client of OxiDB (ADR-0021): it provisions
//! tenant databases with `CREATE DATABASE` and stores its own state (accounts,
//! projects) in a normal `oxibase` database over the PostgREST surface. All
//! calls go over `oxidb-http::client`.

use oxidb_http::client;
use serde_json::{Value, json};

use crate::crypto::{Claims, encode_jwt};

/// The metadata database OxiBase keeps its accounts + projects in.
pub const META_DB: &str = "oxibase";

pub struct Upstream {
    base: String,       // e.g. http://127.0.0.1:14580
    jwt_secret: String, // shared OXIDB_JWT_SECRET — signs the admin token
}

impl Upstream {
    pub fn new(base: String, jwt_secret: String) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            jwt_secret,
        }
    }

    /// A short-lived admin JWT for the data plane (role=admin, signed with the
    /// shared secret).
    fn admin_token(&self) -> String {
        let now = crate::now_secs();
        encode_jwt(
            &Claims {
                sub: "oxibase".into(),
                role: "admin".into(),
                iat: now,
                exp: now + 300,
            },
            &self.jwt_secret,
        )
    }

    /// Ensure the metadata database exists (idempotent).
    pub fn ensure_meta_db(&self) -> Result<(), String> {
        self.create_database(META_DB).or_else(|e| {
            if e.contains("already exists") {
                Ok(())
            } else {
                Err(e)
            }
        })
    }

    /// `CREATE DATABASE <name>` via the SQL admin surface (works without the SQL
    /// engine — database DDL is handled by the db-admin path).
    pub fn create_database(&self, name: &str) -> Result<(), String> {
        let url = format!("{}/api/sql", self.base);
        let body = json!({ "sql": format!("CREATE DATABASE {name}") });
        let resp = client::post_json(&url, Some(&self.admin_token()), body.to_string().as_bytes())
            .map_err(|e| e.to_string())?;
        if resp.is_success() {
            Ok(())
        } else {
            Err(error_message(&resp))
        }
    }

    pub fn drop_database(&self, name: &str) -> Result<(), String> {
        let url = format!("{}/api/sql", self.base);
        let body = json!({ "sql": format!("DROP DATABASE {name}") });
        let resp = client::post_json(&url, Some(&self.admin_token()), body.to_string().as_bytes())
            .map_err(|e| e.to_string())?;
        if resp.is_success() {
            Ok(())
        } else {
            Err(error_message(&resp))
        }
    }

    /// Insert a document into `META_DB.{col}`; returns the created row.
    pub fn insert(&self, col: &str, doc: &Value) -> Result<Value, String> {
        let url = format!("{}/rest/v1/{col}?db={META_DB}", self.base);
        let resp = client::request(
            "POST",
            &url,
            &[
                ("Content-Type", "application/json"),
                ("Authorization", &format!("Bearer {}", self.admin_token())),
                ("Prefer", "return=representation"),
            ],
            doc.to_string().as_bytes(),
        )
        .map_err(|e| e.to_string())?;
        if !resp.is_success() {
            return Err(error_message(&resp));
        }
        let arr: Value = serde_json::from_slice(&resp.body).map_err(|e| e.to_string())?;
        Ok(arr
            .as_array()
            .and_then(|a| a.first())
            .cloned()
            .unwrap_or(Value::Null))
    }

    /// Documents in `META_DB.{col}` matching a raw PostgREST filter query
    /// (e.g. `email=eq.a%40b.com`).
    pub fn find(&self, col: &str, filter: &str) -> Result<Vec<Value>, String> {
        let sep = if filter.is_empty() { "" } else { "&" };
        let url = format!("{}/rest/v1/{col}?db={META_DB}{sep}{filter}", self.base);
        let resp = client::get(&url, Some(&self.admin_token())).map_err(|e| e.to_string())?;
        if !resp.is_success() {
            return Err(error_message(&resp));
        }
        let arr: Value = serde_json::from_slice(&resp.body).map_err(|e| e.to_string())?;
        Ok(arr.as_array().cloned().unwrap_or_default())
    }

    pub fn count(&self, col: &str, filter: &str) -> Result<usize, String> {
        Ok(self.find(col, filter)?.len())
    }

    /// Patch documents in `META_DB.{col}` matching `filter`.
    pub fn update(&self, col: &str, filter: &str, patch: &Value) -> Result<(), String> {
        let sep = if filter.is_empty() { "" } else { "&" };
        let url = format!("{}/rest/v1/{col}?db={META_DB}{sep}{filter}", self.base);
        let resp = client::request(
            "PATCH",
            &url,
            &[
                ("Content-Type", "application/json"),
                ("Authorization", &format!("Bearer {}", self.admin_token())),
            ],
            patch.to_string().as_bytes(),
        )
        .map_err(|e| e.to_string())?;
        if resp.is_success() {
            Ok(())
        } else {
            Err(error_message(&resp))
        }
    }

    pub fn delete(&self, col: &str, filter: &str) -> Result<(), String> {
        let sep = if filter.is_empty() { "" } else { "&" };
        let url = format!("{}/rest/v1/{col}?db={META_DB}{sep}{filter}", self.base);
        let resp = client::request(
            "DELETE",
            &url,
            &[("Authorization", &format!("Bearer {}", self.admin_token()))],
            &[],
        )
        .map_err(|e| e.to_string())?;
        if resp.is_success() {
            Ok(())
        } else {
            Err(error_message(&resp))
        }
    }
}

fn error_message(resp: &client::Response) -> String {
    serde_json::from_slice::<Value>(&resp.body)
        .ok()
        .and_then(|v| {
            v.get("message")
                .or_else(|| v.get("error"))
                .and_then(|m| m.as_str().map(String::from))
        })
        .unwrap_or_else(|| format!("upstream error (status {})", resp.status))
}

/// Percent-encode a value for a PostgREST filter (`@`, `.`, spaces, etc.).
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
