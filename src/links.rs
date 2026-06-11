//! Linked collections — the FDW-style "this collection lives on another
//! OxiDB instance" mechanism. A LOCAL collection name registered as a
//! link transparently proxies read commands (`find`, `find_one`,
//! `count`, `aggregate`) to a REMOTE OxiDB. Writes to a linked
//! collection are refused — the link is read-only in this version.
//!
//! Storage: a single JSON file at `{data_dir}/_links.json`. Reads are
//! cached in memory behind an `RwLock`; writes go through both the
//! cache and the file (best-effort fsync via `File::sync_all`).
//!
//! Wire format: the remote is reached via OxiDB's regular TCP
//! protocol (length-prefixed JSON). The proxying is implemented in
//! `oxidb-server::remote_client` — this module owns the *config*, not
//! the actual RPC.
//!
//! Auth: v1 does not authenticate against the remote. The URL grammar
//! reserves `user:password@host:port` for v2; in v1 the user/password
//! fields are accepted-and-ignored so deployments can start writing
//! URLs in the final shape without a config migration later.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::locks::RwLock;

/// LinkConfig is one row of the links table — the local name, the
/// remote endpoint, and a creation timestamp. Stored as a plain JSON
/// object on disk, one entry per linked collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkConfig {
    /// Local collection name the proxy intercepts.
    pub name: String,
    /// Remote endpoint. Grammar:
    ///   oxidb://[user:password@]host:port/<remote_collection>
    /// The path after the host MUST be the remote collection name —
    /// no leading slash, no extra path segments. (Multi-DB
    /// addressing is future work.)
    pub url: String,
    /// Wall-clock time the link was created, as an RFC 3339 string.
    pub created_at: String,
}

/// LinksTable is the in-memory + on-disk registry of linked
/// collections. Cheap to read (RwLock cached map); writes are
/// serialised through the same lock and persisted before returning.
#[derive(Debug)]
pub struct LinksTable {
    inner: RwLock<HashMap<String, LinkConfig>>,
    #[cfg(not(target_arch = "wasm32"))]
    path: Option<PathBuf>,
}

impl LinksTable {
    /// Open the on-disk links file (if present) and return the
    /// populated table. A missing file is fine — the table starts
    /// empty and will create the file on the first link write.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(data_dir: &Path) -> Result<Arc<Self>> {
        let path = data_dir.join("_links.json");
        let inner = match std::fs::read(&path) {
            Ok(bytes) => {
                let list: Vec<LinkConfig> = serde_json::from_slice(&bytes).map_err(|e| {
                    Error::Io(std::io::Error::other(format!(
                        "parse {}: {}",
                        path.display(),
                        e
                    )))
                })?;
                let mut map = HashMap::with_capacity(list.len());
                for cfg in list {
                    map.insert(cfg.name.clone(), cfg);
                }
                map
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(err) => return Err(Error::Io(err)),
        };
        Ok(Arc::new(Self {
            inner: RwLock::new(inner),
            path: Some(path),
        }))
    }

    /// In-memory only — for tests and the wasm build.
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(HashMap::new()),
            #[cfg(not(target_arch = "wasm32"))]
            path: None,
        })
    }

    /// Look up the link config for a local collection name. Returns
    /// `None` if the name isn't linked. Cheap — only takes a read
    /// lock; the hot path (every find/count/aggregate) goes through
    /// this.
    pub fn get(&self, name: &str) -> Option<LinkConfig> {
        self.inner.read().get(name).cloned()
    }

    /// Register or replace a link. The on-disk file is rewritten and
    /// fsync'd before this returns, so a crash immediately after
    /// `link_collection` won't lose the registration.
    pub fn insert(&self, cfg: LinkConfig) -> Result<()> {
        validate_url(&cfg.url)?;
        {
            let mut w = self.inner.write();
            w.insert(cfg.name.clone(), cfg);
        }
        self.persist()
    }

    /// Drop a link. Returns `true` if it existed.
    pub fn remove(&self, name: &str) -> Result<bool> {
        let existed = {
            let mut w = self.inner.write();
            w.remove(name).is_some()
        };
        if existed {
            self.persist()?;
        }
        Ok(existed)
    }

    /// Snapshot of every link, ordered by name for deterministic
    /// output (callers feed this straight into JSON for `list_links`).
    pub fn list(&self) -> Vec<LinkConfig> {
        let r = self.inner.read();
        let mut out: Vec<LinkConfig> = r.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Write the current table to disk. Atomic via write-to-tmp +
    /// rename — never leaves a half-written file the parser would
    /// reject on startup.
    #[cfg(not(target_arch = "wasm32"))]
    fn persist(&self) -> Result<()> {
        let path = match &self.path {
            Some(p) => p,
            None => return Ok(()), // in-memory mode (tests)
        };
        let list = self.list();
        let bytes = serde_json::to_vec_pretty(&list)
            .map_err(|e| Error::Io(std::io::Error::other(format!("serialize links: {}", e))))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &bytes)?;
        // best-effort fsync — losing this on a crash is recoverable
        // (operator re-runs `link_collection`) but we still try.
        if let Ok(f) = std::fs::File::open(&tmp) {
            let _ = f.sync_all();
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    fn persist(&self) -> Result<()> {
        Ok(())
    }
}

/// validate_url rejects obviously bad link URLs at registration time
/// — better to fail loudly when the operator wires the link than
/// silently on every subsequent query.
///
/// The engine crate doesn't know which schemes the server's FDW
/// dispatcher actually supports (it might be just `oxidb://`, or it
/// might include `csv://`, `file://*.csv`, `postgres://`, …). Rather
/// than duplicate the scheme list here — and silently desync it from
/// `oxidb-server::fdw::adapter_for` — this only does the shape check
/// that's universally true: there must be a `<scheme>://` prefix.
/// Per-scheme grammar validation happens at adapter-construction
/// time, which surfaces a clear error on the first query against a
/// misconfigured link.
///
/// `oxidb://` URLs additionally go through `parse_remote` so the
/// legacy strict validation (must include /<collection>, port must
/// parse, …) still fires at link-time for the original adapter.
pub fn validate_url(url: &str) -> Result<()> {
    if url.is_empty() {
        return Err(Error::InvalidQuery("link URL is empty".to_string()));
    }
    if !url.contains("://") {
        return Err(Error::InvalidQuery(format!(
            "link URL must include a scheme (e.g. oxidb://, csv://, file://) — got {:?}",
            url
        )));
    }
    if url.starts_with("oxidb://") {
        let _ = parse_remote(url)?;
    }
    Ok(())
}

/// ParsedRemote breaks a link URL into its consumable parts. Public
/// because the proxy client (in oxidb-server) reuses this — keeping
/// the grammar in one place means a server upgrade can't drift the
/// URL shape from the config it persists.
#[derive(Debug, Clone)]
pub struct ParsedRemote {
    pub host: String,
    pub port: u16,
    pub remote_collection: String,
    /// Reserved for v2 auth. v1 ignores both.
    pub user: Option<String>,
    pub password: Option<String>,
}

/// parse_remote applies the URL grammar:
///   oxidb://[user:password@]host:port/<remote_collection>
///
/// Returns a typed view or a clear error string the handler can
/// surface as `{"ok": false, "error": ...}`.
pub fn parse_remote(url: &str) -> Result<ParsedRemote> {
    let rest = url.strip_prefix("oxidb://").ok_or_else(|| {
        Error::InvalidQuery(format!("link URL must start with oxidb:// — got {:?}", url))
    })?;

    // Split off the userinfo (if any).
    let (userinfo, host_path) = match rest.find('@') {
        Some(at) => (Some(&rest[..at]), &rest[at + 1..]),
        None => (None, rest),
    };
    let (user, password) = match userinfo {
        None => (None, None),
        Some(ui) => match ui.find(':') {
            Some(c) => (Some(ui[..c].to_string()), Some(ui[c + 1..].to_string())),
            None => (Some(ui.to_string()), None),
        },
    };

    // host:port / collection
    let (hostport, coll) = host_path.split_once('/').ok_or_else(|| {
        Error::InvalidQuery(format!(
            "link URL must include /<remote_collection> after host:port — got {:?}",
            url
        ))
    })?;
    if coll.is_empty() || coll.contains('/') {
        return Err(Error::InvalidQuery(format!(
            "link URL collection path must be a single segment — got {:?}",
            coll
        )));
    }

    let (host, port_str) = hostport.rsplit_once(':').ok_or_else(|| {
        Error::InvalidQuery(format!("link URL must include :port — got {:?}", hostport))
    })?;
    let port: u16 = port_str.parse().map_err(|_| {
        Error::InvalidQuery(format!(
            "link URL port must be a number — got {:?}",
            port_str
        ))
    })?;
    if host.is_empty() {
        return Err(Error::InvalidQuery("link URL host is empty".to_string()));
    }

    Ok(ParsedRemote {
        host: host.to_string(),
        port,
        remote_collection: coll.to_string(),
        user,
        password,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remote_minimal() {
        let r = parse_remote("oxidb://central.example.com:4444/users").unwrap();
        assert_eq!(r.host, "central.example.com");
        assert_eq!(r.port, 4444);
        assert_eq!(r.remote_collection, "users");
        assert!(r.user.is_none() && r.password.is_none());
    }

    #[test]
    fn parse_remote_with_userinfo() {
        let r = parse_remote("oxidb://alice:s3cret@db.example:5555/orders").unwrap();
        assert_eq!(r.user.as_deref(), Some("alice"));
        assert_eq!(r.password.as_deref(), Some("s3cret"));
        assert_eq!(r.remote_collection, "orders");
    }

    #[test]
    fn parse_remote_user_only_no_password() {
        let r = parse_remote("oxidb://token@db:4444/c").unwrap();
        assert_eq!(r.user.as_deref(), Some("token"));
        assert!(r.password.is_none());
    }

    #[test]
    fn parse_remote_rejects_missing_scheme() {
        assert!(parse_remote("central:4444/users").is_err());
    }

    #[test]
    fn parse_remote_rejects_missing_collection() {
        assert!(parse_remote("oxidb://central:4444").is_err());
        assert!(parse_remote("oxidb://central:4444/").is_err());
    }

    #[test]
    fn parse_remote_rejects_nested_path() {
        assert!(parse_remote("oxidb://central:4444/db/users").is_err());
    }

    #[test]
    fn parse_remote_rejects_non_numeric_port() {
        assert!(parse_remote("oxidb://central:abcd/users").is_err());
    }

    #[test]
    fn in_memory_insert_get_remove_roundtrip() {
        let t = LinksTable::in_memory();
        t.insert(LinkConfig {
            name: "remote_users".into(),
            url: "oxidb://central:4444/users".into(),
            created_at: "2026-05-17T00:00:00Z".into(),
        })
        .unwrap();
        let got = t.get("remote_users").unwrap();
        assert_eq!(got.url, "oxidb://central:4444/users");
        assert_eq!(t.list().len(), 1);
        assert!(t.remove("remote_users").unwrap());
        assert!(t.get("remote_users").is_none());
        assert_eq!(t.list().len(), 0);
    }

    #[test]
    fn insert_rejects_bad_url() {
        let t = LinksTable::in_memory();
        let err = t.insert(LinkConfig {
            name: "bad".into(),
            url: "not-a-url".into(),
            created_at: "x".into(),
        });
        assert!(err.is_err());
        assert!(t.get("bad").is_none());
    }
}
