//! OxiDB-to-OxiDB adapter — the original FDW behavior, now wearing
//! the Adapter trait so the handler can treat it identically to the
//! CSV / Postgres / REST adapters.
//!
//! All the heavy lifting (connection pool, SCRAM auth, wire format)
//! lives in `crate::remote_client`; this file is just the trait shim
//! plus the local-collection→remote-collection rewrite.

use serde_json::Value;

use oxidb::links::ParsedRemote;

use crate::fdw::Adapter;
use crate::remote_client;

/// OxiDbAdapter forwards CRUD commands to a peer OxiDB instance over
/// the wire protocol. The remote endpoint + credentials + remote
/// collection name are baked in at construction time from the link
/// URL (`oxidb://[user:pw@]host:port/<collection>`).
pub struct OxiDbAdapter {
    remote: ParsedRemote,
}

impl OxiDbAdapter {
    /// Parse the link URL once at construction. Re-validating on
    /// every call would let a torn `_links.json` write surface as a
    /// proxy error on the FIRST mutating call after restart — which
    /// is fine, but doing it here means an obviously-broken link
    /// fails fast at `link_collection` time rather than at the first
    /// query.
    pub fn from_url(url: &str) -> Result<Self, String> {
        let remote = remote_client::parse_remote(url)
            .map_err(|e| format!("parse oxidb link URL: {}", e))?;
        Ok(Self { remote })
    }
}

impl Adapter for OxiDbAdapter {
    fn execute(&self, _cmd: &str, request: &Value) -> Result<Value, String> {
        // Rewrite the collection field from the LOCAL link alias to
        // the REMOTE collection name. The handler hands us the
        // original request as the user sent it, so the alias lives
        // in there now; the remote OxiDB has no idea what alias the
        // local server gave it, only what its own collection is
        // called.
        let mut rewritten = request.clone();
        if let Some(obj) = rewritten.as_object_mut() {
            obj.insert(
                "collection".to_string(),
                Value::String(self.remote.remote_collection.clone()),
            );
        }
        remote_client::proxy_command(&self.remote, &rewritten)
    }
}
