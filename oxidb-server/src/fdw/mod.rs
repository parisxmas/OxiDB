//! Foreign-Data-Wrapper adapter framework. The handler used to call
//! `remote_client::proxy_command` directly for every linked
//! collection — fine when the only remote was another OxiDB. This
//! module generalises that: each adapter implements the `Adapter`
//! trait, and a single URL-scheme dispatcher picks the right impl
//! for a given link.
//!
//! Schemes wired in (v3a):
//!   - `oxidb://[user:pw@]host:port/<collection>` — peer OxiDB
//!   - `file:///path/to/file.csv`                  — local CSV file
//!   - `csv:///path/to/file.csv`                   — same, explicit
//!
//! Future PRs (#71) will plug in Postgres + REST without touching
//! the handler.

use serde_json::Value;

pub mod csv_adapter;
pub mod oxidb_adapter;

/// An FDW adapter knows how to translate the local server's CRUD
/// commands into whatever shape the remote understands (another
/// OxiDB's wire protocol, a CSV file's raw rows, Postgres rows, REST
/// resources, …) and to translate the result back into the standard
/// `{"ok": true, "data": <something>}` / `{"ok": false, "error": ...}`
/// envelope the local handler forwards verbatim.
///
/// The collection-name rewrite (local link alias → whatever the
/// remote calls "the thing this URL points at") happens INSIDE the
/// adapter — the dispatcher just hands it the original request as
/// the user sent it. That way an OxiDB adapter can keep its existing
/// JSON-rewrite path, and a CSV adapter (which has no concept of a
/// "remote collection name" at all) doesn't have to care.
pub trait Adapter: Send + Sync {
    /// Execute one CRUD command. Errors are returned as `Err(String)`
    /// — the handler wraps those in the standard error envelope.
    fn execute(&self, cmd: &str, request: &Value) -> Result<Value, String>;
}

/// adapter_for inspects the URL scheme and returns the matching
/// adapter. Adapter construction may itself fail (bad URL, missing
/// file, …) — those errors propagate to the caller, which turns them
/// into an error response on the wire.
///
/// Adapters are cheap-to-construct (essentially URL parsing + maybe
/// path canonicalisation); we create a fresh one per request rather
/// than caching, so a link-URL update via `unlink_collection` +
/// `link_collection` takes effect immediately.
pub fn adapter_for(url: &str) -> Result<Box<dyn Adapter>, String> {
    if url.starts_with("oxidb://") {
        Ok(Box::new(oxidb_adapter::OxiDbAdapter::from_url(url)?))
    } else if url.starts_with("csv://") {
        Ok(Box::new(csv_adapter::CsvAdapter::from_url(
            url.trim_start_matches("csv://"),
        )?))
    } else if let Some(path) = url.strip_prefix("file://") {
        // Only file URLs whose path ends in .csv hit the CSV adapter.
        // Other file types will land here in future PRs (json, ndjson,
        // parquet, …); for now anything else is an explicit error so
        // a typo doesn't silently get the wrong adapter.
        if path.ends_with(".csv") {
            Ok(Box::new(csv_adapter::CsvAdapter::from_url(path)?))
        } else {
            Err(format!(
                "no FDW adapter for file URL: {} \
                — only .csv files are supported in v3a",
                url
            ))
        }
    } else {
        Err(format!(
            "no FDW adapter for URL scheme: {} \
            — supported: oxidb://, file://*.csv, csv://",
            url
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Box<dyn Adapter> isn't Debug, so we hand-roll the err extraction
    // instead of unwrap_err(): an Ok case here is itself a test failure.
    fn expect_err(url: &str) -> String {
        match adapter_for(url) {
            Ok(_) => panic!("adapter_for({:?}) must reject this URL", url),
            Err(e) => e,
        }
    }

    #[test]
    fn unknown_scheme_is_rejected_with_actionable_message() {
        let err = expect_err("ftp://example.com/data");
        assert!(err.contains("no FDW adapter"));
        assert!(err.contains("supported"));
    }

    #[test]
    fn non_csv_file_scheme_is_rejected() {
        // A file:// URL that doesn't end in .csv must NOT silently
        // get routed to the CSV adapter — that would be the wrong
        // parser and would corrupt the response.
        let err = expect_err("file:///tmp/data.parquet");
        assert!(err.contains(".csv"));
    }
}
