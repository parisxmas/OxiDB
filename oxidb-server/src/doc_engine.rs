//! The document-engine switch (`OXIDB_DOC`).
//!
//! The document engine is the server's default and is **on unless turned off**
//! — the inverse of `OXIDB_SQL` / `OXIDB_TSDB`, which are off unless turned on.
//! Setting `OXIDB_DOC=0` runs the process as a SQL/TSDB server only: no
//! document data directory is created, no per-database TTL eviction or alert
//! threads run, and every document command is refused by name rather than
//! served from a store that will not persist.
//!
//! Refusing loudly is the point. The alternative — quietly accepting document
//! writes into an engine the operator asked not to have — is the failure mode
//! this codebase avoids everywhere else (see the `pg` catalog handling, which
//! refuses unknown catalog queries rather than answering them empty).

use std::sync::LazyLock;

/// `false` only when `OXIDB_DOC` is explicitly one of `0`/`false`/`no`/`off`.
/// Anything else — including unset — leaves the document engine on, so no
/// existing deployment changes behaviour.
static ENABLED: LazyLock<bool> = LazyLock::new(|| {
    !matches!(
        std::env::var("OXIDB_DOC")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
});

pub fn enabled() -> bool {
    *ENABLED
}

/// The one wording every surface uses when it turns a document request away,
/// so an operator sees the same sentence wherever they hit it.
pub const REFUSAL: &str = "the document engine is disabled (OXIDB_DOC=0); this server serves the SQL \
     and time-series engines only — unset OXIDB_DOC to enable documents";

#[cfg(test)]
mod tests {
    /// The default must be *on*: an unset or unrecognised value keeps the
    /// document engine, because every deployment predating this flag has it
    /// unset and none of them asked for a SQL-only server.
    #[test]
    fn only_explicit_off_values_disable_the_engine() {
        fn is_off(v: &str) -> bool {
            matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        }
        for on in ["", "1", "true", "yes", "on", "maybe", "2"] {
            assert!(!is_off(on), "{on:?} must not disable the document engine");
        }
        for off in ["0", "false", "no", "off", "OFF", "False"] {
            assert!(is_off(off), "{off:?} must disable the document engine");
        }
    }
}
