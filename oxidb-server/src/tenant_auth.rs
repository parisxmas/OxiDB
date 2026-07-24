//! The data plane's only piece of OxiBase (ADR-0021): resolving a tenant
//! project's **public** verification key so the REST listener can verify a
//! `?db=<ref>` token.
//!
//! The control plane (accounts, provisioning, key rotation, the dashboard) is a
//! separate `oxibase` binary; it writes project rows — the ES256 public key in
//! the clear, the private key sealed — into a normal `oxibase` metadata database
//! over the wire. This hook reads that database **locally** on the authenticated
//! hot path and returns the public key. It holds **no secret and no seal key**:
//! verification is asymmetric, which is what lets project tokens be verified by
//! any number of data-plane nodes (the multi-node property).

use base64::Engine;
use oxidb::DatabaseManager;
use serde_json::json;

/// The metadata database OxiBase records projects in.
const META_DB: &str = "oxibase";
/// The collection of project rows within [`META_DB`].
const PROJECTS: &str = "projects";

/// `true` when this data plane participates in OxiBase (`OXIDB_PLATFORM=1`), so
/// the REST listener consults [`project_pubkey`] for `?db=<ref>` requests.
pub fn enabled() -> bool {
    matches!(
        std::env::var("OXIDB_PLATFORM")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether `db` names a reserved control-plane store that must never be served
/// over the data-plane REST surface. The `oxibase` metadata database holds
/// developer accounts (password hashes) and projects (sealed private keys); the
/// control plane reaches it in-process over the wire, never via `?db=`. Only
/// reserved when the platform is active — a plain OxiDB deployment may legitimately
/// have a user database of that name. (`_`-prefixed global stores like `_auth`
/// are already unreachable via `get_database`.)
pub fn is_reserved_db(db: &str) -> bool {
    enabled() && db == META_DB
}

/// Resolve a path-tenant segment (a project **ref or slug**) to the database
/// name to target. With the platform off, the segment is taken as a database
/// name directly (plain OxiDB multi-db). With it on, the segment is looked up in
/// the `oxibase.projects` metadata by ref or slug and the project's ref (its
/// database) is returned; an unknown segment yields `None` (→ 404).
pub fn resolve_tenant(mgr: &DatabaseManager, segment: &str) -> Option<String> {
    if !enabled() {
        return Some(segment.to_string());
    }
    if segment == META_DB {
        return None;
    }
    let pdb = mgr.get_database(META_DB).ok()?;
    let doc = pdb
        .find_one(
            PROJECTS,
            &json!({ "$or": [{ "ref": segment }, { "slug": segment }] }),
        )
        .ok()??;
    doc.get("ref").and_then(|v| v.as_str()).map(String::from)
}

/// Default per-project resource caps, applied when a project row does not carry
/// an explicit value (e.g. rows created before quotas existed). The control
/// plane owns the real numbers per plan; these are only the floor the data plane
/// falls back to. Overridable via `OXIDB_PROJECT_DEFAULT_MAX_COLLECTIONS` /
/// `OXIDB_PROJECT_DEFAULT_MAX_TABLES` (0 = unlimited).
fn default_limit(env_key: &str, fallback: usize) -> usize {
    std::env::var(env_key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}

/// Per-project resource caps for `db_ref`. The control plane writes them onto
/// each project row (plan-based); the data plane reads them here and enforces
/// them — the only place it can, since collections/tables/documents are created
/// straight against the data plane and never pass through the control plane.
pub struct ProjectLimits {
    pub max_collections: usize,
    pub max_tables: usize,
    pub max_documents: usize,
    /// Requests per minute this project may make of the REST surface.
    /// `0` = unlimited, which is the default: an existing deployment must opt
    /// into throttling rather than discover it under load.
    pub max_rpm: u32,
}

/// Read the caps for `db_ref`. `None` for the metadata db, an unknown ref, or
/// when the platform is off (no quotas at all).
pub fn project_limits(mgr: &DatabaseManager, db_ref: &str) -> Option<ProjectLimits> {
    if !enabled() || db_ref == META_DB {
        return None;
    }
    let pdb = mgr.get_database(META_DB).ok()?;
    let doc = pdb.find_one(PROJECTS, &json!({ "ref": db_ref })).ok()??;
    let field = |key: &str, default_env: &str, fallback: usize| {
        doc.get(key)
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or_else(|| default_limit(default_env, fallback))
    };
    Some(ProjectLimits {
        max_collections: field(
            "max_collections",
            "OXIDB_PROJECT_DEFAULT_MAX_COLLECTIONS",
            5,
        ),
        max_tables: field("max_tables", "OXIDB_PROJECT_DEFAULT_MAX_TABLES", 5),
        max_documents: field(
            "max_documents",
            "OXIDB_PROJECT_DEFAULT_MAX_DOCUMENTS",
            10_000,
        ),
        max_rpm: field("max_requests_per_min", "OXIDB_PROJECT_DEFAULT_MAX_RPM", 0) as u32,
    })
}

// ---------------------------------------------------------------------------
// Per-project request rate limiting
//
// The size quotas above bound what a tenant *stores*; this bounds what it
// *costs everyone else*, which is the noisy-neighbour risk of running many
// tenants in one process. A fixed one-minute window per project: cheap (one
// lock, one integer) and easy to reason about at a glance, unlike a rolling
// window whose refusals depend on history the operator cannot see.
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Above this many tracked projects, expired windows are swept — so a
/// deployment that has seen a million refs does not keep a million counters.
const SWEEP_ABOVE: usize = 4_096;
const WINDOW_SECS: u64 = 60;

fn counters() -> &'static Mutex<HashMap<String, (u32, u64)>> {
    static C: OnceLock<Mutex<HashMap<String, (u32, u64)>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Count one request against `db_ref`'s window. `Some(retry_after_secs)` when
/// the request must be refused (and it is *not* counted — a client hammering a
/// closed window cannot extend it), `None` when it may proceed.
pub fn rate_limit_hit(db_ref: &str, limit: u32) -> Option<u64> {
    if limit == 0 {
        return None;
    }
    let mut map = counters().lock().ok()?;
    let now = now_secs();
    if map.len() > SWEEP_ABOVE {
        map.retain(|_, (_, start)| now.saturating_sub(*start) < WINDOW_SECS);
    }
    check_window(&mut map, db_ref, limit, now)
}

/// The window arithmetic, split out so it can be tested against a synthetic
/// clock rather than by sleeping.
fn check_window(
    map: &mut HashMap<String, (u32, u64)>,
    key: &str,
    limit: u32,
    now: u64,
) -> Option<u64> {
    let entry = map.entry(key.to_string()).or_insert((0, now));
    if now.saturating_sub(entry.1) >= WINDOW_SECS {
        *entry = (0, now);
    }
    if entry.0 >= limit {
        // At least a second, so a client never reads "retry after 0".
        return Some((WINDOW_SECS - now.saturating_sub(entry.1)).max(1));
    }
    entry.0 += 1;
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_allows_up_to_the_limit_then_refuses() {
        let mut m = HashMap::new();
        for i in 0..3 {
            assert!(
                check_window(&mut m, "p", 3, 1_000).is_none(),
                "request {i} should pass"
            );
        }
        let retry = check_window(&mut m, "p", 3, 1_000).expect("fourth is refused");
        assert_eq!(retry, 60, "a full window remains");
    }

    #[test]
    fn the_window_resets_and_refusals_do_not_extend_it() {
        let mut m = HashMap::new();
        assert!(check_window(&mut m, "p", 1, 1_000).is_none());
        // Refused 30s in: the caller is told to wait out the remaining 30s, and
        // the refusal itself must not push that deadline further away.
        assert_eq!(check_window(&mut m, "p", 1, 1_030), Some(30));
        assert_eq!(check_window(&mut m, "p", 1, 1_050), Some(10));
        // Once the window rolls over, the budget is fresh.
        assert!(check_window(&mut m, "p", 1, 1_060).is_none());
    }

    #[test]
    fn projects_are_counted_independently() {
        let mut m = HashMap::new();
        assert!(check_window(&mut m, "a", 1, 0).is_none());
        assert!(check_window(&mut m, "a", 1, 0).is_some());
        assert!(
            check_window(&mut m, "b", 1, 0).is_none(),
            "one tenant's flood must not spend another's budget"
        );
    }

    #[test]
    fn zero_means_unlimited() {
        for _ in 0..1_000 {
            assert!(rate_limit_hit("unlimited-project", 0).is_none());
        }
    }
}

/// Per-project storage cap in **bytes** for `db_ref` (the blob-store quota).
/// Read off the project row like [`project_limits`]; falls back to
/// `OXIDB_PROJECT_DEFAULT_MAX_STORAGE_BYTES` (default 100 MiB). `0` =
/// unlimited. `None` when the platform is off or `db_ref` is not a project.
pub fn project_storage_limit(mgr: &DatabaseManager, db_ref: &str) -> Option<u64> {
    if !enabled() || db_ref == META_DB {
        return None;
    }
    let pdb = mgr.get_database(META_DB).ok()?;
    let doc = pdb.find_one(PROJECTS, &json!({ "ref": db_ref })).ok()??;
    Some(
        doc.get("max_storage_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| {
                default_limit("OXIDB_PROJECT_DEFAULT_MAX_STORAGE_BYTES", 104_857_600) as u64
            }),
    )
}

/// The per-project ES256 **public** key (SEC1 uncompressed, 65 bytes) for
/// `db_ref`, if it names an OxiBase project. Read in the clear — no seal key,
/// no secret — so a data-plane node verifies project tokens without holding any
/// signing material. `None` for the metadata db or an unknown ref.
pub fn project_pubkey(mgr: &DatabaseManager, db_ref: &str) -> Option<Vec<u8>> {
    if db_ref == META_DB {
        return None;
    }
    let pdb = mgr.get_database(META_DB).ok()?;
    let doc = pdb.find_one(PROJECTS, &json!({ "ref": db_ref })).ok()??;
    let pub_b64 = doc.get("pubkey")?.as_str()?;
    base64::engine::general_purpose::STANDARD
        .decode(pub_b64)
        .ok()
}
