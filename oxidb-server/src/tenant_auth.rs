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
