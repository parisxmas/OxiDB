//! CERN-grade upgrade-chain test (category 6 in
//! `docs/testing-roadmap.md`).
//!
//! Reads every `tests/fixtures/upgrade/v*.tar.gz` fixture with the
//! CURRENT engine binary and asserts a known set of invariants. This
//! is the "N→N+1 backward read" test — any committed fixture in that
//! directory MUST open cleanly with the current engine.
//!
//! Two test fns:
//!
//!   read_all_committed_fixtures
//!       The actual test. Iterates every `.tar.gz` under
//!       `tests/fixtures/upgrade/`, restores it to a temp dir,
//!       opens with the current engine, asserts the known shape.
//!       Run as part of the `--ignored` slice.
//!
//!   generate_fixture_for_current_version
//!       Helper that BUILDS a fresh fixture for the current engine.
//!       Bootstraps the fixture when a new version ships. Only run
//!       when explicitly invoked by name. See
//!       `tests/fixtures/upgrade/README.md` for the workflow.

use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::tempdir;

use oxidb::OxiDb;

/// Path to the fixture directory (relative to the test binary's
/// build location → up to the workspace root, then into tests/).
fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("upgrade")
}

/// Build the known-shape fixture used by all the upgrade tests.
/// Same shape every time so the assertions in `verify_fixture_shape`
/// are version-agnostic. Used by both the generator and the verifier.
fn build_fixture(db: &OxiDb) {
    // 10-doc collection — exercises Storage + WAL.
    for i in 0..10i64 {
        db.insert(
            "events",
            json!({
                "id": i,
                "name": format!("e{i}"),
                "n": i * 7,
            }),
        )
        .expect("insert event");
    }

    // Per-collection index — exercises .fidx OXIX-headed file.
    db.create_index("events", "n").expect("create index");

    // Transactional write — exercises _tx_commit_log (OXTX) gating.
    let tx = db.begin_transaction();
    db.tx_insert(tx, "meta", json!({"key": "build_id", "value": "fixture-v1"}))
        .expect("tx_insert");
    db.tx_insert(tx, "meta", json!({"key": "purpose", "value": "upgrade-chain"}))
        .expect("tx_insert");
    db.commit_transaction(tx).expect("commit");

    // Blob — exercises .meta JSON `format_version`.
    db.put_object(
        "audit",
        "handover.txt",
        b"upgrade-chain fixture: signed by the test that built me",
        "text/plain",
        HashMap::new(),
    )
    .expect("put_object");
}

/// Open the (already-built) fixture and check the invariants every
/// shipped version must continue to satisfy.
fn verify_fixture_shape(db: &OxiDb, fixture_label: &str) {
    // 1. Documents present and queryable.
    let all_events = db.find("events", &json!({})).expect("find events");
    assert_eq!(
        all_events.len(),
        10,
        "[{fixture_label}] expected 10 events, got {}",
        all_events.len()
    );

    // 2. Index-backed point query works.
    let by_n = db
        .find("events", &json!({ "n": 35 }))
        .expect("index query");
    assert_eq!(
        by_n.len(),
        1,
        "[{fixture_label}] index query on n=35 (which is 5*7) must hit exactly 1 doc"
    );
    assert_eq!(by_n[0]["id"].as_i64(), Some(5));

    // 3. Transactionally-inserted docs replayed correctly.
    let meta = db.find("meta", &json!({})).expect("find meta");
    assert_eq!(
        meta.len(),
        2,
        "[{fixture_label}] expected 2 tx-committed meta docs, got {}",
        meta.len()
    );
    let build_id = db
        .find_one("meta", &json!({"key": "build_id"}))
        .expect("find_one")
        .expect("build_id present");
    assert_eq!(build_id["value"].as_str(), Some("fixture-v1"));

    // 4. Blob survives + readable + content unchanged.
    let (bytes, _meta) = db
        .get_object("audit", "handover.txt")
        .expect("get blob");
    assert_eq!(
        bytes,
        b"upgrade-chain fixture: signed by the test that built me"
    );

    // 5. Aggregation still functions over restored data.
    let agg = db
        .aggregate(
            "events",
            &json!([{ "$group": { "_id": null, "total": { "$sum": "$n" } } }]),
        )
        .expect("aggregate");
    let total = agg[0]["total"].as_i64().unwrap();
    let expected: i64 = (0..10i64).map(|i| i * 7).sum(); // 0..63 step 7 → 315
    assert_eq!(
        total, expected,
        "[{fixture_label}] aggregation sum diverged"
    );
}

#[test]
#[ignore]
fn read_all_committed_fixtures() {
    let dir = fixtures_dir();
    assert!(
        dir.is_dir(),
        "fixtures dir missing: {} — create it and commit at least one fixture",
        dir.display()
    );

    let fixtures: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read fixtures dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("gz"))
        .collect();

    assert!(
        !fixtures.is_empty(),
        "no .tar.gz fixtures in {} — bootstrap with `cargo test --test cern_upgrade_chain \
         generate_fixture_for_current_version -- --ignored --nocapture`",
        dir.display()
    );

    eprintln!("[upgrade] reading {} fixture(s) from {}", fixtures.len(), dir.display());

    for fixture in &fixtures {
        let label = fixture.file_name().unwrap().to_string_lossy().to_string();
        let restore_dir = tempdir().expect("restore tmp");

        OxiDb::restore(fixture, restore_dir.path())
            .unwrap_or_else(|e| panic!("[{label}] restore failed: {e}"));

        let db = OxiDb::open(restore_dir.path())
            .unwrap_or_else(|e| panic!("[{label}] open restored failed: {e}"));

        verify_fixture_shape(&db, &label);
        eprintln!("[upgrade]   ✓ {label}");
    }

    eprintln!("[upgrade] ALL FIXTURES READ CLEANLY — {} version(s)", fixtures.len());
}

/// Bootstrap helper — only run when adding a new version's fixture.
/// Writes `tests/fixtures/upgrade/v<CURRENT_VERSION>.tar.gz`.
/// Idempotent: if the target already exists, refuses to overwrite so
/// accidental re-runs don't perturb committed bytes.
#[test]
#[ignore]
fn generate_fixture_for_current_version() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");

    // Pin to whatever version we're cutting. Manually update before
    // running for a new release.
    let target_version = "v0.28.4";
    let target_path = dir.join(format!("{target_version}.tar.gz"));

    if target_path.exists() {
        panic!(
            "fixture already exists at {} — refusing to overwrite. \
             Delete it manually if you really want to regenerate.",
            target_path.display()
        );
    }

    let data_dir = tempdir().expect("temp data dir");
    {
        let db = OxiDb::open(data_dir.path()).expect("open");
        build_fixture(&db);
    }

    // Reopen for the backup (backup() needs an OxiDb handle).
    let db = OxiDb::open(data_dir.path()).expect("reopen for backup");
    db.backup(&target_path).expect("backup");

    eprintln!(
        "[upgrade-gen] wrote fixture {} ({} bytes)",
        target_path.display(),
        std::fs::metadata(&target_path).unwrap().len()
    );

    // Smoke-check: the fixture we just wrote round-trips through the
    // verifier. If THIS fails, the fixture is broken before it ever
    // gets committed.
    let restore_dir = tempdir().expect("smoke restore tmp");
    OxiDb::restore(&target_path, restore_dir.path()).expect("smoke restore");
    let db = OxiDb::open(restore_dir.path()).expect("smoke reopen");
    verify_fixture_shape(&db, &format!("{target_version} (just generated)"));
    eprintln!("[upgrade-gen] ✓ fixture verifies; safe to commit");
}

/// Sanity: the build_fixture / verify_fixture_shape contract holds
/// against a freshly-built (in-memory, no backup) DB. Catches "the
/// helper functions disagree with themselves" bugs. NOT `#[ignore]`
/// because it's fast and the contract being broken would mean any
/// future fixture-generation is also broken.
#[test]
fn fresh_fixture_passes_verifier() {
    let data_dir = tempdir().unwrap();
    let db = OxiDb::open(data_dir.path()).unwrap();
    build_fixture(&db);
    verify_fixture_shape(&db, "fresh-in-memory");
}
