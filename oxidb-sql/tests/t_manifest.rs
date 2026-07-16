//! Generational persistence + the MANIFEST commit point (crash atomicity).
//!
//! A checkpoint writes a whole new `gen.<N>/` and promotes it with a single
//! atomic MANIFEST rename. These tests assert that recovery only ever loads the
//! committed generation — never a half-written one a crash left behind — and
//! that the frequently-saved sequence counters can't desync a generation's
//! catalog from its snapshots.

mod common;

use std::path::Path;

use common::*;
use oxidb_sql::{SqlEngine, Value};

/// The committed generation recorded in the MANIFEST.
fn committed_generation(root: &Path) -> u64 {
    let bytes = std::fs::read(root.join("MANIFEST")).expect("MANIFEST after a checkpoint");
    let m: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    m["generation"].as_u64().unwrap()
}

/// A freshly written generation is committed by, and only by, the MANIFEST
/// rename. A higher-numbered generation directory with no MANIFEST pointing at
/// it — exactly what a checkpoint that crashed before its commit leaves — is
/// ignored at open and swept away.
#[test]
fn uncommitted_generation_is_ignored_and_swept() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 10), (2, 20)").unwrap();
    db.checkpoint().unwrap();
    drop(db);

    let committed = committed_generation(dir.path());

    // Forge two orphan generations, as crashed checkpoints would: higher
    // numbers, with a corrupt catalog that would blow up if ever loaded.
    for g in [committed + 1, committed + 4] {
        let orphan = dir.path().join(format!("gen.{g}"));
        std::fs::create_dir_all(&orphan).unwrap();
        std::fs::write(orphan.join("catalog.json"), b"{ not valid json at all").unwrap();
        std::fs::write(orphan.join("u.rdat"), b"garbage").unwrap();
    }

    // Recovery uses the committed generation (so it succeeds and reads right),
    // and the orphans are gone afterwards.
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT SUM(v) FROM u"),
        vec![vec![Value::Int(30)]]
    );
    for g in [committed + 1, committed + 4] {
        assert!(
            !dir.path().join(format!("gen.{g}")).exists(),
            "orphan gen.{g} must be swept at open"
        );
    }
    assert!(dir.path().join(format!("gen.{committed}")).exists());
}

/// Each checkpoint advances the committed generation and GCs the one it
/// superseded, so exactly one generation directory is live at a time.
#[test]
fn checkpoint_advances_generation_and_gcs_the_old() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 1)").unwrap();

    db.checkpoint().unwrap();
    let g1 = committed_generation(dir.path());
    db.execute("INSERT INTO u VALUES (2, 2)").unwrap();
    db.checkpoint().unwrap();
    let g2 = committed_generation(dir.path());

    assert_eq!(g2, g1 + 1, "generation advances each checkpoint");
    assert!(
        !dir.path().join(format!("gen.{g1}")).exists(),
        "old gen GC'd"
    );
    assert!(dir.path().join(format!("gen.{g2}")).exists());

    // Only one generation directory remains on disk.
    let gen_dirs = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("gen."))
        .count();
    assert_eq!(gen_dirs, 1);
}

/// The scenario the MANIFEST exists to make safe: a table checkpointed at one
/// arity, then a lazy `ADD COLUMN` that is *not* checkpointed, then a sequence
/// operation (which persists separately). Reopen must reconstruct the table —
/// with the catalog and snapshot at agreeing arities — and the sequence.
///
/// Before sequences were split out of the generationed catalog, the sequence
/// save would have written the new (in-memory) arity into the committed
/// generation's catalog while its snapshot stayed at the old arity — a mismatch
/// that corrupts recovery.
#[test]
fn sequence_op_after_lazy_alter_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    db.execute("CREATE TABLE u (id INT PRIMARY KEY, a INT)")
        .unwrap();
    db.execute("INSERT INTO u VALUES (1, 10)").unwrap();
    db.checkpoint().unwrap(); // u snapshot committed at arity 2

    db.execute("CREATE SEQUENCE s START WITH 100").unwrap();
    // Lazy ADD: in-memory arity 3, committed snapshot still arity 2, no checkpoint.
    db.execute("ALTER TABLE u ADD COLUMN b INT DEFAULT 7")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT NEXT VALUE FOR s"),
        vec![vec![Value::Int(100)]]
    );
    drop(db); // crash-equivalent: no checkpoint after the ALTER

    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, a, b FROM u"),
        vec![vec![Value::Int(1), Value::Int(10), Value::Int(7)]]
    );
    assert_eq!(
        rows(&db, "SELECT NEXT VALUE FOR s"),
        vec![vec![Value::Int(101)]]
    );
}

/// A fresh database has no MANIFEST; its first checkpoint creates one and the
/// `gen.1/` layout. (Legacy flat-layout databases migrate the same way at their
/// first checkpoint — the root `catalog.json`/`.rdat` files are the "gen 0".)
#[test]
fn first_checkpoint_creates_manifest_and_gen_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    assert!(!dir.path().join("MANIFEST").exists());
    db.execute("CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO u VALUES (1), (2)").unwrap();
    db.checkpoint().unwrap();

    assert_eq!(committed_generation(dir.path()), 1);
    assert!(dir.path().join("gen.1").join("catalog.json").exists());
    assert!(dir.path().join("gen.1").join("u.rdat").exists());
    // The pre-checkpoint root catalog (if any) is cleaned up.
    assert!(!dir.path().join("catalog.json").exists());

    drop(db);
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM u"),
        vec![vec![Value::Int(2)]]
    );
}

/// A database written by an older build — flat root-level `catalog.json` +
/// `<table>.rdat`, no MANIFEST — opens as generation 0 and migrates to the
/// `gen.1/` layout at its first checkpoint, cleaning up the root files.
#[test]
fn legacy_flat_layout_opens_and_migrates() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.execute("CREATE TABLE u (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO u VALUES (1, 5), (2, 15)").unwrap();
        db.checkpoint().unwrap();
    }
    // Downgrade to the pre-MANIFEST flat form: move the committed generation's
    // files to the root and remove the MANIFEST.
    let g = committed_generation(dir.path());
    let gd = dir.path().join(format!("gen.{g}"));
    for entry in std::fs::read_dir(&gd).unwrap().flatten() {
        std::fs::rename(entry.path(), dir.path().join(entry.file_name())).unwrap();
    }
    std::fs::remove_dir_all(&gd).unwrap();
    std::fs::remove_file(dir.path().join("MANIFEST")).unwrap();
    assert!(dir.path().join("catalog.json").exists());
    assert!(dir.path().join("u.rdat").exists());

    // Opens as legacy (generation 0).
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT SUM(v) FROM u"),
        vec![vec![Value::Int(20)]]
    );
    // First checkpoint migrates to gen.1 and removes the root-level files.
    db.execute("INSERT INTO u VALUES (3, 30)").unwrap();
    db.checkpoint().unwrap();
    assert_eq!(committed_generation(dir.path()), 1);
    assert!(!dir.path().join("catalog.json").exists());
    assert!(!dir.path().join("u.rdat").exists());

    drop(db);
    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(&db, "SELECT SUM(v) FROM u"),
        vec![vec![Value::Int(50)]]
    );
}
