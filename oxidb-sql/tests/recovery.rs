//! Crash-recovery tests for the standalone SQL engine.
//!
//! These prove the engine recovers its state purely from its own on-disk files
//! (`.rdat` snapshots + WAL) with no reference to the document engine — the core
//! guarantee of the two-engine, separate-files design (ADR-0010).

use oxidb_sql::{Column, SqlEngine, SqlType, Table, Value};

fn users() -> Table {
    Table::new(
        "users",
        vec![
            Column::new("id", SqlType::Int).primary_key(),
            Column::new("name", SqlType::Text).not_null(),
            Column::new("age", SqlType::Int),
        ],
    )
}

/// Mutations made without an explicit checkpoint must survive a reopen, because
/// each was durably logged to the WAL and is replayed on open.
#[test]
fn wal_replay_recovers_uncheckpointed_writes() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        db.insert(
            "users",
            vec![Value::Int(1), Value::Text("ada".into()), Value::Int(36)],
        )
        .unwrap();
        db.insert(
            "users",
            vec![Value::Int(2), Value::Text("bob".into()), Value::Null],
        )
        .unwrap();
        let id3 = db
            .insert(
                "users",
                vec![Value::Int(3), Value::Text("cy".into()), Value::Int(20)],
            )
            .unwrap();
        db.delete("users", id3).unwrap();
        // NOTE: no checkpoint() — simulate a crash right here.
    }

    let db = SqlEngine::open(dir.path()).unwrap();
    let rows = db.scan("users").unwrap();
    assert_eq!(rows.len(), 2, "ada + bob survive, cy was deleted");
    assert_eq!(rows[0].1[1], Value::Text("ada".into()));
    assert_eq!(rows[1].1[2], Value::Null);
}

/// After a checkpoint the WAL is truncated and state lives in the `.rdat`
/// snapshots; a reopen must still see exactly the checkpointed rows, and further
/// post-checkpoint writes must layer on top via WAL replay.
#[test]
fn checkpoint_then_more_writes_recovers() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        db.insert(
            "users",
            vec![Value::Int(1), Value::Text("ada".into()), Value::Int(36)],
        )
        .unwrap();
        db.checkpoint().unwrap(); // WAL now truncated; ada is in the snapshot
        db.insert(
            "users",
            vec![Value::Int(2), Value::Text("bob".into()), Value::Int(41)],
        )
        .unwrap();
        // crash: bob is only in the WAL, ada only in the snapshot
    }

    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(db.row_count("users").unwrap(), 2);
    let names: Vec<_> = db
        .scan("users")
        .unwrap()
        .into_iter()
        .map(|(_, cells)| cells[1].clone())
        .collect();
    assert!(names.contains(&Value::Text("ada".into())));
    assert!(names.contains(&Value::Text("bob".into())));
}

/// Row ids must not be reused across a reopen — the counter is restored from the
/// max id seen in snapshot + WAL.
#[test]
fn row_ids_monotonic_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    let first;
    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        first = db
            .insert(
                "users",
                vec![Value::Int(1), Value::Text("a".into()), Value::Null],
            )
            .unwrap();
    }

    let db = SqlEngine::open(dir.path()).unwrap();
    let second = db
        .insert(
            "users",
            vec![Value::Int(2), Value::Text("b".into()), Value::Null],
        )
        .unwrap();
    assert!(second > first, "row ids must keep increasing after reopen");
}

/// DDL (create/drop) is recovered from the WAL too.
#[test]
fn ddl_recovers() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = SqlEngine::open(dir.path()).unwrap();
        db.create_table(users()).unwrap();
        db.create_table(Table::new("logs", vec![Column::new("msg", SqlType::Text)]))
            .unwrap();
        db.drop_table("logs").unwrap();
    }

    let db = SqlEngine::open(dir.path()).unwrap();
    assert_eq!(db.table_names(), vec!["users".to_string()]);
    assert!(db.table_def("logs").is_none());
}
