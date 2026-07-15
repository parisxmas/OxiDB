//! CREATE SEQUENCE / NEXT VALUE FOR — EF Core HiLo value generation.

mod common;

use common::*;
use oxidb_sql::Value;

#[test]
fn hilo_sequence_hands_out_incrementing_blocks() {
    let (_d, db) = open();
    db.execute("CREATE SEQUENCE catalog_hilo START WITH 1 INCREMENT BY 10 NO CYCLE")
        .unwrap();
    // Each NEXT VALUE FOR returns the current value then advances by increment.
    assert_eq!(rows(&db, "SELECT NEXT VALUE FOR catalog_hilo"), vec![vec![i(1)]]);
    assert_eq!(rows(&db, "SELECT NEXT VALUE FOR catalog_hilo"), vec![vec![i(11)]]);
    // Quoted name (EF emits "catalog_hilo").
    assert_eq!(
        rows(&db, "SELECT NEXT VALUE FOR \"catalog_hilo\""),
        vec![vec![i(21)]]
    );
}

#[test]
fn sequence_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        db.execute("CREATE SEQUENCE s START WITH 100 INCREMENT BY 1")
            .unwrap();
        assert_eq!(rows(&db, "SELECT NEXT VALUE FOR s"), vec![vec![i(100)]]);
        assert_eq!(rows(&db, "SELECT NEXT VALUE FOR s"), vec![vec![i(101)]]);
    }
    // Reopen: the sequence and its position persist (catalog.json).
    let db = open_at(dir.path());
    assert_eq!(rows(&db, "SELECT NEXT VALUE FOR s"), vec![vec![i(102)]]);
}

#[test]
fn drop_sequence() {
    let (_d, db) = open();
    db.execute("CREATE SEQUENCE s START WITH 1 INCREMENT BY 1")
        .unwrap();
    db.execute("DROP SEQUENCE s").unwrap();
    assert!(db.execute("SELECT NEXT VALUE FOR s").is_err());
    // IF EXISTS is a no-op on a missing sequence.
    db.execute("DROP SEQUENCE IF EXISTS s").unwrap();
}

#[test]
fn default_start_and_increment() {
    let (_d, db) = open();
    db.execute("CREATE SEQUENCE s").unwrap();
    assert_eq!(rows(&db, "SELECT NEXT VALUE FOR s"), vec![vec![i(1)]]);
    assert_eq!(rows(&db, "SELECT NEXT VALUE FOR s"), vec![vec![i(2)]]);
}
