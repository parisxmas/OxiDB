//! Basic single-column FOREIGN KEY enforcement.
//!
//! Child side: INSERT/UPDATE must find the referenced parent row (NULL FK
//! references nothing). Parent side: DELETE honours ON DELETE NO ACTION /
//! RESTRICT (reject), CASCADE (delete children), SET NULL (null children);
//! a referenced key can't be UPDATEd while children point at it.

use oxidb_sql::{QueryResult, SqlEngine};

fn open() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    (dir, db)
}

/// Number of rows a SELECT returns.
fn count(db: &SqlEngine, sql: &str) -> usize {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Select { rows, .. } => rows.len(),
        other => panic!("expected SELECT, got {other:?}"),
    }
}

fn setup(db: &SqlEngine, on_delete: &str) {
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute(&format!(
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT REFERENCES users(id) {on_delete}, note TEXT)"
    ))
    .unwrap();
    db.execute("INSERT INTO users VALUES (1,'ada'),(2,'bob')")
        .unwrap();
}

#[test]
fn insert_child_requires_parent() {
    let (_d, db) = open();
    setup(&db, "");
    // Valid parent → ok.
    db.execute("INSERT INTO orders VALUES (10, 1, 'x')")
        .unwrap();
    // Missing parent → FK violation.
    assert!(
        db.execute("INSERT INTO orders VALUES (11, 999, 'y')")
            .is_err(),
        "insert referencing a non-existent parent must fail"
    );
    // A NULL foreign key references nothing → allowed.
    db.execute("INSERT INTO orders VALUES (12, NULL, 'z')")
        .unwrap();
    assert_eq!(count(&db, "SELECT * FROM orders"), 2);
}

#[test]
fn update_child_requires_parent() {
    let (_d, db) = open();
    setup(&db, "");
    db.execute("INSERT INTO orders VALUES (10, 1, 'x')")
        .unwrap();
    db.execute("UPDATE orders SET user_id = 2 WHERE id = 10")
        .unwrap(); // valid parent
    assert!(
        db.execute("UPDATE orders SET user_id = 999 WHERE id = 10")
            .is_err(),
        "update to a non-existent parent must fail"
    );
    db.execute("UPDATE orders SET user_id = NULL WHERE id = 10")
        .unwrap(); // NULL is fine
}

#[test]
fn delete_parent_restrict_by_default() {
    let (_d, db) = open();
    setup(&db, ""); // no ON DELETE ⇒ NO ACTION / RESTRICT
    db.execute("INSERT INTO orders VALUES (10, 1, 'x')")
        .unwrap();
    assert!(
        db.execute("DELETE FROM users WHERE id = 1").is_err(),
        "cannot delete a parent that a child references"
    );
    // The unreferenced parent deletes fine.
    db.execute("DELETE FROM users WHERE id = 2").unwrap();
    assert_eq!(count(&db, "SELECT * FROM users"), 1);
}

#[test]
fn delete_parent_cascade() {
    let (_d, db) = open();
    setup(&db, "ON DELETE CASCADE");
    db.execute("INSERT INTO orders VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'z')")
        .unwrap();
    db.execute("DELETE FROM users WHERE id = 1").unwrap();
    // Orders of user 1 cascaded away; user 2's order survives.
    assert_eq!(count(&db, "SELECT * FROM orders"), 1);
    assert_eq!(count(&db, "SELECT * FROM orders WHERE user_id = 2"), 1);
}

#[test]
fn delete_parent_set_null() {
    let (_d, db) = open();
    setup(&db, "ON DELETE SET NULL");
    db.execute("INSERT INTO orders VALUES (10, 1, 'x'), (11, 2, 'y')")
        .unwrap();
    db.execute("DELETE FROM users WHERE id = 1").unwrap();
    // The row survives with a NULL foreign key.
    assert_eq!(count(&db, "SELECT * FROM orders"), 2);
    assert_eq!(count(&db, "SELECT * FROM orders WHERE user_id IS NULL"), 1);
}

#[test]
fn update_referenced_key_is_restricted() {
    let (_d, db) = open();
    setup(&db, "");
    db.execute("INSERT INTO orders VALUES (10, 1, 'x')")
        .unwrap();
    assert!(
        db.execute("UPDATE users SET id = 99 WHERE id = 1").is_err(),
        "cannot change a key a child references"
    );
    // Changing an unreferenced parent key is fine.
    db.execute("UPDATE users SET id = 99 WHERE id = 2").unwrap();
}
