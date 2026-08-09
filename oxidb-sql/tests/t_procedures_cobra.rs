//! COBRA stored procedures (ADR-0014 Phase 2): `CREATE PROCEDURE ...
//! LANGUAGE COBRA AS '<base64 .cobrac>'`, CALL dispatch through the Cobra
//! VM, the `db` handle, result shaping, notices, the fuel limit, and
//! catalog/WAL persistence.
//!
//! Fixtures in `tests/data/cobra/` were compiled with `cobra build
//! --portable` (Go CLI); the `.cobrac` bytes are committed so the tests
//! never need the Go toolchain.

mod common;

use base64::Engine as _;
use common::*;
use oxidb_sql::{QueryResult, Value};

/// Base64 of a compiled fixture, ready to embed in a CREATE statement.
fn payload(name: &str) -> String {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/cobra")
        .join(format!("{name}.cobrac"));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("fixture {path:?}: {e}"));
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn create_cobra(db: &oxidb_sql::SqlEngine, signature: &str, fixture: &str) {
    db.execute(&format!(
        "CREATE PROCEDURE {signature} LANGUAGE COBRA AS '{}'",
        payload(fixture)
    ))
    .unwrap();
}

fn setup(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE people (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, age INT)")
        .unwrap();
    db.execute("INSERT INTO people (name, age) VALUES ('ali', 40), ('ayse', 25)")
        .unwrap();
}

// ─── CREATE + CALL ───────────────────────────────────────────────────────

#[test]
fn add_row_inserts_and_returns_affected() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "add_row(name TEXT, age INT)", "add_row");

    // run returns db.execute's affected count -> single "value" column.
    let (cols, rws) = cols_rows(&db, "CALL add_row('veli', 30)");
    assert_eq!(cols, vec!["value"]);
    assert_eq!(rws, vec![vec![i(1)]]);

    assert_eq!(
        rows(&db, "SELECT name, age FROM people WHERE id = 3"),
        vec![vec![t("veli"), i(30)]]
    );
}

#[test]
fn stats_returns_dict_with_notices() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "stats()", "stats");

    let r = db.execute("CALL stats()").unwrap().pop().unwrap();
    let QueryResult::Called { inner, notices } = r else {
        panic!("expected Called (the proc prints), got {r:?}");
    };
    assert_eq!(notices, vec!["stats over 2 rows".to_string()]);
    let QueryResult::Select { columns, rows, .. } = *inner else {
        panic!("expected Select inside Called, got {inner:?}");
    };
    assert_eq!(columns, vec!["count", "total", "oldest"]);
    assert_eq!(rows, vec![vec![i(2), i(65), t("ali")]]);
}

#[test]
fn query_rows_shape_as_result_set() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "all_rows()", "all_rows");

    let (cols, rws) = cols_rows(&db, "CALL all_rows()");
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(rws, vec![vec![i(1), t("ali")], vec![i(2), t("ayse")]]);
}

#[test]
fn call_args_are_coerced_and_validated() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "add_row(name TEXT, age INT)", "add_row");
    // Arity errors mirror SQL procedures.
    assert!(db.execute("CALL add_row('x')").is_err());
    assert!(db.execute("CALL add_row('x', 1, 2)").is_err());
    // Type mismatch names the parameter.
    let err = db
        .execute("CALL add_row('x', 'y')")
        .unwrap_err()
        .to_string();
    assert!(err.contains("\"age\""), "unexpected error: {err}");
    // Bind parameters work as CALL arguments.
    assert_eq!(
        rows_p(&db, "CALL add_row($1, $2)", &[t("can"), i(7)]),
        vec![vec![i(1)]]
    );
}

// ─── Errors: catchable vs fuel ───────────────────────────────────────────

#[test]
fn constraint_violation_is_catchable_in_cobra() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "safe_insert(id INT)", "safe_insert");

    // id=1 exists: the PK violation is caught, a fallback value returned.
    let (cols, rws) = cols_rows(&db, "CALL safe_insert(1)");
    assert_eq!(cols, vec!["value"]);
    let Value::Text(msg) = &rws[0][0] else {
        panic!("expected text fallback, got {rws:?}");
    };
    assert!(msg.starts_with("fallback: "), "got: {msg}");
    assert!(msg.contains("duplicate key"), "got: {msg}");
    // The failed insert changed nothing.
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM people"), vec![vec![i(2)]]);

    // id=99 is free: the insert lands and "inserted" comes back.
    assert_eq!(rows(&db, "CALL safe_insert(99)"), vec![vec![t("inserted")]]);
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM people"), vec![vec![i(3)]]);
}

#[test]
fn uncaught_error_rolls_back_the_whole_call() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "atomic_fail()", "atomic_fail");

    let err = db.execute("CALL atomic_fail()").unwrap_err().to_string();
    assert!(err.contains("duplicate key"), "got: {err}");
    // The first insert ('ghost') must have rolled back with the CALL.
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM people"), vec![vec![i(2)]]);
}

#[test]
fn fuel_limit_kills_runaway_loops_uncatchably() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "spin()", "spin");

    let err = db.execute("CALL spin()").unwrap_err().to_string();
    assert!(err.contains("instruction limit exceeded"), "got: {err}");
    // The proc's own try/catch must NOT have swallowed the kill (it would
    // have returned "caught" successfully).
    assert!(!err.contains("caught"), "got: {err}");
}

// ─── CREATE-time validation ──────────────────────────────────────────────

#[test]
fn create_time_rejections() {
    let (_d, db) = open();
    setup(&db);

    // Bad base64.
    let err = db
        .execute("CREATE PROCEDURE p() LANGUAGE COBRA AS '!!!not-base64!!!'")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("invalid base64 in COBRA procedure body"),
        "got: {err}"
    );

    // Valid base64, not COBRAP bytecode.
    let err = db
        .execute("CREATE PROCEDURE p() LANGUAGE COBRA AS 'aGVsbG8='")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("not a portable compiled Cobra file"),
        "got: {err}"
    );

    // No `run` function.
    let err = db
        .execute(&format!(
            "CREATE PROCEDURE p() LANGUAGE COBRA AS '{}'",
            payload("norun")
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("COBRA procedure must define a function 'run'"),
        "got: {err}"
    );

    // Declared parameter count must match run's arity (db + N).
    let err = db
        .execute(&format!(
            "CREATE PROCEDURE p() LANGUAGE COBRA AS '{}'",
            payload("add_row")
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("'run' must take 1 parameter(s) (db + 0 declared), got 3"),
        "got: {err}"
    );

    // Nothing bad reached the catalog.
    assert!(db.execute("CALL p()").is_err());

    // Other languages keep the established rejection path.
    assert!(
        db.execute("CREATE PROCEDURE p() LANGUAGE PLPGSQL AS BEGIN SELECT 1; END")
            .is_err()
    );
}

#[test]
fn or_alter_and_drop() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "add_row(name TEXT, age INT)", "add_row");

    // Duplicate without OR ALTER errors; with it, the definition swaps.
    assert!(
        db.execute(&format!(
            "CREATE PROCEDURE add_row(name TEXT, age INT) LANGUAGE COBRA AS '{}'",
            payload("add_row")
        ))
        .is_err()
    );
    db.execute(&format!(
        "CREATE OR ALTER PROCEDURE add_row(name TEXT, age INT) LANGUAGE COBRA AS '{}'",
        payload("add_row")
    ))
    .unwrap();

    db.execute("DROP PROCEDURE add_row").unwrap();
    assert!(db.execute("CALL add_row('x', 1)").is_err());
}

// ─── Introspection + persistence ─────────────────────────────────────────

#[test]
fn show_procedures_has_language_column() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "stats()", "stats");
    db.execute("CREATE PROCEDURE sqlp(n INT) AS BEGIN SELECT n FROM people LIMIT 1; END")
        .unwrap();

    let (cols, rws) = cols_rows(&db, "SHOW PROCEDURES");
    assert_eq!(cols, vec!["procedure", "params", "language", "definition"]);
    assert_eq!(rws.len(), 2);
    // BTreeMap order: sqlp, stats.
    assert_eq!(rws[0][0], t("sqlp"));
    assert_eq!(rws[0][2], t("sql"));
    assert_eq!(rws[1][0], t("stats"));
    assert_eq!(rws[1][2], t("cobra"));
    let Value::Text(def) = &rws[1][3] else {
        panic!("expected text definition");
    };
    assert!(
        def.starts_with("<cobra bytecode, ") && def.ends_with(" bytes>"),
        "definition: {def}"
    );
}

#[test]
fn persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open_at(dir.path());
        setup(&db);
        create_cobra(&db, "add_row(name TEXT, age INT)", "add_row");
        assert_eq!(rows(&db, "CALL add_row('ilk', 1)"), vec![vec![i(1)]]);
    }
    // WAL replay path restores the cobra procedure.
    {
        let db = open_at(dir.path());
        assert_eq!(rows(&db, "CALL add_row('iki', 2)"), vec![vec![i(1)]]);
        db.checkpoint().unwrap();
    }
    // Checkpointed-catalog path (bytecode round-trips through catalog.json).
    {
        let db = open_at(dir.path());
        assert_eq!(rows(&db, "CALL add_row('son', 3)"), vec![vec![i(1)]]);
        assert_eq!(rows(&db, "SELECT COUNT(*) FROM people"), vec![vec![i(5)]]);
    }
}

// ─── Wire (JSON) shape ───────────────────────────────────────────────────

#[test]
fn json_wire_carries_notices() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "stats()", "stats");

    // The server's `{"cmd":"sql"}` path: notices ride a "notices" key next
    // to the usual columns/rows.
    let out = oxidb_sql::json::execute_json(&db, "CALL stats()", None, false).unwrap();
    let entry = &out.as_array().unwrap()[0];
    assert_eq!(
        entry["columns"],
        serde_json::json!(["count", "total", "oldest"])
    );
    assert_eq!(entry["rows"], serde_json::json!([[2, 65, "ali"]]));
    assert_eq!(entry["notices"], serde_json::json!(["stats over 2 rows"]));

    // A notice-free cobra CALL has no "notices" key at all.
    create_cobra(&db, "all_rows()", "all_rows");
    let out = oxidb_sql::json::execute_json(&db, "CALL all_rows()", None, false).unwrap();
    assert!(out.as_array().unwrap()[0].get("notices").is_none());
}

// ─── Transactions ────────────────────────────────────────────────────────

#[test]
fn cobra_call_joins_open_transaction() {
    let (_d, db) = open();
    setup(&db);
    create_cobra(&db, "add_row(name TEXT, age INT)", "add_row");

    db.execute("BEGIN; CALL add_row('gecici', 9); ROLLBACK")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM people"), vec![vec![i(2)]]);

    db.execute("BEGIN; CALL add_row('kalici', 9); COMMIT")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM people"), vec![vec![i(3)]]);
}

// ─── Extension methods (ADR-0025 Phase 4) ────────────────────────────────

/// Without a host-installed extension, `db.rec_*` refuses by name — the
/// standalone SQL engine has no rec surface, and the error must say so
/// rather than pretend the method does not exist.
#[test]
fn rec_methods_without_an_extension_refuse_by_name() {
    let (_d, db) = open();
    create_cobra(&db, "rec_related(item TEXT)", "rec_related");
    let err = db.execute("CALL rec_related('kahve')").unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("rec_related") && msg.contains("extension"),
        "the refusal must name the method and the reason: {msg}"
    );
}

/// With an extension installed, the call crosses the JSON boundary intact:
/// the dict argument arrives as the wire-shaped request, the JSON answer
/// comes back as COBRA values, and the procedure's own code consumes it.
#[test]
fn an_installed_extension_serves_rec_methods() {
    struct Fake;
    impl oxidb_sql::NativeExt for Fake {
        fn call(
            &self,
            method: &str,
            args: &serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            assert_eq!(method, "rec_related");
            let req = &args[0];
            assert_eq!(req["model"], "purchase");
            assert_eq!(req["item"], "kahve");
            assert_eq!(req["scoring"], "count");
            Ok(serde_json::json!({
                "recommendations": [
                    {"item": "süt", "score": 20.0},
                    {"item": "filtre", "score": 5.0},
                ]
            }))
        }
    }

    let (_d, db) = open();
    db.set_native_ext(std::sync::Arc::new(Fake));
    create_cobra(&db, "rec_related(item TEXT)", "rec_related");
    let (cols, rws) = cols_rows(&db, "CALL rec_related('kahve')");
    // The procedure returns the recommendations list; each dict becomes a row.
    assert_eq!(cols, vec!["item", "score"]);
    assert_eq!(
        rws,
        vec![vec![t("süt"), d(20.0)], vec![t("filtre"), d(5.0)]]
    );
}

/// An extension error surfaces as a catchable procedure error, and the
/// method name that failed is in it.
#[test]
fn an_extension_error_names_its_method() {
    struct Failing;
    impl oxidb_sql::NativeExt for Failing {
        fn call(&self, _: &str, _: &serde_json::Value) -> Result<serde_json::Value, String> {
            Err("rec engine is not enabled (set OXIDB_REC=1)".into())
        }
    }
    let (_d, db) = open();
    db.set_native_ext(std::sync::Arc::new(Failing));
    create_cobra(&db, "rec_related(item TEXT)", "rec_related");
    let err = db.execute("CALL rec_related('x')").unwrap_err();
    assert!(format!("{err}").contains("OXIDB_REC"), "{err}");
}

// ─── 0.13 fused opcodes (compiler ahead of the VM) ───────────────────────

/// The 0.13 compiler emits fused opcodes (52..=62) the VM did not know —
/// container format v2 decoded fine and execution then died on the first
/// fusion. This fixture provokes them and checks the arithmetic; the
/// bytecode assertion below is the vacuity guard: if a future compiler
/// stops fusing (or the fixture is recompiled unfused), the execution test
/// alone would pass without testing anything.
#[test]
fn fused_opcodes_execute_correctly() {
    let (_d, db) = open();
    create_cobra(&db, "fusions()", "fusions");
    let (cols, rws) = cols_rows(&db, "CALL fusions()");
    let get = |name: &str| {
        let at = cols.iter().position(|c| c == name).unwrap();
        rws[0][at].clone()
    };
    assert_eq!(get("sum"), i(12));
    assert_eq!(get("total"), i(100));
    assert_eq!(get("count"), i(8));
    assert_eq!(get("scaled"), i(200));
    assert_eq!(get("inv"), i(0));
    assert_eq!(get("boxv"), i(13));
    assert_eq!(get("acc"), i(47));
}

/// The vacuity guard: the compiled fixture must actually CONTAIN fused
/// opcodes, across the whole constant pool (functions live there).
#[test]
fn the_fusion_fixture_really_contains_fusions() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/cobra/fusions.cobrac");
    let bytes = std::fs::read(&path).unwrap();
    let bc = oxidb_cobra::bytecode::decode(&bytes).unwrap();

    let mut seen = std::collections::BTreeSet::new();
    let mut scan = |ins: &[u8]| {
        let mut ip = 0usize;
        while ip < ins.len() {
            let op = oxidb_cobra::bytecode::Op::from_byte(ins[ip]).unwrap_or_else(|| {
                panic!(
                    "unknown opcode {} at {ip} — VM behind the compiler AGAIN",
                    ins[ip]
                )
            });
            if (op as u8) >= 52 {
                seen.insert(op.name());
            }
            ip += 1 + op.operand_widths().iter().sum::<usize>();
        }
    };
    scan(&bc.instructions);
    for c in &bc.constants {
        if let oxidb_cobra::bytecode::Constant::Func(f) = c {
            scan(&f.instructions);
        }
    }
    assert!(
        seen.len() >= 5,
        "expected a spread of fused opcodes in the fixture, found only: {seen:?}"
    );
}
