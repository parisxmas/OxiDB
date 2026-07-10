//! `run_procedure` (ADR-0014 Phase 2): main-then-call driving, NativeObject
//! method dispatch, notices capture, and the non-catchable fuel limit.
//! Fixtures are pre-compiled with `cobra build --portable` (no Go needed).

use std::cell::RefCell;
use std::rc::Rc;

use oxidb_cobra::value::{NativeError, NativeObject, Value, inspect};
use oxidb_cobra::{Bytecode, run_procedure};

fn load(name: &str) -> Bytecode {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.cobrac"));
    let data = std::fs::read(path).expect("fixture exists");
    oxidb_cobra::decode(&data).expect("decodes")
}

/// A stand-in for the SQL engine's db handle.
struct MockDb;

impl NativeObject for MockDb {
    fn type_name(&self) -> &str {
        "db"
    }
    fn call_method(&self, name: &str, _args: &[Value]) -> Result<Value, NativeError> {
        match name {
            "query" => Ok(Value::List(Rc::new(RefCell::new(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
            ])))),
            _ => Err(NativeError::new(format!("db has no method '{name}'"))),
        }
    }
}

fn db() -> Value {
    Value::Native(Rc::new(MockDb))
}

#[test]
fn runs_named_function_with_args_and_notices() {
    let out = run_procedure(
        &load("proc_add"),
        "run",
        vec![db(), Value::Int(2), Value::Int(40)],
        None,
    )
    .unwrap();
    assert_eq!(inspect(&out.result), "42");
    assert_eq!(out.notices, "adding 2 40\n");
}

#[test]
fn native_object_methods_and_errors() {
    let out = run_procedure(&load("proc_native"), "run", vec![db()], None).unwrap();
    // 1+2+3 from db.query; the unknown-method error was catchable.
    assert_eq!(inspect(&out.result), "6");
    assert!(
        out.notices
            .contains("caught: line 10: db has no method 'boom'"),
        "notices: {}",
        out.notices
    );
    assert!(
        out.notices.contains("db is <db> db"),
        "notices: {}",
        out.notices
    );
}

#[test]
fn missing_run_function() {
    // trycatch defines no global named `run`.
    let err = run_procedure(&load("trycatch"), "run", vec![], None).unwrap_err();
    assert_eq!(err, "procedure has no function 'run'");
}

#[test]
fn run_global_that_is_not_a_function() {
    let err = run_procedure(&load("proc_notfn"), "run", vec![], None).unwrap_err();
    assert_eq!(err, "'run' is not a function");
}

#[test]
fn arity_mismatch_reuses_wrong_number_error() {
    let err = run_procedure(&load("proc_add"), "run", vec![db()], None).unwrap_err();
    assert_eq!(err, "wrong number of arguments to run: want=3, got=1");
}

#[test]
fn fuel_limit_is_not_catchable() {
    // The infinite loop sits inside try/catch; the kill must bypass it.
    let err = run_procedure(&load("proc_spin"), "run", vec![db()], Some(100_000)).unwrap_err();
    assert_eq!(err, "instruction limit exceeded");
}

#[test]
fn ample_fuel_leaves_normal_runs_untouched() {
    let out = run_procedure(
        &load("proc_add"),
        "run",
        vec![db(), Value::Int(1), Value::Int(1)],
        Some(100_000_000),
    )
    .unwrap();
    assert_eq!(inspect(&out.result), "2");
}
