//! Fixture-based VM tests: each `tests/fixtures/*.cobrac` was compiled by
//! the Go reference (`cobra build --portable`) and its `*.expected.txt`
//! captured from `cobra run` — so these pin exact cross-engine output
//! (try/catch/finally mechanics, dict ordering, decimal math, slices,
//! rune-based strings, float formatting) without needing Go at test time.

use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn run_fixture(name: &str) -> String {
    let data = std::fs::read(fixture(&format!("{name}.cobrac"))).expect("fixture exists");
    let bytecode = oxidb_cobra::decode(&data).expect("decodes");
    oxidb_cobra::validate(&bytecode).expect("validates");
    let mut vm = oxidb_cobra::Vm::new(&bytecode);
    vm.run().expect("runs cleanly");
    vm.output()
}

fn expected(name: &str) -> String {
    std::fs::read_to_string(fixture(&format!("{name}.expected.txt"))).expect("expected exists")
}

#[test]
fn try_catch_finally_matches_reference() {
    assert_eq!(run_fixture("trycatch"), expected("trycatch"));
}

#[test]
fn mixed_semantics_match_reference() {
    assert_eq!(run_fixture("mixed"), expected("mixed"));
}

/// Inheritance, super, records + with, getters/setters, sealed-field typo
/// suggestions, boxed closure captures, exact error strings.
#[test]
fn oop_probe_matches_reference() {
    assert_eq!(run_fixture("oop_probe"), expected("oop_probe"));
}

/// Builtin corner cases: chr/ord/hash/int/float, zip/enumerate/any/all over
/// ranges, string method edges, dict get/del, statics as values.
#[test]
fn builtins_probe_matches_reference() {
    assert_eq!(run_fixture("builtins_probe"), expected("builtins_probe"));
}
