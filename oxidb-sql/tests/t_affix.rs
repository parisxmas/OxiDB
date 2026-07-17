//! `STARTS_WITH` / `ENDS_WITH` — exact, literal, case-sensitive affix tests.
//!
//! These exist so .NET's ordinal `String.StartsWith`/`EndsWith` have a direct
//! rendering. The two properties that make them *not* a synonym for `LIKE` are
//! what these tests pin:
//!   1. **case-sensitive** — the engine's LIKE is ASCII case-insensitive,
//!      these are not;
//!   2. **literal** — `%` and `_` in the affix are ordinary characters, so a
//!      user-supplied needle can never turn into a wildcard.
//!
//! The borrowed fast path (`eval_func_ref`) and the owned fallback
//! (`eval_scalar_func`) must agree, so everything here is also checked through
//! a column (fast path) and through a folded literal (fallback).

mod common;

use common::*;
use oxidb_sql::Value;

fn b(db: &oxidb_sql::SqlEngine, expr: &str) -> Value {
    rows(db, &format!("SELECT {expr}"))[0][0].clone()
}

#[test]
fn affix_basics_and_nulls() {
    let (_d, db) = open();
    for (expr, want) in [
        ("STARTS_WITH('hello world', 'hello')", Value::Bool(true)),
        ("STARTS_WITH('hello world', 'world')", Value::Bool(false)),
        ("ENDS_WITH('hello world', 'world')", Value::Bool(true)),
        ("ENDS_WITH('hello world', 'hello')", Value::Bool(false)),
        // The whole string is both a prefix and a suffix of itself.
        ("STARTS_WITH('abc', 'abc')", Value::Bool(true)),
        ("ENDS_WITH('abc', 'abc')", Value::Bool(true)),
        // An empty affix matches everything (as .NET and Rust both agree).
        ("STARTS_WITH('abc', '')", Value::Bool(true)),
        ("ENDS_WITH('abc', '')", Value::Bool(true)),
        // A longer affix than the string cannot match.
        ("STARTS_WITH('ab', 'abc')", Value::Bool(false)),
        ("ENDS_WITH('ab', 'abc')", Value::Bool(false)),
        // NULL propagates from either side.
        ("STARTS_WITH(NULL, 'a')", Value::Null),
        ("STARTS_WITH('a', NULL)", Value::Null),
        ("ENDS_WITH(NULL, 'a')", Value::Null),
        ("ENDS_WITH('a', NULL)", Value::Null),
        // Multi-byte: must not split a character or mis-count.
        ("STARTS_WITH('çilek', 'çi')", Value::Bool(true)),
        ("ENDS_WITH('çilek', 'ek')", Value::Bool(true)),
        ("ENDS_WITH('naïve', 'ïve')", Value::Bool(true)),
    ] {
        assert_eq!(b(&db, expr), want, "{expr}");
    }
    // Aliases.
    assert_eq!(b(&db, "STARTSWITH('abc', 'a')"), Value::Bool(true));
    assert_eq!(b(&db, "ENDSWITH('abc', 'c')"), Value::Bool(true));
}

#[test]
fn affix_is_case_sensitive_unlike_like() {
    let (_d, db) = open();
    // The engine's LIKE is ASCII case-insensitive...
    assert_eq!(b(&db, "'ABC' LIKE 'abc%'"), Value::Bool(true));
    // ...these are not. This is the whole reason they exist: .NET's ordinal
    // StartsWith/EndsWith must not match across case.
    assert_eq!(b(&db, "STARTS_WITH('ABC', 'abc')"), Value::Bool(false));
    assert_eq!(b(&db, "STARTS_WITH('abc', 'ABC')"), Value::Bool(false));
    assert_eq!(b(&db, "ENDS_WITH('ABC', 'bc')"), Value::Bool(false));
    assert_eq!(b(&db, "ENDS_WITH('abc', 'BC')"), Value::Bool(false));
    assert_eq!(b(&db, "STARTS_WITH('abc', 'abc')"), Value::Bool(true));
}

#[test]
fn affix_treats_wildcards_as_literal_text() {
    let (_d, db) = open();
    // `%` and `_` are wildcards to LIKE...
    assert_eq!(b(&db, "'abc' LIKE 'a_c'"), Value::Bool(true));
    // ...but plain characters here, so a needle from user input is safe.
    assert_eq!(b(&db, "STARTS_WITH('abc', 'a_')"), Value::Bool(false));
    assert_eq!(b(&db, "STARTS_WITH('a_c', 'a_')"), Value::Bool(true));
    assert_eq!(b(&db, "ENDS_WITH('abc', '%c')"), Value::Bool(false));
    assert_eq!(b(&db, "ENDS_WITH('50%', '%')"), Value::Bool(true));
    assert_eq!(b(&db, "STARTS_WITH('100%', '100%')"), Value::Bool(true));
}

#[test]
fn affix_over_columns_matches_the_folded_form() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, s TEXT)").unwrap();
    for (id, s) in [
        (1, "'Customer 000007'"),
        (2, "'Customer 000017'"),
        (3, "'Customer 000021'"),
        (4, "'customer 000037'"), // lowercase: prefix must NOT match
        (5, "NULL"),
    ] {
        db.execute(&format!("INSERT INTO t VALUES ({id}, {s})"))
            .unwrap();
    }
    let ids = |sql: &str| -> Vec<i64> {
        rows(&db, sql)
            .iter()
            .map(|r| match r[0] {
                Value::Int(i) => i,
                ref v => panic!("not an int: {v:?}"),
            })
            .collect()
    };
    // Column arguments take the borrowed fast path.
    assert_eq!(
        ids("SELECT id FROM t WHERE STARTS_WITH(s, 'Customer 00') ORDER BY id"),
        [1, 2, 3]
    );
    assert_eq!(
        ids("SELECT id FROM t WHERE ENDS_WITH(s, '7') ORDER BY id"),
        [1, 2, 4]
    );
    // The exact shape the EF provider emits for
    // `StartsWith("Customer 00") && EndsWith("7")`.
    assert_eq!(
        ids(
            "SELECT id FROM t WHERE STARTS_WITH(s, 'Customer 00') AND ENDS_WITH(s, '7') ORDER BY id"
        ),
        [1, 2]
    );
    // A NULL column yields NULL, so the row is not kept (and NOT is not TRUE
    // either — a classic three-valued trap).
    assert_eq!(
        ids("SELECT id FROM t WHERE NOT STARTS_WITH(s, 'Customer 00') ORDER BY id"),
        [4]
    );
    // The borrowed path and the owned fallback must agree: same predicate over
    // a non-column (folded) expression.
    assert_eq!(
        ids(
            "SELECT id FROM t WHERE STARTS_WITH('Customer 00' || '0007', 'Customer 00') ORDER BY id"
        ),
        [1, 2, 3, 4, 5]
    );
}
