//! Tests for the EF-Core-motivated surface: date/time functions (NOW,
//! EXTRACT, DATE_TRUNC, INTERVAL arithmetic), aggregate DISTINCT,
//! math/string scalars + `%`, and CROSS JOIN.

mod common;

use common::*;
use oxidb_sql::Value;

// ── date/time ───────────────────────────────────────────────────────────────

#[test]
fn now_returns_a_current_timestamp() {
    let (_d, db) = open();
    for sql in ["SELECT NOW()", "SELECT CURRENT_TIMESTAMP"] {
        let v = rows(&db, sql)[0][0].clone();
        let Value::Timestamp(ms) = v else {
            panic!("{sql} returned {v:?}");
        };
        // Between 2026-01-01 and 2100-01-01.
        assert!((1_767_225_600_000..4_102_444_800_000).contains(&ms), "{ms}");
    }
}

#[test]
fn extract_parts_utc() {
    let (_d, db) = open();
    // 2026-07-13 is a Monday; DOY 194; ISO week 29.
    let q = |part: &str| -> Vec<Vec<Value>> {
        rows(
            &db,
            &format!("SELECT EXTRACT({part} FROM TIMESTAMP '2026-07-13 18:45:30.250')"),
        )
    };
    assert_eq!(q("YEAR"), vec![vec![i(2026)]]);
    assert_eq!(q("MONTH"), vec![vec![i(7)]]);
    assert_eq!(q("DAY"), vec![vec![i(13)]]);
    assert_eq!(q("HOUR"), vec![vec![i(18)]]);
    assert_eq!(q("MINUTE"), vec![vec![i(45)]]);
    assert_eq!(q("SECOND"), vec![vec![i(30)]]);
    // PostgreSQL MILLISECONDS includes the seconds field.
    assert_eq!(q("MILLISECONDS"), vec![vec![i(30_250)]]);
    assert_eq!(q("DOW"), vec![vec![i(1)]]); // Monday (Sunday = 0)
    assert_eq!(q("DOY"), vec![vec![i(194)]]);
    assert_eq!(q("WEEK"), vec![vec![i(29)]]);
    // EPOCH is fractional seconds, as DOUBLE.
    let Value::Double(epoch) = q("EPOCH")[0][0] else {
        panic!("EPOCH not a double");
    };
    assert_eq!(epoch, 1_783_968_330.25);
    // NULL propagates; extracting from a non-timestamp errors.
    assert_eq!(
        rows(&db, "SELECT EXTRACT(YEAR FROM CAST(NULL AS TIMESTAMP))"),
        vec![vec![Value::Null]]
    );
    assert!(db.execute("SELECT EXTRACT(YEAR FROM 5)").is_err());
}

#[test]
fn extract_dow_sunday_is_zero() {
    let (_d, db) = open();
    // 2026-07-12 is a Sunday.
    assert_eq!(
        rows(
            &db,
            "SELECT EXTRACT(DOW FROM TIMESTAMP '2026-07-12'), EXTRACT(DOW FROM TIMESTAMP '2026-07-18')"
        ),
        vec![vec![i(0), i(6)]] // Sunday, Saturday
    );
}

#[test]
fn date_trunc_boundaries() {
    let (_d, db) = open();
    let check = |unit: &str, want: &str| {
        assert_eq!(
            rows(
                &db,
                &format!(
                    "SELECT date_trunc('{unit}', TIMESTAMP '2026-07-13 18:45:30.250') \
                     = TIMESTAMP '{want}'"
                )
            ),
            vec![vec![b(true)]],
            "unit {unit}"
        );
    };
    check("second", "2026-07-13 18:45:30");
    check("minute", "2026-07-13 18:45:00");
    check("hour", "2026-07-13 18:00:00");
    check("day", "2026-07-13");
    check("week", "2026-07-13"); // already a Monday
    check("month", "2026-07-01");
    check("year", "2026-01-01");
    // A mid-week day truncates back to Monday.
    assert_eq!(
        rows(
            &db,
            "SELECT date_trunc('week', TIMESTAMP '2026-07-16') = TIMESTAMP '2026-07-13'"
        ),
        vec![vec![b(true)]]
    );
    // The unit must be a literal; unknown units are rejected.
    assert!(db.execute("SELECT date_trunc('fortnight', NOW())").is_err());
}

#[test]
fn interval_arithmetic() {
    let (_d, db) = open();
    let eq = |sql: &str| {
        assert_eq!(
            rows(&db, &format!("SELECT {sql}")),
            vec![vec![b(true)]],
            "{sql}"
        );
    };
    eq("TIMESTAMP '2026-07-13' + INTERVAL '1 day' = TIMESTAMP '2026-07-14'");
    eq("TIMESTAMP '2026-07-13' - INTERVAL '30 minutes' = TIMESTAMP '2026-07-12 23:30:00'");
    eq("TIMESTAMP '2026-07-13' + INTERVAL '1 day 2 hours' = TIMESTAMP '2026-07-14 02:00:00'");
    eq("TIMESTAMP '2026-07-13' + INTERVAL '1' HOUR = TIMESTAMP '2026-07-13 01:00:00'");
    // ts - ts is the difference in milliseconds.
    assert_eq!(
        rows(
            &db,
            "SELECT TIMESTAMP '2026-07-13 01:00:00' - TIMESTAMP '2026-07-13'"
        ),
        vec![vec![i(3_600_000)]]
    );
    // Calendar units have no fixed length.
    assert!(db.execute("SELECT NOW() + INTERVAL '1 month'").is_err());
    assert!(db.execute("SELECT NOW() + INTERVAL '1' YEAR").is_err());
    // Fractional-ms arithmetic (EF AddDays(double)) stays a timestamp.
    eq("TIMESTAMP '2026-07-13' + 0.5 * 86400000 = TIMESTAMP '2026-07-13 12:00:00'");
}

#[test]
fn date_part_function_form() {
    let (_d, db) = open();
    // The EF provider emits date_part()/date_trunc() function calls.
    assert_eq!(
        rows(
            &db,
            "SELECT date_part('year', TIMESTAMP '2026-07-13 18:45:30'), \
                    date_part('dow', TIMESTAMP '2026-07-13'), \
                    date_part('doy', TIMESTAMP '2026-07-13')"
        ),
        vec![vec![i(2026), i(1), i(194)]]
    );
    assert!(db.execute("SELECT date_part('century', NOW())").is_err());
}

#[test]
fn timestamp_arithmetic_on_columns() {
    let (_d, db) = open();
    db.execute("CREATE TABLE ev (id INT, at TIMESTAMP)")
        .unwrap();
    db.execute(
        "INSERT INTO ev VALUES (1, TIMESTAMP '2026-07-13 12:00:00'), \
         (2, TIMESTAMP '2026-07-10 12:00:00')",
    )
    .unwrap();
    // The EF-ish shape: WHERE at > <literal> - INTERVAL.
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM ev WHERE at > TIMESTAMP '2026-07-13 18:00:00' - INTERVAL '1 day'"
        ),
        vec![vec![i(1)]]
    );
    assert_eq!(
        rows(&db, "SELECT EXTRACT(DAY FROM at) FROM ev ORDER BY id"),
        vec![vec![i(13)], vec![i(10)]]
    );
}

// ── aggregate DISTINCT ──────────────────────────────────────────────────────

#[test]
fn aggregate_distinct() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (g INT, x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(1,2),(1,2),(1,3),(1,NULL),(2,5),(2,5)")
        .unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(x), COUNT(DISTINCT x), SUM(DISTINCT x), AVG(DISTINCT x) \
             FROM t WHERE g = 1"
        ),
        vec![vec![i(4), i(3), i(6), d(2.0)]]
    );
    // Per group.
    assert_eq!(
        rows(
            &db,
            "SELECT g, COUNT(DISTINCT x) FROM t GROUP BY g ORDER BY g"
        ),
        vec![vec![i(1), i(3)], vec![i(2), i(1)]]
    );
    // MIN/MAX(DISTINCT) parse and are a no-op.
    assert_eq!(
        rows(&db, "SELECT MIN(DISTINCT x), MAX(DISTINCT x) FROM t"),
        vec![vec![i(1), i(5)]]
    );
    // Rejected shapes.
    assert!(db.execute("SELECT COUNT(DISTINCT *) FROM t").is_err());
    assert!(db.execute("SELECT upper(DISTINCT 'a') FROM t").is_err());
    assert!(db.execute("SELECT SUM(DISTINCT x) OVER () FROM t").is_err());
}

// ── math / string scalars + % ───────────────────────────────────────────────

#[test]
fn modulo_operator_and_function() {
    let (_d, db) = open();
    assert_eq!(
        rows(&db, "SELECT 7 % 3, -7 % 3, mod(9, 4)"),
        vec![vec![i(1), i(-1), i(1)]]
    );
    let r = rows(&db, "SELECT CAST(7.5 AS DOUBLE) % 2");
    assert_eq!(r, vec![vec![d(1.5)]]);
    assert!(db.execute("SELECT 1 % 0").is_err());
}

#[test]
fn math_scalars() {
    let (_d, db) = open();
    assert_eq!(
        rows(
            &db,
            "SELECT floor(CAST(1.7 AS DOUBLE)), ceil(CAST(1.2 AS DOUBLE)), \
             floor(CAST(-1.5 AS DOUBLE)), ceiling(CAST(-1.5 AS DOUBLE))"
        ),
        vec![vec![d(1.0), d(2.0), d(-2.0), d(-1.0)]]
    );
    // Exact decimal floor/ceil; integers pass through.
    assert_eq!(
        rows(
            &db,
            "SELECT CAST(floor(2.5) AS INT), CAST(ceil(2.5) AS INT), floor(4)"
        ),
        vec![vec![i(2), i(3), i(4)]]
    );
    assert_eq!(
        rows(&db, "SELECT power(2, 10), sqrt(9)"),
        vec![vec![d(1024.0), d(3.0)]]
    );
    assert!(db.execute("SELECT sqrt(-1)").is_err());
}

#[test]
fn string_scalars() {
    let (_d, db) = open();
    assert_eq!(
        rows(
            &db,
            "SELECT strpos('hello', 'll'), POSITION('ll' IN 'hello'), strpos('hello', 'x')"
        ),
        vec![vec![i(3), i(3), i(0)]]
    );
    // Character-based, not byte-based.
    assert_eq!(rows(&db, "SELECT strpos('çilek', 'lek')"), vec![vec![i(3)]]);
    assert_eq!(
        rows(
            &db,
            "SELECT lpad('7', 3, '0'), rpad('ab', 4, '.'), lpad('hello', 3), lpad('x', 3)"
        ),
        vec![vec![t("007"), t("ab.."), t("hel"), t("  x")]]
    );
}

// ── LATERAL ─────────────────────────────────────────────────────────────────

fn seed_blogs(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE blogs (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE posts (id INT, blog_id INT, title TEXT, score INT)")
        .unwrap();
    db.execute("INSERT INTO blogs VALUES (1,'a'), (2,'b'), (3,'empty')")
        .unwrap();
    db.execute(
        "INSERT INTO posts VALUES \
         (10,1,'a1',5),(11,1,'a2',9),(12,1,'a3',7),(20,2,'b1',1)",
    )
    .unwrap();
}

#[test]
fn lateral_top_n_per_group() {
    let (_d, db) = open();
    seed_blogs(&db);
    // The EF collection-projection shape: top 2 posts by score per blog.
    assert_eq!(
        rows(
            &db,
            "SELECT b.id, p.title FROM blogs b \
             JOIN LATERAL (SELECT title FROM posts WHERE posts.blog_id = b.id \
                           ORDER BY score DESC LIMIT 2) p ON TRUE \
             ORDER BY b.id, p.title"
        ),
        vec![
            vec![i(1), t("a2")],
            vec![i(1), t("a3")],
            vec![i(2), t("b1")],
        ]
    );
}

#[test]
fn left_join_lateral_pads_nulls() {
    let (_d, db) = open();
    seed_blogs(&db);
    assert_eq!(
        rows(
            &db,
            "SELECT b.id, p.n FROM blogs b \
             LEFT JOIN LATERAL (SELECT COUNT(*) AS n FROM posts \
                                WHERE posts.blog_id = b.id AND score > 4) p ON TRUE \
             ORDER BY b.id"
        ),
        // The aggregate subquery always returns one row, so n is a count
        // (0 for the post-less blog), never a padded NULL here.
        vec![vec![i(1), i(3)], vec![i(2), i(0)], vec![i(3), i(0)]]
    );
    // A filtering (non-aggregate) body can come back empty → NULL padding.
    assert_eq!(
        rows(
            &db,
            "SELECT b.id, p.title FROM blogs b \
             LEFT JOIN LATERAL (SELECT title FROM posts \
                                WHERE posts.blog_id = b.id AND score >= 9) p ON TRUE \
             ORDER BY b.id"
        ),
        vec![
            vec![i(1), t("a2")],
            vec![i(2), Value::Null],
            vec![i(3), Value::Null],
        ]
    );
}

#[test]
fn cross_join_lateral_and_on_predicate() {
    let (_d, db) = open();
    seed_blogs(&db);
    // CROSS JOIN LATERAL == INNER ... ON TRUE.
    assert_eq!(
        rows(
            &db,
            "SELECT b.id, p.title FROM blogs b \
             CROSS JOIN LATERAL (SELECT title, score FROM posts \
                                 WHERE posts.blog_id = b.id) p \
             WHERE p.score > 6 ORDER BY p.title"
        ),
        vec![vec![i(1), t("a2")], vec![i(1), t("a3")]]
    );
    // A non-trivial ON filters the lateral rows per left row.
    assert_eq!(
        rows(
            &db,
            "SELECT b.id, p.score FROM blogs b \
             JOIN LATERAL (SELECT score FROM posts WHERE posts.blog_id = b.id) p \
               ON p.score > b.id * 4 \
             ORDER BY b.id, p.score"
        ),
        vec![vec![i(1), i(5)], vec![i(1), i(7)], vec![i(1), i(9)]]
    );
}

#[test]
fn lateral_without_correlation_and_rejections() {
    let (_d, db) = open();
    seed_blogs(&db);
    // No outer references — behaves like a plain derived table.
    assert_eq!(
        rows(
            &db,
            "SELECT b.id, x.v FROM blogs b \
             JOIN LATERAL (SELECT MAX(score) AS v FROM posts) x ON TRUE \
             ORDER BY b.id"
        ),
        vec![vec![i(1), i(9)], vec![i(2), i(9)], vec![i(3), i(9)]]
    );
    assert!(
        db.execute(
            "SELECT * FROM blogs b RIGHT JOIN LATERAL \
             (SELECT 1 WHERE b.id = 1) x ON TRUE"
        )
        .is_err()
    );
}

// ── CROSS JOIN ──────────────────────────────────────────────────────────────

#[test]
fn cross_join_cartesian_product() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (y INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (10),(20),(30)").unwrap();
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM a CROSS JOIN b"),
        vec![vec![i(6)]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT x, y FROM a CROSS JOIN b WHERE x = 2 AND y > 10 ORDER BY y"
        ),
        vec![vec![i(2), i(20)], vec![i(2), i(30)]]
    );
}

#[test]
fn add_months_calendar_math() {
    let (_d, db) = open();
    let eq = |sql: &str| {
        assert_eq!(
            rows(&db, &format!("SELECT {sql}")),
            vec![vec![b(true)]],
            "{sql}"
        );
    };
    eq("add_months(TIMESTAMP '2026-07-13 10:30:00', 1) = TIMESTAMP '2026-08-13 10:30:00'");
    // Day clamps to the target month's length.
    eq("add_months(TIMESTAMP '2026-01-31', 1) = TIMESTAMP '2026-02-28'");
    eq("add_months(TIMESTAMP '2024-01-31', 1) = TIMESTAMP '2024-02-29'"); // leap
    // Negative months and year rollover.
    eq("add_months(TIMESTAMP '2026-01-15', -2) = TIMESTAMP '2025-11-15'");
    // AddYears is n * 12.
    eq("add_months(TIMESTAMP '2024-02-29', 12) = TIMESTAMP '2025-02-28'");
    assert!(db.execute("SELECT add_months(NOW(), 'x')").is_err());
}

#[test]
fn ef_generated_shapes_smoke() {
    let (_d, db) = open();
    db.execute("CREATE TABLE m (id INT, kayit TIMESTAMP, ad TEXT, puan INT)")
        .unwrap();
    db.execute("INSERT INTO m VALUES (1, TIMESTAMP '2026-02-01', 'ayse', 25)")
        .unwrap();
    // EF quotes identifiers and parameterizes constants.
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM \"m\" AS \"x\" \
             WHERE date_part('year', \"x\".\"kayit\") = 2026 \
               AND date_part('month', \"x\".\"kayit\") = 2"
        ),
        vec![vec![i(1)]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM m WHERE kayit < NOW() + -1.0 * 86400000.0"
        ),
        vec![vec![i(1)]]
    );
}

#[test]
fn dml_table_alias() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (id INT, tag TEXT, score INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'rt',30),(2,'lang',90)")
        .unwrap();
    // EF ExecuteUpdate/ExecuteDelete shapes: UPDATE/DELETE with an alias.
    db.execute(
        "UPDATE \"t\" AS \"c\" SET \"score\" = \"c\".\"score\" + 1 WHERE \"c\".\"tag\" = 'rt'",
    )
    .unwrap();
    assert_eq!(
        rows(&db, "SELECT score FROM t WHERE id = 1"),
        vec![vec![i(31)]]
    );
    db.execute("DELETE FROM \"t\" AS \"c\" WHERE \"c\".\"tag\" = 'lang'")
        .unwrap();
    assert_eq!(rows(&db, "SELECT COUNT(*) FROM t"), vec![vec![i(1)]]);
}

#[test]
fn correlated_ref_inside_derived_table() {
    let (_d, db) = open();
    db.execute("CREATE TABLE b (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE p (bid INT, score INT)").unwrap();
    db.execute("INSERT INTO b VALUES (1,'x'),(2,'y')").unwrap();
    db.execute("INSERT INTO p VALUES (1,90),(1,70),(1,40),(2,80)")
        .unwrap();
    // The EF collection-projection shape: the outer ref sits inside a
    // LIMIT'd derived table inside a scalar subquery.
    assert_eq!(
        rows(
            &db,
            "SELECT (SELECT COUNT(*) FROM (SELECT 1 AS one FROM p \
                     WHERE b.id = p.bid ORDER BY p.score DESC LIMIT 2) AS q) \
             FROM b ORDER BY b.name"
        ),
        vec![vec![i(2)], vec![i(1)]]
    );
}

#[test]
fn like_is_ascii_case_insensitive() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Maria Anders'), ('ANA TRUJILLO'), ('bolido')")
        .unwrap();
    // SQLite/SQL-Server-style ASCII case-insensitivity (what EF expects).
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM t WHERE s LIKE '%an%'"),
        vec![vec![i(2)]]
    );
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM t WHERE s LIKE 'maria%'"),
        vec![vec![i(1)]]
    );
    // ESCAPE still works case-insensitively.
    db.execute("INSERT INTO t VALUES ('50% OFF')").unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM t WHERE s LIKE '%!% off' ESCAPE '!'"
        ),
        vec![vec![i(1)]]
    );
}

#[test]
fn collate_nocase_and_binary() {
    let (_d, db) = open();
    db.execute("CREATE TABLE t (s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Maria Anders'), ('maria anders'), ('Ana')")
        .unwrap();
    // NOCASE folds; equality becomes case-insensitive when both sides fold.
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM t WHERE s COLLATE \"NOCASE\" = 'maria anders'"
        ),
        vec![vec![i(2)]]
    );
    // BINARY is the identity (case-sensitive).
    assert_eq!(
        rows(
            &db,
            "SELECT COUNT(*) FROM t WHERE s COLLATE \"BINARY\" = 'maria anders'"
        ),
        vec![vec![i(1)]]
    );
    // Unknown collations are rejected.
    assert!(
        db.execute("SELECT COUNT(*) FROM t WHERE s COLLATE \"tr_TR\" = 'x'")
            .is_err()
    );
}

#[test]
fn multi_level_correlation() {
    let (_d, db) = open();
    db.execute("CREATE TABLE c (id INT, tag TEXT)").unwrap();
    db.execute("CREATE TABLE o (id INT, cid INT)").unwrap();
    db.execute("CREATE TABLE od (oid INT, tag TEXT)").unwrap();
    db.execute("INSERT INTO c VALUES (1,'a'), (2,'b')").unwrap();
    db.execute("INSERT INTO o VALUES (10,1),(11,1),(20,2)")
        .unwrap();
    db.execute("INSERT INTO od VALUES (10,'a'),(10,'a'),(11,'b'),(20,'b')")
        .unwrap();

    // Level-2 subquery referencing level-0 (`c.tag`) THROUGH level-1 (`o`):
    // count of order details whose tag matches the customer's tag, maxed
    // over the customer's orders.
    assert_eq!(
        rows(
            &db,
            "SELECT c.id, (SELECT MAX((SELECT COUNT(*) FROM od \
                                       WHERE od.oid = o.id AND od.tag = c.tag)) \
                          FROM o WHERE o.cid = c.id) \
             FROM c ORDER BY c.id"
        ),
        vec![vec![i(1), i(2)], vec![i(2), i(1)]]
    );

    // The EF aggregate-over-nested-subquery shape: outer ref inside a
    // derived table inside a scalar subquery inside an aggregate.
    assert_eq!(
        rows(
            &db,
            "SELECT MIN((SELECT SUM(n) FROM \
                          (SELECT COUNT(*) AS n FROM o WHERE o.cid = c.id) x)) \
             FROM c"
        ),
        vec![vec![i(1)]]
    );

    // Shadowing: the intervening scope's own `tag` column wins; only the
    // truly unresolvable ref correlates to level 0.
    assert_eq!(
        rows(
            &db,
            "SELECT c.id FROM c WHERE EXISTS \
               (SELECT 1 FROM o WHERE o.cid = c.id AND EXISTS \
                  (SELECT 1 FROM od WHERE od.oid = o.id AND od.tag = c.tag)) \
             ORDER BY c.id"
        ),
        vec![vec![i(1)], vec![i(2)]]
    );
}
