//! Joins: INNER, LEFT, RIGHT, FULL OUTER, self-join, 3-way, join + aggregate.

mod common;
use common::*;

fn seed(db: &oxidb_sql::SqlEngine) {
    db.execute("CREATE TABLE c (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE o (id INT, cust INT, amt INT)")
        .unwrap();
    // cy(3) has no orders; order 12 (cust 9) has no matching customer.
    db.execute("INSERT INTO c VALUES (1,'ada'),(2,'bob'),(3,'cy')")
        .unwrap();
    db.execute("INSERT INTO o VALUES (10,1,100),(11,2,200),(12,9,50)")
        .unwrap();
}

#[test]
fn inner_join_matches_only() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, o.amt FROM c JOIN o ON c.id = o.cust ORDER BY o.amt",
    );
    assert_eq!(rws, vec![vec![t("ada"), i(100)], vec![t("bob"), i(200)]]);
}

#[test]
fn left_join_keeps_unmatched_left_with_nulls() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, o.amt FROM c LEFT JOIN o ON c.id = o.cust ORDER BY c.name",
    );
    // ada, bob matched; cy padded with NULL.
    assert_eq!(
        rws,
        vec![
            vec![t("ada"), i(100)],
            vec![t("bob"), i(200)],
            vec![t("cy"), NULL],
        ]
    );
}

#[test]
fn left_join_where_on_right_null_finds_unmatched() {
    let (_d, db) = open();
    seed(&db);
    // Customers with no orders.
    let rws = rows(
        &db,
        "SELECT c.name FROM c LEFT JOIN o ON c.id = o.cust WHERE o.id IS NULL",
    );
    assert_eq!(rws, r1(vec![t("cy")]));
}

#[test]
fn right_join_keeps_unmatched_right_with_nulls() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name, o.id FROM c RIGHT JOIN o ON c.id = o.cust WHERE o.id = 12",
    );
    // Order 12 has no customer -> c.name NULL.
    assert_eq!(rws, r1(vec![NULL, i(12)]));
}

#[test]
fn full_join_keeps_both_sides() {
    let (_d, db) = open();
    seed(&db);
    let n = rows(&db, "SELECT c.id, o.id FROM c FULL JOIN o ON c.id = o.cust").len();
    // 2 matched + cy (left-only) + order12 (right-only) = 4 rows.
    assert_eq!(n, 4);
}

#[test]
fn self_join() {
    let (_d, db) = open();
    db.execute("CREATE TABLE n (id INT)").unwrap();
    db.execute("INSERT INTO n VALUES (1),(2),(3)").unwrap();
    let rws = rows(
        &db,
        "SELECT x.id, y.id FROM n x JOIN n y ON x.id < y.id ORDER BY x.id, y.id",
    );
    assert_eq!(
        rws,
        vec![vec![i(1), i(2)], vec![i(1), i(3)], vec![i(2), i(3)],]
    );
}

#[test]
fn three_table_join() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (id INT, x INT)").unwrap();
    db.execute("CREATE TABLE b (id INT, y INT)").unwrap();
    db.execute("CREATE TABLE d (id INT, z INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO b VALUES (1, 20)").unwrap();
    db.execute("INSERT INTO d VALUES (1, 30)").unwrap();
    let rws = rows(
        &db,
        "SELECT a.x, b.y, d.z FROM a JOIN b ON a.id = b.id JOIN d ON b.id = d.id",
    );
    assert_eq!(rws, r1(vec![i(10), i(20), i(30)]));
}

#[test]
fn join_with_group_by_and_count_ignores_null() {
    let (_d, db) = open();
    seed(&db);
    // LEFT JOIN so cy appears with COUNT(o.id) = 0.
    let rws = rows(
        &db,
        "SELECT c.name, COUNT(o.id) AS n FROM c LEFT JOIN o ON c.id = o.cust \
         GROUP BY c.name ORDER BY c.name",
    );
    assert_eq!(
        rws,
        vec![
            vec![t("ada"), i(1)],
            vec![t("bob"), i(1)],
            vec![t("cy"), i(0)],
        ]
    );
}

#[test]
fn join_with_where_filter() {
    let (_d, db) = open();
    seed(&db);
    let rws = rows(
        &db,
        "SELECT c.name FROM c JOIN o ON c.id = o.cust WHERE o.amt >= 200",
    );
    assert_eq!(rws, r1(vec![t("bob")]));
}

#[test]
fn cross_join_is_a_cartesian_product() {
    let (_d, db) = open();
    seed(&db);
    // CROSS JOIN = INNER ... ON TRUE; comma joins are still rejected.
    let n = match rows(&db, "SELECT COUNT(*) AS n FROM c CROSS JOIN o")[0][0] {
        oxidb_sql::Value::Int(n) => n,
        ref other => panic!("count returned {other:?}"),
    };
    let c = match rows(&db, "SELECT COUNT(*) AS n FROM c")[0][0] {
        oxidb_sql::Value::Int(n) => n,
        ref other => panic!("count returned {other:?}"),
    };
    let o = match rows(&db, "SELECT COUNT(*) AS n FROM o")[0][0] {
        oxidb_sql::Value::Int(n) => n,
        ref other => panic!("count returned {other:?}"),
    };
    assert_eq!(n, c * o);
    assert!(db.execute("SELECT * FROM c, o").is_err());
}

/// Composite equi-join key (`a.k1 = b.k1 AND a.k2 = b.k2`) — exercises the
/// multi-column hash-join key.
#[test]
fn composite_key_hash_join() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (k1 INT, k2 INT, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b (k1 INT, k2 INT, w INT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1,1,10),(1,2,20),(2,1,30)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (1,1,100),(2,1,300),(1,2,200),(9,9,999)")
        .unwrap();
    let rws = rows(
        &db,
        "SELECT a.v, b.w FROM a JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2 ORDER BY a.v",
    );
    assert_eq!(
        rws,
        vec![
            vec![i(10), i(100)],
            vec![i(20), i(200)],
            vec![i(30), i(300)]
        ]
    );
}

/// Equi-join with a residual (non-equi) conjunct — the hash join must re-check
/// the full ON so the extra `b.w > 150` filters candidates correctly.
#[test]
fn equi_join_with_residual_predicate() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (k INT, v INT)").unwrap();
    db.execute("CREATE TABLE b (k INT, w INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10),(2,20)").unwrap();
    db.execute("INSERT INTO b VALUES (1,100),(1,200),(2,50)")
        .unwrap();
    let rws = rows(
        &db,
        "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k AND b.w > 150 ORDER BY a.v",
    );
    assert_eq!(rws, r1(vec![i(10), i(200)]));
}

/// Left join whose key is NULL on some left rows: NULL never equi-matches, so
/// those rows are padded (LEFT semantics), not dropped.
#[test]
fn hash_join_null_keys_do_not_match() {
    let (_d, db) = open();
    db.execute("CREATE TABLE a (id INT, k INT)").unwrap();
    db.execute("CREATE TABLE b (k INT, w INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10),(2,NULL),(3,10)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (10,99)").unwrap();
    let rws = rows(
        &db,
        "SELECT a.id, b.w FROM a LEFT JOIN b ON a.k = b.k ORDER BY a.id",
    );
    // id 1 and 3 match (k=10); id 2 has NULL key -> padded.
    assert_eq!(
        rws,
        vec![vec![i(1), i(99)], vec![i(2), NULL], vec![i(3), i(99)]]
    );
}

/// WHERE push-down into join scans must be invisible — especially around
/// outer joins, where pushing a predicate into a NULL-paddable side would
/// manufacture padded rows (`IS NULL` anti-joins are the classic casualty).
#[test]
fn join_filter_pushdown_semantics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE pa (id INT PRIMARY KEY, city TEXT)")
        .unwrap();
    db.execute("CREATE TABLE pb (id INT PRIMARY KEY, pa_id INT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO pa VALUES (1,'x'), (2,'y'), (3,'x')")
        .unwrap();
    db.execute("INSERT INTO pb VALUES (10,1,5), (11,1,50), (12,2,50)")
        .unwrap();

    // INNER join, conjuncts on both tables (both pushed).
    assert_eq!(
        rows(
            &db,
            "SELECT pa.id, pb.id FROM pa JOIN pb ON pa.id = pb.pa_id \
                   WHERE pa.city = 'x' AND pb.amt > 10 ORDER BY pb.id"
        ),
        vec![vec![i(1), i(11)]]
    );

    // LEFT JOIN anti-join: WHERE on the nullable side must NOT be pushed —
    // pa=3 has no pb rows and must survive via the padded row.
    assert_eq!(
        rows(
            &db,
            "SELECT pa.id FROM pa LEFT JOIN pb ON pa.id = pb.pa_id \
                   WHERE pb.id IS NULL"
        ),
        vec![vec![i(3)]]
    );

    // LEFT JOIN with a FROM-side conjunct (pushable) plus a nullable-side
    // predicate in the same WHERE.
    assert_eq!(
        rows(
            &db,
            "SELECT pa.id, pb.id FROM pa LEFT JOIN pb ON pa.id = pb.pa_id \
                   WHERE pa.city = 'x' AND pb.amt = 50"
        ),
        vec![vec![i(1), i(11)]]
    );

    // RIGHT JOIN: FROM side is paddable — its conjuncts must not be pushed.
    assert_eq!(
        rows(
            &db,
            "SELECT pb.id FROM pa RIGHT JOIN pb ON pa.id = pb.pa_id AND pa.city = 'zzz' \
                   WHERE pa.id IS NULL ORDER BY pb.id"
        ),
        vec![vec![i(10)], vec![i(11)], vec![i(12)]]
    );
}

/// Correlated EXISTS with a single outer-equality decorrelates into a
/// materialized semi-join set; semantics must stay exact — including NULL
/// keys, NOT EXISTS, and value (projection) context.
#[test]
fn exists_decorrelation_semantics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE ca (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE ob (id INT PRIMARY KEY, ca_id INT, amt INT)")
        .unwrap();
    db.execute("INSERT INTO ca VALUES (1,'a'), (2,'b'), (3,'c')")
        .unwrap();
    // ca=1 has a big order; ca=2 only small; a NULL ca_id row exists.
    db.execute("INSERT INTO ob VALUES (10,1,900), (11,2,5), (12,NULL,900)")
        .unwrap();

    assert_eq!(
        rows(
            &db,
            "SELECT ca.id FROM ca WHERE EXISTS \
                   (SELECT 1 FROM ob WHERE ob.ca_id = ca.id AND ob.amt > 100)"
        ),
        vec![vec![i(1)]]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT ca.id FROM ca WHERE NOT EXISTS \
                   (SELECT 1 FROM ob WHERE ob.ca_id = ca.id AND ob.amt > 100) ORDER BY ca.id"
        ),
        vec![vec![i(2)], vec![i(3)]]
    );
    // Projection context: the rewritten form must yield booleans, not NULLs.
    assert_eq!(
        rows(
            &db,
            "SELECT ca.id, EXISTS (SELECT 1 FROM ob WHERE ob.ca_id = ca.id) AS h \
                   FROM ca ORDER BY ca.id"
        ),
        vec![
            vec![i(1), oxidb_sql::Value::Bool(true)],
            vec![i(2), oxidb_sql::Value::Bool(true)],
            vec![i(3), oxidb_sql::Value::Bool(false)],
        ]
    );
    // A row-slicing EXISTS (EF's ElementAtOrDefault renders OFFSET inside
    // EXISTS): the slice applies PER OUTER ROW — must not decorrelate.
    // ca=1 has two matching orders (skip 1 leaves one); ca=2 has one (none left).
    db.execute("INSERT INTO ob VALUES (13,1,50)").unwrap();
    assert_eq!(
        rows(
            &db,
            "SELECT ca.id FROM ca WHERE EXISTS \
                   (SELECT 1 FROM ob WHERE ob.ca_id = ca.id OFFSET 1)"
        ),
        vec![vec![i(1)]]
    );

    // Correlation used twice (not a single-eq shape): must stay correlated
    // and still be correct.
    assert_eq!(
        rows(
            &db,
            "SELECT ca.id FROM ca WHERE EXISTS \
                   (SELECT 1 FROM ob WHERE ob.ca_id = ca.id AND ob.id > ca.id)"
        ),
        vec![vec![i(1)], vec![i(2)]]
    );
}

/// Tekil eşitlik korelasyonlu skaler-aggregate alt sorgular, satır başına
/// yeniden çalışmak yerine bir kez GROUP BY'a decorrelate edilir. Semantik
/// birebir korunmalı: eksik anahtar için COUNT 0 / SUM NULL, NULL dış değer,
/// residual filtre ve COALESCE-varsayılanı dahil.
#[test]
fn scalar_aggregate_decorrelation_semantics() {
    let (_d, db) = open();
    db.execute("CREATE TABLE mus (id INT PRIMARY KEY, ad TEXT)")
        .unwrap();
    db.execute("CREATE TABLE sip (id INT PRIMARY KEY, mid INT, tutar INT, durum INT)")
        .unwrap();
    // mus 3'ün hiç siparişi yok; mus 4 NULL (ilişkisiz).
    db.execute("INSERT INTO mus VALUES (1,'a'), (2,'b'), (3,'c')")
        .unwrap();
    db.execute("INSERT INTO sip VALUES (10,1,100,1), (11,1,50,0), (12,2,900,1)")
        .unwrap();

    // COUNT: eksik anahtar (mus 3) -> 0, NULL değil.
    assert_eq!(
        rows(
            &db,
            "SELECT id, (SELECT COUNT(*) FROM sip WHERE sip.mid = mus.id) \
                   FROM mus ORDER BY id"
        ),
        vec![vec![i(1), i(2)], vec![i(2), i(1)], vec![i(3), i(0)]]
    );
    // SUM: eksik anahtar -> NULL.
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM mus WHERE \
                   (SELECT SUM(tutar) FROM sip WHERE sip.mid = mus.id) IS NULL"
        ),
        vec![vec![i(3)]]
    );
    // Residual filtre (durum = 1) korelasyon eşitliğinin yanında: mus 1 için
    // yalnızca 100'lük sipariş sayılır.
    assert_eq!(
        rows(
            &db,
            "SELECT id, (SELECT COALESCE(SUM(tutar),0) FROM sip \
                   WHERE sip.mid = mus.id AND durum = 1) FROM mus ORDER BY id"
        ),
        vec![vec![i(1), i(100)], vec![i(2), i(900)], vec![i(3), i(0)]]
    );
    // MAX: eksik -> NULL; var olan doğru değer.
    assert_eq!(
        rows(
            &db,
            "SELECT id, (SELECT MAX(tutar) FROM sip WHERE sip.mid = mus.id) \
                   FROM mus WHERE id <= 2 ORDER BY id"
        ),
        vec![vec![i(1), i(100)], vec![i(2), i(900)]]
    );
    // WHERE'de kullanım: siparişi 2'den az olan müşteriler (decorrelate + filtre).
    assert_eq!(
        rows(
            &db,
            "SELECT id FROM mus WHERE \
                   (SELECT COUNT(*) FROM sip WHERE sip.mid = mus.id) < 2 ORDER BY id"
        ),
        vec![vec![i(2)], vec![i(3)]]
    );
}

/// `FROM big JOIN small` ile `FROM small JOIN big` aynı sonucu vermeli:
/// planlayıcı en küçük tabloyu sürücü seçer (choose_driver), sonuç
/// yazım sırasından bağımsız olmalı — filtreler, çoklu join ve
/// projeksiyon dahil.
#[test]
fn driver_choice_is_result_preserving() {
    let (_d, db) = open();
    db.execute("CREATE TABLE kucuk (id INT PRIMARY KEY, kat INT)")
        .unwrap();
    db.execute("CREATE TABLE buyuk (id INT PRIMARY KEY, kid INT, deger INT)")
        .unwrap();
    db.execute("CREATE INDEX b_kid ON buyuk (kid)").unwrap();
    db.execute("INSERT INTO kucuk VALUES (1,7), (2,9), (3,7)")
        .unwrap();
    let vals: Vec<String> = (1..=60)
        .map(|i| format!("({i}, {}, {})", i % 3 + 1, i))
        .collect();
    db.execute(&format!("INSERT INTO buyuk VALUES {}", vals.join(", ")))
        .unwrap();

    // İki tablolu join, küçük tarafta filtre — iki yazım da aynı toplamı verir.
    let a = rows(
        &db,
        "SELECT SUM(b.deger) FROM buyuk b JOIN kucuk k ON b.kid = k.id WHERE k.kat = 7",
    );
    let b = rows(
        &db,
        "SELECT SUM(b.deger) FROM kucuk k JOIN buyuk b ON b.kid = k.id WHERE k.kat = 7",
    );
    assert_eq!(a, b);
    assert_eq!(
        a,
        rows(&db, "SELECT SUM(deger) FROM buyuk WHERE kid IN (1, 3)")
    );

    // GROUP BY + üç yol (üçüncü tablo) sonuç sırası korunur.
    db.execute("CREATE TABLE etiket (id INT PRIMARY KEY, ad TEXT)")
        .unwrap();
    db.execute("INSERT INTO etiket VALUES (1,'a'), (2,'b'), (3,'c')")
        .unwrap();
    let g = rows(
        &db,
        "SELECT k.kat, COUNT(*) FROM buyuk b JOIN kucuk k ON b.kid = k.id \
         JOIN etiket e ON k.id = e.id WHERE k.kat = 7 GROUP BY k.kat",
    );
    assert_eq!(g, vec![vec![i(7), i(40)]]);
}
