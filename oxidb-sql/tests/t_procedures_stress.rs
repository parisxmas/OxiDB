//! Stress test: a 1000+ line, join- and math-heavy stored procedure
//! (`tests/data/complex_procedure.sql`) — created, called, and verified
//! against independently computed invariants, plus a differential run
//! (CALL vs the same body executed inline in a transaction).

mod common;

use common::*;
use oxidb_sql::{SqlEngine, Value};

const PROC_SQL: &str = include_str!("data/complex_procedure.sql");

fn seed(db: &SqlEngine) {
    db.execute(
        "CREATE TABLE musteri (id INT PRIMARY KEY AUTO_INCREMENT, ad TEXT,
                               segment TEXT, puan INT, bakiye DOUBLE);
         CREATE TABLE urun (id INT PRIMARY KEY AUTO_INCREMENT, ad TEXT,
                            kategori TEXT, fiyat DOUBLE, maliyet DOUBLE, stok INT);
         CREATE TABLE siparis (id INT PRIMARY KEY AUTO_INCREMENT, musteri_id INT,
                               urun_id INT, adet INT, brut DOUBLE, net DOUBLE, durum TEXT);
         CREATE TABLE denetim (id INT PRIMARY KEY AUTO_INCREMENT, donem INT,
                               olay TEXT, deger DOUBLE);
         CREATE TABLE rapor (id INT PRIMARY KEY AUTO_INCREMENT, donem INT, kategori TEXT,
                             ciro DOUBLE, kar DOUBLE, siparis_sayisi INT)",
    )
    .unwrap();

    // Deterministic seed: 40 customers, 60 products over the procedure's 20
    // categories, 400 orders (deterministic pseudo-random via multiplication
    // mod primes).
    for i in 0..40 {
        let seg = ["vip", "yeni", "bronz"][i % 3];
        db.execute_params(
            "INSERT INTO musteri (ad, segment, puan, bakiye) VALUES ($1, $2, $3, $4)",
            &[
                Value::Text(format!("musteri{i:02}").into()),
                Value::Text(seg.to_string().into()),
                Value::Int((i as i64 * 37) % 500),
                Value::Double(((i as i64 * 131) % 9000) as f64),
            ],
        )
        .unwrap();
    }
    for i in 0..60 {
        let kat = format!("k{:02}", (i % 20) + 1);
        let fiyat = 10.0 + ((i as i64 * 53) % 400) as f64;
        db.execute_params(
            "INSERT INTO urun (ad, kategori, fiyat, maliyet, stok) VALUES ($1, $2, $3, $4, $5)",
            &[
                Value::Text(format!("urun{i:02}").into()),
                Value::Text(kat.into()),
                Value::Double(fiyat),
                Value::Double(fiyat * 0.6),
                Value::Int(100_000),
            ],
        )
        .unwrap();
    }
    for i in 0..400i64 {
        let adet = (i * 7) % 95 + 1;
        let brut = ((i * 197) % 15_000 + 50) as f64;
        db.execute_params(
            "INSERT INTO siparis (musteri_id, urun_id, adet, brut, net, durum)
             VALUES ($1, $2, $3, $4, 0, 'acik')",
            &[
                Value::Int(i % 40 + 1),
                Value::Int(i % 60 + 1),
                Value::Int(adet),
                Value::Double(brut),
            ],
        )
        .unwrap();
    }
}

fn f64_of(v: &Value) -> f64 {
    match v {
        Value::Double(d) => *d,
        Value::Int(i) => *i as f64,
        other => panic!("expected number, got {other:?}"),
    }
}

const ARGS: &str = "7, 0.20, 1.5, 100, 29.9, 0.05";

#[test]
fn thousand_line_procedure_runs_and_balances() {
    assert!(
        PROC_SQL.lines().count() >= 1000,
        "the stress procedure must stay 1000+ lines, got {}",
        PROC_SQL.lines().count()
    );

    let (_d, db) = open();
    seed(&db);
    db.execute(PROC_SQL).unwrap();

    // Pre-state aggregates the invariants below are checked against.
    let acik = f64_of(&rows(&db, "SELECT COUNT(*) FROM siparis WHERE durum = 'acik'")[0][0]);
    let stok_once = f64_of(&rows(&db, "SELECT SUM(stok) FROM urun")[0][0]);
    let adet_once = f64_of(&rows(&db, "SELECT SUM(adet) FROM siparis WHERE durum = 'acik'")[0][0]);
    let bakiye_once = f64_of(&rows(&db, "SELECT SUM(bakiye) FROM musteri")[0][0]);

    let summary = rows(&db, &format!("CALL donem_kapanisi({ARGS})"));

    // The CALL's result is section M: per-segment rows + a TOPLAM row.
    assert!(summary.len() >= 2, "summary: {summary:?}");
    let toplam = summary
        .iter()
        .find(|r| r[0] == Value::Text("TOPLAM".into()))
        .expect("TOPLAM row");
    assert_eq!(f64_of(&toplam[1]), acik, "all open orders settled");
    let toplam_ciro = f64_of(&toplam[2]);
    let toplam_kar = f64_of(&toplam[3]);
    assert!(toplam_ciro > 0.0 && toplam_kar < toplam_ciro);
    // Segment rows sum to the total (grand-total consistency).
    let seg_ciro: f64 = summary
        .iter()
        .filter(|r| r[0] != Value::Text("TOPLAM".into()))
        .map(|r| f64_of(&r[2]))
        .sum();
    assert!(
        (seg_ciro - toplam_ciro).abs() < 1e-6,
        "segment cirolari {seg_ciro} != toplam {toplam_ciro}"
    );

    // Invariants against the pre-state.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM siparis WHERE durum = 'acik'"),
        vec![vec![Value::Int(0)]]
    );
    let stok_sonra = f64_of(&rows(&db, "SELECT SUM(stok) FROM urun")[0][0]);
    assert!(
        (stok_once - stok_sonra - adet_once).abs() < 1e-6,
        "stock deducted exactly by units sold"
    );
    // Customers were charged the settled revenue (penalties only push the
    // total further down).
    let bakiye_sonra = f64_of(&rows(&db, "SELECT SUM(bakiye) FROM musteri")[0][0]);
    assert!(
        bakiye_once - bakiye_sonra >= toplam_ciro - 1e-6,
        "balances dropped by at least the settled revenue"
    );
    // Audit trail: 10 opening + 20 per-category + 4 closing rows.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM denetim WHERE donem = 7"),
        vec![vec![Value::Int(34)]]
    );
    // One report row per category, revenue matching the total.
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM rapor WHERE donem = 7"),
        vec![vec![Value::Int(20)]]
    );
    let rapor_ciro = f64_of(&rows(&db, "SELECT SUM(ciro) FROM rapor WHERE donem = 7")[0][0]);
    assert!(
        (rapor_ciro - toplam_ciro).abs() < 1e-6,
        "rapor {rapor_ciro} != toplam {toplam_ciro}"
    );
}

#[test]
fn call_matches_inline_body_execution() {
    // Differential: CALL on engine A must leave the exact same state as the
    // stored (rewritten) body executed inline in a transaction on engine B.
    let (_da, a) = open();
    let (_db_, b) = open();
    seed(&a);
    seed(&b);
    a.execute(PROC_SQL).unwrap();
    b.execute(PROC_SQL).unwrap();

    let args = &[
        Value::Int(7),
        Value::Double(0.20),
        Value::Double(1.5),
        Value::Double(100.0),
        Value::Double(29.9),
        Value::Double(0.05),
    ];
    a.execute_params("CALL donem_kapanisi($1, $2, $3, $4, $5, $6)", args)
        .unwrap();

    let body = b.procedure_def("donem_kapanisi").unwrap().body;
    b.execute_params(&format!("BEGIN; {body}; COMMIT"), args)
        .unwrap();

    for dump in [
        "SELECT id, ad, segment, puan, bakiye FROM musteri ORDER BY id",
        "SELECT id, kategori, fiyat, maliyet, stok FROM urun ORDER BY id",
        "SELECT id, musteri_id, urun_id, adet, brut, net, durum FROM siparis ORDER BY id",
        "SELECT donem, olay, deger FROM denetim ORDER BY id",
        "SELECT donem, kategori, ciro, kar, siparis_sayisi FROM rapor ORDER BY id",
    ] {
        assert_eq!(rows(&a, dump), rows(&b, dump), "diverged: {dump}");
    }
}

#[test]
fn stress_procedure_is_atomic_on_failure() {
    // Re-create the procedure with a poisoned tail: everything the 1000-line
    // body did must roll back when the appended statement fails.
    let (_d, db) = open();
    seed(&db);
    let poisoned = PROC_SQL.trim_end().trim_end_matches("END").to_string()
        + "  INSERT INTO denetim (id, donem, olay, deger) VALUES (1, 0, 'dup-pk', 0);\nEND";
    db.execute(&poisoned).unwrap();
    // A pre-existing denetim row for the PK collision.
    db.execute("INSERT INTO denetim (donem, olay, deger) VALUES (0, 'ilk', 0)")
        .unwrap();

    let once = rows(&db, "SELECT SUM(bakiye) FROM musteri");
    assert!(db.execute(&format!("CALL donem_kapanisi({ARGS})")).is_err());
    assert_eq!(rows(&db, "SELECT SUM(bakiye) FROM musteri"), once);
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM siparis WHERE durum = 'acik'"),
        vec![vec![Value::Int(400)]]
    );
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM rapor"),
        vec![vec![Value::Int(0)]]
    );
}
