//! Hard-join benchmark for oxidb-sql, mirroring the PostgreSQL comparison.
//!
//! Seeds a deterministic e-commerce dataset (identical row values to the
//! Postgres `generate_series` seed) and times four multi-way join queries.
//! Prints each query's result (for a cross-engine parity check) and the best
//! wall-clock time over several runs.
//!
//! Run: `cargo run --release --example join_bench -p oxidb-sql`

use std::time::Instant;

use oxidb_sql::{QueryResult, SqlEngine, Value};

const R: usize = 10; // regions
const S: usize = 10; // suppliers
const C: usize = 1000; // customers
const P: usize = 300; // products
const O: usize = 5000; // orders
const I: usize = 15000; // items
const RUNS: usize = 5;

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqlEngine::open(dir.path()).unwrap();
    seed(&db);

    let queries: &[(&str, &str)] = &[
        (
            "Q1 5-way INNER  revenue/region",
            "SELECT r.name, SUM(it.qty * p.price) AS rev \
             FROM regions r \
             JOIN customers c ON c.region_id = r.id \
             JOIN orders o    ON o.customer_id = c.id \
             JOIN items it    ON it.order_id = o.id \
             JOIN products p  ON p.id = it.product_id \
             GROUP BY r.name ORDER BY r.name",
        ),
        (
            "Q2 6-way INNER  revenue/supplier",
            "SELECT s.name, SUM(it.qty * p.price) AS rev \
             FROM regions r \
             JOIN customers c ON c.region_id = r.id \
             JOIN orders o    ON o.customer_id = c.id \
             JOIN items it    ON it.order_id = o.id \
             JOIN products p  ON p.id = it.product_id \
             JOIN suppliers s ON s.id = p.supplier_id \
             GROUP BY s.name ORDER BY s.name",
        ),
        (
            "Q3 LEFT chain   orders/region",
            "SELECT r.name, COUNT(o.id) AS n \
             FROM regions r \
             LEFT JOIN customers c ON c.region_id = r.id \
             LEFT JOIN orders o    ON o.customer_id = c.id \
             GROUP BY r.name ORDER BY r.name",
        ),
        (
            "Q4 FULL join    row count",
            "SELECT COUNT(*) AS n FROM customers c FULL JOIN orders o ON c.id = o.customer_id",
        ),
    ];

    println!("oxidb-sql join benchmark — R={R} S={S} C={C} P={P} O={O} I={I}, best of {RUNS}\n");
    for (label, sql) in queries {
        // Warm up + correctness capture.
        let first = db.execute(sql).unwrap().pop().unwrap();
        // Time.
        let mut best = f64::MAX;
        for _ in 0..RUNS {
            let t = Instant::now();
            let _ = db.execute(sql).unwrap();
            best = best.min(t.elapsed().as_secs_f64() * 1000.0);
        }
        println!("{label:<34}  {best:>9.2} ms");
        print_result(&first);
        println!();
    }
}

fn print_result(r: &QueryResult) {
    if let QueryResult::Select { rows, .. } = r {
        for row in rows {
            let cells: Vec<String> = row.iter().map(fmt_value).collect();
            println!("    {}", cells.join("|"));
        }
    }
}

fn fmt_value(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Int(n) => n.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Timestamp(t) => t.to_string(),
    }
}

fn seed(db: &SqlEngine) {
    db.execute("CREATE TABLE regions   (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE suppliers (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, region_id INT)")
        .unwrap();
    db.execute("CREATE TABLE products  (id INT PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL, supplier_id INT)").unwrap();
    db.execute("CREATE TABLE orders    (id INT PRIMARY KEY, customer_id INT NOT NULL, total INT)")
        .unwrap();
    db.execute(
        "CREATE TABLE items     (id INT PRIMARY KEY, order_id INT, product_id INT, qty INT)",
    )
    .unwrap();

    // One multi-row INSERT per table (a single WAL fsync each).
    let mut vals = String::new();
    for i in 0..R {
        push_tuple(&mut vals, i, format_args!("({}, 'R{}')", i + 1, i + 1));
    }
    exec_insert(db, "regions", &vals);

    vals.clear();
    for i in 0..S {
        push_tuple(&mut vals, i, format_args!("({}, 'S{}')", i + 1, i + 1));
    }
    exec_insert(db, "suppliers", &vals);

    vals.clear();
    for i in 0..C {
        let region = if i % 7 == 0 {
            "NULL".to_string()
        } else {
            ((i % R) + 1).to_string()
        };
        push_tuple(
            &mut vals,
            i,
            format_args!("({}, 'c{}', {})", i + 1, i + 1, region),
        );
    }
    exec_insert(db, "customers", &vals);

    vals.clear();
    for i in 0..P {
        let supplier = if i % 5 == 0 {
            "NULL".to_string()
        } else {
            ((i % S) + 1).to_string()
        };
        push_tuple(
            &mut vals,
            i,
            format_args!("({}, 'p{}', {}, {})", i + 1, i + 1, (i % 9) + 1, supplier),
        );
    }
    exec_insert(db, "products", &vals);

    vals.clear();
    for i in 0..O {
        let cust = if i % 11 == 0 {
            1_000_000 + i
        } else {
            (i % C) + 1
        };
        push_tuple(
            &mut vals,
            i,
            format_args!("({}, {}, {})", i + 1, cust, i % 100),
        );
    }
    exec_insert(db, "orders", &vals);

    vals.clear();
    for i in 0..I {
        push_tuple(
            &mut vals,
            i,
            format_args!(
                "({}, {}, {}, {})",
                i + 1,
                (i % O) + 1,
                (i % P) + 1,
                (i % 5) + 1
            ),
        );
    }
    exec_insert(db, "items", &vals);
}

fn push_tuple(buf: &mut String, i: usize, args: std::fmt::Arguments) {
    use std::fmt::Write;
    if i > 0 {
        buf.push(',');
    }
    write!(buf, "{args}").unwrap();
}

fn exec_insert(db: &SqlEngine, table: &str, values: &str) {
    db.execute(&format!("INSERT INTO {table} VALUES {values}"))
        .unwrap();
}
