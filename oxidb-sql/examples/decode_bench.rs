//! How much of a disk-first scan is actually the row decoder?
//!
//! The query benchmark says a full scan of 400k rows takes ~17 ms where
//! PostgreSQL takes ~11 ms, and the obvious suspect is the per-cell decode: a
//! disk-first row is bytes in a mapping, so every scanned row is rebuilt into
//! `Value` cells. But "obvious suspect" is how this session has been wrong
//! twice, and the arithmetic that pointed here was a subtraction between two
//! queries that differ by *both* a cell and a predicate term.
//!
//! So this times the decoder alone, on the same row shape as the benchmark's
//! `orders` table, with nothing else in the loop. Compared against the whole
//! query's time it says what fraction is worth attacking.
//!
//! ```bash
//! cargo run --release -p oxidb-sql --example decode_bench
//! ```

use oxidb_sql::{Value, decode_row_into, decode_row_masked, encode_row};

const ROWS: usize = 400_000;

fn timed(label: &str, rows: &[Vec<u8>], ncols: usize, want: Option<&[bool]>) {
    let mut buf: Vec<Value> = Vec::new();
    // Warm the branch predictor and the caches the same way for every variant.
    for r in rows.iter().take(1000) {
        match want {
            Some(w) => decode_row_masked(r, ncols, w, &mut buf).unwrap(),
            None => decode_row_into(r, ncols, &mut buf).unwrap(),
        }
    }
    let t = std::time::Instant::now();
    let mut sink = 0i64;
    for r in rows {
        match want {
            Some(w) => decode_row_masked(r, ncols, w, &mut buf).unwrap(),
            None => decode_row_into(r, ncols, &mut buf).unwrap(),
        }
        // Touch a cell so the decode cannot be optimized away.
        if let Some(Value::Int(n)) = buf.first() {
            sink = sink.wrapping_add(*n);
        }
    }
    let ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "  {label:<44} {ms:>7.2} ms   {:>5.1} ns/row   (checksum {sink})",
        ms * 1e6 / ROWS as f64
    );
}

fn main() {
    // The `orders` row from bench/pg-memory/schema.sql:
    // id INT, customer_id INT, status TEXT, total DOUBLE, created TIMESTAMP.
    let rows: Vec<Vec<u8>> = (0..ROWS)
        .map(|i| {
            encode_row(&[
                Value::Int(i as i64),
                Value::Int((i % 200_000) as i64),
                Value::Text(["pending", "paid", "shipped", "delivered", "refunded"][i % 5].into()),
                Value::Double(i as f64 * 1.5),
                Value::Timestamp(1_704_067_200_000 + i as i64 * 3),
            ])
        })
        .collect();
    let bytes: usize = rows.iter().map(|r| r.len()).sum();
    println!(
        "{ROWS} rows of the benchmark's `orders` shape, {:.1} MB encoded\n",
        bytes as f64 / 1e6
    );

    timed("full decode (5 cells)", &rows, 5, None);
    // What the projection-aware decode does for `sum(total) WHERE id > n`.
    timed(
        "masked: id + total (skips text + 2)",
        &rows,
        5,
        Some(&[true, false, false, true, false]),
    );
    // What a text GROUP BY needs: the text cell plus one integer.
    timed(
        "masked: id + status (text kept)",
        &rows,
        5,
        Some(&[true, false, true, false, false]),
    );
    timed(
        "masked: id only (one integer)",
        &rows,
        5,
        Some(&[true, false, false, false, false]),
    );
    // Is a text cell's ~21 ns the allocation, or the UTF-8 validation, or the
    // copy? Only the first needs a borrowed cell type to fix, so it matters
    // which. Time each part on its own.
    let words: Vec<&str> = vec!["pending", "paid", "shipped", "delivered", "refunded"];
    let t = std::time::Instant::now();
    let mut n = 0usize;
    for i in 0..ROWS {
        // `black_box` on both sides: without it LLVM proves the allocation
        // unobservable and deletes it, which reported 0.5 ns/row — faster than
        // any allocator, and the giveaway that nothing was being measured.
        let b: Box<str> = std::hint::black_box(words[i % 5]).into();
        n += std::hint::black_box(&b).len();
    }
    println!(
        "\n  {:<44} {:>7.2} ms   {:>5.1} ns/row   (len {n})",
        "Box<str> alloc+copy+free alone",
        t.elapsed().as_secs_f64() * 1e3,
        t.elapsed().as_secs_f64() * 1e9 / ROWS as f64
    );
    let t = std::time::Instant::now();
    let mut ok = 0usize;
    for i in 0..ROWS {
        // UTF-8 validation alone, no allocation.
        ok += std::str::from_utf8(words[i % 5].as_bytes()).unwrap().len();
    }
    println!(
        "  {:<44} {:>7.2} ms   {:>5.1} ns/row   (len {ok})",
        "UTF-8 validation alone (no alloc)",
        t.elapsed().as_secs_f64() * 1e3,
        t.elapsed().as_secs_f64() * 1e9 / ROWS as f64
    );

    println!(
        "\nCompare against the whole query: a 400k-row scan measures ~17 ms\n\
         (`SELECT count(*) FROM orders WHERE total > 0`, disk-first). Whatever\n\
         the decoder does not account for is elsewhere — predicate evaluation,\n\
         the per-row visitor call, the aggregate fold."
    );
}
