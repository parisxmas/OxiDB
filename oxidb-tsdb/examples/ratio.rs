use oxidb_tsdb::{Point, Tsdb};
fn main() {
    let base = 1_700_000_000_000i64;
    // Case A: realistic rounded gauge (holds for stretches)
    let mut a = Tsdb::new();
    for i in 0..1_000_000 {
        let v = ((60.0 + (i as f64 / 5000.0).sin() * 15.0) * 2.0).round() / 2.0;
        a.write(
            &Point::new("cpu", base + i * 1000)
                .tag("host", "h1")
                .field("usage", v),
        );
    }
    // Case B: financial-ish tick price (many small moves)
    let mut b = Tsdb::new();
    let mut px = 30000.0f64;
    for i in 0..1_000_000 {
        px += ((i as f64 * 0.7).sin() * 0.5).round() / 100.0;
        b.write(
            &Point::new("btc", base + i * 250)
                .tag("sym", "BTCUSDT")
                .field("price", (px * 100.0).round() / 100.0),
        );
    }
    for (name, db) in [("gauge (1M pts)", &a), ("tick price (1M pts)", &b)] {
        let raw = db.point_count() * 16;
        let comp = db.compressed_bytes();
        println!(
            "{name:22}  raw {:>5} MB  →  compressed {:>5.1} MB   ({:.1}x, {:.2} bytes/pt)",
            raw / 1_048_576,
            comp as f64 / 1_048_576.0,
            raw as f64 / comp as f64,
            comp as f64 / db.point_count() as f64
        );
    }
}
