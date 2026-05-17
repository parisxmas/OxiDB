//! Fuzz the PostgreSQL wire-protocol frontend-message decoder
//! (`oxidb_server::pg_wire::codec::read_message`).
//!
//! pg_wire is what real Postgres clients (libpq, asyncpg, pgx,
//! Hibernate, etc.) talk. The decoder reads a 1-byte tag + i32 length
//! + payload, then dispatches by tag to Query / Parse / Bind /
//! Execute / etc. Classic fuzz surface:
//!   - i32 length < 4 or > buffer (overflow / underflow arithmetic)
//!   - tag bytes that look valid but payload doesn't match
//!   - Bind messages with parameter counts that don't match payload
//!   - UTF-8 boundaries on string columns

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

use oxidb_server::pg_wire::codec;

fuzz_target!(|data: &[u8]| {
    // pg_wire has two entry points — startup and regular messages.
    // First byte of the corpus picks which one we exercise; this lets
    // libfuzzer drive both code paths from the same target.
    if data.is_empty() {
        return;
    }
    let mut cursor = Cursor::new(&data[1..]);
    if data[0] & 1 == 0 {
        let _ = codec::read_startup(&mut cursor);
    } else {
        let _ = codec::read_message(&mut cursor);
    }
});
