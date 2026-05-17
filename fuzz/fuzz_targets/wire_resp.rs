//! Fuzz the RESP (Redis-compatible) parser (`oxidb_server::resp::read_value`).
//!
//! RESP underpins OxiMem — the Redis-wire-compatible in-memory KV
//! layer. It's the parser that real `redis-cli`, `ioredis`, `lettuce`
//! etc. all talk to. RESP's bulk-string framing and array nesting
//! are classic fuzz fodder:
//!   - bulk-string length lies (claim 1GB, send 0 bytes)
//!   - integer parse on non-digits
//!   - CRLF / LF / CR confusion at line boundaries
//!   - deeply-nested arrays consuming stack

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::{BufReader, Cursor};

use oxidb_server::resp;

fuzz_target!(|data: &[u8]| {
    let mut reader = BufReader::new(Cursor::new(data));
    let _ = resp::read_value(&mut reader);
});
