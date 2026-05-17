//! Fuzz the OxiWire binary-format decoder (`oxidb_server::oxiwire::decode_request`).
//!
//! OxiWire is OxiDB's compact MsgPack-derived wire format. Decoder
//! hand-rolls bytes-to-Value walking, including recursive maps/arrays
//! and variable-length integers — the exact code shape where
//! hand-written parsers historically panic on:
//!   - oversized length fields (alloc bombs)
//!   - recursion past stack limit (nested arrays/maps)
//!   - off-by-one on truncated payloads
//!   - integer overflow in length arithmetic

#![no_main]

use libfuzzer_sys::fuzz_target;
use oxidb_server::oxiwire;

fuzz_target!(|data: &[u8]| {
    let _ = oxiwire::decode_request(data);
});
