//! Fuzz the auto-detecting wire-protocol message decoder
//! (`oxidb_server::protocol::deserialize_message`).
//!
//! This is the primary entry point for incoming TCP requests — it
//! looks at the first byte (`{`, `[`, or `0xDB`) and dispatches to
//! JSON, MsgPack, or OxiWire parsers. Any panic on arbitrary input
//! is a real bug — TCP-facing code must NEVER panic on hostile bytes,
//! because the connection handler runs in a worker thread whose panic
//! would propagate or (worse) leave engine state inconsistent.

#![no_main]

use libfuzzer_sys::fuzz_target;
use oxidb_server::protocol;

fuzz_target!(|data: &[u8]| {
    // The contract: deserialize_message returns Result, never panics.
    // libfuzzer counts a panic as a finding; we deliberately swallow
    // the Result because both Ok and Err are valid outcomes — we're
    // only proving the panic-free property.
    let _ = protocol::deserialize_message(data);
});
