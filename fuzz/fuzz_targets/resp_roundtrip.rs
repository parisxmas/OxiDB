//! Structure-aware fuzz: RESP encode → decode → equality.
//!
//! Mirror of `oxiwire_roundtrip.rs` for the RESP (Redis-compatible)
//! wire format. Generates a typed `RespValue` tree via `Arbitrary`,
//! encodes through `resp::write_value`, decodes via `resp::read_value`,
//! and asserts the re-encoded bytes match exactly.
//!
//! Catches:
//!   - SimpleString / Error containing embedded CR or LF (the line-
//!     based framing breaks if either appears in the payload — the
//!     fuzz target normalises CR/LF out at input so legitimate
//!     roundtrip equality is achievable; if/when the encoder grows
//!     escape support, the normalisation comes out)
//!   - Empty/zero-length arrays vs Null
//!   - Bulk strings with non-UTF-8 bytes
//!   - Deeply nested arrays
//!   - Integer edge cases (i64::MIN / MAX)
//!
//! Bytes-equality (re-encode) is the cleanest comparison because
//! `RespValue` doesn't derive `PartialEq` — and two values that encode
//! to the same bytes are semantically the same on the wire by
//! definition, which is exactly what we care about.

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

use oxidb_server::resp::{self, RespValue};

#[derive(Arbitrary, Debug)]
enum ArbitraryResp {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<ArbitraryResp>),
}

const MAX_DEPTH: usize = 6;
const MAX_BYTES: usize = 256;
const MAX_COLL_LEN: usize = 32;

/// RESP simple-string / error lines are CRLF-terminated. An embedded
/// CR or LF in the payload would let the decoder split the line at
/// the wrong place, breaking roundtrip equality. Substitute spaces
/// so the input is *encodable*; we're testing the encoder/decoder
/// pair's consistency, not its handling of out-of-protocol bytes
/// (the mutation-based `wire_resp` target already covers the
/// "garbage in, no panic out" property).
fn normalise_line(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
}

fn truncate_str(s: &str, max_bytes: usize) -> String {
    let mut len = 0;
    let mut last = 0;
    for (i, c) in s.char_indices() {
        if len + c.len_utf8() > max_bytes {
            break;
        }
        len += c.len_utf8();
        last = i + c.len_utf8();
    }
    s[..last].to_string()
}

fn to_resp(av: &ArbitraryResp, depth: usize) -> RespValue {
    if depth > MAX_DEPTH {
        return RespValue::Null;
    }
    match av {
        ArbitraryResp::Simple(s) => {
            let t = truncate_str(s, MAX_BYTES);
            RespValue::SimpleString(normalise_line(&t))
        }
        ArbitraryResp::Error(s) => {
            let t = truncate_str(s, MAX_BYTES);
            RespValue::Error(normalise_line(&t))
        }
        ArbitraryResp::Integer(n) => RespValue::Integer(*n),
        ArbitraryResp::Bulk(b) => {
            // BulkString is length-prefixed framing — CR/LF inside is
            // totally fine (the decoder reads exactly N bytes before
            // expecting the trailing \r\n).
            let take: Vec<u8> = b.iter().take(MAX_BYTES).copied().collect();
            RespValue::BulkString(take)
        }
        ArbitraryResp::Null => RespValue::Null,
        ArbitraryResp::Array(items) => {
            let take = items.iter().take(MAX_COLL_LEN);
            RespValue::Array(take.map(|v| to_resp(v, depth + 1)).collect())
        }
    }
}

fn encode(value: &RespValue) -> Vec<u8> {
    let mut buf = Vec::new();
    resp::write_value(&mut buf, value).expect("encoder should not fail on valid RespValue");
    buf
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(av) = ArbitraryResp::arbitrary(&mut u) else {
        return;
    };
    let original = to_resp(&av, 0);

    let encoded = encode(&original);

    let mut reader = Cursor::new(&encoded);
    let decoded = match resp::read_value(&mut reader) {
        Ok(v) => v,
        Err(e) => panic!(
            "decode failed after encode: error={e}\n  original = {original:?}\n  encoded bytes = {encoded:?}"
        ),
    };

    // Canonical comparison via re-encoding. If both encode to the same
    // bytes, the values are semantically identical on the wire — that's
    // the property RESP roundtrip is supposed to preserve.
    let re_encoded = encode(&decoded);
    if encoded != re_encoded {
        panic!(
            "RESP roundtrip mismatch:\n  original = {:?}\n  encoded         = {:?}\n  decoded re-enc  = {:?}",
            original, encoded, re_encoded
        );
    }
});
