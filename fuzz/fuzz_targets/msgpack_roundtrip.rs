//! Structure-aware fuzz: MsgPack encode → decode → JSON-canonical equality.
//!
//! Mirror of `oxiwire_roundtrip.rs` for the MsgPack format used by
//! the OxiWire dispatcher's "else → MsgPack" branch. Encoder is the
//! hand-rolled `protocol::value_to_msgpack` (which exists because
//! `rmp_serde::to_vec(&Value)` breaks under `serde_json`'s
//! `arbitrary_precision` feature — see protocol.rs line 124).
//! Decoder is `rmp_serde::from_slice::<Value>`.
//!
//! Catches:
//!   - Encoder writes a MsgPack tag the decoder doesn't recognise
//!     (or vice versa)
//!   - Integer-width selection mismatch (sint vs uint vs fixint)
//!   - Float precision loss
//!   - Map / array length mis-encoding at length boundaries
//!     (e.g. 15 ↔ 16 transition between fixmap and map16)
//!   - String length-prefix off-by-one on UTF-8 multi-byte boundaries

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

use oxidb_server::protocol;

#[derive(Arbitrary, Debug)]
enum ArbitraryValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<ArbitraryValue>),
    Object(Vec<(String, ArbitraryValue)>),
}

const MAX_DEPTH: usize = 6;
const MAX_STR_LEN: usize = 256;
const MAX_COLL_LEN: usize = 32;

fn truncate_at_char_boundary(s: &str, max_bytes: usize) -> String {
    let mut last = 0;
    for (i, c) in s.char_indices() {
        if i + c.len_utf8() > max_bytes {
            break;
        }
        last = i + c.len_utf8();
    }
    s[..last].to_string()
}

fn to_json(av: &ArbitraryValue, depth: usize) -> Value {
    if depth > MAX_DEPTH {
        return Value::Null;
    }
    match av {
        ArbitraryValue::Null => Value::Null,
        ArbitraryValue::Bool(b) => Value::Bool(*b),
        ArbitraryValue::Int(i) => {
            // `value_to_msgpack` picks `write_sint` for any i64 with
            // `n.as_i64()` Some, regardless of sign. So we don't need
            // to pre-normalise the sign here (unlike OxiWire which
            // splits positive into UINT64).
            serde_json::json!(*i)
        }
        ArbitraryValue::Float(f) => {
            if f.is_finite() {
                serde_json::Number::from_f64(*f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            } else {
                // MsgPack CAN encode NaN/Inf bit-for-bit, but
                // `serde_json::Number::from_f64` refuses them on
                // construction — so the decoded `Value::Number` can
                // never represent NaN/Inf. Roundtrip equality demands
                // they be Null on both sides.
                Value::Null
            }
        }
        ArbitraryValue::Str(s) => {
            Value::String(truncate_at_char_boundary(s, MAX_STR_LEN))
        }
        ArbitraryValue::Array(items) => {
            let take = items.iter().take(MAX_COLL_LEN);
            Value::Array(take.map(|v| to_json(v, depth + 1)).collect())
        }
        ArbitraryValue::Object(pairs) => {
            let mut m = serde_json::Map::new();
            for (k, v) in pairs.iter().take(MAX_COLL_LEN) {
                let k_trunc = truncate_at_char_boundary(k, MAX_STR_LEN);
                m.insert(k_trunc, to_json(v, depth + 1));
            }
            Value::Object(m)
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(av) = ArbitraryValue::arbitrary(&mut u) else {
        return;
    };
    let original = to_json(&av, 0);

    // Encode via OxiDB's hand-rolled MsgPack encoder.
    let mut buf = Vec::new();
    protocol::value_to_msgpack(&original, &mut buf);

    // Decode via rmp_serde — the canonical Rust MsgPack reader.
    // A failure here is a roundtrip bug.
    let decoded: Value = match rmp_serde::from_slice(&buf) {
        Ok(v) => v,
        Err(e) => panic!(
            "MsgPack decode failed after encode: error={e}\n  original = {original}\n  bytes len = {}",
            buf.len()
        ),
    };

    // Canonical JSON equality — sidesteps serde_json's internal
    // number-representation quirks (Number::from(5i64) vs
    // Number::from(5u64) which compare unequal in some configs but
    // both serialise to "5").
    let orig_canonical = serde_json::to_string(&original)
        .expect("original always serialises");
    let dec_canonical = serde_json::to_string(&decoded)
        .expect("decoded always serialises");
    if orig_canonical != dec_canonical {
        panic!(
            "MsgPack roundtrip mismatch:\n  original = {orig_canonical}\n  decoded  = {dec_canonical}\n  bytes    = {buf:?}"
        );
    }
});
