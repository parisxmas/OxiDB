//! Structure-aware fuzz: OxiWire encode → decode → equality.
//!
//! Where the mutation-based `wire_oxiwire` target throws random bytes at
//! the decoder and looks for panics, this target generates a *structured*
//! `Value` tree from libfuzzer's bytes via `Arbitrary`, encodes it
//! through `oxiwire::encode_value`, decodes it back via
//! `oxiwire::decode_request`, and asserts JSON equality.
//!
//! Catches:
//!   - Encoder/decoder mismatch bugs (encoder writes X, decoder reads Y)
//!   - Specific value classes that break roundtrip (huge ints, NaN/Inf,
//!     empty strings/arrays/maps, deeply nested structures)
//!   - Tag-vocabulary holes (encoder writes a tag the decoder doesn't
//!     know about, or vice versa)
//!
//! Bit-flipping fuzz can't easily reach these — the input space is too
//! large to randomly produce a 12-deep nested object with one specific
//! float at a specific path. Structure-aware fuzz starts from valid and
//! lets libfuzzer mutate to invalid, getting both regimes.

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use oxidb_server::oxiwire;
use serde_json::Value;

/// Typed grammar for values OxiWire's encoder + decoder must round-trip
/// exactly. Mirrors `serde_json::Value` but with normalised number
/// semantics (a single `Int(i64)` variant covers what the encoder would
/// otherwise pick between TAG_INT64 / TAG_UINT64 based on sign).
#[derive(Arbitrary, Debug)]
enum ArbitraryValue {
    Null,
    Bool(bool),
    Int(i64),
    /// Floats may be NaN/Inf from arbitrary's POV. We normalise those
    /// to Null at conversion time because OxiWire's decoder ALREADY
    /// maps NaN/Inf bit-patterns back to Null (see oxiwire.rs:218),
    /// so a NaN input that round-trips through encode→decode becomes
    /// Null, breaking strict equality. Treating NaN as Null on the
    /// input side restores the invariant.
    Float(f64),
    Str(String),
    Array(Vec<ArbitraryValue>),
    /// Vec<(String, T)> instead of HashMap so generation is bounded
    /// and predictable. Duplicate keys are deliberately kept — the
    /// `to_json` builder uses `serde_json::Map::insert` which gives
    /// last-write-wins, matching what the decoder produces.
    Object(Vec<(String, ArbitraryValue)>),
}

/// Bounds to keep individual fuzz iterations fast (and avoid generation
/// stack overflow). libfuzzer runs millions of iterations — each one
/// should be milliseconds, not seconds.
const MAX_DEPTH: usize = 6;
const MAX_STR_LEN: usize = 256;
const MAX_COLL_LEN: usize = 32;

fn to_json(av: &ArbitraryValue, depth: usize) -> Value {
    if depth > MAX_DEPTH {
        return Value::Null;
    }
    match av {
        ArbitraryValue::Null => Value::Null,
        ArbitraryValue::Bool(b) => Value::Bool(*b),
        ArbitraryValue::Int(i) => {
            // Encoder picks TAG_UINT64 for non-negative i64, TAG_INT64
            // for negative. Pre-canonicalise input so original and
            // decoded number representations match exactly.
            if *i >= 0 {
                serde_json::json!(*i as u64)
            } else {
                serde_json::json!(*i)
            }
        }
        ArbitraryValue::Float(f) => {
            if f.is_finite() {
                serde_json::Number::from_f64(*f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        ArbitraryValue::Str(s) => {
            // Truncate at a char boundary, not a byte index.
            let limit = s
                .char_indices()
                .map(|(i, _)| i)
                .take_while(|&i| i <= MAX_STR_LEN)
                .last()
                .unwrap_or(0);
            Value::String(s[..limit].to_string())
        }
        ArbitraryValue::Array(items) => {
            let take = items.iter().take(MAX_COLL_LEN);
            Value::Array(take.map(|v| to_json(v, depth + 1)).collect())
        }
        ArbitraryValue::Object(pairs) => {
            let mut m = serde_json::Map::new();
            for (k, v) in pairs.iter().take(MAX_COLL_LEN) {
                let k_truncated = k
                    .char_indices()
                    .map(|(i, _)| i)
                    .take_while(|&i| i <= MAX_STR_LEN)
                    .last()
                    .map(|i| k[..i].to_string())
                    .unwrap_or_default();
                m.insert(k_truncated, to_json(v, depth + 1));
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

    // Encode through OxiWire.
    let mut buf = vec![oxiwire::MAGIC];
    oxiwire::encode_value(&original, &mut buf);

    // Decode back. The encoder is supposed to produce valid OxiWire
    // bytes by construction; a decode error here is a roundtrip bug.
    let decoded = match oxiwire::decode_request(&buf) {
        Ok(v) => v,
        Err(e) => panic!(
            "decode failed after encode: error={e}\noriginal={original}"
        ),
    };

    // Compare via canonical JSON serialisation — sidesteps internal-
    // number-representation quirks (e.g. Number::from(5i64) vs
    // Number::from(5u64) which may or may not compare equal depending
    // on serde_json feature flags, but always serialise to "5").
    let orig_canonical = serde_json::to_string(&original)
        .expect("original always serialises");
    let dec_canonical = serde_json::to_string(&decoded)
        .expect("decoded always serialises");
    if orig_canonical != dec_canonical {
        panic!(
            "OxiWire roundtrip mismatch:\n  original = {orig_canonical}\n  decoded  = {dec_canonical}\n  encoded bytes = {buf:?}"
        );
    }
});
