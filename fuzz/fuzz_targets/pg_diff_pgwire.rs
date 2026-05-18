//! Differential fuzz: OxiDB's pg_wire frontend-message decoder vs
//! the `pgwire` crate's `PgWireFrontendMessage::decode`.
//!
//! Mirror of `resp_diff_redis` / `msgpack_roundtrip` — feed identical
//! raw bytes to TWO independently-written decoders. If BOTH accept,
//! assert they produced the same VARIANT KIND (Query/Parse/Bind/etc).
//! Payload details aren't compared (the two decoders surface different
//! detail in their parsed structs); variant-kind divergence is the
//! strongest signal: one parser thinks the bytes are a Query, the
//! other thinks they're a Bind. In a mixed-client production
//! environment that's "two clients talking to the same Postgres-wire
//! port see fundamentally different commands" — a real bug.
//!
//! Scoped to **tagged frontend messages** (the post-startup phase,
//! where the first byte is one of `Q P B D E S H C X` per the
//! Postgres frontend protocol). Three deliberate exclusions:
//!
//!   1. Untagged inputs (startup / SSL / CancelRequest) — different
//!      framing, different entry points in both decoders
//!   2. Specifically the `S` byte — overlaps with the StartupMessage
//!      magic prefix; surfacing the divergence here would drown the
//!      tagged-message signal
//!   3. Incomplete messages (pgwire returns `Ok(None)` when buf has
//!      less than the length prefix promises; we skip those — they're
//!      "need more bytes" not "valid msg"). OxiDB would similarly
//!      block on the read; we treat both as "not a finding"
//!
//! What we DO flag (panic → libfuzzer minimises + saves the input):
//!
//!   - Both decoders return a complete message, the variants don't
//!     match — that's the variant-confusion bug class.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use std::io::Cursor;
use std::sync::Once;

use oxidb_server::pg_wire::codec::{self, FrontendMessage};
use pgwire::messages::{DecodeContext, PgWireFrontendMessage};

/// Install (once, on the first fuzz iteration) a panic hook that
/// SILENCES panics originating inside pgwire or its bytes-crate
/// dependency. pgwire 0.40 has multiple panic-on-truncated-message
/// classes (Execute / Bind / Describe decoders all advance past
/// available buffer on minimal inputs); since we're using pgwire
/// as a REFERENCE not the target, its panics should not abort our
/// fuzz run. OxiDB panics still propagate through libfuzzer's
/// original hook and abort — they're real findings.
///
/// `catch_unwind` then catches the (now silent) pgwire unwind and
/// turns it into a non-finding (treat as "pgwire rejected").
fn install_pgwire_panic_filter() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let original = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(loc) = info.location() {
                let file = loc.file();
                if file.contains("pgwire") || file.contains("bytes-1.") {
                    return; // swallow — known reference-impl bug class
                }
            }
            original(info);
        }));
    });
}

/// Map an OxiDB `FrontendMessage` to a short string naming its kind.
/// Payload details intentionally not included — variant-kind match
/// is the comparable property across the two decoders.
fn oxi_kind(m: &FrontendMessage) -> &'static str {
    match m {
        FrontendMessage::SslRequest => "ssl_request",
        FrontendMessage::Startup(_) => "startup",
        FrontendMessage::Query(_) => "query",
        FrontendMessage::Parse { .. } => "parse",
        FrontendMessage::Bind { .. } => "bind",
        FrontendMessage::Describe { .. } => "describe",
        FrontendMessage::Execute { .. } => "execute",
        FrontendMessage::Sync => "sync",
        FrontendMessage::Flush => "flush",
        FrontendMessage::Close { .. } => "close",
        FrontendMessage::Terminate => "terminate",
    }
}

/// Same for pgwire. Variants not relevant to the tagged-message
/// scope (Startup, SslNegotiation, CancelRequest, etc.) map to
/// kinds that won't match OxiDB's tagged enum — guaranteeing the
/// diff-target sees a mismatch which the scope filter then drops.
fn pgw_kind(m: &PgWireFrontendMessage) -> &'static str {
    match m {
        PgWireFrontendMessage::SslNegotiation(_) => "ssl_request",
        PgWireFrontendMessage::Startup(_) => "startup",
        PgWireFrontendMessage::CancelRequest(_) => "startup", // close-ish
        PgWireFrontendMessage::PasswordMessageFamily(_) => "password",
        PgWireFrontendMessage::Query(_) => "query",
        PgWireFrontendMessage::Parse(_) => "parse",
        PgWireFrontendMessage::Close(_) => "close",
        PgWireFrontendMessage::Bind(_) => "bind",
        PgWireFrontendMessage::Describe(_) => "describe",
        PgWireFrontendMessage::Execute(_) => "execute",
        PgWireFrontendMessage::PortalSuspended(_) => "portal_suspended",
        PgWireFrontendMessage::Flush(_) => "flush",
        PgWireFrontendMessage::Sync(_) => "sync",
        PgWireFrontendMessage::Terminate(_) => "terminate",
        PgWireFrontendMessage::CopyData(_) => "copy_data",
        PgWireFrontendMessage::CopyFail(_) => "copy_fail",
        PgWireFrontendMessage::CopyDone(_) => "copy_done",
    }
}

/// Tagged frontend-message first bytes per the Postgres wire
/// protocol that we ATTEMPT to diff. Three deliberate exclusions:
///
/// - `S` (Sync) — collides with StartupMessage magic prefix in
///   pgwire's awaiting-startup state. Different state machine, not
///   a roundtrip bug.
/// - `B` (Bind) — pgwire 0.40's Bind decoder has multiple
///   stdlib-`raw_vec`-capacity-overflow panic paths on
///   under-specified inputs. Those panic at file paths INSIDE
///   stdlib (not pgwire or bytes), so the panic-location filter
///   can't catch them. Excluding Bind keeps the differential
///   stable; the mutation `wire_pg` target still fuzzes the
///   OxiDB Bind decoder.
/// - `E` (Execute) — same shape as Bind, hard-coded payload
///   minimums in pgwire's decoder.
const TAGGED_FIRST_BYTES: &[u8] = b"QPDHCX";

fuzz_target!(|data: &[u8]| {
    install_pgwire_panic_filter();

    // Scope: tagged frontend messages only. See module doc for the
    // rationale on the exclusions.
    let Some(&first) = data.first() else { return; };
    if !TAGGED_FIRST_BYTES.contains(&first) {
        return;
    }

    // Filter pathological length-prefix values. pgwire alloc-bombs
    // on huge claimed-lengths (Vec::with_capacity → capacity-
    // overflow panic in stdlib raw_vec, not pgwire's own code,
    // so the panic-hook file filter doesn't catch it). OxiDB has
    // a bounded MAX_PG_MESSAGE_LEN of 16 MiB (PR #47) — anything
    // larger is rejected as InvalidData. Skip lengths past 16 MiB
    // here: both decoders fail differently on those, neither
    // failure is meaningful for the differential.
    if data.len() < 5 {
        return; // not enough for even tag + length field
    }
    let claimed_len = i32::from_be_bytes([data[1], data[2], data[3], data[4]]);
    const MAX_PLAUSIBLE: i32 = 16 * 1024 * 1024;
    // Length must be in [4, 16 MiB] per spec (4 = just-the-length-
    // field-itself, the protocol minimum). Anything smaller or
    // negative would have pgwire alloc-bomb on payload reads.
    if !(4..=MAX_PLAUSIBLE).contains(&claimed_len) {
        return;
    }
    // Buffer must actually contain the bytes the length claims.
    let needed = 1usize + claimed_len as usize;
    if data.len() < needed {
        return;
    }

    // OxiDB side — read_message expects the tag byte + i32 length +
    // payload directly. Cursor wraps the whole buffer.
    let oxi_result = {
        let mut cursor = Cursor::new(data);
        codec::read_message(&mut cursor)
    };

    // pgwire side — needs a BytesMut buffer + a DecodeContext that
    // says "we're past startup, expect a tagged message".
    // DecodeContext is `#[non_exhaustive]` so we can't construct it
    // with a literal; default() + field mutation is the supported
    // path.
    //
    // pgwire panics inside its own decoder paths are filtered out
    // by the panic hook installed above + caught here via
    // `catch_unwind`. OxiDB panics propagate normally.
    let pgw_result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut buf = BytesMut::from(data);
            let mut ctx = DecodeContext::default();
            ctx.awaiting_frontend_ssl = false;
            ctx.awaiting_frontend_startup = false;
            PgWireFrontendMessage::decode(&mut buf, &ctx)
        }));

    match (oxi_result, pgw_result) {
        // pgwire panicked (known truncated-message bug class). Skip.
        (_, Err(_)) => {}
        (Ok(o), Ok(Ok(Some(p)))) => {
            let ok = oxi_kind(&o);
            let pk = pgw_kind(&p);
            if ok != pk {
                // OxiDB's FrontendMessage doesn't impl Debug, so we
                // can only show the kind names + raw bytes — enough
                // to reproduce.
                panic!(
                    "PG_WIRE DECODER VARIANT DIVERGENCE — both parsers \
                     accepted, variants disagree:\n  \
                     oxidb_kind  = {ok}\n  \
                     pgwire_kind = {pk} ({p:?})\n  \
                     bytes       = {data:?}\n  \
                     (data-corruption-class: a real mixed-client \
                     Postgres-wire production environment would see \
                     two clients interpret the same bytes as different \
                     commands)"
                );
            }
        }
        // pgwire's Ok(None) = "need more bytes". OxiDB's read_exact
        // would error on EOF. Treat as "not a complete message".
        (_, Ok(Ok(None))) => {}
        // Both reject: agreed-invalid, no signal.
        (Err(_), Ok(Err(_))) => {}
        // Accept/reject divergence — same nuance as resp_diff_redis:
        // one parser is more permissive on edge cases. Not
        // necessarily a bug; the mutation `wire_pg` target covers
        // "no panic on malformed input" separately.
        (Ok(_), Ok(Err(_))) | (Err(_), Ok(Ok(Some(_)))) => {}
    }
});
