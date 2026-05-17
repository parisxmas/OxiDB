#!/bin/bash -eu
# Fuzz coverage runner — produces line / region / function coverage
# reports for one or all fuzz targets in this crate.
#
# Closes the "coverage reporting" item from ADR-0006 §5.
#
# Usage:
#   ./coverage.sh                  # all targets, text summary
#   ./coverage.sh wire_resp        # one target, text summary
#   ./coverage.sh all html         # all targets, also generate HTML
#                                  # under coverage/<target>/html/
#
# Requirements:
#   - nightly rustc with llvm-tools-preview component (installed via
#     `rustup component add llvm-tools-preview --toolchain nightly`)
#   - cargo-fuzz (installed via `cargo install cargo-fuzz`)
#   - the target's corpus has at least one input (any previous
#     `cargo +nightly fuzz run <target>` session leaves a corpus)

cd "$(dirname "$0")"

TARGET="${1:-all}"
FORMAT="${2:-text}"   # text | html

# Locate llvm-cov inside the nightly toolchain — it's not on $PATH by
# default, but `rustup component add llvm-tools-preview` puts it in
# the toolchain's bin dir.
SYSROOT=$(rustc +nightly --print sysroot)
HOST=$(rustc +nightly -vV | sed -n 's/host: //p')
LLVM_BIN="$SYSROOT/lib/rustlib/$HOST/bin"
LLVM_COV="$LLVM_BIN/llvm-cov"

if [ ! -x "$LLVM_COV" ]; then
    echo "ERROR: llvm-cov not found at $LLVM_COV" >&2
    echo "Install with: rustup component add llvm-tools-preview --toolchain nightly" >&2
    exit 1
fi

ALL_TARGETS="wire_deserialize wire_oxiwire wire_resp wire_pg \
             oxiwire_roundtrip resp_roundtrip msgpack_roundtrip"

cover_one() {
    local tgt="$1"
    echo
    echo "════════════════════════════════════════════════════════════"
    echo " coverage: $tgt"
    echo "════════════════════════════════════════════════════════════"

    # Sanity: corpus must exist or `fuzz coverage` has nothing to
    # measure against.
    if [ ! -d "corpus/$tgt" ] || [ -z "$(ls -A corpus/$tgt 2>/dev/null)" ]; then
        echo "WARN: corpus/$tgt is empty — running a 30s fuzz session first to populate it"
        cargo +nightly fuzz run "$tgt" -- -max_total_time=30 >/dev/null 2>&1 || true
    fi

    # Build the coverage-instrumented binary, then run it across the
    # whole corpus. Writes coverage/<target>/coverage.profdata.
    cargo +nightly fuzz coverage "$tgt" 2>&1 | tail -2

    local bin="target/$HOST/coverage/$HOST/release/$tgt"
    local profdata="coverage/$tgt/coverage.profdata"

    if [ ! -f "$profdata" ]; then
        echo "ERROR: $profdata missing — coverage run failed for $tgt" >&2
        return 1
    fi

    # Focused report — filter out cargo dep cache and the rust
    # stdlib (those aren't OUR code and would dwarf the signal).
    "$LLVM_COV" report \
        --instr-profile="$profdata" \
        "$bin" \
        --ignore-filename-regex='/\.cargo/|/rustc/|/library/std|/library/core|/library/alloc' \
        2>&1 | awk -v tgt="$tgt" '
            # Compact view: keep the header / separator / TOTAL rows
            # always; drop file rows the target never touched.
            #
            # Default-split columns (whitespace-separated):
            #   $1=Filename  $2=Regions  $3=MissedRegions  $4=Cover(reg%)
            #   $5=Functions $6=MissedFunctions  $7=Cover(fn%)
            #   $8=Lines     $9=MissedLines      $10=Cover(line%)
            /^Filename/ || /^-+$/ || /^TOTAL/ { print; next }
            # Show files with ANY region OR line coverage > 0%.
            { if (($4 != "0.00%" && $4 != "-") || ($10 != "0.00%" && $10 != "-")) print }
        '

    if [ "$FORMAT" = "html" ]; then
        local html_dir="coverage/$tgt/html"
        rm -rf "$html_dir"
        "$LLVM_COV" show \
            --instr-profile="$profdata" \
            --format=html \
            --output-dir="$html_dir" \
            "$bin" \
            --ignore-filename-regex='/\.cargo/|/rustc/|/library/std|/library/core|/library/alloc'
        echo
        echo "→ HTML report: $html_dir/index.html"
    fi
}

if [ "$TARGET" = "all" ]; then
    for t in $ALL_TARGETS; do
        cover_one "$t"
    done
else
    cover_one "$TARGET"
fi
