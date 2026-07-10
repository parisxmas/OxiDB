#!/usr/bin/env bash
# Conformance harness (ADR-0014 Phase 1): for every allowed example, diff
# the Rust VM's output against the Go reference implementation.
#
#   PASS      — byte-identical output
#   REJECTED  — validate refused the program, as expected (async/import/
#               concurrency examples)
#   FAIL      — any divergence
#
# Prereqs: the Go reference binary at /tmp/cobra-bin (rebuild with
#   cd ~/source/cobra && go build -o /tmp/cobra-bin .)
set -u

COBRA_REPO="${COBRA_REPO:-$HOME/source/cobra}"
COBRA_BIN="${COBRA_BIN:-/tmp/cobra-bin}"
CRATE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${TMPDIR:-/tmp}/cobra-conformance"
mkdir -p "$WORK"

if [ ! -x "$COBRA_BIN" ]; then
    echo "building Go reference binary at $COBRA_BIN ..."
    (cd "$COBRA_REPO" && go build -o "$COBRA_BIN" .) || exit 1
fi

echo "building cobra-run ..."
(cd "$CRATE_DIR" && cargo build --release --quiet --bin cobra-run) || exit 1
RUN="$CRATE_DIR/../target/release/cobra-run"
[ -x "$RUN" ] || RUN="$(cd "$CRATE_DIR" && cargo metadata --format-version 1 2>/dev/null \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')/release/cobra-run"

# Byte-for-byte conformance targets.
EXAMPLES="hello fib loops ternary collections strings floats functional errors syntax_extras structs oop geometry decorators pmap"
# Programs validate must refuse (async functions / imports / concurrency).
REJECT="parallel async"
# contracts: excluded — the Go portable encoder cannot serialize Contract
# constants ("portable encode: unsupported constant ... *object.Contract"),
# so no .cobrac exists to run. Tracked as an upstream Phase-0 encoder gap.

pass=0 fail=0
printf '%-16s %s\n' "example" "result"
printf '%-16s %s\n' "-------" "------"

for ex in $EXAMPLES; do
    src="$COBRA_REPO/examples/$ex.cobra"
    exp="$WORK/$ex.expected.txt"
    act="$WORK/$ex.actual.txt"
    cc="$WORK/$ex.cobrac"

    if ! "$COBRA_BIN" "$src" > "$exp" 2> "$WORK/$ex.goerr"; then
        printf '%-16s FAIL (go reference run failed)\n' "$ex"; fail=$((fail+1)); continue
    fi
    if ! "$COBRA_BIN" build --portable "$src" "$cc" > /dev/null 2> "$WORK/$ex.builderr"; then
        printf '%-16s FAIL (portable build failed: %s)\n' "$ex" "$(cat "$WORK/$ex.builderr")"
        fail=$((fail+1)); continue
    fi
    if ! "$RUN" "$cc" > "$act" 2> "$WORK/$ex.rusterr"; then
        printf '%-16s FAIL (cobra-run exited nonzero: %s)\n' "$ex" "$(head -1 "$WORK/$ex.rusterr")"
        fail=$((fail+1)); continue
    fi
    if diff -q "$exp" "$act" > /dev/null; then
        printf '%-16s PASS\n' "$ex"; pass=$((pass+1))
    else
        printf '%-16s FAIL (output diff, see %s)\n' "$ex" "$WORK/$ex.diff"
        diff "$exp" "$act" > "$WORK/$ex.diff"
        fail=$((fail+1))
    fi
done

for ex in $REJECT; do
    src="$COBRA_REPO/examples/$ex.cobra"
    cc="$WORK/$ex.cobrac"
    if ! "$COBRA_BIN" build --portable "$src" "$cc" > /dev/null 2>&1; then
        printf '%-16s FAIL (portable build failed for rejection case)\n' "$ex"
        fail=$((fail+1)); continue
    fi
    "$RUN" "$cc" > "$WORK/$ex.out" 2> "$WORK/$ex.err"
    status=$?
    if [ $status -eq 2 ] && grep -q "not allowed" "$WORK/$ex.err"; then
        printf '%-16s REJECTED (as expected: %s)\n' "$ex" "$(head -1 "$WORK/$ex.err")"
        pass=$((pass+1))
    else
        printf '%-16s FAIL (expected validation rejection, exit=%s)\n' "$ex" "$status"
        fail=$((fail+1))
    fi
done

echo
echo "contracts         EXCLUDED (Go portable encoder cannot serialize Contract constants)"
echo
echo "$pass passed, $fail failed"
[ $fail -eq 0 ]
