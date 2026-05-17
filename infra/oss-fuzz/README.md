# OSS-Fuzz integration

[OSS-Fuzz](https://google.github.io/oss-fuzz/) is Google's
continuous-fuzzing service for open-source projects. Once an
upstream `projects/oxidb/` directory is accepted, Google runs every
fuzz target in `fuzz/` for **24 hours a day on their compute, every
day, forever** at no cost to us. Findings are emailed automatically
and filed as embargoed bug-tracker issues; the embargo lifts after
90 days unless the project owner disables it.

This directory holds the canonical source for the three files OSS-
Fuzz needs (`project.yaml`, `Dockerfile`, `build.sh`), kept here so
the project itself owns them rather than having them live only in a
fork of the OSS-Fuzz repo. When submitting to upstream, copy these
files to `projects/oxidb/` in https://github.com/google/oss-fuzz.

## Status

- ✅ Project metadata + Dockerfile + build script committed in this
  directory
- ⏳ NOT YET submitted to OSS-Fuzz upstream — submission is a
  separate manual step (see "How to submit" below)
- ⏳ NOT YET verified end-to-end against the OSS-Fuzz local helper
  (verification is documented but not run as part of this commit)

## Files in this directory

| File | What it does |
|---|---|
| [`project.yaml`](project.yaml) | OSS-Fuzz project metadata — language, sanitizers, contacts. Contains email placeholders that **must** be replaced before upstream submission. |
| [`Dockerfile`](Dockerfile) | Build container; pulls Google's pre-baked `base-builder-rust` image, shallow-clones our repo. |
| [`build.sh`](build.sh) | Invoked inside the container; runs `cargo +nightly fuzz build -O --debug-assertions` and copies every fuzz binary to `$OUT`. |

## Email substitution (REQUIRED before submitting)

[`project.yaml`](project.yaml) has `REPLACE-WITH-MAINTAINER-EMAIL@example.com`
in both `primary_contact` and `auto_ccs`. This is intentional — the
`parisxmas/OxiDB` repo is public, and the project memory rule says
"never put emails in committed files." Before submitting upstream,
replace these placeholders in the COPY going into `projects/oxidb/`
(NOT in our local copy — keep our committed file with placeholders
so the rule stays enforced here).

Suggested replacement: a project-mailbox address like
`oxidb-security@<your-domain>` rather than a personal email, so
findings route to a list that survives any one person leaving.

## How to verify locally before submitting

```bash
# 1. Clone OSS-Fuzz upstream (we'll only use its helper, never push)
git clone --depth=1 https://github.com/google/oss-fuzz.git /tmp/oss-fuzz

# 2. Stage our files under the expected project name
mkdir -p /tmp/oss-fuzz/projects/oxidb
cp -r infra/oss-fuzz/. /tmp/oss-fuzz/projects/oxidb/
# Replace the email placeholders for the local run
sed -i.bak 's/REPLACE-WITH-MAINTAINER-EMAIL@example.com/local-test@example.com/g' \
    /tmp/oss-fuzz/projects/oxidb/project.yaml

# 3. Build the fuzzers via OSS-Fuzz's helper (needs Docker running)
cd /tmp/oss-fuzz
python infra/helper.py build_image oxidb
python infra/helper.py build_fuzzers --sanitizer address oxidb

# 4. Run one target for 60 seconds as a smoke check
python infra/helper.py run_fuzzer oxidb wire_resp -- -max_total_time=60

# 5. Run check_build to confirm OSS-Fuzz's invariants hold
python infra/helper.py check_build oxidb
```

If `check_build` is green, the project is ready for upstream
submission.

## How to submit to OSS-Fuzz upstream

1. Fork https://github.com/google/oss-fuzz
2. Create `projects/oxidb/` in your fork:
   ```bash
   cd <your-fork-of-oss-fuzz>
   mkdir -p projects/oxidb
   cp -r <oxidb-checkout>/infra/oss-fuzz/. projects/oxidb/
   # Replace the email placeholders with real maintainer addresses
   $EDITOR projects/oxidb/project.yaml
   ```
3. Verify the build still passes after the email substitution:
   ```bash
   python infra/helper.py build_image oxidb
   python infra/helper.py build_fuzzers oxidb
   python infra/helper.py check_build oxidb
   ```
4. Open a PR against `google/oss-fuzz` with the new
   `projects/oxidb/` directory. OSS-Fuzz reviewers respond within
   a few days; merging the PR enrolls the project.

## Once enrolled

- Findings appear at https://bugs.chromium.org/p/oss-fuzz/ tagged
  `Project:oxidb` (initially embargoed 90 days)
- Email notifications go to `primary_contact` + `auto_ccs`
- Public corpus + crash reproducers downloadable from
  https://storage.googleapis.com/oxidb-corpus.clusterfuzz-external.appspot.com/
- Add the OSS-Fuzz badge to the README's "Adoption status" line

## Sanitizers we run

- **AddressSanitizer** — heap overflows, use-after-free, double-
  free, out-of-bounds reads
- **UndefinedBehaviorSanitizer** — signed-int overflow, nullptr-
  deref, divide-by-zero, invalid-cast

MemorySanitizer is intentionally OFF — Rust's safe defaults make
MSAN findings rare relative to the daily CI cost (the build farm is
shared across thousands of projects, so each cycle matters).

## Targets registered

All 5 targets from `fuzz/fuzz_targets/` are listed by name in
`build.sh`. When adding a new target, append it to the for-loop in
`build.sh` (and remember to update this list):

- `wire_deserialize` — top-level message dispatcher
- `wire_oxiwire` — OxiWire binary decoder
- `wire_resp` — RESP / OxiMem
- `wire_pg` — pg_wire frontend
- `oxiwire_roundtrip` — structure-aware encode↔decode roundtrip
