# OSS-Fuzz build container for OxiDB.
#
# Built from Google's pre-baked Rust+libfuzzer base image, which
# already has `cargo-fuzz`, the right nightly toolchain, libfuzzer
# headers, and sanitizer-friendly linker config installed and
# verified to work with their build farm.
#
# Mirrored to projects/oxidb/Dockerfile in the OSS-Fuzz upstream
# repo when submitting. See infra/oss-fuzz/README.md.

FROM gcr.io/oss-fuzz-base/base-builder-rust

# Shallow clone — OSS-Fuzz fuzzes against HEAD continuously and
# rebuilds on every push, so deep history isn't worth the bandwidth.
RUN git clone --depth=1 https://github.com/parisxmas/OxiDB.git oxidb

# OSS-Fuzz convention: build.sh lives at $SRC/ alongside the cloned
# repo so it's discoverable by the helper scripts.
WORKDIR $SRC/oxidb
COPY build.sh $SRC/
