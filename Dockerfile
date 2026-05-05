FROM rust:1.88-bookworm AS builder

# Tesseract / Leptonica dev libs needed by the `leptess` crate's
# bindgen step. clang/libclang let bindgen parse the C headers.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libtesseract-dev libleptonica-dev libclang-dev clang pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY examples/ examples/
COPY oxidb-server/ oxidb-server/
COPY oxidb-client-ffi/ oxidb-client-ffi/
COPY oxidb-embedded-ffi/ oxidb-embedded-ffi/
COPY oxidb-cli/ oxidb-cli/
COPY oxipool/ oxipool/

# Create stubs for workspace members not needed for server build
RUN mkdir -p oxidb-app/src-tauri/src && \
    echo '[package]\nname = "oxidb-app"\nversion = "0.1.0"\nedition = "2024"\n\n[dependencies]\n' > oxidb-app/src-tauri/Cargo.toml && \
    echo '' > oxidb-app/src-tauri/src/lib.rs && \
    mkdir -p oxidb-wasm/src && \
    echo '[package]\nname = "oxidb-wasm"\nversion = "0.1.0"\nedition = "2024"\n\n[dependencies]\n' > oxidb-wasm/Cargo.toml && \
    echo '' > oxidb-wasm/src/lib.rs && \
    mkdir -p oxidb-tail/src && \
    echo '[package]\nname = "oxidb-tail"\nversion = "0.1.0"\nedition = "2024"\n\n[dependencies]\n' > oxidb-tail/Cargo.toml && \
    echo 'fn main() {}' > oxidb-tail/src/main.rs

RUN cargo build --release --package oxidb-server --features cluster,ocr

FROM debian:bookworm-slim

# Runtime needs libtesseract.so + the language traineddata files that
# leptess loads at OCR time. eng + tur cover the demo's typical mix
# (Sherlock/Pride excerpts + Turkish government PDFs).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libtesseract5 \
    tesseract-ocr-eng \
    tesseract-ocr-tur \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/oxidb-server /usr/local/bin/oxidb-server

RUN mkdir -p /data

ENV OXIDB_ADDR=0.0.0.0:4444
ENV OXIDB_DATA=/data
ENV OXIDB_POOL_SIZE=4

EXPOSE 4444

VOLUME ["/data"]

CMD ["oxidb-server"]
