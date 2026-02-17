FROM rust:latest AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY examples/ examples/
COPY oxidb-server/ oxidb-server/
COPY oxidb-client-ffi/ oxidb-client-ffi/

RUN cargo build --release --package oxidb-server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/oxidb-server /usr/local/bin/oxidb-server

RUN mkdir -p /data

ENV OXIDB_ADDR=0.0.0.0:4444
ENV OXIDB_DATA=/data
ENV OXIDB_POOL_SIZE=4

EXPOSE 4444

VOLUME ["/data"]

CMD ["oxidb-server"]
