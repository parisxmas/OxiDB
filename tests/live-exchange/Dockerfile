# Self-contained OxiDB Exchange demo image: builds the current oxidb-server
# and the Go exchange, then runs the whole market (server + matcher + web +
# traders) from one container. Build context = repo root.
#
#   docker build -f tests/live-exchange/Dockerfile -t oxidb-exchange .

FROM rust:1.88-bookworm AS rust
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY oxidb-server/ oxidb-server/
COPY oxidb-sql/ oxidb-sql/
# oxidb-server only depends on the root `oxidb` crate and `oxidb-sql`. Stub the
# other workspace members so the workspace resolves without copying them.
RUN for c in oxidb-client-ffi oxidb-embedded-ffi oxidb-cli oxidb-tail oxipool oxidb-wasm oxidb-agg-merge; do \
      mkdir -p "$c/src" && \
      printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n' "$c" > "$c/Cargo.toml" && \
      : > "$c/src/lib.rs" ; done
RUN cargo build --release -p oxidb-server

FROM golang:1.23-bookworm AS go
WORKDIR /ex
COPY tests/live-exchange/go.mod ./
COPY tests/live-exchange/*.go ./
RUN go build -o exchange .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates procps curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=rust /build/target/release/oxidb-server /usr/local/bin/oxidb-server
COPY --from=go /ex/exchange /usr/local/bin/exchange
COPY tests/live-exchange/docker-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 8090
ENTRYPOINT ["/entrypoint.sh"]
