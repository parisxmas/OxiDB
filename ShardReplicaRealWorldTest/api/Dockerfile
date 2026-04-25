# Build context = repo root (..). API source under ShardReplicaRealWorldTest/api/.
# Vendored deps via local replace into go/oxidb and go/oxiwire.

FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git
WORKDIR /build

# Bring in the OxiDB Go client + wire helper that api/go.mod replaces against
COPY go/oxidb ./go/oxidb
COPY go/oxiwire ./go/oxiwire

# API source
COPY ShardReplicaRealWorldTest/api ./ShardReplicaRealWorldTest/api

WORKDIR /build/ShardReplicaRealWorldTest/api
RUN go mod tidy
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/api .

# ─── runtime ────────────────────────────────────────────────
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/api /usr/local/bin/api

EXPOSE 8080
ENV API_LISTEN=0.0.0.0:8080
ENTRYPOINT ["/usr/local/bin/api"]
