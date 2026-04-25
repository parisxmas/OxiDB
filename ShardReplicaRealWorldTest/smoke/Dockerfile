# Build context = repo root (..). Sources under ShardReplicaRealWorldTest/smoke/.

FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git
WORKDIR /build

COPY go/oxidb ./go/oxidb
COPY go/oxiwire ./go/oxiwire
COPY ShardReplicaRealWorldTest/smoke ./ShardReplicaRealWorldTest/smoke

WORKDIR /build/ShardReplicaRealWorldTest/smoke
RUN go mod tidy
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/smoke .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/smoke /usr/local/bin/smoke
ENTRYPOINT ["/usr/local/bin/smoke"]
