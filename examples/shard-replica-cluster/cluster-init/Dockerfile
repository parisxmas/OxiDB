# Build context = repo root (..). Sources live under ShardReplicaRealWorldTest/cluster-init/
FROM golang:1.22-alpine AS builder

WORKDIR /build
COPY ShardReplicaRealWorldTest/cluster-init/go.mod ./
COPY ShardReplicaRealWorldTest/cluster-init/main.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/cluster-init .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/cluster-init /usr/local/bin/cluster-init
ENTRYPOINT ["/usr/local/bin/cluster-init"]
