FROM golang:1.23-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod ./
COPY *.go ./
COPY index.html ./
RUN go mod tidy && go build -o dashboard .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/dashboard /usr/local/bin/
ENTRYPOINT ["dashboard"]
