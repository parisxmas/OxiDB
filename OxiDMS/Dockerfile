# Stage 1: Build frontend
FROM node:20-alpine AS frontend-builder
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Stage 2: Build Go backend
FROM golang:1.21-alpine AS backend-builder
WORKDIR /build
COPY _deps/oxidb/ /build/go/oxidb/
COPY go.mod go.sum /build/OxiDMS/
WORKDIR /build/OxiDMS
RUN go mod download
COPY internal/ /build/OxiDMS/internal/
COPY main.go /build/OxiDMS/
RUN CGO_ENABLED=0 GOOS=linux go build -o /oxidms-server .

# Stage 3: Final image
FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=backend-builder /oxidms-server /usr/local/bin/oxidms-server
COPY --from=frontend-builder /app/dist /var/www/oxidms

EXPOSE 8080

CMD ["oxidms-server"]
