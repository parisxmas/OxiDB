#!/bin/bash
# Deploy oxidbftsdemo.baltavista.com from this Mac, *without* shipping
# any source to the remote. Strategy:
#
#   1. docker buildx build --platform=linux/amd64  → Linux/amd64 images
#      compiled locally (QEMU on arm64 Macs)
#   2. docker save                                  → single tar of both images
#   3. scp the tar + docker-compose.yml + nginx-site.conf
#   4. docker load on remote
#   5. docker compose up -d
#   6. install nginx site + certbot
#
# What ends up on the server: two image layers (binaries), a tiny
# compose file, a tiny nginx site. No source tree.

set -euo pipefail

# All connection details come from env vars — no committed defaults.
# Set these before running (or put them in a sourced file outside the
# repo, e.g. ~/.config/oxidb-ftsdemo.env):
#
#   export REMOTE_HOST=your.server
#   export REMOTE_PORT=22                 # ssh port
#   export REMOTE_USER=root
#   export DOMAIN=demo.example.com        # must already resolve to REMOTE_HOST
#   export ADMIN_EMAIL=you@example.com    # Let's Encrypt notification
#
REMOTE_HOST="${REMOTE_HOST:?REMOTE_HOST env var required}"
REMOTE_PORT="${REMOTE_PORT:-22}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/opt/oxidb-ftsdemo}"
DOMAIN="${DOMAIN:?DOMAIN env var required}"
ADMIN_EMAIL="${ADMIN_EMAIL:?ADMIN_EMAIL env var required}"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/ftstests/deploy"

OXIDB_IMAGE="oxidb-ftsdemo-oxidb:latest"
WEB_IMAGE="oxidb-ftsdemo-web:latest"

ssh_cmd() {
    ssh -p "$REMOTE_PORT" "$REMOTE_USER@$REMOTE_HOST" "$@"
}

echo "[1/6] buildx → linux/amd64 images (local; QEMU on arm64 Macs)"
cd "$REPO_ROOT"
docker buildx build \
    --platform=linux/amd64 \
    --load \
    --tag "$OXIDB_IMAGE" \
    --file Dockerfile \
    .

docker buildx build \
    --platform=linux/amd64 \
    --load \
    --tag "$WEB_IMAGE" \
    --file ftstests/deploy/Dockerfile.web \
    .

echo "[2/6] docker save → /tmp/oxidb-ftsdemo-images.tar"
TAR=/tmp/oxidb-ftsdemo-images.tar
docker save "$OXIDB_IMAGE" "$WEB_IMAGE" -o "$TAR"
ls -lh "$TAR"

echo "[3/6] scp images + compose + nginx site → $REMOTE_DIR"
ssh_cmd "mkdir -p $REMOTE_DIR"
scp -P "$REMOTE_PORT" \
    "$TAR" \
    "$DEPLOY_DIR/docker-compose.yml" \
    "$DEPLOY_DIR/nginx-site.conf" \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/"

echo "[4/6] docker load + compose up"
ssh_cmd "docker load -i $REMOTE_DIR/$(basename $TAR)"
ssh_cmd "cd $REMOTE_DIR && docker compose up -d"

echo "[5/6] install nginx site + reload"
# This server's nginx loads sites from /etc/nginx/conf.d/ (the existing
# baltavista pattern). sites-available/sites-enabled is unused for the
# public-facing sites here, so we drop a single .conf into conf.d.
ssh_cmd "install -m 0644 $REMOTE_DIR/nginx-site.conf /etc/nginx/conf.d/$DOMAIN.conf"
# Clean up any stale legacy entry from earlier deploy attempts.
ssh_cmd "rm -f /etc/nginx/sites-available/$DOMAIN /etc/nginx/sites-enabled/$DOMAIN"

echo "[6/6] obtain cert (idempotent) + reload nginx"
# Step A — install a temporary HTTP-only site so certbot --webroot can
# answer the ACME challenge without a server_name match later. We do
# this only on first run when no cert exists yet.
HAS_CERT=$(ssh_cmd "test -f /etc/letsencrypt/live/$DOMAIN/fullchain.pem && echo yes || echo no")
if [ "$HAS_CERT" = "no" ]; then
    echo "      no cert yet — issuing via certbot --webroot"
    ssh_cmd "mkdir -p /var/www/html"
    # Provision a tiny HTTP-only nginx site just for the challenge.
    ssh_cmd "cat > /etc/nginx/conf.d/$DOMAIN.conf <<'EOF'
server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN;
    location /.well-known/acme-challenge/ { root /var/www/html; }
    location / { return 200 'pending tls\n'; }
}
EOF"
    ssh_cmd "nginx -t && systemctl reload nginx"
    ssh_cmd "certbot certonly --webroot -w /var/www/html --non-interactive --agree-tos --email $ADMIN_EMAIL -d $DOMAIN"
fi

# Step B — install the real conf (HTTP→HTTPS redirect + SSL block on
# 127.0.0.1:8444 with proxy_protocol). Reload.
ssh_cmd "install -m 0644 $REMOTE_DIR/nginx-site.conf /etc/nginx/conf.d/$DOMAIN.conf"
ssh_cmd "nginx -t && systemctl reload nginx"

echo "      verify HTTPS"
ssh_cmd "curl -sk -o /dev/null -w '      https://$DOMAIN/healthz → %{http_code}\\n' https://$DOMAIN/healthz || true"

echo
echo "✓ deployed:  https://$DOMAIN/"
echo "  remote files (everything):  $(ssh_cmd "du -sh $REMOTE_DIR" | awk '{print $1}')"
echo "  containers:                 oxidb-ftsdemo-oxidb-1, oxidb-ftsdemo-web-1"
echo "  logs:                       ssh -p $REMOTE_PORT $REMOTE_USER@$REMOTE_HOST 'cd $REMOTE_DIR && docker compose logs -f --tail=80'"
