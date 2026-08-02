#!/bin/bash
# Deploy s3demo.baltavista.com from this Mac, *without* shipping any
# source. Strategy mirrors ftstests/deploy/deploy.sh but skips the
# image build entirely — the same `oxidb-ftsdemo-oxidb:latest` image
# already on the remote box has the S3 listener compiled in. We
# only ship a tiny compose file, an nginx site, and obtain a cert.
#
# Steps:
#   1. scp compose + nginx site
#   2. docker compose up -d (uses image already loaded on the host)
#   3. install nginx site + certbot
#
# IMPORTANT: pipefail is set so this script's exit code reflects
# real build/deploy failures even when caller pipes through tee.

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:?REMOTE_HOST env var required}"
REMOTE_PORT="${REMOTE_PORT:-22}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/opt/oxidb-s3demo}"
DOMAIN="${DOMAIN:?DOMAIN env var required}"
ADMIN_EMAIL="${ADMIN_EMAIL:?ADMIN_EMAIL env var required}"

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"

ssh_cmd() {
    ssh -p "$REMOTE_PORT" "$REMOTE_USER@$REMOTE_HOST" "$@"
}

echo "[1/4] verify the oxidb image is already loaded on the remote"
if ! ssh_cmd "docker image inspect oxidb-ftsdemo-oxidb:latest >/dev/null 2>&1"; then
    echo "  ERROR: oxidb-ftsdemo-oxidb:latest not present on remote." >&2
    echo "  Run ftstests/deploy/deploy.sh first (or push the tar manually)." >&2
    exit 1
fi
echo "      ok — image present"

echo "[2/4] scp compose + nginx site → $REMOTE_DIR"
ssh_cmd "mkdir -p $REMOTE_DIR"
scp -P "$REMOTE_PORT" \
    "$DEPLOY_DIR/docker-compose.yml" \
    "$DEPLOY_DIR/nginx-site.conf" \
    "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/"

echo "[3/4] obtain cert (idempotent) + install nginx site"
HAS_CERT=$(ssh_cmd "test -f /etc/letsencrypt/live/$DOMAIN/fullchain.pem && echo yes || echo no")
if [ "$HAS_CERT" = "no" ]; then
    echo "      no cert yet — provisioning HTTP-only stub for ACME"
    ssh_cmd "mkdir -p /var/www/html"
    ssh_cmd "cat > /etc/nginx/conf.d/$DOMAIN.conf <<EOF
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

# Install the real conf (HTTP→HTTPS redirect + 8444 SSL block).
ssh_cmd "install -m 0644 $REMOTE_DIR/nginx-site.conf /etc/nginx/conf.d/$DOMAIN.conf"
ssh_cmd "nginx -t && systemctl reload nginx"

echo "[4/4] docker compose up -d"
ssh_cmd "cd $REMOTE_DIR && docker compose up -d"

echo
echo "      verifying"
ssh_cmd "curl -sk -o /dev/null -w '      https://$DOMAIN/ → %{http_code}\\n' https://$DOMAIN/ || true"
ssh_cmd "curl -sk -o /dev/null -w '      origin :9000     → %{http_code}\\n' http://127.0.0.1:9000/ || true"

echo
echo "✓ deployed:  https://$DOMAIN/"
echo "  containers:  oxidb-s3demo-oxidb-1"
echo "  test:        AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \\"
echo "                AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \\"
echo "                aws --endpoint-url=https://$DOMAIN s3 ls"
