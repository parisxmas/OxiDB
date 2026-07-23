# Dockerized OxiBase deployment

Runs the whole stack in three containers behind a single host, with **path-based
tenants** (`https://<host>/<slug>/rest/v1/…`) so no wildcard certificate is
needed.

| Container | What | Exposed |
| --- | --- | --- |
| `data-plane` (`oxidb-server`) | document + SQL + TSDB engines, REST + wire; owns the data volume | internal only |
| `oxibase` | control plane — accounts, projects, per-project + end-user auth; holds the seal key | internal only |
| `web` (nginx) | dashboard SPA + reverse proxy (routes `/platform/v1`→control plane, `/<tenant>/rest\|api` and `/rest\|api`→data plane) | `:WEB_PORT` on the host |

Only `web` is published. The wire port (`4444`) and both engines stay on the
internal Docker network. The data plane holds **no signing key** — it verifies
project/end-user tokens with the projects' public keys alone; only `oxibase`
holds `OXIDB_SEAL_KEY`.

## 1. Configure

```bash
cp deploy/.env.example deploy/.env
# generate strong secrets
for k in OXIDB_SEAL_KEY OXIDB_PLATFORM_SECRET OXIDB_JWT_SECRET; do
  echo "$k=$(openssl rand -hex 32)"
done
# paste those into deploy/.env, and set OXIBASE_HOST=your.host
```

`deploy/.env` is gitignored — never commit it.

## 2. Build & run

From the repo root (the build context is the whole repo):

```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d --build
```

First build compiles the Rust binaries (a few minutes); later builds are cached.

## 3. Put TLS in front

The `web` container speaks plain HTTP on `WEB_PORT`. Terminate TLS in front of
it — either:

- **Cloudflare** (recommended, matches the existing setup): a first-level
  subdomain (`<host>`) is already covered by Cloudflare's free `*.<zone>` edge
  certificate. Set the DNS record to proxied and SSL mode to *Full*; point the
  origin at `http://<server>:WEB_PORT`. No cert on the box.
- **Your existing SNI terminator / any reverse proxy**: forward `<host>:443` →
  `127.0.0.1:WEB_PORT`.

Path-based tenants mean a single `<host>` serves every project, so no wildcard
certificate is required.

## 4. Verify

```bash
# health
curl -s http://localhost:WEB_PORT/v1/hello
# create a developer account + project (or use the dashboard at https://<host>/)
curl -s -X POST https://<host>/platform/v1/signup \
  -H 'content-type: application/json' -d '{"email":"me@x.com","password":"hunter2hunter2"}'
# → token; create a project → returns { ref, slug, anon_key, service_role_key }
# then the project is reachable path-based:
#   https://<host>/<slug>/rest/v1/<table>
```

The dashboard (`https://<host>/`) does signup/login, project management, the
table/SQL editors and the rules editor.

## 5. Operate

```bash
docker compose -f deploy/docker-compose.yml logs -f oxibase data-plane
docker compose -f deploy/docker-compose.yml pull   # after a code update, rebuild:
docker compose -f deploy/docker-compose.yml up -d --build
```

**Backups**: the only state is the `oxidb-data` named volume (all projects,
users, sessions). Back it up (e.g. `docker run --rm -v oxidb_oxidb-data:/d -v
$PWD:/b alpine tar czf /b/oxidb-backup.tgz -C /d .`), or use the engine's own
`backup`/PITR commands.

## Security notes

- Wire (`4444`) and both engine REST ports are **not** published — only the
  nginx edge is. Keep it that way.
- The seal key lives only on `oxibase`; a compromised data plane cannot mint
  tokens.
- Rotate a project's keys from the dashboard to invalidate a leaked key (and all
  its end-user sessions) instantly.
- **Hardening (optional)**: enable wire SCRAM (`OXIDB_AUTH=true` on the data
  plane + `OXIBASE_UPSTREAM_USER`/`OXIBASE_UPSTREAM_PASSWORD` on `oxibase`) so
  the internal wire is authenticated too, not just network-isolated.
