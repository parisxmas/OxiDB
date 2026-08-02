# OxiDB Bug Tracker

A GitHub-issues-style bug reporter for the OxiDB website. Anyone can read the
list; **signing in with Google is required to open a bug or comment**. Reports
are stored in a **dedicated OxiDB instance** (SQL engine, via EF Core) that runs
next to a small ASP.NET minimal API — completely separate from every production
OxiDB instance on the host.

The point, like the ColdChain demo, is that this is an entirely **standard .NET
stack** — ASP.NET Core, EF Core, and Google's own auth library — pointed at one
ordinary OxiDB process. OxiDB tracking its own bugs.

```
browser (/bugs page, Google Sign-In)
   │  ID token as  Authorization: Bearer …
   ▼
nginx  oxidb.baltavista.com/bugs-api/  ──►  API container (:8124)
                                              │  EF Core, Host=oxidb;Database=bugtracker
                                              ▼
                                         oxidb-server (SQL engine, own volume)
```

## Pieces

- `BugTracker.Api/` — the API. Minimal API + EF Core; `Auth.cs` validates Google
  ID tokens with `Google.Apis.Auth`; `Domain.cs` is the `BugReport`/`BugComment`
  model. Data lives in its own OxiDB database `bugtracker`.
- `docker-compose.yml` — the isolated `oxidb` + the `api`. Only the API port is
  published (`127.0.0.1:8124`); nothing else can collide with the other instances.
- `Dockerfile` / `Dockerfile.oxidb` — build the API (context = repo root, because
  the OxiDB client packages are referenced by project) and the server image.
- `nginx-bugs-api.conf.snippet` — the `location /bugs-api/` block to paste into
  the site's vhost.
- Frontend: `website-react/app/bugs/page.tsx` — the `/bugs` page (client-side
  Google Sign-In, list, report form, detail drawer, admin close/reopen).

## One-time: create the Google OAuth client

1. Google Cloud Console → **APIs & Services → Credentials → Create credentials →
   OAuth client ID**.
2. Application type: **Web application**.
3. **Authorized JavaScript origins**: `https://oxidb.baltavista.com`
   (add `http://localhost:3000` for local dev).
4. Copy the **Client ID** (`…apps.googleusercontent.com`). No client *secret* is
   needed — this uses the Google Identity Services ID-token flow, not a server
   redirect.

That one client id goes in **two** places:
- the API, as `BUGS_GOOGLE_CLIENT_ID` (it is the required `aud` on every token);
- the website build, as `NEXT_PUBLIC_GOOGLE_CLIENT_ID`.

## Run locally

```bash
# 1. Build the server binary for the alpine image (musl static):
cargo build --release -p oxidb-server --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/oxidb-server bugtracker/

# 2. Configure:
cd bugtracker
cp .env.example .env      # fill in BUGS_GOOGLE_CLIENT_ID

# 3. Up:
docker compose up --build -d
curl -s localhost:8124/api | jq
```

Website against the local API:

```bash
cd website-react
NEXT_PUBLIC_GOOGLE_CLIENT_ID=…apps.googleusercontent.com \
NEXT_PUBLIC_BUGS_API=http://localhost:8124 \
npm run dev
# open http://localhost:3000/bugs
```

## Deploy (same host as ColdChain)

```bash
# On the build machine — cross-compile the musl server binary and copy it in:
cargo build --release -p oxidb-server --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/oxidb-server bugtracker/

# Ship bugtracker/ + dotnet/ to the host (the Dockerfile context is the repo
# root because the OxiDB client packages are project references), e.g. rsync the
# repo, then on the host:
cd /opt/bugtracker            # bugtracker/ contents, with dotnet/ alongside
cp .env.example .env          # set BUGS_GOOGLE_CLIENT_ID
docker compose up --build -d

# nginx: paste nginx-bugs-api.conf.snippet into the oxidb.baltavista.com vhost,
# then:  nginx -t && systemctl reload nginx

# Website: rebuild the static export WITH the client id baked in, and deploy:
cd website-react
NEXT_PUBLIC_GOOGLE_CLIENT_ID=…apps.googleusercontent.com npm run build
rsync -a out/ root@host:/var/www/oxidb/
```

Because the API is served under `/bugs-api` on the same origin as the page, the
production build needs **no** `NEXT_PUBLIC_BUGS_API` — it defaults to `/bugs-api`.

## API

| Method | Path | Auth | |
|---|---|---|---|
| GET | `/bugs?status=&q=` | public | list (open/closed/all, optional search) |
| GET | `/bugs/{id}` | public | one bug + its comments |
| POST | `/bugs` | signed in | `{title, body}` |
| POST | `/bugs/{id}/comments` | signed in | `{body}` |
| PATCH | `/bugs/{id}` | **admin** | `{status: "open"\|"closed"}` |
| GET | `/me` | any | identity from the token (+ `isAdmin`) |

Auth is enforced server-side on every write; the token's `aud` must equal the
configured client id and its email must be Google-verified. The frontend's
knowledge of "am I admin" only decides which buttons to draw.
