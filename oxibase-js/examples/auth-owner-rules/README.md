# Per-project end-user auth + owner-only rules

The Supabase **auth + RLS** story, per OxiBase project: a project's own users
sign up against it, get a token signed with the **project's key**, and security
rules enforce per-user, per-row authorization.

`quickstart.mjs` runs a shared task board where any signed-in user reads the
board, but may only create rows they own and update/delete their own — enforced
by rules on the server, not the client.

## How it works

- **`oxibase.auth.signUp / signInWithPassword`** hit the **control plane**
  (`authUrl`), which mints an **ES256 token signed with the project's private
  key**, carrying the user's email as `sub` and `role: "authenticated"`.
- The **data plane** verifies that token with the project's *public* key alone
  (no secret) and exposes `auth.username` (email) + `auth.role` to rules.
- After login the client's `.from()` / `.sql()` run **as that user** until
  `signOut()`.

Rules (installed once by the operator with the service_role key):

| op | rule | effect |
| --- | --- | --- |
| read | `auth.role == 'authenticated'` | only signed-in users (not the anon key) |
| create | `auth.username == newDoc.owner` | may only create rows they own |
| update | `auth.username == doc.owner` | may only edit their own (per row) |
| delete | `auth.username == doc.owner` | may only delete their own (per row) |

`service_role` bypasses rules entirely (Supabase semantic) — used here only for
the operator's rule install and cleanup, never in the browser.

## Run

```bash
cd oxibase-js && npm run build

OXIBASE_URL=http://127.0.0.1:8087 \        # data plane
OXIBASE_AUTH_URL=http://127.0.0.1:4460 \   # control plane (for .auth)
OXIBASE_REF=<ref> \
OXIBASE_ANON_KEY=<anon key> \
OXIBASE_SERVICE_ROLE_KEY=<service_role key> \
  node examples/auth-owner-rules/quickstart.mjs
```

Get the ref and keys from the OxiBase dashboard (Open a project → API keys).

## Client sketch

```ts
import { createClient } from "oxibase-js";

const oxibase = createClient(DATA_URL, ANON_KEY, {
  ref: PROJECT_REF,
  authUrl: CONTROL_PLANE_URL,   // where .auth lives
});

await oxibase.auth.signUp({ email, password });     // or signInWithPassword
// now every call runs as this user:
await oxibase.from("tasks").insert({ title, owner: email });
oxibase.auth.signOut();                              // back to the anon key
```

## Scope note

Row-level **read** filtering ("see only my rows" on `select`) is done by the app
(`.eq("owner", me)`): read rules are evaluated at the collection level. Owner-only
**writes** (create/update/delete) are enforced per-row by the rules themselves.
