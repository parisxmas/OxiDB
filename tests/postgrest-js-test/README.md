# postgrest-js integration test

Proves that the **real, unmodified [`@supabase/postgrest-js`](https://github.com/supabase/postgrest-js)
client** — the same library `supabase-js` uses — drives OxiDB's
PostgREST-compatible surface (ADR-0019) over **both engines** through the same
base URL (`/rest/v1`):

- **Document collections** — `customers`/`orders` — filters (`gt`, `in`, `or`,
  `like`), ordering, resource embedding (belongs-to + has-many), and
  insert/update/delete with `return=representation`.
- **SQL tables** — `authors`/`books` — the identical client calls, routed to the
  SQL engine, including embedding inferred from the declared `REFERENCES`
  foreign key.

## Run

```bash
npm install

# Start an OxiDB server with SQL enabled and the REST listener on :14590
OXIDB_SQL=1 OXIDB_HTTP_PORT=14590 OXIDB_ADDR=127.0.0.1:14591 \
  OXIDB_DATA=/tmp/oxidb-pgjs cargo run --release -p oxidb-server &

OXIDB_REST_URL=http://127.0.0.1:14590 node test.mjs
```

Exits `0` when all assertions pass, `1` on the first failure.

> The only raw call is a `POST /api/sql` to issue the `CREATE TABLE` DDL that
> sets up the SQL-engine tables; every data operation goes through the real
> `PostgrestClient`.
