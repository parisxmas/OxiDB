# Task-tracker — a fuller oxibase-js walkthrough

A single runnable script (`quickstart.mjs`) that exercises the whole client
against a real OxiBase project: the document engine (bulk insert, filters,
ordering, `range` pagination, resource embedding, update/delete), the SQL engine
(`.sql` with bound params + `GROUP BY`), the time-series engine
(`.schema("tsdb")`), and `{ data, error }` handling.

Unlike [`../notes`](../notes) (a browser app with the anon key), this is a
**server-side** script using the **service_role** key — the shape of a backend
job or API route.

## Run

```bash
cd oxibase-js
npm run build          # produces dist/ that the example imports

OXIBASE_URL=http://127.0.0.1:8087 \
OXIBASE_REF=<project ref> \
OXIBASE_KEY=<service_role key> \
  node examples/task-tracker/quickstart.mjs
```

Get `ref` and the `service_role` key from the OxiBase dashboard (Open a project →
API keys). The SQL section needs the data plane started with `OXIDB_SQL=1`, and
the time-series section needs `OXIDB_TSDB=1`; both skip themselves cleanly when
the engine is off.

## What it shows

| § | Feature | API |
| --- | --- | --- |
| 1 | Bulk insert + return rows | `.from(t).insert([...]).select()` |
| 2 | Foreign-key rows | `<singular(parent)>_id` link field |
| 3 | Filter + order + paginate | `.neq().order().range(0,1)` |
| 4 | Resource embedding (belongs-to) | `.select("*, parent(cols)")` |
| 5 | Update / delete | `.update().eq()` · `.delete().eq()` |
| 6 | SQL analytics | `.sql("… GROUP BY …", params)` |
| 7 | Time-series | `.schema("tsdb").from(m).insert/select` |

Each object is namespaced with a per-run tag and cleaned up at the end, so the
script is safe to run repeatedly against a shared project.
