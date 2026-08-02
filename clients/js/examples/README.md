# OxiDB Node.js examples

15 self-contained CommonJS scripts using the `oxidb` SDK. Each runs in
isolation against a local `oxidb-server` with REST + WebSocket + JWT
enabled:

```bash
OXIDB_HTTP_PORT=9080  \
OXIDB_WS_PORT=9082    \
OXIDB_JWT_SECRET=demo-secret \
OXIDB_DATA=./oxidb-data       \
oxidb-server &

cd oxidb-js/examples
node 01_hello.js
```

| # | File | What it shows |
|---|---|---|
| 01 | `01_hello.js`               | Smallest possible: connect, ping, insert, find |
| 02 | `02_bulk_insert.js`         | `insertMany` 1 000 docs, count |
| 03 | `03_query_operators.js`     | `$gte` / `$in` / `$or` / `$and` / `$regex` |
| 04 | `04_pagination.js`          | `skip` / `limit` + total `count` |
| 05 | `05_aggregation.js`         | `$match → $group → $sort → $limit` pipeline |
| 06 | `06_atomic_update.js`       | Combine `$inc` + `$push` + `$addToSet` per doc |
| 07 | `07_indexes.js`             | Field / unique / composite indexes; `listIndexes` / `dropIndex` |
| 08 | `08_ttl_index.js`           | Sessions auto-expire via TTL index |
| 09 | `09_sql_query.js`           | `SELECT … GROUP BY` over the same store |
| 10 | `10_oxiscript_proc.js`      | Server-side OxiScript stored procedure |
| 11 | `11_jwt_auth.js`            | `signup` → `login` → `verify` → use token |
| 12 | `12_security_rules.js`      | Per-collection ACL: owner-only updates |
| 13 | `13_realtime_snapshot.js`   | WebSocket `onSnapshot` change events |
| 14 | `14_http_api.js`            | Tiny REST API for a `tasks` collection (no external deps) |
| 15 | `15_chat_room.js`           | Pub/sub-style chat using `onSnapshot` |

For an installed package use `require("oxidb")` instead of `require("../index.js")`.
