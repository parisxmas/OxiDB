## 0.1.0

Initial release: the OxiDB engine in-process for Flutter and Dart over
`dart:ffi` — no server, no network.

- CRUD with the full query language ($eq…$expr, $elemMatch, $regex, …)
  and upsert.
- Every index type: field, unique, composite, text (BM25), geo
  ($near/$geoWithin/$geoNear), TTL.
- Aggregation pipeline incl. $group, $lookup, $facet, $geoNear.
- ACID transactions, blob buckets, the SQL engine surface.
- AES-256-GCM encryption at rest with a 32-byte platform-keystore key.
- `Preferences` key-value sugar; `OxiDb.background()` runs the engine on
  a worker isolate behind a Future API that mirrors the sync one.
- Native library discovery via bundled binaries or the `OXIDB_FFI_LIB`
  override.
