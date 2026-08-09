## 0.43.2

License rewritten commercial-first: use in anything shipped to third
parties requires a commercial license (contact in LICENSE); evaluation
before purchase is a narrow permission, not a headline.

## 0.43.1

Licensing stated precisely: evaluation and development are free; shipping
an app that includes this package distributes the engine and requires a
commercial license (the engine's own source-available rule, stated up
front because embedding is what this package is for).

## 0.43.0

Version aligned with the OxiDB engine release line — this package wraps the
engine itself, so its version now states which engine it is. No API changes
from 0.1.0.

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
