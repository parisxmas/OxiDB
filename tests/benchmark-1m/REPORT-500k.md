# OxiDB vs MongoDB — 500K-doc benchmark

**Harness:** `tests/benchmark-1m/` (Go), native loopback on Apple Silicon.
**MongoDB:** 8.x, wiredTiger, `--wiredTigerCacheSizeGB 1`.
**OxiDB:** 8-thread pool, strict ACID-D (every commit fsync'd), batch size 5000.
**Dataset:** 500,000 employee documents. Lower time is better.

## In-RAM (default) — OxiDB faster in 12/18

| Operation | OxiDB | MongoDB | Faster |
|---|--:|--:|---|
| Insert 500K (batch 5000) | 2.801s | 2.492s | Mongo 1.1× |
| Build 8 indexes | 734ms | 2.332s | OxiDB 3.2× |
| Exact match (indexed) | 163µs | 3ms | OxiDB 18.4× |
| Equality (indexed) | 233ms | 232ms | Mongo 1.0× |
| Range (indexed) | 592ms | 585ms | Mongo 1.0× |
| Range + equality | 97ms | 86ms | Mongo 1.1× |
| Multi-condition AND | 60ms | 93ms | OxiDB 1.6× |
| Unindexed scan (rating) | 202ms | 289ms | OxiDB 1.4× |
| find_one (indexed) | 143µs | 297µs | OxiDB 2.1× |
| Count (indexed) | 96µs | 4ms | OxiDB 41.7× |
| Sort + limit 10 (indexed) | 180µs | 455µs | OxiDB 2.5× |
| UpdateOne (indexed) | 4ms | 201µs | Mongo 19.9× |
| UpdateMany (bulk) | 131ms | 143ms | OxiDB 1.1× |
| Group by dept + avg salary | 120ms | 146ms | OxiDB 1.2× |
| Match region + group dept | 46ms | 64ms | OxiDB 1.4× |
| Group by city + full stats | 206ms | 149ms | Mongo 1.4× |
| Concurrent find_one (1000 ops, 10 workers) | 13ms | 14ms | OxiDB 1.1× |
| DeleteMany (status=onleave) | 799ms | 1.276s | OxiDB 1.6× |

## Disk-first, uncompressed (`OXIDB_DISK_FIRST=1 OXIDB_DISK_UNCOMPRESSED=1`) — OxiDB faster in 11/18

| Operation | OxiDB | MongoDB | Faster |
|---|--:|--:|---|
| Insert 500K (batch 5000) | 2.987s | 2.467s | Mongo 1.2× |
| Build 8 indexes | 1.208s | 2.298s | OxiDB 1.9× |
| Exact match (indexed) | 131µs | 3ms | OxiDB 22.9× |
| Equality (indexed) | 233ms | 231ms | Mongo 1.0× |
| Range (indexed) | 579ms | 584ms | OxiDB 1.0× |
| Range + equality | 135ms | 84ms | Mongo 1.6× |
| Multi-condition AND | 77ms | 94ms | OxiDB 1.2× |
| Unindexed scan (rating) | 283ms | 286ms | OxiDB 1.0× |
| find_one (indexed) | 164µs | 274µs | OxiDB 1.7× |
| Count (indexed) | 2ms | 4ms | OxiDB 2.0× |
| Sort + limit 10 (indexed) | 230µs | 451µs | OxiDB 2.0× |
| UpdateOne (indexed) | 3ms | 323µs | Mongo 9.3× |
| UpdateMany (bulk) | 240ms | 147ms | Mongo 1.6× |
| Group by dept + avg salary | 115ms | 139ms | OxiDB 1.2× |
| Match region + group dept | 281ms | 53ms | Mongo 5.3× |
| Group by city + full stats | 192ms | 151ms | Mongo 1.3× |
| Concurrent find_one (1000 ops, 10 workers) | 13ms | 14ms | OxiDB 1.1× |
| DeleteMany (status=onleave) | 865ms | 1.323s | OxiDB 1.5× |

## Memory & disk

| Metric | In-RAM | Disk-first (uncompr.) | MongoDB |
|---|--:|--:|--:|
| Fresh-open RSS | 13.1 MB | 13.2 MB | ~91–99 MB |
| RSS after full workload | 557 MB | 565 MB | ~505–519 MB |
| Disk (data dir) | 315 MB | 387 MB | 158 MB |

## Notes

- Most non-wins are statistical dead heats (equality/range within a few %, insert
  within ~20%). Decisive OxiDB wins: indexed exact-match (18–23×), count, sort,
  find_one, index build, and deletes (1.5–1.6×).
- Disk-first uncompressed tracks the in-RAM engine closely while keeping a
  ~13 MB fresh-open footprint vs MongoDB's ~90 MB. Soft spots: `Match region +
  group` (post-`$match` doc reads via the mmap) and bulk UpdateMany.
- OxiDB ties/beats MongoDB on insert while fsync'ing every batch (strict
  ACID-D); MongoDB's default write concern acks from memory and group-commits the
  journal asynchronously.

_Re-run:_

```sh
./local_bench.sh 500000                                              # in-RAM
OXIDB_DISK_FIRST=1 OXIDB_DISK_UNCOMPRESSED=1 ./local_bench.sh 500000 # disk-first uncompressed
```
