# Memory: OxiDB vs PostgreSQL over the same 1,000,000 rows

Both engines get the same schema, the same rows, and the same client. OxiDB
speaks the PostgreSQL v3 wire ([ADR-0023](decisions/0023-postgres-wire-protocol.md)),
so `psql` loads both from one file over one protocol — nothing about the load
path differs.

```bash
bench/pg-memory/run.sh [workdir]
```

## The dataset

1,000,000 rows across five related tables, chosen so every index shape that
costs an engine memory is present ([`schema.sql`](../bench/pg-memory/schema.sql),
[`gen.py`](../bench/pg-memory/gen.py), fixed seed):

| Table | Rows | Keys and indexes |
|---|---:|---|
| `customers` | 200,000 | surrogate PK, `UNIQUE` email, 1 single + 1 multi-column index |
| `products` | 50,000 | surrogate PK, `UNIQUE` sku, 1 single + 1 multi-column index |
| `orders` | 400,000 | surrogate PK, **FK** → customers, 1 single + 1 multi-column index |
| `order_items` | 300,000 | **composite PK** `(order_id, line_no)`, **FK** → orders, 1 index |
| `inventory` | 50,000 | **composite PK** `(product_id, warehouse)`, **FK** → products, 1 index |

Five primary keys (two composite), three enforced foreign keys, two unique
columns, eight secondary indexes (three multi-column).

## How memory is measured

macOS `phys_footprint` (what Activity Monitor calls Memory), summed over each
engine's process family. **Not `ps rss`**: a shared page is counted once per
process that maps it, so summing RSS over PostgreSQL's nine processes counts
`shared_buffers` nine times. The script prints both, and the gap is the reason
to distrust the RSS column — at boot it reads 64 MB against a true 39 MB.

PostgreSQL 18.4 runs stock: `shared_buffers=128MB`, `max_connections=100`.

## Results

| | PostgreSQL 18.4 | OxiDB (resident) | OxiDB (disk-first) |
|---|---:|---:|---:|
| Boot, empty database | 39 MB | **4 MB** | **4 MB** |
| After loading 1M rows | 70 MB | 870 MB | 693 MB |
| Restart, before any query | 26 MB | *4 MB** | *4 MB** |
| **Warm — every table read** | **110 MB** | 620 MB | 423 MB |
| Processes | 9 | **1** | **1** |
| Load time | **7.5 s** | 21 s | 22 s |
| Data directory on disk | 552 MB | **81 MB** | **81 MB** |
| Tables + indexes on disk | 153 MB | **81 MB** | **81 MB** |

\** Lazy open — see below. This number is real but does not mean what it looks
like, and should not be quoted on its own.

## Read this before quoting the startup row

**OxiDB's 4 MB after a restart is not efficiency; it is not having opened the
database yet.** The SQL engine is created on the first statement that needs it,
so a freshly restarted server that has been asked nothing is holding a listener
and nothing else. One `SELECT 1` — which touches no table — takes it straight to
620 MB, because opening the engine loads the catalog, replays the WAL tail and
rebuilds every index:

```
after boot (no query):            4368 KB
after 'SELECT 1' (engine open):    620 MB
after COUNT(*) FROM orders:        620 MB
```

PostgreSQL's 26 MB at the same point is a server that has already opened its
cluster and is ready to serve. Comparing the two is comparing a shop before it
unlocks with one that is open. The **warm** row is the comparison that means
something.

## What the warm row says

**On this schema OxiDB uses 4-6× more memory than PostgreSQL** — 620 MB
resident, 423 MB disk-first, against 110 MB. The reason is structural, not a
tuning gap:

- **PostgreSQL has a bounded buffer pool.** `shared_buffers` is a 128 MB cap,
  and pages — table *and* index — are read into it on demand and evicted under
  pressure. Its memory is a function of the configured cache, not the dataset.
- **OxiDB's SQL engine keeps every index and primary-key map fully resident,**
  and `OXIDB_SQL_DISK_FIRST` does not change that: it moves *row data* to an
  mmap'd snapshot (620 → 423 MB here) while keys stay in RAM by design.

Splitting the disk-first figure by dropping the eight secondary indexes and
restarting attributes it exactly:

| Component | Memory |
|---|---:|
| 8 secondary indexes | 230 MB |
| 5 primary-key maps + row offsets + catalog | 193 MB |
| **Total (disk-first, 1M rows)** | **423 MB** |

Two things stand out. 230 MB for eight indexes is ~29 bytes per entry, which is
`BTreeMap` overhead more than key data. And 193 MB of primary-key maps over 1M
rows is heavy for what is mostly integer keys — the two composite keys are
`Vec<IndexKey>`, one heap allocation per key, which is where a 350,000-row
composite-PK table gets expensive.

## What OxiDB does win

- **Startup floor**: 4 MB against 39 MB for an empty database, and one process
  against nine. For many small databases per host — the OxiBase multi-tenant
  case — that per-instance floor matters more than the warm figure.
- **Disk**: 81 MB against 552 MB for the whole data directory, and against
  153 MB counting only tables and indexes. Roughly half the bytes for the same
  million rows, before counting PostgreSQL's WAL and free-space maps.

## What this benchmark does not measure

Query speed (see [the wire benchmark](wire-benchmark.md)), concurrency, larger
datasets, or a tuned PostgreSQL. It also flatters neither engine on load time:
7.5 s against 21 s is real, but both went through `psql` with multi-row
`INSERT`s — PostgreSQL's `COPY` would be far faster and OxiDB has no equivalent.

The honest summary is that OxiDB is small until it opens a database and large
afterwards, because it holds keys in memory where PostgreSQL holds a capped
cache of pages. Below roughly a few hundred thousand rows, or across many small
databases, that trades well. On an index-heavy million-row table it does not.
