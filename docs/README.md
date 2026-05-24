# OxiDB Documentation

OxiDB is a fast, embeddable document database engine written in Rust. It supports JSON-based queries, ACID transactions, full-text search, blob storage, stored procedures, and more. It can run as an embedded library, a standalone TCP server, or be accessed via client libraries in Python, Go, Julia, .NET, and Swift.

## Quick Start

```bash
# Start the server
./oxidb-server

# Insert a document (Python)
from oxidb import OxiDbClient
with OxiDbClient() as db:
    db.insert("users", {"name": "Alice", "age": 30})
    print(db.find("users", {"name": "Alice"}))
```

## Documentation

### Getting Started

- **[Getting Started](getting-started.md)** -- Installation, starting the server, connecting, and first operations with examples in all 6 client languages.

### Core Operations

- **[Querying Documents](queries.md)** -- Comparison operators (`$eq`, `$ne`, `$gt`, `$lt`, `$in`, `$regex`, ...), logical operators (`$and`, `$or`), dot notation, sort/skip/limit, find_one, count.

- **[Updating Documents](updates.md)** -- Field operators (`$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate`) and array operators (`$push`, `$pull`, `$addToSet`, `$pop`).

- **[Indexes](indexes.md)** -- Field, unique, composite, text, and vector indexes. Value ordering, auto date detection, and persistent index cache.

- **[Vector Search](vector-search.md)** -- Vector similarity search with cosine, Euclidean, and dot product metrics. Flat (exact) and HNSW (approximate) algorithms.

- **[Transactions](transactions.md)** -- Optimistic concurrency control (OCC), begin/commit/rollback, transaction helpers per language, conflict handling and retry patterns.

### Advanced Features

- **[Aggregation Pipeline](aggregation.md)** -- Pipeline stages (`$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`), accumulators, and arithmetic expressions.

- **[Blob Storage](blobs.md)** -- S3-style bucket and object storage with full-text search on stored documents (PDF, DOCX, XLSX, HTML, images with OCR).

- **[Stored Procedures](stored-procedures.md)** -- JSON-defined multi-step workflows with variables, conditionals, and automatic transaction wrapping.

- **[Cron Scheduler](scheduler.md)** -- Schedule stored procedures with cron expressions or interval strings.

### Operations

- **[Server Configuration](server.md)** -- Environment variables, SCRAM-SHA-256 authentication, RBAC roles, TLS, encryption at rest (AES-256-GCM), audit logging, GELF logging, Raft clustering, backup/restore, change streams.

### Reference

- **[Client Libraries](client-libraries.md)** -- Per-language setup, connection, error handling, transaction helpers, blob handling, and complete API method tables for Python, Go, Julia, .NET, and Swift.

- **[Protocol Reference](protocol-reference.md)** -- Complete TCP protocol command reference with all fields, return values, and RBAC role requirements.

### 1.0 release policy

These four documents define what the 1.0 stability promise covers and how it
will be operated. They derive from [ADR-0003](decisions/0003-1.0-stability-scope.md)
(scope) and [ADR-0004](decisions/0004-phase-0-answers.md) (release policy).

- **[Semantic Versioning](SEMVER.md)** -- What counts as patch / minor / major; the additive vs breaking change rules.

- **[Stability Policy](STABILITY.md)** -- The 1.0 stable surface (engine, wire, on-disk, env vars, Tier-A clients) vs experimental surface; promotion criteria.

- **[Deprecation Policy](DEPRECATION.md)** -- Notice period, code markers, removal timing, and the special-case rules for reserved operator names and security removals.

- **[Security Policy](SECURITY.md)** -- Reporting channel (GitHub Security Advisories), disclosure timeline, supported-versions backport matrix, and operational hardening checklist.
