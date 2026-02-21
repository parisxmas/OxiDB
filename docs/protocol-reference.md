# TCP Protocol Command Reference

This document describes every command supported by the OxiDB server protocol.

## Protocol Format

Communication uses length-prefixed JSON over TCP:

```
[4 bytes: payload length as u32 little-endian][JSON payload]
```

Maximum message size: **16 MiB**.

### Request Format

Every request must include a `command` field:

```json
{"command": "insert", "collection": "users", "doc": {"name": "Alice"}}
```

### Response Format

Success:

```json
{"ok": true, "data": <result>}
```

Error:

```json
{"ok": false, "error": "error description"}
```

## Command Reference

### Authentication

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `ping` | - | - | `"pong"` | Any |
| `authenticate` | `payload` | - | SCRAM server-first message | Any |
| `authenticate_continue` | `payload` | - | SCRAM server-final message | Any |
| `auth_simple` | `username`, `password` | - | `{"ok": true}` | Any |

### User Management

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `create_user` | `username`, `password` | `role` (default: `"read"`) | `{"ok": true}` | Admin |
| `drop_user` | `username` | - | `{"ok": true}` | Admin |
| `update_user` | `username` | `password`, `role` | `{"ok": true}` | Admin |
| `list_users` | - | - | `[{"username": "...", "role": "..."}]` | Admin |

### Collections

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `create_collection` | `collection` | - | `{"ok": true}` | ReadWrite |
| `list_collections` | - | - | `["col1", "col2", ...]` | Read |
| `drop_collection` | `collection` | - | `{"ok": true}` | ReadWrite |
| `compact` | `collection` | - | `{"old_size": N, "new_size": N, "docs_kept": N}` | ReadWrite |

### CRUD

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `insert` | `collection`, `doc` | - | `{"id": N}` | ReadWrite |
| `insert_many` | `collection`, `docs` | - | `{"ids": [N, ...]}` | ReadWrite |
| `find` | `collection` | `query`, `sort`, `skip`, `limit` | `[{doc}, ...]` | Read |
| `find_one` | `collection` | `query` | `{doc}` or `null` | Read |
| `update` | `collection`, `query`, `update` | - | `{"modified": N}` | ReadWrite |
| `update_one` | `collection`, `query`, `update` | - | `{"modified": N}` | ReadWrite |
| `delete` | `collection`, `query` | - | `{"deleted": N}` | ReadWrite |
| `delete_one` | `collection`, `query` | - | `{"deleted": N}` | ReadWrite |
| `count` | `collection` | `query` | `N` | Read |

#### find Options

The `find` command supports these optional top-level fields:

```json
{
  "command": "find",
  "collection": "users",
  "query": {"status": "active"},
  "sort": {"name": 1, "age": -1},
  "skip": 20,
  "limit": 10
}
```

- `sort`: Object mapping field names to `1` (ascending) or `-1` (descending)
- `skip`: Number of documents to skip (integer)
- `limit`: Maximum documents to return (integer)

#### query Syntax

See [Querying Documents](queries.md) for the full query operator reference.

#### update Syntax

See [Updating Documents](updates.md) for the full update operator reference.

### Indexes

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `create_index` | `collection`, `field` | - | `{"ok": true}` | ReadWrite |
| `create_unique_index` | `collection`, `field` | - | `{"ok": true}` | ReadWrite |
| `create_composite_index` | `collection`, `fields` | - | `{"index": "field1_field2_..."}` | ReadWrite |
| `create_text_index` | `collection`, `fields` | - | `{"ok": true}` | ReadWrite |
| `list_indexes` | `collection` | - | `[{"field": "...", "type": "..."}]` | Read |
| `drop_index` | `collection`, `index` | - | `{"ok": true}` | ReadWrite |

### Full-Text Search

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `text_search` | `collection`, `query` | `limit` (default: 10) | `[{doc with _score}, ...]` | Read |
| `search` | `query` | `bucket`, `limit` (default: 10) | `[{"bucket": "...", "key": "...", "score": N}]` | Read |

### Vector Search

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `create_vector_index` | `collection`, `field`, `dimension` | `metric` (default: `"cosine"`) | `{"ok": true}` | ReadWrite |
| `vector_search` | `collection`, `field`, `vector` | `limit` (default: 10), `ef_search` | `[{doc with _similarity, _distance}, ...]` | Read |

Supported `metric` values: `"cosine"`, `"euclidean"`, `"dot_product"`. See [Vector Search](vector-search.md).

### Aggregation

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `aggregate` | `collection`, `pipeline` | - | `[{doc}, ...]` | Read |

The `pipeline` field is an array of stage objects. See [Aggregation](aggregation.md) for the full stage reference.

### SQL

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `sql` | `query` | - | Varies by statement type | ReadWrite |

Return values by statement type:

- **SELECT**: `[{doc}, ...]`
- **INSERT**: `{"id": N}` or `{"ids": [N, ...]}`
- **UPDATE**: `{"modified": N}`
- **DELETE**: `{"deleted": N}`
- **CREATE TABLE**: `{"ok": true}`
- **DROP TABLE**: `{"ok": true}`
- **CREATE INDEX**: `{"ok": true}`
- **SHOW TABLES**: `["col1", "col2", ...]`

See [SQL](sql.md) for the full SQL syntax reference.

### Transactions

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `begin_tx` | - | - | `{"tx_id": "tx_N"}` | ReadWrite |
| `commit_tx` | - | - | `{"ok": true}` | ReadWrite |
| `rollback_tx` | - | - | `{"ok": true}` | ReadWrite |

After `begin_tx`, all subsequent CRUD operations on the connection execute within the transaction context. See [Transactions](transactions.md).

### Blob Storage

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `create_bucket` | `bucket` | - | `{"ok": true}` | ReadWrite |
| `list_buckets` | - | - | `["bucket1", ...]` | Read |
| `delete_bucket` | `bucket` | - | `{"ok": true}` | ReadWrite |
| `put_object` | `bucket`, `key`, `data` | `content_type`, `metadata` | `{"ok": true}` | ReadWrite |
| `get_object` | `bucket`, `key` | - | `{key, bucket, content, content_type, size, etag, created_at, metadata}` | Read |
| `head_object` | `bucket`, `key` | - | `{key, bucket, content_type, size, etag, created_at, metadata}` | Read |
| `delete_object` | `bucket`, `key` | - | `{"ok": true}` | ReadWrite |
| `list_objects` | `bucket` | `prefix`, `limit` (default: 1000) | `[{key, size, content_type, etag, created_at}]` | Read |

The `data` field in `put_object` must be base64-encoded. The `content` field in `get_object` responses is base64-encoded.

`content_type` defaults to `"application/octet-stream"`. `metadata` is an optional object of string key-value pairs.

### Stored Procedures

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `create_procedure` | `name`, `params`, `steps` | - | `{"ok": true}` | Admin |
| `call_procedure` | `name` | `params` | Procedure return value | ReadWrite |
| `list_procedures` | - | - | `["proc1", "proc2", ...]` | Read |
| `get_procedure` | `name` | - | `{name, params, steps}` | Read |
| `delete_procedure` | `name` | - | `{"ok": true}` | Admin |

See [Stored Procedures](stored-procedures.md) for step types and variable resolution.

### Cron Schedules

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `create_schedule` | `name`, `procedure` | `cron`, `every`, `params`, `enabled` | `{"ok": true}` | Admin |
| `list_schedules` | - | - | `["sched1", ...]` | Read |
| `get_schedule` | `name` | - | `{name, procedure, cron/every, params, enabled, last_run, last_status, last_error, run_count}` | Read |
| `delete_schedule` | `name` | - | `{"ok": true}` | Admin |
| `enable_schedule` | `name` | - | `{"ok": true}` | ReadWrite |
| `disable_schedule` | `name` | - | `{"ok": true}` | ReadWrite |

One of `cron` or `every` must be provided in `create_schedule`. See [Scheduler](scheduler.md).

### Backup and Restore

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `backup` | `path` | - | `{"path": "...", "size_bytes": N, "collections": N}` | Admin |
| `restore` | `archive`, `target` | - | `{"ok": true}` | Admin |

### Change Streams

| Command | Required Fields | Optional Fields | Return | Min Role |
|---------|----------------|-----------------|--------|----------|
| `watch` | - | `collection`, `resume_after` | Stream of events | Admin |

Events are streamed as individual JSON messages. Not supported over TLS. See [Server Configuration](server.md#change-streams).

## RBAC Role Summary

| Role | Access Level |
|------|-------------|
| **Admin** | All commands |
| **ReadWrite** | CRUD, transactions, indexes, collections, blobs, search, aggregation, SQL, `call_procedure`, `enable_schedule`, `disable_schedule`, `create_vector_index`, `vector_search` |
| **Read** | `find`, `find_one`, `count`, `aggregate`, `text_search`, `search`, `vector_search`, `list_*`, `get_*`, `head_object` |

## See Also

- [Getting Started](getting-started.md) -- protocol overview and first connection
- [Client Libraries](client-libraries.md) -- language-specific clients that wrap this protocol
- [Server Configuration](server.md) -- authentication, TLS, and RBAC setup
