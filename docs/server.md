# Server Configuration and Security

The OxiDB server (`oxidb-server`) is a standalone TCP server that provides authenticated, encrypted access to OxiDB. It supports SCRAM-SHA-256 authentication, role-based access control, TLS encryption, audit logging, and Raft-based clustering.

## Starting the Server

```bash
./oxidb-server
```

With verbose startup logging:

```bash
./oxidb-server --verbose
```

## Environment Variables

All configuration is done through environment variables.

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_ADDR` | `127.0.0.1:4444` | TCP listen address and port |
| `OXIDB_DATA` | `./oxidb_data` | Data directory path |
| `OXIDB_POOL_SIZE` | `4` | Number of worker threads |
| `OXIDB_IDLE_TIMEOUT` | `30` | Connection idle timeout in seconds (0 = no timeout) |
| `OXIDB_VERBOSE` | - | Enable with `--verbose` flag |

### Security Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_AUTH` | `false` | Enable SCRAM-SHA-256 authentication |
| `OXIDB_ENCRYPTION_KEY` | - | Path to 32-byte AES-256 key file |
| `OXIDB_TLS_CERT` | - | Path to TLS certificate PEM file |
| `OXIDB_TLS_KEY` | - | Path to TLS private key PEM file |
| `OXIDB_AUDIT` | `false` | Enable audit logging |

### Logging Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_GELF_ADDR` | - | GELF UDP endpoint for remote logging (e.g., `172.17.0.1:12201`) |

### Clustering Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_NODE_ID` | - | Numeric node ID (enables Raft cluster mode) |
| `OXIDB_RAFT_ADDR` | `127.0.0.1:4445` | Raft inter-node communication address |
| `OXIDB_RAFT_PEERS` | - | Comma-separated peer list: `1=host1:4445,2=host2:4445` |

### Example: Production Configuration

```bash
OXIDB_ADDR=0.0.0.0:4444 \
OXIDB_DATA=/var/lib/oxidb \
OXIDB_POOL_SIZE=16 \
OXIDB_IDLE_TIMEOUT=60 \
OXIDB_AUTH=true \
OXIDB_TLS_CERT=/etc/oxidb/server.pem \
OXIDB_TLS_KEY=/etc/oxidb/server-key.pem \
OXIDB_ENCRYPTION_KEY=/etc/oxidb/encryption.key \
OXIDB_AUDIT=true \
./oxidb-server
```

## Authentication

### SCRAM-SHA-256

When `OXIDB_AUTH=true`, all clients must authenticate before executing commands (except `ping`).

OxiDB implements SCRAM-SHA-256 (RFC 7677) with the following flow:

1. Client sends: `{"command": "authenticate", "payload": "n,,n=<username>,r=<client_nonce>"}`
2. Server responds with salt, iteration count (4096), and combined nonce
3. Client sends: `{"command": "authenticate_continue", "payload": "c=biws,r=<combined_nonce>,p=<proof>"}`
4. Server verifies proof and returns server signature

Passwords are stored hashed with Argon2.

### Simple Authentication

For simpler integrations, a direct authentication method is also available:

```json
{"command": "auth_simple", "username": "admin", "password": "secret"}
```

### Default Admin Account

On first startup with auth enabled, a random 24-character admin password is generated and printed to stdout. Store it securely.

## RBAC (Role-Based Access Control)

Three roles control command access:

### Admin

Full access to all commands, including user management, backup/restore, and watch.

### ReadWrite

CRUD operations, transactions, indexes, collections, aggregation, blobs, search, SQL, procedure calls, and schedule enable/disable.

### Read

Read-only access: `find`, `find_one`, `count`, `aggregate`, `list_collections`, `list_buckets`, `get_object`, `head_object`, `list_objects`, `search`, `list_procedures`, `get_procedure`, `list_schedules`, `get_schedule`.

### Permission Matrix

| Category | Commands | Admin | ReadWrite | Read |
|----------|----------|-------|-----------|------|
| CRUD | insert, insert_many, update, update_one, delete, delete_one | Yes | Yes | No |
| Queries | find, find_one, count | Yes | Yes | Yes |
| Indexes | create_index, create_unique_index, create_composite_index, create_text_index, drop_index | Yes | Yes | No |
| Indexes | list_indexes | Yes | Yes | Yes |
| Collections | create_collection, drop_collection, compact | Yes | Yes | No |
| Collections | list_collections | Yes | Yes | Yes |
| Aggregation | aggregate, text_search | Yes | Yes | Yes |
| Transactions | begin_tx, commit_tx, rollback_tx | Yes | Yes | No |
| Blobs | create_bucket, delete_bucket, put_object, delete_object | Yes | Yes | No |
| Blobs | list_buckets, list_objects, get_object, head_object | Yes | Yes | Yes |
| Search | search | Yes | Yes | Yes |
| SQL | sql | Yes | Yes | No |
| Procedures | create_procedure, delete_procedure | Yes | No | No |
| Procedures | call_procedure | Yes | Yes | No |
| Procedures | list_procedures, get_procedure | Yes | No | Yes |
| Schedules | create_schedule, delete_schedule | Yes | No | No |
| Schedules | enable_schedule, disable_schedule | Yes | Yes | No |
| Schedules | list_schedules, get_schedule | Yes | No | Yes |
| User Mgmt | create_user, drop_user, update_user, list_users | Yes | No | No |
| Backup | backup, restore | Yes | No | No |
| Watch | watch | Yes | No | No |

## User Management

Requires Admin role.

### Create User

```json
{"command": "create_user", "username": "analyst", "password": "secure_password", "role": "read"}
```

The `role` field defaults to `"read"` if not specified. Valid roles: `"admin"`, `"readwrite"`, `"read"`.

### Update User

```json
{"command": "update_user", "username": "analyst", "role": "readwrite"}
```

Can update `password`, `role`, or both.

### Drop User

```json
{"command": "drop_user", "username": "analyst"}
```

### List Users

```json
{"command": "list_users"}
```

Returns usernames and roles (never passwords).

## TLS

Enable TLS by providing certificate and key files in PEM format:

```bash
OXIDB_TLS_CERT=/path/to/server.pem OXIDB_TLS_KEY=/path/to/server-key.pem ./oxidb-server
```

Both variables must be set for TLS to activate. The server uses Rustls for the TLS implementation.

### Generating a Self-Signed Certificate

```bash
openssl req -x509 -newkey rsa:4096 -keyout server-key.pem -out server.pem -days 365 -nodes
```

## Encryption at Rest

Encrypt all data on disk with AES-256-GCM by providing a 32-byte key file:

```bash
# Generate a random 32-byte key
head -c 32 /dev/urandom > /etc/oxidb/encryption.key
chmod 600 /etc/oxidb/encryption.key

# Start with encryption
OXIDB_ENCRYPTION_KEY=/etc/oxidb/encryption.key ./oxidb-server
```

Encryption is applied transparently to:
- Document storage files
- WAL entries
- Blob data and metadata files

Each encryption operation uses a random 12-byte nonce. The format is `[nonce:12 bytes][ciphertext + GCM tag]`.

## Audit Logging

Enable audit logging to track all operations:

```bash
OXIDB_AUDIT=true ./oxidb-server
```

Audit logs are written to `{data_dir}/_audit/audit.log` with the format:

```
[timestamp] user=<username> cmd=<command> collection=<collection> result=<ok|denied> detail=<info>
```

Timestamps use RFC 3339 format. Permission denials are logged with their details.

## GELF Logging

Ship logs to a centralized logging system (e.g., Graylog) via GELF over UDP:

```bash
OXIDB_GELF_ADDR=172.17.0.1:12201 ./oxidb-server
```

## Clustering

OxiDB supports Raft-based clustering for high availability using the OpenRaft library.

### Configuration

Set up a 3-node cluster:

**Node 1:**
```bash
OXIDB_NODE_ID=1 OXIDB_ADDR=0.0.0.0:4444 OXIDB_RAFT_ADDR=0.0.0.0:4445 \
OXIDB_RAFT_PEERS="1=node1:4445,2=node2:4445,3=node3:4445" ./oxidb-server
```

**Node 2:**
```bash
OXIDB_NODE_ID=2 OXIDB_ADDR=0.0.0.0:4444 OXIDB_RAFT_ADDR=0.0.0.0:4445 \
OXIDB_RAFT_PEERS="1=node1:4445,2=node2:4445,3=node3:4445" ./oxidb-server
```

**Node 3:**
```bash
OXIDB_NODE_ID=3 OXIDB_ADDR=0.0.0.0:4444 OXIDB_RAFT_ADDR=0.0.0.0:4445 \
OXIDB_RAFT_PEERS="1=node1:4445,2=node2:4445,3=node3:4445" ./oxidb-server
```

### Raft Timing

| Parameter | Value |
|-----------|-------|
| Heartbeat interval | 500 ms |
| Election timeout (min) | 1500 ms |
| Election timeout (max) | 3000 ms |

The cluster supports automatic leader election and sub-second failover.

## Backup and Restore

### Backup

Create a backup archive (Admin only):

```json
{"command": "backup", "path": "/backups/oxidb_backup.tar.gz"}
```

Response:

```json
{"ok": true, "data": {"path": "/backups/oxidb_backup.tar.gz", "size_bytes": 1048576, "collections": 5}}
```

### Restore

Restore from a backup archive (Admin only):

```json
{"command": "restore", "archive": "/backups/oxidb_backup.tar.gz", "target": "/var/lib/oxidb_restored"}
```

A server restart is required after restore to load the restored data.

## Change Streams

Watch for real-time changes to collections (Admin only):

```json
{"command": "watch", "collection": "orders"}
```

Watch all collections:

```json
{"command": "watch"}
```

With resume token (to resume after disconnection):

```json
{"command": "watch", "collection": "orders", "resume_after": "token_value"}
```

Events are streamed as they occur:

```json
{"event": "insert", "collection": "orders", "document": {...}, "timestamp": "2025-03-15T10:30:00Z"}
{"event": "update", "collection": "orders", "document": {...}, "timestamp": "2025-03-15T10:30:01Z"}
{"event": "delete", "collection": "orders", "document_id": 42, "timestamp": "2025-03-15T10:30:02Z"}
```

Change streams are not supported over TLS connections.

## See Also

- [Getting Started](getting-started.md) -- basic server setup
- [Protocol Reference](protocol-reference.md) -- complete command reference
- [Client Libraries](client-libraries.md) -- connecting from different languages
