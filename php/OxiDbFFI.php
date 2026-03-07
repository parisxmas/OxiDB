<?php
/**
 * OxiDB PHP FFI Client Library
 *
 * Drop-in replacement for OxiDbClient that uses PHP FFI to call the native
 * Rust client library (liboxidb_client_ffi) instead of pure PHP sockets.
 *
 * Benefits over the pure PHP OxiWire client:
 *   - Native TCP handling (no PHP socket_* overhead)
 *   - Rust-speed JSON serialization
 *   - Zero-copy wire framing
 *
 * Requires: PHP 8.0+ with FFI extension enabled.
 *
 * Usage:
 *   $db = new OxiDbFFIClient('127.0.0.1', 4444);
 *   $db->insert('users', ['name' => 'Alice', 'age' => 30]);
 *   $docs = $db->find('users', ['name' => 'Alice']);
 *   $db->close();
 */

if (!class_exists('OxiDbException')) {
    class OxiDbException extends \RuntimeException {}
}
if (!class_exists('TransactionConflictException')) {
    class TransactionConflictException extends OxiDbException {}
}

class OxiDbFFIClient
{
    private \FFI $ffi;
    private ?\FFI\CData $conn = null;
    private ?\FFI\CData $sendBuf = null;
    private int $sendBufSize = 0;

    private static ?string $libPath = null;

    /**
     * Set the path to the native library before constructing any client.
     * If not set, auto-detects based on OS.
     */
    public static function setLibraryPath(string $path): void
    {
        self::$libPath = $path;
    }

    /**
     * Connect to an OxiDB server via the native FFI client.
     */
    public function __construct(string $host = '127.0.0.1', int $port = 4444)
    {
        if (!extension_loaded('ffi')) {
            throw new OxiDbException('PHP FFI extension is not loaded');
        }

        $libPath = self::resolveLibPath();

        $this->ffi = \FFI::cdef("
            typedef void OxiDbConn;
            typedef struct { uint8_t* data; uint32_t len; } RawResponse;
            OxiDbConn* oxidb_connect(const char* host, uint16_t port);
            void oxidb_disconnect(OxiDbConn* conn);
            char* oxidb_execute(OxiDbConn* conn, const char* cmd_json);
            RawResponse* oxidb_send_raw(OxiDbConn* conn, const uint8_t* data, uint32_t data_len);
            void oxidb_free_raw(RawResponse* ptr);
            void oxidb_free_string(char* ptr);
        ", $libPath);

        $this->conn = $this->ffi->oxidb_connect($host, $port);
        if (\FFI::isNull($this->conn)) {
            throw new OxiDbException("Failed to connect to $host:$port via FFI");
        }
    }

    public function close(): void
    {
        if ($this->conn !== null && !$this->isNullConn()) {
            $this->ffi->oxidb_disconnect($this->conn);
            $this->conn = null;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    // ── Low-level ────────────────────────────────────────────────

    /**
     * Send a command via native FFI using OxiWire binary protocol.
     * Uses oxidb_send_raw for OxiWire encoding → native TCP → OxiWire decoding.
     */
    private function request(array $payload)
    {
        if (class_exists('OxiWire')) {
            return $this->requestOxiWire($payload);
        }
        return $this->requestJson($payload);
    }

    /**
     * OxiWire path: PHP encodes to binary, Rust handles TCP framing,
     * PHP decodes the binary response.
     */
    private function requestOxiWire(array $payload)
    {
        $encoded = OxiWire::encodeRequest($payload);
        $len = strlen($encoded);

        // Reuse pre-allocated buffer, grow if needed
        if ($len > $this->sendBufSize) {
            $newSize = max($len, $this->sendBufSize * 2, 4096);
            $this->sendBuf = $this->ffi->new("uint8_t[$newSize]");
            $this->sendBufSize = $newSize;
        }
        \FFI::memcpy($this->sendBuf, $encoded, $len);

        $resp = $this->ffi->oxidb_send_raw($this->conn, $this->sendBuf, $len);
        if (\FFI::isNull($resp)) {
            throw new OxiDbException('FFI raw call returned NULL (connection error)');
        }

        // Extract response bytes
        $respLen = $resp->len;
        $respData = \FFI::string($resp->data, $respLen);
        $this->ffi->oxidb_free_raw($resp);

        // Decode OxiWire response
        if (OxiWire::isOxiWire($respData)) {
            [$ok, $data] = OxiWire::decodeResponse($respData);
            if (!$ok) {
                $msg = is_string($data) ? $data : (is_array($data) ? ($data['error'] ?? json_encode($data)) : 'unknown error');
                if (stripos($msg, 'conflict') !== false) {
                    throw new TransactionConflictException($msg);
                }
                throw new OxiDbException($msg);
            }
            return $data;
        }

        // Fallback: JSON response
        $data = json_decode($respData, true);
        if (!($data['ok'] ?? false)) {
            $msg = $data['error'] ?? 'unknown error';
            throw new OxiDbException($msg);
        }
        return $data['data'] ?? null;
    }

    /**
     * JSON path: PHP encodes to JSON, Rust handles TCP + JSON framing.
     */
    private function requestJson(array $payload)
    {
        $json = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $ptr = $this->ffi->oxidb_execute($this->conn, $json);

        if (\FFI::isNull($ptr)) {
            throw new OxiDbException('FFI call returned NULL (connection error)');
        }

        $response = \FFI::string($ptr);
        $this->ffi->oxidb_free_string($ptr);

        $data = json_decode($response, true);
        if ($data === null && json_last_error() !== JSON_ERROR_NONE) {
            throw new OxiDbException('Invalid JSON response: ' . json_last_error_msg());
        }

        if (!($data['ok'] ?? false)) {
            $msg = $data['error'] ?? 'unknown error';
            if (stripos($msg, 'conflict') !== false) {
                throw new TransactionConflictException($msg);
            }
            throw new OxiDbException($msg);
        }

        return $data['data'] ?? null;
    }

    // ── Utility ───────────────────────────────────────────────────

    public function ping(): string
    {
        return $this->request(['cmd' => 'ping']);
    }

    // ── Collection management ─────────────────────────────────────

    public function createCollection(string $name)
    {
        return $this->request(['cmd' => 'create_collection', 'collection' => $name]);
    }

    public function listCollections(): array
    {
        return $this->request(['cmd' => 'list_collections']);
    }

    public function dropCollection(string $name)
    {
        return $this->request(['cmd' => 'drop_collection', 'collection' => $name]);
    }

    // ── CRUD ──────────────────────────────────────────────────────

    public function insert(string $collection, array $doc)
    {
        return $this->request(['cmd' => 'insert', 'collection' => $collection, 'doc' => $doc]);
    }

    public function insertMany(string $collection, array $docs)
    {
        return $this->request(['cmd' => 'insert_many', 'collection' => $collection, 'docs' => $docs]);
    }

    public function find(string $collection, array $query = [], array $options = []): array
    {
        $payload = ['cmd' => 'find', 'collection' => $collection, 'query' => $query ?: new \stdClass()];
        if (isset($options['sort']))  $payload['sort']  = $options['sort'];
        if (isset($options['skip']))  $payload['skip']  = $options['skip'];
        if (isset($options['limit'])) $payload['limit'] = $options['limit'];
        return $this->request($payload);
    }

    public function findOne(string $collection, array $query = [])
    {
        return $this->request(['cmd' => 'find_one', 'collection' => $collection, 'query' => $query ?: new \stdClass()]);
    }

    public function update(string $collection, array $query, array $update)
    {
        return $this->request([
            'cmd' => 'update', 'collection' => $collection,
            'query' => $query, 'update' => $update,
        ]);
    }

    public function updateOne(string $collection, array $query, array $update)
    {
        return $this->request([
            'cmd' => 'update_one', 'collection' => $collection,
            'query' => $query, 'update' => $update,
        ]);
    }

    public function delete(string $collection, array $query)
    {
        return $this->request(['cmd' => 'delete', 'collection' => $collection, 'query' => $query]);
    }

    public function deleteOne(string $collection, array $query)
    {
        return $this->request(['cmd' => 'delete_one', 'collection' => $collection, 'query' => $query]);
    }

    public function count(string $collection, array $query = []): int
    {
        $result = $this->request(['cmd' => 'count', 'collection' => $collection, 'query' => $query ?: new \stdClass()]);
        return $result['count'];
    }

    // ── Indexes ───────────────────────────────────────────────────

    public function createIndex(string $collection, string $field)
    {
        return $this->request(['cmd' => 'create_index', 'collection' => $collection, 'field' => $field]);
    }

    public function createUniqueIndex(string $collection, string $field)
    {
        return $this->request(['cmd' => 'create_unique_index', 'collection' => $collection, 'field' => $field]);
    }

    public function createCompositeIndex(string $collection, array $fields)
    {
        return $this->request(['cmd' => 'create_composite_index', 'collection' => $collection, 'fields' => $fields]);
    }

    public function listIndexes(string $collection): array
    {
        return $this->request(['cmd' => 'list_indexes', 'collection' => $collection]);
    }

    public function dropIndex(string $collection, string $indexName)
    {
        return $this->request(['cmd' => 'drop_index', 'collection' => $collection, 'index' => $indexName]);
    }

    // ── Aggregation ───────────────────────────────────────────────

    public function aggregate(string $collection, array $pipeline): array
    {
        return $this->request(['cmd' => 'aggregate', 'collection' => $collection, 'pipeline' => $pipeline]);
    }

    // ── Transactions ──────────────────────────────────────────────

    public function beginTransaction()
    {
        return $this->request(['cmd' => 'begin_tx']);
    }

    public function commit()
    {
        return $this->request(['cmd' => 'commit_tx']);
    }

    public function rollback()
    {
        return $this->request(['cmd' => 'rollback_tx']);
    }

    public function transaction(callable $fn)
    {
        $this->beginTransaction();
        try {
            $result = $fn($this);
            $this->commit();
            return $result;
        } catch (\Throwable $e) {
            try { $this->rollback(); } catch (OxiDbException $_) {}
            throw $e;
        }
    }

    // ── Compaction ─────────────────────────────────────────────────

    public function compact(string $collection): array
    {
        return $this->request(['cmd' => 'compact', 'collection' => $collection]);
    }

    // ── SQL ───────────────────────────────────────────────────────

    public function sql(string $query, ?string $dialect = null)
    {
        $payload = ['cmd' => 'sql', 'query' => $query];
        if ($dialect !== null) {
            $payload['dialect'] = $dialect;
        }
        return $this->request($payload);
    }

    public function setDialect(string $dialect)
    {
        return $this->request(['cmd' => 'set_dialect', 'dialect' => $dialect]);
    }

    // ── Blob storage ──────────────────────────────────────────────

    public function createBucket(string $bucket)
    {
        return $this->request(['cmd' => 'create_bucket', 'bucket' => $bucket]);
    }

    public function listBuckets(): array
    {
        return $this->request(['cmd' => 'list_buckets']);
    }

    public function deleteBucket(string $bucket)
    {
        return $this->request(['cmd' => 'delete_bucket', 'bucket' => $bucket]);
    }

    public function putObject(string $bucket, string $key, string $data, string $contentType = 'application/octet-stream', ?array $metadata = null)
    {
        $payload = [
            'cmd' => 'put_object', 'bucket' => $bucket, 'key' => $key,
            'data' => base64_encode($data), 'content_type' => $contentType,
        ];
        if ($metadata !== null) {
            $payload['metadata'] = $metadata;
        }
        return $this->request($payload);
    }

    public function getObject(string $bucket, string $key): array
    {
        $result = $this->request(['cmd' => 'get_object', 'bucket' => $bucket, 'key' => $key]);
        return [base64_decode($result['content']), $result['metadata']];
    }

    public function headObject(string $bucket, string $key): array
    {
        return $this->request(['cmd' => 'head_object', 'bucket' => $bucket, 'key' => $key]);
    }

    public function deleteObject(string $bucket, string $key)
    {
        return $this->request(['cmd' => 'delete_object', 'bucket' => $bucket, 'key' => $key]);
    }

    public function listObjects(string $bucket, ?string $prefix = null, ?int $limit = null): array
    {
        $payload = ['cmd' => 'list_objects', 'bucket' => $bucket];
        if ($prefix !== null) $payload['prefix'] = $prefix;
        if ($limit !== null) $payload['limit'] = $limit;
        return $this->request($payload);
    }

    // ── Full-text search ──────────────────────────────────────────

    public function textSearch(string $collection, string $query, int $limit = 10): array
    {
        return $this->request(['cmd' => 'text_search', 'collection' => $collection, 'query' => $query, 'limit' => $limit]);
    }

    public function search(string $query, ?string $bucket = null, int $limit = 10): array
    {
        $payload = ['cmd' => 'search', 'query' => $query, 'limit' => $limit];
        if ($bucket !== null) $payload['bucket'] = $bucket;
        return $this->request($payload);
    }

    // ── Vector search ─────────────────────────────────────────────

    public function createVectorIndex(string $collection, string $field, int $dimension, string $metric = 'cosine')
    {
        return $this->request([
            'cmd' => 'create_vector_index', 'collection' => $collection,
            'field' => $field, 'dimension' => $dimension, 'metric' => $metric,
        ]);
    }

    public function vectorSearch(string $collection, string $field, array $vector, int $limit = 10): array
    {
        return $this->request([
            'cmd' => 'vector_search', 'collection' => $collection,
            'field' => $field, 'vector' => $vector, 'limit' => $limit,
        ]);
    }

    // ── Text index ────────────────────────────────────────────────

    public function createTextIndex(string $collection, array $fields)
    {
        return $this->request(['cmd' => 'create_text_index', 'collection' => $collection, 'fields' => $fields]);
    }

    public function extractText(string $bucket, string $key): string
    {
        $result = $this->request(['cmd' => 'extract_text', 'bucket' => $bucket, 'key' => $key]);
        return $result['text'];
    }

    // ── Stored procedures ────────────────────────────────────────

    public function createProcedure(string $name, array $definition)
    {
        $payload = array_merge(['cmd' => 'create_procedure', 'name' => $name], $definition);
        return $this->request($payload);
    }

    public function callProcedure(string $name, array $params = [])
    {
        return $this->request(['cmd' => 'call_procedure', 'name' => $name, 'params' => $params ?: new \stdClass()]);
    }

    public function listProcedures(): array
    {
        return $this->request(['cmd' => 'list_procedures']);
    }

    public function getProcedure(string $name)
    {
        return $this->request(['cmd' => 'get_procedure', 'name' => $name]);
    }

    public function deleteProcedure(string $name)
    {
        return $this->request(['cmd' => 'delete_procedure', 'name' => $name]);
    }

    // ── Cron schedules ───────────────────────────────────────────

    public function createSchedule(string $name, array $definition)
    {
        $payload = array_merge(['cmd' => 'create_schedule', 'name' => $name], $definition);
        return $this->request($payload);
    }

    public function listSchedules(): array
    {
        return $this->request(['cmd' => 'list_schedules']);
    }

    public function getSchedule(string $name)
    {
        return $this->request(['cmd' => 'get_schedule', 'name' => $name]);
    }

    public function deleteSchedule(string $name)
    {
        return $this->request(['cmd' => 'delete_schedule', 'name' => $name]);
    }

    public function enableSchedule(string $name)
    {
        return $this->request(['cmd' => 'enable_schedule', 'name' => $name]);
    }

    public function disableSchedule(string $name)
    {
        return $this->request(['cmd' => 'disable_schedule', 'name' => $name]);
    }

    // ── User management (requires admin) ─────────────────────────

    public function createUser(string $username, string $password, string $role = 'read')
    {
        return $this->request(['cmd' => 'create_user', 'username' => $username, 'password' => $password, 'role' => $role]);
    }

    public function dropUser(string $username)
    {
        return $this->request(['cmd' => 'drop_user', 'username' => $username]);
    }

    public function updateUser(string $username, ?string $password = null, ?string $role = null)
    {
        $payload = ['cmd' => 'update_user', 'username' => $username];
        if ($password !== null) $payload['password'] = $password;
        if ($role !== null) $payload['role'] = $role;
        return $this->request($payload);
    }

    public function listUsers(): array
    {
        return $this->request(['cmd' => 'list_users']);
    }

    public function grantDbRole(string $username, string $database, string $role)
    {
        return $this->request(['cmd' => 'grant_db_role', 'username' => $username, 'database' => $database, 'role' => $role]);
    }

    public function revokeDbRole(string $username, string $database)
    {
        return $this->request(['cmd' => 'revoke_db_role', 'username' => $username, 'database' => $database]);
    }

    // ── Database management ───────────────────────────────────────

    public function useDatabase(string $name)
    {
        return $this->request(['cmd' => 'use_db', 'name' => $name]);
    }

    public function createDatabase(string $name)
    {
        return $this->request(['cmd' => 'create_database', 'name' => $name]);
    }

    public function dropDatabase(string $name)
    {
        return $this->request(['cmd' => 'drop_database', 'name' => $name]);
    }

    public function listDatabases(): array
    {
        return $this->request(['cmd' => 'list_databases']);
    }

    // ── Backup / Restore ─────────────────────────────────────────

    public function backup(string $path): array
    {
        return $this->request(['cmd' => 'backup', 'path' => $path]);
    }

    public function restore(string $archive, string $target): array
    {
        return $this->request(['cmd' => 'restore', 'archive' => $archive, 'target' => $target]);
    }

    // ── Private helpers ──────────────────────────────────────────

    private function isNullConn(): bool
    {
        try {
            return \FFI::isNull($this->conn);
        } catch (\FFI\Exception $e) {
            return true;
        }
    }

    private static function resolveLibPath(): string
    {
        if (self::$libPath !== null) {
            if (!file_exists(self::$libPath)) {
                throw new OxiDbException('Library not found: ' . self::$libPath);
            }
            return self::$libPath;
        }

        // Auto-detect based on OS
        $os = PHP_OS_FAMILY;
        $searchDirs = [
            __DIR__,
            __DIR__ . '/../target/release',
            '/usr/local/lib',
            '/usr/lib',
        ];

        $libName = match ($os) {
            'Darwin' => 'liboxidb_client_ffi.dylib',
            'Windows' => 'oxidb_client_ffi.dll',
            default => 'liboxidb_client_ffi.so',
        };

        foreach ($searchDirs as $dir) {
            $path = $dir . '/' . $libName;
            if (file_exists($path)) {
                return $path;
            }
        }

        throw new OxiDbException(
            "Cannot find $libName. Set path with OxiDbFFIClient::setLibraryPath() " .
            "or place it in one of: " . implode(', ', $searchDirs)
        );
    }
}
