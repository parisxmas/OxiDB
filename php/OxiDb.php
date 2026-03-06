<?php
/**
 * OxiDB PHP Client Library
 *
 * Zero external dependencies. Communicates with oxidb-server over TCP
 * using the OxiWire binary protocol for maximum performance.
 *
 * Protocol: each message is [4-byte LE length][OxiWire payload].
 * OxiWire envelope: request  = [0xDB][value]
 *                   response = [0xDB][status:0=ok,1=error][value]
 *
 * Usage:
 *   $db = new OxiDbClient('127.0.0.1', 4444);
 *   $db->insert('users', ['name' => 'Alice', 'age' => 30]);
 *   $docs = $db->find('users', ['name' => 'Alice']);
 *   $db->close();
 */

class OxiDbException extends \RuntimeException {}
class TransactionConflictException extends OxiDbException {}

class OxiWire
{
    const MAGIC    = 0xDB;
    const TAG_NULL   = 0x00;
    const TAG_FALSE  = 0x01;
    const TAG_TRUE   = 0x02;
    const TAG_INT64  = 0x03;
    const TAG_UINT64 = 0x04;
    const TAG_FLOAT  = 0x05;
    const TAG_STRING = 0x06;
    const TAG_ARRAY  = 0x07;
    const TAG_MAP    = 0x08;

    /**
     * Encode a request payload as OxiWire binary with magic prefix.
     */
    public static function encodeRequest(array $payload): string
    {
        $buf = chr(self::MAGIC);
        $buf .= self::encodeValue($payload);
        return $buf;
    }

    /**
     * Decode an OxiWire response envelope.
     * Returns [bool $ok, mixed $data].
     */
    public static function decodeResponse(string $data): array
    {
        $len = strlen($data);
        if ($len < 2 || ord($data[0]) !== self::MAGIC) {
            throw new OxiDbException('OxiWire: invalid magic byte');
        }

        $status = ord($data[1]);
        $pos = 2;
        $value = self::decodeValue($data, $pos, $len);
        return [$status === 0, $value];
    }

    /**
     * Check if payload starts with OxiWire magic byte.
     */
    public static function isOxiWire(string $data): bool
    {
        return strlen($data) > 0 && ord($data[0]) === self::MAGIC;
    }

    // ── Encoder ────────────────────────────────────────────────────

    private static function encodeValue($value): string
    {
        if ($value === null) {
            return chr(self::TAG_NULL);
        }

        if (is_bool($value)) {
            return chr($value ? self::TAG_TRUE : self::TAG_FALSE);
        }

        if (is_int($value)) {
            if ($value >= 0) {
                return chr(self::TAG_UINT64) . pack('P', $value);
            } else {
                return chr(self::TAG_INT64) . pack('P', $value);
            }
        }

        if (is_float($value)) {
            return chr(self::TAG_FLOAT) . pack('e', $value);
        }

        if (is_string($value)) {
            return self::encodeString($value);
        }

        if ($value instanceof \stdClass) {
            // stdClass → empty map
            return self::encodeMap((array)$value);
        }

        if (is_array($value)) {
            // Distinguish associative arrays (maps) from sequential arrays
            if (self::isAssoc($value)) {
                return self::encodeMap($value);
            } else {
                return self::encodeArray($value);
            }
        }

        // Fallback: JSON-encode and send as string
        return self::encodeString(json_encode($value));
    }

    private static function encodeString(string $s): string
    {
        $bytes = $s; // PHP strings are already byte sequences
        return chr(self::TAG_STRING) . pack('V', strlen($bytes)) . $bytes;
    }

    private static function encodeMap(array $map): string
    {
        $buf = chr(self::TAG_MAP) . pack('V', count($map));
        foreach ($map as $key => $val) {
            $keyStr = (string)$key;
            // Map keys: bare string (length + bytes, no type tag)
            $buf .= pack('V', strlen($keyStr)) . $keyStr;
            $buf .= self::encodeValue($val);
        }
        return $buf;
    }

    private static function encodeArray(array $arr): string
    {
        $buf = chr(self::TAG_ARRAY) . pack('V', count($arr));
        foreach ($arr as $item) {
            $buf .= self::encodeValue($item);
        }
        return $buf;
    }

    private static function isAssoc(array $arr): bool
    {
        if (empty($arr)) return true; // empty arrays → empty map (server expects objects)
        return array_keys($arr) !== range(0, count($arr) - 1);
    }

    // ── Decoder ────────────────────────────────────────────────────

    private static function decodeValue(string $buf, int &$pos, int $len)
    {
        if ($pos >= $len) {
            throw new OxiDbException('OxiWire: unexpected end of input');
        }

        $tag = ord($buf[$pos++]);

        switch ($tag) {
            case self::TAG_NULL:
                return null;

            case self::TAG_FALSE:
                return false;

            case self::TAG_TRUE:
                return true;

            case self::TAG_INT64:
                self::ensureBytes($pos, 8, $len);
                $val = unpack('P', substr($buf, $pos, 8))[1];
                $pos += 8;
                // Convert unsigned to signed
                if ($val > PHP_INT_MAX) {
                    $val = $val - (1 << 64);
                }
                return $val;

            case self::TAG_UINT64:
                self::ensureBytes($pos, 8, $len);
                $val = unpack('P', substr($buf, $pos, 8))[1];
                $pos += 8;
                return $val;

            case self::TAG_FLOAT:
                self::ensureBytes($pos, 8, $len);
                $val = unpack('e', substr($buf, $pos, 8))[1];
                $pos += 8;
                return $val;

            case self::TAG_STRING:
                self::ensureBytes($pos, 4, $len);
                $strLen = unpack('V', substr($buf, $pos, 4))[1];
                $pos += 4;
                self::ensureBytes($pos, $strLen, $len);
                $val = substr($buf, $pos, $strLen);
                $pos += $strLen;
                return $val;

            case self::TAG_ARRAY:
                self::ensureBytes($pos, 4, $len);
                $count = unpack('V', substr($buf, $pos, 4))[1];
                $pos += 4;
                $arr = [];
                for ($i = 0; $i < $count; $i++) {
                    $arr[] = self::decodeValue($buf, $pos, $len);
                }
                return $arr;

            case self::TAG_MAP:
                self::ensureBytes($pos, 4, $len);
                $count = unpack('V', substr($buf, $pos, 4))[1];
                $pos += 4;
                $map = [];
                for ($i = 0; $i < $count; $i++) {
                    // Map keys: bare string (length + bytes, no type tag)
                    self::ensureBytes($pos, 4, $len);
                    $keyLen = unpack('V', substr($buf, $pos, 4))[1];
                    $pos += 4;
                    self::ensureBytes($pos, $keyLen, $len);
                    $key = substr($buf, $pos, $keyLen);
                    $pos += $keyLen;
                    $map[$key] = self::decodeValue($buf, $pos, $len);
                }
                return $map;

            default:
                throw new OxiDbException(sprintf(
                    'OxiWire: unknown tag 0x%02X at position %d', $tag, $pos - 1
                ));
        }
    }

    private static function ensureBytes(int $pos, int $need, int $len): void
    {
        if ($pos + $need > $len) {
            throw new OxiDbException(sprintf(
                'OxiWire: truncated data at position %d, need %d bytes', $pos, $need
            ));
        }
    }
}

class OxiDbClient
{
    private $socket;

    /**
     * Connect to an OxiDB server.
     */
    public function __construct(string $host = '127.0.0.1', int $port = 4444, float $timeout = 5.0)
    {
        $this->socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($this->socket === false) {
            throw new OxiDbException('Failed to create socket: ' . socket_strerror(socket_last_error()));
        }

        socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, [
            'sec' => (int)$timeout,
            'usec' => (int)(($timeout - (int)$timeout) * 1_000_000),
        ]);
        socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, [
            'sec' => (int)$timeout,
            'usec' => (int)(($timeout - (int)$timeout) * 1_000_000),
        ]);
        socket_set_option($this->socket, SOL_TCP, TCP_NODELAY, 1);

        if (!@socket_connect($this->socket, $host, $port)) {
            $err = socket_strerror(socket_last_error($this->socket));
            throw new OxiDbException("Failed to connect to $host:$port — $err");
        }
    }

    public function close(): void
    {
        if ($this->socket) {
            @socket_close($this->socket);
            $this->socket = null;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    // ── Low-level protocol ────────────────────────────────────────

    private function sendRaw(string $data): void
    {
        $frame = pack('V', strlen($data)) . $data;
        $total = strlen($frame);
        $sent = 0;
        while ($sent < $total) {
            $n = @socket_write($this->socket, substr($frame, $sent), $total - $sent);
            if ($n === false) {
                throw new OxiDbException('Socket write failed: ' . socket_strerror(socket_last_error($this->socket)));
            }
            $sent += $n;
        }
    }

    private function recvExact(int $n): string
    {
        $buf = '';
        while (strlen($buf) < $n) {
            $chunk = @socket_read($this->socket, $n - strlen($buf), PHP_BINARY_READ);
            if ($chunk === false || $chunk === '') {
                throw new OxiDbException('Connection closed by server');
            }
            $buf .= $chunk;
        }
        return $buf;
    }

    private function recvRaw(): string
    {
        $header = $this->recvExact(4);
        $length = unpack('V', $header)[1];
        return $this->recvExact($length);
    }

    /**
     * Send an OxiWire request and return the decoded response data.
     */
    private function request(array $payload)
    {
        $this->sendRaw(OxiWire::encodeRequest($payload));
        $resp = $this->recvRaw();

        if (OxiWire::isOxiWire($resp)) {
            [$ok, $data] = OxiWire::decodeResponse($resp);
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
        $resp = json_decode($resp, true);
        if (!($resp['ok'] ?? false)) {
            $msg = $resp['error'] ?? 'unknown error';
            if (stripos($msg, 'conflict') !== false) {
                throw new TransactionConflictException($msg);
            }
            throw new OxiDbException($msg);
        }
        return $resp['data'] ?? null;
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

    /**
     * Execute a callable within a transaction.
     * Auto-commits on success, auto-rolls back on exception.
     */
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

    // ── Backup / Restore ─────────────────────────────────────────

    public function backup(string $path): array
    {
        return $this->request(['cmd' => 'backup', 'path' => $path]);
    }

    public function restore(string $archive, string $target): array
    {
        return $this->request(['cmd' => 'restore', 'archive' => $archive, 'target' => $target]);
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
}
