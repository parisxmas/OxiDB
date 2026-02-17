<?php

declare(strict_types=1);

namespace OxiDb;

/**
 * TCP client for oxidb-server.
 *
 * Protocol: each message is [4-byte little-endian length][JSON payload].
 * Server responds with {"ok": true, "data": ...} or {"ok": false, "error": "..."}.
 *
 * Zero external dependencies — uses only PHP sockets and json.
 */
class OxiDbClient
{
    /** @var resource */
    private $socket;

    /**
     * Connect to oxidb-server.
     *
     * @param string $host Server host (default "127.0.0.1")
     * @param int    $port Server port (default 4444)
     * @param float  $timeout Connect/read timeout in seconds (default 5.0)
     * @throws OxiDbException
     */
    public function __construct(string $host = '127.0.0.1', int $port = 4444, float $timeout = 5.0)
    {
        $this->socket = @fsockopen($host, $port, $errno, $errstr, $timeout);
        if ($this->socket === false) {
            throw new OxiDbException("Failed to connect to OxiDB at {$host}:{$port}: {$errstr}");
        }
        stream_set_timeout($this->socket, (int)$timeout, (int)(($timeout - (int)$timeout) * 1000000));
    }

    /**
     * Close the TCP connection.
     */
    public function close(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    // ------------------------------------------------------------------
    // Low-level protocol
    // ------------------------------------------------------------------

    private function sendRaw(string $data): void
    {
        $len = pack('V', strlen($data));
        $this->writeAll($len);
        $this->writeAll($data);
    }

    private function recvRaw(): string
    {
        $lenBytes = $this->readExact(4);
        $unpacked = unpack('Vlength', $lenBytes);
        $length = $unpacked['length'];
        return $this->readExact($length);
    }

    private function writeAll(string $data): void
    {
        $remaining = strlen($data);
        $offset = 0;
        while ($remaining > 0) {
            $written = @fwrite($this->socket, substr($data, $offset), $remaining);
            if ($written === false || $written === 0) {
                throw new OxiDbException('Failed to write to socket');
            }
            $offset += $written;
            $remaining -= $written;
        }
    }

    private function readExact(int $n): string
    {
        $buf = '';
        while (strlen($buf) < $n) {
            $chunk = @fread($this->socket, $n - strlen($buf));
            if ($chunk === false || $chunk === '') {
                throw new OxiDbException('Connection closed by server');
            }
            $buf .= $chunk;
        }
        return $buf;
    }

    /**
     * @return mixed
     * @throws OxiDbException
     */
    private function request(array $payload)
    {
        $json = json_encode($payload, JSON_THROW_ON_ERROR);
        $this->sendRaw($json);
        $respBytes = $this->recvRaw();
        return json_decode($respBytes, true, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * @return mixed
     * @throws OxiDbException|TransactionConflictException
     */
    private function checked(array $payload)
    {
        $resp = $this->request($payload);
        if (empty($resp['ok'])) {
            $error = $resp['error'] ?? 'unknown error';
            if (stripos($error, 'conflict') !== false) {
                throw new TransactionConflictException($error);
            }
            throw new OxiDbException($error);
        }
        return $resp['data'] ?? null;
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    /**
     * Ping the server. Returns "pong".
     */
    public function ping(): string
    {
        return $this->checked(['cmd' => 'ping']);
    }

    // ------------------------------------------------------------------
    // Collection management
    // ------------------------------------------------------------------

    public function createCollection(string $name): mixed
    {
        return $this->checked(['cmd' => 'create_collection', 'collection' => $name]);
    }

    public function listCollections(): array
    {
        return $this->checked(['cmd' => 'list_collections']);
    }

    public function dropCollection(string $name): mixed
    {
        return $this->checked(['cmd' => 'drop_collection', 'collection' => $name]);
    }

    // ------------------------------------------------------------------
    // CRUD
    // ------------------------------------------------------------------

    public function insert(string $collection, array $doc): mixed
    {
        return $this->checked(['cmd' => 'insert', 'collection' => $collection, 'doc' => (object)$doc]);
    }

    public function insertMany(string $collection, array $docs): mixed
    {
        $objects = array_map(fn($d) => (object)$d, $docs);
        return $this->checked(['cmd' => 'insert_many', 'collection' => $collection, 'docs' => $objects]);
    }

    public function find(string $collection, array $query = [], ?array $sort = null, ?int $skip = null, ?int $limit = null): array
    {
        $payload = ['cmd' => 'find', 'collection' => $collection, 'query' => (object)$query];
        if ($sort !== null) $payload['sort'] = (object)$sort;
        if ($skip !== null) $payload['skip'] = $skip;
        if ($limit !== null) $payload['limit'] = $limit;
        return $this->checked($payload);
    }

    public function findOne(string $collection, array $query = []): mixed
    {
        return $this->checked(['cmd' => 'find_one', 'collection' => $collection, 'query' => (object)$query]);
    }

    public function update(string $collection, array $query, array $update): mixed
    {
        return $this->checked([
            'cmd' => 'update', 'collection' => $collection,
            'query' => (object)$query, 'update' => (object)$update,
        ]);
    }

    public function delete(string $collection, array $query): mixed
    {
        return $this->checked(['cmd' => 'delete', 'collection' => $collection, 'query' => (object)$query]);
    }

    public function count(string $collection, array $query = []): int
    {
        $result = $this->checked(['cmd' => 'count', 'collection' => $collection, 'query' => (object)$query]);
        return $result['count'];
    }

    // ------------------------------------------------------------------
    // Indexes
    // ------------------------------------------------------------------

    public function createIndex(string $collection, string $field): mixed
    {
        return $this->checked(['cmd' => 'create_index', 'collection' => $collection, 'field' => $field]);
    }

    public function createUniqueIndex(string $collection, string $field): mixed
    {
        return $this->checked(['cmd' => 'create_unique_index', 'collection' => $collection, 'field' => $field]);
    }

    public function createCompositeIndex(string $collection, array $fields): mixed
    {
        return $this->checked(['cmd' => 'create_composite_index', 'collection' => $collection, 'fields' => $fields]);
    }

    // ------------------------------------------------------------------
    // Aggregation
    // ------------------------------------------------------------------

    public function aggregate(string $collection, array $pipeline): array
    {
        return $this->checked(['cmd' => 'aggregate', 'collection' => $collection, 'pipeline' => $pipeline]);
    }

    // ------------------------------------------------------------------
    // Compaction
    // ------------------------------------------------------------------

    public function compact(string $collection): array
    {
        return $this->checked(['cmd' => 'compact', 'collection' => $collection]);
    }

    // ------------------------------------------------------------------
    // Transactions
    // ------------------------------------------------------------------

    public function beginTx(): mixed
    {
        return $this->checked(['cmd' => 'begin_tx']);
    }

    public function commitTx(): mixed
    {
        return $this->checked(['cmd' => 'commit_tx']);
    }

    public function rollbackTx(): mixed
    {
        return $this->checked(['cmd' => 'rollback_tx']);
    }

    /**
     * Execute a callable within a transaction.
     * Auto-commits on success, auto-rolls back on exception.
     *
     * @param callable $fn The operations to perform
     * @throws \Throwable
     */
    public function transaction(callable $fn): void
    {
        $this->beginTx();
        try {
            $fn();
            $this->commitTx();
        } catch (\Throwable $e) {
            try {
                $this->rollbackTx();
            } catch (OxiDbException $ignored) {
            }
            throw $e;
        }
    }

    // ------------------------------------------------------------------
    // Blob storage
    // ------------------------------------------------------------------

    public function createBucket(string $bucket): mixed
    {
        return $this->checked(['cmd' => 'create_bucket', 'bucket' => $bucket]);
    }

    public function listBuckets(): array
    {
        return $this->checked(['cmd' => 'list_buckets']);
    }

    public function deleteBucket(string $bucket): mixed
    {
        return $this->checked(['cmd' => 'delete_bucket', 'bucket' => $bucket]);
    }

    /**
     * Upload a blob object. Data is base64-encoded automatically.
     */
    public function putObject(string $bucket, string $key, string $data, string $contentType = 'application/octet-stream', ?array $metadata = null): mixed
    {
        $payload = [
            'cmd' => 'put_object',
            'bucket' => $bucket,
            'key' => $key,
            'data' => base64_encode($data),
            'content_type' => $contentType,
        ];
        if ($metadata !== null && !empty($metadata)) {
            $payload['metadata'] = (object)$metadata;
        }
        return $this->checked($payload);
    }

    /**
     * Download a blob object. Returns [data_string, metadata_array].
     */
    public function getObject(string $bucket, string $key): array
    {
        $result = $this->checked(['cmd' => 'get_object', 'bucket' => $bucket, 'key' => $key]);
        $data = base64_decode($result['content']);
        return [$data, $result['metadata']];
    }

    public function headObject(string $bucket, string $key): array
    {
        return $this->checked(['cmd' => 'head_object', 'bucket' => $bucket, 'key' => $key]);
    }

    public function deleteObject(string $bucket, string $key): mixed
    {
        return $this->checked(['cmd' => 'delete_object', 'bucket' => $bucket, 'key' => $key]);
    }

    public function listObjects(string $bucket, ?string $prefix = null, ?int $limit = null): array
    {
        $payload = ['cmd' => 'list_objects', 'bucket' => $bucket];
        if ($prefix !== null) $payload['prefix'] = $prefix;
        if ($limit !== null) $payload['limit'] = $limit;
        return $this->checked($payload);
    }

    // ------------------------------------------------------------------
    // Full-text search
    // ------------------------------------------------------------------

    public function search(string $query, ?string $bucket = null, int $limit = 10): array
    {
        $payload = ['cmd' => 'search', 'query' => $query, 'limit' => $limit];
        if ($bucket !== null) $payload['bucket'] = $bucket;
        return $this->checked($payload);
    }
}
