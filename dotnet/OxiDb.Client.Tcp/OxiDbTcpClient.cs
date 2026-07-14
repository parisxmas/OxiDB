using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text.Json;

namespace OxiDb.Client.Tcp;

/// <summary>
/// Pure managed TCP client for OxiDB server.
/// Protocol: [4-byte LE length][JSON payload].
/// Thread-safe via SemaphoreSlim (async-friendly).
/// </summary>
public sealed class OxiDbTcpClient : IOxiDbClient
{
    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _disposed;
    private bool _broken;
    private bool _useOxiWire;

    private OxiDbTcpClient(TcpClient tcp)
    {
        _tcp = tcp;
        _stream = tcp.GetStream();
    }

    public static async Task<OxiDbTcpClient> ConnectAsync(
        string host = "127.0.0.1",
        int port = 4444,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        var tcp = new TcpClient();
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout ?? TimeSpan.FromSeconds(5));
            await tcp.ConnectAsync(host, port, cts.Token);
            tcp.NoDelay = true;
            return new OxiDbTcpClient(tcp);
        }
        catch
        {
            tcp.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Connect and authenticate with username/password (simple auth).
    /// </summary>
    public static async Task<OxiDbTcpClient> ConnectAsync(
        string host,
        int port,
        string username,
        string password,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        var client = await ConnectAsync(host, port, timeout, ct);
        try
        {
            await client.AuthSimpleAsync(username, password, ct);
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Enable OxiWire binary protocol for faster encoding/decoding.
    /// Requests and responses use OxiDB's custom binary format instead of JSON.
    /// </summary>
    public void UseOxiWire() => _useOxiWire = true;

    /// <summary>
    /// Flag-only usability check (no syscall): false when disposed, marked
    /// broken by an interrupted exchange, or locally known-disconnected.
    /// Cannot see a server-side close — use <see cref="IsAlive"/> for that.
    /// </summary>
    public bool IsUsable => !_disposed && !_broken && _tcp.Connected;

    /// <summary>
    /// Cheap liveness probe (no round trip): false once the peer has closed
    /// its end (e.g. the server's idle timeout), an exchange was interrupted
    /// mid-conversation, or this client is disposed. Connection pools use it
    /// to discard entries that must not be handed to another consumer.
    /// </summary>
    public bool IsAlive
    {
        get
        {
            if (_disposed || _broken || !_tcp.Connected) return false;
            try
            {
                // Any readable state disqualifies: readable-with-zero-bytes
                // is an orderly remote close, and pending bytes on an idle
                // strict request/response socket are a stale response that
                // would cross-talk into the next consumer's first request.
                return !_tcp.Client.Poll(0, SelectMode.SelectRead);
            }
            catch
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Execute SQL and return the raw response frame (OxiWire envelope when
    /// this connection is in OxiWire mode, JSON otherwise). The hot path for
    /// <c>OxiDb.Data</c>: the caller decodes straight to CLR values with no
    /// JsonDocument round trip. Error envelopes are returned, not thrown.
    /// </summary>
    public async Task<byte[]> SqlRawBytesAsync(
        string sql,
        object?[]? @params = null,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ct.ThrowIfCancellationRequested();
        var payload = new Dictionary<string, object?>
        {
            ["engine"] = "sql",
            ["cmd"] = "sql",
            ["sql"] = sql,
        };
        if (@params is not null)
        {
            payload["params"] = @params;
        }
        var reqBytes = _useOxiWire
            ? OxiWire.EncodeRequest(payload)
            : JsonSerializer.SerializeToUtf8Bytes(payload);
        await _lock.WaitAsync(ct);
        try
        {
            return await ExchangeAsync(reqBytes, ct);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Synchronous twin of <see cref="SqlRawBytesAsync"/>: blocking socket
    /// I/O, no async state machine or thread-pool hop. This is the ADO sync
    /// path (`ExecuteReader` et al.) — worth real microseconds per query.
    /// </summary>
    public byte[] SqlRawBytes(string sql, object?[]? @params = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var payload = new Dictionary<string, object?>
        {
            ["engine"] = "sql",
            ["cmd"] = "sql",
            ["sql"] = sql,
        };
        if (@params is not null)
        {
            payload["params"] = @params;
        }
        var reqBytes = _useOxiWire
            ? OxiWire.EncodeRequest(payload)
            : JsonSerializer.SerializeToUtf8Bytes(payload);
        _lock.Wait();
        try
        {
            try
            {
                var frame = new byte[4 + reqBytes.Length];
                BinaryPrimitives.WriteUInt32LittleEndian(frame, (uint)reqBytes.Length);
                reqBytes.CopyTo(frame, 4);
                _stream.Write(frame);
                Span<byte> lenBuf = stackalloc byte[4];
                ReadExact(lenBuf);
                var resp = new byte[BinaryPrimitives.ReadUInt32LittleEndian(lenBuf)];
                ReadExact(resp);
                return resp;
            }
            catch
            {
                _broken = true; // mid-conversation stream state — never pool
                throw;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private void ReadExact(Span<byte> buffer)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = _stream.Read(buffer[offset..]);
            if (read == 0)
                throw new OxiDbConnectionException("Connection closed by server");
            offset += read;
        }
    }

    /// <summary>
    /// One request/response exchange. Any transport failure (or cancellation)
    /// in between leaves the stream mid-conversation — an unsent, unread, or
    /// half-read frame — so the client is permanently marked broken.
    /// </summary>
    private async Task<byte[]> ExchangeAsync(byte[] reqBytes, CancellationToken ct)
    {
        try
        {
            await SendAsync(reqBytes, ct);
            return await ReceiveAsync(ct);
        }
        catch
        {
            _broken = true;
            throw;
        }
    }

    // ── Low-level protocol ──────────────────────────────────────────────

    private async Task SendAsync(byte[] data, CancellationToken ct)
    {
        // One buffer, one write: a separate 4-byte length write costs an
        // extra syscall and (with NoDelay) its own TCP segment.
        var frame = new byte[4 + data.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame, (uint)data.Length);
        data.CopyTo(frame, 4);
        await _stream.WriteAsync(frame, ct);
    }

    private async Task<byte[]> ReceiveAsync(CancellationToken ct)
    {
        var lenBuf = new byte[4];
        await ReadExactAsync(lenBuf, ct);
        var length = BinaryPrimitives.ReadUInt32LittleEndian(lenBuf);
        var payload = new byte[length];
        await ReadExactAsync(payload, ct);
        return payload;
    }

    private async Task ReadExactAsync(byte[] buffer, CancellationToken ct)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await _stream.ReadAsync(buffer.AsMemory(offset), ct);
            if (read == 0)
                throw new OxiDbConnectionException("Connection closed by server");
            offset += read;
        }
    }

    /// <summary>
    /// Escape hatch: send an arbitrary wire-level payload and return the
    /// server's raw <c>data</c> field as <see cref="JsonElement"/>.
    /// Used by typed wrappers for commands not yet on the interface.
    /// Prefer the typed overloads where they exist.
    /// </summary>
    public Task<JsonElement> ExecRawAsync(Dictionary<string, object?> payload, CancellationToken ct = default)
        => RequestAsync(payload, ct);

    /// <summary>
    /// Like <see cref="ExecRawAsync"/> but returns the **entire response
    /// envelope** instead of just <c>data</c>. Use this for commands
    /// whose response shape isn't <c>{"ok": true, "data": ...}</c> —
    /// notably HELLO, which puts its payload under <c>server</c>.
    /// </summary>
    public Task<JsonElement> ExecRawEnvelopeAsync(Dictionary<string, object?> payload, CancellationToken ct = default)
        => RequestEnvelopeAsync(payload, ct);

    private async Task<JsonElement> RequestEnvelopeAsync(Dictionary<string, object?> payload, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            byte[] reqBytes = _useOxiWire
                ? OxiWire.EncodeRequest(payload)
                : JsonSerializer.SerializeToUtf8Bytes(payload);

            var respBytes = await ExchangeAsync(reqBytes, ct);

            // For OxiWire-encoded responses, DecodeResponse already
            // returns the full envelope as a JsonElement.
            if (_useOxiWire && OxiWire.IsOxiWire(respBytes))
            {
                var (ok, data) = OxiWire.DecodeResponse(respBytes);
                if (!ok)
                {
                    var errMsg = data.ValueKind == JsonValueKind.String
                        ? data.GetString() ?? "unknown error"
                        : "unknown error";
                    throw OxiDbException.FromServerMessage(errMsg);
                }
                return data;
            }

            // JSON response: server returns `{"ok": ..., "data" | "server" | "error": ...}`.
            // Return the whole root so the caller can pick the right field.
            using var doc = JsonDocument.Parse(respBytes);
            var root = doc.RootElement.Clone();
            if (root.TryGetProperty("ok", out var okProp) && !okProp.GetBoolean())
            {
                var errMsg = root.TryGetProperty("error", out var errProp)
                    ? errProp.GetString() ?? "unknown error"
                    : "unknown error";
                throw OxiDbException.FromServerMessage(errMsg);
            }
            return root;
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<JsonElement> RequestAsync(Dictionary<string, object?> payload, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        // An already-canceled token fails with the exact OperationCanceledException
        // (an awaited WaitAsync would surface a TaskCanceledException instead).
        ct.ThrowIfCancellationRequested();
        await _lock.WaitAsync(ct);
        try
        {
            byte[] reqBytes;
            if (_useOxiWire)
                reqBytes = OxiWire.EncodeRequest(payload);
            else
                reqBytes = JsonSerializer.SerializeToUtf8Bytes(payload);

            var respBytes = await ExchangeAsync(reqBytes, ct);

            if (_useOxiWire && OxiWire.IsOxiWire(respBytes))
            {
                var (ok, data) = OxiWire.DecodeResponse(respBytes);
                if (!ok)
                {
                    var errMsg = data.ValueKind == JsonValueKind.String
                        ? data.GetString() ?? "unknown error"
                        : "unknown error";
                    throw OxiDbException.FromServerMessage(errMsg);
                }
                return data;
            }

            using var doc = JsonDocument.Parse(respBytes);
            var root = doc.RootElement;

            if (!root.TryGetProperty("ok", out var okProp) || !okProp.GetBoolean())
            {
                var errMsg = root.TryGetProperty("error", out var errProp)
                    ? errProp.GetString() ?? "unknown error"
                    : "unknown error";
                throw OxiDbException.FromServerMessage(errMsg);
            }

            if (root.TryGetProperty("data", out var dataProp))
                return dataProp.Clone();

            return default;
        }
        finally
        {
            _lock.Release();
        }
    }

    // ── Authentication ───────────────────────────────────────────────

    public async Task AuthSimpleAsync(string username, string password, CancellationToken ct = default)
    {
        await RequestAsync(new()
        {
            ["cmd"] = "auth_simple",
            ["username"] = username,
            ["password"] = password
        }, ct);
    }

    // ── Utility ─────────────────────────────────────────────────────────

    public async Task<string> PingAsync(CancellationToken ct = default)
    {
        var data = await RequestAsync(new() { ["cmd"] = "ping" }, ct);
        return data.GetString() ?? "pong";
    }

    // ── Collection management ───────────────────────────────────────────

    public async Task CreateCollectionAsync(string name, CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "create_collection", ["collection"] = name }, ct);
    }

    public async Task CreateCollectionWithOptionsAsync(string name, StorageOptions options, CancellationToken ct = default)
    {
        await RequestAsync(new()
        {
            ["cmd"] = "create_collection_with_options",
            ["collection"] = name,
            ["options"] = options.ToWire(),
        }, ct);
    }

    public async Task<List<string>> ListCollectionsAsync(CancellationToken ct = default)
    {
        var data = await RequestAsync(new() { ["cmd"] = "list_collections" }, ct);
        var result = new List<string>();
        if (data.ValueKind == JsonValueKind.Array)
            foreach (var item in data.EnumerateArray())
                result.Add(item.GetString()!);
        return result;
    }

    public async Task DropCollectionAsync(string name, CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "drop_collection", ["collection"] = name }, ct);
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    public async Task<JsonElement> InsertAsync(string collection, object doc, CancellationToken ct = default)
    {
        return await RequestAsync(new() { ["cmd"] = "insert", ["collection"] = collection, ["doc"] = doc }, ct);
    }

    public async Task<JsonElement> InsertManyAsync(string collection, IEnumerable<object> docs, CancellationToken ct = default)
    {
        return await RequestAsync(new() { ["cmd"] = "insert_many", ["collection"] = collection, ["docs"] = docs }, ct);
    }

    public async Task<JsonElement> FindAsync(
        string collection,
        object? query = null,
        object? sort = null,
        int? skip = null,
        int? limit = null,
        CancellationToken ct = default)
    {
        var payload = new Dictionary<string, object?>
        {
            ["cmd"] = "find",
            ["collection"] = collection,
            ["query"] = query ?? new Dictionary<string, object?>()
        };
        if (sort != null) payload["sort"] = sort;
        if (skip.HasValue) payload["skip"] = skip.Value;
        if (limit.HasValue) payload["limit"] = limit.Value;
        return await RequestAsync(payload, ct);
    }

    public async Task<JsonElement> FindOneAsync(string collection, object query, CancellationToken ct = default)
    {
        return await RequestAsync(new()
        {
            ["cmd"] = "find_one",
            ["collection"] = collection,
            ["query"] = query
        }, ct);
    }

    public async Task<JsonElement> UpdateAsync(string collection, object query, object update, CancellationToken ct = default)
    {
        return await RequestAsync(new()
        {
            ["cmd"] = "update",
            ["collection"] = collection,
            ["query"] = query,
            ["update"] = update
        }, ct);
    }

    public async Task<JsonElement> UpdateOneAsync(string collection, object query, object update, CancellationToken ct = default)
    {
        return await RequestAsync(new()
        {
            ["cmd"] = "update_one",
            ["collection"] = collection,
            ["query"] = query,
            ["update"] = update
        }, ct);
    }

    public async Task<JsonElement> DeleteAsync(string collection, object query, CancellationToken ct = default)
    {
        return await RequestAsync(new()
        {
            ["cmd"] = "delete",
            ["collection"] = collection,
            ["query"] = query
        }, ct);
    }

    public async Task<JsonElement> DeleteOneAsync(string collection, object query, CancellationToken ct = default)
    {
        return await RequestAsync(new()
        {
            ["cmd"] = "delete_one",
            ["collection"] = collection,
            ["query"] = query
        }, ct);
    }

    public async Task<int> CountAsync(string collection, object? query = null, CancellationToken ct = default)
    {
        var data = await RequestAsync(new()
        {
            ["cmd"] = "count",
            ["collection"] = collection,
            ["query"] = query ?? new Dictionary<string, object?>()
        }, ct);
        if (data.ValueKind == JsonValueKind.Object && data.TryGetProperty("count", out var countProp))
            return countProp.GetInt32();
        return 0;
    }

    // ── Indexes ─────────────────────────────────────────────────────────

    public async Task CreateIndexAsync(string collection, string field, CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "create_index", ["collection"] = collection, ["field"] = field }, ct);
    }

    public async Task CreateUniqueIndexAsync(string collection, string field, CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "create_unique_index", ["collection"] = collection, ["field"] = field }, ct);
    }

    public async Task CreateCompositeIndexAsync(string collection, string[] fields, CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "create_composite_index", ["collection"] = collection, ["fields"] = fields }, ct);
    }

    public async Task<JsonElement> ListIndexesAsync(string collection, CancellationToken ct = default)
    {
        return await RequestAsync(new() { ["cmd"] = "list_indexes", ["collection"] = collection }, ct);
    }

    public async Task DropIndexAsync(string collection, string indexName, CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "drop_index", ["collection"] = collection, ["index"] = indexName }, ct);
    }

    // ── Aggregation ─────────────────────────────────────────────────────

    public async Task<JsonElement> AggregateAsync(string collection, object[] pipeline, CancellationToken ct = default)
    {
        return await RequestAsync(new()
        {
            ["cmd"] = "aggregate",
            ["collection"] = collection,
            ["pipeline"] = pipeline
        }, ct);
    }

    // ── SQL engine (second engine, ADR-0010) ────────────────────────────

    public async Task<JsonElement> SqlAsync(string sql, object?[]? @params = null, CancellationToken ct = default)
    {
        var payload = new Dictionary<string, object?>
        {
            ["engine"] = "sql",
            ["cmd"] = "sql",
            ["sql"] = sql
        };
        if (@params is not null)
        {
            payload["params"] = @params;
        }
        return await RequestAsync(payload, ct);
    }

    // ── Transactions ────────────────────────────────────────────────────

    public async Task<JsonElement> BeginTransactionAsync(CancellationToken ct = default)
    {
        return await RequestAsync(new() { ["cmd"] = "begin_tx" }, ct);
    }

    public async Task CommitTransactionAsync(CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "commit_tx" }, ct);
    }

    public async Task RollbackTransactionAsync(CancellationToken ct = default)
    {
        await RequestAsync(new() { ["cmd"] = "rollback_tx" }, ct);
    }

    public async Task WithTransactionAsync(Func<Task> action, CancellationToken ct = default)
    {
        await BeginTransactionAsync(ct);
        try
        {
            await action();
            await CommitTransactionAsync(ct);
        }
        catch
        {
            await RollbackTransactionAsync(ct);
            throw;
        }
    }

    // ── Compaction ──────────────────────────────────────────────────────

    public async Task<JsonElement> CompactAsync(string collection, CancellationToken ct = default)
    {
        return await RequestAsync(new() { ["cmd"] = "compact", ["collection"] = collection }, ct);
    }

    // ── Dispose ─────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _stream.Dispose();
            _tcp.Dispose();
            _lock.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _stream.DisposeAsync();
            _tcp.Dispose();
            _lock.Dispose();
        }
    }
}

// OxiDbTcpException now lives in Exceptions.cs as a legacy alias of
// OxiDbException — kept for binary compatibility, marked [Obsolete].
