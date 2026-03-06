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

    // ── Low-level protocol ──────────────────────────────────────────────

    private async Task SendAsync(byte[] data, CancellationToken ct)
    {
        var lenBuf = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(lenBuf, (uint)data.Length);
        await _stream.WriteAsync(lenBuf, ct);
        await _stream.WriteAsync(data, ct);
        await _stream.FlushAsync(ct);
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
                throw new OxiDbTcpException("Connection closed by server");
            offset += read;
        }
    }

    private async Task<JsonElement> RequestAsync(Dictionary<string, object?> payload, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _lock.WaitAsync(ct);
        try
        {
            byte[] reqBytes;
            if (_useOxiWire)
                reqBytes = OxiWire.EncodeRequest(payload);
            else
                reqBytes = JsonSerializer.SerializeToUtf8Bytes(payload);

            await SendAsync(reqBytes, ct);
            var respBytes = await ReceiveAsync(ct);

            if (_useOxiWire && OxiWire.IsOxiWire(respBytes))
            {
                var (ok, data) = OxiWire.DecodeResponse(respBytes);
                if (!ok)
                {
                    var errMsg = data.ValueKind == JsonValueKind.String
                        ? data.GetString() ?? "unknown error"
                        : "unknown error";
                    throw new OxiDbTcpException(errMsg);
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
                throw new OxiDbTcpException(errMsg);
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

public class OxiDbTcpException : Exception
{
    public OxiDbTcpException(string message) : base(message) { }
}
