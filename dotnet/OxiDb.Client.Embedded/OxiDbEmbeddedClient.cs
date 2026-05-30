using System.Runtime.InteropServices;
using System.Text.Json;
using OxiDb.Client.Tcp;

namespace OxiDb.Client.Embedded;

/// <summary>
/// In-process OxiDB client via FFI. Implements the same IOxiDbClient interface as the TCP client.
/// All operations are synchronous under the hood (FFI is sync) but wrapped in Task for interface compatibility.
/// Thread-safe via SemaphoreSlim.
/// </summary>
public sealed class OxiDbEmbeddedClient : IOxiDbClient
{
    private nint _handle;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _disposed;

    private OxiDbEmbeddedClient(nint handle)
    {
        _handle = handle;
    }

    public static OxiDbEmbeddedClient Open(string path)
    {
        var handle = NativeInterop.Open(path);
        if (handle == 0)
            throw new OxiDbEmbeddedException("Failed to open database at: " + path);
        return new OxiDbEmbeddedClient(handle);
    }

    public static OxiDbEmbeddedClient OpenEncrypted(string path, string keyPath)
    {
        var handle = NativeInterop.OpenEncrypted(path, keyPath);
        if (handle == 0)
            throw new OxiDbEmbeddedException("Failed to open encrypted database at: " + path);
        return new OxiDbEmbeddedClient(handle);
    }

    private JsonElement Execute(Dictionary<string, object?> command)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _lock.Wait();
        try
        {
            var cmdJson = JsonSerializer.Serialize(command);
            var resultPtr = NativeInterop.Execute(_handle, cmdJson);
            if (resultPtr == 0)
                throw new OxiDbEmbeddedException("FFI call returned null");

            try
            {
                var resultJson = Marshal.PtrToStringUTF8(resultPtr)
                    ?? throw new OxiDbEmbeddedException("Failed to read FFI response");

                using var doc = JsonDocument.Parse(resultJson);
                var root = doc.RootElement;

                if (!root.TryGetProperty("ok", out var okProp) || !okProp.GetBoolean())
                {
                    var errMsg = root.TryGetProperty("error", out var errProp)
                        ? errProp.GetString() ?? "unknown error"
                        : "unknown error";
                    throw new OxiDbEmbeddedException(errMsg);
                }

                if (root.TryGetProperty("data", out var dataProp))
                    return dataProp.Clone();

                return default;
            }
            finally
            {
                NativeInterop.FreeString(resultPtr);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    // ── Authentication ───────────────────────────────────────────────

    /// <summary>
    /// No-op for embedded mode — authentication is not applicable.
    /// </summary>
    public Task AuthSimpleAsync(string username, string password, CancellationToken ct = default)
        => Task.CompletedTask;

    // ── Utility ─────────────────────────────────────────────────────────

    public Task<string> PingAsync(CancellationToken ct = default)
    {
        var data = Execute(new() { ["cmd"] = "ping" });
        return Task.FromResult(data.GetString() ?? "pong");
    }

    // ── Collection management ───────────────────────────────────────────

    public Task CreateCollectionAsync(string name, CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "create_collection", ["collection"] = name });
        return Task.CompletedTask;
    }

    public Task CreateCollectionWithOptionsAsync(string name, StorageOptions options, CancellationToken ct = default)
    {
        Execute(new()
        {
            ["cmd"] = "create_collection_with_options",
            ["collection"] = name,
            ["options"] = options.ToWire(),
        });
        return Task.CompletedTask;
    }

    public Task<List<string>> ListCollectionsAsync(CancellationToken ct = default)
    {
        var data = Execute(new() { ["cmd"] = "list_collections" });
        var result = new List<string>();
        if (data.ValueKind == JsonValueKind.Array)
            foreach (var item in data.EnumerateArray())
                result.Add(item.GetString()!);
        return Task.FromResult(result);
    }

    public Task DropCollectionAsync(string name, CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "drop_collection", ["collection"] = name });
        return Task.CompletedTask;
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    public Task<JsonElement> InsertAsync(string collection, object doc, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new() { ["cmd"] = "insert", ["collection"] = collection, ["doc"] = doc }));
    }

    public Task<JsonElement> InsertManyAsync(string collection, IEnumerable<object> docs, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new() { ["cmd"] = "insert_many", ["collection"] = collection, ["docs"] = docs }));
    }

    public Task<JsonElement> FindAsync(
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
        return Task.FromResult(Execute(payload));
    }

    public Task<JsonElement> FindOneAsync(string collection, object query, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new()
        {
            ["cmd"] = "find_one",
            ["collection"] = collection,
            ["query"] = query
        }));
    }

    public Task<JsonElement> UpdateAsync(string collection, object query, object update, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new()
        {
            ["cmd"] = "update",
            ["collection"] = collection,
            ["query"] = query,
            ["update"] = update
        }));
    }

    public Task<JsonElement> UpdateOneAsync(string collection, object query, object update, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new()
        {
            ["cmd"] = "update_one",
            ["collection"] = collection,
            ["query"] = query,
            ["update"] = update
        }));
    }

    public Task<JsonElement> DeleteAsync(string collection, object query, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new()
        {
            ["cmd"] = "delete",
            ["collection"] = collection,
            ["query"] = query
        }));
    }

    public Task<JsonElement> DeleteOneAsync(string collection, object query, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new()
        {
            ["cmd"] = "delete_one",
            ["collection"] = collection,
            ["query"] = query
        }));
    }

    public Task<int> CountAsync(string collection, object? query = null, CancellationToken ct = default)
    {
        var data = Execute(new()
        {
            ["cmd"] = "count",
            ["collection"] = collection,
            ["query"] = query ?? new Dictionary<string, object?>()
        });
        if (data.ValueKind == JsonValueKind.Object && data.TryGetProperty("count", out var countProp))
            return Task.FromResult(countProp.GetInt32());
        return Task.FromResult(0);
    }

    // ── Indexes ─────────────────────────────────────────────────────────

    public Task CreateIndexAsync(string collection, string field, CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "create_index", ["collection"] = collection, ["field"] = field });
        return Task.CompletedTask;
    }

    public Task CreateUniqueIndexAsync(string collection, string field, CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "create_unique_index", ["collection"] = collection, ["field"] = field });
        return Task.CompletedTask;
    }

    public Task CreateCompositeIndexAsync(string collection, string[] fields, CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "create_composite_index", ["collection"] = collection, ["fields"] = fields });
        return Task.CompletedTask;
    }

    public Task<JsonElement> ListIndexesAsync(string collection, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new() { ["cmd"] = "list_indexes", ["collection"] = collection }));
    }

    public Task DropIndexAsync(string collection, string indexName, CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "drop_index", ["collection"] = collection, ["index"] = indexName });
        return Task.CompletedTask;
    }

    // ── Aggregation ─────────────────────────────────────────────────────

    public Task<JsonElement> AggregateAsync(string collection, object[] pipeline, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new()
        {
            ["cmd"] = "aggregate",
            ["collection"] = collection,
            ["pipeline"] = pipeline
        }));
    }

    // ── Transactions ────────────────────────────────────────────────────

    public Task<JsonElement> BeginTransactionAsync(CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new() { ["cmd"] = "begin_tx" }));
    }

    public Task CommitTransactionAsync(CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "commit_tx" });
        return Task.CompletedTask;
    }

    public Task RollbackTransactionAsync(CancellationToken ct = default)
    {
        Execute(new() { ["cmd"] = "rollback_tx" });
        return Task.CompletedTask;
    }

    // ── Compaction ──────────────────────────────────────────────────────

    public Task<JsonElement> CompactAsync(string collection, CancellationToken ct = default)
    {
        return Task.FromResult(Execute(new() { ["cmd"] = "compact", ["collection"] = collection }));
    }

    // ── Dispose ─────────────────────────────────────────────────────────

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            NativeInterop.Close(_handle);
            _handle = 0;
            _lock.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}

public class OxiDbEmbeddedException : Exception
{
    public OxiDbEmbeddedException(string message) : base(message) { }
}
