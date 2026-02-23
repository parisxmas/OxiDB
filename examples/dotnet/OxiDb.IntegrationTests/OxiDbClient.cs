using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace OxiDb.IntegrationTests;

/// <summary>
/// TCP client for oxidb-server. Protocol: [u32 LE length][JSON payload].
/// </summary>
public sealed class OxiDbClient : IDisposable
{
    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;

    public OxiDbClient(string host = "127.0.0.1", int port = 4444, int timeoutMs = 10_000)
    {
        _tcp = new TcpClient();
        _tcp.NoDelay = true;
        _tcp.Connect(host, port);
        _stream = _tcp.GetStream();
        _stream.ReadTimeout = timeoutMs;
        _stream.WriteTimeout = timeoutMs;
    }

    public void Dispose()
    {
        _stream.Dispose();
        _tcp.Dispose();
    }

    // ---------------------------------------------------------------
    // Low-level protocol
    // ---------------------------------------------------------------

    private void SendRaw(byte[] data)
    {
        Span<byte> lenBuf = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(lenBuf, (uint)data.Length);
        _stream.Write(lenBuf);
        _stream.Write(data);
        _stream.Flush();
    }

    private byte[] RecvRaw()
    {
        Span<byte> lenBuf = stackalloc byte[4];
        ReadExact(lenBuf);
        var len = (int)BinaryPrimitives.ReadUInt32LittleEndian(lenBuf);
        var buf = new byte[len];
        ReadExact(buf);
        return buf;
    }

    private void ReadExact(Span<byte> buffer)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = _stream.Read(buffer[offset..]);
            if (read == 0)
                throw new IOException("Connection closed by server");
            offset += read;
        }
    }

    public JsonNode Send(JsonObject request)
    {
        var json = request.ToJsonString();
        SendRaw(Encoding.UTF8.GetBytes(json));
        var respBytes = RecvRaw();
        return JsonNode.Parse(respBytes)!;
    }

    private JsonNode Checked(JsonObject request)
    {
        var resp = Send(request);
        if (resp["ok"]?.GetValue<bool>() != true)
        {
            var error = resp["error"]?.GetValue<string>() ?? "unknown error";
            throw new OxiDbException(error);
        }
        return resp["data"]!;
    }

    // ---------------------------------------------------------------
    // Utility
    // ---------------------------------------------------------------

    public string Ping() => Checked(new JsonObject { ["cmd"] = "ping" }).GetValue<string>();

    // ---------------------------------------------------------------
    // Collection management
    // ---------------------------------------------------------------

    public void CreateCollection(string name) =>
        Checked(new JsonObject { ["cmd"] = "create_collection", ["collection"] = name });

    public JsonArray ListCollections() =>
        Checked(new JsonObject { ["cmd"] = "list_collections" }).AsArray();

    public void DropCollection(string name) =>
        Checked(new JsonObject { ["cmd"] = "drop_collection", ["collection"] = name });

    // ---------------------------------------------------------------
    // CRUD
    // ---------------------------------------------------------------

    public JsonNode Insert(string collection, JsonObject doc)
    {
        return Checked(new JsonObject
        {
            ["cmd"] = "insert",
            ["collection"] = collection,
            ["doc"] = doc
        });
    }

    public JsonNode InsertMany(string collection, JsonArray docs)
    {
        return Checked(new JsonObject
        {
            ["cmd"] = "insert_many",
            ["collection"] = collection,
            ["docs"] = docs
        });
    }

    public JsonArray Find(string collection, JsonObject? query = null,
        JsonObject? sort = null, int? skip = null, int? limit = null)
    {
        var req = new JsonObject
        {
            ["cmd"] = "find",
            ["collection"] = collection,
            ["query"] = query ?? new JsonObject()
        };
        if (sort != null) req["sort"] = sort;
        if (skip.HasValue) req["skip"] = skip.Value;
        if (limit.HasValue) req["limit"] = limit.Value;
        return Checked(req).AsArray();
    }

    public JsonNode? FindOne(string collection, JsonObject? query = null)
    {
        var result = Checked(new JsonObject
        {
            ["cmd"] = "find_one",
            ["collection"] = collection,
            ["query"] = query ?? new JsonObject()
        });
        return result is JsonValue v && v.TryGetValue<string>(out _) == false
            ? result
            : result;
    }

    public JsonNode Update(string collection, JsonObject query, JsonObject update)
    {
        return Checked(new JsonObject
        {
            ["cmd"] = "update",
            ["collection"] = collection,
            ["query"] = query,
            ["update"] = update
        });
    }

    public JsonNode Delete(string collection, JsonObject query)
    {
        return Checked(new JsonObject
        {
            ["cmd"] = "delete",
            ["collection"] = collection,
            ["query"] = query
        });
    }

    public int Count(string collection, JsonObject? query = null)
    {
        var result = Checked(new JsonObject
        {
            ["cmd"] = "count",
            ["collection"] = collection,
            ["query"] = query ?? new JsonObject()
        });
        return result["count"]!.GetValue<int>();
    }

    // ---------------------------------------------------------------
    // Indexes
    // ---------------------------------------------------------------

    public void CreateIndex(string collection, string field) =>
        Checked(new JsonObject
        {
            ["cmd"] = "create_index",
            ["collection"] = collection,
            ["field"] = field
        });

    public void CreateUniqueIndex(string collection, string field) =>
        Checked(new JsonObject
        {
            ["cmd"] = "create_unique_index",
            ["collection"] = collection,
            ["field"] = field
        });

    public JsonNode CreateCompositeIndex(string collection, JsonArray fields) =>
        Checked(new JsonObject
        {
            ["cmd"] = "create_composite_index",
            ["collection"] = collection,
            ["fields"] = fields
        });

    // ---------------------------------------------------------------
    // Aggregation
    // ---------------------------------------------------------------

    public JsonArray Aggregate(string collection, JsonArray pipeline) =>
        Checked(new JsonObject
        {
            ["cmd"] = "aggregate",
            ["collection"] = collection,
            ["pipeline"] = pipeline
        }).AsArray();

    // ---------------------------------------------------------------
    // Compaction
    // ---------------------------------------------------------------

    public JsonNode Compact(string collection) =>
        Checked(new JsonObject
        {
            ["cmd"] = "compact",
            ["collection"] = collection
        });

    // ---------------------------------------------------------------
    // Transactions
    // ---------------------------------------------------------------

    public JsonNode BeginTx() =>
        Checked(new JsonObject { ["cmd"] = "begin_tx" });

    public void CommitTx() =>
        Checked(new JsonObject { ["cmd"] = "commit_tx" });

    public void RollbackTx() =>
        Checked(new JsonObject { ["cmd"] = "rollback_tx" });

    // ---------------------------------------------------------------
    // Blob storage
    // ---------------------------------------------------------------

    public void CreateBucket(string bucket) =>
        Checked(new JsonObject { ["cmd"] = "create_bucket", ["bucket"] = bucket });

    public JsonArray ListBuckets() =>
        Checked(new JsonObject { ["cmd"] = "list_buckets" }).AsArray();

    public void DeleteBucket(string bucket) =>
        Checked(new JsonObject { ["cmd"] = "delete_bucket", ["bucket"] = bucket });

    public JsonNode PutObject(string bucket, string key, byte[] data,
        string contentType = "application/octet-stream", JsonObject? metadata = null)
    {
        var req = new JsonObject
        {
            ["cmd"] = "put_object",
            ["bucket"] = bucket,
            ["key"] = key,
            ["data"] = Convert.ToBase64String(data),
            ["content_type"] = contentType
        };
        if (metadata != null) req["metadata"] = metadata;
        return Checked(req);
    }

    public (byte[] Data, JsonNode Metadata) GetObject(string bucket, string key)
    {
        var result = Checked(new JsonObject
        {
            ["cmd"] = "get_object",
            ["bucket"] = bucket,
            ["key"] = key
        });
        var data = Convert.FromBase64String(result["content"]!.GetValue<string>());
        return (data, result["metadata"]!);
    }

    public JsonNode HeadObject(string bucket, string key) =>
        Checked(new JsonObject
        {
            ["cmd"] = "head_object",
            ["bucket"] = bucket,
            ["key"] = key
        });

    public void DeleteObject(string bucket, string key) =>
        Checked(new JsonObject
        {
            ["cmd"] = "delete_object",
            ["bucket"] = bucket,
            ["key"] = key
        });

    public JsonArray ListObjects(string bucket, string? prefix = null, int? limit = null)
    {
        var req = new JsonObject
        {
            ["cmd"] = "list_objects",
            ["bucket"] = bucket
        };
        if (prefix != null) req["prefix"] = prefix;
        if (limit.HasValue) req["limit"] = limit.Value;
        return Checked(req).AsArray();
    }

    // ---------------------------------------------------------------
    // Full-text search
    // ---------------------------------------------------------------

    public JsonArray Search(string query, string? bucket = null, int limit = 10)
    {
        var req = new JsonObject
        {
            ["cmd"] = "search",
            ["query"] = query,
            ["limit"] = limit
        };
        if (bucket != null) req["bucket"] = bucket;
        return Checked(req).AsArray();
    }
}

public class OxiDbException : Exception
{
    public OxiDbException(string message) : base(message) { }
}
