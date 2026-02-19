using System.Runtime.InteropServices;
using System.Text.Json;

namespace OxiDb.Client;

public sealed class OxiDbClient : IDisposable
{
    private nint _conn;
    private bool _disposed;

    private OxiDbClient(nint conn)
    {
        _conn = conn;
    }

    public static OxiDbClient Connect(string host = "127.0.0.1", ushort port = 4444)
    {
        var conn = NativeInterop.Connect(host, port);
        if (conn == 0)
            throw new OxiDbException("Failed to connect to OxiDB server");
        return new OxiDbClient(conn);
    }

    public string Ping()
    {
        return Call(() => NativeInterop.Ping(_conn));
    }

    public JsonDocument Insert(string collection, string docJson)
    {
        var raw = Call(() => NativeInterop.Insert(_conn, collection, docJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument InsertMany(string collection, string docsJson)
    {
        var raw = Call(() => NativeInterop.InsertMany(_conn, collection, docsJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Find(string collection, string queryJson)
    {
        var raw = Call(() => NativeInterop.Find(_conn, collection, queryJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Find(string collection, Filter filter) =>
        Find(collection, filter.ToJson());

    public JsonDocument FindOne(string collection, string queryJson)
    {
        var raw = Call(() => NativeInterop.FindOne(_conn, collection, queryJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument FindOne(string collection, Filter filter) =>
        FindOne(collection, filter.ToJson());

    public JsonDocument Update(string collection, string queryJson, string updateJson)
    {
        var raw = Call(() => NativeInterop.Update(_conn, collection, queryJson, updateJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Update(string collection, Filter filter, UpdateDef update) =>
        Update(collection, filter.ToJson(), update.ToJson());

    public JsonDocument UpdateOne(string collection, string queryJson, string updateJson)
    {
        var raw = Call(() => NativeInterop.UpdateOne(_conn, collection, queryJson, updateJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument UpdateOne(string collection, Filter filter, UpdateDef update) =>
        UpdateOne(collection, filter.ToJson(), update.ToJson());

    public JsonDocument Delete(string collection, string queryJson)
    {
        var raw = Call(() => NativeInterop.Delete(_conn, collection, queryJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Delete(string collection, Filter filter) =>
        Delete(collection, filter.ToJson());

    public JsonDocument DeleteOne(string collection, string queryJson)
    {
        var raw = Call(() => NativeInterop.DeleteOne(_conn, collection, queryJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument DeleteOne(string collection, Filter filter) =>
        DeleteOne(collection, filter.ToJson());

    public JsonDocument Count(string collection)
    {
        var raw = Call(() => NativeInterop.Count(_conn, collection));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument CreateIndex(string collection, string field)
    {
        var raw = Call(() => NativeInterop.CreateIndex(_conn, collection, field));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument CreateCompositeIndex(string collection, string fieldsJson)
    {
        var raw = Call(() => NativeInterop.CreateCompositeIndex(_conn, collection, fieldsJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument CreateUniqueIndex(string collection, string field)
    {
        var raw = Call(() => NativeInterop.CreateUniqueIndex(_conn, collection, field));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument CreateTextIndex(string collection, string fieldsJson)
    {
        var raw = Call(() => NativeInterop.CreateTextIndex(_conn, collection, fieldsJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument ListIndexes(string collection)
    {
        var raw = Call(() => NativeInterop.ListIndexes(_conn, collection));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument DropIndex(string collection, string indexName)
    {
        var raw = Call(() => NativeInterop.DropIndex(_conn, collection, indexName));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument TextSearch(string collection, string query, int limit = 10)
    {
        var raw = Call(() => NativeInterop.TextSearch(_conn, collection, query, limit));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument CreateCollection(string collection)
    {
        var raw = Call(() => NativeInterop.CreateCollection(_conn, collection));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument ListCollections()
    {
        var raw = Call(() => NativeInterop.ListCollections(_conn));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument DropCollection(string collection)
    {
        var raw = Call(() => NativeInterop.DropCollection(_conn, collection));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Aggregate(string collection, string pipelineJson)
    {
        var raw = Call(() => NativeInterop.Aggregate(_conn, collection, pipelineJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Compact(string collection)
    {
        var raw = Call(() => NativeInterop.Compact(_conn, collection));
        return JsonDocument.Parse(raw);
    }

    // Transactions

    public JsonDocument BeginTransaction()
    {
        var raw = Call(() => NativeInterop.BeginTx(_conn));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument CommitTransaction()
    {
        var raw = Call(() => NativeInterop.CommitTx(_conn));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument RollbackTransaction()
    {
        var raw = Call(() => NativeInterop.RollbackTx(_conn));
        return JsonDocument.Parse(raw);
    }

    // Blob storage + FTS

    public JsonDocument CreateBucket(string bucket)
    {
        var raw = Call(() => NativeInterop.CreateBucket(_conn, bucket));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument ListBuckets()
    {
        var raw = Call(() => NativeInterop.ListBuckets(_conn));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument DeleteBucket(string bucket)
    {
        var raw = Call(() => NativeInterop.DeleteBucket(_conn, bucket));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument PutObject(string bucket, string key, string dataB64, string? contentType = null, string? metadataJson = null)
    {
        var raw = Call(() => NativeInterop.PutObject(_conn, bucket, key, dataB64, contentType, metadataJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument GetObject(string bucket, string key)
    {
        var raw = Call(() => NativeInterop.GetObject(_conn, bucket, key));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument HeadObject(string bucket, string key)
    {
        var raw = Call(() => NativeInterop.HeadObject(_conn, bucket, key));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument DeleteObject(string bucket, string key)
    {
        var raw = Call(() => NativeInterop.DeleteObject(_conn, bucket, key));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument ListObjects(string bucket, string? prefix = null, int limit = 0)
    {
        var raw = Call(() => NativeInterop.ListObjects(_conn, bucket, prefix, limit));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Search(string query, string? bucket = null, int limit = 0)
    {
        var raw = Call(() => NativeInterop.Search(_conn, query, bucket, limit));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Sql(string query)
    {
        var raw = Call(() => NativeInterop.Sql(_conn, query));
        return JsonDocument.Parse(raw);
    }

    private string Call(Func<nint> nativeCall)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var ptr = nativeCall();
        if (ptr == 0)
            throw new OxiDbException("Native call returned null (connection error or invalid input)");

        try
        {
            return Marshal.PtrToStringUTF8(ptr)
                   ?? throw new OxiDbException("Failed to marshal response string");
        }
        finally
        {
            NativeInterop.FreeString(ptr);
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            if (_conn != 0)
            {
                NativeInterop.Disconnect(_conn);
                _conn = 0;
            }
        }
    }
}

public class OxiDbException : Exception
{
    public OxiDbException(string message) : base(message) { }
}
