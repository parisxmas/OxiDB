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

    public JsonDocument FindOne(string collection, string queryJson)
    {
        var raw = Call(() => NativeInterop.FindOne(_conn, collection, queryJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Update(string collection, string queryJson, string updateJson)
    {
        var raw = Call(() => NativeInterop.Update(_conn, collection, queryJson, updateJson));
        return JsonDocument.Parse(raw);
    }

    public JsonDocument Delete(string collection, string queryJson)
    {
        var raw = Call(() => NativeInterop.Delete(_conn, collection, queryJson));
        return JsonDocument.Parse(raw);
    }

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
