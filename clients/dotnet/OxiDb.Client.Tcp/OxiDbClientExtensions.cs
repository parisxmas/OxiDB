using System.Runtime.CompilerServices;
using System.Text.Json;

namespace OxiDb.Client.Tcp;

/// <summary>
/// Developer-friendly extension methods on <see cref="IOxiDbClient"/> — the
/// typed find/insert/update overloads that the base interface returns
/// <see cref="JsonElement"/> for. These build on top of the existing wire
/// API; they don't add new server-side commands.
/// </summary>
public static class OxiDbClientExtensions
{
    private static readonly JsonSerializerOptions DefaultJson = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    // ── HELLO handshake ─────────────────────────────────────────────────

    /// <summary>
    /// Invoke the wire-level HELLO handshake. Returns the server's
    /// version, supported wire versions, advertised feature sets, and
    /// auth methods. Pre-auth: safe to call on a fresh connection.
    /// Idempotent.
    /// </summary>
    /// <param name="client">The connected client (must be an <see cref="OxiDbTcpClient"/>
    /// — embedded clients have no remote server to handshake with).</param>
    /// <param name="clientId">Optional client-identification string the
    /// server logs (e.g. <c>"oxidb-net/1.0"</c>).</param>
    /// <param name="wireVersions">Wire versions this client knows how to
    /// speak. Defaults to <c>[1]</c>.</param>
    public static async Task<HelloResponse> HelloAsync(
        this IOxiDbClient client,
        string? clientId = null,
        IReadOnlyList<uint>? wireVersions = null,
        CancellationToken ct = default)
    {
        if (client is not OxiDbTcpClient tcp)
            throw new NotSupportedException(
                "HELLO is a wire-protocol handshake — only meaningful on TCP clients. " +
                "Embedded clients link the engine directly and have no server to negotiate with.");

        var payload = new Dictionary<string, object?>
        {
            ["cmd"] = "hello",
        };
        if (clientId is not null) payload["client"] = clientId;
        payload["wire_versions"] = wireVersions ?? new uint[] { 1 };

        // HELLO doesn't use the usual `{"ok": true, "data": ...}` envelope —
        // the info is under `server`. ExecRawEnvelopeAsync returns the
        // whole response root so we can pick the right field.
        var envelope = await tcp.ExecRawEnvelopeAsync(payload, ct);
        if (envelope.TryGetProperty("server", out var serverProp))
        {
            return JsonSerializer.Deserialize<HelloResponse>(serverProp.GetRawText(), DefaultJson)
                ?? throw new OxiDbException("HELLO response was empty");
        }
        throw new OxiDbException("HELLO response missing 'server' field");
    }

    // ── Typed CRUD ──────────────────────────────────────────────────────

    /// <summary>
    /// Find documents and deserialize each match to <typeparamref name="T"/>.
    /// </summary>
    public static async Task<List<T>> FindAsync<T>(
        this IOxiDbClient client,
        string collection,
        object? query = null,
        object? sort = null,
        int? skip = null,
        int? limit = null,
        CancellationToken ct = default)
    {
        var result = await client.FindAsync(collection, query, sort, skip, limit, ct);
        if (result.ValueKind != JsonValueKind.Array)
            return new List<T>();

        var list = new List<T>(result.GetArrayLength());
        foreach (var elem in result.EnumerateArray())
        {
            var item = elem.Deserialize<T>(DefaultJson);
            if (item is not null) list.Add(item);
        }
        return list;
    }

    /// <summary>
    /// Find at most one document matching the query, deserialized to
    /// <typeparamref name="T"/>. Returns <c>default</c> if no match.
    /// </summary>
    public static async Task<T?> FindOneAsync<T>(
        this IOxiDbClient client,
        string collection,
        object query,
        CancellationToken ct = default)
    {
        var result = await client.FindOneAsync(collection, query, ct);
        if (result.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return default;
        return result.Deserialize<T>(DefaultJson);
    }

    /// <summary>
    /// Insert a document and return its server-assigned <c>_id</c> as a
    /// strongly-typed <see cref="long"/>.
    /// </summary>
    public static async Task<long> InsertReturningIdAsync(
        this IOxiDbClient client,
        string collection,
        object doc,
        CancellationToken ct = default)
    {
        var result = await client.InsertAsync(collection, doc, ct);

        // Server response shape: { "id": <number> }
        if (result.TryGetProperty("id", out var idProp))
        {
            return idProp.ValueKind switch
            {
                JsonValueKind.Number => idProp.GetInt64(),
                JsonValueKind.String when long.TryParse(idProp.GetString(), out var n) => n,
                _ => throw new OxiDbException($"Insert returned non-numeric id: {idProp.GetRawText()}"),
            };
        }
        throw new OxiDbException($"Insert response missing 'id' field: {result.GetRawText()}");
    }

    /// <summary>
    /// Insert many documents and return the server-assigned <c>_id</c>
    /// values in insertion order.
    /// </summary>
    public static async Task<long[]> InsertManyReturningIdsAsync(
        this IOxiDbClient client,
        string collection,
        IEnumerable<object> docs,
        CancellationToken ct = default)
    {
        var result = await client.InsertManyAsync(collection, docs, ct);

        if (result.TryGetProperty("ids", out var idsProp) && idsProp.ValueKind == JsonValueKind.Array)
        {
            var ids = new long[idsProp.GetArrayLength()];
            int i = 0;
            foreach (var idElem in idsProp.EnumerateArray())
            {
                ids[i++] = idElem.ValueKind switch
                {
                    JsonValueKind.Number => idElem.GetInt64(),
                    _ => throw new OxiDbException($"InsertMany returned non-numeric id at index {i - 1}"),
                };
            }
            return ids;
        }
        throw new OxiDbException($"InsertMany response missing 'ids' field: {result.GetRawText()}");
    }

    // ── IAsyncEnumerable streaming ─────────────────────────────────────

    /// <summary>
    /// Stream a query's result set as an async sequence, fetched in
    /// batches via LIMIT/SKIP pagination. The result set is materialised
    /// progressively — useful for large queries that would otherwise
    /// blow up memory if you called <c>FindAsync&lt;T&gt;</c>.
    /// </summary>
    /// <param name="client">The OxiDB client.</param>
    /// <param name="collection">Collection name.</param>
    /// <param name="query">JSON query (anonymous-object or <see cref="Query"/> builder result).</param>
    /// <param name="batchSize">Documents per server roundtrip. Default 1000.</param>
    /// <param name="sort">Sort spec — REQUIRED if you want a stable
    /// iteration order across paginated batches. Without one, two batches
    /// can overlap or skip rows when the underlying storage order shifts.</param>
    public static async IAsyncEnumerable<T> StreamAsync<T>(
        this IOxiDbClient client,
        string collection,
        object? query = null,
        int batchSize = 1000,
        object? sort = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (batchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be positive");

        int skip = 0;
        while (true)
        {
            var batch = await client.FindAsync<T>(collection, query, sort, skip, batchSize, ct);
            if (batch.Count == 0) yield break;

            foreach (var item in batch)
                yield return item;

            if (batch.Count < batchSize) yield break;
            skip += batch.Count;
        }
    }
}
