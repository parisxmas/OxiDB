using System.Text.Json;

namespace OxiDb.Client.Tcp;

/// <summary>
/// Common interface for OxiDB clients (TCP and embedded).
/// Both transports use the same JSON command protocol.
/// </summary>
public interface IOxiDbClient : IAsyncDisposable, IDisposable
{
    Task<string> PingAsync(CancellationToken ct = default);

    // Authentication
    Task AuthSimpleAsync(string username, string password, CancellationToken ct = default);

    // Collection management
    Task CreateCollectionAsync(string name, CancellationToken ct = default);
    Task CreateCollectionWithOptionsAsync(string name, StorageOptions options, CancellationToken ct = default);
    Task<List<string>> ListCollectionsAsync(CancellationToken ct = default);
    Task DropCollectionAsync(string name, CancellationToken ct = default);

    // CRUD
    Task<JsonElement> InsertAsync(string collection, object doc, CancellationToken ct = default);
    Task<JsonElement> InsertManyAsync(string collection, IEnumerable<object> docs, CancellationToken ct = default);
    Task<JsonElement> FindAsync(string collection, object? query = null, object? sort = null, int? skip = null, int? limit = null, CancellationToken ct = default);
    Task<JsonElement> FindOneAsync(string collection, object query, CancellationToken ct = default);
    Task<JsonElement> UpdateAsync(string collection, object query, object update, CancellationToken ct = default);
    Task<JsonElement> UpdateOneAsync(string collection, object query, object update, CancellationToken ct = default);
    Task<JsonElement> DeleteAsync(string collection, object query, CancellationToken ct = default);
    Task<JsonElement> DeleteOneAsync(string collection, object query, CancellationToken ct = default);
    Task<int> CountAsync(string collection, object? query = null, CancellationToken ct = default);

    // Indexes
    Task CreateIndexAsync(string collection, string field, CancellationToken ct = default);
    Task CreateUniqueIndexAsync(string collection, string field, CancellationToken ct = default);
    Task CreateCompositeIndexAsync(string collection, string[] fields, CancellationToken ct = default);
    Task<JsonElement> ListIndexesAsync(string collection, CancellationToken ct = default);
    Task DropIndexAsync(string collection, string indexName, CancellationToken ct = default);

    // Aggregation
    Task<JsonElement> AggregateAsync(string collection, object[] pipeline, CancellationToken ct = default);

    // Transactions
    Task<JsonElement> BeginTransactionAsync(CancellationToken ct = default);
    Task CommitTransactionAsync(CancellationToken ct = default);
    Task RollbackTransactionAsync(CancellationToken ct = default);

    // Maintenance
    Task<JsonElement> CompactAsync(string collection, CancellationToken ct = default);
}
