using System.Linq.Expressions;
using OxiDb.Client.Tcp;

namespace OxiDb.Linq;

/// <summary>
/// A typed view over an OxiDB collection. Use it as the entry point for LINQ
/// queries (it implements <see cref="IOrderedQueryable{T}"/>) and for direct
/// document insertion. Materialise queries with the <see cref="OxiQueryAsyncExtensions"/>
/// methods (<c>ToListAsync</c>, <c>FirstAsync</c>, <c>CountAsync</c>, ...).
/// </summary>
public sealed class OxiCollection<T> : IOrderedQueryable<T>, IOxiCollectionInternal
{
    public string Name { get; }
    public IOxiDbClient Client { get; }
    public Type ElementType => typeof(T);

    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public OxiCollection(IOxiDbClient client, string name)
    {
        Client = client ?? throw new ArgumentNullException(nameof(client));
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Provider = new OxiQueryProvider(this);
        Expression = Expression.Constant(this);
    }

    /// <summary>Insert a single document. Returns the server-assigned id.</summary>
    public async Task<string> InsertAsync(T document, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(document);
        var resp = await Client.InsertAsync(Name, document!, ct).ConfigureAwait(false);
        return resp.TryGetProperty("id", out var id) ? id.ToString() : resp.ToString();
    }

    /// <summary>Bulk insert. Order preserved.</summary>
    public Task InsertManyAsync(IEnumerable<T> documents, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(documents);
        return Client.InsertManyAsync(Name, documents.Cast<object>(), ct);
    }

    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression)!.GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// Intermediate queryable returned by chained LINQ operators
/// (<c>Where</c>, <c>OrderBy</c>, <c>Select</c>, ...). Not constructed by users.
/// </summary>
public sealed class OxiQueryable<T> : IOrderedQueryable<T>
{
    private readonly IOxiCollectionInternal _source;

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public OxiQueryable(IOxiCollectionInternal source, Expression expression)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        Provider = new OxiQueryProvider(source);
    }

    public IEnumerator<T> GetEnumerator()
        => Provider.Execute<IEnumerable<T>>(Expression)!.GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// Internal handshake between the provider and translator/executor.
/// Public so the provider in this assembly can carry a typed reference back
/// to the originating collection (which knows its name and client).
/// </summary>
public interface IOxiCollectionInternal
{
    string Name { get; }
    IOxiDbClient Client { get; }
    Type ElementType { get; }
}
