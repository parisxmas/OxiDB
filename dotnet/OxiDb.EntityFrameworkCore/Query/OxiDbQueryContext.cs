using Microsoft.EntityFrameworkCore.Query;
using OxiDb.Client.Tcp;

namespace OxiDb.EntityFrameworkCore.Query;

/// <summary>
/// Query context that holds a reference to the client for query execution.
/// </summary>
public sealed class OxiDbQueryContext : QueryContext
{
    public IOxiDbClient Client { get; }

    public OxiDbQueryContext(
        QueryContextDependencies dependencies,
        IOxiDbClient client)
        : base(dependencies)
    {
        Client = client;
    }
}
