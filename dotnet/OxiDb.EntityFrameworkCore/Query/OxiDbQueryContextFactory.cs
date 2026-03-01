using Microsoft.EntityFrameworkCore.Query;
using OxiDb.EntityFrameworkCore.Storage;

namespace OxiDb.EntityFrameworkCore.Query;

public sealed class OxiDbQueryContextFactory : IQueryContextFactory
{
    private readonly QueryContextDependencies _dependencies;
    private readonly OxiDbClientManager _clientManager;

    public OxiDbQueryContextFactory(
        QueryContextDependencies dependencies,
        OxiDbClientManager clientManager)
    {
        _dependencies = dependencies;
        _clientManager = clientManager;
    }

    public QueryContext Create()
    {
        var client = _clientManager.GetClientAsync().GetAwaiter().GetResult();
        return new OxiDbQueryContext(_dependencies, client);
    }
}
