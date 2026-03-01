using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace OxiDb.EntityFrameworkCore.Infrastructure;

public sealed class OxiDbDatabaseProvider : DatabaseProvider<OxiDbOptionsExtension>
{
    public OxiDbDatabaseProvider(DatabaseProviderDependencies dependencies)
        : base(dependencies)
    {
    }
}
