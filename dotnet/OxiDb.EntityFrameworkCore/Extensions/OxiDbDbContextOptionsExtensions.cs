using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using OxiDb.EntityFrameworkCore.Infrastructure;

namespace OxiDb.EntityFrameworkCore.Extensions;

public static class OxiDbDbContextOptionsExtensions
{
    public static DbContextOptionsBuilder UseOxiDb(
        this DbContextOptionsBuilder optionsBuilder,
        string host = "127.0.0.1",
        int port = 4444,
        string? username = null,
        string? password = null)
    {
        var extension = new OxiDbOptionsExtension(host, port, username, password);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseOxiDbEmbedded(
        this DbContextOptionsBuilder optionsBuilder,
        string dataPath,
        string? encryptionKeyPath = null)
    {
        var extension = new OxiDbOptionsExtension(dataPath, encryptionKeyPath);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        return optionsBuilder;
    }
}
