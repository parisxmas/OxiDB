using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace OxiDb.EntityFrameworkCore;

/// <summary>EF Core options extension carrying the OxiDB connection string.</summary>
public sealed class OxiDbOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public OxiDbOptionsExtension() { }
    private OxiDbOptionsExtension(OxiDbOptionsExtension copyFrom) : base(copyFrom) { }

    public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    protected override RelationalOptionsExtension Clone() => new OxiDbOptionsExtension(this);

    public override void ApplyServices(IServiceCollection services) =>
        services.AddEntityFrameworkOxiDb();

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension)
        : RelationalExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => true;
        public override string LogFragment => "using OxiDB ";

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) =>
            debugInfo["OxiDb"] = "1";
    }
}

/// <summary>
/// Provider-specific options (`UseOxiDb(cs, o => o.MaxBatchSize(100))`); the
/// relational base supplies MaxBatchSize/MinBatchSize/CommandTimeout.
/// </summary>
public sealed class OxiDbDbContextOptionsBuilder
    : RelationalDbContextOptionsBuilder<OxiDbDbContextOptionsBuilder, OxiDbOptionsExtension>
{
    public OxiDbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder) { }
}

/// <summary>`optionsBuilder.UseOxiDb("Host=...;Port=...")`.</summary>
public static class OxiDbDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseOxiDb(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<OxiDbDbContextOptionsBuilder>? oxiDbOptionsAction = null)
    {
        var extension = (optionsBuilder.Options.FindExtension<OxiDbOptionsExtension>()
                ?? new OxiDbOptionsExtension())
            .WithConnectionString(connectionString);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder)
            .AddOrUpdateExtension((OxiDbOptionsExtension)extension);
        oxiDbOptionsAction?.Invoke(new OxiDbDbContextOptionsBuilder(optionsBuilder));
        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseOxiDb<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString,
        Action<OxiDbDbContextOptionsBuilder>? oxiDbOptionsAction = null)
        where TContext : DbContext =>
        (DbContextOptionsBuilder<TContext>)UseOxiDb(
            (DbContextOptionsBuilder)optionsBuilder, connectionString, oxiDbOptionsAction);
}
