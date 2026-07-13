using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using OxiDb.Data;
using OxiDb.EntityFrameworkCore;

namespace OxiDb.EFCore.SpecTests;

/// <summary>
/// A test database on the live OxiDB server (`OXIDB_PORT`, default 4544).
/// Each store name maps to its own server-side database; Clean drops and
/// recreates it over the wire.
/// </summary>
public class OxiDbTestStore : RelationalTestStore
{
    private static readonly int Port =
        int.Parse(Environment.GetEnvironmentVariable("OXIDB_PORT") ?? "4544");

    public static OxiDbTestStore Create(string name) => new(name, shared: false);
    public static OxiDbTestStore GetOrCreate(string name) => new(name, shared: true);

    private OxiDbTestStore(string name, bool shared)
        : base(name, shared, new OxiDbConnection(MakeConnectionString(name)))
    {
    }

    private static string MakeConnectionString(string name) =>
        $"Host=127.0.0.1;Port={Port};Database={Sanitize(name)}";

    // Store names may carry characters the engine's identifier rules dislike.
    private static string Sanitize(string name) =>
        "spec_" + new string(name.Select(c => char.IsLetterOrDigit(c) ? char.ToLowerInvariant(c) : '_').ToArray());

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder) =>
        builder.UseOxiDb(ConnectionString);

    protected override async Task InitializeAsync(
        Func<DbContext> createContext,
        Func<DbContext, Task>? seed,
        Func<DbContext, Task>? clean)
    {
        await EnsureDatabaseAsync(recreate: false);
        using var context = createContext();
        if (await context.Database.EnsureCreatedResilientlyAsync())
        {
            if (seed is not null)
                await seed(context);
        }
        else
        {
            if (clean is not null)
                await clean(context);
            await CleanAsync(context);
            if (seed is not null)
                await seed(context);
        }
    }

    public override async Task CleanAsync(DbContext context)
    {
        await EnsureDatabaseAsync(recreate: true);
        await context.Database.EnsureCreatedResilientlyAsync();
    }

    private async Task EnsureDatabaseAsync(bool recreate)
    {
        await using var boot = await OxiDb.Client.Tcp.OxiDbTcpClient.ConnectAsync("127.0.0.1", Port);
        var db = Sanitize(Name);
        if (recreate)
            await boot.SqlAsync($"DROP DATABASE IF EXISTS {db}");
        await boot.SqlAsync($"CREATE DATABASE IF NOT EXISTS {db}");
    }
}

public class OxiDbTestStoreFactory : RelationalTestStoreFactory
{
    public static OxiDbTestStoreFactory Instance { get; } = new();

    protected OxiDbTestStoreFactory()
    {
    }

    public override TestStore Create(string storeName) => OxiDbTestStore.Create(storeName);
    public override TestStore GetOrCreate(string storeName) => OxiDbTestStore.GetOrCreate(storeName);

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection) =>
        serviceCollection.AddEntityFrameworkOxiDb();
}
