using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.Extensions.DependencyInjection;
using OxiDb.Data;

[assembly: DesignTimeProviderServices("OxiDb.EntityFrameworkCore.OxiDbDesignTimeServices")]

namespace OxiDb.EntityFrameworkCore;

/// <summary>Design-time service registrations (`dotnet ef dbcontext scaffold`).</summary>
public sealed class OxiDbDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection services)
    {
        services.AddEntityFrameworkOxiDb();
        new EntityFrameworkRelationalDesignServicesBuilder(services)
            .TryAdd<IDatabaseModelFactory, OxiDbDatabaseModelFactory>()
            .TryAdd<IProviderConfigurationCodeGenerator, OxiDbCodeGenerator>()
            .TryAddCoreServices();
    }
}

/// <summary>Emits the `UseOxiDb("...")` call in scaffolded contexts.</summary>
public sealed class OxiDbCodeGenerator : ProviderCodeGenerator
{
    // The MethodInfo form lets the code generator emit the provider's
    // `using OxiDb.EntityFrameworkCore;` in the scaffolded context.
    private static readonly System.Reflection.MethodInfo UseOxiDbMethod =
        typeof(OxiDbDbContextOptionsBuilderExtensions).GetMethod(
            nameof(OxiDbDbContextOptionsBuilderExtensions.UseOxiDb),
            [typeof(Microsoft.EntityFrameworkCore.DbContextOptionsBuilder), typeof(string)])!;

    public OxiDbCodeGenerator(ProviderCodeGeneratorDependencies dependencies)
        : base(dependencies) { }

    public override MethodCallCodeFragment GenerateUseProvider(
        string connectionString,
        MethodCallCodeFragment? providerOptions) =>
        providerOptions is null
            ? new(UseOxiDbMethod, connectionString)
            : new(UseOxiDbMethod, connectionString,
                new NestedClosureCodeFragment("x", providerOptions));
}

/// <summary>
/// Reads the live schema over `SHOW TABLES` / `DESCRIBE` / `SHOW INDEXES`
/// into a <see cref="DatabaseModel"/> for reverse engineering.
/// </summary>
public sealed class OxiDbDatabaseModelFactory : DatabaseModelFactory
{
    public override DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
    {
        using var connection = new OxiDbConnection(connectionString);
        connection.Open();
        return Create(connection, options);
    }

    public override DatabaseModel Create(DbConnection connection, DatabaseModelFactoryOptions options)
    {
        var wasOpen = connection.State == ConnectionState.Open;
        if (!wasOpen)
            connection.Open();
        try
        {
            var model = new DatabaseModel { DatabaseName = connection.Database };
            var filter = options.Tables.ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (var table in ListTables(connection))
            {
                if (table == HistoryRepository.DefaultTableName
                    || (filter.Count > 0 && !filter.Contains(table)))
                    continue;
                model.Tables.Add(ReadTable(connection, model, table));
            }
            return model;
        }
        finally
        {
            if (!wasOpen)
                connection.Close();
        }
    }

    private static List<string> ListTables(DbConnection connection)
    {
        var tables = new List<string>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SHOW TABLES";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            tables.Add(reader.GetString(0));
        return tables;
    }

    private static DatabaseTable ReadTable(DbConnection connection, DatabaseModel model, string name)
    {
        var table = new DatabaseTable { Database = model, Name = name };

        // Columns: (column, type, nullable, primary_key, auto_increment).
        var pkColumns = new List<DatabaseColumn>();
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"DESCRIBE \"{name}\"";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var column = new DatabaseColumn
                {
                    Table = table,
                    Name = reader.GetString(0),
                    StoreType = reader.GetString(1),
                    IsNullable = reader.GetBoolean(2),
                };
                if (reader.GetBoolean(4))
                    column.ValueGenerated = Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAdd;
                table.Columns.Add(column);
                if (reader.GetBoolean(3))
                    pkColumns.Add(column);
            }
        }
        if (pkColumns.Count > 0)
        {
            var pk = new DatabasePrimaryKey { Table = table, Name = $"PK_{name}" };
            foreach (var c in pkColumns)
                pk.Columns.Add(c);
            table.PrimaryKey = pk;
        }

        // Secondary indexes: (index, table, columns) — columns comma-joined.
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"SHOW INDEXES FROM \"{name}\"";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var index = new DatabaseIndex { Table = table, Name = reader.GetString(0) };
                foreach (var col in reader.GetString(2).Split(',', StringSplitOptions.TrimEntries))
                {
                    var column = table.Columns.FirstOrDefault(c => c.Name == col);
                    if (column is not null)
                        index.Columns.Add(column);
                }
                if (index.Columns.Count > 0)
                    table.Indexes.Add(index);
            }
        }
        return table;
    }
}
