using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace OxiDb.EntityFrameworkCore.Storage;

/// <summary>
/// IDatabase implementation that translates SaveChanges entries into OxiDB insert/update/delete commands.
/// </summary>
public sealed class OxiDbDatabase : Database
{
    private readonly OxiDbClientManager _clientManager;
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    public OxiDbDatabase(DatabaseDependencies dependencies, OxiDbClientManager clientManager)
        : base(dependencies)
    {
        _clientManager = clientManager;
    }

    public override int SaveChanges(IList<IUpdateEntry> entries)
    {
        return SaveChangesAsync(entries).GetAwaiter().GetResult();
    }

    public override async Task<int> SaveChangesAsync(
        IList<IUpdateEntry> entries,
        CancellationToken ct = default)
    {
        var client = await _clientManager.GetClientAsync(ct);
        int affected = 0;

        foreach (var entry in entries)
        {
            var entityType = entry.EntityType;
            var collectionName = entityType.ShortName().ToLowerInvariant();

            switch (entry.EntityState)
            {
                case EntityState.Added:
                    await HandleInsert(client, collectionName, entry, ct);
                    affected++;
                    break;

                case EntityState.Modified:
                    await HandleUpdate(client, collectionName, entry, ct);
                    affected++;
                    break;

                case EntityState.Deleted:
                    await HandleDelete(client, collectionName, entry, ct);
                    affected++;
                    break;
            }
        }

        return affected;
    }

    private static async Task HandleInsert(
        Client.Tcp.IOxiDbClient client,
        string collection,
        IUpdateEntry entry,
        CancellationToken ct)
    {
        var doc = new Dictionary<string, object?>();

        foreach (var prop in entry.EntityType.GetProperties())
        {
            var value = entry.GetCurrentValue(prop);
            var fieldName = prop.IsPrimaryKey() ? "_key" : ToSnakeCase(prop.Name);

            if (value is List<string> list)
                doc[fieldName] = list;
            else
                doc[fieldName] = value;
        }

        await client.InsertAsync(collection, doc, ct);
    }

    private static async Task HandleUpdate(
        Client.Tcp.IOxiDbClient client,
        string collection,
        IUpdateEntry entry,
        CancellationToken ct)
    {
        // Build filter from primary key
        var pkFilter = BuildPkFilter(entry);

        // Build $set with modified properties
        var setFields = new Dictionary<string, object?>();
        foreach (var prop in entry.EntityType.GetProperties())
        {
            if (prop.IsPrimaryKey()) continue;
            if (!entry.IsModified(prop)) continue;

            var fieldName = ToSnakeCase(prop.Name);
            var value = entry.GetCurrentValue(prop);
            if (value is List<string> list)
                setFields[fieldName] = list;
            else
                setFields[fieldName] = value;
        }

        if (setFields.Count > 0)
        {
            var update = new Dictionary<string, object?> { ["$set"] = setFields };
            await client.UpdateAsync(collection, pkFilter, update, ct);
        }
    }

    private static async Task HandleDelete(
        Client.Tcp.IOxiDbClient client,
        string collection,
        IUpdateEntry entry,
        CancellationToken ct)
    {
        var pkFilter = BuildPkFilter(entry);
        await client.DeleteAsync(collection, pkFilter, ct);
    }

    private static Dictionary<string, object?> BuildPkFilter(IUpdateEntry entry)
    {
        var filter = new Dictionary<string, object?>();
        foreach (var keyProp in entry.EntityType.FindPrimaryKey()!.Properties)
        {
            filter["_key"] = entry.GetCurrentValue(keyProp);
        }
        return filter;
    }

    private static string ToSnakeCase(string name)
    {
        var result = new System.Text.StringBuilder();
        for (int i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0) result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }
}
