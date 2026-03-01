using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace OxiDb.EntityFrameworkCore.Storage;

/// <summary>
/// IDatabaseCreator that drops/creates OxiDB collections per DbSet entity type.
/// </summary>
public sealed class OxiDbDatabaseCreator : IDatabaseCreator
{
    private readonly OxiDbClientManager _clientManager;
    private readonly IModel _model;

    public OxiDbDatabaseCreator(OxiDbClientManager clientManager, ICurrentDbContext currentContext)
    {
        _clientManager = clientManager;
        _model = currentContext.Context.Model;
    }

    public bool EnsureCreated()
    {
        return EnsureCreatedAsync().GetAwaiter().GetResult();
    }

    public async Task<bool> EnsureCreatedAsync(CancellationToken ct = default)
    {
        var client = await _clientManager.GetClientAsync(ct);

        foreach (var entityType in _model.GetEntityTypes())
        {
            var collectionName = entityType.ShortName().ToLowerInvariant();
            try
            {
                await client.DropCollectionAsync(collectionName, ct);
            }
            catch
            {
                // Collection may not exist
            }
            await client.CreateCollectionAsync(collectionName, ct);
        }

        return true;
    }

    public bool EnsureDeleted()
    {
        return EnsureDeletedAsync().GetAwaiter().GetResult();
    }

    public async Task<bool> EnsureDeletedAsync(CancellationToken ct = default)
    {
        var client = await _clientManager.GetClientAsync(ct);

        foreach (var entityType in _model.GetEntityTypes())
        {
            var collectionName = entityType.ShortName().ToLowerInvariant();
            try
            {
                await client.DropCollectionAsync(collectionName, ct);
            }
            catch
            {
                // Ignore if not exists
            }
        }

        return true;
    }

    public bool CanConnect() => CanConnectAsync().GetAwaiter().GetResult();

    public async Task<bool> CanConnectAsync(CancellationToken ct = default)
    {
        try
        {
            var client = await _clientManager.GetClientAsync(ct);
            await client.PingAsync(ct);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
