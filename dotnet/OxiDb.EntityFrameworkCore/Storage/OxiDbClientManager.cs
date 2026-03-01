using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using OxiDb.Client.Embedded;
using OxiDb.Client.Tcp;
using OxiDb.EntityFrameworkCore.Infrastructure;

namespace OxiDb.EntityFrameworkCore.Storage;

/// <summary>
/// Manages a singleton client per service provider scope.
/// Supports both TCP and embedded modes.
/// </summary>
public sealed class OxiDbClientManager : IDisposable
{
    private IOxiDbClient? _client;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly OxiDbOptionsExtension _ext;

    public OxiDbClientManager(IDbContextOptions options)
    {
        _ext = options.Extensions.OfType<OxiDbOptionsExtension>().First();
    }

    public async Task<IOxiDbClient> GetClientAsync(CancellationToken ct = default)
    {
        if (_client != null)
            return _client;

        await _lock.WaitAsync(ct);
        try
        {
            if (_client == null)
            {
                if (_ext.IsEmbedded)
                {
                    _client = _ext.EncryptionKeyPath != null
                        ? OxiDbEmbeddedClient.OpenEncrypted(_ext.DataPath!, _ext.EncryptionKeyPath)
                        : OxiDbEmbeddedClient.Open(_ext.DataPath!);
                }
                else
                {
                    if (_ext.HasCredentials)
                        _client = await OxiDbTcpClient.ConnectAsync(
                            _ext.Host, _ext.Port, _ext.Username!, _ext.Password!, ct: ct);
                    else
                        _client = await OxiDbTcpClient.ConnectAsync(_ext.Host, _ext.Port, ct: ct);
                }
            }
            return _client;
        }
        finally
        {
            _lock.Release();
        }
    }

    public void Dispose()
    {
        _client?.Dispose();
        _lock.Dispose();
    }
}
