using OxiDb.Client.Tcp;

namespace ColdChain;

/// <summary>
/// A self-healing OxiDB connection.
///
/// OxiDbTcpClient is a plain TCP client: it connects once and stays connected.
/// That is fine until the engine restarts — a deploy, a checkpoint, a crash —
/// and every holder of a socket is left with a broken pipe it will never
/// recover from on its own. A long-lived service cannot assume its database
/// outlives it, so rebuild the connection on demand rather than at startup.
/// </summary>
public sealed class OxiConnection : IAsyncDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private OxiDbTcpClient? _client;

    public async Task<OxiDbTcpClient> GetAsync()
    {
        if (_client is { } live) return live;
        await _gate.WaitAsync();
        try { return _client ??= await OxiDbTcpClient.ConnectAsync(Endpoints.Host, Endpoints.Tcp); }
        finally { _gate.Release(); }
    }

    /// <summary>Discard the current socket; the next <see cref="GetAsync"/> dials a new one.</summary>
    public void Drop()
    {
        var dead = Interlocked.Exchange(ref _client, null);
        if (dead is not null) _ = dead.DisposeAsync().AsTask();
    }

    /// <summary>
    /// Run a <b>read</b> and, if the connection was stale, transparently retry it once
    /// on a fresh one. Only safe for reads: a write that failed after the engine saw it
    /// would be applied twice, so writers call <see cref="GetAsync"/>/<see cref="Drop"/>
    /// and let the caller decide.
    /// </summary>
    public async Task<T> ReadAsync<T>(Func<OxiDbTcpClient, Task<T>> read)
    {
        try { return await read(await GetAsync()); }
        catch (Exception ex) when (IsBrokenConnection(ex))
        {
            Drop();
            return await read(await GetAsync());
        }
    }

    private static bool IsBrokenConnection(Exception ex) =>
        ex is IOException or ObjectDisposedException ||
        ex.InnerException is System.Net.Sockets.SocketException;

    public ValueTask DisposeAsync()
    {
        var c = Interlocked.Exchange(ref _client, null);
        return c?.DisposeAsync() ?? ValueTask.CompletedTask;
    }
}
