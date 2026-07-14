using System.Collections.Concurrent;
using OxiDb.Client.Tcp;

namespace OxiDb.Data;

/// <summary>
/// Process-wide pool of wire connections, keyed by <c>host:port/database</c>
/// (the session's <c>use_db</c> is part of the key, so a pooled client's
/// session state always matches its key; databases are resolved by name per
/// request server-side, so drop/re-create of a database does not stale the
/// session). Only transaction-free connections are returned; entries are
/// discarded when dead (<see cref="OxiDbTcpClient.IsAlive"/>) or idle past
/// <see cref="MaxIdle"/> — kept below the server's 30s default idle timeout
/// so the pool rarely hands out a connection the server is about to kill.
/// </summary>
internal static class OxiDbClientPool
{
    private sealed record Entry(OxiDbTcpClient Client, long ReturnedAtTicks);

    private static readonly ConcurrentDictionary<string, ConcurrentQueue<Entry>> Pools = new();
    private const int MaxPerKey = 32;
    private static readonly TimeSpan MaxIdle = TimeSpan.FromSeconds(10);

    /// <summary>A live pooled client for `key`, or null (caller connects).</summary>
    public static OxiDbTcpClient? TryRent(string key)
    {
        if (!Pools.TryGetValue(key, out var queue))
            return null;
        while (queue.TryDequeue(out var entry))
        {
            var idle = TimeSpan.FromTicks(DateTime.UtcNow.Ticks - entry.ReturnedAtTicks);
            if (idle <= MaxIdle && entry.Client.IsAlive)
                return entry.Client;
            entry.Client.Dispose();
        }
        return null;
    }

    /// <summary>Return a session-clean client to the pool (or dispose it when full).</summary>
    public static void Return(string key, OxiDbTcpClient client)
    {
        var queue = Pools.GetOrAdd(key, _ => new ConcurrentQueue<Entry>());
        if (queue.Count >= MaxPerKey)
        {
            client.Dispose();
            return;
        }
        queue.Enqueue(new Entry(client, DateTime.UtcNow.Ticks));
    }
}
