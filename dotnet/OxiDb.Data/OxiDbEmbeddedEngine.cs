using System.Collections.Concurrent;
using System.Reflection;

namespace OxiDb.Data;

/// <summary>
/// The in-process transport behind <c>Path=</c> connection strings: an
/// embedded engine handle per data directory, shared process-wide.
///
/// One directory can only be served by one engine instance in a process
/// (two would race the WAL), while ADO.NET churns connections around every
/// query — so unlike the TCP pool, entries here are opened once and never
/// closed; the engine lives as long as the process, exactly as it would
/// inside the server. Per-connection state (the interactive SQL
/// transaction) is NOT here: it rides each request as a token
/// (<c>sql_tx</c>), so any number of connections share a handle without
/// sharing transactions.
///
/// Reached by reflection so <c>OxiDb.Data</c> keeps no reference to
/// <c>OxiDb.Client.Embedded</c> — a TCP-only application should not carry
/// native libraries for five platforms. The application that writes
/// <c>Path=</c> references the embedded package; anyone else never loads it.
/// </summary>
internal sealed class OxiDbEmbeddedEngine
{
    private static readonly ConcurrentDictionary<string, Lazy<OxiDbEmbeddedEngine>> Engines = new();

    private readonly Func<string, byte[]> _executeRaw;

    private OxiDbEmbeddedEngine(Func<string, byte[]> executeRaw) => _executeRaw = executeRaw;

    /// <summary>The engine for <paramref name="path"/>, opened on first use.</summary>
    public static OxiDbEmbeddedEngine Get(string path)
    {
        var key = System.IO.Path.GetFullPath(path);
        // Lazy so a race on first open still opens the directory exactly once.
        return Engines.GetOrAdd(key, p => new Lazy<OxiDbEmbeddedEngine>(() => Open(p))).Value;
    }

    private static OxiDbEmbeddedEngine Open(string path)
    {
        var type = Type.GetType("OxiDb.Client.Embedded.OxiDbEmbeddedClient, OxiDb.Client.Embedded")
            ?? throw new InvalidOperationException(
                "an embedded (Path=) connection string requires the application to reference " +
                "the OxiDb.Client.Embedded package, which carries the native engine");
        var open = type.GetMethod("Open", BindingFlags.Public | BindingFlags.Static, [typeof(string)])
            ?? throw new MissingMethodException(type.FullName, "Open");
        object client;
        try
        {
            client = open.Invoke(null, [path])!;
        }
        catch (TargetInvocationException e) when (e.InnerException is not null)
        {
            throw e.InnerException; // the real "failed to open" message
        }
        var raw = type.GetMethod("ExecuteRawBytes", BindingFlags.Public | BindingFlags.Instance, [typeof(string)])
            ?? throw new InvalidOperationException(
                "this OxiDb.Client.Embedded version has no ExecuteRawBytes — " +
                "embedded ADO.NET needs 0.41.35 or later");
        return new OxiDbEmbeddedEngine(raw.CreateDelegate<Func<string, byte[]>>(client));
    }

    /// <summary>Run a raw request; returns the raw response envelope bytes.</summary>
    public byte[] ExecuteRaw(string requestJson) => _executeRaw(requestJson);
}
