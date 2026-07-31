using System.Data;
using System.Data.Common;
using System.Text.Json;
using OxiDb.Client.Tcp;

namespace OxiDb.Data;

/// <summary>
/// ADO.NET connection to the OxiDB SQL engine (ADR-0013 Phase C).
///
/// Connection string keys: <c>Host</c> (default 127.0.0.1), <c>Port</c>
/// (default 4444), <c>Database</c> (default the server's default database),
/// <c>Pooling</c> (default true), <c>OxiWire</c> (default true — binary wire
/// format; JSON when false). One open <see cref="OxiDbConnection"/> maps
/// to one wire connection, so the server-side session (current database,
/// interactive SQL transaction) behaves exactly like any other OxiDB client
/// connection. Close returns the wire connection to a process-wide pool
/// (EF Core opens/closes around every query — without pooling each query
/// would pay a TCP connect plus a use_db round trip).
///
/// <para><c>Path=&lt;data dir&gt;</c> (alias <c>DataDir</c>) selects
/// <b>embedded</b> mode instead: no server, the engine runs in-process via
/// <c>OxiDb.Client.Embedded</c> (which the application must reference), SQL
/// data under <c>&lt;dir&gt;/sql</c>. One engine per directory, shared
/// process-wide; each connection carries its own interactive transaction as
/// a request token, so transactions stay per-connection exactly as they are
/// over TCP. <c>Host/Port/Database/Pooling</c> are meaningless there.</para>
/// </summary>
public sealed class OxiDbConnection : DbConnection
{
    private string _connectionString = "";
    private string _host = "127.0.0.1";
    private int _port = 4444;
    private string _database = "";
    private string _path = "";
    private bool _pooling = true;
    private bool _oxiwire = true;
    private string? _poolKey;
    private OxiDbTcpClient? _client;
    private OxiDbEmbeddedEngine? _embedded;
    // The parked interactive SQL transaction (embedded mode): a token this
    // connection carries between requests, mirrored from every response.
    private ulong? _sqlTx;
    private ConnectionState _state = ConnectionState.Closed;
    internal OxiDbTransaction? ActiveTransaction;

    // The wire format is sticky per socket, so it's part of the pool key.
    // Cached: EF cycles Open/Close around every query on one connection.
    private string PoolKey => _poolKey ??= $"{_host}:{_port}/{_database}#{(_oxiwire ? "w" : "j")}";

    public OxiDbConnection() { }

    public OxiDbConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            _connectionString = value ?? "";
            _poolKey = null;
            foreach (var part in _connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var idx = part.IndexOf('=');
                if (idx <= 0) continue;
                var key = part[..idx].Trim().ToLowerInvariant();
                var val = part[(idx + 1)..].Trim();
                switch (key)
                {
                    case "host" or "server" or "data source": _host = val; break;
                    case "port": _port = int.Parse(val); break;
                    case "database" or "initial catalog": _database = val; break;
                    case "path" or "datadir": _path = val; break;
                    case "pooling": _pooling = !val.Equals("false", StringComparison.OrdinalIgnoreCase); break;
                    case "oxiwire": _oxiwire = !val.Equals("false", StringComparison.OrdinalIgnoreCase); break;
                }
            }
        }
    }

    public override string Database => _database;
    public override string DataSource => Embedded is not null ? _path : $"{_host}:{_port}";
    public override string ServerVersion => "oxidb";
    public override ConnectionState State => _state;

    internal OxiDbTcpClient Client =>
        _client ?? throw new InvalidOperationException("connection is not open");

    private OxiDbEmbeddedEngine? Embedded => _embedded;

    public override void Open() => OpenAsync(default).GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken ct)
    {
        if (_state == ConnectionState.Open) return;
        // Exact OperationCanceledException for an already-canceled token (an
        // awaited connect would surface TaskCanceledException instead).
        ct.ThrowIfCancellationRequested();
        if (_path.Length != 0)
        {
            // Embedded: the process-wide engine for this directory. Nothing
            // to pool — the engine is already shared and stays open.
            _embedded = OxiDbEmbeddedEngine.Get(_path);
            _state = ConnectionState.Open;
            return;
        }
        _client = _pooling ? OxiDbClientPool.TryRent(PoolKey) : null;
        if (_client is null)
        {
            _client = await OxiDbTcpClient.ConnectAsync(_host, _port, ct: ct).ConfigureAwait(false);
            // Binary requests; the server replies in kind per request, and the
            // client falls back to parsing JSON responses, so an older server
            // that ignores the magic byte still works. `OxiWire=false` opts out.
            if (_oxiwire) _client.UseOxiWire();
            if (!string.IsNullOrEmpty(_database))
            {
                // Session default: every subsequent request targets this
                // database. Pooled clients already carry it (it's the key).
                await _client.ExecRawAsync(
                    new() { ["cmd"] = "use_db", ["name"] = _database },
                    ct
                ).ConfigureAwait(false);
            }
        }
        _state = ConnectionState.Open;
    }

    public override void Close()
    {
        if (_embedded is not null)
        {
            // A transaction leaked past its connection would stay parked in
            // the engine for the life of the process (over TCP the server
            // rolls it back on disconnect); do the equivalent here.
            if (_sqlTx is not null)
            {
                try { EmbeddedSqlRaw("ROLLBACK", null); } catch { /* engine may be torn down */ }
            }
            _embedded = null;
            _sqlTx = null;
            ActiveTransaction = null;
            _state = ConnectionState.Closed;
            return;
        }
        if (_client is not null)
        {
            // Only a session-clean connection may be pooled: no interactive
            // transaction (the server rolls one back on disconnect — a pooled
            // socket would leak it into the next renter instead). Flag-only
            // check here; the rent side probes liveness for aged entries.
            if (_pooling && ActiveTransaction is null && _client.IsUsable)
                OxiDbClientPool.Return(PoolKey, _client);
            else
                _client.Dispose();
        }
        ActiveTransaction = null;
        _client = null;
        _state = ConnectionState.Closed;
    }

    public override void ChangeDatabase(string databaseName)
    {
        if (_embedded is not null)
            throw new NotSupportedException(
                "an embedded (Path=) connection serves exactly one data directory");
        Client.ExecRawAsync(new() { ["cmd"] = "use_db", ["name"] = databaseName })
            .GetAwaiter().GetResult();
        _database = databaseName;
        _poolKey = null;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        if (ActiveTransaction is not null)
            throw new InvalidOperationException("a transaction is already active on this connection");
        SqlAsync("BEGIN", null, default).GetAwaiter().GetResult();
        ActiveTransaction = new OxiDbTransaction(this);
        return ActiveTransaction;
    }

    protected override DbCommand CreateDbCommand() => new OxiDbCommand { Connection = this };

    /// <summary>Run SQL on this connection's session and return the per-statement results array.</summary>
    internal Task<JsonElement> SqlAsync(string sql, object?[]? parameters, CancellationToken ct)
    {
        if (_embedded is null) return Client.SqlAsync(sql, parameters, ct);
        // Embedded: same envelope, decoded here; errors throw like the TCP
        // client's SqlAsync does (this is the BEGIN/COMMIT/ROLLBACK path).
        var raw = EmbeddedSqlRaw(sql, parameters);
        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;
        if (root.TryGetProperty("ok", out var ok) && !ok.GetBoolean())
        {
            var msg = root.TryGetProperty("error", out var e)
                ? e.GetString() ?? "unknown error"
                : "unknown error";
            throw OxiDbException.FromServerMessage(msg);
        }
        return Task.FromResult(
            root.TryGetProperty("data", out var d) ? d.Clone() : default);
    }

    /// <summary>Run SQL and return the raw response frame (hot read path).</summary>
    internal Task<byte[]> SqlRawAsync(string sql, object?[]? parameters, CancellationToken ct) =>
        _embedded is null
            ? Client.SqlRawBytesAsync(sql, parameters, ct)
            : Task.FromResult(EmbeddedSqlRaw(sql, parameters));

    /// <summary>Synchronous twin of <see cref="SqlRawAsync"/> (blocking I/O).</summary>
    internal byte[] SqlRaw(string sql, object?[]? parameters) =>
        _embedded is null
            ? Client.SqlRawBytes(sql, parameters)
            : EmbeddedSqlRaw(sql, parameters);

    /// <summary>
    /// Embedded SQL: attach this connection's parked-transaction token to the
    /// request, and mirror it back from the response — including error
    /// responses, since a failed statement inside a transaction does not end
    /// the transaction. The envelope is only re-parsed when a token could
    /// possibly be present (one is live, or the text can start one); the hot
    /// no-transaction read path hands the bytes straight through.
    /// </summary>
    private byte[] EmbeddedSqlRaw(string sql, object?[]? parameters)
    {
        var engine = _embedded
            ?? throw new InvalidOperationException("connection is not open");
        var payload = new Dictionary<string, object?>
        {
            ["engine"] = "sql",
            ["cmd"] = "sql",
            ["sql"] = sql,
        };
        if (parameters is not null) payload["params"] = parameters;
        var tracking = _sqlTx is not null
            || sql.Contains("BEGIN", StringComparison.OrdinalIgnoreCase);
        if (_sqlTx is { } tx) payload["sql_tx"] = tx;
        var raw = engine.ExecuteRaw(JsonSerializer.Serialize(payload));
        if (tracking)
        {
            using var doc = JsonDocument.Parse(raw);
            _sqlTx = doc.RootElement.TryGetProperty("sql_tx", out var t)
                ? t.GetUInt64()
                : null;
        }
        return raw;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Close();
        base.Dispose(disposing);
    }
}

/// <summary>Provider factory for dependency-injected / config-driven stacks.</summary>
public sealed class OxiDbFactory : DbProviderFactory
{
    public static readonly OxiDbFactory Instance = new();
    public override DbConnection CreateConnection() => new OxiDbConnection();
    public override DbCommand CreateCommand() => new OxiDbCommand();
    public override DbParameter CreateParameter() => new OxiDbParameter();
}
