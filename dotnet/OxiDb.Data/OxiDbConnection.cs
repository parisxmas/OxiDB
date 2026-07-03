using System.Data;
using System.Data.Common;
using System.Text.Json;
using OxiDb.Client.Tcp;

namespace OxiDb.Data;

/// <summary>
/// ADO.NET connection to the OxiDB SQL engine (ADR-0013 Phase C).
///
/// Connection string keys: <c>Host</c> (default 127.0.0.1), <c>Port</c>
/// (default 4444), <c>Database</c> (default the server's default database).
/// One <see cref="OxiDbConnection"/> maps to one wire connection, so the
/// server-side session (current database, interactive SQL transaction)
/// behaves exactly like any other OxiDB client connection.
/// </summary>
public sealed class OxiDbConnection : DbConnection
{
    private string _connectionString = "";
    private string _host = "127.0.0.1";
    private int _port = 4444;
    private string _database = "";
    private OxiDbTcpClient? _client;
    private ConnectionState _state = ConnectionState.Closed;
    internal OxiDbTransaction? ActiveTransaction;

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
                }
            }
        }
    }

    public override string Database => _database;
    public override string DataSource => $"{_host}:{_port}";
    public override string ServerVersion => "oxidb";
    public override ConnectionState State => _state;

    internal OxiDbTcpClient Client =>
        _client ?? throw new InvalidOperationException("connection is not open");

    public override void Open() => OpenAsync(default).GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken ct)
    {
        if (_state == ConnectionState.Open) return;
        _client = await OxiDbTcpClient.ConnectAsync(_host, _port, ct: ct).ConfigureAwait(false);
        if (!string.IsNullOrEmpty(_database))
        {
            // Session default: every subsequent request targets this database.
            await _client.ExecRawAsync(
                new() { ["cmd"] = "use_db", ["name"] = _database },
                ct
            ).ConfigureAwait(false);
        }
        _state = ConnectionState.Open;
    }

    public override void Close()
    {
        ActiveTransaction = null;
        _client?.Dispose(); // server rolls back any open transaction
        _client = null;
        _state = ConnectionState.Closed;
    }

    public override void ChangeDatabase(string databaseName)
    {
        Client.ExecRawAsync(new() { ["cmd"] = "use_db", ["name"] = databaseName })
            .GetAwaiter().GetResult();
        _database = databaseName;
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
    internal Task<JsonElement> SqlAsync(string sql, object?[]? parameters, CancellationToken ct) =>
        Client.SqlAsync(sql, parameters, ct);

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
