using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.Json;

namespace OxiDb.Data;

/// <summary>
/// ADO.NET command over the OxiDB SQL engine. Named parameters
/// (<c>@name</c>) are rewritten to positional <c>$N</c> placeholders in
/// first-appearance order; plain <c>?</c>/<c>$N</c> text passes through with
/// parameters bound in collection order.
/// </summary>
public sealed class OxiDbCommand : DbCommand
{
    private readonly OxiDbParameterCollection _parameters = new();

    [AllowNull]
    public override string CommandText { get; set; } = "";
    public override int CommandTimeout { get; set; } = 30;
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbConnection? DbConnection { get; set; }
    protected override DbParameterCollection DbParameterCollection => _parameters;
    protected override DbTransaction? DbTransaction { get; set; }

    private OxiDbConnection Conn =>
        DbConnection as OxiDbConnection
        ?? throw new InvalidOperationException("command has no open OxiDbConnection");

    public override void Cancel() { }
    public override void Prepare() { }
    protected override DbParameter CreateDbParameter() => new OxiDbParameter();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
        ExecuteDbDataReaderAsync(behavior, default).GetAwaiter().GetResult();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var (sql, args) = BindParameters();
        var results = await Conn.SqlAsync(sql, args, ct).ConfigureAwait(false);
        return new OxiDbDataReader(results);
    }

    public override int ExecuteNonQuery() =>
        ExecuteNonQueryAsync(default).GetAwaiter().GetResult();

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken ct)
    {
        var (sql, args) = BindParameters();
        var results = await Conn.SqlAsync(sql, args, ct).ConfigureAwait(false);
        var affected = 0;
        foreach (var r in results.EnumerateArray())
        {
            if (r.TryGetProperty("affected", out var a))
                affected += a.GetInt32();
        }
        return affected;
    }

    public override object? ExecuteScalar() =>
        ExecuteScalarAsync(default).GetAwaiter().GetResult();

    public override async Task<object?> ExecuteScalarAsync(CancellationToken ct)
    {
        using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, ct)
            .ConfigureAwait(false);
        return await reader.ReadAsync(ct).ConfigureAwait(false) && reader.FieldCount > 0
            ? reader.IsDBNull(0) ? DBNull.Value : reader.GetValue(0)
            : null;
    }

    /// <summary>
    /// Rewrite <c>@name</c> placeholders (outside string literals) to
    /// <c>$N</c> and produce the positional argument array.
    /// </summary>
    internal (string Sql, object?[]? Args) BindParameters()
    {
        var text = CommandText;
        if (!text.Contains('@'))
        {
            if (_parameters.Count == 0) return (text, null);
            var positional = new object?[_parameters.Count];
            for (var i = 0; i < _parameters.Count; i++)
                positional[i] = ToWire(((OxiDbParameter)_parameters[i]).Value);
            return (text, positional);
        }

        var sb = new StringBuilder(text.Length + 8);
        var order = new List<string>();
        var slotByName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var inString = false;

        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '\'')
            {
                inString = !inString;
                sb.Append(c);
                continue;
            }
            if (inString || c != '@')
            {
                sb.Append(c);
                continue;
            }
            // @identifier
            var j = i + 1;
            while (j < text.Length && (char.IsLetterOrDigit(text[j]) || text[j] == '_')) j++;
            if (j == i + 1)
            {
                sb.Append(c); // lone '@'
                continue;
            }
            var name = text[(i + 1)..j];
            if (!slotByName.TryGetValue(name, out var slot))
            {
                slot = order.Count + 1;
                slotByName[name] = slot;
                order.Add(name);
            }
            sb.Append('$').Append(slot);
            i = j - 1;
        }

        // One pass over the collection (a per-placeholder Find would be an
        // allocating O(n) scan — batched commands carry hundreds of params).
        var byName = new Dictionary<string, OxiDbParameter>(
            _parameters.Count, StringComparer.OrdinalIgnoreCase);
        for (var k = 0; k < _parameters.Count; k++)
        {
            var p = (OxiDbParameter)_parameters[k];
            byName[p.BareName] = p;
        }

        var args = new object?[order.Count];
        for (var k = 0; k < order.Count; k++)
        {
            if (!byName.TryGetValue(order[k], out var p))
                throw new InvalidOperationException($"missing parameter @{order[k]}");
            args[k] = ToWire(p.Value);
        }
        return (sb.ToString(), args);
    }

    /// <summary>Map CLR values onto the wire's JSON scalars.</summary>
    private static object? ToWire(object? v) => v switch
    {
        null or DBNull => null,
        // Unspecified kinds are taken as UTC (the store is epoch ms); only a
        // Local kind is actually converted.
        DateTime dt => new DateTimeOffset(
            dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : DateTime.SpecifyKind(dt, DateTimeKind.Utc)
        ).ToUnixTimeMilliseconds(),
        DateTimeOffset dto => dto.ToUnixTimeMilliseconds(),
        Guid g => g.ToString(),
        char c => c.ToString(),
        decimal m => (double)m,
        float f => (double)f,
        byte or sbyte or short or ushort or int or uint => Convert.ToInt64(v),
        Enum e => Convert.ToInt64(e),
        _ => v,
    };
}
