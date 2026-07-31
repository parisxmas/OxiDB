using System.Collections;
using System.Data.Common;
using System.Text.Json;
using OxiDb.Client.Tcp;

namespace OxiDb.Data;

/// <summary>
/// Forward-only reader over the SQL engine's per-statement results. Each
/// SELECT-shaped result is one result set (<see cref="NextResult"/> walks
/// them); non-query results contribute to <see cref="RecordsAffected"/>.
/// Cells are plain CLR values decoded straight off the wire (OxiWire binary
/// or JSON — no JsonDocument round trip on the binary path). Column CLR
/// types come from the wire's <c>types</c> metadata: INT→long,
/// DOUBLE→double, TEXT→string, BOOL→bool, TIMESTAMP→DateTime.
/// </summary>
public sealed class OxiDbDataReader : DbDataReader
{
    private sealed class ResultSet
    {
        public string[] Columns = [];
        public Type[] Types = [];
        public object?[][] Rows = [];
    }

    private readonly List<ResultSet> _sets = new();
    private readonly int _recordsAffected;
    private int _set;
    private int _row = -1;

    /// <summary>
    /// Parse a raw response frame (OxiWire when it carries the magic byte,
    /// JSON otherwise), throwing the server's error when the envelope is not
    /// ok.
    /// </summary>
    internal static OxiDbDataReader Parse(byte[] raw)
    {
        if (OxiWire.IsOxiWire(raw))
        {
            var (ok, data) = OxiWire.DecodeResponseClr(raw);
            if (!ok)
                throw OxiDbException.FromServerMessage(data as string ?? "unknown error");
            return new OxiDbDataReader(data);
        }
        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;
        if (root.TryGetProperty("ok", out var okProp) && !okProp.GetBoolean())
        {
            var msg = root.TryGetProperty("error", out var e)
                ? e.GetString() ?? "unknown error"
                : "unknown error";
            throw OxiDbException.FromServerMessage(msg);
        }
        return new OxiDbDataReader(
            root.TryGetProperty("data", out var d) ? JsonToClr(d) : null);
    }

    /// <summary>From decoded per-statement results (array of maps).</summary>
    private OxiDbDataReader(object? results)
    {
        var affected = 0;
        if (results is object?[] stmts)
        {
            foreach (var stmt in stmts)
            {
                if (stmt is not Dictionary<string, object?> map) continue;
                if (map.TryGetValue("columns", out var cols))
                {
                    var columns = ((object?[])cols!).Select(c => (string?)c ?? "").ToArray();
                    var types = map.TryGetValue("types", out var t) && t is object?[] tn
                        ? tn.Select(MapType).ToArray()
                        : Enumerable.Repeat(typeof(object), columns.Length).ToArray();
                    var rows = map.TryGetValue("rows", out var r) && r is object?[] rr
                        ? rr.Select(row => (object?[])row!).ToArray()
                        : [];
                    _sets.Add(new ResultSet { Columns = columns, Types = types, Rows = rows });
                }
                else if (map.TryGetValue("affected", out var a) && a is long n)
                {
                    affected += (int)n;
                }
            }
        }
        _recordsAffected = affected;
    }

    /// <summary>JsonElement → the same CLR value shapes the binary decoder produces.</summary>
    private static object? JsonToClr(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.String => e.GetString(),
        JsonValueKind.Number => e.TryGetInt64(out var n) ? n : e.GetDouble(),
        JsonValueKind.Array => e.EnumerateArray().Select(JsonToClr).ToArray(),
        JsonValueKind.Object => e.EnumerateObject()
            .ToDictionary(p => p.Name, p => JsonToClr(p.Value)),
        _ => null,
    };

    private static Type MapType(object? t) => (t as string) switch
    {
        "INT" => typeof(long),
        "DOUBLE" => typeof(double),
        "TEXT" => typeof(string),
        "BOOL" => typeof(bool),
        "TIMESTAMP" => typeof(DateTime),
        _ => typeof(object),
    };

    private ResultSet Cur => _sets[_set];

    public override bool Read()
    {
        if (_set >= _sets.Count) return false;
        _row++;
        return _row < Cur.Rows.Length;
    }

    public override Task<bool> ReadAsync(CancellationToken ct) => Task.FromResult(Read());

    public override bool NextResult()
    {
        if (_set + 1 >= _sets.Count)
        {
            _set = _sets.Count;
            return false;
        }
        _set++;
        _row = -1;
        return true;
    }

    public override Task<bool> NextResultAsync(CancellationToken ct) =>
        Task.FromResult(NextResult());

    public override int FieldCount => _set < _sets.Count ? Cur.Columns.Length : 0;
    public override bool HasRows => _set < _sets.Count && Cur.Rows.Length > 0;
    public override bool IsClosed => false;
    public override int RecordsAffected => _recordsAffected;
    public override int Depth => 0;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override string GetName(int ordinal) => Cur.Columns[ordinal];

    public override int GetOrdinal(string name)
    {
        var cols = Cur.Columns;
        for (var i = 0; i < cols.Length; i++)
            if (string.Equals(cols[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new IndexOutOfRangeException($"no column named {name}");
    }

    public override Type GetFieldType(int ordinal) => Cur.Types[ordinal];
    public override string GetDataTypeName(int ordinal) => Cur.Types[ordinal].Name;

    private object? Cell(int ordinal) => Cur.Rows[_row][ordinal];

    public override bool IsDBNull(int ordinal) => Cell(ordinal) is null;

    public override object GetValue(int ordinal)
    {
        var cell = Cell(ordinal);
        if (cell is null) return DBNull.Value;
        if (Cur.Types[ordinal] == typeof(DateTime) && cell is long ms)
            return DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
        // Nested values (arrays/maps) surface as their JSON text.
        if (cell is object?[] or Dictionary<string, object?>)
            return JsonSerializer.Serialize(cell);
        return cell;
    }

    public override int GetValues(object[] values)
    {
        var n = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < n; i++) values[i] = GetValue(i);
        return n;
    }

    public override bool GetBoolean(int ordinal) => (bool)Cell(ordinal)!;
    public override byte GetByte(int ordinal) => (byte)GetInt64(ordinal);
    public override char GetChar(int ordinal) => GetString(ordinal)[0];
    public override short GetInt16(int ordinal) => (short)GetInt64(ordinal);
    public override int GetInt32(int ordinal) => (int)GetInt64(ordinal);

    public override long GetInt64(int ordinal) => Cell(ordinal) switch
    {
        long n => n,
        double d => (long)d,
        string s => long.Parse(s),
        var other => throw new InvalidCastException($"cannot read {other?.GetType().Name} as long"),
    };

    public override float GetFloat(int ordinal) => (float)GetDouble(ordinal);

    public override double GetDouble(int ordinal) => Cell(ordinal) switch
    {
        double d => d,
        long n => n,
        string s => double.Parse(s),
        var other => throw new InvalidCastException($"cannot read {other?.GetType().Name} as double"),
    };

    public override decimal GetDecimal(int ordinal) => Cell(ordinal) switch
    {
        double d => (decimal)d,
        long n => n,
        string s => decimal.Parse(s),
        var other => throw new InvalidCastException($"cannot read {other?.GetType().Name} as decimal"),
    };

    public override string GetString(int ordinal) => (string)Cell(ordinal)!;
    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));

    public override DateTime GetDateTime(int ordinal) =>
        DateTimeOffset.FromUnixTimeMilliseconds(GetInt64(ordinal)).UtcDateTime;

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
        throw new NotSupportedException("binary columns are not supported yet (ADR-0013 Phase D)");

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var s = GetString(ordinal);
        if (buffer is null) return s.Length;
        var n = Math.Min(length, s.Length - (int)dataOffset);
        s.CopyTo((int)dataOffset, buffer, bufferOffset, n);
        return n;
    }

    public override IEnumerator GetEnumerator() => new DbEnumerator(this);
}
