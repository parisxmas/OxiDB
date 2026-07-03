using System.Collections;
using System.Data.Common;
using System.Text.Json;

namespace OxiDb.Data;

/// <summary>
/// Forward-only reader over the SQL engine's per-statement results. Each
/// SELECT-shaped result is one result set (<see cref="NextResult"/> walks
/// them); non-query results contribute to <see cref="RecordsAffected"/>.
/// Column CLR types come from the wire's <c>types</c> metadata:
/// INT→long, DOUBLE→double, TEXT→string, BOOL→bool, TIMESTAMP→DateTime.
/// </summary>
public sealed class OxiDbDataReader : DbDataReader
{
    private readonly List<JsonElement> _resultSets = new();
    private readonly int _recordsAffected;
    private int _set;
    private int _row = -1;

    private string[] _columns = Array.Empty<string>();
    private Type[] _types = Array.Empty<Type>();
    private JsonElement _rows;

    internal OxiDbDataReader(JsonElement results)
    {
        var affected = 0;
        foreach (var r in results.EnumerateArray())
        {
            if (r.TryGetProperty("columns", out _))
                _resultSets.Add(r.Clone());
            else if (r.TryGetProperty("affected", out var a))
                affected += a.GetInt32();
        }
        _recordsAffected = affected;
        LoadSet();
    }

    private void LoadSet()
    {
        if (_set >= _resultSets.Count)
        {
            _columns = Array.Empty<string>();
            _types = Array.Empty<Type>();
            _rows = default;
            return;
        }
        var rs = _resultSets[_set];
        _columns = rs.GetProperty("columns").EnumerateArray()
            .Select(c => c.GetString() ?? "")
            .ToArray();
        _types = rs.TryGetProperty("types", out var types)
            ? types.EnumerateArray().Select(MapType).ToArray()
            : Enumerable.Repeat(typeof(object), _columns.Length).ToArray();
        _rows = rs.GetProperty("rows");
        _row = -1;
    }

    private static Type MapType(JsonElement t) => t.ValueKind == JsonValueKind.String
        ? t.GetString() switch
        {
            "INT" => typeof(long),
            "DOUBLE" => typeof(double),
            "TEXT" => typeof(string),
            "BOOL" => typeof(bool),
            "TIMESTAMP" => typeof(DateTime),
            _ => typeof(object),
        }
        : typeof(object);

    public override bool Read()
    {
        if (_set >= _resultSets.Count) return false;
        _row++;
        return _row < _rows.GetArrayLength();
    }

    public override Task<bool> ReadAsync(CancellationToken ct) => Task.FromResult(Read());

    public override bool NextResult()
    {
        if (_set + 1 >= _resultSets.Count)
        {
            _set = _resultSets.Count;
            return false;
        }
        _set++;
        LoadSet();
        return true;
    }

    public override Task<bool> NextResultAsync(CancellationToken ct) =>
        Task.FromResult(NextResult());

    public override int FieldCount => _columns.Length;
    public override bool HasRows => _set < _resultSets.Count && _rows.GetArrayLength() > 0;
    public override bool IsClosed => false;
    public override int RecordsAffected => _recordsAffected;
    public override int Depth => 0;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override string GetName(int ordinal) => _columns[ordinal];

    public override int GetOrdinal(string name)
    {
        for (var i = 0; i < _columns.Length; i++)
            if (string.Equals(_columns[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new IndexOutOfRangeException($"no column named {name}");
    }

    public override Type GetFieldType(int ordinal) => _types[ordinal];
    public override string GetDataTypeName(int ordinal) => _types[ordinal].Name;

    private JsonElement Cell(int ordinal) => _rows[_row][ordinal];

    public override bool IsDBNull(int ordinal) => Cell(ordinal).ValueKind == JsonValueKind.Null;

    public override object GetValue(int ordinal)
    {
        var cell = Cell(ordinal);
        if (cell.ValueKind == JsonValueKind.Null) return DBNull.Value;
        var t = _types[ordinal];
        if (t == typeof(long)) return cell.GetInt64();
        if (t == typeof(double)) return cell.GetDouble();
        if (t == typeof(string)) return cell.GetString()!;
        if (t == typeof(bool)) return cell.GetBoolean();
        if (t == typeof(DateTime))
            return DateTimeOffset.FromUnixTimeMilliseconds(cell.GetInt64()).UtcDateTime;
        // Untyped column: infer from the JSON value.
        return cell.ValueKind switch
        {
            JsonValueKind.String => cell.GetString()!,
            JsonValueKind.True or JsonValueKind.False => cell.GetBoolean(),
            JsonValueKind.Number => cell.TryGetInt64(out var n) ? n : cell.GetDouble(),
            _ => cell.GetRawText(),
        };
    }

    public override int GetValues(object[] values)
    {
        var n = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < n; i++) values[i] = GetValue(i);
        return n;
    }

    public override bool GetBoolean(int ordinal) => Cell(ordinal).GetBoolean();
    public override byte GetByte(int ordinal) => (byte)GetInt64(ordinal);
    public override char GetChar(int ordinal) => GetString(ordinal)[0];
    public override short GetInt16(int ordinal) => (short)GetInt64(ordinal);
    public override int GetInt32(int ordinal) => (int)GetInt64(ordinal);

    public override long GetInt64(int ordinal)
    {
        var cell = Cell(ordinal);
        return cell.ValueKind == JsonValueKind.Number ? cell.GetInt64() : long.Parse(cell.GetString()!);
    }

    public override float GetFloat(int ordinal) => (float)GetDouble(ordinal);

    public override double GetDouble(int ordinal)
    {
        var cell = Cell(ordinal);
        return cell.ValueKind == JsonValueKind.Number ? cell.GetDouble() : double.Parse(cell.GetString()!);
    }

    public override decimal GetDecimal(int ordinal) => (decimal)GetDouble(ordinal);
    public override string GetString(int ordinal) => Cell(ordinal).GetString()!;
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
