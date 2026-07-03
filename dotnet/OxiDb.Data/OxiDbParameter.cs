using System.Collections;
using System.Data;
using System.Data.Common;

namespace OxiDb.Data;

public sealed class OxiDbParameter : DbParameter
{
    public override DbType DbType { get; set; } = DbType.Object;
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; } = true;

    [AllowNull]
    public override string ParameterName { get; set; } = "";

    public override int Size { get; set; }

    [AllowNull]
    public override string SourceColumn { get; set; } = "";
    public override bool SourceColumnNullMapping { get; set; }
    public override object? Value { get; set; }

    public override void ResetDbType() => DbType = DbType.Object;

    /// <summary>The name without a leading <c>@</c>, for lookups.</summary>
    internal string BareName => ParameterName.TrimStart('@');
}

public sealed class OxiDbParameterCollection : DbParameterCollection
{
    private readonly List<OxiDbParameter> _items = new();

    public override int Count => _items.Count;
    public override object SyncRoot => _items;

    public override int Add(object value)
    {
        _items.Add((OxiDbParameter)value);
        return _items.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var v in values) Add(v!);
    }

    public override void Clear() => _items.Clear();
    public override bool Contains(object value) => _items.Contains((OxiDbParameter)value);
    public override bool Contains(string value) => Find(value) is not null;
    public override void CopyTo(Array array, int index) => ((IList)_items).CopyTo(array, index);
    public override IEnumerator GetEnumerator() => _items.GetEnumerator();

    protected override DbParameter GetParameter(int index) => _items[index];
    protected override DbParameter GetParameter(string parameterName) =>
        Find(parameterName)
        ?? throw new IndexOutOfRangeException($"no parameter named {parameterName}");

    public override int IndexOf(object value) => _items.IndexOf((OxiDbParameter)value);
    public override int IndexOf(string parameterName)
    {
        var bare = parameterName.TrimStart('@');
        return _items.FindIndex(p =>
            string.Equals(p.BareName, bare, StringComparison.OrdinalIgnoreCase));
    }

    public override void Insert(int index, object value) =>
        _items.Insert(index, (OxiDbParameter)value);

    public override void Remove(object value) => _items.Remove((OxiDbParameter)value);
    public override void RemoveAt(int index) => _items.RemoveAt(index);
    public override void RemoveAt(string parameterName)
    {
        var i = IndexOf(parameterName);
        if (i >= 0) _items.RemoveAt(i);
    }

    protected override void SetParameter(int index, DbParameter value) =>
        _items[index] = (OxiDbParameter)value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var i = IndexOf(parameterName);
        if (i >= 0) _items[i] = (OxiDbParameter)value;
        else Add(value);
    }

    internal OxiDbParameter? Find(string name)
    {
        var i = IndexOf(name);
        return i >= 0 ? _items[i] : null;
    }
}
