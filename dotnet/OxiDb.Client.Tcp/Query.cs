namespace OxiDb.Client.Tcp;

/// <summary>
/// Type-safe query builder. Each helper returns a plain
/// <see cref="Dictionary{TKey, TValue}"/> that the wire protocol's
/// query layer accepts directly — anywhere the typed CRUD overloads
/// take an <c>object query</c>, you can pass a <c>Query.*</c> result.
/// </summary>
/// <remarks>
/// The shapes match the JSON query operators documented at
/// <see href="https://oxidb.baltavista.com/docs/">oxidb.baltavista.com/docs</see>:
/// <c>$eq</c>, <c>$ne</c>, <c>$gt</c>, <c>$gte</c>, <c>$lt</c>, <c>$lte</c>,
/// <c>$in</c>, <c>$nin</c>, <c>$exists</c>, <c>$regex</c>, <c>$and</c>,
/// <c>$or</c>, <c>$nor</c>, <c>$not</c>.
///
/// All helpers are pure functions. The returned dictionaries are
/// independent — you can compose them freely.
///
/// <para>
/// Prefer the LINQ provider (<see cref="OxiDb.Linq.OxiDbClientExtensions"/>)
/// when your query naturally maps to LINQ — the source generator picks
/// it up and translates at compile time. Use <see cref="Query"/> when
/// you want a value-typed builder for runtime-constructed queries
/// (e.g. mapping from user input).
/// </para>
/// </remarks>
public static class Query
{
    // ── Equality / comparison ───────────────────────────────────────────

    /// <summary>Match documents where <paramref name="field"/> equals
    /// <paramref name="value"/>. Equivalent to MongoDB's implicit equality.</summary>
    public static Dictionary<string, object?> Eq(string field, object? value)
        => new() { [field] = value };

    /// <summary>Match documents where <paramref name="field"/> is not equal
    /// to <paramref name="value"/>.</summary>
    public static Dictionary<string, object?> Ne(string field, object? value)
        => new() { [field] = new Dictionary<string, object?> { ["$ne"] = value } };

    /// <summary>Match documents where <paramref name="field"/> &gt; <paramref name="value"/>.</summary>
    public static Dictionary<string, object?> Gt(string field, object? value)
        => new() { [field] = new Dictionary<string, object?> { ["$gt"] = value } };

    /// <summary>Match documents where <paramref name="field"/> &gt;= <paramref name="value"/>.</summary>
    public static Dictionary<string, object?> Gte(string field, object? value)
        => new() { [field] = new Dictionary<string, object?> { ["$gte"] = value } };

    /// <summary>Match documents where <paramref name="field"/> &lt; <paramref name="value"/>.</summary>
    public static Dictionary<string, object?> Lt(string field, object? value)
        => new() { [field] = new Dictionary<string, object?> { ["$lt"] = value } };

    /// <summary>Match documents where <paramref name="field"/> &lt;= <paramref name="value"/>.</summary>
    public static Dictionary<string, object?> Lte(string field, object? value)
        => new() { [field] = new Dictionary<string, object?> { ["$lte"] = value } };

    /// <summary>Match documents where <paramref name="field"/>'s value is in
    /// the given set. Index-accelerated.</summary>
    public static Dictionary<string, object?> In<T>(string field, IEnumerable<T> values)
        => new() { [field] = new Dictionary<string, object?> { ["$in"] = values.Cast<object?>().ToArray() } };

    /// <summary>Match documents where <paramref name="field"/>'s value is
    /// NOT in the given set.</summary>
    public static Dictionary<string, object?> Nin<T>(string field, IEnumerable<T> values)
        => new() { [field] = new Dictionary<string, object?> { ["$nin"] = values.Cast<object?>().ToArray() } };

    /// <summary>Match documents where <paramref name="field"/> exists (or
    /// doesn't, when <paramref name="exists"/> is false).</summary>
    public static Dictionary<string, object?> Exists(string field, bool exists = true)
        => new() { [field] = new Dictionary<string, object?> { ["$exists"] = exists } };

    /// <summary>Match documents where <paramref name="field"/>'s string
    /// value matches the given regex pattern.</summary>
    public static Dictionary<string, object?> Regex(string field, string pattern)
        => new() { [field] = new Dictionary<string, object?> { ["$regex"] = pattern } };

    // ── Logical combinators ─────────────────────────────────────────────

    /// <summary>Match documents satisfying ALL sub-queries.</summary>
    public static Dictionary<string, object?> And(params Dictionary<string, object?>[] subQueries)
        => new() { ["$and"] = subQueries };

    /// <summary>Match documents satisfying AT LEAST ONE sub-query.</summary>
    public static Dictionary<string, object?> Or(params Dictionary<string, object?>[] subQueries)
        => new() { ["$or"] = subQueries };

    /// <summary>Match documents satisfying NONE of the sub-queries.</summary>
    public static Dictionary<string, object?> Nor(params Dictionary<string, object?>[] subQueries)
        => new() { ["$nor"] = subQueries };

    // ── Range ───────────────────────────────────────────────────────────

    /// <summary>Match documents where <paramref name="field"/> lies in the
    /// half-open interval <c>[low, high)</c>. Single-field wrapper that
    /// builds <c>{ $gte: low, $lt: high }</c> on the same field.</summary>
    public static Dictionary<string, object?> Range(string field, object? low, object? high)
        => new()
        {
            [field] = new Dictionary<string, object?>
            {
                ["$gte"] = low,
                ["$lt"] = high,
            },
        };
}
