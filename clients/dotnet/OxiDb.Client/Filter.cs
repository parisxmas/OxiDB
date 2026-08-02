using System.Text.Json;

namespace OxiDb.Client;

/// <summary>
/// Builds MongoDB-style query filter JSON strings.
/// </summary>
public sealed class Filter
{
    private readonly Dictionary<string, object?> _doc;

    private Filter(Dictionary<string, object?> doc)
    {
        _doc = doc;
    }

    // --- Comparison operators ---

    /// <summary>Matches documents where the field equals the value. Uses shorthand form: {"field": value}.</summary>
    public static Filter Eq(string field, object? value) =>
        new(new Dictionary<string, object?> { [field] = value });

    /// <summary>Matches documents where the field does not equal the value.</summary>
    public static Filter Ne(string field, object? value) =>
        new(new Dictionary<string, object?> { [field] = new Dictionary<string, object?> { ["$ne"] = value } });

    /// <summary>Matches documents where the field is greater than the value.</summary>
    public static Filter Gt(string field, object? value) =>
        new(new Dictionary<string, object?> { [field] = new Dictionary<string, object?> { ["$gt"] = value } });

    /// <summary>Matches documents where the field is greater than or equal to the value.</summary>
    public static Filter Gte(string field, object? value) =>
        new(new Dictionary<string, object?> { [field] = new Dictionary<string, object?> { ["$gte"] = value } });

    /// <summary>Matches documents where the field is less than the value.</summary>
    public static Filter Lt(string field, object? value) =>
        new(new Dictionary<string, object?> { [field] = new Dictionary<string, object?> { ["$lt"] = value } });

    /// <summary>Matches documents where the field is less than or equal to the value.</summary>
    public static Filter Lte(string field, object? value) =>
        new(new Dictionary<string, object?> { [field] = new Dictionary<string, object?> { ["$lte"] = value } });

    /// <summary>Matches documents where the field value is in the given array.</summary>
    public static Filter In(string field, params object?[] values) =>
        new(new Dictionary<string, object?> { [field] = new Dictionary<string, object?> { ["$in"] = values } });

    /// <summary>Matches documents where the field exists (or not).</summary>
    public static Filter Exists(string field, bool exists = true) =>
        new(new Dictionary<string, object?> { [field] = new Dictionary<string, object?> { ["$exists"] = exists } });

    // --- Logical operators ---

    /// <summary>Combines filters with $and.</summary>
    public static Filter And(params Filter[] filters) =>
        new(new Dictionary<string, object?> { ["$and"] = filters.Select(f => f._doc).ToArray() });

    /// <summary>Combines filters with $or.</summary>
    public static Filter Or(params Filter[] filters) =>
        new(new Dictionary<string, object?> { ["$or"] = filters.Select(f => f._doc).ToArray() });

    /// <summary>Combines two filters with $and using the &amp; operator.</summary>
    public static Filter operator &(Filter left, Filter right) => And(left, right);

    /// <summary>Combines two filters with $or using the | operator.</summary>
    public static Filter operator |(Filter left, Filter right) => Or(left, right);

    /// <summary>Serializes the filter to a JSON string.</summary>
    public string ToJson() => JsonSerializer.Serialize(_doc);

    /// <inheritdoc/>
    public override string ToString() => ToJson();
}
