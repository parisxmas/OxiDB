using System.Text.Json;

namespace OxiDb.Client;

/// <summary>
/// Builds MongoDB-style update definition JSON strings.
/// </summary>
public sealed class UpdateDef
{
    private readonly Dictionary<string, Dictionary<string, object?>> _ops;

    private UpdateDef(string op, string field, object? value)
    {
        _ops = new Dictionary<string, Dictionary<string, object?>>
        {
            [op] = new Dictionary<string, object?> { [field] = value }
        };
    }

    private UpdateDef(Dictionary<string, Dictionary<string, object?>> ops)
    {
        _ops = ops;
    }

    // --- Field update operators ---

    /// <summary>Sets the value of a field.</summary>
    public static UpdateDef Set(string field, object? value) => new("$set", field, value);

    /// <summary>Removes a field from the document.</summary>
    public static UpdateDef Unset(string field) => new("$unset", field, "");

    /// <summary>Increments a field by the given amount.</summary>
    public static UpdateDef Inc(string field, object value) => new("$inc", field, value);

    /// <summary>Multiplies a field by the given amount.</summary>
    public static UpdateDef Mul(string field, object value) => new("$mul", field, value);

    /// <summary>Updates the field if the specified value is less than the current value.</summary>
    public static UpdateDef Min(string field, object value) => new("$min", field, value);

    /// <summary>Updates the field if the specified value is greater than the current value.</summary>
    public static UpdateDef Max(string field, object value) => new("$max", field, value);

    /// <summary>Renames a field.</summary>
    public static UpdateDef Rename(string field, string newName) => new("$rename", field, newName);

    /// <summary>Sets the field to the current date.</summary>
    public static UpdateDef CurrentDate(string field) => new("$currentDate", field, true);

    // --- Array update operators ---

    /// <summary>Appends a value to an array field.</summary>
    public static UpdateDef Push(string field, object? value) => new("$push", field, value);

    /// <summary>Removes all instances of a value from an array field.</summary>
    public static UpdateDef Pull(string field, object? value) => new("$pull", field, value);

    /// <summary>Adds a value to an array field only if it doesn't already exist.</summary>
    public static UpdateDef AddToSet(string field, object? value) => new("$addToSet", field, value);

    /// <summary>Removes the first element from an array field.</summary>
    public static UpdateDef PopFirst(string field) => new("$pop", field, -1);

    /// <summary>Removes the last element from an array field.</summary>
    public static UpdateDef PopLast(string field) => new("$pop", field, 1);

    /// <summary>
    /// Combines two update definitions. Same-operator fields are merged into a single operator.
    /// </summary>
    public static UpdateDef operator +(UpdateDef left, UpdateDef right)
    {
        var merged = new Dictionary<string, Dictionary<string, object?>>();

        foreach (var (op, fields) in left._ops)
            merged[op] = new Dictionary<string, object?>(fields);

        foreach (var (op, fields) in right._ops)
        {
            if (merged.TryGetValue(op, out var existing))
            {
                foreach (var (field, value) in fields)
                    existing[field] = value;
            }
            else
            {
                merged[op] = new Dictionary<string, object?>(fields);
            }
        }

        return new UpdateDef(merged);
    }

    /// <summary>Serializes the update definition to a JSON string.</summary>
    public string ToJson() => JsonSerializer.Serialize(_ops);

    /// <inheritdoc/>
    public override string ToString() => ToJson();
}
