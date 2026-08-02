using System.Linq.Expressions;

namespace OxiDb.Linq;

/// <summary>
/// Fluent mutation extensions: build a filter with LINQ <c>Where</c>, then
/// finish with <see cref="UpdateAsync{T}"/>, <see cref="UpdateOneAsync{T}"/>,
/// <see cref="DeleteAsync{T}"/>, or <see cref="DeleteOneAsync{T}"/>.
/// </summary>
public static class OxiMutationExtensions
{
    /// <summary>
    /// Apply a raw Mongo-style update document. Pass a dictionary or any type
    /// that serializes to <c>{"$set": {...}, "$inc": {...}, ...}</c>. For the
    /// common ops use <see cref="SetAsync"/>, <see cref="IncAsync"/>, etc.
    /// </summary>
    public static Task UpdateAsync<T>(
        this IQueryable<T> source, object update, CancellationToken ct = default)
    {
        var (collection, filter) = ExtractFilter(source);
        return collection.Client.UpdateAsync(collection.Name, filter, update, ct);
    }

    public static Task UpdateOneAsync<T>(
        this IQueryable<T> source, object update, CancellationToken ct = default)
    {
        var (collection, filter) = ExtractFilter(source);
        return collection.Client.UpdateOneAsync(collection.Name, filter, update, ct);
    }

    /// <summary>$set — overwrite or add fields.</summary>
    public static Task SetAsync<T>(this IQueryable<T> source, object fields, CancellationToken ct = default)
        => UpdateAsync(source, Op("$set", fields), ct);

    /// <summary>$unset — remove fields.</summary>
    public static Task UnsetAsync<T>(this IQueryable<T> source, object fields, CancellationToken ct = default)
        => UpdateAsync(source, Op("$unset", fields), ct);

    /// <summary>$inc — atomic numeric increment / decrement.</summary>
    public static Task IncAsync<T>(this IQueryable<T> source, object fields, CancellationToken ct = default)
        => UpdateAsync(source, Op("$inc", fields), ct);

    /// <summary>$push — append elements to arrays.</summary>
    public static Task PushAsync<T>(this IQueryable<T> source, object fields, CancellationToken ct = default)
        => UpdateAsync(source, Op("$push", fields), ct);

    /// <summary>$pull — remove array elements matching a filter.</summary>
    public static Task PullAsync<T>(this IQueryable<T> source, object fields, CancellationToken ct = default)
        => UpdateAsync(source, Op("$pull", fields), ct);

    /// <summary>$addToSet — push only if not already present.</summary>
    public static Task AddToSetAsync<T>(this IQueryable<T> source, object fields, CancellationToken ct = default)
        => UpdateAsync(source, Op("$addToSet", fields), ct);

    private static Dictionary<string, object?> Op(string op, object fields)
        => new() { [op] = fields };

    public static Task DeleteAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
    {
        var (collection, filter) = ExtractFilter(source);
        return collection.Client.DeleteAsync(collection.Name, filter, ct);
    }

    public static Task DeleteOneAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
    {
        var (collection, filter) = ExtractFilter(source);
        return collection.Client.DeleteOneAsync(collection.Name, filter, ct);
    }

    private static (IOxiCollectionInternal Collection, Dictionary<string, object?> Filter) ExtractFilter<T>(
        IQueryable<T> source)
    {
        if (source.Provider is not OxiQueryProvider provider)
            throw new InvalidOperationException(
                "UpdateAsync/DeleteAsync require a query built from an OxiCollection<T>.");

        var query = OxiQueryTranslator.Translate(source.Expression, out _);
        var filter = query.Filter ?? new Dictionary<string, object?>();
        return (provider.Source, filter);
    }
}
