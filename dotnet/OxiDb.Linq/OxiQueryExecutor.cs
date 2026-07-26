using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace OxiDb.Linq;

internal static class Json
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
    };
}

/// <summary>
/// Turns an <see cref="OxiQuery"/> into one or more wire calls and shapes
/// the response into the result type expected by the caller.
/// </summary>
internal static class OxiQueryExecutor
{
    public static async Task<TResult> ExecuteAsync<TResult>(
        IOxiCollectionInternal source,
        Type elementType,
        OxiQuery query,
        CancellationToken ct)
    {
        var client = source.Client;
        var name = source.Name;

        switch (query.ResultKind)
        {
            case OxiResultKind.Count:
                {
                    var n = await client.CountAsync(name, query.Filter, ct).ConfigureAwait(false);
                    return Cast<TResult>(n);
                }

            case OxiResultKind.Any:
                {
                    var docs = await client
                        .FindAsync(name, query.Filter, sort: null, skip: null, limit: 1, ct)
                        .ConfigureAwait(false);
                    return Cast<TResult>(docs.GetArrayLength() > 0);
                }

            case OxiResultKind.Sum:
            case OxiResultKind.Min:
            case OxiResultKind.Max:
            case OxiResultKind.Average:
                {
                    var pipeline = BuildAggregatePipeline(query);
                    var resp = await client.AggregateAsync(name, pipeline, ct).ConfigureAwait(false);
                    var first = resp.GetArrayLength() == 0 ? default : resp[0];
                    if (first.ValueKind != JsonValueKind.Object || !first.TryGetProperty("v", out var v))
                        return default!;
                    return v.Deserialize<TResult>(Json.Options)!;
                }

            case OxiResultKind.First:
            case OxiResultKind.Single:
                {
                    var docs = await client
                        .FindAsync(name, query.Filter, BuildSort(query.Sort),
                                   query.Skip, query.Take ?? 1, ct)
                        .ConfigureAwait(false);
                    var len = docs.GetArrayLength();
                    if (len == 0)
                    {
                        if (query.DefaultIfEmpty) return default!;
                        throw new InvalidOperationException("Sequence contains no elements.");
                    }
                    if (query.SingleResult && len > 1)
                        throw new InvalidOperationException("Sequence contains more than one element.");

                    var deserType = query.Projection?.Parameters[0].Type ?? elementType;
                    var item = docs[0].Deserialize(deserType, Json.Options)!;
                    if (query.Projection is not null)
                        item = query.Projection.Compile().DynamicInvoke(item)!;
                    return Cast<TResult>(item);
                }

            case OxiResultKind.List:
            default:
                {
                    var docs = await client
                        .FindAsync(name, query.Filter, BuildSort(query.Sort),
                                   query.Skip, query.Take, ct)
                        .ConfigureAwait(false);
                    var items = MaterialiseList(docs, elementType, query.Projection);
                    return Cast<TResult>(items);
                }
        }
    }

    private static object[] BuildAggregatePipeline(OxiQuery query)
    {
        var pipeline = new List<object>();
        if (query.Filter is { Count: > 0 })
            pipeline.Add(new Dictionary<string, object?> { ["$match"] = query.Filter });

        var fieldRef = "$";
        if (query.AggregateSelector is { } sel)
            fieldRef = "$" + OxiQueryTranslator.ResolveMemberPath(sel.Body);

        var op = query.ResultKind switch
        {
            OxiResultKind.Sum     => "$sum",
            OxiResultKind.Min     => "$min",
            OxiResultKind.Max     => "$max",
            OxiResultKind.Average => "$avg",
            _ => throw new NotSupportedException()
        };

        pipeline.Add(new Dictionary<string, object?>
        {
            ["$group"] = new Dictionary<string, object?>
            {
                ["_id"] = (object?)null,
                ["v"] = new Dictionary<string, object?> { [op] = fieldRef }
            }
        });

        return pipeline.ToArray();
    }

    private static object? BuildSort(Dictionary<string, int>? sort)
        => sort is null || sort.Count == 0 ? null : sort;

    private static object MaterialiseList(JsonElement docs, Type elementType, LambdaExpression? projection)
    {
        var resultType = projection?.ReturnType ?? elementType;
        var deserType = projection?.Parameters[0].Type ?? elementType;

        var listType = typeof(List<>).MakeGenericType(resultType);
        var list = (IList)Activator.CreateInstance(listType)!;

        var compiled = projection?.Compile();

        foreach (var doc in docs.EnumerateArray())
        {
            var item = doc.Deserialize(deserType, Json.Options)!;
            if (compiled is not null)
                item = compiled.DynamicInvoke(item)!;
            list.Add(item);
        }
        return list;
    }

    private static T Cast<T>(object? value)
    {
        if (value is null) return default!;
        if (value is T t) return t;
        if (typeof(T).IsAssignableFrom(value.GetType())) return (T)value;
        return (T)Convert.ChangeType(value, typeof(T))!;
    }
}
