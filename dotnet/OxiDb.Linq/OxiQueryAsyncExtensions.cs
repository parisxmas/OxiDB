using System.Linq.Expressions;

namespace OxiDb.Linq;

/// <summary>
/// Async terminators for OxiDb LINQ queries — equivalent to EF Core's
/// <c>ToListAsync</c>, <c>FirstAsync</c>, <c>CountAsync</c>, ... but without
/// taking a dependency on EF Core.
/// </summary>
public static class OxiQueryAsyncExtensions
{
    public static Task<List<T>> ToListAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<List<T>>(source, source.Expression, ct);

    public static Task<T[]> ToArrayAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<List<T>>(source, source.Expression, ct).ContinueWith(t => t.Result.ToArray(), ct);

    public static Task<T> FirstAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<T>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.First)), ct);

    public static Task<T> FirstAsync<T>(
        this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken ct = default)
        => Run<T>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.First), predicate), ct);

    public static Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<T?>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.FirstOrDefault)), ct);

    public static Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken ct = default)
        => Run<T?>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.FirstOrDefault), predicate), ct);

    public static Task<T> SingleAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<T>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.Single)), ct);

    public static Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<T?>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.SingleOrDefault)), ct);

    public static Task<int> CountAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<int>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.Count)), ct);

    public static Task<int> CountAsync<T>(
        this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken ct = default)
        => Run<int>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.Count), predicate), ct);

    public static Task<bool> AnyAsync<T>(
        this IQueryable<T> source, CancellationToken ct = default)
        => Run<bool>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.Any)), ct);

    public static Task<bool> AnyAsync<T>(
        this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken ct = default)
        => Run<bool>(source, AppendTerminator<T>(source.Expression, nameof(Queryable.Any), predicate), ct);

    public static Task<TKey> SumAsync<T, TKey>(
        this IQueryable<T> source, Expression<Func<T, TKey>> selector, CancellationToken ct = default)
        => RunAggregate<T, TKey>(source, OxiResultKind.Sum, selector, ct);

    public static Task<TKey> MinAsync<T, TKey>(
        this IQueryable<T> source, Expression<Func<T, TKey>> selector, CancellationToken ct = default)
        => RunAggregate<T, TKey>(source, OxiResultKind.Min, selector, ct);

    public static Task<TKey> MaxAsync<T, TKey>(
        this IQueryable<T> source, Expression<Func<T, TKey>> selector, CancellationToken ct = default)
        => RunAggregate<T, TKey>(source, OxiResultKind.Max, selector, ct);

    public static Task<double> AverageAsync<T>(
        this IQueryable<T> source, Expression<Func<T, double>> selector, CancellationToken ct = default)
        => RunAggregate<T, double>(source, OxiResultKind.Average, selector, ct);

    // ─── Plumbing ─────────────────────────────────────────────────────────

    private static Task<TResult> Run<TResult>(
        IQueryable source, Expression expression, CancellationToken ct)
    {
        if (source.Provider is OxiQueryProvider provider)
            return provider.ExecuteAsync<TResult>(expression, ct);
        // Fallback to sync execution
        return Task.FromResult((TResult)source.Provider.Execute(expression)!);
    }

    private static Expression AppendTerminator<T>(Expression source, string method)
    {
        var m = typeof(Queryable).GetMethods()
            .First(x => x.Name == method && x.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(T));
        return Expression.Call(null, m, source);
    }

    private static Expression AppendTerminator<T>(Expression source, string method, Expression<Func<T, bool>> predicate)
    {
        var m = typeof(Queryable).GetMethods()
            .First(x =>
                x.Name == method &&
                x.GetParameters().Length == 2 &&
                x.GetParameters()[1].ParameterType.IsGenericType &&
                x.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>))
            .MakeGenericMethod(typeof(T));
        return Expression.Call(null, m, source, Expression.Quote(predicate));
    }

    /// <summary>
    /// Sum/Min/Max/Average overloads in Queryable are typed per result type
    /// (decimal, int, double, ...), which makes generic method lookup brittle.
    /// We bypass it: build the IR directly with the requested aggregate kind.
    /// </summary>
    private static Task<TKey> RunAggregate<T, TKey>(
        IQueryable<T> source, OxiResultKind kind,
        Expression<Func<T, TKey>> selector, CancellationToken ct)
    {
        if (source.Provider is not OxiQueryProvider provider)
            throw new InvalidOperationException(
                "Aggregate terminators require an OxiCollection<T>-rooted query.");

        var query = OxiQueryTranslator.Translate(source.Expression, out var elementType);
        query.ResultKind = kind;
        query.AggregateSelector = selector;
        return OxiQueryExecutor.ExecuteAsync<TKey>(provider.Source, elementType, query, ct);
    }
}
