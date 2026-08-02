using System.Linq.Expressions;
using System.Reflection;

namespace OxiDb.Linq;

internal sealed class OxiQueryProvider(IOxiCollectionInternal source) : IQueryProvider
{
    internal IOxiCollectionInternal Source { get; } = source;

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type
            .GetInterfaces()
            .Concat(new[] { expression.Type })
            .First(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            .GetGenericArguments()[0];

        return (IQueryable)Activator.CreateInstance(
            typeof(OxiQueryable<>).MakeGenericType(elementType),
            Source, expression)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        => new OxiQueryable<TElement>(Source, expression);

    public object? Execute(Expression expression)
        => ExecuteSync(expression, expression.Type);

    public TResult Execute<TResult>(Expression expression)
        => (TResult)ExecuteSync(expression, typeof(TResult))!;

    internal Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken ct)
    {
        var query = OxiQueryTranslator.Translate(expression, out var elementType);
        return OxiQueryExecutor.ExecuteAsync<TResult>(Source, elementType, query, ct);
    }

    private object? ExecuteSync(Expression expression, Type resultType)
    {
        var query = OxiQueryTranslator.Translate(expression, out var elementType);
        var method = typeof(OxiQueryExecutor)
            .GetMethod(nameof(OxiQueryExecutor.ExecuteAsync), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(resultType);
        var task = (Task)method.Invoke(null, new object?[] { Source, elementType, query, CancellationToken.None })!;
        task.GetAwaiter().GetResult();
        return task.GetType().GetProperty("Result")!.GetValue(task);
    }
}
