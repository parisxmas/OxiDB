using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using OxiDb.Client.Tcp;
using OxiDb.EntityFrameworkCore.Storage;

namespace OxiDb.EntityFrameworkCore.Query;

/// <summary>
/// Custom IQueryCompiler that translates LINQ expression trees to OxiDB JSON queries.
/// Handles: Where, OrderBy(Descending), ThenBy(Descending), Take, Skip,
/// Count, Any, First(OrDefault), Single(OrDefault), ToList.
/// </summary>
public sealed class OxiDbQueryCompiler : IQueryCompiler
{
    private readonly OxiDbClientManager _clientManager;
    private readonly IModel _model;
    private readonly ICurrentDbContext _currentContext;

    public OxiDbQueryCompiler(OxiDbClientManager clientManager, ICurrentDbContext currentContext)
    {
        _clientManager = clientManager;
        _currentContext = currentContext;
        _model = currentContext.Context.Model;
    }

    public Func<QueryContext, TResult> CreateCompiledQuery<TResult>(Expression query)
    {
        return qc => Execute<TResult>(query);
    }

    public Func<QueryContext, TResult> CreateCompiledAsyncQuery<TResult>(Expression query)
    {
        return qc => Execute<TResult>(query);
    }

    public Expression<Func<QueryContext, TResult>> PrecompileQuery<TResult>(Expression query, bool async)
    {
        // No precompilation support — return a simple wrapper
        var param = Expression.Parameter(typeof(QueryContext), "qc");
        var body = Expression.Call(
            Expression.Constant(this),
            typeof(OxiDbQueryCompiler).GetMethod(nameof(Execute))!.MakeGenericMethod(typeof(TResult)),
            Expression.Constant(query));
        return Expression.Lambda<Func<QueryContext, TResult>>(body, param);
    }

    public TResult Execute<TResult>(Expression query)
    {
        return ExecuteCore<TResult>(query, CancellationToken.None);
    }

    public TResult ExecuteAsync<TResult>(Expression query, CancellationToken ct)
    {
        return ExecuteCore<TResult>(query, ct);
    }

    private TResult ExecuteCore<TResult>(Expression query, CancellationToken ct)
    {
        var client = _clientManager.GetClientAsync(ct).GetAwaiter().GetResult();
        var parsed = ParseQuery(query);

        // Determine result type
        var resultType = typeof(TResult);

        // Handle IAsyncEnumerable<T>
        if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            var elementType = resultType.GetGenericArguments()[0];
            return (TResult)ExecuteToAsyncEnumerable(client, parsed, elementType, ct);
        }

        // EF Core async calls use Task<T> as TResult — unwrap and re-wrap
        if (resultType.IsGenericType && resultType.GetGenericTypeDefinition() == typeof(Task<>))
        {
            var innerType = resultType.GetGenericArguments()[0];
            var method = typeof(OxiDbQueryCompiler)
                .GetMethod(nameof(ExecuteCoreInner), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(innerType);
            var innerResult = method.Invoke(this, [client, parsed, ct]);
            return (TResult)typeof(Task).GetMethod(nameof(Task.FromResult))!
                .MakeGenericMethod(innerType)
                .Invoke(null, [innerResult])!;
        }

        return ExecuteCoreInner<TResult>(client, parsed, ct);
    }

    private TResult ExecuteCoreInner<TResult>(IOxiDbClient client, ParsedQuery parsed, CancellationToken ct)
    {
        // Handle scalar operations (Count, Any, First, Single)
        if (parsed.ResultOperator != null)
        {
            return ExecuteScalar<TResult>(client, parsed, ct);
        }

        // Handle enumerable results
        if (IsEnumerableResult<TResult>())
        {
            return ExecuteEnumerable<TResult>(client, parsed, ct);
        }

        // Single entity result
        return ExecuteSingle<TResult>(client, parsed, ct);
    }

    private object ExecuteToAsyncEnumerable(
        IOxiDbClient client, ParsedQuery parsed, Type elementType, CancellationToken ct)
    {
        var method = typeof(OxiDbQueryCompiler)
            .GetMethod(nameof(ExecuteAsyncEnumerableGeneric), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(elementType);
        return method.Invoke(this, [client, parsed, ct])!;
    }

    private async IAsyncEnumerable<T> ExecuteAsyncEnumerableGeneric<T>(
        IOxiDbClient client, ParsedQuery parsed,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var results = await ExecuteFindAsync(client, parsed, ct);
        foreach (var item in results.EnumerateArray())
        {
            yield return DeserializeEntity<T>(item, parsed.EntityType);
        }
    }

    private TResult ExecuteScalar<TResult>(
        IOxiDbClient client, ParsedQuery parsed, CancellationToken ct)
    {
        switch (parsed.ResultOperator)
        {
            case "Count":
            case "LongCount":
                var count = client.CountAsync(parsed.Collection, parsed.Filter, ct)
                    .GetAwaiter().GetResult();
                return ChangeType<TResult>(count);

            case "Any":
                var anyCount = client.CountAsync(parsed.Collection, parsed.Filter, ct)
                    .GetAwaiter().GetResult();
                return ChangeType<TResult>(anyCount > 0);

            case "First":
            case "FirstOrDefault":
                parsed.Limit = 1;
                var firstResult = ExecuteFindAsync(client, parsed, ct).GetAwaiter().GetResult();
                if (firstResult.ValueKind == JsonValueKind.Array && firstResult.GetArrayLength() > 0)
                    return DeserializeEntity<TResult>(firstResult[0], parsed.EntityType);
                if (parsed.ResultOperator == "FirstOrDefault")
                    return default!;
                throw new InvalidOperationException("Sequence contains no elements");

            case "Single":
            case "SingleOrDefault":
                parsed.Limit = 2;
                var singleResult = ExecuteFindAsync(client, parsed, ct).GetAwaiter().GetResult();
                if (singleResult.ValueKind == JsonValueKind.Array)
                {
                    var len = singleResult.GetArrayLength();
                    if (len == 1)
                        return DeserializeEntity<TResult>(singleResult[0], parsed.EntityType);
                    if (len > 1)
                        throw new InvalidOperationException("Sequence contains more than one element");
                }
                if (parsed.ResultOperator == "SingleOrDefault")
                    return default!;
                throw new InvalidOperationException("Sequence contains no elements");

            default:
                return default!;
        }
    }

    private TResult ExecuteEnumerable<TResult>(
        IOxiDbClient client, ParsedQuery parsed, CancellationToken ct)
    {
        var result = ExecuteFindAsync(client, parsed, ct).GetAwaiter().GetResult();
        var entityType = GetElementType(typeof(TResult)) ?? parsed.EntityType.ClrType;

        var method = typeof(OxiDbQueryCompiler)
            .GetMethod(nameof(DeserializeList), BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(entityType);

        return (TResult)method.Invoke(this, [result, parsed.EntityType])!;
    }

    private TResult ExecuteSingle<TResult>(
        IOxiDbClient client, ParsedQuery parsed, CancellationToken ct)
    {
        parsed.Limit = 1;
        var result = ExecuteFindAsync(client, parsed, ct).GetAwaiter().GetResult();
        if (result.ValueKind == JsonValueKind.Array && result.GetArrayLength() > 0)
            return DeserializeEntity<TResult>(result[0], parsed.EntityType);
        return default!;
    }

    private static async Task<JsonElement> ExecuteFindAsync(
        IOxiDbClient client, ParsedQuery parsed, CancellationToken ct)
    {
        return await client.FindAsync(
            parsed.Collection,
            parsed.Filter.Count > 0 ? parsed.Filter : null,
            parsed.Sort.Count > 0 ? parsed.Sort : null,
            parsed.Skip,
            parsed.Limit,
            ct);
    }

    private ParsedQuery ParseQuery(Expression expression)
    {
        var parsed = new ParsedQuery();
        ParseExpression(expression, parsed);
        return parsed;
    }

    private void ParseExpression(Expression expression, ParsedQuery parsed)
    {
        switch (expression)
        {
            case MethodCallExpression method:
                // Process the source first (inner query)
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], parsed);

                switch (method.Method.Name)
                {
                    case "Where":
                        if (method.Arguments.Count > 1)
                        {
                            var predicate = StripQuotes(method.Arguments[1]);
                            var filter = OxiDbLinqTranslator.TranslateWhere(predicate, parsed.EntityType);
                            MergeFilter(parsed, filter);
                        }
                        break;

                    case "OrderBy":
                    case "ThenBy":
                        if (method.Arguments.Count > 1)
                        {
                            var keySelector = StripQuotes(method.Arguments[1]);
                            var sort = OxiDbLinqTranslator.TranslateOrderBy(keySelector, parsed.EntityType, false);
                            foreach (var kv in sort) parsed.Sort[kv.Key] = kv.Value;
                        }
                        break;

                    case "OrderByDescending":
                    case "ThenByDescending":
                        if (method.Arguments.Count > 1)
                        {
                            var keySelector = StripQuotes(method.Arguments[1]);
                            var sort = OxiDbLinqTranslator.TranslateOrderBy(keySelector, parsed.EntityType, true);
                            foreach (var kv in sort) parsed.Sort[kv.Key] = kv.Value;
                        }
                        break;

                    case "Take":
                        if (method.Arguments.Count > 1)
                            parsed.Limit = (int)OxiDbLinqTranslator.EvaluateExpression(method.Arguments[1])!;
                        break;

                    case "Skip":
                        if (method.Arguments.Count > 1)
                            parsed.Skip = (int)OxiDbLinqTranslator.EvaluateExpression(method.Arguments[1])!;
                        break;

                    case "Count":
                    case "LongCount":
                        parsed.ResultOperator = "Count";
                        if (method.Arguments.Count > 1)
                        {
                            var predicate = StripQuotes(method.Arguments[1]);
                            var filter = OxiDbLinqTranslator.TranslateWhere(predicate, parsed.EntityType);
                            MergeFilter(parsed, filter);
                        }
                        break;

                    case "Any":
                        parsed.ResultOperator = "Any";
                        if (method.Arguments.Count > 1)
                        {
                            var predicate = StripQuotes(method.Arguments[1]);
                            var filter = OxiDbLinqTranslator.TranslateWhere(predicate, parsed.EntityType);
                            MergeFilter(parsed, filter);
                        }
                        break;

                    case "First":
                        parsed.ResultOperator = "First";
                        if (method.Arguments.Count > 1)
                        {
                            var predicate = StripQuotes(method.Arguments[1]);
                            var filter = OxiDbLinqTranslator.TranslateWhere(predicate, parsed.EntityType);
                            MergeFilter(parsed, filter);
                        }
                        break;

                    case "FirstOrDefault":
                        parsed.ResultOperator = "FirstOrDefault";
                        if (method.Arguments.Count > 1)
                        {
                            var predicate = StripQuotes(method.Arguments[1]);
                            var filter = OxiDbLinqTranslator.TranslateWhere(predicate, parsed.EntityType);
                            MergeFilter(parsed, filter);
                        }
                        break;

                    case "Single":
                        parsed.ResultOperator = "Single";
                        if (method.Arguments.Count > 1)
                        {
                            var predicate = StripQuotes(method.Arguments[1]);
                            var filter = OxiDbLinqTranslator.TranslateWhere(predicate, parsed.EntityType);
                            MergeFilter(parsed, filter);
                        }
                        break;

                    case "SingleOrDefault":
                        parsed.ResultOperator = "SingleOrDefault";
                        if (method.Arguments.Count > 1)
                        {
                            var predicate = StripQuotes(method.Arguments[1]);
                            var filter = OxiDbLinqTranslator.TranslateWhere(predicate, parsed.EntityType);
                            MergeFilter(parsed, filter);
                        }
                        break;
                }
                break;

            case ConstantExpression constant:
                // This is the DbSet<T> root — extract entity type info
                if (constant.Type.IsGenericType)
                {
                    var entityClrType = constant.Type.GetGenericArguments().FirstOrDefault();
                    if (entityClrType != null)
                    {
                        var entityType = _model.FindEntityType(entityClrType);
                        if (entityType != null)
                        {
                            parsed.EntityType = entityType;
                            parsed.Collection = entityType.ShortName().ToLowerInvariant();
                        }
                    }
                }
                break;

            default:
                // EF Core 10+ uses EntityQueryRootExpression instead of ConstantExpression
                // for DbSet<T> roots. Check via reflection to avoid hard dependency.
                var exprType = expression.GetType();
                if (exprType.Name == "EntityQueryRootExpression")
                {
                    var entityTypeProp = exprType.GetProperty("EntityType");
                    if (entityTypeProp?.GetValue(expression) is IEntityType et)
                    {
                        parsed.EntityType = et;
                        parsed.Collection = et.ShortName().ToLowerInvariant();
                    }
                }
                break;
        }
    }

    private static void MergeFilter(ParsedQuery parsed, Dictionary<string, object?> filter)
    {
        if (parsed.Filter.Count == 0)
        {
            foreach (var kv in filter)
                parsed.Filter[kv.Key] = kv.Value;
        }
        else
        {
            // Combine with $and
            var existing = new Dictionary<string, object?>(parsed.Filter);
            parsed.Filter.Clear();
            parsed.Filter["$and"] = new[] { existing, filter };
        }
    }

    private static Expression StripQuotes(Expression expression)
    {
        while (expression.NodeType == ExpressionType.Quote)
            expression = ((UnaryExpression)expression).Operand;
        return expression;
    }

    private T DeserializeEntity<T>(JsonElement element, IEntityType entityType)
    {
        var entity = Activator.CreateInstance<T>();
        if (entity == null) return default!;

        foreach (var prop in entityType.GetProperties())
        {
            var fieldName = prop.IsPrimaryKey() ? "_key" : ToSnakeCase(prop.Name);
            if (!element.TryGetProperty(fieldName, out var jsonValue)) continue;
            if (jsonValue.ValueKind == JsonValueKind.Null) continue;

            var clrProp = entityType.ClrType.GetProperty(prop.Name);
            if (clrProp == null || !clrProp.CanWrite) continue;

            try
            {
                var value = DeserializeValue(jsonValue, clrProp.PropertyType);
                clrProp.SetValue(entity, value);
            }
            catch
            {
                // Skip properties that can't be deserialized
            }
        }

        return TrackEntity(entity, entityType);
    }

    /// <summary>
    /// Integrates a freshly-materialized entity with the context's change tracker so that
    /// read-modify-SaveChanges round-trips. If a row with the same primary key is already
    /// tracked, that instance is returned (identity resolution); otherwise the new entity
    /// is attached as Unchanged so a later mutation is detected and persisted.
    /// </summary>
    private T TrackEntity<T>(T entity, IEntityType entityType)
    {
        if (entity is null)
            return entity;

        var key = entityType.FindPrimaryKey();
        if (key is null)
            return entity; // keyless entity — nothing to track against

        var keyValues = new object?[key.Properties.Count];
        for (int i = 0; i < key.Properties.Count; i++)
        {
            var clrProp = entityType.ClrType.GetProperty(key.Properties[i].Name);
            keyValues[i] = clrProp?.GetValue(entity);
            if (keyValues[i] is null)
                return entity; // incomplete key — can't track reliably
        }

        var context = _currentContext.Context;
        var stateManager = context.GetService<IStateManager>();

        // Identity resolution: if this row is already tracked, hand back the tracked
        // instance so mutations land on the object SaveChanges inspects.
        var existing = stateManager.TryGetEntry(key, keyValues);
        if (existing != null)
            return (T)existing.Entity;

        // Otherwise track it as Unchanged — a later mutation is then picked up by
        // DetectChanges and translated into an update on SaveChanges.
        context.Attach((object)entity);
        return entity;
    }

    private static object? DeserializeValue(JsonElement element, Type targetType)
    {
        if (element.ValueKind == JsonValueKind.Null) return null;

        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlyingType == typeof(string))
            return element.GetString();
        if (underlyingType == typeof(int))
            return element.GetInt32();
        if (underlyingType == typeof(long))
            return element.GetInt64();
        if (underlyingType == typeof(double))
            return element.GetDouble();
        if (underlyingType == typeof(float))
            return element.GetSingle();
        if (underlyingType == typeof(decimal))
            return element.GetDecimal();
        if (underlyingType == typeof(bool))
            return element.GetBoolean();
        if (underlyingType == typeof(DateTime))
            return element.GetDateTime();
        if (underlyingType == typeof(Guid))
            return Guid.Parse(element.GetString()!);

        // List<string>
        if (underlyingType == typeof(List<string>) && element.ValueKind == JsonValueKind.Array)
        {
            var list = new List<string>();
            foreach (var item in element.EnumerateArray())
                list.Add(item.GetString()!);
            return list;
        }

        // Fallback: use System.Text.Json
        return element.Deserialize(targetType);
    }

    private List<T> DeserializeList<T>(JsonElement array, IEntityType entityType)
    {
        var list = new List<T>();
        if (array.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in array.EnumerateArray())
                list.Add(DeserializeEntity<T>(item, entityType));
        }
        return list;
    }

    private static bool IsEnumerableResult<TResult>()
    {
        var type = typeof(TResult);
        if (type.IsGenericType)
        {
            var def = type.GetGenericTypeDefinition();
            return def == typeof(List<>) || def == typeof(IList<>) ||
                   def == typeof(IEnumerable<>) || def == typeof(ICollection<>);
        }
        return false;
    }

    private static Type? GetElementType(Type type)
    {
        if (type.IsGenericType)
            return type.GetGenericArguments().FirstOrDefault();
        return null;
    }

    private static TTarget ChangeType<TTarget>(object value)
    {
        if (value is TTarget t) return t;

        var targetType = typeof(TTarget);

        // Handle async wrapper: TResult is Task<T> but value is T
        if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Task<>))
        {
            var innerType = targetType.GetGenericArguments()[0];
            var converted = Convert.ChangeType(value, innerType);
            return (TTarget)typeof(Task).GetMethod(nameof(Task.FromResult))!
                .MakeGenericMethod(innerType)
                .Invoke(null, [converted])!;
        }

        return (TTarget)Convert.ChangeType(value, targetType);
    }

    private static string ToSnakeCase(string name)
    {
        var result = new System.Text.StringBuilder();
        for (int i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0) result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }

    private sealed class ParsedQuery
    {
        public IEntityType EntityType { get; set; } = null!;
        public string Collection { get; set; } = "";
        public Dictionary<string, object?> Filter { get; } = new();
        public Dictionary<string, int> Sort { get; } = new();
        public int? Skip { get; set; }
        public int? Limit { get; set; }
        public string? ResultOperator { get; set; }
    }
}
