using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;

namespace OxiDb.Linq;

/// <summary>
/// Walks a LINQ Expression tree and lowers it into an <see cref="OxiQuery"/>.
/// Supported pipeline ops: Where, OrderBy/Descending/ThenBy/ThenByDescending,
/// Skip, Take, Select, First/FirstOrDefault, Single/SingleOrDefault, Count,
/// Any, Sum, Min, Max, Average.
/// </summary>
internal static class OxiQueryTranslator
{
    public static OxiQuery Translate(Expression expression, out Type elementType)
    {
        var query = new OxiQuery();
        elementType = WalkPipeline(expression, query);
        return query;
    }

    private static Type WalkPipeline(Expression expression, OxiQuery query)
    {
        // Pipeline is a chain of MethodCallExpressions ending at the source ConstantExpression.
        // We walk from the outside in (latest call first), but we want to apply them in
        // source-first order (Where then OrderBy then Take). Easiest: collect, reverse, apply.
        var stack = new Stack<MethodCallExpression>();
        Expression current = expression;
        while (current is MethodCallExpression call &&
               (call.Method.DeclaringType == typeof(Queryable) ||
                call.Method.DeclaringType == typeof(Enumerable)))
        {
            stack.Push(call);
            current = call.Arguments[0];
        }

        // current should now be a ConstantExpression wrapping IOxiCollectionInternal.
        var elementType = current.Type.IsGenericType
            ? current.Type.GetGenericArguments()[0]
            : current.Type;

        while (stack.Count > 0)
        {
            var call = stack.Pop();
            elementType = ApplyMethod(call, query, elementType);
        }

        return elementType;
    }

    private static Type ApplyMethod(MethodCallExpression call, OxiQuery query, Type elementType)
    {
        switch (call.Method.Name)
        {
            case "Where":
                {
                    var predicate = Unwrap(call.Arguments[1]);
                    var filter = TranslatePredicate(predicate);
                    query.Filter = MergeAnd(query.Filter, filter);
                    return elementType;
                }

            case "OrderBy":
            case "ThenBy":
                AddSort(call, query, ascending: true);
                return elementType;

            case "OrderByDescending":
            case "ThenByDescending":
                AddSort(call, query, ascending: false);
                return elementType;

            case "Skip":
                query.Skip = (int)((ConstantExpression)call.Arguments[1]).Value!;
                return elementType;

            case "Take":
                query.Take = (int)((ConstantExpression)call.Arguments[1]).Value!;
                return elementType;

            case "Select":
                {
                    var projection = (LambdaExpression)Unwrap(call.Arguments[1]);
                    query.Projection = projection;
                    return projection.ReturnType;
                }

            case "First":
            case "FirstOrDefault":
                if (call.Arguments.Count == 2)
                {
                    var predicate = Unwrap(call.Arguments[1]);
                    query.Filter = MergeAnd(query.Filter, TranslatePredicate(predicate));
                }
                query.ResultKind = OxiResultKind.First;
                query.DefaultIfEmpty = call.Method.Name == "FirstOrDefault";
                query.Take = 1;
                return elementType;

            case "Single":
            case "SingleOrDefault":
                if (call.Arguments.Count == 2)
                {
                    var predicate = Unwrap(call.Arguments[1]);
                    query.Filter = MergeAnd(query.Filter, TranslatePredicate(predicate));
                }
                query.ResultKind = OxiResultKind.Single;
                query.DefaultIfEmpty = call.Method.Name == "SingleOrDefault";
                query.SingleResult = true;
                query.Take = 2; // pull 2 to detect duplicates
                return elementType;

            case "Count":
            case "LongCount":
                if (call.Arguments.Count == 2)
                {
                    var predicate = Unwrap(call.Arguments[1]);
                    query.Filter = MergeAnd(query.Filter, TranslatePredicate(predicate));
                }
                query.ResultKind = OxiResultKind.Count;
                return typeof(int);

            case "Any":
                if (call.Arguments.Count == 2)
                {
                    var predicate = Unwrap(call.Arguments[1]);
                    query.Filter = MergeAnd(query.Filter, TranslatePredicate(predicate));
                }
                query.ResultKind = OxiResultKind.Any;
                query.Take = 1;
                return typeof(bool);

            case "Sum":
            case "Min":
            case "Max":
            case "Average":
                if (call.Arguments.Count == 2)
                    query.AggregateSelector = (LambdaExpression)Unwrap(call.Arguments[1]);
                query.ResultKind = call.Method.Name switch
                {
                    "Sum"     => OxiResultKind.Sum,
                    "Min"     => OxiResultKind.Min,
                    "Max"     => OxiResultKind.Max,
                    "Average" => OxiResultKind.Average,
                    _         => throw new NotSupportedException()
                };
                return call.Method.ReturnType;

            default:
                throw new NotSupportedException($"LINQ method '{call.Method.Name}' is not supported by OxiDb.Linq.");
        }
    }

    private static void AddSort(MethodCallExpression call, OxiQuery query, bool ascending)
    {
        var lambda = (LambdaExpression)Unwrap(call.Arguments[1]);
        var field = ResolveMemberPath(lambda.Body);
        query.Sort ??= new();
        query.Sort[field] = ascending ? 1 : -1;
    }

    // ─── Predicate translation ───────────────────────────────────────────────

    public static Dictionary<string, object?> TranslatePredicate(Expression expr)
    {
        if (expr is LambdaExpression lambda) expr = lambda.Body;
        return TranslateBoolExpression(expr);
    }

    private static Dictionary<string, object?> TranslateBoolExpression(Expression expr)
    {
        switch (expr)
        {
            case BinaryExpression bin:
                return TranslateBinary(bin);

            case UnaryExpression { NodeType: ExpressionType.Not } not:
                {
                    var inner = TranslateBoolExpression(not.Operand);
                    return new() { ["$nor"] = new object?[] { inner } };
                }

            case MethodCallExpression call:
                return TranslateMethodCall(call);

            case MemberExpression member when member.Type == typeof(bool):
                // bool field used as predicate: x => x.Active
                return new() { [ResolveMemberPath(member)] = true };

            case ConstantExpression { Value: true }:
                return new();

            case ConstantExpression { Value: false }:
                return new() { ["$nor"] = new object?[] { new Dictionary<string, object?>() } };

            default:
                throw new NotSupportedException($"Cannot translate predicate: {expr}");
        }
    }

    private static Dictionary<string, object?> TranslateBinary(BinaryExpression bin)
    {
        switch (bin.NodeType)
        {
            case ExpressionType.AndAlso:
                return MergeAnd(TranslateBoolExpression(bin.Left), TranslateBoolExpression(bin.Right));

            case ExpressionType.OrElse:
                return new()
                {
                    ["$or"] = new object?[]
                    {
                        TranslateBoolExpression(bin.Left),
                        TranslateBoolExpression(bin.Right)
                    }
                };

            case ExpressionType.Equal:
            case ExpressionType.NotEqual:
            case ExpressionType.GreaterThan:
            case ExpressionType.GreaterThanOrEqual:
            case ExpressionType.LessThan:
            case ExpressionType.LessThanOrEqual:
                return TranslateComparison(bin);

            default:
                throw new NotSupportedException($"Binary operator {bin.NodeType} is not supported.");
        }
    }

    private static Dictionary<string, object?> TranslateComparison(BinaryExpression bin)
    {
        // Field on one side, value on the other. Normalise so field is on the left.
        if (TryGetMemberPath(bin.Left, out var field) && TryEvaluate(bin.Right, out var value))
        {
            return ToComparison(field, value, bin.NodeType);
        }
        if (TryGetMemberPath(bin.Right, out field) && TryEvaluate(bin.Left, out value))
        {
            return ToComparison(field, value, Reverse(bin.NodeType));
        }
        throw new NotSupportedException(
            $"Comparison must be between a field and a constant: {bin}");
    }

    private static Dictionary<string, object?> ToComparison(string field, object? value, ExpressionType op)
        => op switch
        {
            ExpressionType.Equal              => new() { [field] = value },
            ExpressionType.NotEqual           => new() { [field] = new Dictionary<string, object?> { ["$ne"]  = value } },
            ExpressionType.GreaterThan        => new() { [field] = new Dictionary<string, object?> { ["$gt"]  = value } },
            ExpressionType.GreaterThanOrEqual => new() { [field] = new Dictionary<string, object?> { ["$gte"] = value } },
            ExpressionType.LessThan           => new() { [field] = new Dictionary<string, object?> { ["$lt"]  = value } },
            ExpressionType.LessThanOrEqual    => new() { [field] = new Dictionary<string, object?> { ["$lte"] = value } },
            _ => throw new NotSupportedException($"Unsupported comparison {op}")
        };

    private static ExpressionType Reverse(ExpressionType op) => op switch
    {
        ExpressionType.GreaterThan        => ExpressionType.LessThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThan           => ExpressionType.GreaterThan,
        ExpressionType.LessThanOrEqual    => ExpressionType.GreaterThanOrEqual,
        _ => op
    };

    private static Dictionary<string, object?> TranslateMethodCall(MethodCallExpression call)
    {
        // string methods
        if (call.Method.DeclaringType == typeof(string))
        {
            return call.Method.Name switch
            {
                "Contains"   => RegexOn(call, body => Escape(body)),
                "StartsWith" => RegexOn(call, body => "^" + Escape(body)),
                "EndsWith"   => RegexOn(call, body => Escape(body) + "$"),
                "IsNullOrEmpty" => IsNullOrEmpty(call.Arguments[0]),
                _ => throw new NotSupportedException($"Unsupported string method: {call.Method.Name}")
            };
        }

        // Collection.Contains(value) → {field: {$in: [value]}}
        // Two shapes:
        //   IEnumerable<T>.Contains(field): static call
        //   Generic instance method on List<T>
        if (call.Method.Name == "Contains")
        {
            var (collExpr, valueExpr) = call.Method.IsStatic
                ? (call.Arguments[0], call.Arguments[1])
                : (call.Object!, call.Arguments[0]);

            // Two cases: collection is constant (value IN coll) → {value: {$in: coll}}
            //            value is the field, coll is the constant
            if (TryEvaluate(collExpr, out var collValue) && collValue is IEnumerable enumerable
                && TryGetMemberPath(valueExpr, out var field))
            {
                var list = new List<object?>();
                foreach (var item in enumerable) list.Add(item);
                return new() { [field] = new Dictionary<string, object?> { ["$in"] = list } };
            }
        }

        throw new NotSupportedException($"Unsupported method call: {call.Method.DeclaringType?.Name}.{call.Method.Name}");
    }

    private static Dictionary<string, object?> RegexOn(MethodCallExpression call, Func<string, string> shape)
    {
        var fieldExpr = call.Object ?? call.Arguments[0];
        var valueExpr = call.Object is null ? call.Arguments[1] : call.Arguments[0];
        var field = ResolveMemberPath(fieldExpr);
        var literal = (string)EvaluateExpression(valueExpr)!;
        return new()
        {
            [field] = new Dictionary<string, object?> { ["$regex"] = shape(literal) }
        };
    }

    private static Dictionary<string, object?> IsNullOrEmpty(Expression target)
    {
        var field = ResolveMemberPath(target);
        return new()
        {
            ["$or"] = new object?[]
            {
                new Dictionary<string, object?> { [field] = null },
                new Dictionary<string, object?> { [field] = "" }
            }
        };
    }

    private static string Escape(string s)
    {
        var sb = new System.Text.StringBuilder(s.Length);
        foreach (var c in s)
        {
            if ("\\.^$|?*+()[]{}".IndexOf(c) >= 0) sb.Append('\\');
            sb.Append(c);
        }
        return sb.ToString();
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static Expression Unwrap(Expression e)
        => e is UnaryExpression u && u.NodeType == ExpressionType.Quote ? u.Operand : e;

    private static bool TryGetMemberPath(Expression e, out string path)
    {
        // Unwrap conversions
        while (e is UnaryExpression { NodeType: ExpressionType.Convert } u) e = u.Operand;

        if (e is MemberExpression m && IsParameterChain(m))
        {
            path = ResolveMemberPath(m);
            return true;
        }
        path = "";
        return false;
    }

    private static bool IsParameterChain(MemberExpression m)
    {
        Expression? cur = m;
        while (cur is MemberExpression mem) cur = mem.Expression;
        return cur is ParameterExpression;
    }

    public static string ResolveMemberPath(Expression expression)
    {
        // Unwrap convert
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } u)
            expression = u.Operand;

        if (expression is not MemberExpression member)
            throw new NotSupportedException($"Expected a property access, got {expression.NodeType}");

        var parts = new Stack<string>();
        Expression? cur = member;
        while (cur is MemberExpression mem)
        {
            parts.Push(MapName(mem.Member));
            cur = mem.Expression;
        }
        return string.Join('.', parts);
    }

    private static string MapName(MemberInfo m)
    {
        var attr = m.GetCustomAttribute<JsonPropertyNameAttribute>();
        if (attr is not null) return attr.Name;
        // Default: the property name as-is. Map well-known "Id" → "_id".
        if (string.Equals(m.Name, "Id", StringComparison.Ordinal)) return "_id";
        return m.Name;
    }

    private static bool TryEvaluate(Expression e, out object? value)
    {
        try { value = EvaluateExpression(e); return true; }
        catch { value = null; return false; }
    }

    public static object? EvaluateExpression(Expression e)
    {
        if (e is ConstantExpression c) return c.Value;
        // Compile and run (covers captured variables, method calls, etc.)
        var lambda = Expression.Lambda(Expression.Convert(e, typeof(object)));
        return lambda.Compile().DynamicInvoke();
    }

    private static Dictionary<string, object?> MergeAnd(
        Dictionary<string, object?>? a, Dictionary<string, object?>? b)
    {
        if (a is null || a.Count == 0) return b ?? new();
        if (b is null || b.Count == 0) return a;

        // No key collisions → trivial merge
        var collide = false;
        foreach (var k in b.Keys) { if (a.ContainsKey(k)) { collide = true; break; } }
        if (!collide)
        {
            var merged = new Dictionary<string, object?>(a);
            foreach (var (k, v) in b) merged[k] = v;
            return merged;
        }

        // Otherwise wrap in $and
        return new() { ["$and"] = new object?[] { a, b } };
    }
}
