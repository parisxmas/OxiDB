using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Metadata;

namespace OxiDb.EntityFrameworkCore.Query;

/// <summary>
/// Translates LINQ expression trees into OxiDB JSON query objects.
/// Supports Where, OrderBy, Take, Skip, Count, First, Any.
/// </summary>
public static class OxiDbLinqTranslator
{
    /// <summary>
    /// Translates a Where predicate expression into an OxiDB filter dictionary.
    /// </summary>
    public static Dictionary<string, object?> TranslateWhere(Expression expression, IEntityType entityType)
    {
        return TranslateExpression(expression, entityType);
    }

    private static Dictionary<string, object?> TranslateExpression(Expression expr, IEntityType entityType)
    {
        return expr switch
        {
            BinaryExpression binary => TranslateBinary(binary, entityType),
            MethodCallExpression method => TranslateMethodCall(method, entityType),
            UnaryExpression { NodeType: ExpressionType.Not } unary =>
                TranslateNot(unary.Operand, entityType),
            LambdaExpression lambda => TranslateExpression(lambda.Body, entityType),
            _ => new Dictionary<string, object?>()
        };
    }

    private static Dictionary<string, object?> TranslateBinary(BinaryExpression binary, IEntityType entityType)
    {
        // Logical operators
        if (binary.NodeType == ExpressionType.AndAlso)
        {
            var left = TranslateExpression(binary.Left, entityType);
            var right = TranslateExpression(binary.Right, entityType);
            return new Dictionary<string, object?>
            {
                ["$and"] = new[] { left, right }
            };
        }

        if (binary.NodeType == ExpressionType.OrElse)
        {
            var left = TranslateExpression(binary.Left, entityType);
            var right = TranslateExpression(binary.Right, entityType);
            return new Dictionary<string, object?>
            {
                ["$or"] = new[] { left, right }
            };
        }

        // Comparison operators
        var (fieldName, value) = ExtractFieldAndValue(binary, entityType);
        if (fieldName == null) return new Dictionary<string, object?>();

        return binary.NodeType switch
        {
            ExpressionType.Equal => new Dictionary<string, object?> { [fieldName] = value },
            ExpressionType.NotEqual => new Dictionary<string, object?>
            {
                [fieldName] = new Dictionary<string, object?> { ["$ne"] = value }
            },
            ExpressionType.GreaterThan => new Dictionary<string, object?>
            {
                [fieldName] = new Dictionary<string, object?> { ["$gt"] = value }
            },
            ExpressionType.GreaterThanOrEqual => new Dictionary<string, object?>
            {
                [fieldName] = new Dictionary<string, object?> { ["$gte"] = value }
            },
            ExpressionType.LessThan => new Dictionary<string, object?>
            {
                [fieldName] = new Dictionary<string, object?> { ["$lt"] = value }
            },
            ExpressionType.LessThanOrEqual => new Dictionary<string, object?>
            {
                [fieldName] = new Dictionary<string, object?> { ["$lte"] = value }
            },
            _ => new Dictionary<string, object?>()
        };
    }

    private static Dictionary<string, object?> TranslateMethodCall(MethodCallExpression method, IEntityType entityType)
    {
        // Contains for $in: list.Contains(field) or field.Contains(value)
        if (method.Method.Name == "Contains" && method.Object != null)
        {
            // String.Contains -> not directly supported, approximate with $eq
            // List<T>.Contains -> $in
            if (method.Object.Type.IsGenericType &&
                method.Object.Type.GetGenericTypeDefinition() == typeof(List<>))
            {
                var fieldName = ExtractFieldName(method.Arguments[0], entityType);
                var listValue = EvaluateExpression(method.Object);
                if (fieldName != null)
                {
                    return new Dictionary<string, object?>
                    {
                        [fieldName] = new Dictionary<string, object?> { ["$in"] = listValue }
                    };
                }
            }
        }

        // Enumerable.Contains(source, value) -> $in
        if (method.Method.Name == "Contains" && method.Object == null && method.Arguments.Count == 2)
        {
            var fieldName = ExtractFieldName(method.Arguments[1], entityType);
            var listValue = EvaluateExpression(method.Arguments[0]);
            if (fieldName != null)
            {
                return new Dictionary<string, object?>
                {
                    [fieldName] = new Dictionary<string, object?> { ["$in"] = listValue }
                };
            }
        }

        return new Dictionary<string, object?>();
    }

    private static Dictionary<string, object?> TranslateNot(Expression operand, IEntityType entityType)
    {
        // !bool_field -> {field: false}
        var fieldName = ExtractFieldName(operand, entityType);
        if (fieldName != null)
        {
            return new Dictionary<string, object?> { [fieldName] = false };
        }
        return new Dictionary<string, object?>();
    }

    private static (string? fieldName, object? value) ExtractFieldAndValue(
        BinaryExpression binary, IEntityType entityType)
    {
        // Try left = member, right = value
        var leftField = ExtractFieldName(binary.Left, entityType);
        if (leftField != null)
        {
            var value = EvaluateExpression(binary.Right);
            return (leftField, value);
        }

        // Try right = member, left = value (reversed comparison)
        var rightField = ExtractFieldName(binary.Right, entityType);
        if (rightField != null)
        {
            var value = EvaluateExpression(binary.Left);
            return (rightField, value);
        }

        return (null, null);
    }

    /// <summary>
    /// Extracts an OxiDB field name from a member access expression.
    /// PK properties map to "_key", others to snake_case.
    /// </summary>
    public static string? ExtractFieldName(Expression expr, IEntityType entityType)
    {
        if (expr is MemberExpression member && member.Member is PropertyInfo prop)
        {
            var efProp = entityType.FindProperty(prop.Name);
            if (efProp != null)
            {
                return efProp.IsPrimaryKey() ? "_key" : ToSnakeCase(prop.Name);
            }
        }
        return null;
    }

    /// <summary>
    /// Evaluates a constant or closure-captured value from an expression.
    /// </summary>
    public static object? EvaluateExpression(Expression expr)
    {
        return expr switch
        {
            ConstantExpression c => c.Value,
            MemberExpression m => EvaluateMemberExpression(m),
            UnaryExpression { NodeType: ExpressionType.Convert } u => EvaluateExpression(u.Operand),
            _ => Expression.Lambda(expr).Compile().DynamicInvoke()
        };
    }

    private static object? EvaluateMemberExpression(MemberExpression member)
    {
        if (member.Expression is ConstantExpression constant)
        {
            return member.Member switch
            {
                FieldInfo field => field.GetValue(constant.Value),
                PropertyInfo prop => prop.GetValue(constant.Value),
                _ => null
            };
        }

        return Expression.Lambda(member).Compile().DynamicInvoke();
    }

    /// <summary>
    /// Extracts sort specification from an OrderBy key selector.
    /// </summary>
    public static Dictionary<string, int> TranslateOrderBy(
        Expression keySelector, IEntityType entityType, bool descending = false)
    {
        var body = keySelector is LambdaExpression lambda ? lambda.Body : keySelector;
        var fieldName = ExtractFieldName(body, entityType);
        if (fieldName != null)
        {
            return new Dictionary<string, int> { [fieldName] = descending ? -1 : 1 };
        }
        return new Dictionary<string, int>();
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
}
