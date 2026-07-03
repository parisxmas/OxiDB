using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace OxiDb.EntityFrameworkCore;

/// <summary>Adds the string-method translations on top of the defaults.</summary>
public sealed class OxiDbMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public OxiDbMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies) =>
        AddTranslators([new OxiDbStringMethodTranslator(dependencies.SqlExpressionFactory)]);
}

/// <summary>Adds <c>string.Length</c> → <c>LENGTH()</c> on top of the defaults.</summary>
public sealed class OxiDbMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public OxiDbMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies) =>
        AddTranslators([new OxiDbStringLengthTranslator(dependencies.SqlExpressionFactory)]);
}

/// <summary>
/// Contains/StartsWith/EndsWith → LIKE over CONCAT (so parameters work too;
/// wildcard characters inside the searched value are not escaped in v1),
/// ToUpper/ToLower → UPPER/LOWER, Trim → TRIM, Replace → REPLACE,
/// Substring → SUBSTRING (CLR 0-based → SQL 1-based).
/// </summary>
internal sealed class OxiDbStringMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    public OxiDbStringMethodTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(string) || instance is null)
            return null;

        SqlExpression Pct() => _sql.Constant("%");
        SqlExpression Concat(params SqlExpression[] args) =>
            _sql.Function("CONCAT", args, nullable: true,
                argumentsPropagateNullability: args.Select(_ => true).ToArray(),
                typeof(string), instance!.TypeMapping);

        switch (method.Name)
        {
            case nameof(string.Contains) when arguments is [{ Type: var t }] && t == typeof(string):
                return _sql.Like(instance, Concat(Pct(), arguments[0], Pct()));
            case nameof(string.StartsWith) when arguments is [{ Type: var t }] && t == typeof(string):
                return _sql.Like(instance, Concat(arguments[0], Pct()));
            case nameof(string.EndsWith) when arguments is [{ Type: var t }] && t == typeof(string):
                return _sql.Like(instance, Concat(Pct(), arguments[0]));
            case nameof(string.ToUpper) when arguments.Count == 0:
                return Fn("UPPER", instance);
            case nameof(string.ToLower) when arguments.Count == 0:
                return Fn("LOWER", instance);
            case nameof(string.Trim) when arguments.Count == 0:
                return Fn("TRIM", instance);
            case nameof(string.Replace) when arguments.Count == 2:
                return Fn("REPLACE", instance, arguments[0], arguments[1]);
            case nameof(string.Substring) when arguments.Count is 1 or 2:
            {
                var from = _sql.Add(arguments[0], _sql.Constant(1));
                return arguments.Count == 1
                    ? Fn("SUBSTRING", instance, from)
                    : Fn("SUBSTRING", instance, from, arguments[1]);
            }
            default:
                return null;
        }

        SqlExpression Fn(string name, params SqlExpression[] args) =>
            _sql.Function(name, args, nullable: true,
                argumentsPropagateNullability: args.Select(_ => true).ToArray(),
                method.ReturnType, instance!.TypeMapping);
    }
}

internal sealed class OxiDbStringLengthTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sql;

    public OxiDbStringLengthTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger) =>
        member.DeclaringType == typeof(string) && member.Name == nameof(string.Length)
            && instance is not null
            ? _sql.Function("LENGTH", [instance], nullable: true,
                argumentsPropagateNullability: [true], returnType)
            : null;
}
