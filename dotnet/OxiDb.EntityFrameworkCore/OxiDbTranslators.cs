using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace OxiDb.EntityFrameworkCore;

/// <summary>
/// SQL translating visitor: opts in to LEAST/GREATEST generation
/// (EF normalizes <c>Math.Min/Max</c> and <c>EF.Functions.Least/Greatest</c>
/// through these hooks).
/// </summary>
public class OxiDbSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    public OxiDbSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
    }

    public override SqlExpression? GenerateLeast(
        IReadOnlyList<SqlExpression> expressions, Type resultType) =>
        Dependencies.SqlExpressionFactory.Function("LEAST", expressions, nullable: true,
            argumentsPropagateNullability: expressions.Select(_ => false).ToArray(), resultType);

    public override SqlExpression? GenerateGreatest(
        IReadOnlyList<SqlExpression> expressions, Type resultType) =>
        Dependencies.SqlExpressionFactory.Function("GREATEST", expressions, nullable: true,
            argumentsPropagateNullability: expressions.Select(_ => false).ToArray(), resultType);
}

public class OxiDbSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    private readonly RelationalSqlTranslatingExpressionVisitorDependencies _dependencies;

    public OxiDbSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies) =>
        _dependencies = dependencies;

    public virtual RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor) =>
        new OxiDbSqlTranslatingExpressionVisitor(
            _dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor);
}

/// <summary>Adds the string/math/DateTime method translations on top of the defaults.</summary>
public sealed class OxiDbMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public OxiDbMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies) =>
        AddTranslators(
        [
            new OxiDbStringMethodTranslator(dependencies.SqlExpressionFactory),
            new OxiDbMathMethodTranslator(dependencies.SqlExpressionFactory),
            new OxiDbDateTimeMethodTranslator(dependencies.SqlExpressionFactory),
            new OxiDbToStringTranslator(dependencies.SqlExpressionFactory),
            new OxiDbRegexTranslator(dependencies.SqlExpressionFactory),
            new OxiDbStringEnumerableTranslator(dependencies.SqlExpressionFactory),
        ]);
}

/// <summary>
/// Adds <c>string.Length</c> → <c>LENGTH()</c> and the <c>DateTime</c>
/// member translations on top of the defaults.
/// </summary>
public sealed class OxiDbMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public OxiDbMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies) =>
        AddTranslators(
        [
            new OxiDbStringLengthTranslator(dependencies.SqlExpressionFactory),
            new OxiDbDateTimeMemberTranslator(dependencies.SqlExpressionFactory),
        ]);
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
        if (method.DeclaringType != typeof(string))
            return null;
        // Statics.
        if (instance is null)
        {
            return method.Name switch
            {
                nameof(string.IsNullOrEmpty) when arguments.Count == 1 =>
                    _sql.OrElse(
                        _sql.IsNull(arguments[0]),
                        _sql.Equal(arguments[0], _sql.Constant(string.Empty))),
                nameof(string.IsNullOrWhiteSpace) when arguments.Count == 1 =>
                    _sql.OrElse(
                        _sql.IsNull(arguments[0]),
                        _sql.Equal(
                            _sql.Function("TRIM", [arguments[0]], nullable: true,
                                argumentsPropagateNullability: [true], typeof(string),
                                arguments[0].TypeMapping),
                            _sql.Constant(string.Empty))),
                _ => null,
            };
        }
        // string.FirstOrDefault()/LastOrDefault() arrive as Enumerable
        // extension calls, handled below; instance methods from here on.

        SqlExpression Pct() => _sql.Constant("%");
        SqlExpression Concat(params SqlExpression[] args) =>
            _sql.Function("CONCAT", args, nullable: true,
                argumentsPropagateNullability: args.Select(_ => true).ToArray(),
                typeof(string), instance!.TypeMapping);

        switch (method.Name)
        {
            case nameof(string.Contains) when arguments is [{ Type: var t }] && t == typeof(string):
                return _sql.Like(instance, Concat(Pct(), arguments[0], Pct()));
            // The char overload: position of the one-char needle, not LIKE
            // (a char like '%' must not act as a wildcard).
            case nameof(string.Contains) when arguments is [{ Type: var t }] && t == typeof(char):
                return _sql.GreaterThan(
                    _sql.Function("STRPOS", [instance, Stringify(arguments[0])], nullable: true,
                        argumentsPropagateNullability: [true, true], typeof(int)),
                    _sql.Constant(0));
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
            // Trim family with a char / char[] argument → the engine's
            // two-argument TRIM/LTRIM/RTRIM (second arg = character set).
            case nameof(string.Trim) or nameof(string.TrimStart) or nameof(string.TrimEnd):
            {
                var fn = method.Name switch
                {
                    nameof(string.TrimStart) => "LTRIM",
                    nameof(string.TrimEnd) => "RTRIM",
                    _ => "TRIM",
                };
                if (arguments.Count == 0)
                    return Fn(fn, instance);
                if (arguments is [SqlConstantExpression { Value: var v }])
                {
                    var set = v switch
                    {
                        char c => c.ToString(),
                        char[] { Length: > 0 } cs => new string(cs),
                        char[] or null => null, // empty/null array = whitespace
                        _ => "\0", // unreachable sentinel
                    };
                    return set is null
                        ? Fn(fn, instance)
                        : Fn(fn, instance, _sql.Constant(set, instance.TypeMapping));
                }
                return null;
            }
            case nameof(string.Replace) when arguments.Count == 2:
                return Fn("REPLACE", instance, arguments[0], arguments[1]);
            case nameof(string.Substring) when arguments.Count is 1 or 2:
            {
                var from = _sql.Add(arguments[0], _sql.Constant(1));
                return arguments.Count == 1
                    ? Fn("SUBSTRING", instance, from)
                    : Fn("SUBSTRING", instance, from, arguments[1]);
            }
            // CLR IndexOf is 0-based (-1 = absent); STRPOS is 1-based (0 = absent).
            case nameof(string.IndexOf) when arguments is [{ Type: var t }] && t == typeof(string):
                return _sql.Subtract(
                    _sql.Function("STRPOS", [instance, arguments[0]], nullable: true,
                        argumentsPropagateNullability: [true, true], typeof(int)),
                    _sql.Constant(1));
            case nameof(string.PadLeft) when arguments.Count is 1 or 2:
            case nameof(string.PadRight) when arguments.Count is 1 or 2:
            {
                var fn = method.Name == nameof(string.PadLeft) ? "LPAD" : "RPAD";
                // The char overload's fill argument becomes a one-char string.
                var args = arguments.Count == 1
                    ? new[] { instance, arguments[0] }
                    : [instance, arguments[0], Stringify(arguments[1])];
                return _sql.Function(fn, args, nullable: true,
                    argumentsPropagateNullability: args.Select(_ => true).ToArray(),
                    typeof(string), instance.TypeMapping);
            }
            default:
                return null;
        }

        SqlExpression Stringify(SqlExpression e) =>
            e is SqlConstantExpression { Value: char c }
                ? _sql.Constant(c.ToString(), instance!.TypeMapping)
                : e;

        SqlExpression Fn(string name, params SqlExpression[] args) =>
            _sql.Function(name, args, nullable: true,
                argumentsPropagateNullability: args.Select(_ => true).ToArray(),
                method.ReturnType, instance!.TypeMapping);
    }
}

/// <summary>
/// <c>Regex.IsMatch(input, pattern)</c> → the engine's REGEXP_LIKE (Rust
/// regex syntax; .NET syntax is compatible for the common subset). Only the
/// no-options overload translates.
/// </summary>
internal sealed class OxiDbRegexTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    public OxiDbRegexTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(System.Text.RegularExpressions.Regex)
            || method.Name != nameof(System.Text.RegularExpressions.Regex.IsMatch)
            || arguments.Count != 2)
            return null;
        return _sql.Function("regexp_like", arguments, nullable: true,
            argumentsPropagateNullability: [true, true], typeof(bool));
    }
}

/// <summary>
/// <c>str.FirstOrDefault()</c> / <c>str.LastOrDefault()</c> (Enumerable over
/// a string) → SUBSTRING of the first/last character.
/// </summary>
internal sealed class OxiDbStringEnumerableTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    public OxiDbStringEnumerableTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(Enumerable)
            || arguments.Count != 1
            || arguments[0].Type != typeof(string))
            return null;
        var s = arguments[0];
        var from = method.Name switch
        {
            nameof(Enumerable.FirstOrDefault) => (SqlExpression)_sql.Constant(1),
            nameof(Enumerable.LastOrDefault) => _sql.Function("LENGTH", [s], nullable: true,
                argumentsPropagateNullability: [true], typeof(int)),
            _ => null!,
        };
        if (from is null)
            return null;
        return _sql.Function("SUBSTRING", [s, from, _sql.Constant(1)], nullable: true,
            argumentsPropagateNullability: [true, true, true], method.ReturnType);
    }
}

/// <summary>
/// <c>x.ToString()</c> on integral types and <c>Guid</c> → <c>CAST(x AS
/// TEXT)</c>. Floating/decimal/DateTime are left untranslated: their CLR
/// string formats are culture-dependent and would not match the cast.
/// </summary>
internal sealed class OxiDbToStringTranslator : IMethodCallTranslator
{
    private static readonly HashSet<Type> Castable =
    [
        typeof(int), typeof(long), typeof(short), typeof(byte),
        typeof(uint), typeof(ulong), typeof(ushort), typeof(sbyte),
        typeof(Guid),
    ];

    private readonly ISqlExpressionFactory _sql;

    public OxiDbToStringTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.Name != nameof(ToString) || arguments.Count != 0 || instance is null)
            return null;
        if (instance.Type == typeof(string))
            return instance;
        return Castable.Contains(Nullable.GetUnderlyingType(instance.Type) ?? instance.Type)
            ? _sql.Convert(instance, typeof(string))
            : null;
    }
}

/// <summary>
/// <c>Math</c>/<c>MathF</c> → engine scalars: Abs/Floor/Ceiling/Round already
/// exist engine-side as ABS/FLOOR/CEILING/ROUND; Pow → POWER; Sqrt → SQRT.
/// </summary>
internal sealed class OxiDbMathMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<string, string> Names = new()
    {
        [nameof(Math.Abs)] = "ABS",
        [nameof(Math.Floor)] = "FLOOR",
        [nameof(Math.Ceiling)] = "CEILING",
        [nameof(Math.Round)] = "ROUND",
        [nameof(Math.Pow)] = "POWER",
        [nameof(Math.Sqrt)] = "SQRT",
        [nameof(Math.Sin)] = "sin",
        [nameof(Math.Cos)] = "cos",
        [nameof(Math.Tan)] = "tan",
        [nameof(Math.Asin)] = "asin",
        [nameof(Math.Acos)] = "acos",
        [nameof(Math.Atan)] = "atan",
        [nameof(Math.Atan2)] = "atan2",
        [nameof(Math.Exp)] = "exp",
        [nameof(Math.Log10)] = "log10",
        [nameof(Math.Truncate)] = "trunc",
        [nameof(Math.Sign)] = "sign",
        [nameof(Math.Min)] = "LEAST",
        [nameof(Math.Max)] = "GREATEST",
    };

    private readonly ISqlExpressionFactory _sql;

    public OxiDbMathMethodTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // double.RadiansToDegrees / DegreesToRadians (and float.*) are the
        // .NET 8+ shapes EF produces for degree/radian conversion.
        if ((method.DeclaringType == typeof(double) || method.DeclaringType == typeof(float))
            && arguments.Count == 1)
        {
            return method.Name switch
            {
                "RadiansToDegrees" => _sql.Function("degrees", arguments, nullable: true,
                    argumentsPropagateNullability: [true], method.ReturnType),
                "DegreesToRadians" => _sql.Function("radians", arguments, nullable: true,
                    argumentsPropagateNullability: [true], method.ReturnType),
                _ => null,
            };
        }
        if (method.DeclaringType != typeof(Math) && method.DeclaringType != typeof(MathF))
            return null;
        if (arguments.Count is not (1 or 2))
            return null;
        // Math.Log(x) is the natural log; Math.Log(x, base) → LOG(base, x)
        // (the engine's PostgreSQL argument order).
        if (method.Name == nameof(Math.Log))
        {
            return arguments.Count == 1
                ? Fn("ln", [arguments[0]])
                : Fn("log", [arguments[1], arguments[0]]);
        }
        if (!Names.TryGetValue(method.Name, out var fn))
            return null;
        return Fn(fn, arguments);

        SqlExpression Fn(string name, IReadOnlyList<SqlExpression> args) =>
            _sql.Function(name, args, nullable: true,
                argumentsPropagateNullability: args.Select(_ => true).ToArray(),
                method.ReturnType);
    }
}

/// <summary>
/// <c>DateTime.AddDays/AddHours/AddMinutes/AddSeconds/AddMilliseconds</c> →
/// timestamp + milliseconds (the engine folds INTERVALs to ms integers, and
/// <c>timestamp ± double</c> rounds to the nearest ms).
/// </summary>
internal sealed class OxiDbDateTimeMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<string, double> MsPerUnit = new()
    {
        [nameof(DateTime.AddDays)] = 86_400_000d,
        [nameof(DateTime.AddHours)] = 3_600_000d,
        [nameof(DateTime.AddMinutes)] = 60_000d,
        [nameof(DateTime.AddSeconds)] = 1_000d,
        [nameof(DateTime.AddMilliseconds)] = 1d,
    };

    private readonly ISqlExpressionFactory _sql;

    public OxiDbDateTimeMethodTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(DateTime) || instance is null || arguments.Count != 1)
            return null;
        // Calendar units go through the engine's ADD_MONTHS (day clamps to
        // the target month, like PostgreSQL `+ INTERVAL`).
        if (method.Name is nameof(DateTime.AddMonths) or nameof(DateTime.AddYears))
        {
            var months = method.Name == nameof(DateTime.AddYears)
                ? _sql.Multiply(arguments[0], _sql.Constant(12))
                : arguments[0];
            return _sql.Function("add_months",
                [instance, _sql.ApplyDefaultTypeMapping(months)], nullable: true,
                argumentsPropagateNullability: [true, true],
                method.ReturnType, instance.TypeMapping);
        }
        if (!MsPerUnit.TryGetValue(method.Name, out var factor))
            return null;
        // Pin the ms expression to its own (double) mapping so the timestamp
        // mapping of `instance` can't be inferred onto it.
        var ms = _sql.ApplyDefaultTypeMapping(_sql.Multiply(
            _sql.Convert(arguments[0], typeof(double)),
            _sql.Constant(factor)));
        return _sql.Add(instance, ms, instance.TypeMapping);
    }
}

/// <summary>
/// <c>DateTime</c> members → <c>date_part</c>/<c>date_trunc</c>, plus
/// <c>DateTime.Now/UtcNow</c> → <c>NOW()</c> (the engine clock is UTC).
/// </summary>
internal sealed class OxiDbDateTimeMemberTranslator : IMemberTranslator
{
    private static readonly Dictionary<string, string> Parts = new()
    {
        [nameof(DateTime.Year)] = "year",
        [nameof(DateTime.Month)] = "month",
        [nameof(DateTime.Day)] = "day",
        [nameof(DateTime.Hour)] = "hour",
        [nameof(DateTime.Minute)] = "minute",
        [nameof(DateTime.Second)] = "second",
        [nameof(DateTime.DayOfYear)] = "doy",
    };

    private readonly ISqlExpressionFactory _sql;

    public OxiDbDateTimeMemberTranslator(ISqlExpressionFactory sql) => _sql = sql;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (member.DeclaringType != typeof(DateTime))
            return null;
        if (instance is null)
        {
            // Static members. NOW() is UTC on the engine.
            return member.Name switch
            {
                nameof(DateTime.Now) or nameof(DateTime.UtcNow) => Now(),
                nameof(DateTime.Today) => _sql.Function("date_trunc",
                    [_sql.Constant("day"), Now()], nullable: false,
                    argumentsPropagateNullability: [false, false], returnType),
                _ => null,
            };

            SqlExpression Now() => _sql.Function("NOW", [], nullable: false,
                argumentsPropagateNullability: [], typeof(DateTime));
        }
        if (Parts.TryGetValue(member.Name, out var part))
            return DatePart(part);
        return member.Name switch
        {
            nameof(DateTime.Date) => _sql.Function("date_trunc",
                [_sql.Constant("day"), instance], nullable: true,
                argumentsPropagateNullability: [false, true],
                returnType, instance.TypeMapping),
            // Engine 'millisecond' includes the seconds field (PostgreSQL
            // semantics); CLR Millisecond is 0..999.
            nameof(DateTime.Millisecond) =>
                _sql.Modulo(DatePart("millisecond"), _sql.Constant(1000)),
            // Engine DOW numbering (Sunday = 0) matches System.DayOfWeek.
            nameof(DateTime.DayOfWeek) => _sql.Convert(
                _sql.Function("date_part", [_sql.Constant("dow"), instance],
                    nullable: true, argumentsPropagateNullability: [false, true],
                    typeof(int)),
                returnType),
            _ => null,
        };

        SqlExpression DatePart(string p) =>
            _sql.Function("date_part", [_sql.Constant(p), instance!], nullable: true,
                argumentsPropagateNullability: [false, true], typeof(int));
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
