using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Update;

namespace OxiDb.EntityFrameworkCore;

/// <summary>
/// SELECT generation. The relational base already speaks the engine's
/// dialect for joins/predicates/aggregates; LIMIT/OFFSET is emitted in the
/// engine's (PostgreSQL-style) form.
/// </summary>
public sealed class OxiDbQuerySqlGenerator : QuerySqlGenerator
{
    public OxiDbQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit is not null)
        {
            Sql.AppendLine().Append("LIMIT ");
            Visit(selectExpression.Limit);
        }
        if (selectExpression.Offset is not null)
        {
            if (selectExpression.Limit is null)
            {
                // The engine requires LIMIT before OFFSET.
                Sql.AppendLine().Append("LIMIT ").Append(long.MaxValue.ToString());
            }
            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
    }

    // EF's correlated-collection shapes arrive as CROSS/OUTER APPLY (SQL
    // Server syntax); the engine speaks the PostgreSQL form of the same
    // operator: [LEFT] JOIN LATERAL ... ON TRUE.
    protected override System.Linq.Expressions.Expression VisitCrossApply(
        CrossApplyExpression crossApplyExpression)
    {
        Sql.Append("JOIN LATERAL ");
        Visit(crossApplyExpression.Table);
        Sql.Append(" ON TRUE");
        return crossApplyExpression;
    }

    protected override System.Linq.Expressions.Expression VisitOuterApply(
        OuterApplyExpression outerApplyExpression)
    {
        Sql.Append("LEFT JOIN LATERAL ");
        Visit(outerApplyExpression.Table);
        Sql.Append(" ON TRUE");
        return outerApplyExpression;
    }

    // The engine's parser wants the collation name delimited:
    // `expr COLLATE "NOCASE"` (the base emits it bare).
    protected override System.Linq.Expressions.Expression VisitCollate(
        CollateExpression collateExpression)
    {
        Visit(collateExpression.Operand);
        Sql.Append(" COLLATE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(collateExpression.Collation));
        return collateExpression;
    }
}

public sealed class OxiDbQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    public OxiDbQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies) =>
        _dependencies = dependencies;

    public QuerySqlGenerator Create() => new OxiDbQuerySqlGenerator(_dependencies);
}

/// <summary>
/// INSERT/UPDATE/DELETE generation. The relational base reads generated keys
/// back with a `RETURNING` clause — exactly what the engine implements.
/// </summary>
public sealed class OxiDbUpdateSqlGenerator : UpdateSqlGenerator
{
    public OxiDbUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies) { }
}

/// <summary>
/// Statement batching: many modification commands ride one wire round-trip.
/// The statements are joined with the statement terminator and sent as a
/// single multi-statement request; the engine executes each against the same
/// batch-level <c>$N</c> parameter array and returns one result per
/// statement, which <see cref="OxiDb.Data.OxiDbDataReader"/> exposes as
/// consecutive result sets — exactly the shape
/// <see cref="AffectedCountModificationCommandBatch"/> consumes (RETURNING
/// rows for generated keys and affected-count checks; plain-INSERT results
/// carry no result set and are skipped).
/// </summary>
public sealed class OxiDbModificationCommandBatch : AffectedCountModificationCommandBatch
{
    /// <summary>
    /// Default statement cap per batch. Keeps the request comfortably under
    /// the wire's 16 MiB frame for ordinary rows; tune per context with
    /// <c>UseOxiDb(cs, o => o.MaxBatchSize(n))</c>.
    /// </summary>
    public const int DefaultMaxBatchSize = 500;

    public OxiDbModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies,
        int? maxBatchSize)
        : base(dependencies, maxBatchSize ?? DefaultMaxBatchSize) { }
}

public sealed class OxiDbModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;
    private readonly IDbContextOptions _options;

    public OxiDbModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies,
        IDbContextOptions options)
    {
        _dependencies = dependencies;
        _options = options;
    }

    public ModificationCommandBatch Create() =>
        new OxiDbModificationCommandBatch(
            _dependencies,
            _options.Extensions.OfType<OxiDbOptionsExtension>().FirstOrDefault()?.MaxBatchSize);
}
