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

/// <summary>One modification command per batch (no batching in v1).</summary>
public sealed class OxiDbModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    public OxiDbModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies) =>
        _dependencies = dependencies;

    public ModificationCommandBatch Create() =>
        new SingularModificationCommandBatch(_dependencies);
}
