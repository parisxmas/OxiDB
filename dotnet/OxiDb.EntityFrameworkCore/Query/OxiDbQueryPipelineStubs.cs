using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace OxiDb.EntityFrameworkCore.Query;

/// <summary>
/// Stub factory implementations for EF Core query pipeline services.
/// OxiDbQueryCompiler bypasses the standard pipeline entirely,
/// but EF Core's DI requires these to be registered.
/// All translate methods return null (unsupported) since they are never called.
/// </summary>

public sealed class OxiDbQueryableMethodTranslatingExpressionVisitorFactory
    : IQueryableMethodTranslatingExpressionVisitorFactory
{
    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => throw new InvalidOperationException("OxiDB uses a custom query compiler; the standard query pipeline is not used.");
}

public sealed class OxiDbShapedQueryCompilingExpressionVisitorFactory
    : IShapedQueryCompilingExpressionVisitorFactory
{
    public ShapedQueryCompilingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => throw new InvalidOperationException("OxiDB uses a custom query compiler; the standard query pipeline is not used.");
}

public sealed class OxiDbQueryTranslationPreprocessorFactory
    : IQueryTranslationPreprocessorFactory
{
    private readonly QueryTranslationPreprocessorDependencies _dependencies;

    public OxiDbQueryTranslationPreprocessorFactory(QueryTranslationPreprocessorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public QueryTranslationPreprocessor Create(QueryCompilationContext queryCompilationContext)
        => new QueryTranslationPreprocessor(_dependencies, queryCompilationContext);
}

public sealed class OxiDbQueryTranslationPostprocessorFactory
    : IQueryTranslationPostprocessorFactory
{
    private readonly QueryTranslationPostprocessorDependencies _dependencies;

    public OxiDbQueryTranslationPostprocessorFactory(QueryTranslationPostprocessorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public QueryTranslationPostprocessor Create(QueryCompilationContext queryCompilationContext)
        => new QueryTranslationPostprocessor(_dependencies, queryCompilationContext);
}
