using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.Extensions.DependencyInjection;
using OxiDb.EntityFrameworkCore.Diagnostics.Internal;
using OxiDb.EntityFrameworkCore.Infrastructure;
using OxiDb.EntityFrameworkCore.Query;
using OxiDb.EntityFrameworkCore.Storage;
using OxiDb.EntityFrameworkCore.ValueGeneration;

namespace OxiDb.EntityFrameworkCore;

public static class OxiDbServiceCollectionExtensions
{
    public static IServiceCollection AddOxiDb(this IServiceCollection services)
    {
        new EntityFrameworkServicesBuilder(services)
            .TryAdd<IDatabaseProvider, OxiDbDatabaseProvider>()
            .TryAdd<ITypeMappingSource, OxiDbTypeMappingSource>()
            .TryAdd<IValueGeneratorSelector, OxiDbValueGeneratorSelector>()
            .TryAdd<IDatabase, OxiDbDatabase>()
            .TryAdd<IDatabaseCreator, OxiDbDatabaseCreator>()
            .TryAdd<IQueryCompiler, OxiDbQueryCompiler>()
            .TryAdd<IQueryContextFactory, OxiDbQueryContextFactory>()
            .TryAdd<IDbContextTransactionManager, OxiDbTransactionManager>()
            .TryAdd<LoggingDefinitions, OxiDbLoggingDefinitions>()
            .TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory, OxiDbQueryableMethodTranslatingExpressionVisitorFactory>()
            .TryAdd<IShapedQueryCompilingExpressionVisitorFactory, OxiDbShapedQueryCompilingExpressionVisitorFactory>()
            .TryAdd<IQueryTranslationPreprocessorFactory, OxiDbQueryTranslationPreprocessorFactory>()
            .TryAdd<IQueryTranslationPostprocessorFactory, OxiDbQueryTranslationPostprocessorFactory>()
            .TryAddCoreServices();

        services.AddScoped<OxiDbClientManager>();

        return services;
    }
}
