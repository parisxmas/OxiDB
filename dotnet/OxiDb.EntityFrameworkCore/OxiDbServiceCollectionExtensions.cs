using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace OxiDb.EntityFrameworkCore;

public static class OxiDbServiceCollectionExtensions
{
    /// <summary>Register the OxiDB EF Core provider's services.</summary>
    public static IServiceCollection AddEntityFrameworkOxiDb(this IServiceCollection services)
    {
        new EntityFrameworkRelationalServicesBuilder(services)
            .TryAdd<LoggingDefinitions, OxiDbLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<OxiDbOptionsExtension>>()
            .TryAdd<IProviderConventionSetBuilder, OxiDbConventionSetBuilder>()
            .TryAdd<ISqlGenerationHelper, OxiDbSqlGenerationHelper>()
            .TryAdd<IRelationalTypeMappingSource, OxiDbTypeMappingSource>()
            .TryAdd<IModificationCommandBatchFactory, OxiDbModificationCommandBatchFactory>()
            .TryAdd<IUpdateSqlGenerator, OxiDbUpdateSqlGenerator>()
            .TryAdd<IQuerySqlGeneratorFactory, OxiDbQuerySqlGeneratorFactory>()
            .TryAdd<IMethodCallTranslatorProvider, OxiDbMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, OxiDbMemberTranslatorProvider>()
            .TryAdd<IRelationalDatabaseCreator, OxiDbDatabaseCreator>()
            .TryAdd<IHistoryRepository, OxiDbHistoryRepository>()
            .TryAdd<IMigrationsSqlGenerator, OxiDbMigrationsSqlGenerator>()
            .TryAdd<IRelationalConnection>(p => p.GetRequiredService<IOxiDbRelationalConnection>())
            .TryAddProviderSpecificServices(b =>
                b.TryAddScoped<IOxiDbRelationalConnection, OxiDbRelationalConnection>())
            .TryAddCoreServices();
        return services;
    }
}

public sealed class OxiDbLoggingDefinitions : RelationalLoggingDefinitions;

/// <summary>Relational model conventions (table/column mapping et al.).</summary>
public sealed class OxiDbConventionSetBuilder : RelationalConventionSetBuilder
{
    public OxiDbConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies) { }
}
