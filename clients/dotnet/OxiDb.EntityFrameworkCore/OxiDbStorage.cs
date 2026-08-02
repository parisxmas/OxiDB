using System.Data.Common;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using OxiDb.Data;

namespace OxiDb.EntityFrameworkCore;

/// <summary>Connection service: creates <see cref="OxiDbConnection"/>s.</summary>
public interface IOxiDbRelationalConnection : IRelationalConnection;

public sealed class OxiDbRelationalConnection : RelationalConnection, IOxiDbRelationalConnection
{
    public OxiDbRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies) { }

    protected override DbConnection CreateDbConnection() =>
        new OxiDbConnection(ConnectionString!);
}

/// <summary>Identifier quoting (`"name"`) and the `@` parameter prefix.</summary>
public sealed class OxiDbSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public OxiDbSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies) { }
}

/// <summary>
/// CLR ↔ OxiDB type mappings: INT (integral types, enums), DOUBLE
/// (double/float/decimal — documented lossy), TEXT (string, Guid, char),
/// BOOL, TIMESTAMP (DateTime/DateTimeOffset as epoch ms), BLOB (byte[]).
/// </summary>
public sealed class OxiDbTypeMappingSource : RelationalTypeMappingSource
{
    private readonly Dictionary<Type, RelationalTypeMapping> _clr;
    private readonly Dictionary<string, RelationalTypeMapping> _store;

    public OxiDbTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        var i64 = new LongTypeMapping("INT", System.Data.DbType.Int64);
        var i32 = new IntTypeMapping("INT", System.Data.DbType.Int32);
        var i16 = new ShortTypeMapping("INT", System.Data.DbType.Int16);
        var u8 = new ByteTypeMapping("INT", System.Data.DbType.Byte);
        var f64 = new DoubleTypeMapping("DOUBLE", System.Data.DbType.Double);
        var f32 = new FloatTypeMapping("DOUBLE", System.Data.DbType.Single);
        var dec = new DecimalTypeMapping("DOUBLE", System.Data.DbType.Double);
        var text = new StringTypeMapping("TEXT", System.Data.DbType.String);
        var boolean = new OxiDbBoolTypeMapping();
        var ts = new DateTimeTypeMapping("TIMESTAMP", System.Data.DbType.DateTime);
        var guid = new GuidTypeMapping("TEXT", System.Data.DbType.String);
        var blob = new ByteArrayTypeMapping("BLOB", System.Data.DbType.Binary);
        var ch = new CharTypeMapping("TEXT", System.Data.DbType.String);

        _clr = new()
        {
            [typeof(long)] = i64,
            [typeof(int)] = i32,
            [typeof(short)] = i16,
            [typeof(byte)] = u8,
            [typeof(double)] = f64,
            [typeof(float)] = f32,
            [typeof(decimal)] = dec,
            [typeof(string)] = text,
            [typeof(bool)] = boolean,
            [typeof(DateTime)] = ts,
            [typeof(Guid)] = guid,
            [typeof(byte[])] = blob,
            [typeof(char)] = ch,
        };
        _store = new(StringComparer.OrdinalIgnoreCase)
        {
            ["INT"] = i64,
            ["INTEGER"] = i64,
            ["BIGINT"] = i64,
            ["DOUBLE"] = f64,
            ["DECIMAL"] = dec,
            ["TEXT"] = text,
            ["VARCHAR"] = text,
            ["BOOL"] = boolean,
            ["BOOLEAN"] = boolean,
            ["TIMESTAMP"] = ts,
            ["DATETIME"] = ts,
            ["BLOB"] = blob,
        };
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clr = mappingInfo.ClrType;
        if (clr is not null && _clr.TryGetValue(clr, out var byClr))
            return byClr;
        var store = mappingInfo.StoreTypeNameBase ?? mappingInfo.StoreTypeName;
        if (store is not null && _store.TryGetValue(store, out var byStore))
            return byStore;
        return base.FindMapping(mappingInfo);
    }
}

/// <summary>
/// Bool literals must render as TRUE/FALSE — the default's 1/0 is not a
/// boolean to the engine (`WHERE CASE ... THEN 1 ELSE 0 END` would never
/// match).
/// </summary>
public sealed class OxiDbBoolTypeMapping : BoolTypeMapping
{
    public OxiDbBoolTypeMapping()
        : base("BOOL", System.Data.DbType.Boolean) { }

    private OxiDbBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters) { }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
        new OxiDbBoolTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value) =>
        (bool)value ? "TRUE" : "FALSE";
}

/// <summary>
/// Database lifecycle. The wire connection targets an already-existing
/// database (the server creates `oxidb` on startup; named databases are made
/// with `create_database`), so Create/Exists are cheap.
/// </summary>
public sealed class OxiDbDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IOxiDbRelationalConnection _connection;
    private readonly IRawSqlCommandBuilder _rawSql;

    public OxiDbDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IOxiDbRelationalConnection connection,
        IRawSqlCommandBuilder rawSql)
        : base(dependencies)
    {
        _connection = connection;
        _rawSql = rawSql;
    }

    public override bool Exists() => true;
    public override void Create() { }
    public override void Delete() =>
        throw new NotSupportedException("drop the database over the wire protocol");

    public override bool HasTables()
    {
        using var reader = _rawSql
            .Build("SHOW TABLES")
            .ExecuteReader(new RelationalCommandParameterObject(
                _connection, null, null, null, Dependencies.CommandLogger));
        return reader.DbDataReader.Read();
    }
}

/// <summary>Provider annotation names.</summary>
public static class OxiDbAnnotations
{
    public const string AutoIncrement = "OxiDb:AutoIncrement";
}

/// <summary>
/// Emits the <c>OxiDb:AutoIncrement</c> column annotation for store-generated
/// integer keys, so migrations carry the fact without needing the model.
/// </summary>
public sealed class OxiDbAnnotationProvider : RelationalAnnotationProvider
{
    public OxiDbAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
        : base(dependencies) { }

    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
    {
        foreach (var a in base.For(column, designTime))
            yield return a;
        var property = column.PropertyMappings.Select(m => m.Property).FirstOrDefault();
        if (property is { ValueGenerated: ValueGenerated.OnAdd }
            && property.IsPrimaryKey()
            && (property.ClrType == typeof(long) || property.ClrType == typeof(int)
                || property.ClrType == typeof(short)))
        {
            yield return new Annotation(OxiDbAnnotations.AutoIncrement, true);
        }
    }
}

/// <summary>Migrations history over a plain table (CRUD only).</summary>
public sealed class OxiDbHistoryRepository : HistoryRepository
{
    public OxiDbHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies) { }

    protected override string ExistsSql =>
        // Errors when absent; InterpretExistsResult never sees that case —
        // Exists() catches it below.
        $"SELECT COUNT(*) FROM {SqlGenerationHelper.DelimitIdentifier(TableName)}";

    protected override bool InterpretExistsResult(object? value) => true;

    public override bool Exists()
    {
        try
        {
            return base.Exists();
        }
        catch
        {
            return false; // table missing
        }
    }

    public override IMigrationsDatabaseLock AcquireDatabaseLock() => new NoopLock(this);

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default) =>
        Task.FromResult<IMigrationsDatabaseLock>(new NoopLock(this));

    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Transaction;

    public override string GetBeginIfExistsScript(string migrationId) =>
        throw new NotSupportedException("conditional migration scripts");

    public override string GetBeginIfNotExistsScript(string migrationId) =>
        throw new NotSupportedException("conditional migration scripts");

    public override string GetCreateIfNotExistsScript() =>
        GetCreateScript().Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

    public override string GetEndIfScript() =>
        throw new NotSupportedException("conditional migration scripts");

    private sealed class NoopLock(IHistoryRepository owner) : IMigrationsDatabaseLock
    {
        public IHistoryRepository HistoryRepository => owner;
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

/// <summary>
/// Standard relational DDL is already in the engine's dialect; additions are
/// <c>AUTO_INCREMENT</c> on store-generated integer keys and the
/// engine-dialect forms of the operations the relational base leaves to the
/// provider (RENAME COLUMN, DROP INDEX). ALTER COLUMN and table renames have
/// no engine DDL and fail with a clear message.
/// </summary>
public sealed class OxiDbMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public OxiDbMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        base.ColumnDefinition(schema, table, name, operation, model, builder);
        var property = model?.GetRelationalModel()
            .FindTable(table, schema)?
            .FindColumn(name)?
            .PropertyMappings.Select(m => m.Property)
            .FirstOrDefault();
        // Two sources: the target model (EF-generated migrations carry it in
        // the designer file) or an explicit column annotation (hand-written
        // migrations, and what OxiDbAnnotationProvider emits).
        if ((property is { ValueGenerated: ValueGenerated.OnAdd }
                && property.IsPrimaryKey()
                && (property.ClrType == typeof(long) || property.ClrType == typeof(int)
                    || property.ClrType == typeof(short)))
            || operation.FindAnnotation(OxiDbAnnotations.AutoIncrement)?.Value is true)
        {
            builder.Append(" AUTO_INCREMENT");
        }
    }

    // The engine rejects ALTER TABLE / DROP INDEX inside a transaction, so
    // those commands are emitted with suppressTransaction: the migrator
    // commits the surrounding transaction, runs them standalone, and resumes.

    protected override void Generate(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        base.Generate(operation, model, builder, terminate: false);
        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder, suppressTransaction: true);
        }
    }

    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        base.Generate(operation, model, builder, terminate: false);
        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder, suppressTransaction: true);
        }
    }

    protected override void Generate(
        RenameColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" RENAME COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder, suppressTransaction: true);
    }

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        // A UNIQUE index enables a constraint: the engine validates and seeds
        // it against committed state, which a buffered transaction does not
        // have — so, like the ALTER-class commands above, it runs standalone.
        base.Generate(operation, model, builder, terminate: false);
        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder, suppressTransaction: operation.IsUnique);
        }
    }

    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        // Engine indexes are database-global by name: DROP INDEX <name>.
        builder
            .Append("DROP INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));
        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder, suppressTransaction: true);
        }
    }

    protected override void Generate(
        RenameTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder) =>
        throw new NotSupportedException(
            "OxiDB has no table-rename DDL. Create the new table, copy the rows, drop the old one.");

    protected override void Generate(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder) =>
        throw new NotSupportedException(
            "OxiDB cannot alter a column in place. Add a new column, copy the values, drop the old one.");
}
