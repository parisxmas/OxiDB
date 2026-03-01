using Microsoft.EntityFrameworkCore.Storage;

namespace OxiDb.EntityFrameworkCore.Storage;

/// <summary>
/// Type mapping source for OxiDB. Since OxiDB stores everything as JSON natively,
/// we accept all common .NET primitive types.
/// </summary>
public sealed class OxiDbTypeMappingSource : TypeMappingSource
{
    private static readonly HashSet<Type> SupportedTypes =
    [
        typeof(bool),
        typeof(byte), typeof(sbyte),
        typeof(short), typeof(ushort),
        typeof(int), typeof(uint),
        typeof(long), typeof(ulong),
        typeof(float), typeof(double), typeof(decimal),
        typeof(string),
        typeof(char),
        typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly), typeof(TimeOnly), typeof(TimeSpan),
        typeof(Guid),
        typeof(byte[]),
        typeof(List<string>),
    ];

    public OxiDbTypeMappingSource(TypeMappingSourceDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override CoreTypeMapping? FindMapping(in TypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        if (clrType == null)
            return null;

        var underlying = Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (SupportedTypes.Contains(underlying) || underlying.IsEnum)
        {
            return new OxiDbTypeMapping(clrType);
        }

        return null;
    }
}

internal sealed class OxiDbTypeMapping : CoreTypeMapping
{
    public OxiDbTypeMapping(Type clrType)
        : base(new CoreTypeMappingParameters(clrType))
    {
    }

    private OxiDbTypeMapping(CoreTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override CoreTypeMapping Clone(CoreTypeMappingParameters parameters)
        => new OxiDbTypeMapping(parameters);

    public override CoreTypeMapping WithComposedConverter(
        Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter? converter,
        Microsoft.EntityFrameworkCore.ChangeTracking.ValueComparer? comparer,
        Microsoft.EntityFrameworkCore.ChangeTracking.ValueComparer? keyComparer,
        CoreTypeMapping? elementMapping,
        Microsoft.EntityFrameworkCore.Storage.Json.JsonValueReaderWriter? jsonValueReaderWriter)
        => new OxiDbTypeMapping(Parameters.WithComposedConverter(converter, comparer, keyComparer, elementMapping, jsonValueReaderWriter));
}
