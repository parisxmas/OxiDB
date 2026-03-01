using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.ValueGeneration;

namespace OxiDb.EntityFrameworkCore.ValueGeneration;

/// <summary>
/// Generates GUID string keys for new entities when no key value is provided.
/// </summary>
public sealed class OxiDbValueGeneratorSelector : ValueGeneratorSelector
{
    public OxiDbValueGeneratorSelector(ValueGeneratorSelectorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override ValueGenerator? FindForType(IProperty property, ITypeBase typeBase, Type clrType)
    {
        if (property.IsPrimaryKey() && property.ClrType == typeof(string))
            return new OxiDbStringKeyGenerator();

        return base.FindForType(property, typeBase, clrType);
    }
}

internal sealed class OxiDbStringKeyGenerator : ValueGenerator<string>
{
    public override bool GeneratesTemporaryValues => false;

    public override string Next(EntityEntry entry) => Guid.NewGuid().ToString("N");
}
