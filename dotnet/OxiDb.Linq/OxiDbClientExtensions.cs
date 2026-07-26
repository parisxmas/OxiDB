using OxiDb.Client.Tcp;

namespace OxiDb.Linq;

/// <summary>
/// Convenience entry points for getting a typed <see cref="OxiCollection{T}"/>
/// off any <see cref="IOxiDbClient"/>.
/// </summary>
public static class OxiDbClientExtensions
{
    /// <summary>
    /// Get a LINQ-aware view over a collection. The returned object can be
    /// used directly with the standard LINQ operators (<c>Where</c>,
    /// <c>OrderBy</c>, <c>Select</c>, ...) plus the async terminators on
    /// <see cref="OxiQueryAsyncExtensions"/>.
    /// </summary>
    public static OxiCollection<T> GetCollection<T>(this IOxiDbClient client, string name)
        => new(client, name);
}
