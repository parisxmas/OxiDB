using System.Text.Json.Serialization;

namespace OxiDb.Client.Tcp;

/// <summary>
/// Strongly-typed view of the server's response to a <c>hello</c> command.
/// Returned by <see cref="OxiDbClientExtensions.HelloAsync(IOxiDbClient, string?, IReadOnlyList{uint}?, System.Threading.CancellationToken)"/>.
/// </summary>
/// <remarks>
/// HELLO is the OxiWire wire-protocol handshake landed in OxiDB v0.28.13
/// (ADR-0003 Phase 2). It's pre-auth, idempotent, and free — call it once
/// at connection time to discover what the server supports.
/// </remarks>
public sealed record HelloResponse(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("version")] string Version,
    [property: JsonPropertyName("wire_version")] uint WireVersion,
    [property: JsonPropertyName("supported_wire_versions")] IReadOnlyList<uint> SupportedWireVersions,
    [property: JsonPropertyName("stable_surface_version")] string StableSurfaceVersion,
    [property: JsonPropertyName("features")] IReadOnlyList<string> Features,
    [property: JsonPropertyName("experimental_features")] IReadOnlyList<string> ExperimentalFeatures,
    [property: JsonPropertyName("auth_methods")] IReadOnlyList<string> AuthMethods)
{
    /// <summary>
    /// Returns true if the server advertises <paramref name="feature"/> in
    /// either its stable feature set or its experimental feature set.
    /// Case-sensitive.
    /// </summary>
    public bool HasFeature(string feature)
        => Features.Contains(feature) || ExperimentalFeatures.Contains(feature);

    /// <summary>
    /// Returns true if the server advertises <paramref name="feature"/> in
    /// its 1.0-stable feature set (i.e. covered by the semver promise).
    /// </summary>
    public bool HasStableFeature(string feature) => Features.Contains(feature);

    /// <summary>
    /// Returns true if the server supports the given OxiWire wire version.
    /// </summary>
    public bool SupportsWireVersion(uint version) => SupportedWireVersions.Contains(version);
}
