using Amazon.S3;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace OxiDb.Client.S3;

/// <summary>
/// DI helpers so ASP.NET Core / generic-host apps can register an
/// <see cref="IAmazonS3"/> against an OxiDB endpoint with a single
/// call.
/// </summary>
public static class OxiDbS3ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="IAmazonS3"/> configured for
    /// the OxiDB endpoint described by <paramref name="configure"/>.
    /// </summary>
    public static IServiceCollection AddOxiDbS3(
        this IServiceCollection services,
        Action<OxiDbS3Options> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        services.TryAddSingleton<IAmazonS3>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<OxiDbS3Options>>().Value;
            return OxiDbS3ClientFactory.Create(options);
        });
        return services;
    }

    /// <summary>
    /// Same as the action overload but binds straight from a
    /// configuration section, e.g.
    /// <c>builder.Services.AddOxiDbS3(builder.Configuration.GetSection("OxiDbS3"))</c>.
    /// </summary>
    public static IServiceCollection AddOxiDbS3(
        this IServiceCollection services,
        IConfiguration configurationSection)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurationSection);
        services.Configure<OxiDbS3Options>(configurationSection);
        services.TryAddSingleton<IAmazonS3>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<OxiDbS3Options>>().Value;
            return OxiDbS3ClientFactory.Create(options);
        });
        return services;
    }
}
