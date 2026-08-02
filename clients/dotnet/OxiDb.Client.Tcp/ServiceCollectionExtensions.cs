using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace OxiDb.Client.Tcp;

/// <summary>
/// Configuration knobs for the OxiDB TCP client registered via
/// <see cref="ServiceCollectionExtensions.AddOxiDbTcp(IServiceCollection, Action{OxiDbTcpClientOptions}?)"/>.
/// </summary>
public sealed class OxiDbTcpClientOptions
{
    /// <summary>Server host. Defaults to <c>127.0.0.1</c>.</summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>Server port. Defaults to <c>4444</c>.</summary>
    public int Port { get; set; } = 4444;

    /// <summary>Use the OxiWire binary wire format. Default true.</summary>
    public bool UseOxiWire { get; set; } = true;

    /// <summary>Optional SCRAM-SHA-256 username — if set together with
    /// <see cref="Password"/>, the client authenticates on connect.</summary>
    public string? Username { get; set; }

    /// <summary>Optional SCRAM-SHA-256 password.</summary>
    public string? Password { get; set; }
}

/// <summary>
/// Microsoft.Extensions.DependencyInjection integration. Lets consumers
/// register an <see cref="IOxiDbClient"/> in their container with a
/// single call. The client is registered as a singleton — internal
/// locking serialises requests, so a single instance shared across
/// the app is the intended pattern.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register an <see cref="IOxiDbClient"/> backed by a TCP connection
    /// to an OxiDB server. The client is added as a singleton and
    /// connects lazily on first use.
    /// </summary>
    /// <example>
    /// <code>
    /// services.AddOxiDbTcp(opts =>
    /// {
    ///     opts.Host = "oxidb.internal";
    ///     opts.Port = 4444;
    ///     opts.Username = "app";
    ///     opts.Password = config["OXIDB_PASSWORD"];
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddOxiDbTcp(
        this IServiceCollection services,
        Action<OxiDbTcpClientOptions>? configure = null)
    {
        var options = new OxiDbTcpClientOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<IOxiDbClient>(_ =>
        {
            var client = OxiDbTcpClient.ConnectAsync(options.Host, options.Port)
                .GetAwaiter()
                .GetResult();

            if (options.UseOxiWire) client.UseOxiWire();

            if (options.Username is not null && options.Password is not null)
            {
                client.AuthSimpleAsync(options.Username, options.Password)
                    .GetAwaiter()
                    .GetResult();
            }

            return client;
        });

        return services;
    }
}
