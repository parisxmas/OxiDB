using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace OxiDb.EntityFrameworkCore.Infrastructure;

public sealed class OxiDbOptionsExtension : IDbContextOptionsExtension
{
    public string Host { get; }
    public int Port { get; }
    public string? DataPath { get; }
    public string? EncryptionKeyPath { get; }
    public string? Username { get; }
    public string? Password { get; }

    public bool IsEmbedded => DataPath != null;
    public bool HasCredentials => Username != null && Password != null;

    /// <summary>TCP mode constructor.</summary>
    public OxiDbOptionsExtension(string host, int port, string? username = null, string? password = null)
    {
        Host = host;
        Port = port;
        Username = username;
        Password = password;
    }

    /// <summary>Embedded mode constructor.</summary>
    public OxiDbOptionsExtension(string dataPath, string? encryptionKeyPath)
    {
        Host = "";
        Port = 0;
        DataPath = dataPath;
        EncryptionKeyPath = encryptionKeyPath;
    }

    public DbContextOptionsExtensionInfo Info => new OxiDbOptionsExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
        services.AddOxiDb();
    }

    public void Validate(IDbContextOptions options) { }

    private sealed class OxiDbOptionsExtensionInfo : DbContextOptionsExtensionInfo
    {
        private readonly OxiDbOptionsExtension _extension;

        public OxiDbOptionsExtensionInfo(OxiDbOptionsExtension extension) : base(extension)
        {
            _extension = extension;
        }

        public override bool IsDatabaseProvider => true;

        public override string LogFragment => _extension.IsEmbedded
            ? $"OxiDb(embedded:{_extension.DataPath})"
            : _extension.HasCredentials
                ? $"OxiDb({_extension.Username}@{_extension.Host}:{_extension.Port})"
                : $"OxiDb({_extension.Host}:{_extension.Port})";

        public override int GetServiceProviderHashCode()
            => _extension.IsEmbedded
                ? HashCode.Combine("embedded", _extension.DataPath, _extension.EncryptionKeyPath)
                : HashCode.Combine(_extension.Host, _extension.Port, _extension.Username);

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is OxiDbOptionsExtensionInfo otherInfo
               && otherInfo._extension.IsEmbedded == _extension.IsEmbedded
               && otherInfo._extension.Host == _extension.Host
               && otherInfo._extension.Port == _extension.Port
               && otherInfo._extension.DataPath == _extension.DataPath
               && otherInfo._extension.EncryptionKeyPath == _extension.EncryptionKeyPath
               && otherInfo._extension.Username == _extension.Username;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            if (_extension.IsEmbedded)
            {
                debugInfo["OxiDb:DataPath"] = _extension.DataPath!;
                if (_extension.EncryptionKeyPath != null)
                    debugInfo["OxiDb:EncryptionKeyPath"] = _extension.EncryptionKeyPath;
            }
            else
            {
                debugInfo["OxiDb:Host"] = _extension.Host;
                debugInfo["OxiDb:Port"] = _extension.Port.ToString();
                if (_extension.Username != null)
                    debugInfo["OxiDb:Username"] = _extension.Username;
            }
        }
    }
}
