using System.Diagnostics;

namespace OxiDb.IntegrationTests;

/// <summary>
/// Starts a real oxidb-server process on a random port for integration testing.
/// Shared across all tests via ICollectionFixture for efficiency.
/// </summary>
public sealed class TestServerFixture : IAsyncLifetime
{
    private Process? _serverProcess;
    private string? _tempDir;

    public int Port { get; private set; }

    public async Task InitializeAsync()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"oxidb_dotnet_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        // Find a free port
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        Port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();

        // Find the server binary — try cargo build output
        var repoRoot = FindRepoRoot();
        var serverBin = Path.Combine(repoRoot, "target", "debug", "oxidb-server");

        if (!File.Exists(serverBin))
        {
            // Build it
            var build = Process.Start(new ProcessStartInfo
            {
                FileName = "cargo",
                Arguments = "build --package oxidb-server",
                WorkingDirectory = repoRoot,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            })!;
            await build.WaitForExitAsync();
            if (build.ExitCode != 0)
            {
                var stderr = await build.StandardError.ReadToEndAsync();
                throw new Exception($"cargo build failed: {stderr}");
            }
        }

        _serverProcess = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = serverBin,
                Environment =
                {
                    ["OXIDB_ADDR"] = $"127.0.0.1:{Port}",
                    ["OXIDB_DATA"] = _tempDir,
                    ["OXIDB_IDLE_TIMEOUT"] = "60",
                    ["OXIDB_POOL_SIZE"] = "4",
                },
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            }
        };
        _serverProcess.Start();

        // Wait for server to be ready (try connecting)
        for (int i = 0; i < 50; i++)
        {
            try
            {
                using var probe = new OxiDbClient("127.0.0.1", Port, timeoutMs: 2000);
                probe.Ping();
                return; // Server is ready
            }
            catch
            {
                await Task.Delay(100);
            }
        }

        throw new Exception("oxidb-server failed to start within 5 seconds");
    }

    public Task DisposeAsync()
    {
        if (_serverProcess is { HasExited: false })
        {
            _serverProcess.Kill();
            _serverProcess.WaitForExit(3000);
        }
        _serverProcess?.Dispose();

        if (_tempDir != null && Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, true); } catch { }
        }

        return Task.CompletedTask;
    }

    public OxiDbClient CreateClient() => new("127.0.0.1", Port);

    private static string FindRepoRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null)
        {
            if (File.Exists(Path.Combine(dir, "Cargo.toml")) &&
                Directory.Exists(Path.Combine(dir, "oxidb-server")))
                return dir;
            dir = Path.GetDirectoryName(dir);
        }
        // fallback
        return Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));
    }
}

[CollectionDefinition("OxiDb")]
public class OxiDbCollection : ICollectionFixture<TestServerFixture> { }
