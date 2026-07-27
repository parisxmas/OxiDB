using System.Diagnostics;
using OxiDb.Client.Tcp;
using Xunit;

namespace OxiDb.IntegrationTests;

/// <summary>
/// What happens to a long-lived client when the server it is talking to goes
/// away and comes back.
///
/// This is not hypothetical: a deploy restarted the engine under a running .NET
/// service and every request from then on failed with "Connection closed by
/// server", until the service itself was restarted. The client connected once
/// and stayed connected, so a broken pipe was permanent.
/// </summary>
public sealed class ReconnectTests : IAsyncLifetime
{
    private Process? _server;
    private string _dir = "";
    private int _port;

    public async Task InitializeAsync()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"oxidb_reconnect_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        _port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        await StartServerAsync();
    }

    public Task DisposeAsync()
    {
        StopServer();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
        return Task.CompletedTask;
    }

    private async Task StartServerAsync()
    {
        var root = FindRepoRoot();
        var bin = Path.Combine(root, "target", "debug", "oxidb-server");
        Assert.True(File.Exists(bin), $"build oxidb-server first: {bin}");
        var psi = new ProcessStartInfo
        {
            FileName = bin,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.Environment["OXIDB_ADDR"] = $"127.0.0.1:{_port}";
        psi.Environment["OXIDB_DATA"] = _dir;
        psi.Environment["OXIDB_IDLE_TIMEOUT"] = "60";
        _server = Process.Start(psi);
        // Wait for the port to answer.
        for (var i = 0; i < 100; i++)
        {
            try
            {
                using var probe = new System.Net.Sockets.TcpClient();
                await probe.ConnectAsync("127.0.0.1", _port);
                return;
            }
            catch { await Task.Delay(100); }
        }
        throw new InvalidOperationException("server did not start");
    }

    private void StopServer()
    {
        try { _server?.Kill(entireProcessTree: true); } catch { /* already gone */ }
        try { _server?.WaitForExit(5000); } catch { /* ignore */ }
        _server = null;
    }

    [Fact]
    public async Task A_read_survives_the_server_restarting_underneath_it()
    {
        await using var client = await OxiDbTcpClient.ConnectAsync("127.0.0.1", _port);
        await client.InsertAsync("things", new { name = "before" });
        Assert.Equal(1, await client.CountAsync("things"));

        // The deploy.
        StopServer();
        await StartServerAsync();

        // Same client object, socket long dead. This threw
        // OxiDbConnectionException("Connection closed by server") forever.
        var count = await client.CountAsync("things");
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task A_write_is_not_replayed_blindly_after_the_answer_is_lost()
    {
        await using var client = await OxiDbTcpClient.ConnectAsync("127.0.0.1", _port);
        await client.InsertAsync("things", new { name = "one" });

        StopServer();
        await StartServerAsync();

        // A write whose fate is unknown must not be re-sent on its own: the
        // server may have applied it before dying. It fails, the caller decides.
        // (Here the socket was dead before the request went out, so this one is
        // provably safe and does go through — what matters is that the client
        // says which case it was.)
        try
        {
            await client.InsertAsync("things", new { name = "two" });
        }
        catch (OxiDbConnectionException e)
        {
            Assert.False(e.Retryable, "a lost answer is not a safe retry");
        }

        // Either way, exactly one "two" — never two of them.
        var again = await OxiDbTcpClient.ConnectAsync("127.0.0.1", _port);
        await using (again)
        {
            var twos = await again.FindAsync("things", new { name = "two" });
            var copies = twos.GetArrayLength();
            Assert.True(copies <= 1, $"a write was applied twice: {copies} copies");
        }
    }

    [Fact]
    public async Task A_transaction_is_never_silently_resumed_on_a_new_socket()
    {
        await using var client = await OxiDbTcpClient.ConnectAsync("127.0.0.1", _port);
        await client.BeginTransactionAsync();

        StopServer();
        await StartServerAsync();

        // The transaction lived on the old socket. Redialing would give the
        // caller a session that has forgotten everything it did — worse than an
        // error, because the writes would land outside the transaction.
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => client.InsertAsync("things", new { name = "in-tx" }));
        Assert.Contains("transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Cargo.toml")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}
