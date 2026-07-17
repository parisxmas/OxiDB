using OxiDb.Client.Tcp;

namespace ColdChain.Api;

/// Keeps the demo from growing without bound — and does it the way this
/// industry actually does.
///
/// Raw readings arrive every two seconds per probe. Kept forever they are
/// ~32 MB/day and answer a question nobody asks: nobody wants the reading from
/// 09:41:22 two years ago. They want to know the load stayed in range.
///
/// So: a one-minute rollup (mean/min/max) is materialised continuously and kept
/// FOREVER — it is ~30x smaller and it still proves the case, because `max`
/// over a minute cannot hide a breach. The raw stream is then dropped after 30
/// days. That is not a compromise to save disk; it is the retention rule the
/// regulation implies.
public sealed class Retention(ILogger<Retention> log) : BackgroundService
{
    const int KeepRawDays = 30;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // Give the engine a moment; this races container startup otherwise.
        await Task.Delay(TimeSpan.FromSeconds(10), ct);

        try
        {
            await using var c = await OxiDbTcpClient.ConnectAsync(Endpoints.Host, Endpoints.Tcp, ct: ct);
            // min/max matter as much as mean: an average hides a spike, and a
            // spike is the whole point.
            await c.TsdbAddRollupAsync("temperature", TimeSpan.FromMinutes(1),
                [TsdbAgg.Mean, TsdbAgg.Min, TsdbAgg.Max], label: "1m", ct: ct);
            log.LogInformation("rollup temperature@1m registered (kept forever)");
        }
        catch (Exception e) { log.LogWarning("rollup registration failed: {m}", e.Message); }

        var lastRetention = DateTime.MinValue;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var c = await OxiDbTcpClient.ConnectAsync(Endpoints.Host, Endpoints.Tcp, ct: ct);

                // Roll up completed buckets. Only closed minutes are folded, so
                // this is safe to run as often as we like.
                await c.TsdbRefreshRollupsAsync(ct: ct);

                // Drop raw points the rollup has long since captured.
                if (DateTime.UtcNow - lastRetention > TimeSpan.FromHours(6))
                {
                    var r = await c.TsdbEnforceRetentionAsync(DateTime.UtcNow.AddDays(-KeepRawDays), ct);
                    lastRetention = DateTime.UtcNow;
                    log.LogInformation("retention: raw older than {d}d dropped {r} blocks", KeepRawDays, r);
                }
            }
            catch (Exception e) when (!ct.IsCancellationRequested)
            {
                log.LogWarning("retention pass failed: {m}", e.Message);
            }
            await Task.Delay(TimeSpan.FromMinutes(1), ct);
        }
    }
}
