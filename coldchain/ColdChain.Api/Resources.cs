using System.Diagnostics;

namespace ColdChain.Api;

/// <summary>
/// What the two processes actually cost.
/// </summary>
/// <remarks>
/// The engine already measures itself — <c>proc_status</c> on the wire returns
/// the same numbers its Prometheus endpoint serves — so this asks it rather
/// than guessing from outside. The API measures itself the same way, from its
/// own process, because a demo that claims "one small binary" should show the
/// number instead of asserting it.
/// </remarks>
public sealed record Usage(double CpuPercent, double MemoryMb, int Threads, long UptimeSeconds);

public static class SelfUsage
{
    private static readonly object Gate = new();
    private static TimeSpan _lastCpu;
    private static DateTime _lastAt;

    /// <summary>This process's CPU and resident memory.</summary>
    /// <remarks>
    /// CPU is a delta since the previous call, normalised by core count, so it
    /// reads on the same 0–100 scale a person expects rather than 800% on an
    /// 8-core box. The first call has nothing to subtract from and reports 0.
    /// </remarks>
    public static Usage Sample()
    {
        var p = Process.GetCurrentProcess();
        var now = DateTime.UtcNow;
        var cpu = p.TotalProcessorTime;

        double percent = 0;
        lock (Gate)
        {
            if (_lastAt != default)
            {
                var elapsed = (now - _lastAt).TotalMilliseconds;
                if (elapsed > 0)
                    percent = (cpu - _lastCpu).TotalMilliseconds
                              / (elapsed * Environment.ProcessorCount) * 100.0;
            }
            _lastCpu = cpu;
            _lastAt = now;
        }

        return new Usage(
            Math.Round(Math.Clamp(percent, 0, 100), 1),
            Math.Round(p.WorkingSet64 / 1024.0 / 1024.0, 1),
            p.Threads.Count,
            (long)(now - p.StartTime.ToUniversalTime()).TotalSeconds);
    }
}
