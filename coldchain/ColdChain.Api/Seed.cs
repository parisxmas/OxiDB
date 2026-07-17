using ColdChain;
using Microsoft.EntityFrameworkCore;
using OxiDb.Client.Tcp;
using OxiDb.EntityFrameworkCore;

namespace ColdChain.Api;

public static class Seed
{
    public static async Task RunAsync()
    {
        // The SQL engine is per-database; create ours before EF touches it.
        await using (var boot = await OxiDbTcpClient.ConnectAsync(Endpoints.Host, Endpoints.Tcp))
        {
            await boot.SqlAsync("CREATE DATABASE IF NOT EXISTS coldchain");
        }

        var opts = new DbContextOptionsBuilder<ColdChainDb>()
            .UseOxiDb(Endpoints.SqlConnectionString).Options;
        await using var db = new ColdChainDb(opts);
        await db.Database.EnsureCreatedAsync();

        if (await db.Customers.AnyAsync()) { Console.WriteLine("seed: already seeded"); return; }

        var pharma = new Customer { Name = "Meridian Pharma", PenaltyPerBreach = 2500m };
        var grocer = new Customer { Name = "Nordfresh Foods", PenaltyPerBreach = 400m };
        db.Customers.AddRange(pharma, grocer);

        var t0 = DateTime.UtcNow.AddHours(-2);
        db.Shipments.AddRange(
            // Vaccines: 2–8°C, the classic cold-chain contract.
            new Shipment { Reference = "SHP-1001", Customer = pharma, DeviceId = "probe-01", MinCelsius = 2, MaxCelsius = 8, DepartedUtc = t0 },
            new Shipment { Reference = "SHP-1002", Customer = pharma, DeviceId = "probe-02", MinCelsius = 2, MaxCelsius = 8, DepartedUtc = t0 },
            // Frozen: -20..-15
            new Shipment { Reference = "SHP-1003", Customer = grocer, DeviceId = "probe-03", MinCelsius = -20, MaxCelsius = -15, DepartedUtc = t0 },
            new Shipment { Reference = "SHP-1004", Customer = grocer, DeviceId = "probe-04", MinCelsius = -20, MaxCelsius = -15, DepartedUtc = t0 },
            new Shipment { Reference = "SHP-1005", Customer = grocer, DeviceId = "probe-05", MinCelsius = 2, MaxCelsius = 8, DepartedUtc = t0 },
            new Shipment { Reference = "SHP-1006", Customer = pharma, DeviceId = "probe-06", MinCelsius = 2, MaxCelsius = 8, DepartedUtc = t0 }
        );
        await db.SaveChangesAsync();
        Console.WriteLine($"seed: {await db.Customers.CountAsync()} customers, {await db.Shipments.CountAsync()} shipments");
    }
}
