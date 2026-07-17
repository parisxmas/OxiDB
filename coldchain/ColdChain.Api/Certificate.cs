using System.Text;

namespace ColdChain.Api;

/// <summary>
/// The paperwork, as prose.
/// </summary>
/// <remarks>
/// Deliberately written rather than serialised. A certificate is a document a
/// person signs and an auditor reads, and the reason it belongs in a blob store
/// and not a column is that its value is the text. Putting it in S3 also makes
/// it searchable for free: the engine full-text indexes objects on PUT without
/// being asked, so "which certificates mention Nordfresh" is answerable from
/// the same binary that holds the readings.
/// </remarks>
public static class Certificate
{
    public static string For(Shipment s)
    {
        var b = new StringBuilder();
        var breached = s.Excursions.Count > 0;

        b.AppendLine("CERTIFICATE OF CONFORMITY — COLD CHAIN CUSTODY");
        b.AppendLine($"Shipment reference : {s.Reference}");
        b.AppendLine($"Consignee          : {s.Customer?.Name ?? "unknown"}");
        b.AppendLine($"Probe              : {s.DeviceId}");
        b.AppendLine($"Contracted range   : {s.MinCelsius}°C to {s.MaxCelsius}°C");
        b.AppendLine($"Departed           : {s.DepartedUtc:u}");
        b.AppendLine($"Issued             : {DateTime.UtcNow:u}");
        b.AppendLine();

        if (!breached)
        {
            b.AppendLine("VERDICT: CONFORMING. Continuous monitoring recorded no excursion");
            b.AppendLine("outside the contracted range for the duration of this consignment.");
            b.AppendLine("The load is released for distribution without qualification.");
        }
        else
        {
            var worst = s.Excursions.OrderByDescending(e => Math.Abs(e.Celsius - e.LimitCelsius)).First();
            b.AppendLine($"VERDICT: BREACHED. {s.Excursions.Count} excursions were recorded outside");
            b.AppendLine($"the contracted range. The worst reading was {worst.Celsius:0.0}°C against a");
            b.AppendLine($"limit of {worst.LimitCelsius:0.0}°C, at {worst.AtUtc:u}.");
            b.AppendLine();
            b.AppendLine("The consignee is notified under the terms of the custody agreement.");
            b.AppendLine($"Contractual penalty per excursion: {s.Customer?.PenaltyPerBreach ?? 0}.");
            b.AppendLine("This load must not be distributed before quality review.");
        }

        b.AppendLine();
        b.AppendLine("Issued by ColdChain Custody Services. Retain for seven years.");
        b.AppendLine("This document is stored immutably and is recoverable to any point in time.");
        return b.ToString();
    }
}
