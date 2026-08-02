using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.EntityFrameworkCore;

namespace ColdChain.Api;

/// <summary>
/// Files a certificate for every shipment, and re-files them as the journeys
/// accumulate excursions.
/// </summary>
/// <remarks>
/// A certificate is a point-in-time document, so re-issuing one is a real act
/// rather than a cache refresh — which is why each is written whole rather than
/// patched. The demo needs them to exist for the paperwork panel to have any
/// paperwork in it; a real deployment would issue one when the load is signed
/// for.
/// </remarks>
public sealed class Paperwork(IServiceProvider services, IAmazonS3 s3, ILogger<Paperwork> log)
    : BackgroundService
{
    public const string Bucket = "coldchain-certificates";

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // Let the seed create the tables first.
        await Task.Delay(TimeSpan.FromSeconds(12), ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                try { await s3.PutBucketAsync(Bucket, ct); } catch (AmazonS3Exception) { /* exists */ }

                using var scope = services.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<ColdChainDb>();
                var shipments = await db.Shipments
                    .Include(s => s.Customer).Include(s => s.Excursions)
                    .ToListAsync(ct);

                foreach (var s in shipments)
                {
                    await s3.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = Bucket,
                        Key = $"{s.Reference}/certificate.txt",
                        ContentBody = Certificate.For(s),
                        ContentType = "text/plain",
                    }, ct);
                }
                log.LogInformation("filed {n} certificates", shipments.Count);
            }
            catch (Exception e) when (!ct.IsCancellationRequested)
            {
                log.LogWarning("filing certificates failed: {m}", e.Message);
            }

            await Task.Delay(TimeSpan.FromMinutes(10), ct);
        }
    }
}
