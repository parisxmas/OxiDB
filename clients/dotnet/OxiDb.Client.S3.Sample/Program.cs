// End-to-end smoke test for OxiDb.Client.S3.
//
// Usage:
//     dotnet run --project dotnet/OxiDb.Client.S3.Sample
//
// Override the endpoint / creds via environment if you don't want
// to hit the public demo:
//     OXIDB_S3_ENDPOINT=https://s3demo.baltavista.com
//     OXIDB_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
//     OXIDB_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

using System.Security.Cryptography;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using OxiDb.Client.S3;

var options = new OxiDbS3Options
{
    Endpoint  = Environment.GetEnvironmentVariable("OXIDB_S3_ENDPOINT")
                ?? "https://s3demo.baltavista.com",
    AccessKey = Environment.GetEnvironmentVariable("OXIDB_S3_ACCESS_KEY")
                ?? "AKIAIOSFODNN7EXAMPLE",
    SecretKey = Environment.GetEnvironmentVariable("OXIDB_S3_SECRET_KEY")
                ?? "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    Region    = "us-east-1",
};

Console.WriteLine($"endpoint = {options.Endpoint}");
using var s3 = OxiDbS3ClientFactory.Create(options);

const string bucket = "dotnet-smoke";
const string key    = "hello.txt";
var payload = Encoding.UTF8.GetBytes("Hello from OxiDb.Client.S3 — .NET 9 smoke test.\n");

// 1. ensure bucket exists (ignore conflict if it's there).
try
{
    await s3.PutBucketAsync(bucket);
    Console.WriteLine($"[mb]  created s3://{bucket}");
}
catch (Amazon.S3.AmazonS3Exception ex) when (ex.ErrorCode == "BucketAlreadyOwnedByYou")
{
    Console.WriteLine($"[mb]  exists  s3://{bucket}");
}

// 2. plaintext PUT (chunked encoding disabled per-request to keep
// the sample green against older OxiDB builds).
var put = new PutObjectRequest
{
    BucketName            = bucket,
    Key                   = key,
    InputStream           = new MemoryStream(payload),
    ContentType           = "text/plain",
    DisablePayloadSigning = true,
    UseChunkEncoding      = false,
};
var putResp = await s3.PutObjectAsync(put);
Console.WriteLine($"[put] etag={putResp.ETag}");

// 3. SSE-S3 PUT (server-managed key from OXIDB_S3_ENCRYPTION_KEY).
var sseKey = key + ".sse";
await s3.PutObjectAsync(new PutObjectRequest
{
    BucketName                 = bucket,
    Key                        = sseKey,
    InputStream                = new MemoryStream(payload),
    ContentType                = "text/plain",
    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256,
    DisablePayloadSigning      = true,
    UseChunkEncoding           = false,
});
Console.WriteLine($"[put] {sseKey} (SSE-S3)");

// 4. SSE-C PUT (client-supplied key — even the operator can't read).
var ssecKey      = key + ".ssec";
var customerKey  = RandomNumberGenerator.GetBytes(32);
var customerKeyB64 = Convert.ToBase64String(customerKey);
await s3.PutObjectAsync(new PutObjectRequest
{
    BucketName                              = bucket,
    Key                                     = ssecKey,
    InputStream                             = new MemoryStream(payload),
    ContentType                             = "text/plain",
    ServerSideEncryptionCustomerMethod      = ServerSideEncryptionCustomerMethod.AES256,
    ServerSideEncryptionCustomerProvidedKey = customerKeyB64,
    DisablePayloadSigning                   = true,
    UseChunkEncoding                        = false,
});
Console.WriteLine($"[put] {ssecKey} (SSE-C, key={Convert.ToHexString(customerKey).AsSpan(0, 16)}…)");

// 5. roundtrip GETs.
async Task<byte[]> GetAsync(string k, GetObjectRequest? extra = null)
{
    var req = extra ?? new GetObjectRequest();
    req.BucketName = bucket;
    req.Key = k;
    using var resp = await s3.GetObjectAsync(req);
    using var ms = new MemoryStream();
    await resp.ResponseStream.CopyToAsync(ms);
    return ms.ToArray();
}

var got1 = await GetAsync(key);
var got2 = await GetAsync(sseKey);
var got3 = await GetAsync(ssecKey, new GetObjectRequest
{
    ServerSideEncryptionCustomerMethod      = ServerSideEncryptionCustomerMethod.AES256,
    ServerSideEncryptionCustomerProvidedKey = customerKeyB64,
});

Console.WriteLine($"[get] {key,-20} {(got1.AsSpan().SequenceEqual(payload) ? "✓ match" : "✗ mismatch")}");
Console.WriteLine($"[get] {sseKey,-20} {(got2.AsSpan().SequenceEqual(payload) ? "✓ match" : "✗ mismatch")}");
Console.WriteLine($"[get] {ssecKey,-20} {(got3.AsSpan().SequenceEqual(payload) ? "✓ match" : "✗ mismatch")}");

// 6. listing.
var list = await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket });
Console.WriteLine($"[ls]  s3://{bucket} → {list.S3Objects.Count} object(s)");
foreach (var o in list.S3Objects)
{
    Console.WriteLine($"        {o.Key,-30} {o.Size,8}b  {o.ETag}");
}
