# OxiDb.Client.S3

S3-compatible client helpers for OxiDB. A thin layer over
`AWSSDK.S3` that wires up the right defaults for an OxiDB endpoint:

- **path-style addressing** (OxiDB does not do virtual-hosted-style)
- **SigV4** signing (the only flavour OxiDB validates)
- **region pinning** so SigV4 stays in sync with the server
- optional **chunked-encoding bypass** for the older
  `≤ 0.25.17` server bug that wrote raw chunk frames as object bytes

The library returns a regular `IAmazonS3`, so anything you'd do
with the AWS SDK still works.

## Targets

`net9.0` and `net10.0` (multi-target).

## Usage

### Console / one-off

```csharp
using OxiDb.Client.S3;
using Amazon.S3.Model;

var s3 = OxiDbS3ClientFactory.Create(new OxiDbS3Options
{
    Endpoint  = "https://s3demo.baltavista.com",
    AccessKey = "AKIAIOSFODNN7EXAMPLE",
    SecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    Region    = "us-east-1",
});

await s3.PutBucketAsync("photos");

await s3.PutObjectAsync(new PutObjectRequest
{
    BucketName = "photos",
    Key        = "cat.jpg",
    FilePath   = "cat.jpg",
    // SSE-S3 — server-managed key from OXIDB_S3_ENCRYPTION_KEY
    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256,
});

var resp = await s3.GetObjectAsync("photos", "cat.jpg");
await using var fs = File.Create("downloaded.jpg");
await resp.ResponseStream.CopyToAsync(fs);
```

### ASP.NET Core / generic host

```csharp
// Program.cs
builder.Services.AddOxiDbS3(builder.Configuration.GetSection("OxiDbS3"));

// or fluently:
builder.Services.AddOxiDbS3(o =>
{
    o.Endpoint  = builder.Configuration["OxiDbS3:Endpoint"]!;
    o.AccessKey = builder.Configuration["OxiDbS3:AccessKey"]!;
    o.SecretKey = builder.Configuration["OxiDbS3:SecretKey"]!;
    o.Region    = "us-east-1";
});

// Inject anywhere:
public sealed class PhotoController(IAmazonS3 s3) : ControllerBase
{
    [HttpGet("/photos/{key}")]
    public async Task<IActionResult> Get(string key, CancellationToken ct)
    {
        var resp = await s3.GetObjectAsync("photos", key, ct);
        return File(resp.ResponseStream, resp.Headers.ContentType ?? "application/octet-stream");
    }
}
```

`appsettings.json`:

```json
{
  "OxiDbS3": {
    "Endpoint": "https://s3demo.baltavista.com",
    "AccessKey": "AKIAIOSFODNN7EXAMPLE",
    "SecretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "Region": "us-east-1"
  }
}
```

## Server-side encryption (SSE-S3 / SSE-C)

OxiDB supports both `SSE-S3` (server-managed key from
`OXIDB_S3_ENCRYPTION_KEY`) and `SSE-C` (caller supplies a key per
request). Use the AWS SDK's standard properties:

```csharp
// SSE-S3 (server keeps the key)
var put = new PutObjectRequest
{
    BucketName = "vault",
    Key        = "secret.pdf",
    FilePath   = "secret.pdf",
    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256,
};

// SSE-C (you keep the key — even the operator can't read the file)
var key = RandomNumberGenerator.GetBytes(32);
var put = new PutObjectRequest
{
    BucketName = "vault",
    Key        = "topsecret.pdf",
    FilePath   = "topsecret.pdf",
    ServerSideEncryptionCustomerMethod   = ServerSideEncryptionCustomerMethod.AES256,
    ServerSideEncryptionCustomerProvidedKey = Convert.ToBase64String(key),
};
// Reads need the same key:
var resp = await s3.GetObjectAsync(new GetObjectRequest
{
    BucketName = "vault",
    Key        = "topsecret.pdf",
    ServerSideEncryptionCustomerMethod   = ServerSideEncryptionCustomerMethod.AES256,
    ServerSideEncryptionCustomerProvidedKey = Convert.ToBase64String(key),
});
```

## Working around older servers

Versions ≤ 0.25.17 had a bug where AWS chunked transfer encoding
came through to disk as the object's bytes. If you can't upgrade
the server, set the flags **per-request** (the AWS SDK for .NET
doesn't expose them on `AmazonS3Config`):

```csharp
var put = new PutObjectRequest
{
    BucketName = "vault",
    Key        = "doc.pdf",
    FilePath   = "doc.pdf",
    DisablePayloadSigning = true,
    UseChunkEncoding      = false,
};
await s3.PutObjectAsync(put);
```

Or wrap it in a small helper if every PUT in your app needs it:

```csharp
public static class OxiDbS3Compat
{
    public static Task<PutObjectResponse> PutLegacyAsync(
        this IAmazonS3 s3, PutObjectRequest req, CancellationToken ct = default)
    {
        req.DisablePayloadSigning = true;
        req.UseChunkEncoding      = false;
        return s3.PutObjectAsync(req, ct);
    }
}
```
