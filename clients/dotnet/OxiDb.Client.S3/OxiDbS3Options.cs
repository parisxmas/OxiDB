namespace OxiDb.Client.S3;

/// <summary>
/// Configuration for an OxiDB S3 endpoint. Only <see cref="Endpoint"/>,
/// <see cref="AccessKey"/>, and <see cref="SecretKey"/> are required;
/// the rest pick OxiDB-friendly defaults.
/// </summary>
public sealed class OxiDbS3Options
{
    /// <summary>
    /// Public-facing URL of the OxiDB S3 listener, e.g.
    /// <c>https://s3demo.baltavista.com</c>. Trailing slash is fine
    /// either way.
    /// </summary>
    public string Endpoint { get; set; } = string.Empty;

    /// <summary>SigV4 access key id. Server-side this is matched
    /// against <c>OXIDB_S3_ACCESS_KEY</c> (or one of the entries in
    /// <c>OXIDB_S3_CREDENTIALS</c>).</summary>
    public string AccessKey { get; set; } = string.Empty;

    /// <summary>SigV4 secret access key. Pair with
    /// <see cref="AccessKey"/>; never log this value.</summary>
    public string SecretKey { get; set; } = string.Empty;

    /// <summary>Region used during SigV4 signing. The server must
    /// have <c>OXIDB_S3_REGION</c> set to the same string. Defaults
    /// to <c>us-east-1</c> to match AWS-CLI/boto3 defaults.</summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// Disables AWS chunked transfer encoding on uploads. Older
    /// OxiDB-server builds (≤ 0.25.17) wrote the raw chunk frames
    /// to disk instead of the decoded payload; setting this to
    /// <c>true</c> forces a classic Content-Length body and works
    /// around the bug. Newer servers handle both, so leaving the
    /// default <c>false</c> is safe against an up-to-date deployment.
    /// </summary>
    public bool DisableChunkedEncoding { get; set; }

    /// <summary>
    /// Skip TLS certificate validation. Only useful for self-signed
    /// dev deployments — never enable against production.
    /// </summary>
    public bool DisableTlsValidation { get; set; }
}
