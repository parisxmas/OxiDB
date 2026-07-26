using Amazon;
using Amazon.Runtime;
using Amazon.S3;

namespace OxiDb.Client.S3;

/// <summary>
/// Builds <see cref="IAmazonS3"/> instances pre-configured for an
/// OxiDB S3 endpoint. Use <see cref="Create(OxiDbS3Options)"/> for
/// one-off clients, or the DI extensions for hosted apps.
/// </summary>
public static class OxiDbS3ClientFactory
{
    /// <summary>
    /// Creates a new <see cref="IAmazonS3"/> bound to the OxiDB
    /// endpoint described by <paramref name="options"/>. Caller
    /// owns the returned client and must dispose it.
    /// </summary>
    public static IAmazonS3 Create(OxiDbS3Options options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.Endpoint))
            throw new ArgumentException("Endpoint is required", nameof(options));
        if (string.IsNullOrWhiteSpace(options.AccessKey))
            throw new ArgumentException("AccessKey is required", nameof(options));
        if (string.IsNullOrWhiteSpace(options.SecretKey))
            throw new ArgumentException("SecretKey is required", nameof(options));

        var config = BuildConfig(options);
        var creds = new BasicAWSCredentials(options.AccessKey, options.SecretKey);
        return new AmazonS3Client(creds, config);
    }

    /// <summary>
    /// Builds an <see cref="AmazonS3Config"/> with OxiDB defaults.
    /// Exposed in case callers want to construct the client through
    /// some path other than the factory (e.g. wiring it into an
    /// existing AWS SDK abstraction).
    /// </summary>
    /// <remarks>
    /// <see cref="OxiDbS3Options.DisableChunkedEncoding"/> is honored
    /// per-request, not on the client config — set
    /// <c>PutObjectRequest.DisablePayloadSigning = true</c> and
    /// <c>UseChunkEncoding = false</c> on the request object. The
    /// AWS SDK for .NET exposes the toggle there, not on
    /// <see cref="AmazonS3Config"/>. See README for the snippet.
    /// </remarks>
    public static AmazonS3Config BuildConfig(OxiDbS3Options options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var config = new AmazonS3Config
        {
            ServiceURL = NormalizeEndpoint(options.Endpoint),
            // OxiDB does not implement virtual-hosted-style buckets
            // (no wildcard DNS); always address paths as
            // /{bucket}/{key}.
            ForcePathStyle = true,
            // Same region SigV4 will sign with — must match the
            // server's OXIDB_S3_REGION env var.
            AuthenticationRegion = options.Region,
            UseHttp = options.Endpoint.StartsWith("http://", StringComparison.OrdinalIgnoreCase),
            // AWS SDK v4 always signs SigV4; the legacy
            // SignatureVersion knob is gone, so nothing to set here.

            // OxiDB returns SHA-256-truncated ETags (16 bytes hex),
            // not AWS-style MD5(body). The SDK's default checksum
            // validation insists on MD5 equality and throws
            // "Expected hash not equal to calculated hash" on every
            // PUT/GET otherwise. WHEN_REQUIRED skips the work
            // unless the server explicitly negotiates a checksum.
            RequestChecksumCalculation =
                Amazon.Runtime.RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation =
                Amazon.Runtime.ResponseChecksumValidation.WHEN_REQUIRED,
        };

        // Always inject our own HttpClientFactory so we can strip
        // ETag/Content-MD5 from responses — the AWS SDK's
        // HashStream still treats those as MD5-of-body even after
        // ResponseChecksumValidation=WHEN_REQUIRED, and OxiDB's
        // SHA-truncated ETags trigger spurious mismatch failures.
        config.HttpClientFactory = options.DisableTlsValidation
            ? new OxiDbHttpClientFactory(insecure: true)
            : new OxiDbHttpClientFactory(insecure: false);

        return config;
    }

    private static string NormalizeEndpoint(string raw)
    {
        var trimmed = raw.TrimEnd('/');
        if (trimmed.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            return trimmed;
        }
        return $"https://{trimmed}";
    }

    /// <summary>
    /// HttpClientFactory whose pipeline strips ETag / Content-MD5
    /// headers from responses before the AWS SDK's HashStream
    /// fires. OxiDB's truncated-SHA ETag is the same shape as an
    /// MD5 hex string, so the SDK tries to validate it as MD5(body)
    /// and bails. Removing the header from the response object
    /// keeps the byte stream intact (HashStream only kicks in when
    /// it has an expected hash to compare against).
    /// </summary>
    private sealed class OxiDbHttpClientFactory : HttpClientFactory
    {
        private readonly bool _insecure;
        public OxiDbHttpClientFactory(bool insecure) { _insecure = insecure; }

        public override HttpClient CreateHttpClient(IClientConfig clientConfig)
        {
            var inner = new HttpClientHandler();
            if (_insecure)
            {
                inner.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
            }
            return new HttpClient(new OxiDbResponseRewriter { InnerHandler = inner });
        }
    }

    private sealed class OxiDbResponseRewriter : DelegatingHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var resp = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            // Apply ONLY to GetObject responses (or anything that
            // streams a body). For PUT we want the typed
            // PutObjectResponse.ETag to come through so callers can
            // store it. The cheapest discriminator is "did this
            // response carry a body?" — GETs do, PUTs return XML or
            // empty.
            if (request.Method == HttpMethod.Get && resp.Content != null)
            {
                // Remove the ETag header that the SDK feeds into
                // HashStream as expected MD5. Strip Content-MD5 too
                // for completeness. The body bytes are unchanged;
                // only the validation-trigger headers go away.
                resp.Headers.Remove("ETag");
                resp.Content.Headers.Remove("Content-MD5");
            }
            return resp;
        }
    }
}
