namespace ColdChain;

/// Every engine this demo uses lives in ONE oxidb-server process. Replacing it
/// would mean running Mosquitto, InfluxDB, PostgreSQL, MongoDB, Redis and MinIO
/// — six systems to deploy, secure, back up and keep in sync.
public static class Endpoints
{
    public const string Host = "127.0.0.1";

    /// Document + SQL + time-series engines (length-prefixed JSON).
    public const int Tcp = 4444;
    /// Sensors publish here (MQTT 3.1.1).
    public const int Mqtt = 1883;
    /// Live state + pub/sub (Redis wire protocol).
    public const int Redis = 6379;
    /// Certificates and photos (S3 API).
    public const int S3 = 9000;

    public static readonly string SqlConnectionString = $"Host={Host};Port={Tcp};Database=coldchain";
    public static readonly string RedisConfiguration = $"{Host}:{Redis}";
    public static readonly string S3ServiceUrl = $"http://{Host}:{S3}";
}
