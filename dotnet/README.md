# OxiDB .NET Clients

Three published NuGet packages + two helper libraries cover the
.NET integration story:

| Package | What it does | NuGet | Target |
|---|---|---|---|
| **`OxiDb.Client.Tcp`** | Pure-managed TCP/OxiWire client | `dotnet add package OxiDb.Client.Tcp` | `net10.0` |
| **`OxiDb.Client.Embedded`** | Embedded FFI client (no server) | `dotnet add package OxiDb.Client.Embedded` | `net10.0` |
| **`OxiDb.EntityFrameworkCore`** | EF Core provider | `dotnet add package OxiDb.EntityFrameworkCore` | `net10.0` |
| **`OxiDb.Linq`** | LINQ provider over either client | `dotnet add package OxiDb.Linq` *(0.28.18+)* | `net10.0` |
| `OxiDb.Client.S3` | S3-API client (multi-target) | — | `net9.0; net10.0` |

All packages ship at the same version as the server (`0.28.18`), bumped
together via `dotnet/Directory.Build.props`.

## 60-second start (TCP)

```csharp
using OxiDb.Client.Tcp;

await using var client = await OxiDbTcpClient.ConnectAsync("127.0.0.1", 4444);

// Verify version + features at connect time (HELLO handshake, ADR-0003 Phase 2).
var hello = await client.HelloAsync(clientId: "myapp/1.0");
Console.WriteLine($"Connected to {hello.Name} {hello.Version} (wire v{hello.WireVersion})");

// Insert a doc and get the auto-assigned id.
var aliceId = await client.InsertReturningIdAsync("users",
    new { name = "Alice", age = 30, active = true });

// Typed find with a strongly-typed Query.
public record User(long _id, string Name, int Age, bool Active);

var adults = await client.FindAsync<User>("users",
    Query.And(Query.Gte("age", 18), Query.Eq("active", true)));

// Stream millions of rows without materialising the whole result set.
await foreach (var user in client.StreamAsync<User>("users", batchSize: 1000, sort: new { _id = 1 }))
{
    // process one at a time
}
```

## DI integration

```csharp
// Program.cs
services.AddOxiDbTcp(opts =>
{
    opts.Host = "oxidb.internal";
    opts.Port = 4444;
    opts.Username = "app";
    opts.Password = config["OXIDB_PASSWORD"];
});

// Then anywhere:
public class UserService(IOxiDbClient db)
{
    public Task<List<User>> RecentAsync() =>
        db.FindAsync<User>("users", sort: new { _id = -1 }, limit: 100);
}
```

## LINQ

`OxiDb.Linq` lets you use LINQ syntax against any `IOxiDbClient`:

```csharp
using OxiDb.Linq;

var users = client.GetCollection<User>("users");

var adults = await users
    .Where(u => u.Age >= 18 && u.Active)
    .OrderByDescending(u => u.Age)
    .Take(50)
    .ToListAsync();
```

## Exceptions

The base class is `OxiDbException`. Catch this if you don't care which
specific failure happened. Subclasses for the common cases:

| Type | When it fires |
|---|---|
| `OxiDbDuplicateKeyException` | Write violated a unique index |
| `OxiDbTransactionConflictException` | OCC commit-time conflict; retry the whole transaction |
| `OxiDbAuthenticationException` | SCRAM / RBAC failure |
| `OxiDbNotFoundException` | Collection / index / user missing |
| `OxiDbImmutableException` | Write hit a WORM lock |
| `OxiDbConnectionException` | Wire-level (connection closed, EOF, ...) |
| `OxiDbProtocolException` | OxiWire decode error — usually wire-version mismatch |

`OxiDbTcpException` is retained as a `[Obsolete]` alias of
`OxiDbException` for binary-compat — it will be removed in 2.0.

## Query operators (without LINQ)

For runtime-constructed queries, prefer the typed `Query.*` helpers
over anonymous-object literals — they get autocomplete and type-checking:

```csharp
// Eq, Ne, Gt, Gte, Lt, Lte
var young = Query.Lt("age", 30);

// In / Nin
var engineeringOrSales = Query.In("department", new[] { "Engineering", "Sales" });

// Exists / Regex
var hasEmail = Query.Exists("email");
var startsWithA = Query.Regex("name", "^A");

// Logical combinators
var senior = Query.And(Query.Eq("active", true), Query.Gte("age", 50));
var nyOrLondon = Query.Or(Query.Eq("city", "NY"), Query.Eq("city", "London"));

// Range (half-open [low, high))
var teens = Query.Range("age", 13, 20);
```

These mix freely with anonymous-object literals — the typed helpers
return `Dictionary<string, object?>` which the wire layer already
accepts.

## EF Core

```csharp
public class AppDb : DbContext
{
    public DbSet<User> Users => Set<User>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseOxiDb("Host=127.0.0.1;Port=4444");
}
```

The EF Core provider currently emits `EF1001` warnings for one
internal-API use (identity resolution). See
[`OxiDb.EntityFrameworkCore/EF1001-AUDIT.md`](OxiDb.EntityFrameworkCore/EF1001-AUDIT.md)
for the planned migration to public surface.

## Roadmap (Phase 3 — 1.0 SDK freeze)

- `api/v1.json` surface snapshot per client + CI diff gate
  ([`docs/PHASE3-SDK-FREEZE.md`](../docs/PHASE3-SDK-FREEZE.md))
- Source generator (`OxiDb.Generators`) that turns
  `[OxiCollection]`-tagged records into compile-time-translated
  queries. Currently a stub; full LINQ compile-time translation is a
  separate PR (estimated 1–2 weeks).
- EF1001-clean EF Core provider — see audit file above.
