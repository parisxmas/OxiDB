using BugTracker;
using Microsoft.EntityFrameworkCore;
using OxiDb.Client.Tcp;
using OxiDb.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// The bug tracker's own isolated OxiDB — a different process and volume from
// the production instances (see docker-compose.yml). `oxidb` in compose,
// localhost from a terminal.
var host = Environment.GetEnvironmentVariable("BUGS_HOST") ?? "127.0.0.1";
var conn = $"Host={host};Port=4444;Database=bugtracker";

builder.Services.AddDbContext<BugDb>(o => o.UseOxiDb(conn));
builder.Services.AddSingleton<GoogleAuth>();

// Serialize with the C# property names verbatim (PascalCase) rather than the
// ASP.NET default camelCase — the frontend's types are written against these
// exact names (Id, Title, IsAdmin, …), so the two contracts match by construction.
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.PropertyNamingPolicy = null);

// Same-origin in production (nginx proxies /bugs-api to this container), so CORS
// is not strictly needed there. It is here so `next dev` on :3000 can talk to
// the API on :8080 during development.
var origins = (Environment.GetEnvironmentVariable("BUGS_ALLOWED_ORIGINS")
               ?? "https://oxidb.baltavista.com,http://localhost:3000")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.WithOrigins(origins).AllowAnyHeader().WithMethods("GET", "POST", "PATCH")));

var app = builder.Build();

// Create the database + tables on startup. Idempotent, so a restart is
// harmless and there is no shell in the container to run migrations from.
for (var attempt = 1; ; attempt++)
{
    try
    {
        // The SQL engine is per-database, and a connection to `Database=bugtracker`
        // fails at open ("database not found") unless the database already exists
        // — the EF provider's Create() is a no-op by design. So create it first,
        // over a plain connection to the default database, then let EF build the
        // tables inside it. Both steps are idempotent.
        await using (var boot = await OxiDbTcpClient.ConnectAsync(host, 4444))
            await boot.SqlAsync("CREATE DATABASE IF NOT EXISTS bugtracker");

        using var scope = app.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<BugDb>().Database.EnsureCreatedAsync();
        break;
    }
    catch (Exception e) when (attempt < 30)
    {
        app.Logger.LogInformation("waiting for oxidb ({attempt}): {msg}", attempt, e.Message);
        await Task.Delay(TimeSpan.FromSeconds(1));
    }
}

app.UseCors();

var auth = app.Services.GetRequiredService<GoogleAuth>();
if (!auth.Configured)
    app.Logger.LogWarning(
        "BUGS_GOOGLE_CLIENT_ID is not set — every write will be rejected as unauthenticated.");

// ── Info ────────────────────────────────────────────────────────────────────
app.MapGet("/api", () => Results.Ok(new
{
    service = "OxiDB bug tracker",
    storage = "its own isolated oxidb-server (SQL engine) via EF Core",
    auth = "Google Sign-In (ID token) — required to open bugs or comment",
    endpoints = new[]
    {
        "GET   /bugs?status=&q=      list bugs (public)",
        "GET   /bugs/{id}            one bug with its comments (public)",
        "POST  /bugs                 open a bug (signed in)",
        "POST  /bugs/{id}/comments   comment (signed in)",
        "PATCH /bugs/{id}            change status (admin)",
        "GET   /me                   who am I (from the ID token)",
    },
}));

// ── Who am I ─────────────────────────────────────────────────────────────────
// The frontend calls this after sign-in to learn the display name and, crucially,
// whether to show the admin controls. The server is still the one that enforces
// it — this only decides what to draw.
app.MapGet("/me", async (HttpRequest req, GoogleAuth ga) =>
{
    var me = await ga.AuthenticateAsync(req);
    return me is null
        ? Results.Ok(new { signedIn = false })
        : Results.Ok(new { signedIn = true, me.Name, me.Email, me.Picture, me.IsAdmin });
});

// ── List + search (public) ───────────────────────────────────────────────────
app.MapGet("/bugs", async (BugDb db, string? status, string? q) =>
{
    var query = db.Bugs.AsNoTracking().AsQueryable();
    if (status is "open" or "closed")
        query = query.Where(b => b.Status == status);

    // With a query: full-text search ranked by BM25 (best matches first),
    // not a substring filter. The corpus is small, so we score in-process.
    if (!string.IsNullOrWhiteSpace(q))
    {
        var raw = await query.Select(b => new
        {
            b.Id, b.Title, b.Body, b.Status,
            Reporter = b.ReporterName,
            b.CreatedUtc, b.UpdatedUtc,
            Comments = b.Comments.Count,
        }).ToListAsync();

        var docs = raw
            .Select(x => new Bm25.Doc(x.Id, x.Title, x.Body, x.Status, x.Reporter,
                x.CreatedUtc, x.UpdatedUtc, x.Comments))
            .ToList();

        var hits = Bm25.Rank(docs, q)
            .Where(h => h.Score > 0)
            .OrderByDescending(h => h.Score)
            .Select(h => new
            {
                h.Doc.Id, h.Doc.Title, h.Doc.Status,
                Reporter = h.Doc.Reporter,
                h.Doc.CreatedUtc, h.Doc.UpdatedUtc,
                Comments = h.Doc.CommentCount,
                Score = Math.Round(h.Score, 3),
            });
        return Results.Ok(hits);
    }

    var rows = await query
        .OrderByDescending(b => b.Status == "open") // open first
        .ThenByDescending(b => b.CreatedUtc)
        .Select(b => new
        {
            b.Id, b.Title, b.Status,
            Reporter = b.ReporterName,
            b.CreatedUtc, b.UpdatedUtc,
            Comments = b.Comments.Count,
        })
        .ToListAsync();
    return Results.Ok(rows);
});

// ── One bug with comments (public) ───────────────────────────────────────────
app.MapGet("/bugs/{id:int}", async (int id, BugDb db) =>
{
    var bug = await db.Bugs.AsNoTracking()
        .Include(b => b.Comments)
        .FirstOrDefaultAsync(b => b.Id == id);
    if (bug is null) return Results.NotFound();

    return Results.Ok(new
    {
        bug.Id, bug.Title, bug.Body, bug.Status,
        Reporter = bug.ReporterName,
        bug.CreatedUtc, bug.UpdatedUtc,
        Comments = bug.Comments.OrderBy(c => c.CreatedUtc).Select(c => new
        {
            c.Id, c.Body, Author = c.AuthorName, c.IsAdmin, c.CreatedUtc,
        }),
    });
});

// ── Open a bug (signed in) ───────────────────────────────────────────────────
app.MapPost("/bugs", async (HttpRequest req, BugDb db, GoogleAuth ga, NewBug input) =>
{
    var me = await ga.AuthenticateAsync(req);
    if (me is null) return Results.Unauthorized();

    var title = (input.Title ?? "").Trim();
    var body = (input.Body ?? "").Trim();
    if (title.Length < 3 || title.Length > 200)
        return Results.BadRequest(new { error = "Title must be 3–200 characters." });
    if (body.Length is < 5 or > 20_000)
        return Results.BadRequest(new { error = "Description must be 5–20000 characters." });

    var now = DateTime.UtcNow;
    var bug = new BugReport
    {
        Title = title, Body = body, Status = "open",
        ReporterSub = me.Sub, ReporterEmail = me.Email, ReporterName = me.Name,
        CreatedUtc = now, UpdatedUtc = now,
    };
    db.Bugs.Add(bug);
    await db.SaveChangesAsync();
    return Results.Created($"/bugs/{bug.Id}", new { bug.Id });
});

// ── Comment (signed in) ──────────────────────────────────────────────────────
app.MapPost("/bugs/{id:int}/comments", async (
    int id, HttpRequest req, BugDb db, GoogleAuth ga, NewComment input) =>
{
    var me = await ga.AuthenticateAsync(req);
    if (me is null) return Results.Unauthorized();

    var body = (input.Body ?? "").Trim();
    if (body.Length is < 1 or > 10_000)
        return Results.BadRequest(new { error = "Comment must be 1–10000 characters." });

    var bug = await db.Bugs.FirstOrDefaultAsync(b => b.Id == id);
    if (bug is null) return Results.NotFound();

    var now = DateTime.UtcNow;
    db.Comments.Add(new BugComment
    {
        BugReportId = id, Body = body,
        AuthorSub = me.Sub, AuthorEmail = me.Email, AuthorName = me.Name, IsAdmin = me.IsAdmin,
        CreatedUtc = now,
    });
    bug.UpdatedUtc = now;
    await db.SaveChangesAsync();
    return Results.Ok(new { ok = true });
});

// ── Change status (admin only) ───────────────────────────────────────────────
app.MapPatch("/bugs/{id:int}", async (
    int id, HttpRequest req, BugDb db, GoogleAuth ga, StatusChange input) =>
{
    var me = await ga.AuthenticateAsync(req);
    if (me is null) return Results.Unauthorized();
    if (!me.IsAdmin) return Results.Forbid();

    var status = (input.Status ?? "").Trim().ToLowerInvariant();
    if (status is not ("open" or "closed"))
        return Results.BadRequest(new { error = "Status must be 'open' or 'closed'." });

    var bug = await db.Bugs.FirstOrDefaultAsync(b => b.Id == id);
    if (bug is null) return Results.NotFound();

    bug.Status = status;
    bug.UpdatedUtc = DateTime.UtcNow;
    await db.SaveChangesAsync();
    return Results.Ok(new { bug.Id, bug.Status });
});

// ── Delete a bug (admin only) ────────────────────────────────────────────────
// Removes the bug and its comments for good. Gated to admins — a reporter can
// open and comment, but only an admin can erase a report from the record.
app.MapDelete("/bugs/{id:int}", async (int id, HttpRequest req, BugDb db, GoogleAuth ga) =>
{
    var me = await ga.AuthenticateAsync(req);
    if (me is null) return Results.Unauthorized();
    if (!me.IsAdmin) return Results.Forbid();

    var bug = await db.Bugs.Include(b => b.Comments).FirstOrDefaultAsync(b => b.Id == id);
    if (bug is null) return Results.NotFound();

    db.Comments.RemoveRange(bug.Comments); // FK children first
    db.Bugs.Remove(bug);
    await db.SaveChangesAsync();
    return Results.Ok(new { deleted = id });
});

app.Run();

// Request bodies.
record NewBug(string? Title, string? Body);
record NewComment(string? Body);
record StatusChange(string? Status);
