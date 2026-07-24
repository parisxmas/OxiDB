import type { Metadata } from "next"

export const metadata: Metadata = {
  title: ".NET Examples",
  description: `End-to-end examples for the OxiDB .NET clients — TCP, Embedded (FFI), and the OxiDb.Linq standalone LINQ provider. Three NuGet packages. .NET 10.0+.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg> .NET Examples</h2>
    <p class="section-desc">A full .NET stack for OxiDB &mdash; a TCP client, an embedded (in-process FFI) client, a standalone <strong>OxiDb.Linq</strong> provider for the document engine, and a complete <strong>Entity Framework Core</strong> provider (with ADO.NET / Dapper) for the SQL engine. Targets <code>net10.0+</code>.</p>

    <!-- Install -->
    <div class="doc-block">
      <h3>Install</h3>
      <pre><code><span class="co"># Document engine — TCP client, embedded client, LINQ provider</span>
dotnet add package OxiDb.Client.Tcp
dotnet add package OxiDb.Client.Embedded
dotnet add package OxiDb.Linq

<span class="co"># SQL engine — Entity Framework Core provider + ADO.NET (Dapper-ready)</span>
dotnet add package OxiDb.EntityFrameworkCore
dotnet add package OxiDb.Data</code></pre>
      <p>Latest: <strong>v0.39.15</strong>. The EF Core packages are hosted on this site &mdash; add it as a NuGet source, or download the <code>.nupkg</code> files directly:</p>
      <pre><code class="lang-bash"><span class="co"># download the EF Core provider + its dependencies</span>
curl -LO https://oxidb.baltavista.com/nuget/OxiDb.EntityFrameworkCore.0.39.15.nupkg
curl -LO https://oxidb.baltavista.com/nuget/OxiDb.Data.0.39.15.nupkg
curl -LO https://oxidb.baltavista.com/nuget/OxiDb.Client.Tcp.0.39.15.nupkg

<span class="co"># add the folder as a source, then install</span>
dotnet nuget add source $(pwd) --name oxidb
dotnet add package OxiDb.EntityFrameworkCore</code></pre>
    </div>

    <!-- EF Core -->
    <div class="doc-block">
      <h3>Entity Framework Core <span class="version-badge latest">SQL engine</span></h3>
      <p>A complete EF Core provider for the OxiDB <a href="/sql/">SQL engine</a> &mdash; it passes all <strong>3832 official EF Core relational specification tests</strong> and beats PostgreSQL across the EF Core benchmark. Migrations, design-time scaffolding, LINQ translation, <code>Include</code>, and <code>ExecuteUpdate</code>/<code>ExecuteDelete</code> all work. Start the server with <code>OXIDB_SQL=1</code>.</p>

      <h4>DbContext</h4>
      <pre><code><span class="kw">using</span> Microsoft.EntityFrameworkCore;
<span class="kw">using</span> OxiDb.EntityFrameworkCore;

<span class="kw">public class</span> Blog {
    <span class="kw">public int</span> Id { <span class="kw">get; set;</span> }
    <span class="kw">public string</span> Url { <span class="kw">get; set;</span> } = <span class="str">""</span>;
    <span class="kw">public int</span> Rating { <span class="kw">get; set;</span> }
    <span class="kw">public</span> List&lt;Post&gt; Posts { <span class="kw">get;</span> } = <span class="kw">new</span>();
}
<span class="kw">public class</span> Post {
    <span class="kw">public int</span> Id { <span class="kw">get; set;</span> }
    <span class="kw">public string</span> Title { <span class="kw">get; set;</span> } = <span class="str">""</span>;
    <span class="kw">public int</span> BlogId { <span class="kw">get; set;</span> }
}

<span class="kw">public class</span> BloggingContext : DbContext {
    <span class="kw">public</span> DbSet&lt;Blog&gt; Blogs =&gt; Set&lt;Blog&gt;();
    <span class="kw">public</span> DbSet&lt;Post&gt; Posts =&gt; Set&lt;Post&gt;();

    <span class="kw">protected override void</span> OnConfiguring(DbContextOptionsBuilder o) =&gt;
        o.UseOxiDb(<span class="str">"Host=127.0.0.1;Port=4444"</span>);
}</code></pre>

      <h4>Migrate &amp; query</h4>
      <pre><code><span class="kw">using var</span> db = <span class="kw">new</span> BloggingContext();
db.Database.Migrate();                         <span class="co">// real __EFMigrationsHistory</span>

db.Blogs.Add(<span class="kw">new</span> Blog { Url = <span class="str">"https://oxidb.dev"</span>, Rating = <span class="num">5</span> });
<span class="kw">await</span> db.SaveChangesAsync();

<span class="co">// LINQ with Include + ordering (translated to SQL joins)</span>
<span class="kw">var</span> top = <span class="kw">await</span> db.Blogs
    .Where(b =&gt; b.Rating &gt;= <span class="num">4</span>)
    .Include(b =&gt; b.Posts)
    .OrderByDescending(b =&gt; b.Rating)
    .ToListAsync();

<span class="co">// Bulk update / delete — one UPDATE / DELETE round trip</span>
<span class="kw">await</span> db.Posts.Where(p =&gt; p.BlogId == <span class="num">1</span>)
    .ExecuteUpdateAsync(s =&gt; s.SetProperty(p =&gt; p.Title, <span class="str">"Updated"</span>));
<span class="kw">await</span> db.Blogs.Where(b =&gt; b.Rating &lt; <span class="num">2</span>).ExecuteDeleteAsync();</code></pre>

      <h4>Migrations &amp; scaffolding (dotnet ef)</h4>
      <pre><code class="lang-bash">dotnet ef migrations add Init
dotnet ef database update

<span class="co"># reverse-engineer a DbContext from an existing database</span>
dotnet ef dbcontext scaffold <span class="str">"Host=127.0.0.1;Port=4444"</span> OxiDb.EntityFrameworkCore</code></pre>

      <h4>Raw SQL &mdash; ADO.NET &amp; Dapper</h4>
      <pre><code><span class="kw">using</span> OxiDb.Data;
<span class="kw">using</span> Dapper;

<span class="kw">using var</span> conn = <span class="kw">new</span> OxiDbConnection(<span class="str">"Host=127.0.0.1;Port=4444"</span>);
conn.Open();

<span class="co">// Dapper</span>
<span class="kw">var</span> blogs = conn.Query&lt;Blog&gt;(<span class="str">"SELECT * FROM Blogs WHERE Rating &gt;= @r"</span>, <span class="kw">new</span> { r = <span class="num">4</span> });

<span class="co">// or plain ADO.NET</span>
<span class="kw">using var</span> cmd = conn.CreateCommand();
cmd.CommandText = <span class="str">"INSERT INTO Blogs (Url, Rating) VALUES (?, ?)"</span>;
cmd.Parameters.Add(<span class="kw">new</span> OxiDbParameter { Value = <span class="str">"https://x.io"</span> });
cmd.Parameters.Add(<span class="kw">new</span> OxiDbParameter { Value = <span class="num">4</span> });
cmd.ExecuteNonQuery();</code></pre>
      <p>Full SQL surface &mdash; joins, CTEs, window functions, transactions, instant online <code>ALTER TABLE</code> &mdash; on the <a href="/sql/">SQL Engine</a> page.</p>
    </div>

    <!-- OxiDb.Linq -->
    <div class="doc-block">
      <h3>OxiDb.Linq — standalone LINQ provider <span class="version-badge latest">net10.0</span></h3>
      <p>Lightweight, EF-free LINQ over OxiDB. <code>IQueryable&lt;T&gt;</code> with <code>Where</code> / <code>OrderBy</code> / <code>Skip</code> / <code>Take</code> / <code>First</code> / <code>Count</code> / <code>Any</code> / <code>Sum</code> / projection, plus fluent <code>SetAsync</code> / <code>IncAsync</code> / <code>DeleteAsync</code> after a filter. Think MongoDB.Driver's <code>IMongoQueryable</code>, but for OxiDB.</p>
      <pre><code><span class="kw">using</span> OxiDb.Client.Tcp;
<span class="kw">using</span> OxiDb.Linq;

<span class="kw">await using var</span> client = <span class="kw">await</span> OxiDbTcpClient.ConnectAsync(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>);
<span class="kw">var</span> users = client.GetCollection&lt;User&gt;(<span class="str">"users"</span>);

<span class="co">// Insert</span>
<span class="kw">await</span> users.InsertAsync(<span class="kw">new</span> User { Name = <span class="str">"Alice"</span>, Age = <span class="num">30</span>, Country = <span class="str">"TR"</span> });

<span class="co">// Where + OrderBy + Take + materialise</span>
<span class="kw">var</span> top = <span class="kw">await</span> users
    .Where(u =&gt; u.Country == <span class="str">"TR"</span> &amp;&amp; u.Age &gt;= <span class="num">18</span>)
    .OrderByDescending(u =&gt; u.Age)
    .Take(<span class="num">10</span>)
    .ToListAsync();

<span class="co">// FirstOrDefault with predicate</span>
<span class="kw">var</span> alice = <span class="kw">await</span> users.FirstOrDefaultAsync(u =&gt; u.Email == <span class="str">"alice@x.com"</span>);

<span class="co">// Aggregations</span>
<span class="kw">int</span> n     = <span class="kw">await</span> users.CountAsync(u =&gt; u.Active);
<span class="kw">decimal</span> total = <span class="kw">await</span> users.Where(u =&gt; u.Active).SumAsync(u =&gt; u.Spend);
<span class="kw">bool</span> anyVip  = <span class="kw">await</span> users.AnyAsync(u =&gt; u.Tier == <span class="str">"vip"</span>);

<span class="co">// Projection</span>
<span class="kw">var</span> names = <span class="kw">await</span> users
    .Where(u =&gt; u.Active)
    .Select(u =&gt; <span class="kw">new</span> { u.Name, u.Email })
    .ToListAsync();

<span class="co">// String predicates → $regex</span>
<span class="kw">var</span> gmail = <span class="kw">await</span> users
    .Where(u =&gt; u.Email.EndsWith(<span class="str">"@gmail.com"</span>))
    .ToListAsync();

<span class="co">// Fluent updates / deletes after a Where</span>
<span class="kw">await</span> users.Where(u =&gt; u.Country == <span class="str">"TR"</span>).SetAsync(<span class="kw">new</span> { tier = <span class="str">"gold"</span> });
<span class="kw">await</span> users.Where(u =&gt; u.Email == <span class="str">"alice@x.com"</span>).IncAsync(<span class="kw">new</span> { loginCount = <span class="num">1</span> });
<span class="kw">await</span> users.Where(u =&gt; u.Expired).DeleteAsync();

<span class="co">// Raw escape hatch — full Mongo-style update document</span>
<span class="kw">await</span> users.Where(u =&gt; u._id == id).UpdateAsync(<span class="kw">new</span> Dictionary&lt;<span class="ty">string</span>, <span class="kw">object</span>?&gt; {
    [<span class="str">"$set"</span>] = <span class="kw">new</span> { lastLogin = DateTime.UtcNow },
    [<span class="str">"$inc"</span>] = <span class="kw">new</span> { loginCount = <span class="num">1</span> }
});</code></pre>

      <h4>Supported LINQ surface</h4>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Op</th><th>Notes</th></tr></thead>
          <tbody>
            <tr><td><code>Where</code></td><td><code>==</code>, <code>!=</code>, <code>&lt;</code>, <code>&lt;=</code>, <code>&gt;</code>, <code>&gt;=</code>, <code>&amp;&amp;</code>, <code>||</code>, <code>!</code>, <code>string.Contains/StartsWith/EndsWith</code>, <code>IEnumerable.Contains</code>, dotted property paths</td></tr>
            <tr><td><code>OrderBy / OrderByDescending / ThenBy / ThenByDescending</code></td><td>—</td></tr>
            <tr><td><code>Skip / Take</code></td><td>Maps to server <code>skip</code> / <code>limit</code></td></tr>
            <tr><td><code>Select</code></td><td>Client-side projection (any output type, including anonymous)</td></tr>
            <tr><td><code>First / FirstOrDefault</code></td><td>With or without predicate</td></tr>
            <tr><td><code>Single / SingleOrDefault</code></td><td>Pulls 2, throws on duplicates</td></tr>
            <tr><td><code>Count / Any</code></td><td>Index-friendly; <code>Count</code> hits the dedicated server endpoint</td></tr>
            <tr><td><code>Sum / Min / Max / Average</code></td><td>Lowered to a <code>$group</code> aggregation pipeline</td></tr>
          </tbody>
        </table>
      </div>

      <h4>Mutation extensions</h4>
      <pre><code><span class="co">// Build the filter with LINQ, then mutate.</span>
<span class="kw">await</span> q.SetAsync(<span class="kw">new</span> { ... });        <span class="co">// $set</span>
<span class="kw">await</span> q.UnsetAsync(<span class="kw">new</span> { ... });      <span class="co">// $unset</span>
<span class="kw">await</span> q.IncAsync(<span class="kw">new</span> { ... });        <span class="co">// $inc</span>
<span class="kw">await</span> q.PushAsync(<span class="kw">new</span> { ... });       <span class="co">// $push</span>
<span class="kw">await</span> q.PullAsync(<span class="kw">new</span> { ... });       <span class="co">// $pull</span>
<span class="kw">await</span> q.AddToSetAsync(<span class="kw">new</span> { ... });   <span class="co">// $addToSet</span>
<span class="kw">await</span> q.UpdateAsync(<span class="kw">new</span> Dictionary&lt;<span class="ty">string</span>, <span class="kw">object</span>?&gt;{ ... });  <span class="co">// raw</span>
<span class="kw">await</span> q.DeleteAsync();
<span class="kw">await</span> q.DeleteOneAsync();</code></pre>

      <h4>Property-name mapping</h4>
      <ul>
        <li>By default, C# property names go to JSON 1:1 (<code>Name</code> → <code>"Name"</code>).</li>
        <li>The single special case: a property called <code>Id</code> maps to <code>_id</code> on the wire.</li>
        <li>Override with <code>[JsonPropertyName("...")]</code> — works in both query translation and serialization.</li>
        <li>Deserialization is case-insensitive, so <code>Tier</code> matches <code>"tier"</code> and <code>"Tier"</code>.</li>
      </ul>

    </div>

    <!-- Connect (TCP) -->
    <div class="doc-block">
      <h3>Connect — TCP client</h3>
      <pre><code><span class="kw">using</span> OxiDb.Client.Tcp;

<span class="co">// Default 127.0.0.1:4444</span>
<span class="kw">await using var</span> client = <span class="kw">new</span> OxiDbTcpClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>);
<span class="kw">await</span> client.PingAsync();

<span class="co">// With SCRAM-SHA-256 auth</span>
<span class="kw">await using var</span> auth = <span class="kw">new</span> OxiDbTcpClient(
    host: <span class="str">"db.internal"</span>,
    port: <span class="num">4444</span>,
    username: <span class="str">"admin"</span>,
    password: <span class="str">"s3cret"</span>);</code></pre>
    </div>

    <!-- Connect (Embedded) -->
    <div class="doc-block">
      <h3>Connect — Embedded (no server)</h3>
      <pre><code><span class="kw">using</span> OxiDb.Client.Embedded;

<span class="co">// In-process. Native lib resolved automatically per platform.</span>
<span class="kw">using var</span> db = <span class="kw">new</span> OxiDbEmbeddedClient(<span class="str">"./data"</span>);
<span class="kw">await</span> db.PingAsync();

<span class="co">// Same IOxiDbClient surface as the TCP client — swap one constructor for the other.</span></code></pre>
    </div>

    <!-- Insert -->
    <div class="doc-block">
      <h3>Insert</h3>
      <pre><code><span class="co">// Single document — pass any object that serializes to JSON</span>
<span class="kw">await</span> client.InsertAsync(<span class="str">"users"</span>, <span class="kw">new</span> {
    name = <span class="str">"Alice"</span>,
    age = <span class="num">30</span>,
    department = <span class="str">"Engineering"</span>,
    tags = <span class="kw">new</span>[] { <span class="str">"vip"</span>, <span class="str">"early-access"</span> }
});

<span class="co">// Bulk insert</span>
<span class="kw">var</span> batch = Enumerable.Range(<span class="num">1</span>, <span class="num">1000</span>).Select(i => <span class="kw">new</span> {
    name = <span class="str">$"User {i}"</span>, age = <span class="num">20</span> + (i % <span class="num">50</span>)
});
<span class="kw">await</span> client.InsertManyAsync(<span class="str">"users"</span>, batch);</code></pre>
    </div>

    <!-- Find -->
    <div class="doc-block">
      <h3>Find &amp; query</h3>
      <pre><code><span class="co">// Simple equality</span>
<span class="kw">var</span> tokyo = <span class="kw">await</span> client.FindAsync(<span class="str">"users"</span>,
    query: <span class="kw">new</span> { city = <span class="str">"Tokyo"</span> });

<span class="co">// Comparison + sort + paging</span>
<span class="kw">var</span> page = <span class="kw">await</span> client.FindAsync(
    <span class="str">"users"</span>,
    query: <span class="kw">new</span> { age = <span class="kw">new</span> { _gte = <span class="num">25</span>, _lt = <span class="num">40</span> } },
    sort:  <span class="kw">new</span> { age = -<span class="num">1</span> },
    skip:  <span class="num">0</span>,
    limit: <span class="num">20</span>);

<span class="co">// Logical operators</span>
<span class="kw">var</span> admins = <span class="kw">await</span> client.FindAsync(<span class="str">"users"</span>, <span class="kw">new</span> {
    _or = <span class="kw">new</span>[] {
        <span class="kw">new</span> { role = <span class="str">"admin"</span> },
        <span class="kw">new</span> { role = <span class="str">"owner"</span> }
    }
});

<span class="co">// Find one</span>
<span class="kw">var</span> alice = <span class="kw">await</span> client.FindOneAsync(<span class="str">"users"</span>, <span class="kw">new</span> { name = <span class="str">"Alice"</span> });</code></pre>
      <p>The wire uses <code>$gte</code>, <code>$lt</code>, <code>$or</code>, etc. In C# anonymous types you can't start a name with <code>$</code>, so the SDK accepts <code>_gte</code> / <code>_lt</code> / <code>_or</code> and rewrites them on the wire.</p>
    </div>

    <!-- Update -->
    <div class="doc-block">
      <h3>Update</h3>
      <pre><code><span class="co">// $set, $inc, $push — combine in one call (OCC requirement)</span>
<span class="kw">await</span> client.UpdateAsync(
    <span class="str">"users"</span>,
    query:  <span class="kw">new</span> { name = <span class="str">"Alice"</span> },
    update: <span class="kw">new</span> {
        _set  = <span class="kw">new</span> { last_login = DateTime.UtcNow },
        _inc  = <span class="kw">new</span> { login_count = <span class="num">1</span> },
        _push = <span class="kw">new</span> { recent_ips = <span class="str">"1.2.3.4"</span> }
    });

<span class="co">// First match only</span>
<span class="kw">await</span> client.UpdateOneAsync(<span class="str">"sessions"</span>,
    <span class="kw">new</span> { user_id = id, expired = <span class="kw">false</span> },
    <span class="kw">new</span> { _set = <span class="kw">new</span> { expired = <span class="kw">true</span> } });</code></pre>
    </div>

    <!-- Delete -->
    <div class="doc-block">
      <h3>Delete</h3>
      <pre><code><span class="kw">await</span> client.DeleteAsync(<span class="str">"sessions"</span>, <span class="kw">new</span> { expired = <span class="kw">true</span> });
<span class="kw">await</span> client.DeleteOneAsync(<span class="str">"users"</span>, <span class="kw">new</span> { _id = userId });</code></pre>
    </div>

    <!-- Count -->
    <div class="doc-block">
      <h3>Count</h3>
      <pre><code><span class="kw">int</span> total  = <span class="kw">await</span> client.CountAsync(<span class="str">"users"</span>);
<span class="kw">int</span> active = <span class="kw">await</span> client.CountAsync(<span class="str">"users"</span>, <span class="kw">new</span> { active = <span class="kw">true</span> });</code></pre>
    </div>

    <!-- Aggregation -->
    <div class="doc-block">
      <h3>Aggregation pipeline</h3>
      <pre><code><span class="kw">var</span> revenue = <span class="kw">await</span> client.AggregateAsync(<span class="str">"orders"</span>, <span class="kw">new</span>[] {
    <span class="kw">new</span> { _match = <span class="kw">new</span> { status = <span class="str">"paid"</span>, year = <span class="num">2026</span> } },
    <span class="kw">new</span> { _group = <span class="kw">new</span> {
        _id = <span class="str">"$category"</span>,
        total = <span class="kw">new</span> { _sum = <span class="str">"$amount"</span> },
        n     = <span class="kw">new</span> { _sum = <span class="num">1</span> }
    }},
    <span class="kw">new</span> { _sort  = <span class="kw">new</span> { total = -<span class="num">1</span> } },
    <span class="kw">new</span> { _limit = <span class="num">10</span> }
});</code></pre>
    </div>

    <!-- Transactions -->
    <div class="doc-block">
      <h3>Transactions</h3>
      <pre><code><span class="kw">var</span> tx = <span class="kw">await</span> client.BeginTransactionAsync();
<span class="kw">try</span>
{
    <span class="kw">await</span> tx.UpdateAsync(<span class="str">"accounts"</span>, <span class="kw">new</span> { _id = fromId },
        <span class="kw">new</span> { _inc = <span class="kw">new</span> { balance = -amount } });
    <span class="kw">await</span> tx.UpdateAsync(<span class="str">"accounts"</span>, <span class="kw">new</span> { _id = toId },
        <span class="kw">new</span> { _inc = <span class="kw">new</span> { balance =  amount } });
    <span class="kw">await</span> tx.InsertAsync(<span class="str">"ledger"</span>, <span class="kw">new</span> { from = fromId, to = toId, amount });
    <span class="kw">await</span> tx.CommitAsync();
}
<span class="kw">catch</span>
{
    <span class="kw">await</span> tx.RollbackAsync();
    <span class="kw">throw</span>;
}</code></pre>
    </div>

    <!-- Stored procedures -->
    <div class="doc-block">
      <h3>Stored procedures (OxiScript)</h3>
      <pre><code><span class="kw">await</span> client.CreateProcedureAsync(<span class="str">"transfer"</span>, @<span class="str">"
    proc transfer(from, to, amount) {
        let s = find_one(""accounts"", {_id: from})
        if s == null            { abort ""sender not found"" }
        if s.balance &lt; amount   { abort ""insufficient funds"" }
        update(""accounts"", {_id: from}, {$inc: {balance: -amount}})
        update(""accounts"", {_id: to},   {$inc: {balance:  amount}})
        return {ok: true}
    }"</span>);

<span class="kw">var</span> result = <span class="kw">await</span> client.CallProcedureAsync(<span class="str">"transfer"</span>,
    <span class="kw">new</span> { from = <span class="str">"alice"</span>, to = <span class="str">"bob"</span>, amount = <span class="num">1500</span> });</code></pre>
      <p>Full <a href="/oxiscript/">OxiScript reference</a>.</p>
    </div>

    <!-- Indexes -->
    <div class="doc-block">
      <h3>Indexes</h3>
      <pre><code><span class="kw">await</span> client.CreateIndexAsync(<span class="str">"users"</span>, <span class="str">"email"</span>, unique: <span class="kw">true</span>);
<span class="kw">await</span> client.CreateIndexAsync(<span class="str">"orders"</span>, <span class="str">"created_at"</span>);

<span class="kw">var</span> indexes = <span class="kw">await</span> client.ListIndexesAsync(<span class="str">"users"</span>);
<span class="kw">await</span> client.DropIndexAsync(<span class="str">"users"</span>, <span class="str">"old_field"</span>);</code></pre>
    </div>

    <!-- Maintenance -->
    <div class="doc-block">
      <h3>Maintenance</h3>
      <pre><code><span class="co">// Reclaim space from soft-deleted records</span>
<span class="kw">await</span> client.CompactAsync(<span class="str">"users"</span>);

<span class="co">// Inspect collections</span>
<span class="kw">var</span> names = <span class="kw">await</span> client.ListCollectionsAsync();</code></pre>
    </div>

    <!-- Mode comparison -->
    <div class="doc-block">
      <h3>TCP vs. Embedded — when to pick which</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Package</th><th>Process model</th><th>Use when</th></tr></thead>
          <tbody>
            <tr><td><code>OxiDb.Client.Tcp</code></td><td>Talks to a separate <code>oxidb-server</code></td><td>Multiple apps share the DB, you want auth/RBAC, container deploys</td></tr>
            <tr><td><code>OxiDb.Client.Embedded</code></td><td>In-process via native FFI</td><td>Single-process apps, desktop tools, short-lived workers, tests</td></tr>
            <tr><td><code>OxiDb.Linq</code></td><td>Layered on either of the above</td><td>You want LINQ syntax (<code>Where</code>/<code>OrderBy</code>/...) over a document collection</td></tr>
          </tbody>
        </table>
      </div>
    </div>

  </div>
</section>` }} />
}
