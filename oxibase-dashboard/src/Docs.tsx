import { useState } from "react";

// Public JavaScript tutorial — reachable at /docs without signing in.
// Everything here runs against a real OxiBase project: create one in the
// console, copy the ref + keys, and paste the snippets.

const SECTIONS = [
  ["setup", "1 · Project & keys"],
  ["install", "2 · Install & connect"],
  ["documents", "3 · Documents (CRUD + queries)"],
  ["sql", "4 · SQL engine"],
  ["tsdb", "5 · Time series"],
  ["auth", "6 · End-user auth"],
  ["rules", "7 · Security rules"],
  ["realtime", "8 · Realtime subscriptions"],
  ["storage", "9 · File storage"],
  ["rest", "10 · Plain REST (no SDK)"],
  ["app", "11 · A complete app"],
] as const;

function Code({ title, children }: { title?: string; children: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="codeblock">
      {title && <div className="codetitle">{title}</div>}
      <button
        className="copybtn"
        onClick={() => {
          navigator.clipboard.writeText(children).then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 1200);
          });
        }}
      >
        {copied ? "copied" : "copy"}
      </button>
      <pre>
        <code>{children}</code>
      </pre>
    </div>
  );
}

export function Docs({ onOpenConsole }: { onOpenConsole: () => void }) {
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <a href="/">
            <img src="/logo-horizontal.svg" alt="OxiBase" className="brand-logo" />
          </a>
          <span className="muted" style={{ fontWeight: 500, fontSize: 14 }}>
            / JavaScript tutorial
          </span>
        </div>
        <div className="who">
          <button className="primary" onClick={onOpenConsole}>
            Open console
          </button>
        </div>
      </header>

      <div className="docs">
        <nav className="docs-toc">
          <div className="side-title">On this page</div>
          {SECTIONS.map(([id, label]) => (
            <a key={id} href={`#${id}`} className="toc-link">
              {label}
            </a>
          ))}
        </nav>

        <main className="docs-body">
          <h1>Using OxiBase from JavaScript</h1>
          <p className="lead">
            OxiBase is a complete backend platform on the OxiDB engine: every project is an isolated
            database with a REST data plane (PostgREST-compatible), a SQL engine, a time-series
            engine, per-project API keys, end-user auth, and security rules. This tutorial takes you
            from an empty project to a working app, from the browser or Node (18+, for built-in{" "}
            <code>fetch</code>).
          </p>

          {/* ── 1 ─────────────────────────────────────────────────────────── */}
          <h2 id="setup">1 · Project &amp; keys</h2>
          <p>
            In the <a href="/">console</a>, create a project. Opening it shows the three values every
            snippet below uses:
          </p>
          <ul>
            <li>
              <strong>URL</strong> — the data-plane origin. On this deployment it is{" "}
              <code>https://oxibase.baltavista.com</code>.
            </li>
            <li>
              <strong>ref</strong> — the project id (e.g. <code>li5a06ql78dlfgjm</code>). Every
              request carries it, so requests land in <em>your</em> database.
            </li>
            <li>
              <strong>API keys</strong> — two JWTs signed with your project&apos;s own key:
              <ul>
                <li>
                  <code>anon_key</code> (role <em>read</em>) — browser-safe. Reads work out of the
                  box; writes are denied until a security rule allows them (§7).
                </li>
                <li>
                  <code>service_role_key</code> (role <em>admin</em>) — bypasses rules, can use the
                  SQL engine for writes. <strong>Server-side only, never ship it to a browser.</strong>
                </li>
              </ul>
            </li>
            <li>
              <strong>Quotas</strong> — collections, SQL tables, documents, stored bytes and, if the
              deployment sets one, a <strong>request rate</strong>. Past the rate a request comes
              back <code>429</code> with a <code>Retry-After</code> header (seconds until the minute
              rolls over); the console&apos;s <em>Usage &amp; quotas</em> panel shows every cap.
            </li>
          </ul>

          {/* ── 2 ─────────────────────────────────────────────────────────── */}
          <h2 id="install">2 · Install &amp; connect</h2>
          <p>
            The client is <code>oxibase-js</code> — a thin client over OxiBase&apos;s
            PostgREST-compatible API, with a full-featured query builder.
            It is not on npm yet; install it straight from this site:
          </p>
          <Code title="shell (Node / bundlers)">{`npm install https://oxibase.baltavista.com/oxibase-js.tgz`}</Code>
          <p>
            Or skip npm entirely — a self-contained ES module (dependencies bundled, ~15&nbsp;kB) is
            hosted too, so a plain <code>&lt;script type="module"&gt;</code> works with no build
            step:
          </p>
          <Code title="index.html (no build tools)">{`<script type="module">
  import { createClient } from "https://oxibase.baltavista.com/oxibase-js.esm.js";

  const oxibase = createClient("https://oxibase.baltavista.com", ANON_KEY, {
    ref: "YOUR_PROJECT_REF",
  });
  const { data } = await oxibase.from("todos").select("*");
</script>`}</Code>
          <p>Connecting is one call:</p>
          <Code title="app.js">{`import { createClient } from "oxibase-js";

const oxibase = createClient(
  "https://oxibase.baltavista.com",   // data-plane URL
  OXIBASE_ANON_KEY,                    // or service_role key on a server
  { ref: "YOUR_PROJECT_REF" },
);`}</Code>
          <p className="note">
            No SDK? Everything below is plain HTTP — §10 shows the same operations with raw{" "}
            <code>fetch</code>, and any PostgREST client library works too.
          </p>

          {/* ── 3 ─────────────────────────────────────────────────────────── */}
          <h2 id="documents">3 · Documents (CRUD + queries)</h2>
          <p>
            The document engine stores schemaless JSON in <strong>collections</strong> —
            auto-created on first insert. <code>.from()</code> is a full PostgREST query
            builder:
          </p>
          <Code title="insert / select / update / delete">{`// insert one (or an array for many)
const { data, error } = await oxibase
  .from("todos")
  .insert({ title: "ship it", done: false })
  .select();          // .select() echoes the created row(s), _id included

// read — filters, ordering, paging
const { data: open } = await oxibase
  .from("todos")
  .select("*")
  .eq("done", false)
  .order("_id", { ascending: false })
  .limit(20);

// update rows matching a filter
await oxibase.from("todos").update({ done: true }).eq("_id", data[0]._id);

// delete
await oxibase.from("todos").delete().eq("done", true);`}</Code>
          <p>Richer filters — the PostgREST operator set:</p>
          <Code title="filters">{`await oxibase.from("products").select("*")
  .gt("price", 100)                //  >   (gte, lt, lte, neq too)
  .in("category", ["a", "b"])      //  IN (…)
  .like("name", "%pro%")           //  LIKE (ilike for case-insensitive)
  .not("stock", "eq", 0)           //  negation
  .or("price.lt.10,price.gt.900"); //  OR groups

// count without fetching rows
const { count } = await oxibase
  .from("products")
  .select("*", { count: "exact", head: true });`}</Code>
          <p>
            Related data can be stitched server-side (<em>resource embedding</em>): a{" "}
            <code>comments</code> row with a <code>post_id</code> field belongs to a{" "}
            <code>posts</code> row, and:
          </p>
          <Code title="embedding">{`// each post with its comments (has-many, inferred from comments.post_id)
const { data } = await oxibase.from("posts").select("*, comments(*)");

// each comment with its post (belongs-to)
await oxibase.from("comments").select("*, posts(title)");`}</Code>

          {/* ── 4 ─────────────────────────────────────────────────────────── */}
          <h2 id="sql">4 · SQL engine</h2>
          <p>
            Each project also has a full relational engine — separate storage, real tables, joins,
            transactions. <code>oxibase.sql(text, params)</code> runs statements;{" "}
            <code>?</code> placeholders bind <code>params</code> so values are never spliced into
            SQL text.
          </p>
          <p className="note">
            RBAC: the anon key may only <code>SELECT</code>. DDL and writes need the
            service_role key — do them server-side.
          </p>
          <Code title="tables + parameterized CRUD">{`await oxibase.sql(\`CREATE TABLE IF NOT EXISTS orders (
  id     INT PRIMARY KEY AUTO_INCREMENT,
  item   VARCHAR(80),
  qty    INT,
  price  DECIMAL(10,2),
  ts     TIMESTAMP
)\`);

await oxibase.sql(
  "INSERT INTO orders (item, qty, price, ts) VALUES (?, ?, ?, ?)",
  ["widget", 3, "9.99", Date.now()],     // TIMESTAMP = epoch ms
);

const { results, error } = await oxibase.sql(
  "SELECT item, qty * price AS total FROM orders WHERE qty > ?", [1],
);
// results[0] = { columns: ["item","total"], rows: [["widget", 29.97]], types: [...] }`}</Code>
          <p>
            SQL <strong>tables</strong> are also served by the same REST grammar — if{" "}
            <code>orders</code> is a SQL table, <code>oxibase.from("orders")</code> reads it too
            (the server dispatches by name; a collection and a table never share one).
          </p>
          <Code title="joins, aggregates — it's real SQL">{`await oxibase.sql(\`
  SELECT c.name, COUNT(*) AS orders, SUM(o.price) AS spent
  FROM orders o
  JOIN customers c ON c.id = o.customer_id
  GROUP BY c.name
  HAVING COUNT(*) > 1
  ORDER BY spent DESC
\`);`}</Code>

          {/* ── 5 ─────────────────────────────────────────────────────────── */}
          <h2 id="tsdb">5 · Time series</h2>
          <p>
            The third engine ingests metrics (Gorilla-compressed, InfluxDB-style).{" "}
            <code>.schema("tsdb")</code> routes <code>.from()</code> to it — a{" "}
            <em>measurement</em> instead of a table:
          </p>
          <Code title="write + query metrics">{`const tsdb = oxibase.schema("tsdb");

// write: strings become tags, numbers become fields, ts = epoch ms
await tsdb.from("cpu").insert([
  { ts: Date.now(), host: "web-1", usage: 0.42 },
  { ts: Date.now(), host: "web-2", usage: 0.61 },
]);

// query: mean usage in 1-minute buckets for one host
const { data } = await tsdb.from("cpu")
  .select("usage")
  .eq("host", "web-1")
  .gte("ts", Date.now() - 3600_000);
// rows come back flattened: { ts, value, host }`}</Code>

          {/* ── 6 ─────────────────────────────────────────────────────────── */}
          <h2 id="auth">6 · End-user auth</h2>
          <p>
            Your app&apos;s own users (not you, the developer) sign up <em>against the project</em>.
            Pass <code>authUrl</code> (the control
            plane, same origin on this deployment) and start from the anon key:
          </p>
          <Code title="signup / login / session">{`const oxibase = createClient("https://oxibase.baltavista.com", ANON_KEY, {
  ref: "YOUR_PROJECT_REF",
  authUrl: "https://oxibase.baltavista.com",   // control plane (for .auth)
});

// register + start a session (or signInWithPassword for returning users)
const { user, error } = await oxibase.auth.signUp({
  email: "ada@example.com",
  password: "correct-horse",
});

// every .from()/.sql() call now runs AS this user:
// tokens carry auth.username = email, auth.role = "authenticated"
await oxibase.from("posts").insert({ owner: "ada@example.com", body: "hi" });

oxibase.auth.getSession();   // { token, refreshToken } | null
oxibase.auth.signOut();      // back to the anon key
// expired tokens refresh automatically on 401 (refresh tokens rotate)`}</Code>
          <p>
            <strong>Persisting a session</strong> (localStorage, a cookie, a native store):
            refresh tokens are <strong>single-use</strong> — the moment the access token is first
            renewed, the token you got at sign-in is revoked. So re-store the session on{" "}
            <em>every</em> change, not just at sign-in; saving only the sign-in copy leaves a spent
            token behind and the next reload cannot resume it:
          </p>
          <Code title="persisting a session across reloads">{`// re-store on every change — signedIn, tokenRefreshed, signedOut
oxibase.auth.onAuthStateChange((event, session) => {
  if (session) localStorage.setItem("session", JSON.stringify(session));
  else localStorage.removeItem("session");
});

// on load, resume as that user
const stored = JSON.parse(localStorage.getItem("session") ?? "null");
if (stored) oxibase.auth.setSession(stored);

// setSession does not fire the callback — you supplied that session,
// so a listener that persists cannot loop back into it.`}</Code>
          <p>
            <strong>Email verification</strong>: on this deployment, new users must confirm their
            address before they can sign in — <code>signUp</code> returns{" "}
            <code>verificationRequired: true</code> (no session yet), the user clicks the emailed
            link, and <code>signInWithPassword</code> works from then on.{" "}
            <strong>Password reset</strong> is two calls — the emailed link lands on a hosted form,
            so most apps only need the first:
          </p>
          <Code title="verification + reset">{`// after signUp:
const { verificationRequired } = await oxibase.auth.signUp({ email, password });
if (verificationRequired) showMessage("Check your inbox to activate your account");

// didn't get the mail?
await oxibase.auth.resendVerification(email);

// "Forgot password?" — always resolves (no account enumeration)
await oxibase.auth.resetPasswordForEmail(email);
// the emailed link opens a hosted set-new-password page; sessions are revoked

// managing users: the console's Users tab lists your project's users,
// with per-user verify / set-password / delete`}</Code>
          <p>
            <strong>Social sign-in</strong> (Google, GitHub) is per project. In the console&apos;s{" "}
            <em>Users</em> tab, register an OAuth app with the provider, paste its client ID and
            secret, copy the shown redirect URI into the provider&apos;s settings, and list the URLs
            your app may be sent back to. Then:
          </p>
          <Code title="sign in with a provider">{`// which methods does this project offer? (public — no key needed)
const { providers } = await oxibase.auth.getSettings();   // e.g. ["github"]

// start the flow: this navigates to the provider's consent screen
oxibase.auth.signInWithOAuth({
  provider: "github",
  redirectTo: "https://app.example.com/callback",  // must be an allowed URL
});

// …on that page, adopt the session the provider round-trip handed back:
const session = oxibase.auth.getSessionFromUrl();
if (session?.error) showMessage(session.error);      // e.g. "access_denied"
else if (session) showApp();                         // signed in

// already running Google Identity Services? skip the redirect entirely:
await oxibase.auth.signInWithIdToken({ provider: "google", token: credential });`}</Code>
          <p>
            <strong>Magic links</strong> are passwordless sign-in and need no provider at all — just
            one allowed redirect URL. The click both creates the account (if it is new) and verifies
            the address, and lands on the same <code>getSessionFromUrl()</code> you use for OAuth:
          </p>
          <Code title="passwordless sign-in">{`await oxibase.auth.signInWithMagicLink({
  email: "ada@example.com",
  redirectTo: "https://app.example.com/callback",   // must be an allowed URL
});
// → "check your inbox for a sign-in link" (same answer for unknown addresses)

// on the callback page, exactly as with OAuth:
const session = oxibase.auth.getSessionFromUrl();`}</Code>
          <p className="muted small">
            Links last 15 minutes and work once — a second click is refused, so a link forwarded or
            left in an inbox cannot start a new session.
          </p>
          <p className="muted small">
            The session arrives in the URL <em>fragment</em>, so it never reaches a server log, and{" "}
            <code>getSessionFromUrl()</code> strips it from the address bar once adopted. A user who
            signs in with a provider is matched to an existing account by <strong>verified</strong>{" "}
            email — signing up with a password and later using Google lands in one account, not two.
            Only addresses the provider says it verified are accepted.
          </p>

          {/* ── 7 ─────────────────────────────────────────────────────────── */}
          <h2 id="rules">7 · Security rules</h2>
          <p>
            Rules are OxiBase&apos;s row-level security. Without a rule, a collection is readable but{" "}
            <strong>anon/user writes are denied</strong>. Rules are per-collection expressions over{" "}
            <code>auth</code> (the caller) and <code>doc</code>/<code>newDoc</code> (the row) — set
            them in the console&apos;s <em>Rules</em> tab, or via the API with the service_role key:
          </p>
          <Code title="owner-only board (Rules tab, or POST /api/rules/posts)">{`{
  "read":   "auth.role == 'authenticated'",
  "create": "auth.username == newDoc.owner",
  "update": "auth.username == doc.owner",
  "delete": "auth.username == doc.owner"
}`}</Code>
          <p>
            With that in place, the flow of §6 is safe end-to-end: users see the board, but can only
            create rows they own and touch their own rows. The service_role key bypasses rules —
            that&apos;s what makes it dangerous in a browser. A fully public demo collection would use{" "}
            <code>"true"</code> for all four.
          </p>

          {/* ── 8 ─────────────────────────────────────────────────────────── */}
          <h2 id="realtime">8 · Realtime subscriptions</h2>
          <p>
            Live changes push over a WebSocket.{" "}
            <code>oxibase.subscribe(collection, callback)</code> opens one shared connection
            (authenticated with your key or the signed-in user&apos;s session), reconnects
            automatically, and delivers <code>insert</code> / <code>update</code> /{" "}
            <code>delete</code> events:
          </p>
          <Code title="live todos">{`const sub = oxibase.subscribe("todos", (event) => {
  // event = { op: "insert" | "update" | "delete", collection, docId, doc }
  console.log(event.op, event.doc);
  reloadList(); // or patch local state from event.doc
}, {
  query: { done: false },                  // optional server-side equality filter
  onError: (msg) => console.error(msg),    // access denied, connection loss, …
});

// later:
sub.unsubscribe();`}</Code>
          <p>
            Security rules apply <strong>per row</strong>: a subscriber only receives events for
            rows its identity may read. With the owner-only rule of §7, two signed-in users
            subscribed to the same collection each see only their own rows change. One caveat:
            under a per-row read rule, delete events are not delivered (the server won&apos;t
            reveal even the id of a row you can&apos;t read).
          </p>
          <p className="note">
            A denied collection rejects at subscribe time with{" "}
            <code>access denied: read on '…'</code> via <code>onError</code>. Requires the server&apos;s
            WebSocket listener (<code>OXIDB_WS_PORT</code>) — enabled on this deployment at{" "}
            <code>wss://oxibase.baltavista.com/ws</code>, which the client derives automatically.
          </p>

          {/* ── 9 ─────────────────────────────────────────────────────────── */}
          <h2 id="storage">9 · File storage</h2>
          <p>
            Every project has an isolated blob store for files. Buckets are
            created on first upload (or explicitly); objects keep their MIME type and get an ETag.
            Reads work with the anon key; <strong>uploads and deletes need the service_role key</strong>{" "}
            (do them server-side), and each project has a storage quota (visible in the console).
          </p>
          <Code title="upload / list / download / remove">{`// server-side (service_role key)
const { data, error } = await oxibase.storage
  .from("avatars")
  .upload("users/ada.png", file, { contentType: "image/png" });
// -> { key, size, etag, content_type, ... }

// list a folder
const { data: files } = await oxibase.storage.from("avatars").list({ prefix: "users/" });

// download (browser-safe with the anon key)
const { data: blob } = await oxibase.storage.from("avatars").download("users/ada.png");
imgEl.src = URL.createObjectURL(blob);

// delete
await oxibase.storage.from("avatars").remove("users/ada.png");

// buckets + usage against the quota
const { data: b } = await oxibase.storage.listBuckets();
// -> { buckets: ["avatars"], totalBytes: 18432 }`}</Code>
          <p className="note">
            Plain HTTP underneath: <code>PUT/GET/DELETE /api/storage/&#123;bucket&#125;/&#123;key&#125;?db=&lt;ref&gt;</code>{" "}
            with a bearer key — the body is the raw file, the <code>Content-Type</code> header is
            stored and served back. The console&apos;s <strong>Files</strong> tab manages the same
            store visually.
          </p>

          {/* ── 10 ────────────────────────────────────────────────────────── */}
          <h2 id="rest">10 · Plain REST (no SDK)</h2>
          <p>
            The data plane is plain HTTP — PostgREST grammar on{" "}
            <code>/rest/v1/&lt;collection&gt;</code>, SQL on <code>/api/sql</code>. Add{" "}
            <code>?db=&lt;ref&gt;</code> and a bearer key and any HTTP client works:
          </p>
          <Code title="fetch — documents">{`const BASE = "https://oxibase.baltavista.com";
const REF  = "YOUR_PROJECT_REF";
const H    = { "Content-Type": "application/json", Authorization: "Bearer " + KEY };

// insert (Prefer echoes the created row)
await fetch(BASE + "/rest/v1/todos?db=" + REF, {
  method: "POST",
  headers: { ...H, Prefer: "return=representation" },
  body: JSON.stringify({ title: "ship it", done: false }),
});

// select with filters/order/limit — PostgREST query grammar
const rows = await (await fetch(
  BASE + "/rest/v1/todos?db=" + REF + "&done=eq.false&order=_id.desc&limit=20",
  { headers: H },
)).json();

// update / delete by filter
await fetch(BASE + "/rest/v1/todos?db=" + REF + "&_id=eq.5", {
  method: "PATCH", headers: H, body: JSON.stringify({ done: true }),
});
await fetch(BASE + "/rest/v1/todos?db=" + REF + "&done=eq.true", {
  method: "DELETE", headers: H,
});`}</Code>
          <Code title="fetch — SQL">{`const { results } = await (await fetch(BASE + "/api/sql?db=" + REF, {
  method: "POST",
  headers: H,   // service_role key for writes/DDL
  body: JSON.stringify({
    sql: "SELECT item, qty FROM orders WHERE qty > ?",
    params: [1],
  }),
})).json();`}</Code>

          {/* ── 11 ────────────────────────────────────────────────────────── */}
          <h2 id="app">11 · A complete app</h2>
          <p>
            A minimal React component with everything wired — list, add, toggle, delete against a{" "}
            <code>todos</code> collection. Swap in your ref and anon key (plus a rule from §7, or a
            public <code>"true"</code> rule while prototyping):
          </p>
          <Code title="Todos.jsx">{`import { useEffect, useState } from "react";
import { createClient } from "oxibase-js";

const oxibase = createClient(
  "https://oxibase.baltavista.com",
  import.meta.env.VITE_OXIBASE_KEY,
  { ref: import.meta.env.VITE_OXIBASE_REF },
);

export function Todos() {
  const [todos, setTodos] = useState([]);
  const [draft, setDraft] = useState("");

  const load = async () => {
    const { data } = await oxibase.from("todos").select("*").order("_id");
    setTodos(data ?? []);
  };
  useEffect(() => { load(); }, []);

  const add = async (e) => {
    e.preventDefault();
    if (!draft.trim()) return;
    await oxibase.from("todos").insert({ title: draft.trim(), done: false });
    setDraft("");
    load();
  };
  const toggle = async (t) => {
    await oxibase.from("todos").update({ done: !t.done }).eq("_id", t._id);
    load();
  };
  const remove = async (t) => {
    await oxibase.from("todos").delete().eq("_id", t._id);
    load();
  };

  return (
    <div>
      <form onSubmit={add}>
        <input value={draft} onChange={(e) => setDraft(e.target.value)} />
        <button>Add</button>
      </form>
      <ul>
        {todos.map((t) => (
          <li key={t._id}>
            <input type="checkbox" checked={t.done} onChange={() => toggle(t)} />
            {t.title}
            <button onClick={() => remove(t)}>✕</button>
          </li>
        ))}
      </ul>
    </div>
  );
}`}</Code>
          <p>
            Fuller walkthroughs live in the repository under <code>oxibase-js/examples/</code>: a
            two-engine app (<code>notes</code> — SQL table + collection side by side), a task-tracker
            covering every query feature (<code>task-tracker</code>), and the end-user-auth +
            owner-rules flow (<code>auth-owner-rules</code>).
          </p>

          <p className="muted" style={{ marginTop: 40 }}>
            Questions or a missing recipe? The console is one click away — everything here works
            against a free project.
          </p>
        </main>
      </div>
    </div>
  );
}
