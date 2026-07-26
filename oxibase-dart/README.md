# oxibase — Dart & Flutter client for OxiBase

One backend behind one URL: documents governed by row-level security rules, SQL
tables, time-series, file storage, realtime over a WebSocket, and end-user auth.

**Targets Android and iOS.** It is plain Dart over `http` and
`web_socket_channel`, so it runs anywhere those do, but mobile is what it is
built and tested for — Flutter web is deliberately out of scope.

```dart
final client = OxibaseClient(
  url: 'https://oxibase.example.com',
  key: anonKey,                 // public by design — the rules decide what it may do
  ref: 'my-project',
  authUrl: 'https://oxibase.example.com',
);

await client.auth.signInWithPassword(email: email, password: password);

final posts = await client
    .from('posts')
    .select()
    .isNull('reply_to')
    .order('ts', ascending: false)
    .limit(20)
    .get();
```

Two dependencies, both already in every Flutter app: `http` and
`web_socket_channel`. No code generation, no build step.

## Storing a session — read this one

Refresh tokens are **single-use**. The moment the access token is first renewed,
the refresh token you were handed at sign-in is revoked. So persist on *every*
change, not just after sign-in:

```dart
client.auth.onAuthStateChange.listen((change) async {
  final (event, session) = change;          // signedIn | tokenRefreshed | signedOut
  if (session == null) {
    await storage.delete(key: 'session');
  } else {
    await storage.write(key: 'session', value: jsonEncode(session.toJson()));
  }
});

// on launch
final stored = await storage.read(key: 'session');
if (stored != null) {
  client.auth.setSession(Session.fromJson(jsonDecode(stored))!);
}
```

Saving only the sign-in copy leaves a spent token in storage, and the next launch
cannot resume with it — the app looks signed in and every request fails. (This is
the bug that produced this note; the client's own refresh is single-flight, so
parallel requests cannot cause it.)

`setSession` deliberately does **not** fire the callback: you supplied it, so
echoing it back would loop a listener that persists on every event.

## What each part talks to

| call | engine |
|---|---|
| `client.from('posts')` | documents, or a SQL table of that name — the server decides |
| `client.series('cpu')` | time-series (selected by profile, since a measurement exists only once written) |
| `client.sql('SELECT …', [args])` | the SQL engine; values always bound, never interpolated |
| `client.textSearch('posts', 'query')` | BM25 full-text over a collection |
| `client.storage.from('photos')` | file storage, incl. `.search()` over file *contents* |
| `client.subscribe('posts', …)` | realtime, one shared socket, rules applied to the stream |

## What an app-shipped key may not do

The anon key is meant to ship. It is bounded server-side, and three things are
refused outright because those engines have no per-row rules to adjudicate with:

* writing a **SQL table**
* appending to a **time-series**
* writing **files**

Do those through your own server with the service_role key — never ship that key
in an app.

## Errors say which kind of "no"

```dart
try {
  await client.from('posts').insert(post);
} on OxibaseException catch (e) {
  if (e.isRateLimited) retryAfter(e.retryAfter);   // 429 — not yet
  else if (e.isConflict) alreadyPosted();          // 409 — a unique index holds it
  else if (e.isDenied) signInAgain();              // 401/403 — a rule said no
}
```

`isRateLimited` is deliberately separate from `isDenied`: "not yet" and "no" are
different answers, and a per-identity rate limit answers the first.

## Realtime

```dart
final sub = await client.subscribe('posts', (event) {
  if (event.op == 'insert') addToFeed(event.doc!);
});
// later
sub.unsubscribe();
```

One socket for the whole client. The server applies read rules to the stream, so a
subscriber receives only rows it could have fetched. Signing in or out reconnects
(the identity changed); a token rotation does not, since it is the same reader.

## Images from storage

Storage reads are authenticated, so `Image.network(bucket.publicUrl(key))` will
not work on its own — the request needs the header:

```dart
Image.network(
  client.storage.from('photos').publicUrl(key).toString(),
  headers: {'Authorization': 'Bearer $accessToken'},
);
```

## Counting

```dart
final open = await client.from('todos').eq('done', false).count();
```

Uses the native count endpoint rather than a PostgREST count: this server's
`Content-Range` reports the page but leaves the total as `*`, so there is nothing
to read a total from. Equality filters only — that is what the endpoint takes.

## Not here, on purpose

`rpc()` — the JavaScript client exposes it because it wraps `postgrest-js`, but
OxiBase does not serve `/rest/v1/rpc`; calling it returns 404. Stored procedures
are reached through `.sql('CALL …')`.

## Tests

`dart test` runs hermetically against a mock server. To also run the integration
suite against a real one:

```bash
OXIBASE_URL=http://127.0.0.1:8087 OXIBASE_KEY=<admin JWT> dart test
```
