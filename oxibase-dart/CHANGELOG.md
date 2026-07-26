## 0.1.0

First release. Documents with the PostgREST grammar, SQL, time-series, BM25 full
text over collections and over stored files, file storage, realtime, and end-user
auth.

Auth carries the lessons from the JavaScript client: refreshes are single-flight,
a request whose token rotated mid-flight retries rather than re-refreshing, and
every rotation is reported through `onAuthStateChange` so a stored session can be
kept current.

### Since the first cut

* Realtime **reconnects on its own** after a dropped socket — 500ms doubling to
  15s, only while something is subscribed, reset on a successful connect. On a
  phone the socket dies routinely; before this the subscriptions simply stopped.
* `count()` on the query builder, via the native count endpoint.
* `client.url` and `client.ref` accessors.
* No `rpc()`: OxiBase does not serve `/rest/v1/rpc`, so a method for it could only
  ever 404.
