## 0.1.0

First release. Documents with the PostgREST grammar, SQL, time-series, BM25 full
text over collections and over stored files, file storage, realtime, and end-user
auth.

Auth carries the lessons from the JavaScript client: refreshes are single-flight,
a request whose token rotated mid-flight retries rather than re-refreshing, and
every rotation is reported through `onAuthStateChange` so a stored session can be
kept current.
