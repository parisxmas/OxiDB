/// Dart and Flutter client for OxiBase.
///
/// One backend behind one URL: documents governed by row-level security rules,
/// SQL tables, time-series, file storage, realtime over a WebSocket, and end-user
/// auth. The key an app ships is public by design — what it may do is decided
/// server-side by the project's rules.
///
/// ```dart
/// final client = OxibaseClient(url: '…', key: anonKey, ref: 'my-project', authUrl: '…');
/// await client.auth.signInWithPassword(email: e, password: p);
/// final rows = await client.from('posts').select().order('ts', ascending: false).limit(20).get();
/// ```
///
/// If you store the session, store it on **every** `auth.onAuthStateChange`
/// event: refresh tokens are single-use, so the one handed out at sign-in is
/// revoked as soon as the access token is first renewed.
library;

export 'src/auth.dart' show OxibaseAuth, AuthSettings;
export 'src/client.dart' show OxibaseClient;
export 'src/query.dart' show OxibaseQuery;
export 'src/realtime.dart' show RealtimeSubscription;
export 'src/storage.dart' show OxibaseStorage, OxibaseBucket;
export 'src/types.dart'
    show
        AuthChangeEvent,
        ChangeEvent,
        OxibaseException,
        Session,
        SqlResult,
        StorageObject,
        StorageSearchHit;
