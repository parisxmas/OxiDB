# oxidb_embedded — OxiDB for Flutter/Dart, in-process

The full OxiDB engine running **inside your app** over `dart:ffi` — no
server, no network. A local database in the Hive/Isar weight class, with a
server-grade engine behind it:

- **Documents + queries**: MongoDB-style operators (`$gte`, `$in`,
  `$regex`, `$or`, …), sort/skip/limit
- **Upsert**: `updateOne(..., upsert: true)` — insert-or-replace in one call
- **Indexes**: field, unique, composite, full-text (BM25), **geo**
  (geohash: `$near`, `$geoWithin` box/circle/polygon), **TTL** (self-cleaning
  caches)
- **Aggregation**: `$match`/`$group`/`$lookup`/`$geoNear`/window functions —
  the same pipeline the server runs
- **ACID transactions** with a `transaction(() { ... })` helper
- **Blob storage**: S3-style buckets for images/files
- **SQL engine**: real relational tables in the same directory (`db.sql(...)`)
- **Encryption at rest**: AES-256-GCM over data files *and* WAL, key
  supplied as 32 raw bytes from the platform keystore
- **Disk-first storage**: documents live in an mmap'd file; resident memory
  stays bounded regardless of collection size — phone-friendly by default
- **Durable writes**: WAL-backed; when a call returns, the write survives
  a crash

```dart
import 'package:oxidb_embedded/oxidb_embedded.dart';

final db = OxiDb.open('${appDir.path}/oxidb');

// Documents
db.insert('rides', {'driver': 'u42', 'loc': [29.0, 41.0], 'fare': 120});
final nearby = db.find('rides', query: {
  'loc': {r'$near': {r'$geometry': {'type': 'Point', 'coordinates': [29.0, 41.0]},
                     r'$maxDistance': 2000.0}}
});

// SharedPreferences-shaped sugar, same database
final prefs = Preferences(db);
prefs.put('theme', 'dark');

// SQL, same directory
db.sql('CREATE TABLE IF NOT EXISTS logs (id INT PRIMARY KEY, msg TEXT)');

db.close();
```

## Encryption (Hive comparison)

Like Hive's `HiveAesCipher`, you supply a 32-byte key; unlike Hive, the
engine encrypts **at the storage layer** — data files and the write-ahead
log both — so no plaintext ever reaches disk:

```dart
final key = await readOrCreateKeyInKeystore(); // Keychain / Android Keystore
final db = OxiDb.open(path, encryptionKey: key);
```

Store the key with `flutter_secure_storage` (or platform Keystore APIs
directly). Never write it to a file in the app sandbox.

## Native library

This package binds to `liboxidb_embedded_ffi` (prebuilt binaries ship with
[OxiDB releases](https://github.com/parisxmas/OxiDB/releases), or build with
`cargo build --release -p oxidb-embedded-ffi`):

- **Android**: put `liboxidb_embedded_ffi.so` per ABI under your app's
  `android/app/src/main/jniLibs/{arm64-v8a,armeabi-v7a,x86_64}/`
  (`clients/android/build.sh` in the OxiDB repo cross-builds all ABIs via
  cargo-ndk). Loaded automatically by name.
- **iOS**: link `OxiDBEmbedded.xcframework` (static) into the Runner
  target; symbols are found in-process.
- **macOS / Linux / tests**: set `Bindings.libraryPath` to the
  `.dylib`/`.so`, or let the default name resolution find it.

## Binary size (measured, Android arm64, `--profile mobile`)

| Build | On disk | Download (~gzip) |
|---|---|---|
| Full engine | 9.8 MB | 4.2 MB |
| **Lite** (`--no-default-features`) | **4.8 MB** | **2.0 MB** |

The **lite** build drops the SQL engine and the PDF/DOCX/XLSX text
extractors; everything else — documents, queries, all index types, geo,
aggregation, FTS over text/HTML/JSON, transactions, blobs, encryption —
is identical, and both are covered by this package's test suite
(`OXIDB_LITE=1 dart test`). On lite, `db.sql(...)` throws an
`OxiDbException` naming the lite build, and PDF/DOCX/XLSX blobs are
stored fine but not text-indexed.

```bash
# full
cargo ndk -t arm64-v8a -o jniLibs build --profile mobile -p oxidb-embedded-ffi --features android
# lite
cargo ndk -t arm64-v8a -o jniLibs build --profile mobile -p oxidb-embedded-ffi --no-default-features --features android
```

That buys queries, indexes, aggregation, FTS, blobs and encryption (plus
SQL on the full build) in one dependency — where a typical stack pairs
SharedPreferences + a KV store + sqlite + a file cache.

## Tests

```bash
cargo build --release -p oxidb-embedded-ffi   # from the repo root
cd clients/oxidb-embedded-dart
dart test                                     # OXIDB_FFI_LIB overrides the library path
```
