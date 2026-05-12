# The Oxide Press — a Django blog on embedded OxiDB

A complete blog example that uses **no SQL database** — Django serves
the frontend, sessions live in signed cookies, and every piece of
persistent state (posts, admin users, hero images) lives inside an
embedded [OxiDB](https://github.com/parisxmas/OxiDB) instance running
in-process via FFI.

What you get out of the box:

- A public archive + single-post reading view (editorial / press
  aesthetic — Fraunces display, Newsreader body, JetBrains Mono for
  marginalia, oxide-rust accent that nods to the database).
- A thin **composing-room** admin: log in, create / edit / delete
  posts, upload a hero image per post.
- Images stored in an **S3-style bucket** inside the same OxiDB
  process (`blog-images`), served back via a Django streaming view.
- Sample post + default `admin / admin` user seeded on first boot.

## How the persistence layer looks

There is one OxiDB instance, opened lazily on the first request:

```
_data/
├── posts.btree        # collection — { _id, title, slug, body, author, created_at, image_key }
├── posts.wal          #   write-ahead log (crash-safe commits)
├── admins.btree       # collection — { _id, username, password_hash, created_at }
├── admins.wal
└── _blobs/
    └── blog-images/   # S3-style bucket
        ├── <key>.data
        └── <key>.meta
```

No `migrate`, no `makemigrations`. Indexes (`slug` unique, `created_at`)
are declared in `posts/db.py:_setup_schema()` and are idempotent.

## Run it

```bash
cd examples/python-embedded/django-blog

# 1) Set up a venv and install deps.
python3 -m venv .venv
source .venv/bin/activate

# 2) Install oxidb-embedded. Once the new wheel is on PyPI you can do:
#       pip install oxidb-embedded
#    For now we install the locally-built wheel:
pip install ../../../python-embedded/dist/oxidb_embedded-0.25.21-py3-none-any.whl
pip install -r requirements.txt

# 3a) Dev — single-process, one thread, autoreload:
python manage.py runserver 127.0.0.1:8765

# 3b) Production-style — single worker, multi-threaded (see below):
THREADS=8 PORT=8765 bin/start.sh
```

Open <http://127.0.0.1:8765/>.

The composing room is at <http://127.0.0.1:8765/admin/login>. First
boot seeds an `admin / admin` user and a sample post — change the
credentials right after.

## Why `bin/start.sh` is `--workers 1 --threads N` (and not `--workers N`)

Embedded OxiDB is single-process by construction. The B-tree, the WAL,
the document cache, and the FTS worker pool all live as in-process
state next to the running app. Two processes opening the same data
directory would race on writes and corrupt the on-disk B-tree image —
so spawning multiple gunicorn *workers* over one embedded data dir is
a way to lose data, not a way to scale.

The Rust engine is already internally thread-safe (per-collection
RwLocks for concurrent reads, lock-free buckets for hot writes), so
the right concurrency knob for *this* deployment shape is **one
worker, many threads**. `bin/start.sh` defaults to 8 gthread workers
inside one gunicorn process; bump `THREADS=` to taste.

If you actually need multiple OS processes serving the blog, that is
the job of the standalone `oxidb-server` and the pure-Python `oxidb`
TCP client — a different example, not this one. With this app, more
processes do not buy more throughput; more threads do.

## URL map

| Path                              | What it does                                    |
| --------------------------------- | ----------------------------------------------- |
| `/`                               | Archive — newest pieces first                   |
| `/<slug>`                         | Single post                                     |
| `/media/<key>`                    | Streams an image from the `blog-images` bucket  |
| `/admin/login`                    | Sign in to the composing room                   |
| `/admin/`                         | Dashboard — list every post                     |
| `/admin/new`                      | File a new piece (with optional image)          |
| `/admin/edit/<id>`                | Edit, replace / remove the plate                |
| `/admin/delete/<id>`              | Delete                                          |

## How OxiDB shows up in the code

Every persistence call is in `posts/db.py`. Everywhere else is
straight Django — views, URLs, templates. Highlights:

```python
# Get latest posts (uses the created_at index for an O(limit) sort).
db().find(POSTS, {}, sort={"created_at": -1}, limit=50)

# Atomic post + image: store the bytes in the blob bucket first,
# then insert the doc carrying the resulting key.
db().put_object("blog-images", object_key, data,
                content_type="image/png",
                metadata={"hash": "..."})
db().insert(POSTS, {"title": ..., "slug": ..., "image_key": object_key, ...})

# Unique-index-backed slug lookup (O(1) point read).
db().find_one(POSTS, {"slug": slug})
```

## Why no Django ORM?

Mostly to make the OxiDB story explicit — this app doesn't quietly
fall back to SQLite, and there's no "Django models on top of OxiDB"
adapter to debug. Sessions use the signed-cookie backend so we don't
need *any* SQL database, and the auth contrib app is left out
entirely (admin users live in OxiDB's `admins` collection with
PBKDF2-SHA256 password hashes).

## Tuning knobs

Set via environment variables before launching `manage.py`:

| Variable                | Default                         | Effect                         |
| ----------------------- | ------------------------------- | ------------------------------ |
| `BLOG_DATA_DIR`         | `./_data`                       | Where OxiDB persists everything |
| `DJANGO_SECRET_KEY`     | `insecure-dev-key-replace-me…`  | **Replace for any real deploy** |
| `DJANGO_DEBUG`          | `1`                             | `0` to disable debug pages      |
| `DJANGO_ALLOWED_HOSTS`  | `*` (debug) / unset (prod)      | Comma-separated host whitelist  |

## Limitations / things this example deliberately doesn't ship

- No comment system.
- No Markdown — posts are plain text with paragraph-on-blank-line.
- No image resizing. The raw upload is what gets served (cap is
  10 MB; tune via `DATA_UPLOAD_MAX_MEMORY_SIZE` if needed).
- No multi-user / role separation. One "admin" role.
- Single process. For multi-worker deployments, OxiDB's embedded
  mode is single-process-only — front a single gunicorn worker or
  switch to the TCP server.
- OxiDB itself is **not production-ready** — on-disk format and
  APIs can change between releases. See the project's main README.

## License

Same as the parent project — **MIT OR Apache-2.0**, at your option.
