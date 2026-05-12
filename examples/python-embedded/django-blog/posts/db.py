"""
Data layer — wraps oxidb-embedded so views never touch the FFI client
directly. One process-wide OxiDb instance lives in the running Django
worker; it owns the data dir and serves both document collections and
the S3-style blob bucket we use for post images.

Documents (collections):
  - posts   — { _id, title, slug, body, author, created_at, image_key }
  - admins  — { _id, username, password_hash, created_at }

Blobs (bucket):
  - blog-images — keyed by a content-hash, served via /media/<key>
"""

import hashlib
import os
import re
import secrets
import threading
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from oxidb_embedded import OxiDbEmbedded

from .middleware import record_db_call

# -------------------------------------------------------------------------
# Single, lazily-initialized OxiDb instance.
# -------------------------------------------------------------------------

_DB_INSTANCE: Optional[OxiDbEmbedded] = None
_DB_LOCK = threading.Lock()

POSTS = "posts"
ADMINS = "admins"
IMAGES_BUCKET = "blog-images"

# Cosmetic seed: first time we open an empty DB, drop a sample post and
# an admin user in so the experience isn't "log in to make your first
# log-in", and the index page has something to render.
DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASSWORD = "admin"

# Argon2-light: stdlib only. PBKDF2-HMAC-SHA256, 200k iterations.
_PBKDF2_ITERATIONS = 200_000


def _data_dir() -> str:
    p = os.environ.get("BLOG_DATA_DIR")
    if not p:
        p = str(Path(__file__).resolve().parent.parent / "_data")
    Path(p).mkdir(parents=True, exist_ok=True)
    return p


def db() -> OxiDbEmbedded:
    """
    Process-wide embedded OxiDb handle. Created on first call.

    Embedded mode is single-process by design — the data dir holds
    the B-tree, the WAL, and the in-memory caches that the running
    process owns. Two processes opening the same data dir would race
    on writes and shred the B-tree. For Django that translates to:

      - `runserver`              → fine, one process by default.
      - `gunicorn --workers 1
                  --threads N`   → fine, threads share this one
                                   embedded handle; the Rust engine
                                   itself is internally thread-safe.
      - `gunicorn --workers >1`  → DON'T. Use `bin/start.sh` which
                                   pins workers=1 and ramps threads.

    True multi-process needs the standalone `oxidb-server` and the
    pure-Python TCP client — which is a different example.
    """
    global _DB_INSTANCE
    if _DB_INSTANCE is not None:
        return _DB_INSTANCE
    with _DB_LOCK:
        if _DB_INSTANCE is None:
            inst = OxiDbEmbedded(_data_dir())
            _instrument_for_timing(inst)
            _setup_schema(inst)
            _seed_if_empty(inst)
            _DB_INSTANCE = inst
    return _DB_INSTANCE


def _instrument_for_timing(inst: OxiDbEmbedded) -> None:
    """
    Wrap the FFI boundary (`_execute`) so the timing middleware can
    attribute every call to the request that made it. All ORM-style
    helpers in oxidb_embedded — find, insert, put_object, ... — funnel
    through this one method, so wrapping it once instruments the lot.
    """
    original = inst._execute
    def timed(cmd, *args, **kwargs):
        t0 = time.perf_counter()
        try:
            return original(cmd, *args, **kwargs)
        finally:
            record_db_call((time.perf_counter() - t0) * 1000.0)
    inst._execute = timed  # type: ignore[method-assign]


def _setup_schema(d: OxiDbEmbedded) -> None:
    """Indexes that make our lookups fast. All idempotent."""
    try:
        d.create_unique_index(POSTS, "slug")
    except Exception:
        pass
    try:
        d.create_index(POSTS, "created_at")
    except Exception:
        pass
    try:
        # Full-text index on title + body. BM25 ranking, English
        # tokenization by default (override with OXIDB_FTS_LANG).
        # On first creation, OxiDB scans the existing collection
        # and indexes every document — so adding this knob ships
        # search to a populated collection without a manual reindex
        # step. Idempotent: re-creating on next boot is a no-op.
        d.create_text_index(POSTS, ["title", "body"])
    except Exception:
        pass
    try:
        d.create_unique_index(ADMINS, "username")
    except Exception:
        pass
    try:
        d.create_bucket(IMAGES_BUCKET)
    except Exception:
        pass


def _seed_if_empty(d: OxiDbEmbedded) -> None:
    """
    Idempotent seed. Threads within the single embedded process are
    serialized by `_DB_LOCK` around `db()`, so this only runs once —
    the try/except is belt-and-suspenders against future callers that
    skip the lock or share a data dir across processes by mistake.
    """
    if d.count(ADMINS) == 0:
        try:
            d.insert(ADMINS, {
                "username": DEFAULT_ADMIN_USER,
                "password_hash": hash_password(DEFAULT_ADMIN_PASSWORD),
                "created_at": _now(),
            })
        except Exception:
            pass
    # Idempotent over SEED_POSTS: skip any whose slug already exists.
    # This lets us extend seed_data.py later and pick up the new
    # entries on next boot without redundant inserts or duplicates.
    from .seed_data import SEED_POSTS
    for post in SEED_POSTS:
        if d.find_one(POSTS, {"slug": post["slug"]}):
            continue
        try:
            d.insert(POSTS, post)
        except Exception:
            pass


# -------------------------------------------------------------------------
# Posts
# -------------------------------------------------------------------------

def _expose(doc: Optional[dict]) -> Optional[dict]:
    """Django templates can't access attributes with a leading underscore,
    so mirror `_id` onto a plain `id` key before handing docs to a view."""
    if doc is None:
        return None
    if "_id" in doc and "id" not in doc:
        doc["id"] = doc["_id"]
    return doc


def list_posts(limit: int = 50, skip: int = 0) -> list[dict]:
    return [
        _expose(d)
        for d in db().find(POSTS, {}, sort={"created_at": -1}, limit=limit, skip=skip)
    ]


def count_posts() -> int:
    """Total count — used by pagination on the index page."""
    return db().count(POSTS)


def search_posts(query: str, limit: int = 25) -> list[dict]:
    """
    BM25-ranked full-text search across `title` + `body`. Returns
    matching post dicts (same shape as `list_posts`) ordered by
    relevance.

    OxiDB's `text_search` returns score-ordered raw docs. The wrapper
    exposes the same `id` alias as `list_posts` for template access.
    """
    q = (query or "").strip()
    if not q:
        return []
    try:
        results = db().text_search(POSTS, q, limit=limit)
    except Exception:
        return []
    return [_expose(d) for d in results]


def get_post_by_slug(slug: str) -> Optional[dict]:
    return _expose(db().find_one(POSTS, {"slug": slug}))


def get_post_by_id(post_id: int) -> Optional[dict]:
    return _expose(db().find_one(POSTS, {"_id": post_id}))


def create_post(*, title: str, body: str, author: str,
                image_bytes: Optional[bytes] = None,
                image_content_type: Optional[str] = None) -> dict:
    slug = _unique_slug(title)
    image_key = None
    if image_bytes:
        image_key = _store_image(image_bytes, image_content_type or "application/octet-stream")
    doc = {
        "title": title.strip(),
        "slug": slug,
        "body": body.strip(),
        "author": author,
        "created_at": _now(),
        "image_key": image_key,
    }
    result = db().insert(POSTS, doc)
    doc["_id"] = result["id"]
    return doc


def update_post(post_id: int, *, title: Optional[str] = None,
                body: Optional[str] = None,
                image_bytes: Optional[bytes] = None,
                image_content_type: Optional[str] = None,
                remove_image: bool = False) -> None:
    changes: dict = {}
    if title is not None:
        changes["title"] = title.strip()
        # If title changed, refresh the slug (but check uniqueness).
        existing = get_post_by_id(post_id)
        if existing and existing["title"] != title.strip():
            changes["slug"] = _unique_slug(title, exclude_id=post_id)
    if body is not None:
        changes["body"] = body.strip()
    if image_bytes:
        changes["image_key"] = _store_image(image_bytes, image_content_type or "application/octet-stream")
    elif remove_image:
        changes["image_key"] = None
    if not changes:
        return
    db().update_one(POSTS, {"_id": post_id}, {"$set": changes})


def delete_post(post_id: int) -> None:
    db().delete_one(POSTS, {"_id": post_id})


# -------------------------------------------------------------------------
# Admin / auth
# -------------------------------------------------------------------------

def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${_PBKDF2_ITERATIONS}${salt.hex()}${derived.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, iters, salt_hex, digest_hex = stored.split("$")
    except ValueError:
        return False
    if scheme != "pbkdf2_sha256":
        return False
    derived = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(iters)
    )
    return secrets.compare_digest(derived.hex(), digest_hex)


def authenticate(username: str, password: str) -> Optional[dict]:
    user = db().find_one(ADMINS, {"username": username})
    if not user:
        return None
    if not verify_password(password, user["password_hash"]):
        return None
    return user


# -------------------------------------------------------------------------
# Image storage (S3-style buckets)
# -------------------------------------------------------------------------

def _store_image(data: bytes, content_type: str) -> str:
    """Hash-keyed put. Returns the key (which is also the URL slug)."""
    key = hashlib.sha256(data).hexdigest()[:24]
    # Pick a sensible extension so the served URL is nice.
    ext = _ext_for_content_type(content_type)
    object_key = f"{key}{ext}"
    db().put_object(
        IMAGES_BUCKET, object_key, data,
        content_type=content_type,
        metadata={"hash": key},
    )
    return object_key


def get_image(key: str) -> Optional[tuple[bytes, dict]]:
    try:
        return db().get_object(IMAGES_BUCKET, key)
    except Exception:
        return None


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _slugify(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[\s_-]+", "-", value) or "post"


def _unique_slug(title: str, exclude_id: Optional[int] = None) -> str:
    base = _slugify(title)
    candidate = base
    i = 1
    while True:
        existing = db().find_one(POSTS, {"slug": candidate})
        if not existing or (exclude_id is not None and existing["_id"] == exclude_id):
            return candidate
        i += 1
        candidate = f"{base}-{i}"


def _ext_for_content_type(content_type: str) -> str:
    mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/avif": ".avif",
    }
    return mapping.get(content_type.lower(), "")
