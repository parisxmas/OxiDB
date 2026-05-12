"""
Minimal Django settings for an OxiDB-backed example blog.

Stripped down on purpose: no ORM, no auth contrib app, no built-in
admin. Sessions live in signed cookies so we don't need a SQL backend
at all. Everything persistent lives in the OxiDB data dir.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY",
    # Development default. Set DJANGO_SECRET_KEY for any real deployment.
    "insecure-dev-key-replace-me-via-env",
)

DEBUG = os.environ.get("DJANGO_DEBUG", "1") == "1"

# ---------------------------------------------------------------------------
# Security hardening (active only when DEBUG=False; keeps dev simple).
# Everything below is what bumps Lighthouse Best Practices into the 90s.
# ---------------------------------------------------------------------------
if not DEBUG:
    # HSTS — 1 year, includes subdomains, preload-list eligible.
    SECURE_HSTS_SECONDS = 31536000
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True
    # Send the canonical scheme to the browser even when the request
    # arrives over HTTP (cookies, redirects).
    SECURE_REFERRER_POLICY = "strict-origin-when-cross-origin"
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True

# Always-on hardening:
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = "DENY"

ALLOWED_HOSTS = ["*"] if DEBUG else os.environ.get("DJANGO_ALLOWED_HOSTS", "").split(",")

# We sit behind nginx (which sits behind Cloudflare). Trust nginx's
# X-Forwarded-Proto so request.is_secure() reports the real scheme;
# without this every request looks HTTP to Django and the CSRF
# checker compares HTTP-side Origin to an HTTPS-side referer, which
# fails every POST.
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# CSRF Origin/Referer whitelist for the public hostnames. Django 4+
# requires this list to be filled when accepting POSTs from a
# scheme/host that doesn't match the cookie's host.
_csrf_origins = [o for o in os.environ.get("DJANGO_CSRF_TRUSTED_ORIGINS", "").split(",") if o]
if not _csrf_origins:
    _csrf_origins = [
        "https://oxidb-embedded-django.baltavista.com",
        "http://oxidb-embedded-django.baltavista.com",
    ]
CSRF_TRUSTED_ORIGINS = _csrf_origins

# Bare-minimum apps. We deliberately skip:
#   - django.contrib.admin     (we ship our own thin admin)
#   - django.contrib.auth      (admin users live in OxiDB)
#   - django.contrib.contenttypes (only needed for the above)
# Sessions stay — backed by signed cookies, no DB needed.
INSTALLED_APPS = [
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "posts",
]

MIDDLEWARE = [
    # First in the chain so it captures the full request lifecycle,
    # including session decode + CSRF setup. Has to run before
    # gzip/content-length-sensitive middleware (we have none here).
    "posts.middleware.TimingMiddleware",
    # CSPNonceMiddleware MUST come before any view rendering so the
    # nonce is available in `request.csp_nonce` by the time the
    # template engine fires.
    "posts.middleware.CSPNonceMiddleware",
    "django.middleware.security.SecurityMiddleware",
    # WhiteNoise serves the collected static assets straight from the
    # gunicorn worker so we don't need an nginx/caddy in front for the
    # CSS to load. Has to come immediately after SecurityMiddleware
    # per WhiteNoise's docs.
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

# Cookie-backed sessions: no SQL backend required.
SESSION_ENGINE = "django.contrib.sessions.backends.signed_cookies"
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

ROOT_URLCONF = "blog.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "blog.wsgi.application"

# No SQL database. The dictionary stays empty.
DATABASES = {}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "posts" / "static"] if (BASE_DIR / "posts" / "static").exists() else []
# WhiteNoise serves files from STATIC_ROOT under gunicorn — `runserver`
# bypasses this and serves directly from STATICFILES_DIRS.
STATIC_ROOT = BASE_DIR / "staticfiles"
# Storage strategy:
#   prod (DEBUG=False) → content-hashed filenames (`style.abc123.css`)
#                        + gzip variants. WhiteNoise auto-applies
#                        `Cache-Control: public, max-age=31536000,
#                        immutable` to anything matching its hashed-
#                        filename pattern.
#   dev  (DEBUG=True)  → no hashing; the staticfiles finder serves
#                        unhashed URLs directly so you don't have to
#                        run `collectstatic` between every edit.
_static_backend = (
    "whitenoise.storage.CompressedManifestStaticFilesStorage"
    if not DEBUG else
    "whitenoise.storage.CompressedStaticFilesStorage"
)
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": _static_backend},
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Max upload size — slightly generous for blog hero images.
DATA_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024  # 10 MB
FILE_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024
