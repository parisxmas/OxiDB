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

ALLOWED_HOSTS = ["*"] if DEBUG else os.environ.get("DJANGO_ALLOWED_HOSTS", "").split(",")

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
    "django.middleware.security.SecurityMiddleware",
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

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Max upload size — slightly generous for blog hero images.
DATA_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024  # 10 MB
FILE_UPLOAD_MAX_MEMORY_SIZE = 10 * 1024 * 1024
