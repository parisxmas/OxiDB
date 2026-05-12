"""
Custom template tags for the blog. Currently:

  {% inline_static "posts/style.css" %}

  Reads the static file off disk at template-render time and emits a
  `<style>...</style>` block. Used to inline the stylesheet so the
  browser doesn't have to do an HTML → CSS round trip — eliminates
  the "chaining critical requests" Lighthouse insight at the cost of
  losing CSS cacheability across page navigations (acceptable trade
  for a small site whose HTML is dynamic anyway).

  Reads are cached process-locally — the file barely changes after
  collectstatic runs at image build time.
"""
import functools
import os

from django.contrib.staticfiles import finders
from django.contrib.staticfiles.storage import staticfiles_storage
from django.template import Library
from django.utils.safestring import mark_safe

register = Library()


@functools.lru_cache(maxsize=32)
def _read_static_bytes(orig_path: str) -> str:
    """Load a static file's contents. Resolves the file via the
    same finder + storage chain the {% static %} tag uses, so it
    works with ManifestStaticFilesStorage's hashed paths (in prod)
    AND with the raw STATICFILES_DIRS in dev."""
    # Try staticfiles_storage first (prod path — STATIC_ROOT after
    # collectstatic). Works for both Manifest and Compressed storage.
    try:
        url = staticfiles_storage.url(orig_path)
        # url returns something like /static/posts/style.<hash>.css.
        # Convert it back to a filesystem path: strip STATIC_URL prefix,
        # join with STATIC_ROOT.
        from django.conf import settings
        if url.startswith(settings.STATIC_URL):
            rel = url[len(settings.STATIC_URL):]
            fs = os.path.join(settings.STATIC_ROOT, rel)
            if os.path.isfile(fs):
                with open(fs, "r", encoding="utf-8") as f:
                    return f.read()
    except Exception:
        pass

    # Dev path: hit the finders directly (no STATIC_ROOT yet).
    found = finders.find(orig_path)
    if found and os.path.isfile(found):
        with open(found, "r", encoding="utf-8") as f:
            return f.read()
    return ""


@register.simple_tag(takes_context=True)
def inline_static(context, path: str) -> str:
    """Emit `<style nonce=…>{content}</style>` for the named static
    asset. The nonce comes from CSPNonceMiddleware via request — strict
    CSP without nonce would block this tag."""
    body = _read_static_bytes(path)
    if not body:
        return mark_safe("")
    request = context.get("request")
    nonce_attr = ""
    if request is not None and getattr(request, "csp_nonce", None):
        nonce_attr = f' nonce="{request.csp_nonce}"'
    return mark_safe(f"<style{nonce_attr}>{body}</style>")
