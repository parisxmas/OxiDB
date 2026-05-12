"""
Views — Django request handling on top of the OxiDB data layer (`db.py`).

There is no Django ORM in this project. Models are plain dicts that
come back from `db.list_posts`, `db.get_post_by_slug`, etc., and the
admin pages talk to the same helpers.

Auth: signed-cookie sessions store the authenticated username under
`session["admin_user"]`. The `admin_required` decorator gates the
write-side views.
"""

from functools import wraps
from typing import Callable

from django.contrib import messages
from django.http import (Http404, HttpRequest, HttpResponse,
                         HttpResponseBadRequest, HttpResponseRedirect)
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.http import require_http_methods

from . import db
from .security import (client_ip, login_attempt_blocked, record_failure,
                       reset as reset_login_rate, MAX_FAILURES, WINDOW_SECONDS)


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def admin_required(view: Callable) -> Callable:
    @wraps(view)
    def wrapped(request: HttpRequest, *args, **kwargs):
        if not request.session.get("admin_user"):
            return HttpResponseRedirect(
                reverse("admin_login") + f"?next={request.path}"
            )
        return view(request, *args, **kwargs)
    return wrapped


# ---------------------------------------------------------------------------
# Public pages
# ---------------------------------------------------------------------------

PAGE_SIZE = 8


def index(request: HttpRequest) -> HttpResponse:
    total = db.count_posts()
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    try:
        page = max(1, int(request.GET.get("page", "1")))
    except ValueError:
        page = 1
    if page > total_pages:
        page = total_pages
    posts = db.list_posts(limit=PAGE_SIZE, skip=(page - 1) * PAGE_SIZE)
    return render(request, "posts/index.html", {
        "posts": posts,
        "page": page,
        "total_pages": total_pages,
        "total_posts": total,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "page_numbers": list(range(1, total_pages + 1)),
        # Display-only index continuation across pages: post #1, #2, ...
        # starts at (page-1)*PAGE_SIZE + 1.
        "offset": (page - 1) * PAGE_SIZE,
    })


def search(request: HttpRequest) -> HttpResponse:
    """BM25 full-text search across the posts collection. The
    embedded OxiDB FTS index is built and queried in-process —
    no separate search service, no inverted-index file format
    on the host."""
    q = (request.GET.get("q") or "").strip()
    posts = db.search_posts(q, limit=50) if q else []
    return render(request, "posts/search.html", {
        "q": q,
        "posts": posts,
        "result_count": len(posts),
    })


def detail(request: HttpRequest, slug: str) -> HttpResponse:
    post = db.get_post_by_slug(slug)
    if not post:
        raise Http404("post not found")
    return render(request, "posts/detail.html", {"post": post})


def robots(request: HttpRequest) -> HttpResponse:
    """Minimal robots.txt — crawlers welcome everywhere; sitemap reference
    helps search engines discover the full archive at once."""
    sitemap_url = request.build_absolute_uri(reverse("sitemap"))
    resp = HttpResponse(
        "User-agent: *\n"
        "Allow: /\n"
        f"\nSitemap: {sitemap_url}\n",
        content_type="text/plain; charset=utf-8",
    )
    resp["Cache-Control"] = "public, max-age=86400"
    return resp


def sitemap(request: HttpRequest) -> HttpResponse:
    """Sitemap.xml — index + every post detail + paged archive pages.

    Search engines use this to discover URLs faster than crawling
    link-by-link. We emit per-post lastmod from the post's created_at
    timestamp so revisits know whether the content changed.
    """
    from xml.sax.saxutils import escape

    posts = db.list_posts(limit=10_000)
    base = f"{request.scheme}://{request.get_host()}"

    urls = []
    # Home + paged archive
    total_pages = max(1, (db.count_posts() + PAGE_SIZE - 1) // PAGE_SIZE)
    urls.append((f"{base}/", "weekly", "1.0", None))
    for p in range(2, total_pages + 1):
        urls.append((f"{base}/?page={p}", "weekly", "0.7", None))
    # Detail pages
    for post in posts:
        lastmod = post.get("created_at", "")[:10] or None
        urls.append((
            f"{base}{reverse('detail', args=[post['slug']])}",
            "monthly", "0.8", lastmod,
        ))

    body = ['<?xml version="1.0" encoding="UTF-8"?>',
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for loc, freq, prio, lastmod in urls:
        body.append("  <url>")
        body.append(f"    <loc>{escape(loc)}</loc>")
        if lastmod:
            body.append(f"    <lastmod>{lastmod}</lastmod>")
        body.append(f"    <changefreq>{freq}</changefreq>")
        body.append(f"    <priority>{prio}</priority>")
        body.append("  </url>")
    body.append("</urlset>")
    resp = HttpResponse("\n".join(body), content_type="application/xml; charset=utf-8")
    resp["Cache-Control"] = "public, max-age=86400"
    return resp


def media(request: HttpRequest, key: str) -> HttpResponse:
    """
    Serve an image straight from the OxiDB blob bucket. In a real
    deployment you would put a CDN (or nginx + X-Accel-Redirect, or
    presigned S3 URLs) in front of this; for the example app, a plain
    streaming response is plenty.
    """
    obj = db.get_image(key)
    if obj is None:
        raise Http404("image not found")
    data, meta = obj
    content_type = meta.get("content_type", "application/octet-stream")
    resp = HttpResponse(data, content_type=content_type)
    # The URL key is the sha256 of the bytes, so the content can never
    # change for a given URL. Long-cache + immutable; CDN, browsers,
    # and any intermediate proxy can hold it indefinitely.
    resp["Cache-Control"] = "public, max-age=31536000, immutable"
    return resp


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

@require_http_methods(["GET", "POST"])
def admin_login(request: HttpRequest) -> HttpResponse:
    next_url = request.GET.get("next") or reverse("admin_dashboard")
    ip = client_ip(request)
    blocked, retry = login_attempt_blocked(ip)

    if request.method == "POST":
        if blocked:
            # Refuse before checking the password — both saves CPU on
            # the hash compare and avoids the timing oracle.
            resp = render(request, "posts/admin/login.html", {
                "next": next_url,
                "blocked": True,
                "retry_after": retry,
                "max_failures": MAX_FAILURES,
                "window_minutes": WINDOW_SECONDS // 60,
            }, status=429)
            resp["Retry-After"] = str(retry)
            return resp

        username = (request.POST.get("username") or "").strip()
        password = request.POST.get("password") or ""
        user = db.authenticate(username, password)
        if user:
            reset_login_rate(ip)
            request.session["admin_user"] = user["username"]
            return HttpResponseRedirect(next_url)
        record_failure(ip)
        # Re-check — the failure we just recorded may have crossed
        # the threshold, in which case the next page render should
        # already say "you're locked out".
        blocked, retry = login_attempt_blocked(ip)
        if blocked:
            messages.error(request, "Too many failed attempts. Try later.")
        else:
            messages.error(request, "Wrong username or password.")

    return render(request, "posts/admin/login.html", {
        "next": next_url,
        "blocked": blocked,
        "retry_after": retry,
        "max_failures": MAX_FAILURES,
        "window_minutes": WINDOW_SECONDS // 60,
    })


def admin_logout(request: HttpRequest) -> HttpResponse:
    request.session.pop("admin_user", None)
    return HttpResponseRedirect(reverse("index"))


@admin_required
def admin_dashboard(request: HttpRequest) -> HttpResponse:
    posts = db.list_posts(limit=200)
    return render(request, "posts/admin/list.html", {
        "posts": posts,
        "current_user": request.session.get("admin_user"),
    })


@admin_required
@require_http_methods(["GET", "POST"])
def admin_new_post(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        title = (request.POST.get("title") or "").strip()
        body = (request.POST.get("body") or "").strip()
        if not title or not body:
            messages.error(request, "Title and body are required.")
            return render(request, "posts/admin/edit.html", {
                "post": None,
                "form_action": reverse("admin_new_post"),
                "form": {"title": title, "body": body},
            })
        image = request.FILES.get("image")
        post = db.create_post(
            title=title, body=body,
            author=request.session["admin_user"],
            image_bytes=image.read() if image else None,
            image_content_type=image.content_type if image else None,
        )
        messages.success(request, f"Posted: {post['title']}")
        return HttpResponseRedirect(reverse("admin_dashboard"))
    return render(request, "posts/admin/edit.html", {
        "post": None,
        "form_action": reverse("admin_new_post"),
        "form": {"title": "", "body": ""},
    })


@admin_required
@require_http_methods(["GET", "POST"])
def admin_edit_post(request: HttpRequest, post_id: int) -> HttpResponse:
    post = db.get_post_by_id(post_id)
    if not post:
        raise Http404("post not found")
    if request.method == "POST":
        title = (request.POST.get("title") or "").strip()
        body = (request.POST.get("body") or "").strip()
        if not title or not body:
            messages.error(request, "Title and body are required.")
            return render(request, "posts/admin/edit.html", {
                "post": post,
                "form_action": reverse("admin_edit_post", args=[post_id]),
                "form": {"title": title, "body": body},
            })
        image = request.FILES.get("image")
        db.update_post(
            post_id,
            title=title, body=body,
            image_bytes=image.read() if image else None,
            image_content_type=image.content_type if image else None,
            remove_image=request.POST.get("remove_image") == "1",
        )
        messages.success(request, "Saved.")
        return HttpResponseRedirect(reverse("admin_dashboard"))
    return render(request, "posts/admin/edit.html", {
        "post": post,
        "form_action": reverse("admin_edit_post", args=[post_id]),
        "form": {"title": post["title"], "body": post["body"]},
    })


@admin_required
@require_http_methods(["POST"])
def admin_delete_post(request: HttpRequest, post_id: int) -> HttpResponse:
    post = db.get_post_by_id(post_id)
    if not post:
        raise Http404("post not found")
    db.delete_post(post_id)
    messages.success(request, f"Deleted: {post['title']}")
    return HttpResponseRedirect(reverse("admin_dashboard"))
