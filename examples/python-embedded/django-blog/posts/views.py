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

def index(request: HttpRequest) -> HttpResponse:
    posts = db.list_posts(limit=50)
    return render(request, "posts/index.html", {"posts": posts})


def detail(request: HttpRequest, slug: str) -> HttpResponse:
    post = db.get_post_by_slug(slug)
    if not post:
        raise Http404("post not found")
    return render(request, "posts/detail.html", {"post": post})


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
    resp["Cache-Control"] = "public, max-age=86400"
    return resp


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

@require_http_methods(["GET", "POST"])
def admin_login(request: HttpRequest) -> HttpResponse:
    next_url = request.GET.get("next") or reverse("admin_dashboard")
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = request.POST.get("password") or ""
        user = db.authenticate(username, password)
        if user:
            request.session["admin_user"] = user["username"]
            return HttpResponseRedirect(next_url)
        messages.error(request, "Wrong username or password.")
    return render(request, "posts/admin/login.html", {"next": next_url})


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
