from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("robots.txt", views.robots, name="robots"),
    path("sitemap.xml", views.sitemap, name="sitemap"),
    path("media/<str:key>", views.media, name="media"),

    path("admin/", views.admin_dashboard, name="admin_dashboard"),
    path("admin/login", views.admin_login, name="admin_login"),
    path("admin/logout", views.admin_logout, name="admin_logout"),
    path("admin/new", views.admin_new_post, name="admin_new_post"),
    path("admin/edit/<int:post_id>", views.admin_edit_post, name="admin_edit_post"),
    path("admin/delete/<int:post_id>", views.admin_delete_post, name="admin_delete_post"),

    # Public post page lives at the root to keep URLs readable. Keep this
    # last so it doesn't shadow the static prefixes above.
    path("<slug:slug>", views.detail, name="detail"),
]
