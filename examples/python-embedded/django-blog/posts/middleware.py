"""
Request-scoped timing — exposed in the page footer for the example.

We track two numbers:

  * Page generation time — from middleware entry to middleware exit,
    so it covers the view AND the template render. Measured once,
    then substituted into the rendered HTML via a `<!--TIMING-->`
    placeholder the base template emits.

  * OxiDB time + call count — accumulated by wrapping
    `OxiDbEmbedded._execute` so we capture the FFI boundary itself
    (the Python ctypes call + the engine round-trip). All ORM-style
    helpers funnel through `_execute`, so one wrap covers everything.

Per-request isolation uses `contextvars` rather than thread locals so
this stays correct under ASGI / async views too.
"""

import time
from contextvars import ContextVar

#: Cumulative wall time spent inside OxiDB calls during the request.
db_time_ms: ContextVar[float] = ContextVar("oxidb_blog_db_time_ms", default=0.0)
#: Number of FFI round-trips during the request.
db_calls: ContextVar[int] = ContextVar("oxidb_blog_db_calls", default=0)

# Marker the base template prints in the colophon — middleware swaps
# it for the actual timing line after the response is rendered.
PLACEHOLDER = "<!--TIMING-->"


def record_db_call(elapsed_ms: float) -> None:
    """Called from the OxiDb wrapper after every `_execute`."""
    db_time_ms.set(db_time_ms.get() + elapsed_ms)
    db_calls.set(db_calls.get() + 1)


class TimingMiddleware:
    """Capture page gen + DB timings, inject into the response footer."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Reset per-request counters. ContextVars are isolated per
        # request automatically (each request runs in its own
        # asynccontext / coroutine / threadlocal context).
        db_time_ms.set(0.0)
        db_calls.set(0)
        t0 = time.perf_counter()

        response = self.get_response(request)

        total_ms = (time.perf_counter() - t0) * 1000.0
        db_ms = db_time_ms.get()
        n = db_calls.get()

        ctype = response.get("Content-Type", "")
        if "text/html" in ctype and response.content:
            # The template prints the placeholder in the colophon —
            # if it's not there (e.g. an admin redirect template),
            # we silently skip.
            try:
                body = response.content.decode("utf-8")
            except UnicodeDecodeError:
                return response
            if PLACEHOLDER in body:
                pages = f"<code>{total_ms:.2f}ms</code>"
                db = f"<code>{db_ms:.2f}ms</code>"
                count = f"<code>{n}</code> call{'' if n == 1 else 's'}"
                ratio = "" if total_ms == 0 else f" · {db_ms / total_ms * 100:.0f}% in db"
                replacement = (
                    f"<span class=\"timing\">page {pages} · oxidb {db} across {count}"
                    f"{ratio}</span>"
                )
                body = body.replace(PLACEHOLDER, replacement, 1)
                encoded = body.encode("utf-8")
                response.content = encoded
                if response.has_header("Content-Length"):
                    response["Content-Length"] = str(len(encoded))

        # Server-Timing header so devtools picks it up too.
        response["Server-Timing"] = (
            f"app;dur={total_ms:.2f}, oxidb;dur={db_ms:.2f};desc=\"{n} calls\""
        )
        return response
