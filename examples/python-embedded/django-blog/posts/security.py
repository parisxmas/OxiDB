"""
Brute-force defense for the admin login.

Sliding-window failure counter per client IP. Process-local — we run
a single gunicorn worker (embedded OxiDB is single-process), so the
counter is consistent across threads via a simple Lock. If the
deployment ever moves to multi-process (TCP server), this needs to
move into OxiDB or Redis.

Tuning:
  WINDOW_SECONDS = 300   5-minute window
  MAX_FAILURES   = 5     ≥ this many failures inside the window → block
  HARD_CAP_IPS   = 10000 LRU-trim the dict so a scanner can't OOM us
"""
import collections
import threading
import time

WINDOW_SECONDS = 5 * 60
MAX_FAILURES = 5
HARD_CAP_IPS = 10_000

_buckets: dict[str, collections.deque] = {}
_lock = threading.Lock()


def _prune(b: collections.deque, now: float) -> None:
    cutoff = now - WINDOW_SECONDS
    while b and b[0] < cutoff:
        b.popleft()


def login_attempt_blocked(ip: str) -> tuple[bool, int]:
    """Return (is_blocked, seconds_until_oldest_failure_expires)."""
    now = time.monotonic()
    with _lock:
        b = _buckets.get(ip)
        if not b:
            return False, 0
        _prune(b, now)
        if not b:
            return False, 0
        if len(b) < MAX_FAILURES:
            return False, 0
        retry_after = max(1, int(WINDOW_SECONDS - (now - b[0])))
        return True, retry_after


def record_failure(ip: str) -> None:
    """Add one failed-login timestamp for this IP."""
    now = time.monotonic()
    with _lock:
        b = _buckets.get(ip)
        if b is None:
            b = collections.deque()
            _buckets[ip] = b
        _prune(b, now)
        b.append(now)
        # Don't let a scanner with rotating IPs balloon the dict.
        if len(_buckets) > HARD_CAP_IPS:
            # Cheap LRU: drop the IP whose oldest entry is the oldest.
            victim = min(_buckets, key=lambda k: _buckets[k][0] if _buckets[k] else now)
            _buckets.pop(victim, None)


def reset(ip: str) -> None:
    """Clear an IP's record (call on successful auth)."""
    with _lock:
        _buckets.pop(ip, None)


def client_ip(request) -> str:
    """Real client IP, accounting for the nginx → docker hop.

    The container only ever sees REMOTE_ADDR=127.0.0.1 (the docker
    proxy address). The actual client IP is set by nginx via
    `X-Real-IP` (and `X-Forwarded-For` for chains). nginx itself
    pulls it from `CF-Connecting-IP` so the value here is the real
    end user even when Cloudflare is in front.
    """
    real = request.META.get("HTTP_X_REAL_IP")
    if real:
        return real
    fwd = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR") or "?"
