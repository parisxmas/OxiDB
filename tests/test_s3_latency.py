#!/usr/bin/env python3
"""
OxiDB S3 — Connection & Response Time Benchmark

Measures separately:
  - TCP connect time
  - TLS handshake (N/A for plain HTTP)
  - Time to first byte (TTFB)
  - Transfer time
  - Total response time

Uses raw sockets for precision, no boto3 overhead.
"""

import os, sys, time, socket, hashlib, subprocess, shutil, tempfile, struct
import hmac as hmac_mod, datetime

S3_PORT = 9100
HOST = "127.0.0.1"
ACCESS_KEY = "oxidb-test-key"
SECRET_KEY = "oxidb-test-secret-2026"
BUCKET = "latency-test"
DATA_DIR = None
SERVER_PROC = None


def start_server(data_dir):
    global SERVER_PROC
    env = os.environ.copy()
    env["OXIDB_DATA"] = data_dir
    env["OXIDB_S3_PORT"] = str(S3_PORT)
    env["OXIDB_S3_ACCESS_KEY"] = ACCESS_KEY
    env["OXIDB_S3_SECRET_KEY"] = SECRET_KEY
    env["OXIDB_ADDR"] = "127.0.0.1:0"
    server_bin = os.environ.get("OXIDB_SERVER_BIN", "/tmp/oxidb-target/debug/oxidb-server")
    if not os.path.exists(server_bin):
        print(f"Binary not found: {server_bin}")
        sys.exit(1)
    SERVER_PROC = subprocess.Popen([server_bin], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for _ in range(50):
        try:
            s = socket.create_connection((HOST, S3_PORT), timeout=0.2)
            s.close()
            return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    print("Server failed to start")
    sys.exit(1)


def stop_server():
    global SERVER_PROC
    if SERVER_PROC:
        SERVER_PROC.terminate()
        try: SERVER_PROC.wait(timeout=5)
        except: SERVER_PROC.kill()
        SERVER_PROC = None


# ── AWS SigV4 signing (minimal, for raw requests) ───────────────────────

def sign_request(method, path, headers, body=b"", query=""):
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    region = "us-east-1"
    service = "s3"
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"

    headers["x-amz-date"] = amz_date
    payload_hash = hashlib.sha256(body).hexdigest()
    headers["x-amz-content-sha256"] = payload_hash

    signed_header_keys = sorted(headers.keys())
    signed_headers_str = ";".join(signed_header_keys)
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in signed_header_keys)

    canonical_request = f"{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers_str}\n{payload_hash}"
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"

    def hmac_sha256(key, msg):
        return hmac_mod.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = hmac_sha256(f"AWS4{SECRET_KEY}".encode(), date_stamp)
    k_region = hmac_mod.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac_mod.new(k_region, service.encode(), hashlib.sha256).digest()
    k_signing = hmac_mod.new(k_service, b"aws4_request", hashlib.sha256).digest()
    signature = hmac_mod.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers_str}, Signature={signature}"
    )
    return headers


def raw_request(method, path, body=b"", extra_headers=None, query=""):
    """Send raw HTTP request, return (status, headers_dict, body_bytes, timings)."""
    headers = {"host": f"{HOST}:{S3_PORT}"}
    if extra_headers:
        headers.update(extra_headers)
    if body:
        headers["content-length"] = str(len(body))

    headers = sign_request(method, path, headers, body, query)

    full_path = path if not query else f"{path}?{query}"
    req_line = f"{method} {full_path} HTTP/1.1\r\n"
    for k, v in headers.items():
        req_line += f"{k}: {v}\r\n"
    req_line += "\r\n"
    req_bytes = req_line.encode() + body

    # ── Measure connect ──
    t_start = time.perf_counter()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect((HOST, S3_PORT))
    t_connected = time.perf_counter()

    # ── Send request ──
    sock.sendall(req_bytes)
    t_sent = time.perf_counter()

    # ── Read first byte ──
    sock.settimeout(10)
    first = sock.recv(1)
    t_first_byte = time.perf_counter()

    # ── Read rest of response ──
    buf = first
    while True:
        try:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
        except socket.timeout:
            break
    t_done = time.perf_counter()
    sock.close()

    # Parse response
    header_end = buf.find(b"\r\n\r\n")
    resp_headers_raw = buf[:header_end].decode(errors="replace")
    resp_body = buf[header_end + 4:]

    status_line = resp_headers_raw.split("\r\n")[0]
    status = int(status_line.split(" ")[1])

    resp_headers = {}
    for line in resp_headers_raw.split("\r\n")[1:]:
        if ": " in line:
            k, v = line.split(": ", 1)
            resp_headers[k.lower()] = v

    timings = {
        "connect": (t_connected - t_start) * 1000,
        "send": (t_sent - t_connected) * 1000,
        "ttfb": (t_first_byte - t_sent) * 1000,
        "transfer": (t_done - t_first_byte) * 1000,
        "total": (t_done - t_start) * 1000,
    }
    return status, resp_headers, resp_body, timings


def avg_timings(fn, n=10):
    """Run fn n times, return averaged timings."""
    results = []
    for _ in range(n):
        _, _, _, t = fn()
        results.append(t)
    avg = {}
    for key in results[0]:
        avg[key] = sum(r[key] for r in results) / len(results)
    return avg


def main():
    global DATA_DIR
    DATA_DIR = tempfile.mkdtemp(prefix="oxidb_s3_lat_")

    try:
        print("Starting OxiDB server ...")
        start_server(DATA_DIR)
        print("Server ready.\n")

        # Setup: create bucket + objects
        raw_request("PUT", f"/{BUCKET}")

        sizes = {
            "1 KB":    1024,
            "10 KB":   10 * 1024,
            "100 KB":  100 * 1024,
            "1 MB":    1048576,
            "5 MB":    5 * 1048576,
            "10 MB":   10 * 1048576,
        }
        test_data = {}
        for label, sz in sizes.items():
            data = os.urandom(sz)
            test_data[label] = data
            key = f"bench-{sz}.bin"
            status, _, _, _ = raw_request(
                "PUT", f"/{BUCKET}/{key}", body=data,
                extra_headers={"content-type": "application/octet-stream"})
            assert status == 200, f"upload {label} failed: {status}"

        W = 96
        N = 10  # iterations per benchmark

        print("=" * W)
        print("  OxiDB S3 — Connection & Response Time Report")
        print("=" * W)
        print(f"  Host: {HOST}:{S3_PORT}  |  Iterations: {N} per operation  |  Auth: SigV4")
        print("=" * W)

        # ── 1. Connection-only benchmark ─────────────────────────────
        print(f"\n  [TCP Connection Time]")
        print(f"  {'Operation':<30} {'Connect':>10} {'Send':>10} {'TTFB':>10} {'Transfer':>10} {'Total':>10}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

        # Pure TCP connect (no request)
        connect_times = []
        for _ in range(N):
            t0 = time.perf_counter()
            s = socket.create_connection((HOST, S3_PORT))
            elapsed = (time.perf_counter() - t0) * 1000
            s.close()
            connect_times.append(elapsed)
        avg_connect = sum(connect_times) / len(connect_times)
        min_connect = min(connect_times)
        max_connect = max(connect_times)
        print(f"  {'TCP connect (raw)':<30} avg={avg_connect:>6.3f} ms  min={min_connect:.3f}  max={max_connect:.3f}")

        # ── 2. Request latency breakdown ─────────────────────────────
        print(f"\n  [Request Latency Breakdown]  (avg of {N} iterations)")
        print(f"  {'Operation':<30} {'Connect':>10} {'Send':>10} {'TTFB':>10} {'Transfer':>10} {'Total':>10}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

        benchmarks = [
            ("ListBuckets",    lambda: raw_request("GET", "/")),
            ("HeadBucket",     lambda: raw_request("HEAD", f"/{BUCKET}")),
            ("ListObjects",    lambda: raw_request("GET", f"/{BUCKET}")),
            ("HeadObject 1KB", lambda: raw_request("HEAD", f"/{BUCKET}/bench-1024.bin")),
        ]

        for name, fn in benchmarks:
            avg = avg_timings(fn, N)
            print(f"  {name:<30} {avg['connect']:>9.3f}  {avg['send']:>9.3f}  {avg['ttfb']:>9.3f}  {avg['transfer']:>9.3f}  {avg['total']:>9.3f}")

        # ── 3. GET latency by size ───────────────────────────────────
        print(f"\n  [GET Object — Latency by Size]  (avg of {N} iterations)")
        print(f"  {'Size':<30} {'Connect':>10} {'Send':>10} {'TTFB':>10} {'Transfer':>10} {'Total':>10} {'Throughput':>12}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")

        for label, sz in sizes.items():
            key = f"bench-{sz}.bin"
            avg = avg_timings(lambda k=key: raw_request("GET", f"/{BUCKET}/{k}"), N)
            tp = (sz / 1048576) / (avg["total"] / 1000) if avg["total"] > 0 else 0
            print(f"  {f'GET {label}':<30} {avg['connect']:>9.3f}  {avg['send']:>9.3f}  {avg['ttfb']:>9.3f}  {avg['transfer']:>9.3f}  {avg['total']:>9.3f}  {tp:>9.1f} MB/s")

        # ── 4. PUT latency by size ───────────────────────────────────
        print(f"\n  [PUT Object — Latency by Size]  (avg of {N} iterations)")
        print(f"  {'Size':<30} {'Connect':>10} {'Send':>10} {'TTFB':>10} {'Transfer':>10} {'Total':>10} {'Throughput':>12}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")

        for label, sz in sizes.items():
            data = test_data[label]
            key = f"bench-put-{sz}.bin"
            avg = avg_timings(
                lambda d=data, k=key: raw_request("PUT", f"/{BUCKET}/{k}", body=d,
                    extra_headers={"content-type": "application/octet-stream"}), N)
            tp = (sz / 1048576) / (avg["total"] / 1000) if avg["total"] > 0 else 0
            print(f"  {f'PUT {label}':<30} {avg['connect']:>9.3f}  {avg['send']:>9.3f}  {avg['ttfb']:>9.3f}  {avg['transfer']:>9.3f}  {avg['total']:>9.3f}  {tp:>9.1f} MB/s")

        # ── 5. DELETE latency ────────────────────────────────────────
        print(f"\n  [DELETE Object — Latency]  (avg of {N} iterations)")
        print(f"  {'Operation':<30} {'Connect':>10} {'Send':>10} {'TTFB':>10} {'Transfer':>10} {'Total':>10}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

        # Create objects to delete
        for i in range(N):
            raw_request("PUT", f"/{BUCKET}/del-{i}.bin", body=b"x")
        del_times = []
        for i in range(N):
            _, _, _, t = raw_request("DELETE", f"/{BUCKET}/del-{i}.bin")
            del_times.append(t)
        avg_del = {}
        for key in del_times[0]:
            avg_del[key] = sum(r[key] for r in del_times) / len(del_times)
        print(f"  {'DELETE object':<30} {avg_del['connect']:>9.3f}  {avg_del['send']:>9.3f}  {avg_del['ttfb']:>9.3f}  {avg_del['transfer']:>9.3f}  {avg_del['total']:>9.3f}")

        # ── 6. Auth overhead ─────────────────────────────────────────
        print(f"\n  [Auth Overhead — SigV4 vs No-Auth comparison]")

        # Restart without auth for comparison is complex, so estimate signing overhead
        sign_times = []
        for _ in range(100):
            headers = {"host": f"{HOST}:{S3_PORT}"}
            t0 = time.perf_counter()
            sign_request("GET", "/", headers)
            sign_times.append((time.perf_counter() - t0) * 1000)
        avg_sign = sum(sign_times) / len(sign_times)
        print(f"  SigV4 signing (client-side):  avg={avg_sign:.3f} ms  ({100} iterations)")

        # ── Summary ──────────────────────────────────────────────────
        print()
        print("=" * W)
        print("  Summary")
        print("=" * W)
        print(f"  TCP connect          : {avg_connect:.3f} ms avg")

        # Get TTFB for small request
        meta_avg = avg_timings(lambda: raw_request("HEAD", f"/{BUCKET}/bench-1024.bin"), N)
        print(f"  TTFB (HeadObject)    : {meta_avg['ttfb']:.3f} ms avg")

        get_1k = avg_timings(lambda: raw_request("GET", f"/{BUCKET}/bench-1024.bin"), N)
        print(f"  GET 1 KB total       : {get_1k['total']:.3f} ms avg")

        get_1m = avg_timings(lambda: raw_request("GET", f"/{BUCKET}/bench-1048576.bin"), N)
        print(f"  GET 1 MB total       : {get_1m['total']:.3f} ms avg")

        get_10m = avg_timings(lambda: raw_request("GET", f"/{BUCKET}/bench-10485760.bin"), N)
        print(f"  GET 10 MB total      : {get_10m['total']:.3f} ms avg")

        print(f"  SigV4 sign overhead  : {avg_sign:.3f} ms avg")
        print("=" * W)

    finally:
        stop_server()
        if DATA_DIR and os.path.exists(DATA_DIR):
            shutil.rmtree(DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
