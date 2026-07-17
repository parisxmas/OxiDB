#!/usr/bin/env python3
"""p99 soak driver (stdlib only) — faithful port of oxidb-server/tests/p99_soak.rs.

Spawns an ISOLATED oxidb-server (own tempdir + random free port) using an
existing server binary, drives the document engine from N connections for a
configurable duration, and reports read/write p50/p95/p99/p999/max plus RSS/fd
drift and a PASS/FAIL verdict.

Steady-state workload (fixed pre-seeded keyspace, 70% find_one / 25% update_one
$inc / 5% count, NO net inserts) so RSS/fd growth is a clean leak signal;
latency drift = read p99(late)/p99(early).

SAFETY: binds a fresh random port and a throwaway OXIDB_DATA tempdir, and only
ever kills the child it spawned. It never touches any other process, port, or
data directory on the host.

Usage:  p99_soak.py /path/to/oxidb-server
Env knobs: SOAK_SECS, SOAK_CONNS, SOAK_KEYSPACE, SOAK_WARMUP_SECS,
           SOAK_REPORT_SECS, SOAK_RSS_GROWTH_PCT, SOAK_FD_GROWTH, SOAK_DRIFT,
           SOAK_P99_MS
"""
import json, math, os, random, socket, struct, subprocess, sys, tempfile, threading, time, shutil

def envf(k, d):
    try: return float(os.environ[k])
    except: return d
def envi(k, d):
    try: return int(os.environ[k])
    except: return d

SECS      = envi("SOAK_SECS", 300)
CONNS     = max(1, envi("SOAK_CONNS", 8))
KEYSPACE  = max(1, envi("SOAK_KEYSPACE", 50000))
WARMUP    = envi("SOAK_WARMUP_SECS", 10)
REPORT    = max(1, envi("SOAK_REPORT_SECS", 15))
MAX_RSS   = envf("SOAK_RSS_GROWTH_PCT", 30.0)
MAX_FD    = envi("SOAK_FD_GROWTH", 64)
MAX_DRIFT = envf("SOAK_DRIFT", 3.0)
P99_CEIL  = envf("SOAK_P99_MS", 0.0)
COLL = "soak"

def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p

def recvn(sock, n):
    buf = b""
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c: raise ConnectionError("server closed")
        buf += c
    return buf

def call(sock, req):
    body = json.dumps(req).encode()
    sock.sendall(struct.pack("<I", len(body)) + body)
    n = struct.unpack("<I", recvn(sock, 4))[0]
    return json.loads(recvn(sock, n))

def connect(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=30)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s

def rss_kib(pid):
    try:
        for line in open(f"/proc/{pid}/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except Exception:
        pass
    try:
        return int(subprocess.run(["ps","-o","rss=","-p",str(pid)],
                   capture_output=True,text=True).stdout.strip())
    except Exception:
        return 0

def fd_count(pid):
    try: return len(os.listdir(f"/proc/{pid}/fd"))
    except Exception: return None

def pct_ms(sorted_us, p):
    if not sorted_us: return 0.0
    k = int(math.ceil(p * len(sorted_us))) - 1
    k = min(max(k, 0), len(sorted_us) - 1)
    return sorted_us[k] / 1000.0

def main():
    if len(sys.argv) < 2:
        print("usage: p99_soak.py /path/to/oxidb-server"); sys.exit(2)
    server_bin = sys.argv[1]
    port = free_port()
    data = tempfile.mkdtemp(prefix="oxidb_soak_")
    env = dict(os.environ,
               OXIDB_DATA=data,
               OXIDB_ADDR=f"127.0.0.1:{port}",
               OXIDB_POOL_SIZE=str(CONNS),
               OXIDB_IDLE_TIMEOUT="0")
    # strip any inherited engine/auth toggles so this is a clean doc-only instance
    for k in ("OXIDB_SQL","OXIDB_TSDB","OXIDB_OXIMEM_PORT","OXIDB_MQTT_PORT",
              "OXIDB_S3_PORT","OXIDB_HTTP_PORT","OXIDB_WS_PORT","OXIDB_JWT_SECRET",
              "OXIDB_AUDIT"):
        env.pop(k, None)

    print(f"\n== p99 soak (linux) ==  {SECS}s · {CONNS} conns · {KEYSPACE} keys · "
          f"warmup {WARMUP}s\nserver: {server_bin}\nisolated port {port}, data {data}")

    proc = subprocess.Popen([server_bin], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    pid = proc.pid
    try:
        # wait for port
        deadline = time.time() + 20
        while True:
            try:
                connect(port).close(); break
            except OSError:
                if time.time() > deadline:
                    raise SystemExit("server port never opened")
                time.sleep(0.05)

        # seed a fixed key space
        s = connect(port)
        r = call(s, {"cmd":"create_index","collection":COLL,"field":"k"})
        assert r.get("ok"), f"create_index failed: {r}"
        nxt = 0
        while nxt < KEYSPACE:
            hi = min(nxt + 1000, KEYSPACE)
            docs = [{"k":k,"hits":0,"pad":"xxxxxxxxxxxxxxxx"} for k in range(nxt,hi)]
            r = call(s, {"cmd":"insert_many","collection":COLL,"docs":docs})
            assert r.get("ok"), f"seed failed: {r}"
            nxt = hi
        s.close()
        print(f"seeded {KEYSPACE} docs")

        stop = threading.Event()
        ops = [0]
        ops_lock = threading.Lock()
        start = time.time()
        half = SECS / 2.0

        # monitor: periodic progress + rss/fd timeline
        samples = []  # (elapsed_s, rss_kib, fd)
        def monitor():
            last_ops, last_t = 0, time.time()
            while not stop.is_set():
                time.sleep(0.25)
                if time.time() - last_t >= REPORT:
                    with ops_lock: now = ops[0]
                    dt = time.time() - last_t
                    rate = (now - last_ops) / dt
                    rss = rss_kib(pid); fd = fd_count(pid)
                    el = time.time() - start
                    samples.append((el, rss, fd))
                    print(f"  t={el:>5.0f}s  {rate:>9.0f} ops/s  "
                          f"rss={rss/1024:>6.1f} MB  fd={fd if fd is not None else 'n/a'}")
                    last_ops, last_t = now, time.time()
        mon = threading.Thread(target=monitor, daemon=True); mon.start()

        # per-worker latency lists (µs), split early/late for reads and writes
        results = [None] * CONNS
        def worker(wid):
            sock = connect(port)
            rng = random.Random(0x9E3779B97F4A7C15 ^ (wid * 0x2545F4914F6CDD1D))
            re, rl, we, wl = [], [], [], []
            errs = 0
            local = 0
            while not stop.is_set():
                k = rng.randrange(KEYSPACE)
                roll = rng.randrange(100)
                if roll < 70:
                    req, wr = {"cmd":"find_one","collection":COLL,"query":{"k":k}}, False
                elif roll < 95:
                    req, wr = {"cmd":"update_one","collection":COLL,"query":{"k":k},
                               "update":{"$inc":{"hits":1}}}, True
                else:
                    req, wr = {"cmd":"count","collection":COLL,"query":{"k":{"$lt":100}}}, False
                t0 = time.perf_counter()
                resp = call(sock, req)
                us = (time.perf_counter() - t0) * 1e6
                if not resp.get("ok"): errs += 1
                late = (time.time() - start) >= half
                (wl if late else we).append(us) if wr else (rl if late else re).append(us)
                local += 1
                if local % 64 == 0:
                    with ops_lock: ops[0] += 64; local2 = 0
                    local = 0
            if local:
                with ops_lock: ops[0] += local
            sock.close()
            results[wid] = (re, rl, we, wl, errs)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(CONNS)]
        for t in threads: t.start()
        time.sleep(SECS)
        stop.set()
        for t in threads: t.join()
        mon.join(timeout=2)
        elapsed = time.time() - start

        # aggregate
        RE, RL, WE, WL = [], [], [], []
        errors = 0
        for (re, rl, we, wl, er) in results:
            RE += re; RL += rl; WE += we; WL += wl; errors += er
        reads = sorted(RE + RL)
        writes = sorted(WE + WL)
        re_s, rl_s = sorted(RE), sorted(RL)
        total = len(reads) + len(writes)

        print("\n---- results ----------------------------------------------")
        print(f"ops: {total}  ({total/elapsed:.0f} ops/s over {elapsed:.1f}s)   "
              f"reads {len(reads)}  writes {len(writes)}  errors {errors}")
        def row(name, xs):
            mean = (sum(xs)/len(xs)/1000.0) if xs else 0.0
            mx = (max(xs)/1000.0) if xs else 0.0
            print(f"  {name:<6} mean {mean:>6.3f}  p50 {pct_ms(xs,.50):>6.3f}  "
                  f"p95 {pct_ms(xs,.95):>6.3f}  p99 {pct_ms(xs,.99):>6.3f}  "
                  f"p999 {pct_ms(xs,.999):>6.3f}  max {mx:>7.3f}  (ms)")
        print("latency:")
        row("read", reads); row("write", writes)

        p99_e = pct_ms(re_s, .99); p99_l = pct_ms(rl_s, .99)
        drift = (p99_l / p99_e) if p99_e > 0 else 1.0
        print(f"read-p99 drift: early {p99_e:.3f} ms -> late {p99_l:.3f} ms  ({drift:.2f}x)")

        post = [s for s in samples if s[0] >= WARMUP]
        rss_growth = 0.0; fd_ok = True
        if len(post) >= 2:
            rss0, rss1 = post[0][1], post[-1][1]
            peak = max(s[1] for s in post)
            rss_growth = ((rss1 - rss0)/rss0*100.0) if rss0 else 0.0
            print(f"rss: baseline {rss0/1024:.1f} MB -> final {rss1/1024:.1f} MB "
                  f"({rss_growth:+.1f}%)  peak {peak/1024:.1f} MB")
            fd0, fd1 = post[0][2], post[-1][2]
            if fd0 is not None and fd1 is not None:
                grew = max(0, fd1 - fd0); fd_ok = grew <= MAX_FD
                print(f"fd:  baseline {fd0} -> final {fd1}  (+{grew})")
            else:
                print("fd:  n/a")
        else:
            print(f"rss/fd: not enough samples past warmup ({WARMUP}s) — run longer")

        print("-----------------------------------------------------------")
        fails = []
        if rss_growth > MAX_RSS: fails.append(f"RSS grew {rss_growth:.1f}% > {MAX_RSS:.0f}% (possible leak)")
        if not fd_ok: fails.append(f"fd count grew more than {MAX_FD} (possible fd leak)")
        if p99_e > 0 and drift > MAX_DRIFT: fails.append(f"read-p99 drifted {drift:.2f}x > {MAX_DRIFT:.1f}x")
        if P99_CEIL > 0 and pct_ms(reads,.99) > P99_CEIL:
            fails.append(f"read p99 {pct_ms(reads,.99):.3f} ms > ceiling {P99_CEIL:.3f} ms")
        if total == 0: fails.append("no operations completed")
        if not fails:
            print(f"VERDICT: PASS — stable under {SECS}s of sustained load\n")
            rc = 0
        else:
            print("VERDICT: FAIL"); [print(f"  - {f}") for f in fails]; print()
            rc = 1
    finally:
        try: proc.terminate(); proc.wait(timeout=10)
        except Exception:
            try: proc.kill()
            except Exception: pass
        shutil.rmtree(data, ignore_errors=True)
    sys.exit(rc)

if __name__ == "__main__":
    main()
