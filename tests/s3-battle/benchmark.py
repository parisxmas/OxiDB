#!/usr/bin/env python3
"""
OxiDB S3 vs MinIO — Encrypted S3 Battle

OxiDB: SSE-S3 (AES-256-GCM) encryption on every object
MinIO: Plain storage (no KMS configured)

This is a real-world scenario: OxiDB encrypts everything at rest
while MinIO stores plaintext. We measure if encryption adds overhead.
"""

import hashlib, os, sys, time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ── Config ──

OXIDB = {"endpoint": "http://oxidb:9000", "ak": "benchkey", "sk": "benchsecret123", "name": "OxiDB"}
MINIO = {"endpoint": "http://minio:9000", "ak": "benchkey", "sk": "benchsecret123", "name": "MinIO"}
BUCKET = "battle-bucket"

def client(cfg):
    return boto3.client(
        "s3", endpoint_url=cfg["endpoint"],
        aws_access_key_id=cfg["ak"], aws_secret_access_key=cfg["sk"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"max_attempts": 0}),
    )

def timed(fn):
    t0 = time.perf_counter()
    try:
        r = fn()
        return True, (time.perf_counter() - t0) * 1000, r
    except Exception as e:
        return False, (time.perf_counter() - t0) * 1000, str(e)

# ── Tests ──

results = []

def run_test(name, oxidb_fn, minio_fn):
    ok_o, ms_o, r_o = timed(oxidb_fn)
    ok_m, ms_m, r_m = timed(minio_fn)
    ratio = ms_m / ms_o if ms_o > 0.01 else 0
    status_o = "OK" if ok_o else f"FAIL({r_o})"
    status_m = "OK" if ok_m else f"FAIL({r_m})"
    results.append((name, ok_o, ms_o, ok_m, ms_m, ratio))
    if not ok_o:
        print(f"  !! OxiDB {name}: {r_o}")
    if not ok_m:
        print(f"  !! MinIO {name}: {r_m}")

def battle(o, m):
    """Run all battle tests."""

    # OxiDB uses SSE-S3, MinIO uses plain
    SSE = {"ServerSideEncryption": "AES256"}

    # ── Setup ──
    for s3 in [o, m]:
        try:
            s3.create_bucket(Bucket=BUCKET)
        except Exception:
            pass

    # ── 1. PUT/GET various sizes ──
    for label, size in [("1KB", 1024), ("10KB", 10240), ("100KB", 102400),
                        ("1MB", 1048576), ("5MB", 5*1048576), ("10MB", 10*1048576)]:
        data = os.urandom(size)
        key = f"bench-{label}.bin"

        run_test(f"PUT {label}",
                 lambda d=data, k=key: o.put_object(Bucket=BUCKET, Key=k, Body=d, **SSE),
                 lambda d=data, k=key: m.put_object(Bucket=BUCKET, Key=k, Body=d))

        run_test(f"GET {label}",
                 lambda k=key: o.get_object(Bucket=BUCKET, Key=k)["Body"].read(),
                 lambda k=key: m.get_object(Bucket=BUCKET, Key=k)["Body"].read())

        # Verify data integrity
        got_o = o.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        got_m = m.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if got_o != data:
            print(f"  !! OxiDB DATA MISMATCH for {label} (got {len(got_o)} bytes)")
        if got_m != data:
            print(f"  !! MinIO DATA MISMATCH for {label} (got {len(got_m)} bytes)")

    # ── 2. HEAD ──
    run_test("HEAD object",
             lambda: o.head_object(Bucket=BUCKET, Key="bench-1MB.bin"),
             lambda: m.head_object(Bucket=BUCKET, Key="bench-1MB.bin"))

    # ── 3. Range requests ──
    run_test("Range 0-1023",
             lambda: o.get_object(Bucket=BUCKET, Key="bench-1MB.bin", Range="bytes=0-1023")["Body"].read(),
             lambda: m.get_object(Bucket=BUCKET, Key="bench-1MB.bin", Range="bytes=0-1023")["Body"].read())

    run_test("Range mid-4KB",
             lambda: o.get_object(Bucket=BUCKET, Key="bench-1MB.bin", Range="bytes=524288-528383")["Body"].read(),
             lambda: m.get_object(Bucket=BUCKET, Key="bench-1MB.bin", Range="bytes=524288-528383")["Body"].read())

    # ── 4. Conditional request ──
    etag_o = o.head_object(Bucket=BUCKET, Key="bench-1MB.bin")["ETag"]
    etag_m = m.head_object(Bucket=BUCKET, Key="bench-1MB.bin")["ETag"]

    def cond_oxidb():
        try:
            o.get_object(Bucket=BUCKET, Key="bench-1MB.bin", IfNoneMatch=etag_o)
        except ClientError:
            pass

    def cond_minio():
        try:
            m.get_object(Bucket=BUCKET, Key="bench-1MB.bin", IfNoneMatch=etag_m)
        except ClientError:
            pass

    run_test("If-None-Match (304)", cond_oxidb, cond_minio)

    # ── 5. Copy object ──
    run_test("CopyObject",
             lambda: o.copy_object(Bucket=BUCKET, Key="bench-copy.bin",
                                   CopySource=f"{BUCKET}/bench-1MB.bin", **SSE),
             lambda: m.copy_object(Bucket=BUCKET, Key="bench-copy.bin",
                                   CopySource=f"{BUCKET}/bench-1MB.bin"))

    # ── 6. List objects ──
    run_test("ListObjects",
             lambda: o.list_objects_v2(Bucket=BUCKET),
             lambda: m.list_objects_v2(Bucket=BUCKET))

    # ── 7. Delete object ──
    run_test("DeleteObject",
             lambda: o.delete_object(Bucket=BUCKET, Key="bench-copy.bin"),
             lambda: m.delete_object(Bucket=BUCKET, Key="bench-copy.bin"))

    # ── 8. Metadata with object ──
    meta_data = os.urandom(1024)
    meta = {"author": "oxidb", "version": "1.0", "env": "bench", "tag": "encrypted"}

    def put_meta_oxidb():
        o.put_object(Bucket=BUCKET, Key="meta.txt", Body=meta_data, Metadata=meta, **SSE)
        return o.head_object(Bucket=BUCKET, Key="meta.txt")

    def put_meta_minio():
        m.put_object(Bucket=BUCKET, Key="meta.txt", Body=meta_data, Metadata=meta)
        return m.head_object(Bucket=BUCKET, Key="meta.txt")

    run_test("PUT+HEAD with metadata", put_meta_oxidb, put_meta_minio)

    # ── 9. Tagging ──
    tags = {"TagSet": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "infra"}]}

    def tag_oxidb():
        o.put_object_tagging(Bucket=BUCKET, Key="meta.txt", Tagging=tags)
        return o.get_object_tagging(Bucket=BUCKET, Key="meta.txt")

    def tag_minio():
        m.put_object_tagging(Bucket=BUCKET, Key="meta.txt", Tagging=tags)
        return m.get_object_tagging(Bucket=BUCKET, Key="meta.txt")

    run_test("PUT+GET tagging", tag_oxidb, tag_minio)

    # ── 10. Multipart upload (3 parts, 11MB total) ──
    parts_data = [os.urandom(5*1048576), os.urandom(5*1048576), os.urandom(1048576)]

    def multipart_oxidb():
        resp = o.create_multipart_upload(Bucket=BUCKET, Key="multi.bin", **SSE)
        uid = resp["UploadId"]
        parts = []
        for i, pd in enumerate(parts_data, 1):
            r = o.upload_part(Bucket=BUCKET, Key="multi.bin", UploadId=uid, PartNumber=i, Body=pd)
            parts.append({"ETag": r["ETag"], "PartNumber": i})
        o.complete_multipart_upload(Bucket=BUCKET, Key="multi.bin", UploadId=uid,
                                    MultipartUpload={"Parts": parts})

    def multipart_minio():
        resp = m.create_multipart_upload(Bucket=BUCKET, Key="multi.bin")
        uid = resp["UploadId"]
        parts = []
        for i, pd in enumerate(parts_data, 1):
            r = m.upload_part(Bucket=BUCKET, Key="multi.bin", UploadId=uid, PartNumber=i, Body=pd)
            parts.append({"ETag": r["ETag"], "PartNumber": i})
        m.complete_multipart_upload(Bucket=BUCKET, Key="multi.bin", UploadId=uid,
                                    MultipartUpload={"Parts": parts})

    run_test("Multipart 11MB", multipart_oxidb, multipart_minio)

    # Verify multipart data
    full = b"".join(parts_data)
    got_o = o.get_object(Bucket=BUCKET, Key="multi.bin")["Body"].read()
    got_m = m.get_object(Bucket=BUCKET, Key="multi.bin")["Body"].read()
    if got_o != full:
        print(f"  !! OxiDB multipart mismatch ({len(got_o)} vs {len(full)})")
    if got_m != full:
        print(f"  !! MinIO multipart mismatch ({len(got_m)} vs {len(full)})")

    # ── 11. Batch delete ──
    batch_keys = []
    for i in range(50):
        key = f"batch/{i:04d}.bin"
        o.put_object(Bucket=BUCKET, Key=key, Body=os.urandom(256), **SSE)
        m.put_object(Bucket=BUCKET, Key=key, Body=os.urandom(256))
        batch_keys.append(key)

    delete_req = {"Objects": [{"Key": k} for k in batch_keys]}
    run_test("BatchDelete 50 objects",
             lambda: o.delete_objects(Bucket=BUCKET, Delete=delete_req),
             lambda: m.delete_objects(Bucket=BUCKET, Delete=delete_req))

    # ── 12. Throughput burst: 200x PUT+GET 4KB ──
    burst_data = os.urandom(4096)

    def burst_oxidb():
        for i in range(200):
            o.put_object(Bucket=BUCKET, Key=f"burst/{i:04d}.bin", Body=burst_data, **SSE)
        for i in range(200):
            o.get_object(Bucket=BUCKET, Key=f"burst/{i:04d}.bin")["Body"].read()

    def burst_minio():
        for i in range(200):
            m.put_object(Bucket=BUCKET, Key=f"burst/{i:04d}.bin", Body=burst_data)
        for i in range(200):
            m.get_object(Bucket=BUCKET, Key=f"burst/{i:04d}.bin")["Body"].read()

    run_test("Burst 200x PUT+GET 4KB", burst_oxidb, burst_minio)

    # ── Cleanup ──
    for key in ["bench-1KB.bin", "bench-10KB.bin", "bench-100KB.bin",
                "bench-1MB.bin", "bench-5MB.bin", "bench-10MB.bin",
                "multi.bin", "meta.txt"]:
        for s3 in [o, m]:
            try:
                s3.delete_object(Bucket=BUCKET, Key=key)
            except Exception:
                pass
    for i in range(200):
        for s3 in [o, m]:
            try:
                s3.delete_object(Bucket=BUCKET, Key=f"burst/{i:04d}.bin")
            except Exception:
                pass
    for s3 in [o, m]:
        try:
            s3.delete_bucket(Bucket=BUCKET)
        except Exception:
            pass


def print_report():
    print()
    print("=" * 86)
    print("  OxiDB S3 (SSE-S3 AES-256) vs MinIO (plain) — Battle Results")
    print("  OxiDB encrypts + decrypts every object. MinIO stores plaintext.")
    print("=" * 86)
    print(f"  {'Test':<32} {'OxiDB':>10} {'MinIO':>10} {'Ratio':>8} {'Winner':>10}")
    print("-" * 86)

    oxidb_wins = 0
    minio_wins = 0
    ties = 0

    for name, ok_o, ms_o, ok_m, ms_m, ratio in results:
        status_o = f"{ms_o:8.1f}ms" if ok_o else "    FAIL  "
        status_m = f"{ms_m:8.1f}ms" if ok_m else "    FAIL  "

        if not ok_o and not ok_m:
            winner = "BOTH FAIL"
        elif not ok_o:
            winner = "MinIO"
            minio_wins += 1
        elif not ok_m:
            winner = "\033[32mOxiDB\033[0m"
            oxidb_wins += 1
        elif ratio > 1.05:
            winner = f"\033[32mOxiDB\033[0m"
            oxidb_wins += 1
        elif ratio < 0.95:
            winner = f"\033[33mMinIO\033[0m"
            minio_wins += 1
        else:
            winner = "tie"
            ties += 1

        ratio_str = f"{ratio:.2f}x" if ok_o and ok_m else "N/A"
        print(f"  {name:<32} {status_o} {status_m} {ratio_str:>8} {winner:>10}")

    print("-" * 86)

    # Overall
    total_o = sum(ms for _, ok, ms, _, _, _ in results if ok)
    total_m = sum(ms for _, _, _, ok, ms, _ in results if ok)
    overall = total_m / total_o if total_o > 0 else 0

    print(f"  {'TOTAL':<32} {total_o:8.0f}ms {total_m:8.0f}ms {overall:6.2f}x  ", end="")
    if overall > 1:
        print(f"\033[32mOxiDB {overall:.2f}x faster overall\033[0m")
    elif overall < 1:
        print(f"\033[33mMinIO {1/overall:.2f}x faster overall\033[0m")
    else:
        print("Tie")

    print(f"\n  Scoreboard: OxiDB \033[32m{oxidb_wins}\033[0m — MinIO \033[33m{minio_wins}\033[0m — Ties {ties}")
    if overall > 1:
        print(f"\n  OxiDB encrypts every byte with AES-256-GCM and is STILL {overall:.2f}x faster.")
    print("=" * 86)


def main():
    print("Connecting to OxiDB and MinIO...")
    o = client(OXIDB)
    m = client(MINIO)

    try:
        o.list_buckets()
        print("  OxiDB: OK")
    except Exception as e:
        print(f"  OxiDB: FAILED ({e})")
        sys.exit(1)

    try:
        m.list_buckets()
        print("  MinIO: OK")
    except Exception as e:
        print(f"  MinIO: FAILED ({e})")
        sys.exit(1)

    print("\nRunning battle...\n")
    battle(o, m)
    print_report()


if __name__ == "__main__":
    main()
