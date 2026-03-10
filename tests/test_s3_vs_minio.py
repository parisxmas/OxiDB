#!/usr/bin/env python3
"""
OxiDB S3 vs MinIO — Side-by-Side Comparison Test

Runs the same S3 operations against both OxiDB and MinIO,
compares pass/fail and latency.

Requirements: pip install boto3
"""

import os, sys, time, hashlib

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError:
    print("pip install boto3")
    sys.exit(1)

# ── Configuration ────────────────────────────────────────────────────────

OXIDB_ENDPOINT = "http://127.0.0.1:9100"
OXIDB_ACCESS_KEY = "oxidb-test-key"
OXIDB_SECRET_KEY = "oxidb-test-secret-2026"

MINIO_ENDPOINT = "http://127.0.0.1:9900"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

BUCKET = "s3-vs-test"


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def make_client(endpoint, ak, sk):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
        ),
    )


def timed(fn):
    t0 = time.perf_counter()
    try:
        result = fn()
        elapsed = (time.perf_counter() - t0) * 1000
        return True, elapsed, result, None
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        return False, elapsed, None, str(e)[:100]


# ── Test definitions ─────────────────────────────────────────────────────

class ComparisonTest:
    def __init__(self):
        self.results = []  # (section, name, oxidb_pass, oxidb_ms, minio_pass, minio_ms, detail)

    def section(self, name):
        self.results.append(("SEC", name, None, None, None, None, None))

    def run(self, name, oxidb_fn, minio_fn, detail_fn=None):
        o_ok, o_ms, o_res, o_err = timed(oxidb_fn)
        m_ok, m_ms, m_res, m_err = timed(minio_fn)
        detail = ""
        if detail_fn:
            try:
                detail = detail_fn(o_res if o_ok else m_res)
            except:
                pass
        self.results.append(("TEST", name, o_ok, o_ms, m_ok, m_ms, detail))
        if not o_ok:
            print(f"    OxiDB FAIL: {name} — {o_err}")
        if not m_ok:
            print(f"    MinIO FAIL: {name} — {m_err}")
        return o_res, m_res

    def run_expect_error(self, name, oxidb_fn, minio_fn, expected_status):
        def check(fn):
            try:
                fn()
                return False, 0, "no error"
            except ClientError as e:
                got = e.response["ResponseMetadata"]["HTTPStatusCode"]
                return got == expected_status, 0, f"→ {got}"
            except Exception as e:
                return False, 0, str(e)[:80]

        o_ok, o_ms, _, o_err = timed(lambda: None)
        o_pass, _, o_detail = check(oxidb_fn)
        m_ok, m_ms, _, m_err = timed(lambda: None)
        m_pass, _, m_detail = check(minio_fn)
        self.results.append(("TEST", name, o_pass, 0, m_pass, 0, f"expect {expected_status}"))

    def report(self):
        W = 100
        print()
        print("=" * W)
        print("  OxiDB S3 vs MinIO — Comparison Report")
        print("=" * W)
        print(f"  OxiDB : {OXIDB_ENDPOINT}")
        print(f"  MinIO : {MINIO_ENDPOINT}")
        print("=" * W)
        print(f"  {'Test':<44} {'OxiDB':>10} {'MinIO':>10} {'Ratio':>8}  Detail")
        print("-" * W)

        oxidb_pass = 0
        oxidb_fail = 0
        minio_pass = 0
        minio_fail = 0
        oxidb_total_ms = 0
        minio_total_ms = 0
        test_count = 0

        for kind, name, o_ok, o_ms, m_ok, m_ms, detail in self.results:
            if kind == "SEC":
                print(f"\n  [{name}]")
                continue

            test_count += 1
            o_tag = "PASS" if o_ok else "FAIL"
            m_tag = "PASS" if m_ok else "FAIL"

            if o_ok:
                oxidb_pass += 1
            else:
                oxidb_fail += 1
            if m_ok:
                minio_pass += 1
            else:
                minio_fail += 1

            o_str = f"{o_ms:>7.2f}ms" if o_ms > 0 else f"  {o_tag:>5}"
            m_str = f"{m_ms:>7.2f}ms" if m_ms > 0 else f"  {m_tag:>5}"

            if o_ms > 0 and m_ms > 0:
                ratio = o_ms / m_ms
                oxidb_total_ms += o_ms
                minio_total_ms += m_ms
                if ratio < 0.9:
                    ratio_str = f"{ratio:.2f}x \033[32m▲\033[0m"
                elif ratio > 1.1:
                    ratio_str = f"{ratio:.2f}x \033[31m▼\033[0m"
                else:
                    ratio_str = f"{ratio:.2f}x  "
            else:
                ratio_str = "     "

            det = f"  {detail}" if detail else ""
            o_mark = " " if o_ok else "X"
            m_mark = " " if m_ok else "X"
            print(f"  {o_mark}{m_mark} {name:<42} {o_str} {m_str} {ratio_str:>8}{det}")

        print()
        print("-" * W)
        print(f"  OxiDB: {oxidb_pass} passed, {oxidb_fail} failed")
        print(f"  MinIO: {minio_pass} passed, {minio_fail} failed")
        if minio_total_ms > 0:
            overall = oxidb_total_ms / minio_total_ms
            print(f"  Total time — OxiDB: {oxidb_total_ms:.0f}ms, MinIO: {minio_total_ms:.0f}ms, Ratio: {overall:.2f}x")
        print("=" * W)

        return oxidb_fail == 0


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    oxidb = make_client(OXIDB_ENDPOINT, OXIDB_ACCESS_KEY, OXIDB_SECRET_KEY)
    minio = make_client(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    t = ComparisonTest()

    # ── Cleanup from previous runs ──
    for client in [oxidb, minio]:
        try:
            resp = client.list_objects_v2(Bucket=BUCKET, MaxKeys=10000)
            keys = [c["Key"] for c in resp.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys]})
            client.delete_bucket(Bucket=BUCKET)
        except:
            pass

    # ═══════════════════════════════════════════════════════════════
    # 1. BUCKET OPERATIONS
    # ═══════════════════════════════════════════════════════════════
    t.section("Bucket Operations")

    t.run("CreateBucket",
          lambda: oxidb.create_bucket(Bucket=BUCKET),
          lambda: minio.create_bucket(Bucket=BUCKET))

    t.run("HeadBucket",
          lambda: oxidb.head_bucket(Bucket=BUCKET),
          lambda: minio.head_bucket(Bucket=BUCKET))

    t.run("ListBuckets",
          lambda: oxidb.list_buckets(),
          lambda: minio.list_buckets(),
          lambda r: f"{len(r['Buckets'])} buckets" if r else "")

    t.run_expect_error("HeadBucket (missing) → 404",
                       lambda: oxidb.head_bucket(Bucket="no-such"),
                       lambda: minio.head_bucket(Bucket="no-such"), 404)

    # ═══════════════════════════════════════════════════════════════
    # 2. OBJECT CRUD — various sizes
    # ═══════════════════════════════════════════════════════════════
    t.section("Object PUT")

    sizes = [
        ("1 KB",    1024),
        ("10 KB",   10 * 1024),
        ("100 KB",  100 * 1024),
        ("1 MB",    1024 * 1024),
        ("5 MB",    5 * 1024 * 1024),
        ("10 MB",   10 * 1024 * 1024),
    ]
    test_data = {}

    for label, sz in sizes:
        data = os.urandom(sz)
        key = f"bench-{sz}.bin"
        test_data[key] = data
        t.run(f"PutObject {label}",
              lambda d=data, k=key: oxidb.put_object(Bucket=BUCKET, Key=k, Body=d,
                                                      ContentType="application/octet-stream"),
              lambda d=data, k=key: minio.put_object(Bucket=BUCKET, Key=k, Body=d,
                                                      ContentType="application/octet-stream"),
              lambda r: f"{sz/1024:.0f} KB" if sz < 1048576 else f"{sz/1048576:.0f} MB")

    t.section("Object GET")

    for label, sz in sizes:
        key = f"bench-{sz}.bin"
        expected = test_data[key]
        cs = md5(expected)

        def dl_oxidb(k=key, c=cs, s=sz):
            r = oxidb.get_object(Bucket=BUCKET, Key=k)
            body = r["Body"].read()
            assert len(body) == s and md5(body) == c
            return body

        def dl_minio(k=key, c=cs, s=sz):
            r = minio.get_object(Bucket=BUCKET, Key=k)
            body = r["Body"].read()
            assert len(body) == s and md5(body) == c
            return body

        t.run(f"GetObject {label}", dl_oxidb, dl_minio,
              lambda r: f"{len(r)/1024:.0f} KB" if len(r) < 1048576 else f"{len(r)/1048576:.0f} MB")

    t.section("Object HEAD / DELETE")

    t.run("HeadObject (1 MB)",
          lambda: oxidb.head_object(Bucket=BUCKET, Key="bench-1048576.bin"),
          lambda: minio.head_object(Bucket=BUCKET, Key="bench-1048576.bin"),
          lambda r: f"size={r['ContentLength']}" if r else "")

    t.run("DeleteObject (1 KB)",
          lambda: oxidb.delete_object(Bucket=BUCKET, Key="bench-1024.bin"),
          lambda: minio.delete_object(Bucket=BUCKET, Key="bench-1024.bin"))

    t.run_expect_error("GetObject (missing) → 404",
                       lambda: oxidb.get_object(Bucket=BUCKET, Key="nope"),
                       lambda: minio.get_object(Bucket=BUCKET, Key="nope"), 404)

    # ═══════════════════════════════════════════════════════════════
    # 3. LIST OBJECTS
    # ═══════════════════════════════════════════════════════════════
    t.section("List Objects")

    # Add some hierarchical objects
    for i in range(20):
        d = os.urandom(64)
        oxidb.put_object(Bucket=BUCKET, Key=f"list/item-{i:03d}.txt", Body=d)
        minio.put_object(Bucket=BUCKET, Key=f"list/item-{i:03d}.txt", Body=d)
    for sub in ["sub-a", "sub-b"]:
        d = os.urandom(32)
        oxidb.put_object(Bucket=BUCKET, Key=f"list/{sub}/file.txt", Body=d)
        minio.put_object(Bucket=BUCKET, Key=f"list/{sub}/file.txt", Body=d)

    t.run("ListObjectsV2 (all)",
          lambda: oxidb.list_objects_v2(Bucket=BUCKET),
          lambda: minio.list_objects_v2(Bucket=BUCKET),
          lambda r: f"{r.get('KeyCount', 0)} keys" if r else "")

    t.run("ListObjectsV2 (prefix)",
          lambda: oxidb.list_objects_v2(Bucket=BUCKET, Prefix="list/item-00"),
          lambda: minio.list_objects_v2(Bucket=BUCKET, Prefix="list/item-00"),
          lambda r: f"{len(r.get('Contents', []))} matched" if r else "")

    t.run("ListObjectsV2 (delimiter=/)",
          lambda: oxidb.list_objects_v2(Bucket=BUCKET, Prefix="list/", Delimiter="/"),
          lambda: minio.list_objects_v2(Bucket=BUCKET, Prefix="list/", Delimiter="/"),
          lambda r: f"{len(r.get('CommonPrefixes', []))} prefixes" if r else "")

    t.run("ListObjectsV2 (max-keys=5)",
          lambda: oxidb.list_objects_v2(Bucket=BUCKET, Prefix="list/", MaxKeys=5),
          lambda: minio.list_objects_v2(Bucket=BUCKET, Prefix="list/", MaxKeys=5),
          lambda r: f"{len(r.get('Contents', []))} returned" if r else "")

    # ═══════════════════════════════════════════════════════════════
    # 4. COPY OBJECT
    # ═══════════════════════════════════════════════════════════════
    t.section("Copy Object")

    src_data = b"copy-source-data-" + os.urandom(1000)
    oxidb.put_object(Bucket=BUCKET, Key="copy-src.bin", Body=src_data,
                     Metadata={"author": "test"})
    minio.put_object(Bucket=BUCKET, Key="copy-src.bin", Body=src_data,
                     Metadata={"author": "test"})

    t.run("CopyObject (same bucket)",
          lambda: oxidb.copy_object(Bucket=BUCKET, Key="copy-dst.bin",
                                    CopySource=f"{BUCKET}/copy-src.bin"),
          lambda: minio.copy_object(Bucket=BUCKET, Key="copy-dst.bin",
                                    CopySource=f"{BUCKET}/copy-src.bin"))

    def verify_copy_oxidb():
        r = oxidb.get_object(Bucket=BUCKET, Key="copy-dst.bin")
        body = r["Body"].read()
        assert body == src_data
        return body

    def verify_copy_minio():
        r = minio.get_object(Bucket=BUCKET, Key="copy-dst.bin")
        body = r["Body"].read()
        assert body == src_data
        return body

    t.run("CopyObject verify content", verify_copy_oxidb, verify_copy_minio,
          lambda r: f"{len(r)} bytes" if r else "")

    t.run("CopyObject REPLACE metadata",
          lambda: oxidb.copy_object(Bucket=BUCKET, Key="copy-replaced.bin",
                                    CopySource=f"{BUCKET}/copy-src.bin",
                                    MetadataDirective="REPLACE",
                                    Metadata={"env": "staging"}),
          lambda: minio.copy_object(Bucket=BUCKET, Key="copy-replaced.bin",
                                    CopySource=f"{BUCKET}/copy-src.bin",
                                    MetadataDirective="REPLACE",
                                    Metadata={"env": "staging"}))

    # ═══════════════════════════════════════════════════════════════
    # 5. RANGE REQUESTS
    # ═══════════════════════════════════════════════════════════════
    t.section("Range Requests")

    range_data = bytes(range(256)) * 40  # 10240 bytes
    oxidb.put_object(Bucket=BUCKET, Key="range.bin", Body=range_data)
    minio.put_object(Bucket=BUCKET, Key="range.bin", Body=range_data)

    ranges = [
        ("bytes=0-99",    range_data[0:100]),
        ("bytes=500-999", range_data[500:1000]),
        ("bytes=9000-",   range_data[9000:]),
        ("bytes=-200",    range_data[-200:]),
        ("bytes=0-0",     range_data[0:1]),
    ]

    for range_str, expected in ranges:
        def check_o(rs=range_str, exp=expected):
            r = oxidb.get_object(Bucket=BUCKET, Key="range.bin", Range=rs)
            body = r["Body"].read()
            assert body == exp, f"len {len(body)} != {len(exp)}"
            return body

        def check_m(rs=range_str, exp=expected):
            r = minio.get_object(Bucket=BUCKET, Key="range.bin", Range=rs)
            body = r["Body"].read()
            assert body == exp, f"len {len(body)} != {len(exp)}"
            return body

        t.run(f"Range {range_str}", check_o, check_m,
              lambda r: f"{len(r)} bytes" if r else "")

    # ═══════════════════════════════════════════════════════════════
    # 6. CONDITIONAL REQUESTS
    # ═══════════════════════════════════════════════════════════════
    t.section("Conditional Requests")

    o_head = oxidb.head_object(Bucket=BUCKET, Key="range.bin")
    m_head = minio.head_object(Bucket=BUCKET, Key="range.bin")
    o_etag = o_head["ETag"]
    m_etag = m_head["ETag"]

    t.run_expect_error("If-None-Match (match) → 304",
                       lambda: oxidb.get_object(Bucket=BUCKET, Key="range.bin", IfNoneMatch=o_etag),
                       lambda: minio.get_object(Bucket=BUCKET, Key="range.bin", IfNoneMatch=m_etag), 304)

    t.run_expect_error("If-Match (wrong) → 412",
                       lambda: oxidb.get_object(Bucket=BUCKET, Key="range.bin", IfMatch='"bad"'),
                       lambda: minio.get_object(Bucket=BUCKET, Key="range.bin", IfMatch='"bad"'), 412)

    # ═══════════════════════════════════════════════════════════════
    # 7. MULTIPART UPLOAD
    # ═══════════════════════════════════════════════════════════════
    t.section("Multipart Upload")

    # OxiDB multipart
    o_mpu = oxidb.create_multipart_upload(Bucket=BUCKET, Key="multi.bin",
                                           ContentType="application/octet-stream")
    o_uid = o_mpu["UploadId"]

    # MinIO multipart
    m_mpu = minio.create_multipart_upload(Bucket=BUCKET, Key="multi.bin",
                                           ContentType="application/octet-stream")
    m_uid = m_mpu["UploadId"]

    t.run("CreateMultipartUpload",
          lambda: {"UploadId": o_uid},
          lambda: {"UploadId": m_uid},
          lambda r: f"id={r['UploadId'][:12]}..." if r else "")

    # Upload 5 parts of 200KB each
    parts_data = [os.urandom(200 * 1024) for _ in range(5)]
    o_parts = []
    m_parts = []

    for i, chunk in enumerate(parts_data, 1):
        def up_o(c=chunk, n=i):
            return oxidb.upload_part(Bucket=BUCKET, Key="multi.bin",
                                     UploadId=o_uid, PartNumber=n, Body=c)
        def up_m(c=chunk, n=i):
            return minio.upload_part(Bucket=BUCKET, Key="multi.bin",
                                     UploadId=m_uid, PartNumber=n, Body=c)

        o_r, m_r = t.run(f"UploadPart {i} (200 KB)", up_o, up_m)
        if o_r:
            o_parts.append({"ETag": o_r["ETag"], "PartNumber": i})
        if m_r:
            m_parts.append({"ETag": m_r["ETag"], "PartNumber": i})

    expected_multi = b"".join(parts_data)

    t.run("CompleteMultipartUpload",
          lambda: oxidb.complete_multipart_upload(
              Bucket=BUCKET, Key="multi.bin", UploadId=o_uid,
              MultipartUpload={"Parts": o_parts}),
          lambda: minio.complete_multipart_upload(
              Bucket=BUCKET, Key="multi.bin", UploadId=m_uid,
              MultipartUpload={"Parts": m_parts}))

    def verify_multi_o():
        r = oxidb.get_object(Bucket=BUCKET, Key="multi.bin")
        body = r["Body"].read()
        assert len(body) == len(expected_multi)
        return body

    def verify_multi_m():
        r = minio.get_object(Bucket=BUCKET, Key="multi.bin")
        body = r["Body"].read()
        assert len(body) == len(expected_multi)
        return body

    t.run("Multipart verify (1 MB)", verify_multi_o, verify_multi_m,
          lambda r: f"{len(r)} bytes" if r else "")

    # ═══════════════════════════════════════════════════════════════
    # 8. BATCH DELETE
    # ═══════════════════════════════════════════════════════════════
    t.section("Batch Delete")

    batch_keys = [f"batch/{i:03d}.dat" for i in range(50)]
    for k in batch_keys:
        d = os.urandom(64)
        oxidb.put_object(Bucket=BUCKET, Key=k, Body=d)
        minio.put_object(Bucket=BUCKET, Key=k, Body=d)

    t.run("DeleteObjects (50 keys)",
          lambda: oxidb.delete_objects(
              Bucket=BUCKET,
              Delete={"Objects": [{"Key": k} for k in batch_keys]}),
          lambda: minio.delete_objects(
              Bucket=BUCKET,
              Delete={"Objects": [{"Key": k} for k in batch_keys]}),
          lambda r: f"{len(r.get('Deleted', []))} deleted" if r else "")

    # ═══════════════════════════════════════════════════════════════
    # 9. OBJECT TAGGING
    # ═══════════════════════════════════════════════════════════════
    t.section("Object Tagging")

    oxidb.put_object(Bucket=BUCKET, Key="tagged.txt", Body=b"tag-me")
    minio.put_object(Bucket=BUCKET, Key="tagged.txt", Body=b"tag-me")

    tags = {"TagSet": [
        {"Key": "env", "Value": "production"},
        {"Key": "team", "Value": "platform"},
        {"Key": "cost", "Value": "42"},
    ]}

    t.run("PutObjectTagging (3 tags)",
          lambda: oxidb.put_object_tagging(Bucket=BUCKET, Key="tagged.txt", Tagging=tags),
          lambda: minio.put_object_tagging(Bucket=BUCKET, Key="tagged.txt", Tagging=tags))

    t.run("GetObjectTagging",
          lambda: oxidb.get_object_tagging(Bucket=BUCKET, Key="tagged.txt"),
          lambda: minio.get_object_tagging(Bucket=BUCKET, Key="tagged.txt"),
          lambda r: f"{len(r.get('TagSet', []))} tags" if r else "")

    t.run("DeleteObjectTagging",
          lambda: oxidb.delete_object_tagging(Bucket=BUCKET, Key="tagged.txt"),
          lambda: minio.delete_object_tagging(Bucket=BUCKET, Key="tagged.txt"))

    # ═══════════════════════════════════════════════════════════════
    # 10. THROUGHPUT — repeated small ops
    # ═══════════════════════════════════════════════════════════════
    t.section("Throughput — 100x PUT+GET 1KB")

    small = os.urandom(1024)

    def burst_oxidb():
        for i in range(100):
            oxidb.put_object(Bucket=BUCKET, Key=f"burst/{i}.bin", Body=small)
        for i in range(100):
            r = oxidb.get_object(Bucket=BUCKET, Key=f"burst/{i}.bin")
            r["Body"].read()
        return 200

    def burst_minio():
        for i in range(100):
            minio.put_object(Bucket=BUCKET, Key=f"burst/{i}.bin", Body=small)
        for i in range(100):
            r = minio.get_object(Bucket=BUCKET, Key=f"burst/{i}.bin")
            r["Body"].read()
        return 200

    t.run("100x PUT + 100x GET (1 KB)", burst_oxidb, burst_minio,
          lambda r: "200 ops total")

    # ═══════════════════════════════════════════════════════════════
    # CLEANUP
    # ═══════════════════════════════════════════════════════════════
    t.section("Cleanup")

    def cleanup(client):
        resp = client.list_objects_v2(Bucket=BUCKET, MaxKeys=10000)
        keys = [c["Key"] for c in resp.get("Contents", [])]
        if keys:
            for i in range(0, len(keys), 1000):
                client.delete_objects(
                    Bucket=BUCKET,
                    Delete={"Objects": [{"Key": k} for k in keys[i:i+1000]]})
        client.delete_bucket(Bucket=BUCKET)
        return len(keys)

    t.run("Cleanup (delete all + bucket)",
          lambda: cleanup(oxidb),
          lambda: cleanup(minio),
          lambda r: f"{r} objects" if r else "")

    ok = t.report()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
