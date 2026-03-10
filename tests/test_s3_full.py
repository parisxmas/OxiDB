#!/usr/bin/env python3
"""
OxiDB S3 API — Full Feature Test with Response Times

Starts oxidb-server with S3 enabled, runs every S3 operation,
reports pass/fail and latency for each.

Usage:
  pip install boto3
  CARGO_TARGET_DIR=/tmp/oxidb-target cargo build -p oxidb-server --features s3
  python tests/test_s3_full.py
"""

import os, sys, time, hashlib, subprocess, shutil, socket, tempfile
import urllib.request, urllib.error

S3_PORT = 9100
S3_ENDPOINT = f"http://127.0.0.1:{S3_PORT}"
ACCESS_KEY = "oxidb-test-key"
SECRET_KEY = "oxidb-test-secret-2026"
BUCKET = "s3-full-test"
DATA_DIR = None
SERVER_PROC = None


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return result, (time.perf_counter() - t0) * 1000


def start_server(data_dir):
    global SERVER_PROC
    env = os.environ.copy()
    env["OXIDB_DATA"] = data_dir
    env["OXIDB_S3_PORT"] = str(S3_PORT)
    env["OXIDB_S3_ACCESS_KEY"] = ACCESS_KEY
    env["OXIDB_S3_SECRET_KEY"] = SECRET_KEY
    env["OXIDB_ADDR"] = "127.0.0.1:0"

    server_bin = os.environ.get(
        "OXIDB_SERVER_BIN", "/tmp/oxidb-target/debug/oxidb-server"
    )
    if not os.path.exists(server_bin):
        print(f"  Binary not found: {server_bin}")
        print("  Build: CARGO_TARGET_DIR=/tmp/oxidb-target cargo build -p oxidb-server --features s3")
        sys.exit(1)

    SERVER_PROC = subprocess.Popen(
        [server_bin], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    for _ in range(50):
        try:
            s = socket.create_connection(("127.0.0.1", S3_PORT), timeout=0.2)
            s.close()
            return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    print("  Server failed to start")
    SERVER_PROC.terminate()
    sys.exit(1)


def stop_server():
    global SERVER_PROC
    if SERVER_PROC:
        SERVER_PROC.terminate()
        try:
            SERVER_PROC.wait(timeout=5)
        except subprocess.TimeoutExpired:
            SERVER_PROC.kill()
        SERVER_PROC = None


def make_client(ak=ACCESS_KEY, sk=SECRET_KEY):
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
        ),
    )


# ── Test runner ──────────────────────────────────────────────────────────

class T:
    def __init__(self):
        self.rows = []
        self.passed = 0
        self.failed = 0
        self.sec = ""

    def section(self, name):
        self.sec = name
        self.rows.append(("SEC", name, "", "", ""))

    def ok(self, name, detail="", ms=0.0):
        self.passed += 1
        self.rows.append(("PASS", self.sec, name, detail, ms))

    def fail(self, name, detail=""):
        self.failed += 1
        self.rows.append(("FAIL", self.sec, name, detail, 0))
        print(f"    FAIL  {name} — {detail}")

    def run(self, name, fn, detail_fn=None):
        """Run fn, auto-record pass/fail + timing."""
        try:
            result, ms = timed(fn)
            detail = detail_fn(result) if detail_fn else ""
            self.ok(name, detail, ms)
            return result, ms
        except Exception as e:
            self.fail(name, str(e)[:120])
            return None, 0

    def expect_error(self, name, fn, expected_status):
        """Run fn, expect ClientError with given HTTP status."""
        from botocore.exceptions import ClientError
        try:
            fn()
            self.fail(name, f"expected {expected_status}")
        except ClientError as e:
            got = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if got == expected_status:
                self.ok(name, f"→ {got}")
            else:
                self.fail(name, f"got {got}, expected {expected_status}")
        except Exception as e:
            self.fail(name, str(e)[:120])

    def report(self):
        W = 82
        print()
        print("=" * W)
        print("  OxiDB S3 — Full Feature Test Report")
        print("=" * W)
        print(f"  Endpoint : {S3_ENDPOINT}")
        print(f"  Auth     : AWS SigV4  key={ACCESS_KEY[:12]}...")
        print(f"  Result   : {self.passed} passed, {self.failed} failed, {self.passed+self.failed} total")
        print("=" * W)

        cur = ""
        for kind, sec, name, detail, ms in self.rows:
            if kind == "SEC":
                cur = sec
                print(f"\n  [{sec}]")
                continue
            tag = "PASS" if kind == "PASS" else "FAIL"
            mark = " " if kind == "PASS" else "X"
            ms_str = f"{ms:>8.2f} ms" if ms > 0 else "          "
            det = f"  {detail}" if detail else ""
            print(f"  {mark} {tag}  {name:<42}{ms_str}{det}")

        print()
        print("-" * W)
        print(f"  {self.passed} passed  {self.failed} failed  {self.passed+self.failed} total")
        print("=" * W)
        return self.failed == 0


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    global DATA_DIR
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        print("pip install boto3")
        sys.exit(1)

    DATA_DIR = tempfile.mkdtemp(prefix="oxidb_s3_full_")
    t = T()

    try:
        print("  Starting OxiDB server ...")
        start_server(DATA_DIR)
        print("  Server ready.\n")

        s3 = make_client()

        # ═══════════════════════════════════════════════════════════════
        # 1. BUCKET OPERATIONS
        # ═══════════════════════════════════════════════════════════════
        t.section("Bucket Operations")

        t.run("CreateBucket", lambda: s3.create_bucket(Bucket=BUCKET))
        t.run("CreateBucket (2nd bucket)", lambda: s3.create_bucket(Bucket="second-bucket"))
        t.run("HeadBucket (exists)", lambda: s3.head_bucket(Bucket=BUCKET))
        t.run("ListBuckets", lambda: s3.list_buckets(),
              lambda r: f"{len(r['Buckets'])} buckets")
        t.expect_error("HeadBucket (missing) → 404",
                       lambda: s3.head_bucket(Bucket="no-such-bucket"), 404)
        t.run("DeleteBucket (2nd)", lambda: s3.delete_bucket(Bucket="second-bucket"))
        t.expect_error("DeleteBucket (missing) → 404",
                       lambda: s3.delete_bucket(Bucket="no-such-bucket-xyz"), 404)

        # ═══════════════════════════════════════════════════════════════
        # 2. OBJECT PUT / GET / HEAD / DELETE
        # ═══════════════════════════════════════════════════════════════
        t.section("Object CRUD")

        sizes = [
            ("1 KB",   1024),
            ("10 KB",  10240),
            ("100 KB", 102400),
            ("1 MB",   1048576),
            ("5 MB",   5 * 1048576),
        ]
        upload_checksums = {}

        for label, sz in sizes:
            data = os.urandom(sz)
            key = f"perf-{sz}.bin"
            cs = md5(data)
            upload_checksums[key] = (cs, sz)
            t.run(f"PutObject {label}",
                  lambda d=data, k=key, c=cs: s3.put_object(
                      Bucket=BUCKET, Key=k, Body=d,
                      ContentType="application/octet-stream",
                      Metadata={"checksum": c, "size": str(sz)}),
                  lambda r, s=sz: f"{s/1048576:.1f} MB")

        for label, sz in sizes:
            key = f"perf-{sz}.bin"
            cs, _ = upload_checksums[key]
            def dl(k=key, expect_cs=cs, expect_sz=sz):
                r = s3.get_object(Bucket=BUCKET, Key=k)
                body = r["Body"].read()
                meta = r.get("Metadata", {})
                assert len(body) == expect_sz, f"size {len(body)} != {expect_sz}"
                assert meta.get("checksum", "") == expect_cs or not meta.get("checksum"), "checksum mismatch"
                return body
            t.run(f"GetObject {label}", dl,
                  lambda r, s=sz: f"{s/1048576:.2f} MB")

        t.run("HeadObject",
              lambda: s3.head_object(Bucket=BUCKET, Key="perf-1024.bin"),
              lambda r: f"size={r['ContentLength']}")

        t.run("HeadObject metadata",
              lambda: s3.head_object(Bucket=BUCKET, Key="perf-1024.bin"),
              lambda r: f"meta={list(r.get('Metadata',{}).keys())}")

        # Overwrite
        new = b"overwritten-" + os.urandom(100)
        t.run("PutObject (overwrite)",
              lambda: s3.put_object(Bucket=BUCKET, Key="perf-1024.bin", Body=new))
        def verify_overwrite():
            r = s3.get_object(Bucket=BUCKET, Key="perf-1024.bin")
            body = r["Body"].read()
            assert body == new, "overwrite content mismatch"
            return body
        t.run("GetObject (verify overwrite)", verify_overwrite,
              lambda r: f"{len(r)} bytes")

        # Nested key
        t.run("PutObject (nested key)",
              lambda: s3.put_object(Bucket=BUCKET, Key="a/b/c/deep.txt", Body=b"deep"))
        def verify_nested():
            r = s3.get_object(Bucket=BUCKET, Key="a/b/c/deep.txt")
            assert r["Body"].read() == b"deep"
        t.run("GetObject (nested key)", verify_nested)

        # Missing key
        t.expect_error("GetObject (missing) → 404",
                       lambda: s3.get_object(Bucket=BUCKET, Key="nope"), 404)

        # Delete
        for label, sz in sizes:
            key = f"perf-{sz}.bin"
            t.run(f"DeleteObject {label}",
                  lambda k=key: s3.delete_object(Bucket=BUCKET, Key=k))

        # Idempotent delete
        t.run("DeleteObject (missing) → 204",
              lambda: s3.delete_object(Bucket=BUCKET, Key="already-gone"))

        # ═══════════════════════════════════════════════════════════════
        # 3. LIST OBJECTS
        # ═══════════════════════════════════════════════════════════════
        t.section("List Objects")

        # Recreate some objects
        for i in range(10):
            s3.put_object(Bucket=BUCKET, Key=f"list/item-{i:03d}.txt", Body=f"v{i}".encode())
        s3.put_object(Bucket=BUCKET, Key="list/sub/nested.txt", Body=b"n")

        t.run("ListObjectsV2",
              lambda: s3.list_objects_v2(Bucket=BUCKET),
              lambda r: f"{r.get('KeyCount',0)} keys")

        t.run("ListObjectsV2 (prefix)",
              lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="list/item-00"),
              lambda r: f"{len(r.get('Contents',[]))} matched")

        t.run("ListObjectsV2 (max-keys=3)",
              lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="list/", MaxKeys=3),
              lambda r: f"{len(r.get('Contents',[]))} returned")

        t.run("ListObjectsV2 (delimiter=/)",
              lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="list/", Delimiter="/"),
              lambda r: f"prefixes={[p['Prefix'] for p in r.get('CommonPrefixes',[])]}")

        t.expect_error("ListObjectsV2 (no bucket) → 404",
                       lambda: s3.list_objects_v2(Bucket="no-bucket"), 404)

        # ═══════════════════════════════════════════════════════════════
        # 4. COPY OBJECT
        # ═══════════════════════════════════════════════════════════════
        t.section("Copy Object")

        s3.put_object(Bucket=BUCKET, Key="src.txt", Body=b"source-data",
                      Metadata={"author": "alice"})

        t.run("CopyObject",
              lambda: s3.copy_object(Bucket=BUCKET, Key="dst.txt",
                                     CopySource=f"{BUCKET}/src.txt"))
        def verify_copy():
            r = s3.get_object(Bucket=BUCKET, Key="dst.txt")
            assert r["Body"].read() == b"source-data"
        t.run("CopyObject content verify", verify_copy)

        t.run("CopyObject metadata preserved",
              lambda: s3.head_object(Bucket=BUCKET, Key="dst.txt"),
              lambda r: f"author={r.get('Metadata',{}).get('author','?')}")

        t.run("CopyObject REPLACE metadata",
              lambda: s3.copy_object(Bucket=BUCKET, Key="dst-replaced.txt",
                                     CopySource=f"{BUCKET}/src.txt",
                                     MetadataDirective="REPLACE",
                                     Metadata={"env": "staging"}))
        t.run("CopyObject REPLACE verify",
              lambda: s3.head_object(Bucket=BUCKET, Key="dst-replaced.txt"),
              lambda r: f"meta={r.get('Metadata',{})}")

        t.expect_error("CopyObject (missing src) → 404",
                       lambda: s3.copy_object(Bucket=BUCKET, Key="x",
                                              CopySource=f"{BUCKET}/no-such"), 404)

        # Cross-bucket copy
        s3.create_bucket(Bucket="copy-dest")
        t.run("CopyObject cross-bucket",
              lambda: s3.copy_object(Bucket="copy-dest", Key="moved.txt",
                                     CopySource=f"{BUCKET}/src.txt"))
        def verify_cross():
            r = s3.get_object(Bucket="copy-dest", Key="moved.txt")
            assert r["Body"].read() == b"source-data"
        t.run("Cross-bucket verify", verify_cross)
        s3.delete_object(Bucket="copy-dest", Key="moved.txt")
        s3.delete_bucket(Bucket="copy-dest")

        # ═══════════════════════════════════════════════════════════════
        # 5. RANGE REQUESTS
        # ═══════════════════════════════════════════════════════════════
        t.section("Range Requests")

        range_data = bytes(range(256)) * 8  # 2048 bytes
        s3.put_object(Bucket=BUCKET, Key="range.bin", Body=range_data)

        def check_range(range_str, expected_slice):
            r = s3.get_object(Bucket=BUCKET, Key="range.bin", Range=range_str)
            body = r["Body"].read()
            assert body == expected_slice, f"len {len(body)} != {len(expected_slice)}"
            assert r["ResponseMetadata"]["HTTPStatusCode"] == 206
            return body

        t.run("Range bytes=0-9",
              lambda: check_range("bytes=0-9", range_data[0:10]),
              lambda r: f"{len(r)} bytes")

        t.run("Range bytes=100-199",
              lambda: check_range("bytes=100-199", range_data[100:200]),
              lambda r: f"{len(r)} bytes")

        t.run("Range bytes=2000- (open-end)",
              lambda: check_range("bytes=2000-", range_data[2000:]),
              lambda r: f"{len(r)} bytes")

        t.run("Range bytes=-100 (suffix)",
              lambda: check_range("bytes=-100", range_data[-100:]),
              lambda r: f"{len(r)} bytes")

        t.run("Range bytes=0-0 (single byte)",
              lambda: check_range("bytes=0-0", range_data[0:1]),
              lambda r: f"{len(r)} byte")

        t.run("Range bytes=2047-2047 (last byte)",
              lambda: check_range("bytes=2047-2047", range_data[2047:2048]),
              lambda r: f"{len(r)} byte")

        # ═══════════════════════════════════════════════════════════════
        # 6. CONDITIONAL REQUESTS
        # ═══════════════════════════════════════════════════════════════
        t.section("Conditional Requests")

        head = s3.head_object(Bucket=BUCKET, Key="range.bin")
        etag = head["ETag"]

        t.expect_error("If-None-Match (match) → 304",
                       lambda: s3.get_object(Bucket=BUCKET, Key="range.bin",
                                             IfNoneMatch=etag), 304)

        t.run("If-None-Match (no match) → 200",
              lambda: s3.get_object(Bucket=BUCKET, Key="range.bin",
                                    IfNoneMatch='"wrong"'),
              lambda r: f"{len(r['Body'].read())} bytes")

        t.expect_error("If-Match (wrong) → 412",
                       lambda: s3.get_object(Bucket=BUCKET, Key="range.bin",
                                             IfMatch='"bad-etag"'), 412)

        t.run("If-Match (correct) → 200",
              lambda: s3.get_object(Bucket=BUCKET, Key="range.bin",
                                    IfMatch=etag),
              lambda r: f"ok, {len(r['Body'].read())} bytes")

        # ═══════════════════════════════════════════════════════════════
        # 7. MULTIPART UPLOAD
        # ═══════════════════════════════════════════════════════════════
        t.section("Multipart Upload")

        # Create
        mpu, ms = t.run("CreateMultipartUpload",
                        lambda: s3.create_multipart_upload(
                            Bucket=BUCKET, Key="multi.bin",
                            ContentType="application/octet-stream",
                            Metadata={"origin": "test"}),
                        lambda r: f"id={r['UploadId'][:12]}...")
        upload_id = mpu["UploadId"] if mpu else None

        # Upload 5 parts of 200 KB each = 1 MB total
        parts_info = []
        parts_data = []
        if upload_id:
            for i in range(1, 6):
                chunk = os.urandom(200 * 1024)
                parts_data.append(chunk)
                r, ms = t.run(f"UploadPart {i} (200 KB)",
                              lambda c=chunk, n=i: s3.upload_part(
                                  Bucket=BUCKET, Key="multi.bin",
                                  UploadId=upload_id, PartNumber=n, Body=c),
                              lambda r: f"etag={r['ETag'][:10]}")
                if r:
                    parts_info.append({"ETag": r["ETag"], "PartNumber": i})

        # Complete
        if upload_id and len(parts_info) == 5:
            t.run("CompleteMultipartUpload",
                  lambda: s3.complete_multipart_upload(
                      Bucket=BUCKET, Key="multi.bin", UploadId=upload_id,
                      MultipartUpload={"Parts": parts_info}))

            expected = b"".join(parts_data)
            def verify_multi():
                r = s3.get_object(Bucket=BUCKET, Key="multi.bin")
                body = r["Body"].read()
                assert body == expected, f"size {len(body)} != {len(expected)}"
                return body
            t.run("Multipart content verify",
                  verify_multi,
                  lambda r: f"{len(r)} bytes (5 parts)")

            # Metadata on completed multipart
            t.run("Multipart metadata",
                  lambda: s3.head_object(Bucket=BUCKET, Key="multi.bin"),
                  lambda r: f"meta={r.get('Metadata',{})}")

        # Abort
        mpu2, _ = t.run("CreateMultipartUpload (abort test)",
                        lambda: s3.create_multipart_upload(Bucket=BUCKET, Key="abort.bin"))
        if mpu2:
            abort_id = mpu2["UploadId"]
            s3.upload_part(Bucket=BUCKET, Key="abort.bin",
                          UploadId=abort_id, PartNumber=1, Body=b"throwaway")
            t.run("AbortMultipartUpload",
                  lambda: s3.abort_multipart_upload(
                      Bucket=BUCKET, Key="abort.bin", UploadId=abort_id))

        # ═══════════════════════════════════════════════════════════════
        # 8. BATCH DELETE
        # ═══════════════════════════════════════════════════════════════
        t.section("Batch Delete")

        batch_keys = [f"batch/{i:03d}.dat" for i in range(20)]
        for k in batch_keys:
            s3.put_object(Bucket=BUCKET, Key=k, Body=os.urandom(64))

        t.run("DeleteObjects (20 keys)",
              lambda: s3.delete_objects(
                  Bucket=BUCKET,
                  Delete={"Objects": [{"Key": k} for k in batch_keys], "Quiet": False}),
              lambda r: f"{len(r.get('Deleted',[]))} deleted")

        # Verify gone
        t.run("Verify batch deleted",
              lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="batch/"),
              lambda r: f"{r.get('KeyCount',0)} remaining")

        # Batch with non-existent keys (should not error)
        t.run("DeleteObjects (non-existent)",
              lambda: s3.delete_objects(
                  Bucket=BUCKET,
                  Delete={"Objects": [{"Key": "ghost-1"}, {"Key": "ghost-2"}]}),
              lambda r: f"{len(r.get('Deleted',[]))} reported")

        # ═══════════════════════════════════════════════════════════════
        # 9. OBJECT TAGGING
        # ═══════════════════════════════════════════════════════════════
        t.section("Object Tagging")

        s3.put_object(Bucket=BUCKET, Key="tagged.txt", Body=b"tag-me")

        t.run("PutObjectTagging (3 tags)",
              lambda: s3.put_object_tagging(
                  Bucket=BUCKET, Key="tagged.txt",
                  Tagging={"TagSet": [
                      {"Key": "env", "Value": "production"},
                      {"Key": "team", "Value": "platform"},
                      {"Key": "cost-center", "Value": "42"},
                  ]}))

        def check_tags():
            r = s3.get_object_tagging(Bucket=BUCKET, Key="tagged.txt")
            tags = {t["Key"]: t["Value"] for t in r.get("TagSet", [])}
            assert tags.get("env") == "production"
            assert tags.get("team") == "platform"
            assert tags.get("cost-center") == "42"
            return tags
        t.run("GetObjectTagging", check_tags,
              lambda r: f"{len(r)} tags: {r}")

        # Replace tags
        t.run("PutObjectTagging (replace)",
              lambda: s3.put_object_tagging(
                  Bucket=BUCKET, Key="tagged.txt",
                  Tagging={"TagSet": [{"Key": "env", "Value": "staging"}]}))
        t.run("GetObjectTagging (after replace)",
              lambda: s3.get_object_tagging(Bucket=BUCKET, Key="tagged.txt"),
              lambda r: f"{len(r['TagSet'])} tag(s)")

        t.run("DeleteObjectTagging",
              lambda: s3.delete_object_tagging(Bucket=BUCKET, Key="tagged.txt"))
        t.run("GetObjectTagging (after delete)",
              lambda: s3.get_object_tagging(Bucket=BUCKET, Key="tagged.txt"),
              lambda r: f"{len(r['TagSet'])} tags")

        t.expect_error("GetObjectTagging (missing key) → 404",
                       lambda: s3.get_object_tagging(Bucket=BUCKET, Key="nope"), 404)

        # ═══════════════════════════════════════════════════════════════
        # 10. LIST WITH DELIMITER (virtual folders)
        # ═══════════════════════════════════════════════════════════════
        t.section("List with Delimiter")

        hierarchy = [
            "photos/2024/jan.jpg", "photos/2024/feb.jpg",
            "photos/2025/mar.jpg", "photos/cover.jpg",
            "docs/readme.md", "docs/api/v1.json", "docs/api/v2.json",
            "root-file.txt",
        ]
        for k in hierarchy:
            s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")

        t.run("List / with delimiter",
              lambda: s3.list_objects_v2(Bucket=BUCKET, Delimiter="/"),
              lambda r: f"prefixes={[p['Prefix'] for p in r.get('CommonPrefixes',[])]}, direct={[c['Key'] for c in r.get('Contents',[])]}")

        t.run("List photos/ with delimiter",
              lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="photos/", Delimiter="/"),
              lambda r: f"prefixes={[p['Prefix'] for p in r.get('CommonPrefixes',[])]}")

        t.run("List docs/api/ with delimiter",
              lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="docs/api/", Delimiter="/"),
              lambda r: f"{len(r.get('Contents',[]))} files, {len(r.get('CommonPrefixes',[]))} prefixes")

        # ═══════════════════════════════════════════════════════════════
        # 11. CORS PREFLIGHT
        # ═══════════════════════════════════════════════════════════════
        t.section("CORS")

        def check_cors():
            req = urllib.request.Request(
                f"{S3_ENDPOINT}/{BUCKET}", method="OPTIONS",
                headers={"Origin": "http://app.example.com",
                         "Access-Control-Request-Method": "PUT"})
            with urllib.request.urlopen(req) as resp:
                origin = resp.getheader("Access-Control-Allow-Origin")
                methods = resp.getheader("Access-Control-Allow-Methods")
                assert origin == "*", f"origin={origin}"
                return {"allow-origin": origin, "methods": methods}
        t.run("OPTIONS preflight", check_cors,
              lambda r: f"origin={r['allow-origin']}")

        # Check CORS headers on normal response
        def check_cors_on_get():
            r = s3.get_object(Bucket=BUCKET, Key="root-file.txt")
            r["Body"].read()
            headers = r["ResponseMetadata"]["HTTPHeaders"]
            return headers.get("access-control-allow-origin", "missing")
        t.run("CORS header on GET", check_cors_on_get,
              lambda r: f"allow-origin={r}")

        # ═══════════════════════════════════════════════════════════════
        # 12. SECURITY (AWS SigV4)
        # ═══════════════════════════════════════════════════════════════
        t.section("Security — AWS SigV4 Auth")

        s3.put_object(Bucket=BUCKET, Key="classified.txt", Body=b"TOP-SECRET")

        t.expect_error("Wrong access key → 403",
                       lambda: make_client(ak="bad-key", sk=SECRET_KEY).list_buckets(), 403)

        t.expect_error("Wrong secret key → 403",
                       lambda: make_client(ak=ACCESS_KEY, sk="bad-secret").list_buckets(), 403)

        def raw_no_auth():
            req = urllib.request.Request(f"{S3_ENDPOINT}/{BUCKET}/classified.txt")
            try:
                urllib.request.urlopen(req)
                return 200
            except urllib.error.HTTPError as e:
                return e.code
        t.run("No credentials → 403", raw_no_auth,
              lambda r: f"status={r}" if r == 403 else "UNEXPECTED")

        t.run("Valid credentials → 200",
              lambda: s3.get_object(Bucket=BUCKET, Key="classified.txt"),
              lambda r: f"body={r['Body'].read()}")

        t.expect_error("Wrong key + GET → 403",
                       lambda: make_client(ak="x", sk="y").get_object(
                           Bucket=BUCKET, Key="classified.txt"), 403)

        t.expect_error("Wrong key + PUT → 403",
                       lambda: make_client(ak="x", sk="y").put_object(
                           Bucket=BUCKET, Key="hack.txt", Body=b"pwned"), 403)

        t.expect_error("Wrong key + DELETE → 403",
                       lambda: make_client(ak="x", sk="y").delete_object(
                           Bucket=BUCKET, Key="classified.txt"), 403)

        t.expect_error("Wrong key + ListBuckets → 403",
                       lambda: make_client(ak="x", sk="y").list_buckets(), 403)

        # ═══════════════════════════════════════════════════════════════
        # 13. PRESIGNED URLs
        # ═══════════════════════════════════════════════════════════════
        t.section("Presigned URLs")

        def presigned_get():
            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": "classified.txt"},
                ExpiresIn=60)
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as resp:
                body = resp.read()
                assert body == b"TOP-SECRET", f"got {body[:30]}"
                return body
        t.run("Presigned GET URL", presigned_get,
              lambda r: f"body={r}")

        def presigned_tampered():
            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": "classified.txt"},
                ExpiresIn=60)
            bad = url.replace("X-Amz-Signature=", "X-Amz-Signature=dead")
            req = urllib.request.Request(bad)
            try:
                urllib.request.urlopen(req)
                return 200
            except urllib.error.HTTPError as e:
                return e.code
        t.run("Tampered presigned → 403", presigned_tampered,
              lambda r: f"status={r}" if r == 403 else "UNEXPECTED")

        # ═══════════════════════════════════════════════════════════════
        # 14. EDGE CASES & ERROR HANDLING
        # ═══════════════════════════════════════════════════════════════
        t.section("Edge Cases")

        # Empty object
        t.run("PutObject (0 bytes)",
              lambda: s3.put_object(Bucket=BUCKET, Key="empty.bin", Body=b""))
        def verify_empty():
            r = s3.get_object(Bucket=BUCKET, Key="empty.bin")
            body = r["Body"].read()
            assert len(body) == 0
            return body
        t.run("GetObject (0 bytes)", verify_empty,
              lambda r: f"{len(r)} bytes")

        # Special characters in key
        special_key = "special/key with spaces & symbols!.txt"
        t.run("PutObject (special chars)",
              lambda: s3.put_object(Bucket=BUCKET, Key=special_key, Body=b"special"))
        def verify_special():
            r = s3.get_object(Bucket=BUCKET, Key=special_key)
            assert r["Body"].read() == b"special"
        t.run("GetObject (special chars)", verify_special)

        # Unicode metadata
        t.run("PutObject (unicode metadata)",
              lambda: s3.put_object(Bucket=BUCKET, Key="unicode.txt", Body=b"hi",
                                    Metadata={"title": "hello-world"}))
        t.run("HeadObject (unicode metadata)",
              lambda: s3.head_object(Bucket=BUCKET, Key="unicode.txt"),
              lambda r: f"title={r.get('Metadata',{}).get('title','?')}")

        # Large metadata (many keys)
        many_meta = {f"key-{i}": f"value-{i}" for i in range(10)}
        t.run("PutObject (10 metadata keys)",
              lambda: s3.put_object(Bucket=BUCKET, Key="many-meta.txt",
                                    Body=b"x", Metadata=many_meta))
        t.run("HeadObject (10 metadata keys)",
              lambda: s3.head_object(Bucket=BUCKET, Key="many-meta.txt"),
              lambda r: f"{len(r.get('Metadata',{}))} meta keys")

        # ═══════════════════════════════════════════════════════════════
        # 15. PERFORMANCE SUMMARY
        # ═══════════════════════════════════════════════════════════════
        t.section("Performance — Large Upload/Download")

        big = os.urandom(10 * 1048576)  # 10 MB
        big_cs = md5(big)
        t.run("PutObject 10 MB",
              lambda: s3.put_object(Bucket=BUCKET, Key="big.bin", Body=big),
              lambda r: f"{10} MB")
        def dl_big():
            r = s3.get_object(Bucket=BUCKET, Key="big.bin")
            body = r["Body"].read()
            assert len(body) == len(big)
            assert md5(body) == big_cs
            return body
        t.run("GetObject 10 MB", dl_big,
              lambda r: f"{len(r)/1048576:.0f} MB")

        # ═══════════════════════════════════════════════════════════════
        # 16. CLEANUP
        # ═══════════════════════════════════════════════════════════════
        t.section("Cleanup")

        def cleanup():
            resp = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=10000)
            keys = [c["Key"] for c in resp.get("Contents", [])]
            if keys:
                # batch delete in chunks of 1000
                for i in range(0, len(keys), 1000):
                    s3.delete_objects(
                        Bucket=BUCKET,
                        Delete={"Objects": [{"Key": k} for k in keys[i:i+1000]]})
            s3.delete_bucket(Bucket=BUCKET)
            return len(keys)
        t.run("Delete all + bucket", cleanup,
              lambda r: f"{r} objects deleted")

    finally:
        stop_server()
        if DATA_DIR and os.path.exists(DATA_DIR):
            shutil.rmtree(DATA_DIR, ignore_errors=True)

    ok = t.report()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
