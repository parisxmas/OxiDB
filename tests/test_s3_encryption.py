#!/usr/bin/env python3
"""
OxiDB S3 Integration Tests — Encryption & Full Feature Suite

Tests SSE-S3, SSE-C, default encryption, plus all core S3 ops.
Starts an OxiDB server automatically, runs tests, then shuts down.

Requirements: pip install boto3
"""

import base64, hashlib, os, shutil, signal, subprocess, sys, time

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError:
    print("pip install boto3")
    sys.exit(1)

# ── Config ───────────────────────────────────────────────────────────────

PORT = 4444
S3_PORT = 9100
ENDPOINT = f"http://127.0.0.1:{S3_PORT}"
ACCESS_KEY = "test-access-key"
SECRET_KEY = "test-secret-key-2026"
# 32-byte hex key for SSE-S3
ENCRYPTION_KEY_HEX = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
DATA_DIR = "/tmp/oxidb_s3_enc_test"
BUCKET = "enc-test"

# SSE-C: 32-byte customer key
CUSTOMER_KEY = os.urandom(32)
CUSTOMER_KEY_B64 = base64.b64encode(CUSTOMER_KEY).decode()
CUSTOMER_KEY_MD5 = base64.b64encode(hashlib.md5(CUSTOMER_KEY).digest()).decode()

# Second customer key for re-encryption tests
CUSTOMER_KEY_2 = os.urandom(32)
CUSTOMER_KEY_2_B64 = base64.b64encode(CUSTOMER_KEY_2).decode()
CUSTOMER_KEY_2_MD5 = base64.b64encode(hashlib.md5(CUSTOMER_KEY_2).digest()).decode()

passed = 0
failed = 0
errors = []


def make_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 0},
        ),
    )


def ok(name):
    global passed
    passed += 1
    print(f"  \033[32mPASS\033[0m  {name}")


def fail(name, reason=""):
    global failed
    failed += 1
    errors.append((name, reason))
    print(f"  \033[31mFAIL\033[0m  {name}  {reason}")


def assert_eq(name, got, expected):
    if got == expected:
        ok(name)
    else:
        fail(name, f"expected={expected!r} got={got!r}")


def assert_true(name, cond, detail=""):
    if cond:
        ok(name)
    else:
        fail(name, detail)


def start_server():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)

    env = os.environ.copy()
    env["OXIDB_ADDR"] = f"127.0.0.1:{PORT}"
    env["OXIDB_DATA"] = DATA_DIR
    env["OXIDB_S3_PORT"] = str(S3_PORT)
    env["OXIDB_S3_ACCESS_KEY"] = ACCESS_KEY
    env["OXIDB_S3_SECRET_KEY"] = SECRET_KEY
    env["OXIDB_S3_ENCRYPTION_KEY"] = ENCRYPTION_KEY_HEX

    binary = os.path.join(os.path.dirname(__file__), "..", "target", "release", "oxidb-server")
    proc = subprocess.Popen(
        [binary],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for server to start
    for _ in range(30):
        try:
            import socket
            s = socket.socket()
            s.settimeout(0.5)
            s.connect(("127.0.0.1", S3_PORT))
            s.close()
            return proc
        except Exception:
            time.sleep(0.2)
    print("ERROR: Server did not start within 6 seconds")
    proc.kill()
    sys.exit(1)


def stop_server(proc):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)


# ── Test Groups ──────────────────────────────────────────────────────────

def test_basic_crud(s3):
    """Basic bucket and object CRUD without encryption."""
    print("\n── Basic CRUD ──")

    # Create bucket
    try:
        s3.create_bucket(Bucket=BUCKET)
        ok("create_bucket")
    except Exception as e:
        fail("create_bucket", str(e))
        return

    # PUT object (no encryption header)
    data = b"Hello, OxiDB S3!"
    s3.put_object(Bucket=BUCKET, Key="plain.txt", Body=data, ContentType="text/plain")
    ok("put_object (plain)")

    # GET object
    resp = s3.get_object(Bucket=BUCKET, Key="plain.txt")
    body = resp["Body"].read()
    assert_eq("get_object (plain)", body, data)

    # HEAD object
    resp = s3.head_object(Bucket=BUCKET, Key="plain.txt")
    assert_eq("head_object content_type", resp["ContentType"], "text/plain")
    assert_eq("head_object size", resp["ContentLength"], len(data))

    # DELETE object
    s3.delete_object(Bucket=BUCKET, Key="plain.txt")
    try:
        s3.get_object(Bucket=BUCKET, Key="plain.txt")
        fail("delete_object", "object should be gone")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            ok("delete_object")
        else:
            fail("delete_object", str(e))


def test_sse_s3(s3):
    """SSE-S3: Server-side encryption with server-managed key."""
    print("\n── SSE-S3 (Server-Managed Key) ──")

    data = os.urandom(4096)

    # PUT with SSE-S3
    resp = s3.put_object(
        Bucket=BUCKET, Key="sse-s3.bin", Body=data,
        ServerSideEncryption="AES256",
    )
    sse_header = resp.get("ServerSideEncryption", "")
    assert_eq("put SSE-S3 header", sse_header, "AES256")

    # GET decrypts automatically
    resp = s3.get_object(Bucket=BUCKET, Key="sse-s3.bin")
    body = resp["Body"].read()
    assert_eq("get SSE-S3 roundtrip", body, data)
    assert_eq("get SSE-S3 header", resp.get("ServerSideEncryption", ""), "AES256")

    # HEAD shows encryption
    resp = s3.head_object(Bucket=BUCKET, Key="sse-s3.bin")
    assert_eq("head SSE-S3 header", resp.get("ServerSideEncryption", ""), "AES256")

    # Various sizes
    for size_name, size in [("1B", 1), ("1KB", 1024), ("64KB", 65536), ("1MB", 1048576)]:
        payload = os.urandom(size)
        s3.put_object(Bucket=BUCKET, Key=f"sse-s3-{size_name}.bin", Body=payload,
                      ServerSideEncryption="AES256")
        got = s3.get_object(Bucket=BUCKET, Key=f"sse-s3-{size_name}.bin")["Body"].read()
        assert_eq(f"SSE-S3 roundtrip {size_name}", got, payload)

    # Cleanup
    for k in ["sse-s3.bin", "sse-s3-1B.bin", "sse-s3-1KB.bin", "sse-s3-64KB.bin", "sse-s3-1MB.bin"]:
        s3.delete_object(Bucket=BUCKET, Key=k)


def test_sse_c(s3):
    """SSE-C: Server-side encryption with customer-provided key."""
    print("\n── SSE-C (Customer-Provided Key) ──")

    data = os.urandom(8192)

    # PUT with SSE-C
    resp = s3.put_object(
        Bucket=BUCKET, Key="sse-c.bin", Body=data,
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )
    assert_eq("put SSE-C algo header", resp.get("SSECustomerAlgorithm", ""), "AES256")

    # GET with correct key
    resp = s3.get_object(
        Bucket=BUCKET, Key="sse-c.bin",
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )
    body = resp["Body"].read()
    assert_eq("get SSE-C roundtrip", body, data)

    # GET without key should fail
    try:
        s3.get_object(Bucket=BUCKET, Key="sse-c.bin")
        fail("get SSE-C without key", "should have failed")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        assert_true("get SSE-C without key rejected", code in ("InvalidRequest", "400"), f"code={code}")

    # GET with wrong key should fail
    wrong_key = os.urandom(32)
    wrong_b64 = base64.b64encode(wrong_key).decode()
    wrong_md5 = base64.b64encode(hashlib.md5(wrong_key).digest()).decode()
    try:
        s3.get_object(
            Bucket=BUCKET, Key="sse-c.bin",
            SSECustomerAlgorithm="AES256",
            SSECustomerKey=wrong_b64,
            SSECustomerKeyMD5=wrong_md5,
        )
        fail("get SSE-C wrong key", "should have failed")
    except (ClientError, Exception) as e:
        ok("get SSE-C wrong key rejected")

    # HEAD with SSE-C
    resp = s3.head_object(
        Bucket=BUCKET, Key="sse-c.bin",
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )
    assert_eq("head SSE-C algo", resp.get("SSECustomerAlgorithm", ""), "AES256")

    # Various sizes
    for size_name, size in [("1B", 1), ("16KB", 16384), ("256KB", 262144)]:
        payload = os.urandom(size)
        s3.put_object(Bucket=BUCKET, Key=f"sse-c-{size_name}.bin", Body=payload,
                      SSECustomerAlgorithm="AES256",
                      SSECustomerKey=CUSTOMER_KEY_B64,
                      SSECustomerKeyMD5=CUSTOMER_KEY_MD5)
        got = s3.get_object(Bucket=BUCKET, Key=f"sse-c-{size_name}.bin",
                            SSECustomerAlgorithm="AES256",
                            SSECustomerKey=CUSTOMER_KEY_B64,
                            SSECustomerKeyMD5=CUSTOMER_KEY_MD5)["Body"].read()
        assert_eq(f"SSE-C roundtrip {size_name}", got, payload)

    # Cleanup
    for k in ["sse-c.bin", "sse-c-1B.bin", "sse-c-16KB.bin", "sse-c-256KB.bin"]:
        s3.delete_object(Bucket=BUCKET, Key=k)


def test_sse_mixed(s3):
    """Mix of encrypted and unencrypted objects in the same bucket."""
    print("\n── Mixed Encryption ──")

    plain_data = b"I am plaintext"
    sse_s3_data = b"I am SSE-S3 encrypted"
    sse_c_data = b"I am SSE-C encrypted"

    # Store all three
    s3.put_object(Bucket=BUCKET, Key="mix/plain.txt", Body=plain_data)
    s3.put_object(Bucket=BUCKET, Key="mix/sse-s3.txt", Body=sse_s3_data,
                  ServerSideEncryption="AES256")
    s3.put_object(Bucket=BUCKET, Key="mix/sse-c.txt", Body=sse_c_data,
                  SSECustomerAlgorithm="AES256",
                  SSECustomerKey=CUSTOMER_KEY_B64,
                  SSECustomerKeyMD5=CUSTOMER_KEY_MD5)

    # Retrieve each
    got_plain = s3.get_object(Bucket=BUCKET, Key="mix/plain.txt")["Body"].read()
    assert_eq("mixed: plain roundtrip", got_plain, plain_data)

    got_s3 = s3.get_object(Bucket=BUCKET, Key="mix/sse-s3.txt")["Body"].read()
    assert_eq("mixed: SSE-S3 roundtrip", got_s3, sse_s3_data)

    got_c = s3.get_object(
        Bucket=BUCKET, Key="mix/sse-c.txt",
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )["Body"].read()
    assert_eq("mixed: SSE-C roundtrip", got_c, sse_c_data)

    # List should show all 3
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="mix/")
    keys = [o["Key"] for o in resp.get("Contents", [])]
    assert_eq("mixed: list count", len(keys), 3)

    # Cleanup
    for k in ["mix/plain.txt", "mix/sse-s3.txt", "mix/sse-c.txt"]:
        s3.delete_object(Bucket=BUCKET, Key=k)


def test_sse_s3_range_request(s3):
    """Range requests on SSE-S3 encrypted objects."""
    print("\n── SSE-S3 Range Requests ──")

    data = bytes(range(256)) * 40  # 10240 bytes, predictable pattern
    s3.put_object(Bucket=BUCKET, Key="range-enc.bin", Body=data,
                  ServerSideEncryption="AES256")

    # First 100 bytes
    resp = s3.get_object(Bucket=BUCKET, Key="range-enc.bin", Range="bytes=0-99")
    body = resp["Body"].read()
    assert_eq("range SSE-S3: first 100 bytes", body, data[:100])

    # Middle range
    resp = s3.get_object(Bucket=BUCKET, Key="range-enc.bin", Range="bytes=500-599")
    body = resp["Body"].read()
    assert_eq("range SSE-S3: middle 100 bytes", body, data[500:600])

    # Last 50 bytes
    resp = s3.get_object(Bucket=BUCKET, Key="range-enc.bin", Range="bytes=-50")
    body = resp["Body"].read()
    assert_eq("range SSE-S3: last 50 bytes", body, data[-50:])

    s3.delete_object(Bucket=BUCKET, Key="range-enc.bin")


def test_sse_c_range_request(s3):
    """Range requests on SSE-C encrypted objects."""
    print("\n── SSE-C Range Requests ──")

    data = os.urandom(8192)
    s3.put_object(Bucket=BUCKET, Key="range-ssec.bin", Body=data,
                  SSECustomerAlgorithm="AES256",
                  SSECustomerKey=CUSTOMER_KEY_B64,
                  SSECustomerKeyMD5=CUSTOMER_KEY_MD5)

    resp = s3.get_object(
        Bucket=BUCKET, Key="range-ssec.bin", Range="bytes=0-255",
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )
    body = resp["Body"].read()
    assert_eq("range SSE-C: first 256 bytes", body, data[:256])

    resp = s3.get_object(
        Bucket=BUCKET, Key="range-ssec.bin", Range="bytes=4096-4351",
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )
    body = resp["Body"].read()
    assert_eq("range SSE-C: mid 256 bytes", body, data[4096:4352])

    s3.delete_object(Bucket=BUCKET, Key="range-ssec.bin")


def test_copy_with_encryption(s3):
    """Copy objects between encryption modes."""
    print("\n── Copy with Encryption ──")

    data = b"copy-me-with-encryption"

    # 1. Plain → SSE-S3 copy
    s3.put_object(Bucket=BUCKET, Key="copy/src-plain.txt", Body=data)
    s3.copy_object(
        Bucket=BUCKET, Key="copy/dst-sse-s3.txt",
        CopySource=f"{BUCKET}/copy/src-plain.txt",
        ServerSideEncryption="AES256",
    )
    got = s3.get_object(Bucket=BUCKET, Key="copy/dst-sse-s3.txt")["Body"].read()
    assert_eq("copy plain→SSE-S3", got, data)
    resp = s3.head_object(Bucket=BUCKET, Key="copy/dst-sse-s3.txt")
    assert_eq("copy plain→SSE-S3 header", resp.get("ServerSideEncryption", ""), "AES256")

    # 2. SSE-S3 → Plain copy (no encryption header = plain if no default encryption)
    s3.put_object(Bucket=BUCKET, Key="copy/src-sse-s3.txt", Body=data,
                  ServerSideEncryption="AES256")
    s3.copy_object(
        Bucket=BUCKET, Key="copy/dst-plain.txt",
        CopySource=f"{BUCKET}/copy/src-sse-s3.txt",
    )
    got = s3.get_object(Bucket=BUCKET, Key="copy/dst-plain.txt")["Body"].read()
    assert_eq("copy SSE-S3→plain", got, data)

    # 3. SSE-S3 → SSE-S3 copy
    s3.copy_object(
        Bucket=BUCKET, Key="copy/dst-sse-s3-2.txt",
        CopySource=f"{BUCKET}/copy/src-sse-s3.txt",
        ServerSideEncryption="AES256",
    )
    got = s3.get_object(Bucket=BUCKET, Key="copy/dst-sse-s3-2.txt")["Body"].read()
    assert_eq("copy SSE-S3→SSE-S3", got, data)

    # Cleanup
    for k in ["copy/src-plain.txt", "copy/dst-sse-s3.txt", "copy/src-sse-s3.txt",
              "copy/dst-plain.txt", "copy/dst-sse-s3-2.txt"]:
        s3.delete_object(Bucket=BUCKET, Key=k)


def test_metadata_with_encryption(s3):
    """User metadata preserved through encryption."""
    print("\n── Metadata with Encryption ──")

    data = b"metadata test"
    meta = {"author": "oxidb", "version": "1.0", "tag": "encrypted"}

    # SSE-S3 with metadata
    s3.put_object(Bucket=BUCKET, Key="meta-enc.txt", Body=data,
                  ServerSideEncryption="AES256",
                  Metadata=meta)
    resp = s3.head_object(Bucket=BUCKET, Key="meta-enc.txt")
    got_meta = resp.get("Metadata", {})
    assert_eq("metadata SSE-S3: author", got_meta.get("author"), "oxidb")
    assert_eq("metadata SSE-S3: version", got_meta.get("version"), "1.0")
    assert_eq("metadata SSE-S3: tag", got_meta.get("tag"), "encrypted")

    # SSE-C with metadata
    s3.put_object(Bucket=BUCKET, Key="meta-ssec.txt", Body=data,
                  SSECustomerAlgorithm="AES256",
                  SSECustomerKey=CUSTOMER_KEY_B64,
                  SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
                  Metadata=meta)
    resp = s3.head_object(
        Bucket=BUCKET, Key="meta-ssec.txt",
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )
    got_meta = resp.get("Metadata", {})
    assert_eq("metadata SSE-C: author", got_meta.get("author"), "oxidb")
    assert_eq("metadata SSE-C: version", got_meta.get("version"), "1.0")

    # Cleanup
    s3.delete_object(Bucket=BUCKET, Key="meta-enc.txt")
    s3.delete_object(Bucket=BUCKET, Key="meta-ssec.txt")


def test_overwrite_encryption(s3):
    """Overwrite object with different encryption mode."""
    print("\n── Overwrite Encryption Mode ──")

    key = "overwrite-enc.txt"

    # Start plain
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"version-1-plain")
    got = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert_eq("overwrite: v1 plain", got, b"version-1-plain")

    # Overwrite with SSE-S3
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"version-2-sse-s3",
                  ServerSideEncryption="AES256")
    got = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert_eq("overwrite: v2 SSE-S3", got, b"version-2-sse-s3")

    # Overwrite with SSE-C
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"version-3-sse-c",
                  SSECustomerAlgorithm="AES256",
                  SSECustomerKey=CUSTOMER_KEY_B64,
                  SSECustomerKeyMD5=CUSTOMER_KEY_MD5)
    got = s3.get_object(
        Bucket=BUCKET, Key=key,
        SSECustomerAlgorithm="AES256",
        SSECustomerKey=CUSTOMER_KEY_B64,
        SSECustomerKeyMD5=CUSTOMER_KEY_MD5,
    )["Body"].read()
    assert_eq("overwrite: v3 SSE-C", got, b"version-3-sse-c")

    # Overwrite back to plain
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"version-4-plain-again")
    got = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert_eq("overwrite: v4 plain again", got, b"version-4-plain-again")

    s3.delete_object(Bucket=BUCKET, Key=key)


def test_multipart_sse_s3(s3):
    """Multipart upload with SSE-S3."""
    print("\n── Multipart SSE-S3 ──")

    key = "multipart-sse-s3.bin"
    part_size = 1024 * 1024  # 1MB parts
    part1 = os.urandom(part_size)
    part2 = os.urandom(part_size)
    part3 = os.urandom(512 * 1024)  # 512KB last part
    full_data = part1 + part2 + part3

    # Initiate
    resp = s3.create_multipart_upload(
        Bucket=BUCKET, Key=key,
        ServerSideEncryption="AES256",
    )
    upload_id = resp["UploadId"]
    assert_true("multipart SSE-S3: initiate", bool(upload_id))
    assert_eq("multipart SSE-S3: init header", resp.get("ServerSideEncryption", ""), "AES256")

    # Upload parts
    parts = []
    for i, part_data in enumerate([part1, part2, part3], 1):
        resp = s3.upload_part(
            Bucket=BUCKET, Key=key, UploadId=upload_id,
            PartNumber=i, Body=part_data,
        )
        parts.append({"ETag": resp["ETag"], "PartNumber": i})
    ok("multipart SSE-S3: upload 3 parts")

    # Complete
    resp = s3.complete_multipart_upload(
        Bucket=BUCKET, Key=key, UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    ok("multipart SSE-S3: complete")

    # Verify
    got = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert_eq("multipart SSE-S3: data integrity", got, full_data)
    assert_eq("multipart SSE-S3: size", len(got), len(full_data))

    s3.delete_object(Bucket=BUCKET, Key=key)


def test_conditional_with_encryption(s3):
    """Conditional requests (If-Match, If-None-Match) on encrypted objects."""
    print("\n── Conditional Requests + Encryption ──")

    data = b"conditional-encrypted-data"
    s3.put_object(Bucket=BUCKET, Key="cond-enc.txt", Body=data,
                  ServerSideEncryption="AES256")

    # Get ETag
    head = s3.head_object(Bucket=BUCKET, Key="cond-enc.txt")
    etag = head["ETag"]

    # If-None-Match with matching ETag → 304
    try:
        s3.get_object(Bucket=BUCKET, Key="cond-enc.txt", IfNoneMatch=etag)
        fail("conditional If-None-Match", "should be 304")
    except ClientError as e:
        assert_eq("conditional If-None-Match → 304", e.response["Error"]["Code"], "304")

    # If-Match with matching ETag → 200
    resp = s3.get_object(Bucket=BUCKET, Key="cond-enc.txt", IfMatch=etag)
    assert_eq("conditional If-Match → 200", resp["Body"].read(), data)

    # If-Match with wrong ETag → 412
    try:
        s3.get_object(Bucket=BUCKET, Key="cond-enc.txt", IfMatch='"wrong-etag"')
        fail("conditional If-Match wrong", "should be 412")
    except ClientError as e:
        assert_eq("conditional If-Match wrong → 412", e.response["Error"]["Code"], "PreconditionFailed")

    s3.delete_object(Bucket=BUCKET, Key="cond-enc.txt")


def test_tagging_with_encryption(s3):
    """Object tagging on encrypted objects."""
    print("\n── Tagging + Encryption ──")

    data = b"tagged-encrypted"
    s3.put_object(Bucket=BUCKET, Key="tag-enc.txt", Body=data,
                  ServerSideEncryption="AES256")

    # Put tags
    s3.put_object_tagging(
        Bucket=BUCKET, Key="tag-enc.txt",
        Tagging={"TagSet": [
            {"Key": "env", "Value": "test"},
            {"Key": "team", "Value": "backend"},
        ]},
    )
    ok("put tagging on encrypted object")

    # Get tags
    resp = s3.get_object_tagging(Bucket=BUCKET, Key="tag-enc.txt")
    tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}
    assert_eq("tag: env", tags.get("env"), "test")
    assert_eq("tag: team", tags.get("team"), "backend")

    # Data still readable
    got = s3.get_object(Bucket=BUCKET, Key="tag-enc.txt")["Body"].read()
    assert_eq("tagged encrypted: data intact", got, data)

    # Delete tags
    s3.delete_object_tagging(Bucket=BUCKET, Key="tag-enc.txt")
    resp = s3.get_object_tagging(Bucket=BUCKET, Key="tag-enc.txt")
    assert_eq("delete tagging", len(resp.get("TagSet", [])), 0)

    s3.delete_object(Bucket=BUCKET, Key="tag-enc.txt")


def test_batch_delete_encrypted(s3):
    """Batch delete mix of encrypted and plain objects."""
    print("\n── Batch Delete Encrypted ──")

    keys = []
    for i in range(5):
        key = f"batch/plain-{i}.txt"
        s3.put_object(Bucket=BUCKET, Key=key, Body=f"plain-{i}".encode())
        keys.append(key)
    for i in range(5):
        key = f"batch/enc-{i}.txt"
        s3.put_object(Bucket=BUCKET, Key=key, Body=f"enc-{i}".encode(),
                      ServerSideEncryption="AES256")
        keys.append(key)

    # Batch delete
    resp = s3.delete_objects(
        Bucket=BUCKET,
        Delete={"Objects": [{"Key": k} for k in keys]},
    )
    deleted = resp.get("Deleted", [])
    assert_eq("batch delete: count", len(deleted), 10)

    # Verify gone
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="batch/")
    remaining = resp.get("Contents", [])
    assert_eq("batch delete: all gone", len(remaining), 0)


def test_large_encrypted(s3):
    """Large object encryption roundtrip."""
    print("\n── Large Encrypted Objects ──")

    for size_name, size in [("5MB", 5 * 1024 * 1024), ("10MB", 10 * 1024 * 1024)]:
        data = os.urandom(size)
        checksum = hashlib.sha256(data).hexdigest()

        t0 = time.perf_counter()
        s3.put_object(Bucket=BUCKET, Key=f"large-{size_name}.bin", Body=data,
                      ServerSideEncryption="AES256")
        put_ms = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        got = s3.get_object(Bucket=BUCKET, Key=f"large-{size_name}.bin")["Body"].read()
        get_ms = (time.perf_counter() - t0) * 1000

        got_hash = hashlib.sha256(got).hexdigest()
        assert_eq(f"large SSE-S3 {size_name} integrity", got_hash, checksum)
        print(f"         {size_name}: PUT {put_ms:.0f}ms, GET {get_ms:.0f}ms")

        s3.delete_object(Bucket=BUCKET, Key=f"large-{size_name}.bin")


def test_list_objects_features(s3):
    """List objects with prefix, delimiter, max-keys, start-after."""
    print("\n── List Objects Features ──")

    # Create hierarchical structure
    for path in ["docs/a.txt", "docs/b.txt", "docs/sub/c.txt",
                 "images/x.png", "images/y.png", "data.csv"]:
        s3.put_object(Bucket=BUCKET, Key=path, Body=path.encode())

    # List all
    resp = s3.list_objects_v2(Bucket=BUCKET)
    assert_eq("list all count", resp["KeyCount"], 6)

    # Prefix filter
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="docs/")
    assert_eq("list prefix docs/", resp["KeyCount"], 3)

    # Max keys
    resp = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=2)
    assert_eq("list max-keys=2", len(resp.get("Contents", [])), 2)

    # Start after
    resp = s3.list_objects_v2(Bucket=BUCKET, StartAfter="docs/b.txt")
    keys = [o["Key"] for o in resp.get("Contents", [])]
    assert_true("list start-after", all(k > "docs/b.txt" for k in keys),
                f"keys={keys}")

    # Cleanup
    for path in ["docs/a.txt", "docs/b.txt", "docs/sub/c.txt",
                 "images/x.png", "images/y.png", "data.csv"]:
        s3.delete_object(Bucket=BUCKET, Key=path)


def test_copy_metadata_directive(s3):
    """Copy with REPLACE vs COPY metadata directive."""
    print("\n── Copy Metadata Directive ──")

    s3.put_object(Bucket=BUCKET, Key="cp-src.txt", Body=b"source",
                  Metadata={"original": "true", "author": "alice"})

    # COPY directive (default) — keeps source metadata
    s3.copy_object(
        Bucket=BUCKET, Key="cp-copy.txt",
        CopySource=f"{BUCKET}/cp-src.txt",
    )
    resp = s3.head_object(Bucket=BUCKET, Key="cp-copy.txt")
    assert_eq("copy COPY: original meta", resp.get("Metadata", {}).get("original"), "true")

    # REPLACE directive — replaces metadata
    s3.copy_object(
        Bucket=BUCKET, Key="cp-replace.txt",
        CopySource=f"{BUCKET}/cp-src.txt",
        MetadataDirective="REPLACE",
        Metadata={"replaced": "yes"},
    )
    resp = s3.head_object(Bucket=BUCKET, Key="cp-replace.txt")
    meta = resp.get("Metadata", {})
    assert_eq("copy REPLACE: new meta", meta.get("replaced"), "yes")
    assert_true("copy REPLACE: old meta gone", "original" not in meta, f"meta={meta}")

    for k in ["cp-src.txt", "cp-copy.txt", "cp-replace.txt"]:
        s3.delete_object(Bucket=BUCKET, Key=k)


def test_error_cases(s3):
    """Error handling edge cases."""
    print("\n── Error Cases ──")

    # GET non-existent key
    try:
        s3.get_object(Bucket=BUCKET, Key="does-not-exist.txt")
        fail("get missing key", "should 404")
    except ClientError as e:
        assert_eq("get missing key → NoSuchKey", e.response["Error"]["Code"], "NoSuchKey")

    # HEAD non-existent key
    try:
        s3.head_object(Bucket=BUCKET, Key="does-not-exist.txt")
        fail("head missing key", "should 404")
    except ClientError as e:
        assert_eq("head missing key → 404", e.response["ResponseMetadata"]["HTTPStatusCode"], 404)

    # DELETE non-existent key (should succeed silently per S3 spec)
    try:
        s3.delete_object(Bucket=BUCKET, Key="does-not-exist.txt")
        ok("delete missing key (no error)")
    except Exception as e:
        fail("delete missing key", str(e))

    # PUT empty body
    s3.put_object(Bucket=BUCKET, Key="empty.txt", Body=b"")
    resp = s3.get_object(Bucket=BUCKET, Key="empty.txt")
    assert_eq("put/get empty body", resp["Body"].read(), b"")
    s3.delete_object(Bucket=BUCKET, Key="empty.txt")

    # PUT empty body with SSE-S3
    s3.put_object(Bucket=BUCKET, Key="empty-enc.txt", Body=b"",
                  ServerSideEncryption="AES256")
    resp = s3.get_object(Bucket=BUCKET, Key="empty-enc.txt")
    assert_eq("put/get empty encrypted", resp["Body"].read(), b"")
    s3.delete_object(Bucket=BUCKET, Key="empty-enc.txt")


def test_special_characters(s3):
    """Keys with special characters."""
    print("\n── Special Characters in Keys ──")

    special_keys = [
        "path/to/file with spaces.txt",
        "unicode-\u00e9\u00e8\u00ea.txt",
        "dots...and---dashes.txt",
        "path/deep/nested/key.bin",
    ]

    for key in special_keys:
        data = f"data-for-{key}".encode()
        try:
            s3.put_object(Bucket=BUCKET, Key=key, Body=data,
                          ServerSideEncryption="AES256")
            got = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            assert_eq(f"special key: {key[:30]}", got, data)
            s3.delete_object(Bucket=BUCKET, Key=key)
        except Exception as e:
            fail(f"special key: {key[:30]}", str(e))


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 68)
    print("  OxiDB S3 Integration Tests — Encryption & Features")
    print("=" * 68)

    print("\nStarting OxiDB server...")
    proc = start_server()
    print(f"Server running (PID {proc.pid}), S3 on port {S3_PORT}")

    try:
        s3 = make_client()

        test_basic_crud(s3)
        test_sse_s3(s3)
        test_sse_c(s3)
        test_sse_mixed(s3)
        test_sse_s3_range_request(s3)
        test_sse_c_range_request(s3)
        test_copy_with_encryption(s3)
        test_metadata_with_encryption(s3)
        test_overwrite_encryption(s3)
        test_multipart_sse_s3(s3)
        test_conditional_with_encryption(s3)
        test_tagging_with_encryption(s3)
        test_batch_delete_encrypted(s3)
        test_large_encrypted(s3)
        test_list_objects_features(s3)
        test_copy_metadata_directive(s3)
        test_error_cases(s3)
        test_special_characters(s3)

        # Cleanup bucket
        try:
            s3.delete_bucket(Bucket=BUCKET)
        except Exception:
            pass

    finally:
        print("\nStopping server...")
        stop_server(proc)

    # Report
    total = passed + failed
    print("\n" + "=" * 68)
    print(f"  Results: {passed}/{total} passed", end="")
    if failed:
        print(f", \033[31m{failed} FAILED\033[0m")
        print("-" * 68)
        for name, reason in errors:
            print(f"  FAIL: {name} — {reason}")
    else:
        print(f"  \033[32mALL PASSED\033[0m")
    print("=" * 68)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
