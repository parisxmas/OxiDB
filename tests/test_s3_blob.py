#!/usr/bin/env python3
"""
OxiDB S3-style Blob Storage — Performance Test (Embedded)

Tests: bucket CRUD, object upload/download at various sizes,
       metadata, list, head, delete — with timing report.

Requirements: pip install oxidb-embedded
              (+ native library liboxidb_embedded_ffi.dylib/.so/.dll)
"""

import os
import time
import shutil
import hashlib
from oxidb_embedded import OxiDbEmbedded, OxiDbError

DATA_DIR = "./test_s3_data"
BUCKET = "test-s3-perf"

SIZES = {
    "1 KB": 1024,
    "10 KB": 10 * 1024,
    "100 KB": 100 * 1024,
    "1 MB": 1024 * 1024,
    "5 MB": 5 * 1024 * 1024,
}


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def timed(fn):
    t0 = time.perf_counter()
    result = fn()
    elapsed = (time.perf_counter() - t0) * 1000
    return result, elapsed


def main():
    # Clean up from previous runs
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    results = []

    with OxiDbEmbedded(DATA_DIR) as db:
        # --- Create bucket ---
        _, ms = timed(lambda: db.create_bucket(BUCKET))
        results.append(("Create bucket", "", f"{ms:.2f} ms"))

        # --- Upload objects at various sizes ---
        for label, size in SIZES.items():
            data = os.urandom(size)
            key = f"test-{size}.bin"
            checksum = md5(data)

            _, upload_ms = timed(lambda d=data, k=key, c=checksum: db.put_object(
                BUCKET, k, d,
                content_type="application/octet-stream",
                metadata={"size": str(size), "checksum": c},
            ))
            throughput = (size / 1024 / 1024) / (upload_ms / 1000) if upload_ms > 0 else 0
            results.append((f"Upload {label}", f"{throughput:.2f} MB/s", f"{upload_ms:.2f} ms"))

        # --- Download and verify each ---
        for label, size in SIZES.items():
            key = f"test-{size}.bin"

            (downloaded, meta), download_ms = timed(lambda k=key: db.get_object(BUCKET, k))
            throughput = (size / 1024 / 1024) / (download_ms / 1000) if download_ms > 0 else 0
            user_meta = meta.get("metadata", meta)
            ok = len(downloaded) == size and user_meta.get("checksum") == md5(downloaded)
            status = "OK" if ok else "FAIL"
            results.append((f"Download {label} [{status}]", f"{throughput:.2f} MB/s", f"{download_ms:.2f} ms"))

        # --- Head object ---
        _, ms = timed(lambda: db.head_object(BUCKET, "test-1024.bin"))
        results.append(("Head object", "", f"{ms:.2f} ms"))

        # --- List objects ---
        objs, ms = timed(lambda: db.list_objects(BUCKET))
        results.append((f"List objects ({len(objs)} items)", "", f"{ms:.2f} ms"))

        # --- List with prefix ---
        objs, ms = timed(lambda: db.list_objects(BUCKET, prefix="test-1024"))
        results.append((f"List prefix (found {len(objs)})", "", f"{ms:.2f} ms"))

        # --- Delete objects ---
        for label, size in SIZES.items():
            key = f"test-{size}.bin"
            _, ms = timed(lambda k=key: db.delete_object(BUCKET, k))
            results.append((f"Delete {label}", "", f"{ms:.2f} ms"))

        # --- Delete bucket ---
        _, ms = timed(lambda: db.delete_bucket(BUCKET))
        results.append(("Delete bucket", "", f"{ms:.2f} ms"))

    # --- Cleanup ---
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    # --- Print report ---
    print()
    print("=" * 64)
    print("  OxiDB S3 Blob Storage — Performance Report (Embedded)")
    print("=" * 64)
    print(f"  Mode: Embedded (in-process FFI, no network)")
    print(f"  Sizes tested: {', '.join(SIZES.keys())}")
    print("-" * 64)
    print(f"  {'Operation':<35} {'Throughput':>12} {'Time':>12}")
    print("-" * 64)
    for op, tp, elapsed in results:
        print(f"  {op:<35} {tp:>12} {elapsed:>12}")
    print("=" * 64)
    print()


if __name__ == "__main__":
    main()
