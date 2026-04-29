#!/usr/bin/env python3
"""
Upload every .docx in ./data to a running OxiDB server as a blob.
Each upload triggers async FTS extraction + indexing on the server.

Env:
    OXIDB_HOST  (default: 127.0.0.1)
    OXIDB_PORT  (default: 14888 — ftstests' run.sh spawns the server here)
"""
import os
import sys
import time
from pathlib import Path

# Use the in-tree Python client.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))
import oxidb  # type: ignore

DATA_DIR = Path(__file__).parent / "data"
BUCKET = "ftstests"
CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)

HOST = os.environ.get("OXIDB_HOST", "127.0.0.1")
PORT = int(os.environ.get("OXIDB_PORT", "14888"))


def main() -> int:
    files = sorted(DATA_DIR.glob("*.docx"))
    if not files:
        print(f"no .docx files under {DATA_DIR}", file=sys.stderr)
        return 1

    client = oxidb.OxiDbClient(host=HOST, port=PORT, timeout=30)
    print(f"[upload] {len(files)} files -> {HOST}:{PORT} bucket={BUCKET}", flush=True)

    t0 = time.time()
    for i, f in enumerate(files, 1):
        data = f.read_bytes()
        client.put_object(BUCKET, f.name, data, content_type=CONTENT_TYPE)
        if i % 20 == 0:
            print(f"  uploaded {i}/{len(files)}", flush=True)
    elapsed = time.time() - t0
    print(f"[upload] done in {elapsed:.2f}s ({len(files) / elapsed:.1f} files/s)")

    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
