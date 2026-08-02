# oxidb-embedded

Embedded OxiDB for Python. Run the database engine directly in your Python process via FFI — no server required, zero network overhead.

## Install

```bash
pip install oxidb-embedded
```

You also need the native shared library (`liboxidb_embedded_ffi.dylib` on macOS, `.so` on Linux, `.dll` on Windows). Either:
- Place it next to the installed package
- Set `OXIDB_LIB_PATH=/path/to/liboxidb_embedded_ffi.dylib`
- Install it to a system library path

## Usage

```python
from oxidb_embedded import OxiDbEmbedded

# Context manager (recommended)
with OxiDbEmbedded("./mydata") as db:
    db.insert("users", {"name": "Alice", "age": 30})
    docs = db.find("users", {"name": "Alice"})
    print(docs)

# Manual open/close
db = OxiDbEmbedded("./mydata")
db.insert("users", {"name": "Bob"})
db.close()
```

## Features

- Full CRUD (insert, find, update, delete)
- Indexes (single field, composite, unique, text)
- Aggregation pipeline
- Full-text search
- Transactions (OCC)
- Blob/object storage
- Encryption at rest

## Links

- [OxiDB Website](https://oxidb.baltavista.com)
- [Python Examples](https://oxidb.baltavista.com/python-examples.html)
- [Downloads](https://oxidb.baltavista.com/downloads.html)
