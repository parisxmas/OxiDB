# OxiDB Licensing

As of **v0.40.0**, OxiDB is **source-available**: the source is public,
and you may read it, modify it, and run it in production for your own
applications and business — free, with no registration and no limit on
instances or workload. See [`LICENSE`](LICENSE).

Two things still require a commercial license, and only two:

- **Offering OxiDB as a service** to third parties — as a database, a
  backend, or a platform built on it;
- **Distributing OxiDB** to third parties, on its own or embedded in
  your product, device, or offering.

## 1. What is free

Running the server for your own application, at any scale, in
production. Reading and modifying the source. Building on it internally.
Evaluating it without asking anyone.

If your users reach *your* application and OxiDB is what stands behind
it, that is free use. If your users reach *OxiDB* — you are selling
access to the database or a platform over it — that is the hosted-service
case below.

## 2. What needs a license

A commercial license is negotiated directly and can cover:

- **Hosting** — offering OxiDB, or a service built on it, to third
  parties over a network;
- **Embedding** — shipping OxiDB inside your (closed- or open-source)
  application or device;
- **Redistribution** — bundling OxiDB binaries with your product;
- **Source access and modification rights**, support, and update terms
  as agreed.

**To obtain a commercial license, contact:**

> **barisakin@gmail.com**

## 3. Client libraries

The thin **TCP client libraries** (Python `oxidb`, the JavaScript
clients, `OxiDb.Client.Tcp` / `OxiDb.Linq` / `OxiDb.Data` on NuGet, Go,
Julia, Dart, PHP, Swift) are **MIT-licensed** and are not covered by the
source-available license at all — including redistribution. Shipping a
client inside your application is free.

Packages that **bundle the engine itself** — `oxidb-embedded` on PyPI,
`OxiDb.Client.Embedded` on NuGet, and the FFI/WASM artifacts — contain
the engine and are covered by [`LICENSE`](LICENSE).

## 4. Prior versions

Each version stays under the license it was published with, and those
grants are irrevocable for those specific versions:

| Versions | License |
|---|---|
| Early releases (MIT era) | `MIT OR Apache-2.0` |
| Up to and including **v0.32.x** | `AGPL-3.0-only` (dual: AGPL / commercial) |
| **v0.33.0 – v0.39.x** | Proprietary — commercial license required for any use |
| **v0.40.0 and later** | **Source-available — free for your own production use** |

The v0.33–v0.39 line was proprietary; v0.40.0 opens it up rather than
closing it further. If you hold a commercial license covering those
versions, nothing about it changes.

## 5. Why not open source

The engine is the product. An OSI license would let a cloud provider
sell it as a managed service without contributing anything back — the
asymmetry that pushed MongoDB, Elastic, Redis, and HashiCorp off their
original licenses. This license blocks exactly that case and nothing
else: your own production use, at any scale, is free.

## 6. Contributions

By submitting a contribution (pull request, patch, etc.) to OxiDB, you
assign to the copyright holder the right to distribute your contribution
under the source-available license and any commercial license terms. If
you cannot agree to this, please do not submit contributions.
