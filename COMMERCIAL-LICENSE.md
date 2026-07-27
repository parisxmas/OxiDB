# OxiDB Licensing

As of **v0.33.0**, OxiDB is **proprietary, commercially licensed
software**. There is no longer an open-source license option for new
versions — see [`LICENSE`](LICENSE).

## 1. What this means

All use of OxiDB v0.33.0 or later — running the server, embedding the
engine (the `oxidb` crate, the FFI libraries, the WASM build, or the
embedded client packages), redistributing it, or offering it as a
service — requires a **commercial license** from the copyright holder.

A commercial license is negotiated directly and can cover:

- **Embedding** — shipping OxiDB inside your (closed- or open-source)
  application or device;
- **Redistribution** — bundling OxiDB binaries with your product;
- **Hosting** — offering OxiDB, or a service built on it, to third
  parties over a network;
- **Source access and modification rights**, support, and update terms
  as agreed.

**To obtain a commercial license, contact:**

> **barisakin@gmail.com**

## 2. Client libraries

The thin **TCP client libraries** (Python `oxidb`, npm `oxidb`,
`OxiDb.Client.Tcp` / `OxiDb.Linq` / `OxiDb.Data` on NuGet, Go, Julia,
PHP) remain **MIT-licensed**. Talking to a licensed OxiDB server from
your own application does not require a commercial license of its own.

Packages that **bundle the engine itself** — `oxidb-embedded` on PyPI,
`OxiDb.Client.Embedded` on NuGet, and the FFI/WASM artifacts — contain
the proprietary engine and are covered by [`LICENSE`](LICENSE).

## 3. Prior versions

Earlier versions of OxiDB were published under open-source licenses and
those grants are irrevocable **for those specific versions**:

| Versions | License |
|---|---|
| Early releases (MIT era) | `MIT OR Apache-2.0` |
| Later releases up to and including **v0.32.x** | `AGPL-3.0-only` (dual: AGPL / commercial) |
| **v0.33.0 and later** | **Proprietary — commercial license required** |

You may continue to use those past versions under their original terms.
No new versions will be published under an open-source license.

## 4. Contributions

By submitting a contribution (pull request, patch, etc.) to OxiDB, you
assign to the copyright holder the right to distribute your contribution
under the proprietary license and any commercial license terms. If you
cannot agree to this, please do not submit contributions.
