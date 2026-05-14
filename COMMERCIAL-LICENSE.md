# OxiDB Licensing

OxiDB is **dual-licensed**.

## 1. Open-source license — AGPL-3.0

OxiDB is released to the public under the **GNU Affero General Public
License, version 3** (`AGPL-3.0-only`) — see [`LICENSE`](LICENSE).

The AGPL is a strong copyleft license. In short, you may use, study,
modify, and redistribute OxiDB **for free**, but if you do, you must:

- release the **complete corresponding source code** of your work under
  the AGPL-3.0, and
- do so **even if you only offer it over a network** — under the AGPL,
  letting users interact with OxiDB (or a product built on it) over a
  network counts as distribution, so your users are entitled to that
  source.

If your use of OxiDB fits within those terms, you owe nothing and need
nothing further — the AGPL is your license.

## 2. Commercial license

The AGPL does **not** work for everyone. You need a **commercial
license** if you want to do any of the following without releasing your
own source code under the AGPL:

- **embed** OxiDB (the `oxidb` crate, the FFI libraries, or any client
  library in this repository) inside a **proprietary / closed-source**
  application or product;
- **distribute** OxiDB, or a product containing it, under terms other
  than the AGPL;
- run a **modified** OxiDB — including as a hosted/SaaS service — without
  publishing your modifications.

A commercial license removes the AGPL's copyleft obligations and lets
you use OxiDB in a closed-source product on terms negotiated directly
with the copyright holder.

**To obtain a commercial license, contact:**

> **[ FILL IN: your commercial-licensing email or contact ]**

(Until this is filled in, the only license on offer is the AGPL-3.0.)

## 3. Which one applies to me?

| Your situation | License |
|---|---|
| Personal, research, or hobby use | AGPL-3.0 — free |
| An open-source project that is itself AGPL-compatible | AGPL-3.0 — free |
| Talking to a stock, unmodified `oxidb-server` over the network from your own app | AGPL-3.0 — free (your app is not a derivative work) |
| Embedding OxiDB *into* a closed-source product | **Commercial license required** |
| Shipping a modified OxiDB, or running one as a service, without sharing your changes | **Commercial license required** |

If you are unsure, assume you need a commercial license and reach out.

## 4. Contributions

By submitting a contribution (pull request, patch, etc.) to OxiDB, you
agree that your contribution may be distributed by the project under
**both** licenses above — the AGPL-3.0 and the commercial license. This
is what makes the dual-license model possible: the copyright holder must
be able to offer the *whole* of OxiDB, including your contribution,
under a commercial license. If you cannot agree to this, please do not
submit contributions.

---

*Note: prior releases of OxiDB were published under `MIT OR Apache-2.0`.
That grant cannot be revoked — those specific past versions remain
available under those terms. The dual AGPL-3.0 / commercial licensing
described here applies to this and all future versions.*
