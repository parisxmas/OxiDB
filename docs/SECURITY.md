# Security Policy

## Reporting a vulnerability

**Please do not file public GitHub issues for security vulnerabilities.**

### Preferred channel: GitHub Security Advisories

Report privately via GitHub's private vulnerability reporting:

→ <https://github.com/parisxmas/OxiDB/security/advisories/new>

The advisory thread is the working channel for triage, fix coordination,
and CVE assignment.

### What to include

- A description of the issue, including its impact (what an attacker can do)
- The OxiDB version(s) affected — for releases, the `vX.Y.Z` tag; for
  source builds, the commit SHA
- Reproduction steps, ideally a minimal proof-of-concept (a fuzz corpus
  input, a sequence of CLI / wire-protocol commands, etc.)
- Whether you have already disclosed the issue elsewhere (mailing list,
  social media, conference talk, paper) and the disclosure timeline if so

### What to expect

| When | What |
|---|---|
| Within **3 business days** | Acknowledgement that the report is received and being triaged |
| Within **10 business days** | Initial assessment (severity, affected versions, whether we accept the report) |
| Within **90 days**, or before public disclosure if earlier | Fix released and advisory published — see "Disclosure timeline" |

If you do not receive an acknowledgement within 3 business days, that is
the wrong outcome — please escalate by opening a public issue **without
disclosing the vulnerability details** (something like "private security
report not yet acknowledged, please contact me") so the maintainer is
notified through the standard repo signal.

## Disclosure timeline

OxiDB follows a **90-day coordinated disclosure** model with one
documented exception: if a fix is publicly available earlier (released in
a patched version, with users able to upgrade), the advisory is published
at release time, not at the 90-day mark. The goal is to never have a
patched-but-unannounced gap users could be exploited through.

If the reporter requests a shorter or longer window for valid reasons
(active exploitation in the wild, coordination with other vendors), that
is negotiated in the advisory thread.

## Scope

In scope:

- The OxiDB engine (`oxidb`), server (`oxidb-server`), pool (`oxipool`),
  CLI (`oxidb-cli`), and FFI library (`oxidb-client-ffi`) at the latest
  release tag
- All official Tier-A client libraries (see
  [STABILITY.md §Tier A](STABILITY.md)) at their latest release tag
- The on-disk format, wire protocol, and audit log
- Authentication (SCRAM-SHA-256), authorization (RBAC), TLS termination,
  encryption-at-rest
- Default-on subsystems that handle network traffic from untrusted clients

Out of scope (please don't report as security; file regular issues
instead):

- Bugs in features explicitly marked experimental in
  [STABILITY.md](STABILITY.md). They are not yet under the security
  warranty.
- Denial-of-service via legitimate-but-expensive queries against a server
  configured without resource limits. Configure `OXIDB_POOL_SIZE`,
  `OXIDB_IDLE_TIMEOUT`, and per-collection limits; if a default is too
  permissive in your view, that's an issue worth filing — just not a
  security advisory.
- Vulnerabilities requiring physical access to the host filesystem and
  an existing root-level compromise.
- Best-practice hardening recommendations without a concrete attack path.
- Issues in third-party dependencies — please report those to the
  upstream project first; we'll pick up the fix once it lands. Exception:
  if the dependency cannot or will not fix the issue and OxiDB's usage
  exposes the flaw, that *is* in scope here.

## Supported versions

See [STABILITY.md](STABILITY.md) for what's covered by the stability
promise. The backport policy for security fixes is:

| Version | Status | Receives security fixes? |
|---|---|---|
| Current 1.x minor | Supported | Yes |
| Previous 1.x minor (N-1) | Supported (12 months overlap) | Yes — backported from current |
| Older 1.x minors (N-2 and earlier) | EOL once N+1 ships | No |
| 1.0 series after 24-month LTS window | Security-only mode for 12 additional months | Yes, security only |
| 1.0 series after 24+12 month total window | EOL | No |
| 0.x | Pre-1.0, no backport guarantee | No |

This follows [ADR-0004 §2](decisions/0004-phase-0-answers.md). The
**24-month LTS** is the headline number that unlocks CERN-grade and
enterprise procurement conversations
([`docs/cern-compatibility.md`](cern-compatibility.md)).

### "Critical" vs "important" classification

- **Critical** (data loss, data corruption, authentication bypass, RCE,
  cryptographic break): backported to current minor AND N-1 minor.
  Patch releases within ~7 days of fix landing where possible.
- **Important** (privilege escalation between roles, denial-of-service
  with low attack cost, information disclosure): backported to current
  minor. N-1 minor at maintainer discretion.
- **Low** (information disclosure with high attack cost, configuration
  hardening): rolled into the next regular minor.

## Hall of fame

When advisories are published, reporters who consent to credit are listed
in the advisory and in the release notes for the fix. Anonymous reports
are also welcome — credit is opt-in.

## Cryptographic primitives in use

These are part of the security boundary. Any change to them is a
breaking-change-worthy event and runs through the
[deprecation process](DEPRECATION.md):

- **At-rest encryption**: AES-256-GCM, key from `OXIDB_ENCRYPTION_KEY`
- **TLS**: rustls defaults (TLS 1.2+); certificate from `OXIDB_TLS_CERT` /
  `OXIDB_TLS_KEY`
- **Authentication**: SCRAM-SHA-256 (RFC 7677, stored-verifier model)
- **Password hashing**: argon2 (for the optional JWT-auth user collection
  `_auth_users`)
- **Integrity / checksums**: CRC32 in WAL, btree, and blob layers (for
  detection, not security — corruption resistance, not tamper resistance)

## Operational hardening checklist

For users running OxiDB in production, before going live:

- [ ] `OXIDB_AUTH=true` and at least one Admin user provisioned
- [ ] `OXIDB_TLS_CERT` + `OXIDB_TLS_KEY` configured, TLS 1.2+ only
- [ ] `OXIDB_ENCRYPTION_KEY` configured for sensitive data at rest
- [ ] `OXIDB_AUDIT=true` with rotation policy
  (`OXIDB_AUDIT_MAX_BYTES` / `OXIDB_AUDIT_MAX_AGE_SECS` /
  `OXIDB_AUDIT_CALENDAR`) so logs don't grow unbounded
- [ ] `OXIDB_IDLE_TIMEOUT` set to a non-zero value
- [ ] `OXIDB_ADDR` bound to a private interface unless TLS is fronting a
      public endpoint
- [ ] RBAC roles assigned per principle of least privilege (Admin only
      for ops accounts, ReadWrite for application accounts, Read for
      reporting accounts)
- [ ] Backups configured and tested (the DR drill in
      `tests/cern_dr_drill.rs` exists for a reason; run the equivalent
      against your deployment)
