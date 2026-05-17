# CERN compatibility

**Status of this document:** assessment + roadmap, not a commitment.
**See also:** [ADR-0003 — 1.0 stability surface and scope](decisions/0003-1.0-stability-scope.md)
(the hard prerequisite for any of the below to land).

## TL;DR

OxiDB **cannot be adopted at CERN today**, primarily because it ships at
`0.x` with an explicit "not production-ready / breaking changes between
releases" warning in the README. That alone disqualifies it from every
CERN procurement and risk-review conversation that demands multi-year
stability.

It is **not the right shape** for CERN's flagship workloads either —
the LHC physics-data path (PB/year) is built around ROOT files,
columnar `TTree`/`RDataFrame`, and dedicated tape archives (CTA / EOS);
no document database is going to displace that stack, nor should it.

There *is* a realistic niche — CERN's thousands of departmental
services, R&D test-bench DAQs, internal tooling, and ML experiment
embedding stores — where OxiDB *could* fit once it has shipped 1.0,
acquired the integrations CERN data-centre operations require, and
proved itself in at least one pilot.

This document spells out where OxiDB fits and doesn't, what would need
to be true before a CERN pilot is plausible, and the realistic entry
path.

## Where OxiDB does **not** fit (the majority of CERN's data load)

### LHC physics data (~PB/year per experiment)

ATLAS, CMS, LHCb, ALICE produce structured event data that is read by
analysis code in throughput-oriented columnar formats — `TTree` /
`RNTuple` in ROOT, increasingly Apache Arrow / Parquet for downstream
analysis. The entire HEP toolchain (`RDataFrame`, `coffea`, `awkward`,
`uproot`, scikit-hep) operates on those formats.

OxiDB is a document database. Re-encoding HEP event data as JSON
documents would be the wrong shape (a column-major-friendly access
pattern stuffed into a row-major-friendly engine) and the wrong scale
(OxiDB's tested envelope is `10^5`–`10^7` documents; HEP analysis
campaigns regularly touch `10^9`–`10^{10}` events).

This is not a gap to close. It is correctly out of scope.

### Tape archive and bulk storage (~90 PB/year)

**CTA** (CERN Tape Archive, the CASTOR successor) + **EOS** are
purpose-built for the access-rate and durability profile of
write-once-read-rarely scientific data. Decades of engineering.

OxiDB has S3-compatible blob storage, which is a perfectly reasonable
general-purpose object store — but it is not on the same engineering
budget axis as CTA/EOS.

### Mission-critical institutional databases

WLCG metadata, run conditions, calibration databases (CRIC, COOL,
PandaDB, …) run on Oracle RAC with multi-decade HA, replication, and
consistency engineering. The bar to displace them is enormous and the
benefit minimal; this is not the OxiDB market.

## Where OxiDB **could** fit (the realistic niche)

Most of the thousands of services CERN operates are not the LHC data
path. They are operational tools, monitoring dashboards, R&D
test-benches, ticketing systems, configuration registries, ML
experiment trackers — and most of them today use PostgreSQL, MariaDB,
SQLite, or MongoDB. Several of these slots could plausibly accept
OxiDB once it is ready.

### Embedded / edge tooling

- **Beam diagnostics tools, control-room utilities, custom monitoring
  agents** — single-binary, embedded mode (`OxiDbEmbedded`), AES at
  rest, RBAC. This is the SQLite-equivalent slot.
- **R&D detector test-benches, small DAQ data loggers** — embedded
  engine + transactions + persistence + the TTL index would cover the
  common "log timestamped data + auto-expire" pattern.

### Departmental / R&D services

- Internal config registries, run notebooks, project trackers — JSON
  document model is a natural fit; the FTS, blob storage, and
  encryption story is a complete out-of-the-box stack for a small
  service.
- AI/ML embedding stores at POC scale — the vector-search feature is
  there; the engine would need to demonstrate billion-scale
  competitiveness with FAISS / Milvus / pgvector to be a serious
  option, but for `10^5`–`10^7`-scale experiments it is already viable
  in principle.

### Standalone Redis-shaped caches

- OxiMem (Redis RESP-compatible) with optional disk persistence and
  SQL mirror could plausibly replace small Redis instances in
  departments where the "ephemeral cache but I want it to survive a
  restart" pattern is wanted.

## What needs to be true before a CERN pilot is plausible

Five layers, roughly independent. The earlier layers are hard
prerequisites for the later ones.

### Layer 1 — Production readiness (the gating issue)

| Requirement | Current state | Gap |
|---|---|---|
| 1.0 release with documented backward-compat | not yet | [ADR-0003](decisions/0003-1.0-stability-scope.md) is the plan |
| Documented migration tool between versions | not yet | Phase 4 of ADR-0003 (`oxidb migrate`) |
| LTS branch + backport policy | not yet | Phase 5 of ADR-0003 |
| ABI stability for the embedded FFI | not yet | Phase 1 of ADR-0003 (header versioning) |

Without all of these, CERN procurement conversations end before they
start. Every other layer below assumes Layer 1 is done.

### Layer 2 — Production hardening at CERN scale

| Requirement | Current state | Gap |
|---|---|---|
| Jepsen-style fault injection for Raft | unit tests only | Real chaos test suite (split-brain, partitions, clock skew, slow disks) |
| Continuous CERN-scale benchmark | bench at `10^5` docs | `10^7+` docs, 24-hour soak, leak-free, performance regression CI |
| Crash-recovery test coverage | SIGKILL tests exist (README §Crash-safe) | Sustained, automated, multi-scenario |
| Backup / restore operational toolchain | `backup()` + `restore_to_point` (PITR) exist | Operationally usable: incremental, encrypted, S3-pushable, monitored |

### Layer 3 — Integrations CERN data-centre operations expect

| Requirement | Current state | Gap |
|---|---|---|
| Auth: OIDC / Keycloak (CERN SSO) | SCRAM-SHA-256 only | OIDC provider integration, Keycloak/SAML federation, group claim → RBAC mapping |
| Monitoring: Prometheus exporter | GELF logging only (Graylog/Loki) | `/metrics` Prometheus exporter for engine internals, RED metrics per endpoint |
| Dashboards: Grafana official artifacts | none | Versioned dashboard JSON shipped in the repo |
| Container orchestration: OpenShift Operator + Helm chart | Dockerfile only | Operator (with backup/restore CRDs), Helm chart, CI publication |
| TLS with CERN PKI | static cert/key file | Auto-rotation against an external CA, ACME / cert-manager integration |
| Audit log forwarding | local audit log | SIEM/OpenSearch streaming, structured format with stable schema |

### Layer 4 — Use-case-specific fit work

Depends on which CERN slot is being targeted; each is a separate piece
of work and probably a separate ADR.

- **DAQ slot:** documented sustained write throughput (e.g.
  `>10^5` writes/sec), Arrow / Parquet export tool, time-series
  ergonomics.
- **ML embedding slot:** vector-search benchmark at billion-scale
  vs FAISS / Milvus / pgvector; integration recipes with PyTorch /
  Triton serving.
- **Departmental-service slot:** schema validation hooks,
  multi-tenancy primitives, a web admin UI.

### Layer 5 — Trust & governance

| Requirement | Current state | Gap |
|---|---|---|
| Public third-party security audit | none | Engagement with e.g. Trail of Bits / Cure53 |
| Reproducible builds | not formalised | SOURCE_DATE_EPOCH respected, deterministic output verified in CI |
| Contributor diversity | single-vendor | At least one external maintainer with merge rights |
| Foundation home (optional but valuable) | none | CNCF SDS-WG, Linux Foundation, NumFOCUS … if scope warrants |
| Reference customers in similar science-computing orgs | none | DESY, Fermilab, SLAC, RIKEN — peer adopters |

## Realistic entry path

CERN adoption is not a top-down sale. The mechanics that actually work:

1. **Apply to CERN openlab.** CERN's official industry-collaboration
   programme is designed for exactly this kind of evaluation: a
   project proposes a focused R&D POC, CERN provides infrastructure
   access + a co-investigator. The AGPL + commercial dual-license
   structure fits the programme's framework.

2. **Present at CHEP.** The biennial *Computing in High-Energy and
   Nuclear Physics* conference is where new infrastructure gets first
   seen by the community. A paper or poster paired with one of the
   use-case slots above (DAQ, monitoring, ML embedding) gets the
   relevant people in a room.

3. **Pilot with one small team.** Not "CERN adopted OxiDB", just
   "one detector R&D group / beam test setup / internal monitoring
   tool runs on OxiDB". 1–2 years of that, with the team publishing
   their experience, is the basis on which the second pilot happens.

4. **Word spreads inside CERN.** Once two or three groups use it,
   "is anyone else here on OxiDB?" becomes a question people ask
   rather than a pitch someone delivers.

## Reading order for someone evaluating OxiDB for CERN

1. The README's warning block — understand what 0.x means.
2. [ADR-0003](decisions/0003-1.0-stability-scope.md) — what 1.0 will
   actually cover.
3. This document — where the fit is and isn't.
4. The Julia clients + benchmark
   ([`julia/benchmarks/sqlite_vs_oxidb.jl`](../julia/benchmarks/sqlite_vs_oxidb.jl))
   — an honest, reproducible engine comparison.

## Revisiting this document

This is an assessment, not a target — it should be updated as state
changes. Concretely, update when:

- A layer-1 item ships (e.g. 1.0 GA; migration tool merged)
- A layer-2 or layer-3 integration lands (e.g. Prometheus exporter; OIDC)
- A pilot starts or completes
- A relevant CERN-side decision becomes public (e.g. CTA succession plan,
  ROOT 8 timeline, etc.)
