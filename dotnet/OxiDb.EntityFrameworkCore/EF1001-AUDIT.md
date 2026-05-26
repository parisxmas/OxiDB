# EF1001 — internal-API usage audit

EF Core flags internal-API usage with `EF1001` so providers know which
calls aren't covered by EF's compat promise. This file lists every
EF1001 hit in `OxiDb.EntityFrameworkCore` plus the planned migration
path.

## Inventory (as of v0.28.18)

| Site | Internal type used | Purpose |
|---|---|---|
| `Query/OxiDbQueryCompiler.cs:445` | `Microsoft.EntityFrameworkCore.ChangeTracking.Internal.IStateManager` | Identity-resolution path. After deserializing a row from the OxiDB server, we ask the state manager whether an entity with the same key is already tracked. If yes, hand back the tracked instance so mutations land on the object `SaveChanges` will inspect. |
| `Query/OxiDbQueryCompiler.cs:449` | `IStateManager.TryGetEntry` | The lookup call. |
| `Query/OxiDbQueryCompiler.cs:451` | `InternalEntityEntry.Entity` | Pulls the actual tracked `T` off the matched entry. |

**One real escape**, three syntactic locations (same code path).

## Why it's there

Identity resolution (one instance per key inside a `DbContext`) is a
correctness property the rest of EF Core relies on:

- Two `Find<T>(...)` calls with the same key must return the same
  reference, so a setter on the first object is visible to consumers of
  the second.
- Change-tracking diffs are computed against the snapshot for the
  tracked instance — if two instances exist, only one gets diffed and
  the other's mutations are lost on `SaveChanges`.

Document providers (Cosmos, Mongo-EF, our OxiDB provider) hit this
because we materialise rows ourselves from a JSON payload rather than
letting EF's relational query pipeline build them.

## Migration paths, by cost

### 1. Per-context identity cache (provider-owned)

Maintain our own `Dictionary<(Type, object[]), object>` keyed by entity
type + key values. Look up before calling `Attach`. Survives only the
lifetime of one `DbContext`.

- **Cost:** ~80 LOC, contained to `OxiDbQueryCompiler` + a small
  per-context state object retrieved via `context.GetInfrastructure().GetService<...>()`
  (public API).
- **Risk:** subtle. The cache must respect detach + reattach, and
  Find-then-Modify-then-Find must return the *modified* instance.
- **Wins:** zero EF1001. The contract becomes ours, decoupled from
  EF's internal IStateManager.

### 2. Public `ChangeTracker.Entries<T>()` lookup

`context.ChangeTracker.Entries<T>()` is public and enumerable. For each
materialised row, scan the entries, compare keys, return the tracked
instance if matched.

- **Cost:** ~20 LOC.
- **Risk:** O(tracked) per lookup. For a `Find` returning 10 entities,
  10 × N scans of the change tracker. Fine for ChangeTracker.AutoDetectChanges
  off paths or small contexts; pathological for large tracked sets.
- **Wins:** simplest fix that gets us off the internal type.

### 3. Replace IStateManager with the public InMemoryProvider pattern

EF.InMemory rolls its own state via public APIs (it doesn't reach into
the relational query pipeline at all). Mirror that structure for the
OxiDB provider: skip query compilation altogether, run the LINQ over
OxiDB's own filter API (we already have a translator in `OxiDb.Linq`),
hand results back to EF via `IQueryable<T>` materialisation.

- **Cost:** large — possibly a full rewrite of `OxiDbQueryCompiler` and
  `OxiDbQueryProvider`. Several weeks of focused work.
- **Risk:** identity-resolution + change-tracking become provider-
  managed, which is the right shape for a NoSQL provider but a lot to
  get right. Pulling in lessons from EF.Cosmos / EF.Mongo is mandatory.
- **Wins:** the EF1001 surface goes to zero, the provider becomes
  EF-version-portable, and the design matches every other document-
  oriented EF provider in the wild.

## Decision

**Short-term (next minor):** Option 1 — per-context identity cache.
Eliminates the EF1001 warnings and gives us the contract guarantee.

**Long-term (1.x stable surface):** Option 3 — full alignment with
EF.Cosmos / EF.Mongo patterns. Tracked under ADR-0003 (Phase 6 RC
follow-ups).

## Acceptance for closing this file

This audit file is removed when:

1. `dotnet build OxiDb.EntityFrameworkCore.csproj -c Release` emits
   **zero** `warning EF1001`.
2. A test in `OxiDb.Tests` proves identity resolution across two
   `Find<T>()` calls + a setter mutation visible on the second call.
3. ADR-0003 Phase 6 status notes the provider as "EF1001-clean".
