import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "How OxiDB Is Tested",
  description:
    "Evidence over claims: MongoDB's own specification tests, SIGKILL crash suites, fault injection, red-first proofs, fuzz trophies, and honest benchmark methodology — with every divergence documented.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/></svg> How OxiDB is tested</h2>
    <p class="section-desc">A database vendor saying &ldquo;trust our tests&rdquo; is worth little. This page lists the evidence instead: <strong>tests other people wrote</strong>, tests that were <strong>proven to fail before the feature existed</strong>, crash tests that <strong>kill the process for real</strong>, and every known divergence &mdash; documented, not hidden.</p>

    <h3>MongoDB&rsquo;s own specification tests</h3>
    <p>The MongoDB unified spec tests (<code>mongodb/specifications</code>) are the language-neutral JSON files every official MongoDB driver is validated against. We run all 189 CRUD files against the OxiDB document engine:</p>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Result</th><th>Count</th><th>Meaning</th></tr></thead>
        <tbody>
          <tr><td><strong>Passed</strong></td><td><strong>147</strong></td><td>Byte-for-byte the behavior MongoDB's spec demands: CRUD, upsert seeding, $setOnInsert, pipeline updates, arrayFilters, bulkWrite, findOneAnd*, projections, matched-vs-modified counting</td></tr>
          <tr><td>Unexpected failures</td><td><strong>0</strong></td><td>&mdash;</td></tr>
          <tr><td>Known divergences</td><td>4</td><td>All one root cause, documented in the runner: OxiDB assigns document ids itself, so a duplicate client-supplied <code>_id</code> does not error</td></tr>
          <tr><td>Skipped</td><td>~385</td><td>Every one with a machine-readable reason: legacy-server error emulation (117), driver-internal concepts (command monitoring, read/write concerns, failpoints), 5.0 $setField escape hatches</td></tr>
        </tbody>
      </table>
    </div>
    <p>The suite runs with <code>scripts/run_mongo_spec.sh</code>. The spec files are CC&nbsp;BY-NC-SA licensed, so they are cloned at run time rather than vendored &mdash; and the adapter's one shim (engine-assigned <code>_id</code>) is documented in the runner's header, not smuggled past you.</p>
    <p>On the SQL side the same philosophy already ran its course: the EF&nbsp;Core provider passes <strong>all 3832 of Microsoft's official EF Core relational specification tests</strong>.</p>

    <h3>Crash tests that actually crash</h3>
    <p>Graceful shutdown hides durability bugs: <code>Drop</code> flushes in-memory state and papers over a lost write. Every OxiDB crash suite therefore <strong>SIGKILLs a subprocess</strong> mid-write and verifies acknowledged writes after recovery &mdash; WAL replay, sealed-segment replay, checkpoint interleavings, multi-collection atomicity, encrypted stores.</p>
    <p>Two findings that shaped the method:</p>
    <ul>
      <li><strong>A race window can be too small to test honestly.</strong> The online-checkpoint barrier protects a &micro;s-wide window between WAL append and B-tree apply; the naive 5-round SIGKILL test passed <em>with the barrier deleted</em>. The suite now widens the window with a hidden stall hook so the test fails in seconds without the fix.</li>
      <li><strong>Red-first proofs.</strong> Before MVCC-lite shipped, its torn-read test was extracted and run against the engine <em>without</em> the feature &mdash; and had to be strengthened until it reliably failed (observed sum 40005 vs 40000 within 2s). A test that never went red proves nothing.</li>
    </ul>

    <h3>Fault injection</h3>
    <ul>
      <li><strong>fsync faults:</strong> injected write/sync failures must poison durability &mdash; the engine refuses to persist state containing a rejected transaction and rebuilds from the durable snapshot + WAL.</li>
      <li><strong>Jepsen-style bank workload:</strong> concurrent transfers with SIGKILL rounds; total balance must survive every crash.</li>
      <li><strong>Raft partitions:</strong> symmetric and asymmetric (deaf-node, reply-loss) partition disruptors against 3-node clusters &mdash; each round found or ruled out a real bug, mutation-verified.</li>
      <li><strong>p99 soak:</strong> 1.6M ops at ~5.4k ops/s on Linux; read p99 3.9&nbsp;ms, no drift, RSS plateau.</li>
    </ul>

    <h3>Fuzzing &mdash; with trophies published</h3>
    <p>An unauthenticated fuzz harness against the wire protocol found <strong>4 real denial-of-service bugs</strong> (fixed in 0.28.3; servers older than that are vulnerable). A fuzzer that has never caught anything is a fuzzer that isn't trying; ours has a trophy list and keeps running.</p>

    <h3>Honest semantics: the isolation scorecard</h3>
    <p>OxiDB documents exactly which anomalies its OCC model admits and which it excludes &mdash; a per-anomaly scorecard in <code>docs/isolation.md</code>, pinned by a characterization suite so the docs cannot drift from the engine. Aggregations are snapshot-consistent by default (MVCC-lite): a concurrent aggregate can never observe half a transfer.</p>

    <h3>Benchmark methodology &mdash; including the losses</h3>
    <p>The 1M-document MongoDB comparison runs both engines natively, same machine, same indexes, client outside Docker (an earlier setup measured a Docker port-forward artifact and flattered nobody &mdash; we documented it). Current standing: <strong>OxiDB faster in 12 of 18 operations</strong>, including every aggregation and bulk updates. The losses are printed too, with their causes:</p>
    <ul>
      <li><strong>UpdateOne latency:</strong> OxiDB fsyncs every commit with <code>F_FULLFSYNC</code> (~4&nbsp;ms on Apple SSDs, measured); MongoDB's default acknowledges before its journal reaches disk. At equal durability settings the two engines are within 2&micro;s of each other &mdash; the gap is a durability policy, not engine speed.</li>
      <li><strong>Index build / DeleteMany:</strong> the cost of maintaining three compound indexes that in exchange answer $group aggregations 5&ndash;8&times; faster than MongoDB without reading a single document.</li>
    </ul>
    <p>One harness lesson worth stealing: in an earlier EF&nbsp;Core benchmark, the more OxiDB won a round, the worse its <em>next</em> number looked &mdash; the idle server paid recovery inside the measured window. Benchmarks get root-caused here like bugs do.</p>

    <h3>The numbers today</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Suite</th><th>Standing</th></tr></thead>
        <tbody>
          <tr><td>Core engine (unit + integration, incl. crash suites)</td><td>898 / 898</td></tr>
          <tr><td>MongoDB CRUD specification tests</td><td>147 passed, 0 unexpected failures</td></tr>
          <tr><td>EF Core relational specification tests (SQL engine)</td><td>3832 / 3832</td></tr>
          <tr><td>Server suites (ACID, security, protocol)</td><td>green</td></tr>
          <tr><td>WASM build (document engine)</td><td>clean</td></tr>
        </tbody>
      </table>
    </div>

    <h3>What&rsquo;s next on the evidence ladder</h3>
    <ul>
      <li><strong>Exhaustive crash-point testing:</strong> a fault-injecting storage shim that kills at <em>every</em> fsync boundary, not at random ones &mdash; the SQLite discipline.</li>
      <li><strong>Differential testing:</strong> millions of random operation sequences replayed against a real MongoDB, every divergence either fixed or added to the documented list.</li>
      <li><strong>Jepsen + Elle with published histories</strong>, so the linearizability claim is something you can re-check yourself rather than believe.</li>
    </ul>
  </div>
</section>` }} />
}
