import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Benchmarks",
  description: `1M documents, single cold run, in-network Docker (apples-to-apples). AMD EPYC-Genoa 4 vCPU, 8 GiB RAM.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="benchmarks" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg> Benchmarks</h2>
    <p class="section-desc">1M documents (employee-shaped, ~500B each). Docker-Compose, in-network runner (container-to-container — no port-forward artifact). AMD EPYC-Genoa, 4 vCPU, 8 GiB RAM. Source: <code>tests/comparison-mongodb/</code>.</p>

    <h3>OxiDB vs MongoDB 7 (1M docs)</h3>
    <p class="score-line"><strong>OxiDB 24 &mdash; MongoDB 0</strong></p>
    <p>Across the full 24-test suite at 1M documents, OxiDB wins every measured workload. Largest wins are on counts (index-only path) and aggregation; smallest wins are insert (1.1&times;) and bulk full scans (1.1&ndash;1.3&times;).</p>

    <div class="table-wrap">
      <table>
        <thead><tr><th>Operation</th><th>OxiDB</th><th>MongoDB</th><th>Winner</th></tr></thead>
        <tbody>
          <tr><td>Count all (1M docs)</td><td>200&micro;s</td><td>429ms</td><td class="win-oxi">OxiDB 2189&times;</td></tr>
          <tr><td>Top 5 cities (aggregation)</td><td>1ms</td><td>822ms</td><td class="win-oxi">OxiDB 1262&times;</td></tr>
          <tr><td>Count age &ge; 50</td><td>200&micro;s</td><td>156ms</td><td class="win-oxi">OxiDB 645&times;</td></tr>
          <tr><td>Count dept=Engineering</td><td>300&micro;s</td><td>53ms</td><td class="win-oxi">OxiDB 197&times;</td></tr>
          <tr><td>Compound (composite idx) dept+status</td><td>161ms</td><td>626ms</td><td class="win-oxi">OxiDB 4.1&times;</td></tr>
          <tr><td>Random seq lookup &times; 100K</td><td>8.07s</td><td>27.6s</td><td class="win-oxi">OxiDB 3.4&times;</td></tr>
          <tr><td>Indexed: salary 80K-120K (235K results)</td><td>1.01s</td><td>3.25s</td><td class="win-oxi">OxiDB 3.2&times;</td></tr>
          <tr><td>Indexed: city=Tokyo (100K results)</td><td>448ms</td><td>1.37s</td><td class="win-oxi">OxiDB 3.0&times;</td></tr>
          <tr><td>Indexed: age &ge; 60 (300K results)</td><td>1.43s</td><td>4.12s</td><td class="win-oxi">OxiDB 2.9&times;</td></tr>
          <tr><td>Indexed: dept=Engineering (199K results)</td><td>1.17s</td><td>2.65s</td><td class="win-oxi">OxiDB 2.3&times;</td></tr>
          <tr><td>Create 4 indexes on 1M docs</td><td>2.82s</td><td>7.33s</td><td class="win-oxi">OxiDB 2.6&times;</td></tr>
          <tr><td>Bulk delete (10% of corpus)</td><td>5.88s</td><td>13.3s</td><td class="win-oxi">OxiDB 2.3&times;</td></tr>
          <tr><td>$or city=Tokyo OR Paris (no idx)</td><td>1.48s</td><td>3.15s</td><td class="win-oxi">OxiDB 2.1&times;</td></tr>
          <tr><td>Compound full scan (no idx)</td><td>515ms</td><td>1.03s</td><td class="win-oxi">OxiDB 2.0&times;</td></tr>
          <tr><td>$in country (no idx)</td><td>2.21s</td><td>4.03s</td><td class="win-oxi">OxiDB 1.8&times;</td></tr>
          <tr><td>Range (salary 50K-100K) no idx</td><td>2.06s</td><td>3.95s</td><td class="win-oxi">OxiDB 1.9&times;</td></tr>
          <tr><td>Exact match dept=Engineering (no idx)</td><td>2.18s</td><td>2.95s</td><td class="win-oxi">OxiDB 1.4&times;</td></tr>
          <tr><td>Match + Group (aggregation)</td><td>247ms</td><td>336ms</td><td class="win-oxi">OxiDB 1.4&times;</td></tr>
          <tr><td>Nested address.zip range (dot-path)</td><td>1.47s</td><td>1.91s</td><td class="win-oxi">OxiDB 1.3&times;</td></tr>
          <tr><td>Range query &times; 10K (10.2M rows total)</td><td>30.2s</td><td>35.9s</td><td class="win-oxi">OxiDB 1.2&times;</td></tr>
          <tr><td>Bulk insert 1M docs (batch 1000)</td><td>16.0s</td><td>18.3s</td><td class="win-oxi">OxiDB 1.1&times;</td></tr>
        </tbody>
      </table>
    </div>

    <h3>Resource footprint (post-bench, 1M docs)</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Resource</th><th>OxiDB</th><th>MongoDB</th><th>Note</th></tr></thead>
        <tbody>
          <tr><td>Memory (RSS)</td><td>1.71 GiB</td><td>1.00 GiB</td><td>OxiDB caches are env-tunable (<code>OXIDB_DOC_CACHE_SIZE</code>, <code>OXIDB_DOC_BYTES_CACHE_SIZE</code>); MongoDB WT cache capped at 0.5 GiB.</td></tr>
          <tr><td>Disk (data dir)</td><td>741 MB</td><td>626 MB</td><td>OxiDB stores JSONB; MongoDB stores compressed WiredTiger. ~18% larger on disk.</td></tr>
        </tbody>
      </table>
    </div>

    <h3>Methodology</h3>
    <ul>
      <li><strong>Apples-to-apples networking.</strong> The Go bench runner runs as a container on the same Docker network as the engines (<code>BENCH_MODE=innetwork</code>). Host-mode runs through Docker&apos;s port-forward add ~160&micro;s/round-trip that distorts small-payload workloads.</li>
      <li><strong>Single cold run.</strong> Each test runs once on a freshly-inserted 1M-doc corpus. No warmup.</li>
      <li><strong>Default configurations.</strong> No tuning. OxiDB uses defaults; MongoDB uses WiredTiger with <code>--wiredTigerCacheSizeGB 0.5</code> per the existing compose config.</li>
      <li><strong>Same seed.</strong> Both engines see the identical document corpus (deterministic generator, fixed RNG seed).</li>
    </ul>

    <h3>OxiDB vs PostgreSQL (100K docs)</h3>
    <p class="score-line"><strong>OxiDB 10 &mdash; PostgreSQL 10</strong></p>
    <p>Tied overall at 100K docs. OxiDB wins on document queries, counts, and aggregation. PostgreSQL wins on bulk insert, indexed range queries, and JOIN-heavy workloads. (Pending re-run at 1M scale.)</p>
  </div>
</section>` }} />
}
