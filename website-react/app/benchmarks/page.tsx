import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Benchmarks",
  description: `100K documents, single cold run, localhost TCP. Apple M4, 24 GB RAM.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="benchmarks" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg> Benchmarks</h2>
    <p class="section-desc">100K documents, single cold run, localhost TCP. Apple M4, 24 GB RAM.</p>

    <h3>OxiDB vs MongoDB (100K docs)</h3>
    <p class="score-line"><strong>OxiDB 19 -- MongoDB 1</strong></p>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Operation</th><th>OxiDB</th><th>MongoDB</th><th>Winner</th></tr></thead>
        <tbody>
          <tr><td>Bulk insert (100K, batch 1000)</td><td>870ms</td><td>998ms</td><td class="win-oxi">OxiDB 1.1x</td></tr>
          <tr><td>Exact match (20K results)</td><td>40ms</td><td>140ms</td><td class="win-oxi">OxiDB 3.5x</td></tr>
          <tr><td>Range query (47K results)</td><td>63ms</td><td>287ms</td><td class="win-oxi">OxiDB 4.6x</td></tr>
          <tr><td>Boolean query (50K results)</td><td>65ms</td><td>302ms</td><td class="win-oxi">OxiDB 4.7x</td></tr>
          <tr><td>Indexed: city=Tokyo (10K)</td><td>11ms</td><td>72ms</td><td class="win-oxi">OxiDB 6.3x</td></tr>
          <tr><td>Indexed: salary range (23K)</td><td>28ms</td><td>161ms</td><td class="win-oxi">OxiDB 5.7x</td></tr>
          <tr><td>Count all</td><td>70us</td><td>31ms</td><td class="win-oxi">OxiDB 446x</td></tr>
          <tr><td>Count with filter</td><td>62us</td><td>1.9ms</td><td class="win-oxi">OxiDB 30x</td></tr>
          <tr><td>Top 5 cities (aggregation)</td><td>84us</td><td>21ms</td><td class="win-oxi">OxiDB 247x</td></tr>
          <tr><td>Create 4 indexes</td><td>612ms</td><td>375ms</td><td class="win-other">MongoDB 1.6x</td></tr>
        </tbody>
      </table>
    </div>

    <h3>OxiDB vs PostgreSQL (100K docs)</h3>
    <p class="score-line"><strong>OxiDB 10 -- PostgreSQL 10</strong></p>
    <p>Tied overall. OxiDB wins on document queries, counts, and aggregation. PostgreSQL wins on bulk insert, indexed range queries, and JOIN-heavy workloads.</p>
  </div>
</section>` }} />
}