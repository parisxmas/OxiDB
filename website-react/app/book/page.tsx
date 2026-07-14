import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Book",
  description: `Her Şeyin Veritabanı — a free 235-page Turkish book by Barış AKIN on how a database works inside, from first principles to OxiDB's document, SQL and time-series engines. Download as PDF or EPUB.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 016.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 014 19.5v-15A2.5 2.5 0 016.5 2z"/></svg> The Book</h2>
    <p class="section-desc"><strong>Her Şeyin Veritabanı</strong> — "The Database of Everything". A free book by Barış AKIN on how a database actually works inside: from first principles to the engines that power OxiDB. <strong>Written in Turkish.</strong></p>

    <div class="release-block">
      <div class="release-header">
        <h3 class="version-tag">Her Şeyin Veritabanı</h3>
        <span class="version-date">2026</span>
        <span class="version-badge latest">free</span>
      </div>
      <p class="release-notes">
        OxiDB: Belge, SQL ve Zaman Serisi Motorları, S3 Nesne Depolama ve OxiMem Anahtar-Değer Katmanı.
        235 pages · 32 chapters + 2 appendices · ~83,000 words · 114 code examples, all verified against a running server.
      </p>

      <div class="table-wrap">
        <table class="dl-table">
          <thead>
            <tr><th>Format</th><th>File</th><th>Size</th><th>Notes</th><th></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>PDF</td>
              <td><code>her-seyin-veritabani.pdf</code></td>
              <td>5.3 MB</td>
              <td>235 pages, print-ready</td>
              <td><a href="/kitap/her-seyin-veritabani.pdf?v=20260715b" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>EPUB</td>
              <td><code>her-seyin-veritabani.epub</code></td>
              <td>965 KB</td>
              <td>e-readers, reflowable</td>
              <td><a href="/kitap/her-seyin-veritabani.epub?v=20260715b" class="dl-btn">Download</a></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <h3>What is inside</h3>
    <p>The book takes two journeys at once. The first is conceptual: what a database has to solve and how those problems are solved in general — durability and write-ahead logging, storage engines, indexing, query planning, aggregation, transactions, isolation, replication and sharding. The second is concrete: how every one of those principles becomes a real engineering decision inside OxiDB, with the trade-offs made explicit.</p>

    <div class="table-wrap">
      <table class="dl-table">
        <thead>
          <tr><th>Part</th><th>Chapters</th><th>Subject</th></tr>
        </thead>
        <tbody>
          <tr><td>I — Foundations</td><td>1–4</td><td>Data, data models, the relational-to-document shift, the document model in depth</td></tr>
          <tr><td>II — How a database works inside</td><td>5–14</td><td>Storage engines, WAL and crash recovery, indexing, query processing, aggregation, transactions, concurrency, scaling, memory/cache/disk, security</td></tr>
          <tr><td>III — OxiDB step by step</td><td>15–27</td><td>Architecture, storage, WAL, indexes, query engine, aggregation pipeline, transactions, compaction, full-text search and PITR, server protocol, Raft cluster, clients, memory tuning</td></tr>
          <tr><td>IV — The other engines</td><td>28–32</td><td>The SQL engine, the time-series engine (Gorilla compression), OxiMem (RESP key-value), S3-compatible object storage, and how to use all of them in one application</td></tr>
        </tbody>
      </table>
    </div>

    <h3>Örnekler</h3>
    <p>Kitabın ilk iki kısmı bilinçli olarak kodsuzdur: amaç, sizi bir sözdizimine bağlamadan kavramın kendisini kurmaktır. Üçüncü ve dördüncü kısımlarda ise çalışan sisteme karşı denenmiş <strong>114 örnek</strong> bulunur — tel üzerindeki JSON istekleri, SQL ifadeleri, Python, C#, JavaScript ve kabuk komutları.</p>

    <h3>License</h3>
    <p>The book is free to read and download. © 2026 Barış AKIN. The book text is not covered by the OxiDB software license; see <a href="/license/">License</a> for the engine itself.</p>
  </div>
</section>` }} />
}
