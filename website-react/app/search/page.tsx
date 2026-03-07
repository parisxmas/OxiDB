import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Full-Text Search",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="fts" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 016.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 014 19.5v-15A2.5 2.5 0 016.5 2z"/></svg> Full-Text Search</h2>

    <pre><code class="lang-python"><span class="co"># Create a text index on fields</span>
db.create_text_index(<span class="str">"articles"</span>, [<span class="str">"title"</span>, <span class="str">"body"</span>])

<span class="co"># Search with TF-IDF ranking</span>
results = db.text_search(<span class="str">"articles"</span>, <span class="str">"rust database performance"</span>, limit=<span class="num">10</span>)</code></pre>

    <h3>Supported Document Formats</h3>
    <ul>
      <li>Plain text</li>
      <li>HTML (tags stripped)</li>
      <li>XML</li>
      <li>JSON</li>
      <li>PDF</li>
      <li>DOCX</li>
      <li>XLSX</li>
      <li>Images via OCR (optional <code>ocr</code> feature flag)</li>
    </ul>

    <p>The search engine tokenizes text, removes stop words, and ranks results by TF-IDF similarity. Index is persisted to disk.</p>
  </div>
</section>` }} />
}