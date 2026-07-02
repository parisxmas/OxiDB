"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DocumentViewProvider = exports.SqlResultsView = void 0;
const vscode = require("vscode");
const BASE_STYLE = `
  body { font-family: var(--vscode-font-family); color: var(--vscode-foreground); background: var(--vscode-editor-background); padding: 16px; }
  h2 { margin: 0 0 8px; }
  h3 { margin: 16px 0 8px; }
  .stats { color: var(--vscode-descriptionForeground); margin-bottom: 16px; }
  .toolbar { display: flex; gap: 8px; margin-bottom: 16px; }
  input, textarea { background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border); padding: 6px 10px; border-radius: 4px; font-family: var(--vscode-editor-font-family); }
  textarea { width: 100%; min-height: 60px; resize: vertical; }
  button { background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; padding: 6px 14px; border-radius: 4px; cursor: pointer; }
  button:hover { background: var(--vscode-button-hoverBackground); }
  .grid { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { background: var(--vscode-editor-selectionBackground); text-align: left; padding: 6px 10px; position: sticky; top: 0; }
  td { padding: 4px 10px; border-bottom: 1px solid var(--vscode-widget-border); max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  td.null { color: var(--vscode-descriptionForeground); font-style: italic; }
  tr:hover td { background: var(--vscode-list-hoverBackground); }
  .tabs { display: flex; gap: 0; margin-bottom: 16px; }
  .tab { padding: 8px 16px; cursor: pointer; border-bottom: 2px solid transparent; }
  .tab.active { border-bottom-color: var(--vscode-focusBorder); }
  .panel { display: none; }
  .panel.active { display: block; }
  pre { background: var(--vscode-textBlockQuote-background); padding: 12px; border-radius: 4px; overflow-x: auto; }
  .idx { margin: 4px 0; padding: 4px 8px; background: var(--vscode-badge-background); color: var(--vscode-badge-foreground); border-radius: 4px; display: inline-block; font-size: 12px; }
  .ok { color: var(--vscode-testing-iconPassed, #73c991); }
  .meta { color: var(--vscode-descriptionForeground); font-size: 12px; margin-bottom: 8px; }
`;
function esc(s) {
    return String(s ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}
function fmt(v) {
    if (v === null || v === undefined) {
        return '';
    }
    if (typeof v === 'object') {
        return JSON.stringify(v);
    }
    return String(v);
}
/** Render one SQL statement result as an HTML fragment. */
function sqlResultHtml(r, i, total) {
    const heading = total > 1 ? `<h3>Statement ${i + 1}</h3>` : '';
    if ('columns' in r) {
        const head = r.columns.map((c) => `<th>${esc(c)}</th>`).join('');
        const body = r.rows
            .map((row) => '<tr>' +
            row
                .map((v) => {
                if (v === null || v === undefined) {
                    return '<td class="null">NULL</td>';
                }
                const cls = typeof v === 'number' ? ' class="num"' : '';
                return `<td${cls} title="${esc(fmt(v))}">${esc(fmt(v))}</td>`;
            })
                .join('') +
            '</tr>')
            .join('');
        return `${heading}<div class="meta">${r.rows.length} row${r.rows.length === 1 ? '' : 's'}</div>
<div class="grid"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
    }
    if ('affected' in r) {
        return `${heading}<p class="ok">✓ ${r.affected} row${r.affected === 1 ? '' : 's'} affected</p>`;
    }
    if ('ddl' in r) {
        return `${heading}<p class="ok">✓ OK</p>`;
    }
    return `${heading}<p class="ok">✓ transaction</p>`;
}
/** A single reused panel showing the results of Run SQL. */
class SqlResultsView {
    constructor() {
        this.panel = null;
    }
    show(sql, results, elapsedMs) {
        if (!this.panel) {
            this.panel = vscode.window.createWebviewPanel('oxidb.sqlResults', 'OxiDB SQL Results', { viewColumn: vscode.ViewColumn.Beside, preserveFocus: true }, { enableScripts: false });
            this.panel.onDidDispose(() => { this.panel = null; });
        }
        const fragments = results.map((r, i) => sqlResultHtml(r, i, results.length)).join('\n');
        this.panel.webview.html = `<!DOCTYPE html>
<html><head><style>${BASE_STYLE}</style></head><body>
<div class="meta">${esc(sql.length > 200 ? sql.slice(0, 200) + '…' : sql)} — ${elapsedMs}ms</div>
${fragments}
</body></html>`;
        this.panel.reveal(vscode.ViewColumn.Beside, true);
    }
}
exports.SqlResultsView = SqlResultsView;
class DocumentViewProvider {
    constructor(client) {
        this.client = client;
        this.panel = null;
    }
    async showCollection(collection) {
        const docs = await this.client.find(collection, {}, 100);
        const count = await this.client.count(collection);
        const indexes = await this.client.listIndexes(collection);
        this.panel = vscode.window.createWebviewPanel('oxidb.documents', `OxiDB: ${collection}`, vscode.ViewColumn.One, { enableScripts: true });
        this.panel.webview.html = this.getHtml(collection, docs, count, indexes);
        this.panel.webview.onDidReceiveMessage(async (msg) => {
            try {
                switch (msg.type) {
                    case 'query': {
                        const query = JSON.parse(msg.query || '{}');
                        const results = await this.client.find(collection, query, msg.limit || 50);
                        this.panel?.webview.postMessage({ type: 'results', data: results });
                        break;
                    }
                    case 'insert': {
                        const doc = JSON.parse(msg.doc);
                        await this.client.insert(collection, doc);
                        vscode.window.showInformationMessage('Document inserted');
                        break;
                    }
                    case 'delete': {
                        const q = JSON.parse(msg.query);
                        await this.client.deleteMany(collection, q);
                        vscode.window.showInformationMessage('Documents deleted');
                        break;
                    }
                    case 'aggregate': {
                        const pipeline = JSON.parse(msg.pipeline);
                        const results = await this.client.aggregate(collection, pipeline);
                        this.panel?.webview.postMessage({ type: 'results', data: results });
                        break;
                    }
                }
            }
            catch (e) {
                vscode.window.showErrorMessage(`OxiDB: ${e.message}`);
            }
        });
    }
    async showQueryResults(title, data) {
        const panel = vscode.window.createWebviewPanel('oxidb.results', title, vscode.ViewColumn.One, { enableScripts: true });
        const docs = Array.isArray(data) ? data : [data];
        panel.webview.html = this.getResultsHtml(title, docs);
    }
    getHtml(collection, docs, count, indexes) {
        const fields = new Set();
        docs.forEach((d) => Object.keys(d).forEach((k) => fields.add(k)));
        const cols = Array.from(fields);
        return `<!DOCTYPE html>
<html><head>
<style>${BASE_STYLE}</style>
</head><body>
<h2>${esc(collection)}</h2>
<div class="stats">${count} documents | ${indexes.length} indexes: ${indexes.map((i) => `<span class="idx">${esc(i.name || i.field)}</span>`).join(' ')}</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('query')">Query</div>
  <div class="tab" onclick="showTab('insert')">Insert</div>
  <div class="tab" onclick="showTab('aggregate')">Aggregate</div>
</div>

<div id="query" class="panel active">
  <textarea id="queryInput" placeholder='{"status": "active"}'>{}</textarea>
  <div class="toolbar">
    <button onclick="runQuery()">Find</button>
    <input id="limitInput" type="number" value="50" style="width:60px" placeholder="limit">
  </div>
</div>

<div id="insert" class="panel">
  <textarea id="insertInput" placeholder='{"name": "Alice", "age": 30}'></textarea>
  <button onclick="insertDoc()">Insert</button>
</div>

<div id="aggregate" class="panel">
  <textarea id="aggInput" placeholder='[{"$group": {"_id": "$status", "count": {"$sum": 1}}}]'></textarea>
  <button onclick="runAggregate()">Run Pipeline</button>
</div>

<div id="results" class="grid">
<table><thead><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join('')}</tr></thead>
<tbody>${docs.map((d) => `<tr>${cols.map((c) => `<td title="${esc(JSON.stringify(d[c]))}">${esc(fmt(d[c]))}</td>`).join('')}</tr>`).join('')}</tbody></table>
</div>

<script>
const vscode = acquireVsCodeApi();
function showTab(id) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.target.classList.add('active');
}
function runQuery() {
  vscode.postMessage({ type: 'query', query: document.getElementById('queryInput').value, limit: parseInt(document.getElementById('limitInput').value) || 50 });
}
function insertDoc() {
  vscode.postMessage({ type: 'insert', doc: document.getElementById('insertInput').value });
}
function runAggregate() {
  vscode.postMessage({ type: 'aggregate', pipeline: document.getElementById('aggInput').value });
}
function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function fmt(v) { if (v === null || v === undefined) return ''; if (typeof v === 'object') return JSON.stringify(v); return String(v); }

window.addEventListener('message', e => {
  if (e.data.type === 'results') {
    const docs = e.data.data;
    if (!docs.length) { document.getElementById('results').innerHTML = '<p>No results</p>'; return; }
    const cols = [...new Set(docs.flatMap(d => Object.keys(d)))];
    let html = '<table><thead><tr>' + cols.map(c => '<th>'+escHtml(c)+'</th>').join('') + '</tr></thead><tbody>';
    docs.forEach(d => { html += '<tr>' + cols.map(c => '<td title="'+escHtml(JSON.stringify(d[c]))+'">'+escHtml(fmt(d[c]))+'</td>').join('') + '</tr>'; });
    html += '</tbody></table>';
    document.getElementById('results').innerHTML = html;
  }
});
</script>
</body></html>`;
    }
    getResultsHtml(title, docs) {
        return `<!DOCTYPE html>
<html><head>
<style>${BASE_STYLE}</style>
</head><body>
<h3>${esc(title)}</h3>
<pre>${esc(JSON.stringify(docs, null, 2))}</pre>
</body></html>`;
    }
}
exports.DocumentViewProvider = DocumentViewProvider;
//# sourceMappingURL=documentView.js.map