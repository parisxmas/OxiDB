"""Convert static HTML pages to Next.js App Router pages.
Uses dangerouslySetInnerHTML for code blocks to avoid JSX parsing issues."""
import re
import os

SRC = '/Users/barisakin/source/docdb/website'
DST = '/Users/barisakin/source/docdb/website-react/app'

pages = {
    'index.html': '',
    'downloads.html': 'downloads',
    'features.html': 'features',
    'quickstart.html': 'quickstart',
    'docs.html': 'docs',
    'queries.html': 'queries',
    'updates.html': 'updates',
    'aggregation.html': 'aggregation',
    'indexes.html': 'indexes',
    'transactions.html': 'transactions',
    'sql.html': 'sql',
    'search.html': 'search',
    'vectors.html': 'vectors',
    'blobs.html': 'blobs',
    'streams.html': 'streams',
    'server.html': 'server',
    'clients.html': 'clients',
    'go-examples.html': 'go-examples',
    'python-examples.html': 'python-examples',
    'storage.html': 'storage',
    'benchmarks.html': 'benchmarks',
    'changelog.html': 'changelog',
}

def extract_title(content):
    m = re.search(r'<title>OxiDB\s*-\s*(.+?)</title>', content)
    return m.group(1).strip() if m else 'OxiDB'

def extract_description(content):
    m = re.search(r'class="section-desc"[^>]*>(.+?)</p>', content, re.DOTALL)
    if m:
        return re.sub(r'<[^>]+>', '', m.group(1)).strip()[:200]
    return None

def extract_body(content):
    parts = content.split('</nav>')
    if len(parts) < 2:
        return ''
    after_nav = '</nav>'.join(parts[1:])
    parts2 = after_nav.split('<footer')
    body = parts2[0] if len(parts2) >= 2 else after_nav
    body = re.sub(r'<script>.*?</script>', '', body, flags=re.DOTALL)
    return body.strip()

def has_docs_tabs(body):
    return 'lang-tab' in body

for filename, route in pages.items():
    filepath = os.path.join(SRC, filename)
    if not os.path.exists(filepath):
        print(f'SKIP: {filename}')
        continue

    with open(filepath) as f:
        content = f.read()

    title = extract_title(content)
    desc = extract_description(content)
    body = extract_body(content)
    is_docs = has_docs_tabs(body)

    # For the docs page, we'll create a special client component
    # For all other pages, use dangerouslySetInnerHTML to avoid JSX issues

    out_dir = os.path.join(DST, route) if route else DST
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, 'page.tsx')

    # Escape backticks and ${} in body for template literal
    safe_body = body.replace('\\', '\\\\').replace('`', '\\`').replace('${', '\\${')

    # Convert .html links to clean URLs
    safe_body = re.sub(r'href="(\w[\w-]*)\.html"', r'href="/\1/"', safe_body)
    safe_body = safe_body.replace('href="index.html"', 'href="/"')

    lines = []

    if is_docs:
        lines.append("'use client'\n")
        lines.append("import { useEffect } from 'react'\n")

    lines.append('import type { Metadata } from "next"\n')

    if not is_docs:
        meta_lines = ['export const metadata: Metadata = {', f'  title: "{title}",']
        if desc:
            meta_lines.append(f'  description: `{desc.replace("`", "")}`,' )
        meta_lines.append('}\n')
        lines.append('\n'.join(meta_lines))

    lines.append('export default function Page() {')

    if is_docs:
        lines.append('  useEffect(() => {')
        lines.append('    // Language tab switching')
        lines.append("    document.querySelectorAll('.lang-tab').forEach(tab => {")
        lines.append("      tab.addEventListener('click', () => {")
        lines.append("        const block = tab.closest('.doc-block')")
        lines.append("        if (!block) return")
        lines.append("        block.querySelectorAll('.lang-tab').forEach(t => t.classList.remove('active'))")
        lines.append("        block.querySelectorAll('.lang-panel').forEach(p => p.classList.remove('active'))")
        lines.append("        tab.classList.add('active')")
        lines.append("        const lang = tab.getAttribute('data-lang')")
        lines.append("        block.querySelectorAll(`.lang-panel[data-lang=\"${lang}\"]`).forEach(p => p.classList.add('active'))")
        lines.append('      })')
        lines.append('    })')
        lines.append('    // Double-click to switch all')
        lines.append("    document.querySelectorAll('.lang-tab').forEach(tab => {")
        lines.append("      tab.addEventListener('dblclick', () => {")
        lines.append("        const lang = tab.getAttribute('data-lang')")
        lines.append("        document.querySelectorAll('.doc-block').forEach(block => {")
        lines.append("          const matchTab = block.querySelector(`.lang-tab[data-lang=\"${lang}\"]`)")
        lines.append("          if (matchTab) {")
        lines.append("            block.querySelectorAll('.lang-tab').forEach(t => t.classList.remove('active'))")
        lines.append("            block.querySelectorAll('.lang-panel').forEach(p => p.classList.remove('active'))")
        lines.append("            matchTab.classList.add('active')")
        lines.append("            block.querySelectorAll(`.lang-panel[data-lang=\"${lang}\"]`).forEach(p => p.classList.add('active'))")
        lines.append("          }")
        lines.append("        })")
        lines.append("      })")
        lines.append("    })")
        lines.append('  }, [])\n')

    lines.append(f'  return <div dangerouslySetInnerHTML={{{{ __html: `{safe_body}` }}}} />')
    lines.append('}')

    with open(out_file, 'w') as f:
        f.write('\n'.join(lines))

    print(f'OK: {filename} -> {route or "/"} (docs={is_docs})')

print('\nDone!')
