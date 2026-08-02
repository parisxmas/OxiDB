'use client'

import { usePathname } from 'next/navigation'
import { useEffect } from 'react'

declare global {
  interface Window {
    hljs?: {
      highlightElement: (el: Element) => void
    }
  }
}

const COPY_ICON =
  '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>'
const CHECK_ICON =
  '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>'

// Adds a copy button to every code block, and syntax-highlights the plain
// `<pre><code class="lang-*">` blocks with highlight.js (self-hosted). Blocks
// that already carry manual highlight spans, and the homepage terminal, are
// left alone. Re-runs on client-side navigation.
export default function CodeEnhance() {
  const pathname = usePathname()

  useEffect(() => {
    const run = () => {
      document.querySelectorAll('pre').forEach((pre) => {
        const el = pre as HTMLElement
        if (el.dataset.enhanced) return
        if (pre.closest('.termblock')) return // homepage terminal — leave as-is
        el.dataset.enhanced = '1'

        const code = pre.querySelector('code')
        // Capture the clean text BEFORE highlighting so copy never grabs markup.
        const text = (code || pre).textContent || ''

        const btn = document.createElement('button')
        btn.type = 'button'
        btn.className = 'code-copy'
        btn.setAttribute('aria-label', 'Copy code')
        btn.innerHTML = COPY_ICON
        btn.addEventListener('click', async () => {
          try {
            await navigator.clipboard.writeText(text)
          } catch {
            const ta = document.createElement('textarea')
            ta.value = text
            ta.style.position = 'fixed'
            ta.style.opacity = '0'
            document.body.appendChild(ta)
            ta.select()
            try {
              document.execCommand('copy')
            } catch {}
            ta.remove()
          }
          btn.innerHTML = CHECK_ICON
          btn.classList.add('copied')
          setTimeout(() => {
            btn.innerHTML = COPY_ICON
            btn.classList.remove('copied')
          }, 1400)
        })
        pre.appendChild(btn)
      })

      const hljs = window.hljs
      if (!hljs) return
      // Highlight every lang-* block. hljs reads textContent, so any manual
      // comment spans already inside are flattened and re-highlighted
      // consistently (a hand-styled `.co` becomes `.hljs-comment`, same color).
      document.querySelectorAll('pre > code[class*="lang-"]').forEach((el) => {
        const code = el as HTMLElement
        if (code.dataset.hl) return
        code.dataset.hl = '1'
        try {
          hljs.highlightElement(code)
        } catch {}
      })
    }

    // Load highlight.js once, then run; on later navigations it's already there.
    if (window.hljs) {
      run()
    } else {
      const existing = document.getElementById('hljs-script') as HTMLScriptElement | null
      if (existing) {
        existing.addEventListener('load', run)
        run() // copy buttons don't need hljs
      } else {
        const s = document.createElement('script')
        s.id = 'hljs-script'
        s.src = '/vendor/hljs.min.js'
        s.onload = run
        document.head.appendChild(s)
        run() // add copy buttons immediately, highlight once hljs loads
      }
    }
  }, [pathname])

  return null
}
