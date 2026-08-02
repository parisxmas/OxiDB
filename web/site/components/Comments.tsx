'use client'

import { useEffect, useState, useCallback, useRef, FormEvent } from 'react'
import { usePathname } from 'next/navigation'

type Comment = {
  _id: number
  page: string
  author: string
  body: string
  client_ts: string
}

const API = ''
const MAX_NAME = 50
const MAX_BODY = 2000
const TURNSTILE_SITEKEY = '0x4AAAAAAADDSgn2apBAo8CtA'

declare global {
  interface Window {
    turnstile?: {
      render: (
        container: string | HTMLElement,
        opts: {
          sitekey: string
          theme?: 'light' | 'dark' | 'auto'
          callback?: (token: string) => void
          'expired-callback'?: () => void
          'error-callback'?: () => void
        },
      ) => string
      reset: (widgetId?: string) => void
      remove: (widgetId?: string) => void
    }
  }
}

function timeAgo(iso: string): string {
  const t = Date.parse(iso)
  if (isNaN(t)) return ''
  const sec = Math.max(0, Math.floor((Date.now() - t) / 1000))
  if (sec < 60) return 'just now'
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.floor(min / 60)
  if (hr < 24) return `${hr}h ago`
  const day = Math.floor(hr / 24)
  if (day < 30) return `${day}d ago`
  const mo = Math.floor(day / 30)
  if (mo < 12) return `${mo}mo ago`
  return `${Math.floor(mo / 12)}y ago`
}

export default function Comments() {
  const pathname = usePathname() || '/'
  const [comments, setComments] = useState<Comment[]>([])
  const [loading, setLoading] = useState(true)
  const [name, setName] = useState('')
  const [body, setBody] = useState('')
  const [posting, setPosting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // Honeypot: real users never type here; bots fill all named inputs.
  const [website, setWebsite] = useState('')
  const [turnstileToken, setTurnstileToken] = useState('')

  // Turnstile widget lifecycle.
  const widgetRef = useRef<HTMLDivElement>(null)
  const widgetIdRef = useRef<string | undefined>(undefined)

  useEffect(() => {
    let cancelled = false
    let attempts = 0

    function tryRender() {
      if (cancelled) return
      attempts++
      if (typeof window === 'undefined' || !window.turnstile || !widgetRef.current) {
        if (attempts < 100) setTimeout(tryRender, 100)
        return
      }
      // Already rendered → skip
      if (widgetIdRef.current) return
      widgetIdRef.current = window.turnstile.render(widgetRef.current, {
        sitekey: TURNSTILE_SITEKEY,
        theme: 'dark',
        callback: (token) => setTurnstileToken(token),
        'expired-callback': () => setTurnstileToken(''),
        'error-callback': () => setTurnstileToken(''),
      })
    }

    tryRender()
    return () => {
      cancelled = true
      if (widgetIdRef.current && typeof window !== 'undefined' && window.turnstile) {
        try {
          window.turnstile.remove(widgetIdRef.current)
        } catch {}
        widgetIdRef.current = undefined
      }
    }
  }, [pathname])

  function resetTurnstile() {
    setTurnstileToken('')
    if (widgetIdRef.current && typeof window !== 'undefined' && window.turnstile) {
      try {
        window.turnstile.reset(widgetIdRef.current)
      } catch {}
    }
  }

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const r = await fetch(`${API}/api/comments/list`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ page: pathname }),
      })
      const data = await r.json()
      if (Array.isArray(data)) setComments(data)
      else setComments([])
    } catch {
      setError('Failed to load comments.')
    } finally {
      setLoading(false)
    }
  }, [pathname])

  useEffect(() => {
    load()
    try {
      const saved = localStorage.getItem('oxidb-comment-author')
      if (saved) setName(saved)
    } catch {}
  }, [load])

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    const n = name.trim()
    const b = body.trim()
    if (!n) return setError('Please enter a name.')
    if (!b) return setError('Comment cannot be empty.')
    if (n.length > MAX_NAME) return setError(`Name must be under ${MAX_NAME} characters.`)
    if (b.length > MAX_BODY) return setError(`Comment must be under ${MAX_BODY} characters.`)
    if (!turnstileToken) {
      return setError('Please complete the Turnstile challenge above.')
    }

    setPosting(true)
    try {
      const r = await fetch(`${API}/api/comments/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          page: pathname,
          author: n,
          body: b,
          client_ts: new Date().toISOString(),
          website,
          turnstile_token: turnstileToken,
        }),
      })
      const data = await r.json()
      if (!r.ok || data.error) {
        setError(data.error || 'Could not post comment.')
        resetTurnstile()
      } else {
        setBody('')
        try { localStorage.setItem('oxidb-comment-author', n) } catch {}
        resetTurnstile()
        await load()
      }
    } catch {
      setError('Network error. Try again.')
      resetTurnstile()
    } finally {
      setPosting(false)
    }
  }

  return (
    <section className="comments">
      <div className="comments-inner">
        <h3 className="comments-h">
          Comments
          {!loading && comments.length > 0 && (
            <span className="comments-count">{comments.length}</span>
          )}
        </h3>

        {loading ? (
          <p className="comments-empty">Loading…</p>
        ) : comments.length === 0 ? (
          <p className="comments-empty">Be the first to comment.</p>
        ) : (
          <ul className="comments-list">
            {comments.map((c) => (
              <li key={c._id} className="comment">
                <div className="comment-meta">
                  <span className="comment-author">{c.author}</span>
                  <span className="comment-time">{timeAgo(c.client_ts)}</span>
                </div>
                <div className="comment-body">{c.body}</div>
              </li>
            ))}
          </ul>
        )}

        <form className="comments-form" onSubmit={submit}>
          {/* Honeypot — visually hidden, off-screen, untabbable, no autofill. Real users won't fill it. */}
          <div
            aria-hidden="true"
            style={{ position: 'absolute', left: '-10000px', top: 'auto', width: 1, height: 1, overflow: 'hidden' }}
          >
            <label htmlFor="oxidb-cmt-website">Website (leave empty)</label>
            <input
              type="text"
              id="oxidb-cmt-website"
              name="website"
              tabIndex={-1}
              autoComplete="off"
              value={website}
              onChange={(e) => setWebsite(e.target.value)}
            />
          </div>

          <div className="comments-row">
            <input
              type="text"
              className="comments-input"
              placeholder="Your name"
              maxLength={MAX_NAME}
              value={name}
              onChange={(e) => setName(e.target.value)}
              disabled={posting}
            />
          </div>
          <div className="comments-row">
            <textarea
              className="comments-textarea"
              placeholder="Write a comment…"
              rows={4}
              maxLength={MAX_BODY}
              value={body}
              onChange={(e) => setBody(e.target.value)}
              disabled={posting}
            />
          </div>

          <div className="comments-row comments-turnstile">
            <div ref={widgetRef} />
          </div>

          {error && <p className="comments-error">{error}</p>}
          <div className="comments-actions">
            <span className="comments-hint">
              Stored in OxiDB. Spam-checked by Cloudflare Turnstile.{' '}
              <a href="/oxiscript/">How?</a>
            </span>
            <button type="submit" className="comments-btn" disabled={posting || !turnstileToken}>
              {posting ? 'Posting…' : 'Post comment'}
            </button>
          </div>
        </form>
      </div>
    </section>
  )
}
