'use client'

import { useCallback, useEffect, useRef, useState } from 'react'

// Configured at build time (Next inlines NEXT_PUBLIC_* into the static export).
// The client id is the SAME OAuth 2.0 Web client id the API validates against.
const GOOGLE_CLIENT_ID = process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID || ''
// Same origin in production (nginx proxies /bugs-api to the container); override
// to e.g. http://localhost:8124 when running `next dev` against a local API.
const API_BASE = process.env.NEXT_PUBLIC_BUGS_API || '/bugs-api'

type Me = { signedIn: boolean; Name?: string; Email?: string; Picture?: string; IsAdmin?: boolean }
type BugRow = {
  Id: number; Title: string; Status: string; Reporter: string
  CreatedUtc: string; UpdatedUtc: string; Comments: number
}
type Comment = { Id: number; Body: string; Author: string; IsAdmin: boolean; CreatedUtc: string }
type BugDetail = BugRow & { Body: string; Comments: Comment[] }

declare global {
  interface Window {
    google?: any
    __bugsOnCredential?: (r: { credential: string }) => void
  }
}

function timeAgo(iso: string): string {
  const d = new Date(iso).getTime()
  const s = Math.max(1, Math.floor((Date.now() - d) / 1000))
  if (s < 60) return `${s}s ago`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  const days = Math.floor(h / 24)
  if (days < 30) return `${days}d ago`
  return new Date(iso).toLocaleDateString()
}

export default function BugsPage() {
  const [token, setToken] = useState<string | null>(null)
  const [me, setMe] = useState<Me | null>(null)
  const [bugs, setBugs] = useState<BugRow[]>([])
  const [filter, setFilter] = useState<'open' | 'closed' | 'all'>('open')
  const [query, setQuery] = useState('')
  const [selected, setSelected] = useState<BugDetail | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // New-bug form
  const [showForm, setShowForm] = useState(false)
  const [title, setTitle] = useState('')
  const [body, setBody] = useState('')
  const [submitting, setSubmitting] = useState(false)

  const [comment, setComment] = useState('')
  // Two-step delete confirmation (no native dialog) — admin only.
  const [confirmDelete, setConfirmDelete] = useState(false)
  const signInDiv = useRef<HTMLDivElement>(null)

  const authHeaders = useCallback((): Record<string, string> => {
    const h: Record<string, string> = { 'Content-Type': 'application/json' }
    if (token) h.Authorization = `Bearer ${token}`
    return h
  }, [token])

  // ── Load the token we already have, once. ──────────────────────────────────
  useEffect(() => {
    setToken(localStorage.getItem('bugs_token'))
  }, [])

  // ── Google Identity Services: load the script, wire the callback. ──────────
  useEffect(() => {
    if (!GOOGLE_CLIENT_ID) return

    window.__bugsOnCredential = (resp) => {
      localStorage.setItem('bugs_token', resp.credential)
      setToken(resp.credential)
    }

    const init = () => {
      if (!window.google?.accounts?.id) return
      window.google.accounts.id.initialize({
        client_id: GOOGLE_CLIENT_ID,
        callback: (r: { credential: string }) => window.__bugsOnCredential?.(r),
      })
      renderButton()
    }

    if (window.google?.accounts?.id) {
      init()
    } else {
      const existing = document.getElementById('gsi-script')
      if (existing) {
        existing.addEventListener('load', init)
      } else {
        const s = document.createElement('script')
        s.src = 'https://accounts.google.com/gsi/client'
        s.async = true
        s.id = 'gsi-script'
        s.onload = init
        document.head.appendChild(s)
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const renderButton = useCallback(() => {
    if (window.google?.accounts?.id && signInDiv.current) {
      signInDiv.current.innerHTML = ''
      window.google.accounts.id.renderButton(signInDiv.current, {
        theme: 'outline',
        size: 'large',
        text: 'signin_with',
        shape: 'pill',
      })
    }
  }, [])

  // Re-render the Google button whenever we are signed out and the slot exists.
  useEffect(() => {
    if (!token && me?.signedIn !== true) renderButton()
  }, [token, me, renderButton])

  // ── Resolve identity from the token. ───────────────────────────────────────
  useEffect(() => {
    if (!token) {
      setMe(null)
      return
    }
    fetch(`${API_BASE}/me`, { headers: { Authorization: `Bearer ${token}` } })
      .then((r) => r.json())
      .then((m: Me) => {
        if (!m.signedIn) {
          // Expired / invalid — drop it and show the button again.
          localStorage.removeItem('bugs_token')
          setToken(null)
          setMe(null)
        } else {
          setMe(m)
        }
      })
      .catch(() => setMe(null))
  }, [token])

  // ── Load the list. ─────────────────────────────────────────────────────────
  const loadBugs = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams()
      if (filter !== 'all') params.set('status', filter)
      if (query.trim()) params.set('q', query.trim())
      const r = await fetch(`${API_BASE}/bugs?${params.toString()}`)
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      setBugs(await r.json())
    } catch (e: any) {
      setError('Could not reach the bug tracker API.')
    } finally {
      setLoading(false)
    }
  }, [filter, query])

  useEffect(() => {
    loadBugs()
  }, [loadBugs])

  const openDetail = useCallback(async (id: number) => {
    setConfirmDelete(false)
    const r = await fetch(`${API_BASE}/bugs/${id}`)
    if (r.ok) setSelected(await r.json())
  }, [])

  const signOut = () => {
    localStorage.removeItem('bugs_token')
    setToken(null)
    setMe(null)
    window.google?.accounts?.id?.disableAutoSelect?.()
  }

  const submitBug = async () => {
    setSubmitting(true)
    setError(null)
    try {
      const r = await fetch(`${API_BASE}/bugs`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ title, body }),
      })
      const data = await r.json().catch(() => ({}))
      if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`)
      setTitle('')
      setBody('')
      setShowForm(false)
      await loadBugs()
    } catch (e: any) {
      setError(e.message || 'Failed to submit.')
    } finally {
      setSubmitting(false)
    }
  }

  const submitComment = async () => {
    if (!selected) return
    const r = await fetch(`${API_BASE}/bugs/${selected.Id}/comments`, {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify({ body: comment }),
    })
    if (r.ok) {
      setComment('')
      await openDetail(selected.Id)
      await loadBugs()
    }
  }

  const setStatus = async (id: number, status: 'open' | 'closed') => {
    const r = await fetch(`${API_BASE}/bugs/${id}`, {
      method: 'PATCH',
      headers: authHeaders(),
      body: JSON.stringify({ status }),
    })
    if (r.ok) {
      await openDetail(id)
      await loadBugs()
    }
  }

  const deleteBug = async (id: number) => {
    const r = await fetch(`${API_BASE}/bugs/${id}`, {
      method: 'DELETE',
      headers: authHeaders(),
    })
    if (r.ok) {
      setConfirmDelete(false)
      setSelected(null)
      await loadBugs()
    }
  }

  const signedIn = me?.signedIn === true

  return (
    <section className="section">
      <div className="container">
        <h2>
          <svg
            className="section-icon"
            width="22"
            height="22"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <rect x="8" y="6" width="8" height="14" rx="4" />
            <path d="M19 7l-3 2M5 7l3 2M18 13h3M3 13h3M19 18l-3-1.5M5 18l3-1.5M12 2v4M9 4l1.5 2M15 4l-1.5 2" />
          </svg>{' '}
          Bug Reports
        </h2>
        <p className="section-desc">
          Found a bug in OxiDB? Open an issue below. Sign in with Google to report
          or comment — the list itself is public. Every report is stored in a{' '}
          <strong>dedicated OxiDB instance</strong> (SQL engine, via EF Core)
          running next to a small .NET&nbsp;API — OxiDB tracking its own bugs.
        </p>

        {!GOOGLE_CLIENT_ID && (
          <div className="bug-warn">
            Google Sign-In is not configured for this build. Set{' '}
            <code>NEXT_PUBLIC_GOOGLE_CLIENT_ID</code> and rebuild.
          </div>
        )}

        {/* ── Identity bar ──────────────────────────────────────────────── */}
        <div className="bug-authbar">
          {signedIn ? (
            <>
              {me?.Picture && (
                // eslint-disable-next-line @next/next/no-img-element
                <img className="bug-avatar" src={me.Picture} alt="" referrerPolicy="no-referrer" />
              )}
              <span className="bug-who">
                {me?.Name}
                {me?.IsAdmin && <span className="bug-admin-tag">admin</span>}
              </span>
              <button className="btn btn-secondary bug-btn-sm" onClick={signOut}>
                Sign out
              </button>
              <button
                className="btn btn-primary bug-btn-sm"
                onClick={() => setShowForm((v) => !v)}
              >
                {showForm ? 'Cancel' : 'Report a bug'}
              </button>
            </>
          ) : (
            <>
              <span className="bug-who bug-muted">Sign in with Google to report a bug</span>
              <div ref={signInDiv} />
            </>
          )}
        </div>

        {/* ── New-bug form ──────────────────────────────────────────────── */}
        {signedIn && showForm && (
          <div className="bug-form">
            <input
              className="bug-input"
              placeholder="Short, specific title (e.g. $regex ignores anchors on indexed fields)"
              value={title}
              maxLength={200}
              onChange={(e) => setTitle(e.target.value)}
            />
            <textarea
              className="bug-input bug-textarea"
              placeholder={
                'What happened, what you expected, and steps to reproduce.\nInclude version, OS, and a minimal query if you can.'
              }
              value={body}
              maxLength={20000}
              onChange={(e) => setBody(e.target.value)}
            />
            <div className="bug-form-actions">
              <button
                className="btn btn-primary"
                onClick={submitBug}
                disabled={submitting || title.trim().length < 3 || body.trim().length < 5}
              >
                {submitting ? 'Submitting…' : 'Submit report'}
              </button>
            </div>
          </div>
        )}

        {error && <div className="bug-warn">{error}</div>}

        {/* ── Filters ───────────────────────────────────────────────────── */}
        <div className="bug-toolbar">
          <div className="bug-tabs">
            {(['open', 'closed', 'all'] as const).map((f) => (
              <button
                key={f}
                className={`bug-tab${filter === f ? ' active' : ''}`}
                onClick={() => setFilter(f)}
              >
                {f[0].toUpperCase() + f.slice(1)}
              </button>
            ))}
          </div>
          <input
            className="bug-input bug-search"
            placeholder="Search…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>

        {/* ── List ──────────────────────────────────────────────────────── */}
        {loading ? (
          <p className="bug-muted">Loading…</p>
        ) : bugs.length === 0 ? (
          <p className="bug-muted">No bugs here. Either it is quiet, or you found the first one.</p>
        ) : (
          <ul className="bug-list">
            {bugs.map((b) => (
              <li key={b.Id} className="bug-item" onClick={() => openDetail(b.Id)}>
                <span className={`bug-status bug-status-${b.Status}`}>{b.Status}</span>
                <div className="bug-item-main">
                  <span className="bug-item-title">{b.Title}</span>
                  <span className="bug-item-meta">
                    #{b.Id} · {b.Reporter} · opened {timeAgo(b.CreatedUtc)}
                  </span>
                </div>
                {b.Comments > 0 && <span className="bug-comment-count">{b.Comments}</span>}
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* ── Detail drawer ───────────────────────────────────────────────── */}
      {selected && (
        <div className="bug-overlay" onClick={() => setSelected(null)}>
          <div className="bug-drawer" onClick={(e) => e.stopPropagation()}>
            <button className="bug-close" onClick={() => setSelected(null)} aria-label="Close">
              ×
            </button>
            <div className="bug-drawer-head">
              <span className={`bug-status bug-status-${selected.Status}`}>{selected.Status}</span>
              <h3 className="bug-drawer-title">{selected.Title}</h3>
            </div>
            <p className="bug-item-meta">
              #{selected.Id} · reported by {selected.Reporter} · {timeAgo(selected.CreatedUtc)}
            </p>

            {me?.IsAdmin && (
              <div className="bug-admin-actions">
                {selected.Status === 'open' ? (
                  <button
                    className="btn btn-secondary bug-btn-sm"
                    onClick={() => setStatus(selected.Id, 'closed')}
                  >
                    Close bug
                  </button>
                ) : (
                  <button
                    className="btn btn-secondary bug-btn-sm"
                    onClick={() => setStatus(selected.Id, 'open')}
                  >
                    Reopen
                  </button>
                )}
                {!confirmDelete ? (
                  <button
                    className="btn btn-secondary bug-btn-sm bug-delete"
                    onClick={() => setConfirmDelete(true)}
                  >
                    Delete
                  </button>
                ) : (
                  <>
                    <button
                      className="btn bug-btn-sm bug-delete-confirm"
                      onClick={() => deleteBug(selected.Id)}
                    >
                      Confirm delete
                    </button>
                    <button
                      className="btn btn-secondary bug-btn-sm"
                      onClick={() => setConfirmDelete(false)}
                    >
                      Cancel
                    </button>
                  </>
                )}
              </div>
            )}

            <div className="bug-body">{selected.Body}</div>

            <div className="bug-comments">
              <h4>Comments ({selected.Comments.length})</h4>
              {selected.Comments.map((c) => (
                <div key={c.Id} className="bug-comment">
                  <div className="bug-comment-head">
                    <strong>{c.Author}</strong>
                    {c.IsAdmin && <span className="bug-admin-tag">admin</span>}
                    <span className="bug-item-meta">{timeAgo(c.CreatedUtc)}</span>
                  </div>
                  <div className="bug-comment-body">{c.Body}</div>
                </div>
              ))}
              {selected.Comments.length === 0 && (
                <p className="bug-muted">No comments yet.</p>
              )}

              {signedIn ? (
                <div className="bug-comment-form">
                  <textarea
                    className="bug-input bug-textarea"
                    placeholder="Add a comment…"
                    value={comment}
                    maxLength={10000}
                    onChange={(e) => setComment(e.target.value)}
                  />
                  <button
                    className="btn btn-primary bug-btn-sm"
                    onClick={submitComment}
                    disabled={comment.trim().length === 0}
                  >
                    Comment
                  </button>
                </div>
              ) : (
                <p className="bug-muted">Sign in to comment.</p>
              )}
            </div>
          </div>
        </div>
      )}

      <BugStyles />
    </section>
  )
}

// Scoped styles for the bug-specific UI. Everything else reuses the site's
// existing classes (.section, .container, .btn, .btn-primary/secondary).
function BugStyles() {
  return (
    <style>{`
      .bug-authbar { display:flex; align-items:center; gap:.75rem; flex-wrap:wrap;
        margin:1.25rem 0; padding:.75rem 1rem; border:1px solid var(--border,#e5e7eb);
        border-radius:12px; background:var(--card,rgba(127,127,127,.04)); }
      .bug-avatar { width:32px; height:32px; border-radius:50%; }
      .bug-who { font-weight:600; display:flex; align-items:center; gap:.5rem; }
      .bug-muted { opacity:.65; }
      .bug-btn-sm { padding:.4rem .85rem; font-size:.85rem; }
      .bug-admin-tag { font-size:.65rem; font-weight:700; text-transform:uppercase;
        letter-spacing:.04em; padding:.1rem .4rem; border-radius:6px;
        background:#7c3aed; color:#fff; }
      .bug-warn { margin:1rem 0; padding:.75rem 1rem; border-radius:10px;
        border:1px solid #f59e0b55; background:#f59e0b18; font-size:.9rem; }
      .bug-form { display:flex; flex-direction:column; gap:.6rem; margin:1rem 0 1.5rem; }
      .bug-input { width:100%; padding:.6rem .8rem; border-radius:10px;
        border:1px solid var(--border,#d1d5db); background:var(--bg,transparent);
        color:inherit; font:inherit; }
      .bug-textarea { min-height:130px; resize:vertical; line-height:1.5; }
      .bug-form-actions { display:flex; justify-content:flex-end; }
      .bug-toolbar { display:flex; align-items:center; justify-content:space-between;
        gap:1rem; margin:1.25rem 0 .75rem; flex-wrap:wrap; }
      .bug-tabs { display:inline-flex; border:1px solid var(--border,#e5e7eb);
        border-radius:10px; overflow:hidden; }
      .bug-tab { padding:.45rem .95rem; background:transparent; border:0;
        color:inherit; cursor:pointer; font:inherit; opacity:.7; }
      .bug-tab.active { background:var(--accent,#2563eb); color:#fff; opacity:1; }
      .bug-search { max-width:340px; flex:1 1 240px; }
      .bug-list { list-style:none; padding:0; margin:0;
        border:1px solid var(--border,#e5e7eb); border-radius:12px; overflow:hidden; }
      .bug-item { display:flex; align-items:center; gap:.9rem; padding:.85rem 1rem;
        border-top:1px solid var(--border,#eef0f3); cursor:pointer; }
      .bug-item:first-child { border-top:0; }
      .bug-item:hover { background:var(--card,rgba(127,127,127,.05)); }
      .bug-item-main { display:flex; flex-direction:column; gap:.15rem; flex:1; min-width:0; }
      .bug-item-title { font-weight:600; }
      .bug-item-meta { font-size:.8rem; opacity:.6; }
      .bug-comment-count { font-size:.8rem; opacity:.7; padding:.15rem .5rem;
        border:1px solid var(--border,#e5e7eb); border-radius:20px; }
      .bug-status { font-size:.7rem; font-weight:700; text-transform:uppercase;
        letter-spacing:.04em; padding:.2rem .55rem; border-radius:20px; white-space:nowrap; }
      .bug-status-open { background:#16a34a22; color:#16a34a; }
      .bug-status-closed { background:#6b728022; color:#6b7280; }
      .bug-overlay { position:fixed; inset:0; background:rgba(0,0,0,.45);
        display:flex; justify-content:flex-end; z-index:1000; }
      .bug-drawer { width:min(560px,100%); height:100%; overflow-y:auto;
        background:var(--bg,#fff); padding:2rem 1.75rem; position:relative;
        box-shadow:-8px 0 40px rgba(0,0,0,.2); }
      @media (prefers-color-scheme: dark) { .bug-drawer { background:#0e1116; } }
      .bug-close { position:absolute; top:1rem; right:1.25rem; background:transparent;
        border:0; font-size:1.8rem; line-height:1; cursor:pointer; color:inherit; opacity:.6; }
      .bug-drawer-head { display:flex; align-items:center; gap:.6rem; margin-right:2rem; }
      .bug-drawer-title { margin:0; }
      .bug-admin-actions { margin:.85rem 0; display:flex; gap:.5rem; flex-wrap:wrap; }
      .bug-delete { color:#d96b6b; border-color:rgba(217,107,107,.35); }
      .bug-delete:hover { background:rgba(217,107,107,.12); }
      .bug-delete-confirm { background:#d96b6b; color:#fff; border-color:#c0504f; }
      .bug-delete-confirm:hover { background:#c0504f; }
      .bug-body { white-space:pre-wrap; line-height:1.6; margin:1.1rem 0 1.75rem;
        padding:1rem 1.1rem; border-radius:10px; background:var(--card,rgba(127,127,127,.05)); }
      .bug-comments h4 { margin:.5rem 0 1rem; }
      .bug-comment { padding:.75rem 0; border-top:1px solid var(--border,#eef0f3); }
      .bug-comment-head { display:flex; align-items:center; gap:.5rem; margin-bottom:.35rem; }
      .bug-comment-body { white-space:pre-wrap; line-height:1.55; }
      .bug-comment-form { display:flex; flex-direction:column; gap:.6rem; margin-top:1.25rem; }
    `}</style>
  )
}
