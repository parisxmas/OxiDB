'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useEffect, useState } from 'react'

const navGroups = [
  {
    title: 'Getting Started',
    items: [
      { href: '/quickstart', label: 'Quick Start' },
      { href: '/features', label: 'Features' },
      { href: '/downloads', label: 'Downloads' },
    ],
  },
  {
    title: 'Document Engine',
    items: [
      { href: '/queries', label: 'Queries' },
      { href: '/updates', label: 'Updates' },
      { href: '/aggregation', label: 'Aggregation' },
      { href: '/indexes', label: 'Indexes' },
      { href: '/transactions', label: 'Transactions' },
      { href: '/search', label: 'Search' },
      { href: '/vectors', label: 'Vectors' },
    ],
  },
  {
    title: 'SQL Engine',
    items: [{ href: '/sql', label: 'SQL Reference' }],
  },
  {
    title: 'Time-Series Engine',
    items: [{ href: '/tsdb', label: 'Time-Series' }],
  },
  {
    title: 'Storage & Realtime',
    items: [
      { href: '/blobs', label: 'Blobs (S3)' },
      { href: '/streams', label: 'Streams' },
      { href: '/storage', label: 'Storage' },
    ],
  },
  {
    title: 'Server & Clients',
    items: [
      { href: '/server', label: 'Server' },
      { href: '/clients', label: 'Clients' },
      { href: '/wasm', label: 'WebAssembly' },
      { href: '/benchmarks', label: 'Benchmarks' },
    ],
  },
  {
    title: 'Reference',
    items: [
      { href: '/docs', label: 'Docs' },
      { href: '/book', label: 'Book' },
      { href: '/dotnet-examples', label: '.NET / EF Core' },
      { href: '/go-examples', label: 'Go Examples' },
      { href: '/python-examples', label: 'Python Examples' },
      { href: '/changelog', label: 'Changelog' },
      { href: '/license', label: 'License' },
    ],
  },
]

const isActive = (pathname: string, href: string) =>
  pathname === href || pathname === href + '/'

function groupOf(pathname: string) {
  const g = navGroups.find((grp) => grp.items.some((i) => isActive(pathname, i.href)))
  return g ? g.title : 'Getting Started'
}

export default function Nav() {
  const pathname = usePathname()
  const [open, setOpen] = useState(false)
  // Only the group holding the current page is expanded by default — keeps the
  // sidebar short (no scrollbar). Any group can be toggled open/closed.
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  // Expand the group of the current page whenever navigation happens.
  useEffect(() => {
    const active = groupOf(pathname)
    setExpanded((e) => (e[active] ? e : { ...e, [active]: true }))
  }, [pathname])

  const isOpen = (title: string) =>
    expanded[title] ?? title === groupOf(pathname)
  const toggle = (title: string) =>
    setExpanded((e) => ({ ...e, [title]: !isOpen(title) }))

  return (
    <nav className={`nav${open ? ' open' : ''}`}>
      <div className="container nav-inner">
        <Link href="/" className="logo" onClick={() => setOpen(false)}>
          Oxi<span>DB</span>
        </Link>
        <div className="nav-links">
          {navGroups.map((group) => {
            const groupOpen = isOpen(group.title)
            return (
              <div className={`nav-group${groupOpen ? ' expanded' : ''}`} key={group.title}>
                <button
                  className="nav-group-title"
                  onClick={() => toggle(group.title)}
                  aria-expanded={groupOpen}
                >
                  <span>{group.title}</span>
                  <svg
                    className="nav-chevron"
                    width="12"
                    height="12"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <polyline points="6 9 12 15 18 9" />
                  </svg>
                </button>
                <div className="nav-group-items">
                  {group.items.map(({ href, label }) => (
                    <Link
                      key={href}
                      href={href}
                      className={isActive(pathname, href) ? 'active' : ''}
                      onClick={() => setOpen(false)}
                    >
                      {label}
                    </Link>
                  ))}
                </div>
              </div>
            )
          })}
        </div>
        <button
          className="nav-toggle"
          onClick={() => setOpen(!open)}
          aria-label="Menu"
          aria-expanded={open}
        >
          <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
            <path d="M3 5h14M3 10h14M3 15h14" stroke="currentColor" strokeWidth="1.5" />
          </svg>
        </button>
      </div>
    </nav>
  )
}
