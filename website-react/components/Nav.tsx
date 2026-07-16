'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useState } from 'react'

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
      { href: '/go-examples', label: 'Go Examples' },
      { href: '/python-examples', label: 'Python Examples' },
      { href: '/changelog', label: 'Changelog' },
      { href: '/license', label: 'License' },
    ],
  },
]

export default function Nav() {
  const pathname = usePathname()
  const [open, setOpen] = useState(false)

  return (
    <nav className={`nav${open ? ' open' : ''}`}>
      <div className="container nav-inner">
        <Link href="/" className="logo">
          Oxi<span>DB</span>
        </Link>
        <div className="nav-links">
          {navGroups.map((group) => (
            <div className="nav-group" key={group.title}>
              <div className="nav-group-title">{group.title}</div>
              {group.items.map(({ href, label }) => (
                <Link
                  key={href}
                  href={href}
                  className={pathname === href || pathname === href + '/' ? 'active' : ''}
                  onClick={() => setOpen(false)}
                >
                  {label}
                </Link>
              ))}
            </div>
          ))}
        </div>
        <button
          className="nav-toggle"
          onClick={() => setOpen(!open)}
          aria-label="Menu"
        >
          <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
            <path d="M3 5h14M3 10h14M3 15h14" stroke="currentColor" strokeWidth="1.5" />
          </svg>
        </button>
      </div>
    </nav>
  )
}
