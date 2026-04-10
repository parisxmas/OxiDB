'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useState } from 'react'

const navItems = [
  { href: '/downloads', label: 'Downloads' },
  { href: '/features', label: 'Features' },
  { href: '/quickstart', label: 'Quick Start' },
  { href: '/docs', label: 'Docs' },
  { href: '/queries', label: 'Queries' },
  { href: '/updates', label: 'Updates' },
  { href: '/aggregation', label: 'Aggregation' },
  { href: '/indexes', label: 'Indexes' },
  { href: '/transactions', label: 'Transactions' },
  { href: '/sql', label: 'SQL' },
  { href: '/search', label: 'Search' },
  { href: '/vectors', label: 'Vectors' },
  { href: '/blobs', label: 'Blobs' },
  { href: '/streams', label: 'Streams' },
  { href: '/server', label: 'Server' },
  { href: '/clients', label: 'Clients' },
  { href: '/wasm', label: 'WebAssembly' },
  { href: '/go-examples', label: 'Go Examples' },
  { href: '/python-examples', label: 'Python Examples' },
  { href: '/storage', label: 'Storage' },
  { href: '/benchmarks', label: 'Benchmarks' },
  { href: '/changelog', label: 'Changelog' },
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
          {navItems.map(({ href, label }) => (
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
