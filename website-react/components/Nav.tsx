'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useEffect, useState, type ReactNode } from 'react'

// Small line icons (feather/lucide-style), rendered at 15px inside each link.
const ICON: Record<string, ReactNode> = {
  zap: <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />,
  grid: (
    <>
      <rect x="3" y="3" width="7" height="7" />
      <rect x="14" y="3" width="7" height="7" />
      <rect x="14" y="14" width="7" height="7" />
      <rect x="3" y="14" width="7" height="7" />
    </>
  ),
  download: (
    <>
      <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
      <polyline points="7 10 12 15 17 10" />
      <line x1="12" y1="15" x2="12" y2="3" />
    </>
  ),
  search: (
    <>
      <circle cx="11" cy="11" r="8" />
      <line x1="21" y1="21" x2="16.65" y2="16.65" />
    </>
  ),
  edit: (
    <>
      <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" />
      <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
    </>
  ),
  activity: <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />,
  list: (
    <>
      <line x1="8" y1="6" x2="21" y2="6" />
      <line x1="8" y1="12" x2="21" y2="12" />
      <line x1="8" y1="18" x2="21" y2="18" />
      <line x1="3" y1="6" x2="3.01" y2="6" />
      <line x1="3" y1="12" x2="3.01" y2="12" />
      <line x1="3" y1="18" x2="3.01" y2="18" />
    </>
  ),
  shield: <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />,
  target: (
    <>
      <circle cx="12" cy="12" r="10" />
      <circle cx="12" cy="12" r="6" />
      <circle cx="12" cy="12" r="2" />
    </>
  ),
  database: (
    <>
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </>
  ),
  wifi: (
    <>
      <path d="M5 12.55a11 11 0 0114.08 0" />
      <path d="M1.42 9a16 16 0 0121.16 0" />
      <path d="M8.53 16.11a6 6 0 016.95 0" />
      <line x1="12" y1="20" x2="12.01" y2="20" />
    </>
  ),
  radio: (
    <>
      <circle cx="12" cy="12" r="2" />
      <path d="M4.93 19.07a10 10 0 010-14.14" />
      <path d="M7.76 16.24a6 6 0 010-8.49" />
      <path d="M16.24 7.76a6 6 0 010 8.49" />
      <path d="M19.07 4.93a10 10 0 010 14.14" />
    </>
  ),
  drive: (
    <>
      <line x1="22" y1="12" x2="2" y2="12" />
      <path d="M5.45 5.11L2 12v6a2 2 0 002 2h16a2 2 0 002-2v-6l-3.45-6.89A2 2 0 0016.76 4H7.24a2 2 0 00-1.79 1.11z" />
      <line x1="6" y1="16" x2="6.01" y2="16" />
      <line x1="10" y1="16" x2="10.01" y2="16" />
    </>
  ),
  server: (
    <>
      <rect x="2" y="2" width="20" height="8" rx="2" />
      <rect x="2" y="14" width="20" height="8" rx="2" />
      <line x1="6" y1="6" x2="6.01" y2="6" />
      <line x1="6" y1="18" x2="6.01" y2="18" />
    </>
  ),
  code: (
    <>
      <polyline points="16 18 22 12 16 6" />
      <polyline points="8 6 2 12 8 18" />
    </>
  ),
  box: (
    <>
      <path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z" />
      <polyline points="3.27 6.96 12 12.01 20.73 6.96" />
      <line x1="12" y1="22.08" x2="12" y2="12" />
    </>
  ),
  bars: (
    <>
      <line x1="18" y1="20" x2="18" y2="10" />
      <line x1="12" y1="20" x2="12" y2="4" />
      <line x1="6" y1="20" x2="6" y2="14" />
    </>
  ),
  book: (
    <>
      <path d="M4 19.5A2.5 2.5 0 016.5 17H20" />
      <path d="M6.5 2H20v20H6.5A2.5 2.5 0 014 19.5v-15A2.5 2.5 0 016.5 2z" />
    </>
  ),
  terminal: (
    <>
      <polyline points="4 17 10 11 4 5" />
      <line x1="12" y1="19" x2="20" y2="19" />
    </>
  ),
  hash: (
    <>
      <line x1="4" y1="9" x2="20" y2="9" />
      <line x1="4" y1="15" x2="20" y2="15" />
      <line x1="10" y1="3" x2="8" y2="21" />
      <line x1="16" y1="3" x2="14" y2="21" />
    </>
  ),
  file: (
    <>
      <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" />
      <polyline points="14 2 14 8 20 8" />
    </>
  ),
}

const navGroups: {
  title: string
  items: { href: string; label: string; icon: string }[]
}[] = [
  {
    title: 'Getting Started',
    items: [
      { href: '/quickstart', label: 'Quick Start', icon: 'zap' },
      { href: '/features', label: 'Features', icon: 'grid' },
      { href: '/downloads', label: 'Downloads', icon: 'download' },
    ],
  },
  {
    title: 'Document Engine',
    items: [
      { href: '/queries', label: 'Queries', icon: 'search' },
      { href: '/updates', label: 'Updates', icon: 'edit' },
      { href: '/aggregation', label: 'Aggregation', icon: 'activity' },
      { href: '/indexes', label: 'Indexes', icon: 'list' },
      { href: '/transactions', label: 'Transactions', icon: 'shield' },
      { href: '/search', label: 'Search', icon: 'search' },
      { href: '/vectors', label: 'Vectors', icon: 'target' },
    ],
  },
  {
    title: 'SQL Engine',
    items: [
      { href: '/sql', label: 'SQL Reference', icon: 'database' },
      { href: '/procedures', label: 'Stored Procedures', icon: 'code' },
    ],
  },
  {
    title: 'Time-Series Engine',
    items: [{ href: '/tsdb', label: 'Time-Series', icon: 'clock' }],
  },
  {
    title: 'In-Memory & Messaging',
    items: [
      { href: '/oximem', label: 'OxiMem (Redis)', icon: 'zap' },
      { href: '/mqtt', label: 'MQTT Broker', icon: 'wifi' },
    ],
  },
  {
    title: 'Storage & Realtime',
    items: [
      { href: '/blobs', label: 'Blobs (S3)', icon: 'database' },
      { href: '/streams', label: 'Streams', icon: 'radio' },
      { href: '/storage', label: 'Storage', icon: 'drive' },
    ],
  },
  {
    title: 'Server & Clients',
    items: [
      { href: '/server', label: 'Server', icon: 'server' },
      { href: '/clients', label: 'Clients', icon: 'code' },
      { href: '/wasm', label: 'WebAssembly', icon: 'box' },
    ],
  },
  {
    title: 'Reference',
    items: [
      { href: '/docs', label: 'Docs', icon: 'file' },
      { href: '/book', label: 'Book', icon: 'book' },
      { href: '/dotnet-examples', label: '.NET / EF Core', icon: 'hash' },
      { href: '/go-examples', label: 'Go Examples', icon: 'terminal' },
      { href: '/python-examples', label: 'Python Examples', icon: 'terminal' },
      { href: '/changelog', label: 'Changelog', icon: 'clock' },
      { href: '/license', label: 'License', icon: 'file' },
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
                  {group.items.map(({ href, label, icon }) => (
                    <Link
                      key={href}
                      href={href}
                      className={isActive(pathname, href) ? 'active' : ''}
                      onClick={() => setOpen(false)}
                    >
                      <svg
                        className="nav-icon"
                        width="15"
                        height="15"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      >
                        {ICON[icon]}
                      </svg>
                      <span>{label}</span>
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
