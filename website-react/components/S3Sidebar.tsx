'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

type Item = { href: string; label: string }
type Group = { label: string; items: Item[] }

const groups: Group[] = [
  {
    label: 'Get Going',
    items: [
      { href: '/s3/', label: 'Overview' },
      { href: '/s3/quickstart/', label: 'Quick Start' },
      { href: '/s3/auth/', label: 'Authentication' },
    ],
  },
  {
    label: 'API',
    items: [
      { href: '/s3/buckets/', label: 'Buckets' },
      { href: '/s3/objects/', label: 'Objects' },
      { href: '/s3/multipart/', label: 'Multipart upload' },
      { href: '/s3/encryption/', label: 'Encryption (SSE)' },
    ],
  },
  {
    label: 'Use it From',
    items: [
      { href: '/s3/clients/', label: 'AWS CLI · boto3 · mc · JS' },
    ],
  },
]

function isActive(p: string, h: string): boolean {
  if (p === h) return true
  if (p === h + '/') return true
  return p.replace(/\/$/, '') === h.replace(/\/$/, '')
}

export default function S3Sidebar() {
  const pathname = usePathname() || ''
  return (
    <aside className="docs-sidebar">
      <h3><Link href="/s3/">S3 API</Link></h3>
      {groups.map((group) => {
        const open = group.items.some((it) => isActive(pathname, it.href))
        return (
          <details key={group.label} open={open}>
            <summary>{group.label}</summary>
            <ul>
              {group.items.map((it) => (
                <li key={it.href}>
                  <Link
                    href={it.href}
                    className={isActive(pathname, it.href) ? 'docs-active' : ''}
                  >
                    {it.label}
                  </Link>
                </li>
              ))}
            </ul>
          </details>
        )
      })}
    </aside>
  )
}
