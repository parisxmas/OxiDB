'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

type Item = { href: string; label: string }
type Group = { label: string; items: Item[] }

const groups: Group[] = [
  {
    label: 'Getting Started',
    items: [
      { href: '/oxiscript/tutorial/', label: 'Tutorial overview' },
      { href: '/oxiscript/getting-started/install/', label: '1. Install & enable' },
      { href: '/oxiscript/getting-started/hello-world/', label: '2. Hello, OxiScript' },
      { href: '/oxiscript/getting-started/first-procedure/', label: '3. Your first real procedure' },
    ],
  },
  {
    label: 'Language Syntax',
    items: [
      { href: '/oxiscript/syntax/types/', label: 'Types & literals' },
      { href: '/oxiscript/syntax/variables/', label: 'Variables (let)' },
      { href: '/oxiscript/syntax/operators/', label: 'Operators' },
      { href: '/oxiscript/syntax/control-flow/', label: 'if / else' },
      { href: '/oxiscript/syntax/loops/', label: 'for / in loops' },
      { href: '/oxiscript/syntax/comments/', label: 'Comments' },
    ],
  },
  {
    label: 'Database Operations',
    items: [
      { href: '/oxiscript/db/find/', label: 'find / find_one' },
      { href: '/oxiscript/db/insert/', label: 'insert' },
      { href: '/oxiscript/db/update/', label: 'update / update_one' },
      { href: '/oxiscript/db/delete/', label: 'delete / delete_one' },
      { href: '/oxiscript/db/count/', label: 'count' },
      { href: '/oxiscript/db/aggregate/', label: 'aggregate' },
    ],
  },
  {
    label: 'Patterns',
    items: [
      { href: '/oxiscript/patterns/validation/', label: 'Input validation' },
      { href: '/oxiscript/patterns/transactions/', label: 'Atomic transactions' },
      { href: '/oxiscript/patterns/composition/', label: 'Procedure composition' },
      { href: '/oxiscript/patterns/upsert-soft-delete/', label: 'Upsert & soft-delete' },
    ],
  },
  {
    label: 'Real-world Recipes',
    items: [
      { href: '/oxiscript/recipes/banking/', label: 'Banking & transfers' },
      { href: '/oxiscript/recipes/ecommerce/', label: 'E-commerce orders' },
      { href: '/oxiscript/recipes/inventory/', label: 'Inventory & restock' },
      { href: '/oxiscript/recipes/audit-log/', label: 'Audit log' },
      { href: '/oxiscript/recipes/rate-limiting/', label: 'Rate limiting' },
      { href: '/oxiscript/recipes/leaderboard/', label: 'Leaderboards' },
    ],
  },
  {
    label: 'API Reference',
    items: [
      { href: '/oxiscript/api/tcp/', label: 'TCP / OxiWire commands' },
      { href: '/oxiscript/api/rest/', label: 'REST endpoints' },
      { href: '/oxiscript/api/sdks/', label: 'SDKs (Go, Python, .NET)' },
    ],
  },
]

function isActive(pathname: string, href: string): boolean {
  if (pathname === href) return true
  if (pathname === href + '/') return true
  if (pathname.replace(/\/$/, '') === href.replace(/\/$/, '')) return true
  return false
}

function groupContainsPath(group: Group, pathname: string): boolean {
  return group.items.some((it) => isActive(pathname, it.href))
}

export default function OxiscriptSidebar() {
  const pathname = usePathname() || ''
  return (
    <aside className="docs-sidebar">
      <h3>
        <Link href="/oxiscript/">OxiScript</Link>
      </h3>
      {groups.map((group) => {
        const open = groupContainsPath(group, pathname)
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
