import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'Bug Reports',
  description:
    'Report a bug in OxiDB. Sign in with Google to open an issue or comment. Every report is stored in a dedicated OxiDB instance behind a .NET API.',
}

export default function BugsLayout({ children }: { children: React.ReactNode }) {
  return children
}
