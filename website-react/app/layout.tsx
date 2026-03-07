import type { Metadata } from 'next'
import Nav from '@/components/Nav'
import Footer from '@/components/Footer'
import './globals.css'

export const metadata: Metadata = {
  title: {
    default: 'OxiDB - Fast Versatile Document Database',
    template: '%s | OxiDB',
  },
  description:
    'A fast, versatile document database. JSON & SQL queries, ACID transactions, full-text & vector search, Raft replication, encryption at rest.',
  keywords: [
    'database', 'document database', 'nosql', 'embedded database', 'rust',
    'json', 'sql', 'full-text search', 'vector search', 'transactions',
  ],
  openGraph: {
    title: 'OxiDB - Fast Versatile Document Database',
    description: 'A fast, versatile document database.',
    url: 'https://oxidb.baltavista.com',
    siteName: 'OxiDB',
    type: 'website',
  },
  metadataBase: new URL('https://oxidb.baltavista.com'),
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>
        <Nav />
        {children}
        <Footer />
      </body>
    </html>
  )
}
