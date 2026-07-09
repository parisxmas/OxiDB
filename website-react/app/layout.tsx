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
    'A fast, versatile document database. MongoDB-style JSON queries, ACID transactions, full-text & vector search, Raft replication, encryption at rest.',
  keywords: [
    'database', 'document database', 'nosql', 'embedded database', 'rust',
    'json', 'mongodb', 'full-text search', 'vector search', 'transactions',
  ],
  openGraph: {
    title: 'OxiDB - Fast Versatile Document Database',
    description:
      'MongoDB-style JSON queries. ACID transactions. Sharded routing via oxipool. Persistent Raft replication. Verified at 1M records under failover. v0.34.0.',
    url: 'https://oxidb.baltavista.com',
    siteName: 'OxiDB',
    type: 'website',
    images: [
      {
        url: '/og-card.png',
        width: 1200,
        height: 630,
        alt: 'OxiDB v0.34.0 — sharded, Raft-replicated, persistent. 1M records verified under failover.',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'OxiDB - Fast Versatile Document Database',
    description:
      'MongoDB-style JSON queries. ACID transactions. Sharded routing via oxipool. Persistent Raft replication. v0.34.0.',
    images: ['/og-card.png'],
  },
  icons: {
    icon: '/logo.png',
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
