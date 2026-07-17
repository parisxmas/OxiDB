import type { Metadata } from 'next'
import Nav from '@/components/Nav'
import Footer from '@/components/Footer'
import './globals.css'

export const metadata: Metadata = {
  title: {
    default: 'OxiDB - Fast Multi-Model Database',
    template: '%s | OxiDB',
  },
  description:
    'A fast, multi-model database — document, SQL, and time-series in one binary. MongoDB-style JSON queries, a full SQL engine with an EF Core provider that beats PostgreSQL, ACID transactions, full-text & vector search, Raft replication.',
  keywords: [
    'database', 'multi-model database', 'document database', 'sql database',
    'time series database', 'nosql', 'embedded database', 'rust', 'json',
    'mongodb', 'ef core', 'full-text search', 'vector search', 'transactions',
  ],
  openGraph: {
    title: 'OxiDB - Fast Multi-Model Database',
    description:
      'MongoDB-style JSON queries. ACID transactions. Sharded routing via oxipool. Persistent Raft replication. Verified at 1M records under failover. v0.36.0.',
    url: 'https://oxidb.baltavista.com',
    siteName: 'OxiDB',
    type: 'website',
    images: [
      {
        url: '/og-card.png?v=0352',
        width: 1200,
        height: 630,
        alt: 'OxiDB v0.36.0 — sharded, Raft-replicated, persistent. 1M records verified under failover.',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'OxiDB - Fast Multi-Model Database',
    description:
      'MongoDB-style JSON queries. ACID transactions. Sharded routing via oxipool. Persistent Raft replication. v0.36.0.',
    images: ['/og-card.png?v=0352'],
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
        {/* Set the theme before first paint so there is no flash. Defaults to
            dark; light only when the visitor has chosen it. */}
        <script
          dangerouslySetInnerHTML={{
            __html: `(function(){try{var t=localStorage.getItem('theme');if(t!=='light'&&t!=='dark'){t='dark';}document.documentElement.setAttribute('data-theme',t);}catch(e){document.documentElement.setAttribute('data-theme','dark');}})();`,
          }}
        />
        <Nav />
        <div className="dev-banner" role="alert">
          <span className="dev-banner-dot" />
          <span>
            <strong>Under active development.</strong> OxiDB is not yet
            recommended for production use.
          </span>
        </div>
        {children}
        {/* Site-wide: a quick link to the bug tracker from any page. */}
        <a href="/bugs/" className="report-tab" aria-label="Report an issue">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <rect x="8" y="6" width="8" height="14" rx="4" />
            <path d="M19 7l-3 2M5 7l3 2M18 13h3M3 13h3M19 18l-3-1.5M5 18l3-1.5M12 2v4M9 4l1.5 2M15 4l-1.5 2" />
          </svg>
          <span>Report Issue</span>
        </a>
        <Footer />
      </body>
    </html>
  )
}
