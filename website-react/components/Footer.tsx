import Link from 'next/link'

export default function Footer() {
  return (
    <footer className="footer">
      <div className="container">
        <div className="footer-inner">
          <div className="footer-brand">
            <span className="logo">
              Oxi<span>DB</span>
            </span>
            <p>A fast, versatile document database.</p>
          </div>
          <div className="footer-links">
            <div>
              <h4>Resources</h4>
              <a href="https://crates.io/crates/oxidb">crates.io</a>
            </div>
            <div>
              <h4>Docs</h4>
              <Link href="/quickstart">Quick Start</Link>
              <Link href="/queries">Query API</Link>
              <Link href="/server">Server</Link>
              <Link href="/clients">Client Libraries</Link>
            </div>
          </div>
        </div>
        <div className="footer-bottom">
          <p>OxiDB v0.18.0 -- Built with Rust</p>
        </div>
      </div>
    </footer>
  )
}
