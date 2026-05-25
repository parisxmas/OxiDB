import OxiscriptSidebar from '@/components/OxiscriptSidebar'

export default function OxiscriptLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <div className="docs-shell">
      <OxiscriptSidebar />
      <main className="docs-content">{children}</main>
    </div>
  )
}
